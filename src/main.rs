use axum::{
    body::Body,
    extract::State,
    http::{HeaderValue, Request, StatusCode},
    response::{IntoResponse, Response},
    routing::any,
    Router,
};
use ipnet::IpNet;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    net::{IpAddr, SocketAddr},
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::sync::RwLock;
use tokio_stream::wrappers::ReceiverStream;
use tracing::{debug, error, info, trace, warn};

// ── Config ──────────────────────────────────────────────────────────

#[derive(Deserialize, Clone)]
struct Config {
    listen: String,
    #[allow(dead_code)]
    strategy: Option<String>,
    rate_limit_cooldown_secs: Option<u64>,
    /// Seconds between utilization probes per account (0 = disabled). Default: 300 (5 min)
    probe_interval_secs: Option<u64>,
    /// Shared secret clients must send as x-api-key to access the proxy. None = open.
    proxy_key: Option<String>,
    /// Source IP allowlist. Supports individual IPs and CIDR ranges. None/empty = allow all.
    allowed_ips: Option<Vec<String>>,
    /// Unified routing endpoints. Each entry is either Anthropic-native or
    /// OpenAI-compatible, distinguished by `protocol`. The sole endpoint pool —
    /// the legacy [[accounts]] / [[upstreams]] / fallback_upstream keys are
    /// rejected at startup by `reject_legacy_config_keys`.
    #[serde(default)]
    endpoints: Vec<EndpointConfig>,
    /// IP-to-client-name mapping. Falls back to x-client-id header, then "-".
    #[serde(default)]
    client_names: HashMap<String, String>,
    /// Auto-inject prompt cache breakpoints for requests without them. Default: true.
    auto_cache: Option<bool>,
    /// Path for JSONL shadow log of request metadata. None = disabled.
    shadow_log: Option<String>,
    /// Per-client daily token budgets: client_id → max tokens per day. Uncapped if absent.
    #[serde(default)]
    client_budgets: HashMap<String, u64>,
    /// Per-client utilization limits: client_id → max utilization (0.0-1.0).
    /// Client gets 429 when ALL model-compatible accounts exceed their limit.
    #[serde(default)]
    client_utilization_limits: HashMap<String, f64>,
    /// Client IDs that bypass all budget/ceiling/emergency checks.
    /// NOTE: operator identity is trust-based (all users are trusted). The x-client-id
    /// header is not verified against client_names IP mapping.
    #[serde(default)]
    operators: Vec<String>,
    /// Enable the emergency brake. Default: true.
    emergency_brake: Option<bool>,
    /// Emergency brake threshold (0.0-1.0). When ALL accounts exceed this,
    /// non-operator traffic is blocked. Default: 0.88.
    emergency_threshold: Option<f64>,
    /// Utilization soft ceiling (0.0–1.0). Accounts above this are excluded from routing
    /// unless ALL accounts exceed it. Breaks client affinity stickiness on overloaded accounts.
    /// Default: 0.90.
    soft_limit: Option<f64>,
    /// Redis/Valkey URL for distributed state. When set, budget enforcement and hard-limit
    /// propagation use Redis for cross-replica coordination. None = local-only (single instance).
    redis_url: Option<String>,
    /// Path for debug log file. When set, writes debug-level logs to this file while
    /// keeping info-level on stderr. For investigating cache/auth behavior.
    debug_log: Option<String>,
    /// Priority penalty added to an account's effective priority tier while it is
    /// serving via Anthropic overage (paid extra usage). Keeps free subscription
    /// capacity preferred over paid overage. Default: 10.
    overage_penalty: Option<u32>,
    /// Aggregate in-flight request-body memory budget, in MiB. New requests are
    /// load-shed with `503 + Retry-After` once the sum of in-flight request
    /// bodies would exceed this, bounding peak memory under bursts of concurrent
    /// large requests. Default: 128. Set to 0 to disable (unbounded — old behavior).
    max_inflight_body_mb: Option<u64>,
    /// Wall-clock ceiling (seconds) for receiving a request body. A slow or
    /// stalled client is shed with `408` when the body has not fully arrived
    /// within this window, releasing its body-memory reservation — otherwise a
    /// handful of hung uploads could pin the `max_inflight_body_mb` budget
    /// indefinitely. Default: 60. Set to 0 to disable.
    body_read_timeout_secs: Option<u64>,
}

#[derive(Deserialize, Clone)]
struct EndpointConfig {
    name: String,
    /// Wire format. Default: anthropic (sends to api.anthropic.com via x-api-key).
    #[serde(default)]
    protocol: Protocol,
    /// Override base URL. For protocol = anthropic, defaults to https://api.anthropic.com.
    /// For protocol = openai, this field is required (validated at startup).
    base_url: Option<String>,
    /// Auth credential. "passthrough" (anthropic only) forwards caller's auth headers.
    token: String,
    /// Model allowlist (supports "*" suffix wildcards). Empty = all models.
    #[serde(default)]
    models: Vec<String>,
    /// Priority tier (0 = highest). Lower tiers tried first.
    #[serde(default)]
    priority: u32,
    /// Whether this account's plan includes Fable usage. Max plans include
    /// Fable up to 50% of the weekly limit; Pro / standard Team plans bill
    /// Fable as paid credits from the first token. Set to false for such
    /// accounts: Fable requests then treat the endpoint as paid capacity,
    /// demoting its priority by `overage_penalty` so included (Max) capacity
    /// drains first. Non-Fable routing is unaffected. Default: true.
    fable_included: Option<bool>,
}

/// Wire format on config / state: "anthropic" | "openai".
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
enum Protocol {
    #[default]
    Anthropic,
    OpenAI,
}

#[derive(Copy, Clone, Debug, Default, Eq, PartialEq)]
enum RoutingStrategy {
    #[default]
    DynamicCapacityV1,
    StickyWeightedV2,
}

impl RoutingStrategy {
    fn parse(raw: Option<&str>) -> Result<Self, String> {
        match raw.unwrap_or("dynamic-capacity-v1") {
            "dynamic-capacity" | "dynamic-capacity-v1" => Ok(Self::DynamicCapacityV1),
            "sticky-weighted" | "sticky-weighted-v2" => Ok(Self::StickyWeightedV2),
            other => Err(format!(
                "unknown strategy '{other}' (expected dynamic-capacity-v1 or sticky-weighted-v2)"
            )),
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::DynamicCapacityV1 => "dynamic-capacity-v1",
            Self::StickyWeightedV2 => "sticky-weighted-v2",
        }
    }
}

// ── Runtime state ───────────────────────────────────────────────────

/// Per-claim utilization data for a single rate-limit window.
/// The API can report model-specific sub-budgets (e.g., "seven_day_sonnet")
/// alongside general windows ("seven_day"). Each gets its own entry.
#[derive(Default, Clone, Serialize, Deserialize)]
struct ClaimWindowData {
    /// None = no utilization data received yet (reset/status-only placeholder).
    /// Consumers must treat None as "unknown", not "healthy at 0%".
    utilization: Option<f64>,
    reset: Option<u64>,
    status: Option<String>,
    /// Epoch seconds when this entry was last updated. Used to age out entries
    /// that never received a reset timestamp (aligns runtime eviction with
    /// load_state behavior which drops reset-less entries).
    #[serde(default)]
    last_seen: u64,
}

/// Subset of RateLimitInfo for cross-replica sync via Redis.
/// Excludes Instant-based fields (non-serializable) and hard_limited_until (synced separately).
#[derive(Serialize, Deserialize)]
struct RedisRateInfo {
    utilization: Option<f64>,
    utilization_5h: Option<f64>,
    utilization_7d: Option<f64>,
    reset_5h: Option<u64>,
    reset_7d: Option<u64>,
    status_5h: Option<String>,
    status_7d: Option<String>,
    claims_7d: HashMap<String, ClaimWindowData>,
    representative_claim: Option<String>,
    remaining_requests: Option<u64>,
    remaining_tokens: Option<u64>,
    limit_requests: Option<u64>,
    limit_tokens: Option<u64>,
    #[serde(default)]
    overage_in_use: bool,
    #[serde(default)]
    overage_status: Option<String>,
    #[serde(default)]
    overage_utilization: Option<f64>,
    #[serde(default)]
    overage_reset: Option<u64>,
    updated_at: u64,
}

#[derive(Default)]
struct RateLimitInfo {
    remaining_requests: Option<u64>,
    remaining_tokens: Option<u64>,
    limit_requests: Option<u64>,
    limit_tokens: Option<u64>,
    /// Unified utilization (0.0 = fresh, 1.0 = exhausted). Derived: max across all windows.
    utilization: Option<f64>,
    /// Per-claim 7d windows: "seven_day" (general), "seven_day_sonnet" (model-specific), etc.
    /// Source of truth for 7d utilization routing.
    claims_7d: HashMap<String, ClaimWindowData>,
    /// Derived convenience: max utilization across all claims_7d entries.
    /// Used for backward-compatible logging/stats. Routing reads claims_7d directly.
    utilization_7d: Option<f64>,
    utilization_5h: Option<f64>,
    /// Which window is the binding constraint (e.g. "five_hour", "seven_day_sonnet")
    representative_claim: Option<String>,
    /// Epoch seconds when the 5h rate-limit window resets.
    reset_5h: Option<u64>,
    /// Derived convenience: min reset across all claims_7d entries.
    reset_7d: Option<u64>,
    /// API-side pressure signal. "allowed" = normal, "allowed_warning" = approaching limits
    /// (floor 0.80), "throttled" = actively constrained (floor 0.98, soft-excluded),
    /// "rejected" = hard refusal (floor 1.0, zero bucket share).
    status_5h: Option<String>,
    /// Derived convenience: worst status across all claims_7d entries.
    status_7d: Option<String>,
    hard_limited_until: Option<Instant>,
    /// Overage (paid extra usage) is actively serving requests for this account.
    /// Account-level signal — overage covers whichever subscription window is exhausted.
    /// Always overwritten per response: header absent/false → false.
    overage_in_use: bool,
    /// Overage window status ("allowed", "allowed_warning", "rejected"). Feeds the
    /// routing gate via `status_to_floor` when overage is in use.
    overage_status: Option<String>,
    /// Overage budget consumed (0.0 = fresh, 1.0 = overage exhausted).
    overage_utilization: Option<f64>,
    /// Epoch seconds when the overage window resets.
    overage_reset: Option<u64>,
    /// Counts consecutive burst 429s (no retry-after) for exponential backoff.
    /// Reset to 0 on any successful response.
    consecutive_burst_429s: u32,
    /// Consecutive upstream transport failures (ETIMEDOUT/reset/closed/DNS).
    /// Transport health, NOT rate-limit state — independent of
    /// hard_limited_until, and deliberately process-scoped (never persisted or
    /// Redis-synced, same as `upstream_transport_errors`): each replica has its
    /// own egress path, so another replica's connectivity says nothing about ours.
    consecutive_transport_failures: u32,
    /// Circuit breaker: while set and in the future, the endpoint is excluded
    /// from `routing_candidates` so a stateless affinity recompute cannot snap
    /// a session back to a persistently-dead endpoint every request. Opened
    /// after TRANSPORT_FAILURE_THRESHOLD consecutive transport failures;
    /// cleared on any successful forward or after the cooldown elapses.
    transport_unhealthy_until: Option<Instant>,
    #[allow(dead_code)]
    last_updated: Option<Instant>,
    /// Wall-clock epoch of last update, for cross-replica age comparison.
    last_updated_epoch: Option<u64>,
}

/// Exponentially weighted moving average with time-constant-based decay.
/// Handles variable inter-sample intervals correctly — the half-life is
/// wall-clock time, not dependent on request frequency.
struct Ewma {
    value: f64,
    /// Time constant (seconds). Half-life = tau * ln(2).
    tau: f64,
    last_update: Instant,
}

/// Minimum elapsed time between EWMA updates. Prevents division-by-zero
/// and inf propagation when requests arrive in the same Instant tick.
const EWMA_MIN_ELAPSED_SECS: f64 = 0.001;

/// EWMA stale threshold. If no updates for this long, reset to zero.
const EWMA_STALE_SECS: f64 = 3600.0;

impl Ewma {
    fn new(tau: f64) -> Self {
        Self {
            value: 0.0,
            tau,
            last_update: Instant::now(),
        }
    }

    fn update(&mut self, now: Instant) -> f64 {
        let elapsed = now
            .duration_since(self.last_update)
            .as_secs_f64()
            .max(EWMA_MIN_ELAPSED_SECS);
        self.last_update = now;

        // Stale guard: long idle → reset rather than extrapolate
        if elapsed > EWMA_STALE_SECS {
            self.value = 0.0;
            return self.value;
        }

        let instant_rate = 60.0 / elapsed; // requests per minute
        let alpha = 1.0 - (-elapsed / self.tau).exp();
        self.value = alpha * instant_rate + (1.0 - alpha) * self.value;

        // NaN/inf guard (belt-and-suspenders)
        if !self.value.is_finite() {
            self.value = 0.0;
        }
        self.value
    }

    #[cfg(test)]
    fn value(&self) -> f64 {
        self.value
    }
}

/// EWMA time constants for burn rate windows.
/// Half-life = tau * ln(2): TAU_5M → ~3.5min half-life, TAU_1H → ~42min, TAU_6H → ~4.2hr.
const TAU_5M: f64 = 300.0;
const TAU_1H: f64 = 3600.0;

/// Upper bound on distinct clients tracked in the per-client metric maps
/// (`client_usage`, `client_request_rates`). These are keyed by the
/// user-controlled `x-client-id` header with no eviction, so an unbounded set of
/// distinct values would grow them without limit (a memory-DoS vector). Real
/// deployments have a handful of clients, far below this; the cap only bounds
/// unknown/abusive header values — existing clients keep updating past it.
const MAX_TRACKED_CLIENTS: usize = 10_000;
const TAU_6H: f64 = 21600.0;

/// Per-account burn rate tracker: requests per minute at three time scales.
struct BurnRate {
    rate_5m: Ewma,
    rate_1h: Ewma,
    rate_6h: Ewma,
}

impl BurnRate {
    fn new() -> Self {
        Self {
            rate_5m: Ewma::new(TAU_5M),
            rate_1h: Ewma::new(TAU_1H),
            rate_6h: Ewma::new(TAU_6H),
        }
    }

    fn update(&mut self, now: Instant) {
        self.rate_5m.update(now);
        self.rate_1h.update(now);
        self.rate_6h.update(now);
    }

    #[cfg(test)]
    fn rates(&self) -> (f64, f64, f64) {
        (
            self.rate_5m.value(),
            self.rate_1h.value(),
            self.rate_6h.value(),
        )
    }
}

/// Default emergency brake threshold. When ALL accounts exceed this, non-operator traffic is blocked.
const DEFAULT_EMERGENCY_THRESHOLD: f64 = 0.88;

/// Claude Code system prompt required by the Anthropic API for OAuth tokens (sk-ant-oat*)
/// to access sonnet/opus models. Must be the FIRST system block in the request.
const OAUTH_SYSTEM_PROMPT: &str = "You are Claude Code, Anthropic's official CLI for Claude.";

/// Max bytes of 429 response body to include in debug logs.
const MAX_429_BODY_LOG_BYTES: usize = 512;

/// Substrings that mark a header as sensitive — any header whose name contains
/// one of these is redacted from debug logs. Safer than a denylist: new
/// sensitive headers (e.g. `x-auth-foo`, `session-token`) are caught by default.
const SENSITIVE_HEADER_SUBSTRINGS: &[&str] =
    &["auth", "cookie", "token", "key", "secret", "session"];

/// Format 429 response headers and body for a single debug log line.
/// Redacts sensitive headers, truncates body to MAX_429_BODY_LOG_BYTES.
async fn log_429_details(account_name: &str, resp: reqwest::Response) {
    let headers_fmt: Vec<String> = resp
        .headers()
        .iter()
        .map(|(k, v)| {
            let name = k.as_str();
            if is_sensitive_header(name) {
                format!("{}=<redacted>", name)
            } else {
                format!("{}={}", name, v.to_str().unwrap_or("<binary>"))
            }
        })
        .collect();
    let body_str = resp
        .bytes()
        .await
        .ok()
        .map(|b| {
            let slice = &b[..b.len().min(MAX_429_BODY_LOG_BYTES)];
            let s = std::str::from_utf8(slice).unwrap_or("<binary>").to_string();
            if b.len() > MAX_429_BODY_LOG_BYTES {
                format!("{}(truncated, {}B total)", s, b.len())
            } else {
                s
            }
        })
        .unwrap_or_default();
    debug!(
        account = account_name,
        headers = headers_fmt.join(" | "),
        body = body_str,
        "429 response details"
    );
}

/// Budget status thresholds for X-Budget-Status response header.
const STATUS_HEALTHY_CEILING: f64 = 0.70;
const STATUS_ELEVATED_CEILING: f64 = 0.85;
const STATUS_EMERGENCY_FLOOR: f64 = 0.95;

/// Compute the budget pressure status for a response header.
/// Returns one of "healthy", "elevated", "critical", "emergency".
fn compute_pressure_status(effective_util: f64, client_id: &str, state: &AppState) -> &'static str {
    // Operator always sees healthy
    if state.is_operator(client_id) {
        return "healthy";
    }

    let mut status = if effective_util < STATUS_HEALTHY_CEILING {
        "healthy"
    } else if effective_util < STATUS_ELEVATED_CEILING {
        "elevated"
    } else if effective_util < STATUS_EMERGENCY_FLOOR {
        "critical"
    } else {
        "emergency"
    };

    // Upgrade status if client's utilization limit proximity exceeds 80%
    if let Some(&limit) = state.client_utilization_limits.get(client_id) {
        if effective_util >= limit * 0.80 {
            status = match status {
                "healthy" => "elevated",
                "elevated" => "critical",
                "critical" => "emergency",
                _ => status,
            };
        }
    }

    status
}

/// Unified routing endpoint — the sole runtime endpoint pool.
///
/// Rate-limit/utilization fields are populated only for `Protocol::Anthropic`.
/// `Protocol::OpenAI` endpoints carry a stub `RateLimitInfo` (all fields None);
/// three code sites branch on `protocol` to handle this correctly:
///   1. `routing_candidates()` — short-circuits to a fixed RoutingCandidate.
///   2. `is_emergency_brake_active()` — iterates only Anthropic endpoints.
///   3. probe loop — skips OpenAI endpoints.
struct Endpoint {
    name: String,
    protocol: Protocol,
    /// Resolved at startup: api.anthropic.com for Anthropic default, else the
    /// explicit base_url. No trailing slash.
    base_url: String,
    token: String,
    /// True iff token == "passthrough". Only meaningful for Protocol::Anthropic.
    passthrough: bool,
    models: Vec<String>,
    priority: u32,
    /// Plan includes Fable's 50%-of-weekly band. False = Fable is always paid
    /// here (Pro plan) → Fable requests demote this endpoint by `overage_penalty`.
    fable_included: bool,
    requests: AtomicU64,
    rate_info: RwLock<RateLimitInfo>,
    burn_rate: Mutex<BurnRate>,
    input_tokens: AtomicU64,
    output_tokens: AtomicU64,
    cache_creation_tokens: AtomicU64,
    cache_read_tokens: AtomicU64,
    last_routing_weight: AtomicU64,
    last_routing_share: AtomicU64,
    last_effective_gate: AtomicU64,
}

impl Endpoint {
    /// Check if this endpoint can serve the given model. Empty allowlist = all.
    /// Identical to the historical `Account::serves_model` predicate.
    fn serves_model(&self, model: &str) -> bool {
        if self.models.is_empty() || model.is_empty() {
            return true;
        }
        self.models.iter().any(|pattern| {
            if let Some(prefix) = pattern.strip_suffix('*') {
                model.starts_with(prefix)
            } else {
                model == pattern
            }
        })
    }
}

struct AppState {
    client: Client,
    /// Upstream client for NON-streaming requests (`"stream"` false/absent).
    /// A non-streaming `/v1/messages` emits ZERO response bytes until
    /// generation completes, so `client`'s read_timeout — tuned for SSE
    /// inter-chunk silence — kills any generation longer than 180s as
    /// "operation timed out" (LAB-718 GEO judge wedge, 2026-07-24: ~20k-token
    /// structured-output calls died 18×/hour across 9 accounts and the SDK
    /// retried for hours). No read_timeout here; the 900s total budget is the
    /// only cap, and the h2 keep-alive PING still evicts dead connections.
    client_nonstreaming: Client,
    /// Unified routing endpoints — the sole endpoint pool.
    endpoints: Vec<Endpoint>,
    robin: AtomicUsize,
    routing_strategy: RoutingStrategy,
    cooldown: Duration,
    /// How long a transport-circuit-broken endpoint stays out of routing.
    /// TRANSPORT_UNHEALTHY_COOLDOWN in production; overridable so tests can
    /// exercise breaker re-entry without a 30s sleep.
    transport_cooldown: Duration,
    state_path: PathBuf,
    proxy_key: Option<String>,
    allowed_ips: Vec<IpAllowEntry>,
    client_names: HashMap<String, String>,
    auto_cache: bool,
    /// Per-client token usage: client_id → [input, output, cache_creation, cache_read]
    client_usage: Mutex<HashMap<String, [u64; 4]>>,
    /// Shadow log sender (fire-and-forget JSONL appends). None = disabled.
    shadow_log_tx: Option<tokio::sync::mpsc::Sender<String>>,
    /// Count of shadow log entries dropped due to channel backpressure.
    shadow_log_dropped: AtomicU64,
    /// Per-client daily token budgets: client_id → max tokens per day.
    client_budgets: HashMap<String, u64>,
    /// Budget tracking: client_id → (epoch_day, tokens_used). Resets on new day.
    budget_usage: Mutex<HashMap<String, (u64, u64)>>,
    /// Per-client utilization limits: client_id → max effective utilization.
    client_utilization_limits: HashMap<String, f64>,
    /// Operator client IDs — never throttled by budgets, ceilings, or emergency brake.
    operators: Vec<String>,
    /// Whether the emergency brake is enabled. Default: true.
    emergency_brake: bool,
    /// Emergency brake threshold. Default: 0.88.
    emergency_threshold: f64,
    /// Per-client request tracking: client_id → (total_requests, rate_ewma)
    client_request_rates: Mutex<HashMap<String, (u64, Ewma)>>,
    /// Utilization soft ceiling. Accounts above this are excluded from routing
    /// unless all candidates exceed it. Default: 0.90.
    soft_limit: f64,
    /// Redis connection for distributed state. None = local-only (single instance).
    redis: Option<redis::aio::ConnectionManager>,
    /// Cached cluster info from Redis, updated by background sync task.
    cluster_info_cache: Mutex<Option<serde_json::Value>>,
    /// Monotonic request ID counter for log correlation.
    next_req_id: AtomicU64,
    /// Random instance ID for cross-replica log disambiguation.
    instance_id: u16,
    /// Probe interval in seconds. Used for freshness check and distributed lock TTL.
    probe_interval_secs: u64,
    /// Priority penalty added to an endpoint's effective priority while it serves
    /// via overage. Default: 10.
    overage_penalty: u32,
    /// Per-tick DELTA accumulator of upstream transport send-failures, keyed
    /// by kind (`timeout`/`connect`/`other`). Surfaces a flaky egress on the
    /// dashboard (`anthropic_upstream_transport_errors_total`) before it
    /// becomes client errors. Drained into the shared Redis hash each sync
    /// tick (`flush_transport_errors`); without Redis it simply accumulates
    /// and feeds `/metrics` directly. Not persisted across restarts.
    upstream_transport_errors: Mutex<HashMap<&'static str, u64>>,
    /// Current sum of reserved in-flight request-body bytes (admission control).
    inflight_body_bytes: AtomicU64,
    /// Ceiling for `inflight_body_bytes`; over it, requests are shed with 503.
    /// 0 = disabled (unbounded).
    max_inflight_body_bytes: u64,
    /// Count of requests load-shed because admitting them would exceed
    /// `max_inflight_body_bytes`. Exposed as `anthropic_body_shed_total`.
    body_shed_total: AtomicU64,
    /// Wall-clock ceiling for buffering a request body (P1-01). Bounds how long
    /// a slow or stalled client can hold its body-memory reservation.
    /// `Duration::ZERO` disables the timeout.
    body_read_timeout: Duration,
    /// Count of requests shed because the body was not fully received within
    /// `body_read_timeout`. Exposed as `anthropic_body_read_timeout_total`.
    body_read_timeout_total: AtomicU64,
}

/// RAII reservation against `AppState::inflight_body_bytes`. Holding it keeps the
/// reserved bytes counted as in-flight; dropping it releases them. Held for the
/// duration of a request handler so the budget reflects real resident body memory.
struct BodyReservation {
    state: Arc<AppState>,
    bytes: u64,
}

impl Drop for BodyReservation {
    fn drop(&mut self) {
        if self.bytes > 0 {
            self.state
                .inflight_body_bytes
                .fetch_sub(self.bytes, Ordering::AcqRel);
        }
    }
}

impl AppState {
    /// Try to reserve `bytes` of the in-flight request-body memory budget.
    /// Returns a guard that releases the reservation on drop, or `None` if
    /// admitting would push `inflight_body_bytes` over `max_inflight_body_bytes`
    /// (the caller then load-sheds with `503 + Retry-After`). A budget of 0
    /// disables the limit (always admits, no tracking). Lock-free CAS so the
    /// hot path never blocks.
    fn try_reserve_body(self: &Arc<Self>, bytes: u64) -> Option<BodyReservation> {
        let max = self.max_inflight_body_bytes;
        if max == 0 {
            return Some(BodyReservation {
                state: Arc::clone(self),
                bytes: 0,
            });
        }
        let mut cur = self.inflight_body_bytes.load(Ordering::Acquire);
        loop {
            if cur.saturating_add(bytes) > max {
                return None;
            }
            match self.inflight_body_bytes.compare_exchange_weak(
                cur,
                cur + bytes,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    return Some(BodyReservation {
                        state: Arc::clone(self),
                        bytes,
                    })
                }
                Err(actual) => cur = actual,
            }
        }
    }
}

/// Admission control (P1-01): reserve the request-body memory budget from the
/// request's Content-Length before buffering. Returns the reservation guard to
/// hold for the request, or a `503 + Retry-After` Response to return when the
/// budget is exhausted (load-shedding). Shared by `proxy_handler` and
/// `openai_chat_handler` so the two paths can't drift.
fn reserve_request_body(
    state: &Arc<AppState>,
    parts: &axum::http::request::Parts,
    req_id: &str,
    client_ip: IpAddr,
) -> Result<BodyReservation, Box<Response>> {
    let reserve_bytes = parts
        .headers
        .get(axum::http::header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(MAX_REQUEST_BODY_BYTES as u64)
        .min(MAX_REQUEST_BODY_BYTES as u64);
    match state.try_reserve_body(reserve_bytes) {
        Some(g) => Ok(g),
        None => {
            warn!(
                req_id,
                client = %client_ip,
                reserve = reserve_bytes,
                "rejected: in-flight request-body memory budget exhausted (load-shedding)"
            );
            state.body_shed_total.fetch_add(1, Ordering::Relaxed);
            let resp = (
                StatusCode::SERVICE_UNAVAILABLE,
                [("retry-after", "1")],
                "overloaded: request-body memory budget exhausted",
            )
                .into_response();
            Err(Box::new(resp))
        }
    }
}

/// Buffer the request body with a wall-clock ceiling (P1-01): the caller holds
/// a `BodyReservation` while this runs, so a slow-loris or stalled upload must
/// not be allowed to wait forever — six header-complete uploads that omit
/// Content-Length (reserving the full 25 MiB each) and then stall would
/// otherwise pin the entire `max_inflight_body_bytes` budget and shed all
/// traffic. Times out with `408` (retried by the Anthropic SDKs); read errors
/// (body over `MAX_REQUEST_BODY_BYTES`, client disconnect) stay `400`. Shared
/// by `proxy_handler` and `openai_chat_handler` so the two paths can't drift.
async fn read_body_bounded(
    state: &Arc<AppState>,
    body: Body,
    req_id: &str,
) -> Result<bytes::Bytes, Box<Response>> {
    let read = axum::body::to_bytes(body, MAX_REQUEST_BODY_BYTES);
    let result = if state.body_read_timeout.is_zero() {
        read.await
    } else {
        match tokio::time::timeout(state.body_read_timeout, read).await {
            Ok(r) => r,
            Err(_) => {
                state
                    .body_read_timeout_total
                    .fetch_add(1, Ordering::Relaxed);
                warn!(
                    req_id,
                    timeout_secs = state.body_read_timeout.as_secs(),
                    "request body read timed out (releasing body-memory reservation)"
                );
                let resp =
                    (StatusCode::REQUEST_TIMEOUT, "request body read timed out").into_response();
                return Err(Box::new(resp));
            }
        }
    };
    match result {
        Ok(b) => Ok(b),
        Err(e) => {
            error!("failed to read request body: {e}");
            Err(Box::new(
                (StatusCode::BAD_REQUEST, "bad request body").into_response(),
            ))
        }
    }
}

/// Index into `AppState.endpoints` — the sole runtime endpoint pool.
type EndpointIdx = usize;

#[derive(Clone, Copy, Debug)]
struct RoutingCandidate {
    endpoint: EndpointIdx,
    /// Effective priority tier — includes the overage penalty when applicable.
    priority: u32,
    gate_5h: f64,
    gate_7d: f64,
    gate: f64,
    wr: f64,
    weight: f64,
    source: &'static str,
}

fn stable_affinity_hash(key: &str) -> u64 {
    use std::hash::Hasher;

    // Pinned SipHash keeps sticky routing stable across rebuilds and process
    // restarts without depending on RandomState's per-process seeding.
    #[allow(deprecated)]
    let mut hasher = std::hash::SipHasher::new_with_keys(0, 0);
    hasher.write(key.as_bytes());
    hasher.finish()
}

/// Measurement-only: walk the request in Anthropic cache-prefix order (tools →
/// system → messages) and, at every `cache_control` breakpoint, snapshot
/// `(prefix_byte_len, prefix_digest)`. This exposes the *actual* cacheable-prefix
/// hierarchy (the first breakpoint is the stable system/tools prefix; later ones
/// grow with the conversation), which a flat system+first-user fingerprint can't
/// see. No body content is retained — only lengths and digests. Used to size the
/// avoidable-cache_write prize before deciding on a routing key.
fn prefix_breakpoint_hashes(body: &serde_json::Value) -> Vec<(usize, String)> {
    // Accumulate a canonical, STRUCTURE-PRESERVING representation in Anthropic
    // cache-prefix order. Each element is tagged with its section/role (unit
    // separator) and serialized whole, so distinct payloads with the same raw
    // text can't collide and overstate cache reuse. `pos` is the accumulated
    // BYTE length (a valid char boundary — we only ever push whole strings).
    let mut acc = String::new();
    let mut positions: Vec<usize> = Vec::new();
    let push = |acc: &mut String, tag: &str, v: &serde_json::Value| {
        acc.push('\u{1f}');
        acc.push_str(tag);
        acc.push('\u{1f}');
        acc.push_str(&serde_json::to_string(v).unwrap_or_default());
    };
    // tools (cache_control marks the end of the tools prefix)
    if let Some(tools) = body.get("tools").and_then(|t| t.as_array()) {
        for t in tools {
            push(&mut acc, "tool", t);
            if t.get("cache_control").is_some() {
                positions.push(acc.len());
            }
        }
    }
    // system (string or array of blocks)
    match body.get("system") {
        Some(serde_json::Value::String(s)) => {
            acc.push_str("\u{1f}sys\u{1f}");
            acc.push_str(s);
        }
        Some(serde_json::Value::Array(arr)) => {
            for b in arr {
                push(&mut acc, "sys", b);
                if b.get("cache_control").is_some() {
                    positions.push(acc.len());
                }
            }
        }
        _ => {}
    }
    // messages (role-tagged; content string, or array of blocks)
    if let Some(msgs) = body.get("messages").and_then(|m| m.as_array()) {
        for m in msgs {
            let role = m.get("role").and_then(|r| r.as_str()).unwrap_or("?");
            match m.get("content") {
                Some(serde_json::Value::String(s)) => {
                    acc.push('\u{1f}');
                    acc.push_str(role);
                    acc.push('\u{1f}');
                    acc.push_str(s);
                    if m.get("cache_control").is_some() {
                        positions.push(acc.len());
                    }
                }
                Some(serde_json::Value::Array(blocks)) => {
                    for b in blocks {
                        push(&mut acc, role, b);
                        if b.get("cache_control").is_some() {
                            positions.push(acc.len());
                        }
                    }
                }
                _ => {}
            }
        }
    }
    positions
        .into_iter()
        .map(|pos| {
            let h = stable_affinity_hash(&acc[..pos]) & 0xFFFF_FFFF_FFFF;
            (pos, format!("{h:012x}"))
        })
        .collect()
}

/// Privacy-safe session fingerprints for headerless traffic. Returns
/// `(system+first-user, system-only)` as 12-hex-char digests of the pinned
/// SipHash. Only the IMMUTABLE conversation prefix is hashed (system blocks +
/// the first user message), so the value is stable across a conversation's
/// growing turns. No content is retained or logged — only the digest. Used to
/// measure whether a content fingerprint could separate the headerless fleet.
fn content_fingerprints(body: &serde_json::Value) -> (String, String) {
    // Extract text from an Anthropic `system`/`content` field, which may be a
    // plain string or an array of `{type, text}` blocks.
    fn block_text(v: &serde_json::Value) -> String {
        if let Some(s) = v.as_str() {
            return s.to_string();
        }
        let mut out = String::new();
        if let Some(arr) = v.as_array() {
            for b in arr {
                if let Some(t) = b.get("text").and_then(|t| t.as_str()) {
                    out.push_str(t);
                    out.push('\n');
                }
            }
        }
        out
    }

    let system = body.get("system").map(block_text).unwrap_or_default();
    let first_user = body
        .get("messages")
        .and_then(|m| m.as_array())
        .and_then(|arr| {
            arr.iter()
                .find(|m| m.get("role").and_then(|r| r.as_str()) == Some("user"))
        })
        .and_then(|m| m.get("content"))
        .map(block_text)
        .unwrap_or_default();

    let fps = stable_affinity_hash(&system);
    let mut combined = system;
    combined.push('\u{0}');
    combined.push_str(&first_user);
    let fp = stable_affinity_hash(&combined);
    (
        format!("{:012x}", fp & 0xFFFF_FFFF_FFFF),
        format!("{:012x}", fps & 0xFFFF_FFFF_FFFF),
    )
}

/// Parsed IP allow entry — either a single IP or a CIDR range.
enum IpAllowEntry {
    Addr(IpAddr),
    Net(IpNet),
}

impl IpAllowEntry {
    fn contains(&self, ip: &IpAddr) -> bool {
        match self {
            Self::Addr(a) => a == ip,
            Self::Net(n) => n.contains(ip),
        }
    }
}

impl AppState {
    fn is_ip_allowed(&self, ip: &IpAddr) -> bool {
        self.allowed_ips.is_empty() || self.allowed_ips.iter().any(|e| e.contains(ip))
    }

    /// Resolve client identity: x-client-id header → IP map fallback → "-"
    ///
    /// Header takes precedence to support multiple clients per IP.
    fn resolve_client_id(&self, ip: &IpAddr, headers: &hyper::HeaderMap) -> String {
        if let Some(id) = headers.get("x-client-id").and_then(|v| v.to_str().ok()) {
            let id = id.trim();
            if !id.is_empty() && id != "-" {
                return id.to_string();
            }
        }
        // Fallback: IP mapping or unknown
        self.client_names
            .get(&ip.to_string())
            .cloned()
            .unwrap_or_else(|| "-".to_string())
    }
}

// ── Persistence ─────────────────────────────────────────────────────

#[derive(Serialize, Deserialize)]
struct PersistedState {
    endpoints: Vec<PersistedEndpoint>,
    #[serde(default)]
    saved_at: u64,
}

#[derive(Serialize, Deserialize)]
struct PersistedEndpoint {
    name: String,
    requests_total: u64,
    utilization: Option<f64>,
    #[serde(default)]
    utilization_7d: Option<f64>,
    #[serde(default)]
    utilization_5h: Option<f64>,
    representative_claim: Option<String>,
    #[serde(default)]
    reset_5h: Option<u64>,
    #[serde(default)]
    reset_7d: Option<u64>,
    #[serde(default)]
    status_5h: Option<String>,
    #[serde(default)]
    status_7d: Option<String>,
    #[serde(default)]
    claims_7d: HashMap<String, ClaimWindowData>,
    remaining_requests: Option<u64>,
    remaining_tokens: Option<u64>,
    limit_requests: Option<u64>,
    limit_tokens: Option<u64>,
    #[serde(default)]
    overage_in_use: bool,
    #[serde(default)]
    overage_status: Option<String>,
    #[serde(default)]
    overage_utilization: Option<f64>,
    #[serde(default)]
    overage_reset: Option<u64>,
    /// Absolute unix timestamp (secs) when hard limit expires
    hard_limited_until_epoch: Option<u64>,
    /// Wall-clock epoch when this account's rate info was last updated.
    /// Used by sync_from_redis "most recent wins" merge after restart.
    #[serde(default)]
    last_updated_epoch: Option<u64>,
    /// Burst-429 backoff stage (consecutive no-Retry-After 429s). Persisted so a
    /// restart mid-escalation doesn't reset exponential backoff to stage 0 (B3-07).
    #[serde(default)]
    consecutive_burst_429s: u32,
}

/// Process-global monotonic nonce for unique state-file temp names, so
/// concurrent save_state calls never share a temp path (which could interleave
/// into a torn file before the atomic rename promotes it).
static STATE_SAVE_NONCE: AtomicU64 = AtomicU64::new(0);

impl AppState {
    fn now_epoch() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs()
    }

    /// Convert epoch seconds to ISO 8601 UTC string (no chrono dependency).
    fn epoch_to_iso8601(epoch: u64) -> String {
        // Days from epoch, accounting for leap years
        let secs_per_day: u64 = 86400;
        let mut remaining = epoch;
        let mut year: u64 = 1970;
        loop {
            let days_in_year = if (year.is_multiple_of(4) && !year.is_multiple_of(100))
                || year.is_multiple_of(400)
            {
                366
            } else {
                365
            };
            let secs_in_year = days_in_year * secs_per_day;
            if remaining < secs_in_year {
                break;
            }
            remaining -= secs_in_year;
            year += 1;
        }
        let is_leap =
            (year.is_multiple_of(4) && !year.is_multiple_of(100)) || year.is_multiple_of(400);
        let days_in_months: [u64; 12] = [
            31,
            if is_leap { 29 } else { 28 },
            31,
            30,
            31,
            30,
            31,
            31,
            30,
            31,
            30,
            31,
        ];
        let mut day_of_year = remaining / secs_per_day;
        remaining %= secs_per_day;
        let mut month: u64 = 1;
        for &dim in &days_in_months {
            if day_of_year < dim {
                break;
            }
            day_of_year -= dim;
            month += 1;
        }
        let day = day_of_year + 1;
        let hour = remaining / 3600;
        remaining %= 3600;
        let minute = remaining / 60;
        let second = remaining % 60;
        format!("{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}Z")
    }

    async fn save_state(&self) {
        // Serialize save executions (not just temp filenames): hold this lock
        // across snapshot + write + rename so the last writer observes the
        // freshest in-memory state and its rename lands last. Without it, two
        // overlapping saves could let an older snapshot's rename finish last and
        // roll back fresher persisted state (e.g. a hard-limit just recorded).
        // Saves are infrequent (probe / hard-limit / recovery / shutdown), so
        // contention is negligible.
        static SAVE_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
        let _save_guard = SAVE_LOCK.lock().await;

        let mut endpoints = Vec::new();
        let now = Instant::now();

        // Build a PersistedEndpoint from an endpoint's (name, requests,
        // rate_info) fields.
        async fn persist_one(
            name: &str,
            requests: &AtomicU64,
            rate_info: &RwLock<RateLimitInfo>,
            now: Instant,
        ) -> PersistedEndpoint {
            let info = rate_info.read().await;
            let hard_until_epoch = info.hard_limited_until.and_then(|until| {
                if until > now {
                    let remaining = until.duration_since(now);
                    Some(AppState::now_epoch() + remaining.as_secs())
                } else {
                    None
                }
            });
            PersistedEndpoint {
                name: name.to_string(),
                requests_total: requests.load(Ordering::Relaxed),
                utilization: info.utilization,
                utilization_7d: info.utilization_7d,
                utilization_5h: info.utilization_5h,
                representative_claim: info.representative_claim.clone(),
                reset_5h: info.reset_5h,
                reset_7d: info.reset_7d,
                status_5h: info.status_5h.clone(),
                status_7d: info.status_7d.clone(),
                claims_7d: info.claims_7d.clone(),
                remaining_requests: info.remaining_requests,
                remaining_tokens: info.remaining_tokens,
                limit_requests: info.limit_requests,
                limit_tokens: info.limit_tokens,
                overage_in_use: info.overage_in_use,
                overage_status: info.overage_status.clone(),
                overage_utilization: info.overage_utilization,
                overage_reset: info.overage_reset,
                hard_limited_until_epoch: hard_until_epoch,
                last_updated_epoch: info.last_updated_epoch,
                consecutive_burst_429s: info.consecutive_burst_429s,
            }
        }

        // Skip OpenAI endpoints — their rate_info is a permanent stub with no
        // state worth persisting.
        for ep in &self.endpoints {
            if ep.protocol == Protocol::OpenAI {
                continue;
            }
            endpoints.push(persist_one(&ep.name, &ep.requests, &ep.rate_info, now).await);
        }

        let state = PersistedState {
            endpoints,
            saved_at: Self::now_epoch(),
        };

        // Atomic write: serialize compactly to a unique temp sibling, then rename
        // it into place. Rename is atomic on the same filesystem, so a crash
        // mid-write leaves the previous complete file intact (not a truncated
        // one) and concurrent writers can't interleave into a torn file. Compact
        // `to_vec` (not `to_string_pretty`) roughly halves the bytes per save.
        match serde_json::to_vec(&state) {
            Ok(json) => {
                let nonce = STATE_SAVE_NONCE.fetch_add(1, Ordering::Relaxed);
                let mut tmp_os = self.state_path.clone().into_os_string();
                tmp_os.push(format!(".{nonce}.tmp"));
                let tmp_path = PathBuf::from(tmp_os);
                if let Err(e) = tokio::fs::write(&tmp_path, &json).await {
                    error!(path = %tmp_path.display(), error = %e, "failed to write temp state");
                    // A failed write may still have created a partial temp file
                    // (open truncates before write_all). Best-effort cleanup, same
                    // as the rename branch, so failed saves don't leak temp files.
                    let _ = tokio::fs::remove_file(&tmp_path).await;
                    return;
                }
                if let Err(e) = tokio::fs::rename(&tmp_path, &self.state_path).await {
                    error!(path = %self.state_path.display(), error = %e, "failed to rename state into place");
                    let _ = tokio::fs::remove_file(&tmp_path).await;
                } else {
                    trace!(path = %self.state_path.display(), "state saved");
                }
            }
            Err(e) => error!(error = %e, "failed to serialize state"),
        }
    }

    /// True if `model`'s 7d claim was refreshed recently enough to skip a probe.
    ///
    /// Pure freshness check used by `probe_endpoint`.
    /// Looks up only the model-specific claim key (e.g. `seven_day_opus`), NOT the
    /// general `seven_day` fallback, so probing one family doesn't suppress another.
    /// An empty `model_family` (unrecognized model) never counts as fresh.
    /// "Recent" means the claim's age is under half the probe interval.
    fn claim_recently_probed(
        info: &RateLimitInfo,
        model: &str,
        probe_interval_secs: u64,
        now_epoch: u64,
    ) -> bool {
        let family = model_family(model);
        if family.is_empty() {
            return false;
        }
        let claim_key = format!("seven_day_{}", family);
        match info.claims_7d.get(&claim_key) {
            Some(claim) => {
                let age = now_epoch.saturating_sub(claim.last_seen);
                age < probe_interval_secs / 2
            }
            None => false,
        }
    }

    /// Fire a minimal request (max_tokens=1) to refresh rate-limit headers for
    /// the endpoint at `idx`. The `model` parameter controls which model is
    /// probed, rotating across families so that per-model 7d utilization claims
    /// get populated for each family. Skips `Protocol::OpenAI` endpoints — they
    /// expose no Anthropic rate-limit headers, so a probe would only burn a
    /// request. One of the three named `match protocol` sites.
    async fn probe_endpoint(&self, idx: usize, model: &str) {
        let ep = &self.endpoints[idx];
        if ep.protocol == Protocol::OpenAI {
            debug!(endpoint = ep.name, "skipping probe for openai endpoint");
            return;
        }
        if ep.passthrough {
            debug!(
                endpoint = ep.name,
                "skipping probe for passthrough endpoint"
            );
            return;
        }

        // Check if hard-limited — don't waste a request
        {
            let info = ep.rate_info.read().await;
            if let Some(until) = info.hard_limited_until {
                if Instant::now() < until {
                    debug!(
                        endpoint = ep.name,
                        "skipping probe, endpoint is hard-limited"
                    );
                    return;
                }
            }
        }

        // Local freshness check: skip if this model's 7d claim was recently refreshed.
        let now_epoch = Self::now_epoch();
        {
            let info = ep.rate_info.read().await;
            if Self::claim_recently_probed(&info, model, self.probe_interval_secs, now_epoch) {
                trace!(
                    endpoint = ep.name,
                    probe_model = model,
                    "probe skipped, model claim is fresh"
                );
                return;
            }
        }

        // Distributed probe lock: one pod per endpoint+model per interval.
        if let Some(redis) = &self.redis {
            let mut conn = redis.clone();
            let lock_key = format!("alb:probe:{}:{}", ep.name, model);
            let lock_ttl = self.probe_interval_secs.max(1);
            let acquired: redis::RedisResult<bool> = redis::cmd("SET")
                .arg(&lock_key)
                .arg(1u8)
                .arg("NX")
                .arg("EX")
                .arg(lock_ttl)
                .query_async(&mut conn)
                .await;
            match acquired {
                Ok(true) => {} // Lock acquired, proceed with probe
                Ok(false) => {
                    trace!(
                        endpoint = ep.name,
                        probe_model = model,
                        "probe skipped, another replica is probing"
                    );
                    return;
                }
                Err(e) => {
                    // Fail-open: if Redis is down, probe anyway
                    trace!(endpoint = ep.name, error = %e, "probe lock failed, probing anyway");
                }
            }
        }

        // Each endpoint carries its own base URL.
        let url = format!("{}/v1/messages", ep.base_url);
        let body = serde_json::json!({
            "model": model,
            "max_tokens": 1,
            "system": [{"type": "text", "text": "You are Claude Code, Anthropic's official CLI for Claude."}],
            "messages": [{"role": "user", "content": "."}]
        });

        // Build headers in a HeaderMap so auth injection can reuse the shared
        // `inject_account_auth` (token-prefix dispatch + OAuth beta-flag merge)
        // instead of a hand-rolled copy.
        let mut headers = axum::http::HeaderMap::new();
        headers.insert("content-type", HeaderValue::from_static("application/json"));
        headers.insert("anthropic-version", HeaderValue::from_static("2023-06-01"));
        headers.insert(
            "anthropic-beta",
            HeaderValue::from_str(&OAUTH_BETA_FLAGS.join(",")).unwrap(),
        );
        headers.insert(
            "user-agent",
            HeaderValue::from_static("claude-cli/2.1.2 (external, cli)"),
        );
        headers.insert("x-app", HeaderValue::from_static("cli"));
        headers.insert(
            "anthropic-dangerous-direct-browser-access",
            HeaderValue::from_static("true"),
        );
        inject_account_auth(&mut headers, &ep.token, ep.passthrough);

        let req = self.client.post(&url).headers(headers).json(&body);

        match req.send().await {
            Ok(resp) => {
                let status = resp.status();
                self.update_rate_info_for(&ep.rate_info, &ep.name, resp.headers())
                    .await;
                if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
                    self.mark_hard_limited_for(&ep.rate_info, &ep.name, resp.headers())
                        .await;
                } else if status.is_success() {
                    // 2xx only: endpoint is responsive — clear hard limit and
                    // burst counter. 5xx/529 are upstream errors, not recovery.
                    let recovered = {
                        let mut info = ep.rate_info.write().await;
                        let was_hard_limited = info.hard_limited_until.is_some();
                        if was_hard_limited {
                            info.hard_limited_until = None;
                            debug!(
                                endpoint = ep.name,
                                "cleared hard limit after successful probe"
                            );
                        }
                        info.consecutive_burst_429s = 0;
                        was_hard_limited
                    };
                    if recovered {
                        self.signal_hard_limit_recovery(&ep.name).await;
                    }
                }
                // else: 5xx/529 — leave endpoint state untouched.
                self.save_state().await;
                let info = ep.rate_info.read().await;
                let now_epoch = Self::now_epoch();
                let (eff_util, constraint, _adj_5h, _adj_7d) =
                    effective_utilization(&info, now_epoch, model);
                // Only compute routing weight on 2xx — non-success responses leave
                // the endpoint state either mutated (429 → hard-limited) or untouched
                // (5xx), and the pre-response weight no longer reflects reality.
                let rw = if status.is_success() {
                    compute_routing_weight(&info, model, now_epoch, false)
                } else {
                    None
                };
                // DEBUG, not INFO: per-probe completion is high-volume periodic
                // noise (every probe_interval × model-family × endpoint). The
                // routing-weight metrics this computes are still exported via
                // Prometheus; this line is only the human-readable echo.
                debug!(
                    endpoint = ep.name,
                    status = status.as_u16(),
                    probe_model = model,
                    utilization = format_args!("{eff_util:.2}"),
                    util_5h = info
                        .utilization_5h
                        .map(|v| format!("{v:.2}"))
                        .as_deref()
                        .unwrap_or("-"),
                    util_7d = info
                        .utilization_7d
                        .map(|v| format!("{v:.2}"))
                        .as_deref()
                        .unwrap_or("-"),
                    constraint,
                    n_claims_7d = info.claims_7d.len(),
                    gate_5h = rw
                        .as_ref()
                        .map(|r| format!("{:.4}", r.gate_5h))
                        .as_deref()
                        .unwrap_or("-"),
                    gate_7d = rw
                        .as_ref()
                        .map(|r| format!("{:.4}", r.gate_7d))
                        .as_deref()
                        .unwrap_or("-"),
                    waste_risk = rw
                        .as_ref()
                        .map(|r| format!("{:.4}", r.wr))
                        .as_deref()
                        .unwrap_or("-"),
                    weight = rw
                        .as_ref()
                        .map(|r| format!("{:.4}", r.weight))
                        .as_deref()
                        .unwrap_or("-"),
                    weight_source = rw.as_ref().map(|r| r.source).unwrap_or("-"),
                    "probe complete"
                );
            }
            Err(e) => {
                warn!(endpoint = ep.name, error = %e, detail = %describe_reqwest_error(&e), "probe failed");
            }
        }
    }

    async fn load_state(&self) {
        let data = match tokio::fs::read_to_string(&self.state_path).await {
            Ok(d) => d,
            Err(_) => {
                info!(path = %self.state_path.display(), "no persisted state found, starting fresh");
                return;
            }
        };

        let persisted: PersistedState = match serde_json::from_str(&data) {
            Ok(s) => s,
            Err(e) => {
                warn!(error = %e, "failed to parse persisted state (possible legacy 'accounts'-keyed format — that schema was removed); starting fresh");
                return;
            }
        };

        let now_epoch = Self::now_epoch();
        let now_instant = Instant::now();

        for pa in &persisted.endpoints {
            // Match each persisted entry to the endpoint with the same name.
            let restore_target: Option<(&AtomicU64, &RwLock<RateLimitInfo>)> = self
                .endpoints
                .iter()
                .find(|e| e.name == pa.name)
                .map(|e| (&e.requests, &e.rate_info));
            if let Some((requests, rate_info)) = restore_target {
                requests.store(pa.requests_total, Ordering::Relaxed);
                let mut info = rate_info.write().await;
                info.utilization = pa.utilization;
                info.utilization_7d = pa.utilization_7d;
                info.utilization_5h = pa.utilization_5h;
                info.representative_claim = pa.representative_claim.clone();
                info.reset_5h = pa.reset_5h;
                info.status_5h = pa.status_5h.clone();
                info.overage_in_use = pa.overage_in_use;
                info.overage_status = pa.overage_status.clone();
                info.overage_utilization = pa.overage_utilization;
                info.overage_reset = pa.overage_reset;
                info.consecutive_burst_429s = pa.consecutive_burst_429s;

                // Load claims_7d: either from persisted map or migrate from flat fields
                if !pa.claims_7d.is_empty() {
                    info.claims_7d = pa.claims_7d.clone();
                } else if let Some(util_7d) = pa.utilization_7d {
                    // Migration: old state file with flat 7d fields only
                    let key = pa
                        .representative_claim
                        .as_deref()
                        .filter(|c| c.starts_with("seven_day"))
                        .unwrap_or("seven_day")
                        .to_string();
                    info.claims_7d.insert(
                        key,
                        ClaimWindowData {
                            utilization: Some(util_7d),
                            reset: pa.reset_7d,
                            status: pa.status_7d.clone(),
                            ..Default::default()
                        },
                    );
                }

                // Evict stale claims (reset in the past)
                info.claims_7d
                    .retain(|_, c| c.reset.is_some_and(|r| r > now_epoch));

                // Derive flat 7d fields from claims_7d — a persisted carve-out
                // claim (Fable band) must not resurrect into the emergency
                // brake's input on boot; see derive_flat_7d_fields.
                (info.utilization_7d, info.reset_7d, info.status_7d) =
                    derive_flat_7d_fields(&info.claims_7d);

                // Invalidate stale 5h data
                if info.reset_5h.is_none_or(|r| r <= now_epoch) {
                    info.utilization_5h = None;
                    info.reset_5h = None;
                    info.status_5h = None;
                }

                // Recompute unified utilization from surviving windows
                let mut max_util: Option<f64> = info.utilization_5h;
                if let Some(u7) = info.utilization_7d {
                    max_util = Some(max_util.map_or(u7, |cur| cur.max(u7)));
                }
                info.utilization = max_util;

                info.remaining_requests = pa.remaining_requests;
                info.remaining_tokens = pa.remaining_tokens;
                info.limit_requests = pa.limit_requests;
                info.limit_tokens = pa.limit_tokens;

                if let Some(until_epoch) = pa.hard_limited_until_epoch {
                    if until_epoch > now_epoch {
                        let remaining_secs = until_epoch - now_epoch;
                        info.hard_limited_until =
                            Some(now_instant + Duration::from_secs(remaining_secs));
                        info!(
                            account = pa.name,
                            remaining_secs, "restored hard limit from persisted state"
                        );
                    }
                }

                info.last_updated = Some(now_instant);
                // Prefer per-account epoch (accurate); fall back to global saved_at
                // (correct for old state files without per-account timestamps).
                info.last_updated_epoch = Some(pa.last_updated_epoch.unwrap_or(persisted.saved_at));
                info!(
                    account = pa.name,
                    utilization = ?pa.utilization,
                    requests = pa.requests_total,
                    "restored account state"
                );
            }
        }
    }
}

// ── Time-adjusted utilization ──────────────────────────────────────
//
// Anthropic rate limits use fixed time blocks (5h, 7d) that reset at known timestamps.
// An account at 95% utilization with 5 minutes until reset is about to become fresh,
// but raw utilization treats it the same as one with 4 hours remaining.
//
// We apply a threshold-based discount: only in the near-reset zone at the end of each
// block does utilization get reduced. Mid-block, raw utilization is used unchanged.
// This avoids the "compression problem" where a continuous linear discount would make
// all accounts look similar at mid-block, destroying routing differentiation.
//
// Status headers act as circuit breakers — Anthropic can signal pressure (burst limits,
// concurrent request limits, per-model sub-limits) that raw utilization doesn't capture.
// These floors can only increase effective utilization, never decrease it.

/// Last 20% of 5h block. Low consequence — resets soon, safe to route here.
const NEAR_RESET_5H_SECS: f64 = 3600.0;
/// Last ~3.5% of 7d block. Conservative — overshoot has multi-day consequence.
const NEAR_RESET_7D_SECS: f64 = 21600.0;
/// Overage window near-reset threshold. Overage utilization is the real signal;
/// the time discount only matters in the final hour before the window resets.
const NEAR_RESET_OVERAGE_SECS: f64 = 3600.0;
/// Minimum discount factor — prevents utilization from collapsing to zero near reset.
const TIME_FRACTION_FLOOR: f64 = 0.05;
/// Above soft_limit (0.90) so throttled accounts are excluded from routing unless
/// ALL accounts are throttled. Captures API-side pressure before a hard 429.
const THROTTLE_UTIL_FLOOR: f64 = 0.98;
/// Below soft_limit so warned accounts still participate, but with reduced bucket share.
const WARNING_UTIL_FLOOR: f64 = 0.80;
/// "rejected" = hard refusal from the API. Treat as fully exhausted — zero bucket share.
/// Distinct from hard_limited_until (which skips the account entirely) because rejected
/// status can arrive on one window while the other is still valid.
const REJECTED_UTIL_FLOOR: f64 = 1.0;
/// Maximum number of BEBO (binary exponential backoff) retry rounds for 529
/// (overloaded) responses. After exhausting all accounts, the proxy waits and
/// retries all accounts up to this many additional times. Total attempts through
/// the full account list = MAX_529_RETRIES + 1.
const MAX_529_RETRIES: u32 = 3;

/// Base delay for 529 BEBO retries. Doubles each round: 1s, 2s, 4s.
const RETRY_529_BASE_DELAY: Duration = Duration::from_secs(1);

/// Base backoff for a round that failed on transient transport errors
/// (ETIMEDOUT/reset/closed/DNS). Doubles per round: 150ms, 300ms. Kept short
/// because egress blips are sub-second; 529 overload uses the longer 1s base.
const TRANSIENT_BASE_DELAY: Duration = Duration::from_millis(150);

/// Transient (transport-error) rounds get a SMALLER budget than 529 overload.
/// Round 0 retries the affinity/cache-warm endpoint in place (no rotation);
/// rounds 1..=MAX_TRANSIENT_RETRIES rotate the pool. Total rounds = this + 1.
/// Capped at 1 so a genuinely-down egress fails clean (→ 503) in ~one rotation
/// instead of stalling through all MAX_529_RETRIES rounds of connect timeouts.
const MAX_TRANSIENT_RETRIES: u32 = 1;

/// Consecutive transport failures before an endpoint is circuit-broken out of
/// the routing candidate set. One dead-endpoint request contributes at most 2
/// failures (round-0 in-place retry + round 1), so 3 only trips across ≥2
/// separate requests — a sub-second blip (which round-gating rides out) cannot
/// open the breaker, only a persistently-dead endpoint can.
const TRANSPORT_FAILURE_THRESHOLD: u32 = 3;

/// How long a circuit-broken endpoint stays out of the candidate set. Bounds
/// the affinity tax: without the breaker a stateless affinity recompute snaps
/// back to the dead endpoint every request (~8s of connect timeouts each);
/// with it, at most ~1.5 requests per cooldown window pay the probe cost.
/// ponytail: constant, add a config knob if operators ever need to tune it.
const TRANSPORT_UNHEALTHY_COOLDOWN: Duration = Duration::from_secs(30);

/// Sentinel value written to `alb:hard:{account}` when a replica has observed
/// recovery from a hard rate limit. Other replicas interpret this as an
/// instruction to proactively clear their local `hard_limited_until`, which
/// DEL alone could not do (sync_from_redis ignores missing keys). Distinct
/// from "no key" (no data) and "epoch > 0" (active hard limit).
const HARD_LIMIT_CLEARED_SENTINEL: u64 = 0;

/// TTL for the recovery sentinel in Redis. Long enough for every replica to
/// observe it via sync_from_redis (5s interval = 12 opportunities), short
/// enough that the key does not linger past the intended recovery window.
const HARD_LIMIT_SENTINEL_TTL_SECS: u64 = 60;

/// Redis hash aggregating upstream transport send-failures across all replicas,
/// keyed by kind (`timeout`/`connect`/`other`). Each replica flushes its local
/// delta accumulator into this hash via `HINCRBY` every sync tick, so the
/// dashboard/metrics endpoint reports a fleet-wide count rather than one pod's
/// local observations. A single fixed key (not per-day like budgets) → one
/// monotonic counter family that survives pod restarts.
const TRANSPORT_ERRORS_KEY: &str = "alb:transport_errors";

/// TTL refreshed on every flush of `TRANSPORT_ERRORS_KEY`. Chosen far larger
/// than the 5s sync interval so the key never expires while any replica is
/// alive (some pod re-`EXPIRE`s it every 5s). It only lapses once the ENTIRE
/// fleet has been down for two days, at which point resetting the counter is
/// correct — there is no live count left to preserve — and it prevents an
/// orphaned key lingering forever after a permanent teardown.
const TRANSPORT_ERRORS_TTL_SECS: u64 = 172_800;

/// Maximum request body size (25 MiB). Kept deliberately below Anthropic's own
/// 32 MB Messages API request limit so multi-image/PDF payloads upstream would
/// accept still pass. Aggregate concurrent-body memory is NOT this × N — that
/// is bounded by the `max_inflight_body_mb` admission budget (P1-01), and how
/// long a body can hold its reservation is bounded by `body_read_timeout_secs`;
/// this cap only bounds a single request's share of the budget (and its
/// transient JSON parse amplification). `to_bytes` stops buffering at this
/// ceiling, so an oversized body is rejected without being fully buffered.
const MAX_REQUEST_BODY_BYTES: usize = 25 * 1024 * 1024;

/// Default wall-clock ceiling for receiving a request body (seconds). Real
/// clients push even a max-size body in seconds; 60s is generous headroom for
/// a slow relayed path while guaranteeing a stalled upload cannot pin its
/// body-memory reservation (up to `MAX_REQUEST_BODY_BYTES` when Content-Length
/// is absent) against the P1-01 budget indefinitely. Override with the
/// `body_read_timeout_secs` config key; 0 disables.
const DEFAULT_BODY_READ_TIMEOUT_SECS: u64 = 60;

/// Default aggregate in-flight request-body memory budget (bytes). Requests are
/// admission-controlled against this ceiling: when the sum of in-flight request
/// bodies would exceed it, new requests are load-shed with `503 + Retry-After`
/// rather than buffered. Unbounded buffering OOM-kills the pod under a burst of
/// concurrent large requests, and a dropped pod takes ALL its in-flight requests
/// with it — far worse than shedding a few. Sized as a backstop for a ~512Mi pod
/// with headroom for parse amplification + baseline RSS. Override with the
/// `max_inflight_body_mb` config key; set it to 0 to disable the limit.
const DEFAULT_MAX_INFLIGHT_BODY_BYTES: u64 = 128 * 1024 * 1024;

/// Required OAuth beta flags. Both needed: oauth-2025-04-20 for OAuth auth,
/// claude-code-20250219 for Claude Code API access quota routing.
const OAUTH_BETA_FLAGS: &[&str] = &["oauth-2025-04-20", "claude-code-20250219"];

/// Legacy dynamic-capacity override threshold. If the affinity-picked account's
/// weight is below 50% of the alternative, stickiness is broken immediately.
const LEGACY_AFFINITY_OVERRIDE_RATIO: f64 = 0.5;

/// Sticky-weighted override threshold. Lower than the legacy algorithm to
/// preserve cache locality and only break stickiness for egregious disparities.
const STICKY_WEIGHTED_OVERRIDE_RATIO: f64 = 0.25;

/// Extract version string from User-Agent header.
/// "claude-cli/2.1.68 (external, cli)" → "2.1.68"
/// "anthropic-sdk/1.0.0" → "1.0.0"
/// Returns None for unrecognizable formats.
fn extract_client_version(ua: &str) -> Option<&str> {
    let ver = ua
        .split_once('/')
        .map(|(_, rest)| rest.split_once(' ').map_or(rest, |(ver, _)| ver))?;
    if ver.is_empty() {
        None
    } else {
        Some(ver)
    }
}

/// Used to look up model-specific rate-limit claims (e.g., "seven_day_sonnet").
/// Returns "" for unrecognized models, which triggers worst-case routing.
fn model_family(model: &str) -> &str {
    if model.contains("sonnet") {
        "sonnet"
    } else if model.contains("opus") {
        "opus"
    } else if model.contains("haiku") {
        "haiku"
    } else if model.contains("fable") {
        "fable"
    } else {
        ""
    }
}

/// Internal claim key for the Fable included-usage band. On Max plans Fable is
/// included only up to 50% of the weekly limit; past that it bills as paid
/// usage credits (support.claude.com article 15424964). Unlike other per-model
/// claims, this band is a carve-out *within* the shared weekly pool, not an
/// independent sub-budget — see `constraining_7d_claims`.
///
/// WIRE MAPPING (verified against a live claude-fable-5 response, 2026-07-21):
/// the API does NOT emit a `seven_day_fable` representative claim. The band
/// arrives as the `anthropic-ratelimit-unified-7d_oi-{utilization,reset,status}`
/// triplet ("oi" = overage-included), present ONLY on Fable responses — a
/// sonnet response from the same account omits it. `update_rate_info_for`
/// normalises that triplet into this claims_7d entry so the standard claims
/// machinery (gating, waste-risk, persistence, Redis sync, eviction) applies.
const FABLE_BAND_CLAIM: &str = "seven_day_fable";

/// Claims that participate in the model-agnostic worst case (emergency brake,
/// stats with no model context). Allowlist, NOT denylist: only the general
/// weekly pool and the per-family sub-budgets that gate whole traffic classes
/// qualify. Carve-outs (the Fable band) and any future unknown keys are
/// excluded — an unknown carve-out silently joining the brake input could
/// freeze ALL traffic while every regular budget is healthy. Under-braking on
/// a genuinely new model family is the safer failure mode: per-model routing
/// gates still protect it, the brake is only a last-resort backstop.
fn claim_gates_all_traffic(key: &str) -> bool {
    matches!(
        key,
        "seven_day" | "seven_day_sonnet" | "seven_day_opus" | "seven_day_haiku"
    )
}

/// Derive the flat 7d convenience fields from a claims map: utilization_7d =
/// max utilization, reset_7d = min reset, status_7d = worst status. Only
/// claims that gate ALL traffic participate (`claim_gates_all_traffic`) —
/// these fields feed `effective_utilization()`'s model-agnostic fallback
/// chain, so a carve-out (Fable band) leaking in could trip the emergency
/// brake for all traffic. Single implementation shared by `load_state()` and
/// `update_rate_info_for()` so the filter policy cannot drift between them.
fn derive_flat_7d_fields(
    claims_7d: &HashMap<String, ClaimWindowData>,
) -> (Option<f64>, Option<u64>, Option<String>) {
    let utilization_7d = claims_7d
        .iter()
        .filter(|(k, _)| claim_gates_all_traffic(k))
        .filter_map(|(_, c)| c.utilization)
        .reduce(f64::max);
    let reset_7d = claims_7d
        .iter()
        .filter(|(k, _)| claim_gates_all_traffic(k))
        .filter_map(|(_, c)| c.reset)
        .min();
    let status_7d = claims_7d
        .iter()
        .filter(|(k, _)| claim_gates_all_traffic(k))
        .filter_map(|(_, c)| c.status.as_deref())
        .max_by(|a, b| {
            status_to_floor(Some(a))
                .partial_cmp(&status_to_floor(Some(b)))
                .unwrap_or(std::cmp::Ordering::Equal)
        })
        .map(|s| s.to_string());
    (utilization_7d, reset_7d, status_7d)
}

fn resolve_7d_claim<'a>(info: &'a RateLimitInfo, model: &str) -> Option<&'a ClaimWindowData> {
    if info.claims_7d.is_empty() {
        return None;
    }
    if model.is_empty() {
        return None;
    }
    let family = model_family(model);
    if !family.is_empty() {
        let key = format!("seven_day_{}", family);
        info.claims_7d
            .get(&key)
            .or_else(|| info.claims_7d.get("seven_day"))
    } else {
        info.claims_7d.get("seven_day")
    }
}

/// The 7d claims that constrain `model` on this account: `(primary, pool_cap)`.
///
/// The primary is `resolve_7d_claim`'s pick (model-specific claim, falling back
/// to the general `seven_day` bucket). For Fable, when the band claim exists,
/// the general claim is returned as a second constraint: Fable's usable
/// headroom is `min(band remaining, weekly pool remaining)`, so a roomy band
/// must not mask a drained pool. For every other family the pool cap is `None`
/// — their sub-budget claim is already the binding constraint (unchanged
/// pre-Fable behaviour).
fn constraining_7d_claims<'a>(
    info: &'a RateLimitInfo,
    model: &str,
) -> (Option<&'a ClaimWindowData>, Option<&'a ClaimWindowData>) {
    let primary = resolve_7d_claim(info, model);
    if model_family(model) != "fable" || !info.claims_7d.contains_key(FABLE_BAND_CLAIM) {
        // Without a band claim the primary already IS the general claim.
        return (primary, None);
    }
    (primary, info.claims_7d.get("seven_day"))
}

const TOTAL_7D_SECS: f64 = 604800.0;
const WASTE_RISK_MIN_REMAINING: u64 = 60;

/// Compute waste risk: how much quota will be wasted if we don't route here.
/// Higher = more urgency to use this account's remaining 7d budget.
/// Returns 0.0 when reset data is unavailable or stale.
fn waste_risk(util: Option<f64>, reset_epoch: Option<u64>, now_epoch: u64) -> f64 {
    let util = match util {
        Some(u) => u,
        None => return 0.0, // No utilization data — can't compute waste risk
    };
    let reset = match reset_epoch {
        Some(r) if r > now_epoch + WASTE_RISK_MIN_REMAINING => r,
        _ => return 0.0,
    };
    let remaining_fraction = (reset - now_epoch) as f64 / TOTAL_7D_SECS;
    let unused = (1.0 - util).max(0.0);
    (unused / remaining_fraction).min(10.0)
}

/// Map a rate-limit status string to a utilization floor.
/// Unknown non-"allowed" values are treated as warning-level pressure and logged,
/// so new API statuses degrade gracefully before we add explicit support.
fn status_to_floor(status: Option<&str>) -> f64 {
    match status {
        Some("rejected") => REJECTED_UTIL_FLOOR,
        Some("throttled") => THROTTLE_UTIL_FLOOR,
        Some("allowed_warning") => WARNING_UTIL_FLOOR,
        Some("allowed") | None => 0.0,
        Some(unknown) => {
            warn!(
                status = unknown,
                "unknown rate-limit status, applying warning floor"
            );
            WARNING_UTIL_FLOOR
        }
    }
}

/// Map a rate-limit status string to a Prometheus ordinal gauge value.
/// 0=allowed, 1=allowed_warning, 2=throttled, 3=rejected.
/// Unknown statuses map to 1 (warning-level) for visibility.
fn status_to_ordinal(status: Option<&str>) -> f64 {
    match status {
        Some("rejected") => 3.0,
        Some("throttled") => 2.0,
        Some("allowed_warning") => 1.0,
        Some("allowed") | None => 0.0,
        Some(unknown) => {
            warn!(
                status = unknown,
                "unknown rate-limit status in ordinal mapping"
            );
            1.0
        }
    }
}

/// Compute time-adjusted utilization for a single rate-limit window.
///
/// In the near-reset zone (final `near_reset_secs` of the block), raw utilization is
/// discounted proportionally — an account about to reset is treated as healthier.
/// Outside the zone, raw utilization is returned unchanged.
///
/// Status floors are applied AFTER time discount and can only increase the result:
/// - "rejected" → 1.0 (fully exhausted, zero bucket share — API is refusing requests)
/// - "throttled" → 0.98 (effectively soft-excluded above soft_limit=0.90)
/// - "allowed_warning" → 0.80
///
/// Returns `None` if:
/// - `raw_util` is `None` (no data)
/// - `reset_epoch` is in the past (stale data — window already reset)
fn time_adjusted_utilization(
    raw_util: Option<f64>,
    reset_epoch: Option<u64>,
    status: Option<&str>,
    near_reset_secs: f64,
    now_epoch: u64,
) -> Option<f64> {
    let util = raw_util?;

    if let Some(reset) = reset_epoch {
        // Stale data guard: if the window already reset, our utilization number is meaningless.
        // Returning None lets the caller fall through to the other window or the legacy path.
        // Probes will refresh this within 5 minutes.
        if reset <= now_epoch {
            return None;
        }

        let remaining = (reset - now_epoch) as f64;

        // Threshold-based discount: only kick in near the end of the block.
        // Outside the zone: discount=1.0 (raw util unchanged, preserves differentiation).
        // Inside the zone: linear ramp from 1.0 → TIME_FRACTION_FLOOR as reset approaches.
        let discount = if remaining < near_reset_secs {
            (remaining / near_reset_secs).max(TIME_FRACTION_FLOOR)
        } else {
            1.0
        };
        let adjusted = util * discount;

        // Status floor: Anthropic's signal of pressure beyond what utilization numbers show.
        // Applied after discount so it acts as a hard minimum — can only raise effective util.
        // Unknown non-"allowed" statuses get WARNING_UTIL_FLOOR defensively (Bug #4).
        let floor = status_to_floor(status);
        Some(adjusted.max(floor))
    } else {
        // No reset timestamp available — can't do time adjustment, but status floors still apply.
        // This handles the transition period when we get status headers but not reset headers.
        let floor = status_to_floor(status);
        Some(util.max(floor))
    }
}

/// Compute effective utilization for an account using the full fallback chain.
/// Returns (utilization, source_label, adj_5h, adj_7d) for logging/routing.
///
/// Fallback chain:
/// 1. Both windows adjusted → take max (most constrained wins)
/// 2. Only one window → use it (the other is stale or absent)
/// 3. Neither window → raw unified util, then legacy token ratio, then 0.5
fn effective_utilization(
    info: &RateLimitInfo,
    now_epoch: u64,
    model: &str,
) -> (f64, &'static str, Option<f64>, Option<f64>) {
    // 5h window — always flat (no per-model sub-budgets from API)
    let adj_5h = time_adjusted_utilization(
        info.utilization_5h,
        info.reset_5h,
        info.status_5h.as_deref(),
        NEAR_RESET_5H_SECS,
        now_epoch,
    );

    // 7d window — model-aware lookup from claims_7d map
    let adj_7d = if !info.claims_7d.is_empty() {
        if !model.is_empty() {
            let adj = |c: &ClaimWindowData| {
                time_adjusted_utilization(
                    c.utilization,
                    c.reset,
                    c.status.as_deref(),
                    NEAR_RESET_7D_SECS,
                    now_epoch,
                )
            };
            let (primary, pool_cap) = constraining_7d_claims(info, model);
            match (primary.and_then(adj), pool_cap.and_then(adj)) {
                // Fable: band and shared weekly pool both constrain — the more
                // utilized window is the binding one.
                (Some(band), Some(pool)) => Some(band.max(pool)),
                (band, pool) => band.or(pool),
            }
        } else {
            // No model specified (emergency brake, stats) — worst-case across the
            // claims that gate ALL traffic (allowlist). Carve-outs like the Fable
            // band constrain only their own requests (which gate on them
            // per-model above); letting a carve-out's exhaustion drive the
            // model-agnostic worst case would trip the emergency brake for ALL
            // traffic while every regular sub-budget still has capacity.
            info.claims_7d
                .iter()
                .filter(|(key, _)| claim_gates_all_traffic(key))
                .filter_map(|(_, c)| {
                    time_adjusted_utilization(
                        c.utilization,
                        c.reset,
                        c.status.as_deref(),
                        NEAR_RESET_7D_SECS,
                        now_epoch,
                    )
                })
                .reduce(f64::max)
        }
    } else {
        // No claims_7d data — fall back to derived flat fields (migration/compat)
        time_adjusted_utilization(
            info.utilization_7d,
            info.reset_7d,
            info.status_7d.as_deref(),
            NEAR_RESET_7D_SECS,
            now_epoch,
        )
    };

    match (adj_5h, adj_7d) {
        (Some(a), Some(b)) if a >= b => (a, "5h", adj_5h, adj_7d),
        (Some(_), Some(b)) => (b, "7d", adj_5h, adj_7d),
        (Some(a), None) => (a, "5h", adj_5h, adj_7d),
        (None, Some(b)) => (b, "7d", adj_5h, adj_7d),
        (None, None) => {
            // Fallback: raw unified (no time adjustment), legacy tokens, or unknown
            if let Some(util) = info.utilization {
                (util, "unified", None, None)
            } else if let Some(remaining) = info.remaining_tokens {
                let limit = info.limit_tokens.unwrap_or(1_000_000);
                (
                    (1.0 - (remaining as f64 / limit as f64)).clamp(0.0, 1.0),
                    "legacy",
                    None,
                    None,
                )
            } else {
                (0.5, "unknown", None, None)
            }
        }
    }
}

/// Computed routing weight for a single endpoint+model. Extracted so both
/// `routing_candidates()` (real requests) and `probe_endpoint()` (periodic
/// probes) use identical logic. Returns `None` when the endpoint's 7d claim is
/// actively rejected (caller should skip it).
struct RoutingWeight {
    gate_5h: f64,
    gate_7d: f64,
    gate: f64,
    wr: f64,
    weight: f64,
    source: &'static str,
    /// Account is serving via paid overage — caller demotes its priority tier.
    overage_active: bool,
}

fn compute_routing_weight(
    info: &RateLimitInfo,
    model: &str,
    now_epoch: u64,
    stale_after_hard_limit: bool,
) -> Option<RoutingWeight> {
    // Overage active: the account's exhausted subscription window is being covered
    // by paid overage. The subscription gates are moot — the overage window governs.
    let overage_active = info.overage_in_use && !stale_after_hard_limit;

    // 5h gate: time-adjusted 5h utilization with status floors
    let gate_5h = if stale_after_hard_limit {
        0.5
    } else {
        time_adjusted_utilization(
            info.utilization_5h,
            info.reset_5h,
            info.status_5h.as_deref(),
            NEAR_RESET_5H_SECS,
            now_epoch,
        )
        .unwrap_or_else(|| {
            // Fallback: raw unified, legacy, or unknown
            if let Some(util) = info.utilization {
                util
            } else if let Some(remaining) = info.remaining_tokens {
                let limit = info.limit_tokens.unwrap_or(1_000_000);
                (1.0 - (remaining as f64 / limit as f64)).clamp(0.0, 1.0)
            } else {
                0.5
            }
        })
    };

    // 7d model-specific gate and waste risk. For Fable both the band claim and
    // the general weekly claim constrain (headroom = min of the two remainders);
    // for other families pool_cap is None and this reduces to the single-claim
    // logic below.
    let (primary_7d, pool_cap_7d) = constraining_7d_claims(info, model);
    let (gate_7d, wr_7d, source_7d) = if let Some(claim) = primary_7d {
        let rejected = |c: &ClaimWindowData| {
            c.status.as_deref() == Some("rejected") && c.reset.is_none_or(|reset| reset > now_epoch)
        };
        let rejected_claim_active = rejected(claim) || pool_cap_7d.is_some_and(rejected);
        // A rejected 7d claim normally skips the account — but not while overage is
        // covering it (overage serves requests despite the rejected subscription claim).
        if rejected_claim_active && !stale_after_hard_limit && !overage_active {
            return None; // caller should skip this account
        }
        let gate_of = |c: &ClaimWindowData| {
            time_adjusted_utilization(
                Some(0.0),
                c.reset,
                c.status.as_deref(),
                NEAR_RESET_7D_SECS,
                now_epoch,
            )
            .unwrap_or(0.0)
        };
        let gate = if stale_after_hard_limit {
            0.5
        } else {
            // Worse status of band vs pool governs (max of the two floors).
            pool_cap_7d
                .iter()
                .fold(gate_of(claim), |g, c| g.max(gate_of(c)))
        };
        let mut wr = waste_risk(claim.utilization, claim.reset, now_epoch);
        if let Some(pool) = pool_cap_7d {
            // A drained weekly pool caps how much of the Fable band is actually
            // usable — but only cap on a meaningful signal: waste_risk yields
            // 0.0 for missing/stale inputs (util OR reset absent), and a 0.0
            // cap would erase the band's urgency rather than bound it. Cost: a
            // pool at exactly util=1.0 (true wr of 0.0) also skips the cap —
            // acceptable, its status floor gates the account instead.
            let pool_wr = waste_risk(pool.utilization, pool.reset, now_epoch);
            if pool_wr > 0.0 {
                wr = wr.min(pool_wr);
            }
        }
        (gate, wr, "waste_risk")
    } else {
        (
            if stale_after_hard_limit { 0.5 } else { 0.0 },
            0.0,
            "headroom_only",
        )
    };

    // Effective gate: when overage is in use, the overage window governs — the
    // exhausted 5h/7d gates are superseded. waste_risk is moot for an overage account.
    let (gate, wr, source) = if overage_active {
        let gate_overage = time_adjusted_utilization(
            info.overage_utilization,
            info.overage_reset,
            info.overage_status.as_deref(),
            NEAR_RESET_OVERAGE_SECS,
            now_epoch,
        )
        .unwrap_or(0.0);
        (gate_overage, 0.0, "overage")
    } else {
        (gate_5h.max(gate_7d), wr_7d, source_7d)
    };

    let headroom = (1.0 - gate).max(0.01);
    let weight = if wr > 0.0 { wr * headroom } else { headroom };
    let weight = if gate >= 1.0 { 0.0 } else { weight };

    Some(RoutingWeight {
        gate_5h,
        gate_7d,
        gate,
        wr,
        weight,
        source,
        overage_active,
    })
}

/// Classification of a remote `alb:hard:{account}` value read from Redis.
/// Pure function output — unit-testable without a Redis client.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HardLimitSync {
    /// Remote stored the recovery sentinel (`HARD_LIMIT_CLEARED_SENTINEL`).
    /// Another replica observed recovery — this replica should clear its
    /// local `hard_limited_until` but must NOT reset local burst-backoff state.
    Clear,
    /// Remote stored a valid future epoch. Apply it as a hard limit until
    /// the given `Instant`.
    Update(Instant),
    /// Missing, stale (epoch <= now that isn't the sentinel), or bogus value.
    /// Take no action — local state is already correct or more current.
    Ignore,
}

/// Classify a remote hard-limit value from Redis into an action.
///
/// Clamps `until_epoch` to at most 24h in the future to prevent a corrupt or
/// malicious Redis value (e.g. `u64::MAX`) from producing an `Instant` that
/// panics on arithmetic or creates a permanent undead hard limit.
fn classify_hard_limit_sync(
    remote: Option<u64>,
    now_epoch: u64,
    now_instant: Instant,
) -> HardLimitSync {
    const MAX_HARD_LIMIT_SECS: u64 = 86_400; // 24h ceiling — matches mark_hard_limited
    match remote {
        None => HardLimitSync::Ignore,
        Some(HARD_LIMIT_CLEARED_SENTINEL) => HardLimitSync::Clear,
        Some(epoch) if epoch > now_epoch => {
            let delta = (epoch - now_epoch).min(MAX_HARD_LIMIT_SECS);
            HardLimitSync::Update(now_instant + Duration::from_secs(delta))
        }
        // Stale (epoch <= now_epoch but non-zero) — ignore. The key is expiring
        // naturally via TTL; local state is unaffected.
        Some(_) => HardLimitSync::Ignore,
    }
}

impl AppState {
    /// Display name for an endpoint, for logging.
    fn endpoint_name(&self, ep: EndpointIdx) -> &str {
        &self.endpoints[ep].name
    }

    async fn routing_candidates(&self, model: &str, skip: &[EndpointIdx]) -> Vec<RoutingCandidate> {
        let now = Instant::now();
        let now_epoch = Self::now_epoch();
        let mut candidates: Vec<RoutingCandidate> = Vec::new();
        for (i, ep) in self.endpoints.iter().enumerate() {
            if skip.contains(&i) {
                continue;
            }
            if !ep.serves_model(model) {
                continue;
            }
            match ep.protocol {
                Protocol::OpenAI => {
                    // OpenAI endpoints carry no rate-limit data, but they DO
                    // carry transport health — a circuit-broken endpoint leaves
                    // the pool the same way an Anthropic one does. If the whole
                    // pool is circuit-broken this fails closed (429), matching
                    // the hard-limited precedent; the cooldown bounds the window.
                    {
                        let info = ep.rate_info.read().await;
                        if let Some(until) = info.transport_unhealthy_until {
                            if now < until {
                                trace!(
                                    endpoint = ep.name,
                                    unhealthy_secs = until.duration_since(now).as_secs(),
                                    "pick: skipping transport-unhealthy endpoint"
                                );
                                continue;
                            }
                        }
                    }
                    // Push a fixed candidate at the configured priority. This
                    // is one of the three named `match protocol` sites (see
                    // Endpoint struct docs).
                    trace!(
                        endpoint = ep.name,
                        priority = ep.priority,
                        "pick: candidate (openai, fixed)"
                    );
                    candidates.push(RoutingCandidate {
                        endpoint: i,
                        priority: ep.priority,
                        gate_5h: 0.0,
                        gate_7d: 0.0,
                        gate: 0.0,
                        wr: 0.0,
                        weight: 1.0,
                        source: "openai",
                    });
                }
                Protocol::Anthropic => {
                    let info = ep.rate_info.read().await;
                    if let Some(until) = info.hard_limited_until {
                        if now < until {
                            trace!(
                                endpoint = ep.name,
                                hard_limited_secs = until.duration_since(now).as_secs(),
                                "pick: skipping hard-limited endpoint"
                            );
                            continue;
                        }
                    }
                    // Transport circuit breaker — independent of the 429 path
                    // above (rate limit ≠ transport health).
                    if let Some(until) = info.transport_unhealthy_until {
                        if now < until {
                            trace!(
                                endpoint = ep.name,
                                unhealthy_secs = until.duration_since(now).as_secs(),
                                "pick: skipping transport-unhealthy endpoint"
                            );
                            continue;
                        }
                    }
                    let stale_after_hard_limit = info
                        .hard_limited_until
                        .is_some_and(|until| info.last_updated.is_none_or(|lu| lu <= until));
                    let rw = match compute_routing_weight(
                        &info,
                        model,
                        now_epoch,
                        stale_after_hard_limit,
                    ) {
                        Some(rw) => rw,
                        None => {
                            trace!(
                                endpoint = ep.name,
                                model = model,
                                "pick: skipping, 7d claim rejected"
                            );
                            continue;
                        }
                    };
                    // Paid-capacity demotion: overage in use, or a Fable request
                    // on an account whose plan bills Fable from the first token
                    // (fable_included = false). Same penalty for both — the
                    // semantics are identical: drain included capacity first.
                    let fable_paid = !ep.fable_included && model_family(model) == "fable";
                    let effective_priority = if rw.overage_active || fable_paid {
                        ep.priority.saturating_add(self.overage_penalty)
                    } else {
                        ep.priority
                    };
                    trace!(
                        endpoint = ep.name,
                        gate = format!("{:.4}", rw.gate),
                        weight = format!("{:.4}", rw.weight),
                        priority = effective_priority,
                        "pick: candidate (anthropic)"
                    );
                    candidates.push(RoutingCandidate {
                        endpoint: i,
                        priority: effective_priority,
                        gate_5h: rw.gate_5h,
                        gate_7d: rw.gate_7d,
                        gate: rw.gate,
                        wr: rw.wr,
                        weight: rw.weight,
                        source: rw.source,
                    });
                }
            }
        }
        candidates
    }

    /// Recompute and persist a representative routing weight per account for
    /// metrics consumers. Called from the probe loop on the same cadence as
    /// rate-limit data refreshes — never per-request, so the gauges reflect a
    /// model-agnostic steady state instead of whichever model the last
    /// inbound request happened to use.
    ///
    /// "Representative" means: 5h gate from the (model-agnostic) 5h window,
    /// 7d gate from the convenience min-reset / max-utilization aggregates
    /// already maintained on `RateLimitInfo`. This intentionally diverges from
    /// `routing_candidates()` (which is model-specific) — pick decisions still
    /// use the precise per-model claim.
    ///
    /// DIVERGENCE from `routing_candidates()`:
    ///   1. Selects a representative `ClaimWindowData` model-agnostically
    ///      (via `representative_claim` → `seven_day` general → highest
    ///      waste_risk fallback) instead of the model-specific
    ///      `resolve_7d_claim(model)` lookup. The chosen claim's util,
    ///      reset, and status are read as a coherent triple — no
    ///      Frankenstein from independently aggregated max/min.
    ///   2. No "rejected claim → continue" branch; this is purely a metric
    ///      snapshot, not a routing decision, so we still emit a gauge for
    ///      such accounts (it ends up at zero via the `gate >= 1.0` clamp).
    ///   3. Soft-limit handling matches `pick_account`'s graceful-degradation
    ///      semantics: if at least one account is healthy, soft-limited
    ///      accounts are zeroed; if NO account is healthy, all are kept so
    ///      the dashboard reflects the still-routable degraded pool.
    async fn refresh_metrics_weights(&self) {
        let now_epoch = Self::now_epoch();
        let now = Instant::now();

        // Collect (gate, weight) per endpoint. None = excluded entirely
        // (passthrough or hard-limited — never weighted in any condition).
        // OpenAI endpoints carry no rate-limit data — their representative
        // weight is the fixed (gate 0.0, weight 1.0) candidate that
        // routing_candidates produces. Anthropic endpoints run the identical
        // Anthropic computation.
        let mut entries: Vec<Option<(f64, f64)>> = vec![None; self.endpoints.len()];
        for (i, ep) in self.endpoints.iter().enumerate() {
            match ep.protocol {
                Protocol::OpenAI => {
                    // NOTE: unlike persistence / stats / Redis sync (which all
                    // `continue` on Protocol::OpenAI), metrics intentionally
                    // emits OpenAI endpoints with a fixed (gate 0.0,
                    // weight 1.0) — an OpenAI endpoint is a real routing
                    // candidate and belongs on dashboards.
                    entries[i] = Some((0.0, 1.0));
                }
                Protocol::Anthropic => {
                    if ep.passthrough {
                        continue;
                    }
                    let info = ep.rate_info.read().await;
                    entries[i] = metrics_gate_weight(&info, now_epoch, now);
                }
            }
        }

        self.store_metrics_weights(&entries);
    }

    /// Normalize per-endpoint (gate, weight) pairs into the three gauge atomics
    /// of each endpoint, applying pick_endpoint's graceful soft-limit
    /// degradation.
    fn store_metrics_weights(&self, entries: &[Option<(f64, f64)>]) {
        // Mirror pick_endpoint's graceful-degradation: only filter soft-limited
        // members when at least one healthy member exists in the pool.
        let has_healthy = entries
            .iter()
            .any(|e| matches!(e, Some((gate, _)) if *gate < self.soft_limit));

        let mut weights = vec![0f64; self.endpoints.len()];
        for (i, entry) in entries.iter().enumerate() {
            if let Some((gate, weight)) = entry {
                if has_healthy && *gate >= self.soft_limit {
                    continue; // soft-limited and there's a healthy alternative
                }
                weights[i] = *weight;
            }
        }

        let total: f64 = weights.iter().sum();
        for (i, ep) in self.endpoints.iter().enumerate() {
            let w = weights[i];
            let share = if total > 0.0 { w / total } else { 0.0 };
            // Excluded members (passthrough, hard-limited) report gate=1.0
            // (fully gated) since they receive zero traffic.
            let gate = entries[i].map(|(g, _)| g).unwrap_or(1.0);
            // Weight, share and gate are independent gauges, not a joint
            // invariant — a torn read across them is harmless.
            ep.last_routing_weight.store(w.to_bits(), Ordering::Relaxed);
            ep.last_routing_share
                .store(share.to_bits(), Ordering::Relaxed);
            ep.last_effective_gate
                .store(gate.to_bits(), Ordering::Relaxed);
        }
    }
}

/// Per-entry representative `(gate, weight)` for metrics gauges, model-agnostic.
/// Returns `None` for hard-limited members (they contribute zero in any state).
/// Computed purely from a `RateLimitInfo`.
fn metrics_gate_weight(info: &RateLimitInfo, now_epoch: u64, now: Instant) -> Option<(f64, f64)> {
    // Hard-limited members contribute zero (mirrors routing_candidates filter).
    if let Some(until) = info.hard_limited_until {
        if now < until {
            return None;
        }
    }

    let stale_after_hard_limit = info
        .hard_limited_until
        .is_some_and(|until| info.last_updated.is_none_or(|lu| lu <= until));

    // 5h gate — same logic as routing_candidates
    let gate_5h = if stale_after_hard_limit {
        0.5
    } else {
        time_adjusted_utilization(
            info.utilization_5h,
            info.reset_5h,
            info.status_5h.as_deref(),
            NEAR_RESET_5H_SECS,
            now_epoch,
        )
        .unwrap_or_else(|| {
            if let Some(util) = info.utilization {
                util
            } else if let Some(remaining) = info.remaining_tokens {
                let limit = info.limit_tokens.unwrap_or(1_000_000);
                (1.0 - (remaining as f64 / limit as f64)).clamp(0.0, 1.0)
            } else {
                0.5
            }
        })
    };

    // 7d gate + waste_risk from a SINGLE representative ClaimWindowData
    // — utilization, reset and status are read as a coherent triple
    // from one real claim, not Frankensteined from independently
    // aggregated maxima/minima across claims.
    //
    // Selection precedence:
    //   1. info.representative_claim if it points to a 7d entry
    //      (this is the LB's own "binding constraint" signal)
    //   2. The general "seven_day" claim if present
    //   3. The model-specific claim with the highest waste_risk
    //      (worst-case representative for the dashboard)
    //   4. None → no 7d data, fall back to headroom-only
    let claim_is_fresh = |c: &&ClaimWindowData| {
        c.reset.is_some()
            && time_adjusted_utilization(
                Some(0.0),
                c.reset,
                c.status.as_deref(),
                NEAR_RESET_7D_SECS,
                now_epoch,
            )
            .is_some()
    };
    let representative: Option<&ClaimWindowData> = {
        let rep_key = info.representative_claim.as_deref();
        rep_key
            .filter(|k| k.starts_with("seven_day"))
            .and_then(|k| info.claims_7d.get(k))
            .filter(claim_is_fresh)
            .or_else(|| info.claims_7d.get("seven_day").filter(claim_is_fresh))
            .or_else(|| {
                info.claims_7d
                    .values()
                    .filter(claim_is_fresh)
                    .max_by(|a, b| {
                        let wr_a = waste_risk(a.utilization, a.reset, now_epoch);
                        let wr_b = waste_risk(b.utilization, b.reset, now_epoch);
                        wr_a.partial_cmp(&wr_b).unwrap_or(std::cmp::Ordering::Equal)
                    })
            })
    };

    let (gate_7d, wr) = if let Some(claim) = representative {
        let g = if stale_after_hard_limit {
            0.5
        } else {
            time_adjusted_utilization(
                Some(0.0),
                claim.reset,
                claim.status.as_deref(),
                NEAR_RESET_7D_SECS,
                now_epoch,
            )
            .unwrap_or(0.0)
        };
        let w = waste_risk(claim.utilization, claim.reset, now_epoch);
        (g, w)
    } else {
        // No 7d claim at all — headroom-only
        let g = if stale_after_hard_limit { 0.5 } else { 0.0 };
        (g, 0.0)
    };

    let gate = gate_5h.max(gate_7d);
    let headroom = (1.0 - gate).max(0.01);
    let weight = if wr > 0.0 { wr * headroom } else { headroom };
    let weight = if gate >= 1.0 { 0.0 } else { weight };

    Some((gate, weight))
}

impl AppState {
    fn routing_weight_publish_ttl(probe_interval_secs: u64) -> u64 {
        const FALLBACK_PUBLISH_INTERVAL_SECS: u64 = 60;

        let effective_interval = if probe_interval_secs == 0 {
            FALLBACK_PUBLISH_INTERVAL_SECS
        } else {
            probe_interval_secs
        };

        effective_interval.saturating_mul(2).max(1)
    }

    /// Publish precomputed routing weights to Redis so non-probing pods
    /// can set their gauge atomics without recomputing.
    async fn publish_routing_weights(&self) {
        let redis = match &self.redis {
            Some(r) => r,
            None => return,
        };
        use redis::AsyncCommands;
        let ttl = Self::routing_weight_publish_ttl(self.probe_interval_secs);
        let publish = |name: &str, weight: &AtomicU64, share: &AtomicU64, gate: &AtomicU64| {
            let w = f64::from_bits(weight.load(Ordering::Relaxed));
            let s = f64::from_bits(share.load(Ordering::Relaxed));
            let g = f64::from_bits(gate.load(Ordering::Relaxed));
            let key = format!("alb:weight:{}", name);
            let val = format!("{w},{s},{g}");
            let mut conn = redis.clone();
            tokio::spawn(async move {
                let result: redis::RedisResult<()> = conn.set_ex(&key, val, ttl).await;
                if let Err(e) = result {
                    tracing::warn!(error = %e, "redis routing weight publish failed");
                }
            });
        };
        // OpenAI endpoints are skipped — sync_from_redis only reads weights
        // for Anthropic targets.
        for ep in &self.endpoints {
            if ep.protocol == Protocol::OpenAI {
                continue;
            }
            publish(
                &ep.name,
                &ep.last_routing_weight,
                &ep.last_routing_share,
                &ep.last_effective_gate,
            );
        }
    }

    /// Distributed hard-limit recovery: notifies other replicas that the local
    /// hard limit has been cleared. Writes a sentinel (`HARD_LIMIT_CLEARED_SENTINEL`)
    /// to `alb:hard:{account}` so `sync_from_redis` can proactively clear other
    /// replicas' stale `hard_limited_until` Instants — DEL alone leaves them stuck
    /// until their own probe sees recovery.
    ///
    /// The write uses a Lua CAS script: only clears if the current value is absent
    /// or already `<= now_epoch` (i.e. stale/expired). This prevents a TOCTOU race
    /// where `mark_hard_limited` spawns a write of `until_epoch=now+cooldown` at
    /// roughly the same moment, and unordered tokio::spawn tasks reach Redis in
    /// reversed order — without CAS, the stale sentinel would clobber the fresh
    /// hard-limit write and propagate a false "cleared" state across replicas.
    ///
    /// Also refreshes metric gauges and publishes updated routing weights so all
    /// replicas reflect the recovery within the next sync tick.
    /// Broadcast a hard-limit recovery for an endpoint by name. The Redis
    /// sentinel key is derived from the name alone.
    async fn signal_hard_limit_recovery(&self, endpoint_name: &str) {
        if let Some(redis) = &self.redis {
            let mut conn = redis.clone();
            let key = format!("alb:hard:{}", endpoint_name);
            let now_epoch = Self::now_epoch();
            // Lua CAS: only write the sentinel if the current value is absent,
            // already the sentinel, or an expired hard-limit (epoch <= now).
            // Rejects a concurrent mark_hard_limited write with epoch > now.
            let script = redis::Script::new(
                r#"
                local current = redis.call('GET', KEYS[1])
                if current == false then
                    return redis.call('SET', KEYS[1], ARGV[1], 'EX', ARGV[2])
                end
                local n = tonumber(current)
                if n == nil or n <= tonumber(ARGV[3]) then
                    return redis.call('SET', KEYS[1], ARGV[1], 'EX', ARGV[2])
                end
                return 0
                "#,
            );
            tokio::spawn(async move {
                let result: redis::RedisResult<redis::Value> = script
                    .key(&key)
                    .arg(HARD_LIMIT_CLEARED_SENTINEL)
                    .arg(HARD_LIMIT_SENTINEL_TTL_SECS)
                    .arg(now_epoch)
                    .invoke_async(&mut conn)
                    .await;
                if let Err(e) = result {
                    tracing::warn!(error = %e, "redis sentinel write failed for hard-limit clear");
                }
            });
        }
        self.refresh_metrics_weights().await;
        self.publish_routing_weights().await;
    }

    fn pick_weighted_bucket<'a>(
        &self,
        effective: &[&'a RoutingCandidate],
        total_weight: f64,
        affinity_key: Option<&str>,
    ) -> &'a RoutingCandidate {
        let walk_buckets = |target: f64| -> &'a RoutingCandidate {
            let mut picked = effective.last().unwrap();
            let mut cumulative = 0.0;
            for c in effective {
                cumulative += c.weight;
                if target < cumulative {
                    picked = c;
                    break;
                }
            }
            picked
        };

        if let Some(key) = affinity_key {
            let target = (stable_affinity_hash(key) as f64 / u64::MAX as f64) * total_weight;
            walk_buckets(target)
        } else {
            let counter = self.robin.fetch_add(1, Ordering::Relaxed) as u64;
            let position = (counter.wrapping_mul(11400714819323198485) % 10000) as f64;
            let target = position / 10000.0 * total_weight;
            walk_buckets(target)
        }
    }

    fn pick_dynamic_capacity_v1<'a>(
        &self,
        effective: &[&'a RoutingCandidate],
        total_weight: f64,
        affinity_key: Option<&str>,
    ) -> &'a RoutingCandidate {
        let mut picked = self.pick_weighted_bucket(effective, total_weight, affinity_key);

        if affinity_key.is_some() && effective.len() == 2 {
            let other = if picked.endpoint == effective[0].endpoint {
                effective[1]
            } else {
                effective[0]
            };
            if picked.weight < other.weight * LEGACY_AFFINITY_OVERRIDE_RATIO {
                // Loud on purpose — see the StickyWeightedV2 override below for
                // the cascade rationale. Breaking affinity is a pool-health
                // warning sign, not routine.
                warn!(
                    strategy = RoutingStrategy::DynamicCapacityV1.as_str(),
                    affinity = affinity_key.unwrap_or("-"),
                    picked_account = self.endpoint_name(picked.endpoint),
                    picked_weight = format!("{:.3}", picked.weight),
                    other_account = self.endpoint_name(other.endpoint),
                    other_weight = format!("{:.3}", other.weight),
                    ratio = format!("{:.3}", picked.weight / other.weight),
                    "affinity broken: sticky endpoint too loaded, migrating session (cascade risk)"
                );
                picked = other;
            }
            // NOTE: Request-balance override intentionally disabled. The previous
            // implementation used Account.requests counters which are replica-local
            // and not pool-scoped, so multi-replica deployments could make routing
            // decisions on partial history. Re-enable only after adding shared,
            // pool-scoped counters that are synchronized across replicas.
        }

        picked
    }

    fn pick_sticky_weighted_v2<'a>(
        &self,
        effective: &[&'a RoutingCandidate],
        total_weight: f64,
        affinity_key: Option<&str>,
    ) -> &'a RoutingCandidate {
        let mut picked = self.pick_weighted_bucket(effective, total_weight, affinity_key);

        if let Some(key) = affinity_key {
            let best = effective
                .iter()
                .max_by(|a, b| a.weight.partial_cmp(&b.weight).unwrap())
                .copied()
                .unwrap();
            if best.endpoint != picked.endpoint
                && picked.weight < best.weight * STICKY_WEIGHTED_OVERRIDE_RATIO
            {
                // The sticky account is too loaded to keep this session. Do NOT
                // migrate to `best` (the global argmax): that target rotates every
                // request as utilizations drift, so a session chases it across the
                // whole pool and pays a cold-cache `cache_creation` charge on every
                // hop (measured in prod: a swept client ran a 1.18 create:read ratio
                // vs ~0.05 for sticky clients — it created more cache than it ever
                // read back). Instead re-pick over the healthy remainder using a
                // SALTED hash of the same affinity key: deterministic per session
                // (so the cache warms on the replacement and stays there) yet spread
                // across sessions (distinct keys → distinct replacements), and
                // independent of which account is momentarily `best`.
                let remaining: Vec<&RoutingCandidate> = effective
                    .iter()
                    .copied()
                    .filter(|c| c.endpoint != picked.endpoint)
                    .collect();
                let remaining_weight: f64 = remaining.iter().map(|c| c.weight).sum();
                let replacement = if remaining_weight > 0.0 {
                    // NUL separator can't appear in a header-derived affinity key
                    // (ip:client:agent:session), so the salted key never collides
                    // with a real one.
                    let salted = format!("{key}\u{0}migrate");
                    self.pick_weighted_bucket(&remaining, remaining_weight, Some(&salted))
                } else {
                    best
                };
                // Loud on purpose: sustained breaking means the pool is the
                // bottleneck — add capacity, don't tune the ratio.
                warn!(
                    strategy = RoutingStrategy::StickyWeightedV2.as_str(),
                    affinity = key,
                    picked_account = self.endpoint_name(picked.endpoint),
                    picked_weight = format!("{:.3}", picked.weight),
                    replacement_account = self.endpoint_name(replacement.endpoint),
                    replacement_weight = format!("{:.3}", replacement.weight),
                    best_account = self.endpoint_name(best.endpoint),
                    ratio = format!("{:.3}", picked.weight / best.weight),
                    "affinity broken: sticky endpoint too loaded, migrating session to stable replacement"
                );
                picked = replacement;
            }
        }

        picked
    }

    /// Select from a pre-filtered candidate slice using the configured routing strategy.
    fn pick_from_candidates(
        &self,
        effective: &[&RoutingCandidate],
        total_weight: f64,
        affinity_key: Option<&str>,
        tier: u32,
    ) -> EndpointIdx {
        let picked = match self.routing_strategy {
            RoutingStrategy::DynamicCapacityV1 => {
                self.pick_dynamic_capacity_v1(effective, total_weight, affinity_key)
            }
            RoutingStrategy::StickyWeightedV2 => {
                self.pick_sticky_weighted_v2(effective, total_weight, affinity_key)
            }
        };

        debug!(
            strategy = self.routing_strategy.as_str(),
            account = self.endpoint_name(picked.endpoint),
            tier = tier,
            gate = format!("{:.3}", picked.gate),
            gate_5h = format!("{:.3}", picked.gate_5h),
            gate_7d = format!("{:.3}", picked.gate_7d),
            waste_risk = format!("{:.3}", picked.wr),
            weight = format!("{:.3}", picked.weight),
            share = format!("{:.0}%", picked.weight / total_weight * 100.0),
            source = picked.source,
            candidates = effective.len(),
            affinity = affinity_key.unwrap_or("-"),
            "pick: selected"
        );
        picked.endpoint
    }

    /// Pick the best available endpoint (account or fallback upstream).
    ///
    /// Tiers are tried strictly in ascending priority order. Within a tier:
    /// healthy candidates (`gate < soft_limit`) are preferred; if none are healthy
    /// the tier degrades to its soft-limited candidates. Only when a tier has zero
    /// total weight (genuinely exhausted) does routing move to the next tier — so
    /// `soft_limit` is intra-tier load-shedding and never causes a tier jump. This
    /// guarantees free capacity is fully drained before any paid (overage/upstream)
    /// tier is touched.
    async fn pick_endpoint(
        &self,
        affinity_key: Option<&str>,
        model: &str,
        skip: &[EndpointIdx],
    ) -> Option<EndpointIdx> {
        let candidates = self.routing_candidates(model, skip).await;
        if candidates.is_empty() {
            debug!("pick: no available endpoints");
            return None;
        }

        // Unique priority tiers, ascending (0 = highest priority).
        let mut tiers: Vec<u32> = candidates.iter().map(|c| c.priority).collect();
        tiers.sort_unstable();
        tiers.dedup();

        for tier in &tiers {
            let tier_candidates: Vec<&RoutingCandidate> =
                candidates.iter().filter(|c| c.priority == *tier).collect();

            // Prefer healthy candidates; fall back to soft-limited ones within the tier.
            let healthy: Vec<&RoutingCandidate> = tier_candidates
                .iter()
                .filter(|c| c.gate < self.soft_limit)
                .copied()
                .collect();

            let (effective, degraded): (Vec<&RoutingCandidate>, bool) = if !healthy.is_empty() {
                (healthy, false)
            } else {
                (tier_candidates.clone(), true)
            };

            let total_weight: f64 = effective.iter().map(|c| c.weight).sum();
            if total_weight <= 0.0 {
                debug!(
                    tier = tier,
                    "pick: tier exhausted (zero weight), trying next"
                );
                continue;
            }

            if degraded {
                debug!(
                    tier = tier,
                    "pick: tier all soft-limited — degrading within tier"
                );
            }
            return Some(self.pick_from_candidates(&effective, total_weight, affinity_key, *tier));
        }

        debug!("pick: all tiers exhausted");
        None
    }

    /// Test-only convenience: parse rate-limit headers into endpoint `idx`.
    #[cfg(test)]
    async fn update_rate_info(&self, idx: usize, headers: &reqwest::header::HeaderMap) {
        let ep = &self.endpoints[idx];
        self.update_rate_info_for(&ep.rate_info, &ep.name, headers)
            .await;
    }

    /// Parse rate-limit headers from a response into the supplied
    /// `RateLimitInfo` lock.
    async fn update_rate_info_for(
        &self,
        rate_info: &RwLock<RateLimitInfo>,
        endpoint_name: &str,
        headers: &reqwest::header::HeaderMap,
    ) {
        let mut info = rate_info.write().await;

        // Debug: log all ratelimit headers
        for (name, value) in headers.iter() {
            let name_str = name.as_str();
            if name_str.contains("ratelimit") || name_str.contains("retry") {
                if let Ok(v) = value.to_str() {
                    tracing::trace!(
                        account = endpoint_name,
                        header = name_str,
                        value = v,
                        "rate-limit header"
                    );
                }
            }
        }

        // Parse representative claim FIRST — determines where 7d data is stored.
        // Model-specific claims (e.g., "seven_day_sonnet") route 7d data to a per-claim
        // entry so different models don't overwrite each other's utilization.
        let rep_claim = headers
            .get("anthropic-ratelimit-unified-representative-claim")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());

        if let Some(ref claim) = rep_claim {
            info.representative_claim = Some(claim.clone());
        }

        // Determine the claim key for 7d data storage.
        // If claim starts with "seven_day", use it verbatim (e.g., "seven_day_sonnet").
        // Otherwise default to "seven_day" (general bucket).
        let claim_key_7d = rep_claim
            .as_deref()
            .filter(|c| c.starts_with("seven_day"))
            .unwrap_or("seven_day");

        // Capture 5h utilization (flat — no per-model sub-budgets observed for 5h).
        // Track whether we got utilization for sticky status fix (Bug #1).
        let now_epoch = Self::now_epoch();
        let got_5h_util = if let Some(v) = headers.get("anthropic-ratelimit-unified-5h-utilization")
        {
            if let Ok(s) = v.to_str() {
                info.utilization_5h = s.parse::<f64>().ok().map(|v| v.clamp(0.0, 1.0));
                true
            } else {
                false
            }
        } else {
            false
        };

        // Capture 7d utilization → store in claims_7d[claim_key].
        let got_7d_util = if let Some(v) = headers.get("anthropic-ratelimit-unified-7d-utilization")
        {
            if let Ok(s) = v.to_str() {
                if let Ok(util) = s.parse::<f64>() {
                    let entry = info.claims_7d.entry(claim_key_7d.to_string()).or_default();
                    entry.utilization = Some(util.clamp(0.0, 1.0));
                    entry.last_seen = now_epoch;
                    true
                } else {
                    false
                }
            } else {
                false
            }
        } else {
            false
        };

        // Capture per-window reset timestamps (epoch seconds).
        // Sanity-capped: 5h window can't reset >5h out, 7d can't reset >7d out (Bug #6).
        if let Some(v) = headers.get("anthropic-ratelimit-unified-5h-reset") {
            if let Ok(s) = v.to_str() {
                if let Ok(epoch) = s.parse::<u64>() {
                    if epoch <= now_epoch + 18000 {
                        info.reset_5h = Some(epoch);
                    }
                }
            }
        }
        // 7d reset → only update existing claims_7d entries. Creating a placeholder
        // with utilization=None would shadow real fallback entries in resolve_7d_claim
        // (model-specific key takes priority over "seven_day" via or_else).
        if let Some(v) = headers.get("anthropic-ratelimit-unified-7d-reset") {
            if let Ok(s) = v.to_str() {
                if let Ok(epoch) = s.parse::<u64>() {
                    if epoch <= now_epoch + 604800 {
                        if let Some(entry) = info.claims_7d.get_mut(claim_key_7d) {
                            entry.reset = Some(epoch);
                            entry.last_seen = now_epoch;
                        }
                    }
                }
            }
        }

        // Capture per-window status. These signal API-side pressure (burst limits, concurrent
        // request limits, per-model sub-limits) that raw utilization percentages don't reflect.
        // If the API sent utilization for a window but NO status header, clear stale status —
        // absence of the header means pressure has subsided (Bug #1).
        if let Some(v) = headers.get("anthropic-ratelimit-unified-5h-status") {
            if let Ok(s) = v.to_str() {
                info.status_5h = Some(s.to_string());
            }
        } else if got_5h_util {
            info.status_5h = None;
        }
        // 7d status → only update existing claims_7d entries (same shadowing concern).
        if let Some(v) = headers.get("anthropic-ratelimit-unified-7d-status") {
            if let Ok(s) = v.to_str() {
                if let Some(entry) = info.claims_7d.get_mut(claim_key_7d) {
                    entry.status = Some(s.to_string());
                    entry.last_seen = now_epoch;
                }
            }
        } else if got_7d_util {
            // Status absent but util present → pressure subsided for this claim
            if let Some(entry) = info.claims_7d.get_mut(claim_key_7d) {
                entry.status = None;
            }
        }

        // Fable included-band triplet ("7d_oi" = 7d overage-included). Emitted
        // ONLY on Fable responses (verified live 2026-07-21, LAB-387): a sonnet
        // response from the same account carries no 7d_oi headers, and there is
        // no `seven_day_fable` representative claim on the wire. Normalised into
        // the internal FABLE_BAND_CLAIM entry so the standard claims machinery
        // applies. Absence of the triplet (every non-Fable response) must NOT
        // clear the entry — reset-based eviction below handles staleness,
        // exactly like every other claim. Anchored on the utilization header so
        // a partial triplet never creates a utilization-less placeholder (same
        // shadowing rule as the general 7d claim).
        if let Some(v) = headers.get("anthropic-ratelimit-unified-7d_oi-utilization") {
            if let Ok(s) = v.to_str() {
                if let Ok(util) = s.parse::<f64>() {
                    let first_sighting = !info.claims_7d.contains_key(FABLE_BAND_CLAIM);
                    let entry = info
                        .claims_7d
                        .entry(FABLE_BAND_CLAIM.to_string())
                        .or_default();
                    entry.utilization = Some(util.clamp(0.0, 1.0));
                    entry.last_seen = now_epoch;
                    entry.reset = headers
                        .get("anthropic-ratelimit-unified-7d_oi-reset")
                        .and_then(|v| v.to_str().ok())
                        .and_then(|s| s.parse::<u64>().ok())
                        .filter(|&epoch| epoch <= now_epoch + 604800) // 7d sanity cap
                        .or(entry.reset);
                    // Status absent within a present triplet = pressure subsided
                    // (same semantics as the general 7d claim, Bug #1).
                    entry.status = headers
                        .get("anthropic-ratelimit-unified-7d_oi-status")
                        .and_then(|v| v.to_str().ok())
                        .map(|s| s.to_string());
                    if first_sighting {
                        info!(
                            account = endpoint_name,
                            utilization = util,
                            "fable band claim first observed (7d_oi headers) — \
                             fable-aware routing active for this account"
                        );
                    }
                }
            }
        }

        // Overage (paid extra usage) — account-level, covers whichever subscription
        // window is exhausted. `overage-in-use` is always overwritten: header absent
        // or "false" → false (so demotion auto-clears when the window refills).
        info.overage_in_use = headers
            .get("anthropic-ratelimit-unified-overage-in-use")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.eq_ignore_ascii_case("true"))
            .unwrap_or(false);
        if info.overage_in_use {
            info.overage_status = headers
                .get("anthropic-ratelimit-unified-overage-status")
                .and_then(|v| v.to_str().ok())
                .map(|s| s.to_string());
            info.overage_utilization = headers
                .get("anthropic-ratelimit-unified-overage-utilization")
                .and_then(|v| v.to_str().ok())
                .and_then(|s| s.parse::<f64>().ok())
                .map(|v| v.clamp(0.0, 1.0));
            info.overage_reset = headers
                .get("anthropic-ratelimit-unified-overage-reset")
                .and_then(|v| v.to_str().ok())
                .and_then(|s| s.parse::<u64>().ok())
                .filter(|&epoch| epoch <= now_epoch + 2_678_400); // ≤31d sanity cap
        } else {
            // Not in overage — clear stale overage data so routing doesn't read it.
            info.overage_status = None;
            info.overage_utilization = None;
            info.overage_reset = None;
        }

        // Evict stale claims: expired resets (reset <= now) or reset-less entries
        // not seen in >24h. This aligns with load_state() which drops all reset-less
        // entries on boot — the 24h grace period covers the normal case where
        // utilization arrives before reset in the same response cycle.
        const CLAIMS_STALE_SECS: u64 = 86400;
        info.claims_7d.retain(|_, c| {
            if let Some(r) = c.reset {
                r > now_epoch
            } else {
                // No reset — keep only if recently seen
                c.last_seen > 0 && now_epoch.saturating_sub(c.last_seen) < CLAIMS_STALE_SECS
            }
        });

        // Derive flat convenience fields from claims_7d (backward compat for
        // logs/stats; also the model-agnostic fallback input — see
        // derive_flat_7d_fields for why only all-traffic claims participate).
        (info.utilization_7d, info.reset_7d, info.status_7d) =
            derive_flat_7d_fields(&info.claims_7d);

        // Derive unified utilization = max across all windows (5h + all-traffic
        // 7d claims — same allowlist as above; this is the brake's last-resort
        // fallback when both adjusted windows are unavailable).
        // Recompute unconditionally so stale unified values don't survive eviction.
        // Include 5h if reset is absent (no staleness info) or in the future;
        // exclude only when reset is present AND expired (stale data).
        let mut max_util: Option<f64> = info
            .utilization_5h
            .filter(|_| info.reset_5h.is_none_or(|r| r > now_epoch));
        for (key, cd) in info.claims_7d.iter() {
            if !claim_gates_all_traffic(key) {
                continue;
            }
            if let Some(u) = cd.utilization {
                max_util = Some(max_util.map_or(u, |cur| cur.max(u)));
            }
        }
        info.utilization = max_util;

        // Legacy headers (still try them)
        let mut got_legacy = false;
        if let Some(v) = headers.get("x-ratelimit-remaining-requests") {
            if let Ok(s) = v.to_str() {
                info.remaining_requests = s.parse().ok();
                got_legacy = true;
            }
        }
        if let Some(v) = headers.get("x-ratelimit-remaining-tokens") {
            if let Ok(s) = v.to_str() {
                info.remaining_tokens = s.parse().ok();
                got_legacy = true;
            }
        }
        if let Some(v) = headers.get("x-ratelimit-limit-requests") {
            if let Ok(s) = v.to_str() {
                info.limit_requests = s.parse().ok();
                got_legacy = true;
            }
        }
        if let Some(v) = headers.get("x-ratelimit-limit-tokens") {
            if let Ok(s) = v.to_str() {
                info.limit_tokens = s.parse().ok();
                got_legacy = true;
            }
        }

        // Only advance last_updated when we actually parsed rate-limit data.
        // Responses without rate headers (e.g. 5xx) must not clear
        // stale_after_hard_limit, which relies on last_updated <= hard_limited_until.
        let parsed_any = got_5h_util || got_7d_util || rep_claim.is_some() || got_legacy;
        if parsed_any {
            info.last_updated = Some(Instant::now());
            info.last_updated_epoch = Some(Self::now_epoch());
        }

        trace!(
            account = endpoint_name,
            utilization = ?info.utilization,
            util_7d = ?info.utilization_7d,
            util_5h = ?info.utilization_5h,
            reset_5h = ?info.reset_5h,
            reset_7d = ?info.reset_7d,
            status_5h = ?info.status_5h,
            status_7d = ?info.status_7d,
            claim = ?info.representative_claim,
            n_claims_7d = info.claims_7d.len(),
            remaining_requests = ?info.remaining_requests,
            remaining_tokens = ?info.remaining_tokens,
            "rate info updated"
        );

        // Fire-and-forget: publish rate info to Redis for cross-replica sync
        if let Some(redis) = &self.redis {
            let rate_data = RedisRateInfo {
                utilization: info.utilization,
                utilization_5h: info.utilization_5h,
                utilization_7d: info.utilization_7d,
                reset_5h: info.reset_5h,
                reset_7d: info.reset_7d,
                status_5h: info.status_5h.clone(),
                status_7d: info.status_7d.clone(),
                claims_7d: info.claims_7d.clone(),
                representative_claim: info.representative_claim.clone(),
                remaining_requests: info.remaining_requests,
                remaining_tokens: info.remaining_tokens,
                limit_requests: info.limit_requests,
                limit_tokens: info.limit_tokens,
                overage_in_use: info.overage_in_use,
                overage_status: info.overage_status.clone(),
                overage_utilization: info.overage_utilization,
                overage_reset: info.overage_reset,
                updated_at: Self::now_epoch(),
            };
            // Compute TTL from earliest reset timestamp
            let now_epoch = Self::now_epoch();
            let min_reset = info
                .reset_5h
                .into_iter()
                .chain(info.claims_7d.values().filter_map(|c| c.reset))
                .min();
            let ttl = min_reset
                .map(|r| r.saturating_sub(now_epoch).max(60))
                .unwrap_or(3600); // default 1h if no reset known

            let mut conn = redis.clone();
            let key = format!("alb:rate:{}", endpoint_name);
            tokio::spawn(async move {
                if let Ok(json) = serde_json::to_string(&rate_data) {
                    use redis::AsyncCommands;
                    let result: redis::RedisResult<()> = conn.set_ex(&key, json, ttl).await;
                    if let Err(e) = result {
                        tracing::warn!(error = %e, "redis rate info write failed");
                    }
                }
            });
        }
    }

    /// Test-only convenience: mark endpoint `idx` as hard rate-limited.
    #[cfg(test)]
    async fn mark_hard_limited(&self, idx: usize, headers: &reqwest::header::HeaderMap) {
        let ep = &self.endpoints[idx];
        self.mark_hard_limited_for(&ep.rate_info, &ep.name, headers)
            .await;
    }

    /// Apply a 429 cooldown to the supplied `RateLimitInfo` lock.
    async fn mark_hard_limited_for(
        &self,
        rate_info: &RwLock<RateLimitInfo>,
        endpoint_name: &str,
        headers: &reqwest::header::HeaderMap,
    ) {
        let mut info = rate_info.write().await;

        let raw_retry_after = headers
            .get("retry-after")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());

        // Detect transient burst 429: x-should-retry present but no retry-after
        // and no rate-limit headers. These are per-minute burst limits, not
        // capacity exhaustion — use exponential backoff and don't poison state.
        let has_rate_headers = headers.keys().any(|k| {
            let name = k.as_str();
            name.starts_with("anthropic-ratelimit-requests")
                || name.starts_with("anthropic-ratelimit-tokens")
                || name.starts_with("anthropic-ratelimit-unified-")
                || name.starts_with("x-ratelimit-")
        });
        let should_retry =
            headers.get("x-should-retry").and_then(|v| v.to_str().ok()) == Some("true");
        let is_burst_limit = should_retry && raw_retry_after.is_none() && !has_rate_headers;

        let cooldown = if is_burst_limit {
            info.consecutive_burst_429s = info.consecutive_burst_429s.saturating_add(1);
            // Exponential backoff: 5s → 10s → 20s → 40s → 60s (cap).
            // RPM windows are typically 60s; 1s was too short and caused thrashing.
            let burst_secs: u64 = match info.consecutive_burst_429s {
                1 => 5,
                2 => 10,
                3 => 20,
                4 => 40,
                _ => 60,
            };
            Duration::from_secs(burst_secs)
        } else {
            info.consecutive_burst_429s = 0;
            if let Some(ref s) = raw_retry_after {
                if let Ok(secs) = s.parse::<f64>() {
                    if secs.is_finite() && secs > 0.0 && secs < 86400.0 {
                        Duration::from_secs_f64(secs)
                    } else {
                        self.cooldown
                    }
                } else {
                    self.cooldown
                }
            } else {
                self.cooldown
            }
        };

        let until = Instant::now() + cooldown;
        info.hard_limited_until = Some(until);
        // Only poison remaining counts when we have actual rate-limit data
        // confirming exhaustion. Burst 429s have no such data.
        if !is_burst_limit {
            info.remaining_requests = Some(0);
            info.remaining_tokens = Some(0);
        }
        info.last_updated = Some(Instant::now());
        info.last_updated_epoch = Some(Self::now_epoch());

        warn!(
            account = endpoint_name,
            cooldown_secs = cooldown.as_secs(),
            retry_after_raw = ?raw_retry_after,
            burst = is_burst_limit,
            consecutive_burst = info.consecutive_burst_429s,
            "account hard rate-limited (429), cooling down"
        );

        // Propagate to Redis for cross-replica awareness
        if let Some(redis) = &self.redis {
            let mut conn = redis.clone();
            let key = format!("alb:hard:{}", endpoint_name);
            let until_epoch = Self::now_epoch()
                + cooldown.as_secs()
                + if cooldown.subsec_nanos() > 0 { 1 } else { 0 };
            let ttl = cooldown.as_secs().max(1);
            tokio::spawn(async move {
                use redis::AsyncCommands;
                let result: redis::RedisResult<()> = conn.set_ex(&key, until_epoch, ttl).await;
                if let Err(e) = result {
                    tracing::warn!(error = %e, "redis SETEX failed for hard-limit propagation");
                }
            });
        }

        // Drop the write lock explicitly so refresh_metrics_weights can take
        // its read lock without contention.
        drop(info);

        // Hard-limit transitions are sparse, high-impact events. Refresh
        // metric gauges immediately so the dashboard reflects the dropped
        // account within seconds, not after the next probe cycle.
        self.refresh_metrics_weights().await;
        // Republish weights to Redis so non-probing replicas pick up the
        // new routing gauge immediately rather than reading a stale
        // alb:weight:{account} until the next probe cycle.
        self.publish_routing_weights().await;
    }

    /// Record one upstream transport failure (ETIMEDOUT/reset/closed/DNS)
    /// against an endpoint. After TRANSPORT_FAILURE_THRESHOLD consecutive
    /// failures the endpoint is circuit-broken out of routing for
    /// `transport_cooldown` so a stateless affinity recompute stops paying
    /// ~2 connect timeouts per request against a persistently-dead endpoint.
    /// Transport health only — never touches the 429 `hard_limited_until` path.
    async fn record_transport_failure(&self, endpoint_idx: usize) {
        let ep = &self.endpoints[endpoint_idx];
        let mut info = ep.rate_info.write().await;
        let now = Instant::now();
        // Cooldown elapsed → fresh era: the expired breaker's failures don't
        // carry over, so re-opening takes a full threshold of new evidence.
        if info
            .transport_unhealthy_until
            .is_some_and(|until| now >= until)
        {
            info.transport_unhealthy_until = None;
            info.consecutive_transport_failures = 0;
        }
        info.consecutive_transport_failures = info.consecutive_transport_failures.saturating_add(1);
        if info.consecutive_transport_failures >= TRANSPORT_FAILURE_THRESHOLD
            && info.transport_unhealthy_until.is_none()
        {
            info.transport_unhealthy_until = Some(now + self.transport_cooldown);
            warn!(
                endpoint = ep.name,
                consecutive_failures = info.consecutive_transport_failures,
                cooldown_secs = self.transport_cooldown.as_secs(),
                "transport circuit-breaker OPEN: endpoint leaves the routing pool"
            );
        }
    }

    /// Clear transport-failure state after a successful forward (any HTTP
    /// response proves the transport path is alive — even a 429 or 5xx).
    async fn record_transport_success(&self, endpoint_idx: usize) {
        let ep = &self.endpoints[endpoint_idx];
        // Fast path: requests are overwhelmingly healthy-on-healthy; skip the
        // write lock unless there is actually state to clear.
        {
            let info = ep.rate_info.read().await;
            if info.consecutive_transport_failures == 0 && info.transport_unhealthy_until.is_none()
            {
                return;
            }
        }
        let mut info = ep.rate_info.write().await;
        if info.transport_unhealthy_until.is_some() {
            info!(
                endpoint = ep.name,
                "transport circuit-breaker CLOSED: endpoint recovered"
            );
        }
        info.consecutive_transport_failures = 0;
        info.transport_unhealthy_until = None;
    }

    /// Sync shared state from Redis: hard limits + rate info.
    /// Called periodically by background task.
    async fn sync_from_redis(&self) {
        let redis = match &self.redis {
            Some(r) => r,
            None => return,
        };
        use redis::AsyncCommands;
        let mut conn = redis.clone();
        let now_epoch = Self::now_epoch();
        let now_instant = Instant::now();

        // Sync target list over the endpoint pool. OpenAI endpoints are
        // skipped: they carry no Anthropic rate-limit data.
        struct SyncTarget<'a> {
            name: &'a str,
            rate_info: &'a RwLock<RateLimitInfo>,
            weight: &'a AtomicU64,
            share: &'a AtomicU64,
            gate: &'a AtomicU64,
        }
        let mut targets: Vec<SyncTarget<'_>> = Vec::new();
        for e in &self.endpoints {
            if e.protocol == Protocol::OpenAI {
                continue;
            }
            targets.push(SyncTarget {
                name: &e.name,
                rate_info: &e.rate_info,
                weight: &e.last_routing_weight,
                share: &e.last_routing_share,
                gate: &e.last_effective_gate,
            });
        }

        // 1. Sync hard limits (MGET for all targets in one round-trip)
        let hard_keys: Vec<String> = targets
            .iter()
            .map(|t| format!("alb:hard:{}", t.name))
            .collect();

        if let Ok(values) = conn.mget::<_, Vec<Option<String>>>(&hard_keys).await {
            for (i, remote) in values.into_iter().enumerate() {
                let remote = remote.and_then(|value| value.parse::<u64>().ok());
                match classify_hard_limit_sync(remote, now_epoch, now_instant) {
                    HardLimitSync::Clear => {
                        // Another replica observed recovery. Clear our local
                        // `hard_limited_until` so pick_account stops excluding
                        // the account. Do NOT reset `consecutive_burst_429s` —
                        // that counter tracks THIS replica's burst-429 backoff
                        // escalation, and resetting it based on another replica's
                        // unrelated success would mask abuse patterns and thrash
                        // the exponential backoff.
                        let mut info = targets[i].rate_info.write().await;
                        if info.hard_limited_until.is_some() {
                            info.hard_limited_until = None;
                            trace!(
                                endpoint = targets[i].name,
                                "synced hard-limit clear sentinel from redis"
                            );
                        }
                    }
                    HardLimitSync::Update(until_instant) => {
                        let mut info = targets[i].rate_info.write().await;
                        let should_update = info
                            .hard_limited_until
                            .is_none_or(|local| until_instant > local);
                        if should_update {
                            info.hard_limited_until = Some(until_instant);
                            trace!(endpoint = targets[i].name, "synced hard-limit from redis");
                        }
                    }
                    HardLimitSync::Ignore => {}
                }
            }
        }

        // 2. Sync rate info (MGET for all targets in one round-trip)
        let rate_keys: Vec<String> = targets
            .iter()
            .map(|t| format!("alb:rate:{}", t.name))
            .collect();

        if let Ok(values) = conn.mget::<_, Vec<Option<String>>>(&rate_keys).await {
            for (i, val) in values.iter().enumerate() {
                if let Some(json) = val {
                    if let Ok(remote) = serde_json::from_str::<RedisRateInfo>(json) {
                        let mut info = targets[i].rate_info.write().await;
                        // "Most recent wins": only apply remote data if it's newer
                        // Both ages use wall-clock epoch to avoid mixed-clock-domain bugs
                        let local_age = info
                            .last_updated_epoch
                            .map(|epoch| now_epoch.saturating_sub(epoch))
                            .unwrap_or(u64::MAX);
                        let remote_age = now_epoch.saturating_sub(remote.updated_at);
                        if remote_age < local_age {
                            info.utilization = remote.utilization;
                            info.utilization_5h = remote.utilization_5h;
                            info.utilization_7d = remote.utilization_7d;
                            info.reset_5h = remote.reset_5h;
                            info.reset_7d = remote.reset_7d;
                            info.status_5h = remote.status_5h;
                            info.status_7d = remote.status_7d;
                            info.claims_7d = remote.claims_7d;
                            info.representative_claim = remote.representative_claim;
                            info.remaining_requests = remote.remaining_requests;
                            info.remaining_tokens = remote.remaining_tokens;
                            info.limit_requests = remote.limit_requests;
                            info.limit_tokens = remote.limit_tokens;
                            info.overage_in_use = remote.overage_in_use;
                            info.overage_status = remote.overage_status;
                            info.overage_utilization = remote.overage_utilization;
                            info.overage_reset = remote.overage_reset;
                            info.last_updated = Some(now_instant);
                            info.last_updated_epoch = Some(remote.updated_at);
                            trace!(
                                endpoint = targets[i].name,
                                remote_age,
                                "synced rate info from redis"
                            );
                        }
                    }
                }
            }
        }

        // 3. Sync precomputed routing weights (published by probing pod)
        let weight_keys: Vec<String> = targets
            .iter()
            .map(|t| format!("alb:weight:{}", t.name))
            .collect();
        if let Ok(values) = conn.mget::<_, Vec<Option<String>>>(&weight_keys).await {
            for (i, val) in values.iter().enumerate() {
                if let Some(csv) = val {
                    let mut parts = csv.splitn(3, ',');
                    if let (Some(w_str), Some(s_str)) = (parts.next(), parts.next()) {
                        if let (Ok(w), Ok(s)) = (w_str.parse::<f64>(), s_str.parse::<f64>()) {
                            targets[i].weight.store(w.to_bits(), Ordering::Relaxed);
                            targets[i].share.store(s.to_bits(), Ordering::Relaxed);
                            // Gate is optional (backward compat with older publishers)
                            if let Some(Ok(g)) = parts.next().map(|g| g.parse::<f64>()) {
                                targets[i].gate.store(g.to_bits(), Ordering::Relaxed);
                            }
                        }
                    }
                }
            }
        }

        // 4. Flush this replica's transport-error deltas into the shared Redis
        //    hash so the fleet-wide by-kind count is visible cluster-wide.
        //    Drains the local accumulator; the cluster_info refresh below then
        //    reads the freshly-flushed total back via HGETALL.
        self.flush_transport_errors().await;

        // 5. Refresh cluster info cache for /_stats endpoint
        let info = self.cluster_info().await;
        if let Ok(mut cache) = self.cluster_info_cache.lock() {
            *cache = info;
        }
    }

    /// Lock the transport-error accumulator, recovering — and clearing — a
    /// poisoned lock. The map is a plain counter store: a panicking holder
    /// cannot leave it logically inconsistent, only stale by one increment.
    /// Clearing the poison matters because the other lock sites (the two
    /// increment paths and the local `/metrics` fallback) use `if let Ok` /
    /// `.map()` and would otherwise silently skip forever after one panic.
    fn lock_transport_errors(&self) -> std::sync::MutexGuard<'_, HashMap<&'static str, u64>> {
        match self.upstream_transport_errors.lock() {
            Ok(g) => g,
            Err(poisoned) => {
                self.upstream_transport_errors.clear_poison();
                poisoned.into_inner()
            }
        }
    }

    /// Flush this replica's accumulated transport-error deltas into the shared
    /// Redis hash (`TRANSPORT_ERRORS_KEY`) so the fleet-wide count is visible
    /// cluster-wide. `upstream_transport_errors` is a DELTA accumulator: it is
    /// drained here each tick and its counts folded into Redis via `HINCRBY`,
    /// so the same delta is never pushed twice (no double-counting across
    /// ticks). On Redis failure the drained deltas are returned to the local
    /// map — they retry next tick and stay visible via the local metrics
    /// fallback rather than being lost. No-op without Redis, so single-instance
    /// deployments keep accumulating locally; on an idle tick the TTL is still
    /// refreshed so the fleet-wide hash never expires under healthy traffic.
    async fn flush_transport_errors(&self) {
        let redis = match &self.redis {
            Some(r) => r,
            None => return,
        };

        // Drain the accumulator atomically: take the deltas AND reset to empty
        // under one lock, so any increment arriving mid-flush belongs to the
        // NEXT tick's delta and cannot be double-flushed.
        let deltas: Vec<(&'static str, u64)> = {
            let mut m = self.lock_transport_errors();
            m.drain().filter(|(_, n)| *n > 0).collect()
        };
        if deltas.is_empty() {
            // No new errors this tick — still refresh the TTL. The hash must
            // only expire once the whole fleet has been down for the TTL
            // window; skipping this would wipe the fleet-wide counter after
            // 48h of perfectly healthy, error-free traffic. EXPIRE on a
            // not-yet-existing key is a no-op, so this is safe pre-first-error.
            let mut conn = redis.clone();
            let result: redis::RedisResult<()> = redis::cmd("EXPIRE")
                .arg(TRANSPORT_ERRORS_KEY)
                .arg(TRANSPORT_ERRORS_TTL_SECS)
                .query_async(&mut conn)
                .await;
            if let Err(e) = result {
                warn!(error = %e, "redis EXPIRE failed for transport-errors TTL refresh");
            }
            return;
        }

        // One round-trip: HINCRBY every kind, then refresh the TTL. Low-level
        // cmd/arg form so this does not depend on generated fluent pipe methods.
        let mut conn = redis.clone();
        let mut pipe = redis::pipe();
        for (kind, n) in &deltas {
            pipe.cmd("HINCRBY")
                .arg(TRANSPORT_ERRORS_KEY)
                .arg(*kind)
                .arg(*n)
                .ignore();
        }
        pipe.cmd("EXPIRE")
            .arg(TRANSPORT_ERRORS_KEY)
            .arg(TRANSPORT_ERRORS_TTL_SECS)
            .ignore();

        let result: redis::RedisResult<()> = pipe.query_async(&mut conn).await;
        if let Err(e) = result {
            // Redis is unreachable (or the pipeline reply was lost) — return the
            // drained deltas to the local accumulator so error signal is not
            // dropped. This is at-least-once: if the connection died AFTER Redis
            // applied some HINCRBYs, re-queuing can over-count by a few next
            // tick. For an error *counter* that bias is correct — a slight
            // over-report beats a silently missed egress fault. (Contrast
            // record_budget_usage, which deletes-on-failure: over-counting a
            // budget would wrongly throttle a client.) The deltas also stay
            // visible via the local metrics fallback until Redis heals.
            warn!(error = %e, "redis HINCRBY failed for transport errors; re-queuing deltas locally");
            // Poison-recovering lock: an `if let Ok` here would silently DROP
            // every drained delta if the mutex got poisoned mid-cycle.
            let mut m = self.lock_transport_errors();
            for (kind, n) in deltas {
                *m.entry(kind).or_insert(0) += n;
            }
        }
    }
    async fn cluster_info(&self) -> Option<serde_json::Value> {
        let redis = self.redis.as_ref()?;
        use redis::AsyncCommands;
        let mut conn = redis.clone();
        let mut redis_ok = true;

        // Count active replicas via SCAN (non-blocking, unlike KEYS)
        let mut replicas = 0u64;
        let mut cursor: u64 = 0;
        loop {
            let result: redis::RedisResult<(u64, Vec<String>)> = redis::cmd("SCAN")
                .arg(cursor)
                .arg("MATCH")
                .arg("alb:heartbeat:*")
                .arg("COUNT")
                .arg(100)
                .query_async(&mut conn)
                .await;
            match result {
                Ok((next_cursor, keys)) => {
                    replicas += keys.len() as u64;
                    cursor = next_cursor;
                    if cursor == 0 {
                        break;
                    }
                }
                Err(e) => {
                    warn!(error = %e, "redis SCAN failed in cluster_info");
                    redis_ok = false;
                    break;
                }
            }
        }

        // Aggregate budget usage from Redis (batch MGET)
        let mut redis_budgets = serde_json::Map::new();
        if redis_ok && !self.client_budgets.is_empty() {
            let today = Self::now_epoch() / 86400;
            let client_ids: Vec<&String> = self.client_budgets.keys().collect();
            let budget_keys: Vec<String> = client_ids
                .iter()
                .map(|id| format!("alb:budget:{id}:{today}"))
                .collect();
            match conn.mget::<_, Vec<Option<u64>>>(&budget_keys).await {
                Ok(values) => {
                    for (i, client_id) in client_ids.iter().enumerate() {
                        let used = values.get(i).copied().flatten().unwrap_or(0);
                        let limit = self.client_budgets[*client_id];
                        redis_budgets.insert(
                            (*client_id).clone(),
                            serde_json::json!({ "limit": limit, "used": used }),
                        );
                    }
                }
                Err(e) => {
                    warn!(error = %e, "redis MGET failed for budget aggregation");
                    redis_ok = false;
                }
            }
        }

        // Aggregate upstream transport errors from the shared Redis hash so the
        // fleet-wide by-kind count surfaces on the dashboard/metrics endpoint.
        // Only included when the HGETALL succeeds: its absence tells the metrics
        // handler to fall back to this replica's local accumulator.
        let mut transport_errors: Option<serde_json::Map<String, serde_json::Value>> = None;
        if redis_ok {
            match conn
                .hgetall::<_, HashMap<String, u64>>(TRANSPORT_ERRORS_KEY)
                .await
            {
                Ok(map) => {
                    let mut te = serde_json::Map::new();
                    for (kind, n) in map {
                        te.insert(kind, serde_json::json!(n));
                    }
                    transport_errors = Some(te);
                }
                Err(e) => {
                    warn!(error = %e, "redis HGETALL failed for transport-error aggregation");
                    redis_ok = false;
                }
            }
        }

        let mut out = serde_json::Map::new();
        out.insert("redis_connected".into(), serde_json::json!(redis_ok));
        out.insert("replicas_seen".into(), serde_json::json!(replicas));
        out.insert(
            "budget_usage".into(),
            serde_json::Value::Object(redis_budgets),
        );
        if let Some(te) = transport_errors {
            out.insert("transport_errors".into(), serde_json::Value::Object(te));
        }
        Some(serde_json::Value::Object(out))
    }
}

// ── Token usage extraction ──────────────────────────────────────────

#[derive(Default, Debug, Clone)]
struct TokenUsage {
    input_tokens: u64,
    output_tokens: u64,
    cache_creation_input_tokens: u64,
    cache_read_input_tokens: u64,
}

impl TokenUsage {
    /// Parse usage from an Anthropic API response body (non-streaming JSON).
    fn from_response_body(body: &serde_json::Value) -> Self {
        let usage = match body.get("usage") {
            Some(u) => u,
            None => return Self::default(),
        };
        Self {
            input_tokens: usage
                .get("input_tokens")
                .and_then(|v| v.as_u64())
                .unwrap_or(0),
            output_tokens: usage
                .get("output_tokens")
                .and_then(|v| v.as_u64())
                .unwrap_or(0),
            cache_creation_input_tokens: usage
                .get("cache_creation_input_tokens")
                .and_then(|v| v.as_u64())
                .unwrap_or(0),
            cache_read_input_tokens: usage
                .get("cache_read_input_tokens")
                .and_then(|v| v.as_u64())
                .unwrap_or(0),
        }
    }

    /// Parse usage from SSE chunks (accumulated from streaming response).
    /// Looks for message_start (input_tokens, cache tokens) and message_delta (output_tokens).
    fn from_sse_text(text: &str) -> Self {
        let mut usage = Self::default();
        for line in text.lines() {
            let line = line.trim();
            if !line.starts_with("data: ") {
                continue;
            }
            let data = &line[6..];
            let Ok(event) = serde_json::from_str::<serde_json::Value>(data) else {
                continue;
            };
            let event_type = event.get("type").and_then(|t| t.as_str()).unwrap_or("");
            match event_type {
                "message_start" => {
                    if let Some(msg_usage) = event.get("message").and_then(|m| m.get("usage")) {
                        usage.input_tokens = msg_usage
                            .get("input_tokens")
                            .and_then(|v| v.as_u64())
                            .unwrap_or(0);
                        usage.cache_creation_input_tokens = msg_usage
                            .get("cache_creation_input_tokens")
                            .and_then(|v| v.as_u64())
                            .unwrap_or(0);
                        usage.cache_read_input_tokens = msg_usage
                            .get("cache_read_input_tokens")
                            .and_then(|v| v.as_u64())
                            .unwrap_or(0);
                    }
                }
                "message_delta" => {
                    if let Some(delta_usage) = event.get("usage") {
                        usage.output_tokens = delta_usage
                            .get("output_tokens")
                            .and_then(|v| v.as_u64())
                            .unwrap_or(0);
                    }
                }
                _ => {}
            }
        }
        usage
    }

    fn is_empty(&self) -> bool {
        self.input_tokens == 0
            && self.output_tokens == 0
            && self.cache_creation_input_tokens == 0
            && self.cache_read_input_tokens == 0
    }
}

/// Inject account authentication headers. Handles API keys, OAuth tokens,
/// and passthrough mode. For OAuth, merges required beta flags with any
/// existing flags from the client.
fn inject_account_auth(headers: &mut axum::http::HeaderMap, token: &str, passthrough: bool) {
    if passthrough {
        return;
    }
    headers.remove("authorization");
    headers.remove("x-api-key");
    if token.starts_with("sk-ant-api") {
        headers.insert("x-api-key", HeaderValue::from_str(token).unwrap());
    } else if token.starts_with("sk-ant-oat") {
        headers.insert(
            "authorization",
            HeaderValue::from_str(&format!("Bearer {}", token)).unwrap(),
        );
        headers.insert(
            "anthropic-dangerous-direct-browser-access",
            HeaderValue::from_static("true"),
        );
        // Merge required OAuth beta flags with any existing client flags
        // Use get_all to handle multiple anthropic-beta headers
        let mut flags: Vec<String> = headers
            .get_all("anthropic-beta")
            .iter()
            .filter_map(|v| v.to_str().ok())
            .flat_map(|s| s.split(','))
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();
        for flag in OAUTH_BETA_FLAGS {
            if !flags.iter().any(|f| f == flag) {
                flags.push(flag.to_string());
            }
        }
        headers.insert(
            "anthropic-beta",
            HeaderValue::from_str(&flags.join(",")).unwrap(),
        );
    } else {
        headers.insert("x-api-key", HeaderValue::from_str(token).unwrap());
    }
}

/// Client identity extracted from request headers.
struct RequestContext {
    client_id: String,
    client_ver: String,
    agent_id: String,
    session_id: String,
}

/// Build the routing affinity key. `fp` is the content fingerprint
/// (system+first-user digest); when present it is APPENDED as the finest
/// discriminator so fan-out agents that share one coarse session-id (e.g. an
/// 80-agent workflow all tagged with the parent session) get distinct keys and
/// distribute, while a stable-prefix conversation keeps a stable fp and stays
/// sticky. When `fp` is absent the key is byte-identical to the legacy
/// header-only form (no mass rehash of existing traffic).
fn affinity_routing_key(
    client_ip: &IpAddr,
    client_id: &str,
    agent_id: &str,
    session_id: &str,
    fp: Option<&str>,
) -> Option<String> {
    let has_identity = client_id != "-" || agent_id != "-" || session_id != "-" || fp.is_some();
    if !has_identity {
        return None;
    }
    let base = format!("{}:{}:{}:{}", client_ip, client_id, agent_id, session_id);
    Some(match fp {
        Some(f) => format!("{base}:{f}"),
        None => base,
    })
}

impl RequestContext {
    fn from_request(state: &AppState, client_ip: &IpAddr, headers: &axum::http::HeaderMap) -> Self {
        Self {
            client_id: state.resolve_client_id(client_ip, headers),
            client_ver: headers
                .get("user-agent")
                .and_then(|v| v.to_str().ok())
                .and_then(extract_client_version)
                .unwrap_or("-")
                .to_string(),
            agent_id: headers
                .get("x-agent-id")
                .and_then(|v| v.to_str().ok())
                .map(|s| s.trim())
                .filter(|s| !s.is_empty())
                .unwrap_or("-")
                .to_string(),
            session_id: headers
                .get("x-claude-code-session-id")
                .and_then(|v| v.to_str().ok())
                .map(|s| s.trim())
                .filter(|s| !s.is_empty())
                .or_else(|| {
                    headers
                        .get("x-session-id")
                        .and_then(|v| v.to_str().ok())
                        .map(|s| s.trim())
                        .filter(|s| !s.is_empty())
                })
                .unwrap_or("-")
                .to_string(),
        }
    }

    /// Build the routing affinity key. `fp` is the content fingerprint
    /// (system+first-user digest); when present it is APPENDED as the finest
    /// discriminator so fan-out agents that share one coarse session-id (e.g. an
    /// 80-agent workflow all tagged with the parent session) get distinct keys
    /// and distribute, while a stable-prefix conversation keeps a stable fp and
    /// stays sticky. When `fp` is absent the key is unchanged from header-only
    /// behaviour (no mass rehash).
    fn affinity_key(&self, client_ip: &IpAddr, fp: Option<&str>) -> Option<String> {
        affinity_routing_key(
            client_ip,
            &self.client_id,
            &self.agent_id,
            &self.session_id,
            fp,
        )
    }
}

fn log_usage(req_id: &str, client_id: &str, model: &str, account: &str, usage: &TokenUsage) {
    // INFO (temporarily): surfaces per-request cache_read vs cache_write so we can
    // join it to the `fingerprint` line by req_id and learn whether fan-out agents
    // share a cacheable preamble (reads) or are independent (creates) — and whether
    // a real multi-turn session keeps getting reads after fp-keying (the fp
    // stability check). Revert to debug! once that question is answered. Token
    // accounting is independent of log level (Prometheus counters + optional
    // shadow-log); this line is only the human-readable echo.
    info!(
        req_id,
        client_id,
        model,
        account,
        input = usage.input_tokens,
        output = usage.output_tokens,
        cached = usage.cache_read_input_tokens,
        cache_write = usage.cache_creation_input_tokens,
        "usage"
    );
}

/// Finalize a streaming response: extract usage, log, and shadow log.
/// Shared by proxy_handler and openai_chat_handler.
#[allow(clippy::too_many_arguments)]
async fn finalize_stream(
    state: &AppState,
    ep: &Endpoint,
    req_id: &str,
    client_id: &str,
    model: &str,
    acct_name: &str,
    client_ip: &str,
    agent: &str,
    session: &str,
    status_code: u16,
    sse_buf: &[u8],
    request_start: std::time::Instant,
    client_disconnected: bool,
    upstream_error: bool,
    openai_compat: bool,
) {
    let text = String::from_utf8_lossy(sse_buf);
    let usage = TokenUsage::from_sse_text(&text);
    let elapsed_ms = request_start.elapsed().as_millis() as u64;
    if !usage.is_empty() {
        state.record_usage(ep, client_id, &usage).await;
        log_usage(req_id, client_id, model, acct_name, &usage);
    } else {
        let reason = if upstream_error {
            "upstream_error"
        } else if client_disconnected {
            "client_disconnect"
        } else {
            "no_usage_event"
        };
        // Log structural metadata only — SSE payloads contain user content.
        let sse_text = String::from_utf8_lossy(sse_buf);
        let sse_event_types: Vec<&str> = sse_text
            .lines()
            .filter_map(|l| {
                l.strip_prefix("event: ")
                    .or_else(|| l.strip_prefix("event:"))
            })
            .collect();
        let total_events = sse_event_types.len();
        let truncated = total_events > 5;
        let preview: Vec<&str> = sse_event_types.into_iter().take(5).collect();
        warn!(
            req_id,
            client_id,
            model,
            account = acct_name,
            reason,
            elapsed_ms,
            sse_bytes = sse_buf.len(),
            sse_event_count = total_events,
            sse_events = ?preview,
            truncated,
            "stream_end_no_usage"
        );
    }
    let mut log = serde_json::json!({
        "ts": AppState::now_epoch(),
        "client": client_ip,
        "client_id": client_id,
        "agent": agent,
        "session": session,
        "model": model,
        "account": acct_name,
        "status": status_code,
        "stream": true,
        "latency_ms": elapsed_ms,
        "input_tokens": usage.input_tokens,
        "output_tokens": usage.output_tokens,
        "cache_creation_input_tokens": usage.cache_creation_input_tokens,
        "cache_read_input_tokens": usage.cache_read_input_tokens,
        "client_disconnected": client_disconnected,
    });
    if openai_compat {
        log["openai_compat"] = serde_json::json!(true);
    }
    state.shadow_log(log);
}

/// Finalize a non-streaming response: extract usage from JSON body, log, and shadow log.
#[allow(clippy::too_many_arguments)]
async fn finalize_non_stream(
    state: &AppState,
    ep: &Endpoint,
    req_id: &str,
    client_id: &str,
    model: &str,
    acct_name: &str,
    client_ip: &str,
    agent: &str,
    session: &str,
    status_code: u16,
    usage: &TokenUsage,
    latency_ms: u64,
    openai_compat: bool,
) {
    if !usage.is_empty() {
        state.record_usage(ep, client_id, usage).await;
        log_usage(req_id, client_id, model, acct_name, usage);
    }
    let mut log = serde_json::json!({
        "ts": AppState::now_epoch(),
        "client": client_ip,
        "client_id": client_id,
        "agent": agent,
        "session": session,
        "model": model,
        "account": acct_name,
        "status": status_code,
        "stream": false,
        "latency_ms": latency_ms,
        "input_tokens": usage.input_tokens,
        "output_tokens": usage.output_tokens,
        "cache_creation_input_tokens": usage.cache_creation_input_tokens,
        "cache_read_input_tokens": usage.cache_read_input_tokens,
    });
    if openai_compat {
        log["openai_compat"] = serde_json::json!(true);
    }
    state.shadow_log(log);
}

impl AppState {
    /// Record token usage for an endpoint and client.
    async fn record_usage(&self, ep: &Endpoint, client_id: &str, usage: &TokenUsage) {
        if usage.is_empty() {
            return;
        }
        ep.input_tokens
            .fetch_add(usage.input_tokens, Ordering::Relaxed);
        ep.output_tokens
            .fetch_add(usage.output_tokens, Ordering::Relaxed);
        ep.cache_creation_tokens
            .fetch_add(usage.cache_creation_input_tokens, Ordering::Relaxed);
        ep.cache_read_tokens
            .fetch_add(usage.cache_read_input_tokens, Ordering::Relaxed);

        // Per-client tracking
        if client_id != "-" {
            let total = usage.input_tokens
                + usage.output_tokens
                + usage.cache_creation_input_tokens
                + usage.cache_read_input_tokens;
            if let Ok(mut map) = self.client_usage.lock() {
                // Bound new-key growth (user-controlled x-client-id); already-tracked
                // clients keep accumulating past the cap.
                if map.len() < MAX_TRACKED_CLIENTS || map.contains_key(client_id) {
                    let entry = map.entry(client_id.to_owned()).or_insert([0; 4]);
                    entry[0] += usage.input_tokens;
                    entry[1] += usage.output_tokens;
                    entry[2] += usage.cache_creation_input_tokens;
                    entry[3] += usage.cache_read_input_tokens;
                }
            }
            // Budget accounting
            self.record_budget_usage(client_id, total).await;
        }
    }

    /// Update burn rate for an account and per-client request tracking.
    fn update_burn_rate(&self, burn_rate: &Mutex<BurnRate>, client_id: &str) {
        let now = Instant::now();
        if let Ok(mut br) = burn_rate.lock() {
            br.update(now);
        }
        if let Ok(mut rates) = self.client_request_rates.lock() {
            // Bound new-key growth (client_id is the user-controlled x-client-id
            // header); already-tracked clients keep updating past the cap.
            if rates.len() < MAX_TRACKED_CLIENTS || rates.contains_key(client_id) {
                let entry = rates
                    .entry(client_id.to_owned())
                    .or_insert_with(|| (0, Ewma::new(TAU_1H)));
                entry.0 += 1;
                entry.1.update(now);
            }
        }
    }

    /// Write a shadow log entry (fire-and-forget).
    fn shadow_log(&self, entry: serde_json::Value) {
        if let Some(ref tx) = self.shadow_log_tx {
            if let Ok(line) = serde_json::to_string(&entry) {
                if tx.try_send(line).is_err() {
                    let dropped = self.shadow_log_dropped.fetch_add(1, Ordering::Relaxed) + 1;
                    // Rate-limit warning: log at powers of 2 to avoid log spam
                    if dropped.is_power_of_two() {
                        warn!(
                            total_dropped = dropped,
                            "shadow log channel full, entries being dropped"
                        );
                    }
                }
            }
        }
    }

    /// Check if a client is within their daily token budget. Returns Ok(()) or Err with remaining.
    /// When Redis is available, checks the global counter; falls back to local on error.
    async fn check_budget(&self, client_id: &str) -> Result<(), u64> {
        let limit = match self.client_budgets.get(client_id) {
            Some(&limit) => limit,
            None => return Ok(()), // no budget configured = unlimited
        };
        let today = Self::now_epoch() / 86400;

        // Try Redis first for cross-replica budget enforcement
        if let Some(redis) = &self.redis {
            use redis::AsyncCommands;
            let key = format!("alb:budget:{client_id}:{today}");
            let mut conn = redis.clone();
            match conn.get::<_, Option<u64>>(&key).await {
                Ok(Some(used)) if used >= limit => return Err(0),
                Ok(_) => return Ok(()),
                Err(e) => {
                    warn!(error = %e, "redis budget check failed, falling back to local");
                }
            }
        }

        // Local fallback
        if let Ok(map) = self.budget_usage.lock() {
            if let Some(&(day, used)) = map.get(client_id) {
                if day == today && used >= limit {
                    return Err(limit - (used.min(limit)));
                }
            }
        }
        Ok(())
    }

    /// Record tokens against a client's daily budget.
    /// Updates local state synchronously; awaits Redis INCRBY inline to prevent TOCTOU races.
    /// On Redis INCRBY failure, deletes the stale key so check_budget falls through to local.
    async fn record_budget_usage(&self, client_id: &str, tokens: u64) {
        if tokens == 0 || !self.client_budgets.contains_key(client_id) {
            return;
        }
        let today = Self::now_epoch() / 86400;

        // Always update local state (for stats + fallback)
        if let Ok(mut map) = self.budget_usage.lock() {
            let entry = map.entry(client_id.to_owned()).or_insert((today, 0));
            if entry.0 != today {
                *entry = (today, 0); // reset on new day
            }
            entry.1 += tokens;
        }

        // Await Redis INCRBY (not fire-and-forget) so check_budget always sees latest counter
        if let Some(redis) = &self.redis {
            use redis::AsyncCommands;
            let mut conn = redis.clone();
            let key = format!("alb:budget:{client_id}:{today}");
            let result: redis::RedisResult<u64> = conn.incr(&key, tokens).await;
            match result {
                Ok(_) => {
                    let expire_result: redis::RedisResult<bool> = conn.expire(&key, 172800).await;
                    if let Err(e) = expire_result {
                        tracing::warn!(error = %e, "redis EXPIRE failed for budget key");
                    }
                }
                Err(e) => {
                    // Delete stale key so check_budget falls through to local state
                    tracing::warn!(error = %e, "redis INCRBY failed, deleting stale budget key");
                    let _: redis::RedisResult<()> = conn.del(&key).await;
                }
            }
        }
    }

    /// Check if client_id is an operator.
    fn is_operator(&self, client_id: &str) -> bool {
        self.operators.iter().any(|op| op == client_id)
    }

    /// Check if all model-compatible endpoints exceed this client's utilization limit.
    /// Returns Ok(()) if no limit configured or at least one endpoint is below the limit.
    /// Returns Err(retry_after_secs) if all endpoints exceed the limit.
    /// OpenAI endpoints carry no rate-limit data and are skipped — they neither
    /// gate nor relieve the limit (mirrors `is_emergency_brake_active`).
    async fn check_utilization_limit(&self, client_id: &str, model: &str) -> Result<(), u64> {
        let limit = match self.client_utilization_limits.get(client_id) {
            Some(&limit) => limit,
            None => return Ok(()), // no limit configured
        };

        let now_epoch = Self::now_epoch();
        let mut nearest_reset: Option<u64> = None;
        let mut all_above = true;
        let mut any_compatible = false;
        let mut any_known = false;

        for ep in &self.endpoints {
            if ep.protocol != Protocol::Anthropic {
                continue;
            }
            if !ep.serves_model(model) {
                continue;
            }
            any_compatible = true;
            let info = ep.rate_info.read().await;
            let (util, source, _, _) = effective_utilization(&info, now_epoch, model);
            if source == "unknown" {
                all_above = false; // fail-open: unknown endpoint may have capacity
                break;
            }
            any_known = true;
            if util < limit {
                all_above = false;
                break;
            }
            // Track nearest reset from the binding constraint only
            let reset_epoch = match source {
                "5h" => info.reset_5h,
                "7d" => resolve_7d_claim(&info, model)
                    .and_then(|c| c.reset)
                    .or(info.reset_7d),
                _ => info.reset_5h.or(info.reset_7d), // unified best-effort
            };
            if let Some(r) = reset_epoch {
                if r > now_epoch {
                    let secs = r - now_epoch;
                    nearest_reset = Some(nearest_reset.map_or(secs, |cur: u64| cur.min(secs)));
                }
            }
        }

        if all_above && any_compatible && any_known {
            let retry_after = nearest_reset.unwrap_or(300).clamp(60, 3600);
            Err(retry_after)
        } else {
            Ok(())
        }
    }

    /// Check if all endpoints are above the emergency threshold.
    /// Fail-open: returns false if all endpoints return (0.5, "unknown") — no data.
    async fn is_emergency_brake_active(&self) -> bool {
        if !self.emergency_brake {
            return false;
        }
        let now_epoch = Self::now_epoch();
        let mut all_above = true;
        let mut any_known = false;

        // ONLY Protocol::Anthropic endpoints. OpenAI endpoints carry a stub
        // RateLimitInfo (all None) which effective_utilization() resolves to
        // (0.5, "unknown"); including them would force all_above = false and the
        // brake could never fire. One of the three named `match protocol` sites.
        for ep in &self.endpoints {
            if ep.protocol != Protocol::Anthropic {
                continue;
            }
            let info = ep.rate_info.read().await;
            let (util, source, _, _) = effective_utilization(&info, now_epoch, "");
            if source != "unknown" {
                any_known = true;
            }
            if util < self.emergency_threshold {
                all_above = false;
                break;
            }
        }

        // Fail-open: if no endpoint has real data, don't activate
        all_above && any_known
    }

    /// Shared pre-request gate for all Anthropic-proxied requests.
    /// Returns Ok(()) or an error Response (429).
    async fn pre_request_gate(&self, client_id: &str, model: &str) -> Result<(), Response> {
        if self.is_operator(client_id) {
            return Ok(()); // operator bypasses everything
        }

        // 1. Daily token budget (existing)
        if client_id != "-" && self.check_budget(client_id).await.is_err() {
            warn!(client_id = %client_id, "rejected: daily token budget exceeded");
            return Err(
                (StatusCode::TOO_MANY_REQUESTS, "daily token budget exceeded").into_response(),
            );
        }

        // 2. Utilization limit (new)
        if let Err(retry_after) = self.check_utilization_limit(client_id, model).await {
            warn!(
                client_id = %client_id,
                retry_after = retry_after,
                "rejected: utilization limit exceeded"
            );
            let mut resp = (
                StatusCode::TOO_MANY_REQUESTS,
                format!("utilization limit exceeded for client '{client_id}'"),
            )
                .into_response();
            resp.headers_mut().insert(
                "retry-after",
                HeaderValue::from_str(&retry_after.to_string()).unwrap(),
            );
            return Err(resp);
        }

        // 3. Emergency brake (new)
        if self.is_emergency_brake_active().await {
            warn!(
                client_id = %client_id,
                "rejected: emergency brake active"
            );
            return Err((
                StatusCode::TOO_MANY_REQUESTS,
                "emergency: all accounts near exhaustion",
            )
                .into_response());
        }

        Ok(())
    }
}

// ── OAuth system prompt injection ──────────────────────────────────

/// Check whether the request body already contains the OAuth system prompt
/// as a prefix of the first system block (string or array form).
fn has_oauth_system_prompt(body: &serde_json::Value) -> bool {
    match body.get("system") {
        Some(system) if system.is_string() => system
            .as_str()
            .is_some_and(|s| s.starts_with(OAUTH_SYSTEM_PROMPT)),
        Some(system) if system.is_array() => system
            .as_array()
            .map(|arr| {
                arr.iter().any(|b| {
                    b.get("text")
                        .and_then(|t| t.as_str())
                        .is_some_and(|t| t.starts_with(OAUTH_SYSTEM_PROMPT))
                })
            })
            .unwrap_or(false),
        _ => false,
    }
}

/// Inject the Claude Code system prompt as the first system block.
///
/// OAuth tokens (sk-ant-oat*) require this exact prompt as the first system
/// block to access sonnet/opus models. Haiku works without it, but we inject
/// unconditionally for OAuth accounts to keep things simple.
///
/// The prompt is prepended to any existing system content:
/// - No system field → creates `"system": [{"type":"text","text":"..."}]`
/// - String system → converts to array with CC prompt first, original second
/// - Array system → prepends CC prompt block if not already present
fn inject_oauth_system_prompt(body: &mut serde_json::Value) {
    if has_oauth_system_prompt(body) {
        return;
    }

    let cc_block = serde_json::json!({"type": "text", "text": OAUTH_SYSTEM_PROMPT});

    match body.get("system") {
        None | Some(&serde_json::Value::Null) => {
            body["system"] = serde_json::json!([cc_block]);
        }
        Some(system) => {
            if let Some(text) = system.as_str() {
                // Convert string to array: CC prompt first, original second
                body["system"] = serde_json::json!([
                    cc_block,
                    {"type": "text", "text": text}
                ]);
            } else if let Some(arr) = system.as_array() {
                // Prepend CC prompt block
                let mut new_arr = vec![cc_block];
                new_arr.extend(arr.iter().cloned());
                body["system"] = serde_json::Value::Array(new_arr);
            }
        }
    }
}

// ── Auto-cache injection ────────────────────────────────────────────

struct CacheInjection {
    tools: bool,
    system: bool,
    messages: bool,
    skipped: bool,
}

/// Inject prompt cache breakpoints into an Anthropic API request body.
///
/// Strategy: up to 3 breakpoints — last tool, last system block, last user message.
/// No-op if any `cache_control` is already present anywhere in the body.
fn inject_cache_breakpoints(body: &mut serde_json::Value) -> CacheInjection {
    let cache_marker = serde_json::json!({"type": "ephemeral"});
    let mut result = CacheInjection {
        tools: false,
        system: false,
        messages: false,
        skipped: false,
    };

    // Bail if any cache_control already present
    if has_existing_cache_control(body) {
        result.skipped = true;
        return result;
    }

    // 1. Tools — add cache_control to last tool
    if let Some(tools) = body.get_mut("tools").and_then(|t| t.as_array_mut()) {
        if let Some(last) = tools.last_mut() {
            if let Some(obj) = last.as_object_mut() {
                obj.insert("cache_control".to_string(), cache_marker.clone());
                result.tools = true;
            }
        }
    }

    // 2. System — string → array conversion, or annotate last block
    if let Some(system) = body.get_mut("system") {
        if let Some(text) = system.as_str().map(String::from) {
            *system = serde_json::json!([{
                "type": "text",
                "text": text,
                "cache_control": cache_marker,
            }]);
            result.system = true;
        } else if let Some(arr) = system.as_array_mut() {
            if let Some(last) = arr.last_mut() {
                if let Some(obj) = last.as_object_mut() {
                    obj.insert("cache_control".to_string(), cache_marker.clone());
                    result.system = true;
                }
            }
        }
    }

    // 3. Messages — find last user message, annotate its content
    if let Some(messages) = body.get_mut("messages").and_then(|m| m.as_array_mut()) {
        if let Some(last_user) = messages
            .iter_mut()
            .rev()
            .find(|m| m.get("role").and_then(|r| r.as_str()) == Some("user"))
        {
            if let Some(content) = last_user.get_mut("content") {
                if let Some(text) = content.as_str().map(String::from) {
                    *content = serde_json::json!([{
                        "type": "text",
                        "text": text,
                        "cache_control": cache_marker,
                    }]);
                    result.messages = true;
                } else if let Some(arr) = content.as_array_mut() {
                    if let Some(last) = arr.last_mut() {
                        if let Some(obj) = last.as_object_mut() {
                            obj.insert("cache_control".to_string(), cache_marker.clone());
                            result.messages = true;
                        }
                    }
                }
            }
        }
    }

    result
}

/// Check if any cache_control key exists in tools, system, or messages.
fn has_existing_cache_control(body: &serde_json::Value) -> bool {
    // Check tools
    if let Some(tools) = body.get("tools").and_then(|t| t.as_array()) {
        for tool in tools {
            if tool.get("cache_control").is_some() {
                return true;
            }
        }
    }
    // Check system
    if let Some(system) = body.get("system") {
        if system.get("cache_control").is_some() {
            return true;
        }
        if let Some(arr) = system.as_array() {
            for block in arr {
                if block.get("cache_control").is_some() {
                    return true;
                }
            }
        }
    }
    // Check messages
    if let Some(messages) = body.get("messages").and_then(|m| m.as_array()) {
        for msg in messages {
            if msg.get("cache_control").is_some() {
                return true;
            }
            if let Some(content) = msg.get("content") {
                if content.get("cache_control").is_some() {
                    return true;
                }
                if let Some(arr) = content.as_array() {
                    for block in arr {
                        if block.get("cache_control").is_some() {
                            return true;
                        }
                    }
                }
            }
        }
    }
    false
}

/// Whether a header name looks sensitive (substring match).
fn is_sensitive_header(name: &str) -> bool {
    SENSITIVE_HEADER_SUBSTRINGS
        .iter()
        .any(|sub| name.contains(sub))
}

/// Return the header value for debug logging, redacting sensitive headers.
fn debug_header_value<'a>(name: &axum::http::HeaderName, value: &'a HeaderValue) -> &'a str {
    if is_sensitive_header(name.as_str()) {
        "<redacted>"
    } else {
        value.to_str().unwrap_or("<binary>")
    }
}

/// Debug: dump all cache_control objects found in the request body.
fn debug_dump_cache_control(body: &serde_json::Value, req_id: &str) {
    let mut count = 0u32;

    if let Some(tools) = body.get("tools").and_then(|t| t.as_array()) {
        for (i, tool) in tools.iter().enumerate() {
            if let Some(cc) = tool.get("cache_control") {
                debug!(req_id, location = format_args!("tools[{i}]"), cache_control = %cc, "body cache_control");
                count += 1;
            }
        }
    }
    if let Some(system) = body.get("system") {
        if let Some(cc) = system.get("cache_control") {
            debug!(req_id, location = "system", cache_control = %cc, "body cache_control");
            count += 1;
        }
        if let Some(arr) = system.as_array() {
            for (i, block) in arr.iter().enumerate() {
                if let Some(cc) = block.get("cache_control") {
                    debug!(req_id, location = format_args!("system[{i}]"), cache_control = %cc, "body cache_control");
                    count += 1;
                }
            }
        }
    }
    if let Some(messages) = body.get("messages").and_then(|m| m.as_array()) {
        for (i, msg) in messages.iter().enumerate() {
            let role = msg.get("role").and_then(|r| r.as_str()).unwrap_or("-");
            if let Some(cc) = msg.get("cache_control") {
                debug!(req_id, location = format_args!("messages[{i}]"), role, cache_control = %cc, "body cache_control");
                count += 1;
            }
            if let Some(content) = msg.get("content") {
                if let Some(arr) = content.as_array() {
                    for (j, block) in arr.iter().enumerate() {
                        if let Some(cc) = block.get("cache_control") {
                            debug!(
                                req_id,
                                location = format_args!("messages[{i}].content[{j}]"),
                                role,
                                cache_control = %cc,
                                "body cache_control"
                            );
                            count += 1;
                        }
                    }
                }
            }
        }
    }
    let msg_count = body
        .get("messages")
        .and_then(|m| m.as_array())
        .map(|a| a.len())
        .unwrap_or(0);
    let tool_count = body
        .get("tools")
        .and_then(|t| t.as_array())
        .map(|a| a.len())
        .unwrap_or(0);
    debug!(
        req_id,
        cache_control_count = count,
        messages = msg_count,
        tools = tool_count,
        "body summary"
    );
}

// ── Handler ─────────────────────────────────────────────────────────

/// Outcome of a single forward attempt to an Anthropic-protocol endpoint.
/// The retry loop in `proxy_handler` interprets the outcome:
///   - `Done(resp)`: final response — return it to the caller.
///   - `Retry { saw_529, push_skip, transient }`: try the next candidate. If
///     `push_skip` is true the caller appends the failed endpoint index to
///     its `skip` list; if `saw_529` is true the loop will BEBO-retry once
///     this round exhausts. `transient` marks a transport-level send failure
///     (ETIMEDOUT/reset/closed/DNS) — the loop treats these with round-gated
///     rotation (retry the affinity/cache-warm endpoint in place on round 0,
///     rotate only on later rounds) and a transient-aware exhaustion status.
enum ForwardOutcome {
    Done(Response),
    Retry {
        saw_529: bool,
        push_skip: bool,
        transient: bool,
    },
}

/// Surface the real cause of a `reqwest::Error`. The Display form only shows
/// "error sending request for url (...)" — the actionable detail (stale pooled
/// connection closed, connection reset, DNS failure, connect refused, timeout)
/// lives in the error's `source()` chain and its `is_*` classifiers. Returns a
/// compact `kind=... cause=a -> b -> c` string for structured logs so we can
/// tell *why* upstream sends fail without guessing.
fn describe_reqwest_error(e: &reqwest::Error) -> String {
    let mut kinds: Vec<&str> = Vec::new();
    if e.is_connect() {
        kinds.push("connect");
    }
    if e.is_timeout() {
        kinds.push("timeout");
    }
    if e.is_body() {
        kinds.push("body");
    }
    if e.is_decode() {
        kinds.push("decode");
    }
    if e.is_request() {
        kinds.push("request");
    }
    if e.is_redirect() {
        kinds.push("redirect");
    }
    let kind = if kinds.is_empty() {
        "other".to_string()
    } else {
        kinds.join("+")
    };

    // Walk the source chain (reqwest -> hyper -> io) for the root cause.
    let mut causes: Vec<String> = Vec::new();
    let mut src = std::error::Error::source(e);
    while let Some(s) = src {
        causes.push(s.to_string());
        src = s.source();
    }
    if causes.is_empty() {
        format!("kind={kind}")
    } else {
        format!("kind={kind} cause={}", causes.join(" -> "))
    }
}

/// Classify an upstream Anthropic response status into a retry decision.
///
/// Shared by both `forward_anthropic` and `forward_openai_compat_anthropic`,
/// whose retry-classification logic is byte-identical. For 429 / 529 / other
/// 5xx it records hard-limit state, persists, and logs exactly as the prior
/// inline blocks did, returning `Err(ForwardOutcome::Retry { .. })`. For any
/// non-retry status (2xx success or 4xx client error) it returns
/// `Ok(resp)` — handing the response back so the caller can continue.
async fn classify_retry_status(
    state: &Arc<AppState>,
    status: StatusCode,
    rate_info: &RwLock<RateLimitInfo>,
    endpoint_name: &str,
    resp: reqwest::Response,
) -> Result<reqwest::Response, ForwardOutcome> {
    // 429 → mark hard-limited and try next account
    if status == StatusCode::TOO_MANY_REQUESTS {
        state
            .mark_hard_limited_for(rate_info, endpoint_name, resp.headers())
            .await;
        log_429_details(endpoint_name, resp).await;
        state.save_state().await;
        info!(account = endpoint_name, "got 429, rotating to next account");
        return Err(ForwardOutcome::Retry {
            saw_529: false,
            push_skip: true,
            transient: false,
        });
    }

    // 529 → overloaded, try next account; flag for BEBO retry if all exhausted
    if status.as_u16() == 529 {
        warn!(account = endpoint_name, "got 529, rotating to next account");
        return Err(ForwardOutcome::Retry {
            saw_529: true,
            push_skip: true,
            transient: false,
        });
    }

    // Other 5xx → transient, try next account (no BEBO retry)
    if status.is_server_error() {
        warn!(
            account = endpoint_name,
            status = status.as_u16(),
            "got server error, rotating to next account"
        );
        return Err(ForwardOutcome::Retry {
            saw_529: false,
            push_skip: true,
            transient: false,
        });
    }

    Ok(resp)
}

/// What the retry loop should do after one forward attempt's `ForwardOutcome`.
enum RetryStep {
    /// `Done` — hand this response back to the caller (return from the handler).
    Return(Response),
    /// Try the next candidate this round (`continue` the attempt loop).
    NextAttempt,
    /// End this round now (`break` the attempt loop) → backoff, then retry the
    /// pool. Used for a round-0 transient so the affinity/cache-warm endpoint is
    /// retried in place rather than rotated away from.
    EndRound,
}

/// Apply one forward attempt's outcome to the round bookkeeping and decide the
/// loop's next move. Shared by `proxy_handler` and `openai_chat_handler` so the
/// round-gated transient policy lives in exactly ONE place — the two retry loops
/// are otherwise byte-identical and have diverged before.
///
/// `retry_round` gates rotation: a transient (transport-level) failure on round
/// 0 keeps the affinity/cache-warm endpoint (`EndRound` → backoff → retry IT);
/// on rounds ≥1 it rotates (push skip). 429/5xx/529 always rotate immediately.
fn apply_round_outcome(
    retry_round: u32,
    outcome: ForwardOutcome,
    picked_idx: EndpointIdx,
    skip: &mut Vec<EndpointIdx>,
    saw_529: &mut bool,
    saw_transient: &mut bool,
) -> RetryStep {
    match outcome {
        ForwardOutcome::Done(resp) => RetryStep::Return(resp),
        ForwardOutcome::Retry {
            saw_529: s,
            push_skip,
            transient,
        } => {
            if s {
                *saw_529 = true;
            }
            if transient {
                *saw_transient = true;
                // Round 0: do NOT rotate — keep the affinity/cache-warm endpoint
                // and end the round so the backoff retries IT (a sub-second blip
                // becomes a cache HIT, not a cold-cache write on a cold endpoint).
                if retry_round == 0 {
                    return RetryStep::EndRound;
                }
                // Round ≥1: the warm endpoint failed a backoff-retry too — it is
                // genuinely down, so rotate across the pool.
                skip.push(picked_idx);
            } else if push_skip {
                skip.push(picked_idx); // 429/5xx/529: immediate rotation (unchanged)
            }
            RetryStep::NextAttempt
        }
    }
}

/// After a round completes, decide whether to retry the whole pool (after a
/// backoff). Pure rate-limit exhaustion (no 529, no transient) does not retry —
/// rotating cannot help. A transient-only round gets the smaller
/// `MAX_TRANSIENT_RETRIES` budget; a 529 round keeps the loop's full
/// `MAX_529_RETRIES` budget.
fn round_should_continue(retry_round: u32, saw_529: bool, saw_transient: bool) -> bool {
    if !(saw_529 || saw_transient) {
        return false; // (a) pure rate-limit exhaustion — rotating won't help
    }
    if !saw_529 && saw_transient && retry_round >= MAX_TRANSIENT_RETRIES {
        return false; // (b) transient budget spent → fail clean
    }
    true // (c) 529 keeps its full loop budget
}

/// Backoff before re-trying the whole pool. A 529 (overload) round uses the long
/// base; a purely-transient round uses the short base. Doubles per round.
fn round_backoff_delay(retry_round: u32, last_saw_529: bool) -> Duration {
    let base = if last_saw_529 {
        RETRY_529_BASE_DELAY
    } else {
        TRANSIENT_BASE_DELAY
    };
    base * 2u32.pow(retry_round - 1)
}

/// Final response when all retry rounds are exhausted. A transient-only
/// exhaustion (transport failures, no 529) is a retryable `503 + Retry-After`:
/// the client reads `Retry-After` to time its backoff, and `503` honestly
/// signals a transient upstream rather than account rate-limiting. Both Claude
/// Code and the Anthropic SDKs retry 429 and 503 alike — `Retry-After` is the
/// load-bearing signal here, not the status class. Rate-limit exhaustion (or any
/// round that also saw a 529) stays `429` with NO `Retry-After`: recovery there
/// is on the order of minutes/hours, so a short retry hint would tight-loop the
/// client into a still-exhausted pool.
fn exhaustion_response(last_saw_transient: bool, last_saw_529: bool) -> Response {
    if last_saw_transient && !last_saw_529 {
        warn!("all endpoints transient-failed after backoff; returning retryable 503");
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            [("retry-after", "1")],
            "upstream temporarily unreachable",
        )
            .into_response();
    }
    warn!("all endpoints exhausted (rate-limited)");
    (StatusCode::TOO_MANY_REQUESTS, "exhausted all endpoints").into_response()
}

/// Forward one Anthropic-protocol request to a single `Endpoint`. The caller
/// passes the picked endpoint and its pool index (used for `skip` and usage
/// accounting).
/// `ep` is the endpoint to forward to; `endpoint_idx` is its index in
/// `state.endpoints`. Both are required: the streaming path spawns a
/// detached 'static task that must re-borrow the endpoint from a cloned
/// Arc<AppState> — a borrowed &Endpoint cannot cross the spawn boundary,
/// so the task captures the Copy `endpoint_idx` and re-indexes.
/// Shared knob chain for both upstream clients — `client` layers the SSE-tuned
/// `read_timeout` on top; `client_nonstreaming` takes it as-is (LAB-718).
fn upstream_client_builder() -> reqwest::ClientBuilder {
    Client::builder()
        .timeout(Duration::from_secs(900))
        // 4s (was 10): a blackholed connect fails fast so the transient
        // backoff-retry recovers in seconds. pool_idle_timeout stays 300s —
        // it keeps conns warm across Claude Code think-pauses.
        .connect_timeout(Duration::from_secs(4))
        .tcp_keepalive(Duration::from_secs(30))
        .http2_keep_alive_interval(Duration::from_secs(20))
        .http2_keep_alive_timeout(Duration::from_secs(10))
        .http2_keep_alive_while_idle(true)
        .pool_idle_timeout(Duration::from_secs(300))
}

/// True when the request body asks for SSE (`"stream": true`). Absent flag or
/// an unparseable body counts as non-streaming — Anthropic's default. Parsing
/// the body again here (it was already parsed for fingerprints/cache
/// injection) costs microseconds against a multi-second LLM call and keeps
/// the wide `forward_anthropic` signature unchanged.
fn request_wants_stream(body: &[u8]) -> bool {
    serde_json::from_slice::<serde_json::Value>(body)
        .ok()
        .and_then(|v| v.get("stream").and_then(|s| s.as_bool()))
        .unwrap_or(false)
}

#[allow(clippy::too_many_arguments)]
async fn forward_anthropic(
    state: &Arc<AppState>,
    parts: &axum::http::request::Parts,
    body_bytes: &bytes::Bytes,
    oauth_body_bytes: &bytes::Bytes,
    ep: &Endpoint,
    endpoint_idx: usize,
    req_id: &str,
    client_id: &str,
    client_ver: &str,
    client_ip: &std::net::IpAddr,
    agent_id: &str,
    session_id: &str,
    model: &str,
    request_start: std::time::Instant,
) -> ForwardOutcome {
    let token = ep.token.as_str();
    let passthrough = ep.passthrough;
    let endpoint_name = ep.name.as_str();
    let rate_info = &ep.rate_info;
    let url = format!(
        "{}{}",
        ep.base_url,
        parts
            .uri
            .path_and_query()
            .map(|pq| pq.as_str())
            .unwrap_or("/")
    );

    // Non-streaming requests get the client WITHOUT the SSE-tuned read_timeout:
    // their only response bytes arrive when generation completes, so a
    // read_timeout is a de-facto 180s cap on generation time (LAB-718).
    let http_client = if request_wants_stream(body_bytes) {
        &state.client
    } else {
        &state.client_nonstreaming
    };
    let mut upstream_req = http_client.request(parts.method.clone(), &url);

    // Forward headers
    let mut headers = parts.headers.clone();
    headers.remove("host");
    headers.remove("content-length"); // body size may change after cache injection
    headers.remove("accept-encoding"); // need plaintext SSE to extract token usage

    // Default anthropic-version if client didn't set it
    if !headers.contains_key("anthropic-version") {
        headers.insert("anthropic-version", HeaderValue::from_static("2023-06-01"));
    }

    // Auth: passthrough keeps caller's headers, otherwise inject account token
    inject_account_auth(&mut headers, token, passthrough);

    // Debug: log outbound auth method and key headers
    if tracing::enabled!(tracing::Level::DEBUG) {
        let auth_method = if passthrough {
            "passthrough"
        } else if token.starts_with("sk-ant-oat") {
            "oauth"
        } else {
            "api-key"
        };
        debug!(
            req_id,
            account = endpoint_name,
            auth_method,
            body_bytes = if token.starts_with("sk-ant-oat") {
                oauth_body_bytes.len()
            } else {
                body_bytes.len()
            },
            has_anthropic_beta = headers
                .get("anthropic-beta")
                .map(|v| v.to_str().unwrap_or("-")),
            has_anthropic_version = headers
                .get("anthropic-version")
                .map(|v| v.to_str().unwrap_or("-")),
            "<<< outbound to upstream"
        );
    }

    upstream_req = upstream_req.headers(headers);
    // Use OAuth variant (with CC system prompt) for OAuth tokens
    let req_body = if token.starts_with("sk-ant-oat") {
        oauth_body_bytes
    } else {
        body_bytes
    };
    upstream_req = upstream_req.body(req_body.clone());

    let resp = match upstream_req.send().await {
        Ok(r) => r,
        Err(e) => {
            error!(account = endpoint_name, detail = %describe_reqwest_error(&e), "upstream request failed: {e}");
            // Surface the failure on the dashboard by kind before it becomes a
            // client error. `is_timeout`/`is_connect` are the same classifiers
            // `describe_reqwest_error` uses for the log line above.
            let kind = if e.is_timeout() {
                "timeout"
            } else if e.is_connect() {
                "connect"
            } else {
                "other"
            };
            if let Ok(mut m) = state.upstream_transport_errors.lock() {
                *m.entry(kind).or_insert(0) += 1;
            }
            // Feed the per-endpoint circuit breaker: enough consecutive
            // failures and this endpoint leaves the routing pool entirely.
            state.record_transport_failure(endpoint_idx).await;
            // Transport-level send failure (ETIMEDOUT/reset/closed/DNS). Mark it
            // `transient`; rotation policy is round-gated and owned by the retry
            // loop (it knows `retry_round`), so `push_skip` stays false here —
            // round 0 retries the SAME affinity/cache-warm endpoint after a
            // backoff rather than rotating to a cold-cache endpoint on every blip.
            return ForwardOutcome::Retry {
                saw_529: false,
                push_skip: false,
                transient: true,
            };
        }
    };

    let status = resp.status();
    ep.requests.fetch_add(1, Ordering::Relaxed);
    // Any HTTP response (even 429/5xx) proves the transport path is alive —
    // clear the circuit-breaker counter.
    state.record_transport_success(endpoint_idx).await;

    // Debug: dump all response headers
    if tracing::enabled!(tracing::Level::DEBUG) {
        debug!(
            req_id,
            status = status.as_u16(),
            account = endpoint_name,
            "<<< upstream response"
        );
        for (k, v) in resp.headers().iter() {
            debug!(req_id, header = %k, value = debug_header_value(k, v), "<<< resp header");
        }
    }

    // Always update rate limit info and persist
    state
        .update_rate_info_for(rate_info, endpoint_name, resp.headers())
        .await;

    // Update burn rate (after rate-limit headers are parsed)
    state.update_burn_rate(&ep.burn_rate, client_id);

    // Classify 429 / 529 / other 5xx into a retry decision (shared helper).
    let mut resp = match classify_retry_status(state, status, rate_info, endpoint_name, resp).await
    {
        Ok(resp) => resp,
        Err(outcome) => return outcome,
    };

    // Clear hard limit and burst counter only on a genuine 2xx success.
    // A 4xx (e.g. invalid_request_error, auth failure) is not evidence
    // that the rate-limit window has drained — don't clobber state on
    // client errors.
    let recovered = if status.is_success() {
        let mut info = rate_info.write().await;
        let was = info.hard_limited_until.is_some();
        info.hard_limited_until = None;
        info.consecutive_burst_429s = 0;
        was
    } else {
        false
    };

    // Per-request persistence removed: it re-serialized the whole endpoint pool
    // and did a blocking write on every successful request (a memory + IO
    // amplifier under load). Persist only on the hard-limit RECOVERY transition
    // here; 429 hard-limit entry still persists immediately, and utilization /
    // request counts persist at probe cadence + shutdown.
    if recovered {
        state.save_state().await;
        state.signal_hard_limit_recovery(endpoint_name).await;
    }

    // Log with capacity info + inject budget status header
    let budget_status = {
        let info = rate_info.read().await;
        let (eff_util, constraint, _adj_5h, _adj_7d) =
            effective_utilization(&info, AppState::now_epoch(), model);
        info!(
            req_id,
            client = %client_ip,
            client_id = %client_id,
            ver = %client_ver,
            agent = %agent_id,
            session = %session_id,
            model = %model,
            account = endpoint_name,
            status = status.as_u16(),
            utilization = format_args!("{eff_util:.2}"),
            util_5h = info.utilization_5h.map(|v| format!("{v:.2}")).as_deref().unwrap_or("-"),
            util_7d = info.utilization_7d.map(|v| format!("{v:.2}")).as_deref().unwrap_or("-"),
            constraint,
            overage = info.overage_in_use,
            total = ep.requests.load(Ordering::Relaxed),
            "proxied"
        );
        compute_pressure_status(eff_util, client_id, state)
    };

    let latency_ms = request_start.elapsed().as_millis() as u64;

    // Stream response through, extracting token usage
    let resp_status = StatusCode::from_u16(status.as_u16()).unwrap_or(StatusCode::BAD_GATEWAY);
    let resp_headers = resp.headers().clone();

    let mut builder = Response::builder().status(resp_status);
    for (k, v) in resp_headers.iter() {
        if k == "transfer-encoding" {
            continue;
        }
        builder = builder.header(k, v);
    }

    // Inject budget status header
    builder = builder.header("x-budget-status", budget_status);

    // Detect streaming from content-type
    let is_streaming = resp_headers
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .map(|ct| ct.contains("text/event-stream"))
        .unwrap_or(false);

    if is_streaming {
        // Streaming: tee the byte stream to accumulate SSE text for usage extraction
        let (tx, rx) = tokio::sync::mpsc::channel::<Result<bytes::Bytes, std::io::Error>>(32);
        let state_clone = state.clone();
        // The detached task can't carry the `ep` borrow across the spawn
        // boundary; capture the Copy index and re-borrow from the owned `Arc`.
        let client_id_clone = client_id.to_owned();
        let acct_name = endpoint_name.to_owned();
        let model_clone = model.to_owned();
        let client_ip_str = client_ip.to_string();
        let agent_clone = agent_id.to_owned();
        let session_clone = session_id.to_owned();
        let req_id_clone = req_id.to_owned();
        let status_code = status.as_u16();

        tokio::spawn(async move {
            let mut sse_buf = Vec::new();
            let mut client_disconnected = false;
            let mut upstream_error = false;
            loop {
                match resp.chunk().await {
                    Ok(Some(chunk)) => {
                        sse_buf.extend_from_slice(&chunk);
                        if tx.send(Ok(chunk)).await.is_err() {
                            client_disconnected = true;
                            break;
                        }
                    }
                    Ok(None) => break,
                    Err(e) => {
                        upstream_error = true;
                        warn!(req_id = req_id_clone, error = %e, "upstream SSE read failed");
                        if tx
                            .send(Ok(anthropic_error_frame(&format!(
                                "upstream stream interrupted: {e}"
                            ))))
                            .await
                            .is_err()
                        {
                            client_disconnected = true;
                        }
                        break;
                    }
                }
            }
            // Parse accumulated SSE data for usage. The detached task only
            // holds a cloned Arc<AppState>; re-index it to recover &Endpoint.
            let ep = &state_clone.endpoints[endpoint_idx];
            finalize_stream(
                &state_clone,
                ep,
                &req_id_clone,
                &client_id_clone,
                &model_clone,
                &acct_name,
                &client_ip_str,
                &agent_clone,
                &session_clone,
                status_code,
                &sse_buf,
                request_start,
                client_disconnected,
                upstream_error,
                false,
            )
            .await;
        });

        let body_stream = ReceiverStream::new(rx);
        let response = builder
            .body(Body::from_stream(body_stream))
            .unwrap_or_else(|_| {
                (StatusCode::INTERNAL_SERVER_ERROR, "response build error").into_response()
            });
        ForwardOutcome::Done(response)
    } else {
        // Non-streaming: buffer, extract usage, forward.
        //
        // A socket error while reading the body must NOT be swallowed. The
        // previous `unwrap_or_default()` turned a mid-body connection reset into
        // an empty body forwarded under the upstream's 2xx status — the caller
        // (Claude Code) then saw a truncated "success" and reported it as a
        // socket error, while our logs showed a clean `proxied status=200`
        // (that line is emitted above, before the body is read). Log loudly and
        // return a real 502 so the failure is visible and the SDK gets a
        // well-formed error frame instead of a silent corruption. Mirrors the
        // error-detection structure of the sibling body-read sites
        // (`forward_openai_compat_anthropic`, `try_fallback_upstream`); the
        // exact error-frame format differs per path because each has a different
        // downstream contract.
        let resp_body_bytes = match resp.bytes().await {
            Ok(b) => b,
            Err(e) => {
                error!(
                    req_id,
                    account = endpoint_name,
                    status = status.as_u16(),
                    error = %e,
                    "upstream response body read failed mid-stream"
                );
                let body = serde_json::json!({
                    "type": "error",
                    "error": {
                        "type": "api_error",
                        "message": format!("upstream response body read failed: {e}"),
                    }
                })
                .to_string();
                // Forward the upstream's rate-limit headers + budget status so the
                // client's limit tracking stays consistent with the success arms.
                // Deliberately NOT content-length: the upstream's value describes
                // the truncated body it promised, not our short JSON frame.
                let mut err_builder = Response::builder()
                    .status(StatusCode::BAD_GATEWAY)
                    .header("content-type", "application/json")
                    .header("x-budget-status", budget_status);
                for (k, v) in resp_headers.iter() {
                    if k.as_str().starts_with("anthropic-ratelimit-") {
                        err_builder = err_builder.header(k, v);
                    }
                }
                return ForwardOutcome::Done(err_builder.body(Body::from(body)).unwrap_or_else(
                    |_| (StatusCode::BAD_GATEWAY, "upstream body read failed").into_response(),
                ));
            }
        };
        let mut usage = TokenUsage::default();
        if let Ok(parsed) = serde_json::from_slice::<serde_json::Value>(&resp_body_bytes) {
            usage = TokenUsage::from_response_body(&parsed);
        }
        finalize_non_stream(
            state,
            ep,
            req_id,
            client_id,
            model,
            endpoint_name,
            &client_ip.to_string(),
            agent_id,
            session_id,
            status.as_u16(),
            &usage,
            latency_ms,
            false,
        )
        .await;
        let response = builder
            .body(Body::from(resp_body_bytes))
            .unwrap_or_else(|_| {
                (StatusCode::INTERNAL_SERVER_ERROR, "response build error").into_response()
            });
        ForwardOutcome::Done(response)
    }
}

async fn proxy_handler(
    State(state): State<Arc<AppState>>,
    axum::extract::ConnectInfo(client_addr): axum::extract::ConnectInfo<SocketAddr>,
    req: Request<Body>,
) -> Response {
    let client_ip = client_addr.ip();
    let request_start = Instant::now();

    // IP allowlist check
    if !state.is_ip_allowed(&client_ip) {
        warn!(client = %client_ip, "rejected: IP not in allowlist");
        return (StatusCode::FORBIDDEN, "forbidden").into_response();
    }

    // Proxy auth: validate x-api-key against proxy_key if configured
    if let Some(ref key) = state.proxy_key {
        let provided = req.headers().get("x-api-key").and_then(|v| v.to_str().ok());
        if provided != Some(key.as_str()) {
            warn!(client = %client_ip, "rejected: invalid or missing proxy key");
            return (StatusCode::UNAUTHORIZED, "unauthorized").into_response();
        }
    }

    let (parts, body) = req.into_parts();

    let req_id = format!(
        "{:04x}:{}",
        state.instance_id,
        state.next_req_id.fetch_add(1, Ordering::Relaxed)
    );

    // Extract client identification headers
    let rctx = RequestContext::from_request(&state, &client_ip, &parts.headers);
    let RequestContext {
        client_id,
        client_ver,
        agent_id,
        session_id,
    } = rctx;
    // affinity_key is built AFTER the body is parsed, so the content fingerprint
    // (fp) can be folded in as the finest routing discriminator.

    // Debug: dump all inbound request headers
    if tracing::enabled!(tracing::Level::DEBUG) {
        debug!(req_id, client_id = %client_id, ver = %client_ver, ">>> inbound request");
        for (k, v) in parts.headers.iter() {
            debug!(req_id, header = %k, value = debug_header_value(k, v), ">>> req header");
        }
    }

    // Admission control (P1-01): reserve the request-body memory budget BEFORE
    // buffering, so a burst of concurrent large requests load-sheds (503) instead
    // of OOM-killing the pod. Held until the handler returns, by which point the
    // body has been forwarded upstream and freed.
    let _body_reservation = match reserve_request_body(&state, &parts, &req_id, client_ip) {
        Ok(g) => g,
        Err(resp) => return *resp,
    };

    let body_bytes = match read_body_bounded(&state, body, &req_id).await {
        Ok(b) => b,
        Err(resp) => return *resp,
    };

    // Parse body once for model extraction and optional cache injection
    let (body_bytes, oauth_body_bytes, model, fp) =
        if let Ok(mut parsed) = serde_json::from_slice::<serde_json::Value>(&body_bytes) {
            let model = parsed
                .get("model")
                .and_then(|m| m.as_str())
                .unwrap_or("")
                .to_string();
            let mut mutated = false;

            // Privacy-safe fingerprint instrumentation (digests only, no body
            // content), logged for EVERY request and joined to `usage`/`proxied`
            // by req_id. fp/fps = the flat affinity discriminator; bps = the
            // per-breakpoint cacheable-prefix hierarchy used to size the
            // avoidable-cache_write prize. fp/fps hash text only
            // (cache_control-invariant), so they are computed pre-injection and
            // double as the affinity discriminator; bps is computed POST-injection
            // (below) to reflect the breakpoints actually forwarded upstream.
            let (fp, fps) = content_fingerprints(&parsed);

            // Debug: dump cache_control structures found in request body
            if tracing::enabled!(tracing::Level::DEBUG) {
                debug_dump_cache_control(&parsed, &req_id);
            }

            if state.auto_cache {
                let inj = inject_cache_breakpoints(&mut parsed);
                if inj.skipped {
                    debug!("auto-cache: skipped, existing cache_control found");
                } else if inj.tools || inj.system || inj.messages {
                    mutated = true;
                    debug!(
                        tools = inj.tools,
                        system = inj.system,
                        messages = inj.messages,
                        "auto-cache: injected breakpoints"
                    );
                }
            }

            // Cache-prefix breakpoints (pos:digest at each cache_control), in
            // cache order, for prize-sizing. Computed AFTER auto-injection so it
            // reflects the breakpoints actually forwarded upstream (covers the
            // headerless fleet that relies on the proxy's injected breakpoints).
            // model is included because caches are per-model. Join to `usage`
            // (cache_read/write) and `proxied` (account) by req_id offline.
            let bps = prefix_breakpoint_hashes(&parsed)
                .into_iter()
                .map(|(pos, h)| format!("{pos}:{h}"))
                .collect::<Vec<_>>()
                .join("|");
            info!(
                req_id,
                client_id = %client_id,
                session = %session_id,
                agent = %agent_id,
                model = %model,
                fp = %fp,
                fps = %fps,
                bps = %bps,
                "fingerprint"
            );

            // Re-serialize. The `preserve_order` feature on serde_json is critical:
            // without it, serde uses BTreeMap which reorders JSON keys alphabetically,
            // producing different bytes from what the client sent. Anthropic's prompt
            // caching matches on raw byte prefixes, so reordering silently breaks
            // cache hits (0 reads, full writes every turn).
            let bytes = if mutated {
                serde_json::to_vec(&parsed).unwrap_or_else(|_| body_bytes.to_vec())
            } else {
                body_bytes.to_vec()
            };

            // Pre-compute OAuth variant with Claude Code system prompt prepended.
            // OAuth tokens (sk-ant-oat*) require this to access sonnet/opus models.
            // Skip injection when the client already includes the prompt — the
            // normal `bytes` payload (which preserves auto-cache mutations) is
            // already correct for OAuth accounts too.
            let oauth_bytes = if has_oauth_system_prompt(&parsed) {
                bytes.clone()
            } else {
                let mut oauth_parsed = parsed.clone();
                inject_oauth_system_prompt(&mut oauth_parsed);
                serde_json::to_vec(&oauth_parsed).unwrap_or_else(|_| bytes.clone())
            };

            (
                bytes::Bytes::from(bytes),
                bytes::Bytes::from(oauth_bytes),
                model,
                Some(fp),
            )
        } else {
            let clone = body_bytes.clone();
            (body_bytes, clone, String::new(), None)
        };

    // Build the affinity key now that fp is known. fp is the finest routing
    // discriminator: it splits fan-out agents that share one coarse session-id
    // (an 80-agent workflow tagged with the parent session) so they distribute,
    // while a stable-prefix conversation keeps a stable fp and stays sticky.
    let affinity_key = affinity_routing_key(
        &client_ip,
        &client_id,
        &agent_id,
        &session_id,
        fp.as_deref(),
    );
    let affinity = affinity_key.as_deref();

    // Pre-request gate: operator bypass, budget, utilization limit, emergency brake.
    // Note: budget + emergency don't need `model` and could run before body parsing,
    // but those rejections are rare and the JSON parse cost is negligible — not worth
    // splitting the gate for a few microseconds on an almost-never code path.
    if let Err(resp) = state.pre_request_gate(&client_id, &model).await {
        return resp;
    }

    let n = state.endpoints.len();
    let mut last_saw_529 = false;
    let mut last_saw_transient = false;
    for retry_round in 0..=MAX_529_RETRIES {
        if retry_round > 0 {
            let delay = round_backoff_delay(retry_round, last_saw_529);
            warn!(
                retry_round = retry_round,
                delay_ms = delay.as_millis() as u64,
                saw_529 = last_saw_529,
                "backoff: retrying all endpoints after transient/overload round"
            );
            tokio::time::sleep(delay).await;
        }
        let mut skip: Vec<EndpointIdx> = Vec::new();
        let mut saw_529 = false;
        let mut saw_transient = false;
        for _attempt in 0..n {
            // Pick the next endpoint and dispatch by protocol:
            // `forward_anthropic` (Anthropic) or `try_fallback_upstream`
            // (OpenAI). Both return a `ForwardOutcome` so the shared
            // round-gated policy in `apply_round_outcome` covers both.
            let (outcome, picked_idx): (ForwardOutcome, EndpointIdx) =
                match state.pick_endpoint(affinity, &model, &skip).await {
                    Some(i) => {
                        let ep = &state.endpoints[i];
                        match ep.protocol {
                            Protocol::Anthropic => {
                                let out = forward_anthropic(
                                    &state,
                                    &parts,
                                    &body_bytes,
                                    &oauth_body_bytes,
                                    ep,
                                    i,
                                    &req_id,
                                    &client_id,
                                    &client_ver,
                                    &client_ip,
                                    &agent_id,
                                    &session_id,
                                    &model,
                                    request_start,
                                )
                                .await;
                                (out, i)
                            }
                            Protocol::OpenAI => {
                                let out = try_fallback_upstream(
                                    &state,
                                    &body_bytes,
                                    &req_id,
                                    &client_id,
                                    &model,
                                    i,
                                    true,
                                )
                                .await;
                                (out, i)
                            }
                        }
                    }
                    // Candidates exhausted mid-round (all skipped / hard-limited /
                    // model-filtered). Break to the round-end logic rather than
                    // returning here, so a transient-only round still reaches the
                    // transient-aware exhaustion status instead of short-circuiting
                    // to a premature 429.
                    None => break,
                };

            match apply_round_outcome(
                retry_round,
                outcome,
                picked_idx,
                &mut skip,
                &mut saw_529,
                &mut saw_transient,
            ) {
                RetryStep::Return(resp) => return resp,
                RetryStep::NextAttempt => continue,
                RetryStep::EndRound => break,
            }
        }
        last_saw_529 = saw_529;
        last_saw_transient = saw_transient;
        if !round_should_continue(retry_round, saw_529, saw_transient) {
            break;
        }
    }

    exhaustion_response(last_saw_transient, last_saw_529)
}

// ── Fallback upstream handler ────────────────────────────────────────

/// Forward a request to a `Protocol::OpenAI` endpoint. For Anthropic-format
/// callers (`proxy_handler`) it translates the request to OpenAI format and the
/// response back (`translate = true`); for OpenAI-format callers
/// (`openai_chat_handler`) it forwards directly (`translate = false`).
///
/// Returns `ForwardOutcome` so the caller's retry loop applies the SAME
/// round-gated policy as the Anthropic path (`apply_round_outcome`): a
/// transport send failure is `transient` (round 0 retries this endpoint in
/// place, later rounds rotate, exhaustion is a retryable 503) and feeds the
/// per-endpoint circuit breaker; upstream 429/5xx rotate immediately. Other
/// 4xx (e.g. 400, 401) are `Done` — retry won't help on client-side errors.
///
/// Only increments the endpoint's request counter; it does not call
/// `record_usage` (OpenAI-compat responses don't carry the same usage signal we
/// extract for Anthropic).
async fn try_fallback_upstream(
    state: &AppState,
    body_bytes: &[u8],
    req_id: &str,
    client_id: &str,
    model: &str,
    endpoint_idx: usize,
    translate: bool, // true = Anthropic↔OpenAI translation needed
) -> ForwardOutcome {
    // Non-transient rotation: skip this endpoint and try the next candidate.
    // Pre-dates the ForwardOutcome return type as a bare `None`.
    const ROTATE: ForwardOutcome = ForwardOutcome::Retry {
        saw_529: false,
        push_skip: true,
        transient: false,
    };
    let ep = &state.endpoints[endpoint_idx];

    info!(
        req_id,
        client_id,
        model,
        upstream = ep.name,
        translate,
        "fallback: routing to unified OpenAI endpoint"
    );

    ep.requests.fetch_add(1, Ordering::Relaxed);

    // Parse once to extract streaming flag before potential translation
    let parsed: serde_json::Value = match serde_json::from_slice(body_bytes) {
        Ok(v) => v,
        Err(_) => return ROTATE,
    };
    let is_streaming = parsed
        .get("stream")
        .and_then(|s| s.as_bool())
        .unwrap_or(false);

    // Build request body
    let request_body = if translate {
        let openai_body = translate_anthropic_request_to_openai(&parsed);
        match serde_json::to_vec(&openai_body) {
            Ok(b) => b,
            Err(_) => return ROTATE,
        }
    } else {
        body_bytes.to_vec()
    };

    let url = format!("{}/v1/chat/completions", ep.base_url);

    // Same non-streaming read_timeout exemption as forward_anthropic (LAB-718).
    let http_client = if is_streaming {
        &state.client
    } else {
        &state.client_nonstreaming
    };
    let mut resp = match http_client
        .post(&url)
        .header("authorization", format!("Bearer {}", ep.token))
        .header("content-type", "application/json")
        .body(request_body)
        .send()
        .await
    {
        Ok(r) => r,
        Err(e) => {
            error!(
                req_id,
                upstream = ep.name,
                error = %e,
                detail = %describe_reqwest_error(&e),
                "fallback: unified OpenAI endpoint request failed"
            );
            // Same dashboard counter the Anthropic forward paths feed.
            let kind = if e.is_timeout() {
                "timeout"
            } else if e.is_connect() {
                "connect"
            } else {
                "other"
            };
            if let Ok(mut m) = state.upstream_transport_errors.lock() {
                *m.entry(kind).or_insert(0) += 1;
            }
            // Health signal + transient classification — closes the #69 gap
            // where this branch swallowed transport errors to a bare `None`.
            state.record_transport_failure(endpoint_idx).await;
            return ForwardOutcome::Retry {
                saw_529: false,
                push_skip: false,
                transient: true,
            };
        }
    };

    // Any HTTP response (even 429/5xx) proves the transport path is alive —
    // clear the circuit-breaker counter.
    state.record_transport_success(endpoint_idx).await;

    let status = resp.status();
    if !status.is_success() {
        let err_body = resp
            .text()
            .await
            .unwrap_or_else(|_| "upstream error".to_string());
        // Retry-eligible: 429 or 5xx → rotate so the retry loop advances
        // to the next candidate.
        if status == StatusCode::TOO_MANY_REQUESTS || status.is_server_error() {
            warn!(
                req_id,
                upstream = ep.name,
                status = status.as_u16(),
                body = %err_body,
                "fallback: unified endpoint returned retry-eligible error, advancing"
            );
            return ROTATE;
        }
        warn!(
            req_id,
            upstream = ep.name,
            status = status.as_u16(),
            body = %err_body,
            "fallback: unified endpoint returned error"
        );
        if translate {
            // Return error in Anthropic format
            return ForwardOutcome::Done(
                Response::builder()
                    .status(status)
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "type": "error",
                            "error": {
                                "type": "api_error",
                                "message": err_body,
                            }
                        })
                        .to_string(),
                    ))
                    .unwrap_or_else(|_| {
                        (StatusCode::INTERNAL_SERVER_ERROR, "fallback error").into_response()
                    }),
            );
        }
        return ForwardOutcome::Done(
            Response::builder()
                .status(status)
                .header("content-type", "application/json")
                .body(Body::from(err_body))
                .unwrap_or_else(|_| {
                    (StatusCode::INTERNAL_SERVER_ERROR, "fallback error").into_response()
                }),
        );
    }

    // Streaming response
    if is_streaming {
        let (tx, rx) = tokio::sync::mpsc::channel::<Result<bytes::Bytes, std::io::Error>>(32);
        let req_id = req_id.to_string();
        let upstream_name = ep.name.clone();
        let translate_response = translate;

        tokio::spawn(async move {
            let mut buffer: Vec<u8> = Vec::new();
            let mut ctx = ReverseStreamContext::default();
            let mut client_gone = false;
            // Passthrough-only: tracks whether upstream's `[DONE]` terminator
            // has been forwarded verbatim, so an error frame on the next read
            // doesn't ship a second `[DONE]` and break strict OpenAI parsers.
            // Not needed in the translate branch — translation converts
            // `[DONE]` to `message_stop`, which has no analogous terminator.
            let mut sent_done = false;
            // Carries any partial trailing SSE line between chunks so the
            // `[DONE]` terminator is detected across resp.chunk() boundaries.
            // A naive byte-window scan would false-positive on the literal
            // string "data: [DONE]" appearing inside a JSON content delta,
            // so we split on SSE newline boundaries and only treat a complete
            // `data: [DONE]` line as the terminator.
            let mut done_scan_tail: Vec<u8> = Vec::new();

            loop {
                match resp.chunk().await {
                    Ok(Some(chunk)) => {
                        if translate_response {
                            buffer.extend_from_slice(&chunk);
                            while let Some(pos) = buffer.windows(2).position(|w| w == b"\n\n") {
                                let event = String::from_utf8_lossy(&buffer[..pos]).into_owned();
                                buffer.drain(..pos + 2);

                                for line in event.lines() {
                                    if let Some(data) = line.strip_prefix("data: ") {
                                        let events =
                                            translate_openai_sse_to_anthropic(data, &mut ctx);
                                        for ev in events {
                                            if tx.send(Ok(bytes::Bytes::from(ev))).await.is_err() {
                                                client_gone = true;
                                                break;
                                            }
                                        }
                                    }
                                }
                                if client_gone {
                                    break;
                                }
                            }
                        } else {
                            if !sent_done {
                                done_scan_tail.extend_from_slice(&chunk);
                                while let Some(nl) = done_scan_tail.iter().position(|&b| b == b'\n')
                                {
                                    let line_end = if nl > 0 && done_scan_tail[nl - 1] == b'\r' {
                                        nl - 1
                                    } else {
                                        nl
                                    };
                                    let is_done_marker = if let Some(payload) =
                                        done_scan_tail[..line_end].strip_prefix(b"data:")
                                    {
                                        payload.trim_ascii() == b"[DONE]"
                                    } else {
                                        false
                                    };
                                    done_scan_tail.drain(..=nl);
                                    if is_done_marker {
                                        sent_done = true;
                                        done_scan_tail.clear();
                                        break;
                                    }
                                }
                            }
                            if tx.send(Ok(chunk)).await.is_err() {
                                client_gone = true;
                            }
                        }
                        if client_gone {
                            break;
                        }
                    }
                    Ok(None) => break,
                    Err(e) => {
                        warn!(req_id, error = %e, "fallback: unified endpoint SSE read failed");
                        // Downstream protocol depends on whether we're translating:
                        // translate_response=true → /v1/messages client expects
                        // Anthropic SSE; translate_response=false → /v1/chat/completions
                        // passthrough, downstream is the OpenAI SSE format. Skip
                        // the openai error frame when `[DONE]` was already
                        // forwarded — emitting it would ship a second `[DONE]`.
                        let msg = format!("upstream stream interrupted: {e}");
                        let frame = if translate_response {
                            Some(anthropic_error_frame(&msg))
                        } else if !sent_done {
                            Some(openai_error_frame(&msg))
                        } else {
                            None
                        };
                        if let Some(frame) = frame {
                            if tx.send(Ok(frame)).await.is_err() {
                                client_gone = true;
                            }
                        }
                        break;
                    }
                }
            }

            // Flush remaining buffer
            if translate_response && !buffer.is_empty() && !client_gone {
                let remaining = String::from_utf8_lossy(&buffer).into_owned();
                for line in remaining.lines() {
                    if let Some(data) = line.strip_prefix("data: ") {
                        let events = translate_openai_sse_to_anthropic(data, &mut ctx);
                        for ev in events {
                            if tx.send(Ok(bytes::Bytes::from(ev))).await.is_err() {
                                break;
                            }
                        }
                    }
                }
            }

            if client_gone {
                debug!(req_id, "fallback: client disconnected during stream");
            }
            info!(
                req_id,
                upstream = upstream_name,
                "fallback: unified endpoint stream complete"
            );
        });

        return ForwardOutcome::Done(
            Response::builder()
                .status(StatusCode::OK)
                .header("content-type", "text/event-stream")
                .header("cache-control", "no-cache")
                .header("connection", "keep-alive")
                .body(Body::from_stream(
                    tokio_stream::wrappers::ReceiverStream::new(rx),
                ))
                .unwrap_or_else(|_| {
                    (StatusCode::INTERNAL_SERVER_ERROR, "fallback stream error").into_response()
                }),
        );
    }

    // Non-streaming response
    let resp_body = match resp.bytes().await {
        Ok(b) => b,
        Err(e) => {
            // Mid-body failure after a received response: transport reached the
            // upstream, so this is NOT a breaker signal — plain rotation.
            error!(req_id, error = %e, "fallback: failed to read unified endpoint response body");
            return ROTATE;
        }
    };

    if translate {
        let openai_resp: serde_json::Value =
            serde_json::from_slice(&resp_body).unwrap_or(serde_json::json!({}));
        let anthropic_resp = translate_openai_response_to_anthropic(&openai_resp);
        info!(
            req_id,
            upstream = ep.name,
            "fallback: unified endpoint translated response"
        );
        ForwardOutcome::Done(
            Response::builder()
                .status(StatusCode::OK)
                .header("content-type", "application/json")
                .body(Body::from(anthropic_resp.to_string()))
                .unwrap_or_else(|_| {
                    (StatusCode::INTERNAL_SERVER_ERROR, "fallback error").into_response()
                }),
        )
    } else {
        info!(
            req_id,
            upstream = ep.name,
            "fallback: unified endpoint forwarded response"
        );
        ForwardOutcome::Done(
            Response::builder()
                .status(StatusCode::OK)
                .header("content-type", "application/json")
                .body(Body::from(resp_body))
                .unwrap_or_else(|_| {
                    (StatusCode::INTERNAL_SERVER_ERROR, "fallback error").into_response()
                }),
        )
    }
}

// ── Stats endpoint ──────────────────────────────────────────────────

/// Build one `/stats` JSON entry from an endpoint's (name, priority,
/// rate_info, burn_rate, counters) fields. When `protocol` is `Some`, a
/// `"protocol"` field is added to the entry.
#[allow(clippy::too_many_arguments)]
async fn build_stats_entry(
    name: &str,
    passthrough: bool,
    priority: u32,
    protocol: Option<&str>,
    rate_info: &RwLock<RateLimitInfo>,
    burn_rate: &Mutex<BurnRate>,
    requests: &AtomicU64,
    token_counters: [&AtomicU64; 4],
    now_epoch: u64,
    total_headroom: &mut Option<u64>,
) -> serde_json::Value {
    let info = rate_info.read().await;
    let hard_limited = match info.hard_limited_until {
        Some(until) if Instant::now() < until => {
            Some(until.duration_since(Instant::now()).as_secs())
        }
        _ => None,
    };

    // Burn rate from EWMA tracker
    let (br_5m, br_1h, br_6h) = burn_rate
        .lock()
        .map(|br| (br.rate_5m.value, br.rate_1h.value, br.rate_6h.value))
        .unwrap_or((0.0, 0.0, 0.0));

    // Headroom: prefer remaining_requests header, else (1-util)*limit, else null
    let headroom: Option<u64> = if let Some(rem) = info.remaining_requests {
        Some(rem)
    } else if let (Some(util), Some(limit)) = (info.utilization, info.limit_requests) {
        Some(((1.0 - util) * limit as f64).max(0.0) as u64)
    } else {
        None
    };
    match (total_headroom.as_mut(), headroom) {
        (Some(total), Some(h)) => *total += h,
        _ => *total_headroom = None,
    }

    // Projected throttle time
    let (eff_util, _, _, _) = effective_utilization(&info, now_epoch, "");
    let projected_throttle_at: serde_json::Value = if eff_util < 0.5 || br_1h < 0.01 {
        serde_json::Value::Null
    } else if let Some(headroom_reqs) = headroom {
        if headroom_reqs == 0 {
            // Already at limit — check if hard-limited and report cooldown expiry
            if let Some(hl_secs) = hard_limited {
                serde_json::Value::String(AppState::epoch_to_iso8601(now_epoch + hl_secs))
            } else {
                serde_json::Value::String(AppState::epoch_to_iso8601(now_epoch))
            }
        } else {
            let minutes_remaining = headroom_reqs as f64 / br_1h;
            let secs_remaining = (minutes_remaining * 60.0) as u64;
            let projected_epoch = now_epoch + secs_remaining;
            // If projection is beyond next reset, account will recover → null
            let mut next_reset = info.reset_5h.unwrap_or(u64::MAX);
            for c in info.claims_7d.values() {
                if let Some(r) = c.reset {
                    next_reset = next_reset.min(r);
                }
            }
            if projected_epoch > next_reset && next_reset != u64::MAX {
                serde_json::Value::Null
            } else {
                serde_json::Value::String(AppState::epoch_to_iso8601(projected_epoch))
            }
        }
    } else {
        serde_json::Value::Null
    };

    let mut entry = serde_json::json!({
        "name": name,
        "passthrough": passthrough,
        "priority": priority,
        "requests_total": requests.load(Ordering::Relaxed),
        "utilization": info.utilization,
        "utilization_7d": info.utilization_7d,
        "utilization_5h": info.utilization_5h,
        "representative_claim": info.representative_claim,
        "reset_5h": info.reset_5h,
        "reset_7d": info.reset_7d,
        "status_5h": info.status_5h,
        "status_7d": info.status_7d,
        "overage_in_use": info.overage_in_use,
        "overage_status": info.overage_status,
        "overage_utilization": info.overage_utilization,
        "overage_reset": info.overage_reset,
        "claims_7d": info.claims_7d.iter().map(|(k, v)| {
            (k.clone(), serde_json::json!({
                "utilization": v.utilization,
                "reset": v.reset,
                "status": v.status,
                "waste_risk": waste_risk(v.utilization, v.reset, now_epoch),
            }))
        }).collect::<serde_json::Map<String, serde_json::Value>>(),
        "remaining_requests": info.remaining_requests,
        "remaining_tokens": info.remaining_tokens,
        "limit_requests": info.limit_requests,
        "limit_tokens": info.limit_tokens,
        "hard_limited_remaining_secs": hard_limited,
        "burn_rate": {
            "last_5m": (br_5m * 100.0).round() / 100.0,
            "last_1h": (br_1h * 100.0).round() / 100.0,
            "last_6h": (br_6h * 100.0).round() / 100.0,
        },
        "headroom_requests": headroom,
        "projected_throttle_at": projected_throttle_at,
        "token_usage": {
            "input_tokens": token_counters[0].load(Ordering::Relaxed),
            "output_tokens": token_counters[1].load(Ordering::Relaxed),
            "cache_creation_input_tokens": token_counters[2].load(Ordering::Relaxed),
            "cache_read_input_tokens": token_counters[3].load(Ordering::Relaxed),
        },
    });
    if let Some(p) = protocol {
        entry["protocol"] = serde_json::json!(p);
    }
    entry
}

async fn stats_handler(
    State(state): State<Arc<AppState>>,
    axum::extract::ConnectInfo(client_addr): axum::extract::ConnectInfo<SocketAddr>,
    req: Request<Body>,
) -> Response {
    if !state.is_ip_allowed(&client_addr.ip()) {
        return (StatusCode::FORBIDDEN, "forbidden").into_response();
    }
    if let Some(ref key) = state.proxy_key {
        let provided = req.headers().get("x-api-key").and_then(|v| v.to_str().ok());
        if provided != Some(key.as_str()) {
            return (StatusCode::UNAUTHORIZED, "unauthorized").into_response();
        }
    }

    let now_epoch = AppState::now_epoch();
    let mut total_headroom: Option<u64> = Some(0);
    let mut endpoint_stats = Vec::new();
    for ep in &state.endpoints {
        let protocol = match ep.protocol {
            Protocol::Anthropic => "anthropic",
            Protocol::OpenAI => "openai",
        };
        endpoint_stats.push(
            build_stats_entry(
                &ep.name,
                ep.passthrough,
                ep.priority,
                Some(protocol),
                &ep.rate_info,
                &ep.burn_rate,
                &ep.requests,
                [
                    &ep.input_tokens,
                    &ep.output_tokens,
                    &ep.cache_creation_tokens,
                    &ep.cache_read_tokens,
                ],
                now_epoch,
                &mut total_headroom,
            )
            .await,
        );
    }

    // Per-client usage (tokens + request rates)
    let request_rates = state.client_request_rates.lock().ok();
    let client_usage: serde_json::Value = state
        .client_usage
        .lock()
        .map(|map| {
            // Collect all client IDs from both token usage and request rates
            let mut all_clients: std::collections::HashSet<&String> = map.keys().collect();
            if let Some(ref rates) = request_rates {
                all_clients.extend(rates.keys());
            }

            let obj: serde_json::Map<String, serde_json::Value> = all_clients
                .into_iter()
                .map(|k| {
                    // Operator hiding: attribute operator data to a reserved key
                    let display_key = if state.is_operator(k) {
                        "_operator".to_string()
                    } else {
                        k.clone()
                    };
                    let tokens = map.get(k).copied().unwrap_or([0; 4]);
                    let (req_total, req_per_min) = request_rates
                        .as_ref()
                        .and_then(|r| r.get(k))
                        .map(|(total, ewma)| (*total, ewma.value))
                        .unwrap_or((0, 0.0));
                    (
                        display_key,
                        serde_json::json!({
                            "input_tokens": tokens[0],
                            "output_tokens": tokens[1],
                            "cache_creation_input_tokens": tokens[2],
                            "cache_read_input_tokens": tokens[3],
                            "requests_total": req_total,
                            "requests_per_minute": (req_per_min * 100.0).round() / 100.0,
                        }),
                    )
                })
                .collect();
            serde_json::Value::Object(obj)
        })
        .unwrap_or(serde_json::json!({}));

    // Aggregate: total headroom + per-consumer share
    let aggregate = {
        let mut consumers = serde_json::Map::new();
        let mut total_rpm = 0.0_f64;
        if let Some(ref rates) = request_rates {
            for (client, (_, ewma)) in rates.iter() {
                let display_key = if state.is_operator(client) {
                    "_operator".to_string()
                } else {
                    client.clone()
                };
                total_rpm += ewma.value;
                let entry = consumers.entry(display_key).or_insert_with(
                    || serde_json::json!({"requests_per_minute": 0.0, "share": 0.0}),
                );
                if let Some(obj) = entry.as_object_mut() {
                    let cur = obj
                        .get("requests_per_minute")
                        .and_then(|v| v.as_f64())
                        .unwrap_or(0.0);
                    obj.insert(
                        "requests_per_minute".to_string(),
                        serde_json::json!(cur + ewma.value),
                    );
                }
            }
            // Compute shares
            if total_rpm > 0.0 {
                for (_client, val) in consumers.iter_mut() {
                    if let Some(obj) = val.as_object_mut() {
                        let rpm = obj
                            .get("requests_per_minute")
                            .and_then(|v| v.as_f64())
                            .unwrap_or(0.0);
                        obj.insert(
                            "requests_per_minute".to_string(),
                            serde_json::json!((rpm * 100.0).round() / 100.0),
                        );
                        obj.insert(
                            "share".to_string(),
                            serde_json::json!(((rpm / total_rpm) * 1000.0).round() / 1000.0),
                        );
                    }
                }
            }
        }
        serde_json::json!({
            "total_headroom_requests": total_headroom,
            "consumers": serde_json::Value::Object(consumers),
        })
    };

    // Per-client budget status
    let budgets: serde_json::Value = if state.client_budgets.is_empty() {
        serde_json::json!(null)
    } else {
        let today = AppState::now_epoch() / 86400;
        let usage_map = state.budget_usage.lock().ok();
        let obj: serde_json::Map<String, serde_json::Value> = state
            .client_budgets
            .iter()
            .map(|(client, &limit)| {
                let used = usage_map
                    .as_ref()
                    .and_then(|m| m.get(client))
                    .filter(|(day, _)| *day == today)
                    .map(|(_, used)| *used)
                    .unwrap_or(0);
                (
                    client.clone(),
                    serde_json::json!({
                        "daily_limit": limit,
                        "used_today": used,
                        "remaining": limit.saturating_sub(used),
                    }),
                )
            })
            .collect();
        serde_json::Value::Object(obj)
    };

    // Cluster info (when Redis is available)
    // Read from cache (updated by background sync task) to avoid .await in handler
    let cluster: Option<serde_json::Value> =
        state.cluster_info_cache.lock().ok().and_then(|g| g.clone());

    let mut response = serde_json::json!({
        "endpoints": endpoint_stats,
        "client_usage": client_usage,
        "client_budgets": budgets,
        "aggregate": aggregate,
        "strategy": state.routing_strategy.as_str(),
    });
    if let Some(cluster_info) = cluster {
        response["cluster"] = cluster_info;
    }

    axum::Json(response).into_response()
}

// ── Prometheus text exposition helpers ──────────────────────────────────

/// Format a float for Prometheus: no trailing zeros, NaN and Inf handled.
fn prom_fmt(v: f64) -> String {
    if v.is_nan() {
        return "NaN".to_string();
    }
    if v.is_infinite() {
        return if v > 0.0 { "+Inf" } else { "-Inf" }.to_string();
    }
    if v.fract() == 0.0 && v.abs() < 1e15 {
        format!("{}", v as i64)
    } else {
        format!("{}", v)
    }
}

fn prom_escape(s: &str) -> String {
    s.replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
}

fn prom_gauge(buf: &mut String, name: &str, labels: &[(&str, &str)], value: f64) {
    buf.push_str(name);
    if !labels.is_empty() {
        buf.push('{');
        for (i, (k, v)) in labels.iter().enumerate() {
            if i > 0 {
                buf.push(',');
            }
            buf.push_str(k);
            buf.push_str("=\"");
            buf.push_str(&prom_escape(v));
            buf.push('"');
        }
        buf.push('}');
    }
    buf.push(' ');
    buf.push_str(&prom_fmt(value));
    buf.push('\n');
}

fn prom_counter(buf: &mut String, name: &str, labels: &[(&str, &str)], value: u64) {
    buf.push_str(name);
    if !labels.is_empty() {
        buf.push('{');
        for (i, (k, v)) in labels.iter().enumerate() {
            if i > 0 {
                buf.push(',');
            }
            buf.push_str(k);
            buf.push_str("=\"");
            buf.push_str(&prom_escape(v));
            buf.push('"');
        }
        buf.push('}');
    }
    buf.push(' ');
    buf.push_str(&value.to_string());
    buf.push('\n');
}

fn prom_header(buf: &mut String, name: &str, metric_type: &str, help: &str) {
    use std::fmt::Write;
    let _ = writeln!(buf, "# HELP {name} {help}");
    let _ = writeln!(buf, "# TYPE {name} {metric_type}");
}

#[allow(dead_code)]
#[derive(Default, Clone)]
struct ClaimMetricsSnap {
    key: String,
    utilization: Option<f64>,
    waste_risk: f64,
    reset: Option<u64>,
    status: Option<String>,
}

#[allow(dead_code)]
#[derive(Default, Clone)]
struct EndpointMetricsSnap {
    name: String,
    passthrough: bool,
    has_applicable_7d: bool,
    utilization: Option<f64>,
    utilization_5h: Option<f64>,
    utilization_7d: Option<f64>,
    reset_5h: Option<u64>,
    reset_7d: Option<u64>,
    status_5h: Option<String>,
    status_7d: Option<String>,
    stale_after_hard_limit: bool,
    hard_limited_active: bool,
    burn_rate: (f64, f64, f64),
    headroom: Option<u64>,
    remaining_requests: Option<u64>,
    remaining_tokens: Option<u64>,
    limit_requests: Option<u64>,
    limit_tokens: Option<u64>,
    requests_total: u64,
    hard_limited_secs: f64,
    projected_throttle_secs: Option<f64>,
    token_usage: [u64; 4],
    claims: Vec<ClaimMetricsSnap>,
    last_updated_epoch: Option<u64>,
    overage_in_use: bool,
    overage_utilization: Option<f64>,
    /// Routing-weight gauges, captured from the source struct's atomics at
    /// snap time. Snap-carried so the routing-weight emission is pool-agnostic.
    routing_weight: f64,
    routing_share: f64,
    effective_gate: f64,
}

#[cfg(test)]
fn append_routing_weight_metrics(
    buf: &mut String,
    endpoints: &[Endpoint],
    snaps: &[EndpointMetricsSnap],
) {
    prom_header(
        buf,
        "anthropic_account_routing_weight",
        "gauge",
        "Per-account routing weight (headroom * waste_risk, or plain headroom when no 7d claim)",
    );
    prom_header(
        buf,
        "anthropic_account_routing_share",
        "gauge",
        "Per-account share of total routing weight (0.0-1.0)",
    );
    prom_header(
        buf,
        "anthropic_account_effective_gate",
        "gauge",
        "Effective routing gate: max(time_adjusted_5h, time_adjusted_7d) with status floors",
    );

    for (ep, snap) in endpoints.iter().zip(snaps.iter()) {
        if snap.passthrough {
            continue;
        }
        let weight = f64::from_bits(ep.last_routing_weight.load(Ordering::Relaxed));
        let share = f64::from_bits(ep.last_routing_share.load(Ordering::Relaxed));
        let gate = f64::from_bits(ep.last_effective_gate.load(Ordering::Relaxed));
        prom_gauge(
            buf,
            "anthropic_account_routing_weight",
            &[("account", &snap.name)],
            weight,
        );
        prom_gauge(
            buf,
            "anthropic_account_routing_share",
            &[("account", &snap.name)],
            share,
        );
        prom_gauge(
            buf,
            "anthropic_account_effective_gate",
            &[("account", &snap.name)],
            gate,
        );
    }
}

/// Build an `EndpointMetricsSnap` from an endpoint's (name, rate_info,
/// burn_rate, counters, gauge atomics) fields. Callers pass field references
/// rather than an `&Endpoint`.
#[allow(clippy::too_many_arguments)]
async fn build_metrics_snap(
    name: &str,
    passthrough: bool,
    rate_info: &RwLock<RateLimitInfo>,
    burn_rate: &Mutex<BurnRate>,
    requests: &AtomicU64,
    token_counters: [&AtomicU64; 4],
    routing_weight_atomic: &AtomicU64,
    routing_share_atomic: &AtomicU64,
    effective_gate_atomic: &AtomicU64,
    now_epoch: u64,
    total_headroom: &mut Option<u64>,
) -> EndpointMetricsSnap {
    let info = rate_info.read().await;
    let (br_5m, br_1h, br_6h) = burn_rate
        .lock()
        .map(|br| (br.rate_5m.value, br.rate_1h.value, br.rate_6h.value))
        .unwrap_or((0.0, 0.0, 0.0));

    let headroom: Option<u64> = if let Some(rem) = info.remaining_requests {
        Some(rem)
    } else if let (Some(util), Some(limit)) = (info.utilization, info.limit_requests) {
        Some(((1.0 - util) * limit as f64).max(0.0) as u64)
    } else {
        None
    };
    match (total_headroom.as_mut(), headroom) {
        (Some(total), Some(h)) => *total += h,
        _ => *total_headroom = None,
    }

    let hard_limited_secs = info
        .hard_limited_until
        .and_then(|until| until.checked_duration_since(Instant::now()))
        .map(|d| d.as_secs() as f64)
        .unwrap_or(0.0);
    let hard_limited_active = info
        .hard_limited_until
        .is_some_and(|until| Instant::now() < until);
    let stale_after_hard_limit = info
        .hard_limited_until
        .is_some_and(|until| info.last_updated.is_none_or(|lu| lu <= until));

    let (eff_util, _, _, _) = effective_utilization(&info, now_epoch, "");
    let projected_throttle_secs = if eff_util < 0.5 || br_1h < 0.01 {
        None
    } else {
        headroom.map(|h| {
            if h == 0 {
                0.0
            } else {
                (h as f64 / br_1h) * 60.0
            }
        })
    };

    let claims: Vec<ClaimMetricsSnap> = info
        .claims_7d
        .iter()
        .map(|(k, d)| ClaimMetricsSnap {
            key: k.clone(),
            utilization: d.utilization,
            waste_risk: waste_risk(d.utilization, d.reset, now_epoch),
            reset: d.reset,
            status: d.status.clone(),
        })
        .collect();

    EndpointMetricsSnap {
        name: name.to_string(),
        passthrough,
        has_applicable_7d: !claims.is_empty()
            || info.utilization_7d.is_some()
            || info.reset_7d.is_some()
            || info.status_7d.is_some(),
        utilization: info.utilization,
        utilization_5h: info.utilization_5h,
        utilization_7d: info.utilization_7d,
        reset_5h: info.reset_5h,
        reset_7d: info.reset_7d,
        status_5h: info.status_5h.clone(),
        status_7d: info.status_7d.clone(),
        stale_after_hard_limit,
        hard_limited_active,
        burn_rate: (br_5m, br_1h, br_6h),
        headroom,
        remaining_requests: info.remaining_requests,
        remaining_tokens: info.remaining_tokens,
        limit_requests: info.limit_requests,
        limit_tokens: info.limit_tokens,
        requests_total: requests.load(Ordering::Relaxed),
        hard_limited_secs,
        projected_throttle_secs,
        token_usage: [
            token_counters[0].load(Ordering::Relaxed),
            token_counters[1].load(Ordering::Relaxed),
            token_counters[2].load(Ordering::Relaxed),
            token_counters[3].load(Ordering::Relaxed),
        ],
        claims,
        last_updated_epoch: info.last_updated_epoch,
        overage_in_use: info.overage_in_use,
        overage_utilization: info.overage_utilization,
        routing_weight: f64::from_bits(routing_weight_atomic.load(Ordering::Relaxed)),
        routing_share: f64::from_bits(routing_share_atomic.load(Ordering::Relaxed)),
        effective_gate: f64::from_bits(effective_gate_atomic.load(Ordering::Relaxed)),
    }
}

async fn metrics_handler(
    State(state): State<Arc<AppState>>,
    axum::extract::ConnectInfo(client_addr): axum::extract::ConnectInfo<SocketAddr>,
    req: Request<Body>,
) -> Response {
    if !state.is_ip_allowed(&client_addr.ip()) {
        return (StatusCode::FORBIDDEN, "forbidden").into_response();
    }
    if let Some(ref key) = state.proxy_key {
        let provided = req.headers().get("x-api-key").and_then(|v| v.to_str().ok());
        if provided != Some(key.as_str()) {
            return (StatusCode::UNAUTHORIZED, "unauthorized").into_response();
        }
    }

    let now_epoch = AppState::now_epoch();
    let today = now_epoch / 86400;

    // ── Phase 1: Gather data (async — acquires locks) ──────────────

    let mut snaps: Vec<EndpointMetricsSnap> = Vec::with_capacity(state.endpoints.len());
    let mut total_headroom: Option<u64> = Some(0);

    // OpenAI endpoints have a stub RateLimitInfo — their gauges read as
    // zero/None, which is the correct representation for an endpoint with no
    // rate-limit data.
    for ep in &state.endpoints {
        snaps.push(
            build_metrics_snap(
                &ep.name,
                ep.passthrough,
                &ep.rate_info,
                &ep.burn_rate,
                &ep.requests,
                [
                    &ep.input_tokens,
                    &ep.output_tokens,
                    &ep.cache_creation_tokens,
                    &ep.cache_read_tokens,
                ],
                &ep.last_routing_weight,
                &ep.last_routing_share,
                &ep.last_effective_gate,
                now_epoch,
                &mut total_headroom,
            )
            .await,
        );
    }

    // Extract global maps once (single lock per map, then drop guard)
    let client_rates: HashMap<String, (u64, f64)> = state
        .client_request_rates
        .lock()
        .ok()
        .map(|g| {
            g.iter()
                .map(|(k, (total, ewma))| (k.clone(), (*total, ewma.value)))
                .collect()
        })
        .unwrap_or_default();
    let client_usage = state
        .client_usage
        .lock()
        .ok()
        .map(|g| g.clone())
        .unwrap_or_default();
    let budget_usage = state
        .budget_usage
        .lock()
        .ok()
        .map(|g| g.clone())
        .unwrap_or_default();
    let cluster_info = state.cluster_info_cache.lock().ok().and_then(|g| g.clone());

    // ── Phase 2: Serialize (sync — no locks held) ──────────────────

    let mut buf = String::with_capacity(4096);

    // Meta
    prom_header(
        &mut buf,
        "anthropic_lb_info",
        "gauge",
        "Load balancer info, always 1",
    );
    prom_gauge(
        &mut buf,
        "anthropic_lb_info",
        &[("strategy", state.routing_strategy.as_str())],
        1.0,
    );

    // Account utilization
    prom_header(
        &mut buf,
        "anthropic_account_utilization",
        "gauge",
        "Account utilization by time window",
    );
    for s in &snaps {
        if let Some(u) = s.utilization_5h {
            prom_gauge(
                &mut buf,
                "anthropic_account_utilization",
                &[("account", &s.name), ("window", "5h")],
                u,
            );
        }
        if let Some(u) = s.utilization_7d {
            prom_gauge(
                &mut buf,
                "anthropic_account_utilization",
                &[("account", &s.name), ("window", "7d")],
                u,
            );
        }
        // Overage window — emitted only while overage is actively serving, so the
        // metric's presence itself signals an account on paid extra usage.
        if s.overage_in_use {
            if let Some(u) = s.overage_utilization {
                prom_gauge(
                    &mut buf,
                    "anthropic_account_utilization",
                    &[("account", &s.name), ("window", "overage")],
                    u,
                );
            }
        }
    }

    // Rate-limit status (ordinal: 0=allowed, 1=allowed_warning, 2=throttled, 3=rejected)
    prom_header(
        &mut buf,
        "anthropic_account_rate_limit_status",
        "gauge",
        "Rate-limit status ordinal (0=allowed, 1=warning, 2=throttled, 3=rejected)",
    );
    for s in &snaps {
        if s.passthrough {
            continue;
        }
        prom_gauge(
            &mut buf,
            "anthropic_account_rate_limit_status",
            &[("account", &s.name), ("window", "5h")],
            status_to_ordinal(s.status_5h.as_deref()),
        );
        prom_gauge(
            &mut buf,
            "anthropic_account_rate_limit_status",
            &[("account", &s.name), ("window", "7d")],
            status_to_ordinal(s.status_7d.as_deref()),
        );
    }

    // Account reset countdowns
    prom_header(
        &mut buf,
        "anthropic_account_reset_seconds",
        "gauge",
        "Seconds until rate-limit window resets",
    );
    for s in &snaps {
        if let Some(r) = s.reset_5h.filter(|&r| r > now_epoch) {
            prom_gauge(
                &mut buf,
                "anthropic_account_reset_seconds",
                &[("account", &s.name), ("window", "5h")],
                (r - now_epoch) as f64,
            );
        }
        if let Some(r) = s.reset_7d.filter(|&r| r > now_epoch) {
            prom_gauge(
                &mut buf,
                "anthropic_account_reset_seconds",
                &[("account", &s.name), ("window", "7d")],
                (r - now_epoch) as f64,
            );
        }
    }

    // Account burn rate
    prom_header(
        &mut buf,
        "anthropic_account_burn_rate",
        "gauge",
        "Account burn rate (requests/min) by time window",
    );
    for s in &snaps {
        prom_gauge(
            &mut buf,
            "anthropic_account_burn_rate",
            &[("account", &s.name), ("window", "5m")],
            s.burn_rate.0,
        );
        prom_gauge(
            &mut buf,
            "anthropic_account_burn_rate",
            &[("account", &s.name), ("window", "1h")],
            s.burn_rate.1,
        );
        prom_gauge(
            &mut buf,
            "anthropic_account_burn_rate",
            &[("account", &s.name), ("window", "6h")],
            s.burn_rate.2,
        );
    }

    // Account headroom
    prom_header(
        &mut buf,
        "anthropic_account_headroom_requests",
        "gauge",
        "Available request headroom",
    );
    for s in &snaps {
        if let Some(h) = s.headroom {
            prom_gauge(
                &mut buf,
                "anthropic_account_headroom_requests",
                &[("account", &s.name)],
                h as f64,
            );
        }
    }

    // Account remaining requests
    prom_header(
        &mut buf,
        "anthropic_account_remaining_requests",
        "gauge",
        "Remaining requests in rate-limit window",
    );
    for s in &snaps {
        if let Some(v) = s.remaining_requests {
            prom_gauge(
                &mut buf,
                "anthropic_account_remaining_requests",
                &[("account", &s.name)],
                v as f64,
            );
        }
    }

    // Account remaining tokens
    prom_header(
        &mut buf,
        "anthropic_account_remaining_tokens",
        "gauge",
        "Remaining tokens in rate-limit window",
    );
    for s in &snaps {
        if let Some(v) = s.remaining_tokens {
            prom_gauge(
                &mut buf,
                "anthropic_account_remaining_tokens",
                &[("account", &s.name)],
                v as f64,
            );
        }
    }

    // Account limits
    prom_header(
        &mut buf,
        "anthropic_account_limit_requests",
        "gauge",
        "Request limit",
    );
    for s in &snaps {
        if let Some(v) = s.limit_requests {
            prom_gauge(
                &mut buf,
                "anthropic_account_limit_requests",
                &[("account", &s.name)],
                v as f64,
            );
        }
    }
    prom_header(
        &mut buf,
        "anthropic_account_limit_tokens",
        "gauge",
        "Token limit",
    );
    for s in &snaps {
        if let Some(v) = s.limit_tokens {
            prom_gauge(
                &mut buf,
                "anthropic_account_limit_tokens",
                &[("account", &s.name)],
                v as f64,
            );
        }
    }

    // Account requests total (counter)
    prom_header(
        &mut buf,
        "anthropic_account_requests_total",
        "counter",
        "Total requests routed to account",
    );
    for s in &snaps {
        prom_counter(
            &mut buf,
            "anthropic_account_requests_total",
            &[("account", &s.name)],
            s.requests_total,
        );
    }

    // Account hard limited
    prom_header(
        &mut buf,
        "anthropic_account_hard_limited_remaining_seconds",
        "gauge",
        "Seconds until hard limit expires, 0 if not limited",
    );
    for s in &snaps {
        prom_gauge(
            &mut buf,
            "anthropic_account_hard_limited_remaining_seconds",
            &[("account", &s.name)],
            s.hard_limited_secs,
        );
    }

    // Account projected throttle — omit when unprojectable (R2.10)
    prom_header(
        &mut buf,
        "anthropic_account_projected_throttle_seconds",
        "gauge",
        "Seconds until projected throttle",
    );
    for s in &snaps {
        if let Some(secs) = s.projected_throttle_secs {
            prom_gauge(
                &mut buf,
                "anthropic_account_projected_throttle_seconds",
                &[("account", &s.name)],
                secs,
            );
        }
    }

    // Account token usage (counter — _total suffix per Prometheus convention)
    prom_header(
        &mut buf,
        "anthropic_account_token_usage_total",
        "counter",
        "Token usage by type",
    );
    for s in &snaps {
        let n = &s.name;
        prom_counter(
            &mut buf,
            "anthropic_account_token_usage_total",
            &[("account", n), ("type", "input")],
            s.token_usage[0],
        );
        prom_counter(
            &mut buf,
            "anthropic_account_token_usage_total",
            &[("account", n), ("type", "output")],
            s.token_usage[1],
        );
        prom_counter(
            &mut buf,
            "anthropic_account_token_usage_total",
            &[("account", n), ("type", "cache_creation")],
            s.token_usage[2],
        );
        prom_counter(
            &mut buf,
            "anthropic_account_token_usage_total",
            &[("account", n), ("type", "cache_read")],
            s.token_usage[3],
        );
    }

    // Account passthrough flag
    prom_header(
        &mut buf,
        "anthropic_account_passthrough",
        "gauge",
        "1 if passthrough account",
    );
    for s in &snaps {
        prom_gauge(
            &mut buf,
            "anthropic_account_passthrough",
            &[("account", &s.name)],
            if s.passthrough { 1.0 } else { 0.0 },
        );
    }

    // Account-level waste risk (max across 7d claims).
    // Note: refresh_metrics_weights uses a 3-tier claim selection
    // (representative → seven_day → max) for routing_weight/share.
    // This metric intentionally shows the max to surface the worst-case
    // claim regardless of which one the router currently selects.
    prom_header(
        &mut buf,
        "anthropic_account_waste_risk",
        "gauge",
        "Max waste risk across 7d claims (worst-case urgency signal)",
    );
    for s in &snaps {
        if s.passthrough || s.claims.is_empty() {
            continue;
        }
        let wr = s.claims.iter().map(|c| c.waste_risk).fold(0.0f64, f64::max);
        prom_gauge(
            &mut buf,
            "anthropic_account_waste_risk",
            &[("account", &s.name)],
            wr,
        );
    }

    // Probe data age (seconds since last rate-limit header update)
    prom_header(
        &mut buf,
        "anthropic_account_data_age_seconds",
        "gauge",
        "Seconds since last rate-limit data update from upstream",
    );
    for s in &snaps {
        if s.passthrough {
            continue;
        }
        if let Some(epoch) = s.last_updated_epoch {
            let age = if now_epoch > epoch {
                (now_epoch - epoch) as f64
            } else {
                0.0
            };
            prom_gauge(
                &mut buf,
                "anthropic_account_data_age_seconds",
                &[("account", &s.name)],
                age,
            );
        }
    }

    // Claim metrics
    prom_header(
        &mut buf,
        "anthropic_claim_utilization",
        "gauge",
        "Per-claim utilization",
    );
    for s in &snaps {
        for claim in &s.claims {
            if let Some(u) = claim.utilization {
                prom_gauge(
                    &mut buf,
                    "anthropic_claim_utilization",
                    &[("account", &s.name), ("claim", &claim.key)],
                    u,
                );
            }
        }
    }
    prom_header(
        &mut buf,
        "anthropic_claim_waste_risk",
        "gauge",
        "Per-claim waste risk score",
    );
    for s in &snaps {
        for claim in &s.claims {
            prom_gauge(
                &mut buf,
                "anthropic_claim_waste_risk",
                &[("account", &s.name), ("claim", &claim.key)],
                claim.waste_risk,
            );
        }
    }

    // ── Routing weights (refreshed by refresh_metrics_weights per probe cycle) ─────

    prom_header(
        &mut buf,
        "anthropic_account_routing_weight",
        "gauge",
        "Per-account routing weight (headroom * waste_risk, or plain headroom when no 7d claim)",
    );
    prom_header(
        &mut buf,
        "anthropic_account_routing_share",
        "gauge",
        "Per-account share of total routing weight (0.0-1.0)",
    );
    prom_header(
        &mut buf,
        "anthropic_account_effective_gate",
        "gauge",
        "Effective routing gate: max(time_adjusted_5h, time_adjusted_7d) with status floors",
    );

    // Snap-carried gauges (captured at snap time).
    for s in &snaps {
        if s.passthrough {
            continue;
        }
        prom_gauge(
            &mut buf,
            "anthropic_account_routing_weight",
            &[("account", &s.name)],
            s.routing_weight,
        );
        prom_gauge(
            &mut buf,
            "anthropic_account_routing_share",
            &[("account", &s.name)],
            s.routing_share,
        );
        prom_gauge(
            &mut buf,
            "anthropic_account_effective_gate",
            &[("account", &s.name)],
            s.effective_gate,
        );
    }

    // ── Aggregate metrics ──────────────────────────────────────────

    prom_header(
        &mut buf,
        "anthropic_total_headroom_requests",
        "gauge",
        "Sum of all account headroom",
    );
    if let Some(h) = total_headroom {
        prom_gauge(&mut buf, "anthropic_total_headroom_requests", &[], h as f64);
    }

    // Consumer share — aggregate operators into single _operator entry
    prom_header(
        &mut buf,
        "anthropic_consumer_share",
        "gauge",
        "Per-consumer fair share of capacity",
    );
    let total_rpm: f64 = client_rates.values().map(|(_, rpm)| rpm).sum();
    if total_rpm > 0.0 {
        let mut operator_rpm = 0.0f64;
        for (client, (_, rpm)) in &client_rates {
            if state.is_operator(client) {
                operator_rpm += rpm;
            } else {
                prom_gauge(
                    &mut buf,
                    "anthropic_consumer_share",
                    &[("client", client)],
                    rpm / total_rpm,
                );
            }
        }
        if operator_rpm > 0.0 {
            prom_gauge(
                &mut buf,
                "anthropic_consumer_share",
                &[("client", "_operator")],
                operator_rpm / total_rpm,
            );
        }
    }

    // ── Client metrics — aggregate operators into single _operator ──

    // Pre-aggregate operator totals
    let mut op_tokens = [0u64; 4];
    let mut op_requests: u64 = 0;
    let mut op_rpm = 0.0f64;
    let mut all_clients: std::collections::HashSet<&String> = client_usage.keys().collect();
    all_clients.extend(client_rates.keys());
    for client in &all_clients {
        if state.is_operator(client) {
            let t = client_usage.get(*client).copied().unwrap_or([0; 4]);
            for i in 0..4 {
                op_tokens[i] += t[i];
            }
            if let Some((total, rpm)) = client_rates.get(*client) {
                op_requests += total;
                op_rpm += rpm;
            }
        }
    }

    prom_header(
        &mut buf,
        "anthropic_client_token_usage_total",
        "counter",
        "Per-client token usage by type",
    );
    let types = ["input", "output", "cache_creation", "cache_read"];
    let mut emitted_operator_token = false;
    for client in &all_clients {
        if state.is_operator(client) {
            if !emitted_operator_token {
                for (i, t) in types.iter().enumerate() {
                    prom_counter(
                        &mut buf,
                        "anthropic_client_token_usage_total",
                        &[("client", "_operator"), ("type", t)],
                        op_tokens[i],
                    );
                }
                emitted_operator_token = true;
            }
        } else {
            let tokens = client_usage.get(*client).copied().unwrap_or([0; 4]);
            for (i, t) in types.iter().enumerate() {
                prom_counter(
                    &mut buf,
                    "anthropic_client_token_usage_total",
                    &[("client", client), ("type", t)],
                    tokens[i],
                );
            }
        }
    }

    prom_header(
        &mut buf,
        "anthropic_client_requests_total",
        "counter",
        "Per-client total requests",
    );
    let mut emitted_operator_req = false;
    for client in &all_clients {
        if state.is_operator(client) {
            if !emitted_operator_req {
                prom_counter(
                    &mut buf,
                    "anthropic_client_requests_total",
                    &[("client", "_operator")],
                    op_requests,
                );
                emitted_operator_req = true;
            }
        } else {
            let total = client_rates.get(*client).map(|(t, _)| *t).unwrap_or(0);
            prom_counter(
                &mut buf,
                "anthropic_client_requests_total",
                &[("client", client)],
                total,
            );
        }
    }

    prom_header(
        &mut buf,
        "anthropic_client_requests_per_minute",
        "gauge",
        "Per-client request rate (EWMA)",
    );
    let mut emitted_operator_rpm = false;
    for client in &all_clients {
        if state.is_operator(client) {
            if !emitted_operator_rpm {
                prom_gauge(
                    &mut buf,
                    "anthropic_client_requests_per_minute",
                    &[("client", "_operator")],
                    op_rpm,
                );
                emitted_operator_rpm = true;
            }
        } else {
            let rpm = client_rates.get(*client).map(|(_, r)| *r).unwrap_or(0.0);
            prom_gauge(
                &mut buf,
                "anthropic_client_requests_per_minute",
                &[("client", client)],
                rpm,
            );
        }
    }

    // Client budgets
    if !state.client_budgets.is_empty() {
        prom_header(
            &mut buf,
            "anthropic_client_budget_limit",
            "gauge",
            "Configured daily token limit",
        );
        for (client, &limit) in &state.client_budgets {
            prom_gauge(
                &mut buf,
                "anthropic_client_budget_limit",
                &[("client", client)],
                limit as f64,
            );
        }
        prom_header(
            &mut buf,
            "anthropic_client_budget_used",
            "gauge",
            "Tokens used today",
        );
        for client in state.client_budgets.keys() {
            let used = budget_usage
                .get(client)
                .filter(|(day, _)| *day == today)
                .map(|(_, used)| *used)
                .unwrap_or(0);
            prom_gauge(
                &mut buf,
                "anthropic_client_budget_used",
                &[("client", client)],
                used as f64,
            );
        }
        prom_header(
            &mut buf,
            "anthropic_client_budget_remaining",
            "gauge",
            "Tokens remaining today",
        );
        for (client, &limit) in &state.client_budgets {
            let used = budget_usage
                .get(client)
                .filter(|(day, _)| *day == today)
                .map(|(_, used)| *used)
                .unwrap_or(0);
            prom_gauge(
                &mut buf,
                "anthropic_client_budget_remaining",
                &[("client", client)],
                limit.saturating_sub(used) as f64,
            );
        }
    }

    // Cluster (Redis)
    if let Some(ref ci) = cluster_info {
        prom_header(
            &mut buf,
            "anthropic_cluster_redis_connected",
            "gauge",
            "Whether Redis is connected",
        );
        let connected = ci
            .get("redis_connected")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        prom_gauge(
            &mut buf,
            "anthropic_cluster_redis_connected",
            &[],
            if connected { 1.0 } else { 0.0 },
        );

        prom_header(
            &mut buf,
            "anthropic_cluster_replicas_seen",
            "gauge",
            "Number of cluster replicas",
        );
        let replicas = ci
            .get("replicas_seen")
            .and_then(|v| v.as_u64())
            .unwrap_or(0);
        prom_gauge(
            &mut buf,
            "anthropic_cluster_replicas_seen",
            &[],
            replicas as f64,
        );

        if let Some(bu) = ci.get("budget_usage").and_then(|v| v.as_object()) {
            prom_header(
                &mut buf,
                "anthropic_cluster_budget_limit",
                "gauge",
                "Cluster-wide budget limit",
            );
            for (client, data) in bu {
                if let Some(limit) = data.get("limit").and_then(|v| v.as_u64()) {
                    prom_gauge(
                        &mut buf,
                        "anthropic_cluster_budget_limit",
                        &[("client", client)],
                        limit as f64,
                    );
                }
            }
            prom_header(
                &mut buf,
                "anthropic_cluster_budget_used",
                "gauge",
                "Cluster-wide budget used",
            );
            for (client, data) in bu {
                if let Some(used) = data.get("used").and_then(|v| v.as_u64()) {
                    prom_gauge(
                        &mut buf,
                        "anthropic_cluster_budget_used",
                        &[("client", client)],
                        used as f64,
                    );
                }
            }
        }
    }

    // Shadow log dropped entries counter (always exported for baseline visibility)
    let dropped = state.shadow_log_dropped.load(Ordering::Relaxed);
    prom_header(
        &mut buf,
        "anthropic_shadow_log_dropped_total",
        "counter",
        "Shadow log entries dropped due to channel backpressure",
    );
    prom_gauge(
        &mut buf,
        "anthropic_shadow_log_dropped_total",
        &[],
        dropped as f64,
    );

    // Upstream transport send-failures by kind (timeout/connect/other) — a flaky
    // egress shows here before it cascades into client-visible errors.
    prom_header(
        &mut buf,
        "anthropic_upstream_transport_errors_total",
        "counter",
        "Upstream transport send-failures by kind",
    );
    // Prefer the Redis fleet-wide aggregate (cached every 5s by the sync task)
    // so multi-replica deployments report a cluster-wide count; fall back to the
    // local accumulator when Redis is absent or the aggregate is unavailable
    // (single-instance, pre-first-sync, or a Redis blip).
    let transport_errors: Vec<(String, u64)> = cluster_info
        .as_ref()
        .and_then(|ci| ci.get("transport_errors"))
        .and_then(|v| v.as_object())
        .map(|obj| {
            obj.iter()
                .filter_map(|(k, v)| v.as_u64().map(|n| (k.clone(), n)))
                .collect()
        })
        .unwrap_or_else(|| {
            state
                .lock_transport_errors()
                .iter()
                .map(|(k, v)| (k.to_string(), *v))
                .collect()
        });
    for (kind, n) in transport_errors {
        prom_counter(
            &mut buf,
            "anthropic_upstream_transport_errors_total",
            &[("kind", kind.as_str())],
            n,
        );
    }

    // In-flight request-body memory admission (P1-01): current reserved bytes,
    // the configured ceiling, and the load-shed counter — together these let an
    // operator size `max_inflight_body_mb` from observed peak rather than guess.
    prom_header(
        &mut buf,
        "anthropic_inflight_body_bytes",
        "gauge",
        "Current sum of reserved in-flight request-body bytes",
    );
    prom_gauge(
        &mut buf,
        "anthropic_inflight_body_bytes",
        &[],
        state.inflight_body_bytes.load(Ordering::Relaxed) as f64,
    );
    prom_header(
        &mut buf,
        "anthropic_inflight_body_limit_bytes",
        "gauge",
        "Configured in-flight request-body memory budget in bytes (0 = disabled)",
    );
    prom_gauge(
        &mut buf,
        "anthropic_inflight_body_limit_bytes",
        &[],
        state.max_inflight_body_bytes as f64,
    );
    prom_header(
        &mut buf,
        "anthropic_body_shed_total",
        "counter",
        "Requests load-shed because the in-flight body-memory budget was exhausted",
    );
    prom_counter(
        &mut buf,
        "anthropic_body_shed_total",
        &[],
        state.body_shed_total.load(Ordering::Relaxed),
    );
    prom_header(
        &mut buf,
        "anthropic_body_read_timeout_total",
        "counter",
        "Requests shed with 408 because the body was not received within body_read_timeout_secs",
    );
    prom_counter(
        &mut buf,
        "anthropic_body_read_timeout_total",
        &[],
        state.body_read_timeout_total.load(Ordering::Relaxed),
    );

    (
        StatusCode::OK,
        [(
            axum::http::header::CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8",
        )],
        buf,
    )
        .into_response()
}

// ── OpenAI compatibility ─────────────────────────────────────────────

/// Strip markdown JSON fences from LLM output.
/// Claude sometimes wraps JSON in ```json ... ``` even when told not to.
/// Clients using response_format: json_object (e.g. Vercel AI SDK's generateObject)
/// need raw JSON or their parse step blows up.
fn strip_json_fences(s: &str) -> String {
    let trimmed = s.trim();
    if let Some(rest) = trimmed.strip_prefix("```") {
        // Skip language tag on first line (e.g. "json\n")
        let after_tag = match rest.find('\n') {
            Some(pos) => &rest[pos + 1..],
            None => return s.to_string(),
        };
        // Strip closing fence
        if let Some(content) = after_tag.strip_suffix("```") {
            return content.trim().to_string();
        }
    }
    s.to_string()
}

fn map_stop_reason(reason: &str) -> &'static str {
    match reason {
        "end_turn" => "stop",
        "max_tokens" => "length",
        "stop_sequence" => "stop",
        "tool_use" => "tool_calls",
        _ => "stop",
    }
}

struct StreamContext {
    id: String,
    model: String,
    created: u64,
    tool_call_index: i64,
    in_tool_use: bool,
    current_tool_id: String,
}

impl Default for StreamContext {
    fn default() -> Self {
        Self {
            id: format!("chatcmpl-{}", AppState::now_epoch()),
            model: String::new(),
            created: AppState::now_epoch(),
            tool_call_index: -1,
            in_tool_use: false,
            current_tool_id: String::new(),
        }
    }
}

fn make_openai_chunk(
    ctx: &StreamContext,
    delta: serde_json::Value,
    finish_reason: Option<&str>,
) -> String {
    let chunk = serde_json::json!({
        "id": ctx.id,
        "object": "chat.completion.chunk",
        "created": ctx.created,
        "model": ctx.model,
        "choices": [{
            "index": 0,
            "delta": delta,
            "finish_reason": finish_reason,
        }],
    });
    format!("data: {}\n\n", chunk)
}

/// OpenAI `image_url` part → Anthropic `image` block. `data:` URLs unpack into a
/// base64 source; anything else (including malformed data URLs) becomes a url
/// source so upstream rejects it with a real error instead of us dropping it.
fn openai_image_part_to_anthropic(url: &str) -> serde_json::Value {
    if let Some(rest) = url.strip_prefix("data:") {
        if let Some((media_type, data)) = rest.split_once(";base64,") {
            return serde_json::json!({
                "type": "image",
                "source": {"type": "base64", "media_type": media_type, "data": data},
            });
        }
    }
    serde_json::json!({
        "type": "image",
        "source": {"type": "url", "url": url},
    })
}

fn translate_openai_to_anthropic(body: &serde_json::Value) -> serde_json::Value {
    let mut out = serde_json::Map::new();

    // Model
    if let Some(model) = body.get("model") {
        out.insert("model".to_string(), model.clone());
    }

    // Extract system messages, pass through the rest
    let mut system_parts: Vec<String> = Vec::new();
    let mut messages: Vec<serde_json::Value> = Vec::new();

    if let Some(msgs) = body.get("messages").and_then(|m| m.as_array()) {
        for msg in msgs {
            let role = msg.get("role").and_then(|r| r.as_str()).unwrap_or("");
            if role == "system" {
                // content can be a string or an array of text parts
                let content_val = msg.get("content");
                let content_str = content_val
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string())
                    .or_else(|| {
                        content_val?.as_array().map(|parts| {
                            parts
                                .iter()
                                .filter_map(|p| p.get("text").and_then(|t| t.as_str()))
                                .collect::<Vec<_>>()
                                .join("")
                        })
                    });
                if let Some(s) = content_str {
                    system_parts.push(s);
                }
            } else if role == "tool" {
                // OpenAI tool result → Anthropic user message with tool_result block
                // Merge consecutive tool results into a single user message (Anthropic
                // rejects consecutive messages with the same role)
                let tool_call_id = msg
                    .get("tool_call_id")
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                // content can be a string or an array of content parts
                let content_val = msg.get("content");
                let content_str = content_val
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string())
                    .or_else(|| {
                        content_val?.as_array().map(|parts| {
                            parts
                                .iter()
                                .filter_map(|p| p.get("text").and_then(|t| t.as_str()))
                                .collect::<Vec<_>>()
                                .join("")
                        })
                    })
                    .unwrap_or_default();
                let block = serde_json::json!({
                    "type": "tool_result",
                    "tool_use_id": tool_call_id,
                    "content": content_str,
                });

                // If last message is a user message with tool_result blocks, append.
                // Only merge into arrays that already contain tool_result blocks —
                // don't corrupt a regular user message that happens to have array content.
                let merged = messages.last_mut().and_then(|last| {
                    if last.get("role")?.as_str()? == "user" {
                        let arr = last.get_mut("content")?.as_array_mut()?;
                        let has_tool_result = arr.iter().any(|el| {
                            el.get("type").and_then(|t| t.as_str()) == Some("tool_result")
                        });
                        if has_tool_result {
                            Some(arr)
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                });
                if let Some(blocks) = merged {
                    blocks.push(block);
                } else {
                    messages.push(serde_json::json!({
                        "role": "user",
                        "content": [block],
                    }));
                }
            } else if role == "assistant" && msg.get("tool_calls").is_some() {
                // Assistant message with tool_calls → Anthropic content blocks
                let mut blocks: Vec<serde_json::Value> = Vec::new();
                // Preserve any text content (string or array form)
                let text_content = msg.get("content");
                let preamble = text_content
                    .and_then(|c| c.as_str())
                    .map(|s| s.to_string())
                    .or_else(|| {
                        text_content?.as_array().map(|parts| {
                            parts
                                .iter()
                                .filter_map(|p| p.get("text").and_then(|t| t.as_str()))
                                .collect::<Vec<_>>()
                                .join("")
                        })
                    })
                    .unwrap_or_default();
                if !preamble.is_empty() {
                    blocks.push(serde_json::json!({"type": "text", "text": preamble}));
                }
                if let Some(tool_calls) = msg.get("tool_calls").and_then(|t| t.as_array()) {
                    for tc in tool_calls {
                        let id = tc.get("id").and_then(|v| v.as_str()).unwrap_or("");
                        let name = tc
                            .pointer("/function/name")
                            .and_then(|v| v.as_str())
                            .unwrap_or("");
                        let args_str = tc
                            .pointer("/function/arguments")
                            .and_then(|v| v.as_str())
                            .unwrap_or("{}");
                        let input: serde_json::Value =
                            serde_json::from_str(args_str).unwrap_or(serde_json::json!({}));
                        blocks.push(serde_json::json!({
                            "type": "tool_use",
                            "id": id,
                            "name": name,
                            "input": input,
                        }));
                    }
                }
                messages.push(serde_json::json!({"role": "assistant", "content": blocks}));
            } else {
                // Standard message — strip "name" field, keep role + content.
                // Array content: translate OpenAI image_url parts to Anthropic
                // image blocks (untranslated they 400 the whole request upstream).
                let mut clean = serde_json::Map::new();
                clean.insert(
                    "role".to_string(),
                    serde_json::Value::String(role.to_string()),
                );
                if let Some(content) = msg.get("content") {
                    let translated = if let Some(parts) = content.as_array() {
                        serde_json::Value::Array(
                            parts
                                .iter()
                                .map(|p| {
                                    if p.get("type").and_then(|t| t.as_str()) == Some("image_url") {
                                        let url = p
                                            .pointer("/image_url/url")
                                            .and_then(|u| u.as_str())
                                            .unwrap_or("");
                                        openai_image_part_to_anthropic(url)
                                    } else {
                                        p.clone()
                                    }
                                })
                                .collect(),
                        )
                    } else {
                        content.clone()
                    };
                    clean.insert("content".to_string(), translated);
                }
                messages.push(serde_json::Value::Object(clean));
            }
        }
    }

    // response_format: inject JSON mode instruction into system prompt
    if let Some(rf) = body.get("response_format") {
        if rf.get("type").and_then(|t| t.as_str()) == Some("json_object") {
            system_parts.push(
                "You must respond with valid JSON only. No markdown, no code fences, no explanation — just raw JSON.".to_string(),
            );
        }
    }

    if !system_parts.is_empty() {
        out.insert(
            "system".to_string(),
            serde_json::Value::String(system_parts.join("\n\n")),
        );
    }

    out.insert("messages".to_string(), serde_json::Value::Array(messages));

    // max_tokens: try max_tokens, then max_completion_tokens, default 4096
    let max_tokens = body
        .get("max_tokens")
        .or_else(|| body.get("max_completion_tokens"))
        .cloned()
        .unwrap_or(serde_json::json!(4096));
    out.insert("max_tokens".to_string(), max_tokens);

    // Direct passthrough params
    for key in &["temperature", "top_p", "top_k", "stream"] {
        if let Some(v) = body.get(*key) {
            out.insert(key.to_string(), v.clone());
        }
    }

    // stop -> stop_sequences
    if let Some(stop) = body.get("stop") {
        let sequences = if stop.is_array() {
            stop.clone()
        } else if let Some(s) = stop.as_str() {
            serde_json::json!([s])
        } else {
            serde_json::json!([])
        };
        out.insert("stop_sequences".to_string(), sequences);
    }

    // tools: OpenAI function definitions → Anthropic tool format
    if let Some(tools) = body.get("tools").and_then(|t| t.as_array()) {
        let anthropic_tools: Vec<serde_json::Value> = tools
            .iter()
            .filter_map(|tool| {
                let func = tool.get("function")?;
                let name = func.get("name")?.as_str()?;
                let mut t = serde_json::json!({"name": name});
                if let Some(desc) = func.get("description") {
                    t["description"] = desc.clone();
                }
                t["input_schema"] = func
                    .get("parameters")
                    .filter(|v| !v.is_null())
                    .cloned()
                    .unwrap_or_else(|| serde_json::json!({"type": "object", "properties": {}}));
                Some(t)
            })
            .collect();
        if !anthropic_tools.is_empty() {
            out.insert(
                "tools".to_string(),
                serde_json::Value::Array(anthropic_tools),
            );
        }
    }

    // tool_choice translation
    if let Some(tc) = body.get("tool_choice") {
        let anthropic_tc = if let Some(s) = tc.as_str() {
            match s {
                "auto" => Some(serde_json::json!({"type": "auto"})),
                "none" => {
                    out.remove("tools");
                    None
                }
                "required" => Some(serde_json::json!({"type": "any"})),
                _ => None,
            }
        } else {
            tc.pointer("/function/name")
                .and_then(|n| n.as_str())
                .map(|name| serde_json::json!({"type": "tool", "name": name}))
        };
        if let Some(atc) = anthropic_tc {
            out.insert("tool_choice".to_string(), atc);
        }
    }

    serde_json::Value::Object(out)
}

fn translate_anthropic_to_openai(body: &serde_json::Value) -> serde_json::Value {
    let id = body
        .get("id")
        .and_then(|v| v.as_str())
        .unwrap_or("msg_unknown");
    let model = body
        .get("model")
        .and_then(|v| v.as_str())
        .unwrap_or("unknown");

    let blocks = body.get("content").and_then(|c| c.as_array());

    // Concatenate text content blocks, strip markdown JSON fences
    let content = blocks
        .map(|blocks| {
            let raw = blocks
                .iter()
                .filter(|b| b.get("type").and_then(|t| t.as_str()) == Some("text"))
                .filter_map(|b| b.get("text").and_then(|t| t.as_str()))
                .collect::<Vec<_>>()
                .join("");
            strip_json_fences(&raw)
        })
        .unwrap_or_default();

    // Extract tool_use blocks → OpenAI tool_calls
    let tool_calls: Vec<serde_json::Value> = blocks
        .map(|blocks| {
            blocks
                .iter()
                .filter(|b| b.get("type").and_then(|t| t.as_str()) == Some("tool_use"))
                .map(|b| {
                    let tc_id = b.get("id").and_then(|v| v.as_str()).unwrap_or("");
                    let name = b.get("name").and_then(|v| v.as_str()).unwrap_or("");
                    let input = b.get("input").cloned().unwrap_or(serde_json::json!({}));
                    serde_json::json!({
                        "id": tc_id,
                        "type": "function",
                        "function": {
                            "name": name,
                            "arguments": input.to_string(),
                        }
                    })
                })
                .collect()
        })
        .unwrap_or_default();

    let stop_reason = body
        .get("stop_reason")
        .and_then(|v| v.as_str())
        .unwrap_or("end_turn");

    let input_tokens = body
        .pointer("/usage/input_tokens")
        .and_then(|v| v.as_u64())
        .unwrap_or(0);
    let output_tokens = body
        .pointer("/usage/output_tokens")
        .and_then(|v| v.as_u64())
        .unwrap_or(0);

    // Build message: content is null when only tool_calls present (OpenAI convention)
    let mut message = serde_json::json!({"role": "assistant"});
    if !tool_calls.is_empty() {
        message["tool_calls"] = serde_json::Value::Array(tool_calls);
        if content.is_empty() {
            message["content"] = serde_json::Value::Null;
        } else {
            message["content"] = serde_json::Value::String(content);
        }
    } else {
        message["content"] = serde_json::Value::String(content);
    }

    serde_json::json!({
        "id": format!("chatcmpl-{}", id),
        "object": "chat.completion",
        "created": AppState::now_epoch(),
        "model": model,
        "choices": [{
            "index": 0,
            "message": message,
            "finish_reason": map_stop_reason(stop_reason),
        }],
        "usage": {
            "prompt_tokens": input_tokens,
            "completion_tokens": output_tokens,
            "total_tokens": input_tokens + output_tokens,
        },
    })
}

/// Parse a raw SSE event block and translate to OpenAI format.
/// Returns None for events that should be skipped (ping, text content_block_start, etc.).
fn translate_sse_event(raw: &str, ctx: &mut StreamContext) -> Option<String> {
    let mut event_type = String::new();
    let mut data = String::new();

    for line in raw.lines() {
        if let Some(val) = line.strip_prefix("event:") {
            event_type = val.trim().to_string();
        } else if let Some(val) = line.strip_prefix("data:") {
            data = val.trim().to_string();
        }
    }

    if data.is_empty() {
        return None;
    }

    let parsed: serde_json::Value = serde_json::from_str(&data).ok()?;

    match event_type.as_str() {
        "message_start" => {
            if let Some(msg) = parsed.get("message") {
                if let Some(id) = msg.get("id").and_then(|v| v.as_str()) {
                    ctx.id = format!("chatcmpl-{}", id);
                }
                if let Some(model) = msg.get("model").and_then(|v| v.as_str()) {
                    ctx.model = model.to_string();
                }
            }
            Some(make_openai_chunk(
                ctx,
                serde_json::json!({"role": "assistant"}),
                None,
            ))
        }
        "content_block_start" => {
            let block = parsed.get("content_block")?;
            let block_type = block.get("type").and_then(|t| t.as_str()).unwrap_or("");
            if block_type == "tool_use" {
                ctx.in_tool_use = true;
                ctx.tool_call_index += 1;
                ctx.current_tool_id = block
                    .get("id")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                let name = block.get("name").and_then(|v| v.as_str()).unwrap_or("");
                Some(make_openai_chunk(
                    ctx,
                    serde_json::json!({
                        "tool_calls": [{
                            "index": ctx.tool_call_index,
                            "id": ctx.current_tool_id,
                            "type": "function",
                            "function": {"name": name, "arguments": ""}
                        }]
                    }),
                    None,
                ))
            } else {
                ctx.in_tool_use = false;
                None
            }
        }
        "content_block_delta" => {
            let delta_type = parsed
                .pointer("/delta/type")
                .and_then(|v| v.as_str())
                .unwrap_or("");

            if ctx.in_tool_use && delta_type == "input_json_delta" {
                let partial = parsed
                    .pointer("/delta/partial_json")
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                if partial.is_empty() {
                    return None;
                }
                Some(make_openai_chunk(
                    ctx,
                    serde_json::json!({
                        "tool_calls": [{
                            "index": ctx.tool_call_index,
                            "function": {"arguments": partial}
                        }]
                    }),
                    None,
                ))
            } else {
                let text = parsed
                    .pointer("/delta/text")
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                if text.is_empty() {
                    return None;
                }
                Some(make_openai_chunk(
                    ctx,
                    serde_json::json!({"content": text}),
                    None,
                ))
            }
        }
        "content_block_stop" => {
            if ctx.in_tool_use {
                ctx.in_tool_use = false;
            }
            None
        }
        "message_delta" => {
            let stop_reason = parsed
                .pointer("/delta/stop_reason")
                .and_then(|v| v.as_str())
                .unwrap_or("end_turn");
            Some(make_openai_chunk(
                ctx,
                serde_json::json!({}),
                Some(map_stop_reason(stop_reason)),
            ))
        }
        "message_stop" => Some("data: [DONE]\n\n".to_string()),
        _ => None, // ping
    }
}

// ── Reverse translation: Anthropic → OpenAI (for upstream fallback) ──

/// Map OpenAI finish_reason back to Anthropic stop_reason.
fn reverse_map_stop_reason(reason: &str) -> &'static str {
    match reason {
        "stop" => "end_turn",
        "length" => "max_tokens",
        "tool_calls" => "tool_use",
        _ => "end_turn",
    }
}

/// Translate an Anthropic Messages API request body to OpenAI Chat Completions format.
/// Reverse of `translate_openai_to_anthropic`.
/// Anthropic `image` block → OpenAI `image_url` part. Base64 sources repack as a
/// `data:` URL. None only for structurally invalid blocks (missing source fields)
/// that Anthropic itself would have rejected.
fn anthropic_image_block_to_openai(block: &serde_json::Value) -> Option<serde_json::Value> {
    let source = block.get("source")?;
    let url = match source.get("type").and_then(|t| t.as_str())? {
        "url" => source.get("url")?.as_str()?.to_string(),
        "base64" => {
            let media_type = source.get("media_type").and_then(|m| m.as_str())?;
            let data = source.get("data").and_then(|d| d.as_str())?;
            format!("data:{};base64,{}", media_type, data)
        }
        _ => return None,
    };
    Some(serde_json::json!({"type": "image_url", "image_url": {"url": url}}))
}

/// Anthropic user content blocks → OpenAI content: a plain string when text-only
/// (the common case, and what OpenAI-compat upstreams handle most reliably), a
/// content-part array when image blocks are present so images survive translation.
fn anthropic_user_blocks_to_openai_content(blocks: &[&serde_json::Value]) -> serde_json::Value {
    let mut parts: Vec<serde_json::Value> = Vec::new();
    let mut has_image = false;
    for b in blocks {
        match b.get("type").and_then(|t| t.as_str()) {
            Some("text") => {
                if let Some(t) = b.get("text").and_then(|t| t.as_str()) {
                    parts.push(serde_json::json!({"type": "text", "text": t}));
                }
            }
            Some("image") => {
                if let Some(p) = anthropic_image_block_to_openai(b) {
                    parts.push(p);
                    has_image = true;
                }
            }
            _ => {}
        }
    }
    if has_image {
        serde_json::Value::Array(parts)
    } else {
        let text: String = parts
            .iter()
            .filter_map(|p| p.get("text").and_then(|t| t.as_str()))
            .collect::<Vec<_>>()
            .join("");
        serde_json::Value::String(text)
    }
}

fn translate_anthropic_request_to_openai(body: &serde_json::Value) -> serde_json::Value {
    let mut out = serde_json::Map::new();

    if let Some(model) = body.get("model") {
        out.insert("model".to_string(), model.clone());
    }

    let mut messages: Vec<serde_json::Value> = Vec::new();

    // System prompt → system role message(s)
    if let Some(system) = body.get("system") {
        if let Some(s) = system.as_str() {
            if !s.is_empty() {
                messages.push(serde_json::json!({"role": "system", "content": s}));
            }
        } else if let Some(arr) = system.as_array() {
            // Array of system blocks → concatenate text
            let text: String = arr
                .iter()
                .filter_map(|b| b.get("text").and_then(|t| t.as_str()))
                .collect::<Vec<_>>()
                .join("\n\n");
            if !text.is_empty() {
                messages.push(serde_json::json!({"role": "system", "content": text}));
            }
        }
    }

    // Messages
    if let Some(msgs) = body.get("messages").and_then(|m| m.as_array()) {
        for msg in msgs {
            let role = msg.get("role").and_then(|r| r.as_str()).unwrap_or("");
            let content = msg.get("content");

            if role == "assistant" {
                // Check for tool_use blocks in content array
                if let Some(blocks) = content.and_then(|c| c.as_array()) {
                    let has_tool_use = blocks
                        .iter()
                        .any(|b| b.get("type").and_then(|t| t.as_str()) == Some("tool_use"));

                    if has_tool_use {
                        // Extract text blocks as content, tool_use blocks as tool_calls
                        let text: String = blocks
                            .iter()
                            .filter(|b| b.get("type").and_then(|t| t.as_str()) == Some("text"))
                            .filter_map(|b| b.get("text").and_then(|t| t.as_str()))
                            .collect::<Vec<_>>()
                            .join("");

                        let tool_calls: Vec<serde_json::Value> = blocks
                            .iter()
                            .filter(|b| b.get("type").and_then(|t| t.as_str()) == Some("tool_use"))
                            .map(|b| {
                                let id = b.get("id").and_then(|v| v.as_str()).unwrap_or("");
                                let name = b.get("name").and_then(|v| v.as_str()).unwrap_or("");
                                let input =
                                    b.get("input").cloned().unwrap_or(serde_json::json!({}));
                                serde_json::json!({
                                    "id": id,
                                    "type": "function",
                                    "function": {
                                        "name": name,
                                        "arguments": input.to_string(),
                                    }
                                })
                            })
                            .collect();

                        let mut m = serde_json::json!({"role": "assistant"});
                        if text.is_empty() {
                            m["content"] = serde_json::Value::Null;
                        } else {
                            m["content"] = serde_json::Value::String(text);
                        }
                        m["tool_calls"] = serde_json::Value::Array(tool_calls);
                        messages.push(m);
                        continue;
                    }
                }
                // Plain assistant message
                let text = content
                    .and_then(|c| {
                        c.as_str().map(|s| s.to_string()).or_else(|| {
                            c.as_array().map(|arr| {
                                arr.iter()
                                    .filter(|b| {
                                        b.get("type").and_then(|t| t.as_str()) == Some("text")
                                    })
                                    .filter_map(|b| b.get("text").and_then(|t| t.as_str()))
                                    .collect::<Vec<_>>()
                                    .join("")
                            })
                        })
                    })
                    .unwrap_or_default();
                messages.push(serde_json::json!({"role": "assistant", "content": text}));
            } else if role == "user" {
                // Check for tool_result blocks
                if let Some(blocks) = content.and_then(|c| c.as_array()) {
                    let tool_results: Vec<&serde_json::Value> = blocks
                        .iter()
                        .filter(|b| b.get("type").and_then(|t| t.as_str()) == Some("tool_result"))
                        .collect();

                    if !tool_results.is_empty() {
                        // Each tool_result → separate OpenAI tool message
                        for tr in &tool_results {
                            let tool_call_id =
                                tr.get("tool_use_id").and_then(|v| v.as_str()).unwrap_or("");
                            let content_val = tr.get("content");
                            let content_str = content_val
                                .and_then(|c| c.as_str())
                                .map(|s| s.to_string())
                                .or_else(|| {
                                    content_val?.as_array().map(|parts| {
                                        parts
                                            .iter()
                                            .filter_map(|p| p.get("text").and_then(|t| t.as_str()))
                                            .collect::<Vec<_>>()
                                            .join("")
                                    })
                                })
                                .unwrap_or_default();
                            messages.push(serde_json::json!({
                                "role": "tool",
                                "tool_call_id": tool_call_id,
                                "content": content_str,
                            }));
                        }

                        // Also emit any non-tool_result blocks (text + images) as a user message
                        let leftover: Vec<&serde_json::Value> = blocks
                            .iter()
                            .filter(|b| {
                                b.get("type").and_then(|t| t.as_str()) != Some("tool_result")
                            })
                            .collect();
                        let user_content = anthropic_user_blocks_to_openai_content(&leftover);
                        let non_empty = match &user_content {
                            serde_json::Value::String(s) => !s.is_empty(),
                            serde_json::Value::Array(a) => !a.is_empty(),
                            _ => false,
                        };
                        if non_empty {
                            messages.push(serde_json::json!({
                                "role": "user",
                                "content": user_content,
                            }));
                        }
                        continue;
                    }
                }
                // Plain user message — string passes through; block arrays keep
                // text AND images (images previously filtered out silently)
                if let Some(c) = content {
                    if let Some(s) = c.as_str() {
                        messages.push(serde_json::json!({"role": "user", "content": s}));
                    } else if let Some(arr) = c.as_array() {
                        let block_refs: Vec<&serde_json::Value> = arr.iter().collect();
                        let user_content = anthropic_user_blocks_to_openai_content(&block_refs);
                        messages.push(serde_json::json!({"role": "user", "content": user_content}));
                    }
                }
            }
        }
    }

    out.insert("messages".to_string(), serde_json::Value::Array(messages));

    // max_tokens → max_tokens
    if let Some(mt) = body.get("max_tokens") {
        out.insert("max_tokens".to_string(), mt.clone());
    }

    // Passthrough params
    for key in &["temperature", "top_p", "stream"] {
        if let Some(v) = body.get(*key) {
            out.insert(key.to_string(), v.clone());
        }
    }

    // stop_sequences → stop
    if let Some(ss) = body.get("stop_sequences") {
        out.insert("stop".to_string(), ss.clone());
    }

    // tools: Anthropic → OpenAI function format
    if let Some(tools) = body.get("tools").and_then(|t| t.as_array()) {
        let openai_tools: Vec<serde_json::Value> = tools
            .iter()
            .filter_map(|tool| {
                let name = tool.get("name")?.as_str()?;
                let mut func = serde_json::json!({"name": name});
                if let Some(desc) = tool.get("description") {
                    func["description"] = desc.clone();
                }
                if let Some(schema) = tool.get("input_schema") {
                    func["parameters"] = schema.clone();
                }
                Some(serde_json::json!({"type": "function", "function": func}))
            })
            .collect();
        if !openai_tools.is_empty() {
            out.insert("tools".to_string(), serde_json::Value::Array(openai_tools));
        }
    }

    // tool_choice translation
    if let Some(tc) = body.get("tool_choice") {
        let tc_type = tc.get("type").and_then(|t| t.as_str()).unwrap_or("");
        let openai_tc = match tc_type {
            "auto" => Some(serde_json::json!("auto")),
            "any" => Some(serde_json::json!("required")),
            "tool" => tc
                .get("name")
                .and_then(|n| n.as_str())
                .map(|name| serde_json::json!({"type": "function", "function": {"name": name}})),
            _ => None,
        };
        if let Some(otc) = openai_tc {
            out.insert("tool_choice".to_string(), otc);
        }
    }

    serde_json::Value::Object(out)
}

/// Translate an OpenAI Chat Completions response to Anthropic Messages API format.
/// Reverse of `translate_anthropic_to_openai`.
fn translate_openai_response_to_anthropic(body: &serde_json::Value) -> serde_json::Value {
    let id = body
        .get("id")
        .and_then(|v| v.as_str())
        .unwrap_or("chatcmpl-unknown")
        .strip_prefix("chatcmpl-")
        .unwrap_or("msg_unknown");
    let model = body
        .get("model")
        .and_then(|v| v.as_str())
        .unwrap_or("unknown");

    let choice = body
        .pointer("/choices/0")
        .cloned()
        .unwrap_or(serde_json::json!({}));
    let message = choice
        .get("message")
        .cloned()
        .unwrap_or(serde_json::json!({}));

    let mut content_blocks: Vec<serde_json::Value> = Vec::new();

    // Text content
    if let Some(text) = message.get("content").and_then(|c| c.as_str()) {
        if !text.is_empty() {
            content_blocks.push(serde_json::json!({"type": "text", "text": text}));
        }
    }

    // Tool calls → tool_use blocks
    if let Some(tool_calls) = message.get("tool_calls").and_then(|t| t.as_array()) {
        for tc in tool_calls {
            let tc_id = tc.get("id").and_then(|v| v.as_str()).unwrap_or("");
            let name = tc
                .pointer("/function/name")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            let args_str = tc
                .pointer("/function/arguments")
                .and_then(|v| v.as_str())
                .unwrap_or("{}");
            let input: serde_json::Value =
                serde_json::from_str(args_str).unwrap_or(serde_json::json!({}));
            content_blocks.push(serde_json::json!({
                "type": "tool_use",
                "id": tc_id,
                "name": name,
                "input": input,
            }));
        }
    }

    if content_blocks.is_empty() {
        content_blocks.push(serde_json::json!({"type": "text", "text": ""}));
    }

    let finish_reason = choice
        .get("finish_reason")
        .and_then(|v| v.as_str())
        .unwrap_or("stop");

    let input_tokens = body
        .pointer("/usage/prompt_tokens")
        .and_then(|v| v.as_u64())
        .unwrap_or(0);
    let output_tokens = body
        .pointer("/usage/completion_tokens")
        .and_then(|v| v.as_u64())
        .unwrap_or(0);

    serde_json::json!({
        "id": format!("msg_{}", id),
        "type": "message",
        "role": "assistant",
        "model": model,
        "content": content_blocks,
        "stop_reason": reverse_map_stop_reason(finish_reason),
        "stop_sequence": null,
        "usage": {
            "input_tokens": input_tokens,
            "output_tokens": output_tokens,
        },
    })
}

/// State tracker for OpenAI SSE → Anthropic SSE translation.
struct ReverseStreamContext {
    id: String,
    model: String,
    message_started: bool,
    block_index: i64,
    in_text_block: bool,
    in_tool_use: bool,
}

impl Default for ReverseStreamContext {
    fn default() -> Self {
        Self {
            id: format!("msg_{}", AppState::now_epoch()),
            model: String::new(),
            message_started: false,
            block_index: -1,
            in_text_block: false,
            in_tool_use: false,
        }
    }
}

/// Format an Anthropic SSE event.
fn make_anthropic_event(event_type: &str, data: &serde_json::Value) -> String {
    format!("event: {}\ndata: {}\n\n", event_type, data)
}

/// Final-frame Anthropic SSE error event for downstream when an upstream
/// stream dies mid-flight. Without this the client sees a bare TCP FIN
/// ("socket closed unexpectedly"). Emits a single `event: error` frame; the
/// Anthropic SSE protocol has no terminator analogous to OpenAI's `[DONE]`,
/// so the channel may close naturally after this frame.
fn anthropic_error_frame(message: &str) -> bytes::Bytes {
    // `error.type` must be one of Anthropic's documented values
    // (overloaded_error, api_error, invalid_request_error, ...). Using a
    // custom type like "upstream_error" risks the SDK rejecting the frame
    // and the client falling back to "socket closed unexpectedly" — the
    // exact failure mode this helper exists to prevent. `api_error` is the
    // documented catch-all; the descriptive detail lives in `message`.
    let body = serde_json::json!({
        "type": "error",
        "error": { "type": "api_error", "message": message }
    });
    bytes::Bytes::from(make_anthropic_event("error", &body))
}

/// Final-frame OpenAI SSE error for downstream. Emits the error JSON
/// followed by `data: [DONE]` (OpenAI's stream terminator) — callers MUST
/// NOT emit an additional `[DONE]` after this frame.
fn openai_error_frame(message: &str) -> bytes::Bytes {
    let err = serde_json::json!({
        "error": { "message": message, "type": "upstream_error" }
    });
    bytes::Bytes::from(format!("data: {err}\n\ndata: [DONE]\n\n"))
}

/// Translate an OpenAI SSE chunk to Anthropic SSE events.
/// Returns Vec because one OpenAI chunk may produce multiple Anthropic events.
/// `raw` is the raw SSE data line (after stripping "data: " prefix).
fn translate_openai_sse_to_anthropic(raw: &str, ctx: &mut ReverseStreamContext) -> Vec<String> {
    let trimmed = raw.trim();
    if trimmed == "[DONE]" {
        // Only emit message_stop if we started a message
        if ctx.message_started {
            return vec![make_anthropic_event(
                "message_stop",
                &serde_json::json!({"type": "message_stop"}),
            )];
        }
        return vec![];
    }

    let parsed: serde_json::Value = match serde_json::from_str(trimmed) {
        Ok(v) => v,
        Err(_) => return vec![],
    };

    let mut events: Vec<String> = Vec::new();

    // Capture model from first chunk
    if let Some(model) = parsed.get("model").and_then(|v| v.as_str()) {
        if ctx.model.is_empty() {
            ctx.model = model.to_string();
        }
    }
    if let Some(id) = parsed.get("id").and_then(|v| v.as_str()) {
        if let Some(stripped) = id.strip_prefix("chatcmpl-") {
            ctx.id = format!("msg_{}", stripped);
        }
    }

    let delta = match parsed.pointer("/choices/0/delta") {
        Some(d) => d,
        None => return vec![],
    };
    let finish_reason = parsed
        .pointer("/choices/0/finish_reason")
        .and_then(|v| v.as_str());

    // Emit message_start on first meaningful chunk
    if !ctx.message_started && (delta.get("role").is_some() || delta.get("content").is_some()) {
        ctx.message_started = true;
        events.push(make_anthropic_event(
            "message_start",
            &serde_json::json!({
                "type": "message_start",
                "message": {
                    "id": ctx.id,
                    "type": "message",
                    "role": "assistant",
                    "model": ctx.model,
                    "content": [],
                    "stop_reason": null,
                    "usage": {"input_tokens": 0, "output_tokens": 0}
                }
            }),
        ));
    }

    // Text content delta
    if let Some(text) = delta.get("content").and_then(|c| c.as_str()) {
        if !text.is_empty() {
            if !ctx.in_text_block {
                // Close tool block if open
                if ctx.in_tool_use {
                    events.push(make_anthropic_event(
                        "content_block_stop",
                        &serde_json::json!({"type": "content_block_stop", "index": ctx.block_index}),
                    ));
                    ctx.in_tool_use = false;
                }
                ctx.block_index += 1;
                ctx.in_text_block = true;
                events.push(make_anthropic_event(
                    "content_block_start",
                    &serde_json::json!({
                        "type": "content_block_start",
                        "index": ctx.block_index,
                        "content_block": {"type": "text", "text": ""}
                    }),
                ));
            }
            events.push(make_anthropic_event(
                "content_block_delta",
                &serde_json::json!({
                    "type": "content_block_delta",
                    "index": ctx.block_index,
                    "delta": {"type": "text_delta", "text": text}
                }),
            ));
        }
    }

    // Tool calls
    if let Some(tool_calls) = delta.get("tool_calls").and_then(|t| t.as_array()) {
        for tc in tool_calls {
            let tool_name = tc.pointer("/function/name").and_then(|n| n.as_str());
            let args = tc
                .pointer("/function/arguments")
                .and_then(|a| a.as_str())
                .unwrap_or("");

            if let Some(name) = tool_name {
                // New tool call — close previous block if any
                if ctx.in_text_block {
                    events.push(make_anthropic_event(
                        "content_block_stop",
                        &serde_json::json!({"type": "content_block_stop", "index": ctx.block_index}),
                    ));
                    ctx.in_text_block = false;
                }
                if ctx.in_tool_use {
                    events.push(make_anthropic_event(
                        "content_block_stop",
                        &serde_json::json!({"type": "content_block_stop", "index": ctx.block_index}),
                    ));
                }

                if !ctx.message_started {
                    ctx.message_started = true;
                    events.push(make_anthropic_event(
                        "message_start",
                        &serde_json::json!({
                            "type": "message_start",
                            "message": {
                                "id": ctx.id,
                                "type": "message",
                                "role": "assistant",
                                "model": ctx.model,
                                "content": [],
                                "stop_reason": null,
                                "usage": {"input_tokens": 0, "output_tokens": 0}
                            }
                        }),
                    ));
                }

                ctx.block_index += 1;
                ctx.in_tool_use = true;
                let id = tc.get("id").and_then(|v| v.as_str()).unwrap_or("");
                events.push(make_anthropic_event(
                    "content_block_start",
                    &serde_json::json!({
                        "type": "content_block_start",
                        "index": ctx.block_index,
                        "content_block": {"type": "tool_use", "id": id, "name": name, "input": {}}
                    }),
                ));
            }

            // Tool arguments delta
            if !args.is_empty() && ctx.in_tool_use {
                events.push(make_anthropic_event(
                    "content_block_delta",
                    &serde_json::json!({
                        "type": "content_block_delta",
                        "index": ctx.block_index,
                        "delta": {"type": "input_json_delta", "partial_json": args}
                    }),
                ));
            }
        }
    }

    // Finish reason → close blocks + message_delta + message_stop
    if let Some(reason) = finish_reason {
        if ctx.in_text_block || ctx.in_tool_use {
            events.push(make_anthropic_event(
                "content_block_stop",
                &serde_json::json!({"type": "content_block_stop", "index": ctx.block_index}),
            ));
            ctx.in_text_block = false;
            ctx.in_tool_use = false;
        }
        events.push(make_anthropic_event(
            "message_delta",
            &serde_json::json!({
                "type": "message_delta",
                "delta": {"stop_reason": reverse_map_stop_reason(reason)},
                "usage": {"output_tokens": 0}
            }),
        ));
        events.push(make_anthropic_event(
            "message_stop",
            &serde_json::json!({"type": "message_stop"}),
        ));
        ctx.message_started = false; // prevent duplicate from [DONE]
    }

    events
}

/// Forward one OpenAI-compat request to a single Anthropic-protocol `Endpoint`.
/// The caller has already translated the OpenAI request into `anthropic_body`
/// (plus the OAuth variant `oauth_anthropic_body`); this helper picks the right
/// variant by token prefix, forwards to the endpoint, and translates the
/// Anthropic response back to OpenAI format. Structurally similar to
/// `forward_anthropic`, but intentionally kept separate: it speaks OpenAI on
/// both edges (request body already translated, response translated back).
/// `ep` is the endpoint to forward to; `endpoint_idx` is its index in
/// `state.endpoints`. Both are required: the streaming path spawns a
/// detached 'static task that must re-borrow the endpoint from a cloned
/// Arc<AppState> — a borrowed &Endpoint cannot cross the spawn boundary,
/// so the task captures the Copy `endpoint_idx` and re-indexes.
#[allow(clippy::too_many_arguments)]
async fn forward_openai_compat_anthropic(
    state: &Arc<AppState>,
    parts: &axum::http::request::Parts,
    ep: &Endpoint,
    endpoint_idx: usize,
    anthropic_body: &serde_json::Value,
    oauth_anthropic_body: &serde_json::Value,
    req_id: &str,
    client_id: &str,
    client_ver: &str,
    client_ip: &std::net::IpAddr,
    agent_id: &str,
    session_id: &str,
    model: &str,
    is_streaming: bool,
    request_start: std::time::Instant,
) -> ForwardOutcome {
    let token = ep.token.as_str();
    let passthrough = ep.passthrough;
    let endpoint_name = ep.name.as_str();
    let rate_info = &ep.rate_info;
    let url = format!("{}/v1/messages", ep.base_url);

    let mut headers = parts.headers.clone();
    headers.remove("host");
    if !passthrough {
        headers.remove("authorization");
        headers.remove("x-api-key");
    }
    headers.remove("content-length"); // body size changes after translation
    headers.remove("accept-encoding"); // we need plaintext to translate the response

    // Inject required Anthropic headers
    headers.insert("content-type", HeaderValue::from_static("application/json"));
    headers.insert("anthropic-version", HeaderValue::from_static("2023-06-01"));

    // Auth injection
    inject_account_auth(&mut headers, token, passthrough);

    // Use OAuth variant (with CC system prompt) for OAuth tokens
    let req_body = if token.starts_with("sk-ant-oat") {
        oauth_anthropic_body
    } else {
        anthropic_body
    };
    let body_str = req_body.to_string();
    debug!(
        account = endpoint_name,
        model = %model,
        body_len = body_str.len(),
        "openai-compat: upstream request"
    );

    // Same non-streaming read_timeout exemption as forward_anthropic (LAB-718).
    let http_client = if is_streaming {
        &state.client
    } else {
        &state.client_nonstreaming
    };
    let upstream_req = http_client
        .request(reqwest::Method::POST, &url)
        .headers(headers)
        .body(body_str);

    let resp = match upstream_req.send().await {
        Ok(r) => r,
        Err(e) => {
            error!(account = endpoint_name, detail = %describe_reqwest_error(&e), "upstream request failed: {e}");
            // Surface the failure on the dashboard by kind before it becomes a
            // client error. `is_timeout`/`is_connect` are the same classifiers
            // `describe_reqwest_error` uses for the log line above.
            let kind = if e.is_timeout() {
                "timeout"
            } else if e.is_connect() {
                "connect"
            } else {
                "other"
            };
            if let Ok(mut m) = state.upstream_transport_errors.lock() {
                *m.entry(kind).or_insert(0) += 1;
            }
            // Feed the per-endpoint circuit breaker: enough consecutive
            // failures and this endpoint leaves the routing pool entirely.
            state.record_transport_failure(endpoint_idx).await;
            // Transport-level send failure (ETIMEDOUT/reset/closed/DNS). Mark it
            // `transient`; rotation policy is round-gated and owned by the retry
            // loop (it knows `retry_round`), so `push_skip` stays false here —
            // round 0 retries the SAME affinity/cache-warm endpoint after a
            // backoff rather than rotating to a cold-cache endpoint on every blip.
            return ForwardOutcome::Retry {
                saw_529: false,
                push_skip: false,
                transient: true,
            };
        }
    };

    let status = resp.status();
    ep.requests.fetch_add(1, Ordering::Relaxed);
    // Any HTTP response (even 429/5xx) proves the transport path is alive —
    // clear the circuit-breaker counter.
    state.record_transport_success(endpoint_idx).await;
    state
        .update_rate_info_for(rate_info, endpoint_name, resp.headers())
        .await;

    // Update burn rate (after rate-limit headers are parsed)
    state.update_burn_rate(&ep.burn_rate, client_id);

    // Classify 429 / 529 / other 5xx into a retry decision (shared helper).
    let mut resp = match classify_retry_status(state, status, rate_info, endpoint_name, resp).await
    {
        Ok(resp) => resp,
        Err(outcome) => return outcome,
    };

    // Clear hard limit and burst counter only on a genuine 2xx success.
    // A 4xx (e.g. invalid_request_error, auth failure) is not evidence
    // that the rate-limit window has drained — don't clobber state on
    // client errors.
    let recovered = if status.is_success() {
        let mut info = rate_info.write().await;
        let was = info.hard_limited_until.is_some();
        info.hard_limited_until = None;
        info.consecutive_burst_429s = 0;
        was
    } else {
        false
    };

    // Per-request persistence removed: it re-serialized the whole endpoint pool
    // and did a blocking write on every successful request (a memory + IO
    // amplifier under load). Persist only on the hard-limit RECOVERY transition
    // here; 429 hard-limit entry still persists immediately, and utilization /
    // request counts persist at probe cadence + shutdown.
    if recovered {
        state.save_state().await;
        state.signal_hard_limit_recovery(endpoint_name).await;
    }

    // Compute budget pressure status for response header + log
    let budget_status = {
        let info = rate_info.read().await;
        let (eff_util, constraint, _adj_5h, _adj_7d) =
            effective_utilization(&info, AppState::now_epoch(), model);
        info!(
            req_id,
            client = %client_ip,
            client_id = %client_id,
            ver = %client_ver,
            agent = %agent_id,
            session = %session_id,
            model = %model,
            account = endpoint_name,
            status = status.as_u16(),
            utilization = format_args!("{eff_util:.2}"),
            util_5h = info.utilization_5h.map(|v| format!("{v:.2}")).as_deref().unwrap_or("-"),
            util_7d = info.utilization_7d.map(|v| format!("{v:.2}")).as_deref().unwrap_or("-"),
            constraint,
            openai_compat = true,
            stream = is_streaming,
            "proxied (openai-compat)"
        );
        compute_pressure_status(eff_util, client_id, state)
    };

    // Non-2xx: log error detail, translate to OpenAI error format, return
    if !status.is_success() {
        let error_body = resp.bytes().await.unwrap_or_default();
        let error_msg = serde_json::from_slice::<serde_json::Value>(&error_body)
            .ok()
            .and_then(|v| {
                v.pointer("/error/message")
                    .and_then(|m| m.as_str())
                    .map(String::from)
            });
        warn!(
            account = endpoint_name,
            model = %model,
            status = status.as_u16(),
            error_message = ?error_msg,
            "openai-compat: upstream error"
        );

        // Translate Anthropic error to OpenAI error format so clients
        // (LiteLLM, etc.) can parse the actual error message.
        let openai_error =
            if let Ok(parsed) = serde_json::from_slice::<serde_json::Value>(&error_body) {
                // Anthropic: {"type":"error","error":{"type":"...","message":"..."}}
                let msg = parsed
                    .pointer("/error/message")
                    .and_then(|v| v.as_str())
                    .unwrap_or("unknown upstream error");
                let err_type = parsed
                    .pointer("/error/type")
                    .and_then(|v| v.as_str())
                    .unwrap_or("api_error");
                serde_json::json!({
                    "error": {
                        "message": msg,
                        "type": err_type,
                        "param": null,
                        "code": null
                    }
                })
            } else {
                let raw = String::from_utf8_lossy(&error_body);
                serde_json::json!({
                    "error": {
                        "message": raw.as_ref(),
                        "type": "api_error",
                        "param": null,
                        "code": null
                    }
                })
            };

        let response = Response::builder()
            .status(StatusCode::from_u16(status.as_u16()).unwrap_or(StatusCode::BAD_GATEWAY))
            .header("content-type", "application/json")
            .header("x-budget-status", budget_status)
            .body(Body::from(
                serde_json::to_vec(&openai_error).unwrap_or_default(),
            ))
            .unwrap_or_else(|_| {
                (StatusCode::INTERNAL_SERVER_ERROR, "response build error").into_response()
            });
        return ForwardOutcome::Done(response);
    }

    if is_streaming {
        let (tx, rx) = tokio::sync::mpsc::channel::<Result<bytes::Bytes, std::io::Error>>(32);
        let state_clone = state.clone();
        // The detached task can't carry the `ep` borrow across the spawn
        // boundary; capture the Copy index and re-borrow from the owned `Arc`.
        let client_id_clone = client_id.to_owned();
        let acct_name = endpoint_name.to_owned();
        let model_clone = model.to_owned();
        let client_ip_str = client_ip.to_string();
        let agent_clone = agent_id.to_owned();
        let session_clone = session_id.to_owned();
        let req_id_clone = req_id.to_owned();
        let status_code = status.as_u16();

        tokio::spawn(async move {
            let mut buffer: Vec<u8> = Vec::new();
            let mut raw_sse: Vec<u8> = Vec::new();
            let mut ctx = StreamContext::default();
            let mut sent_done = false;

            let mut client_gone = false;
            let mut upstream_error = false;

            loop {
                match resp.chunk().await {
                    Ok(Some(chunk)) => {
                        buffer.extend_from_slice(&chunk);
                        raw_sse.extend_from_slice(&chunk);

                        while let Some(pos) = buffer.windows(2).position(|w| w == b"\n\n") {
                            let event = String::from_utf8_lossy(&buffer[..pos]).into_owned();
                            buffer.drain(..pos + 2);

                            if event.trim().is_empty() {
                                continue;
                            }

                            if let Some(translated) = translate_sse_event(&event, &mut ctx) {
                                if translated.trim() == "data: [DONE]" {
                                    sent_done = true;
                                }
                                if tx.send(Ok(bytes::Bytes::from(translated))).await.is_err() {
                                    client_gone = true;
                                    break;
                                }
                            }
                        }
                        if client_gone {
                            break;
                        }
                    }
                    Ok(None) => break,
                    Err(e) => {
                        upstream_error = true;
                        warn!(req_id = req_id_clone, error = %e, "upstream SSE read failed");
                        // The post-loop "ensure DONE sent" block gates on
                        // !upstream_error (set above), so emitting the error
                        // frame here — which already ships [DONE] — cannot
                        // race with a second [DONE] from the post-loop guard.
                        if !sent_done
                            && tx
                                .send(Ok(openai_error_frame(&format!(
                                    "upstream stream interrupted: {e}"
                                ))))
                                .await
                                .is_err()
                        {
                            client_gone = true;
                        }
                        break;
                    }
                }
            }

            // Process any remaining data in buffer (skip if upstream errored)
            if !upstream_error && !buffer.is_empty() {
                let remaining = String::from_utf8_lossy(&buffer).into_owned();
                if !remaining.trim().is_empty() {
                    if let Some(translated) = translate_sse_event(&remaining, &mut ctx) {
                        if translated.trim() == "data: [DONE]" {
                            sent_done = true;
                        }
                        if tx.send(Ok(bytes::Bytes::from(translated))).await.is_err() {
                            client_gone = true;
                        }
                    }
                }
            }

            // Ensure [DONE] is always sent (skip on upstream error — would fake clean completion)
            if !sent_done
                && !client_gone
                && !upstream_error
                && tx
                    .send(Ok(bytes::Bytes::from("data: [DONE]\n\n")))
                    .await
                    .is_err()
            {
                client_gone = true;
            }

            // Extract and record token usage from accumulated SSE data. The
            // detached task only holds a cloned Arc<AppState>; re-index it.
            let ep = &state_clone.endpoints[endpoint_idx];
            finalize_stream(
                &state_clone,
                ep,
                &req_id_clone,
                &client_id_clone,
                &model_clone,
                &acct_name,
                &client_ip_str,
                &agent_clone,
                &session_clone,
                status_code,
                &raw_sse,
                request_start,
                client_gone,
                upstream_error,
                true,
            )
            .await;
        });

        let response = Response::builder()
            .status(StatusCode::OK)
            .header("content-type", "text/event-stream")
            .header("cache-control", "no-cache")
            .header("connection", "keep-alive")
            .header("x-budget-status", budget_status)
            .body(Body::from_stream(ReceiverStream::new(rx)))
            .unwrap_or_else(|_| {
                (StatusCode::INTERNAL_SERVER_ERROR, "response build error").into_response()
            });
        return ForwardOutcome::Done(response);
    }

    // Non-streaming: buffer, translate, return
    let resp_bytes = match resp.bytes().await {
        Ok(b) => b,
        Err(e) => {
            error!("failed to read upstream response: {e}");
            return ForwardOutcome::Done(
                (StatusCode::BAD_GATEWAY, "failed to read upstream response").into_response(),
            );
        }
    };

    let anthropic_resp: serde_json::Value = match serde_json::from_slice(&resp_bytes) {
        Ok(v) => v,
        Err(_) => {
            let response = Response::builder()
                .status(StatusCode::OK)
                .header("content-type", "application/json")
                .header("x-budget-status", budget_status)
                .body(Body::from(resp_bytes))
                .unwrap_or_else(|_| {
                    (StatusCode::INTERNAL_SERVER_ERROR, "response build error").into_response()
                });
            return ForwardOutcome::Done(response);
        }
    };

    let openai_resp = translate_anthropic_to_openai(&anthropic_resp);

    // Extract and record token usage from non-streaming response
    let usage = TokenUsage::from_response_body(&anthropic_resp);
    finalize_non_stream(
        state,
        ep,
        req_id,
        client_id,
        model,
        endpoint_name,
        &client_ip.to_string(),
        agent_id,
        session_id,
        status.as_u16(),
        &usage,
        request_start.elapsed().as_millis() as u64,
        true,
    )
    .await;

    let response = Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "application/json")
        .header("x-budget-status", budget_status)
        .body(Body::from(
            serde_json::to_vec(&openai_resp).unwrap_or_default(),
        ))
        .unwrap_or_else(|_| {
            (StatusCode::INTERNAL_SERVER_ERROR, "response build error").into_response()
        });
    ForwardOutcome::Done(response)
}

async fn openai_chat_handler(
    State(state): State<Arc<AppState>>,
    axum::extract::ConnectInfo(client_addr): axum::extract::ConnectInfo<SocketAddr>,
    req: Request<Body>,
) -> Response {
    let client_ip = client_addr.ip();
    let request_start = Instant::now();

    // IP allowlist check
    if !state.is_ip_allowed(&client_ip) {
        warn!(client = %client_ip, "rejected: IP not in allowlist");
        return (StatusCode::FORBIDDEN, "forbidden").into_response();
    }

    // Proxy auth: accept if either x-api-key or Authorization: Bearer matches
    if let Some(ref key) = state.proxy_key {
        let from_header = req.headers().get("x-api-key").and_then(|v| v.to_str().ok());
        let from_bearer = req
            .headers()
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| {
                // RFC 7235: auth scheme is case-insensitive
                if v.len() >= 7 && v[..7].eq_ignore_ascii_case("bearer ") {
                    Some(&v[7..])
                } else {
                    None
                }
            });
        let authorized = from_header == Some(key.as_str()) || from_bearer == Some(key.as_str());
        if !authorized {
            warn!(client = %client_ip, "rejected: invalid or missing proxy key");
            return (StatusCode::UNAUTHORIZED, "unauthorized").into_response();
        }
    }

    let (parts, body) = req.into_parts();

    let req_id = format!(
        "{:04x}:{}",
        state.instance_id,
        state.next_req_id.fetch_add(1, Ordering::Relaxed)
    );

    // Extract client identification headers
    let rctx = RequestContext::from_request(&state, &client_ip, &parts.headers);
    let affinity_key = rctx.affinity_key(&client_ip, None);
    let affinity = affinity_key.as_deref();
    let RequestContext {
        client_id,
        client_ver,
        agent_id,
        session_id,
    } = rctx;

    // Admission control (P1-01): same body-memory backstop as proxy_handler.
    let _body_reservation = match reserve_request_body(&state, &parts, &req_id, client_ip) {
        Ok(g) => g,
        Err(resp) => return *resp,
    };

    let body_bytes = match read_body_bounded(&state, body, &req_id).await {
        Ok(b) => b,
        Err(resp) => return *resp,
    };

    let openai_body: serde_json::Value = match serde_json::from_slice(&body_bytes) {
        Ok(v) => v,
        Err(e) => {
            error!("failed to parse request JSON: {e}");
            return (StatusCode::BAD_REQUEST, "invalid JSON").into_response();
        }
    };

    let is_streaming = openai_body
        .get("stream")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let model = openai_body
        .get("model")
        .and_then(|m| m.as_str())
        .unwrap_or("")
        .to_string();

    // Pre-request gate: operator bypass, budget, utilization limit, emergency brake.
    // Note: budget + emergency don't need `model` and could run before body parsing,
    // but those rejections are rare and the JSON parse cost is negligible — not worth
    // splitting the gate for a few microseconds on an almost-never code path.
    if let Err(resp) = state.pre_request_gate(&client_id, &model).await {
        return resp;
    }

    let mut anthropic_body = translate_openai_to_anthropic(&openai_body);

    if state.auto_cache {
        let inj = inject_cache_breakpoints(&mut anthropic_body);
        if inj.skipped {
            debug!("auto-cache: skipped, existing cache_control found");
        } else if inj.tools || inj.system || inj.messages {
            debug!(
                tools = inj.tools,
                system = inj.system,
                messages = inj.messages,
                "auto-cache: injected breakpoints"
            );
        }
    }

    // Pre-compute OAuth variant with Claude Code system prompt.
    // OAuth tokens (sk-ant-oat*) require this to access sonnet/opus models.
    let mut oauth_anthropic_body = anthropic_body.clone();
    inject_oauth_system_prompt(&mut oauth_anthropic_body);

    let n = state.endpoints.len();
    let mut last_saw_529 = false;
    let mut last_saw_transient = false;
    for retry_round in 0..=MAX_529_RETRIES {
        if retry_round > 0 {
            let delay = round_backoff_delay(retry_round, last_saw_529);
            warn!(
                retry_round = retry_round,
                delay_ms = delay.as_millis() as u64,
                saw_529 = last_saw_529,
                "backoff: retrying all endpoints after transient/overload round"
            );
            tokio::time::sleep(delay).await;
        }
        let mut skip: Vec<EndpointIdx> = Vec::new();
        let mut saw_529 = false;
        let mut saw_transient = false;
        for _attempt in 0..n {
            // Pick the next endpoint and dispatch by protocol. Both forwards
            // return a `ForwardOutcome` so the shared round-gated policy in
            // `apply_round_outcome` covers both.
            let (outcome, picked_idx): (ForwardOutcome, EndpointIdx) =
                match state.pick_endpoint(affinity, &model, &skip).await {
                    Some(i) => {
                        let ep = &state.endpoints[i];
                        match ep.protocol {
                            Protocol::Anthropic => {
                                let out = forward_openai_compat_anthropic(
                                    &state,
                                    &parts,
                                    ep,
                                    i,
                                    &anthropic_body,
                                    &oauth_anthropic_body,
                                    &req_id,
                                    &client_id,
                                    &client_ver,
                                    &client_ip,
                                    &agent_id,
                                    &session_id,
                                    &model,
                                    is_streaming,
                                    request_start,
                                )
                                .await;
                                (out, i)
                            }
                            Protocol::OpenAI => {
                                // The endpoint is OpenAI-native — forward the
                                // original request body without translation.
                                let out = try_fallback_upstream(
                                    &state,
                                    &body_bytes,
                                    &req_id,
                                    &client_id,
                                    &model,
                                    i,
                                    false,
                                )
                                .await;
                                (out, i)
                            }
                        }
                    }
                    // Candidates exhausted mid-round (all skipped / hard-limited /
                    // model-filtered). Break to the round-end logic rather than
                    // returning here, so a transient-only round still reaches the
                    // transient-aware exhaustion status instead of short-circuiting
                    // to a premature 429.
                    None => break,
                };

            match apply_round_outcome(
                retry_round,
                outcome,
                picked_idx,
                &mut skip,
                &mut saw_529,
                &mut saw_transient,
            ) {
                RetryStep::Return(resp) => return resp,
                RetryStep::NextAttempt => continue,
                RetryStep::EndRound => break,
            }
        }
        last_saw_529 = saw_529;
        last_saw_transient = saw_transient;
        if !round_should_continue(retry_round, saw_529, saw_transient) {
            break;
        }
    }

    exhaustion_response(last_saw_transient, last_saw_529)
}

// ── Main ────────────────────────────────────────────────────────────

/// Validate endpoint configuration. Returns the first hard error encountered.
/// Soft warnings (non-canonical anthropic host, priority collision with an
/// openai endpoint) are emitted as `warn!` and do not return an error.
fn validate_endpoints(endpoints: &[EndpointConfig]) -> Result<(), String> {
    for ep in endpoints {
        if let Some(url) = ep.base_url.as_deref() {
            if !url.starts_with("https://") {
                return Err(format!(
                    "endpoint '{}': base_url must start with https:// (got '{}')",
                    ep.name, url
                ));
            }
        }
        match ep.protocol {
            Protocol::OpenAI => {
                if ep.base_url.is_none() {
                    return Err(format!(
                        "endpoint '{}': base_url is required for protocol = openai",
                        ep.name
                    ));
                }
            }
            Protocol::Anthropic => {
                if let Some(url) = ep.base_url.as_deref() {
                    // Parse the URL and compare hosts exactly. A naive
                    // `starts_with("https://api.anthropic.com")` would accept
                    // `https://api.anthropic.com.evil.example` as canonical.
                    let host = reqwest::Url::parse(url)
                        .ok()
                        .and_then(|u| u.host_str().map(str::to_string));
                    if host.as_deref() != Some("api.anthropic.com") {
                        warn!(
                            endpoint = ep.name,
                            base_url = url,
                            "anthropic endpoint base_url is non-canonical — verify this is intentional"
                        );
                    }
                }
            }
        }
    }

    // Priority-collision warning: an OpenAI endpoint sharing the lowest tier
    // with any Anthropic endpoint recreates the bug §Problem cites.
    let lowest = endpoints.iter().map(|e| e.priority).min().unwrap_or(0);
    let openai_at_lowest: Vec<&str> = endpoints
        .iter()
        .filter(|e| e.protocol == Protocol::OpenAI && e.priority == lowest)
        .map(|e| e.name.as_str())
        .collect();
    let anthropic_at_lowest: Vec<&str> = endpoints
        .iter()
        .filter(|e| e.protocol == Protocol::Anthropic && e.priority == lowest)
        .map(|e| e.name.as_str())
        .collect();
    if !openai_at_lowest.is_empty() && !anthropic_at_lowest.is_empty() {
        warn!(
            priority = lowest,
            openai = ?openai_at_lowest,
            anthropic = ?anthropic_at_lowest,
            "openai endpoint(s) share the lowest priority tier with anthropic endpoint(s) — paid OpenAI capacity will compete with free Anthropic capacity"
        );
    }
    Ok(())
}

/// Reject removed config keys with explicit errors. Run after the raw TOML
/// has been parsed to a `toml::Value`, before strongly-typed deserialization.
///
/// `serde` silently drops unknown keys by default; this gives the operator
/// a clear migration message instead of a silent misconfiguration.
fn reject_legacy_config_keys(value: &toml::Value) -> Result<(), String> {
    let table = match value.as_table() {
        Some(t) => t,
        None => return Ok(()),
    };
    if table.contains_key("accounts") {
        return Err(
            "config: [[accounts]] is no longer supported — use [[endpoints]] (see CLAUDE.md)"
                .to_string(),
        );
    }
    if table.contains_key("upstreams") {
        return Err(
            "config: [[upstreams]] is no longer supported — use [[endpoints]] with protocol = \"openai\" (see CLAUDE.md)"
                .to_string(),
        );
    }
    if table.contains_key("fallback_upstream") {
        return Err(
            "config: fallback_upstream is no longer supported — set a high priority on the OpenAI endpoint instead (see CLAUDE.md)"
                .to_string(),
        );
    }
    Ok(())
}

#[tokio::main]
async fn main() {
    // Parse config first so debug_log path is available for tracing setup
    let config_path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "config.toml".to_string());
    let config_str = std::fs::read_to_string(&config_path)
        .unwrap_or_else(|e| panic!("failed to read {config_path}: {e}"));
    let raw_value: toml::Value =
        toml::from_str(&config_str).unwrap_or_else(|e| panic!("config parse error: {e}"));
    if let Err(msg) = reject_legacy_config_keys(&raw_value) {
        panic!("{msg}");
    }
    let config: Config = raw_value
        .try_into()
        .unwrap_or_else(|e| panic!("config parse error: {e}"));
    if let Err(msg) = validate_endpoints(&config.endpoints) {
        panic!("{msg}");
    }

    // Set up tracing: stderr (info+) always, plus optional debug log file
    {
        use tracing_subscriber::prelude::*;
        let stderr_layer = tracing_subscriber::fmt::layer().with_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "anthropic_lb=info".into()),
        );
        if let Some(ref debug_path) = config.debug_log {
            let file = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(debug_path)
                .unwrap_or_else(|e| panic!("failed to open debug log {debug_path}: {e}"));
            let file_layer = tracing_subscriber::fmt::layer()
                .with_ansi(false)
                .with_writer(std::sync::Mutex::new(file))
                .with_filter(tracing_subscriber::EnvFilter::new("anthropic_lb=debug"));
            tracing_subscriber::registry()
                .with(stderr_layer)
                .with(file_layer)
                .init();
            eprintln!("debug logging to {debug_path}");
        } else {
            tracing_subscriber::registry().with(stderr_layer).init();
        }
    }

    let routing_strategy = RoutingStrategy::parse(config.strategy.as_deref())
        .unwrap_or_else(|e| panic!("invalid strategy: {e}"));

    assert!(
        !config.endpoints.is_empty(),
        "at least one [[endpoints]] entry required"
    );

    // Validate new config fields
    for (client, limit) in &config.client_utilization_limits {
        assert!(
            (0.0..=1.0).contains(limit),
            "client_utilization_limits.{client}: must be 0.0-1.0, got {limit}"
        );
    }
    for op in &config.operators {
        assert!(
            op != "-",
            "operators cannot contain '-' (the unknown-client sentinel)"
        );
    }
    if let Some(thresh) = config.emergency_threshold {
        assert!(
            (0.0..=1.0).contains(&thresh),
            "emergency_threshold must be 0.0-1.0, got {thresh}"
        );
    }

    let cooldown = Duration::from_secs(config.rate_limit_cooldown_secs.unwrap_or(5));

    // Parse IP allowlist
    let allowed_ips: Vec<IpAllowEntry> = config
        .allowed_ips
        .unwrap_or_default()
        .iter()
        .map(|s| {
            if let Ok(net) = s.parse::<IpNet>() {
                IpAllowEntry::Net(net)
            } else if let Ok(addr) = s.parse::<IpAddr>() {
                IpAllowEntry::Addr(addr)
            } else {
                panic!("invalid allowed_ips entry: {s}");
            }
        })
        .collect();
    if allowed_ips.is_empty() {
        warn!("IP allowlist DISABLED — all source IPs accepted");
    } else {
        info!(count = allowed_ips.len(), "IP allowlist enabled");
    }

    // Build the unified endpoint vector from the [[endpoints]] config.
    let endpoints: Vec<Endpoint> = config
        .endpoints
        .iter()
        .map(|ec| {
            let passthrough = ec.token == "passthrough";
            let base_url = match ec.protocol {
                Protocol::Anthropic => ec
                    .base_url
                    .clone()
                    .unwrap_or_else(|| "https://api.anthropic.com".to_string()),
                Protocol::OpenAI => ec.base_url.clone().expect(
                    "validate_endpoints should have rejected an openai endpoint without base_url",
                ),
            };
            info!(
                name = ec.name,
                protocol = ?ec.protocol,
                base_url = base_url.as_str(),
                priority = ec.priority,
                passthrough,
                models = ?ec.models,
                "loaded endpoint"
            );
            Endpoint {
                name: ec.name.clone(),
                protocol: ec.protocol,
                base_url: base_url.trim_end_matches('/').to_string(),
                token: ec.token.clone(),
                passthrough,
                models: ec.models.clone(),
                priority: ec.priority,
                fable_included: ec.fable_included.unwrap_or(true),
                requests: AtomicU64::new(0),
                rate_info: RwLock::new(RateLimitInfo::default()),
                burn_rate: Mutex::new(BurnRate::new()),
                input_tokens: AtomicU64::new(0),
                output_tokens: AtomicU64::new(0),
                cache_creation_tokens: AtomicU64::new(0),
                cache_read_tokens: AtomicU64::new(0),
                last_routing_weight: AtomicU64::new(0),
                last_routing_share: AtomicU64::new(0),
                last_effective_gate: AtomicU64::new(0),
            }
        })
        .collect();

    if config.proxy_key.is_some() {
        info!("proxy authentication enabled (x-api-key)");
    } else {
        warn!("proxy authentication DISABLED — proxy is open to all");
    }

    info!(
        strategy = routing_strategy.as_str(),
        num_endpoints = endpoints.len(),
        "routing strategy selected"
    );

    let state_path = PathBuf::from(&config_path).with_extension("state.json");

    // Set up shadow log writer if configured
    let shadow_log_tx = if let Some(ref path) = config.shadow_log {
        let (tx, mut rx) = tokio::sync::mpsc::channel::<String>(10_000);
        let log_path = PathBuf::from(path);
        info!(path = %log_path.display(), "shadow log enabled");
        tokio::spawn(async move {
            use tokio::io::AsyncWriteExt;
            let mut file = match tokio::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&log_path)
                .await
            {
                Ok(f) => f,
                Err(e) => {
                    error!(path = %log_path.display(), error = %e, "failed to open shadow log");
                    return;
                }
            };
            while let Some(line) = rx.recv().await {
                let data = format!("{}\n", line);
                if let Err(e) = file.write_all(data.as_bytes()).await {
                    error!(error = %e, "shadow log write failed");
                }
            }
        });
        Some(tx)
    } else {
        None
    };

    // Set up Redis connection for distributed state (if configured)
    let redis = if let Some(ref url) = config.redis_url {
        match redis::Client::open(url.as_str()) {
            Ok(client) => {
                let mgr_config = redis::aio::ConnectionManagerConfig::new()
                    .set_response_timeout(Some(Duration::from_secs(2)))
                    .set_connection_timeout(Some(Duration::from_secs(5)));
                match client.get_connection_manager_with_config(mgr_config).await {
                    Ok(mgr) => {
                        info!("redis connected for distributed state");
                        Some(mgr)
                    }
                    Err(e) => {
                        warn!(error = %e, "redis connection failed — running in local-only mode");
                        None
                    }
                }
            }
            Err(e) => {
                warn!(error = %e, "invalid redis_url — running in local-only mode");
                None
            }
        }
    } else {
        None
    };

    let state = Arc::new(AppState {
        // Liveness knobs are load-bearing against Anthropic's Cloudflare edge:
        // h2 PING (while_idle) evicts half-closed pooled streams before they're
        // reused; read_timeout catches mid-stream stalls without waiting out
        // the full request budget; pool_idle_timeout keeps connections warm
        // through Claude Code read/think pauses to avoid paying a fresh TLS
        // handshake on every burst. read_timeout is set at 180s so Opus's
        // extended-thinking pauses (which can exceed 90s of inter-chunk
        // silence on deep reasoning) don't trip a false-positive interruption.
        client: upstream_client_builder()
            .read_timeout(Duration::from_secs(180))
            .build()
            .expect("failed to build HTTP client"),
        // Same knobs MINUS read_timeout: a non-streaming response has no
        // inter-chunk cadence to police — the only bytes arrive when
        // generation finishes, so a read_timeout is a hard cap on generation
        // time (LAB-718). The 900s total budget still bounds the request.
        client_nonstreaming: upstream_client_builder()
            .build()
            .expect("failed to build non-streaming HTTP client"),
        endpoints,
        robin: AtomicUsize::new(0),
        routing_strategy,
        cooldown,
        transport_cooldown: TRANSPORT_UNHEALTHY_COOLDOWN,
        state_path,
        proxy_key: config.proxy_key.clone(),
        allowed_ips,
        client_names: config.client_names.clone(),
        auto_cache: config.auto_cache.unwrap_or(true),
        client_usage: Mutex::new(HashMap::new()),
        shadow_log_tx,
        shadow_log_dropped: AtomicU64::new(0),
        client_budgets: config.client_budgets.clone(),
        budget_usage: Mutex::new(HashMap::new()),
        client_utilization_limits: config.client_utilization_limits.clone(),
        operators: config.operators.clone(),
        emergency_brake: config.emergency_brake.unwrap_or(true),
        emergency_threshold: config
            .emergency_threshold
            .unwrap_or(DEFAULT_EMERGENCY_THRESHOLD),
        client_request_rates: Mutex::new(HashMap::new()),
        soft_limit: config.soft_limit.unwrap_or(0.90),
        redis,
        cluster_info_cache: Mutex::new(None),
        next_req_id: AtomicU64::new(0),
        instance_id: {
            use std::collections::hash_map::RandomState;
            use std::hash::{BuildHasher, Hasher};
            RandomState::new().build_hasher().finish() as u16
        },
        probe_interval_secs: config.probe_interval_secs.unwrap_or(300),
        overage_penalty: config.overage_penalty.unwrap_or(10),
        upstream_transport_errors: Mutex::new(HashMap::new()),
        inflight_body_bytes: AtomicU64::new(0),
        max_inflight_body_bytes: config
            .max_inflight_body_mb
            .map(|mb| mb.saturating_mul(1024 * 1024))
            .unwrap_or(DEFAULT_MAX_INFLIGHT_BODY_BYTES),
        body_shed_total: AtomicU64::new(0),
        body_read_timeout: Duration::from_secs(
            config
                .body_read_timeout_secs
                .unwrap_or(DEFAULT_BODY_READ_TIMEOUT_SECS),
        ),
        body_read_timeout_total: AtomicU64::new(0),
    });

    if state.auto_cache {
        info!("auto-cache enabled");
    }

    // Restore persisted state (cooldowns, utilization, request counts)
    state.load_state().await;

    // Seed metric weights from restored state so gauges aren't zero on cold start.
    state.refresh_metrics_weights().await;

    let app = Router::new()
        .route("/_stats", axum::routing::get(stats_handler))
        .route("/metrics", axum::routing::get(metrics_handler))
        .route(
            "/v1/chat/completions",
            axum::routing::post(openai_chat_handler),
        )
        .fallback(any(proxy_handler))
        .with_state(state.clone());

    let addr: SocketAddr = config
        .listen
        .parse()
        .unwrap_or_else(|e| panic!("invalid listen address: {e}"));

    info!(
        %addr,
        rate_limit_cooldown_secs = cooldown.as_secs(),
        configured = ?config.rate_limit_cooldown_secs,
        "anthropic-lb starting"
    );

    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .unwrap_or_else(|e| panic!("failed to bind {addr}: {e}"));

    // Spawn periodic probe task. `probe_endpoint` internally skips OpenAI
    // endpoints (they expose no Anthropic rate-limit headers).
    let probe_interval = config.probe_interval_secs.unwrap_or(300);
    if probe_interval > 0 {
        let probe_state = state.clone();
        let n_endpoints = probe_state.endpoints.len();
        tokio::spawn(async move {
            // Fable is deliberately NOT probed: probes burn real quota, Fable
            // burns the shared weekly pool faster than other families, and past
            // the included band a probe would spend paid credits. The Fable band
            // claim (seven_day_fable) refreshes from organic Fable traffic; until
            // one is seen, routing falls back to the general seven_day claim.
            const PROBE_MODELS: &[&str] =
                &["claude-haiku-4-5", "claude-sonnet-4-6", "claude-opus-4-6"];
            // Stagger initial probes: wait 10s then probe all endpoints
            tokio::time::sleep(Duration::from_secs(10)).await;
            info!(
                interval_secs = probe_interval,
                "starting utilization probes"
            );
            loop {
                for i in 0..n_endpoints {
                    let ep = &probe_state.endpoints[i];
                    // Probe all model families per endpoint per cycle
                    for model in PROBE_MODELS {
                        if ep.serves_model(model) {
                            probe_state.probe_endpoint(i, model).await;
                            tokio::time::sleep(Duration::from_secs(2)).await;
                        }
                    }
                }
                // Recompute metric weights once per cycle, after all probes
                // have refreshed rate-limit data. Keeps the gauges aligned
                // with steady-state pool health, not per-request bias.
                probe_state.refresh_metrics_weights().await;
                probe_state.publish_routing_weights().await;
                tokio::time::sleep(Duration::from_secs(probe_interval)).await;
            }
        });
    } else {
        // Probes disabled — but `update_rate_info_for()` still refreshes data
        // on every inbound request. Without this fallback ticker the routing
        // weight gauges would freeze at startup values forever.
        let metrics_state = state.clone();
        tokio::spawn(async move {
            const FALLBACK_INTERVAL: Duration = Duration::from_secs(60);
            tokio::time::sleep(Duration::from_secs(10)).await;
            info!("probes disabled — metrics weights refresh on a 60s timer");
            loop {
                metrics_state.refresh_metrics_weights().await;
                metrics_state.publish_routing_weights().await;
                tokio::time::sleep(FALLBACK_INTERVAL).await;
            }
        });
    }

    // Spawn Redis state sync + heartbeat task (if Redis configured)
    if state.redis.is_some() {
        let sync_state = state.clone();
        let instance_id: u64 = {
            use std::collections::hash_map::RandomState;
            use std::hash::{BuildHasher, Hasher};
            RandomState::new().build_hasher().finish()
        };
        tokio::spawn(async move {
            // Wait for initial startup to complete
            tokio::time::sleep(Duration::from_secs(5)).await;
            info!("starting redis state sync (5s interval)");
            loop {
                sync_state.sync_from_redis().await;
                // Heartbeat: register this instance
                if let Some(redis) = &sync_state.redis {
                    use redis::AsyncCommands;
                    let mut conn = redis.clone();
                    let key = format!("alb:heartbeat:{instance_id}");
                    let _: redis::RedisResult<()> =
                        conn.set_ex(&key, AppState::now_epoch(), 30u64).await;
                }
                tokio::time::sleep(Duration::from_secs(5)).await;
            }
        });
    }

    // Bounded drain so a wedged stream can't hold the process past the
    // orchestrator's kill deadline and earn a SIGKILL with unsaved state.
    // In k8s this is `terminationGracePeriodSeconds`; keep SHUTDOWN_DRAIN_SECS
    // strictly less so the proxy exits cleanly before SIGKILL. Long Claude
    // Code streams (Opus on long outputs) can run 60–120s, so the budget
    // must accommodate that or in-flight responses get cut.
    const SHUTDOWN_DRAIN_SECS: u64 = 160;
    let shutdown_state = state.clone();

    // The drain deadline must start ticking from signal-arrival, not from
    // process start — `tokio::time::timeout(d, server)` would arm `d` when
    // first polled and force-exit any uptime > d. The signal handler fires
    // notify_waiters(), and the deadline future awaits that before its sleep.
    //
    // `drain_triggered` closes a race in `Notify::notify_waiters()`: it only
    // wakes waiters registered at call time. Without the flag, a signal that
    // arrives before drain_deadline registers its waiter is lost and the
    // deadline never fires. With the flag, drain_deadline registers the
    // waiter eagerly via Notified::enable(), then checks the flag — so
    // either path (signal-before-poll, signal-after-poll) is observed.
    let drain_signal = Arc::new(tokio::sync::Notify::new());
    let drain_signal_in = drain_signal.clone();
    let drain_triggered = Arc::new(AtomicBool::new(false));
    let drain_triggered_in = drain_triggered.clone();

    let shutdown = async move {
        let ctrl_c = tokio::signal::ctrl_c();
        let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to register SIGTERM");
        tokio::select! {
            _ = ctrl_c => info!("received SIGINT"),
            _ = sigterm.recv() => info!("received SIGTERM"),
        }
        info!(
            drain_budget_secs = SHUTDOWN_DRAIN_SECS,
            "draining in-flight requests"
        );
        drain_triggered_in.store(true, Ordering::SeqCst);
        drain_signal_in.notify_waiters();
    };

    let server = axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .with_graceful_shutdown(shutdown);

    let drain_deadline = async {
        let notified = drain_signal.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        if !drain_triggered.load(Ordering::SeqCst) {
            notified.await;
        }
        tokio::time::sleep(Duration::from_secs(SHUTDOWN_DRAIN_SECS)).await;
    };

    tokio::select! {
        res = server => match res {
            Ok(()) => info!("drain complete"),
            Err(e) => error!(error = %e, "server error during drain"),
        },
        _ = drain_deadline => warn!(
            drain_budget_secs = SHUTDOWN_DRAIN_SECS,
            "drain timeout exceeded — forcing exit; in-flight streams will be cut"
        ),
    }
    info!("saving state");
    shutdown_state.save_state().await;
    info!("state saved, shutdown complete");
}

// ── Tests ────────────────────────────────────────────────────────────

#[cfg(test)]
#[path = "tests.rs"]
mod tests;
