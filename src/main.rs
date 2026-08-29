use axum::{
    body::Body,
    extract::State,
    http::{HeaderValue, Request, StatusCode},
    response::{IntoResponse, Response},
    routing::any,
    Router,
};
use fred::{
    clients::RedisClient,
    interfaces::{ClientLike, EventInterface, HashesInterface, KeysInterface, LuaInterface},
    types::{
        ConnectionConfig, Expiration, PerformanceConfig, ReconnectPolicy, RedisConfig, RedisValue,
        SetOptions,
    },
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
use subtle::ConstantTimeEq;
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
    /// LEGACY single shared secret, sent as x-api-key. None = open.
    /// Superseded by `[[clients]]`; configuring both is rejected at startup
    /// (`reject_legacy_config_keys`) rather than silently precedence-ordered.
    proxy_key: Option<String>,
    /// Per-client credentials (LAB-1083). When non-empty, EVERY authenticated
    /// entry point requires a credential matching one of these keys, and the
    /// matched entry's `name` becomes the request's `client_id` — the
    /// `x-client-id` header and the `client_names` IP map are then ignored
    /// entirely. That is the whole point: budgets, utilization ceilings,
    /// operator bypass, the model allow-list and the response-cache tenant all
    /// key on `client_id`, so making it unforgeable makes all five unforgeable
    /// at once.
    #[serde(default)]
    clients: Vec<ClientConfig>,
    /// Source IP allowlist. Supports individual IPs and CIDR ranges. None/empty = allow all.
    allowed_ips: Option<Vec<String>>,
    /// LAB-1192: the ONE escape hatch from default-deny authentication.
    /// Startup FAILS when neither `[[clients]]` nor `proxy_key` is configured
    /// unless this is explicitly true. Trusted-network-only (e.g. behind the
    /// lab NetworkPolicy) — never on a public ingress. Mutually exclusive
    /// with configured credentials, so it cannot mask a half-applied
    /// migration.
    allow_unauthenticated: Option<bool>,
    /// LAB-1192: IPs/CIDRs of load balancers whose `x-forwarded-for` is
    /// trusted. When the TCP peer is inside this list, the client IP becomes
    /// the rightmost `x-forwarded-for` entry that is NOT itself in the list;
    /// otherwise the peer address is used and the header is ignored entirely.
    /// Empty/absent = header never consulted (direct-connection behaviour).
    trusted_proxies: Option<Vec<String>>,
    /// LAB-1193 amendment: failed-auth throttle — failures per client IP
    /// inside the window before further INVALID credentials from that IP get
    /// 429; valid credentials always pass. Supersedes LAB-1192 AC-11's
    /// pre-comparison ordering after the 2026-08-24 shared-IP outage.
    /// 0 disables. Default: 10.
    auth_failure_limit: Option<u32>,
    /// LAB-1192: failed-auth throttle window in seconds. Default: 300.
    auth_failure_window_secs: Option<u64>,
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
    /// Max entries in the live session registry (context-window visibility on
    /// `/_stats`). Registry keys are affinity routing keys, so fan-out agents
    /// each get an entry. Default: 1000. Set to 0 to disable the registry.
    session_registry_max: Option<usize>,
    /// Seconds after a session's last request before its registry entry is
    /// evicted. Default: 1800 (30 min).
    session_registry_ttl_secs: Option<u64>,
    /// Opt-in encrypted response cache on `/v1/messages` (LAB-933).
    /// Absent, or present with an empty `clients` list = feature entirely inert.
    response_cache: Option<ResponseCacheConfig>,
    /// Reflect upstream `anthropic-ratelimit-*` response headers to callers.
    /// They reveal the pooled capacity of every account behind the proxy, so
    /// this is trusted-network-only. Default: false (LAB-1191).
    expose_upstream_ratelimit_headers: Option<bool>,
    /// Client-supplied `anthropic-beta` flags forwarded upstream on OAuth
    /// endpoints ("*" suffix wildcards, like `endpoints[].models`). Absent =
    /// built-in default (`DEFAULT_CLIENT_BETA_ALLOWLIST`); a configured
    /// value REPLACES that default (it does not extend it), so include the
    /// defaults alongside any addition. Flags not on the list are dropped,
    /// logged, and counted (`anthropic_beta_flag_dropped_total`) — otherwise
    /// any caller could activate arbitrary beta features against the
    /// operator's accounts (LAB-1191).
    allowed_client_betas: Option<Vec<String>>,
}

/// `[[clients]]` — one authenticated caller. The `key` IS the identity: it is
/// what the caller presents, and `name` is what the proxy attributes the
/// request to. No `x-client-id` header can override it.
#[derive(Deserialize, Clone)]
struct ClientConfig {
    /// Identity this credential resolves to. Becomes `client_id`, so it is what
    /// `client_budgets`, `client_utilization_limits`, `operators` and
    /// `[response_cache].clients` must name.
    name: String,
    /// Shared secret this client presents as `x-api-key` (or, on the
    /// OpenAI-compat surface, `Authorization: Bearer`). Compared in constant
    /// time against the whole table.
    key: String,
    /// Models this client may request. Empty = all models, mirroring
    /// `EndpointConfig.models` semantics. Same exact + `*`-suffix matcher.
    #[serde(default)]
    models: Vec<String>,
}

/// Hand-written, NOT derived: a derived `Debug` would print `key` verbatim into
/// any log line, panic message or test assertion that formats this struct —
/// which is precisely how credentials end up in a log aggregator. Same posture
/// as `debug_header_value`'s redaction of sensitive headers.
impl std::fmt::Debug for ClientConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClientConfig")
            .field("name", &self.name)
            .field("key", &"<redacted>")
            .field("models", &self.models)
            .finish()
    }
}

/// `[response_cache]` — opt-in, client-side-encrypted response cache on
/// non-streaming `/v1/messages` (LAB-933). Cached bodies contain prompt
/// content, so SecureCache (AES-256-GCM, encrypted before any layer — L1
/// included) is mandatory: there is no plaintext-storage configuration.
#[derive(Deserialize, Clone)]
struct ResponseCacheConfig {
    /// Client IDs allowed to read/write the cache. Empty = inert (AC2).
    /// May not contain "-" (the unknown-client sentinel) — that would opt in
    /// every unidentified caller.
    #[serde(default)]
    clients: Vec<String>,
    /// Cache backend: "cachekitio" (SaaS, rides the existing reqwest/rustls
    /// stack) or "redis" (local Redis/Valkey via cachekit's fred client).
    backend: String,
    /// Hex-encoded master key for client-side encryption; must decode to at
    /// least 32 bytes. Per-client keys are derived from it via HKDF-SHA256
    /// with the client_id as tenant, so clients are cryptographically
    /// isolated from each other (AC7), not just key-string separated.
    master_key: String,
    /// Entry TTL in seconds. Default: 3600 (1 h).
    ttl_secs: Option<u64>,
    /// Per-operation budget in ms before the cache fails open and the
    /// request proceeds upstream (AC10). Default: 250.
    op_timeout_ms: Option<u64>,
    /// Connection URL for backend = "redis" (redis:// or rediss://).
    redis_url: Option<String>,
    /// API key for backend = "cachekitio".
    api_key: Option<String>,
    /// API URL override for backend = "cachekitio". Default:
    /// https://api.cachekit.io. This knob was cut in the LAB-933 review as
    /// YAGNI and reinstated for a named need: the first rollout targets the
    /// cachekit DEV environment, which lives on a different hostname.
    /// Operator config, same trust as endpoints[].base_url; cachekit's own
    /// validator still enforces HTTPS and rejects private/loopback IPs
    /// (SSRF guard — pinned by a test).
    api_url: Option<String>,
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
    /// Opt-in: allow a `protocol = "anthropic"` endpoint whose `base_url`
    /// host is not `api.anthropic.com` (e.g. a staging mirror). Without it,
    /// startup fails — a typo'd or tampered base_url would otherwise send the
    /// account's OAuth/API token to an arbitrary HTTPS host. Default: false.
    allow_nonstandard_host: Option<bool>,
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

/// Cap on distinct (client, model) labels in the allowlist-denial counter.
/// The model half is caller-controlled, and under legacy auth the client
/// half is too (`x-client-id`), so overflow lumps into a single global
/// ("_other", "_other") bucket — a HARD bound of cap + 1 entries (LAB-2332,
/// mirroring the LAB-2330 fix to `client_model_usage`).
const MAX_MODEL_DENIED_LABELS: usize = 64;

/// Cap on distinct (client, model) pairs in the per-model usage counter
/// (LAB-2330). The model key is normally response-derived (upstream-validated),
/// but the request-model fallback is caller-influenced and the client key is
/// caller-controlled under legacy auth, so overflow lumps into a single
/// global ("_other", "_other") bucket — a HARD bound of cap + 1 entries.
/// Sized for the real fleet (tens of clients × a handful of models) with
/// generous slack.
const MAX_CLIENT_MODEL_LABELS: usize = 256;

/// Max chars retained from a caller-controlled string used as a metric label
/// or echoed in an error body. The model field is bounded only by the request
/// body cap, so an untruncated copy would be retained for the process lifetime
/// and re-serialized on every `/metrics` scrape.
const MAX_LABEL_CHARS: usize = 64;

/// Truncate a caller-controlled string to `MAX_LABEL_CHARS`, marking that it
/// was cut so an operator does not read a clipped value as the literal input.
/// Char-based, not byte-based: slicing a UTF-8 string mid-codepoint panics.
fn truncate_label(s: &str) -> String {
    if s.chars().count() <= MAX_LABEL_CHARS {
        return s.to_owned();
    }
    let mut out: String = s.chars().take(MAX_LABEL_CHARS).collect();
    out.push('…');
    out
}
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

/// Exact match with `*`-suffix wildcards. Empty pattern list, or an empty
/// model, allows everything.
///
/// The SINGLE list-level implementation behind both model allowlists: which
/// models an *endpoint* may serve (`Endpoint::serves_model`) and which models
/// a *client* may request (`AppState::client_allows_model`, LAB-1083). The
/// per-pattern wildcard semantics live in `suffix_wildcard_match`, shared
/// with the beta-flag allow-list (LAB-1191) — two matchers would be two sets
/// of wildcard semantics to keep in sync, and the divergence would show up as
/// a policy bypass rather than a test failure.
fn model_matches(patterns: &[String], model: &str) -> bool {
    if patterns.is_empty() || model.is_empty() {
        return true;
    }
    patterns.iter().any(|p| suffix_wildcard_match(p, model))
}

impl Endpoint {
    /// Check if this endpoint can serve the given model. Empty allowlist = all.
    /// Identical to the historical `Account::serves_model` predicate.
    fn serves_model(&self, model: &str) -> bool {
        model_matches(&self.models, model)
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
    /// Legacy single shared secret. Mutually exclusive with `clients`.
    proxy_key: Option<String>,
    /// Authenticated client registry (LAB-1083). Non-empty ⇒ every request
    /// through an authenticated entry point carries a verified principal, and
    /// `client_id` is that principal's name rather than a client-asserted
    /// header. Empty ⇒ legacy `proxy_key` / open behaviour.
    clients: Vec<ClientConfig>,
    allowed_ips: Vec<IpAllowEntry>,
    /// Load balancers whose `x-forwarded-for` is trusted (LAB-1192).
    /// Consulted only by `resolve_client_ip`. Empty = header ignored.
    trusted_proxies: Vec<IpAllowEntry>,
    /// Per-client-IP failed-authentication throttle (LAB-1192).
    auth_throttle: AuthThrottle,
    /// Failed authentication attempts by route, for
    /// `anthropic_auth_failures_total{route}`. Routes are the four static
    /// handler names, so cardinality is fixed.
    auth_failures: Mutex<HashMap<&'static str, u64>>,
    /// Last time the `allow_unauthenticated` admin-access warn fired per route,
    /// so it stays visible without one line per scrape (LAB-1192 AC-5).
    open_admin_warn: Mutex<HashMap<&'static str, Instant>>,
    client_names: HashMap<String, String>,
    auto_cache: bool,
    /// Per-client token usage: client_id → [input, output, cache_creation, cache_read]
    client_usage: Mutex<HashMap<String, [u64; 4]>>,
    /// Per-(client, model) token usage (LAB-2330) — same [u64; 4] layout as
    /// `client_usage`, which stays the authoritative per-client total. The
    /// model key is response-derived and truncated; the pair count is hard-
    /// bounded at `MAX_CLIENT_MODEL_LABELS` + 1 (overflow lumps into the
    /// global ("_other", "_other") bucket), so callers cannot inflate the
    /// label set on either axis.
    client_model_usage: Mutex<HashMap<(String, String), [u64; 4]>>,
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
    /// Redis client for distributed state. None = local-only (single instance).
    /// fred clients are cheap to clone; every clone shares one multiplexed
    /// connection driven by a background task with a reconnect policy.
    redis: Option<RedisClient>,
    /// Whether the coordination client has EVER connected. False from process
    /// start until the first successful connect (set once by
    /// `spawn_redis_connect_watcher`, never cleared). Gates coordination ops
    /// off entirely while never-yet-connected — see `coordination_redis`.
    redis_ever_connected: AtomicBool,
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
    /// Reflect upstream `anthropic-ratelimit-*` headers to callers (see
    /// `Config::expose_upstream_ratelimit_headers`). Default: false.
    expose_upstream_ratelimit_headers: bool,
    /// `anthropic-beta` flags a client may forward upstream on OAuth
    /// endpoints ("*" suffix wildcards). Flags outside the list are dropped.
    allowed_client_betas: Vec<String>,
    /// Dropped client beta flags → drop count, for
    /// `anthropic_beta_flag_dropped_total{flag}`. Flag names are
    /// client-controlled input, so the map is bounded
    /// (`MAX_DROPPED_BETA_FLAGS`); overflow lands in the `_other` bucket.
    beta_flags_dropped: Mutex<HashMap<String, u64>>,
    /// Live session registry: affinity routing key → last-seen context-window
    /// occupancy (LAB-916). Visibility only — routing never reads it. Sync
    /// mutex, never held across `.await`; bounded by `session_registry_max`
    /// + TTL eviction.
    sessions: Mutex<HashMap<String, SessionEntry>>,
    /// Session registry entry cap. 0 disables the registry.
    session_registry_max: usize,
    /// Seconds since last request before a session entry is evicted.
    session_registry_ttl_secs: u64,
    /// Upstream "prompt is too long" 400s by model (LAB-916). Exposed as
    /// `anthropic_prompt_too_long_total`; bounded via `_other` overflow.
    prompt_too_long: Mutex<HashMap<String, u64>>,
    /// Per-client model-allowlist denials, keyed (client, model) (LAB-1083).
    /// Exposed as `anthropic_client_model_denied_total`. Under `[[clients]]`
    /// auth `client` is a credential-bound principal, but under legacy
    /// `proxy_key` / `allow_unauthenticated` it comes from the
    /// caller-controlled `x-client-id` header — so overflow lumps into a
    /// single global ("_other", "_other") bucket, hard-bounding the map at
    /// `MAX_MODEL_DENIED_LABELS` + 1 entries (LAB-2332).
    model_denied: Mutex<HashMap<(String, String), u64>>,
    /// (endpoint idx, model) pairs an upstream rejected as unsupported — a
    /// gateway without the model, or a plan without access (LAB-941).
    /// `routing_candidates` skips these until the entry expires; because
    /// session affinity is a stateless hash over the candidate list, the
    /// filter also re-buckets pinned sessions away from the endpoint. Sync
    /// mutex, never held across `.await`; bounded by UNSUPPORTED_MODEL_MAX
    /// + TTL eviction. Per-replica: a fresh replica re-learns in one attempt.
    unsupported_models: Mutex<HashMap<(usize, String), Instant>>,
    /// Opt-in encrypted response cache on non-streaming /v1/messages
    /// (LAB-933). None = feature off — the no-config case is byte-identical
    /// to pre-cache behaviour (AC1).
    response_cache: Option<ResponseCache>,
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

// ── Response cache (LAB-933) ────────────────────────────────────────
//
// Opt-in, client-side-encrypted response cache on non-streaming
// /v1/messages. The prize is headroom, not latency: a hit skips the
// upstream call entirely, so replayed traffic (eval reruns, pipeline
// replays, post-timeout retries) burns zero 5h/7d rate-limit budget.
// Everything below fails OPEN — a sick cache degrades to a miss/skipped
// write and the request proceeds upstream exactly as today.

/// A cached /v1/messages response: status + content-type + body, nothing
/// else. Upstream per-request headers (request ids, rate-limit snapshots,
/// x-budget-status) describe the ORIGINAL exchange and are deliberately not
/// replayed; a hit instead carries `x-alb-cache: hit`.
#[derive(Serialize, Deserialize)]
struct CachedResponse {
    status: u16,
    content_type: String,
    #[serde(with = "serde_bytes")]
    body: Vec<u8>,
}

/// Which endpoint a response-cache entry belongs to (LAB-929). Folded into
/// the cache key itself (not just the metric label) so a client that sends
/// byte-identical bodies to `/v1/messages` and `/v1/messages/count_tokens`
/// (a common pattern — count before you send) can never have one surface's
/// cached entry served back for the other.
#[derive(Clone, Copy)]
enum CacheSurface {
    Messages,
    CountTokens,
}

impl CacheSurface {
    fn label(self) -> &'static str {
        match self {
            Self::Messages => "messages",
            Self::CountTokens => "count_tokens",
        }
    }
}

/// Runtime handle for the response cache: one shared backend, one
/// `cachekit::CacheKit` per allow-listed client with `tenant_id =
/// client_id`. HKDF-SHA256 then derives a distinct AES-256-GCM key per
/// client, so cross-client isolation (AC7) is cryptographic — even a key
/// collision could not decrypt another client's entry. The map doubles as
/// the opt-in allow-list: no entry, no cache (AC2). All values are
/// encrypted before any storage layer sees them, in-process L1 included
/// (verified against cachekit-rs 0.5.0 `SecureCache::set_with_ttl`, which
/// writes ciphertext to both L1 and the backend).
struct ResponseCache {
    clients: HashMap<String, cachekit::CacheKit>,
    op_timeout: Duration,
    hits: AtomicU64,
    misses: AtomicU64,
    errors: AtomicU64,
    stores: AtomicU64,
    // LAB-929: count_tokens gets its own series (AC4) rather than sharing
    // the /v1/messages counters above — a HashMap<&str, _> would be more
    // machinery than two fixed, permanent surfaces warrant.
    count_tokens_hits: AtomicU64,
    count_tokens_misses: AtomicU64,
    count_tokens_errors: AtomicU64,
    count_tokens_stores: AtomicU64,
}

/// First 16 hex chars of a cache-key digest — the ONLY form a cache key may
/// take in any diagnostic output (AC5).
fn key_digest_prefix(key: &str) -> &str {
    &key[..key.len().min(16)]
}

/// Decode a hex master key, enforcing the 32-byte SecureCache minimum.
fn decode_hex_key(s: &str) -> Result<Vec<u8>, String> {
    let s = s.trim();
    if !s.is_ascii() || !s.len().is_multiple_of(2) {
        return Err("response_cache.master_key must be an even-length hex string".into());
    }
    let bytes = (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16))
        .collect::<Result<Vec<u8>, _>>()
        .map_err(|_| "response_cache.master_key contains non-hex characters".to_string())?;
    if bytes.len() < 32 {
        return Err(format!(
            "response_cache.master_key must decode to at least 32 bytes, got {}",
            bytes.len()
        ));
    }
    Ok(bytes)
}

/// Derive the response-cache storage key (AC6): hex Blake2s-256 over
/// (model ␟ canonical body JSON ␟ sorted anthropic-beta values ␟
/// anthropic-version ␟ URI query ␟ client_id ␟ fp ␟ fps ␟ surface). The
/// full-body digest is what carries correctness — two requests differing
/// anywhere, including a nested structural position, serialize differently
/// (`preserve_order` keeps the client's key order) and get different keys.
/// anthropic-version and the query string are keyed because they change the
/// RESPONSE schema for an identical body (an SDK upgrade mid-TTL must miss,
/// not replay the old shape). The 48-bit SipHash content fingerprints are
/// folded in to reuse the existing canonical machinery (per AC6), but are
/// deliberately not trusted alone: 48 bits over prompt content is collision
/// territory, and a key collision here would serve someone the wrong
/// completion. client_id in the material separates keys per client on top
/// of the per-tenant encryption (AC7). `surface` (LAB-929) separates
/// `/v1/messages` from `/v1/messages/count_tokens` so an identical body
/// posted to both endpoints never cross-serves — a count vs. a completion
/// are not interchangeable, no matter how the key material lines up
/// otherwise. The emitted key is a digest only — no prompt content ever
/// appears in a storage key or diagnostic line (AC5).
#[allow(clippy::too_many_arguments)]
fn response_cache_key(
    model: &str,
    body: &serde_json::Value,
    headers: &hyper::HeaderMap,
    query: Option<&str>,
    client_id: &str,
    fp: &str,
    fps: &str,
    surface: &str,
) -> String {
    use blake2::{Blake2s256, Digest};
    let mut betas: Vec<&str> = headers
        .get_all("anthropic-beta")
        .iter()
        .filter_map(|v| v.to_str().ok())
        .collect();
    betas.sort_unstable();
    let version = headers
        .get("anthropic-version")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    let canonical = serde_json::to_string(body).unwrap_or_default();
    let mut h = Blake2s256::new();
    for part in [
        model,
        &canonical,
        &betas.join(","),
        version,
        query.unwrap_or(""),
        client_id,
        fp,
        fps,
        surface,
    ] {
        h.update(part.as_bytes());
        h.update([0x1f]);
    }
    let digest = h.finalize();
    let mut out = String::with_capacity(64);
    for b in digest {
        use std::fmt::Write;
        let _ = write!(out, "{b:02x}");
    }
    out
}

impl ResponseCache {
    /// Storage-key namespace: entries land as `alb-resp:<digest>`.
    const NAMESPACE: &'static str = "alb-resp";
    const DEFAULT_TTL_SECS: u64 = 3600;
    const DEFAULT_OP_TIMEOUT_MS: u64 = 250;
    /// Per-client in-process L1 entries (ciphertext at rest). Together with
    /// `MAX_BODY_BYTES` this bounds worst-case L1 memory per client.
    const L1_CAPACITY: usize = 64;
    /// Bodies larger than this are not cached (the entry would also bump
    /// into cachekit's 5 MiB payload ceiling once encrypted + encoded).
    /// Bounds both backend value growth and worst-case L1 memory.
    const MAX_BODY_BYTES: usize = 1024 * 1024;

    /// Build from config. `Ok(None)` when the allow-list is empty (inert,
    /// AC2). Config errors — bad key, missing backend params — are `Err`:
    /// startup fails loudly rather than running with a silently-disabled
    /// cache. Backend REACHABILITY is not required at startup: connection
    /// failures are logged and every later operation fails open (AC10).
    async fn from_config(cfg: &ResponseCacheConfig) -> Result<Option<Self>, String> {
        if cfg.clients.is_empty() {
            return Ok(None);
        }
        let master_key = decode_hex_key(&cfg.master_key)?;
        let backend: cachekit::SharedBackend = match cfg.backend.as_str() {
            "redis" => {
                let url = cfg
                    .redis_url
                    .as_deref()
                    .ok_or("response_cache.redis_url is required for backend = \"redis\"")?;
                let b = cachekit::backend::redis::RedisBackend::builder()
                    .url(url)
                    .build()
                    .map_err(|e| format!("response_cache redis backend: {e}"))?;
                // fred's connection task detaches when the handle drops.
                // KNOWN LIMITATION (cachekit-rs 0.5.0, flagged upstream):
                // auto-reconnect is pub(crate) and unreachable from this
                // builder, so after a failed initial connect OR any later
                // Redis outage the client does NOT recover — every cache op
                // errors (fail-open, bounded by op_timeout per op) until the
                // process restarts. Watch anthropic_response_cache_errors_total.
                match b.connect().await {
                    Ok(_handle) => info!("response cache redis backend connected"),
                    Err(e) => {
                        warn!(error = %e, "response cache redis connect failed — cache disabled until restart (fail-open)")
                    }
                }
                std::sync::Arc::new(b)
            }
            "cachekitio" => {
                let api_key = cfg
                    .api_key
                    .as_deref()
                    .ok_or("response_cache.api_key is required for backend = \"cachekitio\"")?;
                let mut builder =
                    cachekit::backend::cachekitio::CachekitIO::builder().api_key(api_key);
                if let Some(url) = &cfg.api_url {
                    // allow_custom_host is required for hosts outside
                    // cachekit's built-in allow-list (api.cachekit.io /
                    // api.staging.cachekit.io) — e.g. the dev environment.
                    // The validator still enforces HTTPS and blocks
                    // private/loopback IPs regardless.
                    builder = builder.api_url(url).allow_custom_host(true);
                }
                std::sync::Arc::new(
                    builder
                        .build()
                        .map_err(|e| format!("response_cache cachekitio backend: {e}"))?,
                )
            }
            other => {
                return Err(format!(
                    "response_cache.backend must be \"cachekitio\" or \"redis\", got \"{other}\""
                ))
            }
        };
        Self::from_parts(
            backend,
            &cfg.clients,
            &master_key,
            Duration::from_secs(cfg.ttl_secs.unwrap_or(Self::DEFAULT_TTL_SECS)),
            Duration::from_millis(cfg.op_timeout_ms.unwrap_or(Self::DEFAULT_OP_TIMEOUT_MS)),
        )
        .map(Some)
    }

    /// Assemble per-client caches over a shared backend. Split out from
    /// `from_config` so tests can inject mock backends through the same
    /// construction path production uses.
    fn from_parts(
        backend: cachekit::SharedBackend,
        clients: &[String],
        master_key: &[u8],
        ttl: Duration,
        op_timeout: Duration,
    ) -> Result<Self, String> {
        let mut map = HashMap::new();
        for client_id in clients {
            if client_id == "-" {
                return Err(
                    "response_cache.clients cannot contain \"-\" (the unknown-client sentinel — it would opt in every unidentified caller)"
                        .into(),
                );
            }
            // The builder's default_ttl is the SINGLE source of entry
            // lifetime — writes use plain `set()` so the two can't drift.
            let ck = cachekit::CacheKit::builder()
                .backend(backend.clone())
                .default_ttl(ttl)
                .namespace(Self::NAMESPACE)
                .l1_capacity(Self::L1_CAPACITY)
                .encryption_from_bytes(master_key, client_id)
                .map_err(|e| format!("response_cache encryption init for \"{client_id}\": {e}"))?
                .build()
                .map_err(|e| format!("response_cache init for \"{client_id}\": {e}"))?;
            map.insert(client_id.clone(), ck);
        }
        Ok(Self {
            clients: map,
            op_timeout,
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            errors: AtomicU64::new(0),
            stores: AtomicU64::new(0),
            count_tokens_hits: AtomicU64::new(0),
            count_tokens_misses: AtomicU64::new(0),
            count_tokens_errors: AtomicU64::new(0),
            count_tokens_stores: AtomicU64::new(0),
        })
    }

    fn hits_for(&self, surface: CacheSurface) -> &AtomicU64 {
        match surface {
            CacheSurface::Messages => &self.hits,
            CacheSurface::CountTokens => &self.count_tokens_hits,
        }
    }

    fn misses_for(&self, surface: CacheSurface) -> &AtomicU64 {
        match surface {
            CacheSurface::Messages => &self.misses,
            CacheSurface::CountTokens => &self.count_tokens_misses,
        }
    }

    fn errors_for(&self, surface: CacheSurface) -> &AtomicU64 {
        match surface {
            CacheSurface::Messages => &self.errors,
            CacheSurface::CountTokens => &self.count_tokens_errors,
        }
    }

    fn stores_for(&self, surface: CacheSurface) -> &AtomicU64 {
        match surface {
            CacheSurface::Messages => &self.stores,
            CacheSurface::CountTokens => &self.count_tokens_stores,
        }
    }

    /// Read an entry. Every failure mode — no encryption handle, backend
    /// error, decrypt error, timeout — degrades to `None` and the request
    /// proceeds upstream (AC10). Diagnostics carry the key digest only,
    /// never content (AC5).
    async fn lookup(
        &self,
        client_id: &str,
        key: &str,
        surface: CacheSurface,
    ) -> Option<CachedResponse> {
        let cache = self.clients.get(client_id)?;
        let op = async { cache.secure()?.get::<CachedResponse>(key).await };
        match tokio::time::timeout(self.op_timeout, op).await {
            Ok(Ok(Some(entry))) => {
                self.hits_for(surface).fetch_add(1, Ordering::Relaxed);
                Some(entry)
            }
            Ok(Ok(None)) => {
                self.misses_for(surface).fetch_add(1, Ordering::Relaxed);
                None
            }
            Ok(Err(e)) => {
                self.errors_for(surface).fetch_add(1, Ordering::Relaxed);
                warn!(key_digest = key_digest_prefix(key), error = %e, "response cache read failed — proceeding upstream");
                None
            }
            Err(_) => {
                self.errors_for(surface).fetch_add(1, Ordering::Relaxed);
                warn!(
                    key_digest = key_digest_prefix(key),
                    timeout_ms = self.op_timeout.as_millis() as u64,
                    "response cache read timed out — proceeding upstream"
                );
                None
            }
        }
    }

    /// Write an entry. Failures are counted and logged (digest only) but
    /// never affect the client response (AC10).
    async fn store(
        &self,
        client_id: &str,
        key: &str,
        entry: &CachedResponse,
        surface: CacheSurface,
    ) {
        let Some(cache) = self.clients.get(client_id) else {
            return;
        };
        let op = async { cache.secure()?.set(key, entry).await };
        match tokio::time::timeout(self.op_timeout, op).await {
            Ok(Ok(())) => {
                self.stores_for(surface).fetch_add(1, Ordering::Relaxed);
            }
            Ok(Err(e)) => {
                self.errors_for(surface).fetch_add(1, Ordering::Relaxed);
                warn!(key_digest = key_digest_prefix(key), error = %e, "response cache write failed — skipped");
            }
            Err(_) => {
                self.errors_for(surface).fetch_add(1, Ordering::Relaxed);
                warn!(
                    key_digest = key_digest_prefix(key),
                    timeout_ms = self.op_timeout.as_millis() as u64,
                    "response cache write timed out — skipped"
                );
            }
        }
    }
}

/// Build the client-facing response for a cache hit. Only ever called for
/// entries written from 2xx responses (AC3). The entry came through AEAD
/// decryption, so an invalid stored status means OUR bug — surface a loud
/// 500, never a fabricated 200.
fn cached_hit_response(entry: CachedResponse) -> Response {
    let Ok(status) = StatusCode::from_u16(entry.status) else {
        error!(
            status = entry.status,
            "cached entry carries an invalid status code — refusing to serve"
        );
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            "cached response rebuild error",
        )
            .into_response();
    };
    let content_type = if entry.content_type.is_empty() {
        "application/json"
    } else {
        entry.content_type.as_str()
    };
    Response::builder()
        .status(status)
        .header("content-type", content_type)
        .header("x-alb-cache", "hit")
        .body(Body::from(entry.body))
        .unwrap_or_else(|_| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "cached response rebuild error",
            )
                .into_response()
        })
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

/// Parse a list of IP/CIDR strings into allow entries, panicking (startup) on
/// a malformed entry. Shared by `allowed_ips` and `trusted_proxies` — one
/// parse + panic semantics, named by `field` for the error.
fn parse_ip_entries(entries: Option<&[String]>, field: &str) -> Vec<IpAllowEntry> {
    entries
        .unwrap_or_default()
        .iter()
        .map(|s| {
            if let Ok(net) = s.parse::<IpNet>() {
                IpAllowEntry::Net(net)
            } else if let Ok(addr) = s.parse::<IpAddr>() {
                IpAllowEntry::Addr(addr)
            } else {
                panic!("invalid {field} entry: {s}");
            }
        })
        .collect()
}

/// Default failed-auth attempts per IP inside the window before 429.
const DEFAULT_AUTH_FAILURE_LIMIT: u32 = 10;
/// Default failed-auth throttle window.
const DEFAULT_AUTH_FAILURE_WINDOW_SECS: u64 = 300;
/// Hard cap on tracked IPs. The key is attacker-controlled, so an unbounded
/// map here would itself be the DoS the throttle exists to prevent (AC-12).
/// 4096 entries × ~64 bytes ≈ 256 KiB worst case — memory stays flat.
const AUTH_THROTTLE_CAPACITY: usize = 4096;
/// Minimum spacing between `allow_unauthenticated` admin-access warns per route.
const OPEN_ADMIN_WARN_INTERVAL: Duration = Duration::from_secs(300);
/// Minimum length of any configured credential (LAB-1192 AC-13). A static
/// bearer on a public ingress gets scanned; below this the keyspace is the
/// vulnerability. 32 chars = what `openssl rand -hex 32` (64 hex) exceeds.
const MIN_KEY_LEN: usize = 32;

/// Per-client-IP failed-authentication throttle (LAB-1192).
///
/// Fixed window per IP: the first failure starts the window, subsequent
/// failures increment the count, and once the count reaches `max_failures`,
/// further INVALID credentials from that IP get `429 + retry-after` until the
/// window expires. Valid credentials always pass: an IP may represent many
/// callers behind a NAT or load balancer, so one bad caller must not lock out
/// every authenticated neighbour sharing that address.
struct AuthThrottle {
    /// Failures per window before throttling. 0 = throttle disabled.
    max_failures: u32,
    window: Duration,
    capacity: usize,
    /// ip → (window_start, failures). Bounded by `capacity`: expired windows
    /// are purged first, then the least-established (lowest-count) live entry
    /// is evicted, so a flood of fresh failures cannot flush an active lockout.
    entries: Mutex<HashMap<IpAddr, (Instant, u32)>>,
}

impl AuthThrottle {
    fn new(max_failures: u32, window: Duration) -> Self {
        Self::with_capacity(max_failures, window, AUTH_THROTTLE_CAPACITY)
    }

    fn with_capacity(max_failures: u32, window: Duration, capacity: usize) -> Self {
        Self {
            max_failures,
            window,
            capacity,
            entries: Mutex::new(HashMap::new()),
        }
    }

    /// Returns `Some(retry_after_secs)` while `ip` is throttled. Expired
    /// windows are removed on sight, so steady-state size tracks only IPs
    /// that failed recently.
    fn check(&self, ip: &IpAddr) -> Option<u64> {
        if self.max_failures == 0 {
            return None;
        }
        let mut entries = self.entries.lock().ok()?;
        let (start, count) = *entries.get(ip)?;
        let elapsed = start.elapsed();
        if elapsed >= self.window {
            entries.remove(ip);
            return None;
        }
        if count < self.max_failures {
            return None;
        }
        // Round UP to whole seconds, never advertise 0: `as_secs()` truncates,
        // so a client honouring the header exactly would return a sub-second
        // early and eat a spurious extra 429.
        let remaining = self.window - elapsed;
        let secs = remaining.as_secs() + u64::from(remaining.subsec_nanos() > 0);
        Some(secs.max(1))
    }

    fn record_failure(&self, ip: IpAddr) {
        if self.max_failures == 0 {
            return;
        }
        let Ok(mut entries) = self.entries.lock() else {
            return;
        };
        match entries.get_mut(&ip) {
            Some((start, count)) => {
                if start.elapsed() >= self.window {
                    *start = Instant::now();
                    *count = 1;
                } else {
                    *count = count.saturating_add(1);
                }
            }
            None => {
                if entries.len() >= self.capacity {
                    // ponytail: O(n) purge + min-scan on insert at capacity —
                    // fires only with 4096 live entries under active attack; an
                    // LRU/heap if it ever shows up in a profile.
                    //
                    // Purge expired windows first — they are no active threat
                    // and the correct thing to drop. Only if the table is full
                    // of LIVE windows do we evict, and then the
                    // LEAST-established one (lowest failure count). An
                    // established lockout has `count >= max_failures`, strictly
                    // above an attacker's fresh `count = 1` floods, so a burst
                    // of new IPs cannot evict a real offender's lockout to reset
                    // it (AC-12: the eviction policy must not itself be an
                    // attacker's escape hatch).
                    let window = self.window;
                    entries.retain(|_, (start, _)| start.elapsed() < window);
                    if entries.len() >= self.capacity {
                        if let Some(victim) = entries
                            .iter()
                            .min_by_key(|(_, (start, count))| (*count, *start))
                            .map(|(k, _)| *k)
                        {
                            entries.remove(&victim);
                        }
                    }
                }
                entries.insert(ip, (Instant::now(), 1));
            }
        }
    }
}

impl AppState {
    fn is_ip_allowed(&self, ip: &IpAddr) -> bool {
        self.allowed_ips.is_empty() || self.allowed_ips.iter().any(|e| e.contains(ip))
    }

    /// Resolve the REAL client IP behind a trusted load balancer (LAB-1192).
    ///
    /// When the TCP peer is inside `trusted_proxies`, the client IP is the
    /// rightmost `x-forwarded-for` entry that is not itself a trusted proxy —
    /// the last hop an attacker cannot append to. In every other case the
    /// peer address wins and the header is ignored entirely: an XFF from an
    /// untrusted peer is attacker input, never trusted, never logged as
    /// authoritative. Malformed or empty entries abort the walk and fall back
    /// to the peer address — never guess.
    ///
    /// This is the ONE resolution function (AC-8): every handler calls it
    /// exactly once, immediately, and threads the result into the IP
    /// allowlist, authentication, `client_names`, budgets, the auth throttle
    /// and every log line. A second `client_addr.ip()` use inside a handler
    /// is a review defect.
    fn resolve_client_ip(&self, peer: IpAddr, headers: &hyper::HeaderMap) -> IpAddr {
        // Canonicalize an IPv4-mapped IPv6 peer (`::ffff:a.b.c.d`) to the bare
        // v4 address BEFORE the trust check. On a dual-stack listener (`::`)
        // v4 clients arrive mapped, and a v4 `trusted_proxies` CIDR would
        // otherwise never match — silently disabling XFF resolution and
        // letting the LB's address become the client IP for the allowlist,
        // throttle and budgets.
        let peer = peer.to_canonical();
        let is_trusted = |ip: &IpAddr| self.trusted_proxies.iter().any(|e| e.contains(ip));
        // `any()` over an empty list is already false, so an empty
        // `trusted_proxies` falls through here — direct-connection behaviour.
        if !is_trusted(&peer) {
            return peer;
        }
        // Multiple x-forwarded-for headers are one logical comma-joined list
        // (RFC 7230 §3.2.2); walk values last-to-first, entries right-to-left.
        for value in headers.get_all("x-forwarded-for").iter().rev() {
            let Ok(s) = value.to_str() else { return peer };
            for entry in s.rsplit(',') {
                let Ok(ip) = entry.trim().parse::<IpAddr>().map(|ip| ip.to_canonical()) else {
                    return peer;
                };
                if !is_trusted(&ip) {
                    return ip;
                }
            }
        }
        // Header absent, or every hop is a trusted proxy: the nearest
        // trusted peer is the most authoritative address we have.
        peer
    }

    /// Authenticate a request's credential (LAB-1083).
    ///
    /// Returns the authenticated principal when `[[clients]]` is configured,
    /// `None` when it is not (legacy `proxy_key`, or an open proxy), and a 401
    /// `Response` when a configured credential does not match.
    ///
    /// `allow_bearer` additionally accepts `Authorization: Bearer` — set only
    /// on the OpenAI-compat surface, whose SDKs send nothing else. It is NOT
    /// enabled on the native surface, where `Authorization` may legitimately
    /// carry the caller's own upstream token in `passthrough` mode; widening
    /// acceptance there would be a gratuitous auth surface on a ticket whose
    /// whole purpose is to narrow one.
    fn authenticate(
        &self,
        headers: &hyper::HeaderMap,
        allow_bearer: bool,
    ) -> Result<Option<&ClientConfig>, Box<Response>> {
        // Boxed Err, matching `reserve_request_body` — an inline `Response` is
        // 128+ bytes on the hot success path (clippy::result_large_err).
        let unauthorized = || -> Box<Response> {
            Box::new((StatusCode::UNAUTHORIZED, "unauthorized").into_response())
        };
        let from_header = headers.get("x-api-key").and_then(|v| v.to_str().ok());
        let from_bearer = if allow_bearer {
            headers
                .get("authorization")
                .and_then(|v| v.to_str().ok())
                .and_then(|v| {
                    // RFC 7235: auth scheme is case-insensitive
                    if v.len() >= 7 && v[..7].eq_ignore_ascii_case("bearer ") {
                        Some(&v[7..])
                    } else {
                        None
                    }
                })
        } else {
            None
        };

        if !self.clients.is_empty() {
            for presented in [from_header, from_bearer].into_iter().flatten() {
                if let Some(c) = self.match_client(presented) {
                    return Ok(Some(c));
                }
            }
            return Err(unauthorized());
        }

        // Legacy: one shared secret, no principal. Byte-for-byte the same
        // accept/reject decision as before LAB-1083 — only the comparison
        // primitive changed (audit finding 4).
        if let Some(ref key) = self.proxy_key {
            let ok = [from_header, from_bearer]
                .into_iter()
                .flatten()
                .any(|p| bool::from(key.as_bytes().ct_eq(p.as_bytes())));
            if !ok {
                return Err(unauthorized());
            }
        }
        Ok(None)
    }

    /// Authenticate, then throttle failed credentials (LAB-1193).
    ///
    /// This supersedes LAB-1192 AC-11's pre-comparison ordering after the
    /// 2026-08-24 shared-IP outage. The key comparison deliberately runs
    /// before the throttle decision.
    /// Valid credentials always pass, even when the resolved IP has an active
    /// failure window: behind NAT or an LB that IP may represent unrelated
    /// callers, and rejecting a known-good principal turns ten bad requests
    /// into a five-minute denial of service for every neighbour. Invalid
    /// credentials still get `429 + retry-after` once the IP reaches the
    /// limit, and successful traffic does NOT clear the shared failure state.
    /// This trade relies on `MIN_KEY_LEN` keeping credential guessing
    /// impractical; if the credential floor is lowered, restore AC-11's
    /// pre-comparison ordering.
    ///
    /// Every rejection is counted per route in
    /// `anthropic_auth_failures_total`; throttle 429s keep the metric rising
    /// through a sustained attack instead of plateauing at the limit.
    fn authenticate_throttled(
        &self,
        client_ip: &IpAddr,
        headers: &hyper::HeaderMap,
        allow_bearer: bool,
        route: &'static str,
    ) -> Result<Option<&ClientConfig>, Box<Response>> {
        match self.authenticate(headers, allow_bearer) {
            Ok(principal) => Ok(principal),
            Err(unauthorized) => {
                self.count_auth_failure(route);
                if let Some(retry_after) = self.auth_throttle.check(client_ip) {
                    warn!(
                        client = %client_ip,
                        route,
                        retry_after,
                        "rejected: failed-auth throttle active"
                    );
                    let mut resp = (
                        StatusCode::TOO_MANY_REQUESTS,
                        "too many failed authentication attempts",
                    )
                        .into_response();
                    resp.headers_mut()
                        .insert("retry-after", HeaderValue::from(retry_after));
                    return Err(Box::new(resp));
                }
                self.auth_throttle.record_failure(*client_ip);
                warn!(client = %client_ip, route, "rejected: invalid or missing credential");
                Err(unauthorized)
            }
        }
    }

    fn count_auth_failure(&self, route: &'static str) {
        if let Ok(mut counts) = self.auth_failures.lock() {
            *counts.entry(route).or_insert(0) += 1;
        }
    }

    /// Gate an admin surface (`/_stats`, `/metrics`) behind an OPERATOR
    /// principal (LAB-1192 AC-4). Returns the rejection response, or `None`
    /// when the caller may proceed.
    ///
    /// Under `[[clients]]`: unauthenticated → 401, authenticated
    /// non-operator → 403 — `/_stats` discloses other clients' ids and the
    /// endpoint account names, which a per-client key holder has no business
    /// reading. Under legacy `proxy_key`, a valid key serves: one shared
    /// secret means the key holder IS the operator. Under
    /// `allow_unauthenticated` (the only way to boot with no credentials),
    /// both surfaces serve but each access logs at `warn` (AC-5) so the open
    /// posture stays visible even on a trusted network.
    fn authorize_admin(
        &self,
        client_ip: &IpAddr,
        headers: &hyper::HeaderMap,
        route: &'static str,
    ) -> Option<Box<Response>> {
        match self.authenticate_throttled(client_ip, headers, false, route) {
            Err(resp) => Some(resp),
            Ok(Some(c)) if !self.is_operator(&c.name) => {
                warn!(
                    client = %client_ip,
                    client_id = %c.name,
                    route,
                    "rejected: admin surface requires an operator principal"
                );
                Some(Box::new(
                    (
                        StatusCode::FORBIDDEN,
                        "forbidden: operator principal required",
                    )
                        .into_response(),
                ))
            }
            Ok(Some(_)) => None,
            Ok(None) => {
                // allow_unauthenticated: keep the open posture VISIBLE (AC-5)
                // but rate-limit the warn per route — in the lab a vmagent
                // scrapes /metrics every ~15s, and one warn per scrape is
                // ~11k lines/day/replica that drowns the signal it exists to
                // give. Once per route per OPEN_ADMIN_WARN_INTERVAL preserves
                // visibility without the firehose.
                if self.proxy_key.is_none() && self.should_warn_open_admin(route) {
                    warn!(
                        client = %client_ip,
                        route,
                        "unauthenticated admin access (allow_unauthenticated) — trusted-network-only; rate-limited log"
                    );
                }
                None
            }
        }
    }

    /// Rate-limiter for the `allow_unauthenticated` admin-access warn: true at
    /// most once per route per `OPEN_ADMIN_WARN_INTERVAL`.
    fn should_warn_open_admin(&self, route: &'static str) -> bool {
        let Ok(mut last) = self.open_admin_warn.lock() else {
            return true;
        };
        let now = Instant::now();
        match last.get(route) {
            Some(t) if t.elapsed() < OPEN_ADMIN_WARN_INTERVAL => false,
            _ => {
                last.insert(route, now);
                true
            }
        }
    }

    /// Constant-time lookup of a presented credential in the client table.
    ///
    /// What this actually guarantees, precisely — do not read more into it:
    /// `ct_eq` removes the per-byte prefix oracle that `==` has, which is the
    /// leak that matters (it is what lets an attacker recover a key byte by
    /// byte). Audit finding 4, closed.
    ///
    /// The full-table scan removes the coarse "how far down the table did we
    /// get" signal. It does NOT make the whole function constant-time: the
    /// `hit = Some(c)` store is a data-dependent branch the optimizer may
    /// emit as one, and `ct_eq` short-circuits on unequal lengths. Both
    /// residuals are a handful of instructions against milliseconds of network
    /// and TLS jitter, and key length is not a secret — so neither is
    /// exploitable remotely. Want the stronger property? Accumulate with
    /// `subtle::Choice`; don't assume this already does.
    fn match_client(&self, presented: &str) -> Option<&ClientConfig> {
        let mut hit: Option<&ClientConfig> = None;
        for c in &self.clients {
            if bool::from(c.key.as_bytes().ct_eq(presented.as_bytes())) {
                hit = Some(c);
            }
        }
        hit
    }

    /// Whether `client_id` may request `model` (LAB-1083).
    ///
    /// Unknown client, or a client with an empty list, allows everything.
    /// "Unknown client" is only reachable on the legacy path: with
    /// `[[clients]]` configured, `client_id` is always a principal name.
    ///
    /// FAILS CLOSED on an empty `model`, and this is the one place where the
    /// shared matcher's semantics are deliberately NOT inherited. An empty
    /// model means the caller's model is UNKNOWN to us — the body did not
    /// parse as JSON, or it carries no top-level `model` (the batches API
    /// nests it under `requests[].params.model`, and `proxy_handler` is the
    /// catch-all route). For `Endpoint::serves_model` "unknown" rightly means
    /// "don't narrow the routing pool"; for a policy gate it must mean
    /// "deny", or a restricted client reaches any model by sending a body we
    /// cannot read.
    fn client_allows_model(&self, client_id: &str, model: &str) -> bool {
        match self.clients.iter().find(|c| c.name == client_id) {
            Some(c) if c.models.is_empty() => true,
            Some(c) => !model.is_empty() && model_matches(&c.models, model),
            None => true,
        }
    }

    /// Count + log a model-allowlist denial.
    ///
    /// The model string is caller-controlled and bounded only by the request
    /// body cap, so it is truncated BEFORE becoming a map key: an untruncated
    /// label would be retained for the process lifetime and re-serialized into
    /// the `/metrics` body on every scrape. Label COUNT is separately
    /// hard-bounded at `MAX_MODEL_DENIED_LABELS` + 1: once the cap is
    /// reached, every new pair lumps into a single global
    /// `("_other", "_other")` bucket.
    ///
    /// Logs at `warn` the first time a (client, model) pair is denied and at
    /// `debug` thereafter — a client hammering a denied model must not be able
    /// to drive unbounded warn-level log volume. Pairs lumped into the
    /// overflow bucket share its first-seen flag (deliberate: client-id
    /// rotation must not mint warns). The counter still records every denial.
    /// Mirrors the once-per-model pattern used for unsupported-model
    /// warnings.
    fn note_model_denied(&self, client_id: &str, model: &str) {
        let model = truncate_label(model);
        let mut first_time = true;
        if let Ok(mut counts) = self.model_denied.lock() {
            let key = (client_id.to_owned(), model.clone());
            let label = if counts.len() < MAX_MODEL_DENIED_LABELS || counts.contains_key(&key) {
                key
            } else {
                // Map full and this pair is new: lump into ONE global
                // overflow bucket — hard bound of MAX_MODEL_DENIED_LABELS
                // + 1 entries. A per-client ("<client>", "_other") key
                // would let x-client-id rotation (legacy auth modes) grow
                // the map without bound (expert-panel finding, LAB-2330;
                // mirrored here by LAB-2332).
                ("_other".to_owned(), "_other".to_owned())
            };
            let entry = counts.entry(label).or_insert(0);
            first_time = *entry == 0;
            *entry += 1;
        }
        if first_time {
            warn!(
                client_id = %client_id,
                model = %model,
                "rejected: model not in client allow-list"
            );
        } else {
            debug!(
                client_id = %client_id,
                model = %model,
                "rejected: model not in client allow-list"
            );
        }
    }

    /// Resolve client identity: x-client-id header → IP map fallback → "-"
    ///
    /// Header takes precedence to support multiple clients per IP.
    ///
    /// ONLY reached when no `[[clients]]` table is configured. Under
    /// `[[clients]]`, identity comes from the verified credential
    /// (`RequestContext::from_request`) and this client-asserted path is dead.
    ///
    /// The `debug_assert` is the choke point for that invariant. Nothing in the
    /// type system stops a future handler from calling
    /// `RequestContext::from_request(.., None)` and silently reinstating
    /// header-asserted identity; this makes that mistake fail loudly in tests
    /// and debug builds instead of quietly becoming an identity bypass.
    fn resolve_client_id(&self, ip: &IpAddr, headers: &hyper::HeaderMap) -> String {
        debug_assert!(
            self.clients.is_empty(),
            "resolve_client_id reached with [[clients]] configured — a caller \
             skipped the authenticated principal and identity is now spoofable"
        );
        if let Some(id) = headers.get("x-client-id").and_then(|v| v.to_str().ok()) {
            let id = id.trim();
            // "_operator" is the reserved operator-aggregation label on
            // /_stats and /metrics — a self-asserted claim to it would merge
            // this caller's usage into the hidden operator bucket. "_other"
            // is the reserved metrics overflow-bucket label (LAB-2330/2332) —
            // claiming it would merge this caller into the overflow key.
            if !id.is_empty() && id != "-" && id != "_operator" && id != "_other" {
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
        if let Some(redis) = self.coordination_redis() {
            let lock_key = format!("alb:probe:{}:{}", ep.name, model);
            let lock_ttl = self.probe_interval_secs.max(1);
            // SET NX EX: OK reply when acquired, nil (None) when another
            // replica already holds the lock.
            let acquired: Result<Option<String>, fred::error::RedisError> = redis
                .set(
                    lock_key.as_str(),
                    1,
                    Some(Expiration::EX(lock_ttl as i64)),
                    Some(SetOptions::NX),
                    false,
                )
                .await;
            match acquired {
                Ok(Some(_)) => {} // Lock acquired, proceed with probe
                Ok(None) => {
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
        // Probe headers carry only OAUTH_BETA_FLAGS, which the filter never
        // drops (they are unconditionally re-added), so the returned drop
        // list is provably empty — nothing to record.
        let _ = inject_account_auth(
            &mut headers,
            &ep.token,
            ep.passthrough,
            &self.allowed_client_betas,
        );

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

/// How long a learned "(endpoint, model) unsupported" verdict keeps that
/// endpoint out of the model's routing pool (LAB-941). An account's model set
/// changes only on plan/gateway updates, so a long hold is safe; the TTL is
/// the self-heal path when it does change (no restart needed). The cost of an
/// expiry is one wasted upstream attempt to re-learn the rejection.
/// ponytail: constant, add a config knob if operators ever need to tune it.
const UNSUPPORTED_MODEL_TTL: Duration = Duration::from_secs(900);

/// Bound on distinct learned (endpoint, model) rejections. Model names are
/// client-supplied, so without a cap a client spraying junk model names could
/// grow the map without limit. When full, new learns are dropped (that only
/// costs the pre-LAB-941 behaviour) and TTL expiry drains the map.
const UNSUPPORTED_MODEL_MAX: usize = 256;

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

/// Expiry on `alb:budget:{client}:{day}` keys, refreshed on every INCRBY.
/// 48h: a daily counter only needs to survive its own day plus enough slack
/// for stats/aggregation to read yesterday; after two days it is garbage.
const BUDGET_TTL_SECS: i64 = 172_800;

/// Per-command budget for the coordination client (fred's
/// `default_command_timeout`), carried over from the old ConnectionManager's
/// 2s response timeout.
const REDIS_COMMAND_TIMEOUT: Duration = Duration::from_secs(2);

/// How long after startup an unconnected coordination backend earns its one
/// WARN (`spawn_redis_connect_watcher`). Matches the connection budget the
/// old blocking `init()` gave the first attempt, so the log fires on the
/// same timeline operators already know — it just no longer implies
/// permanence.
const REDIS_STARTUP_GRACE: Duration = Duration::from_secs(5);

/// Build the coordination Redis client and spawn its connection task WITHOUT
/// waiting for the first connect (LAB-1639). `Err` only for an unparseable
/// URL — an unreachable backend is not an error here.
///
/// `fail_fast = false` routes the INITIAL connect through `policy` — the
/// same retry loop that already handles mid-run drops — instead of fred's
/// default single attempt. `connect()` (not `init()`) spawns that task and
/// returns immediately: with a retry-forever policy, awaiting the first
/// connect would block the caller for the whole backend outage, so a pod
/// cold-starting during a Redis window would never bind its listener — a
/// full LB outage instead of degraded local-only mode. The returned handle
/// detaches on drop; the task keeps driving the connection (and reconnects)
/// for the lifetime of the client.
fn start_coordination_redis(
    url: &str,
    perf: PerformanceConfig,
    conn_config: ConnectionConfig,
    policy: ReconnectPolicy,
) -> Result<RedisClient, fred::error::RedisError> {
    let mut redis_config = RedisConfig::from_url(url)?;
    redis_config.fail_fast = false;
    let client = RedisClient::new(redis_config, Some(perf), Some(conn_config), Some(policy));
    // Connection transitions are sparse, high-signal events; log them so a
    // flapping backend is visible in operator logs. fred emits this on EVERY
    // successful connection establishment, the first one included.
    client.on_reconnect(|server| {
        info!(%server, "redis reconnected for distributed state");
        Ok(())
    });
    // Connection-level errors are otherwise INVISIBLE under a retry-forever
    // policy: failed attempts broadcast errors, never a connect result, so
    // neither wait_for_connect nor on_reconnect ever fires for them. A
    // persistently wrong password (NOAUTH retries forever in fred) would be
    // indistinguishable from a backend outage without this. Rate-limited to
    // one line per 60s — the backoff ramp starts sub-second.
    let last_error_log = AtomicU64::new(0);
    client.on_error(move |error| {
        let now = AppState::now_epoch();
        let prev = last_error_log.load(Ordering::Relaxed);
        if now.saturating_sub(prev) >= 60
            && last_error_log
                .compare_exchange(prev, now, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
        {
            warn!(%error, "redis connection error — reconnect policy retrying");
        }
        Ok(())
    });
    let _connect_task = client.connect();
    Ok(client)
}

/// `SET key value EX ttl` — the one-line convenience the redis crate's
/// `set_ex` provided; fred's five-argument `set` buries the common case in
/// `None, false` noise at every call site.
async fn redis_set_ex(
    client: &RedisClient,
    key: &str,
    value: String,
    ttl_secs: i64,
) -> Result<(), fred::error::RedisError> {
    client
        .set(key, value, Some(Expiration::EX(ttl_secs)), None, false)
        .await
}

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

/// Default `anthropic-beta` flags a client may forward upstream on OAuth
/// endpoints (LAB-1191 / audit finding 5). Contains the flags the LB itself
/// depends on (`OAUTH_BETA_FLAGS`, the `context-1m*` flag the context-window
/// accounting reads) plus the flag FAMILIES Claude Code sends on every
/// request, wildcarded on their date suffix so a Claude Code auto-update
/// that bumps a date can't silently degrade the proxy's primary traffic.
/// A configured `allowed_client_betas` REPLACES this list (it does not
/// extend it) — copy these entries alongside any addition; "*" is a suffix
/// wildcard.
const DEFAULT_CLIENT_BETA_ALLOWLIST: &[&str] = &[
    "oauth-2025-04-20",
    "claude-code-20250219",
    "interleaved-thinking-*",
    "fine-grained-tool-streaming-*",
    "prompt-caching-*",
    "context-1m*",
    // The rest of what Claude Code 2.1.x sends on every request (inventory
    // taken from anthropic_beta_flag_dropped_total on the lab fleet,
    // 2026-08-01). The first cut of this list under-enumerated them, which
    // 400'd all primary traffic: several of these flags have a BODY-side
    // counterpart the LB forwards verbatim (context-management →
    // `context_management`, structured-outputs → `output_format`,
    // extended-cache-ttl → `cache_control.ttl`), so stripping only the
    // header leaves an incoherent request that upstream rejects outright
    // rather than degrading to the non-beta behaviour.
    "context-management-*",
    "structured-outputs-*",
    "extended-cache-ttl-*",
    "effort-*",
    "thinking-token-count-*",
    "mid-conversation-system-*",
    "advisor-tool-*",
    "fallback-credit-*",
    "redact-thinking-*",
    "afk-mode-*",
];

/// Cardinality bound for `beta_flags_dropped` — flag names are
/// client-controlled input. Past the cap, drops count under `_other`.
const MAX_DROPPED_BETA_FLAGS: usize = 50;

/// Length bound for a single flag key in `beta_flags_dropped` and its warn
/// log — the value is client-controlled, and 50 multi-kilobyte keys replayed
/// into every `/metrics` scrape is the cardinality decision's spirit broken
/// by size instead of count.
const MAX_DROPPED_BETA_FLAG_LEN: usize = 64;

/// "*" suffix-wildcard match, shared by the model allowlist
/// (`Endpoint::serves_model`) and the beta-flag allowlist
/// (`beta_flag_allowed`) so the two can never drift.
fn suffix_wildcard_match(pattern: &str, value: &str) -> bool {
    if let Some(prefix) = pattern.strip_suffix('*') {
        value.starts_with(prefix)
    } else {
        value == pattern
    }
}

fn beta_flag_allowed(allowed: &[String], flag: &str) -> bool {
    allowed.iter().any(|p| suffix_wildcard_match(p, flag))
}

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

    /// Record that `endpoint_idx` cannot serve `model` — the upstream itself
    /// said so (LAB-941). Routing skips the pair for UNSUPPORTED_MODEL_TTL.
    fn note_model_unsupported(&self, endpoint_name: &str, endpoint_idx: usize, model: &str) {
        if model.is_empty() {
            return;
        }
        let now = Instant::now();
        let Ok(mut map) = self.unsupported_models.lock() else {
            return;
        };
        map.retain(|_, expiry| *expiry > now);
        // Capacity gates NEW pairs only — refreshing an existing pair's TTL
        // doesn't grow the map and must not starve under sustained rejections.
        let key = (endpoint_idx, model.to_string());
        if !map.contains_key(&key) && map.len() >= UNSUPPORTED_MODEL_MAX {
            return;
        }
        warn!(
            account = endpoint_name,
            model,
            cooldown_secs = UNSUPPORTED_MODEL_TTL.as_secs(),
            "model unsupported on account, routing away"
        );
        map.insert(key, now + UNSUPPORTED_MODEL_TTL);
    }

    /// Endpoint indices currently marked unsupported for `model`. One lock +
    /// full scan per pick — the map is capped at UNSUPPORTED_MODEL_MAX and
    /// empty in the common case.
    fn unsupported_endpoints_for(&self, model: &str) -> Vec<usize> {
        if model.is_empty() {
            return Vec::new();
        }
        let now = Instant::now();
        match self.unsupported_models.lock() {
            Ok(map) => map
                .iter()
                .filter(|((_, m), expiry)| m == model && **expiry > now)
                .map(|((idx, _), _)| *idx)
                .collect(),
            Err(_) => Vec::new(),
        }
    }

    /// True when EVERY endpoint whose config allows `model` carries a live
    /// unsupported-model entry — the pool cannot serve the model at all,
    /// regardless of capacity. Used at exhaustion: on a warm negative cache
    /// the candidate pool empties before any forward attempt runs, so there
    /// is no stashed upstream 404 — this check lets the handler synthesize
    /// one instead of degrading to a retryable 429 (LAB-941 follow-up).
    /// Endpoints excluded by their config `models` allowlist never serve the
    /// model and don't count; false when no endpoint could ever serve it
    /// (config-only exclusion keeps its pre-existing 429 semantics).
    fn model_unsupported_everywhere(&self, model: &str) -> bool {
        if model.is_empty() {
            return false;
        }
        let unsupported = self.unsupported_endpoints_for(model);
        let mut eligible = 0usize;
        for (i, ep) in self.endpoints.iter().enumerate() {
            if !ep.serves_model(model) {
                continue;
            }
            eligible += 1;
            if !unsupported.contains(&i) {
                return false;
            }
        }
        eligible > 0
    }

    async fn routing_candidates(&self, model: &str, skip: &[EndpointIdx]) -> Vec<RoutingCandidate> {
        let now = Instant::now();
        let now_epoch = Self::now_epoch();
        let unsupported = self.unsupported_endpoints_for(model);
        let mut candidates: Vec<RoutingCandidate> = Vec::new();
        for (i, ep) in self.endpoints.iter().enumerate() {
            if skip.contains(&i) {
                continue;
            }
            if !ep.serves_model(model) {
                continue;
            }
            if unsupported.contains(&i) {
                trace!(
                    endpoint = ep.name,
                    model,
                    "pick: skipping, model unsupported on endpoint"
                );
                continue;
            }
            match ep.protocol {
                Protocol::OpenAI => {
                    // OpenAI endpoints carry no utilization data, but they DO
                    // carry 429 hard-limit state and transport health — an
                    // endpoint that told us to back off, or is circuit-broken,
                    // leaves the pool the same way an Anthropic one does. If
                    // the whole pool is excluded this fails closed (429); the
                    // cooldown bounds the window.
                    {
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

    /// Redis handle for coordination reads/writes, or `None` while the
    /// client has never yet connected (a process that started during a
    /// backend outage — LAB-1639). Before the first connect fred BUFFERS
    /// every command for `REDIS_COMMAND_TIMEOUT` (2s) and each failure would
    /// WARN, so coordination ops skip outright: no stalls, no per-operation
    /// log spam — the startup grace WARN is the one signal. After the first
    /// connect this is permanently `Some`: mid-run drops keep the LAB-932
    /// contract (bounded 2s buffered failures, request paths gated on
    /// `is_connected`). The `is_connected()` arm only covers the moments
    /// between an early first connect and the watcher task storing the flag.
    fn coordination_redis(&self) -> Option<&RedisClient> {
        let redis = self.redis.as_ref()?;
        if self.redis_ever_connected.load(Ordering::Relaxed) || redis.is_connected() {
            Some(redis)
        } else {
            None
        }
    }

    /// Spawn the tasks that observe the coordination client's FIRST connect
    /// (LAB-1639). Call once after construction; no-op without Redis.
    /// One task records the connect — permanently opening the
    /// `coordination_redis` gate — and one emits the single startup WARN if
    /// the backend is still unreachable after `REDIS_STARTUP_GRACE`.
    fn spawn_redis_connect_watcher(self: &Arc<Self>) {
        let Some(client) = self.redis.clone() else {
            return;
        };
        // Belt for the recorder task below: fred's wait_for_connect reads
        // client state THEN subscribes, so a connect landing between those
        // two ops is missed until the next reconnect — with a mid-run drop
        // in between, the gate would wrongly read closed and skip writes
        // LAB-932 buffers. This subscription fires on every successful
        // connection (the first included), so the flag cannot lag reality
        // past one broadcast.
        let state = self.clone();
        client.on_reconnect(move |_server| {
            state.redis_ever_connected.store(true, Ordering::Relaxed);
            Ok(())
        });
        let state = self.clone();
        let waiter = client.clone();
        tokio::spawn(async move {
            // Resolves immediately when already connected. With a
            // retry-forever policy it otherwise resolves only on success:
            // failed attempts broadcast errors, not connect results.
            match waiter.wait_for_connect().await {
                Ok(()) => {
                    state.redis_ever_connected.store(true, Ordering::Relaxed);
                    info!("redis connected for distributed state");
                }
                // Only broadcast when the connection task exits for good
                // (non-retryable config/URL-class errors). Without this arm
                // that exit would be silent and the startup WARN's "until
                // the backend becomes reachable" a lie.
                Err(e) => error!(
                    error = %e,
                    "redis connection task exited before first connect — coordination stays local-only"
                ),
            }
        });
        let state = self.clone();
        tokio::spawn(async move {
            tokio::time::sleep(REDIS_STARTUP_GRACE).await;
            // Double-check is_connected: a connect racing the recorder task
            // above must not produce a false outage WARN.
            if !state.redis_ever_connected.load(Ordering::Relaxed) && !client.is_connected() {
                warn!(
                    "redis unreachable at startup — running local-only until the backend becomes reachable"
                );
            }
        });
    }

    /// Publish precomputed routing weights to Redis so non-probing pods
    /// can set their gauge atomics without recomputing.
    async fn publish_routing_weights(&self) {
        let redis = match self.coordination_redis() {
            Some(r) => r,
            None => return,
        };
        let ttl = Self::routing_weight_publish_ttl(self.probe_interval_secs);
        let publish = |name: &str, weight: &AtomicU64, share: &AtomicU64, gate: &AtomicU64| {
            let w = f64::from_bits(weight.load(Ordering::Relaxed));
            let s = f64::from_bits(share.load(Ordering::Relaxed));
            let g = f64::from_bits(gate.load(Ordering::Relaxed));
            let key = format!("alb:weight:{}", name);
            let val = format!("{w},{s},{g}");
            let conn = redis.clone();
            tokio::spawn(async move {
                if let Err(e) = redis_set_ex(&conn, &key, val, ttl as i64).await {
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
        if let Some(redis) = self.coordination_redis() {
            let conn = redis.clone();
            let key = format!("alb:hard:{}", endpoint_name);
            let now_epoch = Self::now_epoch();
            // Lua CAS: only write the sentinel if the current value is absent,
            // already the sentinel, or an expired hard-limit (epoch <= now).
            // Rejects a concurrent mark_hard_limited write with epoch > now.
            const RECOVERY_CAS_SCRIPT: &str = r#"
                local current = redis.call('GET', KEYS[1])
                if current == false then
                    return redis.call('SET', KEYS[1], ARGV[1], 'EX', ARGV[2])
                end
                local n = tonumber(current)
                if n == nil or n <= tonumber(ARGV[3]) then
                    return redis.call('SET', KEYS[1], ARGV[1], 'EX', ARGV[2])
                end
                return 0
                "#;
            tokio::spawn(async move {
                // Args travel as decimal strings — byte-identical to the wire
                // encoding the redis crate used, so the stored sentinel still
                // parses as u64 on the sync_from_redis read side.
                let result: Result<RedisValue, fred::error::RedisError> = conn
                    .eval(
                        RECOVERY_CAS_SCRIPT,
                        vec![key],
                        vec![
                            HARD_LIMIT_CLEARED_SENTINEL.to_string(),
                            HARD_LIMIT_SENTINEL_TTL_SECS.to_string(),
                            now_epoch.to_string(),
                        ],
                    )
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
        if let Some(redis) = self.coordination_redis() {
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

            let conn = redis.clone();
            let key = format!("alb:rate:{}", endpoint_name);
            tokio::spawn(async move {
                if let Ok(json) = serde_json::to_string(&rate_data) {
                    if let Err(e) = redis_set_ex(&conn, &key, json, ttl as i64).await {
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
        if let Some(redis) = self.coordination_redis() {
            let conn = redis.clone();
            let key = format!("alb:hard:{}", endpoint_name);
            let until_epoch = Self::now_epoch()
                + cooldown.as_secs()
                + if cooldown.subsec_nanos() > 0 { 1 } else { 0 };
            let ttl = cooldown.as_secs().max(1);
            tokio::spawn(async move {
                if let Err(e) = redis_set_ex(&conn, &key, until_epoch.to_string(), ttl as i64).await
                {
                    tracing::warn!(error = %e, "redis SET EX failed for hard-limit propagation");
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
        let redis = match self.coordination_redis() {
            Some(r) => r,
            None => return,
        };
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

        if let Ok(values) = redis.mget::<Vec<Option<String>>, _>(hard_keys).await {
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

        if let Ok(values) = redis.mget::<Vec<Option<String>>, _>(rate_keys).await {
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
        if let Ok(values) = redis.mget::<Vec<Option<String>>, _>(weight_keys).await {
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

    /// Log + count client `anthropic-beta` flags dropped by the allow-list
    /// (AC-12: silent stripping is not acceptable — a caller whose feature
    /// vanished must be diagnosable from the logs and
    /// `anthropic_beta_flag_dropped_total{flag}`). Flag names are
    /// client-controlled, so the counter map is capped at
    /// `MAX_DROPPED_BETA_FLAGS` distinct flags (overflow counts as `_other`)
    /// and each key is truncated to `MAX_DROPPED_BETA_FLAG_LEN` bytes —
    /// count-capped but length-unbounded keys would still bloat every
    /// `/metrics` scrape.
    fn record_dropped_beta_flags(&self, client_id: &str, dropped: &[String]) {
        if dropped.is_empty() {
            return;
        }
        let truncated: Vec<&str> = dropped
            .iter()
            .map(|f| {
                let mut end = f.len().min(MAX_DROPPED_BETA_FLAG_LEN);
                while !f.is_char_boundary(end) {
                    end -= 1;
                }
                &f[..end]
            })
            .collect();
        let Ok(mut map) = self.beta_flags_dropped.lock() else {
            return;
        };
        // Loud line only on a flag's FIRST sighting — a misconfigured client
        // sends the same unlisted flag at request rate, and the counter
        // already carries the volume. Repeats log at debug for correlation.
        let mut first_seen: Vec<&str> = Vec::new();
        for flag in &truncated {
            if map.contains_key(*flag) || map.len() < MAX_DROPPED_BETA_FLAGS {
                if !map.contains_key(*flag) {
                    first_seen.push(flag);
                }
                *map.entry(flag.to_string()).or_insert(0) += 1;
            } else {
                *map.entry("_other".to_string()).or_insert(0) += 1;
            }
        }
        drop(map);
        if !first_seen.is_empty() {
            warn!(
                client_id,
                flags = %first_seen.join(","),
                "dropped client anthropic-beta flags not on the allow-list \
                 (a configured allowed_client_betas REPLACES the default list — \
                 include the defaults plus the flags to permit)"
            );
        }
        // The repeat subset gets its own debug line even when the same call
        // also carried a first sighting — every dropped flag leaves a log
        // trace on every request it was dropped from (AC-12).
        let repeats: Vec<&str> = truncated
            .iter()
            .filter(|f| !first_seen.contains(f))
            .copied()
            .collect();
        if !repeats.is_empty() {
            debug!(
                client_id,
                flags = %repeats.join(","),
                "dropped client anthropic-beta flags (previously reported)"
            );
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
        let redis = match self.coordination_redis() {
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
            let result: Result<(), fred::error::RedisError> = redis
                .expire(TRANSPORT_ERRORS_KEY, TRANSPORT_ERRORS_TTL_SECS as i64)
                .await;
            if let Err(e) = result {
                warn!(error = %e, "redis EXPIRE failed for transport-errors TTL refresh");
            }
            return;
        }

        // One round-trip: HINCRBY every kind, then refresh the TTL. fred
        // pipeline commands resolve immediately when queued; `all()` sends the
        // batch and surfaces the first error, matching the old atomic
        // success-or-requeue contract.
        let pipe = redis.pipeline();
        for (kind, n) in &deltas {
            let _: Result<(), fred::error::RedisError> =
                pipe.hincrby(TRANSPORT_ERRORS_KEY, *kind, *n as i64).await;
        }
        let _: Result<(), fred::error::RedisError> = pipe
            .expire(TRANSPORT_ERRORS_KEY, TRANSPORT_ERRORS_TTL_SECS as i64)
            .await;

        let result: Result<Vec<RedisValue>, fred::error::RedisError> = pipe.all().await;
        if let Err(e) = result {
            // Redis is unreachable (or the pipeline reply was lost) — return the
            // drained deltas to the local accumulator so error signal is not
            // dropped. This is at-least-once: if the connection died AFTER Redis
            // applied some HINCRBYs, re-queuing can over-count by a few next
            // tick. For an error *counter* that bias is correct — a slight
            // over-report beats a silently missed egress fault. (Contrast
            // record_budget_usage, which never retries a failed INCRBY:
            // over-counting a budget would wrongly throttle a client, so the
            // lost increment is covered by the local floor instead —
            // LAB-1962.) The deltas also stay
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
        let redis = self.coordination_redis()?;
        let mut redis_ok = true;

        // Count active replicas via SCAN (non-blocking, unlike KEYS). The
        // cursor is driven manually as a plain command per page — NOT via
        // fred's scan stream. The stream's pages arrive over a channel that
        // `default_command_timeout` does not cover, and abandoning the stream
        // does not stop the scan: `ScanResult`'s Drop impl auto-requests the
        // next page with nobody listening, so every abandoned scan keeps
        // walking the keyspace against a backend that is already slow. A
        // manual cursor loop keeps each page under the ordinary 2s command
        // budget and genuinely ends the scan when this function returns.
        let mut replicas = 0u64;
        let mut cursor = String::from("0");
        loop {
            let result: Result<(String, Vec<String>), fred::error::RedisError> = redis
                .custom(
                    fred::types::CustomCommand::new_static(
                        "SCAN",
                        fred::types::ClusterHash::FirstKey,
                        false,
                    ),
                    vec![
                        cursor,
                        "MATCH".to_string(),
                        "alb:heartbeat:*".to_string(),
                        "COUNT".to_string(),
                        "100".to_string(),
                    ],
                )
                .await;
            match result {
                Ok((next_cursor, keys)) => {
                    replicas += keys.len() as u64;
                    cursor = next_cursor;
                    if cursor == "0" {
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
            match redis.mget::<Vec<Option<u64>>, _>(budget_keys).await {
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
            match redis
                .hgetall::<HashMap<String, u64>, _>(TRANSPORT_ERRORS_KEY)
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

    /// Parse usage from an OpenAI-format response body (non-streaming JSON).
    /// OpenAI reports `prompt_tokens`/`completion_tokens`; there is no
    /// Anthropic-style cache-token split, so those fields stay 0.
    fn from_openai_response_body(body: &serde_json::Value) -> Self {
        Self {
            input_tokens: body
                .pointer("/usage/prompt_tokens")
                .and_then(|v| v.as_u64())
                .unwrap_or(0),
            output_tokens: body
                .pointer("/usage/completion_tokens")
                .and_then(|v| v.as_u64())
                .unwrap_or(0),
            ..Self::default()
        }
    }

    /// Parse usage from a complete SSE transcript. Test convenience wrapper —
    /// production streaming paths feed chunks through `SseUsageScanner`
    /// incrementally instead of buffering the stream.
    #[cfg(test)]
    fn from_sse_text(text: &str) -> Self {
        let mut scanner = SseUsageScanner::default();
        scanner.push(text.as_bytes());
        scanner.finish();
        scanner.usage
    }

    fn is_empty(&self) -> bool {
        self.input_tokens == 0
            && self.output_tokens == 0
            && self.cache_creation_input_tokens == 0
            && self.cache_read_input_tokens == 0
    }
}

/// Cap on any single SSE line `SseUsageScanner` will hold or scan — applied
/// uniformly whether the line completes within one chunk or is carried across
/// chunks. Usage-bearing lines (`message_start` / `message_delta`) are well
/// under 1 KiB; anything larger is content we don't need, and a malformed
/// upstream must not be able to grow scanner memory without bound (LAB-717).
const SSE_SCAN_MAX_LINE: usize = 64 * 1024;

/// Incremental SSE token-usage extractor: O(1) memory per in-flight stream.
///
/// Replaces the old whole-stream `sse_buf` / `raw_sse` accumulation (LAB-717):
/// each upstream chunk is line-scanned as it passes through, keeping only a
/// capped partial-line carry, the running `TokenUsage` (last `message_start` /
/// `message_delta` wins, matching the old post-hoc parse), and bounded event
/// metadata for the `stream_end_no_usage` diagnostic.
#[derive(Default)]
struct SseUsageScanner {
    usage: TokenUsage,
    /// Model reported by the upstream `message_start` event (LAB-2330) —
    /// the response-derived model used for per-(client, model) accounting,
    /// preferred over the caller-supplied request model.
    model: Option<String>,
    /// Bytes of the current line seen so far, awaiting its `\n`.
    carry: Vec<u8>,
    /// Set when a line overflows `SSE_SCAN_MAX_LINE`; the rest of that line
    /// is discarded up to the next newline.
    skipping_oversized_line: bool,
    /// First five `event:` types, for the `stream_end_no_usage` preview.
    event_preview: Vec<String>,
    event_count: usize,
    bytes_seen: usize,
}

impl SseUsageScanner {
    fn push(&mut self, chunk: &[u8]) {
        self.bytes_seen = self.bytes_seen.saturating_add(chunk.len());
        let mut rest = chunk;
        while let Some(nl) = rest.iter().position(|&b| b == b'\n') {
            let (head, tail) = rest.split_at(nl);
            rest = &tail[1..];
            if self.skipping_oversized_line {
                // `head` is the tail of a discarded oversized line.
                self.skipping_oversized_line = false;
            } else if self.carry.len() + head.len() > SSE_SCAN_MAX_LINE {
                // Line exceeds the cap even though it terminated within this
                // chunk — discard it like the cross-chunk case, and release
                // the backing allocation rather than retaining its capacity.
                self.carry = Vec::new();
            } else if self.carry.is_empty() {
                self.scan_line(head);
            } else {
                self.carry.extend_from_slice(head);
                let line = std::mem::take(&mut self.carry);
                self.scan_line(&line);
                self.carry = line;
                self.carry.clear(); // reuse the allocation, drop the contents
            }
        }
        if rest.is_empty() || self.skipping_oversized_line {
            return;
        }
        self.carry.extend_from_slice(rest);
        if self.carry.len() > SSE_SCAN_MAX_LINE {
            self.carry = Vec::new();
            self.skipping_oversized_line = true;
        }
    }

    /// Flush the trailing unterminated line (a stream may not end with `\n`).
    fn finish(&mut self) {
        if !self.carry.is_empty() {
            let line = std::mem::take(&mut self.carry);
            self.scan_line(&line);
        }
    }

    fn scan_line(&mut self, raw: &[u8]) {
        let line = String::from_utf8_lossy(raw);
        let line = line.trim();
        if let Some(ev) = line.strip_prefix("event:") {
            self.event_count += 1;
            if self.event_preview.len() < 5 {
                self.event_preview.push(ev.trim_start().to_string());
            }
            return;
        }
        let Some(data) = line.strip_prefix("data: ") else {
            return;
        };
        // Cheap pre-filter: only usage-bearing event types are worth a JSON
        // parse, and their type string must appear literally in the payload.
        // False positives (a content delta mentioning "message_start") fall
        // through to the type match below and are ignored, same as before.
        if !data.contains("message_start") && !data.contains("message_delta") {
            return;
        }
        let Ok(event) = serde_json::from_str::<serde_json::Value>(data) else {
            return;
        };
        match event.get("type").and_then(|t| t.as_str()).unwrap_or("") {
            "message_start" => {
                if let Some(m) = event
                    .pointer("/message/model")
                    .and_then(|v| v.as_str())
                    .filter(|s| !s.is_empty())
                {
                    self.model = Some(m.to_owned());
                }
                if let Some(msg_usage) = event.get("message").and_then(|m| m.get("usage")) {
                    self.usage.input_tokens = msg_usage
                        .get("input_tokens")
                        .and_then(|v| v.as_u64())
                        .unwrap_or(0);
                    self.usage.cache_creation_input_tokens = msg_usage
                        .get("cache_creation_input_tokens")
                        .and_then(|v| v.as_u64())
                        .unwrap_or(0);
                    self.usage.cache_read_input_tokens = msg_usage
                        .get("cache_read_input_tokens")
                        .and_then(|v| v.as_u64())
                        .unwrap_or(0);
                }
            }
            "message_delta" => {
                if let Some(delta_usage) = event.get("usage") {
                    self.usage.output_tokens = delta_usage
                        .get("output_tokens")
                        .and_then(|v| v.as_u64())
                        .unwrap_or(0);
                }
            }
            _ => {}
        }
    }
}

/// Inject account authentication headers. Handles API keys, OAuth tokens,
/// and passthrough mode. For OAuth, merges required beta flags with the
/// client flags that survive the `allowed_betas` allow-list; the flags it
/// dropped are returned so the caller can log and count them (LAB-1191 /
/// audit finding 5 — an unfiltered merge let any caller activate arbitrary
/// beta features against the operator's account).
fn inject_account_auth(
    headers: &mut axum::http::HeaderMap,
    token: &str,
    passthrough: bool,
    allowed_betas: &[String],
) -> Vec<String> {
    if passthrough {
        return Vec::new();
    }
    headers.remove("authorization");
    headers.remove("x-api-key");
    let mut dropped: Vec<String> = Vec::new();
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
        // Merge required OAuth beta flags with the allow-listed client flags.
        // Use get_all to handle multiple anthropic-beta headers
        let mut flags: Vec<String> = Vec::new();
        for flag in headers
            .get_all("anthropic-beta")
            .iter()
            .filter_map(|v| v.to_str().ok())
            .flat_map(|s| s.split(','))
            .map(str::trim)
            .filter(|s| !s.is_empty())
        {
            // OAUTH_BETA_FLAGS are unconditionally (re-)added below, so a
            // client flag in that set is never actually dropped — reporting
            // it as such (e.g. under a custom allowlist omitting them) would
            // make the drop diagnostics lie.
            if beta_flag_allowed(allowed_betas, flag) || OAUTH_BETA_FLAGS.contains(&flag) {
                if !flags.iter().any(|f| f == flag) {
                    flags.push(flag.to_string());
                }
            } else if !dropped.iter().any(|f| f == flag) {
                dropped.push(flag.to_string());
            }
        }
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
    dropped
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
    /// `principal` is the authenticated client from `AppState::authenticate`.
    /// When present it IS the identity — `x-client-id` and the `client_names`
    /// IP map are not consulted at all. This one substitution is what makes
    /// budgets, ceilings, operator bypass, the model allow-list and the
    /// response-cache tenant unspoofable: every one of them keys on
    /// `client_id`, and this is where `client_id` is born.
    fn from_request(
        state: &AppState,
        client_ip: &IpAddr,
        headers: &axum::http::HeaderMap,
        principal: Option<&ClientConfig>,
    ) -> Self {
        Self {
            client_id: match principal {
                Some(c) => c.name.clone(),
                None => state.resolve_client_id(client_ip, headers),
            },
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

// ── Session registry: context-window visibility (LAB-916) ──────────
//
// The affinity "pin" is recomputed per request and never stored, so before
// this section there was no way to see which live sessions are close to
// their model's context window — only the resulting upstream "prompt is too
// long" 400s, which were forwarded without a trace. The registry is
// visibility state ONLY: it is written after responses and read by
// `/_stats`; routing never consults it.

/// Default cap on live session registry entries.
const DEFAULT_SESSION_REGISTRY_MAX: usize = 1000;
/// Default seconds after a session's last request before eviction.
const DEFAULT_SESSION_REGISTRY_TTL_SECS: u64 = 1800;
/// Max sessions returned in the `/_stats` `sessions` array (highest
/// context-window % first).
const SESSIONS_STATS_TOP_N: usize = 50;
/// Cap on distinct model labels in the prompt-too-long counter map (the model
/// string is client-controlled; overflow buckets into `_other`).
const MAX_PROMPT_TOO_LONG_MODELS: usize = 32;

/// `le` boundaries (tokens) for the live session-size distribution on
/// `/metrics`. Dense around the 200k window edge — that's where sessions
/// start 400ing — sparse elsewhere; `+Inf` is implicit.
const SESSION_TOKENS_BUCKETS: [u64; 10] = [
    10_000, 25_000, 50_000, 100_000, 150_000, 175_000, 200_000, 300_000, 500_000, 1_000_000,
];

/// Default model context window (tokens). Every current Claude model ships
/// with a 200k window unless the 1M beta is active on the request.
const DEFAULT_CONTEXT_WINDOW: u64 = 200_000;
/// Context window when the request carries the `context-1m` beta flag.
const CONTEXT_WINDOW_1M: u64 = 1_000_000;

/// Live per-session state, keyed by the affinity routing key — the same key
/// `pick_endpoint` pins on, so "session" here has exactly the granularity of
/// a routing pin (fan-out subagents that share a coarse session-id but carry
/// distinct content fingerprints appear as distinct entries).
struct SessionEntry {
    client_id: String,
    agent_id: String,
    session_id: String,
    model: String,
    /// Endpoint the session's last request was served by (its current pin).
    endpoint: String,
    /// `input + cache_read + cache_creation` from the last successful
    /// `Usage` — the prompt's occupancy of the model context window.
    last_prompt_tokens: u64,
    context_window: u64,
    requests: u64,
    last_seen: u64,
}

/// Redacted session label: hash of the affinity key, safe for `/_stats` and
/// logs (the raw key embeds the client IP and session id).
fn session_label(affinity_key: &str) -> String {
    format!("{:016x}", stable_affinity_hash(affinity_key))
}

/// True when any `anthropic-beta` header value activates the 1M context
/// window (flag shape: `context-1m-YYYY-MM-DD`; values may be comma-joined).
fn request_has_1m_beta(headers: &axum::http::HeaderMap) -> bool {
    headers
        .get_all("anthropic-beta")
        .iter()
        .filter_map(|v| v.to_str().ok())
        .flat_map(|v| v.split(','))
        .any(|flag| flag.trim().starts_with("context-1m"))
}

/// Model → context window size. All known Claude models are 200k unless the
/// request carries the `context-1m` beta. Unknown model families fall back to
/// 200k with a once-per-model warning so a new family can't silently skew
/// the `/_stats` context-window % view.
fn context_window_for(model: &str, has_1m_beta: bool) -> u64 {
    if has_1m_beta {
        return CONTEXT_WINDOW_1M;
    }
    if !model.is_empty() && !model.starts_with("claude") {
        static WARNED: std::sync::OnceLock<Mutex<std::collections::HashSet<String>>> =
            std::sync::OnceLock::new();
        if let Ok(mut warned) = WARNED.get_or_init(Default::default).lock() {
            // Bounded like the client maps: model is client-controlled input.
            if warned.len() < MAX_PROMPT_TOO_LONG_MODELS && warned.insert(model.to_string()) {
                warn!(model, "unknown model family, assuming 200k context window");
            }
        }
    }
    DEFAULT_CONTEXT_WINDOW
}

/// Context-window occupancy as a percentage, one decimal. Can exceed 100
/// (that's the signal: the next request will 400).
fn window_pct(tokens: u64, window: u64) -> f64 {
    if window == 0 {
        return 0.0;
    }
    (tokens as f64 / window as f64 * 1000.0).round() / 10.0
}

/// If `body` is the Anthropic "prompt is too long" 400 shape, return its
/// message. `{"type":"error","error":{"type":"invalid_request_error",
/// "message":"prompt is too long: 213462 tokens > 200000 maximum"}}`
fn prompt_too_long_message(body: &serde_json::Value) -> Option<&str> {
    let err = body.get("error")?;
    if err.get("type")?.as_str()? != "invalid_request_error" {
        return None;
    }
    let msg = err.get("message")?.as_str()?;
    msg.contains("prompt is too long").then_some(msg)
}

/// Parse `(observed, max)` token counts from a prompt-too-long message —
/// the first two integers in the text ("… 213462 tokens > 200000 maximum").
fn parse_prompt_too_long(msg: &str) -> Option<(u64, u64)> {
    let mut nums = msg
        .split(|c: char| !c.is_ascii_digit())
        .filter(|s| !s.is_empty())
        .filter_map(|s| s.parse::<u64>().ok());
    Some((nums.next()?, nums.next()?))
}

impl AppState {
    /// Record a completed request into the session registry. Sync-locked,
    /// never held across `.await` (AC7); TTL prune + oldest-eviction run only
    /// on NEW-key inserts, so steady-state traffic is a single map update.
    #[allow(clippy::too_many_arguments)]
    fn record_session(
        &self,
        affinity_key: &str,
        rctx: (&str, &str, &str),
        model: &str,
        endpoint: &str,
        prompt_tokens: u64,
        context_window: u64,
        now: u64,
    ) {
        if self.session_registry_max == 0 {
            return;
        }
        let (client_id, agent_id, session_id) = rctx;
        let Ok(mut map) = self.sessions.lock() else {
            return;
        };
        if !map.contains_key(affinity_key) {
            let ttl = self.session_registry_ttl_secs;
            map.retain(|_, e| now.saturating_sub(e.last_seen) <= ttl);
            if map.len() >= self.session_registry_max {
                // ponytail: O(n) min-scan eviction on new-key insert past the
                // cap; fine at the 1000-entry default, index by last_seen if
                // the cap ever needs to grow orders of magnitude.
                if let Some(oldest) = map
                    .iter()
                    .min_by_key(|(_, e)| e.last_seen)
                    .map(|(k, _)| k.clone())
                {
                    map.remove(&oldest);
                }
            }
        }
        let entry = map
            .entry(affinity_key.to_owned())
            .or_insert_with(|| SessionEntry {
                client_id: client_id.to_owned(),
                agent_id: agent_id.to_owned(),
                session_id: session_id.to_owned(),
                model: String::new(),
                endpoint: String::new(),
                last_prompt_tokens: 0,
                context_window: DEFAULT_CONTEXT_WINDOW,
                requests: 0,
                last_seen: 0,
            });
        model.clone_into(&mut entry.model);
        endpoint.clone_into(&mut entry.endpoint);
        entry.last_prompt_tokens = prompt_tokens;
        entry.context_window = context_window;
        entry.requests += 1;
        entry.last_seen = now;
    }

    /// Count + log an upstream "prompt is too long" 400. The response itself
    /// is forwarded to the client unchanged by the caller; this is the
    /// operator-side trace that previously didn't exist.
    fn note_prompt_too_long(
        &self,
        req_id: &str,
        model: &str,
        affinity_key: Option<&str>,
        message: &str,
    ) {
        if let Ok(mut counts) = self.prompt_too_long.lock() {
            let label = if counts.len() < MAX_PROMPT_TOO_LONG_MODELS || counts.contains_key(model) {
                model
            } else {
                "_other"
            };
            *counts.entry(label.to_owned()).or_insert(0) += 1;
        }
        let (observed, max) = parse_prompt_too_long(message)
            .map(|(o, m)| (Some(o), Some(m)))
            .unwrap_or((None, None));
        warn!(
            req_id,
            model,
            session = affinity_key.map(session_label).as_deref().unwrap_or("-"),
            observed_tokens = observed,
            max_tokens = max,
            "prompt too long"
        );
    }

    /// Snapshot the session registry for `/_stats`: TTL-filtered, sorted by
    /// context-window % desc, capped to `SESSIONS_STATS_TOP_N`. Raw IPs and
    /// session ids never leave the registry — the label is a hash of the
    /// affinity key and agent/session ids are truncated to 8 chars.
    fn sessions_snapshot(&self, now: u64) -> Vec<serde_json::Value> {
        let mut rows: Vec<(f64, serde_json::Value)> = self
            .sessions
            .lock()
            .map(|map| {
                map.iter()
                    .filter(|(_, e)| {
                        now.saturating_sub(e.last_seen) <= self.session_registry_ttl_secs
                    })
                    .map(|(key, e)| {
                        let pct = window_pct(e.last_prompt_tokens, e.context_window);
                        let client_id = if self.is_operator(&e.client_id) {
                            "_operator"
                        } else {
                            &e.client_id
                        };
                        let truncate8 = |s: &str| -> String { s.chars().take(8).collect() };
                        (
                            pct,
                            serde_json::json!({
                                "session": session_label(key),
                                "client_id": client_id,
                                "agent": truncate8(&e.agent_id),
                                "session_prefix": truncate8(&e.session_id),
                                "model": e.model,
                                "endpoint": e.endpoint,
                                "last_prompt_tokens": e.last_prompt_tokens,
                                "context_window": e.context_window,
                                "context_window_pct": pct,
                                "requests": e.requests,
                                "last_seen": e.last_seen,
                            }),
                        )
                    })
                    .collect()
            })
            .unwrap_or_default();
        rows.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));
        rows.truncate(SESSIONS_STATS_TOP_N);
        rows.into_iter().map(|(_, v)| v).collect()
    }

    /// Snapshot the live session registry as a cumulative `le` histogram over
    /// `last_prompt_tokens` (LAB-957): per-boundary cumulative counts with a
    /// trailing `+Inf`, plus the token sum. Fixed cardinality regardless of
    /// session count — per-session Prometheus labels stay ruled out. Counts
    /// sum cleanly across replicas (each pod holds a disjoint registry).
    fn session_tokens_histogram(&self, now: u64) -> ([u64; SESSION_TOKENS_BUCKETS.len() + 1], u64) {
        let mut cumulative = [0u64; SESSION_TOKENS_BUCKETS.len() + 1];
        let mut sum = 0u64;
        match self.sessions.lock() {
            Ok(map) => {
                for e in map.values() {
                    if now.saturating_sub(e.last_seen) > self.session_registry_ttl_secs {
                        continue;
                    }
                    sum += e.last_prompt_tokens;
                    for (i, le) in SESSION_TOKENS_BUCKETS.iter().enumerate() {
                        if e.last_prompt_tokens <= *le {
                            cumulative[i] += 1;
                        }
                    }
                    cumulative[SESSION_TOKENS_BUCKETS.len()] += 1; // +Inf
                }
            }
            // All-zero output with no trace would read as "no sessions";
            // a poisoned registry lock deserves a diagnostic.
            Err(_) => warn!("session_tokens_histogram: sessions registry lock poisoned"),
        }
        (cumulative, sum)
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
    mut scanner: SseUsageScanner,
    request_start: std::time::Instant,
    client_disconnected: bool,
    upstream_error: bool,
    openai_compat: bool,
    session_key: Option<&str>,
    context_window: u64,
) {
    scanner.finish();
    let usage = &scanner.usage;
    // Response-derived model (message_start) preferred; request model fallback.
    let usage_model = scanner.model.as_deref().unwrap_or(model);
    let elapsed_ms = request_start.elapsed().as_millis() as u64;
    if !usage.is_empty() {
        state.record_usage(ep, client_id, usage_model, usage).await;
        // Log the same model the metric records, so the usage log and
        // anthropic_client_model_token_usage_total reconcile.
        log_usage(req_id, client_id, usage_model, acct_name, usage);
        if let Some(key) = session_key {
            state.record_session(
                key,
                (client_id, agent, session),
                model,
                acct_name,
                usage.input_tokens
                    + usage.cache_read_input_tokens
                    + usage.cache_creation_input_tokens,
                context_window,
                AppState::now_epoch(),
            );
        }
    } else {
        let reason = if upstream_error {
            "upstream_error"
        } else if client_disconnected {
            "client_disconnect"
        } else {
            "no_usage_event"
        };
        // Log structural metadata only — SSE payloads contain user content.
        let truncated = scanner.event_count > 5;
        warn!(
            req_id,
            client_id,
            model,
            account = acct_name,
            reason,
            elapsed_ms,
            sse_bytes = scanner.bytes_seen,
            sse_event_count = scanner.event_count,
            sse_events = ?scanner.event_preview,
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
/// `response_model` is the model reported by the upstream response body
/// (LAB-2330) — preferred over the request `model` for per-model accounting.
#[allow(clippy::too_many_arguments)]
async fn finalize_non_stream(
    state: &AppState,
    ep: &Endpoint,
    req_id: &str,
    client_id: &str,
    model: &str,
    response_model: Option<&str>,
    acct_name: &str,
    client_ip: &str,
    agent: &str,
    session: &str,
    status_code: u16,
    usage: &TokenUsage,
    latency_ms: u64,
    openai_compat: bool,
    session_key: Option<&str>,
    context_window: u64,
) {
    if !usage.is_empty() {
        let usage_model = response_model.unwrap_or(model);
        state.record_usage(ep, client_id, usage_model, usage).await;
        // Log the same model the metric records, so the usage log and
        // anthropic_client_model_token_usage_total reconcile.
        log_usage(req_id, client_id, usage_model, acct_name, usage);
        if let Some(key) = session_key {
            state.record_session(
                key,
                (client_id, agent, session),
                model,
                acct_name,
                usage.input_tokens
                    + usage.cache_read_input_tokens
                    + usage.cache_creation_input_tokens,
                context_window,
                AppState::now_epoch(),
            );
        }
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
    /// Record token usage for an endpoint and client. `model` feeds the
    /// per-(client, model) counter (LAB-2330): callers pass the
    /// response-derived model where available, falling back to the request
    /// model — either way it is truncated and the pair-count is capped here,
    /// so callers cannot inflate the label set.
    async fn record_usage(&self, ep: &Endpoint, client_id: &str, model: &str, usage: &TokenUsage) {
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
            // Per-(client, model) accounting (LAB-2330). A usage record only
            // exists when the upstream returned usage, so the model string was
            // accepted upstream; truncation + the pair cap bound the label set
            // regardless.
            let model = if model.is_empty() {
                "unknown".to_owned()
            } else {
                truncate_label(model)
            };
            if let Ok(mut map) = self.client_model_usage.lock() {
                let key = (client_id.to_owned(), model);
                let key = if map.len() < MAX_CLIENT_MODEL_LABELS || map.contains_key(&key) {
                    key
                } else {
                    // Map full and this pair is new: lump into ONE global
                    // overflow bucket — hard bound of MAX_CLIENT_MODEL_LABELS
                    // + 1 entries. A per-client ("<client>", "_other") key
                    // would let x-client-id rotation (legacy auth modes) grow
                    // the map without bound (expert-panel finding, LAB-2330).
                    ("_other".to_owned(), "_other".to_owned())
                };
                let entry = map.entry(key).or_insert([0; 4]);
                entry[0] += usage.input_tokens;
                entry[1] += usage.output_tokens;
                entry[2] += usage.cache_creation_input_tokens;
                entry[3] += usage.cache_read_input_tokens;
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
    /// When Redis is available, a present counter is authoritative; an absent key or a read
    /// error falls through to the local floor — an absent key may be a counter lost to a
    /// failed INCRBY, or a poisoned key record_budget_usage deliberately deleted (LAB-1962).
    /// The poison self-heal DEL is only safe because of this floor: relaxing the absent-key
    /// path back to an authoritative allow would reopen the enforcement bypass.
    async fn check_budget(&self, client_id: &str) -> Result<(), u64> {
        let limit = match self.client_budgets.get(client_id) {
            Some(&limit) => limit,
            None => return Ok(()), // no budget configured = unlimited
        };
        let today = Self::now_epoch() / 86400;

        // Try Redis first for cross-replica budget enforcement. The
        // is_connected gate matters on this request-path call: while fred is
        // reconnecting it BUFFERS commands until default_command_timeout
        // instead of erroring, so without the gate a sustained outage would
        // add ~2s to every budgeted request. Known-down transport skips
        // straight to local enforcement at the old client's speed.
        if let Some(redis) = &self.redis {
            if redis.is_connected() {
                let key = format!("alb:budget:{client_id}:{today}");
                match redis.get::<Option<u64>, _>(key.as_str()).await {
                    Ok(Some(used)) if used >= limit => return Err(0),
                    Ok(Some(_)) => return Ok(()),
                    // Absent key: fall through to the local floor. Treating
                    // absence as an authoritative allow let a single failed
                    // INCRBY widen the budget fleet-wide (LAB-1962/F8).
                    Ok(None) => {}
                    Err(e) => {
                        warn!(error = %e, "redis budget check failed, falling back to local");
                    }
                }
            } else {
                trace!("redis disconnected, budget check falling back to local");
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

    /// Atomic check-and-delete for a poisoned budget key: deletes only if the
    /// value STILL cannot be INCRBY'd at delete time. A bare DEL issued after
    /// the INCRBY error can land late (concurrent replicas also healing, or
    /// fred replaying it after a reconnect) and would erase a counter the
    /// fleet already rebuilt. "Still poisoned" is decided by Redis's own
    /// parser — a zero-increment INCRBY probe — NOT Lua's tonumber(), which
    /// accepts values INCRBY rejects ("1.5", "1e3", hex, out-of-i64-range);
    /// any value in that gap would wedge the counter for its full TTL
    /// (LAB-1962 review). The probe also errors on WRONGTYPE keys, so no
    /// separate TYPE check is needed; a zero increment on a valid counter
    /// leaves its value and TTL untouched and cannot overflow. The EXISTS
    /// gate stops the probe from creating the key at 0 — which check_budget
    /// would treat as an authoritative fleet-wide allow. The probe error
    /// must match the same value-error frames as budget_value_poisoned
    /// (keep the two in lockstep): INCRBY can also fail for non-value
    /// reasons — notably OOM at maxmemory, where DEL still succeeds — and
    /// deleting a valid counter on such an error would be exactly the
    /// erasure this guard exists to prevent.
    const BUDGET_DEL_IF_POISONED_SCRIPT: &'static str = r#"
        if redis.call('EXISTS', KEYS[1]) == 0 then
            return 0
        end
        local probe = redis.pcall('INCRBY', KEYS[1], 0)
        if type(probe) == 'table' and probe.err
            and (probe.err:find('WRONGTYPE', 1, true) == 1
                or probe.err:find('not an integer', 1, true)) then
            return redis.call('DEL', KEYS[1])
        end
        return 0
        "#;

    /// True when a Redis error means the budget key's VALUE is unusable
    /// (server-reported WRONGTYPE or non-integer), as opposed to a transport
    /// failure. Server error frames arrive verbatim in `details()` — Redis
    /// and Dragonfly both emit these exact strings — while IO/timeout errors
    /// never do, so a poisoned key is safely distinguishable from a lost
    /// write. BUDGET_DEL_IF_POISONED_SCRIPT matches the same two frames on
    /// the Lua side — change them together or the guard and classifier
    /// drift.
    fn budget_value_poisoned(e: &fred::error::RedisError) -> bool {
        let details = e.details();
        details.starts_with("WRONGTYPE") || details.contains("not an integer")
    }

    /// Record tokens against a client's daily budget.
    /// Updates local state synchronously; awaits Redis INCRBY inline to prevent TOCTOU races.
    /// On a transport-level INCRBY failure the increment is lost from the shared counter —
    /// the key is left untouched so one replica's failed write never erases fleet-wide
    /// accounting; the local accumulator (updated first, before any Redis I/O) keeps the
    /// floor (LAB-1962). A server-reported poisoned value (WRONGTYPE / non-integer) is the
    /// one case that still deletes: the key is unreadable garbage for its full TTL and
    /// deleting it lets the shared counter rebuild.
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

        // Await Redis INCRBY (not fire-and-forget) so check_budget always sees
        // latest counter. Same is_connected rationale as check_budget: this
        // runs on the request path, and a reconnecting fred client would
        // buffer the INCRBY for 2s instead of failing fast. Skipping while
        // down is equivalent to attempt-and-fail: local state above is
        // already updated, and the increment is lost either way.
        if let Some(redis) = &self.redis {
            if !redis.is_connected() {
                trace!("redis disconnected, budget INCRBY skipped (local state updated)");
                return;
            }
            let key = format!("alb:budget:{client_id}:{today}");
            let result: Result<u64, fred::error::RedisError> =
                redis.incr_by(key.as_str(), tokens as i64).await;
            match result {
                Ok(_) => {
                    let expire_result: Result<bool, fred::error::RedisError> =
                        redis.expire(key.as_str(), BUDGET_TTL_SECS).await;
                    if let Err(e) = expire_result {
                        tracing::warn!(error = %e, "redis EXPIRE failed for budget key");
                    }
                }
                Err(e) if Self::budget_value_poisoned(&e) => {
                    // Poisoned value: delete so the counter can rebuild (see
                    // fn doc). Safe only because check_budget's absent-key
                    // local floor closes the old allow-bypass; the Lua guard
                    // keeps a late DEL from erasing a rebuilt valid counter.
                    tracing::warn!(
                        error = %e,
                        key = %key,
                        "budget key holds a non-integer value, deleting so the shared counter can rebuild"
                    );
                    let del_result: Result<RedisValue, fred::error::RedisError> = redis
                        .eval(
                            Self::BUDGET_DEL_IF_POISONED_SCRIPT,
                            vec![key.as_str()],
                            Vec::<String>::new(),
                        )
                        .await;
                    if let Err(del_err) = del_result {
                        tracing::warn!(error = %del_err, key = %key, "failed to delete poisoned budget key");
                    }
                }
                Err(e) => {
                    // Transport failure: do NOT delete the shared counter —
                    // erasing a valid counter on one replica's write failure
                    // destroyed the fleet's day of accounting and, with the
                    // old absent-key allow in check_budget, bypassed
                    // enforcement entirely (LAB-1962/F8). The increment is
                    // lost; check_budget's local floor covers this replica.
                    tracing::warn!(
                        error = %e,
                        key = %key,
                        "redis INCRBY failed, increment lost from shared budget counter"
                    );
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
    /// Returns Ok(()) or an error Response (403/429).
    ///
    /// Boxed Err — see `ForwardOutcome` (clippy::result_large_err).
    async fn pre_request_gate(&self, client_id: &str, model: &str) -> Result<(), Box<Response>> {
        if self.is_operator(client_id) {
            return Ok(()); // operator bypasses everything
        }

        // 0. Per-client model allow-list (LAB-1083). First because it is a
        //    POLICY denial, not a capacity one: "you may not use this model"
        //    must not be reported as 429 "try again later", which is what the
        //    three checks below all mean. Reached from both proxy_handler and
        //    openai_chat_handler, so one placement covers both surfaces.
        if !self.client_allows_model(client_id, model) {
            self.note_model_denied(client_id, model);
            // `model` is caller-controlled and echoed back — truncate it here
            // too, so a 25 MB model field cannot become a 25 MB error body.
            let body = if model.is_empty() {
                format!(
                    "client '{client_id}' has a model allow-list, but no model could be read from the request"
                )
            } else {
                format!(
                    "client '{client_id}' is not permitted to use model '{}'",
                    truncate_label(model)
                )
            };
            return Err(Box::new((StatusCode::FORBIDDEN, body).into_response()));
        }

        // 1. Daily token budget (existing)
        if client_id != "-" && self.check_budget(client_id).await.is_err() {
            warn!(client_id = %client_id, "rejected: daily token budget exceeded");
            return Err(Box::new(
                (StatusCode::TOO_MANY_REQUESTS, "daily token budget exceeded").into_response(),
            ));
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
            return Err(Box::new(resp));
        }

        // 3. Emergency brake (new)
        if self.is_emergency_brake_active().await {
            warn!(
                client_id = %client_id,
                "rejected: emergency brake active"
            );
            return Err(Box::new(
                (
                    StatusCode::TOO_MANY_REQUESTS,
                    "emergency: all accounts near exhaustion",
                )
                    .into_response(),
            ));
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
///   - `RetryModelUnsupported(resp)`: the endpoint rejected the request's
///     MODEL, not the request itself (LAB-941). The forward path has already
///     negative-cached the (endpoint, model) pair; the loop rotates like a
///     429 while stashing the upstream's error response, so a model no OTHER
///     endpoint can serve still surfaces the real error — a nonexistent-model
///     404 must not morph into a synthetic 429 that invites retries.
// Response payloads are boxed so the enum stays small (one word per payload):
// it rides in the Err of `classify_retry_status`'s Result, where an inline
// `Response` is 128+ bytes on the hot success path (clippy::result_large_err —
// same pattern as `authenticate` / `reserve_request_body`).
enum ForwardOutcome {
    Done(Box<Response>),
    RetryModelUnsupported(Box<Response>),
    Retry {
        saw_529: bool,
        push_skip: bool,
        transient: bool,
    },
}

/// True when an upstream error body says this ACCOUNT cannot serve the
/// requested model — distinct from a malformed request (LAB-941). Matched
/// conservatively against observed wire formats:
///   - Anthropic: 404 `{"type":"error","error":{"type":"not_found_error",
///     "message":"model: <id>"}}` — also what subscription accounts return
///     for models outside their plan.
///   - LiteLLM-style gateways: 400 `{"error":{"message":"... Invalid model
///     name passed in model=<id> ..."}}` (observed live from insight-gateway,
///     2026-07-27).
///   - OpenAI: `{"error":{"code":"model_not_found", ...}}`.
fn is_model_unsupported_error(status: StatusCode, body: &serde_json::Value) -> bool {
    if status != StatusCode::NOT_FOUND && status != StatusCode::BAD_REQUEST {
        return false;
    }
    let Some(err) = body.get("error") else {
        return false;
    };
    let msg = err.get("message").and_then(|v| v.as_str()).unwrap_or("");
    if err.get("type").and_then(|v| v.as_str()) == Some("not_found_error")
        && msg.starts_with("model:")
    {
        return true;
    }
    if err.get("code").and_then(|v| v.as_str()) == Some("model_not_found") {
        return true;
    }
    msg.to_ascii_lowercase().contains("invalid model name")
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
    // With `Policy::none()` on the upstream clients a redirect is a normal
    // 3xx *response* (handled in `classify_retry_status`), so this kind can
    // no longer fire there — kept because this describer is generic.
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
/// Shared by `forward_anthropic`, `forward_openai_compat_anthropic`, and
/// `try_fallback_upstream`, so retry classification cannot drift between
/// protocols. For 429 / 529 / other 5xx it records hard-limit state,
/// persists, and logs exactly as the prior inline blocks did, returning
/// `Err(ForwardOutcome::Retry { .. })`. For any non-retry status (2xx
/// success or 4xx client error) it returns `Ok(resp)` — handing the
/// response back so the caller can continue.
///
/// `openai_error_shape` picks the error-body format for the terminal 3xx
/// arm: callers whose downstream parses OpenAI errors (`/v1/chat/completions`
/// passthrough) get `{"error":{...}}`, Anthropic-surface callers get
/// `{"type":"error",...}` — matching how every other error arm on those
/// paths translates per surface.
async fn classify_retry_status(
    state: &AppState,
    status: StatusCode,
    rate_info: &RwLock<RateLimitInfo>,
    endpoint_name: &str,
    resp: reqwest::Response,
    openai_error_shape: bool,
) -> Result<reqwest::Response, ForwardOutcome> {
    // 3xx → deliberate 502. The upstream client follows no redirects
    // (`Policy::none()`), because following one would re-send the account
    // credential to the Location host. A redirect from a configured endpoint
    // is anomalous (misconfig or tampering), so it terminates the request
    // with a distinct log rather than rotating — retrying other accounts
    // against a redirecting upstream would just spray more credentialed
    // requests at it (LAB-1191 / 2026-06-02 audit finding 2).
    if status.is_redirection() {
        let location = resp
            .headers()
            .get("location")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("-");
        warn!(
            account = endpoint_name,
            status = status.as_u16(),
            location,
            "upstream returned a redirect — refusing to follow with credentials attached"
        );
        let message = format!(
            "upstream returned {} redirect; refusing to follow with credentials",
            status.as_u16()
        );
        let body = if openai_error_shape {
            serde_json::json!({
                "error": {
                    "message": message,
                    "type": "api_error",
                    "param": null,
                    "code": null
                }
            })
        } else {
            serde_json::json!({
                "type": "error",
                "error": { "type": "api_error", "message": message }
            })
        }
        .to_string();
        return Err(ForwardOutcome::Done(Box::new(
            Response::builder()
                .status(StatusCode::BAD_GATEWAY)
                .header("content-type", "application/json")
                .body(Body::from(body))
                .unwrap_or_else(|_| {
                    (StatusCode::BAD_GATEWAY, "upstream redirect refused").into_response()
                }),
        )));
    }

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
    model_unsupported_resp: &mut Option<Response>,
) -> RetryStep {
    match outcome {
        ForwardOutcome::Done(resp) => RetryStep::Return(*resp),
        // Model rejected by this endpoint: rotate immediately (another
        // account may serve it) but keep the upstream's error in hand for
        // the case where none does (LAB-941).
        ForwardOutcome::RetryModelUnsupported(resp) => {
            *model_unsupported_resp = Some(*resp);
            skip.push(picked_idx);
            RetryStep::NextAttempt
        }
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

/// Synthesized model-unsupported 404 for the warm negative-cache path: every
/// eligible endpoint is already cached as rejecting `model`, so the pool
/// empties before any forward attempt runs and there is no upstream response
/// to stash. Mirrors Anthropic's canonical not-found envelope (`"model: <id>"`
/// is the wire format accounts actually return); `openai_shape` emits the
/// OpenAI error shape for the compat handler instead. Cannot echo cached
/// upstream bytes: the cache stores no bodies, and a body cached from one
/// protocol would be wrong for the other handler's clients (LAB-941).
fn model_unsupported_response(model: &str, openai_shape: bool) -> Response {
    let body = if openai_shape {
        // OpenAI's canonical model-not-found envelope: type is
        // invalid_request_error (an OpenAI type — not Anthropic's
        // not_found_error), with the specific cause in `code`.
        serde_json::json!({
            "error": {
                "message": format!("model: {model}"),
                "type": "invalid_request_error",
                "param": null,
                "code": "model_not_found"
            }
        })
    } else {
        serde_json::json!({
            "type": "error",
            "error": { "type": "not_found_error", "message": format!("model: {model}") }
        })
    };
    (
        StatusCode::NOT_FOUND,
        [("content-type", "application/json")],
        body.to_string(),
    )
        .into_response()
}

/// 400 in Anthropic's error envelope when an Anthropic request can't be
/// faithfully translated for an OpenAI-compat fallback endpoint (e.g. an
/// image source type the translator doesn't support). The caller's request
/// was Anthropic Messages API shaped, so the error response matches that,
/// regardless of which protocol the fallback endpoint speaks.
fn untranslatable_request_response(message: &str) -> Response {
    (
        StatusCode::BAD_REQUEST,
        [("content-type", "application/json")],
        serde_json::json!({
            "type": "error",
            "error": { "type": "invalid_request_error", "message": message }
        })
        .to_string(),
    )
        .into_response()
}

/// Copy the allow-listed upstream response headers onto `builder`. Everything
/// not listed here stays behind the proxy (LAB-1191 / 2026-06-02 audit
/// finding 3: the old copy-everything loop leaked `anthropic-ratelimit-*` —
/// the pooled capacity of every account — plus `set-cookie` and
/// org-identifying headers). `expose_ratelimit` (config
/// `expose_upstream_ratelimit_headers`, trusted networks only) restores the
/// `anthropic-ratelimit-*` passthrough for tooling that reads it. One other
/// site reflects upstream headers: `forward_anthropic`'s body-read-failure
/// 502 arm copies `anthropic-ratelimit-*` behind the same flag.
fn reflect_upstream_headers(
    mut builder: axum::http::response::Builder,
    headers: &reqwest::header::HeaderMap,
    expose_ratelimit: bool,
) -> axum::http::response::Builder {
    // What SDKs need to function: body framing (content-type/length),
    // SSE cache hint, the Anthropic request id for error reports, and
    // retry-after on forwarded 4xx.
    const ALLOWED: &[&str] = &[
        "content-type",
        "content-length",
        "cache-control",
        "request-id",
        "retry-after",
    ];
    for (k, v) in headers.iter() {
        let name = k.as_str();
        if ALLOWED.contains(&name) || (expose_ratelimit && name.starts_with("anthropic-ratelimit-"))
        {
            builder = builder.header(k, v);
        }
    }
    builder
}

/// Shared knob chain for both upstream clients — `client` layers the SSE-tuned
/// `read_timeout` on top; `client_nonstreaming` takes it as-is (LAB-718).
fn upstream_client_builder() -> reqwest::ClientBuilder {
    Client::builder()
        // Never follow redirects: every upstream request carries an account
        // credential (Authorization / x-api-key), and reqwest re-sends it to
        // the redirect target. A 3xx surfaces as a response and is turned
        // into a deliberate 502 by `classify_retry_status` (LAB-1191 /
        // 2026-06-02 audit finding 2).
        .redirect(reqwest::redirect::Policy::none())
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
        .map(|v| body_wants_stream(&v))
        .unwrap_or(false)
}

/// The streaming predicate on an already-parsed body. Single definition
/// shared by `request_wants_stream` and the response-cache gate so the two
/// can never disagree on what "streaming" means.
fn body_wants_stream(body: &serde_json::Value) -> bool {
    body.get("stream")
        .and_then(|s| s.as_bool())
        .unwrap_or(false)
}

/// Forward one Anthropic-protocol request to a single `Endpoint`. The caller
/// passes the picked endpoint and its pool index (used for `skip` and usage
/// accounting).
/// `ep` is the endpoint to forward to; `endpoint_idx` is its index in
/// `state.endpoints`. Both are required: the streaming path spawns a
/// detached 'static task that must re-borrow the endpoint from a cloned
/// Arc<AppState> — a borrowed &Endpoint cannot cross the spawn boundary,
/// so the task captures the Copy `endpoint_idx` and re-indexes.
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
    session_key: Option<&str>,
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
    let dropped = inject_account_auth(
        &mut headers,
        token,
        passthrough,
        &state.allowed_client_betas,
    );
    state.record_dropped_beta_flags(client_id, &dropped);

    // Context window for the session registry: 200k, or 1M when the request
    // carries the `context-1m` beta (per-request, so a mixed client is
    // tracked at the window each request actually ran under). Read from the
    // FILTERED outbound headers, not the client's raw ones — if the beta
    // allow-list stripped `context-1m`, the upstream runs this request at
    // 200k and the accounting must agree (PR #116 review).
    let context_window = context_window_for(model, request_has_1m_beta(&headers));

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
    let mut resp =
        match classify_retry_status(state, status, rate_info, endpoint_name, resp, false).await {
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

    let builder = reflect_upstream_headers(
        Response::builder().status(resp_status),
        &resp_headers,
        state.expose_upstream_ratelimit_headers,
    );

    // Inject budget status header
    let builder = builder.header("x-budget-status", budget_status);

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
        let session_key_clone = session_key.map(str::to_owned);
        let status_code = status.as_u16();

        tokio::spawn(async move {
            let mut scanner = SseUsageScanner::default();
            let mut client_disconnected = false;
            let mut upstream_error = false;
            loop {
                match resp.chunk().await {
                    Ok(Some(chunk)) => {
                        scanner.push(&chunk);
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
            // Record scanned usage. The detached task only holds a cloned
            // Arc<AppState>; re-index it to recover &Endpoint.
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
                scanner,
                request_start,
                client_disconnected,
                upstream_error,
                false,
                session_key_clone.as_deref(),
                context_window,
            )
            .await;
        });

        let body_stream = ReceiverStream::new(rx);
        let response = builder
            .body(Body::from_stream(body_stream))
            .unwrap_or_else(|_| {
                (StatusCode::INTERNAL_SERVER_ERROR, "response build error").into_response()
            });
        ForwardOutcome::Done(Box::new(response))
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
                // Forward the upstream's rate-limit headers (behind the same
                // trusted-network flag as the success arms) + budget status so
                // the client's limit tracking stays consistent.
                // Deliberately NOT content-length: the upstream's value describes
                // the truncated body it promised, not our short JSON frame.
                let mut err_builder = Response::builder()
                    .status(StatusCode::BAD_GATEWAY)
                    .header("content-type", "application/json")
                    .header("x-budget-status", budget_status);
                if state.expose_upstream_ratelimit_headers {
                    for (k, v) in resp_headers.iter() {
                        if k.as_str().starts_with("anthropic-ratelimit-") {
                            err_builder = err_builder.header(k, v);
                        }
                    }
                }
                return ForwardOutcome::Done(Box::new(
                    err_builder.body(Body::from(body)).unwrap_or_else(|_| {
                        (StatusCode::BAD_GATEWAY, "upstream body read failed").into_response()
                    }),
                ));
            }
        };
        let mut usage = TokenUsage::default();
        let mut response_model: Option<String> = None;
        if let Ok(parsed) = serde_json::from_slice::<serde_json::Value>(&resp_body_bytes) {
            usage = TokenUsage::from_response_body(&parsed);
            response_model = parsed
                .get("model")
                .and_then(|v| v.as_str())
                .map(str::to_owned);
            // Count + trace upstream context-window overflows (LAB-916). The
            // 400 itself is forwarded below byte-for-byte, as before.
            if status.as_u16() == 400 {
                if let Some(msg) = prompt_too_long_message(&parsed) {
                    state.note_prompt_too_long(req_id, model, session_key, msg);
                }
            }
            // Model unsupported on THIS account (e.g. outside its plan):
            // negative-cache the pair and rotate — another account may serve
            // it. Forwarding the 404 as-is wedges affinity-pinned clients
            // into a permanent retry loop against this account (LAB-941).
            if is_model_unsupported_error(status, &parsed) {
                state.note_model_unsupported(endpoint_name, endpoint_idx, model);
                let response = builder
                    .body(Body::from(resp_body_bytes))
                    .unwrap_or_else(|_| {
                        (StatusCode::INTERNAL_SERVER_ERROR, "response build error").into_response()
                    });
                return ForwardOutcome::RetryModelUnsupported(Box::new(response));
            }
        }
        finalize_non_stream(
            state,
            ep,
            req_id,
            client_id,
            model,
            response_model.as_deref(),
            endpoint_name,
            &client_ip.to_string(),
            agent_id,
            session_id,
            status.as_u16(),
            &usage,
            latency_ms,
            false,
            session_key,
            context_window,
        )
        .await;
        let response = builder
            .body(Body::from(resp_body_bytes))
            .unwrap_or_else(|_| {
                (StatusCode::INTERNAL_SERVER_ERROR, "response build error").into_response()
            });
        ForwardOutcome::Done(Box::new(response))
    }
}

/// LAB-933 write path. Pass-through unless a cache key was derived for this
/// request (opted-in client, non-streaming /v1/messages — AC1/AC2/AC8) AND
/// the response is 2xx (AC3: 4xx/5xx are never written). The body is already
/// fully buffered for non-streaming requests, so `collect()` is a cheap
/// re-assembly, not a wait. A cache-write failure fails open (AC10): the
/// client response is returned unchanged either way.
async fn maybe_cache_store(
    state: &AppState,
    cache_key: Option<(&str, CacheSurface)>,
    client_id: &str,
    req_id: &str,
    resp: Response,
) -> Response {
    let (Some(rc), Some((key, surface))) = (&state.response_cache, cache_key) else {
        return resp;
    };
    if !resp.status().is_success() {
        return resp;
    }
    // Only JSON bodies are cacheable. The request was non-streaming, so the
    // forward path buffered the body — but that is an invariant of TODAY's
    // upstreams, not a law: an upstream answering a `stream:false` request
    // with `text/event-stream` must pass through untouched (never collected,
    // never cached as a bogus non-streaming entry).
    let content_type = resp
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("application/json")
        .to_string();
    if !content_type.starts_with("application/json") {
        debug!(
            req_id,
            content_type, "response cache: non-JSON content-type — not cached"
        );
        return resp;
    }
    let (parts, body) = resp.into_parts();
    let bytes = match http_body_util::BodyExt::collect(body).await {
        Ok(collected) => collected.to_bytes(),
        Err(e) => {
            // The buffered body failed to re-assemble — nothing valid to
            // return OR cache. Surface it; never fabricate a success.
            error!(req_id, error = %e, "response body collect failed during cache store");
            return (
                StatusCode::BAD_GATEWAY,
                "response body collect failed during cache store",
            )
                .into_response();
        }
    };
    if bytes.len() > ResponseCache::MAX_BODY_BYTES {
        // Deliberate policy skip, not a failure: bounds backend value growth
        // and worst-case L1 memory (L1_CAPACITY × MAX_BODY_BYTES per client).
        debug!(
            req_id,
            body_bytes = bytes.len(),
            "response cache: body exceeds size cap — not cached"
        );
        return Response::from_parts(parts, Body::from(bytes));
    }
    rc.store(
        client_id,
        key,
        &CachedResponse {
            status: parts.status.as_u16(),
            content_type,
            body: bytes.to_vec(),
        },
        surface,
    )
    .await;
    Response::from_parts(parts, Body::from(bytes))
}

async fn proxy_handler(
    State(state): State<Arc<AppState>>,
    axum::extract::ConnectInfo(client_addr): axum::extract::ConnectInfo<SocketAddr>,
    req: Request<Body>,
) -> Response {
    // AC-8: the ONLY client_addr.ip() read in this handler — everything
    // downstream consumes the resolved client IP.
    let client_ip = state.resolve_client_ip(client_addr.ip(), req.headers());
    let request_start = Instant::now();

    // IP allowlist check
    if !state.is_ip_allowed(&client_ip) {
        warn!(client = %client_ip, "rejected: IP not in allowlist");
        return (StatusCode::FORBIDDEN, "forbidden").into_response();
    }

    // Proxy auth: x-api-key against the [[clients]] table, else legacy proxy_key.
    let principal = match state.authenticate_throttled(&client_ip, req.headers(), false, "proxy") {
        Ok(p) => p,
        Err(resp) => return *resp,
    };

    let (parts, body) = req.into_parts();

    let req_id = format!(
        "{:04x}:{}",
        state.instance_id,
        state.next_req_id.fetch_add(1, Ordering::Relaxed)
    );

    // Extract client identification headers
    let rctx = RequestContext::from_request(&state, &client_ip, &parts.headers, principal);
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
    let (body_bytes, oauth_body_bytes, model, fp, cache_key) =
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

            // LAB-933/LAB-929: derive the response-cache key on the
            // PRE-injection body — the request exactly as the client sent
            // it — so the deterministic auto-cache/OAuth mutations below
            // never affect cache identity. Gated here on opt-in (AC2), the
            // /v1/messages or /v1/messages/count_tokens path, and
            // non-streaming (AC8; same predicate as `request_wants_stream`,
            // evaluated on the parsed body). `surface` travels with the key
            // so both endpoints share one allow-list and one key scheme
            // (LAB-929 AC2/AC3) while staying unable to cross-serve.
            let cache_key = match &state.response_cache {
                // The per-client map doubles as the opt-in allow-list (AC2).
                Some(rc)
                    if rc.clients.contains_key(&client_id)
                        && parts.method == hyper::Method::POST
                        && !body_wants_stream(&parsed) =>
                {
                    let surface = match parts.uri.path() {
                        "/v1/messages" => Some(CacheSurface::Messages),
                        "/v1/messages/count_tokens" => Some(CacheSurface::CountTokens),
                        _ => None,
                    };
                    surface.map(|surface| {
                        (
                            response_cache_key(
                                &model,
                                &parsed,
                                &parts.headers,
                                parts.uri.query(),
                                &client_id,
                                &fp,
                                &fps,
                                surface.label(),
                            ),
                            surface,
                        )
                    })
                }
                _ => None,
            };

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
                cache_key,
            )
        } else {
            let clone = body_bytes.clone();
            (body_bytes, clone, String::new(), None, None)
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
        return *resp;
    }

    // LAB-933: serve an opted-in replay from the encrypted response cache.
    // Placed AFTER the gate so budget/emergency policy still applies to
    // opted-in clients; a hit then never touches an upstream — no rate-limit
    // headroom burned, no budget decrement, no usage recorded (AC9). Only
    // the hit counter and a digest-only log line observe it (AC5).
    if let Some((key, surface)) = cache_key.as_ref().map(|(k, s)| (k.as_str(), *s)) {
        if let Some(rc) = &state.response_cache {
            if let Some(entry) = rc.lookup(&client_id, key, surface).await {
                info!(
                    req_id,
                    client_id = %client_id,
                    key_digest = key_digest_prefix(key),
                    "response cache hit"
                );
                return cached_hit_response(entry);
            }
        }
    }

    let n = state.endpoints.len();
    let mut last_saw_529 = false;
    let mut last_saw_transient = false;
    // Upstream error from the most recent model-unsupported rejection —
    // returned verbatim if the pool exhausts on nothing but rejections.
    let mut model_unsupported_resp: Option<Response> = None;
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
                                    affinity,
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
                                    &client_ip,
                                    &agent_id,
                                    &session_id,
                                    &model,
                                    i,
                                    request_start,
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
                &mut model_unsupported_resp,
            ) {
                // LAB-933: the single success seam — every proxied response
                // (Anthropic or translated OpenAI) exits proxy_handler here,
                // so the cache write lives in exactly one place.
                RetryStep::Return(resp) => {
                    return maybe_cache_store(
                        &state,
                        cache_key.as_ref().map(|(k, s)| (k.as_str(), *s)),
                        &client_id,
                        &req_id,
                        resp,
                    )
                    .await
                }
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

    // A pool exhausted purely by model rejections (no 529/transient in the
    // final round) returns the upstream's own error — truthful when the model
    // exists nowhere. Overload/transient exhaustion keeps its retryable
    // status; the negative cache already routes follow-up requests away from
    // the rejecting endpoints (LAB-941).
    if !last_saw_529 && !last_saw_transient {
        if let Some(resp) = model_unsupported_resp {
            return resp;
        }
        // Warm-cache path: every eligible endpoint was filtered by the
        // negative cache BEFORE any forward ran, so nothing was stashed.
        // Synthesize the same truthful 404 the first request returned —
        // a 429 here would invite retries of a permanently-failing model.
        if state.model_unsupported_everywhere(&model) {
            warn!(model, "model unsupported on all eligible endpoints");
            return model_unsupported_response(&model, false);
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
/// Non-streaming responses are accounted via `finalize_non_stream`: OpenAI
/// bodies carry `usage.prompt_tokens`/`completion_tokens`, mapped to
/// input/output tokens so per-client token + budget enforcement sees this
/// spend (LAB-712). Streaming responses still record nothing — the SSE
/// translator does not yet extract incremental usage (rides with LAB-717).
#[allow(clippy::too_many_arguments)]
async fn try_fallback_upstream(
    state: &AppState,
    body_bytes: &[u8],
    req_id: &str,
    client_id: &str,
    client_ip: &std::net::IpAddr,
    agent_id: &str,
    session_id: &str,
    model: &str,
    endpoint_idx: usize,
    request_start: std::time::Instant,
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
        let openai_body = match translate_anthropic_request_to_openai(&parsed) {
            Ok(v) => v,
            Err(msg) => {
                warn!(
                    req_id,
                    upstream = ep.name,
                    error = %msg,
                    "fallback: request not representable in OpenAI format"
                );
                // Terminal, not a retry: the request itself is the problem, so
                // rotating to another endpoint would just fail the same way.
                return ForwardOutcome::Done(Box::new(untranslatable_request_response(&msg)));
            }
        };
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
    let resp = match http_client
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

    // Classify 429 / 529 / other 5xx via the shared helper so OpenAI
    // endpoints get the same policy as the Anthropic paths: a 429 marks the
    // endpoint hard-limited (honouring retry-after) so `pick_endpoint` skips
    // it for the cooldown window, and a 529 flags the long-base BEBO backoff.
    // Previously this was a bare rotate — every subsequent request re-hammered
    // the still-rate-limited endpoint before rotating (GH #97).
    // Downstream parses OpenAI errors on the passthrough path
    // (translate = false); Anthropic errors when translating back.
    let mut resp =
        match classify_retry_status(state, status, &ep.rate_info, &ep.name, resp, !translate).await
        {
            Ok(resp) => resp,
            Err(outcome) => return outcome,
        };

    if !status.is_success() {
        let err_body = resp
            .text()
            .await
            .unwrap_or_else(|_| "upstream error".to_string());
        warn!(
            req_id,
            upstream = ep.name,
            status = status.as_u16(),
            body = %err_body,
            "fallback: unified endpoint returned error"
        );
        // Gateway rejected the MODEL (e.g. LiteLLM "Invalid model name"):
        // negative-cache the pair and rotate instead of handing the client a
        // misleading "your request is invalid" 400 — the model is fine, this
        // endpoint just doesn't serve it (LAB-941, observed 2026-07-27 when a
        // 529 storm drained the Anthropic pool into insight-gateway).
        let model_unsupported = serde_json::from_str::<serde_json::Value>(&err_body)
            .map(|v| is_model_unsupported_error(status, &v))
            .unwrap_or(false);
        let response = if translate {
            // Return error in Anthropic format
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
                })
        } else {
            Response::builder()
                .status(status)
                .header("content-type", "application/json")
                .body(Body::from(err_body))
                .unwrap_or_else(|_| {
                    (StatusCode::INTERNAL_SERVER_ERROR, "fallback error").into_response()
                })
        };
        if model_unsupported {
            state.note_model_unsupported(&ep.name, endpoint_idx, model);
            return ForwardOutcome::RetryModelUnsupported(Box::new(response));
        }
        return ForwardOutcome::Done(Box::new(response));
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

        return ForwardOutcome::Done(Box::new(
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
        ));
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

    // OpenAI non-streaming bodies carry usage.prompt_tokens/completion_tokens —
    // record them like the Anthropic path so per-client token + budget
    // enforcement (`pre_request_gate`) sees OpenAI-endpoint spend (LAB-712).
    let openai_resp: serde_json::Value = match serde_json::from_slice(&resp_body) {
        Ok(v) => v,
        Err(e) => {
            // Still serve the response (passthrough forwards the raw bytes),
            // but the malformed body means usage records as zero — say so.
            warn!(
                req_id,
                client_id,
                model,
                upstream = ep.name,
                error = %e,
                "fallback: unified endpoint response body is not valid JSON; usage not recorded"
            );
            serde_json::json!({})
        }
    };
    let usage = TokenUsage::from_openai_response_body(&openai_resp);
    finalize_non_stream(
        state,
        ep,
        req_id,
        client_id,
        model,
        openai_resp.get("model").and_then(|v| v.as_str()),
        &ep.name,
        &client_ip.to_string(),
        agent_id,
        session_id,
        status.as_u16(),
        &usage,
        request_start.elapsed().as_millis() as u64,
        !translate, // openai_compat marks the client surface: translate=false ⇒ OpenAI-format caller
        // OpenAI-protocol endpoints don't consume Anthropic context windows —
        // the session registry (LAB-916) tracks Anthropic-bound traffic only.
        None,
        0,
    )
    .await;

    if translate {
        let anthropic_resp = translate_openai_response_to_anthropic(&openai_resp);
        info!(
            req_id,
            upstream = ep.name,
            "fallback: unified endpoint translated response"
        );
        ForwardOutcome::Done(Box::new(
            Response::builder()
                .status(StatusCode::OK)
                .header("content-type", "application/json")
                .body(Body::from(anthropic_resp.to_string()))
                .unwrap_or_else(|_| {
                    (StatusCode::INTERNAL_SERVER_ERROR, "fallback error").into_response()
                }),
        ))
    } else {
        info!(
            req_id,
            upstream = ep.name,
            "fallback: unified endpoint forwarded response"
        );
        ForwardOutcome::Done(Box::new(
            Response::builder()
                .status(StatusCode::OK)
                .header("content-type", "application/json")
                .body(Body::from(resp_body))
                .unwrap_or_else(|_| {
                    (StatusCode::INTERNAL_SERVER_ERROR, "fallback error").into_response()
                }),
        ))
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
    // AC-8: the ONLY client_addr.ip() read in this handler.
    let client_ip = state.resolve_client_ip(client_addr.ip(), req.headers());
    if !state.is_ip_allowed(&client_ip) {
        return (StatusCode::FORBIDDEN, "forbidden").into_response();
    }
    // AC-4: operator principal required — /_stats discloses other clients'
    // ids, the endpoint account names and pool utilisation.
    if let Some(resp) = state.authorize_admin(&client_ip, req.headers(), "stats") {
        return *resp;
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
        // Live sessions by context-window occupancy, hottest first (LAB-916).
        // Session labels are hashes of the affinity key; raw IPs/session ids
        // never leave the process.
        "sessions": state.sessions_snapshot(now_epoch),
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
    // AC-8: the ONLY client_addr.ip() read in this handler.
    let client_ip = state.resolve_client_ip(client_addr.ip(), req.headers());
    if !state.is_ip_allowed(&client_ip) {
        return (StatusCode::FORBIDDEN, "forbidden").into_response();
    }
    // AC-4: operator principal required, same gate as /_stats — per-account
    // utilisation and budget gauges are pool reconnaissance.
    if let Some(resp) = state.authorize_admin(&client_ip, req.headers(), "metrics") {
        return *resp;
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
    let client_model_usage: Vec<((String, String), [u64; 4])> = state
        .client_model_usage
        .lock()
        .ok()
        .map(|g| g.iter().map(|(k, v)| (k.clone(), *v)).collect())
        .unwrap_or_default();
    let budget_usage = state
        .budget_usage
        .lock()
        .ok()
        .map(|g| g.clone())
        .unwrap_or_default();
    let cluster_info = state.cluster_info_cache.lock().ok().and_then(|g| g.clone());
    let prompt_too_long: Vec<(String, u64)> = state
        .prompt_too_long
        .lock()
        .ok()
        .map(|g| g.iter().map(|(k, v)| (k.clone(), *v)).collect())
        .unwrap_or_default();
    let model_denied: Vec<((String, String), u64)> = state
        .model_denied
        .lock()
        .ok()
        .map(|g| g.iter().map(|(k, v)| (k.clone(), *v)).collect())
        .unwrap_or_default();
    let beta_flags_dropped: Vec<(String, u64)> = state
        .beta_flags_dropped
        .lock()
        .ok()
        .map(|g| g.iter().map(|(k, v)| (k.clone(), *v)).collect())
        .unwrap_or_default();
    let auth_failures: Vec<(&'static str, u64)> = state
        .auth_failures
        .lock()
        .ok()
        .map(|g| g.iter().map(|(k, v)| (*k, *v)).collect())
        .unwrap_or_default();
    let (session_buckets, session_tokens_sum) = state.session_tokens_histogram(now_epoch);

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

    // Per-(client, model) usage (LAB-2330). Sibling of the per-client family
    // above — that one stays authoritative for per-client totals; this one
    // adds the model dimension so per-model pricing can be applied downstream.
    // Operators aggregate into `_operator` per model, matching the house
    // pattern. Cardinality is bounded at record time (MAX_CLIENT_MODEL_LABELS).
    prom_header(
        &mut buf,
        "anthropic_client_model_token_usage_total",
        "counter",
        "Per-client token usage by model and type",
    );
    let mut op_model_tokens: HashMap<&str, [u64; 4]> = HashMap::new();
    for ((client, mdl), tokens) in &client_model_usage {
        if state.is_operator(client) {
            let e = op_model_tokens.entry(mdl.as_str()).or_insert([0; 4]);
            for i in 0..4 {
                e[i] += tokens[i];
            }
        } else {
            for (i, t) in types.iter().enumerate() {
                prom_counter(
                    &mut buf,
                    "anthropic_client_model_token_usage_total",
                    &[("client", client), ("model", mdl), ("type", t)],
                    tokens[i],
                );
            }
        }
    }
    for (mdl, tokens) in &op_model_tokens {
        for (i, t) in types.iter().enumerate() {
            prom_counter(
                &mut buf,
                "anthropic_client_model_token_usage_total",
                &[("client", "_operator"), ("model", mdl), ("type", t)],
                tokens[i],
            );
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

    // Upstream context-window overflows (LAB-916). Per-replica, in-memory —
    // sessions themselves stay on `/_stats` (per-session Prometheus labels
    // would be a cardinality anti-pattern).
    prom_header(
        &mut buf,
        "anthropic_prompt_too_long_total",
        "counter",
        "Upstream 'prompt is too long' 400 responses by model",
    );
    for (model, n) in &prompt_too_long {
        prom_counter(
            &mut buf,
            "anthropic_prompt_too_long_total",
            &[("model", model.as_str())],
            *n,
        );
    }

    // Per-client model-allowlist denials (LAB-1083). A non-zero rate here is
    // either a misconfigured caller or a caller reaching for capacity it was
    // deliberately denied — both worth an alert.
    prom_header(
        &mut buf,
        "anthropic_client_model_denied_total",
        "counter",
        "Requests rejected (403) by the per-client model allow-list",
    );
    for ((client, model), n) in &model_denied {
        prom_counter(
            &mut buf,
            "anthropic_client_model_denied_total",
            &[("client", client.as_str()), ("model", model.as_str())],
            *n,
        );
    }

    // Client anthropic-beta flags dropped by the allow-list (LAB-1191).
    // Per-replica, in-memory; bounded via `_other` overflow.
    prom_header(
        &mut buf,
        "anthropic_beta_flag_dropped_total",
        "counter",
        "Client anthropic-beta flags dropped by the allow-list",
    );
    for (flag, n) in &beta_flags_dropped {
        prom_counter(
            &mut buf,
            "anthropic_beta_flag_dropped_total",
            &[("flag", flag.as_str())],
            *n,
        );
    }

    // Failed authentication attempts (LAB-1192). A non-zero rate on a public
    // ingress is credential scanning — alert on it.
    prom_header(
        &mut buf,
        "anthropic_auth_failures_total",
        "counter",
        "Requests rejected for auth — invalid/missing credential OR throttle 429 — by route",
    );
    for (route, n) in &auth_failures {
        prom_counter(
            &mut buf,
            "anthropic_auth_failures_total",
            &[("route", route)],
            *n,
        );
    }

    // Live session-size distribution (LAB-957). A snapshot of the session
    // registry, not an observation stream: values rise AND fall as sessions
    // grow and expire, so this is declared `gauge` — chart instant values,
    // never rate(). Cumulative `le` buckets keep Grafana heatmaps and
    // histogram_quantile() working, and sum across replicas.
    prom_header(
        &mut buf,
        "anthropic_session_tokens_bucket",
        "gauge",
        "Live sessions with last-prompt tokens <= le (cumulative snapshot; instant values, do not rate())",
    );
    for (i, le) in SESSION_TOKENS_BUCKETS.iter().enumerate() {
        prom_gauge(
            &mut buf,
            "anthropic_session_tokens_bucket",
            &[("le", &le.to_string())],
            session_buckets[i] as f64,
        );
    }
    prom_gauge(
        &mut buf,
        "anthropic_session_tokens_bucket",
        &[("le", "+Inf")],
        session_buckets[SESSION_TOKENS_BUCKETS.len()] as f64,
    );
    prom_header(
        &mut buf,
        "anthropic_session_tokens_sum",
        "gauge",
        "Sum of last-prompt tokens across live sessions",
    );
    prom_gauge(
        &mut buf,
        "anthropic_session_tokens_sum",
        &[],
        session_tokens_sum as f64,
    );
    prom_header(
        &mut buf,
        "anthropic_session_tokens_count",
        "gauge",
        "Number of live sessions",
    );
    prom_gauge(
        &mut buf,
        "anthropic_session_tokens_count",
        &[],
        session_buckets[SESSION_TOKENS_BUCKETS.len()] as f64,
    );

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

    // LAB-933/LAB-929 response cache counters (AC12 / LAB-929 AC4). Emitted
    // only when the cache is configured; `messages` and `count_tokens` are
    // separate series on the same metric names, distinguished by the
    // `surface` label. Looped metric-major (header, then both surfaces'
    // samples, then the next metric) so each family's samples stay
    // contiguous — the exposition format requires all samples of one metric
    // grouped together, and OpenMetrics-strict scrapers reject interleaving.
    if let Some(rc) = &state.response_cache {
        type SurfaceCounter = fn(&ResponseCache, CacheSurface) -> &AtomicU64;
        let series: [(&str, &str, SurfaceCounter); 4] = [
            (
                "anthropic_response_cache_hits_total",
                "Requests served from the response cache (no upstream call, no headroom burned)",
                ResponseCache::hits_for,
            ),
            (
                "anthropic_response_cache_misses_total",
                "Opted-in cacheable requests that proceeded upstream on a cache miss",
                ResponseCache::misses_for,
            ),
            (
                "anthropic_response_cache_stores_total",
                "2xx responses written to the response cache",
                ResponseCache::stores_for,
            ),
            (
                "anthropic_response_cache_errors_total",
                "Response cache operations that failed or timed out (request failed open)",
                ResponseCache::errors_for,
            ),
        ];
        for (name, help, counter_for) in series {
            prom_header(&mut buf, name, "counter", help);
            for surface in [CacheSurface::Messages, CacheSurface::CountTokens] {
                prom_counter(
                    &mut buf,
                    name,
                    &[("surface", surface.label())],
                    counter_for(rc, surface).load(Ordering::Relaxed),
                );
            }
        }
    }

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

/// True when an OpenAI-format request asked for `response_format: json_object`.
/// Single predicate shared by the request-side system nudge and the
/// response-side fence strip so the two can never disagree on what JSON mode is.
fn wants_json_object(body: &serde_json::Value) -> bool {
    body.get("response_format")
        .and_then(|rf| rf.get("type"))
        .and_then(|t| t.as_str())
        == Some("json_object")
}

/// Strip markdown JSON fences from LLM output.
/// Claude sometimes wraps JSON in ```json ... ``` even when told not to.
/// Clients using response_format: json_object (e.g. Vercel AI SDK's generateObject)
/// need raw JSON or their parse step blows up.
/// Only applied when the request asked for JSON mode — a normal chat reply
/// that legitimately is a fenced code block must pass through verbatim.
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
    /// Request asked for `response_format: json_object`.
    json_mode: bool,
    /// Text content buffered while `json_mode` is set, flushed fence-stripped
    /// at end-of-message so streaming content matches the non-streaming strip.
    text_buffer: String,
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
            json_mode: false,
            text_buffer: String::new(),
        }
    }
}

impl StreamContext {
    /// In JSON mode, drain the buffered text and strip fences over the whole
    /// message — the streaming equivalent of the non-streaming strip, applied
    /// once so fences split across deltas are handled. Returns None when not in
    /// JSON mode, the buffer is empty, or the strip yields nothing.
    fn take_stripped_json_content(&mut self) -> Option<String> {
        if !self.json_mode || self.text_buffer.is_empty() {
            return None;
        }
        let stripped = strip_json_fences(&self.text_buffer);
        self.text_buffer.clear();
        (!stripped.is_empty()).then_some(stripped)
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
    if wants_json_object(body) {
        system_parts.push(
            "You must respond with valid JSON only. No markdown, no code fences, no explanation — just raw JSON.".to_string(),
        );
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

fn translate_anthropic_to_openai(body: &serde_json::Value, json_mode: bool) -> serde_json::Value {
    let id = body
        .get("id")
        .and_then(|v| v.as_str())
        .unwrap_or("msg_unknown");
    let model = body
        .get("model")
        .and_then(|v| v.as_str())
        .unwrap_or("unknown");

    let blocks = body.get("content").and_then(|c| c.as_array());

    // Concatenate text content blocks; strip markdown JSON fences only when
    // the request asked for response_format: json_object.
    let content = blocks
        .map(|blocks| {
            let raw = blocks
                .iter()
                .filter(|b| b.get("type").and_then(|t| t.as_str()) == Some("text"))
                .filter_map(|b| b.get("text").and_then(|t| t.as_str()))
                .collect::<Vec<_>>()
                .join("");
            if json_mode {
                strip_json_fences(&raw)
            } else {
                raw
            }
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
                if ctx.json_mode {
                    // Buffer JSON-mode text; fences are stripped once over the
                    // whole message at message_delta (they may be split across
                    // deltas). Partial JSON is unparseable, so deferring the
                    // emit costs the client nothing.
                    ctx.text_buffer.push_str(text);
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
            let finish = make_openai_chunk(
                ctx,
                serde_json::json!({}),
                Some(map_stop_reason(stop_reason)),
            );
            // Flush buffered JSON-mode content (fence-stripped) as its own chunk
            // before the finish chunk, preserving OpenAI content→finish order.
            match ctx.take_stripped_json_content() {
                Some(content) => {
                    let content_chunk =
                        make_openai_chunk(ctx, serde_json::json!({ "content": content }), None);
                    Some(format!("{content_chunk}{finish}"))
                }
                None => Some(finish),
            }
        }
        "message_stop" => {
            // Safety net: if message_delta never arrived (abnormal upstream),
            // flush buffered JSON-mode content before closing so it is never
            // silently dropped.
            match ctx.take_stripped_json_content() {
                Some(content) => {
                    let content_chunk =
                        make_openai_chunk(ctx, serde_json::json!({ "content": content }), None);
                    Some(format!("{content_chunk}data: [DONE]\n\n"))
                }
                None => Some("data: [DONE]\n\n".to_string()),
            }
        }
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
/// `data:` URL. Err when the source can't be represented faithfully — missing
/// fields, or a source type this translator doesn't handle (e.g. Anthropic's
/// `file` source) — so the caller fails the request loudly instead of silently
/// dropping the image.
fn anthropic_image_block_to_openai(block: &serde_json::Value) -> Result<serde_json::Value, String> {
    let source = block
        .get("source")
        .ok_or("image block missing \"source\"")?;
    let source_type = source
        .get("type")
        .and_then(|t| t.as_str())
        .ok_or("image source missing \"type\"")?;
    let url = match source_type {
        "url" => source
            .get("url")
            .and_then(|u| u.as_str())
            .ok_or("image url source missing \"url\"")?
            .to_string(),
        "base64" => {
            let media_type = source
                .get("media_type")
                .and_then(|m| m.as_str())
                .ok_or("image base64 source missing \"media_type\"")?;
            let data = source
                .get("data")
                .and_then(|d| d.as_str())
                .ok_or("image base64 source missing \"data\"")?;
            format!("data:{};base64,{}", media_type, data)
        }
        other => {
            return Err(format!(
                "image source type \"{other}\" is not supported by the OpenAI-compat translator"
            ));
        }
    };
    Ok(serde_json::json!({"type": "image_url", "image_url": {"url": url}}))
}

/// Anthropic user content blocks → OpenAI content: a plain string when text-only
/// (the common case, and what OpenAI-compat upstreams handle most reliably), a
/// content-part array when image blocks are present so images survive translation.
/// Err on an unrepresentable image or an unsupported block type (e.g. `document`)
/// rather than dropping content silently.
fn anthropic_user_blocks_to_openai_content(
    blocks: &[&serde_json::Value],
) -> Result<serde_json::Value, String> {
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
                parts.push(anthropic_image_block_to_openai(b)?);
                has_image = true;
            }
            Some(other) => {
                return Err(format!(
                    "content block type \"{other}\" is not supported by the OpenAI-compat translator"
                ));
            }
            None => {}
        }
    }
    Ok(if has_image {
        serde_json::Value::Array(parts)
    } else {
        let text: String = parts
            .iter()
            .filter_map(|p| p.get("text").and_then(|t| t.as_str()))
            .collect::<Vec<_>>()
            .join("");
        serde_json::Value::String(text)
    })
}

/// Err (with a client-facing message) when a message contains an image this
/// translator can't represent faithfully — the caller must surface a real
/// error instead of silently forwarding a request with the image dropped.
fn translate_anthropic_request_to_openai(
    body: &serde_json::Value,
) -> Result<serde_json::Value, String> {
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
                        let user_content = anthropic_user_blocks_to_openai_content(&leftover)?;
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
                        let user_content = anthropic_user_blocks_to_openai_content(&block_refs)?;
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

    Ok(serde_json::Value::Object(out))
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
    session_key: Option<&str>,
    is_streaming: bool,
    json_mode: bool,
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
    let dropped = inject_account_auth(
        &mut headers,
        token,
        passthrough,
        &state.allowed_client_betas,
    );
    state.record_dropped_beta_flags(client_id, &dropped);

    // Session registry window (LAB-916). OpenAI-compat callers can't send the
    // `context-1m` beta through translation, but check anyway — the header is
    // forwarded when present. Read from the FILTERED outbound headers so the
    // accounting matches what the upstream actually ran under (PR #116
    // review), same as forward_anthropic.
    let context_window = context_window_for(model, request_has_1m_beta(&headers));

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
    let mut resp =
        match classify_retry_status(state, status, rate_info, endpoint_name, resp, true).await {
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
        let mut model_unsupported = false;
        let openai_error =
            if let Ok(parsed) = serde_json::from_slice::<serde_json::Value>(&error_body) {
                // Count + trace context-window overflows here too (LAB-916) —
                // this path consumes the same Anthropic windows as the native one.
                if status.as_u16() == 400 {
                    if let Some(msg) = prompt_too_long_message(&parsed) {
                        state.note_prompt_too_long(req_id, model, session_key, msg);
                    }
                }
                // Same model-rejection detection as the native path (LAB-941).
                model_unsupported = is_model_unsupported_error(status, &parsed);
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
        if model_unsupported {
            state.note_model_unsupported(endpoint_name, endpoint_idx, model);
            return ForwardOutcome::RetryModelUnsupported(Box::new(response));
        }
        return ForwardOutcome::Done(Box::new(response));
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
        let session_key_clone = session_key.map(str::to_owned);
        let status_code = status.as_u16();

        tokio::spawn(async move {
            let mut buffer: Vec<u8> = Vec::new();
            let mut scanner = SseUsageScanner::default();
            let mut ctx = StreamContext {
                json_mode,
                ..StreamContext::default()
            };
            let mut sent_done = false;

            let mut client_gone = false;
            let mut upstream_error = false;

            loop {
                match resp.chunk().await {
                    Ok(Some(chunk)) => {
                        scanner.push(&chunk);
                        buffer.extend_from_slice(&chunk);

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

            // Record usage scanned incrementally from the upstream SSE
            // (LAB-717). The detached task only holds a cloned
            // Arc<AppState>; re-index it.
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
                scanner,
                request_start,
                client_gone,
                upstream_error,
                true,
                session_key_clone.as_deref(),
                context_window,
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
        return ForwardOutcome::Done(Box::new(response));
    }

    // Non-streaming: buffer, translate, return
    let resp_bytes = match resp.bytes().await {
        Ok(b) => b,
        Err(e) => {
            error!("failed to read upstream response: {e}");
            return ForwardOutcome::Done(Box::new(
                (StatusCode::BAD_GATEWAY, "failed to read upstream response").into_response(),
            ));
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
            return ForwardOutcome::Done(Box::new(response));
        }
    };

    let openai_resp = translate_anthropic_to_openai(&anthropic_resp, json_mode);

    // Extract and record token usage from non-streaming response
    let usage = TokenUsage::from_response_body(&anthropic_resp);
    finalize_non_stream(
        state,
        ep,
        req_id,
        client_id,
        model,
        anthropic_resp.get("model").and_then(|v| v.as_str()),
        endpoint_name,
        &client_ip.to_string(),
        agent_id,
        session_id,
        status.as_u16(),
        &usage,
        request_start.elapsed().as_millis() as u64,
        true,
        session_key,
        context_window,
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
    ForwardOutcome::Done(Box::new(response))
}

async fn openai_chat_handler(
    State(state): State<Arc<AppState>>,
    axum::extract::ConnectInfo(client_addr): axum::extract::ConnectInfo<SocketAddr>,
    req: Request<Body>,
) -> Response {
    // AC-8: the ONLY client_addr.ip() read in this handler.
    let client_ip = state.resolve_client_ip(client_addr.ip(), req.headers());
    let request_start = Instant::now();

    // IP allowlist check
    if !state.is_ip_allowed(&client_ip) {
        warn!(client = %client_ip, "rejected: IP not in allowlist");
        return (StatusCode::FORBIDDEN, "forbidden").into_response();
    }

    // Proxy auth: accept the credential from either x-api-key or
    // Authorization: Bearer — OpenAI SDKs send only the latter.
    let principal = match state.authenticate_throttled(&client_ip, req.headers(), true, "openai") {
        Ok(p) => p,
        Err(resp) => return *resp,
    };

    let (parts, body) = req.into_parts();

    let req_id = format!(
        "{:04x}:{}",
        state.instance_id,
        state.next_req_id.fetch_add(1, Ordering::Relaxed)
    );

    // Extract client identification headers
    let rctx = RequestContext::from_request(&state, &client_ip, &parts.headers, principal);
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
    let json_mode = wants_json_object(&openai_body);
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
        return *resp;
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
    // Upstream error from the most recent model-unsupported rejection —
    // returned verbatim if the pool exhausts on nothing but rejections.
    let mut model_unsupported_resp: Option<Response> = None;
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
                                    affinity,
                                    is_streaming,
                                    json_mode,
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
                                    &client_ip,
                                    &agent_id,
                                    &session_id,
                                    &model,
                                    i,
                                    request_start,
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
                &mut model_unsupported_resp,
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

    // Same model-rejection exhaustion rule as `proxy_handler` (LAB-941),
    // in the OpenAI error shape this handler's clients parse.
    if !last_saw_529 && !last_saw_transient {
        if let Some(resp) = model_unsupported_resp {
            return resp;
        }
        if state.model_unsupported_everywhere(&model) {
            warn!(model, "model unsupported on all eligible endpoints");
            return model_unsupported_response(&model, true);
        }
    }
    exhaustion_response(last_saw_transient, last_saw_529)
}

// ── Main ────────────────────────────────────────────────────────────

/// Validate endpoint configuration. Returns the first hard error encountered.
/// A non-canonical host on an anthropic endpoint is a hard error unless the
/// endpoint opts in via `allow_nonstandard_host` (then it degrades to the
/// `warn!`). Priority collision with an openai endpoint stays a soft warning.
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
                        // This endpoint's token is forwarded to that host on
                        // every request — a typo here is credential exfil, so
                        // it must be an explicit opt-in, not a scrolled-past
                        // warning (LAB-1191 / 2026-06-02 audit finding 1).
                        if ep.allow_nonstandard_host != Some(true) {
                            return Err(format!(
                                "endpoint '{}': base_url host '{}' is not api.anthropic.com — \
                                 the endpoint token would be sent to a non-Anthropic host. \
                                 Set allow_nonstandard_host = true on this endpoint if intentional",
                                ep.name,
                                host.as_deref().unwrap_or("<unparseable>")
                            ));
                        }
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

/// Startup validation for the `[[clients]]` registry (LAB-1083).
///
/// Every rule here exists because its violation fails SILENTLY at runtime
/// rather than loudly: a duplicate key resolves to whichever entry the scan
/// saw last, a duplicate or empty name silently merges two callers' budgets and
/// cache tenancy, and a `[response_cache].clients` typo makes the cache inert
/// for that client with no signal at all. Cheap to catch at boot; expensive to
/// notice in production.
fn validate_clients(config: &Config) -> Result<(), String> {
    let clients = &config.clients;
    let mut seen_names: Vec<&str> = Vec::with_capacity(clients.len());
    let mut seen_keys: Vec<&str> = Vec::with_capacity(clients.len());
    for c in clients {
        if c.name.trim().is_empty() {
            return Err("client: name must not be empty".to_string());
        }
        // Stored untrimmed, so " geo" would become a client_id that matches no
        // `client_budgets` / `operators` / `[response_cache].clients` key —
        // silently unenforced budget and cache tenancy. `resolve_client_id`
        // trims its header input; this path has nothing to trim it later.
        if c.name != c.name.trim() {
            return Err(format!(
                "client '{}': name must not have leading or trailing whitespace",
                c.name
            ));
        }
        if c.name == "-" {
            return Err(
                "client: name must not be \"-\" (the unknown-client sentinel — budget enforcement skips it)"
                    .to_string(),
            );
        }
        if c.name == "_operator" {
            return Err(
                "client: name must not be \"_operator\" (the reserved operator-aggregation label on /_stats and /metrics)"
                    .to_string(),
            );
        }
        if c.name == "_other" {
            return Err(
                "client: name must not be \"_other\" (the reserved metrics overflow-bucket label — a real client with this name would merge with, and take the warn-once flag of, the (\"_other\", \"_other\") overflow key)"
                    .to_string(),
            );
        }
        if c.key.is_empty() {
            return Err(format!("client '{}': key must not be empty", c.name));
        }
        // NOTE: the MIN_KEY_LEN strength floor (AC-13) is enforced in
        // validate_exposure, not here — this function's structural rules
        // (empty / duplicate / whitespace) are exercised by tests with
        // deliberately short keys, and the deployment-posture floor belongs
        // with the other posture checks. Both reference the shared const.
        if seen_names.contains(&c.name.as_str()) {
            return Err(format!("client '{}': duplicate name", c.name));
        }
        // Names, not keys, in the error — never log a credential.
        if seen_keys.contains(&c.key.as_str()) {
            return Err(format!(
                "client '{}': duplicate key (already used by another client)",
                c.name
            ));
        }
        seen_names.push(&c.name);
        seen_keys.push(&c.key);
    }

    // One client registry, not five. Every one of these config surfaces keys on
    // a client name, and every one of them fails SILENTLY on a typo — in the
    // dangerous direction: `check_budget` and `check_utilization_limit` both
    // return Ok(()) for an unknown client, so a mistyped budget means UNLIMITED
    // spend against the operator's accounts with no log line and no metric; a
    // mistyped `operators` entry silently gates the caller it was meant to
    // exempt; a mistyped `response_cache.clients` entry silently makes the
    // cache inert. Only enforceable when [[clients]] is configured — on the
    // legacy path client ids are header-derived and there is no registry to
    // check against.
    if clients.is_empty() {
        return Ok(());
    }

    // `token = "passthrough"` forwards the caller's auth headers to the
    // upstream UNTOUCHED (`inject_account_auth` returns before the
    // header-strip). Under [[clients]] the caller's `x-api-key` is its PROXY
    // credential, not an upstream one — so the two modes together would
    // transmit every client key verbatim to that endpoint's `base_url`. They
    // are contradictory by construction: passthrough means "the caller brings
    // its own upstream credential", [[clients]] means "that header is mine".
    // Reject rather than half-handle, exactly as with proxy_key above.
    if let Some(ep) = config.endpoints.iter().find(|e| e.token == "passthrough") {
        return Err(format!(
            "endpoint '{}': token = \"passthrough\" is incompatible with [[clients]] — passthrough forwards the caller's auth headers upstream, which would leak client keys to {}",
            ep.name,
            ep.base_url.as_deref().unwrap_or("https://api.anthropic.com")
        ));
    }

    let known = |name: &str| seen_names.contains(&name);
    for (surface, name) in std::iter::empty()
        .chain(config.client_budgets.keys().map(|k| ("client_budgets", k)))
        .chain(
            config
                .client_utilization_limits
                .keys()
                .map(|k| ("client_utilization_limits", k)),
        )
        .chain(config.operators.iter().map(|k| ("operators", k)))
        .chain(
            config
                .response_cache
                .iter()
                .flat_map(|rc| rc.clients.iter().map(|k| ("response_cache.clients", k))),
        )
    {
        if !known(name) {
            return Err(format!(
                "{surface}: \"{name}\" names no configured [[clients]] entry"
            ));
        }
    }
    Ok(())
}

/// Startup exposure posture (LAB-1192): unauthenticated is a BOOT FAILURE,
/// not a default. Same shape as `reject_legacy_config_keys` — a named error
/// at startup instead of a silent misconfiguration in production, where an
/// open proxy is indistinguishable from a configured one until someone
/// finds it.
fn validate_exposure(config: &Config) -> Result<(), String> {
    let has_credentials = config.proxy_key.is_some() || !config.clients.is_empty();
    let allow_unauthenticated = config.allow_unauthenticated.unwrap_or(false);
    if !has_credentials && !allow_unauthenticated {
        return Err(
            "config: no credentials configured — add [[clients]] entries (or legacy proxy_key), \
             or explicitly set allow_unauthenticated = true for a trusted-network-only deployment \
             (see README §Authentication)"
                .to_string(),
        );
    }
    // One escape hatch, one meaning. Credentials + allow_unauthenticated
    // together would make the flag silently dead (or worse, ambiguous) —
    // exactly the half-applied-migration state the mutual-exclusion rules
    // exist to reject.
    if has_credentials && allow_unauthenticated {
        return Err(
            "config: allow_unauthenticated = true is incompatible with configured credentials — \
             remove it, or remove [[clients]]/proxy_key (see README §Authentication)"
                .to_string(),
        );
    }
    // AC-13: a static bearer credential on a public ingress gets scanned;
    // below 32 characters the keyspace is the vulnerability.
    if let Some(ref key) = config.proxy_key {
        if key.len() < MIN_KEY_LEN {
            return Err(format!(
                "config: proxy_key is shorter than {MIN_KEY_LEN} characters — generate one with `openssl rand -hex 32`"
            ));
        }
    }
    for c in &config.clients {
        if c.key.len() < MIN_KEY_LEN {
            return Err(format!(
                "client '{}': key is shorter than {MIN_KEY_LEN} characters — generate one with `openssl rand -hex 32`",
                c.name
            ));
        }
    }
    if config
        .auth_failure_limit
        .unwrap_or(DEFAULT_AUTH_FAILURE_LIMIT)
        > 0
        && config
            .auth_failure_window_secs
            .unwrap_or(DEFAULT_AUTH_FAILURE_WINDOW_SECS)
            == 0
    {
        return Err(
            "config: auth_failure_window_secs must be > 0 (set auth_failure_limit = 0 to disable the throttle)"
                .to_string(),
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
    // LAB-1083: `proxy_key` is the legacy single shared secret, `[[clients]]`
    // its per-client replacement. Rejecting the combination rather than
    // precedence-ordering it is deliberate — a silent winner between two
    // authentication schemes is exactly the ambiguity that gets an operator's
    // migration half-applied and the weaker one left in force.
    if table.contains_key("proxy_key") && table.contains_key("clients") {
        return Err(
            "config: proxy_key and [[clients]] are mutually exclusive — [[clients]] supersedes it; remove proxy_key (see README §Authentication)"
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
    if let Err(msg) = validate_clients(&config) {
        panic!("config: {msg}");
    }
    if let Err(msg) = validate_exposure(&config) {
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

    // Parse IP allowlist + trusted proxy list (LAB-1192) — same IP/CIDR syntax.
    let allowed_ips = parse_ip_entries(config.allowed_ips.as_deref(), "allowed_ips");
    let trusted_proxies = parse_ip_entries(config.trusted_proxies.as_deref(), "trusted_proxies");
    if allowed_ips.is_empty() {
        warn!("IP allowlist DISABLED — all source IPs accepted");
    } else {
        info!(count = allowed_ips.len(), "IP allowlist enabled");
    }
    if !trusted_proxies.is_empty() {
        info!(
            count = trusted_proxies.len(),
            "trusted proxies configured — x-forwarded-for honoured from these peers"
        );
    } else if !config.clients.is_empty() || config.proxy_key.is_some() {
        // Credentials but no trusted_proxies: behind a load balancer every
        // client collapses to the LB's peer IP, leaving one shared allowlist
        // decision and one shared invalid-credential throttle bucket. Valid
        // credentials still pass, but source attribution and per-client
        // failure isolation are lost. Warn loudly (LAB-1192, amended by
        // LAB-1193).
        warn!(
            "no trusted_proxies configured — if this instance sits behind a load balancer, \
             all clients share the LB's peer IP for the allowlist decision and one \
             invalid-credential throttle bucket; valid credentials still pass, but \
             per-client source attribution and failure isolation are lost; set \
             trusted_proxies to the LB's address range"
        );
    }

    // Operators gate /_stats + /metrics under [[clients]] (LAB-1192 AC-4). An
    // empty operators list there means NO principal can read them — a silent
    // way to blind a monitoring scrape. Warn so the omission is visible.
    if !config.clients.is_empty() && config.operators.is_empty() {
        warn!(
            "[[clients]] configured with an empty operators list — /_stats and /metrics will \
             reject EVERY caller (403); name at least one client in operators or your \
             monitoring scrape goes blind"
        );
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

    if !config.clients.is_empty() {
        let with_allowlist = config
            .clients
            .iter()
            .filter(|c| !c.models.is_empty())
            .count();
        info!(
            clients = config.clients.len(),
            with_model_allowlist = with_allowlist,
            "per-client authentication enabled — x-client-id is ignored, identity comes from the credential"
        );
    } else if config.proxy_key.is_some() {
        warn!("legacy shared proxy_key in use — every caller shares one identity; migrate to [[clients]] (see README §Authentication)");
    } else {
        // validate_exposure guarantees this state is only reachable with the
        // flag explicitly set (AC-2).
        warn!(
            "allow_unauthenticated = true — proxy, /_stats and /metrics accept unauthenticated \
             traffic; safe ONLY on a trusted network (NetworkPolicy/tailnet), never on a public ingress"
        );
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

    // Set up Redis connection for distributed state (if configured).
    // Timeout budgets carry over from the old ConnectionManager: 2s per
    // command, 5s to establish a connection. The startup contract (LAB-1639):
    // the connect runs in the BACKGROUND under the retry-forever reconnect
    // policy — startup never blocks on Redis and never aborts for an
    // unreachable backend. A process that boots during a backend outage
    // serves local-only (coordination ops gated off via
    // `coordination_redis`) and attaches automatically when the backend
    // becomes reachable; `None` here means a config error (unparseable URL),
    // which stays local-only for the process lifetime. Mid-run outage
    // behaviour is unchanged from LAB-932 AC5: once connected, drops
    // reconnect with capped exponential backoff.
    let redis = if let Some(ref url) = config.redis_url {
        let perf = PerformanceConfig {
            default_command_timeout: REDIS_COMMAND_TIMEOUT,
            ..Default::default()
        };
        let conn_config = ConnectionConfig {
            // Keep connection_timeout in step with REDIS_STARTUP_GRACE: the
            // startup WARN is timed to fire after one full connect budget.
            connection_timeout: Duration::from_secs(5),
            internal_command_timeout: Duration::from_secs(5),
            ..Default::default()
        };
        // 0 = retry forever; delays 100ms → 30s, doubling.
        let policy = ReconnectPolicy::new_exponential(0, 100, 30_000, 2);
        match start_coordination_redis(url.as_str(), perf, conn_config, policy) {
            Ok(client) => Some(client),
            Err(e) => {
                warn!(error = %e, "invalid redis_url — running in local-only mode");
                None
            }
        }
    } else {
        None
    };

    // LAB-933: opt-in encrypted response cache. Invalid CONFIG fails startup
    // — an operator who configured a cache must not silently run without
    // one. An unreachable BACKEND does not: operations fail open per-request.
    let response_cache = match config.response_cache {
        Some(ref cfg) => match ResponseCache::from_config(cfg).await {
            Ok(Some(rc)) => {
                info!(
                    clients = rc.clients.len(),
                    backend = %cfg.backend,
                    ttl_secs = cfg.ttl_secs.unwrap_or(ResponseCache::DEFAULT_TTL_SECS),
                    "response cache enabled"
                );
                Some(rc)
            }
            Ok(None) => {
                info!("response_cache configured with an empty client allow-list — inert");
                None
            }
            Err(msg) => panic!("response_cache config error: {msg}"),
        },
        None => None,
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
        clients: config.clients.clone(),
        allowed_ips,
        trusted_proxies,
        auth_throttle: AuthThrottle::new(
            config
                .auth_failure_limit
                .unwrap_or(DEFAULT_AUTH_FAILURE_LIMIT),
            Duration::from_secs(
                config
                    .auth_failure_window_secs
                    .unwrap_or(DEFAULT_AUTH_FAILURE_WINDOW_SECS),
            ),
        ),
        auth_failures: Mutex::new(HashMap::new()),
        open_admin_warn: Mutex::new(HashMap::new()),
        client_names: config.client_names.clone(),
        auto_cache: config.auto_cache.unwrap_or(true),
        client_usage: Mutex::new(HashMap::new()),
        client_model_usage: Mutex::new(HashMap::new()),
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
        redis_ever_connected: AtomicBool::new(false),
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
        sessions: Mutex::new(HashMap::new()),
        session_registry_max: config
            .session_registry_max
            .unwrap_or(DEFAULT_SESSION_REGISTRY_MAX),
        session_registry_ttl_secs: config
            .session_registry_ttl_secs
            .unwrap_or(DEFAULT_SESSION_REGISTRY_TTL_SECS),
        expose_upstream_ratelimit_headers: config
            .expose_upstream_ratelimit_headers
            .unwrap_or(false),
        allowed_client_betas: config.allowed_client_betas.clone().unwrap_or_else(|| {
            DEFAULT_CLIENT_BETA_ALLOWLIST
                .iter()
                .map(|s| s.to_string())
                .collect()
        }),
        beta_flags_dropped: Mutex::new(HashMap::new()),
        prompt_too_long: Mutex::new(HashMap::new()),
        model_denied: Mutex::new(HashMap::new()),
        unsupported_models: Mutex::new(HashMap::new()),
        response_cache,
    });

    if state.auto_cache {
        info!("auto-cache enabled");
    }

    // Observe the coordination client's first connect: opens the
    // `coordination_redis` gate and owns the startup connected/unreachable
    // log lines (LAB-1639).
    state.spawn_redis_connect_watcher();

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
                if let Some(redis) = sync_state.coordination_redis() {
                    let key = format!("alb:heartbeat:{instance_id}");
                    let _ = redis_set_ex(redis, &key, AppState::now_epoch().to_string(), 30).await;
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
