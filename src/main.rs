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
        atomic::{AtomicU64, AtomicUsize, Ordering},
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
    upstream: String,
    #[allow(dead_code)]
    strategy: Option<String>,
    rate_limit_cooldown_secs: Option<u64>,
    /// Seconds between utilization probes per account (0 = disabled). Default: 300 (5 min)
    probe_interval_secs: Option<u64>,
    /// Shared secret clients must send as x-api-key to access the proxy. None = open.
    proxy_key: Option<String>,
    /// Source IP allowlist. Supports individual IPs and CIDR ranges. None/empty = allow all.
    allowed_ips: Option<Vec<String>>,
    accounts: Vec<AccountConfig>,
    /// OpenAI-compatible upstream routes. Requests to /upstream/<name>/... are forwarded.
    #[serde(default)]
    upstreams: Vec<UpstreamConfig>,
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
}

#[derive(Deserialize, Clone)]
struct AccountConfig {
    name: String,
    /// Auth token. Use "passthrough" to forward caller's auth headers as-is.
    token: String,
    /// Optional model allowlist. If set, this account only serves these models.
    /// Supports exact names ("claude-sonnet-4-6") and prefixes ("claude-opus-*").
    #[serde(default)]
    models: Vec<String>,
}

#[derive(Deserialize, Clone)]
struct UpstreamConfig {
    name: String,
    base_url: String,
    api_key: String,
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
    /// Counts consecutive burst 429s (no retry-after) for exponential backoff.
    /// Reset to 0 on any successful response.
    consecutive_burst_429s: u32,
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

struct Account {
    name: String,
    token: String,
    passthrough: bool,
    /// Model allowlist — empty means all models allowed.
    models: Vec<String>,
    requests: AtomicU64,
    rate_info: RwLock<RateLimitInfo>,
    /// Per-account burn rate tracker (requests per minute EWMA)
    burn_rate: Mutex<BurnRate>,
    // Token usage counters (atomic for lock-free concurrent updates)
    input_tokens: AtomicU64,
    output_tokens: AtomicU64,
    cache_creation_tokens: AtomicU64,
    cache_read_tokens: AtomicU64,
    /// Representative routing weight (f64 stored as u64 bits). Refreshed by
    /// `refresh_metrics_weights()` once per probe cycle. Backs the
    /// `anthropic_account_routing_weight` Prometheus gauge.
    last_routing_weight: AtomicU64,
    /// Representative routing share (weight/total, 0.0-1.0, f64 as bits).
    /// Refreshed by `refresh_metrics_weights()` once per probe cycle. Backs
    /// the `anthropic_account_routing_share` Prometheus gauge.
    last_routing_share: AtomicU64,
    /// Effective routing gate (f64 as bits): max(gate_5h, gate_7d) after
    /// time-adjustment and status floors. Refreshed by `refresh_metrics_weights()`.
    last_effective_gate: AtomicU64,
}

struct Upstream {
    name: String,
    base_url: String,
    api_key: String,
    requests: AtomicU64,
}

struct AppState {
    client: Client,
    upstream: String,
    accounts: Vec<Account>,
    robin: AtomicUsize,
    routing_strategy: RoutingStrategy,
    cooldown: Duration,
    state_path: PathBuf,
    proxy_key: Option<String>,
    allowed_ips: Vec<IpAllowEntry>,
    upstreams: Vec<Upstream>,
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
}

impl Account {
    /// Check if this account can serve the given model.
    fn serves_model(&self, model: &str) -> bool {
        if self.models.is_empty() || model.is_empty() {
            return true; // no filter or no model = allow all
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

#[derive(Clone, Copy, Debug)]
struct RoutingCandidate {
    idx: usize,
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
    accounts: Vec<PersistedAccount>,
    #[serde(default)]
    saved_at: u64,
}

#[derive(Serialize, Deserialize)]
struct PersistedAccount {
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
    /// Absolute unix timestamp (secs) when hard limit expires
    hard_limited_until_epoch: Option<u64>,
    /// Wall-clock epoch when this account's rate info was last updated.
    /// Used by sync_from_redis "most recent wins" merge after restart.
    #[serde(default)]
    last_updated_epoch: Option<u64>,
}

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
        let mut accounts = Vec::new();
        let now = Instant::now();

        for acct in &self.accounts {
            let info = acct.rate_info.read().await;
            let hard_until_epoch = info.hard_limited_until.and_then(|until| {
                if until > now {
                    let remaining = until.duration_since(now);
                    Some(Self::now_epoch() + remaining.as_secs())
                } else {
                    None
                }
            });
            accounts.push(PersistedAccount {
                name: acct.name.clone(),
                requests_total: acct.requests.load(Ordering::Relaxed),
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
                hard_limited_until_epoch: hard_until_epoch,
                last_updated_epoch: info.last_updated_epoch,
            });
        }

        let state = PersistedState {
            accounts,
            saved_at: Self::now_epoch(),
        };

        match serde_json::to_string_pretty(&state) {
            Ok(json) => {
                if let Err(e) = tokio::fs::write(&self.state_path, json).await {
                    error!(path = %self.state_path.display(), error = %e, "failed to save state");
                } else {
                    trace!(path = %self.state_path.display(), "state saved");
                }
            }
            Err(e) => error!(error = %e, "failed to serialize state"),
        }
    }

    /// Fire a minimal request (max_tokens=1) to refresh rate limit headers for an account.
    /// The `model` parameter controls which model is probed, rotating across families
    /// so that per-model 7d utilization claims get populated for each family.
    async fn probe_account(&self, idx: usize, model: &str) {
        let acct = &self.accounts[idx];
        if acct.passthrough {
            debug!(
                account = acct.name,
                "skipping probe for passthrough account"
            );
            return;
        }

        // Check if hard-limited — don't waste a request
        {
            let info = acct.rate_info.read().await;
            if let Some(until) = info.hard_limited_until {
                if Instant::now() < until {
                    debug!(
                        account = acct.name,
                        "skipping probe, account is hard-limited"
                    );
                    return;
                }
            }
        }

        // Local freshness check: skip if this model's 7d claim was recently refreshed.
        // Looks up only the model-specific claim key (e.g. "seven_day_opus"), NOT the
        // general "seven_day" fallback, so probing one family doesn't block another.
        let now_epoch = Self::now_epoch();
        {
            let info = acct.rate_info.read().await;
            let family = model_family(model);
            if !family.is_empty() {
                let claim_key = format!("seven_day_{}", family);
                if let Some(claim) = info.claims_7d.get(&claim_key) {
                    let age = now_epoch.saturating_sub(claim.last_seen);
                    if age < self.probe_interval_secs / 2 {
                        trace!(
                            account = acct.name,
                            probe_model = model,
                            claim_key,
                            age_secs = age,
                            "probe skipped, model claim is fresh"
                        );
                        return;
                    }
                }
            }
        }

        // Distributed probe lock: one pod per account+model per interval.
        // TTL = full configured interval so the lock covers the entire
        // dedup window (no early-rollover race).
        if let Some(redis) = &self.redis {
            let mut conn = redis.clone();
            let lock_key = format!("alb:probe:{}:{}", acct.name, model);
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
                        account = acct.name,
                        probe_model = model,
                        "probe skipped, another replica is probing"
                    );
                    return;
                }
                Err(e) => {
                    // Fail-open: if Redis is down, probe anyway
                    trace!(account = acct.name, error = %e, "probe lock failed, probing anyway");
                }
            }
        }

        let url = format!("{}/v1/messages", self.upstream);
        let body = serde_json::json!({
            "model": model,
            "max_tokens": 1,
            "system": [{"type": "text", "text": "You are Claude Code, Anthropic's official CLI for Claude."}],
            "messages": [{"role": "user", "content": "."}]
        });

        let mut req = self
            .client
            .post(&url)
            .header("content-type", "application/json")
            .header("anthropic-version", "2023-06-01")
            .header("anthropic-beta", OAUTH_BETA_FLAGS.join(","))
            .header("user-agent", "claude-cli/2.1.2 (external, cli)")
            .header("x-app", "cli")
            .header("anthropic-dangerous-direct-browser-access", "true")
            .json(&body);

        // Inject auth
        if acct.token.starts_with("sk-ant-api") {
            req = req.header("x-api-key", &acct.token);
        } else if acct.token.starts_with("sk-ant-oat") {
            req = req.header("authorization", format!("Bearer {}", acct.token));
        } else {
            req = req.header("x-api-key", &acct.token);
        }

        match req.send().await {
            Ok(resp) => {
                let status = resp.status();
                self.update_rate_info(idx, resp.headers()).await;
                if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
                    self.mark_hard_limited(idx, resp.headers()).await;
                } else if status.is_success() {
                    // 2xx only: account is responsive, clear hard limit and burst counter
                    // so pick_account stops treating rate data as stale.
                    // 5xx/529 are upstream errors, not proof the account recovered —
                    // clearing the hard limit on those would flood a saturated account
                    // during an Anthropic incident.
                    let recovered = {
                        let mut info = acct.rate_info.write().await;
                        let was_hard_limited = info.hard_limited_until.is_some();
                        if was_hard_limited {
                            info.hard_limited_until = None;
                            debug!(
                                account = acct.name,
                                "cleared hard limit after successful probe"
                            );
                        }
                        info.consecutive_burst_429s = 0;
                        was_hard_limited
                    };
                    // Recovery is a sparse, high-impact event — refresh metrics
                    // immediately so the dashboard reflects the account coming
                    // back online without waiting for the next probe cycle.
                    if recovered {
                        self.signal_hard_limit_recovery(acct).await;
                    }
                }
                // else: 5xx/529 — leave account state untouched. Next probe cycle retries.
                self.save_state().await;
                let info = acct.rate_info.read().await;
                let now_epoch = Self::now_epoch();
                let (eff_util, constraint, _adj_5h, _adj_7d) =
                    effective_utilization(&info, now_epoch, model);
                // Only compute routing weight on 2xx — non-success responses leave
                // the account state either mutated (429 → hard-limited) or untouched
                // (5xx), and the pre-response weight no longer reflects reality.
                let rw = if status.is_success() {
                    compute_routing_weight(&info, model, now_epoch, false)
                } else {
                    None
                };
                info!(
                    account = acct.name,
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
                warn!(account = acct.name, error = %e, "probe failed");
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
                warn!(error = %e, "failed to parse persisted state, starting fresh");
                return;
            }
        };

        let now_epoch = Self::now_epoch();
        let now_instant = Instant::now();

        for pa in &persisted.accounts {
            if let Some(acct) = self.accounts.iter().find(|a| a.name == pa.name) {
                acct.requests.store(pa.requests_total, Ordering::Relaxed);
                let mut info = acct.rate_info.write().await;
                info.utilization = pa.utilization;
                info.utilization_7d = pa.utilization_7d;
                info.utilization_5h = pa.utilization_5h;
                info.representative_claim = pa.representative_claim.clone();
                info.reset_5h = pa.reset_5h;
                info.status_5h = pa.status_5h.clone();

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

                // Derive flat 7d fields from claims_7d
                if info.claims_7d.is_empty() {
                    info.utilization_7d = None;
                    info.reset_7d = None;
                    info.status_7d = None;
                } else {
                    info.utilization_7d = info
                        .claims_7d
                        .values()
                        .filter_map(|c| c.utilization)
                        .reduce(f64::max);
                    info.reset_7d = info.claims_7d.values().filter_map(|c| c.reset).min();
                    info.status_7d = info
                        .claims_7d
                        .values()
                        .filter_map(|c| c.status.as_ref())
                        .max_by(|a, b| {
                            status_to_floor(Some(a))
                                .partial_cmp(&status_to_floor(Some(b)))
                                .unwrap_or(std::cmp::Ordering::Equal)
                        })
                        .cloned();
                }

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

/// Maximum request body size (25 MB).
const MAX_REQUEST_BODY_BYTES: usize = 25 * 1024 * 1024;

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
    } else {
        ""
    }
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
            resolve_7d_claim(info, model).and_then(|c| {
                time_adjusted_utilization(
                    c.utilization,
                    c.reset,
                    c.status.as_deref(),
                    NEAR_RESET_7D_SECS,
                    now_epoch,
                )
            })
        } else {
            // No model specified (emergency brake, stats) — worst-case across all claims
            info.claims_7d
                .values()
                .filter_map(|c| {
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

/// Computed routing weight for a single account+model. Extracted so both
/// `routing_candidates()` (real requests) and `probe_account()` (periodic probes)
/// use identical logic. Returns `None` when the account's 7d claim is actively
/// rejected (caller should skip it).
struct RoutingWeight {
    gate_5h: f64,
    gate_7d: f64,
    gate: f64,
    wr: f64,
    weight: f64,
    source: &'static str,
}

fn compute_routing_weight(
    info: &RateLimitInfo,
    model: &str,
    now_epoch: u64,
    stale_after_hard_limit: bool,
) -> Option<RoutingWeight> {
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

    // 7d model-specific gate and waste risk
    let (gate_7d, wr, source) = if let Some(claim) = resolve_7d_claim(info, model) {
        let rejected_claim_active = claim.status.as_deref() == Some("rejected")
            && claim.reset.is_none_or(|reset| reset > now_epoch);
        if rejected_claim_active && !stale_after_hard_limit {
            return None; // caller should skip this account
        }
        (
            if stale_after_hard_limit {
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
            },
            waste_risk(claim.utilization, claim.reset, now_epoch),
            "waste_risk",
        )
    } else {
        (
            if stale_after_hard_limit { 0.5 } else { 0.0 },
            0.0,
            "headroom_only",
        )
    };

    let gate = gate_5h.max(gate_7d);
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
    async fn routing_candidates(&self, model: &str, skip: &[usize]) -> Vec<RoutingCandidate> {
        let now = Instant::now();
        let now_epoch = Self::now_epoch();
        let mut candidates: Vec<RoutingCandidate> = Vec::new();
        for (i, acct) in self.accounts.iter().enumerate() {
            if skip.contains(&i) {
                trace!(account = acct.name, "pick: skipping, already tried");
                continue;
            }
            if !acct.serves_model(model) {
                trace!(
                    account = acct.name,
                    model = model,
                    "pick: skipping, model not in allowlist"
                );
                continue;
            }

            let info = acct.rate_info.read().await;

            if let Some(until) = info.hard_limited_until {
                if now < until {
                    trace!(
                        account = acct.name,
                        hard_limited_secs = until.duration_since(now).as_secs(),
                        "pick: skipping hard-limited account"
                    );
                    continue;
                }
            }

            // Detect stale data: hard limit expired but no fresh response since.
            // mark_hard_limited poisons remaining_tokens/requests and update_rate_info
            // on the 429 stores high utilization + "rejected" statuses. Without this
            // check, those stale values prevent the account from ever being selected
            // again — only probes can refresh the data, and they run infrequently.
            let stale_after_hard_limit = info
                .hard_limited_until
                .is_some_and(|until| info.last_updated.is_none_or(|lu| lu <= until));

            let rw = match compute_routing_weight(&info, model, now_epoch, stale_after_hard_limit) {
                Some(rw) => rw,
                None => {
                    trace!(
                        account = acct.name,
                        model = model,
                        "pick: skipping, 7d claim rejected"
                    );
                    continue;
                }
            };

            trace!(
                account = acct.name,
                gate_5h = format!("{:.4}", rw.gate_5h),
                gate_7d = format!("{:.4}", rw.gate_7d),
                gate = format!("{:.4}", rw.gate),
                waste_risk = format!("{:.4}", rw.wr),
                weight = format!("{:.4}", rw.weight),
                source = rw.source,
                "pick: candidate"
            );

            candidates.push(RoutingCandidate {
                idx: i,
                gate_5h: rw.gate_5h,
                gate_7d: rw.gate_7d,
                gate: rw.gate,
                wr: rw.wr,
                weight: rw.weight,
                source: rw.source,
            });
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

        // Collect (gate, weight) per account. None = excluded entirely
        // (passthrough or hard-limited — never weighted in any condition).
        let mut entries: Vec<Option<(f64, f64)>> = vec![None; self.accounts.len()];

        for (i, acct) in self.accounts.iter().enumerate() {
            if acct.passthrough {
                continue;
            }

            let info = acct.rate_info.read().await;

            // Hard-limited accounts contribute zero (mirrors routing_candidates filter).
            if let Some(until) = info.hard_limited_until {
                if now < until {
                    continue;
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

            entries[i] = Some((gate, weight));
        }

        // Mirror pick_account's graceful-degradation: only filter soft-limited
        // accounts when at least one healthy account exists in the pool.
        // Otherwise the entire pool is degraded and we still want the dashboard
        // to reflect the routable accounts (pick_account would route to them).
        let has_healthy = entries
            .iter()
            .any(|e| matches!(e, Some((gate, _)) if *gate < self.soft_limit));

        let mut weights = vec![0f64; self.accounts.len()];
        for (i, entry) in entries.iter().enumerate() {
            if let Some((gate, weight)) = entry {
                if has_healthy && *gate >= self.soft_limit {
                    continue; // soft-limited and there's a healthy alternative
                }
                weights[i] = *weight;
            }
        }

        let total: f64 = weights.iter().sum();
        for (i, acct) in self.accounts.iter().enumerate() {
            let w = weights[i];
            let share = if total > 0.0 { w / total } else { 0.0 };
            // Excluded accounts (passthrough, hard-limited) report gate=1.0
            // (fully gated) since they receive zero traffic.
            let gate = entries[i].map(|(g, _)| g).unwrap_or(1.0);
            // Weight, share and gate are independent gauges, not a joint invariant —
            // a torn read across them is harmless for dashboard consumers.
            acct.last_routing_weight
                .store(w.to_bits(), Ordering::Relaxed);
            acct.last_routing_share
                .store(share.to_bits(), Ordering::Relaxed);
            acct.last_effective_gate
                .store(gate.to_bits(), Ordering::Relaxed);
        }
    }

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
        for acct in &self.accounts {
            let w = f64::from_bits(acct.last_routing_weight.load(Ordering::Relaxed));
            let s = f64::from_bits(acct.last_routing_share.load(Ordering::Relaxed));
            let g = f64::from_bits(acct.last_effective_gate.load(Ordering::Relaxed));
            let key = format!("alb:weight:{}", acct.name);
            let val = format!("{w},{s},{g}");
            let mut conn = redis.clone();
            let ttl = Self::routing_weight_publish_ttl(self.probe_interval_secs);
            tokio::spawn(async move {
                let result: redis::RedisResult<()> = conn.set_ex(&key, val, ttl).await;
                if let Err(e) = result {
                    tracing::warn!(error = %e, "redis routing weight publish failed");
                }
            });
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
    async fn signal_hard_limit_recovery(&self, acct: &Account) {
        if let Some(redis) = &self.redis {
            let mut conn = redis.clone();
            let key = format!("alb:hard:{}", acct.name);
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
            let other = if picked.idx == effective[0].idx {
                effective[1]
            } else {
                effective[0]
            };
            if picked.weight < other.weight * LEGACY_AFFINITY_OVERRIDE_RATIO {
                debug!(
                    strategy = RoutingStrategy::DynamicCapacityV1.as_str(),
                    picked_account = self.accounts[picked.idx].name,
                    picked_weight = format!("{:.3}", picked.weight),
                    other_account = self.accounts[other.idx].name,
                    other_weight = format!("{:.3}", other.weight),
                    ratio = format!("{:.3}", picked.weight / other.weight),
                    "pick: affinity override, weight ratio below threshold"
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

        if affinity_key.is_some() {
            let best = effective
                .iter()
                .max_by(|a, b| a.weight.partial_cmp(&b.weight).unwrap())
                .copied()
                .unwrap();
            if best.idx != picked.idx
                && picked.weight < best.weight * STICKY_WEIGHTED_OVERRIDE_RATIO
            {
                debug!(
                    strategy = RoutingStrategy::StickyWeightedV2.as_str(),
                    picked_account = self.accounts[picked.idx].name,
                    picked_weight = format!("{:.3}", picked.weight),
                    best_account = self.accounts[best.idx].name,
                    best_weight = format!("{:.3}", best.weight),
                    ratio = format!("{:.3}", picked.weight / best.weight),
                    "pick: affinity override, breaking session stickiness"
                );
                picked = best;
            }
        }

        picked
    }

    /// Pick the best available account using the configured routing strategy.
    async fn pick_account(
        &self,
        affinity_key: Option<&str>,
        model: &str,
        skip: &[usize],
    ) -> Option<usize> {
        let candidates = self.routing_candidates(model, skip).await;
        if candidates.is_empty() {
            debug!("pick: no available accounts");
            return None;
        }

        // Soft-limit gate: exclude accounts whose effective gate exceeds the ceiling.
        let has_healthy = candidates.iter().any(|c| c.gate < self.soft_limit);
        let effective: Vec<&RoutingCandidate> = if has_healthy {
            let excluded: Vec<&str> = candidates
                .iter()
                .filter(|c| c.gate >= self.soft_limit)
                .map(|c| self.accounts[c.idx].name.as_str())
                .collect();
            if !excluded.is_empty() {
                debug!(soft_limit = self.soft_limit, excluded = ?excluded, "pick: soft-limited accounts excluded");
            }
            candidates
                .iter()
                .filter(|c| c.gate < self.soft_limit)
                .collect()
        } else {
            candidates.iter().collect()
        };

        // Total weight. If zero (all exhausted), return None.
        let total_weight: f64 = effective.iter().map(|c| c.weight).sum();
        if total_weight <= 0.0 {
            debug!("pick: all candidates exhausted (zero weight)");
            return None;
        }

        let picked = match self.routing_strategy {
            RoutingStrategy::DynamicCapacityV1 => {
                self.pick_dynamic_capacity_v1(&effective, total_weight, affinity_key)
            }
            RoutingStrategy::StickyWeightedV2 => {
                self.pick_sticky_weighted_v2(&effective, total_weight, affinity_key)
            }
        };

        debug!(
            strategy = self.routing_strategy.as_str(),
            account = self.accounts[picked.idx].name,
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
        Some(picked.idx)
    }

    /// Update rate limit info from response headers.
    async fn update_rate_info(&self, idx: usize, headers: &reqwest::header::HeaderMap) {
        let acct = &self.accounts[idx];
        let mut info = acct.rate_info.write().await;

        // Debug: log all ratelimit headers
        for (name, value) in headers.iter() {
            let name_str = name.as_str();
            if name_str.contains("ratelimit") || name_str.contains("retry") {
                if let Ok(v) = value.to_str() {
                    tracing::trace!(
                        account = acct.name,
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

        // Derive flat convenience fields from claims_7d (backward compat for logs/stats).
        // utilization_7d = max utilization, reset_7d = min reset, status_7d = worst status.
        // When claims_7d is empty (all evicted or never populated), clear the flat fields
        // so effective_utilization() doesn't fall back to stale derived values.
        if !info.claims_7d.is_empty() {
            info.utilization_7d = info
                .claims_7d
                .values()
                .filter_map(|c| c.utilization)
                .reduce(f64::max);
            info.reset_7d = info.claims_7d.values().filter_map(|c| c.reset).min();
            info.status_7d = info
                .claims_7d
                .values()
                .filter_map(|c| c.status.as_deref())
                .max_by(|a, b| {
                    status_to_floor(Some(a))
                        .partial_cmp(&status_to_floor(Some(b)))
                        .unwrap_or(std::cmp::Ordering::Equal)
                })
                .map(|s| s.to_string());
        } else {
            info.utilization_7d = None;
            info.reset_7d = None;
            info.status_7d = None;
        }

        // Derive unified utilization = max across all windows (5h + all 7d claims).
        // Recompute unconditionally so stale unified values don't survive eviction.
        // Include 5h if reset is absent (no staleness info) or in the future;
        // exclude only when reset is present AND expired (stale data).
        let mut max_util: Option<f64> = info
            .utilization_5h
            .filter(|_| info.reset_5h.is_none_or(|r| r > now_epoch));
        for cd in info.claims_7d.values() {
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
            account = acct.name,
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
            let key = format!("alb:rate:{}", acct.name);
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

    /// Mark an account as hard rate-limited (got a 429).
    async fn mark_hard_limited(&self, idx: usize, headers: &reqwest::header::HeaderMap) {
        let acct = &self.accounts[idx];
        let mut info = acct.rate_info.write().await;

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
            account = acct.name,
            cooldown_secs = cooldown.as_secs(),
            retry_after_raw = ?raw_retry_after,
            burst = is_burst_limit,
            consecutive_burst = info.consecutive_burst_429s,
            "account hard rate-limited (429), cooling down"
        );

        // Propagate to Redis for cross-replica awareness
        if let Some(redis) = &self.redis {
            let mut conn = redis.clone();
            let key = format!("alb:hard:{}", acct.name);
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

        // 1. Sync hard limits (MGET for all accounts in one round-trip)
        let hard_keys: Vec<String> = self
            .accounts
            .iter()
            .map(|a| format!("alb:hard:{}", a.name))
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
                        let mut info = self.accounts[i].rate_info.write().await;
                        if info.hard_limited_until.is_some() {
                            info.hard_limited_until = None;
                            trace!(
                                account = self.accounts[i].name,
                                "synced hard-limit clear sentinel from redis"
                            );
                        }
                    }
                    HardLimitSync::Update(until_instant) => {
                        let mut info = self.accounts[i].rate_info.write().await;
                        let should_update = info
                            .hard_limited_until
                            .is_none_or(|local| until_instant > local);
                        if should_update {
                            info.hard_limited_until = Some(until_instant);
                            trace!(
                                account = self.accounts[i].name,
                                "synced hard-limit from redis"
                            );
                        }
                    }
                    HardLimitSync::Ignore => {}
                }
            }
        }

        // 2. Sync rate info (MGET for all accounts in one round-trip)
        let rate_keys: Vec<String> = self
            .accounts
            .iter()
            .map(|a| format!("alb:rate:{}", a.name))
            .collect();

        if let Ok(values) = conn.mget::<_, Vec<Option<String>>>(&rate_keys).await {
            for (i, val) in values.iter().enumerate() {
                if let Some(json) = val {
                    if let Ok(remote) = serde_json::from_str::<RedisRateInfo>(json) {
                        let mut info = self.accounts[i].rate_info.write().await;
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
                            info.last_updated = Some(now_instant);
                            info.last_updated_epoch = Some(remote.updated_at);
                            trace!(
                                account = self.accounts[i].name,
                                remote_age,
                                "synced rate info from redis"
                            );
                        }
                    }
                }
            }
        }

        // 3. Sync precomputed routing weights (published by probing pod)
        let weight_keys: Vec<String> = self
            .accounts
            .iter()
            .map(|a| format!("alb:weight:{}", a.name))
            .collect();
        if let Ok(values) = conn.mget::<_, Vec<Option<String>>>(&weight_keys).await {
            for (i, val) in values.iter().enumerate() {
                if let Some(csv) = val {
                    let mut parts = csv.splitn(3, ',');
                    if let (Some(w_str), Some(s_str)) = (parts.next(), parts.next()) {
                        if let (Ok(w), Ok(s)) = (w_str.parse::<f64>(), s_str.parse::<f64>()) {
                            self.accounts[i]
                                .last_routing_weight
                                .store(w.to_bits(), Ordering::Relaxed);
                            self.accounts[i]
                                .last_routing_share
                                .store(s.to_bits(), Ordering::Relaxed);
                            // Gate is optional (backward compat with older publishers)
                            if let Some(Ok(g)) = parts.next().map(|g| g.parse::<f64>()) {
                                self.accounts[i]
                                    .last_effective_gate
                                    .store(g.to_bits(), Ordering::Relaxed);
                            }
                        }
                    }
                }
            }
        }

        // 4. Refresh cluster info cache for /_stats endpoint
        let info = self.cluster_info().await;
        if let Ok(mut cache) = self.cluster_info_cache.lock() {
            *cache = info;
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

        Some(serde_json::json!({
            "redis_connected": redis_ok,
            "replicas_seen": replicas,
            "budget_usage": redis_budgets,
        }))
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

    fn affinity_key(&self, client_ip: &IpAddr) -> Option<String> {
        let has_identity = self.client_id != "-" || self.agent_id != "-" || self.session_id != "-";
        if has_identity {
            Some(format!(
                "{}:{}:{}:{}",
                client_ip, self.client_id, self.agent_id, self.session_id
            ))
        } else {
            None
        }
    }
}

fn log_usage(req_id: &str, client_id: &str, model: &str, account: &str, usage: &TokenUsage) {
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
    idx: usize,
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
        state.record_usage(idx, client_id, &usage).await;
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
    idx: usize,
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
        state.record_usage(idx, client_id, usage).await;
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
    /// Record token usage for an account and client.
    async fn record_usage(&self, account_idx: usize, client_id: &str, usage: &TokenUsage) {
        if usage.is_empty() {
            return;
        }
        let acct = &self.accounts[account_idx];
        acct.input_tokens
            .fetch_add(usage.input_tokens, Ordering::Relaxed);
        acct.output_tokens
            .fetch_add(usage.output_tokens, Ordering::Relaxed);
        acct.cache_creation_tokens
            .fetch_add(usage.cache_creation_input_tokens, Ordering::Relaxed);
        acct.cache_read_tokens
            .fetch_add(usage.cache_read_input_tokens, Ordering::Relaxed);

        // Per-client tracking
        if client_id != "-" {
            let total = usage.input_tokens
                + usage.output_tokens
                + usage.cache_creation_input_tokens
                + usage.cache_read_input_tokens;
            if let Ok(mut map) = self.client_usage.lock() {
                let entry = map.entry(client_id.to_owned()).or_insert([0; 4]);
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
    fn update_burn_rate(&self, acct: &Account, client_id: &str) {
        let now = Instant::now();
        if let Ok(mut br) = acct.burn_rate.lock() {
            br.update(now);
        }
        if let Ok(mut rates) = self.client_request_rates.lock() {
            let entry = rates
                .entry(client_id.to_owned())
                .or_insert_with(|| (0, Ewma::new(TAU_1H)));
            entry.0 += 1;
            entry.1.update(now);
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

    /// Check if all model-compatible accounts exceed this client's utilization limit.
    /// Returns Ok(()) if no limit configured or at least one account is below the limit.
    /// Returns Err(retry_after_secs) if all accounts exceed the limit.
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

        for acct in &self.accounts {
            if !acct.serves_model(model) {
                continue;
            }
            any_compatible = true;
            let info = acct.rate_info.read().await;
            let (util, source, _, _) = effective_utilization(&info, now_epoch, model);
            if source == "unknown" {
                all_above = false; // fail-open: unknown account may have capacity
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
                _ => info.reset_5h.or(info.reset_7d), // unified/legacy best-effort
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

    /// Check if all accounts are above the emergency threshold.
    /// Fail-open: returns false if all accounts return (0.5, "unknown") — no data.
    async fn is_emergency_brake_active(&self) -> bool {
        if !self.emergency_brake {
            return false;
        }
        let now_epoch = Self::now_epoch();
        let mut all_above = true;
        let mut any_known = false;

        for acct in &self.accounts {
            let info = acct.rate_info.read().await;
            let (util, source, _, _) = effective_utilization(&info, now_epoch, "");
            if source != "unknown" {
                any_known = true;
            }
            if util < self.emergency_threshold {
                all_above = false;
                break;
            }
        }

        // Fail-open: if no account has real data, don't activate
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
    let affinity_key = rctx.affinity_key(&client_ip);
    let affinity = affinity_key.as_deref();
    let RequestContext {
        client_id,
        client_ver,
        agent_id,
        session_id,
    } = rctx;

    // Debug: dump all inbound request headers
    if tracing::enabled!(tracing::Level::DEBUG) {
        debug!(req_id, client_id = %client_id, ver = %client_ver, ">>> inbound request");
        for (k, v) in parts.headers.iter() {
            debug!(req_id, header = %k, value = debug_header_value(k, v), ">>> req header");
        }
    }

    let body_bytes = match axum::body::to_bytes(body, MAX_REQUEST_BODY_BYTES).await {
        Ok(b) => b,
        Err(e) => {
            error!("failed to read request body: {e}");
            return (StatusCode::BAD_REQUEST, "bad request body").into_response();
        }
    };

    // Parse body once for model extraction and optional cache injection
    let (body_bytes, oauth_body_bytes, model) =
        if let Ok(mut parsed) = serde_json::from_slice::<serde_json::Value>(&body_bytes) {
            let model = parsed
                .get("model")
                .and_then(|m| m.as_str())
                .unwrap_or("")
                .to_string();
            let mut mutated = false;

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
            )
        } else {
            let clone = body_bytes.clone();
            (body_bytes, clone, String::new())
        };

    // Pre-request gate: operator bypass, budget, utilization limit, emergency brake.
    // Note: budget + emergency don't need `model` and could run before body parsing,
    // but those rejections are rare and the JSON parse cost is negligible — not worth
    // splitting the gate for a few microseconds on an almost-never code path.
    if let Err(resp) = state.pre_request_gate(&client_id, &model).await {
        return resp;
    }

    let n = state.accounts.len();
    for retry_round in 0..=MAX_529_RETRIES {
        if retry_round > 0 {
            let delay = RETRY_529_BASE_DELAY * 2u32.pow(retry_round - 1);
            warn!(
                retry_round = retry_round,
                delay_ms = delay.as_millis() as u64,
                "529 backoff: retrying all accounts"
            );
            tokio::time::sleep(delay).await;
        }
        let mut skip: Vec<usize> = Vec::new();
        let mut saw_529 = false;
        for _attempt in 0..n {
            let idx = match state.pick_account(affinity, &model, &skip).await {
                Some(i) => i,
                None => {
                    warn!("all accounts rate-limited");
                    return (
                        StatusCode::TOO_MANY_REQUESTS,
                        "all upstream accounts rate-limited",
                    )
                        .into_response();
                }
            };

            let acct = &state.accounts[idx];
            let url = format!(
                "{}{}",
                state.upstream,
                parts
                    .uri
                    .path_and_query()
                    .map(|pq| pq.as_str())
                    .unwrap_or("/")
            );

            let mut upstream_req = state.client.request(parts.method.clone(), &url);

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
            inject_account_auth(&mut headers, &acct.token, acct.passthrough);

            // Debug: log outbound auth method and key headers
            if tracing::enabled!(tracing::Level::DEBUG) {
                let auth_method = if acct.passthrough {
                    "passthrough"
                } else if acct.token.starts_with("sk-ant-oat") {
                    "oauth"
                } else {
                    "api-key"
                };
                debug!(
                    req_id,
                    account = acct.name,
                    auth_method,
                    body_bytes = if acct.token.starts_with("sk-ant-oat") {
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
            let req_body = if acct.token.starts_with("sk-ant-oat") {
                &oauth_body_bytes
            } else {
                &body_bytes
            };
            upstream_req = upstream_req.body(req_body.clone());

            let mut resp = match upstream_req.send().await {
                Ok(r) => r,
                Err(e) => {
                    error!(account = acct.name, "upstream request failed: {e}");
                    continue;
                }
            };

            let status = resp.status();
            acct.requests.fetch_add(1, Ordering::Relaxed);

            // Debug: dump all response headers
            if tracing::enabled!(tracing::Level::DEBUG) {
                debug!(
                    req_id,
                    status = status.as_u16(),
                    account = acct.name,
                    "<<< upstream response"
                );
                for (k, v) in resp.headers().iter() {
                    debug!(req_id, header = %k, value = debug_header_value(k, v), "<<< resp header");
                }
            }

            // Always update rate limit info and persist
            state.update_rate_info(idx, resp.headers()).await;

            // Update burn rate (after rate-limit headers are parsed)
            state.update_burn_rate(acct, &client_id);

            // 429 → mark hard-limited and try next account
            if status == StatusCode::TOO_MANY_REQUESTS {
                state.mark_hard_limited(idx, resp.headers()).await;
                log_429_details(&acct.name, resp).await;
                state.save_state().await;
                info!(account = acct.name, "got 429, rotating to next account");
                skip.push(idx);
                continue;
            }

            // 529 → overloaded, try next account; flag for BEBO retry if all exhausted
            if status.as_u16() == 529 {
                state.save_state().await;
                warn!(account = acct.name, "got 529, rotating to next account");
                saw_529 = true;
                skip.push(idx);
                continue;
            }

            // Other 5xx → transient, try next account (no BEBO retry)
            if status.is_server_error() {
                state.save_state().await;
                warn!(
                    account = acct.name,
                    status = status.as_u16(),
                    "got server error, rotating to next account"
                );
                skip.push(idx);
                continue;
            }

            // Clear hard limit and burst counter only on a genuine 2xx success.
            // A 4xx (e.g. invalid_request_error, auth failure) is not evidence
            // that the rate-limit window has drained — don't clobber state on
            // client errors.
            let recovered = if status.is_success() {
                let mut info = acct.rate_info.write().await;
                let was = info.hard_limited_until.is_some();
                info.hard_limited_until = None;
                info.consecutive_burst_429s = 0;
                was
            } else {
                false
            };

            // Persist state after updating rate-limit state so completed 4xx
            // responses and other terminal outcomes aren't dropped on restart.
            state.save_state().await;

            if recovered {
                state.signal_hard_limit_recovery(acct).await;
            }

            // Log with capacity info + inject budget status header
            let budget_status = {
                let info = acct.rate_info.read().await;
                let (eff_util, constraint, _adj_5h, _adj_7d) =
                    effective_utilization(&info, AppState::now_epoch(), &model);
                info!(
                    req_id,
                    client = %client_ip,
                    client_id = %client_id,
                    ver = %client_ver,
                    agent = %agent_id,
                    session = %session_id,
                    model = %model,
                    account = acct.name,
                    status = status.as_u16(),
                    utilization = format_args!("{eff_util:.2}"),
                    util_5h = info.utilization_5h.map(|v| format!("{v:.2}")).as_deref().unwrap_or("-"),
                    util_7d = info.utilization_7d.map(|v| format!("{v:.2}")).as_deref().unwrap_or("-"),
                    constraint,
                    total = acct.requests.load(Ordering::Relaxed),
                    "proxied"
                );
                compute_pressure_status(eff_util, &client_id, &state)
            };

            let latency_ms = request_start.elapsed().as_millis() as u64;

            // Stream response through, extracting token usage
            let resp_status =
                StatusCode::from_u16(status.as_u16()).unwrap_or(StatusCode::BAD_GATEWAY);
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
                let (tx, rx) =
                    tokio::sync::mpsc::channel::<Result<bytes::Bytes, std::io::Error>>(32);
                let state_clone = state.clone();
                let client_id_clone = client_id.clone();
                let acct_name = acct.name.clone();
                let model_clone = model.clone();
                let client_ip_str = client_ip.to_string();
                let agent_clone = agent_id.clone();
                let session_clone = session_id.clone();

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
                                warn!(req_id, error = %e, "upstream SSE read failed");
                                break;
                            }
                        }
                    }
                    // Parse accumulated SSE data for usage
                    finalize_stream(
                        &state_clone,
                        idx,
                        &req_id,
                        &client_id_clone,
                        &model_clone,
                        &acct_name,
                        &client_ip_str,
                        &agent_clone,
                        &session_clone,
                        status.as_u16(),
                        &sse_buf,
                        request_start,
                        client_disconnected,
                        upstream_error,
                        false,
                    )
                    .await;
                });

                let body_stream = ReceiverStream::new(rx);
                return builder
                    .body(Body::from_stream(body_stream))
                    .unwrap_or_else(|_| {
                        (StatusCode::INTERNAL_SERVER_ERROR, "response build error").into_response()
                    });
            } else {
                // Non-streaming: buffer, extract usage, forward
                let body_bytes = resp.bytes().await.unwrap_or_default();
                let mut usage = TokenUsage::default();
                if let Ok(parsed) = serde_json::from_slice::<serde_json::Value>(&body_bytes) {
                    usage = TokenUsage::from_response_body(&parsed);
                }
                finalize_non_stream(
                    &state,
                    idx,
                    &req_id,
                    &client_id,
                    &model,
                    &acct.name,
                    &client_ip.to_string(),
                    &agent_id,
                    &session_id,
                    status.as_u16(),
                    &usage,
                    latency_ms,
                    false,
                )
                .await;
                return builder.body(Body::from(body_bytes)).unwrap_or_else(|_| {
                    (StatusCode::INTERNAL_SERVER_ERROR, "response build error").into_response()
                });
            }
        }
        // If no 529s in this round, don't retry (e.g. all were 429s or errors)
        if !saw_529 {
            break;
        }
    }

    (StatusCode::TOO_MANY_REQUESTS, "exhausted all accounts").into_response()
}

// ── Upstream passthrough handler ─────────────────────────────────────

async fn upstream_handler(
    State(state): State<Arc<AppState>>,
    axum::extract::ConnectInfo(client_addr): axum::extract::ConnectInfo<SocketAddr>,
    axum::extract::Path((upstream_name, _rest)): axum::extract::Path<(String, String)>,
    req: Request<Body>,
) -> Response {
    let client_ip = client_addr.ip();

    if !state.is_ip_allowed(&client_ip) {
        warn!(client = %client_ip, "rejected: IP not in allowlist");
        return (StatusCode::FORBIDDEN, "forbidden").into_response();
    }

    if let Some(ref key) = state.proxy_key {
        let provided = req.headers().get("x-api-key").and_then(|v| v.to_str().ok());
        if provided != Some(key.as_str()) {
            warn!(client = %client_ip, "rejected: invalid or missing proxy key");
            return (StatusCode::UNAUTHORIZED, "unauthorized").into_response();
        }
    }

    let upstream = match state.upstreams.iter().find(|u| u.name == upstream_name) {
        Some(u) => u,
        None => {
            warn!(client = %client_ip, upstream = %upstream_name, "unknown upstream");
            return (StatusCode::NOT_FOUND, "unknown upstream").into_response();
        }
    };

    let (parts, body) = req.into_parts();

    // Extract client identification headers
    let client_id = state.resolve_client_id(&client_ip, &parts.headers);

    let body_bytes = match axum::body::to_bytes(body, MAX_REQUEST_BODY_BYTES).await {
        Ok(b) => b,
        Err(e) => {
            error!("failed to read request body: {e}");
            return (StatusCode::BAD_REQUEST, "bad request body").into_response();
        }
    };

    // Extract model from request body for logging
    let model = serde_json::from_slice::<serde_json::Value>(&body_bytes)
        .ok()
        .and_then(|v| v.get("model").and_then(|m| m.as_str().map(String::from)))
        .unwrap_or_default();

    // Build upstream URL: strip /upstream/<name> prefix, forward the rest
    let path = parts.uri.path();
    let prefix = format!("/upstream/{}", upstream_name);
    let remainder = path.strip_prefix(&prefix).unwrap_or("/");
    let remainder = if remainder.is_empty() { "/" } else { remainder };
    let query = parts
        .uri
        .query()
        .map(|q| format!("?{}", q))
        .unwrap_or_default();
    let url = format!("{}{}{}", upstream.base_url, remainder, query);

    let mut headers = parts.headers.clone();
    headers.remove("host");
    headers.remove("authorization");
    headers.remove("x-api-key");
    // Inject upstream API key as Bearer token (OpenAI-compatible)
    headers.insert(
        "authorization",
        HeaderValue::from_str(&format!("Bearer {}", upstream.api_key)).unwrap(),
    );

    let upstream_req = state
        .client
        .request(parts.method.clone(), &url)
        .headers(headers)
        .body(body_bytes);

    let resp = match upstream_req.send().await {
        Ok(r) => r,
        Err(e) => {
            error!(upstream = upstream.name, error = %e, "upstream request failed");
            return (StatusCode::BAD_GATEWAY, "upstream request failed").into_response();
        }
    };

    let status = resp.status();
    upstream.requests.fetch_add(1, Ordering::Relaxed);

    info!(
        client = %client_ip,
        client_id = %client_id,
        model = %model,
        upstream = upstream.name,
        status = status.as_u16(),
        total = upstream.requests.load(Ordering::Relaxed),
        "proxied (upstream)"
    );

    let resp_status = StatusCode::from_u16(status.as_u16()).unwrap_or(StatusCode::BAD_GATEWAY);
    let resp_headers = resp.headers().clone();

    let mut builder = Response::builder().status(resp_status);
    for (k, v) in resp_headers.iter() {
        if k == "transfer-encoding" {
            continue;
        }
        builder = builder.header(k, v);
    }
    builder
        .body(Body::from_stream(resp.bytes_stream()))
        .unwrap_or_else(|_| {
            (StatusCode::INTERNAL_SERVER_ERROR, "response build error").into_response()
        })
}

// ── Stats endpoint ──────────────────────────────────────────────────

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
    let mut out = Vec::new();
    let mut total_headroom: Option<u64> = Some(0);
    for acct in &state.accounts {
        let info = acct.rate_info.read().await;
        let hard_limited = match info.hard_limited_until {
            Some(until) if Instant::now() < until => {
                Some(until.duration_since(Instant::now()).as_secs())
            }
            _ => None,
        };

        // Burn rate from EWMA tracker
        let (br_5m, br_1h, br_6h) = acct
            .burn_rate
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
            _ => total_headroom = None,
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

        out.push(serde_json::json!({
            "name": acct.name,
            "passthrough": acct.passthrough,
            "requests_total": acct.requests.load(Ordering::Relaxed),
            "utilization": info.utilization,
            "utilization_7d": info.utilization_7d,
            "utilization_5h": info.utilization_5h,
            "representative_claim": info.representative_claim,
            "reset_5h": info.reset_5h,
            "reset_7d": info.reset_7d,
            "status_5h": info.status_5h,
            "status_7d": info.status_7d,
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
                "input_tokens": acct.input_tokens.load(Ordering::Relaxed),
                "output_tokens": acct.output_tokens.load(Ordering::Relaxed),
                "cache_creation_input_tokens": acct.cache_creation_tokens.load(Ordering::Relaxed),
                "cache_read_input_tokens": acct.cache_read_tokens.load(Ordering::Relaxed),
            },
        }));
    }
    let mut upstream_stats = Vec::new();
    for u in &state.upstreams {
        upstream_stats.push(serde_json::json!({
            "name": u.name,
            "base_url": u.base_url,
            "requests_total": u.requests.load(Ordering::Relaxed),
        }));
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
        "accounts": out,
        "upstreams": upstream_stats,
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
struct AcctMetricsSnap {
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
}

#[cfg(test)]
fn append_routing_weight_metrics(
    buf: &mut String,
    accounts: &[Account],
    snaps: &[AcctMetricsSnap],
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

    for (acct, snap) in accounts.iter().zip(snaps.iter()) {
        if snap.passthrough {
            continue;
        }
        let weight = f64::from_bits(acct.last_routing_weight.load(Ordering::Relaxed));
        let share = f64::from_bits(acct.last_routing_share.load(Ordering::Relaxed));
        let gate = f64::from_bits(acct.last_effective_gate.load(Ordering::Relaxed));
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

    let mut snaps: Vec<AcctMetricsSnap> = Vec::with_capacity(state.accounts.len());
    let mut total_headroom: Option<u64> = Some(0);

    for acct in &state.accounts {
        let info = acct.rate_info.read().await;
        let (br_5m, br_1h, br_6h) = acct
            .burn_rate
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
            _ => total_headroom = None,
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

        snaps.push(AcctMetricsSnap {
            name: acct.name.clone(),
            passthrough: acct.passthrough,
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
            requests_total: acct.requests.load(Ordering::Relaxed),
            hard_limited_secs,
            projected_throttle_secs,
            token_usage: [
                acct.input_tokens.load(Ordering::Relaxed),
                acct.output_tokens.load(Ordering::Relaxed),
                acct.cache_creation_tokens.load(Ordering::Relaxed),
                acct.cache_read_tokens.load(Ordering::Relaxed),
            ],
            claims,
            last_updated_epoch: info.last_updated_epoch,
        });
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

    for (acct, s) in state.accounts.iter().zip(snaps.iter()) {
        if s.passthrough {
            continue;
        }
        let weight = f64::from_bits(acct.last_routing_weight.load(Ordering::Relaxed));
        let share = f64::from_bits(acct.last_routing_share.load(Ordering::Relaxed));
        let gate = f64::from_bits(acct.last_effective_gate.load(Ordering::Relaxed));
        prom_gauge(
            &mut buf,
            "anthropic_account_routing_weight",
            &[("account", &s.name)],
            weight,
        );
        prom_gauge(
            &mut buf,
            "anthropic_account_routing_share",
            &[("account", &s.name)],
            share,
        );
        prom_gauge(
            &mut buf,
            "anthropic_account_effective_gate",
            &[("account", &s.name)],
            gate,
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

    // Upstreams — label is "upstream" (not "name"), no base_url exposed
    prom_header(
        &mut buf,
        "anthropic_upstream_requests_total",
        "counter",
        "Requests per upstream",
    );
    for u in &state.upstreams {
        prom_counter(
            &mut buf,
            "anthropic_upstream_requests_total",
            &[("upstream", &u.name)],
            u.requests.load(Ordering::Relaxed),
        );
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
                if let Some(content) = msg.get("content").and_then(|c| c.as_str()) {
                    system_parts.push(content.to_string());
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
                // Standard message — strip "name" field, keep role + content
                let mut clean = serde_json::Map::new();
                clean.insert(
                    "role".to_string(),
                    serde_json::Value::String(role.to_string()),
                );
                if let Some(content) = msg.get("content") {
                    clean.insert("content".to_string(), content.clone());
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
    let affinity_key = rctx.affinity_key(&client_ip);
    let affinity = affinity_key.as_deref();
    let RequestContext {
        client_id,
        client_ver,
        agent_id,
        session_id,
    } = rctx;

    let body_bytes = match axum::body::to_bytes(body, MAX_REQUEST_BODY_BYTES).await {
        Ok(b) => b,
        Err(e) => {
            error!("failed to read request body: {e}");
            return (StatusCode::BAD_REQUEST, "bad request body").into_response();
        }
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

    let n = state.accounts.len();
    for retry_round in 0..=MAX_529_RETRIES {
        if retry_round > 0 {
            let delay = RETRY_529_BASE_DELAY * 2u32.pow(retry_round - 1);
            warn!(
                retry_round = retry_round,
                delay_ms = delay.as_millis() as u64,
                "529 backoff: retrying all accounts"
            );
            tokio::time::sleep(delay).await;
        }
        let mut skip: Vec<usize> = Vec::new();
        let mut saw_529 = false;
        for _attempt in 0..n {
            let idx = match state.pick_account(affinity, &model, &skip).await {
                Some(i) => i,
                None => {
                    warn!("all accounts rate-limited");
                    return (
                        StatusCode::TOO_MANY_REQUESTS,
                        "all upstream accounts rate-limited",
                    )
                        .into_response();
                }
            };

            let acct = &state.accounts[idx];
            let url = format!("{}/v1/messages", state.upstream);

            let mut headers = parts.headers.clone();
            headers.remove("host");
            if !acct.passthrough {
                headers.remove("authorization");
                headers.remove("x-api-key");
            }
            headers.remove("content-length"); // body size changes after translation
            headers.remove("accept-encoding"); // we need plaintext to translate the response

            // Inject required Anthropic headers
            headers.insert("content-type", HeaderValue::from_static("application/json"));
            headers.insert("anthropic-version", HeaderValue::from_static("2023-06-01"));

            // Auth injection
            inject_account_auth(&mut headers, &acct.token, acct.passthrough);

            // Use OAuth variant (with CC system prompt) for OAuth tokens
            let req_body = if acct.token.starts_with("sk-ant-oat") {
                &oauth_anthropic_body
            } else {
                &anthropic_body
            };
            let body_str = req_body.to_string();
            debug!(
                account = acct.name,
                model = %model,
                body_len = body_str.len(),
                "openai-compat: upstream request"
            );

            let upstream_req = state
                .client
                .request(reqwest::Method::POST, &url)
                .headers(headers)
                .body(body_str);

            let mut resp = match upstream_req.send().await {
                Ok(r) => r,
                Err(e) => {
                    error!(account = acct.name, "upstream request failed: {e}");
                    continue;
                }
            };

            let status = resp.status();
            acct.requests.fetch_add(1, Ordering::Relaxed);
            state.update_rate_info(idx, resp.headers()).await;

            // Update burn rate (after rate-limit headers are parsed)
            state.update_burn_rate(acct, &client_id);

            if status == StatusCode::TOO_MANY_REQUESTS {
                state.mark_hard_limited(idx, resp.headers()).await;
                log_429_details(&acct.name, resp).await;
                state.save_state().await;
                info!(account = acct.name, "got 429, rotating to next account");
                skip.push(idx);
                continue;
            }

            // 529 → overloaded, try next account; flag for BEBO retry if all exhausted
            if status.as_u16() == 529 {
                state.save_state().await;
                warn!(account = acct.name, "got 529, rotating to next account");
                saw_529 = true;
                skip.push(idx);
                continue;
            }

            // Other 5xx → transient, try next account (no BEBO retry)
            if status.is_server_error() {
                state.save_state().await;
                warn!(
                    account = acct.name,
                    status = status.as_u16(),
                    "got server error, rotating to next account"
                );
                skip.push(idx);
                continue;
            }

            // Clear hard limit and burst counter only on a genuine 2xx success.
            // A 4xx (e.g. invalid_request_error, auth failure) is not evidence
            // that the rate-limit window has drained — don't clobber state on
            // client errors.
            let recovered = if status.is_success() {
                let mut info = acct.rate_info.write().await;
                let was = info.hard_limited_until.is_some();
                info.hard_limited_until = None;
                info.consecutive_burst_429s = 0;
                was
            } else {
                false
            };

            // Persist state after updating rate-limit state so completed 4xx
            // responses and other terminal outcomes aren't dropped on restart.
            state.save_state().await;

            if recovered {
                state.signal_hard_limit_recovery(acct).await;
            }

            // Compute budget pressure status for response header + log
            let budget_status = {
                let info = acct.rate_info.read().await;
                let (eff_util, constraint, _adj_5h, _adj_7d) =
                    effective_utilization(&info, AppState::now_epoch(), &model);
                info!(
                    req_id,
                    client = %client_ip,
                    client_id = %client_id,
                    ver = %client_ver,
                    agent = %agent_id,
                    session = %session_id,
                    model = %model,
                    account = acct.name,
                    status = status.as_u16(),
                    utilization = format_args!("{eff_util:.2}"),
                    util_5h = info.utilization_5h.map(|v| format!("{v:.2}")).as_deref().unwrap_or("-"),
                    util_7d = info.utilization_7d.map(|v| format!("{v:.2}")).as_deref().unwrap_or("-"),
                    constraint,
                    openai_compat = true,
                    stream = is_streaming,
                    "proxied (openai-compat)"
                );
                compute_pressure_status(eff_util, &client_id, &state)
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
                    account = acct.name,
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

                return Response::builder()
                    .status(
                        StatusCode::from_u16(status.as_u16()).unwrap_or(StatusCode::BAD_GATEWAY),
                    )
                    .header("content-type", "application/json")
                    .header("x-budget-status", budget_status)
                    .body(Body::from(
                        serde_json::to_vec(&openai_error).unwrap_or_default(),
                    ))
                    .unwrap_or_else(|_| {
                        (StatusCode::INTERNAL_SERVER_ERROR, "response build error").into_response()
                    });
            }

            if is_streaming {
                let (tx, rx) =
                    tokio::sync::mpsc::channel::<Result<bytes::Bytes, std::io::Error>>(32);
                let state_clone = state.clone();
                let client_id_clone = client_id.clone();
                let acct_name = acct.name.clone();
                let model_clone = model.clone();
                let client_ip_str = client_ip.to_string();
                let agent_clone = agent_id.clone();
                let session_clone = session_id.clone();

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
                                    let event =
                                        String::from_utf8_lossy(&buffer[..pos]).into_owned();
                                    buffer.drain(..pos + 2);

                                    if event.trim().is_empty() {
                                        continue;
                                    }

                                    if let Some(translated) = translate_sse_event(&event, &mut ctx)
                                    {
                                        if translated.trim() == "data: [DONE]" {
                                            sent_done = true;
                                        }
                                        if tx
                                            .send(Ok(bytes::Bytes::from(translated)))
                                            .await
                                            .is_err()
                                        {
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
                                warn!(req_id, error = %e, "upstream SSE read failed");
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

                    // Extract and record token usage from accumulated SSE data
                    finalize_stream(
                        &state_clone,
                        idx,
                        &req_id,
                        &client_id_clone,
                        &model_clone,
                        &acct_name,
                        &client_ip_str,
                        &agent_clone,
                        &session_clone,
                        status.as_u16(),
                        &raw_sse,
                        request_start,
                        client_gone,
                        upstream_error,
                        true,
                    )
                    .await;
                });

                return Response::builder()
                    .status(StatusCode::OK)
                    .header("content-type", "text/event-stream")
                    .header("cache-control", "no-cache")
                    .header("connection", "keep-alive")
                    .header("x-budget-status", budget_status)
                    .body(Body::from_stream(ReceiverStream::new(rx)))
                    .unwrap_or_else(|_| {
                        (StatusCode::INTERNAL_SERVER_ERROR, "response build error").into_response()
                    });
            }

            // Non-streaming: buffer, translate, return
            let resp_bytes = match resp.bytes().await {
                Ok(b) => b,
                Err(e) => {
                    error!("failed to read upstream response: {e}");
                    return (StatusCode::BAD_GATEWAY, "failed to read upstream response")
                        .into_response();
                }
            };

            let anthropic_resp: serde_json::Value = match serde_json::from_slice(&resp_bytes) {
                Ok(v) => v,
                Err(_) => {
                    return Response::builder()
                        .status(StatusCode::OK)
                        .header("content-type", "application/json")
                        .header("x-budget-status", budget_status)
                        .body(Body::from(resp_bytes))
                        .unwrap_or_else(|_| {
                            (StatusCode::INTERNAL_SERVER_ERROR, "response build error")
                                .into_response()
                        });
                }
            };

            let openai_resp = translate_anthropic_to_openai(&anthropic_resp);

            // Extract and record token usage from non-streaming response
            let usage = TokenUsage::from_response_body(&anthropic_resp);
            finalize_non_stream(
                &state,
                idx,
                &req_id,
                &client_id,
                &model,
                &acct.name,
                &client_ip.to_string(),
                &agent_id,
                &session_id,
                status.as_u16(),
                &usage,
                request_start.elapsed().as_millis() as u64,
                true,
            )
            .await;

            return Response::builder()
                .status(StatusCode::OK)
                .header("content-type", "application/json")
                .header("x-budget-status", budget_status)
                .body(Body::from(
                    serde_json::to_vec(&openai_resp).unwrap_or_default(),
                ))
                .unwrap_or_else(|_| {
                    (StatusCode::INTERNAL_SERVER_ERROR, "response build error").into_response()
                });
        }
        if !saw_529 {
            break;
        }
    }

    (StatusCode::TOO_MANY_REQUESTS, "exhausted all accounts").into_response()
}

// ── Main ────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() {
    // Parse config first so debug_log path is available for tracing setup
    let config_path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "config.toml".to_string());
    let config_str = std::fs::read_to_string(&config_path)
        .unwrap_or_else(|e| panic!("failed to read {config_path}: {e}"));
    let config: Config =
        toml::from_str(&config_str).unwrap_or_else(|e| panic!("invalid config: {e}"));

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

    assert!(!config.accounts.is_empty(), "at least one account required");

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

    let accounts: Vec<Account> = config
        .accounts
        .into_iter()
        .map(|a| {
            let passthrough = a.token == "passthrough";
            if !a.models.is_empty() {
                info!(name = a.name, passthrough, models = ?a.models, "loaded account");
            } else {
                info!(name = a.name, passthrough, "loaded account");
            }
            Account {
                name: a.name,
                passthrough,
                models: a.models,
                token: a.token,
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

    let upstreams: Vec<Upstream> = config
        .upstreams
        .iter()
        .map(|u| {
            info!(name = u.name, base_url = u.base_url, "loaded upstream");
            Upstream {
                name: u.name.clone(),
                base_url: u.base_url.clone(),
                api_key: u.api_key.clone(),
                requests: AtomicU64::new(0),
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
        num_accounts = accounts.len(),
        num_upstreams = upstreams.len(),
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
        client: Client::builder()
            .timeout(Duration::from_secs(600))
            .build()
            .expect("failed to build HTTP client"),
        upstream: config.upstream,
        accounts,
        robin: AtomicUsize::new(0),
        routing_strategy,
        cooldown,
        state_path,
        proxy_key: config.proxy_key.clone(),
        allowed_ips,
        upstreams,
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
        .route("/upstream/{name}/{*rest}", any(upstream_handler))
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

    // Spawn periodic probe task
    let probe_interval = config.probe_interval_secs.unwrap_or(300);
    if probe_interval > 0 {
        let probe_state = state.clone();
        let n_accounts = probe_state.accounts.len();
        tokio::spawn(async move {
            const PROBE_MODELS: &[&str] =
                &["claude-haiku-4-5", "claude-sonnet-4-6", "claude-opus-4-6"];
            // Stagger initial probes: wait 10s then probe all accounts
            tokio::time::sleep(Duration::from_secs(10)).await;
            info!(
                interval_secs = probe_interval,
                "starting utilization probes"
            );
            loop {
                for i in 0..n_accounts {
                    let acct = &probe_state.accounts[i];
                    // Probe all model families per account per cycle
                    for model in PROBE_MODELS {
                        if acct.serves_model(model) {
                            probe_state.probe_account(i, model).await;
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
        // Probes disabled — but `update_rate_info()` still refreshes data on
        // every inbound request. Without this fallback ticker the routing
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

    // Graceful shutdown: save state on SIGTERM/SIGINT
    let shutdown_state = state.clone();
    let shutdown = async move {
        let ctrl_c = tokio::signal::ctrl_c();
        let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to register SIGTERM");
        tokio::select! {
            _ = ctrl_c => info!("received SIGINT"),
            _ = sigterm.recv() => info!("received SIGTERM"),
        }
        info!("saving state before shutdown...");
        shutdown_state.save_state().await;
        info!("state saved, shutting down");
    };

    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .with_graceful_shutdown(shutdown)
    .await
    .unwrap_or_else(|e| panic!("server error: {e}"));
}

// ── Tests ────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // ── Helpers ──────────────────────────────────────────────────────

    fn make_account(name: &str, token: &str) -> Account {
        Account {
            name: name.to_string(),
            token: token.to_string(),
            passthrough: token == "passthrough",
            models: vec![],
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
    }

    fn test_state_with_strategy(
        accounts: Vec<Account>,
        routing_strategy: RoutingStrategy,
    ) -> Arc<AppState> {
        Arc::new(AppState {
            client: Client::builder()
                .timeout(Duration::from_secs(5))
                .build()
                .unwrap(),
            upstream: "http://127.0.0.1:1".to_string(), // unused in unit tests
            accounts,
            robin: AtomicUsize::new(0),
            routing_strategy,
            cooldown: Duration::from_secs(60),
            state_path: PathBuf::from("/tmp/anthropic-lb-test.state.json"),
            proxy_key: None,
            allowed_ips: vec![],
            upstreams: vec![],
            client_names: HashMap::new(),
            auto_cache: true,
            client_usage: Mutex::new(HashMap::new()),
            shadow_log_tx: None,
            shadow_log_dropped: AtomicU64::new(0),
            client_budgets: HashMap::new(),
            budget_usage: Mutex::new(HashMap::new()),
            client_utilization_limits: HashMap::new(),
            operators: vec![],
            emergency_brake: true,
            emergency_threshold: DEFAULT_EMERGENCY_THRESHOLD,
            client_request_rates: Mutex::new(HashMap::new()),
            soft_limit: 1.0,
            redis: None,
            cluster_info_cache: Mutex::new(None),
            next_req_id: AtomicU64::new(0),
            instance_id: 0,
            probe_interval_secs: 300,
        })
    }

    fn test_state_with(accounts: Vec<Account>) -> Arc<AppState> {
        test_state_with_strategy(accounts, RoutingStrategy::default())
    }

    fn test_state_with_soft_limit(accounts: Vec<Account>, soft_limit: f64) -> Arc<AppState> {
        let mut state = test_state_with(accounts);
        Arc::get_mut(&mut state)
            .expect("test fixture should be uniquely owned")
            .soft_limit = soft_limit;
        state
    }

    /// Spawn a mock upstream that returns a canned response with rate-limit headers.
    async fn spawn_mock_upstream() -> (String, tokio::task::JoinHandle<()>) {
        let app = Router::new().fallback(any(mock_upstream_handler));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let handle = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        (format!("http://{}", addr), handle)
    }

    async fn mock_upstream_handler(req: Request<Body>) -> Response {
        let has_auth =
            req.headers().contains_key("x-api-key") || req.headers().contains_key("authorization");

        if !has_auth {
            return (StatusCode::UNAUTHORIZED, "missing auth").into_response();
        }

        let mut resp = axum::Json(serde_json::json!({
            "id": "msg_test",
            "type": "message",
            "content": [{"type": "text", "text": "ok"}],
        }))
        .into_response();

        // Inject rate-limit headers the proxy expects
        let headers = resp.headers_mut();
        headers.insert(
            "anthropic-ratelimit-unified-representative-claim",
            HeaderValue::from_static("five_hour"),
        );
        headers.insert(
            "anthropic-ratelimit-unified-5h-utilization",
            HeaderValue::from_static("0.25"),
        );
        // Valid reset 1h in the future so unified derivation includes 5h
        let reset_epoch = (std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs()
            + 3600)
            .to_string();
        headers.insert(
            "anthropic-ratelimit-unified-5h-reset",
            HeaderValue::from_str(&reset_epoch).unwrap(),
        );
        resp
    }

    /// Build the full app router against a given upstream URL.
    fn test_app_with_strategy(
        upstream_url: &str,
        proxy_key: Option<String>,
        routing_strategy: RoutingStrategy,
    ) -> (Router, Arc<AppState>) {
        let accounts = vec![
            make_account("acct-a", "sk-ant-api-test-aaa"),
            make_account("acct-b", "sk-ant-api-test-bbb"),
        ];

        let state = Arc::new(AppState {
            client: Client::builder()
                .timeout(Duration::from_secs(5))
                .build()
                .unwrap(),
            upstream: upstream_url.to_string(),
            accounts,
            robin: AtomicUsize::new(0),
            routing_strategy,
            cooldown: Duration::from_secs(60),
            state_path: PathBuf::from("/tmp/anthropic-lb-test.state.json"),
            proxy_key,
            allowed_ips: vec![],
            upstreams: vec![Upstream {
                name: "mock".to_string(),
                base_url: upstream_url.to_string(),
                api_key: "test-key".to_string(),
                requests: AtomicU64::new(0),
            }],
            client_names: HashMap::new(),
            auto_cache: true,
            client_usage: Mutex::new(HashMap::new()),
            shadow_log_tx: None,
            shadow_log_dropped: AtomicU64::new(0),
            client_budgets: HashMap::new(),
            budget_usage: Mutex::new(HashMap::new()),
            client_utilization_limits: HashMap::new(),
            operators: vec![],
            emergency_brake: true,
            emergency_threshold: DEFAULT_EMERGENCY_THRESHOLD,
            client_request_rates: Mutex::new(HashMap::new()),
            soft_limit: 1.0,
            redis: None,
            cluster_info_cache: Mutex::new(None),
            next_req_id: AtomicU64::new(0),
            instance_id: 0,
            probe_interval_secs: 300,
        });

        (build_router(state.clone()), state)
    }

    /// Build a router from a pre-configured state. Used by integration tests
    /// that need custom AppState (operator, utilization limits, etc.).
    fn build_router(state: Arc<AppState>) -> Router {
        Router::new()
            .route("/_stats", axum::routing::get(stats_handler))
            .route("/metrics", axum::routing::get(metrics_handler))
            .route(
                "/v1/chat/completions",
                axum::routing::post(openai_chat_handler),
            )
            .route("/upstream/{name}/{*rest}", any(upstream_handler))
            .fallback(any(proxy_handler))
            .with_state(state)
    }

    /// Start a test server and return its address. Spawns the axum server
    /// with ConnectInfo support for client IP extraction.
    async fn serve(app: Router) -> SocketAddr {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(
                listener,
                app.into_make_service_with_connect_info::<SocketAddr>(),
            )
            .await
            .unwrap();
        });
        addr
    }

    // ── Unit: IP allowlist ──────────────────────────────────────────

    #[test]
    fn ip_allow_entry_matches_exact_addr() {
        let entry = IpAllowEntry::Addr("10.0.0.1".parse().unwrap());
        assert!(entry.contains(&"10.0.0.1".parse().unwrap()));
        assert!(!entry.contains(&"10.0.0.2".parse().unwrap()));
    }

    #[test]
    fn ip_allow_entry_matches_cidr() {
        let entry = IpAllowEntry::Net("10.0.0.0/24".parse().unwrap());
        assert!(entry.contains(&"10.0.0.1".parse().unwrap()));
        assert!(entry.contains(&"10.0.0.254".parse().unwrap()));
        assert!(!entry.contains(&"10.0.1.1".parse().unwrap()));
    }

    #[test]
    fn empty_allowlist_allows_all() {
        let state = test_state_with(vec![make_account("a", "sk-ant-api-x")]);
        assert!(state.is_ip_allowed(&"192.168.1.1".parse().unwrap()));
        assert!(state.is_ip_allowed(&"8.8.8.8".parse().unwrap()));
    }

    #[test]
    fn populated_allowlist_blocks_unknown() {
        let state = Arc::new(AppState {
            client: Client::builder()
                .timeout(Duration::from_secs(5))
                .build()
                .unwrap(),
            upstream: "http://127.0.0.1:1".to_string(),
            accounts: vec![make_account("a", "sk-ant-api-x")],
            robin: AtomicUsize::new(0),
            routing_strategy: RoutingStrategy::default(),
            cooldown: Duration::from_secs(60),
            state_path: PathBuf::from("/tmp/test.state.json"),
            proxy_key: None,
            allowed_ips: vec![IpAllowEntry::Addr("10.0.0.1".parse().unwrap())],
            upstreams: vec![],
            client_names: HashMap::new(),
            auto_cache: true,
            client_usage: Mutex::new(HashMap::new()),
            shadow_log_tx: None,
            shadow_log_dropped: AtomicU64::new(0),
            client_budgets: HashMap::new(),
            budget_usage: Mutex::new(HashMap::new()),
            client_utilization_limits: HashMap::new(),
            operators: vec![],
            emergency_brake: true,
            emergency_threshold: DEFAULT_EMERGENCY_THRESHOLD,
            client_request_rates: Mutex::new(HashMap::new()),
            soft_limit: 1.0,
            redis: None,
            cluster_info_cache: Mutex::new(None),
            next_req_id: AtomicU64::new(0),
            instance_id: 0,
            probe_interval_secs: 300,
        });
        assert!(state.is_ip_allowed(&"10.0.0.1".parse().unwrap()));
        assert!(!state.is_ip_allowed(&"10.0.0.2".parse().unwrap()));
    }

    // ── Unit: pick_account ──────────────────────────────────────────

    #[tokio::test]
    async fn pick_prefers_lowest_utilization() {
        // With weighted buckets, the account with more headroom should get
        // a proportionally larger share of traffic
        let state = test_state_with(vec![
            make_account("high", "sk-ant-api-high"),
            make_account("low", "sk-ant-api-low"),
        ]);

        // high=0.8 (headroom 0.2), low=0.2 (headroom 0.8) → 80% should go to "low"
        {
            let mut info = state.accounts[0].rate_info.write().await;
            info.utilization = Some(0.8);
        }
        {
            let mut info = state.accounts[1].rate_info.write().await;
            info.utilization = Some(0.2);
        }

        let mut counts = [0u32; 2];
        for _ in 0..1000 {
            let idx = state.pick_account(None, "", &[]).await.unwrap();
            counts[idx] += 1;
        }

        // "low" (idx=1) should get ~80% of traffic (±5%)
        let low_pct = counts[1] as f64 / 1000.0;
        assert!(
            (0.75..=0.85).contains(&low_pct),
            "low-util account should get ~80% traffic, got {:.1}%",
            low_pct * 100.0
        );
    }

    #[tokio::test]
    async fn pick_skips_hard_limited() {
        let state = test_state_with(vec![
            make_account("limited", "sk-ant-api-a"),
            make_account("available", "sk-ant-api-b"),
        ]);

        // Hard-limit the first account
        {
            let mut info = state.accounts[0].rate_info.write().await;
            info.utilization = Some(0.1); // great utilization but hard-limited
            info.hard_limited_until = Some(Instant::now() + Duration::from_secs(3600));
        }
        {
            let mut info = state.accounts[1].rate_info.write().await;
            info.utilization = Some(0.9);
        }

        let idx = state.pick_account(None, "", &[]).await.unwrap();
        assert_eq!(
            idx, 1,
            "should skip hard-limited account despite lower utilization"
        );
    }

    #[tokio::test]
    async fn pick_round_robin_when_no_info() {
        // With no utilization data, all accounts get headroom=0.5 (equal buckets)
        let state = test_state_with(vec![
            make_account("a", "sk-ant-api-a"),
            make_account("b", "sk-ant-api-b"),
            make_account("c", "sk-ant-api-c"),
        ]);

        // Call many times without affinity — Fibonacci scatter should distribute evenly
        let mut counts = [0u32; 3];
        for _ in 0..300 {
            let idx = state.pick_account(None, "", &[]).await.unwrap();
            counts[idx] += 1;
        }

        // Each should get ~33% (±10%)
        for (i, &count) in counts.iter().enumerate() {
            let pct = count as f64 / 300.0;
            assert!(
                (0.23..=0.43).contains(&pct),
                "account {} should get ~33% traffic, got {:.1}%",
                i,
                pct * 100.0
            );
        }
    }

    #[tokio::test]
    async fn pick_returns_none_when_all_limited() {
        let state = test_state_with(vec![
            make_account("a", "sk-ant-api-a"),
            make_account("b", "sk-ant-api-b"),
        ]);

        for acct in &state.accounts {
            let mut info = acct.rate_info.write().await;
            info.hard_limited_until = Some(Instant::now() + Duration::from_secs(3600));
        }

        assert!(state.pick_account(None, "", &[]).await.is_none());
    }

    #[tokio::test]
    async fn pick_recovers_after_hard_limit_expires() {
        // After a hard limit expires with stale data, the account should still be
        // selectable with 0.5 (unknown) utilization instead of being permanently stuck.
        let state = test_state_with(vec![make_account("recovering", "sk-ant-api-a")]);

        // Simulate mark_hard_limited: set hard_limited_until in the past (expired),
        // poison remaining_tokens to 0, set high utilization from the 429 response.
        {
            let mut info = state.accounts[0].rate_info.write().await;
            let hard_limit_time = Instant::now() - Duration::from_secs(10);
            info.hard_limited_until = Some(hard_limit_time);
            info.remaining_tokens = Some(0);
            info.remaining_requests = Some(0);
            info.utilization = Some(1.0);
            info.utilization_5h = Some(1.0);
            // last_updated before the hard limit → stale_after_hard_limit = true
            info.last_updated = Some(hard_limit_time - Duration::from_secs(1));
        }

        let result = state.pick_account(None, "", &[]).await;
        assert!(
            result.is_some(),
            "account with expired hard limit should be selectable despite stale high utilization"
        );
    }

    #[tokio::test]
    async fn pick_ignores_stale_rejected_claim_after_hard_limit() {
        // A "rejected" 7d claim from a 429 response should not permanently block the
        // account once the hard limit has expired without fresh data.
        let state = test_state_with(vec![make_account("recovering", "sk-ant-api-a")]);
        let now_epoch = AppState::now_epoch();

        {
            let mut info = state.accounts[0].rate_info.write().await;
            let hard_limit_time = Instant::now() - Duration::from_secs(10);
            info.hard_limited_until = Some(hard_limit_time);
            info.last_updated = Some(hard_limit_time - Duration::from_secs(1));
            info.utilization_5h = Some(0.95);
            info.reset_5h = Some(now_epoch + 10000);
            info.claims_7d.insert(
                "seven_day_sonnet".to_string(),
                ClaimWindowData {
                    utilization: Some(1.0),
                    reset: Some(now_epoch + 100000),
                    status: Some("rejected".to_string()),
                    ..Default::default()
                },
            );
        }

        let result = state
            .pick_account(Some("test"), "claude-sonnet-4-6", &[])
            .await;
        assert!(
            result.is_some(),
            "stale rejected claim after expired hard limit should not block account"
        );
    }

    #[tokio::test]
    async fn pick_still_skips_fresh_rejected_claim() {
        // If data was refreshed AFTER the hard limit (e.g., by a probe that got fresh
        // "rejected" status), the account should still be skipped.
        let state = test_state_with(vec![
            make_account("rejected", "sk-ant-api-a"),
            make_account("available", "sk-ant-api-b"),
        ]);
        let now_epoch = AppState::now_epoch();

        {
            let mut info = state.accounts[0].rate_info.write().await;
            let hard_limit_time = Instant::now() - Duration::from_secs(300);
            info.hard_limited_until = Some(hard_limit_time);
            // last_updated AFTER the hard limit → data is fresh, not stale
            info.last_updated = Some(Instant::now() - Duration::from_secs(5));
            info.utilization_5h = Some(0.30);
            info.reset_5h = Some(now_epoch + 10000);
            info.claims_7d.insert(
                "seven_day_sonnet".to_string(),
                ClaimWindowData {
                    utilization: Some(1.0),
                    reset: Some(now_epoch + 100000),
                    status: Some("rejected".to_string()),
                    ..Default::default()
                },
            );
        }
        {
            let mut info = state.accounts[1].rate_info.write().await;
            info.utilization = Some(0.5);
        }

        let result = state
            .pick_account(Some("test"), "claude-sonnet-4-6", &[])
            .await;
        assert_eq!(
            result,
            Some(1),
            "fresh rejected claim should still skip the account"
        );
    }

    #[tokio::test]
    async fn pick_ignores_expired_rejected_claim_without_hard_limit() {
        let state = test_state_with(vec![
            make_account("recovered", "sk-ant-api-a"),
            make_account("available", "sk-ant-api-b"),
        ]);
        let now_epoch = AppState::now_epoch();

        {
            let mut info = state.accounts[0].rate_info.write().await;
            info.utilization = Some(0.30);
            info.utilization_5h = Some(0.30);
            info.reset_5h = Some(now_epoch + 10000);
            info.claims_7d.insert(
                "seven_day_sonnet".to_string(),
                ClaimWindowData {
                    utilization: Some(1.0),
                    reset: Some(now_epoch.saturating_sub(1)),
                    status: Some("rejected".to_string()),
                    ..Default::default()
                },
            );
        }
        {
            let mut info = state.accounts[1].rate_info.write().await;
            info.utilization = Some(0.80);
            info.utilization_5h = Some(0.80);
            info.reset_5h = Some(now_epoch + 10000);
        }

        let result = state
            .pick_account(Some("test"), "claude-sonnet-4-6", &[])
            .await;
        assert_eq!(
            result,
            Some(0),
            "expired rejected claim should not block account selection"
        );
    }

    #[tokio::test]
    async fn pick_uses_fresh_data_after_hard_limit_cleared() {
        // After a probe clears hard_limited_until and refreshes data, the normal
        // routing logic should apply (not the 0.5 fallback).
        let state = test_state_with(vec![
            make_account("low_util", "sk-ant-api-a"),
            make_account("high_util", "sk-ant-api-b"),
        ]);

        {
            // hard_limited_until is None (cleared by probe), fresh data available
            let mut info = state.accounts[0].rate_info.write().await;
            info.utilization = Some(0.2);
            info.last_updated = Some(Instant::now());
        }
        {
            let mut info = state.accounts[1].rate_info.write().await;
            info.utilization = Some(0.8);
            info.last_updated = Some(Instant::now());
        }

        // low_util should get ~80% of traffic (headroom=0.8 vs 0.2)
        let mut counts = [0u32; 2];
        for _ in 0..1000 {
            let idx = state.pick_account(None, "", &[]).await.unwrap();
            counts[idx] += 1;
        }
        let low_pct = counts[0] as f64 / 1000.0;
        assert!(
            low_pct > 0.70,
            "low-util account should get majority of traffic, got {:.1}%",
            low_pct * 100.0
        );
    }

    #[tokio::test]
    async fn mark_hard_limited_detects_burst_429() {
        // Burst 429: x-should-retry=true, no retry-after, no rate-limit headers.
        // Should use short cooldown, NOT poison remaining_tokens/requests.
        let state = test_state_with(vec![make_account("burst-test", "sk-ant-api-a")]);

        // Pre-set some remaining tokens to verify they aren't poisoned
        {
            let mut info = state.accounts[0].rate_info.write().await;
            info.remaining_tokens = Some(5000);
            info.remaining_requests = Some(10);
        }

        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert("x-should-retry", HeaderValue::from_static("true"));
        // No retry-after, no anthropic-ratelimit-*, no x-ratelimit-*

        state.mark_hard_limited(0, &headers).await;

        let info = state.accounts[0].rate_info.read().await;
        assert!(
            info.hard_limited_until.is_some(),
            "burst 429 should still set hard_limited_until"
        );
        assert_eq!(
            info.remaining_tokens,
            Some(5000),
            "burst 429 should NOT poison remaining_tokens"
        );
        assert_eq!(
            info.remaining_requests,
            Some(10),
            "burst 429 should NOT poison remaining_requests"
        );
        assert_eq!(info.consecutive_burst_429s, 1);
    }

    #[tokio::test]
    async fn mark_hard_limited_capacity_429_poisons_state() {
        // Capacity 429: has rate-limit headers → should poison remaining_tokens/requests to 0.
        let state = test_state_with(vec![make_account("cap-test", "sk-ant-api-a")]);

        {
            let mut info = state.accounts[0].rate_info.write().await;
            info.remaining_tokens = Some(5000);
            info.remaining_requests = Some(10);
        }

        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert("x-should-retry", HeaderValue::from_static("true"));
        headers.insert(
            "anthropic-ratelimit-unified-5h-utilization",
            HeaderValue::from_static("0.99"),
        );
        // Has rate-limit headers → NOT a burst

        state.mark_hard_limited(0, &headers).await;

        let info = state.accounts[0].rate_info.read().await;
        assert_eq!(
            info.remaining_tokens,
            Some(0),
            "capacity 429 should poison remaining_tokens to 0"
        );
        assert_eq!(
            info.remaining_requests,
            Some(0),
            "capacity 429 should poison remaining_requests to 0"
        );
        assert_eq!(
            info.consecutive_burst_429s, 0,
            "capacity 429 should reset burst counter"
        );
    }

    #[tokio::test]
    async fn mark_hard_limited_burst_exponential_backoff() {
        // Consecutive burst 429s should produce increasing cooldowns.
        let state = test_state_with(vec![make_account("backoff-test", "sk-ant-api-a")]);

        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert("x-should-retry", HeaderValue::from_static("true"));

        // Fire 5 consecutive burst 429s
        let mut cooldowns = Vec::new();
        for _ in 0..5 {
            state.mark_hard_limited(0, &headers).await;
            let info = state.accounts[0].rate_info.read().await;
            let until = info.hard_limited_until.unwrap();
            let remaining = until.duration_since(Instant::now());
            cooldowns.push(remaining.as_secs());
        }

        // Should be roughly 5, 10, 20, 40, 60 (with some timing slack)
        assert!(
            cooldowns[0] <= 6,
            "1st burst should be ~5s, got {}s",
            cooldowns[0]
        );
        assert!(
            (9..=11).contains(&cooldowns[1]),
            "2nd burst should be ~10s, got {}s",
            cooldowns[1]
        );
        assert!(
            (19..=21).contains(&cooldowns[2]),
            "3rd burst should be ~20s, got {}s",
            cooldowns[2]
        );
        assert!(
            (39..=41).contains(&cooldowns[3]),
            "4th burst should be ~40s, got {}s",
            cooldowns[3]
        );
        assert!(
            (59..=61).contains(&cooldowns[4]),
            "5th burst should be ~60s, got {}s",
            cooldowns[4]
        );

        let info = state.accounts[0].rate_info.read().await;
        assert_eq!(info.consecutive_burst_429s, 5);
    }

    #[tokio::test]
    async fn mark_hard_limited_retry_after_overrides_default() {
        // When retry-after is present, it should be used regardless of x-should-retry.
        let state = test_state_with(vec![make_account("retry-test", "sk-ant-api-a")]);

        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert("retry-after", HeaderValue::from_static("30"));

        state.mark_hard_limited(0, &headers).await;

        let info = state.accounts[0].rate_info.read().await;
        let until = info.hard_limited_until.unwrap();
        let remaining = until.duration_since(Instant::now()).as_secs();
        assert!(
            (29..=31).contains(&remaining),
            "retry-after=30 should set ~30s cooldown, got {}s",
            remaining
        );
        assert_eq!(
            info.consecutive_burst_429s, 0,
            "non-burst 429 should reset burst counter"
        );
    }

    #[tokio::test]
    async fn pick_does_not_bias_unknown_accounts() {
        // Unknown accounts get headroom=0.5, known account with 0.1 util gets headroom=0.9
        // Traffic should favor the known account proportionally
        let state = test_state_with(vec![
            make_account("known", "sk-ant-api-known"),
            make_account("unknown", "sk-ant-api-unknown"),
        ]);

        {
            let mut info = state.accounts[0].rate_info.write().await;
            info.utilization = Some(0.1); // headroom = 0.9
        }
        // accounts[1] has no rate info → headroom = 0.5

        // known should get ~64% (0.9 / 1.4), unknown ~36% (0.5 / 1.4)
        let mut counts = [0u32; 2];
        for _ in 0..1000 {
            let idx = state.pick_account(None, "", &[]).await.unwrap();
            counts[idx] += 1;
        }

        let known_pct = counts[0] as f64 / 1000.0;
        assert!(
            (0.57..=0.71).contains(&known_pct),
            "known account should get ~64% traffic, got {:.1}%",
            known_pct * 100.0
        );
    }

    #[tokio::test]
    async fn pick_sticky_same_affinity() {
        // Same affinity key should always return the same account when weights
        // are close enough. AFFINITY_OVERRIDE_RATIO (0.5) compares the picked
        // account's weight to the best — affinity is preserved when the ratio
        // exceeds the threshold, i.e. no single account is 2x better.
        let state = test_state_with(vec![
            make_account("a", "sk-ant-api-a"),
            make_account("b", "sk-ant-api-b"),
            make_account("c", "sk-ant-api-c"),
        ]);

        // Similar utilization → similar weights → ratio stays above 0.5
        {
            let mut info = state.accounts[0].rate_info.write().await;
            info.utilization = Some(0.40);
        }
        {
            let mut info = state.accounts[1].rate_info.write().await;
            info.utilization = Some(0.45);
        }
        {
            let mut info = state.accounts[2].rate_info.write().await;
            info.utilization = Some(0.50);
        }

        let key = "192.168.1.1:client-42:agent-7:session-abc";
        let first = state.pick_account(Some(key), "", &[]).await.unwrap();
        for _ in 0..100 {
            let idx = state.pick_account(Some(key), "", &[]).await.unwrap();
            assert_eq!(
                idx, first,
                "same affinity key must always pick same account"
            );
        }
    }

    #[tokio::test]
    async fn pick_affinity_overridden_by_weight_disparity() {
        // When the affinity-picked account's weight is less than 50% of the best
        // account's weight, affinity should be overridden. This tests the scenario
        // where 5h utilization is similar but 7d utilization is vastly different —
        // the weight formula captures the 7d disparity via waste_risk.
        let state = test_state_with(vec![
            make_account("low_7d", "sk-ant-api-a"),
            make_account("high_7d", "sk-ant-api-b"),
        ]);
        let now_epoch = AppState::now_epoch();

        // Both have similar 5h utilization (so gate_5h is similar)
        // but vastly different 7d utilization via claims_7d.
        {
            let mut info = state.accounts[0].rate_info.write().await;
            info.utilization_5h = Some(0.12);
            info.reset_5h = Some(now_epoch + 10000);
            info.claims_7d.insert(
                "seven_day".to_string(),
                ClaimWindowData {
                    utilization: Some(0.30),
                    reset: Some(now_epoch + 300000),
                    status: None,
                    ..Default::default()
                },
            );
        }
        {
            let mut info = state.accounts[1].rate_info.write().await;
            info.utilization_5h = Some(0.10);
            info.reset_5h = Some(now_epoch + 10000);
            info.claims_7d.insert(
                "seven_day".to_string(),
                ClaimWindowData {
                    utilization: Some(0.85),
                    reset: Some(now_epoch + 300000),
                    status: None,
                    ..Default::default()
                },
            );
        }

        // Every affinity key should pick the low-7d account because
        // its weight is much higher (more waste_risk × similar headroom).
        for i in 0..200 {
            let key = format!("sticky-client-{}", i);
            let idx = state
                .pick_account(Some(&key), "claude-opus-4-6", &[])
                .await
                .unwrap();
            assert_eq!(
                idx, 0,
                "client {} picked account {} but should pick 'low_7d' (weight ratio < 0.5)",
                i, idx
            );
        }
    }

    /// Helper: run `pick_account` with varying affinity keys and assert distribution.
    /// `expect_index: None` → both accounts must be seen (affinity preserved).
    /// `expect_index: Some(i)` → every pick must equal `i` (override active).
    async fn assert_affinity_distribution(
        state: &AppState,
        prefix: &str,
        attempts: usize,
        expect_index: Option<usize>,
        msg: &str,
    ) {
        let mut saw_0 = false;
        let mut saw_1 = false;
        for i in 0..attempts {
            let key = format!("{}-{}", prefix, i);
            let idx = state
                .pick_account(Some(&key), "claude-opus-4-6", &[])
                .await
                .unwrap();
            match expect_index {
                Some(expected) => {
                    assert_eq!(idx, expected, "attempt {}: {}", i, msg);
                }
                None => {
                    if idx == 0 {
                        saw_0 = true;
                    } else {
                        saw_1 = true;
                    }
                    if saw_0 && saw_1 {
                        return;
                    }
                }
            }
        }
        if expect_index.is_none() {
            assert!(saw_0 && saw_1, "{}", msg);
        }
    }

    #[tokio::test]
    async fn affinity_override_balanced_no_7d_data() {
        // Scenario: balanced 5h, no 7d data → affinity preserved
        // Primary 5h=0.20, Jeff 5h=0.25, no claims_7d
        // Weights: headroom_only → 0.80 vs 0.75, ratio=0.94 > 0.5
        let state = test_state_with(vec![
            make_account("primary", "sk-ant-api-a"),
            make_account("jeff", "sk-ant-api-b"),
        ]);
        {
            let mut info = state.accounts[0].rate_info.write().await;
            info.utilization_5h = Some(0.20);
            info.reset_5h = Some(AppState::now_epoch() + 10000);
        }
        {
            let mut info = state.accounts[1].rate_info.write().await;
            info.utilization_5h = Some(0.25);
            info.reset_5h = Some(AppState::now_epoch() + 10000);
        }

        assert_affinity_distribution(
            &state,
            "balanced-client",
            500,
            None,
            "balanced accounts should see traffic on both (affinity preserved)",
        )
        .await;
    }

    #[tokio::test]
    async fn affinity_override_moderate_7d_disparity() {
        // Scenario: similar 5h, moderate 7d difference → affinity preserved
        // Primary 5h=0.15, 7d=0.40 vs Jeff 5h=0.15, 7d=0.60
        // waste_risk ratio isn't extreme enough to trigger override
        let state = test_state_with(vec![
            make_account("primary", "sk-ant-api-a"),
            make_account("jeff", "sk-ant-api-b"),
        ]);
        let now = AppState::now_epoch();
        set_account_utilization(&state, 0, 0.15, 0.40, now + 10000, now + 300000).await;
        set_account_utilization(&state, 1, 0.15, 0.60, now + 10000, now + 300000).await;

        assert_affinity_distribution(
            &state,
            "moderate-client",
            500,
            None,
            "moderate disparity should preserve affinity on both accounts",
        )
        .await;
    }

    #[tokio::test]
    async fn affinity_override_massive_7d_disparity() {
        // Scenario: egregious disparity — one account nearly spent, other fresh
        // Primary 5h=0.10, 7d=0.10 vs Jeff 5h=0.10, 7d=0.95
        // Primary's waste_risk is enormous, jeff's is near zero
        // Weight ratio far below 0.25 → all traffic overridden to primary
        let state = test_state_with(vec![
            make_account("primary", "sk-ant-api-a"),
            make_account("jeff", "sk-ant-api-b"),
        ]);
        let now = AppState::now_epoch();
        set_account_utilization(&state, 0, 0.10, 0.10, now + 10000, now + 300000).await;
        set_account_utilization(&state, 1, 0.10, 0.95, now + 10000, now + 300000).await;

        assert_affinity_distribution(
            &state,
            "production-client",
            200,
            Some(0),
            "routed to jeff despite massive 7d disparity",
        )
        .await;
    }

    #[tokio::test]
    async fn affinity_override_one_exhausted() {
        // Scenario: one account nearly spent on 7d budget
        // Primary 5h=0.10, 7d=0.10 vs Jeff 5h=0.10, 7d=0.90
        // Extreme weight ratio → all traffic to primary
        let state = test_state_with(vec![
            make_account("primary", "sk-ant-api-a"),
            make_account("jeff", "sk-ant-api-b"),
        ]);
        let now = AppState::now_epoch();
        set_account_utilization(&state, 0, 0.10, 0.10, now + 10000, now + 300000).await;
        set_account_utilization(&state, 1, 0.10, 0.90, now + 10000, now + 300000).await;

        assert_affinity_distribution(
            &state,
            "exhausted-client",
            200,
            Some(0),
            "routed to nearly-exhausted account",
        )
        .await;
    }

    #[tokio::test]
    async fn affinity_override_both_rough() {
        // Scenario: both accounts in bad shape — affinity preserved
        // Primary 5h=0.85, 7d=0.20 vs Jeff 5h=0.10, 7d=0.90
        // Primary has 5h pressure but 7d budget; Jeff has 5h headroom but 7d exhausted
        // Weights should be close enough to preserve affinity
        let state = test_state_with(vec![
            make_account("primary", "sk-ant-api-a"),
            make_account("jeff", "sk-ant-api-b"),
        ]);
        let now = AppState::now_epoch();
        set_account_utilization(&state, 0, 0.85, 0.20, now + 10000, now + 300000).await;
        set_account_utilization(&state, 1, 0.10, 0.90, now + 10000, now + 300000).await;

        assert_affinity_distribution(
            &state,
            "both-rough",
            500,
            None,
            "both-rough scenario should preserve affinity on both accounts",
        )
        .await;
    }

    #[tokio::test]
    async fn affinity_override_preserves_stickiness_with_moderate_disparity() {
        // With 3 candidates whose weights are moderately different (none below
        // 0.25 ratio to the best), the override does NOT fire — all three
        // accounts receive sticky traffic via proportional bucket hashing.
        let state = test_state_with_strategy(
            vec![
                make_account("primary", "sk-ant-api-a"),
                make_account("steve", "sk-ant-api-b"),
                make_account("jeff", "sk-ant-api-c"),
            ],
            RoutingStrategy::StickyWeightedV2,
        );
        let now = AppState::now_epoch();
        // primary is clearly best, steve middling, jeff worst
        set_account_utilization(&state, 0, 0.13, 0.41, now + 10000, now + 300000).await;
        set_account_utilization(&state, 1, 0.15, 0.55, now + 10000, now + 300000).await;
        set_account_utilization(&state, 2, 0.12, 0.79, now + 10000, now + 300000).await;

        let mut saw = [false; 3];
        for i in 0..1000 {
            let key = format!("three-way-client-{}", i);
            let idx = state
                .pick_account(Some(&key), "claude-opus-4-6", &[])
                .await
                .unwrap();
            saw[idx] = true;
            if saw[0] && saw[1] && saw[2] {
                break;
            }
        }
        assert!(
            saw[0] && saw[1] && saw[2],
            "all 3 accounts should receive traffic with moderate disparity"
        );
    }

    #[tokio::test]
    async fn affinity_override_fires_with_three_candidates_egregious_disparity() {
        // When one account is near-exhausted (85% util) and others have plenty
        // of headroom, sessions that hash into the exhausted account's tiny
        // bucket should be overridden to the best account.
        let state = test_state_with_strategy(
            vec![
                make_account("primary", "sk-ant-api-a"),
                make_account("jeff", "sk-ant-api-b"),
                make_account("insight", "sk-ant-api-c"),
            ],
            RoutingStrategy::StickyWeightedV2,
        );
        let now = AppState::now_epoch();
        set_account_utilization(&state, 0, 0.09, 0.09, now + 10000, now + 300000).await;
        set_account_utilization(&state, 1, 0.20, 0.31, now + 10000, now + 300000).await;
        set_account_utilization(&state, 2, 0.85, 0.85, now + 10000, now + 300000).await;

        // Find a key that would naturally hash into insight's bucket.
        // Candidates/weights are static, so compute boundaries once.
        let candidates = state.routing_candidates("claude-opus-4-6", &[]).await;
        let total_weight: f64 = candidates.iter().map(|c| c.weight).sum();
        let mut boundaries: Vec<(usize, f64)> = Vec::new();
        let mut cumulative = 0.0;
        for c in &candidates {
            cumulative += c.weight;
            boundaries.push((c.idx, cumulative));
        }

        let mut insight_key = None;
        for i in 0..10000 {
            let key = format!("find-insight-{}", i);
            let target = (stable_affinity_hash(&key) as f64 / u64::MAX as f64) * total_weight;
            for &(idx, boundary) in &boundaries {
                if target < boundary {
                    if idx == 2 {
                        insight_key = Some(key);
                    }
                    break;
                }
            }
            if insight_key.is_some() {
                break;
            }
        }
        let key = insight_key.expect("should find a key that hashes to insight's bucket");

        // With the override, pick_account should redirect away from insight
        let idx = state
            .pick_account(Some(&key), "claude-opus-4-6", &[])
            .await
            .unwrap();
        assert_ne!(
            idx, 2,
            "insight (85% util) should be overridden despite affinity hash landing there"
        );
        assert_eq!(idx, 0, "should override to primary (best headroom)");
    }

    #[tokio::test]
    async fn dynamic_capacity_v1_ignores_replica_local_request_history() {
        let state = test_state_with_strategy(
            vec![
                make_account("primary", "sk-ant-api-a"),
                make_account("jeff", "sk-ant-api-b"),
            ],
            RoutingStrategy::DynamicCapacityV1,
        );
        {
            let mut info = state.accounts[0].rate_info.write().await;
            info.utilization_5h = Some(0.10);
            info.reset_5h = Some(AppState::now_epoch() + 10000);
        }
        {
            let mut info = state.accounts[1].rate_info.write().await;
            info.utilization_5h = Some(0.10);
            info.reset_5h = Some(AppState::now_epoch() + 10000);
        }

        state.accounts[1].requests.store(900, Ordering::Relaxed);
        state.accounts[0].requests.store(100, Ordering::Relaxed);

        let mut primary_count = 0u32;
        let total = 200u32;
        for i in 0..total {
            let key = format!("balance-test-{}", i);
            let idx = state
                .pick_account(Some(&key), "claude-opus-4-6", &[])
                .await
                .unwrap();
            if idx == 0 {
                primary_count += 1;
            }
        }

        assert!(
            (60..=140).contains(&primary_count),
            "dynamic-capacity-v1 should ignore replica-local request skew, got {}/{} to primary",
            primary_count,
            total
        );
    }

    #[tokio::test]
    async fn sticky_weighted_v2_preserves_hash_distribution_under_skewed_history() {
        let state = test_state_with_strategy(
            vec![
                make_account("primary", "sk-ant-api-a"),
                make_account("jeff", "sk-ant-api-b"),
            ],
            RoutingStrategy::StickyWeightedV2,
        );
        {
            let mut info = state.accounts[0].rate_info.write().await;
            info.utilization_5h = Some(0.10);
            info.reset_5h = Some(AppState::now_epoch() + 10000);
        }
        {
            let mut info = state.accounts[1].rate_info.write().await;
            info.utilization_5h = Some(0.10);
            info.reset_5h = Some(AppState::now_epoch() + 10000);
        }

        state.accounts[1].requests.store(900, Ordering::Relaxed);
        state.accounts[0].requests.store(100, Ordering::Relaxed);

        let mut primary_count = 0u32;
        let total = 200u32;
        for i in 0..total {
            let key = format!("balance-test-{}", i);
            let idx = state
                .pick_account(Some(&key), "claude-opus-4-6", &[])
                .await
                .unwrap();
            if idx == 0 {
                primary_count += 1;
            }
        }

        assert!(
            (60..=140).contains(&primary_count),
            "sticky-weighted-v2 should stay near hash distribution, got {}/{} to primary",
            primary_count,
            total
        );
    }

    #[tokio::test]
    async fn pick_unsticky_on_overload() {
        // When a preferred account gets overloaded, sessions should migrate.
        // Hash-based pick assigns ~50% of sessions to each account initially.
        // After overload: primary weight=0.01, backup weight=0.5.
        // Ratio 0.01/0.5 = 0.02 < 0.25 threshold → override fires for all
        // sessions that hashed to primary.
        let state = test_state_with_strategy(
            vec![
                make_account("primary", "sk-ant-api-a"),
                make_account("backup", "sk-ant-api-b"),
            ],
            RoutingStrategy::StickyWeightedV2,
        );

        // Start with primary having lots of headroom
        {
            let mut info = state.accounts[0].rate_info.write().await;
            info.utilization = Some(0.2); // headroom = 0.8
        }
        {
            let mut info = state.accounts[1].rate_info.write().await;
            info.utilization = Some(0.5); // headroom = 0.5
        }

        // Collect keys that initially pick primary
        let mut primary_keys: Vec<String> = Vec::new();
        for i in 0..500 {
            let key = format!("test-client-{}", i);
            if state.pick_account(Some(&key), "", &[]).await.unwrap() == 0 {
                primary_keys.push(key);
            }
        }
        assert!(
            primary_keys.len() >= 50,
            "should find many keys that pick primary"
        );

        // Now overload primary: util=0.99 (headroom=0.01), backup stays at 0.5 (headroom=0.5)
        {
            let mut info = state.accounts[0].rate_info.write().await;
            info.utilization = Some(0.99);
        }

        // All sessions that hashed to primary should migrate (egregious disparity)
        let mut migrated = 0usize;
        for key in &primary_keys {
            if state.pick_account(Some(key), "", &[]).await.unwrap() == 1 {
                migrated += 1;
            }
        }

        let migration_pct = migrated as f64 / primary_keys.len() as f64;
        assert!(
            migration_pct > 0.95,
            "at least 95% of clients should migrate, got {:.1}% ({}/{})",
            migration_pct * 100.0,
            migrated,
            primary_keys.len()
        );
    }

    #[tokio::test]
    async fn affinity_sticky_near_equal_weights() {
        // Game theory: two accounts with similar utilization must produce
        // perfectly stable session routing. This is the prompt-cache scenario —
        // bouncing between accounts wastes cache-creation tokens.
        let state = test_state_with(vec![
            make_account("primary", "sk-ant-api-a"),
            make_account("jeff", "sk-ant-api-b"),
        ]);
        let now = AppState::now_epoch();
        set_account_utilization(&state, 0, 0.07, 0.73, now + 10000, now + 300000).await;
        set_account_utilization(&state, 1, 0.08, 0.71, now + 10000, now + 300000).await;

        let session = "10.42.0.1:claude:first-steps:-:9e8efc8c-2891-4206-ae10-8bcd5fa7e1f0";
        let first = state
            .pick_account(Some(session), "claude-opus-4-6", &[])
            .await
            .unwrap();

        // Same session, 100 consecutive requests: must ALWAYS pick the same account
        for i in 0..100 {
            let pick = state
                .pick_account(Some(session), "claude-opus-4-6", &[])
                .await
                .unwrap();
            assert_eq!(
                pick, first,
                "request {} routed to account {} instead of {}, session is bouncing",
                i, pick, first
            );
        }
    }

    #[tokio::test]
    async fn affinity_stable_despite_utilization_drift() {
        // Game theory: utilization changes slightly after each response,
        // but sessions must remain stable. Simulates the real scenario where
        // each request nudges the 5h utilization up by a tiny amount.
        // Uses multiple sessions and asserts low aggregate migration rate,
        // avoiding boundary-sensitivity from any single hard-coded key.
        let state = test_state_with(vec![
            make_account("primary", "sk-ant-api-a"),
            make_account("jeff", "sk-ant-api-b"),
        ]);
        let now = AppState::now_epoch();
        set_account_utilization(&state, 0, 0.05, 0.70, now + 10000, now + 300000).await;
        set_account_utilization(&state, 1, 0.05, 0.68, now + 10000, now + 300000).await;

        // Record initial picks for 100 distinct sessions
        let num_sessions = 100;
        let sessions: Vec<String> = (0..num_sessions)
            .map(|i| format!("drift-session-{}", i))
            .collect();
        let mut initial_picks = Vec::with_capacity(num_sessions);
        for s in &sessions {
            initial_picks.push(
                state
                    .pick_account(Some(s), "claude-opus-4-6", &[])
                    .await
                    .unwrap(),
            );
        }

        // Simulate 50 requests with drifting utilization
        for i in 0..50 {
            let drift = 0.002 * (i as f64);
            {
                let mut info = state.accounts[0].rate_info.write().await;
                info.utilization_5h = Some(0.05 + drift);
            }
            {
                let mut info = state.accounts[1].rate_info.write().await;
                info.utilization_5h = Some(0.05 + drift * 0.8);
            }
        }

        // After drift, check how many sessions migrated
        let mut migrated = 0usize;
        for (j, s) in sessions.iter().enumerate() {
            let pick = state
                .pick_account(Some(s), "claude-opus-4-6", &[])
                .await
                .unwrap();
            if pick != initial_picks[j] {
                migrated += 1;
            }
        }

        // Allow up to 10% migration from boundary effects — small drift shouldn't
        // cause wholesale session migration.
        assert!(
            migrated <= num_sessions / 10,
            "too many sessions migrated after util drift: {}/{} (max {})",
            migrated,
            num_sessions,
            num_sessions / 10,
        );
    }

    #[tokio::test]
    async fn affinity_different_sessions_distribute() {
        // Game theory: different sessions should naturally distribute across
        // accounts via hash, providing cross-account balance without per-session
        // instability. This is the balancing mechanism.
        let state = test_state_with(vec![
            make_account("primary", "sk-ant-api-a"),
            make_account("jeff", "sk-ant-api-b"),
        ]);
        let now = AppState::now_epoch();
        set_account_utilization(&state, 0, 0.10, 0.50, now + 10000, now + 300000).await;
        set_account_utilization(&state, 1, 0.10, 0.50, now + 10000, now + 300000).await;

        let mut picks = [0u32; 2];
        for i in 0..500 {
            let session = format!("session-{}", i);
            let idx = state
                .pick_account(Some(&session), "claude-opus-4-6", &[])
                .await
                .unwrap();
            picks[idx] += 1;
        }

        // With equal weights, hash should distribute roughly 50/50 (±15%)
        let primary_pct = picks[0] as f64 / 500.0;
        assert!(
            (0.35..=0.65).contains(&primary_pct),
            "expected ~50/50 distribution, got primary={} jeff={} ({:.0}%)",
            picks[0],
            picks[1],
            primary_pct * 100.0
        );
    }

    #[tokio::test]
    async fn affinity_breaks_on_egregious_disparity() {
        // Game theory: when one account is under extreme pressure,
        // cache locality cost is worth paying to avoid quota exhaustion.
        let state = test_state_with(vec![
            make_account("healthy", "sk-ant-api-a"),
            make_account("dying", "sk-ant-api-b"),
        ]);
        let now = AppState::now_epoch();
        // healthy: 5h=0.10, 7d=0.20 (lots of capacity)
        // dying: 5h=0.10, 7d=0.98 (nearly exhausted)
        set_account_utilization(&state, 0, 0.10, 0.20, now + 10000, now + 300000).await;
        set_account_utilization(&state, 1, 0.10, 0.98, now + 10000, now + 300000).await;

        // ALL sessions should go to healthy (dying's weight is negligible)
        let mut healthy_count = 0u32;
        for i in 0..200 {
            let session = format!("session-{}", i);
            if state
                .pick_account(Some(&session), "claude-opus-4-6", &[])
                .await
                .unwrap()
                == 0
            {
                healthy_count += 1;
            }
        }
        assert_eq!(
            healthy_count, 200,
            "all sessions should override to healthy account, got {}/200",
            healthy_count
        );
    }

    #[tokio::test]
    async fn affinity_moderate_disparity_stays_sticky() {
        // Game theory: moderate 7d difference (0.73 vs 0.41) should NOT
        // break stickiness. The cache-creation cost outweighs the routing
        // benefit at this disparity level.
        let state = test_state_with_strategy(
            vec![
                make_account("primary", "sk-ant-api-a"),
                make_account("jeff", "sk-ant-api-b"),
            ],
            RoutingStrategy::StickyWeightedV2,
        );
        let now = AppState::now_epoch();
        set_account_utilization(&state, 0, 0.13, 0.41, now + 10000, now + 300000).await;
        set_account_utilization(&state, 1, 0.12, 0.79, now + 10000, now + 300000).await;

        // Both accounts should retain their hashed sessions
        let mut primary_picks = 0u32;
        let mut jeff_picks = 0u32;
        for i in 0..500 {
            let session = format!("moderate-session-{}", i);
            match state
                .pick_account(Some(&session), "claude-opus-4-6", &[])
                .await
                .unwrap()
            {
                0 => primary_picks += 1,
                _ => jeff_picks += 1,
            }
        }
        // Both should get traffic (no one-sided override)
        assert!(
            primary_picks > 100 && jeff_picks > 100,
            "moderate disparity should preserve stickiness on both: primary={}, jeff={}",
            primary_picks,
            jeff_picks
        );
    }

    #[tokio::test]
    async fn pick_proportional_distribution() {
        // Verify distribution matches headroom ratios over many calls
        let state = test_state_with(vec![
            make_account("a", "sk-ant-api-a"),
            make_account("b", "sk-ant-api-b"),
            make_account("c", "sk-ant-api-c"),
        ]);

        // a=0.2 util (headroom 0.8), b=0.5 util (headroom 0.5), c=0.8 util (headroom 0.2)
        // Total headroom = 1.5. Expected: a=53.3%, b=33.3%, c=13.3%
        {
            let mut info = state.accounts[0].rate_info.write().await;
            info.utilization = Some(0.2);
        }
        {
            let mut info = state.accounts[1].rate_info.write().await;
            info.utilization = Some(0.5);
        }
        {
            let mut info = state.accounts[2].rate_info.write().await;
            info.utilization = Some(0.8);
        }

        let mut counts = [0u32; 3];
        let total = 10000u32;
        for _ in 0..total {
            let idx = state.pick_account(None, "", &[]).await.unwrap();
            counts[idx] += 1;
        }

        let pcts: Vec<f64> = counts.iter().map(|&c| c as f64 / total as f64).collect();
        // Expected: ~53.3%, ~33.3%, ~13.3% (±3%)
        assert!(
            (0.50..=0.57).contains(&pcts[0]),
            "account a should get ~53% traffic, got {:.1}%",
            pcts[0] * 100.0
        );
        assert!(
            (0.30..=0.37).contains(&pcts[1]),
            "account b should get ~33% traffic, got {:.1}%",
            pcts[1] * 100.0
        );
        assert!(
            (0.10..=0.17).contains(&pcts[2]),
            "account c should get ~13% traffic, got {:.1}%",
            pcts[2] * 100.0
        );
    }

    // ── Integration: HTTP handlers ──────────────────────────────────

    #[tokio::test]
    async fn proxy_rejects_missing_auth() {
        let (mock_url, _handle) = spawn_mock_upstream().await;
        let (app, _state) = test_app(&mock_url, Some("secret-key".to_string()));

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(
                listener,
                app.into_make_service_with_connect_info::<SocketAddr>(),
            )
            .await
            .unwrap();
        });

        let client = Client::new();
        let resp = client
            .post(format!("http://{}/v1/messages", addr))
            .header("content-type", "application/json")
            .body(r#"{"model":"test","max_tokens":1,"messages":[{"role":"user","content":"hi"}]}"#)
            .send()
            .await
            .unwrap();

        assert_eq!(resp.status(), reqwest::StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn proxy_accepts_valid_auth_and_forwards() {
        let (mock_url, _handle) = spawn_mock_upstream().await;
        let (app, state) = test_app(&mock_url, Some("secret-key".to_string()));

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(
                listener,
                app.into_make_service_with_connect_info::<SocketAddr>(),
            )
            .await
            .unwrap();
        });

        let client = Client::new();
        let resp = client
            .post(format!("http://{}/v1/messages", addr))
            .header("content-type", "application/json")
            .header("x-api-key", "secret-key")
            .body(r#"{"model":"test","max_tokens":1,"messages":[{"role":"user","content":"hi"}]}"#)
            .send()
            .await
            .unwrap();

        assert_eq!(resp.status(), reqwest::StatusCode::OK);

        // Verify rate info was updated from mock response headers
        let info = state.accounts[0].rate_info.read().await;
        assert_eq!(info.utilization, Some(0.25));
        assert_eq!(info.representative_claim.as_deref(), Some("five_hour"));
    }

    #[tokio::test]
    async fn proxy_preserves_raw_body_bytes_when_auto_cache_skips() {
        let seen_body = Arc::new(std::sync::Mutex::new(None::<Vec<u8>>));
        let seen_body_clone = seen_body.clone();

        let mock_app = Router::new().fallback(any(move |req: Request<Body>| {
            let seen_body = seen_body_clone.clone();
            async move {
                let body_bytes = axum::body::to_bytes(req.into_body(), MAX_REQUEST_BODY_BYTES)
                    .await
                    .unwrap();
                *seen_body.lock().unwrap() = Some(body_bytes.to_vec());

                let mut resp = axum::Json(serde_json::json!({
                    "id": "msg_test",
                    "type": "message",
                    "content": [{"type": "text", "text": "ok"}],
                }))
                .into_response();
                let headers = resp.headers_mut();
                headers.insert(
                    "anthropic-ratelimit-unified-representative-claim",
                    HeaderValue::from_static("five_hour"),
                );
                headers.insert(
                    "anthropic-ratelimit-unified-5h-utilization",
                    HeaderValue::from_static("0.25"),
                );
                let reset_epoch = (std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs()
                    + 3600)
                    .to_string();
                headers.insert(
                    "anthropic-ratelimit-unified-5h-reset",
                    HeaderValue::from_str(&reset_epoch).unwrap(),
                );
                resp
            }
        }));

        let mock_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mock_addr = mock_listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(mock_listener, mock_app).await.unwrap();
        });

        let (app, _state) = test_app(
            &format!("http://{}", mock_addr),
            Some("secret-key".to_string()),
        );

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(
                listener,
                app.into_make_service_with_connect_info::<SocketAddr>(),
            )
            .await
            .unwrap();
        });

        let raw_body = "{\n  \"model\" : \"test\",\n  \"messages\" : [{\"role\" : \"user\", \"content\" : [{\"type\":\"text\", \"text\":\"hi\", \"cache_control\":{\"type\":\"ephemeral\"}}]}],\n  \"max_tokens\" : 1\n}";

        let client = Client::new();
        let resp = client
            .post(format!("http://{}/v1/messages", addr))
            .header("content-type", "application/json")
            .header("x-api-key", "secret-key")
            .body(raw_body)
            .send()
            .await
            .unwrap();

        assert_eq!(resp.status(), reqwest::StatusCode::OK);

        let captured = seen_body
            .lock()
            .unwrap()
            .clone()
            .expect("upstream should receive request body");
        assert_eq!(
            String::from_utf8(captured).unwrap(),
            raw_body,
            "request body bytes should remain unchanged when auto-cache does not mutate payload"
        );
    }

    #[tokio::test]
    async fn proxy_preserves_key_order_when_auto_cache_mutates() {
        let seen_body = Arc::new(std::sync::Mutex::new(None::<Vec<u8>>));
        let seen_body_clone = seen_body.clone();

        let mock_app = Router::new().fallback(any(move |req: Request<Body>| {
            let seen_body = seen_body_clone.clone();
            async move {
                let body_bytes = axum::body::to_bytes(req.into_body(), MAX_REQUEST_BODY_BYTES)
                    .await
                    .unwrap();
                *seen_body.lock().unwrap() = Some(body_bytes.to_vec());

                let mut resp = axum::Json(serde_json::json!({
                    "id": "msg_test",
                    "type": "message",
                    "content": [{"type": "text", "text": "ok"}],
                }))
                .into_response();
                let headers = resp.headers_mut();
                headers.insert(
                    "anthropic-ratelimit-unified-representative-claim",
                    HeaderValue::from_static("five_hour"),
                );
                headers.insert(
                    "anthropic-ratelimit-unified-5h-utilization",
                    HeaderValue::from_static("0.25"),
                );
                let reset_epoch = (std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs()
                    + 3600)
                    .to_string();
                headers.insert(
                    "anthropic-ratelimit-unified-5h-reset",
                    HeaderValue::from_str(&reset_epoch).unwrap(),
                );
                resp
            }
        }));

        let mock_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mock_addr = mock_listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(mock_listener, mock_app).await.unwrap();
        });

        let (app, _state) = test_app(
            &format!("http://{}", mock_addr),
            Some("secret-key".to_string()),
        );

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(
                listener,
                app.into_make_service_with_connect_info::<SocketAddr>(),
            )
            .await
            .unwrap();
        });

        let input_body =
            "{\"model\":\"test\",\"messages\":[{\"role\":\"user\",\"content\":\"hi\"}],\"max_tokens\":1}";
        let expected_body = "{\"model\":\"test\",\"messages\":[{\"role\":\"user\",\"content\":[{\"type\":\"text\",\"text\":\"hi\",\"cache_control\":{\"type\":\"ephemeral\"}}]}],\"max_tokens\":1}";

        let client = Client::new();
        let resp = client
            .post(format!("http://{}/v1/messages", addr))
            .header("content-type", "application/json")
            .header("x-api-key", "secret-key")
            .body(input_body)
            .send()
            .await
            .unwrap();

        assert_eq!(resp.status(), reqwest::StatusCode::OK);

        let captured = seen_body
            .lock()
            .unwrap()
            .clone()
            .expect("upstream should receive request body");
        assert_eq!(
            String::from_utf8(captured).unwrap(),
            expected_body,
            "mutated request body should preserve caller key order while adding cache markers"
        );
    }

    #[tokio::test]
    async fn proxy_oauth_account_preserves_auto_cache_with_system_prompt() {
        // Regression test: when an OAuth account is selected and auto_cache
        // injects breakpoints, oauth_bytes must contain BOTH the OAuth system
        // prompt AND the cache_control markers. Before the fix, the fast-path
        // used raw body_bytes (dropping cache mutations).
        let seen_body = Arc::new(std::sync::Mutex::new(None::<Vec<u8>>));
        let seen_body_clone = seen_body.clone();

        let mock_app = Router::new().fallback(any(move |req: Request<Body>| {
            let seen_body = seen_body_clone.clone();
            async move {
                let body_bytes = axum::body::to_bytes(req.into_body(), MAX_REQUEST_BODY_BYTES)
                    .await
                    .unwrap();
                *seen_body.lock().unwrap() = Some(body_bytes.to_vec());

                let mut resp = axum::Json(serde_json::json!({
                    "id": "msg_test",
                    "type": "message",
                    "content": [{"type": "text", "text": "ok"}],
                }))
                .into_response();
                let headers = resp.headers_mut();
                headers.insert(
                    "anthropic-ratelimit-unified-representative-claim",
                    HeaderValue::from_static("five_hour"),
                );
                headers.insert(
                    "anthropic-ratelimit-unified-5h-utilization",
                    HeaderValue::from_static("0.10"),
                );
                let reset_epoch = (std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs()
                    + 3600)
                    .to_string();
                headers.insert(
                    "anthropic-ratelimit-unified-5h-reset",
                    HeaderValue::from_str(&reset_epoch).unwrap(),
                );
                resp
            }
        }));

        let mock_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mock_addr = mock_listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(mock_listener, mock_app).await.unwrap();
        });

        // Single OAuth account — forces all requests through the oauth_body_bytes path
        let accounts = vec![make_account("oauth-acct", "sk-ant-oat01-test-token")];
        let state = Arc::new(AppState {
            client: Client::builder()
                .timeout(Duration::from_secs(5))
                .build()
                .unwrap(),
            upstream: format!("http://{}", mock_addr),
            accounts,
            robin: AtomicUsize::new(0),
            routing_strategy: RoutingStrategy::default(),
            cooldown: Duration::from_secs(60),
            state_path: PathBuf::from("/tmp/anthropic-lb-oauth-cache-test.state.json"),
            proxy_key: None,
            allowed_ips: vec![],
            upstreams: vec![],
            client_names: HashMap::new(),
            auto_cache: true, // KEY: auto-cache enabled
            client_usage: Mutex::new(HashMap::new()),
            shadow_log_tx: None,
            shadow_log_dropped: AtomicU64::new(0),
            client_budgets: HashMap::new(),
            budget_usage: Mutex::new(HashMap::new()),
            client_utilization_limits: HashMap::new(),
            operators: vec![],
            emergency_brake: true,
            emergency_threshold: DEFAULT_EMERGENCY_THRESHOLD,
            client_request_rates: Mutex::new(HashMap::new()),
            soft_limit: 1.0,
            redis: None,
            cluster_info_cache: Mutex::new(None),
            next_req_id: AtomicU64::new(0),
            instance_id: 0,
            probe_interval_secs: 300,
        });

        let app = build_router(state);
        let addr = serve(app).await;

        // Body with NO cache_control and NO system field — auto-cache will inject
        // breakpoints, and inject_oauth_system_prompt will add the CC prompt.
        let client = Client::new();
        let resp = client
            .post(format!("http://{}/v1/messages", addr))
            .header("content-type", "application/json")
            .body(r#"{"model":"claude-sonnet-4-6","messages":[{"role":"user","content":"hi"}],"max_tokens":5}"#)
            .send()
            .await
            .unwrap();

        assert_eq!(resp.status(), reqwest::StatusCode::OK);

        let captured = seen_body
            .lock()
            .unwrap()
            .clone()
            .expect("upstream should receive request body");
        let body: serde_json::Value = serde_json::from_slice(&captured).unwrap();

        // 1. OAuth system prompt must be injected as first system block
        let system = body
            .get("system")
            .expect("system field must be present (injected by OAuth path)");
        let arr = system.as_array().expect("system should be array");
        assert_eq!(
            arr[0]["text"].as_str().unwrap(),
            OAUTH_SYSTEM_PROMPT,
            "first system block must be CC prompt"
        );

        // 2. Auto-cache breakpoints must be present (not dropped by fast-path)
        let messages = body["messages"].as_array().unwrap();
        let last_user = messages
            .iter()
            .rev()
            .find(|m| m["role"] == "user")
            .expect("should have user message");
        let content = last_user["content"].as_array().unwrap();
        assert!(
            content.last().unwrap().get("cache_control").is_some(),
            "auto-cache breakpoint on last user message must survive OAuth path"
        );
    }

    #[tokio::test]
    async fn stats_endpoint_returns_account_info() {
        let (mock_url, _handle) = spawn_mock_upstream().await;
        let (app, _state) = test_app(&mock_url, None);

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(
                listener,
                app.into_make_service_with_connect_info::<SocketAddr>(),
            )
            .await
            .unwrap();
        });

        let client = Client::new();
        let resp = client
            .get(format!("http://{}/_stats", addr))
            .send()
            .await
            .unwrap();

        assert_eq!(resp.status(), reqwest::StatusCode::OK);
        let body: serde_json::Value = resp.json().await.unwrap();
        let accounts = body["accounts"].as_array().unwrap();
        assert_eq!(accounts.len(), 2);
        assert_eq!(accounts[0]["name"], "acct-a");
        assert_eq!(accounts[1]["name"], "acct-b");
        assert_eq!(body["strategy"], "dynamic-capacity-v1");
        // Upstreams section should be present
        let upstreams = body["upstreams"].as_array().unwrap();
        assert_eq!(upstreams.len(), 1);
        assert_eq!(upstreams[0]["name"], "mock");
    }

    #[tokio::test]
    async fn upstream_handler_forwards_to_named_upstream() {
        let (mock_url, _handle) = spawn_mock_upstream().await;
        let (app, _state) = test_app(&mock_url, None);

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(
                listener,
                app.into_make_service_with_connect_info::<SocketAddr>(),
            )
            .await
            .unwrap();
        });

        let client = Client::new();
        let resp = client
            .post(format!("http://{}/upstream/mock/v1/chat/completions", addr))
            .header("content-type", "application/json")
            .body(r#"{"model":"gpt-4","messages":[{"role":"user","content":"hi"}]}"#)
            .send()
            .await
            .unwrap();

        assert_eq!(resp.status(), reqwest::StatusCode::OK);
    }

    #[tokio::test]
    async fn upstream_handler_rejects_unknown_upstream() {
        let (mock_url, _handle) = spawn_mock_upstream().await;
        let (app, _state) = test_app(&mock_url, None);

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(
                listener,
                app.into_make_service_with_connect_info::<SocketAddr>(),
            )
            .await
            .unwrap();
        });

        let client = Client::new();
        let resp = client
            .post(format!(
                "http://{}/upstream/nonexistent/v1/chat/completions",
                addr
            ))
            .header("content-type", "application/json")
            .body("{}")
            .send()
            .await
            .unwrap();

        assert_eq!(resp.status(), reqwest::StatusCode::NOT_FOUND);
    }

    // ── Unit: OpenAI request translation ────────────────────────────

    #[test]
    fn translate_request_extracts_system() {
        let req = serde_json::json!({
            "model": "claude-sonnet-4-6",
            "messages": [
                {"role": "system", "content": "You are helpful"},
                {"role": "user", "content": "Hello"}
            ],
            "max_tokens": 1024
        });
        let result = translate_openai_to_anthropic(&req);
        assert_eq!(result["system"], "You are helpful");
        let msgs = result["messages"].as_array().unwrap();
        assert_eq!(msgs.len(), 1);
        assert_eq!(msgs[0]["role"], "user");
        assert_eq!(msgs[0]["content"], "Hello");
    }

    #[test]
    fn translate_request_multi_system_concat() {
        let req = serde_json::json!({
            "model": "claude-sonnet-4-6",
            "messages": [
                {"role": "system", "content": "Rule 1"},
                {"role": "system", "content": "Rule 2"},
                {"role": "user", "content": "Hello"}
            ],
            "max_tokens": 100
        });
        let result = translate_openai_to_anthropic(&req);
        assert_eq!(result["system"], "Rule 1\n\nRule 2");
    }

    #[test]
    fn translate_request_no_system() {
        let req = serde_json::json!({
            "model": "claude-sonnet-4-6",
            "messages": [
                {"role": "user", "content": "Hello"}
            ],
            "max_tokens": 100
        });
        let result = translate_openai_to_anthropic(&req);
        assert!(result.get("system").is_none());
    }

    #[test]
    fn translate_request_default_max_tokens() {
        let req = serde_json::json!({
            "model": "claude-sonnet-4-6",
            "messages": [{"role": "user", "content": "Hello"}]
        });
        let result = translate_openai_to_anthropic(&req);
        assert_eq!(result["max_tokens"], 4096);
    }

    #[test]
    fn translate_request_stop_to_stop_sequences() {
        let req = serde_json::json!({
            "model": "claude-sonnet-4-6",
            "messages": [{"role": "user", "content": "Hello"}],
            "max_tokens": 100,
            "stop": ["END", "STOP"]
        });
        let result = translate_openai_to_anthropic(&req);
        let seqs = result["stop_sequences"].as_array().unwrap();
        assert_eq!(seqs.len(), 2);
        assert_eq!(seqs[0], "END");
        assert_eq!(seqs[1], "STOP");
    }

    #[test]
    fn translate_request_passthrough_params() {
        let req = serde_json::json!({
            "model": "claude-sonnet-4-6",
            "messages": [{"role": "user", "content": "Hello"}],
            "max_tokens": 512,
            "temperature": 0.7,
            "top_p": 0.9,
            "stream": true
        });
        let result = translate_openai_to_anthropic(&req);
        assert_eq!(result["model"], "claude-sonnet-4-6");
        assert_eq!(result["max_tokens"], 512);
        assert_eq!(result["temperature"], 0.7);
        assert_eq!(result["top_p"], 0.9);
        assert_eq!(result["stream"], true);
    }

    #[test]
    fn translate_request_strips_name_field() {
        let req = serde_json::json!({
            "model": "claude-sonnet-4-6",
            "messages": [
                {"role": "user", "content": "Hello", "name": "bob"}
            ],
            "max_tokens": 100
        });
        let result = translate_openai_to_anthropic(&req);
        let msgs = result["messages"].as_array().unwrap();
        assert!(msgs[0].get("name").is_none());
    }

    #[test]
    fn translate_request_tools() {
        let req = serde_json::json!({
            "model": "claude-sonnet-4-6",
            "messages": [{"role": "user", "content": "What's the weather?"}],
            "max_tokens": 100,
            "tools": [{
                "type": "function",
                "function": {
                    "name": "get_weather",
                    "description": "Get the weather",
                    "parameters": {
                        "type": "object",
                        "properties": {"location": {"type": "string"}},
                        "required": ["location"]
                    }
                }
            }]
        });
        let result = translate_openai_to_anthropic(&req);
        let tools = result["tools"].as_array().unwrap();
        assert_eq!(tools.len(), 1);
        assert_eq!(tools[0]["name"], "get_weather");
        assert_eq!(tools[0]["description"], "Get the weather");
        assert_eq!(tools[0]["input_schema"]["type"], "object");
        assert!(tools[0].get("type").is_none()); // no OpenAI "type":"function" wrapper
    }

    #[test]
    fn translate_request_tool_choice_variants() {
        // auto
        let req = serde_json::json!({
            "model": "claude-sonnet-4-6",
            "messages": [{"role": "user", "content": "Hi"}],
            "tool_choice": "auto"
        });
        let result = translate_openai_to_anthropic(&req);
        assert_eq!(result["tool_choice"]["type"], "auto");

        // required → any
        let req = serde_json::json!({
            "model": "claude-sonnet-4-6",
            "messages": [{"role": "user", "content": "Hi"}],
            "tool_choice": "required"
        });
        let result = translate_openai_to_anthropic(&req);
        assert_eq!(result["tool_choice"]["type"], "any");

        // specific function
        let req = serde_json::json!({
            "model": "claude-sonnet-4-6",
            "messages": [{"role": "user", "content": "Hi"}],
            "tool_choice": {"type": "function", "function": {"name": "search"}}
        });
        let result = translate_openai_to_anthropic(&req);
        assert_eq!(result["tool_choice"]["type"], "tool");
        assert_eq!(result["tool_choice"]["name"], "search");

        // none → omitted
        let req = serde_json::json!({
            "model": "claude-sonnet-4-6",
            "messages": [{"role": "user", "content": "Hi"}],
            "tool_choice": "none"
        });
        let result = translate_openai_to_anthropic(&req);
        assert!(result.get("tool_choice").is_none());
    }

    #[test]
    fn translate_request_tool_result_message() {
        let req = serde_json::json!({
            "model": "claude-sonnet-4-6",
            "messages": [
                {"role": "user", "content": "What's the weather?"},
                {"role": "assistant", "content": null, "tool_calls": [{
                    "id": "call_123",
                    "type": "function",
                    "function": {"name": "get_weather", "arguments": "{\"location\":\"SF\"}"}
                }]},
                {"role": "tool", "tool_call_id": "call_123", "content": "72°F and sunny"}
            ],
            "max_tokens": 100
        });
        let result = translate_openai_to_anthropic(&req);
        let msgs = result["messages"].as_array().unwrap();
        // First message: user text
        assert_eq!(msgs[0]["role"], "user");
        assert_eq!(msgs[0]["content"], "What's the weather?");
        // Second message: assistant with tool_use block
        assert_eq!(msgs[1]["role"], "assistant");
        let blocks = msgs[1]["content"].as_array().unwrap();
        assert_eq!(blocks[0]["type"], "tool_use");
        assert_eq!(blocks[0]["id"], "call_123");
        assert_eq!(blocks[0]["name"], "get_weather");
        assert_eq!(blocks[0]["input"]["location"], "SF");
        // Third message: tool result → user with tool_result
        assert_eq!(msgs[2]["role"], "user");
        let result_blocks = msgs[2]["content"].as_array().unwrap();
        assert_eq!(result_blocks[0]["type"], "tool_result");
        assert_eq!(result_blocks[0]["tool_use_id"], "call_123");
        assert_eq!(result_blocks[0]["content"], "72°F and sunny");
    }

    #[test]
    fn translate_request_assistant_tool_calls_with_text() {
        let req = serde_json::json!({
            "model": "claude-sonnet-4-6",
            "messages": [
                {"role": "assistant", "content": "Let me check.", "tool_calls": [{
                    "id": "tc_1",
                    "type": "function",
                    "function": {"name": "search", "arguments": "{\"q\":\"rust\"}"}
                }]}
            ],
            "max_tokens": 100
        });
        let result = translate_openai_to_anthropic(&req);
        let msgs = result["messages"].as_array().unwrap();
        let blocks = msgs[0]["content"].as_array().unwrap();
        assert_eq!(blocks.len(), 2);
        assert_eq!(blocks[0]["type"], "text");
        assert_eq!(blocks[0]["text"], "Let me check.");
        assert_eq!(blocks[1]["type"], "tool_use");
        assert_eq!(blocks[1]["name"], "search");
    }

    #[test]
    fn translate_request_consecutive_tool_results_merged() {
        // Parallel tool calls produce consecutive role:"tool" messages.
        // Anthropic rejects consecutive same-role messages, so they must merge.
        let req = serde_json::json!({
            "model": "claude-sonnet-4-6",
            "messages": [
                {"role": "user", "content": "Weather in SF and NYC?"},
                {"role": "assistant", "content": null, "tool_calls": [
                    {"id": "c1", "type": "function", "function": {"name": "weather", "arguments": "{\"city\":\"SF\"}"}},
                    {"id": "c2", "type": "function", "function": {"name": "weather", "arguments": "{\"city\":\"NYC\"}"}}
                ]},
                {"role": "tool", "tool_call_id": "c1", "content": "72F"},
                {"role": "tool", "tool_call_id": "c2", "content": "45F"}
            ],
            "max_tokens": 100
        });
        let result = translate_openai_to_anthropic(&req);
        let msgs = result["messages"].as_array().unwrap();
        // Should be 3 messages: user, assistant, user(merged tool results)
        assert_eq!(msgs.len(), 3);
        assert_eq!(msgs[2]["role"], "user");
        let blocks = msgs[2]["content"].as_array().unwrap();
        assert_eq!(blocks.len(), 2);
        assert_eq!(blocks[0]["tool_use_id"], "c1");
        assert_eq!(blocks[0]["content"], "72F");
        assert_eq!(blocks[1]["tool_use_id"], "c2");
        assert_eq!(blocks[1]["content"], "45F");
    }

    #[test]
    fn translate_request_tool_result_does_not_merge_into_regular_user_msg() {
        // A user message with array-form content should NOT have tool_results
        // appended to it — that would corrupt the original message.
        let req = serde_json::json!({
            "model": "claude-sonnet-4-6",
            "messages": [
                {"role": "user", "content": [{"type": "text", "text": "Hello"}]},
                {"role": "assistant", "content": null, "tool_calls": [{
                    "id": "c1", "type": "function",
                    "function": {"name": "test", "arguments": "{}"}
                }]},
                {"role": "tool", "tool_call_id": "c1", "content": "done"}
            ],
            "max_tokens": 100
        });
        let result = translate_openai_to_anthropic(&req);
        let msgs = result["messages"].as_array().unwrap();
        // user, assistant, user(tool_result) — three separate messages
        assert_eq!(msgs.len(), 3);
        // First user message should be untouched
        let first_user = msgs[0]["content"].as_array().unwrap();
        assert_eq!(first_user.len(), 1);
        assert_eq!(first_user[0]["type"], "text");
        // Third message is the tool_result
        let tool_msg = msgs[2]["content"].as_array().unwrap();
        assert_eq!(tool_msg[0]["type"], "tool_result");
    }

    #[test]
    fn translate_request_tool_choice_none_removes_tools() {
        let req = serde_json::json!({
            "model": "claude-sonnet-4-6",
            "messages": [{"role": "user", "content": "Hi"}],
            "tools": [{"type": "function", "function": {"name": "search", "description": "Search", "parameters": {"type": "object"}}}],
            "tool_choice": "none"
        });
        let result = translate_openai_to_anthropic(&req);
        assert!(result.get("tools").is_none());
        assert!(result.get("tool_choice").is_none());
    }

    #[test]
    fn translate_request_malformed_arguments_json() {
        let req = serde_json::json!({
            "model": "claude-sonnet-4-6",
            "messages": [
                {"role": "assistant", "content": null, "tool_calls": [{
                    "id": "tc_bad",
                    "type": "function",
                    "function": {"name": "test", "arguments": "not valid json"}
                }]}
            ],
            "max_tokens": 100
        });
        let result = translate_openai_to_anthropic(&req);
        let msgs = result["messages"].as_array().unwrap();
        let blocks = msgs[0]["content"].as_array().unwrap();
        // Should fall back to empty object, not panic
        assert_eq!(blocks[0]["input"], serde_json::json!({}));
    }

    #[test]
    fn translate_request_tool_no_parameters_gets_empty_schema() {
        let req = serde_json::json!({
            "model": "claude-sonnet-4-6",
            "messages": [{"role": "user", "content": "Hi"}],
            "max_tokens": 100,
            "tools": [{"type": "function", "function": {"name": "get_time", "description": "Get current time"}}]
        });
        let result = translate_openai_to_anthropic(&req);
        let tools = result["tools"].as_array().unwrap();
        assert_eq!(tools[0]["name"], "get_time");
        // input_schema must always be present for Anthropic API
        assert_eq!(
            tools[0]["input_schema"],
            serde_json::json!({"type": "object", "properties": {}})
        );
    }

    #[test]
    fn translate_request_tool_null_parameters_gets_empty_schema() {
        let req = serde_json::json!({
            "model": "claude-sonnet-4-6",
            "messages": [{"role": "user", "content": "Hi"}],
            "max_tokens": 100,
            "tools": [{"type": "function", "function": {"name": "ping", "description": "Ping", "parameters": null}}]
        });
        let result = translate_openai_to_anthropic(&req);
        let tools = result["tools"].as_array().unwrap();
        assert_eq!(
            tools[0]["input_schema"],
            serde_json::json!({"type": "object", "properties": {}})
        );
    }

    #[test]
    fn translate_request_tool_result_array_content() {
        let req = serde_json::json!({
            "model": "claude-sonnet-4-6",
            "messages": [
                {"role": "tool", "tool_call_id": "c1", "content": [
                    {"type": "text", "text": "Result A"},
                    {"type": "text", "text": " Result B"}
                ]}
            ],
            "max_tokens": 100
        });
        let result = translate_openai_to_anthropic(&req);
        let msgs = result["messages"].as_array().unwrap();
        let blocks = msgs[0]["content"].as_array().unwrap();
        assert_eq!(blocks[0]["content"], "Result A Result B");
    }

    #[test]
    fn translate_request_assistant_array_content_with_tool_calls() {
        let req = serde_json::json!({
            "model": "claude-sonnet-4-6",
            "messages": [
                {"role": "assistant", "content": [{"type": "text", "text": "Thinking..."}], "tool_calls": [{
                    "id": "tc_1",
                    "type": "function",
                    "function": {"name": "search", "arguments": "{}"}
                }]}
            ],
            "max_tokens": 100
        });
        let result = translate_openai_to_anthropic(&req);
        let msgs = result["messages"].as_array().unwrap();
        let blocks = msgs[0]["content"].as_array().unwrap();
        assert_eq!(blocks[0]["type"], "text");
        assert_eq!(blocks[0]["text"], "Thinking...");
        assert_eq!(blocks[1]["type"], "tool_use");
    }

    // ── Unit: OpenAI response translation ───────────────────────────

    #[test]
    fn translate_response_basic() {
        let resp = serde_json::json!({
            "id": "msg_abc123",
            "type": "message",
            "content": [{"type": "text", "text": "Hello!"}],
            "model": "claude-sonnet-4-6",
            "stop_reason": "end_turn",
            "usage": {"input_tokens": 10, "output_tokens": 5}
        });
        let result = translate_anthropic_to_openai(&resp);
        assert_eq!(result["id"], "chatcmpl-msg_abc123");
        assert_eq!(result["object"], "chat.completion");
        assert_eq!(result["choices"][0]["message"]["role"], "assistant");
        assert_eq!(result["choices"][0]["message"]["content"], "Hello!");
        assert_eq!(result["choices"][0]["finish_reason"], "stop");
    }

    #[test]
    fn translate_response_usage_mapping() {
        let resp = serde_json::json!({
            "id": "msg_x",
            "content": [{"type": "text", "text": "ok"}],
            "model": "claude-sonnet-4-6",
            "stop_reason": "end_turn",
            "usage": {"input_tokens": 25, "output_tokens": 15}
        });
        let result = translate_anthropic_to_openai(&resp);
        assert_eq!(result["usage"]["prompt_tokens"], 25);
        assert_eq!(result["usage"]["completion_tokens"], 15);
        assert_eq!(result["usage"]["total_tokens"], 40);
    }

    #[test]
    fn translate_response_stop_reason_mapping() {
        assert_eq!(map_stop_reason("end_turn"), "stop");
        assert_eq!(map_stop_reason("max_tokens"), "length");
        assert_eq!(map_stop_reason("stop_sequence"), "stop");
        assert_eq!(map_stop_reason("unknown"), "stop");
    }

    // ── Unit: JSON fence stripping ──────────────────────────────────

    #[test]
    fn strip_json_fences_with_lang_tag() {
        let input = "```json\n{\"key\": \"value\"}\n```";
        assert_eq!(strip_json_fences(input), r#"{"key": "value"}"#);
    }

    #[test]
    fn strip_json_fences_no_lang_tag() {
        let input = "```\n{\"key\": \"value\"}\n```";
        assert_eq!(strip_json_fences(input), r#"{"key": "value"}"#);
    }

    #[test]
    fn strip_json_fences_passthrough_plain_json() {
        let input = r#"{"key": "value"}"#;
        assert_eq!(strip_json_fences(input), input);
    }

    #[test]
    fn strip_json_fences_with_whitespace() {
        let input = "  ```json\n{\"a\": 1}\n```  ";
        assert_eq!(strip_json_fences(input), r#"{"a": 1}"#);
    }

    #[test]
    fn translate_response_strips_markdown_fences() {
        let resp = serde_json::json!({
            "id": "msg_fenced",
            "content": [{"type": "text", "text": "```json\n{\"skipSearch\": true}\n```"}],
            "model": "claude-sonnet-4-6",
            "stop_reason": "end_turn",
            "usage": {"input_tokens": 10, "output_tokens": 5}
        });
        let result = translate_anthropic_to_openai(&resp);
        assert_eq!(
            result["choices"][0]["message"]["content"],
            r#"{"skipSearch": true}"#
        );
    }

    #[test]
    fn translate_response_tool_use_blocks() {
        let resp = serde_json::json!({
            "id": "msg_tool",
            "type": "message",
            "content": [
                {"type": "tool_use", "id": "toolu_123", "name": "get_weather", "input": {"location": "SF"}}
            ],
            "model": "claude-sonnet-4-6",
            "stop_reason": "tool_use",
            "usage": {"input_tokens": 20, "output_tokens": 15}
        });
        let result = translate_anthropic_to_openai(&resp);
        assert_eq!(result["choices"][0]["finish_reason"], "tool_calls");
        assert!(result["choices"][0]["message"]["content"].is_null());
        let tcs = result["choices"][0]["message"]["tool_calls"]
            .as_array()
            .unwrap();
        assert_eq!(tcs.len(), 1);
        assert_eq!(tcs[0]["id"], "toolu_123");
        assert_eq!(tcs[0]["type"], "function");
        assert_eq!(tcs[0]["function"]["name"], "get_weather");
        let args: serde_json::Value =
            serde_json::from_str(tcs[0]["function"]["arguments"].as_str().unwrap()).unwrap();
        assert_eq!(args["location"], "SF");
    }

    #[test]
    fn translate_response_mixed_text_and_tool_use() {
        let resp = serde_json::json!({
            "id": "msg_mixed",
            "type": "message",
            "content": [
                {"type": "text", "text": "Let me check."},
                {"type": "tool_use", "id": "toolu_456", "name": "search", "input": {"q": "rust"}}
            ],
            "model": "claude-sonnet-4-6",
            "stop_reason": "tool_use",
            "usage": {"input_tokens": 10, "output_tokens": 10}
        });
        let result = translate_anthropic_to_openai(&resp);
        assert_eq!(result["choices"][0]["message"]["content"], "Let me check.");
        let tcs = result["choices"][0]["message"]["tool_calls"]
            .as_array()
            .unwrap();
        assert_eq!(tcs.len(), 1);
        assert_eq!(tcs[0]["function"]["name"], "search");
    }

    // ── Unit: SSE event translation ─────────────────────────────────

    #[test]
    fn translate_sse_message_start() {
        let mut ctx = StreamContext::default();
        let raw = "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_test\",\"model\":\"claude-sonnet-4-6\",\"role\":\"assistant\"}}";
        let result = translate_sse_event(raw, &mut ctx).unwrap();
        assert!(result.starts_with("data: "));
        assert_eq!(ctx.id, "chatcmpl-msg_test");
        assert_eq!(ctx.model, "claude-sonnet-4-6");
        let chunk: serde_json::Value =
            serde_json::from_str(result.strip_prefix("data: ").unwrap().trim()).unwrap();
        assert_eq!(chunk["choices"][0]["delta"]["role"], "assistant");
    }

    #[test]
    fn translate_sse_content_delta() {
        let mut ctx = StreamContext {
            id: "chatcmpl-test".to_string(),
            model: "claude-sonnet-4-6".to_string(),
            ..Default::default()
        };
        let raw = "event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"Hello world\"}}";
        let result = translate_sse_event(raw, &mut ctx).unwrap();
        let chunk: serde_json::Value =
            serde_json::from_str(result.strip_prefix("data: ").unwrap().trim()).unwrap();
        assert_eq!(chunk["choices"][0]["delta"]["content"], "Hello world");
        assert!(chunk["choices"][0]["finish_reason"].is_null());
    }

    #[test]
    fn translate_sse_message_delta() {
        let mut ctx = StreamContext {
            id: "chatcmpl-test".to_string(),
            ..Default::default()
        };
        let raw = "event: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":5}}";
        let result = translate_sse_event(raw, &mut ctx).unwrap();
        let chunk: serde_json::Value =
            serde_json::from_str(result.strip_prefix("data: ").unwrap().trim()).unwrap();
        assert_eq!(chunk["choices"][0]["finish_reason"], "stop");
    }

    #[test]
    fn translate_sse_message_stop() {
        let mut ctx = StreamContext::default();
        let raw = "event: message_stop\ndata: {\"type\":\"message_stop\"}";
        let result = translate_sse_event(raw, &mut ctx).unwrap();
        assert_eq!(result, "data: [DONE]\n\n");
    }

    #[test]
    fn translate_sse_skips_ping() {
        let mut ctx = StreamContext::default();
        let raw = "event: ping\ndata: {\"type\":\"ping\"}";
        assert!(translate_sse_event(raw, &mut ctx).is_none());
    }

    #[test]
    fn translate_sse_tool_use_content_block_start() {
        let mut ctx = StreamContext::default();
        let raw = "event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"tool_use\",\"id\":\"toolu_abc\",\"name\":\"get_weather\",\"input\":{}}}";
        let result = translate_sse_event(raw, &mut ctx).unwrap();
        let chunk: serde_json::Value =
            serde_json::from_str(result.strip_prefix("data: ").unwrap().trim()).unwrap();
        let tc = &chunk["choices"][0]["delta"]["tool_calls"][0];
        assert_eq!(tc["index"], 0);
        assert_eq!(tc["id"], "toolu_abc");
        assert_eq!(tc["function"]["name"], "get_weather");
        assert_eq!(tc["function"]["arguments"], "");
        assert!(ctx.in_tool_use);
        assert_eq!(ctx.tool_call_index, 0);
    }

    #[test]
    fn translate_sse_tool_use_input_json_delta() {
        let mut ctx = StreamContext {
            in_tool_use: true,
            tool_call_index: 0,
            ..Default::default()
        };
        let raw = "event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"input_json_delta\",\"partial_json\":\"{\\\"loc\"}}";
        let result = translate_sse_event(raw, &mut ctx).unwrap();
        let chunk: serde_json::Value =
            serde_json::from_str(result.strip_prefix("data: ").unwrap().trim()).unwrap();
        let tc = &chunk["choices"][0]["delta"]["tool_calls"][0];
        assert_eq!(tc["index"], 0);
        assert_eq!(tc["function"]["arguments"], "{\"loc");
    }

    #[test]
    fn translate_sse_content_block_stop_resets_tool_state() {
        let mut ctx = StreamContext {
            in_tool_use: true,
            tool_call_index: 0,
            ..Default::default()
        };
        let raw = "event: content_block_stop\ndata: {\"type\":\"content_block_stop\",\"index\":0}";
        let result = translate_sse_event(raw, &mut ctx);
        assert!(result.is_none());
        assert!(!ctx.in_tool_use);
    }

    #[test]
    fn translate_sse_tool_use_stop_reason() {
        let mut ctx = StreamContext::default();
        let raw = "event: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"tool_use\"},\"usage\":{\"output_tokens\":10}}";
        let result = translate_sse_event(raw, &mut ctx).unwrap();
        let chunk: serde_json::Value =
            serde_json::from_str(result.strip_prefix("data: ").unwrap().trim()).unwrap();
        assert_eq!(chunk["choices"][0]["finish_reason"], "tool_calls");
    }

    #[test]
    fn translate_sse_text_block_start_skipped() {
        let mut ctx = StreamContext::default();
        let raw = "event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}";
        let result = translate_sse_event(raw, &mut ctx);
        assert!(result.is_none());
        assert!(!ctx.in_tool_use);
    }

    #[test]
    fn translate_sse_multiple_tool_calls() {
        let mut ctx = StreamContext::default();

        // First tool_use block
        let raw1 = "event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"tool_use\",\"id\":\"toolu_1\",\"name\":\"search\",\"input\":{}}}";
        translate_sse_event(raw1, &mut ctx).unwrap();
        assert_eq!(ctx.tool_call_index, 0);

        // Close first
        let stop1 =
            "event: content_block_stop\ndata: {\"type\":\"content_block_stop\",\"index\":0}";
        translate_sse_event(stop1, &mut ctx);

        // Second tool_use block
        let raw2 = "event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":1,\"content_block\":{\"type\":\"tool_use\",\"id\":\"toolu_2\",\"name\":\"fetch\",\"input\":{}}}";
        let result = translate_sse_event(raw2, &mut ctx).unwrap();
        assert_eq!(ctx.tool_call_index, 1);
        let chunk: serde_json::Value =
            serde_json::from_str(result.strip_prefix("data: ").unwrap().trim()).unwrap();
        assert_eq!(chunk["choices"][0]["delta"]["tool_calls"][0]["index"], 1);
        assert_eq!(
            chunk["choices"][0]["delta"]["tool_calls"][0]["id"],
            "toolu_2"
        );
    }

    // ── Integration: OpenAI-compat handler ──────────────────────────

    /// Mock that returns Anthropic /v1/messages format (non-streaming)
    async fn mock_anthropic_handler(req: Request<Body>) -> Response {
        let has_auth =
            req.headers().contains_key("x-api-key") || req.headers().contains_key("authorization");
        if !has_auth {
            return (StatusCode::UNAUTHORIZED, "missing auth").into_response();
        }

        let mut resp = axum::Json(serde_json::json!({
            "id": "msg_integration",
            "type": "message",
            "content": [{"type": "text", "text": "Hello from Claude"}],
            "model": "claude-sonnet-4-6",
            "stop_reason": "end_turn",
            "usage": {"input_tokens": 10, "output_tokens": 5}
        }))
        .into_response();

        let headers = resp.headers_mut();
        headers.insert(
            "anthropic-ratelimit-unified-representative-claim",
            HeaderValue::from_static("five_hour"),
        );
        headers.insert(
            "anthropic-ratelimit-unified-5h-utilization",
            HeaderValue::from_static("0.30"),
        );
        resp
    }

    /// Mock that returns Anthropic SSE streaming format
    async fn mock_anthropic_streaming_handler(req: Request<Body>) -> Response {
        let has_auth =
            req.headers().contains_key("x-api-key") || req.headers().contains_key("authorization");
        if !has_auth {
            return (StatusCode::UNAUTHORIZED, "missing auth").into_response();
        }

        let events = [
            "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_stream\",\"type\":\"message\",\"role\":\"assistant\",\"model\":\"claude-sonnet-4-6\",\"content\":[],\"stop_reason\":null,\"usage\":{\"input_tokens\":10,\"output_tokens\":0}}}\n\n",
            "event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\n",
            "event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"Hello\"}}\n\n",
            "event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\" world\"}}\n\n",
            "event: content_block_stop\ndata: {\"type\":\"content_block_stop\",\"index\":0}\n\n",
            "event: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":5}}\n\n",
            "event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n",
        ];

        let body = events.join("");
        Response::builder()
            .status(StatusCode::OK)
            .header("content-type", "text/event-stream")
            .body(Body::from(body))
            .unwrap()
    }

    /// Build test app with separate handlers for streaming vs non-streaming
    fn test_openai_app(upstream_url: &str, proxy_key: Option<String>) -> (Router, Arc<AppState>) {
        let accounts = vec![
            make_account("acct-a", "sk-ant-api-test-aaa"),
            make_account("acct-b", "sk-ant-api-test-bbb"),
        ];

        let state = Arc::new(AppState {
            client: Client::builder()
                .timeout(Duration::from_secs(5))
                .build()
                .unwrap(),
            upstream: upstream_url.to_string(),
            accounts,
            robin: AtomicUsize::new(0),
            routing_strategy: RoutingStrategy::default(),
            cooldown: Duration::from_secs(60),
            state_path: PathBuf::from("/tmp/anthropic-lb-openai-test.state.json"),
            proxy_key,
            allowed_ips: vec![],
            upstreams: vec![],
            client_names: HashMap::new(),
            auto_cache: true,
            client_usage: Mutex::new(HashMap::new()),
            shadow_log_tx: None,
            shadow_log_dropped: AtomicU64::new(0),
            client_budgets: HashMap::new(),
            budget_usage: Mutex::new(HashMap::new()),
            client_utilization_limits: HashMap::new(),
            operators: vec![],
            emergency_brake: true,
            emergency_threshold: DEFAULT_EMERGENCY_THRESHOLD,
            client_request_rates: Mutex::new(HashMap::new()),
            soft_limit: 1.0,
            redis: None,
            cluster_info_cache: Mutex::new(None),
            next_req_id: AtomicU64::new(0),
            instance_id: 0,
            probe_interval_secs: 300,
        });

        let app = Router::new()
            .route(
                "/v1/chat/completions",
                axum::routing::post(openai_chat_handler),
            )
            .with_state(state.clone());

        (app, state)
    }

    fn test_app(upstream_url: &str, proxy_key: Option<String>) -> (Router, Arc<AppState>) {
        test_app_with_strategy(upstream_url, proxy_key, RoutingStrategy::default())
    }

    #[tokio::test]
    async fn openai_chat_non_streaming() {
        // Spawn a mock that serves /v1/messages with Anthropic format
        let mock_app = Router::new().fallback(any(mock_anthropic_handler));
        let mock_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mock_addr = mock_listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(mock_listener, mock_app).await.unwrap();
        });

        let mock_url = format!("http://{}", mock_addr);
        let (app, _state) = test_openai_app(&mock_url, None);

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(
                listener,
                app.into_make_service_with_connect_info::<SocketAddr>(),
            )
            .await
            .unwrap();
        });

        let client = Client::new();
        let resp = client
            .post(format!("http://{}/v1/chat/completions", addr))
            .header("content-type", "application/json")
            .body(r#"{"model":"claude-sonnet-4-6","messages":[{"role":"user","content":"Hello"}],"max_tokens":100}"#)
            .send()
            .await
            .unwrap();

        assert_eq!(resp.status(), reqwest::StatusCode::OK);
        let body: serde_json::Value = resp.json().await.unwrap();
        assert_eq!(body["object"], "chat.completion");
        assert!(body["id"].as_str().unwrap().starts_with("chatcmpl-"));
        assert_eq!(
            body["choices"][0]["message"]["content"],
            "Hello from Claude"
        );
        assert_eq!(body["choices"][0]["finish_reason"], "stop");
        assert_eq!(body["usage"]["prompt_tokens"], 10);
        assert_eq!(body["usage"]["completion_tokens"], 5);
        assert_eq!(body["usage"]["total_tokens"], 15);
    }

    #[tokio::test]
    async fn openai_chat_streaming() {
        let mock_app = Router::new().fallback(any(mock_anthropic_streaming_handler));
        let mock_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mock_addr = mock_listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(mock_listener, mock_app).await.unwrap();
        });

        let mock_url = format!("http://{}", mock_addr);
        let (app, _state) = test_openai_app(&mock_url, None);

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(
                listener,
                app.into_make_service_with_connect_info::<SocketAddr>(),
            )
            .await
            .unwrap();
        });

        let client = Client::new();
        let resp = client
            .post(format!("http://{}/v1/chat/completions", addr))
            .header("content-type", "application/json")
            .body(r#"{"model":"claude-sonnet-4-6","messages":[{"role":"user","content":"Hello"}],"max_tokens":100,"stream":true}"#)
            .send()
            .await
            .unwrap();

        assert_eq!(resp.status(), reqwest::StatusCode::OK);
        assert_eq!(
            resp.headers().get("content-type").unwrap(),
            "text/event-stream"
        );

        let body = resp.text().await.unwrap();

        // Parse SSE events from response
        let mut chunks: Vec<serde_json::Value> = Vec::new();
        let mut got_done = false;
        for line in body.lines() {
            if line == "data: [DONE]" {
                got_done = true;
            } else if let Some(data) = line.strip_prefix("data: ") {
                if let Ok(v) = serde_json::from_str::<serde_json::Value>(data) {
                    chunks.push(v);
                }
            }
        }

        assert!(got_done, "should have [DONE] sentinel");
        assert!(
            chunks.len() >= 3,
            "expected at least 3 chunks (role + content + finish), got {}",
            chunks.len()
        );

        // First chunk: role
        assert_eq!(chunks[0]["choices"][0]["delta"]["role"], "assistant");
        assert_eq!(chunks[0]["object"], "chat.completion.chunk");
        assert!(chunks[0]["id"].as_str().unwrap().starts_with("chatcmpl-"));

        // Content chunks
        let content_chunks: Vec<&str> = chunks
            .iter()
            .filter_map(|c| c["choices"][0]["delta"]["content"].as_str())
            .collect();
        assert_eq!(content_chunks.join(""), "Hello world");

        // Last data chunk: finish_reason
        let last = chunks.last().unwrap();
        assert_eq!(last["choices"][0]["finish_reason"], "stop");
    }

    #[tokio::test]
    async fn openai_chat_rejects_missing_auth() {
        let mock_app = Router::new().fallback(any(mock_anthropic_handler));
        let mock_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mock_addr = mock_listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(mock_listener, mock_app).await.unwrap();
        });

        let mock_url = format!("http://{}", mock_addr);
        let (app, _state) = test_openai_app(&mock_url, Some("secret-key".to_string()));

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(
                listener,
                app.into_make_service_with_connect_info::<SocketAddr>(),
            )
            .await
            .unwrap();
        });

        let client = Client::new();
        let resp = client
            .post(format!("http://{}/v1/chat/completions", addr))
            .header("content-type", "application/json")
            .body(r#"{"model":"test","messages":[{"role":"user","content":"hi"}],"max_tokens":1}"#)
            .send()
            .await
            .unwrap();

        assert_eq!(resp.status(), reqwest::StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn openai_chat_accepts_bearer_capitalized() {
        let mock_app = Router::new().fallback(any(mock_anthropic_handler));
        let mock_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mock_addr = mock_listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(mock_listener, mock_app).await.unwrap();
        });

        let mock_url = format!("http://{}", mock_addr);
        let (app, _state) = test_openai_app(&mock_url, Some("secret-key".to_string()));

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(
                listener,
                app.into_make_service_with_connect_info::<SocketAddr>(),
            )
            .await
            .unwrap();
        });

        let client = Client::new();
        let resp = client
            .post(format!("http://{}/v1/chat/completions", addr))
            .header("content-type", "application/json")
            .header("authorization", "Bearer secret-key")
            .body(r#"{"model":"test","messages":[{"role":"user","content":"hi"}],"max_tokens":1}"#)
            .send()
            .await
            .unwrap();

        assert_eq!(resp.status(), reqwest::StatusCode::OK);
    }

    #[tokio::test]
    async fn openai_chat_accepts_bearer_lowercase() {
        let mock_app = Router::new().fallback(any(mock_anthropic_handler));
        let mock_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mock_addr = mock_listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(mock_listener, mock_app).await.unwrap();
        });

        let mock_url = format!("http://{}", mock_addr);
        let (app, _state) = test_openai_app(&mock_url, Some("secret-key".to_string()));

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(
                listener,
                app.into_make_service_with_connect_info::<SocketAddr>(),
            )
            .await
            .unwrap();
        });

        let client = Client::new();
        let resp = client
            .post(format!("http://{}/v1/chat/completions", addr))
            .header("content-type", "application/json")
            .header("authorization", "bearer secret-key")
            .body(r#"{"model":"test","messages":[{"role":"user","content":"hi"}],"max_tokens":1}"#)
            .send()
            .await
            .unwrap();

        assert_eq!(resp.status(), reqwest::StatusCode::OK);
    }

    #[tokio::test]
    async fn openai_chat_wrong_apikey_valid_bearer() {
        let mock_app = Router::new().fallback(any(mock_anthropic_handler));
        let mock_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mock_addr = mock_listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(mock_listener, mock_app).await.unwrap();
        });

        let mock_url = format!("http://{}", mock_addr);
        let (app, _state) = test_openai_app(&mock_url, Some("secret-key".to_string()));

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(
                listener,
                app.into_make_service_with_connect_info::<SocketAddr>(),
            )
            .await
            .unwrap();
        });

        let client = Client::new();
        let resp = client
            .post(format!("http://{}/v1/chat/completions", addr))
            .header("content-type", "application/json")
            .header("x-api-key", "wrong-key")
            .header("authorization", "Bearer secret-key")
            .body(r#"{"model":"test","messages":[{"role":"user","content":"hi"}],"max_tokens":1}"#)
            .send()
            .await
            .unwrap();

        assert_eq!(resp.status(), reqwest::StatusCode::OK);
    }

    // ── Unit: auto-cache injection ─────────────────────────────────

    #[test]
    fn inject_cache_no_existing() {
        let mut body = serde_json::json!({
            "model": "claude-sonnet-4-6",
            "max_tokens": 1024,
            "system": "You are a helpful assistant.",
            "tools": [
                {"name": "get_weather", "description": "Gets weather", "input_schema": {"type": "object"}},
                {"name": "search", "description": "Searches", "input_schema": {"type": "object"}}
            ],
            "messages": [
                {"role": "user", "content": "Hello"},
                {"role": "assistant", "content": "Hi there!"},
                {"role": "user", "content": "What's the weather?"}
            ]
        });

        let inj = inject_cache_breakpoints(&mut body);
        assert!(!inj.skipped);
        assert!(inj.tools);
        assert!(inj.system);
        assert!(inj.messages);

        // Last tool should have cache_control
        let tools = body["tools"].as_array().unwrap();
        assert!(tools[0].get("cache_control").is_none());
        assert_eq!(tools[1]["cache_control"]["type"], "ephemeral");

        // System should be converted to array with cache_control
        let system = body["system"].as_array().unwrap();
        assert_eq!(system.len(), 1);
        assert_eq!(system[0]["text"], "You are a helpful assistant.");
        assert_eq!(system[0]["cache_control"]["type"], "ephemeral");

        // Last user message content should be converted to array
        let msgs = body["messages"].as_array().unwrap();
        let last_user = &msgs[2];
        let content = last_user["content"].as_array().unwrap();
        assert_eq!(content[0]["text"], "What's the weather?");
        assert_eq!(content[0]["cache_control"]["type"], "ephemeral");

        // First user message should be untouched
        assert_eq!(msgs[0]["content"], "Hello");
    }

    #[test]
    fn inject_cache_system_array() {
        let mut body = serde_json::json!({
            "model": "claude-sonnet-4-6",
            "system": [
                {"type": "text", "text": "System prompt part 1"},
                {"type": "text", "text": "System prompt part 2"}
            ],
            "messages": [
                {"role": "user", "content": "Hello"}
            ]
        });

        let inj = inject_cache_breakpoints(&mut body);
        assert!(inj.system);

        let system = body["system"].as_array().unwrap();
        assert!(system[0].get("cache_control").is_none());
        assert_eq!(system[1]["cache_control"]["type"], "ephemeral");
    }

    #[test]
    fn inject_cache_already_present() {
        let mut body = serde_json::json!({
            "model": "claude-sonnet-4-6",
            "system": [
                {"type": "text", "text": "Cached system", "cache_control": {"type": "ephemeral"}}
            ],
            "messages": [
                {"role": "user", "content": "Hello"}
            ]
        });

        let inj = inject_cache_breakpoints(&mut body);
        assert!(inj.skipped);
        assert!(!inj.tools);
        assert!(!inj.system);
        assert!(!inj.messages);

        // Verify nothing was modified — messages content is still a string
        assert_eq!(body["messages"][0]["content"], "Hello");
    }

    #[test]
    fn inject_cache_already_present_in_tools() {
        let mut body = serde_json::json!({
            "model": "claude-sonnet-4-6",
            "tools": [
                {"name": "t1", "cache_control": {"type": "ephemeral"}}
            ],
            "messages": [
                {"role": "user", "content": "Hello"}
            ]
        });

        let inj = inject_cache_breakpoints(&mut body);
        assert!(inj.skipped);
    }

    #[test]
    fn inject_cache_already_present_in_message_content() {
        let mut body = serde_json::json!({
            "model": "claude-sonnet-4-6",
            "messages": [
                {"role": "user", "content": [
                    {"type": "text", "text": "hi", "cache_control": {"type": "ephemeral"}}
                ]}
            ]
        });

        let inj = inject_cache_breakpoints(&mut body);
        assert!(inj.skipped);
    }

    #[test]
    fn inject_cache_empty_body() {
        let mut body = serde_json::json!({});
        let inj = inject_cache_breakpoints(&mut body);
        assert!(!inj.skipped);
        assert!(!inj.tools);
        assert!(!inj.system);
        assert!(!inj.messages);
    }

    #[test]
    fn inject_cache_messages_string_content() {
        let mut body = serde_json::json!({
            "messages": [
                {"role": "assistant", "content": "I'm an assistant"},
                {"role": "user", "content": "Tell me a joke"}
            ]
        });

        let inj = inject_cache_breakpoints(&mut body);
        assert!(inj.messages);
        assert!(!inj.tools);
        assert!(!inj.system);

        let content = body["messages"][1]["content"].as_array().unwrap();
        assert_eq!(content[0]["type"], "text");
        assert_eq!(content[0]["text"], "Tell me a joke");
        assert_eq!(content[0]["cache_control"]["type"], "ephemeral");

        // Assistant message should be untouched
        assert_eq!(body["messages"][0]["content"], "I'm an assistant");
    }

    #[test]
    fn inject_cache_user_message_array_content() {
        let mut body = serde_json::json!({
            "messages": [
                {"role": "user", "content": [
                    {"type": "text", "text": "First part"},
                    {"type": "text", "text": "Second part"}
                ]}
            ]
        });

        let inj = inject_cache_breakpoints(&mut body);
        assert!(inj.messages);

        let content = body["messages"][0]["content"].as_array().unwrap();
        assert!(content[0].get("cache_control").is_none());
        assert_eq!(content[1]["cache_control"]["type"], "ephemeral");
    }

    #[test]
    fn inject_cache_no_user_messages() {
        let mut body = serde_json::json!({
            "messages": [
                {"role": "assistant", "content": "Hi"}
            ]
        });

        let inj = inject_cache_breakpoints(&mut body);
        assert!(!inj.messages);
    }

    // ── Unit: token usage extraction ───────────────────────────────

    #[test]
    fn usage_from_non_streaming_response() {
        let body = serde_json::json!({
            "type": "message",
            "usage": {
                "input_tokens": 100,
                "output_tokens": 50,
                "cache_creation_input_tokens": 20,
                "cache_read_input_tokens": 30,
            }
        });
        let usage = TokenUsage::from_response_body(&body);
        assert_eq!(usage.input_tokens, 100);
        assert_eq!(usage.output_tokens, 50);
        assert_eq!(usage.cache_creation_input_tokens, 20);
        assert_eq!(usage.cache_read_input_tokens, 30);
    }

    #[test]
    fn usage_from_response_no_usage_field() {
        let body = serde_json::json!({"type": "error"});
        let usage = TokenUsage::from_response_body(&body);
        assert!(usage.is_empty());
    }

    #[test]
    fn usage_from_sse_stream() {
        let sse_text = "\
event: message_start\n\
data: {\"type\":\"message_start\",\"message\":{\"usage\":{\"input_tokens\":150,\"cache_creation_input_tokens\":10,\"cache_read_input_tokens\":5}}}\n\
\n\
event: content_block_delta\n\
data: {\"type\":\"content_block_delta\",\"delta\":{\"text\":\"Hello\"}}\n\
\n\
event: message_delta\n\
data: {\"type\":\"message_delta\",\"usage\":{\"output_tokens\":75}}\n\
\n\
event: message_stop\n\
data: {\"type\":\"message_stop\"}\n\n";

        let usage = TokenUsage::from_sse_text(sse_text);
        assert_eq!(usage.input_tokens, 150);
        assert_eq!(usage.output_tokens, 75);
        assert_eq!(usage.cache_creation_input_tokens, 10);
        assert_eq!(usage.cache_read_input_tokens, 5);
    }

    #[test]
    fn usage_from_empty_sse() {
        let usage = TokenUsage::from_sse_text("");
        assert!(usage.is_empty());
    }

    #[tokio::test]
    async fn record_usage_updates_account_and_client() {
        let state = test_state_with(vec![make_account("a", "sk-ant-api-x")]);
        let usage = TokenUsage {
            input_tokens: 100,
            output_tokens: 50,
            cache_creation_input_tokens: 20,
            cache_read_input_tokens: 30,
        };
        state.record_usage(0, "test-client", &usage).await;

        assert_eq!(state.accounts[0].input_tokens.load(Ordering::Relaxed), 100);
        assert_eq!(state.accounts[0].output_tokens.load(Ordering::Relaxed), 50);
        assert_eq!(
            state.accounts[0]
                .cache_creation_tokens
                .load(Ordering::Relaxed),
            20
        );
        assert_eq!(
            state.accounts[0].cache_read_tokens.load(Ordering::Relaxed),
            30
        );

        let map = state.client_usage.lock().unwrap();
        let client = map.get("test-client").unwrap();
        assert_eq!(client, &[100, 50, 20, 30]);
    }

    #[tokio::test]
    async fn record_usage_ignores_anonymous() {
        let state = test_state_with(vec![make_account("a", "sk-ant-api-x")]);
        let usage = TokenUsage {
            input_tokens: 100,
            output_tokens: 50,
            cache_creation_input_tokens: 0,
            cache_read_input_tokens: 0,
        };
        state.record_usage(0, "-", &usage).await;

        // Account gets updated
        assert_eq!(state.accounts[0].input_tokens.load(Ordering::Relaxed), 100);
        // But no client entry for anonymous
        let map = state.client_usage.lock().unwrap();
        assert!(!map.contains_key("-"));
    }

    // ── Unit: model-based routing ──────────────────────────────────

    #[test]
    fn account_serves_model_no_filter() {
        let acct = make_account("a", "sk-ant-api-x");
        assert!(acct.serves_model("claude-opus-4-6"));
        assert!(acct.serves_model("claude-haiku-4-5"));
        assert!(acct.serves_model(""));
    }

    #[test]
    fn account_serves_model_exact_match() {
        let mut acct = make_account("a", "sk-ant-api-x");
        acct.models = vec!["claude-sonnet-4-6".to_string()];
        assert!(acct.serves_model("claude-sonnet-4-6"));
        assert!(!acct.serves_model("claude-opus-4-6"));
    }

    #[test]
    fn account_serves_model_prefix_match() {
        let mut acct = make_account("a", "sk-ant-api-x");
        acct.models = vec!["claude-opus-*".to_string(), "claude-sonnet-*".to_string()];
        assert!(acct.serves_model("claude-opus-4-6"));
        assert!(acct.serves_model("claude-sonnet-4-6"));
        assert!(!acct.serves_model("claude-haiku-4-5"));
    }

    #[tokio::test]
    async fn pick_account_filters_by_model() {
        let mut acct_a = make_account("opus-only", "sk-ant-api-a");
        acct_a.models = vec!["claude-opus-*".to_string()];

        let acct_b = make_account("any-model", "sk-ant-api-b");

        let state = test_state_with(vec![acct_a, acct_b]);

        // Requesting opus: both accounts eligible
        let idx = state
            .pick_account(None, "claude-opus-4-6", &[])
            .await
            .unwrap();
        assert!(idx == 0 || idx == 1);

        // Requesting haiku: only acct_b eligible
        let idx = state
            .pick_account(None, "claude-haiku-4-5", &[])
            .await
            .unwrap();
        assert_eq!(idx, 1);
    }

    #[tokio::test]
    async fn soft_limit_excludes_overloaded_accounts() {
        let acct_a = make_account("healthy", "sk-ant-api-a");
        let acct_b = make_account("overloaded", "sk-ant-api-b");

        let accounts = vec![acct_a, acct_b];

        // Set utilizations before building state
        let now = AppState::now_epoch();
        {
            let mut info = accounts[0].rate_info.write().await;
            info.utilization = Some(0.30);
            info.utilization_5h = Some(0.30);
            info.reset_5h = Some(now + 10000);
        }
        {
            let mut info = accounts[1].rate_info.write().await;
            info.utilization = Some(0.95);
            info.utilization_5h = Some(0.95);
            info.reset_5h = Some(now + 10000);
        }

        let state = Arc::new(AppState {
            client: Client::builder()
                .timeout(Duration::from_secs(5))
                .build()
                .unwrap(),
            upstream: "http://127.0.0.1:1".to_string(),
            accounts,
            robin: AtomicUsize::new(0),
            routing_strategy: RoutingStrategy::default(),
            cooldown: Duration::from_secs(60),
            state_path: PathBuf::from("/tmp/anthropic-lb-test.state.json"),
            proxy_key: None,
            allowed_ips: vec![],
            upstreams: vec![],
            client_names: HashMap::new(),
            auto_cache: true,
            client_usage: Mutex::new(HashMap::new()),
            shadow_log_tx: None,
            shadow_log_dropped: AtomicU64::new(0),
            client_budgets: HashMap::new(),
            budget_usage: Mutex::new(HashMap::new()),
            client_utilization_limits: HashMap::new(),
            operators: vec![],
            emergency_brake: true,
            emergency_threshold: DEFAULT_EMERGENCY_THRESHOLD,
            client_request_rates: Mutex::new(HashMap::new()),
            soft_limit: 0.90,
            redis: None,
            cluster_info_cache: Mutex::new(None),
            next_req_id: AtomicU64::new(0),
            instance_id: 0,
            probe_interval_secs: 300,
        });

        // Try many affinity keys — all should route to healthy (idx 0)
        for i in 0..20 {
            let key = format!("client-{}", i);
            let idx = state.pick_account(Some(&key), "any", &[]).await.unwrap();
            assert_eq!(
                idx, 0,
                "client '{}' routed to overloaded account despite soft limit",
                key
            );
        }
    }

    #[tokio::test]
    async fn routing_candidates_ignore_unmatched_7d_state() {
        let state = test_state_with(vec![make_account("acct-a", "sk-ant-api-a")]);
        let now = AppState::now_epoch();

        {
            let mut info = state.accounts[0].rate_info.write().await;
            info.utilization = Some(0.20);
            info.utilization_5h = Some(0.20);
            info.reset_5h = Some(now + 10000);
            info.claims_7d.insert(
                "seven_day_haiku".to_string(),
                ClaimWindowData {
                    utilization: Some(0.95),
                    reset: Some(now + 100000),
                    status: Some("throttled".to_string()),
                    ..Default::default()
                },
            );
            // Derived aggregate 7d fields reflect the unrelated claim, but routing
            // for opus must ignore them when no applicable 7d claim exists.
            info.utilization_7d = Some(0.95);
            info.reset_7d = Some(now + 100000);
            info.status_7d = Some("throttled".to_string());
        }

        let candidates = state.routing_candidates("claude-opus-4-6", &[]).await;
        assert_eq!(candidates.len(), 1, "expected one routing candidate");

        let candidate = &candidates[0];
        assert_eq!(candidate.source, "headroom_only");
        assert!(
            (candidate.gate_7d - 0.0).abs() < f64::EPSILON,
            "unmatched 7d state should not leak into gate_7d: {:?}",
            candidate
        );
        assert!(
            (candidate.weight - 0.8).abs() < 0.0001,
            "weight should remain 5h headroom-only when no 7d claim applies: {:?}",
            candidate
        );
    }

    // ── Unit: per-client budget ────────────────────────────────────

    #[tokio::test]
    async fn budget_check_no_limit_configured() {
        let state = test_state_with(vec![make_account("a", "sk-ant-api-x")]);
        assert!(state.check_budget("any-client").await.is_ok());
    }

    #[tokio::test]
    async fn budget_check_within_limit() {
        let mut budgets = HashMap::new();
        budgets.insert("client-a".to_string(), 1000u64);
        let state = Arc::new(AppState {
            client: Client::builder()
                .timeout(Duration::from_secs(5))
                .build()
                .unwrap(),
            upstream: "http://127.0.0.1:1".to_string(),
            accounts: vec![make_account("a", "sk-ant-api-x")],
            robin: AtomicUsize::new(0),
            routing_strategy: RoutingStrategy::default(),
            cooldown: Duration::from_secs(60),
            state_path: PathBuf::from("/tmp/anthropic-lb-test.state.json"),
            proxy_key: None,
            allowed_ips: vec![],
            upstreams: vec![],
            client_names: HashMap::new(),
            auto_cache: true,
            client_usage: Mutex::new(HashMap::new()),
            shadow_log_tx: None,
            shadow_log_dropped: AtomicU64::new(0),
            client_budgets: budgets,
            budget_usage: Mutex::new(HashMap::new()),
            client_utilization_limits: HashMap::new(),
            operators: vec![],
            emergency_brake: true,
            emergency_threshold: DEFAULT_EMERGENCY_THRESHOLD,
            client_request_rates: Mutex::new(HashMap::new()),
            soft_limit: 1.0,
            redis: None,
            cluster_info_cache: Mutex::new(None),
            next_req_id: AtomicU64::new(0),
            instance_id: 0,
            probe_interval_secs: 300,
        });

        // Within budget
        assert!(state.check_budget("client-a").await.is_ok());

        // Record some usage
        state.record_budget_usage("client-a", 500).await;
        assert!(state.check_budget("client-a").await.is_ok());

        // Exceed budget
        state.record_budget_usage("client-a", 600).await;
        assert!(state.check_budget("client-a").await.is_err());

        // Unknown client has no budget, always ok
        assert!(state.check_budget("unknown").await.is_ok());
    }

    // ── Integration: 5xx retry ─────────────────────────────────────

    #[tokio::test]
    async fn proxy_retries_on_server_error() {
        // Spawn a mock that returns 500 on first request, 200 on second
        let call_count = Arc::new(AtomicU64::new(0));
        let count_clone = call_count.clone();

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mock_addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            loop {
                let (mut stream, _) = listener.accept().await.unwrap();
                let count = count_clone.fetch_add(1, Ordering::Relaxed);
                let response = if count == 0 {
                    "HTTP/1.1 500 Internal Server Error\r\ncontent-length: 14\r\n\r\nserver error!!"
                } else {
                    "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: 15\r\n\r\n{\"id\":\"test-1\"}"
                };
                use tokio::io::AsyncReadExt;
                use tokio::io::AsyncWriteExt;
                let mut buf = vec![0u8; 4096];
                let _ = stream.read(&mut buf).await;
                let _ = stream.write_all(response.as_bytes()).await;
            }
        });

        let upstream_url = format!("http://{}", mock_addr);
        let (app, _state) = test_app(&upstream_url, None);

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let app_addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(
                listener,
                app.into_make_service_with_connect_info::<SocketAddr>(),
            )
            .await
            .unwrap();
        });

        let client = reqwest::Client::new();
        let resp = client
            .post(format!("http://{}/v1/messages", app_addr))
            .header("content-type", "application/json")
            .header("x-api-key", "any")
            .body(r#"{"model":"claude-sonnet-4-6","messages":[{"role":"user","content":"hi"}]}"#)
            .send()
            .await
            .unwrap();

        // The first attempt hits 500, second attempt should succeed with 200
        assert_eq!(resp.status(), 200);
        // Two calls to upstream (500 + 200)
        assert_eq!(call_count.load(Ordering::Relaxed), 2);
    }

    // ── time_adjusted_utilization unit tests ────────────────────────

    #[test]
    fn time_adjust_no_reset() {
        // No reset timestamp → raw util returned unchanged
        let result = time_adjusted_utilization(Some(0.80), None, None, NEAR_RESET_5H_SECS, 1000000);
        assert_eq!(result, Some(0.80));
    }

    #[test]
    fn time_adjust_outside_threshold() {
        // 5h reset in 2 hours = 7200s, threshold is 3600s → no discount
        let now = 1000000u64;
        let reset = now + 7200;
        let result =
            time_adjusted_utilization(Some(0.90), Some(reset), None, NEAR_RESET_5H_SECS, now);
        assert_eq!(result, Some(0.90));
    }

    #[test]
    fn time_adjust_inside_threshold() {
        // 5h reset in 30 min = 1800s, threshold 3600s → discount = 1800/3600 = 0.50
        let now = 1000000u64;
        let reset = now + 1800;
        let result =
            time_adjusted_utilization(Some(0.90), Some(reset), None, NEAR_RESET_5H_SECS, now);
        let expected = 0.90 * 0.50;
        assert!((result.unwrap() - expected).abs() < 1e-10);
    }

    #[test]
    fn time_adjust_at_threshold_boundary() {
        // Reset exactly at threshold boundary (1h = 3600s) → discount = 3600/3600 = 1.0
        let now = 1000000u64;
        let reset = now + 3600;
        let result =
            time_adjusted_utilization(Some(0.90), Some(reset), None, NEAR_RESET_5H_SECS, now);
        assert_eq!(result, Some(0.90));
    }

    #[test]
    fn time_adjust_near_reset_floor() {
        // Reset in 1 minute = 60s → discount = max(60/3600, 0.05) = 0.05 (floor)
        let now = 1000000u64;
        let reset = now + 60;
        let raw = 60.0 / 3600.0; // 0.0167, below TIME_FRACTION_FLOOR
        assert!(raw < TIME_FRACTION_FLOOR);
        let result =
            time_adjusted_utilization(Some(0.95), Some(reset), None, NEAR_RESET_5H_SECS, now);
        let expected = 0.95 * TIME_FRACTION_FLOOR;
        assert!((result.unwrap() - expected).abs() < 1e-10);
    }

    #[test]
    fn time_adjust_past_reset() {
        // Reset already happened → None (stale data)
        let now = 1000000u64;
        let reset = now - 100;
        let result =
            time_adjusted_utilization(Some(0.90), Some(reset), None, NEAR_RESET_5H_SECS, now);
        assert_eq!(result, None);
    }

    #[test]
    fn time_adjust_throttled() {
        // Status=throttled overrides low util → floor at 0.98
        let now = 1000000u64;
        let reset = now + 7200;
        let result = time_adjusted_utilization(
            Some(0.30),
            Some(reset),
            Some("throttled"),
            NEAR_RESET_5H_SECS,
            now,
        );
        assert_eq!(result, Some(THROTTLE_UTIL_FLOOR));
    }

    #[test]
    fn time_adjust_warning() {
        // Status=allowed_warning overrides low util → floor at 0.80
        let now = 1000000u64;
        let reset = now + 7200;
        let result = time_adjusted_utilization(
            Some(0.50),
            Some(reset),
            Some("allowed_warning"),
            NEAR_RESET_5H_SECS,
            now,
        );
        assert_eq!(result, Some(WARNING_UTIL_FLOOR));
    }

    #[test]
    fn time_adjust_warning_already_higher() {
        // Util already above warning floor → util wins
        let now = 1000000u64;
        let reset = now + 7200;
        let result = time_adjusted_utilization(
            Some(0.90),
            Some(reset),
            Some("allowed_warning"),
            NEAR_RESET_5H_SECS,
            now,
        );
        assert_eq!(result, Some(0.90));
    }

    #[test]
    fn time_adjust_none_util() {
        // No utilization data → None
        let result =
            time_adjusted_utilization(None, Some(1000000), None, NEAR_RESET_5H_SECS, 999000);
        assert_eq!(result, None);
    }

    #[test]
    fn time_adjust_7d_window() {
        // 7d reset in 3 hours = 10800s, threshold 21600s → discount = 10800/21600 = 0.50
        let now = 1000000u64;
        let reset = now + 10800;
        let result =
            time_adjusted_utilization(Some(0.80), Some(reset), None, NEAR_RESET_7D_SECS, now);
        let expected = 0.80 * 0.50;
        assert!((result.unwrap() - expected).abs() < 1e-10);
    }

    #[test]
    fn time_adjust_throttled_overrides_discount() {
        // Near reset AND throttled → discount applies but floor wins
        let now = 1000000u64;
        let reset = now + 600;
        let result = time_adjusted_utilization(
            Some(0.50),
            Some(reset),
            Some("throttled"),
            NEAR_RESET_5H_SECS,
            now,
        );
        // Discounted: 0.50 * (600/3600) = 0.083, but throttle floor = 0.98
        assert_eq!(result, Some(THROTTLE_UTIL_FLOOR));
    }

    #[test]
    fn time_adjust_status_without_reset() {
        // Status present but no reset → floor applied to raw util
        let result = time_adjusted_utilization(
            Some(0.30),
            None,
            Some("throttled"),
            NEAR_RESET_5H_SECS,
            1000000,
        );
        assert_eq!(result, Some(THROTTLE_UTIL_FLOOR));
    }

    #[test]
    fn time_adjust_rejected() {
        // Status=rejected → floor at 1.0 (fully exhausted, zero bucket share)
        let now = 1000000u64;
        let reset = now + 7200;
        let result = time_adjusted_utilization(
            Some(0.30),
            Some(reset),
            Some("rejected"),
            NEAR_RESET_5H_SECS,
            now,
        );
        assert_eq!(result, Some(REJECTED_UTIL_FLOOR));
    }

    #[test]
    fn time_adjust_rejected_near_reset() {
        // Even near reset, rejected still maps to 1.0 — API is actively refusing
        let now = 1000000u64;
        let reset = now + 60; // 1 minute from reset
        let result = time_adjusted_utilization(
            Some(0.95),
            Some(reset),
            Some("rejected"),
            NEAR_RESET_5H_SECS,
            now,
        );
        assert_eq!(result, Some(REJECTED_UTIL_FLOOR));
    }

    #[test]
    fn time_adjust_unknown_status_gets_warning_floor() {
        // Unknown non-"allowed" status → defensive WARNING_UTIL_FLOOR (Bug #4)
        let now = 1000000u64;
        let reset = now + 7200;
        let result = time_adjusted_utilization(
            Some(0.30),
            Some(reset),
            Some("some_future_status"),
            NEAR_RESET_5H_SECS,
            now,
        );
        assert_eq!(result, Some(WARNING_UTIL_FLOOR));
    }

    #[test]
    fn time_adjust_allowed_status_no_floor() {
        // Explicit "allowed" status → no floor, just raw util
        let now = 1000000u64;
        let reset = now + 7200;
        let result = time_adjusted_utilization(
            Some(0.30),
            Some(reset),
            Some("allowed"),
            NEAR_RESET_5H_SECS,
            now,
        );
        assert_eq!(result, Some(0.30));
    }

    #[tokio::test]
    async fn status_clears_when_header_absent() {
        // Bug #1: stale status should clear when API sends utilization but no status header
        let accounts = vec![make_account("acct-a", "sk-ant-api-test-aaa")];
        let state = test_state_with(accounts);

        // First response: set throttled status
        let mut headers1 = reqwest::header::HeaderMap::new();
        headers1.insert(
            "anthropic-ratelimit-unified-5h-utilization",
            HeaderValue::from_static("0.30"),
        );
        headers1.insert(
            "anthropic-ratelimit-unified-5h-status",
            HeaderValue::from_static("throttled"),
        );
        headers1.insert(
            "anthropic-ratelimit-unified-5h-reset",
            HeaderValue::from_static("9999999999"),
        );
        state.update_rate_info(0, &headers1).await;
        {
            let info = state.accounts[0].rate_info.read().await;
            assert_eq!(info.status_5h.as_deref(), Some("throttled"));
        }

        // Second response: utilization header present, NO status header → clears
        let mut headers2 = reqwest::header::HeaderMap::new();
        headers2.insert(
            "anthropic-ratelimit-unified-5h-utilization",
            HeaderValue::from_static("0.25"),
        );
        headers2.insert(
            "anthropic-ratelimit-unified-5h-reset",
            HeaderValue::from_static("9999999999"),
        );
        state.update_rate_info(0, &headers2).await;
        {
            let info = state.accounts[0].rate_info.read().await;
            assert_eq!(
                info.status_5h, None,
                "status should clear when header absent"
            );
        }
    }

    #[tokio::test]
    async fn status_persists_when_no_util_header() {
        // If neither util nor status header is present for a window, don't clear status
        // (the response might be for a different window entirely)
        let accounts = vec![make_account("acct-a", "sk-ant-api-test-aaa")];
        let state = test_state_with(accounts);

        // Set throttled status
        let mut headers1 = reqwest::header::HeaderMap::new();
        headers1.insert(
            "anthropic-ratelimit-unified-5h-utilization",
            HeaderValue::from_static("0.30"),
        );
        headers1.insert(
            "anthropic-ratelimit-unified-5h-status",
            HeaderValue::from_static("throttled"),
        );
        state.update_rate_info(0, &headers1).await;

        // Response with no 5h headers at all (maybe only 7d headers)
        let headers2 = reqwest::header::HeaderMap::new();
        state.update_rate_info(0, &headers2).await;
        {
            let info = state.accounts[0].rate_info.read().await;
            assert_eq!(
                info.status_5h.as_deref(),
                Some("throttled"),
                "status should persist when no util header for that window"
            );
        }
    }

    #[tokio::test]
    async fn pick_returns_none_when_all_rejected() {
        // Bug #5: all-rejected accounts should return None (zero total headroom)
        let now_epoch = AppState::now_epoch();
        let state = test_state_with(vec![
            make_account("acct-a", "sk-ant-api-test-aaa"),
            make_account("acct-b", "sk-ant-api-test-bbb"),
        ]);
        for acct in &state.accounts {
            let mut info = acct.rate_info.write().await;
            info.utilization_5h = Some(0.50);
            info.utilization_7d = Some(0.30);
            info.utilization = Some(0.50);
            info.status_5h = Some("rejected".to_string());
            info.reset_5h = Some(now_epoch + 7200);
            info.reset_7d = Some(now_epoch + 86400);
        }
        let result = state.pick_account(None, "", &[]).await;
        assert_eq!(result, None, "all-rejected should return None");
    }

    #[tokio::test]
    async fn reset_sanity_rejects_far_future() {
        // Bug #6: reset timestamp > block duration from now should be rejected
        let accounts = vec![make_account("acct-a", "sk-ant-api-test-aaa")];
        let state = test_state_with(accounts);

        let mut headers = reqwest::header::HeaderMap::new();
        // 5h window: reset 10h from now (> 5h max) → should NOT be stored
        headers.insert(
            "anthropic-ratelimit-unified-5h-reset",
            HeaderValue::from_static("9999999999"),
        );
        // 7d window: reset 30d from now (> 7d max) → should NOT be stored
        headers.insert(
            "anthropic-ratelimit-unified-7d-reset",
            HeaderValue::from_static("9999999999"),
        );
        state.update_rate_info(0, &headers).await;
        {
            let info = state.accounts[0].rate_info.read().await;
            assert_eq!(
                info.reset_5h, None,
                "far-future 5h reset should be rejected"
            );
            assert_eq!(
                info.reset_7d, None,
                "far-future 7d reset should be rejected"
            );
        }
    }

    // ── pick_account integration tests for time-adjusted routing ────

    #[tokio::test]
    async fn pick_prefers_near_reset_account() {
        // Account A: 5h=0.95 reset in 10min, 7d=0.30 (7d binding after discount)
        // Account B: 5h=0.60 reset in 3h, 7d=0.50
        // A's 5h gets heavily discounted, 7d=0.30 becomes binding → A has more headroom than B
        let now_epoch = AppState::now_epoch();
        let accounts = vec![
            make_account("acct-a", "sk-ant-api-test-aaa"),
            make_account("acct-b", "sk-ant-api-test-bbb"),
        ];
        let state = test_state_with(accounts);
        {
            let mut info = state.accounts[0].rate_info.write().await;
            info.utilization_5h = Some(0.95);
            info.utilization_7d = Some(0.30);
            info.utilization = Some(0.95);
            info.reset_5h = Some(now_epoch + 600); // 10 min
            info.reset_7d = Some(now_epoch + 86400); // 1 day out
        }
        {
            let mut info = state.accounts[1].rate_info.write().await;
            info.utilization_5h = Some(0.60);
            info.utilization_7d = Some(0.50);
            info.utilization = Some(0.60);
            info.reset_5h = Some(now_epoch + 10800); // 3 hours
            info.reset_7d = Some(now_epoch + 86400);
        }

        // Run 100 picks without affinity to see distribution
        let mut a_count = 0;
        for _ in 0..100 {
            if let Some(idx) = state.pick_account(None, "", &[]).await {
                if idx == 0 {
                    a_count += 1;
                }
            }
        }
        // A's effective = max(adj_5h, adj_7d) = max(0.95*0.167, 0.30) = 0.30
        // B's effective = max(0.60, 0.50) = 0.60 (5h outside discount zone)
        // A headroom=0.70, B headroom=0.40 → A gets ~64% of traffic
        assert!(
            a_count > 50,
            "Account A (near-reset 5h) should get majority: got {a_count}/100"
        );
    }

    #[tokio::test]
    async fn pick_throttled_excludes() {
        // Account A: status=throttled (floor=0.98, above soft_limit=0.90)
        // Account B: healthy
        let now_epoch = AppState::now_epoch();
        let state = Arc::new(AppState {
            client: Client::builder()
                .timeout(Duration::from_secs(5))
                .build()
                .unwrap(),
            upstream: "http://127.0.0.1:1".to_string(),
            accounts: vec![
                make_account("acct-a", "sk-ant-api-test-aaa"),
                make_account("acct-b", "sk-ant-api-test-bbb"),
            ],
            robin: AtomicUsize::new(0),
            routing_strategy: RoutingStrategy::default(),
            cooldown: Duration::from_secs(60),
            state_path: PathBuf::from("/tmp/anthropic-lb-test.state.json"),
            proxy_key: None,
            allowed_ips: vec![],
            upstreams: vec![],
            client_names: HashMap::new(),
            auto_cache: true,
            client_usage: Mutex::new(HashMap::new()),
            shadow_log_tx: None,
            shadow_log_dropped: AtomicU64::new(0),
            client_budgets: HashMap::new(),
            budget_usage: Mutex::new(HashMap::new()),
            client_utilization_limits: HashMap::new(),
            operators: vec![],
            emergency_brake: true,
            emergency_threshold: DEFAULT_EMERGENCY_THRESHOLD,
            client_request_rates: Mutex::new(HashMap::new()),
            soft_limit: 0.90, // Key: not 1.0 — throttled (0.98) will be excluded
            redis: None,
            cluster_info_cache: Mutex::new(None),
            next_req_id: AtomicU64::new(0),
            instance_id: 0,
            probe_interval_secs: 300,
        });
        {
            let mut info = state.accounts[0].rate_info.write().await;
            info.utilization_5h = Some(0.30);
            info.utilization_7d = Some(0.20);
            info.utilization = Some(0.30);
            info.status_5h = Some("throttled".to_string());
            info.reset_5h = Some(now_epoch + 7200);
            info.reset_7d = Some(now_epoch + 86400);
        }
        {
            let mut info = state.accounts[1].rate_info.write().await;
            info.utilization_5h = Some(0.40);
            info.utilization_7d = Some(0.30);
            info.utilization = Some(0.40);
            info.status_5h = Some("allowed".to_string());
            info.reset_5h = Some(now_epoch + 7200);
            info.reset_7d = Some(now_epoch + 86400);
        }

        let mut b_count = 0;
        for _ in 0..100 {
            if let Some(idx) = state.pick_account(None, "", &[]).await {
                if idx == 1 {
                    b_count += 1;
                }
            }
        }
        // A is throttled → effective=0.98 → excluded by soft_limit=0.90
        // B gets all traffic
        assert_eq!(b_count, 100, "Throttled account A should be soft-excluded");
    }

    #[tokio::test]
    async fn pick_model_specific_7d_throttled_claim_excludes() {
        let now_epoch = AppState::now_epoch();
        let state = Arc::new(AppState {
            client: Client::builder()
                .timeout(Duration::from_secs(5))
                .build()
                .unwrap(),
            upstream: "http://127.0.0.1:1".to_string(),
            accounts: vec![
                make_account("acct-a", "sk-ant-api-test-aaa"),
                make_account("acct-b", "sk-ant-api-test-bbb"),
            ],
            robin: AtomicUsize::new(0),
            routing_strategy: RoutingStrategy::default(),
            cooldown: Duration::from_secs(60),
            state_path: PathBuf::from("/tmp/anthropic-lb-test.state.json"),
            proxy_key: None,
            allowed_ips: vec![],
            upstreams: vec![],
            client_names: HashMap::new(),
            auto_cache: true,
            client_usage: Mutex::new(HashMap::new()),
            shadow_log_tx: None,
            shadow_log_dropped: AtomicU64::new(0),
            client_budgets: HashMap::new(),
            budget_usage: Mutex::new(HashMap::new()),
            client_utilization_limits: HashMap::new(),
            operators: vec![],
            emergency_brake: true,
            emergency_threshold: DEFAULT_EMERGENCY_THRESHOLD,
            client_request_rates: Mutex::new(HashMap::new()),
            soft_limit: 0.90,
            redis: None,
            cluster_info_cache: Mutex::new(None),
            next_req_id: AtomicU64::new(0),
            instance_id: 0,
            probe_interval_secs: 300,
        });
        {
            let mut info = state.accounts[0].rate_info.write().await;
            info.utilization_5h = Some(0.20);
            info.utilization = Some(0.20);
            info.status_5h = Some("allowed".to_string());
            info.reset_5h = Some(now_epoch + 7200);
            info.claims_7d.insert(
                "seven_day_opus".to_string(),
                ClaimWindowData {
                    utilization: Some(0.20),
                    reset: Some(now_epoch + 86400),
                    status: Some("throttled".to_string()),
                    ..Default::default()
                },
            );
        }
        {
            let mut info = state.accounts[1].rate_info.write().await;
            info.utilization_5h = Some(0.40);
            info.utilization = Some(0.40);
            info.status_5h = Some("allowed".to_string());
            info.reset_5h = Some(now_epoch + 7200);
        }

        let mut b_count = 0;
        for i in 0..100 {
            let key = format!("sticky-opus-{i}");
            if let Some(idx) = state.pick_account(Some(&key), "claude-opus-4-6", &[]).await {
                if idx == 1 {
                    b_count += 1;
                }
            }
        }

        assert_eq!(
            b_count, 100,
            "model-specific throttled 7d claim should soft-exclude account A"
        );
    }

    #[tokio::test]
    async fn pick_mid_block_unchanged() {
        // Both accounts mid-block (3h remaining on 5h) — outside discount zone
        // Should behave identically to raw utilization
        let now_epoch = AppState::now_epoch();
        let accounts = vec![
            make_account("acct-a", "sk-ant-api-test-aaa"),
            make_account("acct-b", "sk-ant-api-test-bbb"),
        ];
        let state = test_state_with(accounts);
        {
            let mut info = state.accounts[0].rate_info.write().await;
            info.utilization_5h = Some(0.80);
            info.utilization_7d = Some(0.40);
            info.utilization = Some(0.80);
            info.reset_5h = Some(now_epoch + 10800); // 3h out
            info.reset_7d = Some(now_epoch + 86400);
        }
        {
            let mut info = state.accounts[1].rate_info.write().await;
            info.utilization_5h = Some(0.40);
            info.utilization_7d = Some(0.30);
            info.utilization = Some(0.40);
            info.reset_5h = Some(now_epoch + 10800);
            info.reset_7d = Some(now_epoch + 86400);
        }

        let mut b_count = 0;
        for _ in 0..100 {
            if let Some(idx) = state.pick_account(None, "", &[]).await {
                if idx == 1 {
                    b_count += 1;
                }
            }
        }
        // A: effective=max(0.80, 0.40)=0.80, headroom=0.20
        // B: effective=max(0.40, 0.30)=0.40, headroom=0.60
        // B should get ~75% (0.60 / 0.80)
        assert!(
            b_count > 60,
            "Mid-block: B (lower util) should dominate: got {b_count}/100"
        );
        assert!(
            b_count < 90,
            "Mid-block: A should still get some traffic: B got {b_count}/100"
        );
    }

    // ── Ewma tests ─────────────────────────────────────────────────

    #[test]
    fn ewma_single_update() {
        let start = Instant::now();
        let mut ewma = Ewma {
            value: 0.0,
            tau: TAU_5M,
            last_update: start,
        };
        // Simulate one request after 60 seconds
        let now = start + Duration::from_secs(60);
        let rate = ewma.update(now);
        // instant_rate = 60/60 = 1.0 req/min
        // alpha = 1 - exp(-60/300) = ~0.1813
        // value = 0.1813 * 1.0 + 0.8187 * 0.0 = ~0.1813
        assert!(
            rate > 0.15 && rate < 0.25,
            "rate should be ~0.18, got {rate}"
        );
    }

    #[test]
    fn ewma_burst() {
        let start = Instant::now();
        let mut ewma = Ewma {
            value: 0.0,
            tau: TAU_5M,
            last_update: start,
        };
        // 10 rapid requests, 100ms apart
        for i in 1..=10 {
            let now = start + Duration::from_millis(i * 100);
            ewma.update(now);
        }
        // instant_rate per update = 60/0.1 = 600 req/min
        // After 10 updates, value should be significantly elevated
        assert!(
            ewma.value() > 1.0,
            "burst rate should be high, got {}",
            ewma.value()
        );
    }

    #[test]
    fn ewma_decay() {
        let start = Instant::now();
        let mut ewma = Ewma {
            value: 100.0,
            tau: TAU_5M,
            last_update: start,
        };
        // One update after 5 minutes (one full tau)
        let now = start + Duration::from_secs(300);
        let rate = ewma.update(now);
        // alpha = 1 - exp(-300/300) = 1 - 1/e ≈ 0.6321
        // instant_rate = 60/300 = 0.2
        // value = 0.6321*0.2 + 0.3679*100 = 0.126 + 36.79 ≈ 36.9
        // The old value decays significantly
        assert!(rate < 50.0, "should decay from 100, got {rate}");
        assert!(rate > 20.0, "should retain some memory, got {rate}");
    }

    #[test]
    fn ewma_stale_reset() {
        let start = Instant::now();
        let mut ewma = Ewma {
            value: 42.0,
            tau: TAU_5M,
            last_update: start,
        };
        // Update after 2 hours (well beyond EWMA_STALE_SECS)
        let now = start + Duration::from_secs(7200);
        let rate = ewma.update(now);
        assert_eq!(rate, 0.0, "stale EWMA should reset to 0");
    }

    #[test]
    fn ewma_zero_elapsed() {
        let start = Instant::now();
        let mut ewma = Ewma {
            value: 10.0,
            tau: TAU_5M,
            last_update: start,
        };
        // Same instant — elapsed clamped to EWMA_MIN_ELAPSED_SECS
        let rate = ewma.update(start);
        assert!(rate.is_finite(), "zero elapsed should not produce NaN/inf");
        assert!(rate > 0.0, "should have a positive value");
    }

    #[test]
    fn ewma_nan_guard() {
        let start = Instant::now();
        let mut ewma = Ewma {
            value: f64::NAN,
            tau: TAU_5M,
            last_update: start,
        };
        let now = start + Duration::from_secs(1);
        let rate = ewma.update(now);
        assert!(
            rate.is_finite(),
            "NaN input should be recovered to finite value"
        );
    }

    // ── BurnRate tests ─────────────────────────────────────────────

    #[test]
    fn burn_rate_single_request() {
        let start = Instant::now();
        let mut br = BurnRate {
            rate_5m: Ewma {
                value: 0.0,
                tau: TAU_5M,
                last_update: start,
            },
            rate_1h: Ewma {
                value: 0.0,
                tau: TAU_1H,
                last_update: start,
            },
            rate_6h: Ewma {
                value: 0.0,
                tau: TAU_6H,
                last_update: start,
            },
        };
        let now = start + Duration::from_secs(60);
        br.update(now);
        let (r5m, r1h, r6h) = br.rates();
        // All should be positive after one request
        assert!(r5m > 0.0, "5m rate should be > 0");
        assert!(r1h > 0.0, "1h rate should be > 0");
        assert!(r6h > 0.0, "6h rate should be > 0");
        // Shorter tau → higher alpha → more responsive
        assert!(r5m > r1h, "5m rate should be more responsive than 1h");
        assert!(r1h > r6h, "1h rate should be more responsive than 6h");
    }

    #[test]
    fn burn_rate_burst() {
        let start = Instant::now();
        let mut br = BurnRate {
            rate_5m: Ewma {
                value: 0.0,
                tau: TAU_5M,
                last_update: start,
            },
            rate_1h: Ewma {
                value: 0.0,
                tau: TAU_1H,
                last_update: start,
            },
            rate_6h: Ewma {
                value: 0.0,
                tau: TAU_6H,
                last_update: start,
            },
        };
        // 10 requests, 1 second apart
        for i in 1..=10 {
            br.update(start + Duration::from_secs(i));
        }
        let (r5m, _r1h, r6h) = br.rates();
        // 5m window should spike much higher than 6h window
        assert!(
            r5m > r6h * 2.0,
            "5m should spike much more than 6h: 5m={r5m}, 6h={r6h}"
        );
    }

    #[test]
    fn burn_rate_decay() {
        let start = Instant::now();
        let mut br = BurnRate {
            rate_5m: Ewma {
                value: 60.0,
                tau: TAU_5M,
                last_update: start,
            },
            rate_1h: Ewma {
                value: 60.0,
                tau: TAU_1H,
                last_update: start,
            },
            rate_6h: Ewma {
                value: 60.0,
                tau: TAU_6H,
                last_update: start,
            },
        };
        // One update after 5 minutes of silence
        let now = start + Duration::from_secs(300);
        br.update(now);
        let (r5m, _r1h, r6h) = br.rates();
        // 5m should have decayed much more than 6h
        assert!(
            r5m < r6h,
            "5m should decay faster than 6h: 5m={r5m}, 6h={r6h}"
        );
    }

    // ── Waste risk tests ──────────────────────────────────────────

    #[test]
    fn waste_risk_normal() {
        let now = 1_000_000u64;
        let reset = now + 302400; // 3.5 days remaining (half of 7d)
        let wr = waste_risk(Some(0.40), Some(reset), now);
        // unused=0.60, remaining_fraction=302400/604800=0.5, wr=0.60/0.5=1.2
        assert!((wr - 1.2).abs() < 0.01, "expected ~1.2, got {wr}");
    }

    #[test]
    fn waste_risk_stale_reset() {
        let now = 1_000_000u64;
        let wr = waste_risk(Some(0.40), Some(now - 100), now);
        assert_eq!(wr, 0.0, "stale reset should return 0");
    }

    #[test]
    fn waste_risk_no_reset() {
        let wr = waste_risk(Some(0.40), None, 1_000_000);
        assert_eq!(wr, 0.0, "no reset should return 0");
    }

    #[test]
    fn waste_risk_under_60s_remaining() {
        let now = 1_000_000u64;
        let wr = waste_risk(Some(0.40), Some(now + 30), now);
        assert_eq!(wr, 0.0, "<60s remaining should return 0");
    }

    #[test]
    fn waste_risk_fully_utilized() {
        let now = 1_000_000u64;
        let wr = waste_risk(Some(1.0), Some(now + 302400), now);
        assert_eq!(wr, 0.0, "fully utilized should return 0");
    }

    #[test]
    fn waste_risk_near_reset_urgency() {
        let now = 1_000_000u64;
        // 2 hours remaining, 50% unused — very urgent
        let wr = waste_risk(Some(0.50), Some(now + 7200), now);
        // unused=0.50, remaining_fraction=7200/604800=0.0119, wr=0.50/0.0119=42→clamped to 10
        assert_eq!(wr, 10.0, "near-reset high-unused should clamp to 10");
    }

    #[test]
    fn waste_risk_clamped() {
        let now = 1_000_000u64;
        // 1 hour remaining, 90% unused
        let wr = waste_risk(Some(0.10), Some(now + 3600), now);
        // unused=0.90, remaining_fraction=3600/604800≈0.00595, wr≈151→clamped to 10
        assert_eq!(wr, 10.0, "should clamp to 10.0");
    }

    #[test]
    fn waste_risk_boundary_at_60s() {
        let now = 1_000_000u64;
        // Exactly 60s remaining — guard is > 60, so should return 0
        assert_eq!(waste_risk(Some(0.40), Some(now + 60), now), 0.0);
        // 61s remaining — just above boundary, should return non-zero
        assert!(waste_risk(Some(0.40), Some(now + 61), now) > 0.0);
    }

    #[test]
    fn waste_risk_util_above_one() {
        let now = 1_000_000u64;
        // API can return util > 1.0; unused should clamp to 0
        let wr = waste_risk(Some(1.05), Some(now + 302400), now);
        assert_eq!(wr, 0.0, "util > 1.0 should produce zero waste risk");
    }

    // ── compute_routing_weight tests ──────────────────────────────

    #[test]
    fn routing_weight_basic_5h_only() {
        // No 7d data → headroom_only, weight = headroom = 1 - gate_5h
        let now = 1_000_000u64;
        let info = RateLimitInfo {
            utilization_5h: Some(0.40),
            reset_5h: Some(now + 18000), // 5h window
            status_5h: Some("allowed".to_string()),
            ..Default::default()
        };
        let rw = compute_routing_weight(&info, "claude-sonnet-4-6", now, false)
            .expect("should produce weight");
        assert!(rw.gate_5h > 0.0 && rw.gate_5h < 1.0);
        assert_eq!(rw.source, "headroom_only");
        assert!(rw.wr == 0.0);
        assert!(rw.weight > 0.0);
    }

    #[test]
    fn routing_weight_with_waste_risk() {
        // 7d claim present → waste_risk sourced weight
        let now = 1_000_000u64;
        let mut claims = HashMap::new();
        claims.insert(
            "seven_day_sonnet".to_string(),
            ClaimWindowData {
                utilization: Some(0.40),
                reset: Some(now + 302400), // 3.5 days
                status: Some("allowed".to_string()),
                last_seen: now,
            },
        );
        let info = RateLimitInfo {
            utilization_5h: Some(0.20),
            reset_5h: Some(now + 18000),
            status_5h: Some("allowed".to_string()),
            claims_7d: claims,
            ..Default::default()
        };
        let rw = compute_routing_weight(&info, "claude-sonnet-4-6", now, false)
            .expect("should produce weight");
        assert_eq!(rw.source, "waste_risk");
        assert!(rw.wr > 0.0, "waste_risk should be positive");
        // weight = wr * headroom (both positive)
        assert!(rw.weight > 0.0);
    }

    #[test]
    fn routing_weight_rejected_returns_none() {
        // 7d claim rejected → None (account should be skipped)
        let now = 1_000_000u64;
        let mut claims = HashMap::new();
        claims.insert(
            "seven_day_sonnet".to_string(),
            ClaimWindowData {
                utilization: Some(1.0),
                reset: Some(now + 302400),
                status: Some("rejected".to_string()),
                last_seen: now,
            },
        );
        let info = RateLimitInfo {
            utilization_5h: Some(0.20),
            reset_5h: Some(now + 18000),
            status_5h: Some("allowed".to_string()),
            claims_7d: claims,
            ..Default::default()
        };
        assert!(
            compute_routing_weight(&info, "claude-sonnet-4-6", now, false).is_none(),
            "rejected claim should return None"
        );
    }

    #[test]
    fn routing_weight_expired_rejected_claim_returns_some() {
        let now = 1_000_000u64;
        let mut claims = HashMap::new();
        claims.insert(
            "seven_day_sonnet".to_string(),
            ClaimWindowData {
                utilization: Some(1.0),
                reset: Some(now.saturating_sub(1)),
                status: Some("rejected".to_string()),
                last_seen: now,
            },
        );
        let info = RateLimitInfo {
            utilization_5h: Some(0.20),
            reset_5h: Some(now + 18000),
            status_5h: Some("allowed".to_string()),
            claims_7d: claims,
            ..Default::default()
        };
        assert!(
            compute_routing_weight(&info, "claude-sonnet-4-6", now, false).is_some(),
            "expired rejected claim should not return None"
        );
    }

    #[test]
    fn routing_weight_stale_uses_fallback() {
        // stale_after_hard_limit = true → both gates fallback to 0.5
        let now = 1_000_000u64;
        let info = RateLimitInfo {
            utilization_5h: Some(0.95), // would be high, but stale
            reset_5h: Some(now + 18000),
            status_5h: Some("throttled".to_string()),
            ..Default::default()
        };
        let rw = compute_routing_weight(&info, "claude-sonnet-4-6", now, true)
            .expect("stale should still produce weight");
        assert_eq!(rw.gate_5h, 0.5);
        assert_eq!(rw.gate_7d, 0.5);
        assert_eq!(rw.gate, 0.5);
    }

    #[test]
    fn routing_weight_rejected_but_stale_still_returns_some() {
        // Stale after hard limit + rejected claim → should NOT skip (give it a chance)
        let now = 1_000_000u64;
        let mut claims = HashMap::new();
        claims.insert(
            "seven_day_sonnet".to_string(),
            ClaimWindowData {
                utilization: Some(1.0),
                reset: Some(now + 302400),
                status: Some("rejected".to_string()),
                last_seen: now,
            },
        );
        let info = RateLimitInfo {
            utilization_5h: Some(0.20),
            reset_5h: Some(now + 18000),
            status_5h: Some("allowed".to_string()),
            claims_7d: claims,
            ..Default::default()
        };
        assert!(
            compute_routing_weight(&info, "claude-sonnet-4-6", now, true).is_some(),
            "stale rejected should still return Some (give account a chance)"
        );
    }

    #[test]
    fn routing_weight_publish_ttl_uses_fallback_interval_when_probes_disabled() {
        assert_eq!(AppState::routing_weight_publish_ttl(0), 120);
    }

    #[test]
    fn routing_weight_publish_ttl_doubles_probe_interval() {
        assert_eq!(AppState::routing_weight_publish_ttl(300), 600);
    }

    #[test]
    fn routing_weight_no_data_uses_defaults() {
        // No utilization data at all → gate_5h=0.5 (unknown), headroom=0.5
        let now = 1_000_000u64;
        let info = RateLimitInfo::default();
        let rw = compute_routing_weight(&info, "claude-sonnet-4-6", now, false)
            .expect("should produce weight with defaults");
        assert_eq!(rw.gate_5h, 0.5);
        assert_eq!(rw.source, "headroom_only");
        assert!(rw.weight > 0.0);
    }

    // ── classify_hard_limit_sync tests ────────────────────────────

    #[test]
    fn classify_hard_limit_none_is_ignore() {
        let now_instant = Instant::now();
        assert_eq!(
            classify_hard_limit_sync(None, 1_000_000, now_instant),
            HardLimitSync::Ignore
        );
    }

    #[test]
    fn classify_hard_limit_sentinel_is_clear() {
        let now_instant = Instant::now();
        assert_eq!(
            classify_hard_limit_sync(Some(HARD_LIMIT_CLEARED_SENTINEL), 1_000_000, now_instant),
            HardLimitSync::Clear
        );
    }

    #[test]
    fn classify_hard_limit_future_epoch_is_update() {
        let now_instant = Instant::now();
        let now_epoch = 1_000_000u64;
        let future = now_epoch + 300; // 5 min from now
        match classify_hard_limit_sync(Some(future), now_epoch, now_instant) {
            HardLimitSync::Update(until) => {
                let expected = now_instant + Duration::from_secs(300);
                let delta = if until > expected {
                    until.duration_since(expected)
                } else {
                    expected.duration_since(until)
                };
                assert!(delta < Duration::from_millis(1), "until Instant mismatch");
            }
            other => panic!("expected Update, got {other:?}"),
        }
    }

    #[test]
    fn classify_hard_limit_past_epoch_is_ignore() {
        // Stale non-zero value — e.g. a hard limit that already expired via TTL
        let now_instant = Instant::now();
        let now_epoch = 1_000_000u64;
        assert_eq!(
            classify_hard_limit_sync(Some(now_epoch - 100), now_epoch, now_instant),
            HardLimitSync::Ignore
        );
    }

    #[test]
    fn classify_hard_limit_epoch_equal_now_is_ignore() {
        // Boundary: epoch == now means expired now, not future
        let now_instant = Instant::now();
        let now_epoch = 1_000_000u64;
        assert_eq!(
            classify_hard_limit_sync(Some(now_epoch), now_epoch, now_instant),
            HardLimitSync::Ignore
        );
    }

    #[test]
    fn classify_hard_limit_clamps_far_future() {
        // Corrupt or malicious Redis value must not create a panic-inducing Instant
        // nor a permanent undead hard limit. Clamp to 24h.
        let now_instant = Instant::now();
        let now_epoch = 1_000_000u64;
        match classify_hard_limit_sync(Some(u64::MAX), now_epoch, now_instant) {
            HardLimitSync::Update(until) => {
                let capped = now_instant + Duration::from_secs(86_400);
                let delta = if until > capped {
                    until.duration_since(capped)
                } else {
                    capped.duration_since(until)
                };
                assert!(delta < Duration::from_millis(1), "expected 24h clamp");
            }
            other => panic!("expected Update (clamped), got {other:?}"),
        }
    }

    #[tokio::test]
    async fn signal_hard_limit_recovery_without_redis_is_noop() {
        // Without Redis, the helper must still refresh metrics + publish weights
        // locally. It must not panic and must not attempt Redis I/O.
        let state = test_state_with(vec![make_account("a", "sk-ant-api-test")]);
        // Pre-seed a non-zero weight so we can assert refresh_metrics_weights ran
        state.accounts[0]
            .last_routing_weight
            .store(0u64, Ordering::Relaxed);
        state.signal_hard_limit_recovery(&state.accounts[0]).await;
        // refresh_metrics_weights always writes a value (even zero) — the point
        // is that the method completed without Redis and without panicking.
        let w = f64::from_bits(
            state.accounts[0]
                .last_routing_weight
                .load(Ordering::Relaxed),
        );
        assert!(w.is_finite(), "weight atomic must remain valid: {w}");
    }

    // ── resolve_7d_claim tests ────────────────────────────────────

    #[test]
    fn resolve_7d_claim_model_specific() {
        let mut claims = HashMap::new();
        claims.insert(
            "seven_day_sonnet".to_string(),
            ClaimWindowData {
                utilization: Some(0.80),
                reset: Some(1000000),
                status: None,
                ..Default::default()
            },
        );
        claims.insert(
            "seven_day".to_string(),
            ClaimWindowData {
                utilization: Some(0.50),
                reset: Some(1000000),
                status: None,
                ..Default::default()
            },
        );
        let info = RateLimitInfo {
            claims_7d: claims,
            ..Default::default()
        };
        let claim = resolve_7d_claim(&info, "claude-sonnet-4-6").unwrap();
        assert!(
            (claim.utilization.unwrap() - 0.80).abs() < 0.001,
            "should pick model-specific claim"
        );
    }

    #[test]
    fn resolve_7d_claim_fallback_general() {
        let mut claims = HashMap::new();
        claims.insert(
            "seven_day".to_string(),
            ClaimWindowData {
                utilization: Some(0.50),
                reset: Some(1000000),
                status: None,
                ..Default::default()
            },
        );
        let info = RateLimitInfo {
            claims_7d: claims,
            ..Default::default()
        };
        let claim = resolve_7d_claim(&info, "claude-sonnet-4-6").unwrap();
        assert!(
            (claim.utilization.unwrap() - 0.50).abs() < 0.001,
            "should fall back to general"
        );
    }

    #[test]
    fn resolve_7d_claim_empty_claims() {
        let info = RateLimitInfo::default();
        assert!(resolve_7d_claim(&info, "claude-sonnet-4-6").is_none());
    }

    #[test]
    fn resolve_7d_claim_empty_model() {
        let mut claims = HashMap::new();
        claims.insert(
            "seven_day".to_string(),
            ClaimWindowData {
                utilization: Some(0.50),
                reset: Some(1000000),
                status: None,
                ..Default::default()
            },
        );
        let info = RateLimitInfo {
            claims_7d: claims,
            ..Default::default()
        };
        assert!(
            resolve_7d_claim(&info, "").is_none(),
            "empty model should return None"
        );
    }

    // ── Client identity resolution tests ──────────────────────────

    #[test]
    fn resolve_header_overrides_ip_map() {
        let mut client_names = HashMap::new();
        client_names.insert("10.0.0.1".to_string(), "ray".to_string());
        let state = Arc::new(AppState {
            client: Client::builder()
                .timeout(Duration::from_secs(5))
                .build()
                .unwrap(),
            upstream: "http://127.0.0.1:1".to_string(),
            accounts: vec![make_account("a", "sk-ant-api-x")],
            robin: AtomicUsize::new(0),
            routing_strategy: RoutingStrategy::default(),
            cooldown: Duration::from_secs(60),
            state_path: PathBuf::from("/tmp/test.state.json"),
            proxy_key: None,
            allowed_ips: vec![],
            upstreams: vec![],
            client_names,
            auto_cache: true,
            client_usage: Mutex::new(HashMap::new()),
            shadow_log_tx: None,
            shadow_log_dropped: AtomicU64::new(0),
            client_budgets: HashMap::new(),
            budget_usage: Mutex::new(HashMap::new()),
            client_utilization_limits: HashMap::new(),
            operators: vec![],
            emergency_brake: true,
            emergency_threshold: DEFAULT_EMERGENCY_THRESHOLD,
            client_request_rates: Mutex::new(HashMap::new()),
            soft_limit: 1.0,
            redis: None,
            cluster_info_cache: Mutex::new(None),
            next_req_id: AtomicU64::new(0),
            instance_id: 0,
            probe_interval_secs: 300,
        });

        // Header overrides IP mapping (supports multiple clients per IP)
        let mut headers = hyper::HeaderMap::new();
        headers.insert("x-client-id", HeaderValue::from_static("gastown"));
        let ip: IpAddr = "10.0.0.1".parse().unwrap();
        assert_eq!(state.resolve_client_id(&ip, &headers), "gastown");

        // No header → falls back to IP mapping
        let empty_headers = hyper::HeaderMap::new();
        assert_eq!(state.resolve_client_id(&ip, &empty_headers), "ray");
    }

    #[test]
    fn resolve_header_fallback() {
        let state = test_state_with(vec![make_account("a", "sk-ant-api-x")]);
        let mut headers = hyper::HeaderMap::new();
        headers.insert("x-client-id", HeaderValue::from_static("gastown"));
        let ip: IpAddr = "192.168.1.99".parse().unwrap();
        assert_eq!(state.resolve_client_id(&ip, &headers), "gastown");
    }

    #[test]
    fn resolve_unknown() {
        let state = test_state_with(vec![make_account("a", "sk-ant-api-x")]);
        let headers = hyper::HeaderMap::new();
        let ip: IpAddr = "192.168.1.99".parse().unwrap();
        assert_eq!(state.resolve_client_id(&ip, &headers), "-");
    }

    #[test]
    fn resolve_multi_client_per_ip() {
        // Multiple clients share the same IP — header differentiates them.
        // Operator is identified by client_id, not by IP.
        let mut client_names = HashMap::new();
        client_names.insert("10.0.0.1".to_string(), "ray".to_string());
        let state = Arc::new(AppState {
            client: Client::builder()
                .timeout(Duration::from_secs(5))
                .build()
                .unwrap(),
            upstream: "http://127.0.0.1:1".to_string(),
            accounts: vec![make_account("a", "sk-ant-api-x")],
            robin: AtomicUsize::new(0),
            routing_strategy: RoutingStrategy::default(),
            cooldown: Duration::from_secs(60),
            state_path: PathBuf::from("/tmp/test.state.json"),
            proxy_key: None,
            allowed_ips: vec![],
            upstreams: vec![],
            client_names,
            auto_cache: true,
            client_usage: Mutex::new(HashMap::new()),
            shadow_log_tx: None,
            shadow_log_dropped: AtomicU64::new(0),
            client_budgets: HashMap::new(),
            budget_usage: Mutex::new(HashMap::new()),
            client_utilization_limits: HashMap::new(),
            operators: vec!["ray".to_string()],
            emergency_brake: true,
            emergency_threshold: DEFAULT_EMERGENCY_THRESHOLD,
            client_request_rates: Mutex::new(HashMap::new()),
            soft_limit: 1.0,
            redis: None,
            cluster_info_cache: Mutex::new(None),
            next_req_id: AtomicU64::new(0),
            instance_id: 0,
            probe_interval_secs: 300,
        });

        let ip: IpAddr = "10.0.0.1".parse().unwrap();

        // gastown on same IP as operator → identified as gastown, NOT operator
        let mut headers = hyper::HeaderMap::new();
        headers.insert("x-client-id", HeaderValue::from_static("gastown"));
        assert_eq!(state.resolve_client_id(&ip, &headers), "gastown");
        assert!(!state.is_operator("gastown"));

        // ray on same IP → identified as ray, IS operator
        headers.insert("x-client-id", HeaderValue::from_static("ray"));
        assert_eq!(state.resolve_client_id(&ip, &headers), "ray");
        assert!(state.is_operator("ray"));

        // No header → falls back to IP mapping ("ray")
        let empty = hyper::HeaderMap::new();
        assert_eq!(state.resolve_client_id(&ip, &empty), "ray");
    }

    // ── Effective utilization tests ────────────────────────────────

    #[tokio::test]
    async fn effective_util_both_windows() {
        let now_epoch = AppState::now_epoch();
        let mut claims_7d = HashMap::new();
        claims_7d.insert(
            "seven_day".to_string(),
            ClaimWindowData {
                utilization: Some(0.80),
                reset: Some(now_epoch + 100000),
                status: None,
                ..Default::default()
            },
        );
        let info = RateLimitInfo {
            utilization_5h: Some(0.60),
            reset_5h: Some(now_epoch + 10000),
            claims_7d,
            ..Default::default()
        };
        let (util, source, _, _) = effective_utilization(&info, now_epoch, "");
        // 7d at 0.80 (no penalty). Max(0.60, 0.80) = 0.80
        assert_eq!(source, "7d");
        assert!((util - 0.80).abs() < 0.01, "expected ~0.80, got {util}");
    }

    #[tokio::test]
    async fn effective_util_5h_only() {
        let now_epoch = AppState::now_epoch();
        // 7d claim with reset in the past → stale, evicted by time_adjusted_utilization
        let mut claims_7d = HashMap::new();
        claims_7d.insert(
            "seven_day".to_string(),
            ClaimWindowData {
                utilization: Some(0.90),
                reset: Some(now_epoch - 1),
                status: None,
                ..Default::default()
            },
        );
        let info = RateLimitInfo {
            utilization_5h: Some(0.60),
            reset_5h: Some(now_epoch + 10000),
            claims_7d,
            ..Default::default()
        };
        let (util, source, _, _) = effective_utilization(&info, now_epoch, "");
        assert_eq!(source, "5h");
        assert!(
            (util - 0.60).abs() < 0.01,
            "should use 5h value: got {util}"
        );
    }

    #[tokio::test]
    async fn effective_util_7d_only() {
        let now_epoch = AppState::now_epoch();
        let mut claims_7d = HashMap::new();
        claims_7d.insert(
            "seven_day".to_string(),
            ClaimWindowData {
                utilization: Some(0.50),
                reset: Some(now_epoch + 100000),
                status: None,
                ..Default::default()
            },
        );
        let info = RateLimitInfo {
            // 5h stale
            utilization_5h: Some(0.40),
            reset_5h: Some(now_epoch - 1),
            claims_7d,
            ..Default::default()
        };
        let (util, source, _, _) = effective_utilization(&info, now_epoch, "");
        assert_eq!(source, "7d");
        // 0.50 is below CLAIM_PENALTY_THRESHOLD, so no penalty
        assert!(
            (util - 0.50).abs() < 0.01,
            "should use 7d value: got {util}"
        );
    }

    #[tokio::test]
    async fn effective_util_fallback_unified() {
        let now_epoch = AppState::now_epoch();
        // Both 5h and all 7d claims stale → falls through to unified
        let mut claims_7d = HashMap::new();
        claims_7d.insert(
            "seven_day".to_string(),
            ClaimWindowData {
                utilization: Some(0.50),
                reset: Some(now_epoch - 1),
                status: None,
                ..Default::default()
            },
        );
        let info = RateLimitInfo {
            utilization_5h: Some(0.40),
            reset_5h: Some(now_epoch - 1),
            claims_7d,
            utilization: Some(0.65),
            ..Default::default()
        };
        let (util, source, _, _) = effective_utilization(&info, now_epoch, "");
        assert_eq!(source, "unified");
        assert!(
            (util - 0.65).abs() < 0.001,
            "should use unified: got {util}"
        );
    }

    #[tokio::test]
    async fn effective_util_fallback_legacy() {
        let now_epoch = AppState::now_epoch();
        let info = RateLimitInfo {
            remaining_tokens: Some(300_000),
            limit_tokens: Some(1_000_000),
            ..Default::default()
        };
        let (util, source, _, _) = effective_utilization(&info, now_epoch, "");
        assert_eq!(source, "legacy");
        assert!(
            (util - 0.70).abs() < 0.01,
            "should use legacy token ratio: got {util}"
        );
    }

    #[tokio::test]
    async fn effective_util_fallback_unknown() {
        let now_epoch = AppState::now_epoch();
        let info = RateLimitInfo::default();
        let (util, source, _, _) = effective_utilization(&info, now_epoch, "");
        assert_eq!(source, "unknown");
        assert!(
            (util - 0.50).abs() < 0.001,
            "should default to 0.5: got {util}"
        );
    }

    // ── Per-claim 7d model-specific tests ──────────────────────────

    #[tokio::test]
    async fn model_specific_routing() {
        // Sonnet 7d at 0.85 (no penalty), Opus 7d at 0.10 → Opus sees low util
        let now_epoch = AppState::now_epoch();
        let mut claims_7d = HashMap::new();
        claims_7d.insert(
            "seven_day_sonnet".to_string(),
            ClaimWindowData {
                utilization: Some(0.85),
                reset: Some(now_epoch + 100000),
                status: None,
                ..Default::default()
            },
        );
        claims_7d.insert(
            "seven_day_opus".to_string(),
            ClaimWindowData {
                utilization: Some(0.10),
                reset: Some(now_epoch + 100000),
                status: None,
                ..Default::default()
            },
        );
        let info = RateLimitInfo {
            utilization_5h: Some(0.30),
            reset_5h: Some(now_epoch + 10000),
            claims_7d,
            ..Default::default()
        };
        let (util_sonnet, _, _, _) = effective_utilization(&info, now_epoch, "claude-sonnet-4-6");
        let (util_opus, _, _, _) = effective_utilization(&info, now_epoch, "claude-opus-4-6");
        // Sonnet 7d at 0.85 (max with 5h 0.30) = 0.85, Opus 7d at 0.10 (max with 5h 0.30) = 0.30
        assert!(
            (util_sonnet - 0.85).abs() < 0.01,
            "sonnet should be 0.85: got {util_sonnet}"
        );
        // Opus at 0.10 → should stay at ~0.30 (max with 5h)
        assert!(util_opus < 0.35, "opus should be low: got {util_opus}");
        // The gap should be large — this is the whole point of per-claim routing
        assert!(
            util_sonnet - util_opus > 0.50,
            "sonnet-opus gap should be >0.50"
        );
    }

    #[tokio::test]
    async fn claim_fallback_general() {
        // Only "seven_day" (general) claim → used for all models
        let now_epoch = AppState::now_epoch();
        let mut claims_7d = HashMap::new();
        claims_7d.insert(
            "seven_day".to_string(),
            ClaimWindowData {
                utilization: Some(0.50),
                reset: Some(now_epoch + 100000),
                status: None,
                ..Default::default()
            },
        );
        let info = RateLimitInfo {
            utilization_5h: Some(0.20),
            reset_5h: Some(now_epoch + 10000),
            claims_7d,
            ..Default::default()
        };
        let (util_sonnet, _, _, _) = effective_utilization(&info, now_epoch, "claude-sonnet-4-6");
        let (util_opus, _, _, _) = effective_utilization(&info, now_epoch, "claude-opus-4-6");
        let (util_haiku, _, _, _) = effective_utilization(&info, now_epoch, "claude-haiku-4-5");
        // All should see the same general claim (0.50, no penalty)
        assert!(
            (util_sonnet - 0.50).abs() < 0.01,
            "sonnet: got {util_sonnet}"
        );
        assert!((util_opus - 0.50).abs() < 0.01, "opus: got {util_opus}");
        assert!((util_haiku - 0.50).abs() < 0.01, "haiku: got {util_haiku}");
    }

    #[tokio::test]
    async fn cross_model_isolation() {
        // Sonnet claim at 0.95 should NOT affect Opus effective_utilization
        let now_epoch = AppState::now_epoch();
        let mut claims_7d = HashMap::new();
        claims_7d.insert(
            "seven_day_sonnet".to_string(),
            ClaimWindowData {
                utilization: Some(0.95),
                reset: Some(now_epoch + 100000),
                status: None,
                ..Default::default()
            },
        );
        // No opus claim and no general claim
        let info = RateLimitInfo {
            utilization_5h: Some(0.20),
            reset_5h: Some(now_epoch + 10000),
            claims_7d,
            ..Default::default()
        };
        let (util_opus, source, _, _) = effective_utilization(&info, now_epoch, "claude-opus-4-6");
        // Opus has no specific claim and no general fallback → only 5h at 0.20
        assert_eq!(source, "5h");
        assert!(
            (util_opus - 0.20).abs() < 0.01,
            "opus should only see 5h: got {util_opus}"
        );

        let (util_sonnet, _, _, _) = effective_utilization(&info, now_epoch, "claude-sonnet-4-6");
        assert!(
            (util_sonnet - 0.95).abs() < 0.01,
            "sonnet should be 0.95: got {util_sonnet}"
        );
    }

    #[tokio::test]
    async fn emergency_brake_worst_case() {
        // Emergency brake (model="") should use max across all claims
        let now_epoch = AppState::now_epoch();
        let mut claims_7d = HashMap::new();
        claims_7d.insert(
            "seven_day_sonnet".to_string(),
            ClaimWindowData {
                utilization: Some(0.95),
                reset: Some(now_epoch + 100000),
                status: None,
                ..Default::default()
            },
        );
        claims_7d.insert(
            "seven_day_opus".to_string(),
            ClaimWindowData {
                utilization: Some(0.10),
                reset: Some(now_epoch + 100000),
                status: None,
                ..Default::default()
            },
        );
        let info = RateLimitInfo {
            utilization_5h: Some(0.30),
            reset_5h: Some(now_epoch + 10000),
            claims_7d,
            ..Default::default()
        };
        let (util, _, _, _) = effective_utilization(&info, now_epoch, "");
        // Should pick sonnet's 0.95 (no penalty now)
        assert!(
            (util - 0.95).abs() < 0.01,
            "emergency brake should use worst claim: got {util}"
        );
    }

    #[tokio::test]
    async fn stale_claim_eviction() {
        // Claim with expired reset should be ignored
        let now_epoch = AppState::now_epoch();
        let mut claims_7d = HashMap::new();
        claims_7d.insert(
            "seven_day_sonnet".to_string(),
            ClaimWindowData {
                utilization: Some(0.95),
                reset: Some(now_epoch - 1), // stale
                status: None,
                ..Default::default()
            },
        );
        claims_7d.insert(
            "seven_day_opus".to_string(),
            ClaimWindowData {
                utilization: Some(0.30),
                reset: Some(now_epoch + 100000), // fresh
                status: None,
                ..Default::default()
            },
        );
        let info = RateLimitInfo {
            utilization_5h: Some(0.20),
            reset_5h: Some(now_epoch + 10000),
            claims_7d,
            ..Default::default()
        };
        // For sonnet model: sonnet claim is stale, no general fallback → only 5h
        let (util_sonnet, source, _, _) =
            effective_utilization(&info, now_epoch, "claude-sonnet-4-6");
        assert_eq!(source, "5h");
        assert!(
            (util_sonnet - 0.20).abs() < 0.01,
            "stale sonnet should be ignored: got {util_sonnet}"
        );

        // For opus model: opus claim is fresh
        let (util_opus, source, _, _) = effective_utilization(&info, now_epoch, "claude-opus-4-6");
        assert_eq!(source, "7d");
        assert!(
            (util_opus - 0.30).abs() < 0.01,
            "fresh opus should be used: got {util_opus}"
        );
    }

    #[tokio::test]
    async fn model_family_extraction() {
        assert_eq!(model_family("claude-sonnet-4-6"), "sonnet");
        assert_eq!(model_family("claude-opus-4-6"), "opus");
        assert_eq!(model_family("claude-haiku-4-5"), "haiku");
        assert_eq!(model_family("claude-3-5-sonnet"), "sonnet");
        assert_eq!(model_family("unknown-model"), "");
    }

    #[tokio::test]
    async fn extract_client_version_parsing() {
        assert_eq!(
            extract_client_version("claude-cli/2.1.68 (external, cli)"),
            Some("2.1.68")
        );
        assert_eq!(extract_client_version("anthropic-sdk/1.0.0"), Some("1.0.0"));
        assert_eq!(extract_client_version("curl/8.5.0"), Some("8.5.0"));
        assert_eq!(extract_client_version("no-version"), None);
        assert_eq!(extract_client_version("foo/"), None);
    }

    #[tokio::test]
    async fn flat_field_compat_fallback() {
        // When claims_7d is empty, should fall back to flat utilization_7d fields
        let now_epoch = AppState::now_epoch();
        let info = RateLimitInfo {
            utilization_5h: Some(0.30),
            reset_5h: Some(now_epoch + 10000),
            utilization_7d: Some(0.60),
            reset_7d: Some(now_epoch + 100000),
            // claims_7d empty — migration/compat path
            ..Default::default()
        };
        let (util, source, _, _) = effective_utilization(&info, now_epoch, "claude-sonnet-4-6");
        assert_eq!(source, "7d");
        assert!((util - 0.60).abs() < 0.01, "should use flat 7d: got {util}");
    }

    #[tokio::test]
    async fn claim_aware_routing_prefers_low_model_util() {
        // Account A: Sonnet 7d at 0.90 (high), Opus 7d at 0.10 (low)
        // Account B: Sonnet 7d at 0.10 (low), Opus 7d at 0.90 (high)
        // Routing for Sonnet should prefer B, routing for Opus should prefer A
        let acct_a = make_account("a", "sk-ant-api-a");
        let acct_b = make_account("b", "sk-ant-api-b");
        let state = test_state_with(vec![acct_a, acct_b]);
        let now = AppState::now_epoch();

        // Set 5h low for both
        set_account_utilization(&state, 0, 0.10, 0.10, now + 10000, now + 100000).await;
        set_account_utilization(&state, 1, 0.10, 0.10, now + 10000, now + 100000).await;

        // Set model-specific 7d utilization
        set_model_utilization(&state, 0, "claude-sonnet-4-6", 0.90, now + 100000).await;
        set_model_utilization(&state, 0, "claude-opus-4-6", 0.10, now + 100000).await;
        set_model_utilization(&state, 1, "claude-sonnet-4-6", 0.10, now + 100000).await;
        set_model_utilization(&state, 1, "claude-opus-4-6", 0.90, now + 100000).await;

        // Run many picks — Sonnet routing should consistently prefer B (index 1)
        let mut sonnet_picks = [0u32; 2];
        let mut opus_picks = [0u32; 2];
        for i in 0..100 {
            let key = format!("client_{}", i);
            if let Some(idx) = state
                .pick_account(Some(&key), "claude-sonnet-4-6", &[])
                .await
            {
                sonnet_picks[idx] += 1;
            }
            if let Some(idx) = state.pick_account(Some(&key), "claude-opus-4-6", &[]).await {
                opus_picks[idx] += 1;
            }
        }
        // B should get most Sonnet traffic (has lower Sonnet util)
        assert!(
            sonnet_picks[1] > sonnet_picks[0],
            "Sonnet should prefer B: A={}, B={}",
            sonnet_picks[0],
            sonnet_picks[1]
        );
        // A should get most Opus traffic (has lower Opus util)
        assert!(
            opus_picks[0] > opus_picks[1],
            "Opus should prefer A: A={}, B={}",
            opus_picks[0],
            opus_picks[1]
        );
    }

    // ── pick_account waste_risk routing tests ─────────────────────

    #[tokio::test]
    async fn pick_account_prefers_expiring_quota() {
        // Account A: 7d=0.40, reset in 1 day → high waste_risk
        // Account B: 7d=0.40, reset in 6 days → low waste_risk
        let acct_a = make_account("a", "sk-ant-api-a");
        let acct_b = make_account("b", "sk-ant-api-b");
        let state = test_state_with(vec![acct_a, acct_b]);
        let now = AppState::now_epoch();

        // Both 5h at 0.30
        set_account_utilization(&state, 0, 0.30, 0.40, now + 10000, now + 86400).await;
        set_account_utilization(&state, 1, 0.30, 0.40, now + 10000, now + 518400).await;

        // Override claims with different resets
        {
            let mut info = state.accounts[0].rate_info.write().await;
            info.claims_7d.insert(
                "seven_day_sonnet".to_string(),
                ClaimWindowData {
                    utilization: Some(0.40),
                    reset: Some(now + 86400),
                    status: None,
                    ..Default::default()
                },
            );
        }
        {
            let mut info = state.accounts[1].rate_info.write().await;
            info.claims_7d.insert(
                "seven_day_sonnet".to_string(),
                ClaimWindowData {
                    utilization: Some(0.40),
                    reset: Some(now + 518400),
                    status: None,
                    ..Default::default()
                },
            );
        }

        let mut picks = [0u32; 2];
        for i in 0..200 {
            let key = format!("client_{}", i);
            if let Some(idx) = state
                .pick_account(Some(&key), "claude-sonnet-4-6", &[])
                .await
            {
                picks[idx] += 1;
            }
        }
        assert!(
            picks[0] > picks[1] * 2,
            "A (expiring) should get >2x traffic vs B: A={}, B={}",
            picks[0],
            picks[1]
        );
    }

    #[tokio::test]
    async fn pick_account_dampens_by_5h() {
        // Account A: high waste_risk but high 5h → dampened
        // Account B: lower waste_risk but low 5h → more traffic
        let acct_a = make_account("a", "sk-ant-api-a");
        let acct_b = make_account("b", "sk-ant-api-b");
        let state = test_state_with(vec![acct_a, acct_b]);
        let now = AppState::now_epoch();

        // A: 5h=0.85, 7d=0.20 (waste_risk ~5.0 with 1.5d remaining)
        set_account_utilization(&state, 0, 0.85, 0.20, now + 10000, now + 129600).await;
        {
            let mut info = state.accounts[0].rate_info.write().await;
            info.claims_7d.insert(
                "seven_day_sonnet".to_string(),
                ClaimWindowData {
                    utilization: Some(0.20),
                    reset: Some(now + 129600),
                    status: None,
                    ..Default::default()
                },
            );
        }

        // B: 5h=0.30, 7d=0.50 (waste_risk ~1.2 with 3.5d remaining)
        set_account_utilization(&state, 1, 0.30, 0.50, now + 10000, now + 302400).await;
        {
            let mut info = state.accounts[1].rate_info.write().await;
            info.claims_7d.insert(
                "seven_day_sonnet".to_string(),
                ClaimWindowData {
                    utilization: Some(0.50),
                    reset: Some(now + 302400),
                    status: None,
                    ..Default::default()
                },
            );
        }

        let mut picks = [0u32; 2];
        for i in 0..500 {
            let key = format!("client_{}", i);
            if let Some(idx) = state
                .pick_account(Some(&key), "claude-sonnet-4-6", &[])
                .await
            {
                picks[idx] += 1;
            }
        }
        // A: wr=0.80/0.2143=3.73, weight=3.73*0.15=0.56
        // B: wr=0.50/0.50=1.0, weight=1.0*0.70=0.70
        // B should get more (~55.6% share)
        assert!(
            picks[1] > picks[0],
            "B (low 5h) should get more traffic: A={}, B={}",
            picks[0],
            picks[1]
        );
    }

    #[tokio::test]
    async fn pick_account_fallback_no_7d_data() {
        // No 7d claims → falls back to headroom-only weighting
        // Uses affinity (sticky) traffic to verify weight-proportional distribution
        // across distinct session keys, exercising the affinity path.
        let acct_a = make_account("a", "sk-ant-api-a");
        let acct_b = make_account("b", "sk-ant-api-b");
        let state = test_state_with(vec![acct_a, acct_b]);
        let now = AppState::now_epoch();

        // Set 5h only, no claims_7d
        {
            let mut info = state.accounts[0].rate_info.write().await;
            info.utilization_5h = Some(0.30);
            info.reset_5h = Some(now + 10000);
            info.utilization = Some(0.30);
            info.claims_7d.clear();
        }
        {
            let mut info = state.accounts[1].rate_info.write().await;
            info.utilization_5h = Some(0.70);
            info.reset_5h = Some(now + 10000);
            info.utilization = Some(0.70);
            info.claims_7d.clear();
        }

        // Each distinct session key deterministically hashes into the weight-space;
        // 200 different keys should distribute ~70/30 matching A's higher headroom.
        let mut picks = [0u32; 2];
        for i in 0..200 {
            let key = format!("session-{}", i);
            if let Some(idx) = state
                .pick_account(Some(&key), "claude-sonnet-4-6", &[])
                .await
            {
                picks[idx] += 1;
            }
        }

        // Verify the affinity path returns Some for a well-formed session key
        let session = "10.42.0.1:client-test:agent-1:session-fallback";
        assert!(
            state
                .pick_account(Some(session), "claude-sonnet-4-6", &[])
                .await
                .is_some(),
            "affinity pick should return Some when accounts are available"
        );

        // A headroom=0.70, B headroom=0.30. A should get ~70% of 200 = ~140 picks.
        // Require at least 65% (130) to catch regressions while allowing hash variance.
        assert!(
            picks[0] >= 130,
            "expected A to get ~70% but got A={}, B={}",
            picks[0],
            picks[1]
        );
    }

    #[tokio::test]
    async fn emergency_brake_triggers_at_88() {
        // All accounts at 88% raw 7d → brake engages with new threshold
        let now = AppState::now_epoch();
        let acct_a = make_account("a", "sk-ant-api-a");
        let acct_b = make_account("b", "sk-ant-api-b");
        let state = test_state_with(vec![acct_a, acct_b]);

        // Verify DEFAULT_EMERGENCY_THRESHOLD is 0.88
        assert!(
            (DEFAULT_EMERGENCY_THRESHOLD - 0.88).abs() < 0.001,
            "default emergency threshold should be 0.88"
        );

        set_account_utilization(&state, 0, 0.89, 0.89, now + 10000, now + 100000).await;
        set_account_utilization(&state, 1, 0.89, 0.89, now + 10000, now + 100000).await;

        // effective_utilization for both should be >= 0.88
        let info0 = state.accounts[0].rate_info.read().await;
        let (util0, _, _, _) = effective_utilization(&info0, now, "");
        drop(info0);
        assert!(
            util0 >= DEFAULT_EMERGENCY_THRESHOLD,
            "util should be >= threshold: {util0}"
        );
    }

    #[tokio::test]
    async fn pick_account_all_7d_rejected_returns_none() {
        // If all accounts have rejected 7d claims for the model, pick_account returns None
        let acct_a = make_account("a", "sk-ant-api-a");
        let acct_b = make_account("b", "sk-ant-api-b");
        let state = test_state_with(vec![acct_a, acct_b]);
        let now = AppState::now_epoch();

        for idx in 0..2 {
            let mut info = state.accounts[idx].rate_info.write().await;
            info.utilization_5h = Some(0.30);
            info.reset_5h = Some(now + 10000);
            info.claims_7d.insert(
                "seven_day_sonnet".to_string(),
                ClaimWindowData {
                    utilization: Some(1.0),
                    reset: Some(now + 100000),
                    status: Some("rejected".to_string()),
                    ..Default::default()
                },
            );
        }

        let result = state
            .pick_account(Some("test"), "claude-sonnet-4-6", &[])
            .await;
        assert!(
            result.is_none(),
            "all-rejected should return None, got {:?}",
            result
        );
    }

    // ── Enforcement tests ──────────────────────────────────────────

    /// Helper: set up account utilization for enforcement tests.
    async fn set_account_utilization(
        state: &AppState,
        idx: usize,
        util_5h: f64,
        util_7d: f64,
        reset_5h: u64,
        reset_7d: u64,
    ) {
        let mut info = state.accounts[idx].rate_info.write().await;
        info.utilization_5h = Some(util_5h);
        info.utilization_7d = Some(util_7d);
        info.utilization = Some(util_5h.max(util_7d));
        info.reset_5h = Some(reset_5h);
        info.reset_7d = Some(reset_7d);
        // Populate claims_7d with a general "seven_day" entry
        info.claims_7d.insert(
            "seven_day".to_string(),
            ClaimWindowData {
                utilization: Some(util_7d),
                reset: Some(reset_7d),
                status: None,
                ..Default::default()
            },
        );
    }

    /// Helper: set per-model 7d utilization (e.g. "seven_day_sonnet").
    async fn set_model_utilization(
        state: &AppState,
        idx: usize,
        model: &str,
        util_7d: f64,
        reset_7d: u64,
    ) {
        let family = model_family(model);
        let key = if family.is_empty() {
            "seven_day".to_string()
        } else {
            format!("seven_day_{}", family)
        };
        let mut info = state.accounts[idx].rate_info.write().await;
        info.claims_7d.insert(
            key,
            ClaimWindowData {
                utilization: Some(util_7d),
                reset: Some(reset_7d),
                status: None,
                ..Default::default()
            },
        );
        // Re-derive flat fields
        info.utilization_7d = info
            .claims_7d
            .values()
            .filter_map(|c| c.utilization)
            .reduce(f64::max);
        info.reset_7d = info.claims_7d.values().filter_map(|c| c.reset).min();
        info.utilization = Some(
            info.utilization_5h
                .unwrap_or(0.0)
                .max(info.utilization_7d.unwrap_or(0.0)),
        );
    }

    #[tokio::test]
    async fn limit_all_below() {
        let now = AppState::now_epoch();
        let mut limits = HashMap::new();
        limits.insert("testclient".to_string(), 0.80);
        let state = Arc::new(AppState {
            client: Client::builder()
                .timeout(Duration::from_secs(5))
                .build()
                .unwrap(),
            upstream: "http://127.0.0.1:1".to_string(),
            accounts: vec![
                make_account("a", "sk-ant-api-x"),
                make_account("b", "sk-ant-api-y"),
            ],
            robin: AtomicUsize::new(0),
            routing_strategy: RoutingStrategy::default(),
            cooldown: Duration::from_secs(60),
            state_path: PathBuf::from("/tmp/test.state.json"),
            proxy_key: None,
            allowed_ips: vec![],
            upstreams: vec![],
            client_names: HashMap::new(),
            auto_cache: true,
            client_usage: Mutex::new(HashMap::new()),
            shadow_log_tx: None,
            shadow_log_dropped: AtomicU64::new(0),
            client_budgets: HashMap::new(),
            budget_usage: Mutex::new(HashMap::new()),
            client_utilization_limits: limits,
            operators: vec![],
            emergency_brake: true,
            emergency_threshold: DEFAULT_EMERGENCY_THRESHOLD,
            client_request_rates: Mutex::new(HashMap::new()),
            soft_limit: 1.0,
            redis: None,
            cluster_info_cache: Mutex::new(None),
            next_req_id: AtomicU64::new(0),
            instance_id: 0,
            probe_interval_secs: 300,
        });
        set_account_utilization(&state, 0, 0.50, 0.40, now + 10000, now + 100000).await;
        set_account_utilization(&state, 1, 0.60, 0.50, now + 10000, now + 100000).await;
        assert!(state
            .check_utilization_limit("testclient", "")
            .await
            .is_ok());
    }

    #[tokio::test]
    async fn limit_all_above() {
        let now = AppState::now_epoch();
        let mut limits = HashMap::new();
        limits.insert("testclient".to_string(), 0.50);
        let state = Arc::new(AppState {
            client: Client::builder()
                .timeout(Duration::from_secs(5))
                .build()
                .unwrap(),
            upstream: "http://127.0.0.1:1".to_string(),
            accounts: vec![
                make_account("a", "sk-ant-api-x"),
                make_account("b", "sk-ant-api-y"),
            ],
            robin: AtomicUsize::new(0),
            routing_strategy: RoutingStrategy::default(),
            cooldown: Duration::from_secs(60),
            state_path: PathBuf::from("/tmp/test.state.json"),
            proxy_key: None,
            allowed_ips: vec![],
            upstreams: vec![],
            client_names: HashMap::new(),
            auto_cache: true,
            client_usage: Mutex::new(HashMap::new()),
            shadow_log_tx: None,
            shadow_log_dropped: AtomicU64::new(0),
            client_budgets: HashMap::new(),
            budget_usage: Mutex::new(HashMap::new()),
            client_utilization_limits: limits,
            operators: vec![],
            emergency_brake: true,
            emergency_threshold: DEFAULT_EMERGENCY_THRESHOLD,
            client_request_rates: Mutex::new(HashMap::new()),
            soft_limit: 1.0,
            redis: None,
            cluster_info_cache: Mutex::new(None),
            next_req_id: AtomicU64::new(0),
            instance_id: 0,
            probe_interval_secs: 300,
        });
        set_account_utilization(&state, 0, 0.80, 0.70, now + 10000, now + 100000).await;
        set_account_utilization(&state, 1, 0.90, 0.80, now + 10000, now + 100000).await;
        let result = state.check_utilization_limit("testclient", "").await;
        assert!(result.is_err(), "all above limit should return Err");
        let retry = result.unwrap_err();
        assert!(retry >= 60, "retry-after should be >= 60");
        assert!(retry <= 3600, "retry-after should be <= 3600");
    }

    #[tokio::test]
    async fn limit_one_below() {
        let now = AppState::now_epoch();
        let mut limits = HashMap::new();
        limits.insert("testclient".to_string(), 0.70);
        let state = Arc::new(AppState {
            client: Client::builder()
                .timeout(Duration::from_secs(5))
                .build()
                .unwrap(),
            upstream: "http://127.0.0.1:1".to_string(),
            accounts: vec![
                make_account("a", "sk-ant-api-x"),
                make_account("b", "sk-ant-api-y"),
            ],
            robin: AtomicUsize::new(0),
            routing_strategy: RoutingStrategy::default(),
            cooldown: Duration::from_secs(60),
            state_path: PathBuf::from("/tmp/test.state.json"),
            proxy_key: None,
            allowed_ips: vec![],
            upstreams: vec![],
            client_names: HashMap::new(),
            auto_cache: true,
            client_usage: Mutex::new(HashMap::new()),
            shadow_log_tx: None,
            shadow_log_dropped: AtomicU64::new(0),
            client_budgets: HashMap::new(),
            budget_usage: Mutex::new(HashMap::new()),
            client_utilization_limits: limits,
            operators: vec![],
            emergency_brake: true,
            emergency_threshold: DEFAULT_EMERGENCY_THRESHOLD,
            client_request_rates: Mutex::new(HashMap::new()),
            soft_limit: 1.0,
            redis: None,
            cluster_info_cache: Mutex::new(None),
            next_req_id: AtomicU64::new(0),
            instance_id: 0,
            probe_interval_secs: 300,
        });
        set_account_utilization(&state, 0, 0.90, 0.80, now + 10000, now + 100000).await;
        set_account_utilization(&state, 1, 0.50, 0.40, now + 10000, now + 100000).await;
        assert!(state
            .check_utilization_limit("testclient", "")
            .await
            .is_ok());
    }

    #[tokio::test]
    async fn limit_no_config() {
        let state = test_state_with(vec![make_account("a", "sk-ant-api-x")]);
        assert!(state.check_utilization_limit("anyone", "").await.is_ok());
    }

    #[tokio::test]
    async fn limit_operator_bypass() {
        let now = AppState::now_epoch();
        let mut limits = HashMap::new();
        limits.insert("ray".to_string(), 0.10); // very low limit
        let state = Arc::new(AppState {
            client: Client::builder()
                .timeout(Duration::from_secs(5))
                .build()
                .unwrap(),
            upstream: "http://127.0.0.1:1".to_string(),
            accounts: vec![make_account("a", "sk-ant-api-x")],
            robin: AtomicUsize::new(0),
            routing_strategy: RoutingStrategy::default(),
            cooldown: Duration::from_secs(60),
            state_path: PathBuf::from("/tmp/test.state.json"),
            proxy_key: None,
            allowed_ips: vec![],
            upstreams: vec![],
            client_names: HashMap::new(),
            auto_cache: true,
            client_usage: Mutex::new(HashMap::new()),
            shadow_log_tx: None,
            shadow_log_dropped: AtomicU64::new(0),
            client_budgets: HashMap::new(),
            budget_usage: Mutex::new(HashMap::new()),
            client_utilization_limits: limits,
            operators: vec!["ray".to_string()],
            emergency_brake: true,
            emergency_threshold: DEFAULT_EMERGENCY_THRESHOLD,
            client_request_rates: Mutex::new(HashMap::new()),
            soft_limit: 1.0,
            redis: None,
            cluster_info_cache: Mutex::new(None),
            next_req_id: AtomicU64::new(0),
            instance_id: 0,
            probe_interval_secs: 300,
        });
        set_account_utilization(&state, 0, 0.95, 0.90, now + 10000, now + 100000).await;
        // Operator bypasses everything
        assert!(state.is_operator("ray"));
        assert!(state.pre_request_gate("ray", "").await.is_ok());
        // Non-operator does not bypass
        assert!(!state.is_operator("gastown"));
    }

    #[tokio::test]
    async fn limit_no_compatible_accounts_passes() {
        // When no account serves the requested model, check_utilization_limit
        // should return Ok (let pick_account handle the "no account" error later)
        let mut limits = HashMap::new();
        limits.insert("test-client".to_string(), 0.01); // very low limit
        let mut acct = make_account("a", "sk-ant-api-x");
        acct.models = vec!["claude-sonnet".to_string()]; // only serves sonnet
        let state = Arc::new(AppState {
            client: Client::builder()
                .timeout(Duration::from_secs(5))
                .build()
                .unwrap(),
            upstream: "http://127.0.0.1:1".to_string(),
            accounts: vec![acct],
            robin: AtomicUsize::new(0),
            routing_strategy: RoutingStrategy::default(),
            cooldown: Duration::from_secs(60),
            state_path: PathBuf::from("/tmp/test.state.json"),
            proxy_key: None,
            allowed_ips: vec![],
            upstreams: vec![],
            client_names: HashMap::new(),
            auto_cache: true,
            client_usage: Mutex::new(HashMap::new()),
            shadow_log_tx: None,
            shadow_log_dropped: AtomicU64::new(0),
            client_budgets: HashMap::new(),
            budget_usage: Mutex::new(HashMap::new()),
            client_utilization_limits: limits,
            operators: vec![],
            emergency_brake: true,
            emergency_threshold: DEFAULT_EMERGENCY_THRESHOLD,
            client_request_rates: Mutex::new(HashMap::new()),
            soft_limit: 1.0,
            redis: None,
            cluster_info_cache: Mutex::new(None),
            next_req_id: AtomicU64::new(0),
            instance_id: 0,
            probe_interval_secs: 300,
        });
        // Request for "claude-opus" — no account serves it → should pass
        assert!(
            state
                .check_utilization_limit("test-client", "claude-opus")
                .await
                .is_ok(),
            "should not 429 when no account serves the requested model"
        );
    }

    #[tokio::test]
    async fn limit_unknown_accounts_fail_open() {
        // Accounts with no rate data (source="unknown") should not trigger the limit gate
        let mut limits = HashMap::new();
        limits.insert("testclient".to_string(), 0.30); // below the 0.5 unknown default
        let state = Arc::new(AppState {
            client: Client::builder()
                .timeout(Duration::from_secs(5))
                .build()
                .unwrap(),
            upstream: "http://127.0.0.1:1".to_string(),
            accounts: vec![
                make_account("a", "sk-ant-api-x"),
                make_account("b", "sk-ant-api-y"),
            ],
            robin: AtomicUsize::new(0),
            routing_strategy: RoutingStrategy::default(),
            cooldown: Duration::from_secs(60),
            state_path: PathBuf::from("/tmp/test.state.json"),
            proxy_key: None,
            allowed_ips: vec![],
            upstreams: vec![],
            client_names: HashMap::new(),
            auto_cache: true,
            client_usage: Mutex::new(HashMap::new()),
            shadow_log_tx: None,
            shadow_log_dropped: AtomicU64::new(0),
            client_budgets: HashMap::new(),
            budget_usage: Mutex::new(HashMap::new()),
            client_utilization_limits: limits,
            operators: vec![],
            emergency_brake: true,
            emergency_threshold: DEFAULT_EMERGENCY_THRESHOLD,
            client_request_rates: Mutex::new(HashMap::new()),
            soft_limit: 1.0,
            redis: None,
            cluster_info_cache: Mutex::new(None),
            next_req_id: AtomicU64::new(0),
            instance_id: 0,
            probe_interval_secs: 300,
        });
        // Don't set any utilization — accounts remain "unknown" (0.5)
        // With limit=0.30, unknown 0.5 would appear "above limit" without fail-open
        assert!(
            state
                .check_utilization_limit("testclient", "")
                .await
                .is_ok(),
            "should fail-open when all accounts have unknown utilization"
        );
    }

    #[tokio::test]
    async fn limit_mixed_known_unknown_fails_open() {
        // Known accounts above limit + one unknown account → should NOT 429
        // The unknown account may have capacity; let pick_account route to it
        let now = AppState::now_epoch();
        let mut limits = HashMap::new();
        limits.insert("testclient".to_string(), 0.50);
        let state = Arc::new(AppState {
            client: Client::builder()
                .timeout(Duration::from_secs(5))
                .build()
                .unwrap(),
            upstream: "http://127.0.0.1:1".to_string(),
            accounts: vec![
                make_account("a", "sk-ant-api-x"),
                make_account("b", "sk-ant-api-y"),
                make_account("c", "sk-ant-api-z"),
            ],
            robin: AtomicUsize::new(0),
            routing_strategy: RoutingStrategy::default(),
            cooldown: Duration::from_secs(60),
            state_path: PathBuf::from("/tmp/test.state.json"),
            proxy_key: None,
            allowed_ips: vec![],
            upstreams: vec![],
            client_names: HashMap::new(),
            auto_cache: true,
            client_usage: Mutex::new(HashMap::new()),
            shadow_log_tx: None,
            shadow_log_dropped: AtomicU64::new(0),
            client_budgets: HashMap::new(),
            budget_usage: Mutex::new(HashMap::new()),
            client_utilization_limits: limits,
            operators: vec![],
            emergency_brake: true,
            emergency_threshold: DEFAULT_EMERGENCY_THRESHOLD,
            client_request_rates: Mutex::new(HashMap::new()),
            soft_limit: 1.0,
            redis: None,
            cluster_info_cache: Mutex::new(None),
            next_req_id: AtomicU64::new(0),
            instance_id: 0,
            probe_interval_secs: 300,
        });
        // Two known accounts above limit, one unknown (no data set)
        set_account_utilization(&state, 0, 0.80, 0.70, now + 10000, now + 100000).await;
        set_account_utilization(&state, 1, 0.90, 0.80, now + 10000, now + 100000).await;
        // Account "c" has no rate data → unknown
        assert!(
            state
                .check_utilization_limit("testclient", "")
                .await
                .is_ok(),
            "should fail-open when unknown compatible account may have capacity"
        );
    }

    #[tokio::test]
    async fn emergency_all_above_threshold() {
        let now = AppState::now_epoch();
        let state = test_state_with(vec![
            make_account("a", "sk-ant-api-x"),
            make_account("b", "sk-ant-api-y"),
        ]);
        set_account_utilization(&state, 0, 0.96, 0.90, now + 10000, now + 100000).await;
        set_account_utilization(&state, 1, 0.97, 0.95, now + 10000, now + 100000).await;
        assert!(state.is_emergency_brake_active().await);
    }

    #[tokio::test]
    async fn emergency_one_below() {
        let now = AppState::now_epoch();
        let state = test_state_with(vec![
            make_account("a", "sk-ant-api-x"),
            make_account("b", "sk-ant-api-y"),
        ]);
        set_account_utilization(&state, 0, 0.96, 0.90, now + 10000, now + 100000).await;
        set_account_utilization(&state, 1, 0.80, 0.70, now + 10000, now + 100000).await;
        assert!(!state.is_emergency_brake_active().await);
    }

    #[tokio::test]
    async fn emergency_operator_bypass() {
        let now = AppState::now_epoch();
        let mut limits = HashMap::new();
        limits.insert("ray".to_string(), 0.10);
        let state = Arc::new(AppState {
            client: Client::builder()
                .timeout(Duration::from_secs(5))
                .build()
                .unwrap(),
            upstream: "http://127.0.0.1:1".to_string(),
            accounts: vec![make_account("a", "sk-ant-api-x")],
            robin: AtomicUsize::new(0),
            routing_strategy: RoutingStrategy::default(),
            cooldown: Duration::from_secs(60),
            state_path: PathBuf::from("/tmp/test.state.json"),
            proxy_key: None,
            allowed_ips: vec![],
            upstreams: vec![],
            client_names: HashMap::new(),
            auto_cache: true,
            client_usage: Mutex::new(HashMap::new()),
            shadow_log_tx: None,
            shadow_log_dropped: AtomicU64::new(0),
            client_budgets: HashMap::new(),
            budget_usage: Mutex::new(HashMap::new()),
            client_utilization_limits: limits,
            operators: vec!["ray".to_string()],
            emergency_brake: true,
            emergency_threshold: DEFAULT_EMERGENCY_THRESHOLD,
            client_request_rates: Mutex::new(HashMap::new()),
            soft_limit: 1.0,
            redis: None,
            cluster_info_cache: Mutex::new(None),
            next_req_id: AtomicU64::new(0),
            instance_id: 0,
            probe_interval_secs: 300,
        });
        set_account_utilization(&state, 0, 0.98, 0.96, now + 10000, now + 100000).await;
        assert!(state.is_emergency_brake_active().await);
        // Operator bypasses pre_request_gate even during emergency
        assert!(state.pre_request_gate("ray", "").await.is_ok());
        // Non-operator gets blocked
        assert!(state.pre_request_gate("gastown", "").await.is_err());
    }

    #[tokio::test]
    async fn emergency_no_data() {
        // All accounts have default (0.5, "unknown") — brake should NOT activate (fail-open)
        let state = test_state_with(vec![
            make_account("a", "sk-ant-api-x"),
            make_account("b", "sk-ant-api-y"),
        ]);
        assert!(
            !state.is_emergency_brake_active().await,
            "brake should fail-open with no data"
        );
    }

    #[tokio::test]
    async fn emergency_stale_data_with_unified() {
        // Stale reset times but valid unified utilization at 0.97
        let state = test_state_with(vec![make_account("a", "sk-ant-api-x")]);
        {
            let mut info = state.accounts[0].rate_info.write().await;
            // Resets in the past → stale per-window data
            info.utilization_5h = Some(0.97);
            info.reset_5h = Some(1);
            info.utilization_7d = Some(0.97);
            info.reset_7d = Some(1);
            // But unified utilization is valid
            info.utilization = Some(0.97);
        }
        assert!(
            state.is_emergency_brake_active().await,
            "unified fallback should count"
        );
    }

    #[tokio::test]
    async fn emergency_configurable_threshold() {
        let now = AppState::now_epoch();
        let state = Arc::new(AppState {
            client: Client::builder()
                .timeout(Duration::from_secs(5))
                .build()
                .unwrap(),
            upstream: "http://127.0.0.1:1".to_string(),
            accounts: vec![make_account("a", "sk-ant-api-x")],
            robin: AtomicUsize::new(0),
            routing_strategy: RoutingStrategy::default(),
            cooldown: Duration::from_secs(60),
            state_path: PathBuf::from("/tmp/test.state.json"),
            proxy_key: None,
            allowed_ips: vec![],
            upstreams: vec![],
            client_names: HashMap::new(),
            auto_cache: true,
            client_usage: Mutex::new(HashMap::new()),
            shadow_log_tx: None,
            shadow_log_dropped: AtomicU64::new(0),
            client_budgets: HashMap::new(),
            budget_usage: Mutex::new(HashMap::new()),
            client_utilization_limits: HashMap::new(),
            operators: vec![],
            emergency_brake: true,
            emergency_threshold: 0.80, // custom low threshold
            client_request_rates: Mutex::new(HashMap::new()),
            soft_limit: 1.0,
            redis: None,
            cluster_info_cache: Mutex::new(None),
            next_req_id: AtomicU64::new(0),
            instance_id: 0,
            probe_interval_secs: 300,
        });
        set_account_utilization(&state, 0, 0.85, 0.70, now + 10000, now + 100000).await;
        assert!(
            state.is_emergency_brake_active().await,
            "0.85 should exceed custom 0.80 threshold"
        );
    }

    // ── Emergency brake: known/unknown interaction tests ──
    // These tests document the quota-maximization design intent:
    // Unknown accounts (no rate data) return (0.5, "unknown") from effective_utilization.
    // The brake does NOT fire when unknown accounts exist because:
    //   (a) 0.5 < 0.88 default threshold → all_above = false, OR
    //   (b) even if threshold is low enough, any_known = false blocks activation.
    // This is intentional: unknown accounts might have capacity, and activating
    // the brake blocks ALL non-operator traffic — a blunt instrument that wastes quota.

    #[tokio::test]
    async fn emergency_mixed_known_above_plus_unknown_preserves_capacity() {
        // Two known accounts above threshold + one unknown (no data).
        // Unknown returns (0.5, "unknown") — its 0.5 < 0.88 breaks the all_above check.
        // Brake stays inactive: the unknown account might have available quota.
        let now = AppState::now_epoch();
        let state = test_state_with(vec![
            make_account("known-a", "sk-ant-api-a"),
            make_account("known-b", "sk-ant-api-b"),
            make_account("unknown-c", "sk-ant-api-c"), // no rate data set
        ]);
        set_account_utilization(&state, 0, 0.96, 0.92, now + 10000, now + 100000).await;
        set_account_utilization(&state, 1, 0.95, 0.91, now + 10000, now + 100000).await;
        // Account 2: no data → effective_utilization returns (0.5, "unknown")
        assert!(
            !state.is_emergency_brake_active().await,
            "brake must not fire: unknown account at 0.5 < 0.88 threshold means potential capacity"
        );
    }

    #[tokio::test]
    async fn emergency_mixed_known_below_plus_unknown_inactive() {
        // Some known above, some known below, plus an unknown. Brake inactive on multiple grounds.
        let now = AppState::now_epoch();
        let state = test_state_with(vec![
            make_account("high", "sk-ant-api-a"),
            make_account("low", "sk-ant-api-b"),
            make_account("unknown", "sk-ant-api-c"),
        ]);
        set_account_utilization(&state, 0, 0.96, 0.92, now + 10000, now + 100000).await;
        set_account_utilization(&state, 1, 0.50, 0.40, now + 10000, now + 100000).await;
        // Account 2: no data
        assert!(
            !state.is_emergency_brake_active().await,
            "brake must not fire: known account below threshold + unknown account"
        );
    }

    #[tokio::test]
    async fn emergency_unknown_with_low_threshold_still_fails_open() {
        // Edge case: threshold set to 0.4, below the unknown default of 0.5.
        // Unknown's 0.5 >= 0.4 so all_above stays true, BUT any_known is false.
        // The any_known guard prevents firing — fail-open even with aggressive threshold.
        let state = Arc::new(AppState {
            client: Client::builder()
                .timeout(Duration::from_secs(5))
                .build()
                .unwrap(),
            upstream: "http://127.0.0.1:1".to_string(),
            accounts: vec![make_account("mystery", "sk-ant-api-x")],
            robin: AtomicUsize::new(0),
            routing_strategy: RoutingStrategy::default(),
            cooldown: Duration::from_secs(60),
            state_path: PathBuf::from("/tmp/test.state.json"),
            proxy_key: None,
            allowed_ips: vec![],
            upstreams: vec![],
            client_names: HashMap::new(),
            auto_cache: true,
            client_usage: Mutex::new(HashMap::new()),
            shadow_log_tx: None,
            shadow_log_dropped: AtomicU64::new(0),
            client_budgets: HashMap::new(),
            budget_usage: Mutex::new(HashMap::new()),
            client_utilization_limits: HashMap::new(),
            operators: vec![],
            emergency_brake: true,
            emergency_threshold: 0.40, // below unknown's default 0.5
            client_request_rates: Mutex::new(HashMap::new()),
            soft_limit: 1.0,
            redis: None,
            cluster_info_cache: Mutex::new(None),
            next_req_id: AtomicU64::new(0),
            instance_id: 0,
            probe_interval_secs: 300,
        });
        // No rate data set — account returns (0.5, "unknown")
        // 0.5 >= 0.4 threshold → all_above is true
        // But any_known is false → brake does NOT fire
        assert!(
            !state.is_emergency_brake_active().await,
            "brake must fail-open: no known accounts even though unknown's 0.5 exceeds 0.40 threshold"
        );
    }

    #[tokio::test]
    async fn emergency_mixed_known_above_low_threshold_plus_unknown_fires() {
        // Converse of above: threshold 0.4, one KNOWN account at 0.6, one unknown at default 0.5.
        // Both 0.6 >= 0.4 and 0.5 >= 0.4 → all_above = true.
        // Known account exists → any_known = true.
        // Brake fires. This is correct because we have real data showing distress.
        let now = AppState::now_epoch();
        let state = Arc::new(AppState {
            client: Client::builder()
                .timeout(Duration::from_secs(5))
                .build()
                .unwrap(),
            upstream: "http://127.0.0.1:1".to_string(),
            accounts: vec![
                make_account("known", "sk-ant-api-a"),
                make_account("unknown", "sk-ant-api-b"),
            ],
            robin: AtomicUsize::new(0),
            routing_strategy: RoutingStrategy::default(),
            cooldown: Duration::from_secs(60),
            state_path: PathBuf::from("/tmp/test.state.json"),
            proxy_key: None,
            allowed_ips: vec![],
            upstreams: vec![],
            client_names: HashMap::new(),
            auto_cache: true,
            client_usage: Mutex::new(HashMap::new()),
            shadow_log_tx: None,
            shadow_log_dropped: AtomicU64::new(0),
            client_budgets: HashMap::new(),
            budget_usage: Mutex::new(HashMap::new()),
            client_utilization_limits: HashMap::new(),
            operators: vec![],
            emergency_brake: true,
            emergency_threshold: 0.40,
            client_request_rates: Mutex::new(HashMap::new()),
            soft_limit: 1.0,
            redis: None,
            cluster_info_cache: Mutex::new(None),
            next_req_id: AtomicU64::new(0),
            instance_id: 0,
            probe_interval_secs: 300,
        });
        set_account_utilization(&state, 0, 0.60, 0.55, now + 10000, now + 100000).await;
        // Account 1: no data → (0.5, "unknown"), 0.5 >= 0.4 → all_above stays true
        assert!(
            state.is_emergency_brake_active().await,
            "brake should fire: known account above 0.40 threshold + unknown's 0.5 also above"
        );
    }

    #[tokio::test]
    async fn emergency_all_known_at_exact_threshold_fires() {
        // Boundary: accounts at exactly the threshold. The check is `util < threshold`,
        // so util == threshold means NOT below → all_above stays true → brake fires.
        let now = AppState::now_epoch();
        let state = test_state_with(vec![
            make_account("a", "sk-ant-api-a"),
            make_account("b", "sk-ant-api-b"),
        ]);
        set_account_utilization(&state, 0, 0.88, 0.88, now + 10000, now + 100000).await;
        set_account_utilization(&state, 1, 0.88, 0.88, now + 10000, now + 100000).await;
        assert!(
            state.is_emergency_brake_active().await,
            "at exactly threshold (0.88): util is NOT < threshold, so brake should fire"
        );
    }

    #[tokio::test]
    async fn emergency_one_known_just_below_threshold_inactive() {
        // Boundary: one account at threshold - epsilon. Just below → all_above = false.
        let now = AppState::now_epoch();
        let state = test_state_with(vec![
            make_account("a", "sk-ant-api-a"),
            make_account("b", "sk-ant-api-b"),
        ]);
        set_account_utilization(&state, 0, 0.96, 0.92, now + 10000, now + 100000).await;
        set_account_utilization(&state, 1, 0.879, 0.85, now + 10000, now + 100000).await;
        assert!(
            !state.is_emergency_brake_active().await,
            "0.879 < 0.88 threshold: brake should not fire"
        );
    }

    #[tokio::test]
    async fn emergency_single_known_above_threshold_fires() {
        // Single account fleet, known and above threshold → brake fires.
        let now = AppState::now_epoch();
        let state = test_state_with(vec![make_account("solo", "sk-ant-api-x")]);
        set_account_utilization(&state, 0, 0.95, 0.92, now + 10000, now + 100000).await;
        assert!(
            state.is_emergency_brake_active().await,
            "single known account above threshold: brake should fire"
        );
    }

    #[tokio::test]
    async fn emergency_single_unknown_account_fails_open() {
        // Single unknown account — both guards prevent activation:
        // 0.5 < 0.88 → all_above = false, AND any_known = false.
        let state = test_state_with(vec![make_account("solo", "sk-ant-api-x")]);
        assert!(
            !state.is_emergency_brake_active().await,
            "single unknown account: must fail-open"
        );
    }

    #[tokio::test]
    async fn gate_unknown_client_not_operator() {
        let state = Arc::new(AppState {
            client: Client::builder()
                .timeout(Duration::from_secs(5))
                .build()
                .unwrap(),
            upstream: "http://127.0.0.1:1".to_string(),
            accounts: vec![make_account("a", "sk-ant-api-x")],
            robin: AtomicUsize::new(0),
            routing_strategy: RoutingStrategy::default(),
            cooldown: Duration::from_secs(60),
            state_path: PathBuf::from("/tmp/test.state.json"),
            proxy_key: None,
            allowed_ips: vec![],
            upstreams: vec![],
            client_names: HashMap::new(),
            auto_cache: true,
            client_usage: Mutex::new(HashMap::new()),
            shadow_log_tx: None,
            shadow_log_dropped: AtomicU64::new(0),
            client_budgets: HashMap::new(),
            budget_usage: Mutex::new(HashMap::new()),
            client_utilization_limits: HashMap::new(),
            operators: vec!["ray".to_string()],
            emergency_brake: true,
            emergency_threshold: DEFAULT_EMERGENCY_THRESHOLD,
            client_request_rates: Mutex::new(HashMap::new()),
            soft_limit: 1.0,
            redis: None,
            cluster_info_cache: Mutex::new(None),
            next_req_id: AtomicU64::new(0),
            instance_id: 0,
            probe_interval_secs: 300,
        });
        // "-" is not the operator
        assert!(!state.is_operator("-"));
        assert!(!state.is_operator("gastown"));
        assert!(state.is_operator("ray"));
    }

    // ── compute_pressure_status tests ──

    #[test]
    fn pressure_status_healthy() {
        let state = test_state_with(vec![]);
        assert_eq!(compute_pressure_status(0.0, "client1", &state), "healthy");
        assert_eq!(compute_pressure_status(0.50, "client1", &state), "healthy");
        assert_eq!(compute_pressure_status(0.69, "client1", &state), "healthy");
    }

    #[test]
    fn pressure_status_elevated() {
        let state = test_state_with(vec![]);
        assert_eq!(compute_pressure_status(0.70, "client1", &state), "elevated");
        assert_eq!(compute_pressure_status(0.80, "client1", &state), "elevated");
        assert_eq!(compute_pressure_status(0.84, "client1", &state), "elevated");
    }

    #[test]
    fn pressure_status_critical() {
        let state = test_state_with(vec![]);
        assert_eq!(compute_pressure_status(0.85, "client1", &state), "critical");
        assert_eq!(compute_pressure_status(0.90, "client1", &state), "critical");
        assert_eq!(compute_pressure_status(0.94, "client1", &state), "critical");
    }

    #[test]
    fn pressure_status_emergency() {
        let state = test_state_with(vec![]);
        assert_eq!(
            compute_pressure_status(0.95, "client1", &state),
            "emergency"
        );
        assert_eq!(compute_pressure_status(1.0, "client1", &state), "emergency");
    }

    #[test]
    fn pressure_status_operator_always_healthy() {
        let state = Arc::new(AppState {
            client: Client::builder()
                .timeout(Duration::from_secs(5))
                .build()
                .unwrap(),
            upstream: "http://127.0.0.1:1".to_string(),
            accounts: vec![],
            robin: AtomicUsize::new(0),
            routing_strategy: RoutingStrategy::default(),
            cooldown: Duration::from_secs(60),
            state_path: PathBuf::from("/tmp/test.state.json"),
            proxy_key: None,
            allowed_ips: vec![],
            upstreams: vec![],
            client_names: HashMap::new(),
            auto_cache: true,
            client_usage: Mutex::new(HashMap::new()),
            shadow_log_tx: None,
            shadow_log_dropped: AtomicU64::new(0),
            client_budgets: HashMap::new(),
            budget_usage: Mutex::new(HashMap::new()),
            client_utilization_limits: HashMap::new(),
            operators: vec!["ray".to_string()],
            emergency_brake: true,
            emergency_threshold: DEFAULT_EMERGENCY_THRESHOLD,
            client_request_rates: Mutex::new(HashMap::new()),
            soft_limit: 1.0,
            redis: None,
            cluster_info_cache: Mutex::new(None),
            next_req_id: AtomicU64::new(0),
            instance_id: 0,
            probe_interval_secs: 300,
        });
        // Operator always gets "healthy" regardless of utilization
        assert_eq!(compute_pressure_status(0.99, "ray", &state), "healthy");
        assert_eq!(compute_pressure_status(1.0, "ray", &state), "healthy");
        // Non-operator at same utilization gets emergency
        assert_eq!(compute_pressure_status(0.99, "other", &state), "emergency");
    }

    #[test]
    fn pressure_status_upgrade_near_client_limit() {
        let mut limits = HashMap::new();
        limits.insert("gastown".to_string(), 0.85);
        let state = Arc::new(AppState {
            client: Client::builder()
                .timeout(Duration::from_secs(5))
                .build()
                .unwrap(),
            upstream: "http://127.0.0.1:1".to_string(),
            accounts: vec![],
            robin: AtomicUsize::new(0),
            routing_strategy: RoutingStrategy::default(),
            cooldown: Duration::from_secs(60),
            state_path: PathBuf::from("/tmp/test.state.json"),
            proxy_key: None,
            allowed_ips: vec![],
            upstreams: vec![],
            client_names: HashMap::new(),
            auto_cache: true,
            client_usage: Mutex::new(HashMap::new()),
            shadow_log_tx: None,
            shadow_log_dropped: AtomicU64::new(0),
            client_budgets: HashMap::new(),
            budget_usage: Mutex::new(HashMap::new()),
            client_utilization_limits: limits,
            operators: vec![],
            emergency_brake: true,
            emergency_threshold: DEFAULT_EMERGENCY_THRESHOLD,
            client_request_rates: Mutex::new(HashMap::new()),
            soft_limit: 1.0,
            redis: None,
            cluster_info_cache: Mutex::new(None),
            next_req_id: AtomicU64::new(0),
            instance_id: 0,
            probe_interval_secs: 300,
        });
        // gastown has limit 0.85, 80% of that = 0.68
        // At 0.60, below 0.68 → no upgrade → "healthy"
        assert_eq!(compute_pressure_status(0.60, "gastown", &state), "healthy");
        // At 0.69, above 0.68 → upgrade healthy→elevated
        assert_eq!(compute_pressure_status(0.69, "gastown", &state), "elevated");
        // At 0.70, already elevated, above 0.68 → upgrade elevated→critical
        assert_eq!(compute_pressure_status(0.70, "gastown", &state), "critical");
        // Client without limits: no upgrade
        assert_eq!(compute_pressure_status(0.69, "other", &state), "healthy");
    }

    // ── Integration tests for x-budget-status header ──

    #[tokio::test]
    async fn response_includes_budget_status_header() {
        let (mock_url, _handle) = spawn_mock_upstream().await;
        let (app, _state) = test_app(&mock_url, Some("test-key".to_string()));

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(
                listener,
                app.into_make_service_with_connect_info::<SocketAddr>(),
            )
            .await
            .unwrap();
        });

        let client = Client::new();
        let resp = client
            .post(format!("http://{}/v1/messages", addr))
            .header("content-type", "application/json")
            .header("x-api-key", "test-key")
            .body(r#"{"model":"test","max_tokens":1,"messages":[{"role":"user","content":"hi"}]}"#)
            .send()
            .await
            .unwrap();

        assert_eq!(resp.status(), reqwest::StatusCode::OK);
        let budget_status = resp
            .headers()
            .get("x-budget-status")
            .expect("x-budget-status header should be present on proxy response");
        // Mock returns low utilization (0.25) → healthy
        assert_eq!(budget_status.to_str().unwrap(), "healthy");
    }

    #[tokio::test]
    async fn openai_response_includes_budget_status_header() {
        let (mock_url, _handle) = spawn_mock_upstream().await;
        let (app, _state) = test_openai_app(&mock_url, Some("test-key".to_string()));

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(
                listener,
                app.into_make_service_with_connect_info::<SocketAddr>(),
            )
            .await
            .unwrap();
        });

        let client = Client::new();
        let resp = client
            .post(format!("http://{}/v1/chat/completions", addr))
            .header("content-type", "application/json")
            .header("x-api-key", "test-key")
            .body(r#"{"model":"test","max_tokens":1,"messages":[{"role":"user","content":"hi"}]}"#)
            .send()
            .await
            .unwrap();

        assert_eq!(resp.status(), reqwest::StatusCode::OK);
        let budget_status = resp
            .headers()
            .get("x-budget-status")
            .expect("x-budget-status header should be present on openai-compat response");
        assert_eq!(budget_status.to_str().unwrap(), "healthy");
    }

    // ── Stats handler extension tests ──

    #[tokio::test]
    async fn stats_includes_burn_rate_and_headroom() {
        let (mock_url, _handle) = spawn_mock_upstream().await;
        let (app, _state) = test_app(&mock_url, Some("test-key".to_string()));

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(
                listener,
                app.into_make_service_with_connect_info::<SocketAddr>(),
            )
            .await
            .unwrap();
        });

        let client = Client::new();

        // Send a request to populate rate info and burn rate
        let _ = client
            .post(format!("http://{}/v1/messages", addr))
            .header("content-type", "application/json")
            .header("x-api-key", "test-key")
            .body(r#"{"model":"test","max_tokens":1,"messages":[{"role":"user","content":"hi"}]}"#)
            .send()
            .await
            .unwrap();

        // Now check stats
        let resp = client
            .get(format!("http://{}/_stats", addr))
            .header("x-api-key", "test-key")
            .send()
            .await
            .unwrap();

        assert_eq!(resp.status(), reqwest::StatusCode::OK);
        let body: serde_json::Value = resp.json().await.unwrap();
        let accounts = body["accounts"].as_array().unwrap();
        assert!(!accounts.is_empty());

        // burn_rate object should exist on every account
        let acct = &accounts[0];
        assert!(
            acct["burn_rate"].is_object(),
            "burn_rate should be an object"
        );
        assert!(acct["burn_rate"]["last_5m"].is_number());
        assert!(acct["burn_rate"]["last_1h"].is_number());
        assert!(acct["burn_rate"]["last_6h"].is_number());

        // headroom_requests: mock doesn't return remaining_requests or limit_requests,
        // so headroom is null (both inputs absent). That's the expected behavior.
        // The field should still exist in the JSON output.
        assert!(
            acct.get("headroom_requests").is_some(),
            "headroom_requests field should be present"
        );

        // projected_throttle_at should be present (null or string depending on utilization)
        assert!(
            acct["projected_throttle_at"].is_null() || acct["projected_throttle_at"].is_string(),
            "projected_throttle_at should be null or ISO 8601 string"
        );

        // aggregate section
        assert!(
            body["aggregate"].is_object(),
            "aggregate section should exist"
        );
        assert!(
            body["aggregate"].get("total_headroom_requests").is_some(),
            "total_headroom_requests field should be present"
        );
        assert!(body["aggregate"]["consumers"].is_object());
    }

    #[test]
    fn epoch_to_iso8601_known_values() {
        // 2024-01-01T00:00:00Z = 1704067200
        assert_eq!(
            AppState::epoch_to_iso8601(1704067200),
            "2024-01-01T00:00:00Z"
        );
        // Unix epoch
        assert_eq!(AppState::epoch_to_iso8601(0), "1970-01-01T00:00:00Z");
        // 2026-02-14T12:30:45Z = approximate check
        let result = AppState::epoch_to_iso8601(1771157445);
        assert!(
            result.starts_with("2026-02-"),
            "expected 2026-02, got {result}"
        );
        assert!(result.ends_with('Z'));
    }

    // ── Task 7: Full integration tests ──

    #[tokio::test]
    async fn request_rejected_by_utilization_limit() {
        let (mock_url, _handle) = spawn_mock_upstream().await;
        let now = AppState::now_epoch();
        let mut limits = HashMap::new();
        limits.insert("-".to_string(), 0.50); // default client gets 0.50 limit
        let state = Arc::new(AppState {
            client: Client::builder()
                .timeout(Duration::from_secs(5))
                .build()
                .unwrap(),
            upstream: mock_url.clone(),
            accounts: vec![make_account("a", "sk-ant-api-test-aaa")],
            robin: AtomicUsize::new(0),
            routing_strategy: RoutingStrategy::default(),
            cooldown: Duration::from_secs(60),
            state_path: PathBuf::from("/tmp/test-limit-reject.state.json"),
            proxy_key: Some("key".to_string()),
            allowed_ips: vec![],
            upstreams: vec![],
            client_names: HashMap::new(),
            auto_cache: false,
            client_usage: Mutex::new(HashMap::new()),
            shadow_log_tx: None,
            shadow_log_dropped: AtomicU64::new(0),
            client_budgets: HashMap::new(),
            budget_usage: Mutex::new(HashMap::new()),
            client_utilization_limits: limits,
            operators: vec![],
            emergency_brake: true,
            emergency_threshold: DEFAULT_EMERGENCY_THRESHOLD,
            client_request_rates: Mutex::new(HashMap::new()),
            soft_limit: 1.0,
            redis: None,
            cluster_info_cache: Mutex::new(None),
            next_req_id: AtomicU64::new(0),
            instance_id: 0,
            probe_interval_secs: 300,
        });
        // Set utilization above client's limit (0.80 > 0.50)
        set_account_utilization(&state, 0, 0.80, 0.70, now + 10000, now + 100000).await;

        let app = build_router(state);
        let addr = serve(app).await;

        let client = Client::new();
        let resp = client
            .post(format!("http://{}/v1/messages", addr))
            .header("content-type", "application/json")
            .header("x-api-key", "key")
            .body(r#"{"model":"test","max_tokens":1,"messages":[{"role":"user","content":"hi"}]}"#)
            .send()
            .await
            .unwrap();

        assert_eq!(resp.status(), reqwest::StatusCode::TOO_MANY_REQUESTS);
        assert!(
            resp.headers().get("retry-after").is_some(),
            "429 from utilization limit should include Retry-After"
        );
    }

    #[tokio::test]
    async fn request_passes_utilization_limit() {
        let (mock_url, _handle) = spawn_mock_upstream().await;
        let now = AppState::now_epoch();
        let mut limits = HashMap::new();
        limits.insert("-".to_string(), 0.90);
        let state = Arc::new(AppState {
            client: Client::builder()
                .timeout(Duration::from_secs(5))
                .build()
                .unwrap(),
            upstream: mock_url.clone(),
            accounts: vec![make_account("a", "sk-ant-api-test-aaa")],
            robin: AtomicUsize::new(0),
            routing_strategy: RoutingStrategy::default(),
            cooldown: Duration::from_secs(60),
            state_path: PathBuf::from("/tmp/test-limit-pass.state.json"),
            proxy_key: Some("key".to_string()),
            allowed_ips: vec![],
            upstreams: vec![],
            client_names: HashMap::new(),
            auto_cache: false,
            client_usage: Mutex::new(HashMap::new()),
            shadow_log_tx: None,
            shadow_log_dropped: AtomicU64::new(0),
            client_budgets: HashMap::new(),
            budget_usage: Mutex::new(HashMap::new()),
            client_utilization_limits: limits,
            operators: vec![],
            emergency_brake: true,
            emergency_threshold: DEFAULT_EMERGENCY_THRESHOLD,
            client_request_rates: Mutex::new(HashMap::new()),
            soft_limit: 1.0,
            redis: None,
            cluster_info_cache: Mutex::new(None),
            next_req_id: AtomicU64::new(0),
            instance_id: 0,
            probe_interval_secs: 300,
        });
        // Set utilization below client's limit (0.50 < 0.90)
        set_account_utilization(&state, 0, 0.50, 0.40, now + 10000, now + 100000).await;

        let app = build_router(state);
        let addr = serve(app).await;

        let client = Client::new();
        let resp = client
            .post(format!("http://{}/v1/messages", addr))
            .header("content-type", "application/json")
            .header("x-api-key", "key")
            .body(r#"{"model":"test","max_tokens":1,"messages":[{"role":"user","content":"hi"}]}"#)
            .send()
            .await
            .unwrap();

        assert_eq!(resp.status(), reqwest::StatusCode::OK);
    }

    #[tokio::test]
    async fn emergency_brake_blocks_non_operator() {
        let (mock_url, _handle) = spawn_mock_upstream().await;
        let now = AppState::now_epoch();
        let state = Arc::new(AppState {
            client: Client::builder()
                .timeout(Duration::from_secs(5))
                .build()
                .unwrap(),
            upstream: mock_url.clone(),
            accounts: vec![
                make_account("a", "sk-ant-api-test-aaa"),
                make_account("b", "sk-ant-api-test-bbb"),
            ],
            robin: AtomicUsize::new(0),
            routing_strategy: RoutingStrategy::default(),
            cooldown: Duration::from_secs(60),
            state_path: PathBuf::from("/tmp/test-emergency-block.state.json"),
            proxy_key: Some("key".to_string()),
            allowed_ips: vec![],
            upstreams: vec![],
            client_names: HashMap::new(),
            auto_cache: false,
            client_usage: Mutex::new(HashMap::new()),
            shadow_log_tx: None,
            shadow_log_dropped: AtomicU64::new(0),
            client_budgets: HashMap::new(),
            budget_usage: Mutex::new(HashMap::new()),
            client_utilization_limits: HashMap::new(),
            operators: vec!["ray".to_string()],
            emergency_brake: true,
            emergency_threshold: DEFAULT_EMERGENCY_THRESHOLD,
            client_request_rates: Mutex::new(HashMap::new()),
            soft_limit: 1.0,
            redis: None,
            cluster_info_cache: Mutex::new(None),
            next_req_id: AtomicU64::new(0),
            instance_id: 0,
            probe_interval_secs: 300,
        });
        // All accounts above emergency threshold. 5h=0.96 > emergency threshold (0.88).
        set_account_utilization(&state, 0, 0.96, 0.0, now + 10000, now + 100000).await;
        set_account_utilization(&state, 1, 0.97, 0.0, now + 10000, now + 100000).await;

        let app = build_router(state);
        let addr = serve(app).await;

        let client = Client::new();
        let resp = client
            .post(format!("http://{}/v1/messages", addr))
            .header("content-type", "application/json")
            .header("x-api-key", "key")
            .body(r#"{"model":"test","max_tokens":1,"messages":[{"role":"user","content":"hi"}]}"#)
            .send()
            .await
            .unwrap();

        assert_eq!(resp.status(), reqwest::StatusCode::TOO_MANY_REQUESTS);
        let body = resp.text().await.unwrap();
        assert!(
            body.contains("emergency"),
            "emergency brake response should mention 'emergency': {body}"
        );
    }

    #[tokio::test]
    async fn emergency_brake_allows_operator() {
        let (mock_url, _handle) = spawn_mock_upstream().await;
        let now = AppState::now_epoch();
        let mut client_names = HashMap::new();
        client_names.insert("127.0.0.1".to_string(), "ray".to_string());
        let state = Arc::new(AppState {
            client: Client::builder()
                .timeout(Duration::from_secs(5))
                .build()
                .unwrap(),
            upstream: mock_url.clone(),
            accounts: vec![
                make_account("a", "sk-ant-api-test-aaa"),
                make_account("b", "sk-ant-api-test-bbb"),
            ],
            robin: AtomicUsize::new(0),
            routing_strategy: RoutingStrategy::default(),
            cooldown: Duration::from_secs(60),
            state_path: PathBuf::from("/tmp/test-emergency-operator.state.json"),
            proxy_key: Some("key".to_string()),
            allowed_ips: vec![],
            upstreams: vec![],
            client_names,
            auto_cache: false,
            client_usage: Mutex::new(HashMap::new()),
            shadow_log_tx: None,
            shadow_log_dropped: AtomicU64::new(0),
            client_budgets: HashMap::new(),
            budget_usage: Mutex::new(HashMap::new()),
            client_utilization_limits: HashMap::new(),
            operators: vec!["ray".to_string()],
            emergency_brake: true,
            emergency_threshold: DEFAULT_EMERGENCY_THRESHOLD,
            client_request_rates: Mutex::new(HashMap::new()),
            soft_limit: 1.0,
            redis: None,
            cluster_info_cache: Mutex::new(None),
            next_req_id: AtomicU64::new(0),
            instance_id: 0,
            probe_interval_secs: 300,
        });
        // All accounts above emergency threshold — 5h only (avoid claim penalty on 7d)
        set_account_utilization(&state, 0, 0.96, 0.0, now + 10000, now + 100000).await;
        set_account_utilization(&state, 1, 0.97, 0.0, now + 10000, now + 100000).await;

        let app = build_router(state);
        let addr = serve(app).await;

        // Request comes from 127.0.0.1 which maps to "ray" (the operator)
        let client = Client::new();
        let resp = client
            .post(format!("http://{}/v1/messages", addr))
            .header("content-type", "application/json")
            .header("x-api-key", "key")
            .body(r#"{"model":"test","max_tokens":1,"messages":[{"role":"user","content":"hi"}]}"#)
            .send()
            .await
            .unwrap();

        assert_eq!(
            resp.status(),
            reqwest::StatusCode::OK,
            "operator should bypass emergency brake"
        );
    }

    #[tokio::test]
    async fn openai_handler_enforces_utilization_limit() {
        let (mock_url, _handle) = spawn_mock_upstream().await;
        let now = AppState::now_epoch();
        let mut limits = HashMap::new();
        limits.insert("-".to_string(), 0.50);
        let state = Arc::new(AppState {
            client: Client::builder()
                .timeout(Duration::from_secs(5))
                .build()
                .unwrap(),
            upstream: mock_url.clone(),
            accounts: vec![make_account("a", "sk-ant-api-test-aaa")],
            robin: AtomicUsize::new(0),
            routing_strategy: RoutingStrategy::default(),
            cooldown: Duration::from_secs(60),
            state_path: PathBuf::from("/tmp/test-openai-limit.state.json"),
            proxy_key: Some("key".to_string()),
            allowed_ips: vec![],
            upstreams: vec![],
            client_names: HashMap::new(),
            auto_cache: false,
            client_usage: Mutex::new(HashMap::new()),
            shadow_log_tx: None,
            shadow_log_dropped: AtomicU64::new(0),
            client_budgets: HashMap::new(),
            budget_usage: Mutex::new(HashMap::new()),
            client_utilization_limits: limits,
            operators: vec![],
            emergency_brake: true,
            emergency_threshold: DEFAULT_EMERGENCY_THRESHOLD,
            client_request_rates: Mutex::new(HashMap::new()),
            soft_limit: 1.0,
            redis: None,
            cluster_info_cache: Mutex::new(None),
            next_req_id: AtomicU64::new(0),
            instance_id: 0,
            probe_interval_secs: 300,
        });
        set_account_utilization(&state, 0, 0.80, 0.70, now + 10000, now + 100000).await;

        let app = build_router(state);
        let addr = serve(app).await;

        let client = Client::new();
        let resp = client
            .post(format!("http://{}/v1/chat/completions", addr))
            .header("content-type", "application/json")
            .header("x-api-key", "key")
            .body(r#"{"model":"test","max_tokens":1,"messages":[{"role":"user","content":"hi"}]}"#)
            .send()
            .await
            .unwrap();

        assert_eq!(
            resp.status(),
            reqwest::StatusCode::TOO_MANY_REQUESTS,
            "OpenAI-compat handler should enforce utilization limits"
        );
    }

    #[tokio::test]
    async fn openai_handler_enforces_emergency_brake() {
        let (mock_url, _handle) = spawn_mock_upstream().await;
        let now = AppState::now_epoch();
        let state = Arc::new(AppState {
            client: Client::builder()
                .timeout(Duration::from_secs(5))
                .build()
                .unwrap(),
            upstream: mock_url.clone(),
            accounts: vec![make_account("a", "sk-ant-api-test-aaa")],
            robin: AtomicUsize::new(0),
            routing_strategy: RoutingStrategy::default(),
            cooldown: Duration::from_secs(60),
            state_path: PathBuf::from("/tmp/test-openai-emergency.state.json"),
            proxy_key: Some("key".to_string()),
            allowed_ips: vec![],
            upstreams: vec![],
            client_names: HashMap::new(),
            auto_cache: false,
            client_usage: Mutex::new(HashMap::new()),
            shadow_log_tx: None,
            shadow_log_dropped: AtomicU64::new(0),
            client_budgets: HashMap::new(),
            budget_usage: Mutex::new(HashMap::new()),
            client_utilization_limits: HashMap::new(),
            operators: vec![],
            emergency_brake: true,
            emergency_threshold: DEFAULT_EMERGENCY_THRESHOLD,
            client_request_rates: Mutex::new(HashMap::new()),
            soft_limit: 1.0,
            redis: None,
            cluster_info_cache: Mutex::new(None),
            next_req_id: AtomicU64::new(0),
            instance_id: 0,
            probe_interval_secs: 300,
        });
        set_account_utilization(&state, 0, 0.96, 0.0, now + 10000, now + 100000).await;

        let app = build_router(state);
        let addr = serve(app).await;

        let client = Client::new();
        let resp = client
            .post(format!("http://{}/v1/chat/completions", addr))
            .header("content-type", "application/json")
            .header("x-api-key", "key")
            .body(r#"{"model":"test","max_tokens":1,"messages":[{"role":"user","content":"hi"}]}"#)
            .send()
            .await
            .unwrap();

        assert_eq!(
            resp.status(),
            reqwest::StatusCode::TOO_MANY_REQUESTS,
            "OpenAI-compat handler should enforce emergency brake"
        );
    }

    #[tokio::test]
    async fn no_new_config_identical_behavior() {
        // Default config: no operator, no limits, no emergency override
        // Should behave exactly like before the feature was added
        let (mock_url, _handle) = spawn_mock_upstream().await;
        let (app, _state) = test_app(&mock_url, Some("key".to_string()));
        let addr = serve(app).await;

        let client = Client::new();
        let resp = client
            .post(format!("http://{}/v1/messages", addr))
            .header("content-type", "application/json")
            .header("x-api-key", "key")
            .body(r#"{"model":"test","max_tokens":1,"messages":[{"role":"user","content":"hi"}]}"#)
            .send()
            .await
            .unwrap();

        // Default config: no limits, no emergency → request should succeed
        assert_eq!(resp.status(), reqwest::StatusCode::OK);

        // Budget status header present even with no config (default healthy)
        assert!(resp.headers().get("x-budget-status").is_some());

        // Stats should still work with aggregate section
        let stats = client
            .get(format!("http://{}/_stats", addr))
            .header("x-api-key", "key")
            .send()
            .await
            .unwrap();
        assert_eq!(stats.status(), reqwest::StatusCode::OK);
        let body: serde_json::Value = stats.json().await.unwrap();
        assert!(body["accounts"].is_array());
        assert!(body["aggregate"].is_object());
        assert_eq!(body["strategy"], "dynamic-capacity-v1");
    }

    // ── Additional comprehensive tests ──────────────────────────────────

    #[test]
    fn resolve_client_id_prefers_header() {
        let state = test_state_with(vec![make_account("a", "sk-ant-api-x")]);
        let ip: IpAddr = "192.168.1.100".parse().unwrap();
        let mut headers = hyper::HeaderMap::new();
        headers.insert("x-client-id", HeaderValue::from_static("header-client"));

        let resolved = state.resolve_client_id(&ip, &headers);
        assert_eq!(
            resolved, "header-client",
            "should prefer x-client-id header"
        );
    }

    #[test]
    fn resolve_client_id_falls_back_to_ip_map() {
        let mut client_names = HashMap::new();
        client_names.insert("192.168.1.100".to_string(), "mapped-client".to_string());
        let state = Arc::new(AppState {
            client: Client::new(),
            upstream: "http://127.0.0.1:1".to_string(),
            accounts: vec![make_account("a", "sk-ant-api-x")],
            robin: AtomicUsize::new(0),
            routing_strategy: RoutingStrategy::default(),
            cooldown: Duration::from_secs(60),
            state_path: PathBuf::from("/tmp/test.state.json"),
            proxy_key: None,
            allowed_ips: vec![],
            upstreams: vec![],
            client_names,
            auto_cache: true,
            client_usage: Mutex::new(HashMap::new()),
            shadow_log_tx: None,
            shadow_log_dropped: AtomicU64::new(0),
            client_budgets: HashMap::new(),
            budget_usage: Mutex::new(HashMap::new()),
            client_utilization_limits: HashMap::new(),
            operators: vec![],
            emergency_brake: true,
            emergency_threshold: DEFAULT_EMERGENCY_THRESHOLD,
            client_request_rates: Mutex::new(HashMap::new()),
            soft_limit: 1.0,
            redis: None,
            cluster_info_cache: Mutex::new(None),
            next_req_id: AtomicU64::new(0),
            instance_id: 0,
            probe_interval_secs: 300,
        });

        let ip: IpAddr = "192.168.1.100".parse().unwrap();
        let headers = hyper::HeaderMap::new();

        let resolved = state.resolve_client_id(&ip, &headers);
        assert_eq!(resolved, "mapped-client", "should fall back to IP mapping");
    }

    #[test]
    fn resolve_client_id_defaults_to_dash() {
        let state = test_state_with(vec![make_account("a", "sk-ant-api-x")]);
        let ip: IpAddr = "203.0.113.1".parse().unwrap();
        let headers = hyper::HeaderMap::new();

        let resolved = state.resolve_client_id(&ip, &headers);
        assert_eq!(resolved, "-", "should default to dash for unknown clients");
    }

    #[test]
    fn resolve_client_id_ignores_empty_header() {
        let state = test_state_with(vec![make_account("a", "sk-ant-api-x")]);
        let ip: IpAddr = "192.168.1.100".parse().unwrap();
        let mut headers = hyper::HeaderMap::new();
        headers.insert("x-client-id", HeaderValue::from_static(""));

        let resolved = state.resolve_client_id(&ip, &headers);
        assert_eq!(resolved, "-", "should ignore empty x-client-id header");
    }

    #[test]
    fn resolve_client_id_ignores_dash_header() {
        let state = test_state_with(vec![make_account("a", "sk-ant-api-x")]);
        let ip: IpAddr = "192.168.1.100".parse().unwrap();
        let mut headers = hyper::HeaderMap::new();
        headers.insert("x-client-id", HeaderValue::from_static("-"));

        let resolved = state.resolve_client_id(&ip, &headers);
        assert_eq!(resolved, "-", "should ignore dash as x-client-id header");
    }

    #[test]
    fn compute_pressure_status_operator_always_healthy() {
        let state = Arc::new(AppState {
            client: Client::new(),
            upstream: "http://127.0.0.1:1".to_string(),
            accounts: vec![],
            robin: AtomicUsize::new(0),
            routing_strategy: RoutingStrategy::default(),
            cooldown: Duration::from_secs(60),
            state_path: PathBuf::from("/tmp/test.state.json"),
            proxy_key: None,
            allowed_ips: vec![],
            upstreams: vec![],
            client_names: HashMap::new(),
            auto_cache: true,
            client_usage: Mutex::new(HashMap::new()),
            shadow_log_tx: None,
            shadow_log_dropped: AtomicU64::new(0),
            client_budgets: HashMap::new(),
            budget_usage: Mutex::new(HashMap::new()),
            client_utilization_limits: HashMap::new(),
            operators: vec!["operator-id".to_string()],
            emergency_brake: true,
            emergency_threshold: DEFAULT_EMERGENCY_THRESHOLD,
            client_request_rates: Mutex::new(HashMap::new()),
            soft_limit: 1.0,
            redis: None,
            cluster_info_cache: Mutex::new(None),
            next_req_id: AtomicU64::new(0),
            instance_id: 0,
            probe_interval_secs: 300,
        });

        let status = compute_pressure_status(0.99, "operator-id", &state);
        assert_eq!(
            status, "healthy",
            "operator should always see healthy status"
        );
    }

    #[test]
    fn compute_pressure_status_thresholds() {
        let state = test_state_with(vec![]);

        assert_eq!(compute_pressure_status(0.50, "client", &state), "healthy");
        assert_eq!(compute_pressure_status(0.75, "client", &state), "elevated");
        assert_eq!(compute_pressure_status(0.90, "client", &state), "critical");
        assert_eq!(compute_pressure_status(0.99, "client", &state), "emergency");
    }

    #[test]
    fn compute_pressure_status_limit_proximity_upgrade() {
        let mut limits = HashMap::new();
        limits.insert("client".to_string(), 0.80);
        let state = Arc::new(AppState {
            client: Client::new(),
            upstream: "http://127.0.0.1:1".to_string(),
            accounts: vec![],
            robin: AtomicUsize::new(0),
            routing_strategy: RoutingStrategy::default(),
            cooldown: Duration::from_secs(60),
            state_path: PathBuf::from("/tmp/test.state.json"),
            proxy_key: None,
            allowed_ips: vec![],
            upstreams: vec![],
            client_names: HashMap::new(),
            auto_cache: true,
            client_usage: Mutex::new(HashMap::new()),
            shadow_log_tx: None,
            shadow_log_dropped: AtomicU64::new(0),
            client_budgets: HashMap::new(),
            budget_usage: Mutex::new(HashMap::new()),
            client_utilization_limits: limits,
            operators: vec![],
            emergency_brake: true,
            emergency_threshold: DEFAULT_EMERGENCY_THRESHOLD,
            client_request_rates: Mutex::new(HashMap::new()),
            soft_limit: 1.0,
            redis: None,
            cluster_info_cache: Mutex::new(None),
            next_req_id: AtomicU64::new(0),
            instance_id: 0,
            probe_interval_secs: 300,
        });

        // 0.65 is > 80% of 0.80 limit (80% * 0.80 = 0.64), so should upgrade from healthy to elevated
        let status = compute_pressure_status(0.65, "client", &state);
        assert_eq!(
            status, "elevated",
            "proximity to limit should upgrade status"
        );
    }

    #[test]
    fn status_to_floor_mapping() {
        assert_eq!(status_to_floor(Some("rejected")), REJECTED_UTIL_FLOOR);
        assert_eq!(status_to_floor(Some("throttled")), THROTTLE_UTIL_FLOOR);
        assert_eq!(status_to_floor(Some("allowed_warning")), WARNING_UTIL_FLOOR);
        assert_eq!(status_to_floor(Some("allowed")), 0.0);
        assert_eq!(status_to_floor(None), 0.0);
    }

    #[test]
    fn status_to_floor_unknown_defaults_to_warning() {
        let floor = status_to_floor(Some("unknown_status"));
        assert_eq!(
            floor, WARNING_UTIL_FLOOR,
            "unknown status should map to warning floor"
        );
    }

    #[test]
    fn status_to_ordinal_mapping() {
        assert_eq!(status_to_ordinal(Some("rejected")), 3.0);
        assert_eq!(status_to_ordinal(Some("throttled")), 2.0);
        assert_eq!(status_to_ordinal(Some("allowed_warning")), 1.0);
        assert_eq!(status_to_ordinal(Some("allowed")), 0.0);
        assert_eq!(status_to_ordinal(None), 0.0);
        // Unknown statuses map to 1.0 (warning-level)
        assert_eq!(status_to_ordinal(Some("new_unknown_status")), 1.0);
    }

    #[test]
    fn time_adjusted_utilization_stale_data() {
        let now = 1000000u64;
        let reset_past = 999000u64; // Reset already happened

        let result =
            time_adjusted_utilization(Some(0.50), Some(reset_past), Some("allowed"), 3600.0, now);

        assert_eq!(
            result, None,
            "stale data (reset in past) should return None"
        );
    }

    #[test]
    fn time_adjusted_utilization_near_reset() {
        let now = 1000000u64;
        let reset = now + 1800; // 30 minutes until reset
        let near_reset_threshold = 3600.0; // 1 hour threshold

        let result = time_adjusted_utilization(
            Some(0.80),
            Some(reset),
            Some("allowed"),
            near_reset_threshold,
            now,
        );

        assert!(result.is_some());
        let adjusted = result.unwrap();
        assert!(
            adjusted < 0.80,
            "near-reset utilization should be discounted"
        );
        assert!(
            adjusted >= 0.04,
            "should apply minimum discount floor (0.05)"
        );
    }

    #[test]
    fn time_adjusted_utilization_mid_block() {
        let now = 1000000u64;
        let reset = now + 10800; // 3 hours until reset
        let near_reset_threshold = 3600.0; // 1 hour threshold

        let result = time_adjusted_utilization(
            Some(0.80),
            Some(reset),
            Some("allowed"),
            near_reset_threshold,
            now,
        );

        assert!(result.is_some());
        let adjusted = result.unwrap();
        assert_eq!(adjusted, 0.80, "mid-block utilization should be unchanged");
    }

    #[test]
    fn time_adjusted_utilization_status_floor_minimum() {
        let now = 1000000u64;
        let reset = now + 100; // Very close to reset

        let result = time_adjusted_utilization(
            Some(0.80),
            Some(reset),
            Some("throttled"), // Floor of 0.98
            3600.0,
            now,
        );

        assert!(result.is_some());
        let adjusted = result.unwrap();
        assert_eq!(
            adjusted, THROTTLE_UTIL_FLOOR,
            "status floor should override time discount"
        );
    }

    #[test]
    fn epoch_to_iso8601_leap_year() {
        // 2024-02-29T00:00:00Z (leap day) = 1709164800
        let result = AppState::epoch_to_iso8601(1709164800);
        assert_eq!(
            result, "2024-02-29T00:00:00Z",
            "should handle leap year correctly"
        );
    }

    #[test]
    fn epoch_to_iso8601_edge_of_year() {
        // 2023-12-31T23:59:59Z = 1704067199
        let result = AppState::epoch_to_iso8601(1704067199);
        assert_eq!(
            result, "2023-12-31T23:59:59Z",
            "should handle end of year correctly"
        );
    }

    #[test]
    fn account_serves_model_empty_filter_allows_all() {
        let acct = make_account("test", "sk-ant-api-x");
        assert!(acct.serves_model("claude-sonnet-4-6"));
        assert!(acct.serves_model("claude-opus-4-6"));
        assert!(acct.serves_model(""));
    }

    #[test]
    fn account_serves_model_prefix_wildcard() {
        let mut acct = make_account("test", "sk-ant-api-x");
        acct.models = vec!["claude-opus-*".to_string()];

        assert!(acct.serves_model("claude-opus-4-6"));
        assert!(acct.serves_model("claude-opus-future"));
        assert!(!acct.serves_model("claude-sonnet-4-6"));
    }

    #[test]
    fn account_serves_model_multiple_patterns() {
        let mut acct = make_account("test", "sk-ant-api-x");
        acct.models = vec!["claude-opus-*".to_string(), "claude-sonnet-4-6".to_string()];

        assert!(acct.serves_model("claude-opus-4-6"));
        assert!(acct.serves_model("claude-sonnet-4-6"));
        assert!(!acct.serves_model("claude-sonnet-3-5"));
        assert!(!acct.serves_model("claude-haiku-3-5"));
    }

    #[tokio::test]
    async fn effective_utilization_prefers_most_constrained() {
        // When both windows have data, should return max (most constrained)
        let now = AppState::now_epoch();
        let mut claims_7d = HashMap::new();
        claims_7d.insert(
            "seven_day".to_string(),
            ClaimWindowData {
                utilization: Some(0.60),
                reset: Some(now + 100000),
                status: Some("allowed".to_string()),
                ..Default::default()
            },
        );
        let info = RateLimitInfo {
            utilization_5h: Some(0.80),
            reset_5h: Some(now + 10000),
            status_5h: Some("allowed".to_string()),
            claims_7d,
            ..Default::default()
        };

        let (util, source, _, _) = effective_utilization(&info, now, "");
        assert!(util > 0.60, "should use higher (5h) utilization");
        assert_eq!(source, "5h");
    }

    #[tokio::test]
    async fn effective_utilization_7d_no_penalty() {
        // 7d window should pass through without penalty
        let now = AppState::now_epoch();
        let mut claims_7d = HashMap::new();
        claims_7d.insert(
            "seven_day".to_string(),
            ClaimWindowData {
                utilization: Some(0.85),
                reset: Some(now + 100000),
                status: Some("allowed".to_string()),
                ..Default::default()
            },
        );
        let info = RateLimitInfo {
            utilization_5h: Some(0.50),
            reset_5h: Some(now + 10000),
            status_5h: Some("allowed".to_string()),
            claims_7d,
            ..Default::default()
        };

        let (util, _, _, _) = effective_utilization(&info, now, "");
        assert!(
            (util - 0.85).abs() < 0.01,
            "7d window should pass through at 0.85 without penalty: got {util}"
        );
    }

    #[tokio::test]
    async fn pick_account_rejected_account_gets_no_traffic() {
        let state = test_state_with(vec![
            make_account("rejected", "sk-ant-api-a"),
            make_account("healthy", "sk-ant-api-b"),
        ]);

        let now = AppState::now_epoch();
        {
            let mut info = state.accounts[0].rate_info.write().await;
            info.utilization_5h = Some(0.90);
            info.reset_5h = Some(now + 10000);
            info.status_5h = Some("rejected".to_string()); // Rejected = util floor 1.0
        }
        {
            let mut info = state.accounts[1].rate_info.write().await;
            info.utilization = Some(0.50);
        }

        // All requests should go to healthy account
        for _ in 0..100 {
            let idx = state.pick_account(None, "", &[]).await.unwrap();
            assert_eq!(idx, 1, "rejected account should receive no traffic");
        }
    }

    #[test]
    fn ip_allow_entry_ipv6_support() {
        let entry = IpAllowEntry::Addr("::1".parse().unwrap());
        assert!(entry.contains(&"::1".parse().unwrap()));
        assert!(!entry.contains(&"::2".parse().unwrap()));
    }

    #[test]
    fn ip_allow_entry_ipv6_cidr() {
        let entry = IpAllowEntry::Net("2001:db8::/32".parse().unwrap());
        assert!(entry.contains(&"2001:db8::1".parse().unwrap()));
        assert!(entry.contains(&"2001:db8:ffff::1".parse().unwrap()));
        assert!(!entry.contains(&"2001:db9::1".parse().unwrap()));
    }

    #[tokio::test]
    async fn pick_account_all_throttled_uses_all() {
        // When all accounts are throttled (status=throttled, floor=0.98 > soft_limit=0.90),
        // should use all accounts (graceful degradation)
        let state = test_state_with(vec![
            make_account("a", "sk-ant-api-a"),
            make_account("b", "sk-ant-api-b"),
        ]);

        let now = AppState::now_epoch();
        for acct in &state.accounts {
            let mut info = acct.rate_info.write().await;
            info.utilization_5h = Some(0.80);
            info.reset_5h = Some(now + 10000);
            info.status_5h = Some("throttled".to_string()); // Floor 0.98 > soft_limit 0.90
        }

        // Both accounts should receive traffic
        let mut counts = [0u32; 2];
        for _ in 0..1000 {
            let idx = state.pick_account(None, "", &[]).await.unwrap();
            counts[idx] += 1;
        }

        assert!(
            counts[0] > 0,
            "first throttled account should get some traffic"
        );
        assert!(
            counts[1] > 0,
            "second throttled account should get some traffic"
        );
    }

    #[test]
    fn is_operator_checks_configured_operator() {
        let state = Arc::new(AppState {
            client: Client::new(),
            upstream: "http://127.0.0.1:1".to_string(),
            accounts: vec![],
            robin: AtomicUsize::new(0),
            routing_strategy: RoutingStrategy::default(),
            cooldown: Duration::from_secs(60),
            state_path: PathBuf::from("/tmp/test.state.json"),
            proxy_key: None,
            allowed_ips: vec![],
            upstreams: vec![],
            client_names: HashMap::new(),
            auto_cache: true,
            client_usage: Mutex::new(HashMap::new()),
            shadow_log_tx: None,
            shadow_log_dropped: AtomicU64::new(0),
            client_budgets: HashMap::new(),
            budget_usage: Mutex::new(HashMap::new()),
            client_utilization_limits: HashMap::new(),
            operators: vec!["special-operator".to_string()],
            emergency_brake: true,
            emergency_threshold: DEFAULT_EMERGENCY_THRESHOLD,
            client_request_rates: Mutex::new(HashMap::new()),
            soft_limit: 1.0,
            redis: None,
            cluster_info_cache: Mutex::new(None),
            next_req_id: AtomicU64::new(0),
            instance_id: 0,
            probe_interval_secs: 300,
        });

        assert!(state.is_operator("special-operator"));
        assert!(!state.is_operator("regular-client"));
    }

    #[test]
    fn is_operator_returns_false_when_no_operator_configured() {
        let state = test_state_with(vec![]);
        assert!(!state.is_operator("any-client"));
    }

    #[test]
    fn is_operator_supports_multiple_operators() {
        let state = Arc::new(AppState {
            client: Client::new(),
            upstream: "http://127.0.0.1:1".to_string(),
            accounts: vec![],
            robin: AtomicUsize::new(0),
            routing_strategy: RoutingStrategy::default(),
            cooldown: Duration::from_secs(60),
            state_path: PathBuf::from("/tmp/test.state.json"),
            proxy_key: None,
            allowed_ips: vec![],
            upstreams: vec![],
            client_names: HashMap::new(),
            auto_cache: true,
            client_usage: Mutex::new(HashMap::new()),
            shadow_log_tx: None,
            shadow_log_dropped: AtomicU64::new(0),
            client_budgets: HashMap::new(),
            budget_usage: Mutex::new(HashMap::new()),
            client_utilization_limits: HashMap::new(),
            operators: vec![
                "ray".to_string(),
                "openclaw".to_string(),
                "claude".to_string(),
            ],
            emergency_brake: true,
            emergency_threshold: DEFAULT_EMERGENCY_THRESHOLD,
            client_request_rates: Mutex::new(HashMap::new()),
            soft_limit: 1.0,
            redis: None,
            cluster_info_cache: Mutex::new(None),
            next_req_id: AtomicU64::new(0),
            instance_id: 0,
            probe_interval_secs: 300,
        });
        assert!(state.is_operator("ray"));
        assert!(state.is_operator("openclaw"));
        assert!(state.is_operator("claude"));
        assert!(!state.is_operator("gastown"));
        assert!(!state.is_operator("-"));
    }

    // ── Redis distributed state tests ──────────────────────────────────

    #[tokio::test]
    async fn cluster_info_returns_none_without_redis() {
        let state = test_state_with(vec![make_account("a", "sk-ant-api-x")]);
        assert!(state.cluster_info().await.is_none());
    }

    #[tokio::test]
    async fn sync_from_redis_noop_without_redis() {
        let state = test_state_with(vec![make_account("a", "sk-ant-api-x")]);
        // Should not panic or error when redis is None
        state.sync_from_redis().await;
        // Cluster cache should remain None
        assert!(state.cluster_info_cache.lock().unwrap().is_none());
    }

    #[tokio::test]
    async fn budget_local_fallback_without_redis() {
        let mut budgets = HashMap::new();
        budgets.insert("client-a".to_string(), 100u64);
        let state = Arc::new(AppState {
            client: Client::builder()
                .timeout(Duration::from_secs(5))
                .build()
                .unwrap(),
            upstream: "http://127.0.0.1:1".to_string(),
            accounts: vec![make_account("a", "sk-ant-api-x")],
            robin: AtomicUsize::new(0),
            routing_strategy: RoutingStrategy::default(),
            cooldown: Duration::from_secs(60),
            state_path: PathBuf::from("/tmp/anthropic-lb-test.state.json"),
            proxy_key: None,
            allowed_ips: vec![],
            upstreams: vec![],
            client_names: HashMap::new(),
            auto_cache: true,
            client_usage: Mutex::new(HashMap::new()),
            shadow_log_tx: None,
            shadow_log_dropped: AtomicU64::new(0),
            client_budgets: budgets,
            budget_usage: Mutex::new(HashMap::new()),
            client_utilization_limits: HashMap::new(),
            operators: vec![],
            emergency_brake: true,
            emergency_threshold: DEFAULT_EMERGENCY_THRESHOLD,
            client_request_rates: Mutex::new(HashMap::new()),
            soft_limit: 1.0,
            redis: None,
            cluster_info_cache: Mutex::new(None),
            next_req_id: AtomicU64::new(0),
            instance_id: 0,
            probe_interval_secs: 300,
        });

        // Budget check uses local path when redis is None
        assert!(state.check_budget("client-a").await.is_ok());
        state.record_budget_usage("client-a", 80).await;
        assert!(state.check_budget("client-a").await.is_ok());
        state.record_budget_usage("client-a", 30).await;
        assert!(state.check_budget("client-a").await.is_err());
    }

    #[test]
    fn redis_rate_info_serialization_roundtrip() {
        let mut claims = HashMap::new();
        claims.insert(
            "claude-sonnet-4-6".to_string(),
            ClaimWindowData {
                utilization: Some(0.42),
                reset: Some(1700000000),
                status: Some("active".to_string()),
                ..Default::default()
            },
        );

        let info = RedisRateInfo {
            utilization: Some(0.5),
            utilization_5h: Some(0.3),
            utilization_7d: Some(0.6),
            reset_5h: Some(1700000000),
            reset_7d: Some(1700500000),
            status_5h: Some("active".to_string()),
            status_7d: Some("active".to_string()),
            claims_7d: claims,
            representative_claim: Some("five_hour".to_string()),
            remaining_requests: Some(100),
            remaining_tokens: Some(50000),
            limit_requests: Some(200),
            limit_tokens: Some(100000),
            updated_at: 1700000000,
        };

        let json = serde_json::to_string(&info).unwrap();
        let deserialized: RedisRateInfo = serde_json::from_str(&json).unwrap();

        assert_eq!(info.utilization, deserialized.utilization);
        assert_eq!(info.utilization_5h, deserialized.utilization_5h);
        assert_eq!(info.utilization_7d, deserialized.utilization_7d);
        assert_eq!(info.reset_5h, deserialized.reset_5h);
        assert_eq!(info.reset_7d, deserialized.reset_7d);
        assert_eq!(info.updated_at, deserialized.updated_at);
        assert_eq!(info.claims_7d.len(), deserialized.claims_7d.len());
        let claim = deserialized.claims_7d.get("claude-sonnet-4-6").unwrap();
        assert_eq!(claim.utilization, Some(0.42));
        assert_eq!(claim.reset, Some(1700000000));
    }

    #[test]
    fn redis_rate_info_empty_fields() {
        let info = RedisRateInfo {
            utilization: None,
            utilization_5h: None,
            utilization_7d: None,
            reset_5h: None,
            reset_7d: None,
            status_5h: None,
            status_7d: None,
            claims_7d: HashMap::new(),
            representative_claim: None,
            remaining_requests: None,
            remaining_tokens: None,
            limit_requests: None,
            limit_tokens: None,
            updated_at: 0,
        };

        let json = serde_json::to_string(&info).unwrap();
        let deserialized: RedisRateInfo = serde_json::from_str(&json).unwrap();
        assert!(deserialized.utilization.is_none());
        assert!(deserialized.claims_7d.is_empty());
        assert_eq!(deserialized.updated_at, 0);
    }

    #[tokio::test]
    async fn hard_limit_unchanged_by_sync_without_redis() {
        let state = test_state_with(vec![make_account("a", "sk-ant-api-x")]);

        // Set a local hard limit
        {
            let mut info = state.accounts[0].rate_info.write().await;
            info.hard_limited_until = Some(Instant::now() + Duration::from_secs(30));
        }

        // sync_from_redis should not touch it when redis is None
        state.sync_from_redis().await;

        let info = state.accounts[0].rate_info.read().await;
        assert!(info.hard_limited_until.is_some());
    }

    #[tokio::test]
    async fn record_budget_usage_skips_zero_tokens() {
        let mut budgets = HashMap::new();
        budgets.insert("client-a".to_string(), 100u64);
        let state = Arc::new(AppState {
            client: Client::builder()
                .timeout(Duration::from_secs(5))
                .build()
                .unwrap(),
            upstream: "http://127.0.0.1:1".to_string(),
            accounts: vec![make_account("a", "sk-ant-api-x")],
            robin: AtomicUsize::new(0),
            routing_strategy: RoutingStrategy::default(),
            cooldown: Duration::from_secs(60),
            state_path: PathBuf::from("/tmp/anthropic-lb-test.state.json"),
            proxy_key: None,
            allowed_ips: vec![],
            upstreams: vec![],
            client_names: HashMap::new(),
            auto_cache: true,
            client_usage: Mutex::new(HashMap::new()),
            shadow_log_tx: None,
            shadow_log_dropped: AtomicU64::new(0),
            client_budgets: budgets,
            budget_usage: Mutex::new(HashMap::new()),
            client_utilization_limits: HashMap::new(),
            operators: vec![],
            emergency_brake: true,
            emergency_threshold: DEFAULT_EMERGENCY_THRESHOLD,
            client_request_rates: Mutex::new(HashMap::new()),
            soft_limit: 1.0,
            redis: None,
            cluster_info_cache: Mutex::new(None),
            next_req_id: AtomicU64::new(0),
            instance_id: 0,
            probe_interval_secs: 300,
        });

        // Recording 0 tokens should be a no-op
        state.record_budget_usage("client-a", 0).await;
        let map = state.budget_usage.lock().unwrap();
        assert!(map.get("client-a").is_none());
    }

    #[tokio::test]
    async fn record_budget_usage_skips_unknown_client() {
        let state = test_state_with(vec![make_account("a", "sk-ant-api-x")]);
        // No budgets configured — recording should be a no-op
        state.record_budget_usage("unknown-client", 500).await;
        let map = state.budget_usage.lock().unwrap();
        assert!(map.is_empty());
    }

    // ── Config deserialization (real struct, not toml::Value) ─────

    #[test]
    fn config_deser_minimal() {
        let toml = r#"
listen = "127.0.0.1:8082"
upstream = "https://api.anthropic.com"

[[accounts]]
name = "primary"
token = "sk-ant-api-test"
"#;
        let cfg: Config = toml::from_str(toml).expect("minimal config should deserialize");
        assert_eq!(cfg.listen, "127.0.0.1:8082");
        assert_eq!(cfg.upstream, "https://api.anthropic.com");
        assert_eq!(cfg.accounts.len(), 1);
        assert_eq!(cfg.accounts[0].name, "primary");
        // Optional fields absent
        assert!(cfg.proxy_key.is_none());
        assert!(cfg.redis_url.is_none());
        assert!(cfg.operators.is_empty());
        assert!(cfg.emergency_threshold.is_none());
        assert!(cfg.soft_limit.is_none());
        assert!(cfg.client_budgets.is_empty());
        assert!(cfg.client_utilization_limits.is_empty());
        assert!(cfg.client_names.is_empty());
        assert!(cfg.upstreams.is_empty());
    }

    #[test]
    fn config_deser_all_optional_fields() {
        let toml = r#"
listen = "0.0.0.0:8082"
upstream = "https://api.anthropic.com"
strategy = "dynamic-capacity"
rate_limit_cooldown_secs = 120
probe_interval_secs = 600
proxy_key = "secret"
allowed_ips = ["10.0.0.0/8", "192.168.1.1"]
auto_cache = false
shadow_log = "/tmp/shadow.jsonl"
operators = ["ray", "openclaw"]
emergency_threshold = 0.90
soft_limit = 0.85
redis_url = "redis://10.0.0.5:6379"

[client_names]
"10.0.0.1" = "alice"
"10.0.0.2" = "bob"

[client_budgets]
alice = 1000000
bob = 500000

[client_utilization_limits]
alice = 0.95
bob = 0.80

[[accounts]]
name = "acct-a"
token = "sk-ant-oat01-token1"

[[accounts]]
name = "acct-b"
token = "sk-ant-api-token2"
models = ["claude-opus-*", "claude-sonnet-4-6"]

[[upstreams]]
name = "openai"
base_url = "https://api.openai.com"
api_key = "sk-openai-key"
"#;
        let cfg: Config = toml::from_str(toml).expect("full config should deserialize");
        assert_eq!(cfg.proxy_key.as_deref(), Some("secret"));
        assert_eq!(cfg.allowed_ips.as_ref().unwrap().len(), 2);
        assert_eq!(cfg.auto_cache, Some(false));
        assert_eq!(cfg.shadow_log.as_deref(), Some("/tmp/shadow.jsonl"));
        assert_eq!(cfg.operators, vec!["ray", "openclaw"]);
        assert_eq!(cfg.emergency_threshold, Some(0.90));
        assert_eq!(cfg.soft_limit, Some(0.85));
        assert_eq!(cfg.redis_url.as_deref(), Some("redis://10.0.0.5:6379"));
        assert_eq!(cfg.rate_limit_cooldown_secs, Some(120));
        assert_eq!(cfg.probe_interval_secs, Some(600));
        // Maps
        assert_eq!(cfg.client_names.get("10.0.0.1").unwrap(), "alice");
        assert_eq!(*cfg.client_budgets.get("alice").unwrap(), 1000000u64);
        assert_eq!(*cfg.client_utilization_limits.get("alice").unwrap(), 0.95);
        // Accounts
        assert_eq!(cfg.accounts.len(), 2);
        assert_eq!(cfg.accounts[1].models.len(), 2);
        assert_eq!(cfg.accounts[1].models[0], "claude-opus-*");
        // Upstreams
        assert_eq!(cfg.upstreams.len(), 1);
        assert_eq!(cfg.upstreams[0].name, "openai");
    }

    #[test]
    fn routing_strategy_parses_aliases() {
        assert_eq!(
            RoutingStrategy::parse(None).unwrap(),
            RoutingStrategy::DynamicCapacityV1
        );
        assert_eq!(
            RoutingStrategy::parse(Some("dynamic-capacity")).unwrap(),
            RoutingStrategy::DynamicCapacityV1
        );
        assert_eq!(
            RoutingStrategy::parse(Some("dynamic-capacity-v1")).unwrap(),
            RoutingStrategy::DynamicCapacityV1
        );
        assert_eq!(
            RoutingStrategy::parse(Some("sticky-weighted")).unwrap(),
            RoutingStrategy::StickyWeightedV2
        );
        assert_eq!(
            RoutingStrategy::parse(Some("sticky-weighted-v2")).unwrap(),
            RoutingStrategy::StickyWeightedV2
        );
        assert!(RoutingStrategy::parse(Some("bogus")).is_err());
    }

    #[test]
    fn config_deser_missing_required_field_fails() {
        // Missing `upstream`
        let toml = r#"
listen = "127.0.0.1:8082"

[[accounts]]
name = "test"
token = "sk-ant-api-test"
"#;
        let result = toml::from_str::<Config>(toml);
        assert!(
            result.is_err(),
            "missing upstream should fail deserialization"
        );
    }

    #[test]
    fn config_deser_missing_accounts_fails() {
        let toml = r#"
listen = "127.0.0.1:8082"
upstream = "https://api.anthropic.com"
"#;
        let result = toml::from_str::<Config>(toml);
        assert!(
            result.is_err(),
            "missing accounts should fail deserialization"
        );
    }

    // ── Rate info merge: "most recent wins" ─────────────────────

    #[tokio::test]
    async fn rate_info_merge_remote_newer_wins() {
        let state = test_state_with(vec![make_account("a", "sk-ant-api-x")]);
        let now_epoch = AppState::now_epoch();

        // Set local rate info with an older timestamp
        {
            let mut info = state.accounts[0].rate_info.write().await;
            info.utilization_5h = Some(0.30);
            info.last_updated_epoch = Some(now_epoch - 60); // 60s ago
        }

        // Simulate remote data that's newer (10s ago)
        let remote = RedisRateInfo {
            utilization: Some(0.80),
            utilization_5h: Some(0.75),
            utilization_7d: Some(0.60),
            reset_5h: Some(now_epoch + 3600),
            reset_7d: Some(now_epoch + 86400),
            status_5h: Some("allowed_warning".to_string()),
            status_7d: Some("allowed".to_string()),
            claims_7d: HashMap::new(),
            representative_claim: Some("five_hour".to_string()),
            remaining_requests: Some(50),
            remaining_tokens: Some(25000),
            limit_requests: Some(200),
            limit_tokens: Some(100000),
            updated_at: now_epoch - 10, // 10s ago — newer than local
        };

        // Apply same merge logic as sync_from_redis
        {
            let mut info = state.accounts[0].rate_info.write().await;
            let local_age = info
                .last_updated_epoch
                .map(|epoch| now_epoch.saturating_sub(epoch))
                .unwrap_or(u64::MAX);
            let remote_age = now_epoch.saturating_sub(remote.updated_at);
            assert!(
                remote_age < local_age,
                "remote ({}s) should be newer than local ({}s)",
                remote_age,
                local_age
            );
            // Remote wins — apply
            info.utilization_5h = remote.utilization_5h;
            info.last_updated_epoch = Some(remote.updated_at);
        }

        let info = state.accounts[0].rate_info.read().await;
        assert_eq!(info.utilization_5h, Some(0.75));
        assert_eq!(info.last_updated_epoch, Some(now_epoch - 10));
    }

    #[tokio::test]
    async fn rate_info_merge_local_newer_preserved() {
        let state = test_state_with(vec![make_account("a", "sk-ant-api-x")]);
        let now_epoch = AppState::now_epoch();

        // Set local rate info with a recent timestamp (5s ago)
        {
            let mut info = state.accounts[0].rate_info.write().await;
            info.utilization_5h = Some(0.30);
            info.last_updated_epoch = Some(now_epoch - 5);
        }

        // Remote data is older (120s ago)
        let remote = RedisRateInfo {
            utilization: Some(0.80),
            utilization_5h: Some(0.75),
            utilization_7d: None,
            reset_5h: None,
            reset_7d: None,
            status_5h: None,
            status_7d: None,
            claims_7d: HashMap::new(),
            representative_claim: None,
            remaining_requests: None,
            remaining_tokens: None,
            limit_requests: None,
            limit_tokens: None,
            updated_at: now_epoch - 120, // 120s ago — older than local
        };

        // Apply same merge logic as sync_from_redis
        {
            let info = state.accounts[0].rate_info.read().await;
            let local_age = info
                .last_updated_epoch
                .map(|epoch| now_epoch.saturating_sub(epoch))
                .unwrap_or(u64::MAX);
            let remote_age = now_epoch.saturating_sub(remote.updated_at);
            assert!(
                remote_age >= local_age,
                "local ({}s) should be newer than remote ({}s)",
                local_age,
                remote_age
            );
            // Local wins — do NOT apply remote
        }

        let info = state.accounts[0].rate_info.read().await;
        assert_eq!(
            info.utilization_5h,
            Some(0.30),
            "local data should be preserved"
        );
    }

    #[tokio::test]
    async fn rate_info_merge_no_local_epoch_remote_wins() {
        let state = test_state_with(vec![make_account("a", "sk-ant-api-x")]);
        let now_epoch = AppState::now_epoch();

        // Local has no last_updated_epoch (fresh state)
        {
            let info = state.accounts[0].rate_info.read().await;
            assert!(info.last_updated_epoch.is_none());
        }

        let remote = RedisRateInfo {
            utilization: Some(0.50),
            utilization_5h: Some(0.40),
            utilization_7d: None,
            reset_5h: None,
            reset_7d: None,
            status_5h: None,
            status_7d: None,
            claims_7d: HashMap::new(),
            representative_claim: None,
            remaining_requests: None,
            remaining_tokens: None,
            limit_requests: None,
            limit_tokens: None,
            updated_at: now_epoch - 30,
        };

        // When local has no epoch, local_age = u64::MAX, so remote always wins
        {
            let info = state.accounts[0].rate_info.read().await;
            let local_age = info
                .last_updated_epoch
                .map(|epoch| now_epoch.saturating_sub(epoch))
                .unwrap_or(u64::MAX);
            let remote_age = now_epoch.saturating_sub(remote.updated_at);
            assert!(
                remote_age < local_age,
                "remote should win when local has no epoch"
            );
        }
    }

    // ── State persistence round-trip ─────────────────────────────

    #[tokio::test]
    async fn state_persistence_roundtrip() {
        let tmp = tempfile::NamedTempFile::new().unwrap();
        let state_path = PathBuf::from(tmp.path());

        let state = Arc::new(AppState {
            client: Client::builder()
                .timeout(Duration::from_secs(5))
                .build()
                .unwrap(),
            upstream: "http://127.0.0.1:1".to_string(),
            accounts: vec![
                make_account("primary", "sk-ant-api-aaa"),
                make_account("secondary", "sk-ant-api-bbb"),
            ],
            robin: AtomicUsize::new(0),
            routing_strategy: RoutingStrategy::default(),
            cooldown: Duration::from_secs(60),
            state_path: state_path.clone(),
            proxy_key: None,
            allowed_ips: vec![],
            upstreams: vec![],
            client_names: HashMap::new(),
            auto_cache: true,
            client_usage: Mutex::new(HashMap::new()),
            shadow_log_tx: None,
            shadow_log_dropped: AtomicU64::new(0),
            client_budgets: HashMap::new(),
            budget_usage: Mutex::new(HashMap::new()),
            client_utilization_limits: HashMap::new(),
            operators: vec![],
            emergency_brake: true,
            emergency_threshold: DEFAULT_EMERGENCY_THRESHOLD,
            client_request_rates: Mutex::new(HashMap::new()),
            soft_limit: 1.0,
            redis: None,
            cluster_info_cache: Mutex::new(None),
            next_req_id: AtomicU64::new(0),
            instance_id: 0,
            probe_interval_secs: 300,
        });

        // Set up some state to persist
        let now_epoch = AppState::now_epoch();
        state.accounts[0].requests.store(42, Ordering::Relaxed);
        state.accounts[1].requests.store(17, Ordering::Relaxed);
        {
            let mut info = state.accounts[0].rate_info.write().await;
            info.utilization = Some(0.65);
            info.utilization_5h = Some(0.50);
            info.utilization_7d = Some(0.70);
            info.reset_5h = Some(now_epoch + 18000); // future
            info.reset_7d = Some(now_epoch + 604800);
            info.status_5h = Some("allowed_warning".to_string());
            info.remaining_requests = Some(100);
            info.remaining_tokens = Some(50000);
            info.limit_requests = Some(200);
            info.limit_tokens = Some(100000);
            info.representative_claim = Some("five_hour".to_string());
            // Set a known per-account epoch (older than now — simulates probe from 30s ago)
            info.last_updated_epoch = Some(now_epoch - 30);
            info.claims_7d.insert(
                "seven_day".to_string(),
                ClaimWindowData {
                    utilization: Some(0.70),
                    reset: Some(now_epoch + 604800),
                    status: Some("allowed".to_string()),
                    ..Default::default()
                },
            );
        }
        {
            let mut info = state.accounts[1].rate_info.write().await;
            info.utilization_5h = Some(0.20);
            info.reset_5h = Some(now_epoch + 18000);
            // hard-limit 60s from now
            info.hard_limited_until = Some(Instant::now() + Duration::from_secs(60));
        }

        // Save state
        state.save_state().await;

        // Verify file exists and is valid JSON
        let data = tokio::fs::read_to_string(&state_path).await.unwrap();
        let persisted: PersistedState = serde_json::from_str(&data).unwrap();
        assert_eq!(persisted.accounts.len(), 2);
        assert_eq!(persisted.accounts[0].requests_total, 42);
        assert_eq!(persisted.accounts[1].requests_total, 17);
        assert!(persisted.saved_at > 0);

        // Create a fresh state and load into it
        let state2 = Arc::new(AppState {
            client: Client::builder()
                .timeout(Duration::from_secs(5))
                .build()
                .unwrap(),
            upstream: "http://127.0.0.1:1".to_string(),
            accounts: vec![
                make_account("primary", "sk-ant-api-aaa"),
                make_account("secondary", "sk-ant-api-bbb"),
            ],
            robin: AtomicUsize::new(0),
            routing_strategy: RoutingStrategy::default(),
            cooldown: Duration::from_secs(60),
            state_path,
            proxy_key: None,
            allowed_ips: vec![],
            upstreams: vec![],
            client_names: HashMap::new(),
            auto_cache: true,
            client_usage: Mutex::new(HashMap::new()),
            shadow_log_tx: None,
            shadow_log_dropped: AtomicU64::new(0),
            client_budgets: HashMap::new(),
            budget_usage: Mutex::new(HashMap::new()),
            client_utilization_limits: HashMap::new(),
            operators: vec![],
            emergency_brake: true,
            emergency_threshold: DEFAULT_EMERGENCY_THRESHOLD,
            client_request_rates: Mutex::new(HashMap::new()),
            soft_limit: 1.0,
            redis: None,
            cluster_info_cache: Mutex::new(None),
            next_req_id: AtomicU64::new(0),
            instance_id: 0,
            probe_interval_secs: 300,
        });

        // Load state
        state2.load_state().await;

        // Verify fields survived the round-trip
        assert_eq!(
            state2.accounts[0].requests.load(Ordering::Relaxed),
            42,
            "request count should persist"
        );
        assert_eq!(state2.accounts[1].requests.load(Ordering::Relaxed), 17);

        {
            let info = state2.accounts[0].rate_info.read().await;
            // Unified is recomputed as max(utilization_5h, utilization_7d)
            assert_eq!(info.utilization, Some(0.70));
            assert_eq!(info.utilization_5h, Some(0.50));
            assert_eq!(info.remaining_requests, Some(100));
            assert_eq!(info.remaining_tokens, Some(50000));
            assert_eq!(info.limit_requests, Some(200));
            assert_eq!(info.limit_tokens, Some(100000));
            assert_eq!(info.representative_claim.as_deref(), Some("five_hour"));
            assert!(
                !info.claims_7d.is_empty(),
                "claims_7d should survive round-trip"
            );
            let claim = info.claims_7d.get("seven_day").unwrap();
            assert_eq!(claim.utilization, Some(0.70));
            // last_updated_epoch should be the per-account value, not saved_at or now()
            assert_eq!(
                info.last_updated_epoch,
                Some(now_epoch - 30),
                "last_updated_epoch should be the per-account persisted value"
            );
        }

        {
            let info = state2.accounts[1].rate_info.read().await;
            assert_eq!(info.utilization_5h, Some(0.20));
            // Hard limit should have been restored (future epoch)
            assert!(
                info.hard_limited_until.is_some(),
                "hard_limited_until should survive round-trip"
            );
        }
    }

    // ── Budget day rollover ──────────────────────────────────────

    #[tokio::test]
    async fn budget_day_rollover_resets_counter() {
        let mut budgets = HashMap::new();
        budgets.insert("client-a".to_string(), 10000u64);
        let state = Arc::new(AppState {
            client: Client::builder()
                .timeout(Duration::from_secs(5))
                .build()
                .unwrap(),
            upstream: "http://127.0.0.1:1".to_string(),
            accounts: vec![make_account("a", "sk-ant-api-x")],
            robin: AtomicUsize::new(0),
            routing_strategy: RoutingStrategy::default(),
            cooldown: Duration::from_secs(60),
            state_path: PathBuf::from("/tmp/anthropic-lb-test.state.json"),
            proxy_key: None,
            allowed_ips: vec![],
            upstreams: vec![],
            client_names: HashMap::new(),
            auto_cache: true,
            client_usage: Mutex::new(HashMap::new()),
            shadow_log_tx: None,
            shadow_log_dropped: AtomicU64::new(0),
            client_budgets: budgets,
            budget_usage: Mutex::new(HashMap::new()),
            client_utilization_limits: HashMap::new(),
            operators: vec![],
            emergency_brake: true,
            emergency_threshold: DEFAULT_EMERGENCY_THRESHOLD,
            client_request_rates: Mutex::new(HashMap::new()),
            soft_limit: 1.0,
            redis: None,
            cluster_info_cache: Mutex::new(None),
            next_req_id: AtomicU64::new(0),
            instance_id: 0,
            probe_interval_secs: 300,
        });

        // Pre-populate with yesterday's usage (high usage that would exceed budget)
        let yesterday = AppState::now_epoch() / 86400 - 1;
        {
            let mut map = state.budget_usage.lock().unwrap();
            map.insert("client-a".to_string(), (yesterday, 9500));
        }

        // Recording usage today should reset the counter (day rollover)
        state.record_budget_usage("client-a", 50).await;

        let map = state.budget_usage.lock().unwrap();
        let (day, used) = map.get("client-a").unwrap();
        let today = AppState::now_epoch() / 86400;
        assert_eq!(*day, today, "day should be today after rollover");
        assert_eq!(*used, 50, "usage should be 50 (reset, not 9550)");
    }

    #[tokio::test]
    async fn budget_check_respects_day_boundary() {
        let mut budgets = HashMap::new();
        budgets.insert("client-a".to_string(), 1000u64);
        let state = Arc::new(AppState {
            client: Client::builder()
                .timeout(Duration::from_secs(5))
                .build()
                .unwrap(),
            upstream: "http://127.0.0.1:1".to_string(),
            accounts: vec![make_account("a", "sk-ant-api-x")],
            robin: AtomicUsize::new(0),
            routing_strategy: RoutingStrategy::default(),
            cooldown: Duration::from_secs(60),
            state_path: PathBuf::from("/tmp/anthropic-lb-test.state.json"),
            proxy_key: None,
            allowed_ips: vec![],
            upstreams: vec![],
            client_names: HashMap::new(),
            auto_cache: true,
            client_usage: Mutex::new(HashMap::new()),
            shadow_log_tx: None,
            shadow_log_dropped: AtomicU64::new(0),
            client_budgets: budgets,
            budget_usage: Mutex::new(HashMap::new()),
            client_utilization_limits: HashMap::new(),
            operators: vec![],
            emergency_brake: true,
            emergency_threshold: DEFAULT_EMERGENCY_THRESHOLD,
            client_request_rates: Mutex::new(HashMap::new()),
            soft_limit: 1.0,
            redis: None,
            cluster_info_cache: Mutex::new(None),
            next_req_id: AtomicU64::new(0),
            instance_id: 0,
            probe_interval_secs: 300,
        });

        // Pre-populate with yesterday's exhausted budget
        let yesterday = AppState::now_epoch() / 86400 - 1;
        {
            let mut map = state.budget_usage.lock().unwrap();
            map.insert("client-a".to_string(), (yesterday, 5000));
        }

        // Budget check for today should pass — yesterday's usage doesn't count
        assert!(
            state.check_budget("client-a").await.is_ok(),
            "yesterday's exhausted budget should not block today"
        );
    }

    // ── Unit: Prometheus text exposition helpers ──────────────────────

    #[test]
    fn prometheus_gauge_formats_correctly() {
        let mut buf = String::new();
        prom_gauge(&mut buf, "test_metric", &[("label", "value")], 42.5);
        assert_eq!(buf, "test_metric{label=\"value\"} 42.5\n");
    }

    #[test]
    fn prometheus_gauge_no_labels() {
        let mut buf = String::new();
        prom_gauge(&mut buf, "test_metric", &[], 1.0);
        assert_eq!(buf, "test_metric 1\n");
    }

    #[test]
    fn prometheus_counter_formats_correctly() {
        let mut buf = String::new();
        prom_counter(&mut buf, "test_total", &[("a", "b")], 100);
        assert_eq!(buf, "test_total{a=\"b\"} 100\n");
    }

    #[test]
    fn prometheus_gauge_nan_renders() {
        let mut buf = String::new();
        prom_gauge(&mut buf, "test_metric", &[], f64::NAN);
        assert_eq!(buf, "test_metric NaN\n");
    }

    #[test]
    fn prometheus_gauge_multiple_labels() {
        let mut buf = String::new();
        prom_gauge(&mut buf, "m", &[("a", "1"), ("b", "2")], 0.0);
        assert_eq!(buf, "m{a=\"1\",b=\"2\"} 0\n");
    }

    #[test]
    fn prometheus_label_escaping() {
        let mut buf = String::new();
        prom_gauge(&mut buf, "m", &[("name", "has\"quotes")], 1.0);
        assert_eq!(buf, "m{name=\"has\\\"quotes\"} 1\n");
    }

    #[test]
    fn routing_metrics_present() {
        let acct = make_account("acct-a", "sk-ant-api-a");
        acct.last_routing_weight
            .store(0.4f64.to_bits(), Ordering::Relaxed);
        acct.last_routing_share
            .store(1.0f64.to_bits(), Ordering::Relaxed);

        let mut buf = String::new();
        append_routing_weight_metrics(
            &mut buf,
            &[acct],
            &[AcctMetricsSnap {
                name: "acct-a".to_string(),
                ..Default::default()
            }],
        );

        assert!(
            buf.lines()
                .any(|line| line
                    .starts_with("anthropic_account_routing_weight{account=\"acct-a\"} 0.4")),
            "missing routing_weight line:
{buf}"
        );
        assert!(
            buf.lines()
                .any(|line| line
                    .starts_with("anthropic_account_routing_share{account=\"acct-a\"} 1")),
            "missing routing_share line:
{buf}"
        );
    }
    #[test]
    fn routing_metrics_zero_weight_for_rejected_claim() {
        let acct = make_account("acct-a", "sk-ant-api-a");
        acct.last_routing_weight
            .store(0.0f64.to_bits(), Ordering::Relaxed);
        acct.last_routing_share
            .store(0.0f64.to_bits(), Ordering::Relaxed);

        let mut buf = String::new();
        append_routing_weight_metrics(
            &mut buf,
            &[acct],
            &[AcctMetricsSnap {
                name: "acct-a".to_string(),
                ..Default::default()
            }],
        );

        assert!(
            buf.lines()
                .any(|line| line
                    .starts_with("anthropic_account_routing_weight{account=\"acct-a\"} 0")),
            "rejected claim should zero routing_weight:
{buf}"
        );
        assert!(
            buf.lines()
                .any(|line| line
                    .starts_with("anthropic_account_routing_share{account=\"acct-a\"} 0")),
            "rejected claim should export zero routing_share:
{buf}"
        );
    }
    #[tokio::test]
    async fn passthrough_accounts_participate_in_routing_candidates_and_metrics() {
        let mut state = test_state_with(vec![
            make_account("passthrough", "passthrough"),
            make_account("api", "sk-ant-api-b"),
        ]);
        Arc::get_mut(&mut state).unwrap().soft_limit = 1.0;

        let now_epoch = AppState::now_epoch();
        {
            let mut info = state.accounts[0].rate_info.write().await;
            info.utilization = Some(0.20);
            info.utilization_5h = Some(0.20);
            info.reset_5h = Some(now_epoch + 10000);
        }
        {
            let mut info = state.accounts[1].rate_info.write().await;
            info.utilization = Some(0.40);
            info.utilization_5h = Some(0.40);
            info.reset_5h = Some(now_epoch + 10000);
        }

        let candidates = state.routing_candidates("claude-sonnet-4-6", &[]).await;
        assert_eq!(
            candidates.len(),
            2,
            "passthrough account should remain routable"
        );

        state.refresh_metrics_weights().await;
        let mut buf = String::new();
        append_routing_weight_metrics(
            &mut buf,
            &state.accounts,
            &[
                AcctMetricsSnap {
                    name: "passthrough".to_string(),
                    passthrough: true,
                    utilization: Some(0.20),
                    utilization_5h: Some(0.20),
                    reset_5h: Some(now_epoch + 10000),
                    ..Default::default()
                },
                AcctMetricsSnap {
                    name: "api".to_string(),
                    utilization: Some(0.40),
                    utilization_5h: Some(0.40),
                    reset_5h: Some(now_epoch + 10000),
                    ..Default::default()
                },
            ],
        );

        assert!(
            !buf.contains("anthropic_account_routing_weight{account=\"passthrough\"}"),
            "passthrough account should be omitted from routing metrics:
{buf}"
        );
        assert!(
            buf.contains("anthropic_account_routing_weight{account=\"api\"}"),
            "api account should remain in routing metrics:
{buf}"
        );
    }

    // ── Integration: /metrics endpoint ───────────────────────────────

    #[tokio::test]
    async fn metrics_endpoint_returns_prometheus_format() {
        let (mock_url, _handle) = spawn_mock_upstream().await;
        let (app, state) = test_app(&mock_url, None);

        // Set some state so metrics are interesting
        {
            let mut info = state.accounts[0].rate_info.write().await;
            info.utilization_5h = Some(0.42);
            info.utilization_7d = Some(0.35);
            info.remaining_requests = Some(1000);
            info.remaining_tokens = Some(500000);
            info.limit_requests = Some(4000);
            info.limit_tokens = Some(2000000);
        }
        // Set burn rate values (R2.2)
        {
            let mut br = state.accounts[0].burn_rate.lock().unwrap();
            br.rate_5m.value = 2.5;
            br.rate_1h.value = 1.8;
            br.rate_6h.value = 0.9;
        }
        state.accounts[0].requests.store(123, Ordering::Relaxed);
        state.accounts[0]
            .input_tokens
            .store(90000, Ordering::Relaxed);
        state.accounts[0]
            .output_tokens
            .store(30000, Ordering::Relaxed);
        state.accounts[0]
            .cache_creation_tokens
            .store(5000, Ordering::Relaxed);
        state.accounts[0]
            .cache_read_tokens
            .store(15000, Ordering::Relaxed);

        let addr = serve(app).await;
        let client = Client::new();
        let resp = client
            .get(format!("http://{}/metrics", addr))
            .send()
            .await
            .unwrap();

        assert_eq!(resp.status(), reqwest::StatusCode::OK);
        // R1.1: Exact Prometheus Content-Type
        let ct = resp
            .headers()
            .get("content-type")
            .unwrap()
            .to_str()
            .unwrap();
        assert_eq!(
            ct, "text/plain; version=0.0.4; charset=utf-8",
            "content-type must match Prometheus exposition format"
        );

        let body = resp.text().await.unwrap();

        // Account utilization
        assert!(
            body.contains("anthropic_account_utilization{account=\"acct-a\",window=\"5h\"} 0.42"),
            "missing 5h util:\n{body}"
        );
        assert!(
            body.contains("anthropic_account_utilization{account=\"acct-a\",window=\"7d\"} 0.35"),
            "missing 7d util:\n{body}"
        );

        // R2.2: Burn rate
        assert!(
            body.contains("anthropic_account_burn_rate{account=\"acct-a\",window=\"5m\"} 2.5"),
            "missing burn_rate 5m:\n{body}"
        );
        assert!(
            body.contains("anthropic_account_burn_rate{account=\"acct-a\",window=\"1h\"} 1.8"),
            "missing burn_rate 1h:\n{body}"
        );
        assert!(
            body.contains("anthropic_account_burn_rate{account=\"acct-a\",window=\"6h\"} 0.9"),
            "missing burn_rate 6h:\n{body}"
        );

        // R2.3: Headroom (remaining_requests=1000 → headroom=1000)
        assert!(
            body.contains("anthropic_account_headroom_requests{account=\"acct-a\"} 1000"),
            "missing headroom_requests:\n{body}"
        );

        // Remaining
        assert!(
            body.contains("anthropic_account_remaining_requests{account=\"acct-a\"} 1000"),
            "missing remaining_requests:\n{body}"
        );

        // R2.5: Limits
        assert!(
            body.contains("anthropic_account_limit_requests{account=\"acct-a\"} 4000"),
            "missing limit_requests:\n{body}"
        );
        assert!(
            body.contains("anthropic_account_limit_tokens{account=\"acct-a\"} 2000000"),
            "missing limit_tokens:\n{body}"
        );

        // Requests total
        assert!(
            body.contains("anthropic_account_requests_total{account=\"acct-a\"} 123"),
            "missing requests_total:\n{body}"
        );

        // R2.7: Hard limited (default = 0)
        assert!(
            body.contains("anthropic_account_hard_limited_remaining_seconds{account=\"acct-a\"} 0"),
            "missing hard_limited_remaining_seconds:\n{body}"
        );

        // R2.8: All 4 token types
        assert!(
            body.contains(
                "anthropic_account_token_usage_total{account=\"acct-a\",type=\"input\"} 90000"
            ),
            "missing input tokens:\n{body}"
        );
        assert!(
            body.contains(
                "anthropic_account_token_usage_total{account=\"acct-a\",type=\"output\"} 30000"
            ),
            "missing output tokens:\n{body}"
        );
        assert!(
            body.contains(
                "anthropic_account_token_usage_total{account=\"acct-a\",type=\"cache_creation\"} 5000"
            ),
            "missing cache_creation tokens:\n{body}"
        );
        assert!(
            body.contains(
                "anthropic_account_token_usage_total{account=\"acct-a\",type=\"cache_read\"} 15000"
            ),
            "missing cache_read tokens:\n{body}"
        );

        // Second account should also be present
        assert!(
            body.contains("anthropic_account_requests_total{account=\"acct-b\"}"),
            "missing acct-b:\n{body}"
        );

        // Meta metric
        assert!(
            body.contains("anthropic_lb_info{strategy=\"dynamic-capacity-v1\"} 1"),
            "missing lb_info:\n{body}"
        );

        // HELP/TYPE headers present
        assert!(
            body.contains("# TYPE anthropic_account_utilization gauge"),
            "missing TYPE header:\n{body}"
        );

        // Upstream metrics
        assert!(
            body.contains("anthropic_upstream_requests_total{upstream=\"mock\""),
            "missing upstream:\n{body}"
        );
    }

    #[tokio::test]
    async fn metrics_endpoint_auth() {
        let (mock_url, _handle) = spawn_mock_upstream().await;
        let (app, _state) = test_app(&mock_url, Some("secret-key".into()));

        let addr = serve(app).await;
        let client = Client::new();

        // Without key → 401
        let resp = client
            .get(format!("http://{}/metrics", addr))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), reqwest::StatusCode::UNAUTHORIZED);

        // Wrong key → 401
        let resp = client
            .get(format!("http://{}/metrics", addr))
            .header("x-api-key", "wrong-key")
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), reqwest::StatusCode::UNAUTHORIZED);

        // Correct key → 200
        let resp = client
            .get(format!("http://{}/metrics", addr))
            .header("x-api-key", "secret-key")
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), reqwest::StatusCode::OK);
    }

    #[tokio::test]
    async fn metrics_omits_null_utilization() {
        let (mock_url, _handle) = spawn_mock_upstream().await;
        let (app, _state) = test_app(&mock_url, None);

        // Don't set any rate_info — defaults are all None
        let addr = serve(app).await;
        let client = Client::new();
        let resp = client
            .get(format!("http://{}/metrics", addr))
            .send()
            .await
            .unwrap();
        let body = resp.text().await.unwrap();

        // Utilization lines should be absent (null data omitted, not emitted as 0 or NaN)
        assert!(
            !body.contains("anthropic_account_utilization{account=\"acct-a\""),
            "utilization should be omitted when null:\n{body}"
        );
        assert!(
            !body.contains("anthropic_account_remaining_requests{account=\"acct-a\""),
            "remaining_requests should be omitted when null:\n{body}"
        );
        assert!(
            !body.contains("anthropic_account_remaining_tokens{account=\"acct-a\""),
            "remaining_tokens should be omitted when null:\n{body}"
        );

        // But requests_total should still be present (counter, always emitted)
        assert!(
            body.contains("anthropic_account_requests_total{account=\"acct-a\"} 0"),
            "requests_total should always be present:\n{body}"
        );
    }

    #[tokio::test]
    async fn metrics_operator_hiding() {
        let (mock_url, _handle) = spawn_mock_upstream().await;
        let accounts = vec![make_account("acct-a", "sk-ant-api-test-aaa")];
        let state = Arc::new(AppState {
            client: Client::builder()
                .timeout(Duration::from_secs(5))
                .build()
                .unwrap(),
            upstream: mock_url.clone(),
            accounts,
            robin: AtomicUsize::new(0),
            routing_strategy: RoutingStrategy::default(),
            cooldown: Duration::from_secs(60),
            state_path: PathBuf::from("/tmp/anthropic-lb-test.state.json"),
            proxy_key: None,
            allowed_ips: vec![],
            upstreams: vec![],
            client_names: HashMap::new(),
            auto_cache: true,
            client_usage: Mutex::new(HashMap::new()),
            shadow_log_tx: None,
            shadow_log_dropped: AtomicU64::new(0),
            client_budgets: HashMap::new(),
            budget_usage: Mutex::new(HashMap::new()),
            client_utilization_limits: HashMap::new(),
            operators: vec!["op-alice".to_string(), "op-bob".to_string()],
            emergency_brake: true,
            emergency_threshold: DEFAULT_EMERGENCY_THRESHOLD,
            client_request_rates: Mutex::new(HashMap::new()),
            soft_limit: 1.0,
            redis: None,
            cluster_info_cache: Mutex::new(None),
            next_req_id: AtomicU64::new(0),
            instance_id: 0,
            probe_interval_secs: 300,
        });

        // Seed two operator clients and one regular client with token usage
        {
            let mut usage = state.client_usage.lock().unwrap();
            usage.insert("op-alice".to_string(), [100, 200, 0, 0]);
            usage.insert("op-bob".to_string(), [50, 100, 0, 0]);
            usage.insert("user-charlie".to_string(), [10, 20, 0, 0]);
        }
        {
            let mut rates = state.client_request_rates.lock().unwrap();
            rates.insert(
                "op-alice".to_string(),
                (
                    5,
                    Ewma {
                        value: 2.0,
                        tau: 60.0,
                        last_update: Instant::now(),
                    },
                ),
            );
            rates.insert(
                "op-bob".to_string(),
                (
                    3,
                    Ewma {
                        value: 1.0,
                        tau: 60.0,
                        last_update: Instant::now(),
                    },
                ),
            );
            rates.insert(
                "user-charlie".to_string(),
                (
                    10,
                    Ewma {
                        value: 5.0,
                        tau: 60.0,
                        last_update: Instant::now(),
                    },
                ),
            );
        }

        let app = build_router(state);
        let addr = serve(app).await;
        let client = Client::new();
        let resp = client
            .get(format!("http://{}/metrics", addr))
            .send()
            .await
            .unwrap();
        let body = resp.text().await.unwrap();

        // Operator clients should NOT appear individually
        assert!(
            !body.contains("client=\"op-alice\""),
            "operator op-alice should be hidden:\n{body}"
        );
        assert!(
            !body.contains("client=\"op-bob\""),
            "operator op-bob should be hidden:\n{body}"
        );

        // Single _operator entry with summed values
        assert!(
            body.contains(
                "anthropic_client_token_usage_total{client=\"_operator\",type=\"input\"} 150"
            ),
            "operator input tokens should sum to 150:\n{body}"
        );
        assert!(
            body.contains(
                "anthropic_client_token_usage_total{client=\"_operator\",type=\"output\"} 300"
            ),
            "operator output tokens should sum to 300:\n{body}"
        );
        assert!(
            body.contains("anthropic_client_requests_total{client=\"_operator\"} 8"),
            "operator requests should sum to 8:\n{body}"
        );

        // Regular client should appear normally
        assert!(
            body.contains("client=\"user-charlie\""),
            "non-operator should appear:\n{body}"
        );
    }

    #[tokio::test]
    async fn metrics_help_type_uniqueness() {
        let (mock_url, _handle) = spawn_mock_upstream().await;
        let (app, state) = test_app(&mock_url, None);

        // Set some state so all metric families are populated
        {
            let mut info = state.accounts[0].rate_info.write().await;
            info.utilization_5h = Some(0.5);
            info.remaining_requests = Some(100);
            info.remaining_tokens = Some(50000);
            info.limit_requests = Some(1000);
            info.limit_tokens = Some(100000);
        }
        state.accounts[0].requests.store(10, Ordering::Relaxed);

        let addr = serve(app).await;
        let client = Client::new();
        let resp = client
            .get(format!("http://{}/metrics", addr))
            .send()
            .await
            .unwrap();
        let body = resp.text().await.unwrap();

        // Parse all # TYPE lines and verify each metric family appears exactly once
        let mut type_counts: HashMap<String, u32> = HashMap::new();
        for line in body.lines() {
            if line.starts_with("# TYPE ") {
                let family = line
                    .strip_prefix("# TYPE ")
                    .unwrap()
                    .split_whitespace()
                    .next()
                    .unwrap()
                    .to_string();
                *type_counts.entry(family).or_insert(0) += 1;
            }
        }

        for (family, count) in &type_counts {
            assert_eq!(
                *count, 1,
                "# TYPE for {family} appears {count} times, expected exactly once"
            );
        }

        // Also verify HELP lines match TYPE lines
        let mut help_families: Vec<String> = body
            .lines()
            .filter(|l| l.starts_with("# HELP "))
            .map(|l| {
                l.strip_prefix("# HELP ")
                    .unwrap()
                    .split_whitespace()
                    .next()
                    .unwrap()
                    .to_string()
            })
            .collect();
        let mut type_families: Vec<String> = type_counts.keys().cloned().collect();
        help_families.sort();
        type_families.sort();
        assert_eq!(
            help_families, type_families,
            "HELP and TYPE families should match"
        );
    }

    #[tokio::test]
    async fn metrics_projected_throttle() {
        let (mock_url, _handle) = spawn_mock_upstream().await;
        let (app, state) = test_app(&mock_url, None);

        // acct-a: eff_util >= 0.5 and br_1h >= 0.01 → projected_throttle IS emitted
        {
            let mut info = state.accounts[0].rate_info.write().await;
            info.utilization_5h = Some(0.7);
            info.remaining_requests = Some(500);
            info.limit_requests = Some(2000);
        }
        {
            let mut br = state.accounts[0].burn_rate.lock().unwrap();
            br.rate_1h.value = 2.0;
        }

        // acct-b: defaults (no util, no burn rate) → eff_util < 0.5 → NOT emitted

        let addr = serve(app).await;
        let client = Client::new();
        let resp = client
            .get(format!("http://{}/metrics", addr))
            .send()
            .await
            .unwrap();
        let body = resp.text().await.unwrap();

        // acct-a: headroom=500, br_1h=2.0 → (500/2.0)*60 = 15000
        assert!(
            body.contains("anthropic_account_projected_throttle_seconds{account=\"acct-a\"} 15000"),
            "acct-a should have projected_throttle:\n{body}"
        );
        // acct-b should NOT have projected_throttle (eff_util < 0.5)
        assert!(
            !body.contains("anthropic_account_projected_throttle_seconds{account=\"acct-b\"}"),
            "acct-b should NOT have projected_throttle:\n{body}"
        );
    }

    #[tokio::test]
    async fn metrics_client_budgets_and_rpm() {
        let accounts = vec![make_account("acct-a", "sk-ant-api-test-aaa")];
        let today = AppState::now_epoch() / 86400;

        let mut client_budgets = HashMap::new();
        client_budgets.insert("claude-code".to_string(), 1_000_000u64);

        let mut budget_usage_map = HashMap::new();
        budget_usage_map.insert("claude-code".to_string(), (today, 400_000u64));

        let mut client_rates_map: HashMap<String, (u64, Ewma)> = HashMap::new();
        let mut ewma = Ewma::new(TAU_5M);
        ewma.value = 3.5;
        client_rates_map.insert("claude-code".to_string(), (50, ewma));

        let state = Arc::new(AppState {
            client: Client::builder()
                .timeout(Duration::from_secs(5))
                .build()
                .unwrap(),
            upstream: "http://127.0.0.1:1".to_string(),
            accounts,
            robin: AtomicUsize::new(0),
            routing_strategy: RoutingStrategy::default(),
            cooldown: Duration::from_secs(60),
            state_path: PathBuf::from("/tmp/anthropic-lb-test.state.json"),
            proxy_key: None,
            allowed_ips: vec![],
            upstreams: vec![],
            client_names: HashMap::new(),
            auto_cache: true,
            client_usage: Mutex::new(HashMap::new()),
            shadow_log_tx: None,
            shadow_log_dropped: AtomicU64::new(0),
            client_budgets,
            budget_usage: Mutex::new(budget_usage_map),
            client_utilization_limits: HashMap::new(),
            operators: vec![],
            emergency_brake: true,
            emergency_threshold: DEFAULT_EMERGENCY_THRESHOLD,
            client_request_rates: Mutex::new(client_rates_map),
            soft_limit: 1.0,
            redis: None,
            cluster_info_cache: Mutex::new(None),
            next_req_id: AtomicU64::new(0),
            instance_id: 0,
            probe_interval_secs: 300,
        });

        let app = build_router(state);
        let addr = serve(app).await;
        let client = Client::new();
        let resp = client
            .get(format!("http://{}/metrics", addr))
            .send()
            .await
            .unwrap();
        let body = resp.text().await.unwrap();

        // R3.4: Budget metrics
        assert!(
            body.contains("anthropic_client_budget_limit{client=\"claude-code\"} 1000000"),
            "missing budget_limit:\n{body}"
        );
        assert!(
            body.contains("anthropic_client_budget_used{client=\"claude-code\"} 400000"),
            "missing budget_used:\n{body}"
        );
        assert!(
            body.contains("anthropic_client_budget_remaining{client=\"claude-code\"} 600000"),
            "missing budget_remaining:\n{body}"
        );

        // R3.3: Client RPM
        assert!(
            body.contains("anthropic_client_requests_per_minute{client=\"claude-code\"} 3.5"),
            "missing client RPM:\n{body}"
        );
    }

    #[tokio::test]
    async fn metrics_aggregate_headroom_and_share() {
        let (mock_url, _handle) = spawn_mock_upstream().await;
        let (app, state) = test_app(&mock_url, None);

        // Set headroom on both accounts
        {
            let mut info = state.accounts[0].rate_info.write().await;
            info.remaining_requests = Some(1000);
        }
        {
            let mut info = state.accounts[1].rate_info.write().await;
            info.remaining_requests = Some(500);
        }

        // Set client request rates for consumer_share
        {
            let mut rates = state.client_request_rates.lock().unwrap();
            let mut ewma_a = Ewma::new(TAU_5M);
            ewma_a.value = 6.0;
            rates.insert("user-a".to_string(), (100, ewma_a));
            let mut ewma_b = Ewma::new(TAU_5M);
            ewma_b.value = 4.0;
            rates.insert("user-b".to_string(), (50, ewma_b));
        }

        let addr = serve(app).await;
        let client = Client::new();
        let resp = client
            .get(format!("http://{}/metrics", addr))
            .send()
            .await
            .unwrap();
        let body = resp.text().await.unwrap();

        // R4.1: Total headroom = 1000 + 500
        assert!(
            body.contains("anthropic_total_headroom_requests 1500"),
            "missing total_headroom:\n{body}"
        );

        // R4.2: Consumer share = rpm / total_rpm
        // user-a: 6.0/10.0 = 0.6, user-b: 4.0/10.0 = 0.4
        assert!(
            body.contains("anthropic_consumer_share{client=\"user-a\"} 0.6"),
            "missing consumer_share user-a:\n{body}"
        );
        assert!(
            body.contains("anthropic_consumer_share{client=\"user-b\"} 0.4"),
            "missing consumer_share user-b:\n{body}"
        );
    }

    #[tokio::test]
    async fn metrics_claim_utilization_and_waste_risk() {
        let (mock_url, _handle) = spawn_mock_upstream().await;
        let (app, state) = test_app(&mock_url, None);

        let now_epoch = AppState::now_epoch();
        // Set reset to 3.5 days from now (half of 7d window)
        let reset_epoch = now_epoch + 302400; // 3.5 * 86400

        {
            let mut info = state.accounts[0].rate_info.write().await;
            info.claims_7d.insert(
                "claude-sonnet".to_string(),
                ClaimWindowData {
                    utilization: Some(0.65),
                    reset: Some(reset_epoch),
                    status: None,
                    ..Default::default()
                },
            );
        }

        let addr = serve(app).await;
        let client = Client::new();
        let resp = client
            .get(format!("http://{}/metrics", addr))
            .send()
            .await
            .unwrap();
        let body = resp.text().await.unwrap();

        // R5.1: Claim utilization
        assert!(
            body.contains(
                "anthropic_claim_utilization{account=\"acct-a\",claim=\"claude-sonnet\"} 0.65"
            ),
            "missing claim_utilization:\n{body}"
        );

        // R5.2: Claim waste risk — should be present and > 0
        // waste_risk(0.65, reset_epoch, now_epoch):
        //   remaining_fraction = 302400 / 604800 = 0.5
        //   unused = 1.0 - 0.65 = 0.35
        //   waste_risk = 0.35 / 0.5 = 0.7
        assert!(
            body.contains(
                "anthropic_claim_waste_risk{account=\"acct-a\",claim=\"claude-sonnet\"} 0.7"
            ),
            "missing claim_waste_risk:\n{body}"
        );

        // acct-b has no claims → no claim metrics for it
        assert!(
            !body.contains("anthropic_claim_utilization{account=\"acct-b\""),
            "acct-b should have no claim_utilization:\n{body}"
        );
    }

    #[tokio::test]
    async fn metrics_reset_seconds_and_account_waste_risk() {
        let (mock_url, _handle) = spawn_mock_upstream().await;
        let (app, state) = test_app(&mock_url, None);

        let now_epoch = AppState::now_epoch();
        let reset_5h = now_epoch + 7200; // 2 hours
        let reset_7d = now_epoch + 302400; // 3.5 days

        {
            let mut info = state.accounts[0].rate_info.write().await;
            info.reset_5h = Some(reset_5h);
            info.reset_7d = Some(reset_7d);
            info.claims_7d.insert(
                "claude-sonnet".to_string(),
                ClaimWindowData {
                    utilization: Some(0.30),
                    reset: Some(reset_7d),
                    status: None,
                    ..Default::default()
                },
            );
        }

        let addr = serve(app).await;
        let client = Client::new();
        let resp = client
            .get(format!("http://{}/metrics", addr))
            .send()
            .await
            .unwrap();
        let body = resp.text().await.unwrap();

        // R6.1: reset_seconds for 5h window — should be close to 7200
        assert!(
            body.contains("# TYPE anthropic_account_reset_seconds gauge"),
            "missing reset_seconds type line:\n{body}"
        );
        // Parse the actual value and check within ±5s tolerance
        let line_5h = body
            .lines()
            .find(|l| {
                l.contains("anthropic_account_reset_seconds")
                    && l.contains("acct-a")
                    && l.contains("5h")
            })
            .expect("missing reset_seconds 5h for acct-a");
        let val_5h: f64 = line_5h.split_whitespace().last().unwrap().parse().unwrap();
        assert!(
            (val_5h - 7200.0).abs() < 5.0,
            "reset_seconds 5h should be ~7200, got {val_5h}"
        );

        // R6.2: reset_seconds for 7d window — should be close to 302400
        let line_7d = body
            .lines()
            .find(|l| {
                l.contains("anthropic_account_reset_seconds")
                    && l.contains("acct-a")
                    && l.contains("7d")
            })
            .expect("missing reset_seconds 7d for acct-a");
        let val_7d: f64 = line_7d.split_whitespace().last().unwrap().parse().unwrap();
        assert!(
            (val_7d - 302400.0).abs() < 5.0,
            "reset_seconds 7d should be ~302400, got {val_7d}"
        );

        // R6.3: account_waste_risk — max claim waste_risk for acct-a
        // waste_risk(0.30, reset_7d, now_epoch):
        //   remaining_fraction = 302400 / 604800 = 0.5
        //   unused = 1.0 - 0.30 = 0.70
        //   waste_risk = 0.70 / 0.5 = 1.4
        assert!(
            body.contains("# TYPE anthropic_account_waste_risk gauge"),
            "missing account_waste_risk type line:\n{body}"
        );
        assert!(
            body.contains("anthropic_account_waste_risk{account=\"acct-a\"} 1.4"),
            "missing account_waste_risk for acct-a:\n{body}"
        );

        // R6.4: acct-b has no claims → no account_waste_risk
        assert!(
            !body.contains("anthropic_account_waste_risk{account=\"acct-b\""),
            "acct-b should have no account_waste_risk:\n{body}"
        );

        // R6.5: acct-b has no reset data → no reset_seconds
        assert!(
            !body.contains("anthropic_account_reset_seconds{account=\"acct-b\""),
            "acct-b should have no reset_seconds:\n{body}"
        );
    }

    #[tokio::test]
    async fn metrics_status_gate_and_data_age() {
        let (mock_url, _handle) = spawn_mock_upstream().await;
        let (app, state) = test_app(&mock_url, None);

        let now_epoch = AppState::now_epoch();
        let data_age_secs = 120u64;

        {
            let mut info = state.accounts[0].rate_info.write().await;
            info.utilization_5h = Some(0.60);
            info.reset_5h = Some(now_epoch + 7200);
            info.status_5h = Some("allowed_warning".to_string());
            info.utilization_7d = Some(0.40);
            info.reset_7d = Some(now_epoch + 302400);
            info.status_7d = Some("throttled".to_string());
            info.last_updated_epoch = Some(now_epoch - data_age_secs);
            info.claims_7d.insert(
                "seven_day".to_string(),
                ClaimWindowData {
                    utilization: Some(0.40),
                    reset: Some(now_epoch + 302400),
                    status: Some("throttled".to_string()),
                    ..Default::default()
                },
            );
        }

        // Populate the effective gate atomic
        state.refresh_metrics_weights().await;

        let addr = serve(app).await;
        let client = Client::new();
        let resp = client
            .get(format!("http://{}/metrics", addr))
            .send()
            .await
            .unwrap();
        let body = resp.text().await.unwrap();

        // #49: rate_limit_status — allowed_warning=1 for 5h, throttled=2 for 7d
        assert!(
            body.contains(
                "anthropic_account_rate_limit_status{account=\"acct-a\",window=\"5h\"} 1"
            ),
            "missing status 5h=1 (allowed_warning):\n{body}"
        );
        assert!(
            body.contains(
                "anthropic_account_rate_limit_status{account=\"acct-a\",window=\"7d\"} 2"
            ),
            "missing status 7d=2 (throttled):\n{body}"
        );
        // acct-b has no status data → should still emit 0 (allowed/None)
        assert!(
            body.contains(
                "anthropic_account_rate_limit_status{account=\"acct-b\",window=\"5h\"} 0"
            ),
            "acct-b 5h should be 0 (allowed):\n{body}"
        );

        // #50: effective_gate — should be > 0 for acct-a (has utilization data)
        assert!(
            body.contains("# TYPE anthropic_account_effective_gate gauge"),
            "missing effective_gate TYPE:\n{body}"
        );
        let gate_line = body
            .lines()
            .find(|l| l.contains("anthropic_account_effective_gate") && l.contains("acct-a"))
            .expect("missing effective_gate for acct-a");
        let gate_val: f64 = gate_line
            .split_whitespace()
            .last()
            .unwrap()
            .parse()
            .unwrap();
        assert!(
            gate_val > 0.0,
            "effective_gate for acct-a should be > 0, got {gate_val}"
        );

        // #51: data_age_seconds — should be ~120s for acct-a
        assert!(
            body.contains("# TYPE anthropic_account_data_age_seconds gauge"),
            "missing data_age_seconds TYPE:\n{body}"
        );
        let age_line = body
            .lines()
            .find(|l| l.contains("anthropic_account_data_age_seconds") && l.contains("acct-a"))
            .expect("missing data_age_seconds for acct-a");
        let age_val: f64 = age_line.split_whitespace().last().unwrap().parse().unwrap();
        assert!(
            (age_val - data_age_secs as f64).abs() < 5.0,
            "data_age_seconds should be ~{data_age_secs}, got {age_val}"
        );

        // acct-b has no last_updated_epoch → no data_age line
        assert!(
            !body.contains("anthropic_account_data_age_seconds{account=\"acct-b\""),
            "acct-b should have no data_age_seconds:\n{body}"
        );
    }

    /// Unit: refresh_metrics_weights() persists routing weights and shares to
    /// atomics on each Account, with shares normalized to sum ≈ 1.0 and zeros
    /// for rejected accounts and accounts above soft_limit.
    #[tokio::test]
    async fn refresh_metrics_weights_persists_atomics() {
        let acct_a = make_account("a", "sk-ant-api-a");
        let acct_b = make_account("b", "sk-ant-api-b");
        let acct_c = make_account("c", "sk-ant-api-c");
        let state = test_state_with(vec![acct_a, acct_b, acct_c]);

        let now = AppState::now_epoch();

        // a: 5h=0.20 (healthy), b: 5h=0.30 (healthy)
        for (i, util) in [(0, 0.20), (1, 0.30)].iter() {
            let mut info = state.accounts[*i].rate_info.write().await;
            info.utilization_5h = Some(*util);
            info.reset_5h = Some(now + 10000);
            info.utilization = Some(*util);
            info.claims_7d.clear();
        }
        // c: status=rejected → status_to_floor → gate=1.0 → weight=0
        {
            let mut info = state.accounts[2].rate_info.write().await;
            info.utilization_5h = Some(0.10);
            info.reset_5h = Some(now + 10000);
            info.utilization = Some(0.10);
            info.status_5h = Some("rejected".to_string());
            info.claims_7d.clear();
        }

        state.refresh_metrics_weights().await;

        let read_weight = |i: usize| {
            f64::from_bits(
                state.accounts[i]
                    .last_routing_weight
                    .load(Ordering::Relaxed),
            )
        };
        let read_share =
            |i: usize| f64::from_bits(state.accounts[i].last_routing_share.load(Ordering::Relaxed));

        // a and b are healthy: positive weight + positive share
        assert!(read_weight(0) > 0.0, "a should have non-zero weight");
        assert!(read_weight(1) > 0.0, "b should have non-zero weight");
        assert!(read_share(0) > 0.0, "a should have non-zero share");
        assert!(read_share(1) > 0.0, "b should have non-zero share");

        // c was rejected (gate=1.0) → must be zeroed
        assert_eq!(read_weight(2), 0.0, "c (rejected) must have zero weight");
        assert_eq!(read_share(2), 0.0, "c (rejected) must have zero share");

        // Shares of healthy accounts sum to ≈ 1.0
        let total_share = read_share(0) + read_share(1) + read_share(2);
        assert!(
            (total_share - 1.0).abs() < 1e-9,
            "shares should sum to 1.0, got {total_share}"
        );

        // Lower-utilization account should win the larger share (a < b)
        assert!(
            read_share(0) > read_share(1),
            "a (lower 5h util) should have larger share than b: a={}, b={}",
            read_share(0),
            read_share(1)
        );
    }

    /// Regression: when SOME accounts are above soft_limit but at least one
    /// is healthy, the soft-limited ones must be zeroed (mirrors pick_account
    /// excluding them). When ALL accounts are above soft_limit, none are
    /// zeroed — graceful degradation, the dashboard reflects what
    /// pick_account would actually still route to.
    #[tokio::test]
    async fn refresh_metrics_weights_soft_limit_graceful_degradation() {
        let now = AppState::now_epoch();

        // Scenario 1: mixed pool. a=healthy(0.20), b=soft-limited(0.95).
        // soft_limit=0.90 → b should be zeroed, a gets all weight.
        {
            let state = test_state_with_soft_limit(
                vec![
                    make_account("a", "sk-ant-api-a"),
                    make_account("b", "sk-ant-api-b"),
                ],
                0.90,
            );
            for (i, util) in [(0, 0.20), (1, 0.95)].iter() {
                let mut info = state.accounts[*i].rate_info.write().await;
                info.utilization_5h = Some(*util);
                info.reset_5h = Some(now + 10000);
                info.utilization = Some(*util);
                info.claims_7d.clear();
            }

            state.refresh_metrics_weights().await;

            let w_a = f64::from_bits(
                state.accounts[0]
                    .last_routing_weight
                    .load(Ordering::Relaxed),
            );
            let w_b = f64::from_bits(
                state.accounts[1]
                    .last_routing_weight
                    .load(Ordering::Relaxed),
            );
            let s_a = f64::from_bits(state.accounts[0].last_routing_share.load(Ordering::Relaxed));
            let s_b = f64::from_bits(state.accounts[1].last_routing_share.load(Ordering::Relaxed));

            assert!(w_a > 0.0, "healthy a should have non-zero weight");
            assert_eq!(
                w_b, 0.0,
                "soft-limited b should be zeroed when a is healthy"
            );
            assert!(
                (s_a - 1.0).abs() < 1e-9,
                "a should have 100% share, got {s_a}"
            );
            assert_eq!(s_b, 0.0, "b should have zero share");
        }

        // Scenario 2: ENTIRE pool above soft_limit. a=0.95, b=0.92.
        // soft_limit=0.90 → both above. Without graceful degradation the
        // dashboard would go blank — instead BOTH should keep non-zero shares
        // matching what pick_account would still route to.
        {
            let state = test_state_with_soft_limit(
                vec![
                    make_account("a", "sk-ant-api-a"),
                    make_account("b", "sk-ant-api-b"),
                ],
                0.90,
            );
            for (i, util) in [(0, 0.95), (1, 0.92)].iter() {
                let mut info = state.accounts[*i].rate_info.write().await;
                info.utilization_5h = Some(*util);
                info.reset_5h = Some(now + 10000);
                info.utilization = Some(*util);
                info.claims_7d.clear();
            }

            state.refresh_metrics_weights().await;

            let w_a = f64::from_bits(
                state.accounts[0]
                    .last_routing_weight
                    .load(Ordering::Relaxed),
            );
            let w_b = f64::from_bits(
                state.accounts[1]
                    .last_routing_weight
                    .load(Ordering::Relaxed),
            );
            let s_a = f64::from_bits(state.accounts[0].last_routing_share.load(Ordering::Relaxed));
            let s_b = f64::from_bits(state.accounts[1].last_routing_share.load(Ordering::Relaxed));

            assert!(
                w_a > 0.0,
                "degraded a should still have non-zero weight (graceful degradation)"
            );
            assert!(
                w_b > 0.0,
                "degraded b should still have non-zero weight (graceful degradation)"
            );
            assert!(
                (s_a + s_b - 1.0).abs() < 1e-9,
                "shares should sum to 1.0 in degraded pool"
            );
            // b has lower utilization → larger share
            assert!(
                s_b > s_a,
                "b (lower util) should outweigh a in degraded pool: a={s_a}, b={s_b}"
            );
        }
    }

    #[tokio::test]
    async fn metrics_routing_weight_and_share_present() {
        let (mock_url, _handle) = spawn_mock_upstream().await;
        let (app, state) = test_app(&mock_url, None);

        let now_epoch = AppState::now_epoch();

        {
            let mut info = state.accounts[0].rate_info.write().await;
            info.utilization = Some(0.20);
            info.utilization_5h = Some(0.20);
            info.reset_5h = Some(now_epoch + NEAR_RESET_5H_SECS as u64 + 600);
            info.status_5h = Some("allowed".to_string());
            info.claims_7d.insert(
                "seven_day".to_string(),
                ClaimWindowData {
                    utilization: Some(0.50),
                    reset: Some(now_epoch + TOTAL_7D_SECS as u64),
                    status: Some("allowed".to_string()),
                    ..Default::default()
                },
            );
            info.utilization_7d = Some(0.50);
            info.reset_7d = Some(now_epoch + TOTAL_7D_SECS as u64);
            info.status_7d = Some("allowed".to_string());
        }
        {
            let mut info = state.accounts[1].rate_info.write().await;
            info.utilization = Some(1.0);
            info.utilization_5h = Some(1.0);
            info.reset_5h = Some(now_epoch + NEAR_RESET_5H_SECS as u64 + 600);
            info.status_5h = Some("allowed".to_string());
        }
        state.refresh_metrics_weights().await;

        let addr = serve(app).await;
        let client = Client::new();
        let resp = client
            .get(format!("http://{}/metrics", addr))
            .send()
            .await
            .unwrap();
        let body = resp.text().await.unwrap();

        assert!(
            body.contains("anthropic_account_routing_weight{account=\"acct-a\"} 0.4"),
            "missing routing_weight:\n{body}"
        );
        assert!(
            body.contains("anthropic_account_routing_share{account=\"acct-a\"} 1"),
            "missing routing_share:\n{body}"
        );
        assert!(
            body.contains("# TYPE anthropic_account_routing_weight gauge"),
            "missing routing_weight TYPE header:\n{body}"
        );
        assert!(
            body.contains("# TYPE anthropic_account_routing_share gauge"),
            "missing routing_share TYPE header:\n{body}"
        );
    }

    /// Integration: GET /metrics emits anthropic_account_routing_weight and
    /// anthropic_account_routing_share for non-passthrough accounts only,
    /// with the values populated by refresh_metrics_weights().
    #[tokio::test]
    async fn metrics_routing_weight_and_share() {
        let (mock_url, _handle) = spawn_mock_upstream().await;
        let (app, state) = test_app(&mock_url, None);

        // Default test_app builds 2 non-passthrough accounts: acct-a, acct-b.
        let now = AppState::now_epoch();
        {
            let mut info = state.accounts[0].rate_info.write().await;
            info.utilization_5h = Some(0.25);
            info.reset_5h = Some(now + 10000);
            info.utilization = Some(0.25);
            info.claims_7d.clear();
        }
        {
            let mut info = state.accounts[1].rate_info.write().await;
            info.utilization_5h = Some(0.50);
            info.reset_5h = Some(now + 10000);
            info.utilization = Some(0.50);
            info.claims_7d.clear();
        }
        state.refresh_metrics_weights().await;

        let addr = serve(app).await;
        let client = Client::new();
        let resp = client
            .get(format!("http://{}/metrics", addr))
            .send()
            .await
            .unwrap();
        let body = resp.text().await.unwrap();

        // Both metric families are emitted with the right HELP/TYPE headers
        assert!(
            body.contains("# HELP anthropic_account_routing_weight"),
            "missing routing_weight HELP:\n{body}"
        );
        assert!(
            body.contains("# TYPE anthropic_account_routing_weight gauge"),
            "missing routing_weight TYPE:\n{body}"
        );
        assert!(
            body.contains("# HELP anthropic_account_routing_share"),
            "missing routing_share HELP:\n{body}"
        );

        // Both accounts get a routing_weight line labeled by name
        assert!(
            body.contains("anthropic_account_routing_weight{account=\"acct-a\"}"),
            "missing acct-a routing_weight:\n{body}"
        );
        assert!(
            body.contains("anthropic_account_routing_weight{account=\"acct-b\"}"),
            "missing acct-b routing_weight:\n{body}"
        );

        // Shares for the two accounts must sum to ≈ 1.0 (parsed out of the body)
        let parse_share = |account: &str| -> f64 {
            let needle = format!("anthropic_account_routing_share{{account=\"{account}\"}} ");
            let line = body
                .lines()
                .find(|l| l.starts_with(&needle))
                .unwrap_or_else(|| panic!("no share line for {account}"));
            line[needle.len()..]
                .trim()
                .parse::<f64>()
                .expect("parseable share")
        };
        let total = parse_share("acct-a") + parse_share("acct-b");
        assert!(
            (total - 1.0).abs() < 1e-9,
            "shares should sum to 1.0, got {total}\n{body}"
        );

        // acct-a (lower utilization) should get the larger share
        assert!(
            parse_share("acct-a") > parse_share("acct-b"),
            "acct-a should outweigh acct-b\n{body}"
        );
    }

    #[tokio::test]
    async fn routing_metrics_zero_share_for_soft_limited_account_matches_pick_account() {
        let mut state = test_state_with(vec![
            make_account("healthy", "sk-ant-api-a"),
            make_account("soft-limited", "sk-ant-api-b"),
        ]);
        Arc::get_mut(&mut state).unwrap().soft_limit = 0.90;
        let now_epoch = AppState::now_epoch();

        {
            let mut info = state.accounts[0].rate_info.write().await;
            info.utilization = Some(0.30);
            info.utilization_5h = Some(0.30);
            info.reset_5h = Some(now_epoch + 10000);
            info.status_5h = Some("allowed".to_string());
        }
        {
            let mut info = state.accounts[1].rate_info.write().await;
            info.utilization = Some(0.95);
            info.utilization_5h = Some(0.95);
            info.reset_5h = Some(now_epoch + 10000);
            info.status_5h = Some("allowed".to_string());
        }

        state.refresh_metrics_weights().await;
        let mut buf = String::new();
        append_routing_weight_metrics(
            &mut buf,
            &state.accounts,
            &[
                AcctMetricsSnap {
                    name: "healthy".to_string(),
                    utilization: Some(0.30),
                    utilization_5h: Some(0.30),
                    reset_5h: Some(now_epoch + 10000),
                    status_5h: Some("allowed".to_string()),
                    ..Default::default()
                },
                AcctMetricsSnap {
                    name: "soft-limited".to_string(),
                    utilization: Some(0.95),
                    utilization_5h: Some(0.95),
                    reset_5h: Some(now_epoch + 10000),
                    status_5h: Some("allowed".to_string()),
                    ..Default::default()
                },
            ],
        );

        assert!(
            buf.contains("anthropic_account_routing_share{account=\"healthy\"} 1"),
            "healthy account should receive full routing share:
{buf}"
        );
        assert!(
            buf.contains("anthropic_account_routing_share{account=\"soft-limited\"} 0"),
            "soft-limited account should have zero routing share:
{buf}"
        );
        assert!(
            buf.contains("anthropic_account_routing_weight{account=\"soft-limited\"} 0"),
            "soft-limited account should have zero routing weight:
{buf}"
        );

        for i in 0..20 {
            let key = format!("client-{i}");
            let idx = state.pick_account(Some(&key), "any", &[]).await.unwrap();
            assert_eq!(
                idx, 0,
                "client '{}' routed to soft-limited account despite exported zero share",
                key
            );
        }
    }

    /// Integration: passthrough accounts must NOT appear in routing_weight or
    /// routing_share output (refresh_metrics_weights skips them).
    #[tokio::test]
    async fn metrics_routing_weight_omits_passthrough() {
        let acct_a = make_account("a", "sk-ant-api-a");
        let acct_pt = make_account("pt", "passthrough");
        let state = test_state_with(vec![acct_a, acct_pt]);

        let now = AppState::now_epoch();
        {
            let mut info = state.accounts[0].rate_info.write().await;
            info.utilization_5h = Some(0.25);
            info.reset_5h = Some(now + 10000);
            info.utilization = Some(0.25);
            info.claims_7d.clear();
        }
        state.refresh_metrics_weights().await;

        let app = Router::new()
            .route("/metrics", axum::routing::get(metrics_handler))
            .with_state(state.clone());
        let addr = serve(app).await;
        let client = Client::new();
        let body = client
            .get(format!("http://{}/metrics", addr))
            .send()
            .await
            .unwrap()
            .text()
            .await
            .unwrap();

        // a is present
        assert!(
            body.contains("anthropic_account_routing_weight{account=\"a\"}"),
            "missing routing_weight for a:\n{body}"
        );
        // pt (passthrough) must be absent from BOTH families
        assert!(
            !body.contains("anthropic_account_routing_weight{account=\"pt\""),
            "passthrough account must not appear in routing_weight:\n{body}"
        );
        assert!(
            !body.contains("anthropic_account_routing_share{account=\"pt\""),
            "passthrough account must not appear in routing_share:\n{body}"
        );
    }

    #[test]
    fn oauth_system_prompt_injects_when_missing() {
        let mut body = serde_json::json!({
            "model": "claude-sonnet-4-6",
            "messages": [{"role": "user", "content": "hi"}],
            "max_tokens": 5
        });
        inject_oauth_system_prompt(&mut body);
        let system = body.get("system").unwrap().as_array().unwrap();
        assert_eq!(system.len(), 1);
        assert_eq!(system[0]["text"].as_str().unwrap(), OAUTH_SYSTEM_PROMPT);
    }

    #[test]
    fn oauth_system_prompt_prepends_to_existing_string() {
        let mut body = serde_json::json!({
            "model": "claude-sonnet-4-6",
            "system": "Be helpful.",
            "messages": [{"role": "user", "content": "hi"}],
            "max_tokens": 5
        });
        inject_oauth_system_prompt(&mut body);
        let system = body.get("system").unwrap().as_array().unwrap();
        assert_eq!(system.len(), 2);
        assert_eq!(system[0]["text"].as_str().unwrap(), OAUTH_SYSTEM_PROMPT);
        assert_eq!(system[1]["text"].as_str().unwrap(), "Be helpful.");
    }

    #[test]
    fn oauth_system_prompt_prepends_to_existing_array() {
        let mut body = serde_json::json!({
            "model": "claude-sonnet-4-6",
            "system": [{"type": "text", "text": "Be helpful."}],
            "messages": [{"role": "user", "content": "hi"}],
            "max_tokens": 5
        });
        inject_oauth_system_prompt(&mut body);
        let system = body.get("system").unwrap().as_array().unwrap();
        assert_eq!(system.len(), 2);
        assert_eq!(system[0]["text"].as_str().unwrap(), OAUTH_SYSTEM_PROMPT);
        assert_eq!(system[1]["text"].as_str().unwrap(), "Be helpful.");
    }

    #[test]
    fn oauth_system_prompt_noop_when_already_present() {
        let mut body = serde_json::json!({
            "model": "claude-sonnet-4-6",
            "system": [{"type": "text", "text": OAUTH_SYSTEM_PROMPT}],
            "messages": [{"role": "user", "content": "hi"}],
            "max_tokens": 5
        });
        inject_oauth_system_prompt(&mut body);
        let system = body.get("system").unwrap().as_array().unwrap();
        assert_eq!(system.len(), 1, "should not duplicate");
    }

    #[test]
    fn oauth_system_prompt_noop_when_prompt_is_prefix_string() {
        // CC may send the identity prompt as prefix of a longer system string
        let system_text = format!("{}\n\nYou are an interactive agent.", OAUTH_SYSTEM_PROMPT);
        let mut body = serde_json::json!({
            "model": "claude-sonnet-4-6",
            "system": system_text,
            "messages": [{"role": "user", "content": "hi"}],
            "max_tokens": 5
        });
        inject_oauth_system_prompt(&mut body);
        // Should remain a string, untouched
        assert!(body["system"].is_string(), "should not convert to array");
        assert_eq!(body["system"].as_str().unwrap(), system_text);
    }

    #[test]
    fn oauth_system_prompt_noop_when_prompt_is_prefix_array() {
        // CC may embed the identity prompt as prefix of first block text
        let block_text = format!("{}\n\nYou are an interactive agent.", OAUTH_SYSTEM_PROMPT);
        let mut body = serde_json::json!({
            "model": "claude-sonnet-4-6",
            "system": [{"type": "text", "text": block_text}],
            "messages": [{"role": "user", "content": "hi"}],
            "max_tokens": 5
        });
        inject_oauth_system_prompt(&mut body);
        let system = body.get("system").unwrap().as_array().unwrap();
        assert_eq!(system.len(), 1, "should not prepend duplicate");
        assert_eq!(system[0]["text"].as_str().unwrap(), block_text);
    }

    #[test]
    fn oauth_system_prompt_handles_null_system() {
        let mut body = serde_json::json!({
            "model": "claude-sonnet-4-6",
            "system": null,
            "messages": [{"role": "user", "content": "hi"}],
            "max_tokens": 5
        });
        inject_oauth_system_prompt(&mut body);
        let system = body.get("system").unwrap().as_array().unwrap();
        assert_eq!(system.len(), 1);
        assert_eq!(system[0]["text"].as_str().unwrap(), OAUTH_SYSTEM_PROMPT);
    }

    /// Regression: CC 142+ prepends a billing header as system[0], pushing the
    /// CC identity prompt to system[1+]. has_oauth_system_prompt must scan all
    /// blocks, not just the first — otherwise inject_oauth_system_prompt
    /// re-serializes the body, breaking Anthropic's byte-prefix cache matching.
    #[test]
    fn oauth_system_prompt_detected_in_later_block() {
        // system[0] is a non-prompt block (billing header), system[1] has the CC prompt
        let mut body = serde_json::json!({
            "model": "claude-sonnet-4-6",
            "system": [
                {"type": "text", "text": "x-anthropic-billing-header: cc_version=2.1.109"},
                {"type": "text", "text": OAUTH_SYSTEM_PROMPT}
            ],
            "messages": [{"role": "user", "content": "hi"}],
            "max_tokens": 5
        });

        // Must detect prompt at system[1] — no re-injection
        assert!(
            has_oauth_system_prompt(&body),
            "should detect CC prompt in system[1]"
        );
        inject_oauth_system_prompt(&mut body);
        let system = body["system"].as_array().unwrap();
        assert_eq!(
            system.len(),
            2,
            "should not prepend — prompt already present"
        );
        assert_eq!(
            system[1]["text"].as_str().unwrap(),
            OAUTH_SYSTEM_PROMPT,
            "CC prompt should remain at system[1]"
        );
    }

    /// Regression: full proxy roundtrip with OAuth account where the CC prompt
    /// is at system[1+]. Verifies the proxy does not re-serialize the body
    /// (which would break upstream prompt cache matching).
    #[tokio::test]
    async fn oauth_system_prompt_no_reserialize_when_in_later_block() {
        use std::sync::Arc as StdArc;

        // Mock upstream that captures the raw request body
        let captured_body: StdArc<tokio::sync::Mutex<Vec<u8>>> =
            StdArc::new(tokio::sync::Mutex::new(Vec::new()));
        let captured = captured_body.clone();
        let mock_app = Router::new().fallback(any(move |req: Request<Body>| {
            let captured = captured.clone();
            async move {
                let body_bytes = axum::body::to_bytes(req.into_body(), 1024 * 1024)
                    .await
                    .unwrap();
                *captured.lock().await = body_bytes.to_vec();

                let mut resp = axum::Json(serde_json::json!({
                    "id": "msg_test",
                    "type": "message",
                    "content": [{"type": "text", "text": "ok"}],
                    "model": "claude-sonnet-4-6",
                    "stop_reason": "end_turn",
                    "usage": {"input_tokens": 10, "output_tokens": 1}
                }))
                .into_response();
                resp.headers_mut().insert(
                    "anthropic-ratelimit-unified-representative-claim",
                    HeaderValue::from_static("five_hour"),
                );
                resp.headers_mut().insert(
                    "anthropic-ratelimit-unified-5h-utilization",
                    HeaderValue::from_static("0.10"),
                );
                let reset = (std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs()
                    + 3600)
                    .to_string();
                resp.headers_mut().insert(
                    "anthropic-ratelimit-unified-5h-reset",
                    HeaderValue::from_str(&reset).unwrap(),
                );
                resp
            }
        }));

        let mock_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mock_addr = mock_listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(mock_listener, mock_app).await.unwrap();
        });

        // Build app with an OAuth account, auto_cache off for clean signal
        let accounts = vec![make_account("oauth-acct", "sk-ant-oat01-test-token")];
        let state = Arc::new(AppState {
            client: Client::builder()
                .timeout(Duration::from_secs(5))
                .build()
                .unwrap(),
            upstream: format!("http://{}", mock_addr),
            accounts,
            robin: AtomicUsize::new(0),
            routing_strategy: RoutingStrategy::default(),
            cooldown: Duration::from_secs(60),
            state_path: PathBuf::from("/tmp/anthropic-lb-oauth-regression.state.json"),
            proxy_key: None,
            allowed_ips: vec![],
            upstreams: vec![],
            client_names: HashMap::new(),
            auto_cache: false,
            client_usage: Mutex::new(HashMap::new()),
            shadow_log_tx: None,
            shadow_log_dropped: AtomicU64::new(0),
            client_budgets: HashMap::new(),
            budget_usage: Mutex::new(HashMap::new()),
            client_utilization_limits: HashMap::new(),
            operators: vec![],
            emergency_brake: true,
            emergency_threshold: DEFAULT_EMERGENCY_THRESHOLD,
            client_request_rates: Mutex::new(HashMap::new()),
            soft_limit: 1.0,
            redis: None,
            cluster_info_cache: Mutex::new(None),
            next_req_id: AtomicU64::new(0),
            instance_id: 0,
            probe_interval_secs: 300,
        });

        let app = build_router(state);
        let addr = serve(app).await;

        // Request body: CC prompt at system[1], billing header at system[0]
        let request_body = serde_json::json!({
            "model": "claude-sonnet-4-6",
            "system": [
                {"type": "text", "text": "x-anthropic-billing-header: cc_version=2.1.109"},
                {"type": "text", "text": OAUTH_SYSTEM_PROMPT}
            ],
            "messages": [{"role": "user", "content": "hi"}],
            "max_tokens": 5
        });
        let request_bytes = serde_json::to_vec(&request_body).unwrap();

        let client = Client::new();
        let resp = client
            .post(format!("http://{}/v1/messages", addr))
            .header("content-type", "application/json")
            .body(request_bytes.clone())
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);

        // The proxy should have forwarded the original body byte-for-byte (no re-serialization).
        // Serde roundtrip can reorder keys or change whitespace — only raw byte comparison
        // catches the cache-breaking re-serialization bug this test guards against.
        let forwarded = captured_body.lock().await;
        assert_eq!(
            forwarded.as_slice(),
            request_bytes.as_slice(),
            "forwarded body must be byte-identical to request (re-serialization breaks upstream cache)"
        );
        let forwarded_body: serde_json::Value = serde_json::from_slice(&forwarded).unwrap();
        let system = forwarded_body["system"].as_array().unwrap();
        assert_eq!(
            system.len(),
            2,
            "proxy must not prepend another CC prompt — it was already at system[1]"
        );
        assert_eq!(
            system[0]["text"].as_str().unwrap(),
            "x-anthropic-billing-header: cc_version=2.1.109",
            "system[0] should be the billing header, untouched"
        );
        assert_eq!(
            system[1]["text"].as_str().unwrap(),
            OAUTH_SYSTEM_PROMPT,
            "system[1] should be the CC prompt, untouched"
        );
    }

    /// Integration test: verify OAuth accounts get the CC system prompt injected
    /// in requests sent through the OpenAI-compat endpoint.
    #[tokio::test]
    async fn openai_compat_injects_oauth_system_prompt() {
        // Mock upstream that captures and validates the request body
        let mock_app = Router::new().fallback(any(|req: Request<Body>| async move {
            let body_bytes = axum::body::to_bytes(req.into_body(), 1024 * 1024)
                .await
                .unwrap();
            let body: serde_json::Value = serde_json::from_slice(&body_bytes).unwrap();

            // Verify the CC system prompt is the first system block
            let system = body.get("system").expect("missing system field");
            let arr = system.as_array().expect("system should be array");
            assert_eq!(
                arr[0]["text"].as_str().unwrap(),
                OAUTH_SYSTEM_PROMPT,
                "first system block must be CC prompt"
            );

            // Return valid Anthropic response
            let mut resp = axum::Json(serde_json::json!({
                "id": "msg_test",
                "type": "message",
                "content": [{"type": "text", "text": "ok"}],
                "model": "claude-sonnet-4-6",
                "stop_reason": "end_turn",
                "usage": {"input_tokens": 10, "output_tokens": 1}
            }))
            .into_response();
            resp.headers_mut().insert(
                "anthropic-ratelimit-unified-representative-claim",
                HeaderValue::from_static("five_hour"),
            );
            resp.headers_mut().insert(
                "anthropic-ratelimit-unified-5h-utilization",
                HeaderValue::from_static("0.10"),
            );
            resp
        }));

        let mock_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mock_addr = mock_listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(mock_listener, mock_app).await.unwrap();
        });

        // Build app with an OAuth account
        let accounts = vec![make_account("oauth-acct", "sk-ant-oat01-test-token")];
        let state = Arc::new(AppState {
            client: Client::builder()
                .timeout(Duration::from_secs(5))
                .build()
                .unwrap(),
            upstream: format!("http://{}", mock_addr),
            accounts,
            robin: AtomicUsize::new(0),
            routing_strategy: RoutingStrategy::default(),
            cooldown: Duration::from_secs(60),
            state_path: PathBuf::from("/tmp/anthropic-lb-oauth-test.state.json"),
            proxy_key: None,
            allowed_ips: vec![],
            upstreams: vec![],
            client_names: HashMap::new(),
            auto_cache: false, // disable to keep body simple
            client_usage: Mutex::new(HashMap::new()),
            shadow_log_tx: None,
            shadow_log_dropped: AtomicU64::new(0),
            client_budgets: HashMap::new(),
            budget_usage: Mutex::new(HashMap::new()),
            client_utilization_limits: HashMap::new(),
            operators: vec![],
            emergency_brake: true,
            emergency_threshold: DEFAULT_EMERGENCY_THRESHOLD,
            client_request_rates: Mutex::new(HashMap::new()),
            soft_limit: 1.0,
            redis: None,
            cluster_info_cache: Mutex::new(None),
            next_req_id: AtomicU64::new(0),
            instance_id: 0,
            probe_interval_secs: 300,
        });

        let app = Router::new()
            .route(
                "/v1/chat/completions",
                axum::routing::post(openai_chat_handler),
            )
            .with_state(state);

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(
                listener,
                app.into_make_service_with_connect_info::<SocketAddr>(),
            )
            .await
            .unwrap();
        });

        let client = Client::new();
        let resp = client
            .post(format!("http://{}/v1/chat/completions", addr))
            .header("content-type", "application/json")
            .body(r#"{"model":"claude-sonnet-4-6","messages":[{"role":"user","content":"hi"}],"max_tokens":5}"#)
            .send()
            .await
            .unwrap();

        assert_eq!(resp.status(), reqwest::StatusCode::OK);
    }

    /// Test that non-2xx upstream errors are translated to OpenAI error format.
    #[tokio::test]
    async fn openai_compat_translates_upstream_json_error() {
        // Mock upstream returns 400 with Anthropic-format error
        let mock_app = Router::new().fallback(any(|_req: Request<Body>| async {
            Response::builder()
                .status(StatusCode::BAD_REQUEST)
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"type":"error","error":{"type":"invalid_request_error","message":"max_tokens: must be positive"}}"#,
                ))
                .unwrap()
        }));

        let mock_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mock_addr = mock_listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(mock_listener, mock_app).await.unwrap();
        });

        let (app, _state) = test_openai_app(&format!("http://{}", mock_addr), None);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(
                listener,
                app.into_make_service_with_connect_info::<SocketAddr>(),
            )
            .await
            .unwrap();
        });

        let client = Client::new();
        let resp = client
            .post(format!("http://{}/v1/chat/completions", addr))
            .header("content-type", "application/json")
            .body(r#"{"model":"claude-sonnet-4-6","messages":[{"role":"user","content":"hi"}],"max_tokens":5}"#)
            .send()
            .await
            .unwrap();

        assert_eq!(resp.status(), reqwest::StatusCode::BAD_REQUEST);
        let body: serde_json::Value = resp.json().await.unwrap();
        assert_eq!(body["error"]["message"], "max_tokens: must be positive");
        assert_eq!(body["error"]["type"], "invalid_request_error");
        assert!(body["error"]["param"].is_null());
    }

    /// Test that non-JSON upstream errors are wrapped in OpenAI error format.
    #[tokio::test]
    async fn openai_compat_translates_upstream_raw_error() {
        // Mock upstream returns 422 with plain text (non-retryable, non-JSON)
        let mock_app = Router::new().fallback(any(|_req: Request<Body>| async {
            Response::builder()
                .status(StatusCode::UNPROCESSABLE_ENTITY)
                .header("content-type", "text/plain")
                .body(Body::from("upstream timeout"))
                .unwrap()
        }));

        let mock_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mock_addr = mock_listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(mock_listener, mock_app).await.unwrap();
        });

        let (app, _state) = test_openai_app(&format!("http://{}", mock_addr), None);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(
                listener,
                app.into_make_service_with_connect_info::<SocketAddr>(),
            )
            .await
            .unwrap();
        });

        let client = Client::new();
        let resp = client
            .post(format!("http://{}/v1/chat/completions", addr))
            .header("content-type", "application/json")
            .body(r#"{"model":"claude-sonnet-4-6","messages":[{"role":"user","content":"hi"}],"max_tokens":5}"#)
            .send()
            .await
            .unwrap();

        assert_eq!(resp.status(), reqwest::StatusCode::UNPROCESSABLE_ENTITY);
        let body: serde_json::Value = resp.json().await.unwrap();
        assert_eq!(body["error"]["message"], "upstream timeout");
        assert_eq!(body["error"]["type"], "api_error");
    }

    #[test]
    fn inject_auth_api_key() {
        let mut headers = axum::http::HeaderMap::new();
        headers.insert("authorization", HeaderValue::from_static("Bearer old"));
        inject_account_auth(&mut headers, "sk-ant-api-test123", false);
        assert_eq!(headers.get("x-api-key").unwrap(), "sk-ant-api-test123");
        assert!(headers.get("authorization").is_none());
    }

    #[test]
    fn inject_auth_oauth_token() {
        let mut headers = axum::http::HeaderMap::new();
        inject_account_auth(&mut headers, "sk-ant-oat-test123", false);
        assert_eq!(
            headers.get("authorization").unwrap(),
            "Bearer sk-ant-oat-test123"
        );
        assert_eq!(
            headers
                .get("anthropic-dangerous-direct-browser-access")
                .unwrap(),
            "true"
        );
        let beta = headers.get("anthropic-beta").unwrap().to_str().unwrap();
        assert!(beta.contains("oauth-2025-04-20"));
        assert!(beta.contains("claude-code-20250219"));
    }

    #[test]
    fn inject_auth_oauth_merges_multi_value_beta() {
        let mut headers = axum::http::HeaderMap::new();
        headers.append("anthropic-beta", HeaderValue::from_static("existing-flag"));
        headers.append("anthropic-beta", HeaderValue::from_static("another-flag"));
        inject_account_auth(&mut headers, "sk-ant-oat-test123", false);
        let beta = headers.get("anthropic-beta").unwrap().to_str().unwrap();
        assert!(
            beta.contains("existing-flag"),
            "should preserve first header"
        );
        assert!(
            beta.contains("another-flag"),
            "should preserve second header"
        );
        assert!(beta.contains("oauth-2025-04-20"), "should add oauth flag");
        assert!(beta.contains("claude-code-20250219"), "should add cc flag");
    }

    #[test]
    fn inject_auth_passthrough_preserves_headers() {
        let mut headers = axum::http::HeaderMap::new();
        headers.insert(
            "authorization",
            HeaderValue::from_static("Bearer user-token"),
        );
        headers.insert("x-api-key", HeaderValue::from_static("user-key"));
        inject_account_auth(&mut headers, "passthrough", true);
        assert_eq!(headers.get("authorization").unwrap(), "Bearer user-token");
        assert_eq!(headers.get("x-api-key").unwrap(), "user-key");
    }

    #[test]
    fn request_context_trims_whitespace_headers() {
        let state = test_state_with(vec![make_account("a", "sk-ant-api-x")]);
        let mut headers = axum::http::HeaderMap::new();
        headers.insert("x-agent-id", HeaderValue::from_static("  "));
        headers.insert("x-session-id", HeaderValue::from_static(" \t "));
        let ip: IpAddr = "127.0.0.1".parse().unwrap();
        let rctx = RequestContext::from_request(&state, &ip, &headers);
        assert_eq!(rctx.agent_id, "-", "whitespace-only agent_id should be -");
        assert_eq!(
            rctx.session_id, "-",
            "whitespace-only session_id should be -"
        );
        assert!(rctx.affinity_key(&ip).is_none(), "no meaningful identity");
    }
}
