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
    hash::{Hash, Hasher},
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
    /// Client ID of the operator — bypasses all budget/ceiling/emergency checks.
    /// Must be resolvable via client_names IP mapping (not spoofable via header).
    operator: Option<String>,
    /// Emergency brake threshold (0.0-1.0). When ALL accounts exceed this,
    /// non-operator traffic is blocked. Default: 0.95.
    emergency_threshold: Option<f64>,
    /// Utilization soft ceiling (0.0–1.0). Accounts above this are excluded from routing
    /// unless ALL accounts exceed it. Breaks client affinity stickiness on overloaded accounts.
    /// Default: 0.90.
    soft_limit: Option<f64>,
}

#[derive(Deserialize, Clone)]
struct AccountConfig {
    name: String,
    /// Auth token. Use "passthrough" to forward caller's auth headers as-is.
    token: String,
    /// Optional model allowlist. If set, this account only serves these models.
    /// Supports exact names ("claude-sonnet-4-20250514") and prefixes ("claude-opus-*").
    #[serde(default)]
    models: Vec<String>,
}

#[derive(Deserialize, Clone)]
struct UpstreamConfig {
    name: String,
    base_url: String,
    api_key: String,
}

// ── Runtime state ───────────────────────────────────────────────────

#[derive(Default)]
struct RateLimitInfo {
    remaining_requests: Option<u64>,
    remaining_tokens: Option<u64>,
    limit_requests: Option<u64>,
    limit_tokens: Option<u64>,
    /// Unified utilization (0.0 = fresh, 1.0 = exhausted). From representative claim window.
    utilization: Option<f64>,
    /// Per-window utilization for all known windows
    utilization_7d: Option<f64>,
    utilization_5h: Option<f64>,
    /// Which window is the binding constraint (e.g. "five_hour", "seven_day")
    representative_claim: Option<String>,
    /// Epoch seconds when the 5h/7d rate-limit window resets. Used to time-discount
    /// utilization near reset and to detect stale data (reset already passed).
    reset_5h: Option<u64>,
    reset_7d: Option<u64>,
    /// API-side pressure signal. "allowed" = normal, "allowed_warning" = approaching limits
    /// (floor 0.80), "throttled" = actively constrained (floor 0.98, soft-excluded),
    /// "rejected" = hard refusal (floor 1.0, zero bucket share).
    status_5h: Option<String>,
    status_7d: Option<String>,
    hard_limited_until: Option<Instant>,
    #[allow(dead_code)]
    last_updated: Option<Instant>,
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
const DEFAULT_EMERGENCY_THRESHOLD: f64 = 0.95;

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
    shadow_log_tx: Option<tokio::sync::mpsc::UnboundedSender<String>>,
    /// Per-client daily token budgets: client_id → max tokens per day.
    client_budgets: HashMap<String, u64>,
    /// Budget tracking: client_id → (epoch_day, tokens_used). Resets on new day.
    budget_usage: Mutex<HashMap<String, (u64, u64)>>,
    /// Per-client utilization limits: client_id → max effective utilization.
    client_utilization_limits: HashMap<String, f64>,
    /// Operator client ID — never throttled by budgets, ceilings, or emergency brake.
    operator: Option<String>,
    /// Emergency brake threshold. Default: 0.95.
    emergency_threshold: f64,
    /// Per-client request tracking: client_id → (total_requests, rate_ewma)
    client_request_rates: Mutex<HashMap<String, (u64, Ewma)>>,
    /// Utilization soft ceiling. Accounts above this are excluded from routing
    /// unless all candidates exceed it. Default: 0.90.
    soft_limit: f64,
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
    remaining_requests: Option<u64>,
    remaining_tokens: Option<u64>,
    limit_requests: Option<u64>,
    limit_tokens: Option<u64>,
    /// Absolute unix timestamp (secs) when hard limit expires
    hard_limited_until_epoch: Option<u64>,
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
                remaining_requests: info.remaining_requests,
                remaining_tokens: info.remaining_tokens,
                limit_requests: info.limit_requests,
                limit_tokens: info.limit_tokens,
                hard_limited_until_epoch: hard_until_epoch,
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
    async fn probe_account(&self, idx: usize) {
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

        let url = format!("{}/v1/messages", self.upstream);
        let body = serde_json::json!({
            "model": "claude-sonnet-4-20250514",
            "max_tokens": 1,
            "system": [{"type": "text", "text": "You are Claude Code, Anthropic's official CLI for Claude."}],
            "messages": [{"role": "user", "content": "."}]
        });

        let mut req = self
            .client
            .post(&url)
            .header("content-type", "application/json")
            .header("anthropic-version", "2023-06-01")
            .header("anthropic-beta", "claude-code-20250219,oauth-2025-04-20")
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
                }
                self.save_state().await;
                let info = acct.rate_info.read().await;
                info!(
                    account = acct.name,
                    status = status.as_u16(),
                    utilization = ?info.utilization,
                    util_7d = ?info.utilization_7d,
                    util_5h = ?info.utilization_5h,
                    claim = ?info.representative_claim,
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
                info.reset_7d = pa.reset_7d;
                info.status_5h = pa.status_5h.clone();
                info.status_7d = pa.status_7d.clone();

                // Invalidate stale per-window data: if a window's reset is in the past,
                // the block has reset and our utilization/status numbers are meaningless.
                // Clear them so pick_account falls through to "unknown" (0.5) rather than
                // routing on stale data until the first probe refreshes. (Bug #2, #3)
                let stale_5h = info.reset_5h.is_none_or(|r| r <= now_epoch);
                let stale_7d = info.reset_7d.is_none_or(|r| r <= now_epoch);
                if stale_5h {
                    info.utilization_5h = None;
                    info.reset_5h = None;
                    info.status_5h = None;
                }
                if stale_7d {
                    info.utilization_7d = None;
                    info.reset_7d = None;
                    info.status_7d = None;
                }
                if stale_5h && stale_7d {
                    // Both windows stale — the derived unified field is also meaningless
                    info.utilization = None;
                }

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

/// Seven-day claim penalty threshold. Below this, no penalty applied.
const CLAIM_PENALTY_THRESHOLD: f64 = 0.70;
/// Quadratic penalty multiplier. Calibrated so 90% raw → ~1.033 effective (zero headroom).
const CLAIM_PENALTY_MULTIPLIER: f64 = 3.33;

/// Apply a quadratic penalty to seven-day utilization above the threshold.
/// Five-hour windows are NOT penalized because they recover quickly.
/// Penalty curve: 70%→0.70, 80%→0.833, 85%→0.925, 90%→1.033 (zero headroom).
fn claim_penalty_7d(adj_7d: f64) -> f64 {
    if adj_7d > CLAIM_PENALTY_THRESHOLD {
        let excess = adj_7d - CLAIM_PENALTY_THRESHOLD;
        adj_7d + excess * excess * CLAIM_PENALTY_MULTIPLIER
    } else {
        adj_7d
    }
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
/// Returns (utilization, source_label) for logging/routing.
///
/// Fallback chain:
/// 1. Both windows adjusted → take max (most constrained wins), 7d penalized by claim_penalty_7d
/// 2. Only one window → use it (the other is stale or absent), 7d penalized
/// 3. Neither window → raw unified util, then legacy token ratio, then 0.5
fn effective_utilization(info: &RateLimitInfo, now_epoch: u64) -> (f64, &'static str) {
    let adj_5h = time_adjusted_utilization(
        info.utilization_5h,
        info.reset_5h,
        info.status_5h.as_deref(),
        NEAR_RESET_5H_SECS,
        now_epoch,
    );
    let adj_7d = time_adjusted_utilization(
        info.utilization_7d,
        info.reset_7d,
        info.status_7d.as_deref(),
        NEAR_RESET_7D_SECS,
        now_epoch,
    );
    // Apply claim penalty to 7d BEFORE the max merge
    let adj_7d = adj_7d.map(claim_penalty_7d);

    match (adj_5h, adj_7d) {
        (Some(a), Some(b)) => (a.max(b), "time_adjusted"),
        (Some(a), None) => (a, "time_adjusted_5h"),
        (None, Some(b)) => (b, "time_adjusted_7d"),
        (None, None) => {
            // Fallback: raw unified (no time adjustment), legacy tokens, or unknown
            if let Some(util) = info.utilization {
                (util, "unified")
            } else if let Some(remaining) = info.remaining_tokens {
                let limit = info.limit_tokens.unwrap_or(1_000_000);
                (1.0 - (remaining as f64 / limit as f64), "legacy")
            } else {
                (0.5, "unknown")
            }
        }
    }
}

impl AppState {
    /// Pick the best available account using headroom-proportional weighted bucket hashing.
    ///
    /// Each non-hard-limited account gets a "bucket" proportional to its available headroom
    /// (1.0 - utilization). An affinity key hashes to a stable position in the bucket space,
    /// providing stickiness. As utilization changes, bucket boundaries shift and clients near
    /// the edges naturally migrate — no timers or thresholds needed.
    async fn pick_account(&self, affinity_key: Option<&str>, model: &str) -> Option<usize> {
        let now = Instant::now();
        let now_epoch = Self::now_epoch();

        // Build candidate list: (index, headroom, utilization, source) for non-hard-limited, model-compatible accounts
        let mut candidates: Vec<(usize, f64, f64, &str)> = Vec::new();
        for (i, acct) in self.accounts.iter().enumerate() {
            // Skip accounts that don't serve this model
            if !acct.serves_model(model) {
                trace!(
                    account = acct.name,
                    model = model,
                    "pick: skipping, model not in allowlist"
                );
                continue;
            }

            let info = acct.rate_info.read().await;

            // Skip hard-limited accounts
            if let Some(until) = info.hard_limited_until {
                if now < until {
                    let remaining = until.duration_since(now);
                    trace!(
                        account = acct.name,
                        hard_limited_secs = remaining.as_secs(),
                        "pick: skipping hard-limited account"
                    );
                    continue;
                }
            }

            // Compute effective utilization using the shared fallback chain.
            // This includes claim_penalty_7d on the 7d window.
            let (effective_util, source) = effective_utilization(&info, now_epoch);

            // Headroom = available capacity. Rejected/exhausted accounts (util >= 1.0) get zero
            // headroom so they receive no traffic even in all-exhausted degraded mode (Bug #5).
            // Others are clamped to 0.01 minimum to avoid divide-by-zero and drain gracefully.
            let headroom = if effective_util >= 1.0 {
                0.0
            } else {
                (1.0 - effective_util).max(0.01)
            };

            trace!(
                account = acct.name,
                effective_util = format!("{:.4}", effective_util),
                headroom = format!("{:.4}", headroom),
                source = source,
                raw_5h = ?info.utilization_5h,
                raw_7d = ?info.utilization_7d,
                reset_5h = ?info.reset_5h,
                reset_7d = ?info.reset_7d,
                status_5h = ?info.status_5h,
                status_7d = ?info.status_7d,
                claim = ?info.representative_claim,
                "pick: candidate"
            );

            candidates.push((i, headroom, effective_util, source));
        }

        if candidates.is_empty() {
            debug!("pick: no available accounts");
            return None;
        }

        // Soft-limit gate: exclude accounts with effective utilization above the ceiling.
        // This is where status floors have their biggest effect — a "throttled" account gets
        // floor=0.98 which exceeds soft_limit=0.90, so it's excluded without needing a hard 429.
        // If ALL accounts exceed the ceiling, we fall through and use everyone (graceful degradation).
        let healthy: Vec<(usize, f64, &str)> = candidates
            .iter()
            .filter(|(_, _, util, _)| *util < self.soft_limit)
            .map(|(i, h, _, s)| (*i, *h, *s))
            .collect();
        let effective: Vec<(usize, f64, &str)> = if healthy.is_empty() {
            candidates.iter().map(|(i, h, _, s)| (*i, *h, *s)).collect()
        } else {
            if healthy.len() < candidates.len() {
                let excluded: Vec<&str> = candidates
                    .iter()
                    .filter(|(_, _, util, _)| *util >= self.soft_limit)
                    .map(|(i, _, _, _)| self.accounts[*i].name.as_str())
                    .collect();
                debug!(
                    soft_limit = self.soft_limit,
                    excluded = ?excluded,
                    "pick: soft-limited accounts excluded"
                );
            }
            healthy
        };

        // Total headroom across effective candidates.
        // If zero (all rejected/exhausted), no account can serve traffic — return None.
        let total_headroom: f64 = effective.iter().map(|(_, h, _)| h).sum();
        if total_headroom <= 0.0 {
            debug!("pick: all candidates exhausted (zero headroom)");
            return None;
        }

        // Compute position in [0, 10000) — either stable (affinity) or scattered (round-robin)
        let position = if let Some(key) = affinity_key {
            let mut hasher = std::collections::hash_map::DefaultHasher::new();
            key.hash(&mut hasher);
            (hasher.finish() % 10000) as f64
        } else {
            // Fibonacci hash scatter for even distribution without affinity
            let counter = self.robin.fetch_add(1, Ordering::Relaxed) as u64;
            (counter.wrapping_mul(11400714819323198485) % 10000) as f64
        };

        // Normalize position into [0, total_headroom)
        let target = position / 10000.0 * total_headroom;

        // Walk weighted buckets
        let mut cumulative = 0.0;
        for &(idx, headroom, source) in &effective {
            cumulative += headroom;
            if target < cumulative {
                let util = candidates.iter().find(|(i, _, _, _)| *i == idx).unwrap().2;
                debug!(
                    account = self.accounts[idx].name,
                    util = format!("{:.3}", util),
                    headroom = format!("{:.3}", headroom),
                    share = format!("{:.0}%", headroom / total_headroom * 100.0),
                    source = source,
                    candidates = effective.len(),
                    affinity = affinity_key.unwrap_or("-"),
                    "pick: selected"
                );
                return Some(idx);
            }
        }

        // Floating-point edge case: pick last candidate
        let &(idx, _, source) = effective.last().unwrap();
        let util = candidates.iter().find(|(i, _, _, _)| *i == idx).unwrap().2;
        debug!(
            account = self.accounts[idx].name,
            util = format!("{:.3}", util),
            source = source,
            affinity = affinity_key.unwrap_or("-"),
            "pick: selected (float edge)"
        );
        Some(idx)
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

        // Capture per-window utilization (always, regardless of representative claim)
        // Track whether we got utilization for each window — if we did but got no status header,
        // the pressure has cleared and we must reset the stale status (Bug #1: sticky status fix).
        let got_7d_util = if let Some(v) = headers.get("anthropic-ratelimit-unified-7d-utilization")
        {
            if let Ok(s) = v.to_str() {
                info.utilization_7d = s.parse::<f64>().ok().map(|v| v.clamp(0.0, 1.0));
                true
            } else {
                false
            }
        } else {
            false
        };
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

        // Capture per-window reset timestamps (epoch seconds).
        // Used by time_adjusted_utilization() to discount near-reset accounts and detect stale data.
        // Sanity-capped: 5h window can't reset >5h out, 7d can't reset >7d out (Bug #6).
        let now_epoch = Self::now_epoch();
        if let Some(v) = headers.get("anthropic-ratelimit-unified-5h-reset") {
            if let Ok(s) = v.to_str() {
                if let Ok(epoch) = s.parse::<u64>() {
                    if epoch <= now_epoch + 18000 {
                        info.reset_5h = Some(epoch);
                    }
                }
            }
        }
        if let Some(v) = headers.get("anthropic-ratelimit-unified-7d-reset") {
            if let Ok(s) = v.to_str() {
                if let Ok(epoch) = s.parse::<u64>() {
                    if epoch <= now_epoch + 604800 {
                        info.reset_7d = Some(epoch);
                    }
                }
            }
        }

        // Capture per-window status. These signal API-side pressure (burst limits, concurrent
        // request limits, per-model sub-limits) that raw utilization percentages don't reflect.
        // "allowed_warning" → reduce routing share. "throttled" → effectively exclude.
        // If the API sent utilization for a window but NO status header, clear stale status —
        // absence of the header means pressure has subsided (Bug #1).
        if let Some(v) = headers.get("anthropic-ratelimit-unified-5h-status") {
            if let Ok(s) = v.to_str() {
                info.status_5h = Some(s.to_string());
            }
        } else if got_5h_util {
            info.status_5h = None;
        }
        if let Some(v) = headers.get("anthropic-ratelimit-unified-7d-status") {
            if let Ok(s) = v.to_str() {
                info.status_7d = Some(s.to_string());
            }
        } else if got_7d_util {
            info.status_7d = None;
        }

        // Representative claim tells us which window is the binding constraint
        let rep_claim = headers
            .get("anthropic-ratelimit-unified-representative-claim")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());

        if let Some(ref claim) = rep_claim {
            info.representative_claim = Some(claim.clone());
        }

        // Use the MAX of all known window utilizations as the effective utilization.
        // This prevents the bug where accounts with different representative claims
        // get compared on different time windows (apples vs oranges).
        let mut max_util: Option<f64> = None;
        for u in [info.utilization_7d, info.utilization_5h]
            .into_iter()
            .flatten()
        {
            max_util = Some(max_util.map_or(u, |cur: f64| cur.max(u)));
        }
        if max_util.is_some() {
            info.utilization = max_util;
        }

        // Legacy headers (still try them)
        if let Some(v) = headers.get("x-ratelimit-remaining-requests") {
            if let Ok(s) = v.to_str() {
                info.remaining_requests = s.parse().ok();
            }
        }
        if let Some(v) = headers.get("x-ratelimit-remaining-tokens") {
            if let Ok(s) = v.to_str() {
                info.remaining_tokens = s.parse().ok();
            }
        }
        if let Some(v) = headers.get("x-ratelimit-limit-requests") {
            if let Ok(s) = v.to_str() {
                info.limit_requests = s.parse().ok();
            }
        }
        if let Some(v) = headers.get("x-ratelimit-limit-tokens") {
            if let Ok(s) = v.to_str() {
                info.limit_tokens = s.parse().ok();
            }
        }
        info.last_updated = Some(Instant::now());

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
            remaining_requests = ?info.remaining_requests,
            remaining_tokens = ?info.remaining_tokens,
            "rate info updated"
        );
    }

    /// Mark an account as hard rate-limited (got a 429).
    async fn mark_hard_limited(&self, idx: usize, headers: &reqwest::header::HeaderMap) {
        let acct = &self.accounts[idx];
        let mut info = acct.rate_info.write().await;

        let cooldown = if let Some(v) = headers.get("retry-after") {
            if let Ok(s) = v.to_str() {
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
        } else {
            self.cooldown
        };

        let until = Instant::now() + cooldown;
        info.hard_limited_until = Some(until);
        info.remaining_requests = Some(0);
        info.remaining_tokens = Some(0);
        info.last_updated = Some(Instant::now());

        warn!(
            account = acct.name,
            cooldown_secs = cooldown.as_secs(),
            "account hard rate-limited (429), cooling down"
        );
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

impl AppState {
    /// Record token usage for an account and client.
    fn record_usage(&self, account_idx: usize, client_id: &str, usage: &TokenUsage) {
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
            let total = usage.input_tokens + usage.output_tokens;
            if let Ok(mut map) = self.client_usage.lock() {
                let entry = map.entry(client_id.to_string()).or_insert([0; 4]);
                entry[0] += usage.input_tokens;
                entry[1] += usage.output_tokens;
                entry[2] += usage.cache_creation_input_tokens;
                entry[3] += usage.cache_read_input_tokens;
            }
            // Budget accounting
            self.record_budget_usage(client_id, total);
        }
    }

    /// Write a shadow log entry (fire-and-forget).
    fn shadow_log(&self, entry: serde_json::Value) {
        if let Some(ref tx) = self.shadow_log_tx {
            if let Ok(line) = serde_json::to_string(&entry) {
                let _ = tx.send(line);
            }
        }
    }

    /// Check if a client is within their daily token budget. Returns Ok(()) or Err with remaining.
    fn check_budget(&self, client_id: &str) -> Result<(), u64> {
        let limit = match self.client_budgets.get(client_id) {
            Some(&limit) => limit,
            None => return Ok(()), // no budget configured = unlimited
        };
        let today = Self::now_epoch() / 86400;
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
    fn record_budget_usage(&self, client_id: &str, tokens: u64) {
        if tokens == 0 || !self.client_budgets.contains_key(client_id) {
            return;
        }
        let today = Self::now_epoch() / 86400;
        if let Ok(mut map) = self.budget_usage.lock() {
            let entry = map.entry(client_id.to_string()).or_insert((today, 0));
            if entry.0 != today {
                *entry = (today, 0); // reset on new day
            }
            entry.1 += tokens;
        }
    }

    /// Check if client_id is the operator.
    fn is_operator(&self, client_id: &str) -> bool {
        self.operator.as_deref() == Some(client_id)
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

        for acct in &self.accounts {
            if !acct.serves_model(model) {
                continue;
            }
            any_compatible = true;
            let info = acct.rate_info.read().await;
            let (util, _) = effective_utilization(&info, now_epoch);
            if util < limit {
                all_above = false;
                break;
            }
            // Track nearest reset for Retry-After
            if let Some(r) = info.reset_5h {
                if r > now_epoch {
                    let secs = r - now_epoch;
                    nearest_reset = Some(nearest_reset.map_or(secs, |cur: u64| cur.min(secs)));
                }
            }
            if let Some(r) = info.reset_7d {
                if r > now_epoch {
                    let secs = r - now_epoch;
                    nearest_reset = Some(nearest_reset.map_or(secs, |cur: u64| cur.min(secs)));
                }
            }
        }

        if all_above && any_compatible {
            let retry_after = nearest_reset.unwrap_or(300).clamp(60, 3600);
            Err(retry_after)
        } else {
            Ok(())
        }
    }

    /// Check if all accounts are above the emergency threshold.
    /// Fail-open: returns false if all accounts return (0.5, "unknown") — no data.
    async fn is_emergency_brake_active(&self) -> bool {
        let now_epoch = Self::now_epoch();
        let mut all_above = true;
        let mut any_known = false;

        for acct in &self.accounts {
            let info = acct.rate_info.read().await;
            let (util, source) = effective_utilization(&info, now_epoch);
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
        if client_id != "-" && self.check_budget(client_id).is_err() {
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

    // Extract client identification headers
    let client_id = state.resolve_client_id(&client_ip, &parts.headers);
    let agent_id = parts
        .headers
        .get("x-agent-id")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("-")
        .to_string();
    let session_id = parts
        .headers
        .get("x-session-id")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("-")
        .to_string();

    let body_bytes = match axum::body::to_bytes(body, 10 * 1024 * 1024).await {
        Ok(b) => b,
        Err(e) => {
            error!("failed to read request body: {e}");
            return (StatusCode::BAD_REQUEST, "bad request body").into_response();
        }
    };

    // Parse body once for model extraction and optional cache injection
    let (body_bytes, model) =
        if let Ok(mut parsed) = serde_json::from_slice::<serde_json::Value>(&body_bytes) {
            let model = parsed
                .get("model")
                .and_then(|m| m.as_str())
                .unwrap_or("")
                .to_string();

            if state.auto_cache {
                let inj = inject_cache_breakpoints(&mut parsed);
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

            // Re-serialize (only differs from original if cache was injected)
            let bytes = serde_json::to_vec(&parsed).unwrap_or_else(|_| body_bytes.to_vec());
            (bytes::Bytes::from(bytes), model)
        } else {
            (body_bytes, String::new())
        };

    // Pre-request gate: operator bypass, budget, utilization limit, emergency brake.
    // Note: budget + emergency don't need `model` and could run before body parsing,
    // but those rejections are rare and the JSON parse cost is negligible — not worth
    // splitting the gate for a few microseconds on an almost-never code path.
    if let Err(resp) = state.pre_request_gate(&client_id, &model).await {
        return resp;
    }

    // Build affinity key for sticky routing — only use stickiness when there's
    // meaningful client identification beyond just the IP address
    let affinity_key = format!("{}:{}:{}:{}", client_ip, client_id, agent_id, session_id);
    let has_identity = client_id != "-" || agent_id != "-" || session_id != "-";
    let affinity = if has_identity {
        Some(affinity_key.as_str())
    } else {
        None
    };

    let n = state.accounts.len();
    for _attempt in 0..n {
        let idx = match state.pick_account(affinity, &model).await {
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

        // Default anthropic-version if client didn't set it
        if !headers.contains_key("anthropic-version") {
            headers.insert("anthropic-version", HeaderValue::from_static("2023-06-01"));
        }

        // Auth: passthrough keeps caller's headers, otherwise inject account token
        if !acct.passthrough {
            headers.remove("authorization");
            headers.remove("x-api-key");
            // Detect token type by prefix
            if acct.token.starts_with("sk-ant-api") {
                // Standard API key → x-api-key header
                headers.insert("x-api-key", HeaderValue::from_str(&acct.token).unwrap());
            } else if acct.token.starts_with("sk-ant-oat") {
                // OAuth token → Authorization: Bearer
                headers.insert(
                    "authorization",
                    HeaderValue::from_str(&format!("Bearer {}", acct.token)).unwrap(),
                );
                // OAuth tokens require these headers — client won't send them
                // since it doesn't know the proxy uses OAuth behind the scenes
                headers.insert(
                    "anthropic-dangerous-direct-browser-access",
                    HeaderValue::from_static("true"),
                );
                // Ensure oauth beta flag is present in anthropic-beta
                let existing_beta = headers
                    .get("anthropic-beta")
                    .and_then(|v| v.to_str().ok())
                    .unwrap_or("")
                    .to_string();
                if !existing_beta.contains("oauth-2025-04-20") {
                    let new_beta = if existing_beta.is_empty() {
                        "oauth-2025-04-20".to_string()
                    } else {
                        format!("{},oauth-2025-04-20", existing_beta)
                    };
                    headers.insert("anthropic-beta", HeaderValue::from_str(&new_beta).unwrap());
                }
            } else {
                // Unknown token type → try x-api-key
                headers.insert("x-api-key", HeaderValue::from_str(&acct.token).unwrap());
            }
        }
        // passthrough: caller's auth headers flow through untouched

        upstream_req = upstream_req.headers(headers);
        upstream_req = upstream_req.body(body_bytes.clone());

        let mut resp = match upstream_req.send().await {
            Ok(r) => r,
            Err(e) => {
                error!(account = acct.name, "upstream request failed: {e}");
                continue;
            }
        };

        let status = resp.status();
        acct.requests.fetch_add(1, Ordering::Relaxed);

        // Always update rate limit info and persist
        state.update_rate_info(idx, resp.headers()).await;

        // Update burn rate (after rate-limit headers are parsed)
        {
            let now = Instant::now();
            if let Ok(mut br) = acct.burn_rate.lock() {
                br.update(now);
            }
            // Per-client request tracking
            if let Ok(mut rates) = state.client_request_rates.lock() {
                let entry = rates
                    .entry(client_id.clone())
                    .or_insert_with(|| (0, Ewma::new(TAU_1H)));
                entry.0 += 1;
                entry.1.update(now);
            }
        }

        // 429 → mark hard-limited and try next account
        if status == StatusCode::TOO_MANY_REQUESTS {
            state.mark_hard_limited(idx, resp.headers()).await;
            state.save_state().await;
            info!(account = acct.name, "got 429, rotating to next account");
            continue;
        }

        // 5xx/529 → transient upstream error, try next account (don't mark hard-limited)
        if status.is_server_error() || status.as_u16() == 529 {
            state.save_state().await;
            warn!(
                account = acct.name,
                status = status.as_u16(),
                "got server error, rotating to next account"
            );
            continue;
        }

        // Persist state after successful request
        state.save_state().await;

        // Log with capacity info
        {
            let info = acct.rate_info.read().await;
            info!(
                client = %client_ip,
                client_id = %client_id,
                agent = %agent_id,
                session = %session_id,
                model = %model,
                account = acct.name,
                status = status.as_u16(),
                utilization = ?info.utilization,
                claim = ?info.representative_claim,
                total = acct.requests.load(Ordering::Relaxed),
                "proxied"
            );
        }

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
        {
            let info = acct.rate_info.read().await;
            let (eff_util, _) = effective_utilization(&info, AppState::now_epoch());
            let status_val = compute_pressure_status(eff_util, &client_id, &state);
            builder = builder.header("x-budget-status", status_val);
        }

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
            let client_id_clone = client_id.clone();
            let acct_name = acct.name.clone();
            let model_clone = model.clone();
            let client_ip_str = client_ip.to_string();
            let agent_clone = agent_id.clone();
            let session_clone = session_id.clone();

            tokio::spawn(async move {
                let mut sse_buf = Vec::new();
                while let Ok(Some(chunk)) = resp.chunk().await {
                    sse_buf.extend_from_slice(&chunk);
                    if tx.send(Ok(chunk)).await.is_err() {
                        break; // client disconnected
                    }
                }
                // Parse accumulated SSE data for usage
                let text = String::from_utf8_lossy(&sse_buf);
                let usage = TokenUsage::from_sse_text(&text);
                if !usage.is_empty() {
                    state_clone.record_usage(idx, &client_id_clone, &usage);
                }
                state_clone.shadow_log(serde_json::json!({
                    "ts": AppState::now_epoch(),
                    "client": client_ip_str,
                    "client_id": client_id_clone,
                    "agent": agent_clone,
                    "session": session_clone,
                    "model": model_clone,
                    "account": acct_name,
                    "status": status.as_u16(),
                    "stream": true,
                    "latency_ms": request_start.elapsed().as_millis() as u64,
                    "input_tokens": usage.input_tokens,
                    "output_tokens": usage.output_tokens,
                    "cache_creation_input_tokens": usage.cache_creation_input_tokens,
                    "cache_read_input_tokens": usage.cache_read_input_tokens,
                }));
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
                if !usage.is_empty() {
                    state.record_usage(idx, &client_id, &usage);
                }
            }
            state.shadow_log(serde_json::json!({
                "ts": AppState::now_epoch(),
                "client": client_ip.to_string(),
                "client_id": client_id,
                "agent": agent_id,
                "session": session_id,
                "model": model,
                "account": acct.name,
                "status": status.as_u16(),
                "stream": false,
                "latency_ms": latency_ms,
                "input_tokens": usage.input_tokens,
                "output_tokens": usage.output_tokens,
                "cache_creation_input_tokens": usage.cache_creation_input_tokens,
                "cache_read_input_tokens": usage.cache_read_input_tokens,
            }));
            return builder.body(Body::from(body_bytes)).unwrap_or_else(|_| {
                (StatusCode::INTERNAL_SERVER_ERROR, "response build error").into_response()
            });
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

    let body_bytes = match axum::body::to_bytes(body, 10 * 1024 * 1024).await {
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
        let (eff_util, _) = effective_utilization(&info, now_epoch);
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
                let next_reset = info
                    .reset_5h
                    .unwrap_or(u64::MAX)
                    .min(info.reset_7d.unwrap_or(u64::MAX));
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

    axum::Json(serde_json::json!({
        "accounts": out,
        "upstreams": upstream_stats,
        "client_usage": client_usage,
        "client_budgets": budgets,
        "aggregate": aggregate,
        "strategy": "dynamic-capacity",
    }))
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
        _ => "stop",
    }
}

struct StreamContext {
    id: String,
    model: String,
    created: u64,
}

impl Default for StreamContext {
    fn default() -> Self {
        Self {
            id: format!("chatcmpl-{}", AppState::now_epoch()),
            model: String::new(),
            created: AppState::now_epoch(),
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
            } else {
                // Strip "name" field, keep role + content
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

    // Concatenate text content blocks, strip markdown JSON fences
    let content = body
        .get("content")
        .and_then(|c| c.as_array())
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

    serde_json::json!({
        "id": format!("chatcmpl-{}", id),
        "object": "chat.completion",
        "created": AppState::now_epoch(),
        "model": model,
        "choices": [{
            "index": 0,
            "message": {
                "role": "assistant",
                "content": content,
            },
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
/// Returns None for events that should be skipped (ping, content_block_start, etc.).
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
        "content_block_delta" => {
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
        _ => None, // ping, content_block_start, content_block_stop
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

    // Proxy auth
    if let Some(ref key) = state.proxy_key {
        let provided = req.headers().get("x-api-key").and_then(|v| v.to_str().ok());
        if provided != Some(key.as_str()) {
            warn!(client = %client_ip, "rejected: invalid or missing proxy key");
            return (StatusCode::UNAUTHORIZED, "unauthorized").into_response();
        }
    }

    let (parts, body) = req.into_parts();

    // Extract client identification headers
    let client_id = state.resolve_client_id(&client_ip, &parts.headers);
    let agent_id = parts
        .headers
        .get("x-agent-id")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("-")
        .to_string();
    let session_id = parts
        .headers
        .get("x-session-id")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("-")
        .to_string();

    let body_bytes = match axum::body::to_bytes(body, 10 * 1024 * 1024).await {
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

    // Build affinity key for sticky routing — only use stickiness when there's
    // meaningful client identification beyond just the IP address
    let affinity_key = format!("{}:{}:{}:{}", client_ip, client_id, agent_id, session_id);
    let has_identity = client_id != "-" || agent_id != "-" || session_id != "-";
    let affinity = if has_identity {
        Some(affinity_key.as_str())
    } else {
        None
    };

    let n = state.accounts.len();
    for _attempt in 0..n {
        let idx = match state.pick_account(affinity, &model).await {
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
        headers.remove("authorization");
        headers.remove("x-api-key");
        headers.remove("content-length"); // body size changes after translation
        headers.remove("accept-encoding"); // we need plaintext to translate the response

        // Inject required Anthropic headers
        headers.insert("content-type", HeaderValue::from_static("application/json"));
        headers.insert("anthropic-version", HeaderValue::from_static("2023-06-01"));

        // Auth injection with claude-code beta header for OAuth
        if !acct.passthrough {
            if acct.token.starts_with("sk-ant-api") {
                headers.insert("x-api-key", HeaderValue::from_str(&acct.token).unwrap());
            } else if acct.token.starts_with("sk-ant-oat") {
                headers.insert(
                    "authorization",
                    HeaderValue::from_str(&format!("Bearer {}", acct.token)).unwrap(),
                );
                headers.insert(
                    "anthropic-dangerous-direct-browser-access",
                    HeaderValue::from_static("true"),
                );
                // OAuth tokens need both beta flags for non-Claude-Code clients
                let existing_beta = headers
                    .get("anthropic-beta")
                    .and_then(|v| v.to_str().ok())
                    .unwrap_or("")
                    .to_string();
                let mut betas: Vec<&str> = if existing_beta.is_empty() {
                    vec![]
                } else {
                    existing_beta.split(',').collect()
                };
                for flag in &["oauth-2025-04-20", "claude-code-20250219"] {
                    if !betas.iter().any(|b| b.trim() == *flag) {
                        betas.push(flag);
                    }
                }
                headers.insert(
                    "anthropic-beta",
                    HeaderValue::from_str(&betas.join(",")).unwrap(),
                );
            } else {
                headers.insert("x-api-key", HeaderValue::from_str(&acct.token).unwrap());
            }
        }

        let upstream_req = state
            .client
            .request(reqwest::Method::POST, &url)
            .headers(reqwest_headers(&headers))
            .body(anthropic_body.to_string());

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
        {
            let now = Instant::now();
            if let Ok(mut br) = acct.burn_rate.lock() {
                br.update(now);
            }
            if let Ok(mut rates) = state.client_request_rates.lock() {
                let entry = rates
                    .entry(client_id.clone())
                    .or_insert_with(|| (0, Ewma::new(TAU_1H)));
                entry.0 += 1;
                entry.1.update(now);
            }
        }

        if status == StatusCode::TOO_MANY_REQUESTS {
            state.mark_hard_limited(idx, resp.headers()).await;
            state.save_state().await;
            info!(account = acct.name, "got 429, rotating to next account");
            continue;
        }

        // 5xx/529 → transient upstream error, try next account (don't mark hard-limited)
        if status.is_server_error() || status.as_u16() == 529 {
            state.save_state().await;
            warn!(
                account = acct.name,
                status = status.as_u16(),
                "got server error, rotating to next account"
            );
            continue;
        }

        state.save_state().await;

        // Compute budget pressure status for response header
        let budget_status = {
            let info = acct.rate_info.read().await;
            let (eff_util, _) = effective_utilization(&info, AppState::now_epoch());
            compute_pressure_status(eff_util, &client_id, &state)
        };

        {
            let info = acct.rate_info.read().await;
            info!(
                client = %client_ip,
                client_id = %client_id,
                agent = %agent_id,
                session = %session_id,
                model = %model,
                account = acct.name,
                status = status.as_u16(),
                utilization = ?info.utilization,
                openai_compat = true,
                stream = is_streaming,
                "proxied (openai-compat)"
            );
        }

        // Non-2xx: return error as-is (not SSE even if streaming was requested)
        if !status.is_success() {
            let error_body = resp.bytes().await.unwrap_or_default();
            return Response::builder()
                .status(StatusCode::from_u16(status.as_u16()).unwrap_or(StatusCode::BAD_GATEWAY))
                .header("content-type", "application/json")
                .header("x-budget-status", budget_status)
                .body(Body::from(error_body))
                .unwrap_or_else(|_| {
                    (StatusCode::INTERNAL_SERVER_ERROR, "response build error").into_response()
                });
        }

        if is_streaming {
            let (tx, rx) = tokio::sync::mpsc::channel::<Result<bytes::Bytes, std::io::Error>>(32);
            let state_clone = state.clone();
            let client_id_clone = client_id.clone();
            let acct_name = acct.name.clone();
            let model_clone = model.clone();
            let client_ip_str = client_ip.to_string();
            let agent_clone = agent_id.clone();
            let session_clone = session_id.clone();

            tokio::spawn(async move {
                let mut buffer = String::new();
                let mut raw_sse = String::new(); // accumulate for usage extraction
                let mut ctx = StreamContext::default();
                let mut sent_done = false;

                while let Ok(Some(chunk)) = resp.chunk().await {
                    let chunk_str = String::from_utf8_lossy(&chunk);
                    buffer.push_str(&chunk_str);
                    raw_sse.push_str(&chunk_str);

                    while let Some(pos) = buffer.find("\n\n") {
                        let event = buffer[..pos].to_string();
                        buffer = buffer[pos + 2..].to_string();

                        if event.trim().is_empty() {
                            continue;
                        }

                        if let Some(translated) = translate_sse_event(&event, &mut ctx) {
                            if translated.contains("[DONE]") {
                                sent_done = true;
                            }
                            if tx.send(Ok(bytes::Bytes::from(translated))).await.is_err() {
                                return; // client disconnected
                            }
                        }
                    }
                }

                // Process any remaining data in buffer
                if !buffer.trim().is_empty() {
                    if let Some(translated) = translate_sse_event(&buffer, &mut ctx) {
                        if translated.contains("[DONE]") {
                            sent_done = true;
                        }
                        let _ = tx.send(Ok(bytes::Bytes::from(translated))).await;
                    }
                }

                // Ensure [DONE] is always sent (fallback for abnormal stream termination)
                if !sent_done {
                    let _ = tx.send(Ok(bytes::Bytes::from("data: [DONE]\n\n"))).await;
                }

                // Extract and record token usage from accumulated SSE data
                let usage = TokenUsage::from_sse_text(&raw_sse);
                if !usage.is_empty() {
                    state_clone.record_usage(idx, &client_id_clone, &usage);
                }
                state_clone.shadow_log(serde_json::json!({
                    "ts": AppState::now_epoch(),
                    "client": client_ip_str,
                    "client_id": client_id_clone,
                    "agent": agent_clone,
                    "session": session_clone,
                    "model": model_clone,
                    "account": acct_name,
                    "status": status.as_u16(),
                    "stream": true,
                    "openai_compat": true,
                    "latency_ms": request_start.elapsed().as_millis() as u64,
                    "input_tokens": usage.input_tokens,
                    "output_tokens": usage.output_tokens,
                    "cache_creation_input_tokens": usage.cache_creation_input_tokens,
                    "cache_read_input_tokens": usage.cache_read_input_tokens,
                }));
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
                        (StatusCode::INTERNAL_SERVER_ERROR, "response build error").into_response()
                    });
            }
        };

        let openai_resp = translate_anthropic_to_openai(&anthropic_resp);

        // Extract and record token usage from non-streaming response
        let usage = TokenUsage::from_response_body(&anthropic_resp);
        if !usage.is_empty() {
            state.record_usage(idx, &client_id, &usage);
        }
        state.shadow_log(serde_json::json!({
            "ts": AppState::now_epoch(),
            "client": client_ip.to_string(),
            "client_id": client_id,
            "agent": agent_id,
            "session": session_id,
            "model": model,
            "account": acct.name,
            "status": status.as_u16(),
            "stream": false,
            "openai_compat": true,
            "latency_ms": request_start.elapsed().as_millis() as u64,
            "input_tokens": usage.input_tokens,
            "output_tokens": usage.output_tokens,
            "cache_creation_input_tokens": usage.cache_creation_input_tokens,
            "cache_read_input_tokens": usage.cache_read_input_tokens,
        }));

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

    (StatusCode::TOO_MANY_REQUESTS, "exhausted all accounts").into_response()
}

/// Convert axum HeaderMap to reqwest HeaderMap.
fn reqwest_headers(headers: &axum::http::HeaderMap) -> reqwest::header::HeaderMap {
    let mut out = reqwest::header::HeaderMap::new();
    for (k, v) in headers.iter() {
        if let Ok(name) = reqwest::header::HeaderName::from_bytes(k.as_str().as_bytes()) {
            if let Ok(val) = reqwest::header::HeaderValue::from_bytes(v.as_bytes()) {
                out.insert(name, val);
            }
        }
    }
    out
}

// ── Main ────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "anthropic_lb=info".into()),
        )
        .init();

    let config_path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "config.toml".to_string());
    let config_str = std::fs::read_to_string(&config_path)
        .unwrap_or_else(|e| panic!("failed to read {config_path}: {e}"));
    let config: Config =
        toml::from_str(&config_str).unwrap_or_else(|e| panic!("invalid config: {e}"));

    assert!(!config.accounts.is_empty(), "at least one account required");

    // Validate new config fields
    for (client, limit) in &config.client_utilization_limits {
        assert!(
            (0.0..=1.0).contains(limit),
            "client_utilization_limits.{client}: must be 0.0-1.0, got {limit}"
        );
    }
    if let Some(ref op) = config.operator {
        assert!(
            op != "-",
            "operator cannot be '-' (the unknown-client sentinel)"
        );
    }
    if let Some(thresh) = config.emergency_threshold {
        assert!(
            (0.0..=1.0).contains(&thresh),
            "emergency_threshold must be 0.0-1.0, got {thresh}"
        );
    }

    let cooldown = Duration::from_secs(config.rate_limit_cooldown_secs.unwrap_or(60));

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
        num_accounts = accounts.len(),
        num_upstreams = upstreams.len(),
        "dynamic capacity-based routing enabled"
    );

    let state_path = PathBuf::from(&config_path).with_extension("state.json");

    // Set up shadow log writer if configured
    let shadow_log_tx = if let Some(ref path) = config.shadow_log {
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<String>();
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

    let state = Arc::new(AppState {
        client: Client::builder()
            .timeout(Duration::from_secs(600))
            .build()
            .expect("failed to build HTTP client"),
        upstream: config.upstream,
        accounts,
        robin: AtomicUsize::new(0),
        cooldown,
        state_path,
        proxy_key: config.proxy_key.clone(),
        allowed_ips,
        upstreams,
        client_names: config.client_names.clone(),
        auto_cache: config.auto_cache.unwrap_or(true),
        client_usage: Mutex::new(HashMap::new()),
        shadow_log_tx,
        client_budgets: config.client_budgets.clone(),
        budget_usage: Mutex::new(HashMap::new()),
        client_utilization_limits: config.client_utilization_limits.clone(),
        operator: config.operator.clone(),
        emergency_threshold: config
            .emergency_threshold
            .unwrap_or(DEFAULT_EMERGENCY_THRESHOLD),
        client_request_rates: Mutex::new(HashMap::new()),
        soft_limit: config.soft_limit.unwrap_or(0.90),
    });

    if state.auto_cache {
        info!("auto-cache enabled");
    }

    // Restore persisted state (cooldowns, utilization, request counts)
    state.load_state().await;

    let app = Router::new()
        .route("/_stats", axum::routing::get(stats_handler))
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

    info!(%addr, "anthropic-lb starting");

    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .unwrap_or_else(|e| panic!("failed to bind {addr}: {e}"));

    // Spawn periodic probe task
    let probe_interval = config.probe_interval_secs.unwrap_or(300);
    if probe_interval > 0 {
        let probe_state = state.clone();
        let n_accounts = probe_state.accounts.len();
        tokio::spawn(async move {
            // Stagger initial probes: wait 10s then probe all accounts
            tokio::time::sleep(Duration::from_secs(10)).await;
            info!(
                interval_secs = probe_interval,
                "starting utilization probes"
            );
            loop {
                for i in 0..n_accounts {
                    probe_state.probe_account(i).await;
                    // Small delay between accounts to avoid burst
                    tokio::time::sleep(Duration::from_secs(2)).await;
                }
                tokio::time::sleep(Duration::from_secs(probe_interval)).await;
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
        }
    }

    fn test_state_with(accounts: Vec<Account>) -> Arc<AppState> {
        Arc::new(AppState {
            client: Client::builder()
                .timeout(Duration::from_secs(5))
                .build()
                .unwrap(),
            upstream: "http://127.0.0.1:1".to_string(), // unused in unit tests
            accounts,
            robin: AtomicUsize::new(0),
            cooldown: Duration::from_secs(60),
            state_path: PathBuf::from("/tmp/anthropic-lb-test.state.json"),
            proxy_key: None,
            allowed_ips: vec![],
            upstreams: vec![],
            client_names: HashMap::new(),
            auto_cache: true,
            client_usage: Mutex::new(HashMap::new()),
            shadow_log_tx: None,
            client_budgets: HashMap::new(),
            budget_usage: Mutex::new(HashMap::new()),
            client_utilization_limits: HashMap::new(),
            operator: None,
            emergency_threshold: DEFAULT_EMERGENCY_THRESHOLD,
            client_request_rates: Mutex::new(HashMap::new()),
            soft_limit: 1.0,
        })
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
        resp
    }

    /// Build the full app router against a given upstream URL.
    fn test_app(upstream_url: &str, proxy_key: Option<String>) -> (Router, Arc<AppState>) {
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
            client_budgets: HashMap::new(),
            budget_usage: Mutex::new(HashMap::new()),
            client_utilization_limits: HashMap::new(),
            operator: None,
            emergency_threshold: DEFAULT_EMERGENCY_THRESHOLD,
            client_request_rates: Mutex::new(HashMap::new()),
            soft_limit: 1.0,
        });

        (build_router(state.clone()), state)
    }

    /// Build a router from a pre-configured state. Used by integration tests
    /// that need custom AppState (operator, utilization limits, etc.).
    fn build_router(state: Arc<AppState>) -> Router {
        Router::new()
            .route("/_stats", axum::routing::get(stats_handler))
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
            cooldown: Duration::from_secs(60),
            state_path: PathBuf::from("/tmp/test.state.json"),
            proxy_key: None,
            allowed_ips: vec![IpAllowEntry::Addr("10.0.0.1".parse().unwrap())],
            upstreams: vec![],
            client_names: HashMap::new(),
            auto_cache: true,
            client_usage: Mutex::new(HashMap::new()),
            shadow_log_tx: None,
            client_budgets: HashMap::new(),
            budget_usage: Mutex::new(HashMap::new()),
            client_utilization_limits: HashMap::new(),
            operator: None,
            emergency_threshold: DEFAULT_EMERGENCY_THRESHOLD,
            client_request_rates: Mutex::new(HashMap::new()),
            soft_limit: 1.0,
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
            let idx = state.pick_account(None, "").await.unwrap();
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

        let idx = state.pick_account(None, "").await.unwrap();
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
            let idx = state.pick_account(None, "").await.unwrap();
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

        assert!(state.pick_account(None, "").await.is_none());
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
            let idx = state.pick_account(None, "").await.unwrap();
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
        // Same affinity key should always return the same account
        let state = test_state_with(vec![
            make_account("a", "sk-ant-api-a"),
            make_account("b", "sk-ant-api-b"),
            make_account("c", "sk-ant-api-c"),
        ]);

        // Set some utilization so buckets are non-trivial
        {
            let mut info = state.accounts[0].rate_info.write().await;
            info.utilization = Some(0.3);
        }
        {
            let mut info = state.accounts[1].rate_info.write().await;
            info.utilization = Some(0.5);
        }
        {
            let mut info = state.accounts[2].rate_info.write().await;
            info.utilization = Some(0.7);
        }

        let key = "192.168.1.1:client-42:agent-7:session-abc";
        let first = state.pick_account(Some(key), "").await.unwrap();
        for _ in 0..100 {
            let idx = state.pick_account(Some(key), "").await.unwrap();
            assert_eq!(
                idx, first,
                "same affinity key must always pick same account"
            );
        }
    }

    #[tokio::test]
    async fn pick_unsticky_on_overload() {
        // When a preferred account gets overloaded, most clients should migrate.
        // Primary starts with 61.5% of bucket space (0.8/1.3), then shrinks to
        // 1.96% (0.01/0.51). So ~97% of previously-primary clients should migrate.
        let state = test_state_with(vec![
            make_account("primary", "sk-ant-api-a"),
            make_account("backup", "sk-ant-api-b"),
        ]);

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
            if state.pick_account(Some(&key), "").await.unwrap() == 0 {
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

        // Most of these clients should migrate to backup
        let mut migrated = 0usize;
        for key in &primary_keys {
            if state.pick_account(Some(key), "").await.unwrap() == 1 {
                migrated += 1;
            }
        }

        let migration_pct = migrated as f64 / primary_keys.len() as f64;
        assert!(
            migration_pct > 0.90,
            "at least 90% of clients should migrate, got {:.1}% ({}/{})",
            migration_pct * 100.0,
            migrated,
            primary_keys.len()
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
            let idx = state.pick_account(None, "").await.unwrap();
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
        assert_eq!(body["strategy"], "dynamic-capacity");
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
            "model": "claude-sonnet-4-20250514",
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
            "model": "claude-sonnet-4-20250514",
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
            "model": "claude-sonnet-4-20250514",
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
            "model": "claude-sonnet-4-20250514",
            "messages": [{"role": "user", "content": "Hello"}]
        });
        let result = translate_openai_to_anthropic(&req);
        assert_eq!(result["max_tokens"], 4096);
    }

    #[test]
    fn translate_request_stop_to_stop_sequences() {
        let req = serde_json::json!({
            "model": "claude-sonnet-4-20250514",
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
            "model": "claude-sonnet-4-20250514",
            "messages": [{"role": "user", "content": "Hello"}],
            "max_tokens": 512,
            "temperature": 0.7,
            "top_p": 0.9,
            "stream": true
        });
        let result = translate_openai_to_anthropic(&req);
        assert_eq!(result["model"], "claude-sonnet-4-20250514");
        assert_eq!(result["max_tokens"], 512);
        assert_eq!(result["temperature"], 0.7);
        assert_eq!(result["top_p"], 0.9);
        assert_eq!(result["stream"], true);
    }

    #[test]
    fn translate_request_strips_name_field() {
        let req = serde_json::json!({
            "model": "claude-sonnet-4-20250514",
            "messages": [
                {"role": "user", "content": "Hello", "name": "bob"}
            ],
            "max_tokens": 100
        });
        let result = translate_openai_to_anthropic(&req);
        let msgs = result["messages"].as_array().unwrap();
        assert!(msgs[0].get("name").is_none());
    }

    // ── Unit: OpenAI response translation ───────────────────────────

    #[test]
    fn translate_response_basic() {
        let resp = serde_json::json!({
            "id": "msg_abc123",
            "type": "message",
            "content": [{"type": "text", "text": "Hello!"}],
            "model": "claude-sonnet-4-20250514",
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
            "model": "claude-sonnet-4-20250514",
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
            "model": "claude-sonnet-4-20250514",
            "stop_reason": "end_turn",
            "usage": {"input_tokens": 10, "output_tokens": 5}
        });
        let result = translate_anthropic_to_openai(&resp);
        assert_eq!(
            result["choices"][0]["message"]["content"],
            r#"{"skipSearch": true}"#
        );
    }

    // ── Unit: SSE event translation ─────────────────────────────────

    #[test]
    fn translate_sse_message_start() {
        let mut ctx = StreamContext::default();
        let raw = "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_test\",\"model\":\"claude-sonnet-4-20250514\",\"role\":\"assistant\"}}";
        let result = translate_sse_event(raw, &mut ctx).unwrap();
        assert!(result.starts_with("data: "));
        assert_eq!(ctx.id, "chatcmpl-msg_test");
        assert_eq!(ctx.model, "claude-sonnet-4-20250514");
        let chunk: serde_json::Value =
            serde_json::from_str(result.strip_prefix("data: ").unwrap().trim()).unwrap();
        assert_eq!(chunk["choices"][0]["delta"]["role"], "assistant");
    }

    #[test]
    fn translate_sse_content_delta() {
        let mut ctx = StreamContext {
            id: "chatcmpl-test".to_string(),
            model: "claude-sonnet-4-20250514".to_string(),
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
            "model": "claude-sonnet-4-20250514",
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
            "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_stream\",\"type\":\"message\",\"role\":\"assistant\",\"model\":\"claude-sonnet-4-20250514\",\"content\":[],\"stop_reason\":null,\"usage\":{\"input_tokens\":10,\"output_tokens\":0}}}\n\n",
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
            cooldown: Duration::from_secs(60),
            state_path: PathBuf::from("/tmp/anthropic-lb-openai-test.state.json"),
            proxy_key,
            allowed_ips: vec![],
            upstreams: vec![],
            client_names: HashMap::new(),
            auto_cache: true,
            client_usage: Mutex::new(HashMap::new()),
            shadow_log_tx: None,
            client_budgets: HashMap::new(),
            budget_usage: Mutex::new(HashMap::new()),
            client_utilization_limits: HashMap::new(),
            operator: None,
            emergency_threshold: DEFAULT_EMERGENCY_THRESHOLD,
            client_request_rates: Mutex::new(HashMap::new()),
            soft_limit: 1.0,
        });

        let app = Router::new()
            .route(
                "/v1/chat/completions",
                axum::routing::post(openai_chat_handler),
            )
            .with_state(state.clone());

        (app, state)
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
            .body(r#"{"model":"claude-sonnet-4-20250514","messages":[{"role":"user","content":"Hello"}],"max_tokens":100}"#)
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
            .body(r#"{"model":"claude-sonnet-4-20250514","messages":[{"role":"user","content":"Hello"}],"max_tokens":100,"stream":true}"#)
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

    // ── Unit: auto-cache injection ─────────────────────────────────

    #[test]
    fn inject_cache_no_existing() {
        let mut body = serde_json::json!({
            "model": "claude-sonnet-4-20250514",
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
            "model": "claude-sonnet-4-20250514",
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
            "model": "claude-sonnet-4-20250514",
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
            "model": "claude-sonnet-4-20250514",
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
            "model": "claude-sonnet-4-20250514",
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

    #[test]
    fn record_usage_updates_account_and_client() {
        let state = test_state_with(vec![make_account("a", "sk-ant-api-x")]);
        let usage = TokenUsage {
            input_tokens: 100,
            output_tokens: 50,
            cache_creation_input_tokens: 20,
            cache_read_input_tokens: 30,
        };
        state.record_usage(0, "test-client", &usage);

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

    #[test]
    fn record_usage_ignores_anonymous() {
        let state = test_state_with(vec![make_account("a", "sk-ant-api-x")]);
        let usage = TokenUsage {
            input_tokens: 100,
            output_tokens: 50,
            cache_creation_input_tokens: 0,
            cache_read_input_tokens: 0,
        };
        state.record_usage(0, "-", &usage);

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
        assert!(acct.serves_model("claude-opus-4-20250514"));
        assert!(acct.serves_model("claude-haiku-4-5-20251001"));
        assert!(acct.serves_model(""));
    }

    #[test]
    fn account_serves_model_exact_match() {
        let mut acct = make_account("a", "sk-ant-api-x");
        acct.models = vec!["claude-sonnet-4-20250514".to_string()];
        assert!(acct.serves_model("claude-sonnet-4-20250514"));
        assert!(!acct.serves_model("claude-opus-4-20250514"));
    }

    #[test]
    fn account_serves_model_prefix_match() {
        let mut acct = make_account("a", "sk-ant-api-x");
        acct.models = vec!["claude-opus-*".to_string(), "claude-sonnet-*".to_string()];
        assert!(acct.serves_model("claude-opus-4-20250514"));
        assert!(acct.serves_model("claude-sonnet-4-20250514"));
        assert!(!acct.serves_model("claude-haiku-4-5-20251001"));
    }

    #[tokio::test]
    async fn pick_account_filters_by_model() {
        let mut acct_a = make_account("opus-only", "sk-ant-api-a");
        acct_a.models = vec!["claude-opus-*".to_string()];

        let acct_b = make_account("any-model", "sk-ant-api-b");

        let state = test_state_with(vec![acct_a, acct_b]);

        // Requesting opus: both accounts eligible
        let idx = state
            .pick_account(None, "claude-opus-4-20250514")
            .await
            .unwrap();
        assert!(idx == 0 || idx == 1);

        // Requesting haiku: only acct_b eligible
        let idx = state
            .pick_account(None, "claude-haiku-4-5-20251001")
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
        {
            let mut info = accounts[0].rate_info.write().await;
            info.utilization = Some(0.30);
            info.utilization_5h = Some(0.30);
        }
        {
            let mut info = accounts[1].rate_info.write().await;
            info.utilization = Some(0.95);
            info.utilization_5h = Some(0.95);
        }

        let state = Arc::new(AppState {
            client: Client::builder()
                .timeout(Duration::from_secs(5))
                .build()
                .unwrap(),
            upstream: "http://127.0.0.1:1".to_string(),
            accounts,
            robin: AtomicUsize::new(0),
            cooldown: Duration::from_secs(60),
            state_path: PathBuf::from("/tmp/anthropic-lb-test.state.json"),
            proxy_key: None,
            allowed_ips: vec![],
            upstreams: vec![],
            client_names: HashMap::new(),
            auto_cache: true,
            client_usage: Mutex::new(HashMap::new()),
            shadow_log_tx: None,
            client_budgets: HashMap::new(),
            budget_usage: Mutex::new(HashMap::new()),
            client_utilization_limits: HashMap::new(),
            operator: None,
            emergency_threshold: DEFAULT_EMERGENCY_THRESHOLD,
            client_request_rates: Mutex::new(HashMap::new()),
            soft_limit: 0.90,
        });

        // Try many affinity keys — all should route to healthy (idx 0)
        for i in 0..20 {
            let key = format!("client-{}", i);
            let idx = state.pick_account(Some(&key), "any").await.unwrap();
            assert_eq!(
                idx, 0,
                "client '{}' routed to overloaded account despite soft limit",
                key
            );
        }
    }

    // ── Unit: per-client budget ────────────────────────────────────

    #[test]
    fn budget_check_no_limit_configured() {
        let state = test_state_with(vec![make_account("a", "sk-ant-api-x")]);
        assert!(state.check_budget("any-client").is_ok());
    }

    #[test]
    fn budget_check_within_limit() {
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
            cooldown: Duration::from_secs(60),
            state_path: PathBuf::from("/tmp/anthropic-lb-test.state.json"),
            proxy_key: None,
            allowed_ips: vec![],
            upstreams: vec![],
            client_names: HashMap::new(),
            auto_cache: true,
            client_usage: Mutex::new(HashMap::new()),
            shadow_log_tx: None,
            client_budgets: budgets,
            budget_usage: Mutex::new(HashMap::new()),
            client_utilization_limits: HashMap::new(),
            operator: None,
            emergency_threshold: DEFAULT_EMERGENCY_THRESHOLD,
            client_request_rates: Mutex::new(HashMap::new()),
            soft_limit: 1.0,
        });

        // Within budget
        assert!(state.check_budget("client-a").is_ok());

        // Record some usage
        state.record_budget_usage("client-a", 500);
        assert!(state.check_budget("client-a").is_ok());

        // Exceed budget
        state.record_budget_usage("client-a", 600);
        assert!(state.check_budget("client-a").is_err());

        // Unknown client has no budget, always ok
        assert!(state.check_budget("unknown").is_ok());
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
            .body(r#"{"model":"claude-sonnet-4-20250514","messages":[{"role":"user","content":"hi"}]}"#)
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
        let result = state.pick_account(None, "").await;
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
            if let Some(idx) = state.pick_account(None, "").await {
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
            cooldown: Duration::from_secs(60),
            state_path: PathBuf::from("/tmp/anthropic-lb-test.state.json"),
            proxy_key: None,
            allowed_ips: vec![],
            upstreams: vec![],
            client_names: HashMap::new(),
            auto_cache: true,
            client_usage: Mutex::new(HashMap::new()),
            shadow_log_tx: None,
            client_budgets: HashMap::new(),
            budget_usage: Mutex::new(HashMap::new()),
            client_utilization_limits: HashMap::new(),
            operator: None,
            emergency_threshold: DEFAULT_EMERGENCY_THRESHOLD,
            client_request_rates: Mutex::new(HashMap::new()),
            soft_limit: 0.90, // Key: not 1.0 — throttled (0.98) will be excluded
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
            if let Some(idx) = state.pick_account(None, "").await {
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
            if let Some(idx) = state.pick_account(None, "").await {
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

    // ── Claim penalty tests ────────────────────────────────────────

    #[test]
    fn claim_penalty_at_zero() {
        assert_eq!(claim_penalty_7d(0.0), 0.0);
    }

    #[test]
    fn claim_penalty_below_threshold() {
        let result = claim_penalty_7d(0.60);
        assert!(
            (result - 0.60).abs() < 0.001,
            "below threshold: no penalty, got {result}"
        );
    }

    #[test]
    fn claim_penalty_at_threshold() {
        let result = claim_penalty_7d(0.70);
        assert!(
            (result - 0.70).abs() < 0.001,
            "at threshold: no penalty, got {result}"
        );
    }

    #[test]
    fn claim_penalty_at_85() {
        let result = claim_penalty_7d(0.85);
        // excess = 0.15, penalty = 0.15^2 * 3.33 = 0.0225 * 3.33 = 0.074925
        // total = 0.85 + 0.074925 = 0.924925
        assert!(
            (result - 0.925).abs() < 0.01,
            "85% should map to ~0.925, got {result}"
        );
    }

    #[test]
    fn claim_penalty_at_90() {
        let result = claim_penalty_7d(0.90);
        // excess = 0.20, penalty = 0.04 * 3.33 = 0.1332
        // total = 0.90 + 0.1332 = 1.0332
        assert!(
            result > 1.0,
            "90% should map above 1.0 (zero headroom), got {result}"
        );
        assert!(
            (result - 1.033).abs() < 0.01,
            "90% should map to ~1.033, got {result}"
        );
    }

    #[test]
    fn claim_penalty_at_100() {
        let result = claim_penalty_7d(1.0);
        assert!(result.is_finite(), "100% should not produce NaN/inf");
        // excess = 0.30, penalty = 0.09 * 3.33 = 0.2997
        // total = 1.0 + 0.2997 = 1.2997
        assert!(result > 1.0, "100% should be well above 1.0, got {result}");
    }

    #[test]
    fn claim_penalty_5h_unaffected() {
        // Values below threshold pass through as identity
        assert_eq!(claim_penalty_7d(0.50), 0.50);
        assert_eq!(claim_penalty_7d(0.69), 0.69);
        // Boundary: just above threshold does get penalized
        let penalized = claim_penalty_7d(0.90);
        assert!(penalized > 0.90, "above threshold should be penalized");
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
            cooldown: Duration::from_secs(60),
            state_path: PathBuf::from("/tmp/test.state.json"),
            proxy_key: None,
            allowed_ips: vec![],
            upstreams: vec![],
            client_names,
            auto_cache: true,
            client_usage: Mutex::new(HashMap::new()),
            shadow_log_tx: None,
            client_budgets: HashMap::new(),
            budget_usage: Mutex::new(HashMap::new()),
            client_utilization_limits: HashMap::new(),
            operator: None,
            emergency_threshold: DEFAULT_EMERGENCY_THRESHOLD,
            client_request_rates: Mutex::new(HashMap::new()),
            soft_limit: 1.0,
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
            cooldown: Duration::from_secs(60),
            state_path: PathBuf::from("/tmp/test.state.json"),
            proxy_key: None,
            allowed_ips: vec![],
            upstreams: vec![],
            client_names,
            auto_cache: true,
            client_usage: Mutex::new(HashMap::new()),
            shadow_log_tx: None,
            client_budgets: HashMap::new(),
            budget_usage: Mutex::new(HashMap::new()),
            client_utilization_limits: HashMap::new(),
            operator: Some("ray".to_string()),
            emergency_threshold: DEFAULT_EMERGENCY_THRESHOLD,
            client_request_rates: Mutex::new(HashMap::new()),
            soft_limit: 1.0,
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
        let info = RateLimitInfo {
            utilization_5h: Some(0.60),
            utilization_7d: Some(0.80),
            reset_5h: Some(now_epoch + 10000),
            reset_7d: Some(now_epoch + 100000),
            ..Default::default()
        };
        let (util, source) = effective_utilization(&info, now_epoch);
        // 7d at 0.80 → penalized to 0.833. Max(0.60, 0.833) = 0.833
        assert_eq!(source, "time_adjusted");
        assert!(
            util > 0.80,
            "7d claim penalty should push above raw 0.80, got {util}"
        );
        assert!((util - 0.833).abs() < 0.01, "expected ~0.833, got {util}");
    }

    #[tokio::test]
    async fn effective_util_5h_only() {
        let now_epoch = AppState::now_epoch();
        let info = RateLimitInfo {
            utilization_5h: Some(0.60),
            reset_5h: Some(now_epoch + 10000),
            // 7d reset in the past → stale
            utilization_7d: Some(0.90),
            reset_7d: Some(now_epoch - 1),
            ..Default::default()
        };
        let (util, source) = effective_utilization(&info, now_epoch);
        assert_eq!(source, "time_adjusted_5h");
        assert!(
            (util - 0.60).abs() < 0.01,
            "should use 5h value: got {util}"
        );
    }

    #[tokio::test]
    async fn effective_util_7d_only() {
        let now_epoch = AppState::now_epoch();
        let info = RateLimitInfo {
            // 5h stale
            utilization_5h: Some(0.40),
            reset_5h: Some(now_epoch - 1),
            utilization_7d: Some(0.50),
            reset_7d: Some(now_epoch + 100000),
            ..Default::default()
        };
        let (util, source) = effective_utilization(&info, now_epoch);
        assert_eq!(source, "time_adjusted_7d");
        // 0.50 is below CLAIM_PENALTY_THRESHOLD, so no penalty
        assert!(
            (util - 0.50).abs() < 0.01,
            "should use 7d value: got {util}"
        );
    }

    #[tokio::test]
    async fn effective_util_fallback_unified() {
        let now_epoch = AppState::now_epoch();
        let info = RateLimitInfo {
            // Both windows stale
            utilization_5h: Some(0.40),
            reset_5h: Some(now_epoch - 1),
            utilization_7d: Some(0.50),
            reset_7d: Some(now_epoch - 1),
            utilization: Some(0.65),
            ..Default::default()
        };
        let (util, source) = effective_utilization(&info, now_epoch);
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
        let (util, source) = effective_utilization(&info, now_epoch);
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
        let (util, source) = effective_utilization(&info, now_epoch);
        assert_eq!(source, "unknown");
        assert!(
            (util - 0.50).abs() < 0.001,
            "should default to 0.5: got {util}"
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
            cooldown: Duration::from_secs(60),
            state_path: PathBuf::from("/tmp/test.state.json"),
            proxy_key: None,
            allowed_ips: vec![],
            upstreams: vec![],
            client_names: HashMap::new(),
            auto_cache: true,
            client_usage: Mutex::new(HashMap::new()),
            shadow_log_tx: None,
            client_budgets: HashMap::new(),
            budget_usage: Mutex::new(HashMap::new()),
            client_utilization_limits: limits,
            operator: None,
            emergency_threshold: DEFAULT_EMERGENCY_THRESHOLD,
            client_request_rates: Mutex::new(HashMap::new()),
            soft_limit: 1.0,
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
            cooldown: Duration::from_secs(60),
            state_path: PathBuf::from("/tmp/test.state.json"),
            proxy_key: None,
            allowed_ips: vec![],
            upstreams: vec![],
            client_names: HashMap::new(),
            auto_cache: true,
            client_usage: Mutex::new(HashMap::new()),
            shadow_log_tx: None,
            client_budgets: HashMap::new(),
            budget_usage: Mutex::new(HashMap::new()),
            client_utilization_limits: limits,
            operator: None,
            emergency_threshold: DEFAULT_EMERGENCY_THRESHOLD,
            client_request_rates: Mutex::new(HashMap::new()),
            soft_limit: 1.0,
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
            cooldown: Duration::from_secs(60),
            state_path: PathBuf::from("/tmp/test.state.json"),
            proxy_key: None,
            allowed_ips: vec![],
            upstreams: vec![],
            client_names: HashMap::new(),
            auto_cache: true,
            client_usage: Mutex::new(HashMap::new()),
            shadow_log_tx: None,
            client_budgets: HashMap::new(),
            budget_usage: Mutex::new(HashMap::new()),
            client_utilization_limits: limits,
            operator: None,
            emergency_threshold: DEFAULT_EMERGENCY_THRESHOLD,
            client_request_rates: Mutex::new(HashMap::new()),
            soft_limit: 1.0,
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
            cooldown: Duration::from_secs(60),
            state_path: PathBuf::from("/tmp/test.state.json"),
            proxy_key: None,
            allowed_ips: vec![],
            upstreams: vec![],
            client_names: HashMap::new(),
            auto_cache: true,
            client_usage: Mutex::new(HashMap::new()),
            shadow_log_tx: None,
            client_budgets: HashMap::new(),
            budget_usage: Mutex::new(HashMap::new()),
            client_utilization_limits: limits,
            operator: Some("ray".to_string()),
            emergency_threshold: DEFAULT_EMERGENCY_THRESHOLD,
            client_request_rates: Mutex::new(HashMap::new()),
            soft_limit: 1.0,
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
            cooldown: Duration::from_secs(60),
            state_path: PathBuf::from("/tmp/test.state.json"),
            proxy_key: None,
            allowed_ips: vec![],
            upstreams: vec![],
            client_names: HashMap::new(),
            auto_cache: true,
            client_usage: Mutex::new(HashMap::new()),
            shadow_log_tx: None,
            client_budgets: HashMap::new(),
            budget_usage: Mutex::new(HashMap::new()),
            client_utilization_limits: limits,
            operator: None,
            emergency_threshold: DEFAULT_EMERGENCY_THRESHOLD,
            client_request_rates: Mutex::new(HashMap::new()),
            soft_limit: 1.0,
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
            cooldown: Duration::from_secs(60),
            state_path: PathBuf::from("/tmp/test.state.json"),
            proxy_key: None,
            allowed_ips: vec![],
            upstreams: vec![],
            client_names: HashMap::new(),
            auto_cache: true,
            client_usage: Mutex::new(HashMap::new()),
            shadow_log_tx: None,
            client_budgets: HashMap::new(),
            budget_usage: Mutex::new(HashMap::new()),
            client_utilization_limits: limits,
            operator: Some("ray".to_string()),
            emergency_threshold: DEFAULT_EMERGENCY_THRESHOLD,
            client_request_rates: Mutex::new(HashMap::new()),
            soft_limit: 1.0,
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
            cooldown: Duration::from_secs(60),
            state_path: PathBuf::from("/tmp/test.state.json"),
            proxy_key: None,
            allowed_ips: vec![],
            upstreams: vec![],
            client_names: HashMap::new(),
            auto_cache: true,
            client_usage: Mutex::new(HashMap::new()),
            shadow_log_tx: None,
            client_budgets: HashMap::new(),
            budget_usage: Mutex::new(HashMap::new()),
            client_utilization_limits: HashMap::new(),
            operator: None,
            emergency_threshold: 0.80, // custom low threshold
            client_request_rates: Mutex::new(HashMap::new()),
            soft_limit: 1.0,
        });
        set_account_utilization(&state, 0, 0.85, 0.70, now + 10000, now + 100000).await;
        assert!(
            state.is_emergency_brake_active().await,
            "0.85 should exceed custom 0.80 threshold"
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
            cooldown: Duration::from_secs(60),
            state_path: PathBuf::from("/tmp/test.state.json"),
            proxy_key: None,
            allowed_ips: vec![],
            upstreams: vec![],
            client_names: HashMap::new(),
            auto_cache: true,
            client_usage: Mutex::new(HashMap::new()),
            shadow_log_tx: None,
            client_budgets: HashMap::new(),
            budget_usage: Mutex::new(HashMap::new()),
            client_utilization_limits: HashMap::new(),
            operator: Some("ray".to_string()),
            emergency_threshold: DEFAULT_EMERGENCY_THRESHOLD,
            client_request_rates: Mutex::new(HashMap::new()),
            soft_limit: 1.0,
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
            cooldown: Duration::from_secs(60),
            state_path: PathBuf::from("/tmp/test.state.json"),
            proxy_key: None,
            allowed_ips: vec![],
            upstreams: vec![],
            client_names: HashMap::new(),
            auto_cache: true,
            client_usage: Mutex::new(HashMap::new()),
            shadow_log_tx: None,
            client_budgets: HashMap::new(),
            budget_usage: Mutex::new(HashMap::new()),
            client_utilization_limits: HashMap::new(),
            operator: Some("ray".to_string()),
            emergency_threshold: DEFAULT_EMERGENCY_THRESHOLD,
            client_request_rates: Mutex::new(HashMap::new()),
            soft_limit: 1.0,
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
            cooldown: Duration::from_secs(60),
            state_path: PathBuf::from("/tmp/test.state.json"),
            proxy_key: None,
            allowed_ips: vec![],
            upstreams: vec![],
            client_names: HashMap::new(),
            auto_cache: true,
            client_usage: Mutex::new(HashMap::new()),
            shadow_log_tx: None,
            client_budgets: HashMap::new(),
            budget_usage: Mutex::new(HashMap::new()),
            client_utilization_limits: limits,
            operator: None,
            emergency_threshold: DEFAULT_EMERGENCY_THRESHOLD,
            client_request_rates: Mutex::new(HashMap::new()),
            soft_limit: 1.0,
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
            cooldown: Duration::from_secs(60),
            state_path: PathBuf::from("/tmp/test-limit-reject.state.json"),
            proxy_key: Some("key".to_string()),
            allowed_ips: vec![],
            upstreams: vec![],
            client_names: HashMap::new(),
            auto_cache: false,
            client_usage: Mutex::new(HashMap::new()),
            shadow_log_tx: None,
            client_budgets: HashMap::new(),
            budget_usage: Mutex::new(HashMap::new()),
            client_utilization_limits: limits,
            operator: None,
            emergency_threshold: DEFAULT_EMERGENCY_THRESHOLD,
            client_request_rates: Mutex::new(HashMap::new()),
            soft_limit: 1.0,
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
            cooldown: Duration::from_secs(60),
            state_path: PathBuf::from("/tmp/test-limit-pass.state.json"),
            proxy_key: Some("key".to_string()),
            allowed_ips: vec![],
            upstreams: vec![],
            client_names: HashMap::new(),
            auto_cache: false,
            client_usage: Mutex::new(HashMap::new()),
            shadow_log_tx: None,
            client_budgets: HashMap::new(),
            budget_usage: Mutex::new(HashMap::new()),
            client_utilization_limits: limits,
            operator: None,
            emergency_threshold: DEFAULT_EMERGENCY_THRESHOLD,
            client_request_rates: Mutex::new(HashMap::new()),
            soft_limit: 1.0,
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
            cooldown: Duration::from_secs(60),
            state_path: PathBuf::from("/tmp/test-emergency-block.state.json"),
            proxy_key: Some("key".to_string()),
            allowed_ips: vec![],
            upstreams: vec![],
            client_names: HashMap::new(),
            auto_cache: false,
            client_usage: Mutex::new(HashMap::new()),
            shadow_log_tx: None,
            client_budgets: HashMap::new(),
            budget_usage: Mutex::new(HashMap::new()),
            client_utilization_limits: HashMap::new(),
            operator: Some("ray".to_string()),
            emergency_threshold: DEFAULT_EMERGENCY_THRESHOLD,
            client_request_rates: Mutex::new(HashMap::new()),
            soft_limit: 1.0,
        });
        // All accounts above emergency threshold — use 5h only to avoid
        // claim_penalty_7d pushing effective_util above 1.0 (which would cause
        // zero headroom in pick_account). 5h=0.96 > emergency threshold (0.95).
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
            cooldown: Duration::from_secs(60),
            state_path: PathBuf::from("/tmp/test-emergency-operator.state.json"),
            proxy_key: Some("key".to_string()),
            allowed_ips: vec![],
            upstreams: vec![],
            client_names,
            auto_cache: false,
            client_usage: Mutex::new(HashMap::new()),
            shadow_log_tx: None,
            client_budgets: HashMap::new(),
            budget_usage: Mutex::new(HashMap::new()),
            client_utilization_limits: HashMap::new(),
            operator: Some("ray".to_string()),
            emergency_threshold: DEFAULT_EMERGENCY_THRESHOLD,
            client_request_rates: Mutex::new(HashMap::new()),
            soft_limit: 1.0,
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
            cooldown: Duration::from_secs(60),
            state_path: PathBuf::from("/tmp/test-openai-limit.state.json"),
            proxy_key: Some("key".to_string()),
            allowed_ips: vec![],
            upstreams: vec![],
            client_names: HashMap::new(),
            auto_cache: false,
            client_usage: Mutex::new(HashMap::new()),
            shadow_log_tx: None,
            client_budgets: HashMap::new(),
            budget_usage: Mutex::new(HashMap::new()),
            client_utilization_limits: limits,
            operator: None,
            emergency_threshold: DEFAULT_EMERGENCY_THRESHOLD,
            client_request_rates: Mutex::new(HashMap::new()),
            soft_limit: 1.0,
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
            cooldown: Duration::from_secs(60),
            state_path: PathBuf::from("/tmp/test-openai-emergency.state.json"),
            proxy_key: Some("key".to_string()),
            allowed_ips: vec![],
            upstreams: vec![],
            client_names: HashMap::new(),
            auto_cache: false,
            client_usage: Mutex::new(HashMap::new()),
            shadow_log_tx: None,
            client_budgets: HashMap::new(),
            budget_usage: Mutex::new(HashMap::new()),
            client_utilization_limits: HashMap::new(),
            operator: None,
            emergency_threshold: DEFAULT_EMERGENCY_THRESHOLD,
            client_request_rates: Mutex::new(HashMap::new()),
            soft_limit: 1.0,
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
        assert_eq!(body["strategy"], "dynamic-capacity");
    }

    // ── Additional comprehensive tests ──────────────────────────────────

    #[test]
    fn resolve_client_id_prefers_header() {
        let state = test_state_with(vec![make_account("a", "sk-ant-api-x")]);
        let ip: IpAddr = "192.168.1.100".parse().unwrap();
        let mut headers = hyper::HeaderMap::new();
        headers.insert("x-client-id", HeaderValue::from_static("header-client"));

        let resolved = state.resolve_client_id(&ip, &headers);
        assert_eq!(resolved, "header-client", "should prefer x-client-id header");
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
            cooldown: Duration::from_secs(60),
            state_path: PathBuf::from("/tmp/test.state.json"),
            proxy_key: None,
            allowed_ips: vec![],
            upstreams: vec![],
            client_names,
            auto_cache: true,
            client_usage: Mutex::new(HashMap::new()),
            shadow_log_tx: None,
            client_budgets: HashMap::new(),
            budget_usage: Mutex::new(HashMap::new()),
            client_utilization_limits: HashMap::new(),
            operator: None,
            emergency_threshold: DEFAULT_EMERGENCY_THRESHOLD,
            client_request_rates: Mutex::new(HashMap::new()),
            soft_limit: 1.0,
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
            cooldown: Duration::from_secs(60),
            state_path: PathBuf::from("/tmp/test.state.json"),
            proxy_key: None,
            allowed_ips: vec![],
            upstreams: vec![],
            client_names: HashMap::new(),
            auto_cache: true,
            client_usage: Mutex::new(HashMap::new()),
            shadow_log_tx: None,
            client_budgets: HashMap::new(),
            budget_usage: Mutex::new(HashMap::new()),
            client_utilization_limits: HashMap::new(),
            operator: Some("operator-id".to_string()),
            emergency_threshold: DEFAULT_EMERGENCY_THRESHOLD,
            client_request_rates: Mutex::new(HashMap::new()),
            soft_limit: 1.0,
        });

        let status = compute_pressure_status(0.99, "operator-id", &state);
        assert_eq!(status, "healthy", "operator should always see healthy status");
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
            cooldown: Duration::from_secs(60),
            state_path: PathBuf::from("/tmp/test.state.json"),
            proxy_key: None,
            allowed_ips: vec![],
            upstreams: vec![],
            client_names: HashMap::new(),
            auto_cache: true,
            client_usage: Mutex::new(HashMap::new()),
            shadow_log_tx: None,
            client_budgets: HashMap::new(),
            budget_usage: Mutex::new(HashMap::new()),
            client_utilization_limits: limits,
            operator: None,
            emergency_threshold: DEFAULT_EMERGENCY_THRESHOLD,
            client_request_rates: Mutex::new(HashMap::new()),
            soft_limit: 1.0,
        });

        // 0.65 is > 80% of 0.80 limit (80% * 0.80 = 0.64), so should upgrade from healthy to elevated
        let status = compute_pressure_status(0.65, "client", &state);
        assert_eq!(status, "elevated", "proximity to limit should upgrade status");
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
        assert_eq!(floor, WARNING_UTIL_FLOOR, "unknown status should map to warning floor");
    }

    #[test]
    fn claim_penalty_7d_threshold() {
        // Below threshold: no penalty
        assert_eq!(claim_penalty_7d(0.50), 0.50);
        assert_eq!(claim_penalty_7d(0.70), 0.70);

        // Above threshold: quadratic penalty applied
        let result = claim_penalty_7d(0.80);
        assert!(result > 0.80, "penalty should increase utilization above threshold");

        let result = claim_penalty_7d(0.90);
        assert!(result > 1.0, "high utilization should push above 1.0 with penalty");
    }

    #[test]
    fn time_adjusted_utilization_stale_data() {
        let now = 1000000u64;
        let reset_past = 999000u64; // Reset already happened

        let result = time_adjusted_utilization(
            Some(0.50),
            Some(reset_past),
            Some("allowed"),
            3600.0,
            now,
        );

        assert_eq!(result, None, "stale data (reset in past) should return None");
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
        assert!(adjusted < 0.80, "near-reset utilization should be discounted");
        assert!(adjusted >= 0.04, "should apply minimum discount floor (0.05)");
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
        assert_eq!(adjusted, THROTTLE_UTIL_FLOOR, "status floor should override time discount");
    }

    #[test]
    fn epoch_to_iso8601_leap_year() {
        // 2024-02-29T00:00:00Z (leap day) = 1709164800
        let result = AppState::epoch_to_iso8601(1709164800);
        assert_eq!(result, "2024-02-29T00:00:00Z", "should handle leap year correctly");
    }

    #[test]
    fn epoch_to_iso8601_edge_of_year() {
        // 2023-12-31T23:59:59Z = 1704067199
        let result = AppState::epoch_to_iso8601(1704067199);
        assert_eq!(result, "2023-12-31T23:59:59Z", "should handle end of year correctly");
    }

    #[test]
    fn account_serves_model_empty_filter_allows_all() {
        let acct = make_account("test", "sk-ant-api-x");
        assert!(acct.serves_model("claude-sonnet-4-20250514"));
        assert!(acct.serves_model("claude-opus-4-20241113"));
        assert!(acct.serves_model(""));
    }

    #[test]
    fn account_serves_model_prefix_wildcard() {
        let mut acct = make_account("test", "sk-ant-api-x");
        acct.models = vec!["claude-opus-*".to_string()];

        assert!(acct.serves_model("claude-opus-4-20241113"));
        assert!(acct.serves_model("claude-opus-future"));
        assert!(!acct.serves_model("claude-sonnet-4-20250514"));
    }

    #[test]
    fn account_serves_model_multiple_patterns() {
        let mut acct = make_account("test", "sk-ant-api-x");
        acct.models = vec![
            "claude-opus-*".to_string(),
            "claude-sonnet-4-20250514".to_string(),
        ];

        assert!(acct.serves_model("claude-opus-4-20241113"));
        assert!(acct.serves_model("claude-sonnet-4-20250514"));
        assert!(!acct.serves_model("claude-sonnet-3-5-20240229"));
        assert!(!acct.serves_model("claude-haiku-3-5-20250219"));
    }

    #[tokio::test]
    async fn effective_utilization_prefers_most_constrained() {
        // When both windows have data, should return max (most constrained)
        let info = RateLimitInfo {
            utilization_5h: Some(0.80),
            utilization_7d: Some(0.60),
            reset_5h: Some(AppState::now_epoch() + 10000),
            reset_7d: Some(AppState::now_epoch() + 100000),
            status_5h: Some("allowed".to_string()),
            status_7d: Some("allowed".to_string()),
            ..Default::default()
        };

        let (util, source) = effective_utilization(&info, AppState::now_epoch());
        assert!(util > 0.60, "should use higher (5h) utilization");
        assert_eq!(source, "time_adjusted");
    }

    #[tokio::test]
    async fn effective_utilization_7d_penalty_applies() {
        // 7d window above penalty threshold should be penalized
        let now = AppState::now_epoch();
        let info = RateLimitInfo {
            utilization_5h: Some(0.50),
            utilization_7d: Some(0.85), // Above CLAIM_PENALTY_THRESHOLD (0.70)
            reset_5h: Some(now + 10000),
            reset_7d: Some(now + 100000),
            status_5h: Some("allowed".to_string()),
            status_7d: Some("allowed".to_string()),
            ..Default::default()
        };

        let (util, _) = effective_utilization(&info, now);
        assert!(util > 0.85, "7d window above 0.70 should have penalty applied");
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
            let idx = state.pick_account(None, "").await.unwrap();
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
            let idx = state.pick_account(None, "").await.unwrap();
            counts[idx] += 1;
        }

        assert!(counts[0] > 0, "first throttled account should get some traffic");
        assert!(counts[1] > 0, "second throttled account should get some traffic");
    }

    #[test]
    fn is_operator_checks_configured_operator() {
        let state = Arc::new(AppState {
            client: Client::new(),
            upstream: "http://127.0.0.1:1".to_string(),
            accounts: vec![],
            robin: AtomicUsize::new(0),
            cooldown: Duration::from_secs(60),
            state_path: PathBuf::from("/tmp/test.state.json"),
            proxy_key: None,
            allowed_ips: vec![],
            upstreams: vec![],
            client_names: HashMap::new(),
            auto_cache: true,
            client_usage: Mutex::new(HashMap::new()),
            shadow_log_tx: None,
            client_budgets: HashMap::new(),
            budget_usage: Mutex::new(HashMap::new()),
            client_utilization_limits: HashMap::new(),
            operator: Some("special-operator".to_string()),
            emergency_threshold: DEFAULT_EMERGENCY_THRESHOLD,
            client_request_rates: Mutex::new(HashMap::new()),
            soft_limit: 1.0,
        });

        assert!(state.is_operator("special-operator"));
        assert!(!state.is_operator("regular-client"));
    }

    #[test]
    fn is_operator_returns_false_when_no_operator_configured() {
        let state = test_state_with(vec![]);
        assert!(!state.is_operator("any-client"));
    }
}
}