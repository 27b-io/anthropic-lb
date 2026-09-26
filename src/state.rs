use crate::*;

// ── Runtime state ───────────────────────────────────────────────────

/// Per-claim utilization data for a single rate-limit window.
/// The API can report model-specific sub-budgets (e.g., "seven_day_sonnet")
/// alongside general windows ("seven_day"). Each gets its own entry.
#[derive(Default, Clone, Serialize, Deserialize)]
pub(crate) struct ClaimWindowData {
    /// None = no utilization data received yet (reset/status-only placeholder).
    /// Consumers must treat None as "unknown", not "healthy at 0%".
    pub(crate) utilization: Option<f64>,
    pub(crate) reset: Option<u64>,
    pub(crate) status: Option<String>,
    /// Epoch seconds when this entry was last updated. Used to age out entries
    /// that never received a reset timestamp (aligns runtime eviction with
    /// load_state behavior which drops reset-less entries).
    #[serde(default)]
    pub(crate) last_seen: u64,
}

/// Subset of RateLimitInfo for cross-replica sync via Redis.
/// Excludes Instant-based fields (non-serializable) and hard_limited_until (synced separately).
#[derive(Serialize, Deserialize)]
pub(crate) struct RedisRateInfo {
    pub(crate) utilization: Option<f64>,
    pub(crate) utilization_5h: Option<f64>,
    pub(crate) utilization_7d: Option<f64>,
    pub(crate) reset_5h: Option<u64>,
    pub(crate) reset_7d: Option<u64>,
    pub(crate) status_5h: Option<String>,
    pub(crate) status_7d: Option<String>,
    pub(crate) claims_7d: HashMap<String, ClaimWindowData>,
    pub(crate) representative_claim: Option<String>,
    pub(crate) remaining_requests: Option<u64>,
    pub(crate) remaining_tokens: Option<u64>,
    pub(crate) limit_requests: Option<u64>,
    pub(crate) limit_tokens: Option<u64>,
    #[serde(default)]
    pub(crate) overage_in_use: bool,
    #[serde(default)]
    pub(crate) overage_status: Option<String>,
    #[serde(default)]
    pub(crate) overage_utilization: Option<f64>,
    #[serde(default)]
    pub(crate) overage_reset: Option<u64>,
    pub(crate) updated_at: u64,
}

#[derive(Default)]
pub(crate) struct RateLimitInfo {
    /// Set once this account has refused a claim key for being over
    /// `MAX_CLAIMS_PER_ACCOUNT`, so the WARN is emitted once per process rather
    /// than once per request. Runtime-only: never persisted or synced.
    pub(crate) claim_cap_warned: bool,
    pub(crate) remaining_requests: Option<u64>,
    pub(crate) remaining_tokens: Option<u64>,
    pub(crate) limit_requests: Option<u64>,
    pub(crate) limit_tokens: Option<u64>,
    /// Unified utilization (0.0 = fresh, 1.0 = exhausted). Derived: max across all windows.
    pub(crate) utilization: Option<f64>,
    /// Per-claim 7d windows: "seven_day" (general), "seven_day_sonnet" (model-specific), etc.
    /// Source of truth for 7d utilization routing.
    pub(crate) claims_7d: HashMap<String, ClaimWindowData>,
    /// Derived convenience: max utilization across all claims_7d entries.
    /// Used for backward-compatible logging/stats. Routing reads claims_7d directly.
    pub(crate) utilization_7d: Option<f64>,
    pub(crate) utilization_5h: Option<f64>,
    /// Which window is the binding constraint (e.g. "five_hour", "seven_day_sonnet")
    pub(crate) representative_claim: Option<String>,
    /// Epoch seconds when the 5h rate-limit window resets.
    pub(crate) reset_5h: Option<u64>,
    /// Derived convenience: min reset across all claims_7d entries.
    pub(crate) reset_7d: Option<u64>,
    /// API-side pressure signal. "allowed" = normal, "allowed_warning" = approaching limits
    /// (floor 0.80), "throttled" = actively constrained (floor 0.98, soft-excluded),
    /// "rejected" = hard refusal (floor 1.0, zero bucket share).
    pub(crate) status_5h: Option<String>,
    /// Derived convenience: worst status across all claims_7d entries.
    pub(crate) status_7d: Option<String>,
    pub(crate) hard_limited_until: Option<Instant>,
    /// Overage (paid extra usage) is actively serving requests for this account.
    /// Account-level signal — overage covers whichever subscription window is exhausted.
    /// Always overwritten per response: header absent/false → false.
    pub(crate) overage_in_use: bool,
    /// Overage window status ("allowed", "allowed_warning", "rejected"). Feeds the
    /// routing gate via `status_to_floor` when overage is in use.
    pub(crate) overage_status: Option<String>,
    /// Overage budget consumed (0.0 = fresh, 1.0 = overage exhausted).
    pub(crate) overage_utilization: Option<f64>,
    /// Epoch seconds when the overage window resets.
    pub(crate) overage_reset: Option<u64>,
    /// Counts consecutive burst 429s (no retry-after) for exponential backoff.
    /// Reset to 0 on any successful response.
    pub(crate) consecutive_burst_429s: u32,
    /// Consecutive upstream transport failures (ETIMEDOUT/reset/closed/DNS).
    /// Transport health, NOT rate-limit state — independent of
    /// hard_limited_until, and deliberately process-scoped (never persisted or
    /// Redis-synced, same as `upstream_transport_errors`): each replica has its
    /// own egress path, so another replica's connectivity says nothing about ours.
    pub(crate) consecutive_transport_failures: u32,
    /// Circuit breaker: while set and in the future, the endpoint is excluded
    /// from `routing_candidates` so a stateless affinity recompute cannot snap
    /// a session back to a persistently-dead endpoint every request. Opened
    /// after TRANSPORT_FAILURE_THRESHOLD consecutive transport failures;
    /// cleared on any successful forward or after the cooldown elapses.
    pub(crate) transport_unhealthy_until: Option<Instant>,
    #[allow(dead_code)]
    pub(crate) last_updated: Option<Instant>,
    /// Wall-clock epoch of last update, for cross-replica age comparison.
    pub(crate) last_updated_epoch: Option<u64>,
}

/// Exponentially weighted moving average with time-constant-based decay.
/// Handles variable inter-sample intervals correctly — the half-life is
/// wall-clock time, not dependent on request frequency.
pub(crate) struct Ewma {
    pub(crate) value: f64,
    /// Time constant (seconds). Half-life = tau * ln(2).
    pub(crate) tau: f64,
    pub(crate) last_update: Instant,
}

/// Minimum elapsed time between EWMA updates. Prevents division-by-zero
/// and inf propagation when requests arrive in the same Instant tick.
const EWMA_MIN_ELAPSED_SECS: f64 = 0.001;

/// EWMA stale threshold. If no updates for this long, reset to zero.
const EWMA_STALE_SECS: f64 = 3600.0;

impl Ewma {
    pub(crate) fn new(tau: f64) -> Self {
        Self {
            value: 0.0,
            tau,
            last_update: Instant::now(),
        }
    }

    pub(crate) fn update(&mut self, now: Instant) -> f64 {
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
pub(crate) const TAU_5M: f64 = 300.0;
pub(crate) const TAU_1H: f64 = 3600.0;

/// Upper bound on distinct clients tracked in the per-client metric maps
/// (`client_usage`, `client_request_rates`). These are keyed by the
/// user-controlled `x-client-id` header with no eviction, so an unbounded set of
/// distinct values would grow them without limit (a memory-DoS vector). Real
/// deployments have a handful of clients, far below this; the cap only bounds
/// unknown/abusive header values — existing clients keep updating past it.
pub(crate) const MAX_TRACKED_CLIENTS: usize = 10_000;

/// Cap on distinct (client, model) labels in the allowlist-denial counter.
/// The model half is caller-controlled, and under legacy auth the client
/// half is too (`x-client-id`), so overflow lumps into a single global
/// ("_other", "_other") bucket — a HARD bound of cap + 1 entries (LAB-2332,
/// mirroring the LAB-2330 fix to `client_model_usage`).
pub(crate) const MAX_MODEL_DENIED_LABELS: usize = 64;

/// Cap on distinct clients in the pre-request-gate rejection counter
/// (LAB-2551). Past the cap, rejections for NEW clients lump into a single
/// global `_other` client key (never per-client overflow keys — the CWE-770
/// shape), while already-tracked clients may still add entries under new
/// reasons — a brake event stamps every active client at once, so crossing
/// the cap mid-incident must not split a tracked client's attribution. The
/// reason axis is a closed static set of 3, so entries are hard-bounded at
/// 3 × (cap + 1). Real deployments have tens of clients; only caller-minted
/// ids under legacy header auth can approach this.
pub(crate) const MAX_CLIENT_REJECTION_LABELS: usize = 64;

/// Cap on distinct (client, model) pairs in the per-model usage counter
/// (LAB-2330). The model key is normally response-derived (upstream-validated),
/// but the request-model fallback is caller-influenced and the client key is
/// caller-controlled under legacy auth, so overflow lumps into a single
/// global ("_other", "_other") bucket — a HARD bound of cap + 1 entries.
/// Sized for the real fleet (tens of clients × a handful of models) with
/// generous slack.
pub(crate) const MAX_CLIENT_MODEL_LABELS: usize = 256;

/// Max chars retained from a caller-controlled string used as a metric label
/// or echoed in an error body. The model field is bounded only by the request
/// body cap, so an untruncated copy would be retained for the process lifetime
/// and re-serialized on every `/metrics` scrape.
pub(crate) const MAX_LABEL_CHARS: usize = 64;

/// Truncate a caller-controlled string to `MAX_LABEL_CHARS`, marking that it
/// was cut so an operator does not read a clipped value as the literal input.
/// Char-based, not byte-based: slicing a UTF-8 string mid-codepoint panics.
pub(crate) fn truncate_label(s: &str) -> String {
    if s.chars().count() <= MAX_LABEL_CHARS {
        return s.to_owned();
    }
    let mut out: String = s.chars().take(MAX_LABEL_CHARS).collect();
    out.push('…');
    out
}

/// Cap on distinct UNRESERVED 7d claim keys per account. Keys are minted from
/// the upstream `representative-claim` header — never from client input, but
/// still an unvalidated remote string, and each one becomes a permanent `claim`
/// label on the `anthropic_claim_*` series (CWE-770). Reserved keys are exempt,
/// so the hard bound per account is `MAX_CLAIMS_PER_ACCOUNT` + the reserved set.
const MAX_CLAIMS_PER_ACCOUNT: usize = 32;

/// Claim keys the cap must NEVER refuse: the four that gate all traffic plus the
/// normalised Fable band. A compile-time-closed set, so exempting them costs a
/// bounded five entries — and not exempting them is a routing hazard, not merely
/// a lost metric. `effective_utilization` skips its flat-field fallback whenever
/// `claims_7d` is non-empty, so an account whose map is full of unknown keys
/// with `seven_day` refused derives NO weekly utilization at all: it would route
/// as though it had no weekly limit, and drop out of the emergency brake's
/// all-accounts-saturated test while sitting at 100%.
fn claim_key_is_reserved(key: &str) -> bool {
    claim_gates_all_traffic(key) || key == FABLE_BAND_CLAIM
}

/// Whether a 7d claim key may be stored for this account. Mirrors the
/// `MAX_TRACKED_CLIENTS` admission rule — keys already present always pass, so
/// live claims keep updating past the cap — with reserved keys exempt entirely.
///
/// Overflow is dropped rather than folded into an `_other` bucket the way the
/// caller-labelled counters do it, because bucketing would buy nothing here:
/// both routing lookups (`resolve_7d_claim`, `constraining_7d_claims`) match
/// exact keys and the brake's input is allowlist-filtered, so an unknown key is
/// already inert to routing. Reserving the keys that are NOT inert is what makes
/// the cap safe; the bucket would only add a fake claim to the metrics.
///
/// Logged once per account: the refusal fires per request under a claim flood,
/// so a per-refusal line would let an upstream drive unbounded log volume —
/// the same exhaustion class the cap itself closes.
pub(crate) fn claim_admitted(info: &mut RateLimitInfo, key: &str, account: &str) -> bool {
    if claim_key_is_reserved(key) || info.claims_7d.contains_key(key) {
        return true;
    }
    // Count UNRESERVED keys only, matching `bound_ingested_claims`. Counting the
    // whole map would let the reserved keys eat the budget, so an account
    // carrying all five would admit 27 unknown claims rather than the
    // documented 32 — and the live path would then disagree with the ingest
    // path about the same bound. The map is at most 37 entries, so the scan is
    // cheaper than the allocation it avoids.
    let unreserved = info
        .claims_7d
        .keys()
        .filter(|k| !claim_key_is_reserved(k))
        .count();
    if unreserved < MAX_CLAIMS_PER_ACCOUNT {
        return true;
    }
    if !info.claim_cap_warned {
        info.claim_cap_warned = true;
        warn!(
            account,
            claim = key,
            cap = MAX_CLAIMS_PER_ACCOUNT,
            "claim cap reached; refusing unknown 7d claim keys for this account \
             (routing-relevant keys are still admitted)"
        );
    }
    false
}

/// Apply the key bound to a claims map that arrived whole rather than through
/// the header parser — the persisted state file and the Redis mirror both
/// assign `claims_7d` outright. Without this the bound holds only on the live
/// path, and a state file written by a pre-cap build (or a peer replica still
/// running one) restores an unbounded, untruncated map that `contains_key` then
/// lets every junk key keep updating forever.
///
/// Reserved keys always survive. The rest are kept in sorted order so every
/// replica and every restart retains the same subset rather than a
/// HashMap-iteration-order lottery.
pub(crate) fn bound_ingested_claims(
    claims: HashMap<String, ClaimWindowData>,
) -> HashMap<String, ClaimWindowData> {
    let mut out: HashMap<String, ClaimWindowData> = claims
        .into_iter()
        .map(|(k, v)| (truncate_label(&k), v))
        .collect();
    let mut unreserved: Vec<String> = out
        .keys()
        .filter(|k| !claim_key_is_reserved(k))
        .cloned()
        .collect();
    if unreserved.len() <= MAX_CLAIMS_PER_ACCOUNT {
        return out;
    }
    unreserved.sort_unstable();
    for key in unreserved.into_iter().skip(MAX_CLAIMS_PER_ACCOUNT) {
        out.remove(&key);
    }
    out
}
const TAU_6H: f64 = 21600.0;

/// Per-account burn rate tracker: requests per minute at three time scales.
pub(crate) struct BurnRate {
    pub(crate) rate_5m: Ewma,
    pub(crate) rate_1h: Ewma,
    pub(crate) rate_6h: Ewma,
}

impl BurnRate {
    pub(crate) fn new() -> Self {
        Self {
            rate_5m: Ewma::new(TAU_5M),
            rate_1h: Ewma::new(TAU_1H),
            rate_6h: Ewma::new(TAU_6H),
        }
    }

    pub(crate) fn update(&mut self, now: Instant) {
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
pub(crate) const DEFAULT_EMERGENCY_THRESHOLD: f64 = 0.88;

/// Claude Code system prompt required by the Anthropic API for OAuth tokens (sk-ant-oat*)
/// to access sonnet/opus models. Any position in the `system` array satisfies the upstream;
/// it is inserted at index 0 unless a Claude Code attribution block already holds that slot.
pub(crate) const OAUTH_SYSTEM_PROMPT: &str =
    "You are Claude Code, Anthropic's official CLI for Claude.";

/// Prefix of the attribution block Claude Code sends as `system[0]` (client version +
/// conversation fingerprint). The upstream strips it only when it arrives unchanged at
/// index 0, so anything the proxy inserts must go AFTER it — otherwise the block reaches
/// the model and the prompt-cache key.
pub(crate) const ATTRIBUTION_BLOCK_PREFIX: &str = "x-anthropic-billing-header:";

/// Max bytes of 429 response body to include in debug logs.
const MAX_429_BODY_LOG_BYTES: usize = 512;

/// Substrings that mark a header as sensitive — any header whose name contains
/// one of these is redacted from debug logs. Safer than a denylist: new
/// sensitive headers (e.g. `x-auth-foo`, `session-token`) are caught by default.
pub(crate) const SENSITIVE_HEADER_SUBSTRINGS: &[&str] =
    &["auth", "cookie", "token", "key", "secret", "session"];

/// True when a 429 is a transient BURST limit rather than capacity exhaustion:
/// `x-should-retry` set, but no `retry-after` and no rate-limit headers.
///
/// This distinction is account-level and speed-blind. Anthropic applies burst
/// (per-minute RPM / concurrency) limits to the ACCOUNT, not to a request's
/// rate bucket, so a burst 429 is real evidence about the account even when
/// the request asked for fast mode — which is why the fast-mode exemption in
/// `classify_retry_status` defers to it (LAB-2675 panel finding). Shared with
/// `mark_hard_limited_for`, which uses it to pick the backoff ladder over the
/// capacity cooldown, so the two can never disagree on what "burst" means.
pub(crate) fn is_burst_429(headers: &reqwest::header::HeaderMap) -> bool {
    let has_rate_headers = headers.keys().any(|k| {
        let name = k.as_str();
        name.starts_with("anthropic-ratelimit-requests")
            || name.starts_with("anthropic-ratelimit-tokens")
            || name.starts_with("anthropic-ratelimit-unified-")
            || name.starts_with("x-ratelimit-")
    });
    let should_retry = headers.get("x-should-retry").and_then(|v| v.to_str().ok()) == Some("true");
    should_retry && !headers.contains_key("retry-after") && !has_rate_headers
}

/// Format 429 response headers and body for a single debug log line.
/// Redacts sensitive headers, truncates body to MAX_429_BODY_LOG_BYTES.
pub(crate) async fn log_429_details(account_name: &str, resp: reqwest::Response) {
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
pub(crate) fn compute_pressure_status(
    effective_util: f64,
    client_id: &str,
    state: &AppState,
) -> &'static str {
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
pub(crate) struct Endpoint {
    pub(crate) name: String,
    pub(crate) protocol: Protocol,
    /// Resolved at startup: api.anthropic.com for Anthropic default, else the
    /// explicit base_url. No trailing slash.
    pub(crate) base_url: String,
    pub(crate) token: String,
    /// True iff token == "passthrough". Only meaningful for Protocol::Anthropic.
    pub(crate) passthrough: bool,
    pub(crate) models: Vec<String>,
    pub(crate) priority: u32,
    /// Plan includes Fable's 50%-of-weekly band. False = Fable is always paid
    /// here (Pro plan) → Fable requests demote this endpoint by `overage_penalty`.
    pub(crate) fable_included: bool,
    pub(crate) requests: AtomicU64,
    pub(crate) rate_info: RwLock<RateLimitInfo>,
    pub(crate) burn_rate: Mutex<BurnRate>,
    pub(crate) input_tokens: AtomicU64,
    pub(crate) output_tokens: AtomicU64,
    pub(crate) cache_creation_tokens: AtomicU64,
    pub(crate) cache_read_tokens: AtomicU64,
    pub(crate) last_routing_weight: AtomicU64,
    pub(crate) last_routing_share: AtomicU64,
    pub(crate) last_effective_gate: AtomicU64,
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
pub(crate) fn model_matches(patterns: &[String], model: &str) -> bool {
    if patterns.is_empty() || model.is_empty() {
        return true;
    }
    patterns.iter().any(|p| suffix_wildcard_match(p, model))
}

impl Endpoint {
    /// Check if this endpoint can serve the given model. Empty allowlist = all.
    /// Identical to the historical `Account::serves_model` predicate.
    pub(crate) fn serves_model(&self, model: &str) -> bool {
        model_matches(&self.models, model)
    }
}

pub(crate) struct AppState {
    pub(crate) client: Client,
    /// Upstream client for NON-streaming requests (`"stream"` false/absent).
    /// A non-streaming `/v1/messages` emits ZERO response bytes until
    /// generation completes, so `client`'s read_timeout — tuned for SSE
    /// inter-chunk silence — kills any generation longer than 180s as
    /// "operation timed out" (LAB-718 GEO judge wedge, 2026-07-24: ~20k-token
    /// structured-output calls died 18×/hour across 9 accounts and the SDK
    /// retried for hours). No read_timeout here; the 900s total budget is the
    /// only cap, and the h2 keep-alive PING still evicts dead connections.
    pub(crate) client_nonstreaming: Client,
    /// Unified routing endpoints — the sole endpoint pool.
    pub(crate) endpoints: Vec<Endpoint>,
    pub(crate) robin: AtomicUsize,
    pub(crate) routing_strategy: RoutingStrategy,
    pub(crate) cooldown: Duration,
    /// How long a transport-circuit-broken endpoint stays out of routing.
    /// TRANSPORT_UNHEALTHY_COOLDOWN in production; overridable so tests can
    /// exercise breaker re-entry without a 30s sleep.
    pub(crate) transport_cooldown: Duration,
    pub(crate) state_path: PathBuf,
    /// Legacy single shared secret. Mutually exclusive with `clients`.
    pub(crate) proxy_key: Option<String>,
    /// Authenticated client registry (LAB-1083). Non-empty ⇒ every request
    /// through an authenticated entry point carries a verified principal, and
    /// `client_id` is that principal's name rather than a client-asserted
    /// header. Empty ⇒ legacy `proxy_key` / open behaviour.
    pub(crate) clients: Vec<ClientConfig>,
    /// LAB-3877: Tier 0 content guard — the compiled scanners plus their
    /// metrics. Built once at startup; scanned read-only per request. Present
    /// only under the `guard` feature.
    #[cfg(feature = "guard")]
    pub(crate) guard: guard::Guard,
    pub(crate) allowed_ips: Vec<IpAllowEntry>,
    /// Load balancers whose `x-forwarded-for` is trusted (LAB-1192).
    /// Consulted only by `resolve_client_ip`. Empty = header ignored.
    pub(crate) trusted_proxies: Vec<IpAllowEntry>,
    /// Per-client-IP failed-authentication throttle (LAB-1192).
    pub(crate) auth_throttle: AuthThrottle,
    /// Failed authentication attempts by (route, presented-credential shape)
    /// for `anthropic_auth_failures_total{route,cred}` (LAB-4720). Both are
    /// fixed vocabularies, so cardinality is fixed.
    pub(crate) auth_failures: Mutex<HashMap<AuthFailureKey, u64>>,
    /// Last time the `allow_unauthenticated` admin-access warn fired per route,
    /// so it stays visible without one line per scrape (LAB-1192 AC-5).
    pub(crate) open_admin_warn: Mutex<HashMap<&'static str, Instant>>,
    pub(crate) client_names: HashMap<String, String>,
    pub(crate) auto_cache: bool,
    /// Per-client token usage: client_id → [input, output, cache_creation, cache_read]
    pub(crate) client_usage: Mutex<HashMap<String, [u64; 4]>>,
    /// Per-(client, model) token usage (LAB-2330) — same [u64; 4] layout as
    /// `client_usage`, which stays the authoritative per-client total. The
    /// model key is response-derived and truncated; the pair count is hard-
    /// bounded at `MAX_CLIENT_MODEL_LABELS` + 1 (overflow lumps into the
    /// global ("_other", "_other") bucket), so callers cannot inflate the
    /// label set on either axis.
    pub(crate) client_model_usage: Mutex<HashMap<(String, String), [u64; 4]>>,
    /// Shadow log sender (fire-and-forget JSONL appends). None = disabled.
    pub(crate) shadow_log_tx: Option<tokio::sync::mpsc::Sender<String>>,
    /// Count of shadow log entries dropped due to channel backpressure.
    pub(crate) shadow_log_dropped: AtomicU64,
    /// Per-client daily token budgets: client_id → max tokens per day.
    pub(crate) client_budgets: HashMap<String, u64>,
    /// Budget tracking: client_id → (epoch_day, tokens_used). Resets on new day.
    /// Process-local; with Redis it is re-seeded from the shared counter every
    /// sync tick (`fold_budget_mirror`, LAB-3217) so it survives restarts.
    pub(crate) budget_usage: Mutex<HashMap<String, (u64, u64)>>,
    /// Per-client utilization limits: client_id → max effective utilization.
    pub(crate) client_utilization_limits: HashMap<String, f64>,
    /// Operator client IDs — never throttled by budgets, ceilings, or emergency brake.
    pub(crate) operators: Vec<String>,
    /// Read-only client IDs — `/_stats` + `/metrics` only, 403 on every `/v1`
    /// surface. Disjoint from `operators`; the overlap is a boot error.
    pub(crate) admin_readers: Vec<String>,
    /// Whether the emergency brake is enabled. Default: true.
    pub(crate) emergency_brake: bool,
    /// Emergency brake threshold. Default: 0.88.
    pub(crate) emergency_threshold: f64,
    /// Per-client request tracking: client_id → (total_requests, rate_ewma)
    pub(crate) client_request_rates: Mutex<HashMap<String, (u64, Ewma)>>,
    /// Utilization soft ceiling. Accounts above this are excluded from routing
    /// unless all candidates exceed it. Default: 0.90.
    pub(crate) soft_limit: f64,
    /// Redis client for distributed state. None = local-only (single instance).
    /// fred clients are cheap to clone; every clone shares one multiplexed
    /// connection driven by a background task with a reconnect policy.
    pub(crate) redis: Option<RedisClient>,
    /// Whether the coordination client has EVER connected. False from process
    /// start until the first successful connect (set once by
    /// `spawn_redis_connect_watcher`, never cleared). Gates coordination ops
    /// off entirely while never-yet-connected — see `coordination_redis`.
    pub(crate) redis_ever_connected: AtomicBool,
    /// Cached cluster info from Redis, updated by background sync task.
    pub(crate) cluster_info_cache: Mutex<Option<serde_json::Value>>,
    /// Monotonic request ID counter for log correlation.
    pub(crate) next_req_id: AtomicU64,
    /// Random instance ID for cross-replica log disambiguation.
    pub(crate) instance_id: u16,
    /// Probe interval in seconds. Used for freshness check and distributed lock TTL.
    pub(crate) probe_interval_secs: u64,
    /// Priority penalty added to an endpoint's effective priority while it serves
    /// via overage. Default: 10.
    pub(crate) overage_penalty: u32,
    /// Per-tick DELTA accumulator of upstream transport send-failures, keyed
    /// by kind (`timeout`/`connect`/`other`). Surfaces a flaky egress on the
    /// dashboard (`anthropic_upstream_transport_errors_total`) before it
    /// becomes client errors. Drained into the shared Redis hash each sync
    /// tick (`flush_transport_errors`); without Redis it simply accumulates
    /// and feeds `/metrics` directly. Not persisted across restarts.
    pub(crate) upstream_transport_errors: Mutex<HashMap<&'static str, u64>>,
    /// Current sum of reserved in-flight request-body bytes (admission control).
    pub(crate) inflight_body_bytes: AtomicU64,
    /// Ceiling for `inflight_body_bytes`; over it, requests are shed with 503.
    /// 0 = disabled (unbounded).
    pub(crate) max_inflight_body_bytes: u64,
    /// Count of requests load-shed because admitting them would exceed
    /// `max_inflight_body_bytes`. Exposed as `anthropic_body_shed_total`.
    pub(crate) body_shed_total: AtomicU64,
    /// Wall-clock ceiling for buffering a request body (P1-01). Bounds how long
    /// a slow or stalled client can hold its body-memory reservation.
    /// `Duration::ZERO` disables the timeout.
    pub(crate) body_read_timeout: Duration,
    /// Count of requests shed because the body was not fully received within
    /// `body_read_timeout`. Exposed as `anthropic_body_read_timeout_total`.
    pub(crate) body_read_timeout_total: AtomicU64,
    /// Affinity overrides that migrated a session, indexed by `AffinityBind`
    /// (what bound the sticky account). Exposed as
    /// `anthropic_affinity_migrations_total{reason="loaded"|"spent"|"floored"}`.
    pub(crate) affinity_migrations: [AtomicU64; 3],
    /// Client-facing pool-exhaustion responses, indexed by `PoolExhaustion`.
    /// Exposed as `anthropic_pool_exhausted_total{kind="rate_limited"|"transient"}`.
    /// The only metric trace of the 429/503 a caller actually received when the
    /// whole pool was unavailable (LAB-4189) — before this, exhaustion existed
    /// solely as a `warn!` line, so there was nothing to graph or alert on.
    pub(crate) pool_exhausted: [AtomicU64; 2],
    /// `anthropic_http_request_duration_seconds` cells keyed by
    /// `(route, status)`; bounded by construction — see `route_label`.
    /// Per-process — aggregate with `sum`.
    pub(crate) request_durations: Mutex<HashMap<(&'static str, u16), RequestDurationHist>>,
    /// Unix-epoch second this process built its state. Exported as
    /// `process_start_time_seconds` so a restart is visible directly rather
    /// than only as a counter reset.
    pub(crate) start_epoch: u64,
    /// Reflect upstream `anthropic-ratelimit-*` headers to callers (see
    /// `Config::expose_upstream_ratelimit_headers`). Default: false.
    pub(crate) expose_upstream_ratelimit_headers: bool,
    /// Relay caller-identity headers upstream (see
    /// `Config::forward_caller_identity`). Default: false = stripped.
    pub(crate) forward_caller_identity: bool,
    /// `anthropic-beta` flags a client may forward upstream on OAuth
    /// endpoints ("*" suffix wildcards). Flags outside the list are dropped.
    pub(crate) allowed_client_betas: Vec<String>,
    /// Dropped client beta flags → drop count, for
    /// `anthropic_beta_flag_dropped_total{flag}`. Flag names are
    /// client-controlled input, so the map is bounded
    /// (`MAX_DROPPED_BETA_FLAGS`); overflow lands in the `_other` bucket.
    pub(crate) beta_flags_dropped: Mutex<HashMap<String, u64>>,
    /// Top-level body fields stripped to keep a request coherent after the
    /// beta filter dropped their header (LAB-1261). Exposed as
    /// `anthropic_beta_body_field_stripped_total{field}`.
    ///
    /// This, not `anthropic_beta_flag_dropped_total`, is the alertable
    /// signal. A dropped HEADER is routine — clients steadily send flags the
    /// proxy does not carry, and nothing breaks when they are removed. A
    /// stripped BODY FIELD means the proxy rewrote a caller's request
    /// to stop it 400ing, i.e. a paired beta family arrived that the
    /// allow-list does not know. Keys are JSON object keys from a
    /// client-controlled body, so the map is bounded the same way
    /// (`MAX_DROPPED_BETA_FLAGS`, `_other` overflow).
    pub(crate) beta_body_fields_stripped: Mutex<HashMap<String, u64>>,
    /// Live session registry: affinity routing key → last-seen context-window
    /// occupancy (LAB-916). Visibility only — routing never reads it. Sync
    /// mutex, never held across `.await`; bounded by `session_registry_max`
    /// + TTL eviction.
    pub(crate) sessions: Mutex<HashMap<String, SessionEntry>>,
    /// Session registry entry cap. 0 disables the registry.
    pub(crate) session_registry_max: usize,
    /// Seconds since last request before a session entry is evicted.
    pub(crate) session_registry_ttl_secs: u64,
    /// Upstream "prompt is too long" 400s by model (LAB-916). Exposed as
    /// `anthropic_prompt_too_long_total`; bounded via `_other` overflow.
    pub(crate) prompt_too_long: Mutex<HashMap<String, u64>>,
    /// Upstream 429s on fast-mode requests, by account (LAB-2675). These are
    /// forwarded to the caller instead of cooling the account — fast mode has
    /// its own rate bucket — so this counter is the only operator-visible
    /// trace of a client draining fast capacity. Exposed as
    /// `anthropic_fast_mode_429_total{account}`. Account names come from
    /// config, so the label set is bounded by the operator and needs no
    /// `_other` overflow (unlike the caller-controlled `prompt_too_long` key).
    pub(crate) fast_mode_429: Mutex<HashMap<String, u64>>,
    /// Upstream "out of extra usage" 400s, by account (LAB-4729). The first
    /// per request is re-sent to another account; the account is NOT cooled
    /// (see `note_entitlement_400`). Exposed as
    /// `anthropic_entitlement_400_total{account}`; config-bounded labels, as
    /// `fast_mode_429`.
    pub(crate) entitlement_400: Mutex<HashMap<String, u64>>,
    /// Per-client model-allowlist denials, keyed (client, model) (LAB-1083).
    /// Exposed as `anthropic_client_model_denied_total`. Under `[[clients]]`
    /// auth `client` is a credential-bound principal, but under legacy
    /// `proxy_key` / `allow_unauthenticated` it comes from the
    /// caller-controlled `x-client-id` header — so overflow lumps into a
    /// single global ("_other", "_other") bucket, hard-bounding the map at
    /// `MAX_MODEL_DENIED_LABELS` + 1 entries (LAB-2332).
    pub(crate) model_denied: Mutex<HashMap<(String, String), u64>>,
    /// Pre-request-gate 429 rejections, keyed (client, reason) (LAB-2551).
    /// Exposed as `anthropic_client_rejections_total`. `reason` is the closed
    /// static set budget / utilization / brake — the three capacity denials in
    /// `pre_request_gate`; 403 policy denials stay on `model_denied`. `client`
    /// is caller-controlled under legacy header auth, so it is truncated and
    /// the map is bounded via a global `_other` overflow
    /// (`MAX_CLIENT_REJECTION_LABELS`).
    pub(crate) client_rejections: Mutex<HashMap<(String, &'static str), u64>>,
    /// (endpoint idx, model) pairs an upstream rejected as unsupported — a
    /// gateway without the model, or a plan without access (LAB-941).
    /// `routing_candidates` skips these until the entry expires; because
    /// session affinity is a stateless hash over the candidate list, the
    /// filter also re-buckets pinned sessions away from the endpoint. Sync
    /// mutex, never held across `.await`; bounded by UNSUPPORTED_MODEL_MAX
    /// + TTL eviction. Per-replica: a fresh replica re-learns in one attempt.
    pub(crate) unsupported_models: Mutex<HashMap<(usize, String), Instant>>,
    /// Opt-in encrypted response cache on non-streaming /v1/messages
    /// (LAB-933). None = feature off — the no-config case is byte-identical
    /// to pre-cache behaviour (AC1).
    pub(crate) response_cache: Option<ResponseCache>,
}

/// RAII reservation against `AppState::inflight_body_bytes`. Holding it keeps the
/// reserved bytes counted as in-flight; dropping it releases them. Held for the
/// duration of a request handler so the budget reflects real resident body memory.
pub(crate) struct BodyReservation {
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
pub(crate) fn reserve_request_body(
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
pub(crate) async fn read_body_bounded(
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
            error!(req_id, error = %e, "failed to read request body");
            Err(Box::new(
                (StatusCode::BAD_REQUEST, "bad request body").into_response(),
            ))
        }
    }
}

/// Index into `AppState.endpoints` — the sole runtime endpoint pool.
pub(crate) type EndpointIdx = usize;

#[derive(Clone, Copy, Debug)]
pub(crate) struct RoutingCandidate {
    pub(crate) endpoint: EndpointIdx,
    /// Effective priority tier — includes the overage penalty when applicable.
    pub(crate) priority: u32,
    pub(crate) gate_5h: f64,
    pub(crate) gate_7d: f64,
    pub(crate) gate: f64,
    pub(crate) wr: f64,
    /// Unused share of the model's weekly (7d) quota; 1.0 when unknown or
    /// superseded by overage. Time-free — the affinity override reads this,
    /// never `wr`/`weight` (see `affinity_headroom`).
    pub(crate) unused_7d: f64,
    pub(crate) weight: f64,
    /// The effective `gate` is set by an Anthropic status floor
    /// (`status_to_floor`) that exceeds raw utilisation, not by raw load or
    /// overage. Drives `affinity_headroom`'s `Floored` classification so a
    /// routine status-floor migration logs at INFO, not WARN (LAB-3295).
    pub(crate) gate_floor_bound: bool,
    pub(crate) source: &'static str,
}

pub(crate) fn stable_affinity_hash(key: &str) -> u64 {
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
pub(crate) fn prefix_breakpoint_hashes(body: &serde_json::Value) -> Vec<(usize, String)> {
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
pub(crate) fn content_fingerprints(body: &serde_json::Value) -> (String, String) {
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

#[cfg(test)]
mod tests;
