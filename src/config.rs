use crate::*;

// ── Config ──────────────────────────────────────────────────────────

#[derive(Deserialize, Clone)]
pub(crate) struct Config {
    pub(crate) listen: String,
    #[allow(dead_code)]
    pub(crate) strategy: Option<String>,
    pub(crate) rate_limit_cooldown_secs: Option<u64>,
    /// Seconds between utilization probes per account (0 = disabled). Default: 300 (5 min)
    pub(crate) probe_interval_secs: Option<u64>,
    /// LEGACY single shared secret, sent as x-api-key. None = open.
    /// Superseded by `[[clients]]`; configuring both is rejected at startup
    /// (`reject_legacy_config_keys`) rather than silently precedence-ordered.
    pub(crate) proxy_key: Option<String>,
    /// Per-client credentials (LAB-1083). When non-empty, EVERY authenticated
    /// entry point requires a credential matching one of these keys, and the
    /// matched entry's `name` becomes the request's `client_id` — the
    /// `x-client-id` header and the `client_names` IP map are then ignored
    /// entirely. That is the whole point: budgets, utilization ceilings,
    /// operator bypass, the model allow-list and the response-cache tenant all
    /// key on `client_id`, so making it unforgeable makes all five unforgeable
    /// at once.
    #[serde(default)]
    pub(crate) clients: Vec<ClientConfig>,
    /// Source IP allowlist. Supports individual IPs and CIDR ranges. None/empty = allow all.
    pub(crate) allowed_ips: Option<Vec<String>>,
    /// LAB-1192: the ONE escape hatch from default-deny authentication.
    /// Startup FAILS when neither `[[clients]]` nor `proxy_key` is configured
    /// unless this is explicitly true. Trusted-network-only (e.g. behind the
    /// lab NetworkPolicy) — never on a public ingress. Mutually exclusive
    /// with configured credentials, so it cannot mask a half-applied
    /// migration.
    pub(crate) allow_unauthenticated: Option<bool>,
    /// LAB-1192: IPs/CIDRs of load balancers whose `x-forwarded-for` is
    /// trusted. When the TCP peer is inside this list, the client IP becomes
    /// the rightmost `x-forwarded-for` entry that is NOT itself in the list;
    /// otherwise the peer address is used and the header is ignored entirely.
    /// Empty/absent = header never consulted (direct-connection behaviour).
    pub(crate) trusted_proxies: Option<Vec<String>>,
    /// LAB-1193 amendment: failed-auth throttle — failures per client IP
    /// inside the window before further INVALID credentials from that IP get
    /// 429; valid credentials always pass. Supersedes LAB-1192 AC-11's
    /// pre-comparison ordering after the 2026-08-24 shared-IP outage.
    /// 0 disables. Default: 10.
    pub(crate) auth_failure_limit: Option<u32>,
    /// LAB-1192: failed-auth throttle window in seconds. Default: 300.
    pub(crate) auth_failure_window_secs: Option<u64>,
    /// Unified routing endpoints. Each entry is either Anthropic-native or
    /// OpenAI-compatible, distinguished by `protocol`. The sole endpoint pool —
    /// the legacy [[accounts]] / [[upstreams]] / fallback_upstream keys are
    /// rejected at startup by `reject_legacy_config_keys`.
    #[serde(default)]
    pub(crate) endpoints: Vec<EndpointConfig>,
    /// IP-to-client-name mapping. Falls back to x-client-id header, then "-".
    #[serde(default)]
    pub(crate) client_names: HashMap<String, String>,
    /// Auto-inject prompt cache breakpoints for requests without them. Default: true.
    pub(crate) auto_cache: Option<bool>,
    /// Path for JSONL shadow log of request metadata. None = disabled.
    pub(crate) shadow_log: Option<String>,
    /// Per-client daily token budgets: client_id → max tokens per day. Uncapped if absent.
    #[serde(default)]
    pub(crate) client_budgets: HashMap<String, u64>,
    /// Per-client utilization limits: client_id → max utilization (0.0-1.0).
    /// Client gets 429 when ALL model-compatible accounts exceed their limit.
    #[serde(default)]
    pub(crate) client_utilization_limits: HashMap<String, f64>,
    /// Client IDs that bypass all budget/ceiling/emergency checks.
    /// NOTE: operator identity is trust-based (all users are trusted). The x-client-id
    /// header is not verified against client_names IP mapping.
    #[serde(default)]
    pub(crate) operators: Vec<String>,
    /// Client IDs granted the admin READ surfaces (`/_stats`, `/metrics`) and
    /// nothing else — every `/v1` surface answers 403 (LAB-4395).
    ///
    /// `operators` is one bit meaning two things: "may read the dashboards"
    /// and "bypasses every request policy". A monitoring scrape or a Grafana
    /// datasource needs only the first, but granting it hands out unbudgeted
    /// spend authority over the whole pool — and any per-client budget written
    /// for that name is dead config, because `pre_request_gate` returns before
    /// reading it. This splits the read bit out.
    ///
    /// Only meaningful under `[[clients]]`, where identity is credential-derived:
    /// `validate_clients` rejects the key on the legacy path rather than let it
    /// pretend to scope a shared secret it cannot scope.
    #[serde(default)]
    pub(crate) admin_readers: Vec<String>,
    /// Enable the emergency brake. Default: true.
    pub(crate) emergency_brake: Option<bool>,
    /// Emergency brake threshold (0.0-1.0). When ALL accounts exceed this,
    /// non-operator traffic is blocked. Default: 0.88.
    pub(crate) emergency_threshold: Option<f64>,
    /// Utilization soft ceiling (0.0–1.0). Accounts above this are excluded from routing
    /// unless ALL accounts exceed it. Breaks client affinity stickiness on overloaded accounts.
    /// Default: 0.90.
    pub(crate) soft_limit: Option<f64>,
    /// Redis/Valkey URL for distributed state. When set, budget enforcement and hard-limit
    /// propagation use Redis for cross-replica coordination. None = local-only (single instance).
    pub(crate) redis_url: Option<String>,
    /// Path for debug log file. When set, writes debug-level logs to this file while
    /// keeping info-level on stderr. For investigating cache/auth behavior.
    pub(crate) debug_log: Option<String>,
    /// Priority penalty added to an account's effective priority tier while it is
    /// serving via Anthropic overage (paid extra usage). Keeps free subscription
    /// capacity preferred over paid overage. Default: 10.
    pub(crate) overage_penalty: Option<u32>,
    /// Aggregate in-flight request-body memory budget, in MiB. New requests are
    /// load-shed with `503 + Retry-After` once the sum of in-flight request
    /// bodies would exceed this, bounding peak memory under bursts of concurrent
    /// large requests. Default: 128. Set to 0 to disable (unbounded — old behavior).
    pub(crate) max_inflight_body_mb: Option<u64>,
    /// Wall-clock ceiling (seconds) for receiving a request body. A slow or
    /// stalled client is shed with `408` when the body has not fully arrived
    /// within this window, releasing its body-memory reservation — otherwise a
    /// handful of hung uploads could pin the `max_inflight_body_mb` budget
    /// indefinitely. Default: 60. Set to 0 to disable.
    pub(crate) body_read_timeout_secs: Option<u64>,
    /// Max entries in the live session registry (context-window visibility on
    /// `/_stats`). Registry keys are affinity routing keys, so fan-out agents
    /// each get an entry. Default: 1000. Set to 0 to disable the registry.
    pub(crate) session_registry_max: Option<usize>,
    /// Seconds after a session's last request before its registry entry is
    /// evicted. Default: 1800 (30 min).
    pub(crate) session_registry_ttl_secs: Option<u64>,
    /// Opt-in encrypted response cache on `/v1/messages` (LAB-933).
    /// Absent, or present with an empty `clients` list = feature entirely inert.
    pub(crate) response_cache: Option<ResponseCacheConfig>,
    /// Reflect upstream `anthropic-ratelimit-*` response headers to callers.
    /// They reveal the pooled capacity of every account behind the proxy, so
    /// this is trusted-network-only. Default: false (LAB-1191).
    pub(crate) expose_upstream_ratelimit_headers: Option<bool>,
    /// Relay the caller-identity headers (`CLIENT_IDENTITY_HEADERS`) to the
    /// upstream. Default: false = stripped once the proxy has used them (GH #168).
    pub(crate) forward_caller_identity: Option<bool>,
    /// Client-supplied `anthropic-beta` flags forwarded upstream on OAuth
    /// endpoints ("*" suffix wildcards, like `endpoints[].models`). Absent =
    /// built-in default (`DEFAULT_CLIENT_BETA_ALLOWLIST`); a configured
    /// value REPLACES that default (it does not extend it), so include the
    /// defaults alongside any addition. Flags not on the list are dropped,
    /// logged, and counted (`anthropic_beta_flag_dropped_total`) — otherwise
    /// any caller could activate arbitrary beta features against the
    /// operator's accounts (LAB-1191).
    pub(crate) allowed_client_betas: Option<Vec<String>>,
    /// LAB-3878: `[guard]` — the Tier 1 detector (`[guard.detectors.<name>]`).
    /// Per-client policy stays on `[[clients]].guard`.
    #[cfg(feature = "guard")]
    #[serde(default)]
    pub(crate) guard: guard::GuardConfig,
}

/// `[[clients]]` — one authenticated caller. The `key` IS the identity: it is
/// what the caller presents, and `name` is what the proxy attributes the
/// request to. No `x-client-id` header can override it.
#[derive(Deserialize, Clone)]
pub(crate) struct ClientConfig {
    /// Identity this credential resolves to. Becomes `client_id`, so it is what
    /// `client_budgets`, `client_utilization_limits`, `operators` and
    /// `[response_cache].clients` must name.
    pub(crate) name: String,
    /// Shared secret this client presents as `x-api-key` (or, on the
    /// OpenAI-compat surface, `Authorization: Bearer`). Compared in constant
    /// time against the whole table.
    pub(crate) key: String,
    /// Models this client may request. Empty = all models, mirroring
    /// `EndpointConfig.models` semantics. Same exact + `*`-suffix matcher.
    #[serde(default)]
    pub(crate) models: Vec<String>,
    /// Endpoint names this client is pinned to (LAB-2636 / #151). While at
    /// least one is healthy (serves the model, not hard-limited, not
    /// transport-unhealthy, under `soft_limit`, not serving via paid
    /// overage), routing is restricted to this set; otherwise the request
    /// spills to the full pool. Empty = no pin, routing identical to a
    /// client without the field.
    #[serde(default)]
    pub(crate) preferred_endpoints: Vec<String>,
    /// LAB-3877: Tier 0 content-guard policy for this client —
    /// `"off" | "annotate" | "block"`, default `annotate` (shadow mode). The
    /// operator-bypass client is always `off` regardless of this value.
    #[cfg(feature = "guard")]
    #[serde(default)]
    pub(crate) guard: guard::GuardPolicy,
}

/// Hand-written, NOT derived: a derived `Debug` would print `key` verbatim into
/// any log line, panic message or test assertion that formats this struct —
/// which is precisely how credentials end up in a log aggregator. Same posture
/// as `debug_header_value`'s redaction of sensitive headers.
impl std::fmt::Debug for ClientConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut ds = f.debug_struct("ClientConfig");
        ds.field("name", &self.name)
            .field("key", &"<redacted>")
            .field("models", &self.models)
            .field("preferred_endpoints", &self.preferred_endpoints);
        // guard policy is not a secret; include it so the redacted Debug shows
        // the full (non-key) field set.
        #[cfg(feature = "guard")]
        ds.field("guard", &self.guard);
        ds.finish()
    }
}

/// `[response_cache]` — opt-in, client-side-encrypted response cache on
/// non-streaming `/v1/messages` (LAB-933). Cached bodies contain prompt
/// content, so SecureCache (AES-256-GCM, encrypted before any layer — L1
/// included) is mandatory: there is no plaintext-storage configuration.
#[derive(Deserialize, Clone)]
pub(crate) struct ResponseCacheConfig {
    /// Client IDs allowed to read/write the cache. Empty = inert (AC2).
    /// May not contain "-" (the unknown-client sentinel) — that would opt in
    /// every unidentified caller.
    #[serde(default)]
    pub(crate) clients: Vec<String>,
    /// Cache backend: "cachekitio" (SaaS, rides the existing reqwest/rustls
    /// stack) or "redis" (local Redis/Valkey via cachekit's fred client).
    pub(crate) backend: String,
    /// Hex-encoded master key for client-side encryption; must decode to at
    /// least 32 bytes. Per-client keys are derived from it via HKDF-SHA256
    /// with the client_id as tenant, so clients are cryptographically
    /// isolated from each other (AC7), not just key-string separated.
    pub(crate) master_key: String,
    /// Entry TTL in seconds. Default: 3600 (1 h).
    pub(crate) ttl_secs: Option<u64>,
    /// Per-operation budget in ms before the cache fails open and the
    /// request proceeds upstream (AC10). Default: 250.
    pub(crate) op_timeout_ms: Option<u64>,
    /// Connection URL for backend = "redis" (redis:// or rediss://).
    pub(crate) redis_url: Option<String>,
    /// API key for backend = "cachekitio".
    pub(crate) api_key: Option<String>,
    /// API URL override for backend = "cachekitio". Default:
    /// https://api.cachekit.io. This knob was cut in the LAB-933 review as
    /// YAGNI and reinstated for a named need: the first rollout targets the
    /// cachekit DEV environment, which lives on a different hostname.
    /// Operator config, same trust as endpoints[].base_url; cachekit's own
    /// validator still enforces HTTPS and rejects private/loopback IPs
    /// (SSRF guard — pinned by a test).
    pub(crate) api_url: Option<String>,
}

#[derive(Deserialize, Clone)]
pub(crate) struct EndpointConfig {
    pub(crate) name: String,
    /// Wire format. Default: anthropic (sends to api.anthropic.com via x-api-key).
    #[serde(default)]
    pub(crate) protocol: Protocol,
    /// Override base URL. For protocol = anthropic, defaults to https://api.anthropic.com.
    /// For protocol = openai, this field is required (validated at startup).
    pub(crate) base_url: Option<String>,
    /// Auth credential. "passthrough" (anthropic only) forwards caller's auth headers.
    pub(crate) token: String,
    /// Model allowlist (supports "*" suffix wildcards). Empty = all models.
    #[serde(default)]
    pub(crate) models: Vec<String>,
    /// Priority tier (0 = highest). Lower tiers tried first.
    #[serde(default)]
    pub(crate) priority: u32,
    /// Whether this account's plan includes Fable usage. Max plans include
    /// Fable up to 50% of the weekly limit; Pro / standard Team plans bill
    /// Fable as paid credits from the first token. Set to false for such
    /// accounts: Fable requests then treat the endpoint as paid capacity,
    /// demoting its priority by `overage_penalty` so included (Max) capacity
    /// drains first. Non-Fable routing is unaffected. Default: true.
    pub(crate) fable_included: Option<bool>,
    /// Opt-in: allow a `protocol = "anthropic"` endpoint whose `base_url`
    /// host is not `api.anthropic.com` (e.g. a staging mirror). Without it,
    /// startup fails — a typo'd or tampered base_url would otherwise send the
    /// account's OAuth/API token to an arbitrary HTTPS host. Default: false.
    pub(crate) allow_nonstandard_host: Option<bool>,
}

/// Wire format on config / state: "anthropic" | "openai".
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub(crate) enum Protocol {
    #[default]
    Anthropic,
    OpenAI,
}

#[derive(Copy, Clone, Debug, Default, Eq, PartialEq)]
pub(crate) enum RoutingStrategy {
    #[default]
    DynamicCapacityV1,
    StickyWeightedV2,
}

impl RoutingStrategy {
    pub(crate) fn parse(raw: Option<&str>) -> Result<Self, String> {
        match raw.unwrap_or("dynamic-capacity-v1") {
            "dynamic-capacity" | "dynamic-capacity-v1" => Ok(Self::DynamicCapacityV1),
            "sticky-weighted" | "sticky-weighted-v2" => Ok(Self::StickyWeightedV2),
            other => Err(format!(
                "unknown strategy '{other}' (expected dynamic-capacity-v1 or sticky-weighted-v2)"
            )),
        }
    }

    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::DynamicCapacityV1 => "dynamic-capacity-v1",
            Self::StickyWeightedV2 => "sticky-weighted-v2",
        }
    }
}

#[cfg(test)]
mod tests;
