use crate::*;

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
pub(crate) const NEAR_RESET_5H_SECS: f64 = 3600.0;
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
pub(crate) const WARNING_UTIL_FLOOR: f64 = 0.80;
/// "rejected" = hard refusal from the API. Treat as fully exhausted — zero bucket share.
/// Distinct from hard_limited_until (which skips the account entirely) because rejected
/// status can arrive on one window while the other is still valid.
const REJECTED_UTIL_FLOOR: f64 = 1.0;
/// Maximum number of BEBO (binary exponential backoff) retry rounds for 529
/// (overloaded) responses. After exhausting all accounts, the proxy waits and
/// retries all accounts up to this many additional times. Total attempts through
/// the full account list = MAX_529_RETRIES + 1.
pub(crate) const MAX_529_RETRIES: u32 = 3;

/// Base delay for 529 BEBO retries. Doubles each round: 1s, 2s, 4s.
pub(crate) const RETRY_529_BASE_DELAY: Duration = Duration::from_secs(1);

/// Base backoff for a round that failed on transient transport errors
/// (ETIMEDOUT/reset/closed/DNS). Doubles per round: 150ms, 300ms. Kept short
/// because egress blips are sub-second; 529 overload uses the longer 1s base.
pub(crate) const TRANSIENT_BASE_DELAY: Duration = Duration::from_millis(150);

/// Transient (transport-error) rounds get a SMALLER budget than 529 overload.
/// Round 0 retries the affinity/cache-warm endpoint in place (no rotation);
/// rounds 1..=MAX_TRANSIENT_RETRIES rotate the pool. Total rounds = this + 1.
/// Capped at 1 so a genuinely-down egress fails clean (→ 503) in ~one rotation
/// instead of stalling through all MAX_529_RETRIES rounds of connect timeouts.
pub(crate) const MAX_TRANSIENT_RETRIES: u32 = 1;

/// Consecutive transport failures before an endpoint is circuit-broken out of
/// the routing candidate set. One dead-endpoint request contributes at most 2
/// failures (round-0 in-place retry + round 1), so 3 only trips across ≥2
/// separate requests — a sub-second blip (which round-gating rides out) cannot
/// open the breaker, only a persistently-dead endpoint can.
pub(crate) const TRANSPORT_FAILURE_THRESHOLD: u32 = 3;

/// How long a circuit-broken endpoint stays out of the candidate set. Bounds
/// the affinity tax: without the breaker a stateless affinity recompute snaps
/// back to the dead endpoint every request (~8s of connect timeouts each);
/// with it, at most ~1.5 requests per cooldown window pay the probe cost.
/// ponytail: constant, add a config knob if operators ever need to tune it.
pub(crate) const TRANSPORT_UNHEALTHY_COOLDOWN: Duration = Duration::from_secs(30);

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
pub(crate) const UNSUPPORTED_MODEL_MAX: usize = 256;

/// Sentinel value written to `alb:hard:{account}` when a replica has observed
/// recovery from a hard rate limit. Other replicas interpret this as an
/// instruction to proactively clear their local `hard_limited_until`, which
/// DEL alone could not do (sync_from_redis ignores missing keys). Distinct
/// from "no key" (no data) and "epoch > 0" (active hard limit).
pub(crate) const HARD_LIMIT_CLEARED_SENTINEL: u64 = 0;

/// TTL for the recovery sentinel in Redis. Long enough for every replica to
/// observe it via sync_from_redis (5s interval = 12 opportunities), short
/// enough that the key does not linger past the intended recovery window.
pub(crate) const HARD_LIMIT_SENTINEL_TTL_SECS: u64 = 60;

/// Redis hash aggregating upstream transport send-failures across all replicas,
/// keyed by kind (`timeout`/`connect`/`other`). Each replica flushes its local
/// delta accumulator into this hash via `HINCRBY` every sync tick, so the
/// dashboard/metrics endpoint reports a fleet-wide count rather than one pod's
/// local observations. A single fixed key (not per-day like budgets) → one
/// monotonic counter family that survives pod restarts.
pub(crate) const TRANSPORT_ERRORS_KEY: &str = "alb:transport_errors";

/// TTL refreshed on every flush of `TRANSPORT_ERRORS_KEY`. Chosen far larger
/// than the 5s sync interval so the key never expires while any replica is
/// alive (some pod re-`EXPIRE`s it every 5s). It only lapses once the ENTIRE
/// fleet has been down for two days, at which point resetting the counter is
/// correct — there is no live count left to preserve — and it prevents an
/// orphaned key lingering forever after a permanent teardown.
pub(crate) const TRANSPORT_ERRORS_TTL_SECS: u64 = 172_800;

/// Expiry on `alb:budget:{client}:{day}` keys, refreshed on every INCRBY.
/// 48h: a daily counter only needs to survive its own day plus enough slack
/// for stats/aggregation to read yesterday; after two days it is garbage.
pub(crate) const BUDGET_TTL_SECS: i64 = 172_800;

/// Per-command budget for the coordination client (fred's
/// `default_command_timeout`), carried over from the old ConnectionManager's
/// 2s response timeout.
pub(crate) const REDIS_COMMAND_TIMEOUT: Duration = Duration::from_secs(2);

/// How long after startup an unconnected coordination backend earns its one
/// WARN (`spawn_redis_connect_watcher`). Matches the connection budget the
/// old blocking `init()` gave the first attempt, so the log fires on the
/// same timeline operators already know — it just no longer implies
/// permanence.
pub(crate) const REDIS_STARTUP_GRACE: Duration = Duration::from_secs(5);

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
pub(crate) fn start_coordination_redis(
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
pub(crate) async fn redis_set_ex(
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
pub(crate) const MAX_REQUEST_BODY_BYTES: usize = 25 * 1024 * 1024;

/// Default wall-clock ceiling for receiving a request body (seconds). Real
/// clients push even a max-size body in seconds; 60s is generous headroom for
/// a slow relayed path while guaranteeing a stalled upload cannot pin its
/// body-memory reservation (up to `MAX_REQUEST_BODY_BYTES` when Content-Length
/// is absent) against the P1-01 budget indefinitely. Override with the
/// `body_read_timeout_secs` config key; 0 disables.
pub(crate) const DEFAULT_BODY_READ_TIMEOUT_SECS: u64 = 60;

/// Default aggregate in-flight request-body memory budget (bytes). Requests are
/// admission-controlled against this ceiling: when the sum of in-flight request
/// bodies would exceed it, new requests are load-shed with `503 + Retry-After`
/// rather than buffered. Unbounded buffering OOM-kills the pod under a burst of
/// concurrent large requests, and a dropped pod takes ALL its in-flight requests
/// with it — far worse than shedding a few. Sized as a backstop for a ~512Mi pod
/// with headroom for parse amplification + baseline RSS. Override with the
/// `max_inflight_body_mb` config key; set it to 0 to disable the limit.
pub(crate) const DEFAULT_MAX_INFLIGHT_BODY_BYTES: u64 = 128 * 1024 * 1024;

/// Prefix identifying an Anthropic OAuth token, as opposed to an
/// `sk-ant-api*` API key. This is a protocol discriminator, not a
/// credential: every auth branch that treats a token as OAuth — Bearer
/// auth, the Claude Code system-prompt injection, the client beta-flag
/// filter — keys off it, so the six of them must not be able to drift.
pub(crate) const OAUTH_TOKEN_PREFIX: &str = "sk-ant-oat";

/// Required OAuth beta flags. Both needed: oauth-2025-04-20 for OAuth auth,
/// claude-code-20250219 for Claude Code API access quota routing.
pub(crate) const OAUTH_BETA_FLAGS: &[&str] = &["oauth-2025-04-20", "claude-code-20250219"];

/// Default `anthropic-beta` flags a client may forward upstream on OAuth
/// endpoints (LAB-1191 / audit finding 5). Contains the flags the LB itself
/// depends on (`OAUTH_BETA_FLAGS`, the `context-1m*` flag the context-window
/// accounting reads) plus the flag FAMILIES Claude Code sends on every
/// request, wildcarded on their date suffix so a Claude Code auto-update
/// that bumps a date can't silently degrade the proxy's primary traffic.
/// A configured `allowed_client_betas` REPLACES this list (it does not
/// extend it) — copy these entries alongside any addition; "*" is a suffix
/// wildcard.
pub(crate) const DEFAULT_CLIENT_BETA_ALLOWLIST: &[&str] = &[
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
    // counterpart (context-management → `context_management`,
    // structured-outputs → `output_format`, extended-cache-ttl →
    // `cache_control.ttl`). Since LAB-1261 a dropped header takes its
    // TOP-LEVEL body field with it, so removing one of those from this list
    // degrades the feature instead of 400ing — but keep them: degraded is
    // still worse than working, and the NESTED `extended-cache-ttl-*` pairing
    // is not covered by that mechanism and does still 400.
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
    // Fast mode (LAB-2669): body-paired with top-level `speed`, and mapped in
    // `BETA_BODY_FIELDS` so the pair travels together.
    "fast-mode-*",
    // Auto-mode classifier (LAB-3963): `dangerous-tool-use-*` is body-paired
    // with top-level `safeguards`, and Claude Code answered that 400 by
    // denying every auto-mode tool use for the rest of the conversation.
    // `auto-mode-classifier-*` rides the classifier's own follow-up requests;
    // both are mapped in `BETA_BODY_FIELDS` to the same `safeguards` field.
    "auto-mode-classifier-*",
    "dangerous-tool-use-*",
    // Claude Code 2.1.278 per-turn family (LAB-3964). All three are
    // body-paired with fields on the `role:"system"` entry inside `messages`.
    // The orphaned-body strip only removes TOP-LEVEL fields, so it never
    // reaches these nested ones: dropping any of these flags is still a hard
    // 400 upstream, which is why they must stay on this list:
    //  - `mid-conversation-tool-changes-*` ↔ `tool_addition`/`tool_removal`
    //    content blocks. Claude Code answers the 400 by sticky-rejecting the
    //    beta for the rest of the conversation.
    //  - `per-turn-control-*` ↔ `output_config.effort`.
    //  - `timing-*` ↔ `output_config.timing`. Opt-in (CLAUDE_CODE_PER_TURN_TIMING),
    //    so not yet seen dropped — listed so the first opt-in does not 400.
    "mid-conversation-tool-changes-*",
    "per-turn-control-*",
    "timing-*",
];

/// Cardinality bound for `beta_flags_dropped` — flag names are
/// client-controlled input. Past the cap, drops count under `_other`.
pub(crate) const MAX_DROPPED_BETA_FLAGS: usize = 50;

/// Length bound for a single flag key in `beta_flags_dropped` and its warn
/// log — the value is client-controlled, and 50 multi-kilobyte keys replayed
/// into every `/metrics` scrape is the cardinality decision's spirit broken
/// by size instead of count.
pub(crate) const MAX_DROPPED_BETA_FLAG_LEN: usize = 64;

/// Strips counted (and logged) from any ONE request. A request's top-level
/// key count is client-controlled; without this the per-key cap above is
/// reachable from a single request (LAB-1261 panel finding).
pub(crate) const MAX_STRIPPED_FIELDS_PER_REQUEST: usize = 8;

/// Clamp a client-controlled string to something safe to use as a metric
/// label and a log field: `[A-Za-z0-9_.-]` only, length-bounded on a char
/// boundary. Anything else becomes `_invalid` rather than being escaped —
/// these are JSON object keys, so a legitimate one is always in that set, and
/// an illegitimate one has nothing worth preserving.
///
/// `prom_escape` already stops a crafted key forging a `/metrics` series; this
/// is the log side, where the plain-text subscriber would otherwise let an
/// embedded newline forge whole log LINES.
pub(crate) fn sanitize_metric_key(raw: &str, max_len: usize) -> String {
    let mut end = raw.len().min(max_len);
    while !raw.is_char_boundary(end) {
        end -= 1;
    }
    let clipped = &raw[..end];
    if clipped.is_empty()
        || !clipped
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '.' | '-'))
    {
        return "_invalid".to_string();
    }
    clipped.to_string()
}

/// `(route, cred)` — the label set of `anthropic_auth_failures_total`.
pub(crate) type AuthFailureKey = (&'static str, &'static str);

/// Shape of the credential a rejected request presented (LAB-4720) — a
/// fixed vocabulary, safe as a metrics label, separating the failure modes
/// the IP cannot: `none` (no key at all), `bearer` (a key in a header the
/// native surface does not read — OpenAI SDKs send `Authorization: Bearer`,
/// rejected there by design), `auth-other` (an `Authorization` scheme that
/// is not Bearer, e.g. Basic or a raw token — labelled but not
/// fingerprinted, since there is no bare credential to hash: the README's
/// recipe hashes the key alone, and a scheme-prefixed blob would never
/// reproduce it) and `x-api-key` (a key in the right header that does not
/// match; wins when both headers are present, since it is the one compared
/// first). Returns the presented bytes for `credential_fingerprint`. The
/// Bearer prefix test mirrors `authenticate`.
pub(crate) fn presented_credential(headers: &hyper::HeaderMap) -> (&'static str, Option<&[u8]>) {
    if let Some(k) = headers.get("x-api-key") {
        return ("x-api-key", Some(k.as_bytes()));
    }
    match headers.get("authorization").map(|v| v.as_bytes()) {
        Some(v) if v.len() >= 7 && v[..7].eq_ignore_ascii_case(b"bearer ") => {
            ("bearer", Some(&v[7..]))
        }
        Some(_) => ("auth-other", None),
        None => ("none", None),
    }
}

/// One-way, 12-hex fingerprint of a rejected credential (LAB-4720). Lets an
/// operator tell "one stale key, one caller" from "many callers", and match
/// a suspect by hashing its own key the same way (recipe in the README) —
/// without the log ever carrying the value or any prefix of it. blake2s-256
/// over a purpose tag plus the presented bytes, first 6 bytes as hex.
pub(crate) fn credential_fingerprint(presented: Option<&[u8]>) -> String {
    use blake2::{Blake2s256, Digest};
    let Some(bytes) = presented else {
        return "-".to_owned();
    };
    let mut h = Blake2s256::new();
    h.update(b"anthropic-lb/auth-fp\x1f");
    h.update(bytes);
    h.finalize()[..6]
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

/// User-agent of a rejected request, clipped by `truncate_label` for the log
/// line (LAB-4720). "-" when absent, empty, or not visible ASCII. No further
/// sanitising: `to_str` admits only visible ASCII plus TAB (hyper's parser
/// also passes obs-text bytes 0x80-0xFF), and the caller records it with `?`
/// so it renders quoted and escaped.
pub(crate) fn bounded_user_agent(headers: &hyper::HeaderMap) -> String {
    headers
        .get("user-agent")
        .and_then(|v| v.to_str().ok())
        .filter(|s| !s.is_empty())
        .map(truncate_label)
        .unwrap_or_else(|| "-".to_owned())
}

/// "*" suffix-wildcard match, shared by the model allowlist
/// (`Endpoint::serves_model`) and the beta-flag allowlist
/// (`beta_flag_allowed`) so the two can never drift.
pub(crate) fn suffix_wildcard_match(pattern: &str, value: &str) -> bool {
    if let Some(prefix) = pattern.strip_suffix('*') {
        value.starts_with(prefix)
    } else {
        value == pattern
    }
}

pub(crate) fn beta_flag_allowed(allowed: &[String], flag: &str) -> bool {
    allowed.iter().any(|p| suffix_wildcard_match(p, flag))
}

/// Top-level `/v1/messages` body fields that belong to the BASE (non-beta)
/// API schema — the set an upstream accepts with no `anthropic-beta` header
/// at all. Verified against the Messages and count_tokens API references,
/// 2026-09-20.
///
/// This is the list LAB-1261 trades FOR the old one. The beta-flag allow-list
/// enumerates flag FAMILIES, which Anthropic adds faster than the proxy is
/// updated, and its decay mode is a fleet-wide 400. The base schema is
/// bounded and slow-moving, and its decay mode is one feature silently off
/// on a request that is already carrying an unknown beta. Same enumeration
/// trick, pointed at the finite set.
const BASE_BODY_FIELDS: &[&str] = &[
    "model",
    "messages",
    "max_tokens",
    "system",
    "tools",
    "tool_choice",
    "thinking",
    "output_config",
    "cache_control",
    "metadata",
    "container",
    "inference_geo",
    "service_tier",
    "stop_sequences",
    "stream",
    "temperature",
    "top_k",
    "top_p",
];

/// Every beta family the allow-list can pass, mapped to the TOP-LEVEL body
/// fields it owns. An empty slice means "owns none" and is a real answer, not
/// a placeholder — it is what lets the strip tell "this family brought no body
/// field" apart from "I have never heard of this family".
///
/// **This table must stay TOTAL over `DEFAULT_CLIENT_BETA_ALLOWLIST`**, and
/// `beta_body_field_table_covers_the_allowlist` fails the build if it is not.
/// Totality is the whole mechanism (LAB-1261, Helly R finding 1): a surviving
/// flag protects its body field only if a row claims it, so a missing row
/// means the proxy DELETES a field belonging to a feature the caller was
/// entitled to use. `fallback-credit-*` is the worked example — it is on the
/// allow-list, Claude Code sends it, and its `fallback_credit_token` is a
/// billing instrument redeemable once within five minutes of a refusal.
///
/// A flag that survives while matching NO row means an allow-list this table
/// has not caught up with — `strip_orphaned_beta_body_fields` then declines to
/// strip anything, because it cannot tell that family's body fields from an
/// orphan's. Losing the degrade is the safe failure; deleting a live field is
/// not.
///
/// Only TOP-LEVEL fields belong here. `extended-cache-ttl-*` owns a nested
/// `cache_control.ttl` and `effort-*` nests in `output_config`; both are
/// listed as owning nothing, which is true of the top level and is the reason
/// dropping either still 400s upstream.
/// ponytail: top-level only. If a nested pairing ever fires in the wild, the
/// upgrade is a targeted strip inside that one known structure — NOT a
/// recursive unknown-key walk, which would eat `tools[].input_schema` and
/// `tool_use.input`, both arbitrary client JSON by design.
const BETA_BODY_FIELDS: &[(&str, &[&str])] = &[
    // The proxy's own flags — unconditionally re-added, never body-paired.
    ("oauth-2025-04-20", &[]),
    ("claude-code-20250219", &[]),
    // Body-paired families.
    ("context-management-*", &["context_management"]),
    ("structured-outputs-*", &["output_format"]),
    ("fast-mode-*", &["speed"]),
    ("fallback-credit-*", &["fallback_credit_token"]),
    // `safeguards` is claimed by BOTH halves of the auto-mode classifier pair:
    // an allow-list carrying only one of them must still keep the field.
    ("dangerous-tool-use-*", &["safeguards"]),
    ("auto-mode-classifier-*", &["safeguards"]),
    // Allow-listed and header-only, or paired below the top level.
    ("interleaved-thinking-*", &[]),
    ("fine-grained-tool-streaming-*", &[]),
    ("prompt-caching-*", &[]),
    ("context-1m*", &[]),
    ("extended-cache-ttl-*", &[]),
    ("effort-*", &[]),
    ("thinking-token-count-*", &[]),
    ("mid-conversation-system-*", &[]),
    ("advisor-tool-*", &[]),
    ("redact-thinking-*", &[]),
    ("afk-mode-*", &[]),
    // LAB-3964 per-turn family: paired with fields on the `role:"system"`
    // entry inside `messages` — base schema, so nothing here to strip or keep.
    ("mid-conversation-tool-changes-*", &[]),
    ("per-turn-control-*", &[]),
    ("timing-*", &[]),
];

/// Header/body coherence for the beta allow-list (LAB-1261).
///
/// `inject_account_auth` filters the `anthropic-beta` HEADER. Several betas
/// are paired — a header flag plus a body field that only exists when the flag
/// is declared — so stripping the header alone leaves a request the upstream
/// must reject outright (`speed: Extra inputs are not permitted`). A filter
/// meant to degrade a feature gracefully instead hard-fails every request
/// carrying it: the 2026-08-01 fleet outage, then LAB-2669 (`fast-mode`) and
/// LAB-3963 (`dangerous-tool-use`) from the field.
///
/// So when the filter drops anything, drop the orphaned half of the body too:
/// remove every top-level field that is neither base schema nor owned by a
/// flag that SURVIVED this request. The feature turns off quietly instead of
/// 400ing, and — the point of the ticket — that holds for a beta family the
/// LB has never seen, with no enumeration change.
///
/// **Declines to strip when a surviving flag is not in `BETA_BODY_FIELDS`.**
/// The keep-side of the rule is only as good as that table is total: an
/// unrecognised SURVIVING family may own a top-level field, and stripping it
/// deletes a capability the caller is entitled to (Helly R finding 1 — a
/// custom `allowed_client_betas` carrying `mcp-client-*` kept the header and
/// lost `mcp_servers`, leaving `tools[].mcp_server_name` dangling). Forgoing
/// the degrade costs a 400 the caller already gets today; deleting a live
/// field costs them a feature, or a billing instrument, silently.
///
/// **Scoped to `/v1/messages` and `/v1/messages/count_tokens` by the caller.**
/// `BASE_BODY_FIELDS` is that one schema, and `proxy_handler` is the router's
/// catch-all — `/v1/messages/batches`, `/v1/complete` and every other route
/// reach the same forward path with completely different bodies, which this
/// would otherwise delete wholesale.
///
/// `surviving_betas` is the outbound header value, read back after filtering
/// rather than derived from config: an operator running a custom
/// `allowed_client_betas` gets the right answer without a second list to
/// maintain.
///
/// Returns `None` — body forwarded untouched — when nothing was dropped (the
/// hot path: no parse, no rewrite), when a surviving flag is unrecognised,
/// when the body is not a JSON object, or when every field is accounted for.
///
/// Retained fields are spliced through as `RawValue`, i.e. their original
/// bytes, so nothing below the top level is reformatted. That is not cosmetic:
/// a `serde_json::Value` round-trip rewrites an integer too large for `u64` as
/// a float (`18446744073709551617` → `1.8446744073709552e+19`), silently
/// changing a value inside retained tool history (Helly R finding 2). Only the
/// top-level separators are re-emitted, so the cacheable prefix can still
/// shift on a body that arrived pretty-printed — accepted, since the only
/// requests reaching the rewrite are the ones answering a hard 400 today.
pub(crate) fn strip_orphaned_beta_body_fields(
    body: &bytes::Bytes,
    surviving_betas: &str,
    dropped: &[String],
) -> Option<(bytes::Bytes, Vec<String>)> {
    if dropped.is_empty() {
        return None;
    }
    let surviving: Vec<&str> = surviving_betas
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .collect();
    // Every surviving flag must be one this proxy can account for, or it may
    // own a top-level field indistinguishable from an orphan.
    let mut protected: Vec<&str> = Vec::new();
    for flag in &surviving {
        let mut known = false;
        for (pattern, fields) in BETA_BODY_FIELDS {
            if suffix_wildcard_match(pattern, flag) {
                known = true;
                protected.extend_from_slice(fields);
            }
        }
        if !known {
            debug!(
                flag = %flag,
                "beta body coherence: surviving flag is not in BETA_BODY_FIELDS, \
                 forwarding the body untouched rather than risk deleting a field \
                 it owns"
            );
            return None;
        }
    }

    let parsed: TopLevelObject = match serde_json::from_slice(body) {
        Ok(parsed) => parsed,
        Err(e) => {
            // Reached only because a flag was already dropped, so the body
            // goes upstream untouched and will likely earn the hard rejection
            // this function exists to prevent. Worth a line so the two are
            // connectable; not worth more than a line.
            //
            // `debug!`, not `warn!`: the trigger is a malformed client body,
            // which one caller can repeat at request rate. The sibling
            // `record_dropped_beta_flags` warns on first sighting only and
            // debug-logs the rest for exactly this reason, and it has the
            // per-client state to do so — this is a free function and does
            // not, so the quiet level is the honest choice.
            //
            // The error is rendered by `classify()` and a length, NOT by its
            // `Display`. serde embeds the offending value for a type error
            // (`invalid type: string "<the whole body>", expected …`), so
            // `%e` over a 25 MiB client body would copy prompt content into
            // the operator log at roughly 3x after escaping. That variant is
            // currently unreachable — a non-object body panics earlier in
            // `inject_oauth_system_prompt` — which makes `%e` safe only by
            // accident, and a landmine for whoever fixes that panic.
            debug!(
                error_kind = ?e.classify(),
                line = e.line(),
                column = e.column(),
                body_len = body.len(),
                "beta body coherence: unparseable request body, forwarding it untouched — \
                 the orphaned-field strip cannot run"
            );
            return None;
        }
    };
    let mut removed: Vec<String> = Vec::new();
    let kept: Vec<&(String, Box<serde_json::value::RawValue>)> = parsed
        .0
        .iter()
        .filter(|(key, _)| {
            if BASE_BODY_FIELDS.contains(&key.as_str()) || protected.contains(&key.as_str()) {
                return true;
            }
            removed.push(key.clone());
            false
        })
        .collect();
    if removed.is_empty() {
        return None;
    }
    let mut out = String::from("{");
    for (i, (key, value)) in kept.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        // The key is re-escaped rather than spliced: `RawValue` covers values
        // only, and a key carrying a quote or a control character must not be
        // able to break out of the string it is written into.
        out.push_str(&serde_json::to_string(key).ok()?);
        out.push(':');
        out.push_str(value.get());
    }
    out.push('}');
    Some((bytes::Bytes::from(out), removed))
}

/// A JSON object whose VALUES are kept as their original bytes.
///
/// `serde_json::Map<String, Value>` cannot express this, and pulling in an
/// ordered map crate to hold `RawValue` would be a dependency for thirty
/// lines. Order is preserved because the entries are simply collected in the
/// order the parser yields them.
struct TopLevelObject(Vec<(String, Box<serde_json::value::RawValue>)>);

impl<'de> serde::Deserialize<'de> for TopLevelObject {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        struct Entries;
        impl<'de> serde::de::Visitor<'de> for Entries {
            type Value = TopLevelObject;
            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str("a JSON object")
            }
            fn visit_map<M: serde::de::MapAccess<'de>>(
                self,
                mut map: M,
            ) -> Result<TopLevelObject, M::Error> {
                let mut out = Vec::new();
                while let Some(entry) =
                    map.next_entry::<String, Box<serde_json::value::RawValue>>()?
                {
                    out.push(entry);
                }
                Ok(TopLevelObject(out))
            }
        }
        d.deserialize_map(Entries)
    }
}

/// Legacy dynamic-capacity override threshold. If the affinity-picked account's
/// `affinity_headroom` is below 50% of the alternative's, stickiness is broken.
pub(crate) const LEGACY_AFFINITY_OVERRIDE_RATIO: f64 = 0.5;

/// Sticky-weighted override threshold on `affinity_headroom`. Lower than the
/// legacy algorithm to preserve cache locality and only break stickiness for
/// egregious disparities.
pub(crate) const STICKY_WEIGHTED_OVERRIDE_RATIO: f64 = 0.25;

/// Which window bound the sticky account when an affinity override fired.
/// Label value of `anthropic_affinity_migrations_total{reason}`; also indexes
/// `AppState::affinity_migrations`.
#[derive(Clone, Copy)]
pub(crate) enum AffinityBind {
    /// The effective gate is tighter, bound by raw (time-adjusted) 5h
    /// utilisation or overage — the pool is the bottleneck (operator-actionable).
    Loaded = 0,
    /// Unused weekly quota is tighter — one account's week is nearly spent.
    Spent = 1,
    /// The gate is tighter, but set by an Anthropic status floor
    /// (`status_to_floor`) that exceeds raw utilisation — the LB is moving the
    /// session off an account Anthropic flagged, onto fresh capacity. Routine,
    /// not a pool-health signal: logged at INFO, not WARN (LAB-3295).
    Floored = 2,
}

impl AffinityBind {
    pub(crate) const ALL: [Self; 3] = [Self::Loaded, Self::Spent, Self::Floored];

    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Loaded => "loaded",
            Self::Spent => "spent",
            Self::Floored => "floored",
        }
    }
}

/// Why the proxy gave up on the whole pool and answered the caller itself.
/// Label value of `anthropic_pool_exhausted_total{kind}`; also indexes
/// `AppState::pool_exhausted`. Closed by the type system, so this counter's
/// label set cannot grow.
#[derive(Clone, Copy)]
pub(crate) enum PoolExhaustion {
    /// Every endpoint was rate-limited or gated (or a round saw a 529) — the
    /// `429 exhausted all endpoints` arm. Recovery is minutes-to-hours.
    RateLimited = 0,
    /// Every endpoint failed in transport with no 529 — the retryable
    /// `503 + Retry-After` arm.
    Transient = 1,
}

impl PoolExhaustion {
    pub(crate) const ALL: [Self; 2] = [Self::RateLimited, Self::Transient];

    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::RateLimited => "rate_limited",
            Self::Transient => "transient",
        }
    }
}

/// Capacity a candidate has left to keep a sticky session, and which window
/// binds it. Both terms are time-free. Deliberately NOT `weight`: weight =
/// waste_risk × headroom, and waste_risk's denominator is time-to-weekly-reset,
/// so an account that reset yesterday read ~8x "worse" than one resetting in
/// hours at identical load — which migrated 25% of prod sessions off the
/// FRESHEST accounts in the pool (GH#156). waste_risk stays in the bucket
/// weights, where burning expiring quota first is the point.
pub(crate) fn affinity_headroom(c: &RoutingCandidate) -> (f64, AffinityBind) {
    let loaded = (1.0 - c.gate).max(0.0);
    if c.unused_7d < loaded {
        (c.unused_7d, AffinityBind::Spent)
    } else if c.gate_floor_bound {
        // Same headroom value as `Loaded` — routing decision unchanged — but a
        // distinct reason so a status-floor-forced migration is not mistaken
        // for pool exhaustion (LAB-3295). Only when the gate (not weekly quota)
        // binds; a genuinely spent week is `Spent` above and stays WARN.
        (loaded, AffinityBind::Floored)
    } else {
        (loaded, AffinityBind::Loaded)
    }
}

/// Extract version string from User-Agent header.
/// "claude-cli/2.1.68 (external, cli)" → "2.1.68"
/// "anthropic-sdk/1.0.0" → "1.0.0"
/// Returns None for unrecognizable formats.
pub(crate) fn extract_client_version(ua: &str) -> Option<&str> {
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
pub(crate) fn model_family(model: &str) -> &str {
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

/// Claude ≥ 4.7 hard-rejects sampling `temperature` — the API returns a
/// non-retryable `invalid_request_error` ("`temperature` is deprecated for
/// this model") for any value other than the default 1 (LAB-798, verified
/// against the live API 2026-07-25). The version is the numeric segments
/// after the family name: "claude-sonnet-4-5-20250929" → 4.5,
/// "claude-fable-5[1m]" → 5.0. Old-style ids ("claude-3-5-sonnet-…") put the
/// version before the family and parse as 0.0 → accepts, which is correct
/// (all 3.x models take temperature). Unknown families also parse as
/// accepting: forward unchanged and let the upstream decide.
pub(crate) fn model_rejects_temperature(model: &str) -> bool {
    let family = model_family(model);
    if family.is_empty() {
        return false;
    }
    let Some((_, rest)) = model.split_once(family) else {
        return false;
    };
    let mut nums = rest.split('-').filter(|s| !s.is_empty()).map_while(|seg| {
        let end = seg.find(|c: char| !c.is_ascii_digit()).unwrap_or(seg.len());
        // 4+ digit runs are date stamps ("-20250929"), not version parts
        if end == 0 || end >= 4 {
            None
        } else {
            seg[..end].parse::<u32>().ok()
        }
    });
    let major = nums.next().unwrap_or(0);
    let minor = nums.next().unwrap_or(0);
    (major, minor) >= (4, 7)
}

/// Decide whether a request's `temperature` must be dropped before
/// forwarding, and log the operator-visible warn when it is. Shared by every
/// path that builds an upstream body (LAB-798): the OpenAI→Anthropic shim,
/// the Anthropic→OpenAI fallback translator, and the raw OpenAI passthrough —
/// a `protocol = "openai"` endpoint can front Claude too, so all three must
/// agree.
///
/// Only confirmed non-default numerics are dropped: the default (1) is still
/// accepted upstream, and most OpenAI SDKs send it unprompted — warning on
/// every default-sending request would be spam. Non-numeric junk ("0.7",
/// null) forwards so it earns the same upstream type error it gets on ≤ 4.6
/// models. The per-drop warn is deliberate: the client asked for sampling
/// behavior it will not get, and that must stay visible to operators.
pub(crate) fn drops_deprecated_temperature(model: &str, value: &serde_json::Value) -> bool {
    if !model_rejects_temperature(model) || !matches!(value.as_f64(), Some(t) if t != 1.0) {
        return false;
    }
    warn!(
        model = %truncate_label(model),
        temperature = %value,
        "dropping `temperature`: deprecated and hard-rejected by this model"
    );
    true
}

/// Translate an OpenAI `reasoning_effort` into Anthropic's
/// `output_config.effort`, so OpenAI clients can set thinking effort. Only
/// level names the Anthropic API defines pass; per-model support is not
/// checked here — a model that lacks the level answers with an explicit 400,
/// the same way OpenAI rejects `reasoning_effort` on non-reasoning models.
/// `minimal`/`none`, unknown strings and non-strings would 400 on every model,
/// so they are dropped with a warn that makes the loss visible to operators.
/// `null` is treated as absent: clients that serialise unset fields must not
/// warn on every request. No `thinking` block is synthesised: adaptive-thinking
/// models decide when to think on their own.
pub(crate) fn translate_reasoning_effort(value: &serde_json::Value) -> Option<&str> {
    const EFFORTS: &[&str] = &["low", "medium", "high", "xhigh", "max"];
    if value.is_null() {
        return None;
    }
    let effort = value.as_str().filter(|e| EFFORTS.contains(e));
    if effort.is_none() {
        warn!(
            reasoning_effort = %truncate_label(&value.to_string()),
            "dropping `reasoning_effort`: no Anthropic effort equivalent"
        );
    }
    effort
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
pub(crate) const FABLE_BAND_CLAIM: &str = "seven_day_fable";

/// Claims that participate in the model-agnostic worst case (emergency brake,
/// stats with no model context). Allowlist, NOT denylist: only the general
/// weekly pool and the per-family sub-budgets that gate whole traffic classes
/// qualify. Carve-outs (the Fable band) and any future unknown keys are
/// excluded — an unknown carve-out silently joining the brake input could
/// freeze ALL traffic while every regular budget is healthy. Under-braking on
/// a genuinely new model family is the safer failure mode: per-model routing
/// gates still protect it, the brake is only a last-resort backstop.
pub(crate) fn claim_gates_all_traffic(key: &str) -> bool {
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
pub(crate) fn derive_flat_7d_fields(
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

pub(crate) fn resolve_7d_claim<'a>(
    info: &'a RateLimitInfo,
    model: &str,
) -> Option<&'a ClaimWindowData> {
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

pub(crate) const TOTAL_7D_SECS: f64 = 604800.0;
const WASTE_RISK_MIN_REMAINING: u64 = 60;

/// Compute waste risk: how much quota will be wasted if we don't route here.
/// Higher = more urgency to use this account's remaining 7d budget.
/// Returns 0.0 when reset data is unavailable or stale.
pub(crate) fn waste_risk(util: Option<f64>, reset_epoch: Option<u64>, now_epoch: u64) -> f64 {
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
pub(crate) fn status_to_ordinal(status: Option<&str>) -> f64 {
    match status {
        Some("rejected") => 3.0,
        Some("throttled") => 2.0,
        Some("allowed_warning") => 1.0,
        Some("allowed") | None => 0.0,
        // Unknown maps to the warning tier, silently: this is a read-only
        // exposition helper called once per account per claim per scrape, and
        // `status_to_floor` already WARNs on the same unknown string where it
        // actually changes a routing decision.
        Some(_) => 1.0,
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
pub(crate) fn effective_utilization(
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
pub(crate) struct RoutingWeight {
    pub(crate) gate_5h: f64,
    pub(crate) gate_7d: f64,
    gate: f64,
    pub(crate) wr: f64,
    /// See `RoutingCandidate::unused_7d`.
    unused_7d: f64,
    pub(crate) weight: f64,
    /// See `RoutingCandidate::gate_floor_bound`.
    gate_floor_bound: bool,
    pub(crate) source: &'static str,
    /// Account is serving via paid overage — caller demotes its priority tier.
    pub(crate) overage_active: bool,
}

/// 5h gate: time-adjusted 5h utilization with status floors, falling back to
/// raw unified, legacy token ratio, or 0.5 (unknown). A fixed 0.5 while the
/// account's data predates its last hard limit.
///
/// Shared by `compute_routing_weight` and `metrics_gate_weight` so the gate
/// published on `/metrics` cannot drift from the one the router uses (LAB-4441).
fn gate_5h(info: &RateLimitInfo, now_epoch: u64, stale_after_hard_limit: bool) -> f64 {
    if stale_after_hard_limit {
        return 0.5;
    }
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
}

/// Overage gate: `Some` iff the account is serving via paid overage, in which
/// case this gate REPLACES the exhausted 5h/7d gates — the overage window
/// governs, and a rejected subscription claim does not skip the account.
/// `None` while overage is off or the data predates the last hard limit.
///
/// Shared by `compute_routing_weight` and `metrics_gate_weight` (LAB-4441:
/// the metrics path lacked this branch and published gate 1.0 for accounts
/// the router was actively serving through).
fn overage_gate(info: &RateLimitInfo, now_epoch: u64, stale_after_hard_limit: bool) -> Option<f64> {
    if !info.overage_in_use || stale_after_hard_limit {
        return None;
    }
    Some(
        time_adjusted_utilization(
            info.overage_utilization,
            info.overage_reset,
            info.overage_status.as_deref(),
            NEAR_RESET_OVERAGE_SECS,
            now_epoch,
        )
        .unwrap_or(0.0),
    )
}

pub(crate) fn compute_routing_weight(
    info: &RateLimitInfo,
    model: &str,
    now_epoch: u64,
    stale_after_hard_limit: bool,
) -> Option<RoutingWeight> {
    let gate_overage = overage_gate(info, now_epoch, stale_after_hard_limit);
    let overage_active = gate_overage.is_some();

    let gate_5h = gate_5h(info, now_epoch, stale_after_hard_limit);

    // Whether a status floor (`status_to_floor` inside `time_adjusted_utilization`)
    // raised `gate_5h` above its raw time-adjusted utilisation. Compared against
    // the ALREADY-computed `gate_5h` — not a second status-bearing call — so the
    // two can't drift AND `status_to_floor` is evaluated only once: a duplicate
    // status-bearing call would re-emit its unknown-status WARN, the very noise
    // this ticket removes (CodeRabbit on #175). The floor-free call shares
    // `gate_5h`'s inputs, so it is `None` exactly on `gate_5h`'s fallback path,
    // where no floor applies → not floor-bound. False under staleness (gate is a
    // fixed 0.5). Feeds `AffinityBind::Floored` (LAB-3295).
    let gate_5h_floor_bound = !stale_after_hard_limit
        && time_adjusted_utilization(
            info.utilization_5h,
            info.reset_5h,
            None,
            NEAR_RESET_5H_SECS,
            now_epoch,
        )
        .is_some_and(|unfloored| gate_5h > unfloored);

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
    let (gate, wr, source) = match gate_overage {
        Some(g) => (g, 0.0, "overage"),
        None => (gate_5h.max(gate_7d), wr_7d, source_7d),
    };

    // Weekly headroom for the affinity override (`affinity_headroom`): 1.0 when
    // unknown or superseded by overage, else the tighter of the primary claim
    // and (Fable) the pool it draws from. Reuses the gate's near-reset ramp so
    // quota expiring within NEAR_RESET_7D_SECS reads as headroom, not "spent":
    // the bucket weights pull sessions ONTO expiring accounts and the override
    // must not push them straight back off. Status floors are the gate's job
    // (`None`). Deliberately not hedged under stale_after_hard_limit — a 429
    // says nothing about how much of the week is spent.
    // ponytail: 6h ramp; an account with ≤25% of its week left and 6h–~30h to
    // reset still reads "spent" while the buckets favour it. Widen the ramp if
    // that WARN band shows up in prod.
    let unused_of = |c: &ClaimWindowData| {
        let util =
            time_adjusted_utilization(c.utilization, c.reset, None, NEAR_RESET_7D_SECS, now_epoch)
                .unwrap_or(0.0);
        (1.0 - util).max(0.0)
    };
    let unused_7d = if overage_active {
        1.0
    } else {
        primary_7d.map_or(1.0, |c| {
            pool_cap_7d
                .iter()
                .fold(unused_of(c), |u, p| u.min(unused_of(p)))
        })
    };

    // Floor-bound: the effective `gate` is set by an Anthropic status floor that
    // exceeds raw utilisation, not by raw load or overage. Routine — Anthropic
    // flagged the account and the LB is migrating the session off it — so the
    // affinity override logs it at INFO, not a pool-health WARN (LAB-3295).
    // The 7d gate is a pure status floor (`gate_of` forces util=0), so its
    // winning over gate_5h (which is >= 0) means a non-zero floor bound the gate;
    // the 5h case is `gate_5h_floor_bound`. Overage never floors (its gate comes
    // from the overage window); staleness is already excluded by both terms.
    //
    // ponytail: `Floored` covers EVERY status floor, not just `allowed_warning`
    // (0.80) — `throttled` (0.98) and `rejected` (1.0) too. That is intended
    // (AC-1: "status_to_floor is non-zero and set the gate"): a `rejected`
    // account has weight 0 so it is never the sticky pick and never reaches
    // here, and a lone `throttled` account with a healthy alternative is the
    // same single-account-flagged case, not pool exhaustion. If throttled/
    // rejected migrations ever need to stay loud, split on the floor tier here.
    let gate_floor_bound = !overage_active && (gate_7d > gate_5h || gate_5h_floor_bound);

    let headroom = (1.0 - gate).max(0.01);
    let weight = if wr > 0.0 { wr * headroom } else { headroom };
    let weight = if gate >= 1.0 { 0.0 } else { weight };

    Some(RoutingWeight {
        gate_5h,
        gate_7d,
        gate,
        wr,
        unused_7d,
        gate_floor_bound,
        weight,
        source,
        overage_active,
    })
}

/// Classification of a remote `alb:hard:{account}` value read from Redis.
/// Pure function output — unit-testable without a Redis client.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum HardLimitSync {
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
pub(crate) fn classify_hard_limit_sync(
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
    pub(crate) fn endpoint_name(&self, ep: EndpointIdx) -> &str {
        &self.endpoints[ep].name
    }

    /// Record that `endpoint_idx` cannot serve `model` — the upstream itself
    /// said so (LAB-941). Routing skips the pair for UNSUPPORTED_MODEL_TTL.
    pub(crate) fn note_model_unsupported(
        &self,
        endpoint_name: &str,
        endpoint_idx: usize,
        model: &str,
    ) {
        if model.is_empty() {
            return;
        }
        let now = Instant::now();
        let mut map = self.lock_unsupported_models();
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
        self.lock_unsupported_models()
            .iter()
            .filter(|((_, m), expiry)| m == model && **expiry > now)
            .map(|((idx, _), _)| *idx)
            .collect()
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
    pub(crate) fn model_unsupported_everywhere(&self, model: &str) -> bool {
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

    pub(crate) async fn routing_candidates(
        &self,
        model: &str,
        skip: &[EndpointIdx],
    ) -> Vec<RoutingCandidate> {
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
                        if let Some(until) = info.transport.open_until {
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
                        unused_7d: 1.0,
                        weight: 1.0,
                        gate_floor_bound: false,
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
                    if let Some(until) = info.transport.open_until {
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
                        unused_7d: rw.unused_7d,
                        weight: rw.weight,
                        gate_floor_bound: rw.gate_floor_bound,
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
    pub(crate) async fn refresh_metrics_weights(&self) {
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

    let gate_5h = gate_5h(info, now_epoch, stale_after_hard_limit);

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

    let (gate_7d, wr_7d) = if let Some(claim) = representative {
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

    // Overage supersedes the subscription gates exactly as in
    // `compute_routing_weight`: the overage window governs, waste_risk is moot.
    let (gate, wr) = match overage_gate(info, now_epoch, stale_after_hard_limit) {
        Some(g) => (g, 0.0),
        None => (gate_5h.max(gate_7d), wr_7d),
    };
    let headroom = (1.0 - gate).max(0.01);
    let weight = if wr > 0.0 { wr * headroom } else { headroom };
    let weight = if gate >= 1.0 { 0.0 } else { weight };

    Some((gate, weight))
}

#[cfg(test)]
mod tests;
