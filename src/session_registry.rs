use crate::*;

// ── Session registry: context-window visibility (LAB-916) ──────────
//
// The affinity "pin" is recomputed per request and never stored, so before
// this section there was no way to see which live sessions are close to
// their model's context window — only the resulting upstream "prompt is too
// long" 400s, which were forwarded without a trace. The registry is
// visibility state ONLY: it is written after responses and read by
// `/_stats`; routing never consults it.

/// Default cap on live session registry entries.
pub(crate) const DEFAULT_SESSION_REGISTRY_MAX: usize = 1000;
/// Default seconds after a session's last request before eviction.
pub(crate) const DEFAULT_SESSION_REGISTRY_TTL_SECS: u64 = 1800;
/// Max sessions returned in the `/_stats` `sessions` array (highest
/// context-window % first).
const SESSIONS_STATS_TOP_N: usize = 50;
/// Cap on distinct model labels in the prompt-too-long counter map (the model
/// string is client-controlled; overflow buckets into `_other`).
const MAX_PROMPT_TOO_LONG_MODELS: usize = 32;

/// `le` boundaries (tokens) for the live session-size distribution on
/// `/metrics`. Dense around the 200k window edge — that's where sessions
/// start 400ing — sparse elsewhere; `+Inf` is implicit.
pub(crate) const SESSION_TOKENS_BUCKETS: [u64; 10] = [
    10_000, 25_000, 50_000, 100_000, 150_000, 175_000, 200_000, 300_000, 500_000, 1_000_000,
];

/// Default model context window (tokens). Every current Claude model ships
/// with a 200k window unless the 1M beta is active on the request.
pub(crate) const DEFAULT_CONTEXT_WINDOW: u64 = 200_000;
/// Context window when the request carries the `context-1m` beta flag.
const CONTEXT_WINDOW_1M: u64 = 1_000_000;

/// Live per-session state, keyed by the affinity routing key — the same key
/// `pick_endpoint` pins on, so "session" here has exactly the granularity of
/// a routing pin (fan-out subagents that share a coarse session-id but carry
/// distinct content fingerprints appear as distinct entries).
pub(crate) struct SessionEntry {
    client_id: String,
    agent_id: String,
    session_id: String,
    model: String,
    /// Endpoint the session's last request was served by (its current pin).
    endpoint: String,
    /// `input + cache_read + cache_creation` from the last successful
    /// `Usage` — the prompt's occupancy of the model context window.
    last_prompt_tokens: u64,
    pub(crate) context_window: u64,
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
pub(crate) fn request_has_1m_beta(headers: &axum::http::HeaderMap) -> bool {
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
pub(crate) fn context_window_for(model: &str, has_1m_beta: bool) -> u64 {
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
pub(crate) fn prompt_too_long_message(body: &serde_json::Value) -> Option<&str> {
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
    pub(crate) fn note_prompt_too_long(
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

    /// Count + log an upstream 429 on a fast-mode request. The response is
    /// forwarded to the caller unchanged by `classify_retry_status` (no
    /// cooldown, no rotation — see the reasoning there); this is the operator
    /// trace that makes a client looping `speed: "fast"` visible instead of
    /// silent (LAB-2675).
    pub(crate) fn note_fast_mode_429(
        &self,
        endpoint_name: &str,
        headers: &reqwest::header::HeaderMap,
    ) {
        if let Ok(mut counts) = self.fast_mode_429.lock() {
            *counts.entry(endpoint_name.to_owned()).or_insert(0) += 1;
        }
        let retry_after_raw = headers
            .get("retry-after")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("-");
        warn!(
            account = endpoint_name,
            retry_after_raw,
            "fast-mode 429 forwarded to caller — account NOT cooled (separate rate bucket)"
        );
    }

    /// Count + WARN an upstream entitlement 400 (`is_entitlement_exhausted_400`,
    /// LAB-4729). The retry loop re-sends the request once; the account is
    /// deliberately NOT cooled. The refusal is scoped to a class of request
    /// (e.g. past-band Fable on a credits-exhausted account) — the same account
    /// keeps serving other traffic — so cooling it hands any client a lever:
    /// each refused request would cool two accounts (original + re-send), and
    /// a modest rate of them keeps the whole pool cooled, denying every
    /// client. Same reasoning as the fast-mode 429 exemption in
    /// `classify_retry_status`. The cost is one zero-token round trip per
    /// refused request, visible on this counter.
    pub(crate) fn note_entitlement_400(&self, endpoint_name: &str) {
        if let Ok(mut counts) = self.entitlement_400.lock() {
            *counts.entry(endpoint_name.to_owned()).or_insert(0) += 1;
        }
        warn!(
            account = endpoint_name,
            "upstream 400: account out of extra usage (account not cooled; request re-sent at most once)"
        );
    }

    /// Snapshot the session registry for `/_stats`: TTL-filtered, sorted by
    /// context-window % desc, capped to `SESSIONS_STATS_TOP_N`. Raw IPs and
    /// session ids never leave the registry — the label is a hash of the
    /// affinity key and agent/session ids are truncated to 8 chars.
    pub(crate) fn sessions_snapshot(&self, now: u64) -> Vec<serde_json::Value> {
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
    pub(crate) fn session_tokens_histogram(
        &self,
        now: u64,
    ) -> ([u64; SESSION_TOKENS_BUCKETS.len() + 1], u64) {
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

/// Usage-only log line for the unified-OpenAI-endpoint fallback path
/// (`try_fallback_upstream`), which has no routing/utilization snapshot to
/// merge with (OpenAI endpoints carry stub `RateLimitInfo`). Kept separate
/// from `log_proxied` below — LAB-3214 folded the native and openai-compat
/// paths' `proxied` + `usage` lines into one; this path never had a
/// `proxied` line to fold into, so its usage echo stays as its own line.
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

/// Routing/utilization snapshot captured when upstream response headers
/// arrive (the former standalone `proxied` / `proxied (openai-compat)` INFO
/// lines), carried through to `finalize_stream` / `finalize_non_stream` so it
/// can be logged on the SAME line as token usage — known only once the body
/// or stream completes (LAB-3214: one INFO line per request, not two).
pub(crate) enum ProxiedCtx {
    Anthropic {
        client_ver: String,
        utilization: String,
        util_5h: String,
        util_7d: String,
        constraint: &'static str,
        overage: bool,
        pin: &'static str,
        total: u64,
        /// Content fingerprint (12 hex, `content_fingerprints`) joining this
        /// line to the DEBUG `fingerprint` line by req_id; `"-"` when the
        /// request body was unparseable.
        fp: String,
    },
    OpenaiCompat {
        client_ver: String,
        utilization: String,
        util_5h: String,
        util_7d: String,
        constraint: &'static str,
        pin: &'static str,
        stream: bool,
    },
}

/// Emit the single per-request INFO line merging routing context with token
/// usage. Usage fields are simply zero when nothing was extracted (error
/// response, client disconnect, upstream failure) — still exactly one line,
/// still carrying req_id/account/status (AC4), so no separate branch is
/// needed for the no-usage case.
#[allow(clippy::too_many_arguments)]
pub(crate) fn log_proxied(
    req_id: &str,
    client_id: &str,
    model: &str,
    account: &str,
    client_ip: &str,
    agent: &str,
    session: &str,
    status_code: u16,
    ctx: &ProxiedCtx,
    usage: &TokenUsage,
) {
    match ctx {
        ProxiedCtx::Anthropic {
            client_ver,
            utilization,
            util_5h,
            util_7d,
            constraint,
            overage,
            pin,
            total,
            fp,
        } => {
            info!(
                req_id,
                client = %client_ip,
                client_id,
                ver = %client_ver,
                agent,
                session,
                model,
                account,
                status = status_code,
                utilization = %utilization,
                util_5h = %util_5h,
                util_7d = %util_7d,
                constraint = *constraint,
                overage,
                pin = *pin,
                total,
                fp = %fp,
                input = usage.input_tokens,
                output = usage.output_tokens,
                cached = usage.cache_read_input_tokens,
                cache_write = usage.cache_creation_input_tokens,
                "proxied"
            );
        }
        ProxiedCtx::OpenaiCompat {
            client_ver,
            utilization,
            util_5h,
            util_7d,
            constraint,
            pin,
            stream,
        } => {
            info!(
                req_id,
                client = %client_ip,
                client_id,
                ver = %client_ver,
                agent,
                session,
                model,
                account,
                status = status_code,
                utilization = %utilization,
                util_5h = %util_5h,
                util_7d = %util_7d,
                constraint = *constraint,
                pin = *pin,
                openai_compat = true,
                stream,
                input = usage.input_tokens,
                output = usage.output_tokens,
                cached = usage.cache_read_input_tokens,
                cache_write = usage.cache_creation_input_tokens,
                "proxied (openai-compat)"
            );
        }
    }
}

/// Finalize a streaming response: extract usage, log, and shadow log.
/// Shared by proxy_handler and openai_chat_handler.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn finalize_stream(
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
    ctx: ProxiedCtx,
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
            status = status_code,
            reason,
            elapsed_ms,
            sse_bytes = scanner.bytes_seen,
            sse_event_count = scanner.event_count,
            sse_events = ?scanner.event_preview,
            truncated,
            "stream_end_no_usage"
        );
    }
    // Single terminal line for this request (LAB-3214), routing context
    // merged with usage — zero-valued when none was captured. `usage_model`
    // (not `model`) so this reconciles with
    // `anthropic_client_model_token_usage_total`, which records against the
    // same response-derived model.
    log_proxied(
        req_id,
        client_id,
        usage_model,
        acct_name,
        client_ip,
        agent,
        session,
        status_code,
        &ctx,
        usage,
    );
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
pub(crate) async fn finalize_non_stream(
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
    ctx: Option<ProxiedCtx>,
) {
    let usage_model = response_model.unwrap_or(model);
    if !usage.is_empty() {
        state.record_usage(ep, client_id, usage_model, usage).await;
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
    // Single terminal INFO line for this request (AC1/AC2/AC3/AC4) — merged
    // routing + usage where a routing snapshot exists. The unified-OpenAI-
    // endpoint fallback path (no snapshot) keeps its own usage-only echo,
    // fired only when usage was actually captured (unchanged behavior).
    match &ctx {
        Some(c) => log_proxied(
            req_id,
            client_id,
            usage_model,
            acct_name,
            client_ip,
            agent,
            session,
            status_code,
            c,
            usage,
        ),
        None => {
            if !usage.is_empty() {
                log_usage(req_id, client_id, usage_model, acct_name, usage);
            }
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
    pub(crate) fn update_burn_rate(&self, burn_rate: &Mutex<BurnRate>, client_id: &str) {
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
    pub(crate) async fn check_budget(&self, client_id: &str) -> Result<(), u64> {
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
        if let Some(&(day, used)) = self.lock_budget_usage().get(client_id) {
            if day == today && used >= limit {
                return Err(limit - (used.min(limit)));
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
    pub(crate) const BUDGET_DEL_IF_POISONED_SCRIPT: &'static str = r#"
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
    pub(crate) fn budget_value_poisoned(e: &fred::error::RedisError) -> bool {
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
    pub(crate) async fn record_budget_usage(&self, client_id: &str, tokens: u64) {
        if tokens == 0 || !self.client_budgets.contains_key(client_id) {
            return;
        }
        let today = Self::now_epoch() / 86400;

        // Always update local state (for stats + fallback). Scoped so the
        // guard drops before the INCRBY await below.
        {
            let mut map = self.lock_budget_usage();
            let entry = map.entry(client_id.to_owned()).or_insert((today, 0));
            // `!= today` is right HERE because this `today` is fresh. Do not
            // unify with fold_budget_mirror's stricter `<` / `>` day rule —
            // that fold receives a `today` fetched earlier and must not
            // clobber an entry that already rolled past it (LAB-3217).
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

    /// Fold the fleet-wide `alb:budget:{client}:{day}` counters into the local
    /// `budget_usage` mirror so `/_stats` and the `anthropic_client_budget_*`
    /// gauges report the shared total on every replica — including one that
    /// just restarted with an empty mirror (LAB-3217). Runs on every sync tick
    /// with the values `cluster_info` already MGETs; it touches no Redis key,
    /// so enforcement (`check_budget` / `record_budget_usage`) is unchanged.
    ///
    /// `max`, never overwrite: the local accumulator is also the enforcement
    /// floor for increments lost to a failed INCRBY (LAB-1962), so a counter
    /// that is behind must not lower it. A stale (previous-day) entry is
    /// replaced by today's value, never summed with it; an entry that already
    /// rolled PAST the fetched day is left alone. `today` MUST be the
    /// epoch-day the counters were fetched under, not re-derived here: across
    /// a UTC midnight a re-read would file yesterday's total under today, and
    /// `max` would then pin that over-report for the whole day. A zero/absent
    /// counter carries nothing the mirror lacks and is skipped, so the sync
    /// alone never materialises entries for idle clients.
    pub(crate) fn fold_budget_mirror<'a>(
        &self,
        today: u64,
        remote: impl IntoIterator<Item = (&'a str, u64)>,
    ) {
        let mut map = self.lock_budget_usage();
        for (client_id, used) in remote {
            if used == 0 {
                continue;
            }
            let entry = map.entry(client_id.to_owned()).or_insert((today, 0));
            if entry.0 > today {
                continue;
            }
            if entry.0 < today {
                *entry = (today, 0);
            }
            entry.1 = entry.1.max(used);
        }
    }

    /// Check if client_id is an operator.
    pub(crate) fn is_operator(&self, client_id: &str) -> bool {
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
    pub(crate) async fn is_emergency_brake_active(&self) -> bool {
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
    pub(crate) async fn pre_request_gate(
        &self,
        client_id: &str,
        model: &str,
    ) -> Result<(), Box<Response>> {
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
            self.note_client_rejection(client_id, "budget");
            warn!(client_id = %client_id, "rejected: daily token budget exceeded");
            return Err(Box::new(
                (StatusCode::TOO_MANY_REQUESTS, "daily token budget exceeded").into_response(),
            ));
        }

        // 2. Utilization limit (new)
        if let Err(retry_after) = self.check_utilization_limit(client_id, model).await {
            self.note_client_rejection(client_id, "utilization");
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
            self.note_client_rejection(client_id, "brake");
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

#[cfg(test)]
mod tests;
