use crate::*;

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
///   - `RetryEntitlement(resp)`: the account is out of paid extra usage for
///     this request (`is_entitlement_exhausted_400`, LAB-4729). The loop
///     re-sends ONCE, and a second entitlement 400 — or nothing left to try —
///     returns `resp` to the caller.
// Response payloads are boxed so the enum stays small (one word per payload):
// it rides in the Err of `classify_retry_status`'s Result, where an inline
// `Response` is 128+ bytes on the hot success path (clippy::result_large_err —
// same pattern as `authenticate` / `reserve_request_body`).
pub(crate) enum ForwardOutcome {
    Done(Box<Response>),
    RetryModelUnsupported(Box<Response>),
    RetryEntitlement(Box<Response>),
    Retry {
        saw_529: bool,
        push_skip: bool,
        transient: bool,
    },
}

/// Non-transient rotation: skip this endpoint and try the next candidate.
/// Pre-dates the `ForwardOutcome` return type as a bare `None`.
pub(crate) const ROTATE: ForwardOutcome = ForwardOutcome::Retry {
    saw_529: false,
    push_skip: true,
    transient: false,
};

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
pub(crate) fn is_model_unsupported_error(status: StatusCode, body: &serde_json::Value) -> bool {
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

/// Anchor for the entitlement 400 (LAB-4729). Only the first sentence: the
/// second names who can add more ("Ask your workspace admin …") and varies by
/// plan, the first is the condition itself.
const ENTITLEMENT_400_ANCHOR: &str = "You're out of extra usage";

/// True when an upstream 400 is Anthropic refusing the request because the
/// ACCOUNT's paid extra usage is exhausted — account state wearing a client-
/// error status (LAB-4729). Observed live 2026-09-22:
///   400 `{"type":"error","error":{"type":"invalid_request_error",
///   "message":"You're out of extra usage. Ask your workspace admin to add
///   more so you can keep going."}}`
/// Every other 400 is the caller's own error and must reach it unchanged, so
/// this is deliberately narrow: exact `error.type` and a message ANCHORED at
/// its start — a substring match (`usage`, `extra usage`) would reroute real
/// client errors that merely mention the word.
pub(crate) fn is_entitlement_exhausted_400(status: StatusCode, body: &serde_json::Value) -> bool {
    status == StatusCode::BAD_REQUEST
        && body.pointer("/error/type").and_then(|v| v.as_str()) == Some("invalid_request_error")
        && body
            .pointer("/error/message")
            .and_then(|v| v.as_str())
            .is_some_and(|m| m.starts_with(ENTITLEMENT_400_ANCHOR))
}

/// Surface the real cause of a `reqwest::Error`. The Display form only shows
/// "error sending request for url (...)" — the actionable detail (stale pooled
/// connection closed, connection reset, DNS failure, connect refused, timeout)
/// lives in the error's `source()` chain and its `is_*` classifiers. Returns a
/// compact `kind=... cause=a -> b -> c` string for structured logs so we can
/// tell *why* upstream sends fail without guessing.
pub(crate) fn describe_reqwest_error(e: &reqwest::Error) -> String {
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
/// One exception: a non-burst 429 on a `speed: "fast"` request returns
/// `Ok(resp)` too, uncooled and unrotated — fast mode has its own rate
/// bucket, so that 429 is not evidence about the account (LAB-2675).
///
/// `openai_error_shape` picks the error-body format for the terminal 3xx
/// arm: callers whose downstream parses OpenAI errors (`/v1/chat/completions`
/// passthrough) get `{"error":{...}}`, Anthropic-surface callers get
/// `{"type":"error",...}` — matching how every other error arm on those
/// paths translates per surface.
pub(crate) async fn classify_retry_status(
    state: &AppState,
    status: StatusCode,
    rate_info: &RwLock<RateLimitInfo>,
    endpoint_name: &str,
    resp: reqwest::Response,
    openai_error_shape: bool,
    // `fast_mode_body`: the body actually sent upstream, for the fast-mode
    // test — `None` when the protocol cannot express `speed`. Bytes rather
    // than a pre-computed bool so the parse happens only on the 429 branch
    // that needs it, and so no caller can hand this function a `false` that
    // is only true for non-429 statuses.
    fast_mode_body: Option<&bytes::Bytes>,
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
        // …unless the request asked for fast mode. Fast mode has its own rate
        // bucket, separate from the account's standard 5h/7d windows, so a
        // fast-mode 429 is not evidence the account is exhausted. Cooling the
        // account here would let one client looping `speed: "fast"` drain each
        // account's (smaller) fast bucket in turn and deny STANDARD traffic for
        // every other client until the cooldowns lapse — up to the emergency
        // brake (LAB-2675, from the LAB-2669 security review).
        //
        // Instead the 429 is returned to the caller unchanged: the forward path
        // reflects upstream's `retry-after` (`reflect_upstream_headers`), leaves
        // `hard_limited_until` / `remaining_*` alone, and does not rotate. That
        // is exactly what a direct Anthropic client sees, and the client — not
        // the proxy — decides whether to back off or retry at standard speed.
        // Rotating fast requests with a per-account fast cooldown was rejected:
        // more state, and one client could still sweep every account's bucket.
        //
        // The exemption defers to `is_burst_429`. A burst 429 (`x-should-retry`,
        // no `retry-after`, no rate headers) is a per-minute RPM/concurrency
        // limit on the ACCOUNT, not on a rate bucket, so it is real evidence
        // about the account whatever speed the request asked for. Exempting it
        // would leave the account pinned: standard traffic routed to it would
        // burst-429 and hard-limit it anyway. The caller still gets
        // `x-should-retry` as its transient hint (LAB-2675 panel finding).
        //
        // What this does NOT buy: the ticket assumed the utilization ceilings
        // would still cover a fast request on an exhausted account, because
        // `update_rate_info_for` consumes the unified headers before
        // classification. AC-5's live probe disproved that — a fast-mode
        // response's unified headers describe the fast pool, not the account
        // — and ingesting them corrupts the account's standard view. Separate
        // defect in `update_rate_info_for`, tracked as LAB-2693; not fixable
        // from here, which runs after the ingest.
        if !is_burst_429(resp.headers())
            && fast_mode_body.is_some_and(|b| request_wants_fast_mode(b))
        {
            state.note_fast_mode_429(endpoint_name, resp.headers());
            return Ok(resp);
        }
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
pub(crate) enum RetryStep {
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
#[allow(clippy::too_many_arguments)]
pub(crate) fn apply_round_outcome(
    retry_round: u32,
    outcome: ForwardOutcome,
    picked_idx: EndpointIdx,
    skip: &mut Vec<EndpointIdx>,
    saw_529: &mut bool,
    saw_transient: &mut bool,
    model_unsupported_resp: &mut Option<Response>,
    entitlement_resp: &mut Option<(EndpointIdx, Option<Response>)>,
) -> RetryStep {
    // Any later outcome supersedes a stashed entitlement 400 as the caller's
    // answer (LAB-4729): the re-send's 404, or the 429 of a merely rate-limited
    // pool, is the truer terminal cause. The endpoint stays, so the one-shot is
    // still spent and the refuser still skipped.
    if !matches!(outcome, ForwardOutcome::RetryEntitlement(_)) {
        if let Some((_, stashed)) = entitlement_resp.as_mut() {
            *stashed = None;
        }
    }
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
        // Account out of extra usage (LAB-4729): re-send ONCE per request.
        // The refusal is scoped to a class of request, so every account may
        // give it — rotating on each would sweep the whole pool for a request
        // nothing can serve. A second one goes to the caller as-is. The
        // refusing endpoint rides along so every later round skips it too:
        // it is not cooled, and `skip` resets per round.
        ForwardOutcome::RetryEntitlement(resp) => {
            if entitlement_resp.is_some() {
                return RetryStep::Return(*resp);
            }
            *entitlement_resp = Some((picked_idx, Some(*resp)));
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
pub(crate) fn round_should_continue(retry_round: u32, saw_529: bool, saw_transient: bool) -> bool {
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
pub(crate) fn round_backoff_delay(retry_round: u32, last_saw_529: bool) -> Duration {
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
pub(crate) fn exhaustion_response(
    state: &AppState,
    last_saw_transient: bool,
    last_saw_529: bool,
) -> Response {
    if last_saw_transient && !last_saw_529 {
        state.pool_exhausted[PoolExhaustion::Transient as usize].fetch_add(1, Ordering::Relaxed);
        warn!("all endpoints transient-failed after backoff; returning retryable 503");
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            [("retry-after", "1")],
            "upstream temporarily unreachable",
        )
            .into_response();
    }
    state.pool_exhausted[PoolExhaustion::RateLimited as usize].fetch_add(1, Ordering::Relaxed);
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
pub(crate) fn model_unsupported_response(model: &str, openai_shape: bool) -> Response {
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
///
/// Also `proxy_handler`'s rejection of a valid-JSON non-object body
/// (LAB-4314). That handler is the router fallback, so the rejected request
/// may be any method on any path; the envelope is still the right shape,
/// since it is what the Anthropic upstream returns for the same body.
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
    // SSE cache hint, the Anthropic request id for error reports, retry-after
    // on forwarded 4xx, and the upstream's transient retry hint.
    const ALLOWED: &[&str] = &[
        "content-type",
        "content-length",
        "cache-control",
        "request-id",
        "retry-after",
        "x-should-retry",
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
pub(crate) fn upstream_client_builder() -> reqwest::ClientBuilder {
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

/// True when the request body asks for fast mode (top-level `"speed":
/// "fast"`, the body half of the `fast-mode-*` beta). Absent field or an
/// unparseable body counts as standard speed — Anthropic's default.
///
/// Fast mode bills against a rate bucket that is SEPARATE from the account's
/// standard 5h/7d windows, so a `429` on a fast request says nothing about
/// the account's standard headroom (LAB-2675).
fn request_wants_fast_mode(body: &[u8]) -> bool {
    serde_json::from_slice::<serde_json::Value>(body)
        .ok()
        .map(|v| body_wants_fast_mode(&v))
        .unwrap_or(false)
}

/// The fast-mode predicate on an already-parsed body. Single definition,
/// same split as `request_wants_stream` / `body_wants_stream`, so every
/// fast-mode decision in the proxy agrees on what "fast" means.
fn body_wants_fast_mode(body: &serde_json::Value) -> bool {
    body.get("speed").and_then(|s| s.as_str()) == Some("fast")
}

/// Caller-identity headers that must not leave this proxy. The IP set is what
/// fronting hops (cloudflared, the Cloudflare Worker, nginx-ingress) carry the
/// caller's address in — `resolve_client_ip` reads only `x-forwarded-for`; the
/// rest are stripped so they cannot leak either. The `x-*-id` set is this
/// proxy's own client/agent/session vocabulary. Relayed upstream, any of them
/// ties a pooled-account request to the individual caller behind the proxy
/// (GH #168). Deliberately absent: Claude Code's native
/// `x-claude-code-session-id` (GH #171, operator decision) and edge-added
/// `cf-*`, which the ingress strips (GH #166).
const CLIENT_IDENTITY_HEADERS: &[&str] = &[
    "x-forwarded-for",
    "x-real-ip",
    "forwarded",
    "true-client-ip",
    "x-client-id",
    "x-agent-id",
    "x-session-id",
];

pub(crate) fn strip_client_identity_headers(headers: &mut axum::http::HeaderMap) {
    for name in CLIENT_IDENTITY_HEADERS {
        headers.remove(*name);
    }
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
pub(crate) async fn forward_anthropic(
    state: &Arc<AppState>,
    parts: &axum::http::request::Parts,
    body_bytes: &bytes::Bytes,
    oauth_body_bytes: &bytes::Bytes,
    is_fast_mode: bool,
    ep: &Endpoint,
    endpoint_idx: usize,
    req_id: &str,
    client_id: &str,
    client_ver: &str,
    client_ip: &std::net::IpAddr,
    agent_id: &str,
    session_id: &str,
    model: &str,
    fp: Option<&str>,
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
    if !state.forward_caller_identity {
        strip_client_identity_headers(&mut headers);
    }

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

    // LAB-1261: header/body coherence. The filter above edits the HEADER;
    // several betas are paired with a top-level body field that only exists
    // when the flag is declared, and forwarding that field without its flag
    // is a hard upstream 400 rather than the graceful degrade the filter was
    // written for. Strip the orphaned half here so the request stays
    // coherent — for any beta family, including ones the LB has never seen.
    //
    // Reachable only on the OAuth branch (the only one that filters), which
    // is also the only branch where `req_body` picks the OAuth variant, so
    // that is the variant rewritten.
    // Scoped to the Messages schema `BASE_BODY_FIELDS` actually describes.
    // `proxy_handler` is the router's catch-all, so `/v1/messages/batches`
    // (`requests`), `/v1/complete` (`prompt`) and anything else arrive here
    // too — running a Messages-only field list over those bodies deletes them
    // outright.
    let coherent_body = if matches!(
        parts.uri.path(),
        "/v1/messages" | "/v1/messages/count_tokens"
    ) {
        strip_orphaned_beta_body_fields(
            oauth_body_bytes,
            headers
                .get("anthropic-beta")
                .and_then(|v| v.to_str().ok())
                .unwrap_or(""),
            &dropped,
        )
    } else {
        None
    };
    if let Some((_, stripped)) = &coherent_body {
        state.record_stripped_body_fields(client_id, stripped, &dropped);
    }
    // Stripping `speed` invalidates the fast-mode classification
    // `proxy_handler` made on the pre-filter body: what goes upstream is a
    // standard request, so it draws the standard rate bucket, not the fast
    // one (LAB-2693). Only reachable under a custom `allowed_client_betas`
    // that omits `fast-mode-*`; the default carries it.
    let is_fast_mode = is_fast_mode
        && !coherent_body
            .as_ref()
            .is_some_and(|(_, stripped)| stripped.iter().any(|f| f == "speed"));

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
        } else if token.starts_with(OAUTH_TOKEN_PREFIX) {
            "oauth"
        } else {
            "api-key"
        };
        debug!(
            req_id,
            account = endpoint_name,
            auth_method,
            body_bytes = if token.starts_with(OAUTH_TOKEN_PREFIX) {
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
    // Use OAuth variant (with CC system prompt) for OAuth tokens, and its
    // beta-coherent rewrite when the filter orphaned a body field (LAB-1261).
    let req_body = if token.starts_with(OAUTH_TOKEN_PREFIX) {
        // `dropped` is only ever non-empty on this branch, so a rewrite
        // without it would mean the filter's contract changed underneath us.
        debug_assert!(coherent_body.is_none() || token.starts_with(OAUTH_TOKEN_PREFIX));
        match &coherent_body {
            Some((rewritten, _)) => rewritten,
            None => oauth_body_bytes,
        }
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
            *state.lock_transport_errors().entry(kind).or_insert(0) += 1;
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
        .update_rate_info_for(
            rate_info,
            endpoint_name,
            resp.headers(),
            // The request's speed picks the rate bucket: a fast-mode body's
            // response carries fast-pool headers, not the account's
            // (LAB-2693). Classified in `proxy_handler`, then NARROWED above
            // when the LAB-1261 strip removed `speed` — use that value, not
            // the parameter.
            is_fast_mode,
        )
        .await;

    // Update burn rate (after rate-limit headers are parsed)
    state.update_burn_rate(&ep.burn_rate, client_id);

    // Classify 429 / 529 / other 5xx into a retry decision (shared helper).
    let mut resp = match classify_retry_status(
        state,
        status,
        rate_info,
        endpoint_name,
        resp,
        /* openai_error_shape */ false,
        // The bytes actually sent upstream (the OAuth variant on OAuth
        // tokens) — that body is what picks the rate bucket.
        Some(req_body),
    )
    .await
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

    // Capture the routing/utilization snapshot + inject budget status header.
    // The `proxied` line itself is deferred to `finalize_stream`/
    // `finalize_non_stream`, which merge it with token usage once known
    // (LAB-3214: one INFO line per request, not two).
    let (budget_status, ctx) = {
        let info = rate_info.read().await;
        let (eff_util, constraint, _adj_5h, _adj_7d) =
            effective_utilization(&info, AppState::now_epoch(), model);
        let ctx = ProxiedCtx::Anthropic {
            client_ver: client_ver.to_owned(),
            utilization: format!("{eff_util:.2}"),
            util_5h: info
                .utilization_5h
                .map(|v| format!("{v:.2}"))
                .unwrap_or_else(|| "-".to_string()),
            util_7d: info
                .utilization_7d
                .map(|v| format!("{v:.2}"))
                .unwrap_or_else(|| "-".to_string()),
            constraint,
            overage: info.overage_in_use,
            pin: state.pin_status(client_id, endpoint_idx),
            total: ep.requests.load(Ordering::Relaxed),
            fp: fp.unwrap_or("-").to_string(),
        };
        (compute_pressure_status(eff_util, client_id, state), ctx)
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
                        warn!(req_id = req_id_clone, error = %e, "upstream SSE read failed");
                        // The upstream's own terminator (`message_stop`, or
                        // an in-band `event: error`) already went downstream
                        // verbatim: the client saw a complete stream, and an
                        // error frame after it would make the SDK raise on a
                        // request that succeeded. Typical trigger: peer drops
                        // without the chunked terminator right after
                        // `message_stop` (hyper `IncompleteMessage`).
                        if scanner.terminal.reached() {
                            debug!(
                                req_id = req_id_clone,
                                "transport error after terminator — error frame suppressed"
                            );
                            break;
                        }
                        scanner.terminal.errored = true;
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
            let upstream_error = scanner.terminal.errored;
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
                ctx,
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
        // socket error, while our logs showed no trace of it. Log loudly and
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
                // This branch returns before `finalize_non_stream` — log the
                // merged line here too (no usage: the body never arrived), so
                // the routing/utilization snapshot still lands at INFO (AC4).
                log_proxied(
                    req_id,
                    client_id,
                    model,
                    endpoint_name,
                    &client_ip.to_string(),
                    agent_id,
                    session_id,
                    status.as_u16(),
                    &ctx,
                    &TokenUsage::default(),
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
            // Out of extra usage: re-send once to another account (LAB-4729).
            // Both are account state wearing a 4xx. A streaming request lands
            // here too: upstream sends the 400 as a JSON body, not an event
            // stream, so it re-sends before any byte reaches the client.
            let rotate: Option<fn(Box<Response>) -> ForwardOutcome> =
                if is_model_unsupported_error(status, &parsed) {
                    state.note_model_unsupported(endpoint_name, endpoint_idx, model);
                    Some(ForwardOutcome::RetryModelUnsupported)
                } else if is_entitlement_exhausted_400(status, &parsed) {
                    state.note_entitlement_400(endpoint_name);
                    Some(ForwardOutcome::RetryEntitlement)
                } else {
                    None
                };
            if let Some(retry) = rotate {
                // This branch returns before `finalize_non_stream` — log the
                // merged line here too, so a rotated rejection still gets the
                // routing/utilization snapshot at INFO, same as the old
                // unconditional `proxied` line did (AC4).
                log_proxied(
                    req_id,
                    client_id,
                    model,
                    endpoint_name,
                    &client_ip.to_string(),
                    agent_id,
                    session_id,
                    status.as_u16(),
                    &ctx,
                    &usage,
                );
                let response = builder
                    .body(Body::from(resp_body_bytes))
                    .unwrap_or_else(|_| {
                        (StatusCode::INTERNAL_SERVER_ERROR, "response build error").into_response()
                    });
                return retry(Box::new(response));
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
            Some(ctx),
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
pub(crate) async fn maybe_cache_store(
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

/// LAB-3877: build the HTTP 400 for a `block` verdict. The body carries finding
/// offsets and labels only — never the matched text — so an error surfaced to a
/// client (or captured in its logs) cannot itself leak the secret it flagged.
/// `openai_shape` selects the OpenAI error envelope for `/v1/chat/completions`
/// clients (same convention as `model_unsupported_response`); the cause is
/// machine-readable as `error.code` there and `error.type` on the Anthropic side.
#[cfg(feature = "guard")]
pub(crate) fn guard_blocked_response(
    findings: &[guard::Finding],
    reason: &str,
    openai_shape: bool,
) -> Response {
    let body = if openai_shape {
        serde_json::json!({
            "error": {
                "message": reason,
                "type": "invalid_request_error",
                "param": null,
                "code": "guard_blocked",
                "findings": findings,
            }
        })
    } else {
        serde_json::json!({
            "type": "error",
            "error": {
                "type": "guard_blocked",
                "message": reason,
                "findings": findings,
            }
        })
    };
    (
        StatusCode::BAD_REQUEST,
        [(hyper::header::CONTENT_TYPE, "application/json")],
        body.to_string(),
    )
        .into_response()
}

/// LAB-3877: stamp an `Annotate` verdict's finding count onto the response as
/// `X-Guard-Findings`. A decimal count is always a valid header value.
#[cfg(feature = "guard")]
pub(crate) fn stamp_guard_findings(mut response: Response, count: Option<usize>) -> Response {
    if let Some(count) = count {
        response
            .headers_mut()
            .insert("x-guard-findings", HeaderValue::from(count as u64));
    }
    response
}

pub(crate) async fn proxy_handler(
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
    let principal = match state.authenticate_throttled(
        &client_ip,
        client_addr,
        req.headers(),
        false,
        "proxy",
    ) {
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

    // LAB-3877: the guard's scan input is extracted from the parsed body here,
    // pre-injection, so it sees exactly what the client sent (auto-cache
    // injection adds cache_control only, but extracting before it keeps the
    // guard trivially body-neutral). The scan itself runs later, after
    // `pre_request_gate`.
    //
    // LAB-4358: this holds the scanner's own three-way verdict on the body.
    // `NothingToScan` is the right default for the bodiless request
    // (`GET /v1/models`) — nothing was sent, so nothing failed to scan — and is
    // replaced below by whichever of the three the body turns out to be. No
    // `messages`-shape predicate is re-derived here: `ScanInput::from_body` owns
    // that discrimination for both surfaces, so the two cannot drift.
    #[cfg(feature = "guard")]
    let mut guard_outcome = guard::ScanOutcome::NothingToScan;

    // Parse body once for model extraction, optional cache injection, and
    // the fast-mode flag that picks the rate bucket downstream.
    let (body_bytes, oauth_body_bytes, model, fp, cache_key, is_fast_mode) =
        if let Ok(mut parsed) = serde_json::from_slice::<serde_json::Value>(&body_bytes) {
            // Valid JSON but not an object (`[1]`, `"x"`, `7`, `true`,
            // `null`) can only 400 upstream. Reject it here so it costs no
            // account headroom, no budget sample and no credentialed
            // round-trip; the envelope matches what upstream would say
            // (LAB-4314). Logged like the sibling rejections above so the
            // client is attributable by req_id.
            if !parsed.is_object() {
                warn!(
                    req_id,
                    client = %client_ip,
                    client_id = %client_id,
                    "rejected: request body is not a JSON object"
                );
                return untranslatable_request_response("request body must be a JSON object");
            }
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

            // LAB-3877: extract the newest user text + tool_result blocks for
            // the guard (never `system`). Read-only borrow of `parsed`.
            #[cfg(feature = "guard")]
            {
                guard_outcome = guard::ScanInput::from_body(&parsed);
            }

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
            // model is included because caches are per-model. Join to `proxied`
            // (account + token usage) by req_id offline.
            //
            // DEBUG only (LAB-3214): per-request detail, not the default INFO
            // line. `bps` is unbounded length, so skip building it entirely
            // when debug isn't enabled — it's otherwise unused (fp/fps still
            // feed routing above regardless of log level).
            if tracing::enabled!(tracing::Level::DEBUG) {
                let bps = prefix_breakpoint_hashes(&parsed)
                    .into_iter()
                    .map(|(pos, h)| format!("{pos}:{h}"))
                    .collect::<Vec<_>>()
                    .join("|");
                debug!(
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

            // Pre-compute OAuth variant with Claude Code system prompt inserted.
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

            // Classified once here rather than re-parsed from the outbound
            // bytes per upstream response: neither injector above touches
            // `speed`, so one flag is true of both byte variants (LAB-2693).
            //
            // NOT the last word on it. `forward_anthropic` may strip `speed`
            // after the beta filter (LAB-1261) and re-derives the flag there;
            // any new consumer downstream of that filter must read the
            // narrowed value, or it bills a standard request to the fast pool.
            let is_fast_mode = body_wants_fast_mode(&parsed);

            (
                bytes::Bytes::from(bytes),
                bytes::Bytes::from(oauth_bytes),
                model,
                Some(fp),
                cache_key,
                is_fast_mode,
            )
        } else {
            // Non-empty and not JSON: the scanner cannot read it, and a parse
            // differential against the upstream could smuggle content past the
            // scan. A bodiless request keeps the `NothingToScan` default.
            #[cfg(feature = "guard")]
            if !body_bytes.is_empty() {
                guard_outcome = guard::ScanOutcome::Unscannable(guard::REASON_BODY_UNPARSEABLE);
            }
            let clone = body_bytes.clone();
            (body_bytes, clone, String::new(), None, None, false)
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

    // LAB-3877: Tier 0 content guard — runs after the gate, before endpoint
    // selection (see `AppState::guard_hook`), on the outcome computed above.
    //
    // Deliberately path-agnostic: the path is forwarded verbatim, so scoping by
    // route would let `/v1/messages/` or a percent-encoded spelling skip the
    // fail-closed rule. That stays safe on every path because `from_body`
    // returns `NothingToScan` for a body with no `messages` key at all, which is
    // what `/v1/complete` and `/v1/models` send through this fallback.
    #[cfg(feature = "guard")]
    let guard_annotate: Option<usize> =
        match state.guard_hook(&req_id, &client_id, &guard_outcome, false) {
            Ok(annotate) => annotate,
            Err(resp) => return *resp,
        };

    // The remaining dispatch is wrapped so an `Annotate` verdict can stamp the
    // `X-Guard-Findings` header onto whatever response it yields (cache hit,
    // proxied success, or exhaustion) from one place. The wrapper is an
    // immediately-awaited async block, so it is behaviourally transparent (and
    // a no-op when the guard feature is off) — but note every `return` inside
    // now yields the block's `Response`, not the handler's: a new early-return
    // added below still exits the handler (via `response`) and simply carries
    // the annotate header too, which is the intended behaviour.
    let response: Response = async {
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
    // First entitlement 400 (LAB-4729): its presence spends the one re-send;
    // returned if nothing else can serve the request.
    let mut entitlement_resp: Option<(EndpointIdx, Option<Response>)> = None;
    // OpenAI-shape body, built lazily on the first OpenAI-endpoint attempt
    // and reused across rotations/retries (LAB-716). Lazy so requests served
    // entirely by Anthropic endpoints — the common case — never pay for the
    // translation.
    let mut openai_fallback_body: Option<FallbackBody> = None;
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
        // Seeded with an entitlement-refusing endpoint (LAB-4729): after a
        // 529/transient round it must not be re-picked and spend the re-send.
        let mut skip: Vec<EndpointIdx> = entitlement_resp.iter().map(|(i, _)| *i).collect();
        let mut saw_529 = false;
        let mut saw_transient = false;
        for _attempt in 0..n {
            // Pick the next endpoint and dispatch by protocol:
            // `forward_anthropic` (Anthropic) or `try_fallback_upstream`
            // (OpenAI). Both return a `ForwardOutcome` so the shared
            // round-gated policy in `apply_round_outcome` covers both.
            let (outcome, picked_idx): (ForwardOutcome, EndpointIdx) = match state
                .pick_endpoint_for_client(affinity, &model, &skip, &client_id)
                .await
            {
                Some(i) => {
                    let ep = &state.endpoints[i];
                    match ep.protocol {
                        Protocol::Anthropic => {
                            let out = forward_anthropic(
                                &state,
                                &parts,
                                &body_bytes,
                                &oauth_body_bytes,
                                is_fast_mode,
                                ep,
                                i,
                                &req_id,
                                &client_id,
                                &client_ver,
                                &client_ip,
                                &agent_id,
                                &session_id,
                                &model,
                                fp.as_deref(),
                                affinity,
                                request_start,
                            )
                            .await;
                            (out, i)
                        }
                        Protocol::OpenAI => {
                            let out = match openai_fallback_body
                                .get_or_insert_with(|| build_openai_fallback_body(&body_bytes))
                            {
                                FallbackBody::Ready { body, is_streaming } => {
                                    try_fallback_upstream(
                                        &state,
                                        body,
                                        &req_id,
                                        &client_id,
                                        &client_ip,
                                        &agent_id,
                                        &session_id,
                                        &model,
                                        i,
                                        request_start,
                                        true,
                                        *is_streaming,
                                    )
                                    .await
                                }
                                FallbackBody::Unparseable => {
                                    warn!(
                                            req_id,
                                            upstream = ep.name,
                                            "fallback: request JSON unparseable, skipping OpenAI endpoint"
                                        );
                                    ROTATE
                                }
                                FallbackBody::Untranslatable(msg) => {
                                    warn!(
                                        req_id,
                                        upstream = ep.name,
                                        error = %msg,
                                        "fallback: request not representable in OpenAI format"
                                    );
                                    // Terminal, not a retry: the request itself
                                    // is the problem, so rotating would fail the
                                    // same way on every endpoint.
                                    ForwardOutcome::Done(Box::new(untranslatable_request_response(
                                        msg,
                                    )))
                                }
                            };
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
                &mut entitlement_resp,
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
    // the rejecting endpoints (LAB-941). An entitlement 400 whose one re-send
    // found nothing else to try is returned the same way (LAB-4729): the
    // caller sees why, not a synthetic 429. Present only if no later attempt
    // answered — see `apply_round_outcome`.
    if !last_saw_529 && !last_saw_transient {
        if let Some(resp) = entitlement_resp.and_then(|(_, r)| r).or(model_unsupported_resp) {
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
    exhaustion_response(&state, last_saw_transient, last_saw_529)
    }
    .await;

    #[cfg(feature = "guard")]
    let response = stamp_guard_findings(response, guard_annotate);

    response
}

#[cfg(test)]
mod tests;
