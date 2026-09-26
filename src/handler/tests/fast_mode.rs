use super::*;

// ── LAB-2675: a fast-mode 429 must not hard-limit the whole account ──

/// Two Anthropic endpoints: priority-0 always 429s, priority-1 always 200s.
/// Speed-agnostic — both the fast-mode test and its standard-speed control
/// drive it; only the request body differs.
/// Priority (not affinity hashing) forces the first attempt onto the 429
/// endpoint, so the healthy endpoint's hit count is a clean "did we rotate?"
/// probe. Same trick as `openai_429_sets_cooldown_and_next_request_skips_endpoint`.
async fn two_endpoint_429_then_healthy() -> (
    Arc<AppState>,
    SocketAddr,
    std::sync::Arc<std::sync::atomic::AtomicUsize>,
) {
    let (limited_url, _limited_hits) =
        spawn_status_then_ok_upstream(usize::MAX, HEAD_429_RETRY_AFTER_7, ANTHROPIC_OK_BODY).await;
    let (healthy_url, healthy_hits) = spawn_flaky_upstream(0, ANTHROPIC_OK_BODY).await;

    let mut limited = make_endpoint("limited", Protocol::Anthropic);
    limited.base_url = limited_url;
    let mut healthy = make_endpoint("healthy", Protocol::Anthropic);
    healthy.base_url = healthy_url;
    healthy.priority = 1;

    let state = test_state_with(vec![limited, healthy]);
    let addr = serve(build_router(state.clone())).await;
    (state, addr, healthy_hits)
}

/// AC-1: fast mode bills against a rate bucket that is NOT the account's
/// standard 5h/7d window, so a fast-mode 429 must reach the caller with
/// upstream's `retry-after` and leave the account in rotation. Cooling the
/// account here is the LAB-2669 security finding: one client looping
/// `speed: "fast"` would deny standard traffic for every other client.
#[tokio::test]
async fn fast_mode_429_forwards_to_client_and_leaves_account_alone() {
    use std::sync::atomic::Ordering;
    let (state, addr, healthy_hits) = two_endpoint_429_then_healthy().await;

    let resp = reqwest::Client::new()
        .post(format!("http://{addr}/v1/messages"))
        .header("content-type", "application/json")
        .body(
            r#"{"model":"test","max_tokens":1,"speed":"fast","messages":[{"role":"user","content":"hi"}]}"#,
        )
        .send()
        .await
        .unwrap();

    assert_eq!(
        resp.status(),
        reqwest::StatusCode::TOO_MANY_REQUESTS,
        "a fast-mode 429 must be forwarded, not swallowed into a rotation"
    );
    assert_eq!(
        resp.headers()
            .get("retry-after")
            .and_then(|v| v.to_str().ok()),
        Some("7"),
        "upstream's retry-after must reach the client — it is how the caller backs off"
    );

    let info = state.endpoints[0].rate_info.read().await;
    assert!(
        info.hard_limited_until.is_none(),
        "a fast-bucket 429 must not cool the account for STANDARD traffic"
    );
    assert_eq!(
        (info.remaining_requests, info.remaining_tokens),
        (None, None),
        "a fast-bucket 429 must not poison the account's standard headroom view"
    );
    drop(info);

    assert_eq!(
        healthy_hits.load(Ordering::SeqCst),
        0,
        "a fast-mode 429 must not rotate — rotating just sweeps every account's fast bucket"
    );

    // AC-3: the forwarded 429 is silent unless it is counted, and a looping
    // client is exactly what an operator needs to see.
    let m = reqwest::Client::new()
        .get(format!("http://{addr}/metrics"))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(
        m.contains("anthropic_fast_mode_429_total{account=\"limited\"} 1"),
        "fast-mode 429s must be visible on /metrics per account:\n{m}"
    );
}

/// AC-2 negative control: the same upstream 429, on a body that does NOT ask
/// for fast mode, keeps today's behaviour — hard-limit the account and rotate.
/// Both the absent field and an explicit `"standard"` are standard speed.
///
/// (The ticket suggested extending `try_fallback_upstream_rotates_on_429`;
/// that test drives the OpenAI fallback upstream, which cannot express
/// `speed` at all, so it cannot control for this change. The control has to
/// run through the same Anthropic handler path as AC-1.)
#[tokio::test]
async fn standard_speed_429_still_cools_account_and_rotates() {
    use std::sync::atomic::Ordering;
    for body in [
        r#"{"model":"test","max_tokens":1,"messages":[{"role":"user","content":"hi"}]}"#,
        r#"{"model":"test","max_tokens":1,"speed":"standard","messages":[{"role":"user","content":"hi"}]}"#,
    ] {
        let (state, addr, healthy_hits) = two_endpoint_429_then_healthy().await;

        let resp = reqwest::Client::new()
            .post(format!("http://{addr}/v1/messages"))
            .header("content-type", "application/json")
            .body(body)
            .send()
            .await
            .unwrap();

        assert_eq!(
            resp.status(),
            reqwest::StatusCode::OK,
            "a standard-speed 429 must still rotate to the healthy account: {body}"
        );
        assert!(
            healthy_hits.load(Ordering::SeqCst) >= 1,
            "the request must reach the priority-1 endpoint: {body}"
        );

        let info = state.endpoints[0].rate_info.read().await;
        assert!(
            info.hard_limited_until.is_some(),
            "a standard-speed 429 must still hard-limit the account: {body}"
        );
        assert_eq!(
            (info.remaining_requests, info.remaining_tokens),
            (Some(0), Some(0)),
            "a standard-speed 429 must still poison remaining_* : {body}"
        );
    }
}

/// Panel finding (CRIT): the fast-mode exemption must NOT swallow a transient
/// BURST 429 — `x-should-retry` with no `retry-after` and no rate headers.
/// Burst limits are per-minute RPM/concurrency on the ACCOUNT, not on a rate
/// bucket, so they are real evidence about the account whatever speed was
/// asked for. Exempting it would leave the account pinned: standard traffic
/// routed to it would burst-429 and hard-limit it anyway. The caller still
/// gets `x-should-retry` as its transient hint.
#[tokio::test]
async fn fast_mode_burst_429_still_backs_off_and_rotates() {
    use std::sync::atomic::Ordering;
    const HEAD_429_BURST: &str = "HTTP/1.1 429 Too Many Requests\r\nx-should-retry: true\r\ncontent-type: application/json\r\nconnection: close\r\n\r\n";
    let (limited_url, _) =
        spawn_status_then_ok_upstream(usize::MAX, HEAD_429_BURST, ANTHROPIC_OK_BODY).await;
    let (healthy_url, healthy_hits) = spawn_flaky_upstream(0, ANTHROPIC_OK_BODY).await;
    let mut limited = make_endpoint("limited", Protocol::Anthropic);
    limited.base_url = limited_url;
    let mut healthy = make_endpoint("healthy", Protocol::Anthropic);
    healthy.base_url = healthy_url;
    healthy.priority = 1;
    let state = test_state_with(vec![limited, healthy]);
    let addr = serve(build_router(state.clone())).await;

    let resp = reqwest::Client::new()
        .post(format!("http://{addr}/v1/messages"))
        .header("content-type", "application/json")
        .body(
            r#"{"model":"test","max_tokens":1,"speed":"fast","messages":[{"role":"user","content":"hi"}]}"#,
        )
        .send()
        .await
        .unwrap();

    assert_eq!(
        resp.status(),
        reqwest::StatusCode::OK,
        "a burst 429 must still rotate, even on a fast-mode request"
    );
    assert!(
        healthy_hits.load(Ordering::SeqCst) >= 1,
        "the request must reach the priority-1 endpoint"
    );

    let info = state.endpoints[0].rate_info.read().await;
    assert_eq!(
        info.consecutive_burst_429s, 1,
        "the burst backoff ladder must still engage on a fast-mode request"
    );
    assert!(
        info.hard_limited_until.is_some(),
        "a burst 429 must still apply its (short) cooldown"
    );
    assert_eq!(
        (info.remaining_requests, info.remaining_tokens),
        (None, None),
        "a burst 429 must not poison remaining_* — it is not capacity exhaustion"
    );
}

// ── Fast-mode 200 must not corrupt the account's headroom view (LAB-2693) ──

/// A fast-mode 200's raw wire, exactly as captured in production on
/// 2026-09-02 (LAB-2693 / anthropic-lb#163): the unified
/// headers describe the FAST POOL — `representative-claim: overage`,
/// `overage-in-use: true`, the `overage-*` block, a `unified-reset` ~29d out,
/// a `fallback-percentage` — and carry NO `five_hour`/5h/7d account claim.
/// Body is a normal Anthropic message so usage extraction still parses. The
/// whole response (headers + body) rides in the `bad_head` slot with
/// `connection: close`, so the body is delimited by EOF — same mechanism the
/// burst-429 test uses for a header-only response.
const HEAD_200_FAST_POOL: &str = "HTTP/1.1 200 OK\r\n\
    content-type: application/json\r\n\
    anthropic-ratelimit-unified-status: allowed\r\n\
    anthropic-ratelimit-unified-representative-claim: overage\r\n\
    anthropic-ratelimit-unified-overage-status: allowed\r\n\
    anthropic-ratelimit-unified-overage-in-use: true\r\n\
    anthropic-ratelimit-unified-overage-utilization: 0.0\r\n\
    anthropic-ratelimit-unified-overage-reset: 1790812800\r\n\
    anthropic-ratelimit-unified-reset: 1790812800\r\n\
    anthropic-ratelimit-unified-fallback-percentage: 0.5\r\n\
    connection: close\r\n\r\n\
    {\"id\":\"msg_1\",\"type\":\"message\",\"role\":\"assistant\",\"content\":[{\"type\":\"text\",\"text\":\"hi\"}],\"model\":\"test\",\"stop_reason\":\"end_turn\",\"usage\":{\"input_tokens\":1,\"output_tokens\":1}}";

/// AC-1 + AC-3: one `speed:"fast"` request whose upstream 200 carries the
/// fast-pool unified headers must leave the serving account's standard
/// headroom view exactly as the last standard response left it, and must NOT
/// demote the account out of rotation. The fast-pool headers would otherwise
/// flip `representative_claim` → `overage` and `overage_in_use` → true, which
/// `routing_candidates` turns into a `+overage_penalty` demotion.
#[tokio::test]
async fn fast_mode_200_leaves_account_headroom_untouched() {
    let (url, _hits) =
        spawn_status_then_ok_upstream(usize::MAX, HEAD_200_FAST_POOL, ANTHROPIC_OK_BODY).await;
    let mut acct = make_endpoint("acct", Protocol::Anthropic);
    acct.base_url = url;
    let state = test_state_with(vec![acct]);

    // Seed the account's standard view from a prior standard response, mirroring
    // the live account state before the fast request landed.
    let seeded_reset_5h = AppState::now_epoch() + 3000;
    {
        let mut info = state.endpoints[0].rate_info.write().await;
        info.representative_claim = Some("five_hour".to_string());
        info.utilization_5h = Some(0.31);
        info.reset_5h = Some(seeded_reset_5h);
        // overage_* stay at their defaults: not in overage.
    }

    let addr = serve(build_router(state.clone())).await;
    let resp = reqwest::Client::new()
        .post(format!("http://{addr}/v1/messages"))
        .header("content-type", "application/json")
        .body(
            r#"{"model":"test","max_tokens":8,"speed":"fast","messages":[{"role":"user","content":"hi"}]}"#,
        )
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    // AC-1: every listed field equals its pre-request value.
    let info = state.endpoints[0].rate_info.read().await;
    assert_eq!(
        info.representative_claim.as_deref(),
        Some("five_hour"),
        "fast-pool `overage` claim must not overwrite the account's standard claim"
    );
    assert!(
        !info.overage_in_use,
        "a fast 200 must not put the account into overage"
    );
    assert_eq!(info.overage_status, None);
    assert_eq!(info.overage_utilization, None);
    assert_eq!(info.overage_reset, None);
    // The fast 200 carries no 5h header, so these fields have no on-wire datum
    // to overwrite regardless of the guard; asserted per AC-1 to pin the full
    // "standard view untouched" invariant.
    assert_eq!(info.utilization_5h, Some(0.31), "5h utilization untouched");
    assert_eq!(info.reset_5h, Some(seeded_reset_5h), "5h reset untouched");
    drop(info);

    // AC-3: the endpoint is offered at its configured priority (0), with no
    // `overage_penalty` and not sourced from the overage window.
    let candidates = state.routing_candidates("test", &[]).await;
    assert_eq!(candidates.len(), 1, "the one endpoint must be a candidate");
    assert_eq!(
        candidates[0].priority, 0,
        "no overage_penalty — the account stays in standard rotation"
    );
    assert_ne!(
        candidates[0].source, "overage",
        "routing must gate on the standard window, not a phantom overage window"
    );
}

/// AC-2 negative control: the SAME fast-pool 200, on a body that does NOT ask
/// for fast mode (absent, and explicit `"standard"`), ingests the unified
/// headers exactly as today — `overage_in_use` flips true and
/// `representative_claim` becomes `overage`. This is the behaviour the
/// fast-mode guard suppresses; proving it still fires under standard speed is
/// what shows the guard keys on speed and nothing else.
#[tokio::test]
async fn standard_speed_200_still_ingests_unified_headers() {
    for body in [
        r#"{"model":"test","max_tokens":8,"messages":[{"role":"user","content":"hi"}]}"#,
        r#"{"model":"test","max_tokens":8,"speed":"standard","messages":[{"role":"user","content":"hi"}]}"#,
    ] {
        let (url, _hits) =
            spawn_status_then_ok_upstream(usize::MAX, HEAD_200_FAST_POOL, ANTHROPIC_OK_BODY).await;
        let mut acct = make_endpoint("acct", Protocol::Anthropic);
        acct.base_url = url;
        let state = test_state_with(vec![acct]);
        {
            let mut info = state.endpoints[0].rate_info.write().await;
            info.representative_claim = Some("five_hour".to_string());
        }

        let addr = serve(build_router(state.clone())).await;
        let resp = reqwest::Client::new()
            .post(format!("http://{addr}/v1/messages"))
            .header("content-type", "application/json")
            .body(body)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), reqwest::StatusCode::OK, "body: {body}");

        let info = state.endpoints[0].rate_info.read().await;
        assert!(
            info.overage_in_use,
            "standard speed must still ingest overage-in-use: {body}"
        );
        assert_eq!(
            info.representative_claim.as_deref(),
            Some("overage"),
            "standard speed must still ingest the representative claim: {body}"
        );
    }
}

/// `is_burst_429` is the single definition shared by `mark_hard_limited_for`'s
/// backoff ladder and the fast-mode exemption; all three signals must agree
/// before a 429 counts as burst.
#[test]
fn burst_429_needs_should_retry_and_no_capacity_evidence() {
    let hdrs = |pairs: &[(&str, &str)]| {
        let mut h = reqwest::header::HeaderMap::new();
        for (k, v) in pairs {
            h.insert(
                reqwest::header::HeaderName::from_bytes(k.as_bytes()).unwrap(),
                v.parse().unwrap(),
            );
        }
        h
    };
    assert!(is_burst_429(&hdrs(&[("x-should-retry", "true")])));
    assert!(
        !is_burst_429(&hdrs(&[("retry-after", "7")])),
        "no x-should-retry: not a burst limit"
    );
    assert!(
        !is_burst_429(&hdrs(&[("x-should-retry", "true"), ("retry-after", "7")])),
        "retry-after means upstream told us the capacity window, not a burst"
    );
    assert!(
        !is_burst_429(&hdrs(&[
            ("x-should-retry", "true"),
            ("anthropic-ratelimit-unified-status", "rejected")
        ])),
        "rate-limit headers are capacity evidence, not a burst"
    );
}

/// The body predicate itself: only a top-level string `"fast"` counts. A
/// nested `speed`, a non-string, or an unparseable body is standard speed —
/// the fail-safe direction, since guessing "fast" wrongly would suppress a
/// real account cooldown.
#[test]
fn fast_mode_body_predicate() {
    let fast = br#"{"model":"m","speed":"fast"}"#;
    assert!(request_wants_fast_mode(fast));
    assert!(!request_wants_fast_mode(br#"{"model":"m"}"#));
    assert!(!request_wants_fast_mode(
        br#"{"model":"m","speed":"standard"}"#
    ));
    assert!(!request_wants_fast_mode(br#"{"model":"m","speed":true}"#));
    assert!(
        !request_wants_fast_mode(br#"{"model":"m","metadata":{"speed":"fast"}}"#),
        "only the TOP-LEVEL speed field selects the fast rate bucket"
    );
    assert!(
        !request_wants_fast_mode(b"not json"),
        "an unparseable body must fall back to standard — never suppress a cooldown on a guess"
    );
}

// ── LAB-2687: org-level fast-mode entitlement ───────────────────────

/// Exact upstream wire bytes observed 2026-09-02 from a non-entitled org.
const HEAD_400_FAST_MODE: &str = "HTTP/1.1 400 Bad Request\r\ncontent-type: application/json\r\nconnection: close\r\n\r\n{\"type\":\"error\",\"error\":{\"type\":\"invalid_request_error\",\"message\":\"Fast mode is not enabled for your organization. An organization admin must enable this feature.\"}}";
const STANDARD_BODY: &str =
    r#"{"model":"claude-opus-5","max_tokens":1,"messages":[{"role":"user","content":"hi"}]}"#;

/// AC-1: only the org-entitlement 400 matches. The proxy-induced
/// `speed: Extra inputs are not permitted` 400 (beta header stripped, body
/// forwarded — LAB-2669's failure shape), model-not-found 404s and 429s must
/// not: rotating fixes none of those.
#[test]
fn fast_mode_not_enabled_error_detection() {
    let not_enabled = serde_json::json!({
        "type": "error",
        "error": {"type": "invalid_request_error", "message": "Fast mode is not enabled for your organization. An organization admin must enable this feature."}
    });
    assert!(is_fast_mode_not_enabled_error(
        StatusCode::BAD_REQUEST,
        &not_enabled
    ));
    assert!(
        !is_fast_mode_not_enabled_error(StatusCode::TOO_MANY_REQUESTS, &not_enabled),
        "status gate: only a 400 is an entitlement rejection"
    );

    let extra_inputs = serde_json::json!({
        "type": "error",
        "error": {"type": "invalid_request_error", "message": "speed: Extra inputs are not permitted"}
    });
    assert!(
        !is_fast_mode_not_enabled_error(StatusCode::BAD_REQUEST, &extra_inputs),
        "the proxy-induced header/body mismatch 400 is not an entitlement rejection"
    );

    let model_404 = serde_json::json!({
        "type": "error",
        "error": {"type": "not_found_error", "message": "model: claude-nope-1"}
    });
    assert!(!is_fast_mode_not_enabled_error(
        StatusCode::NOT_FOUND,
        &model_404
    ));

    let rate_limited = serde_json::json!({
        "type": "error",
        "error": {"type": "rate_limit_error", "message": "This request would exceed the rate limit for your organization"}
    });
    assert!(!is_fast_mode_not_enabled_error(
        StatusCode::TOO_MANY_REQUESTS,
        &rate_limited
    ));
}

/// The matcher requires an exact match, not a substring — a message
/// that merely CONTAINS the entitlement clause (trailing wording drift, or a
/// client-echoed field name in an unrelated 400) must not match. Substring
/// matching plus an unguarded caller let one crafted request walk and mark
/// every reachable account.
#[test]
fn fast_mode_not_enabled_error_requires_exact_match() {
    let superstring = serde_json::json!({
        "type": "error",
        "error": {"type": "invalid_request_error", "message": "Fast mode is not enabled for your organization. An organization admin must enable this feature. (extra upstream wording)"}
    });
    assert!(
        !is_fast_mode_not_enabled_error(StatusCode::BAD_REQUEST, &superstring),
        "a superstring of the entitlement message must not match — exact match only"
    );
}

/// Integration regression: an entitlement-shaped 400 on a request that
/// never asked for `speed: "fast"` must not be treated as a fast-mode
/// rejection — no account mark, no rotation, the 400 forwards verbatim.
/// Otherwise a client could craft such a 400 (e.g. an unrecognized top-level
/// field literally named the entitlement message, which upstream may echo
/// back) and walk every reachable account, starving all other tenants' fast
/// traffic.
#[tokio::test]
async fn fast_mode_shaped_400_on_standard_request_is_not_marked_or_rotated() {
    use std::sync::atomic::Ordering;
    let (url, hits) = spawn_status_then_ok_upstream(usize::MAX, HEAD_400_FAST_MODE, b"{}").await;
    let state = test_state_with(vec![mk_endpoint_at("only", "sk-ant-api-o", &url)]);
    let addr = serve(build_router(state.clone())).await;

    let resp = reqwest::Client::new()
        .post(format!("http://{addr}/v1/messages"))
        .header("content-type", "application/json")
        .body(STANDARD_BODY)
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::BAD_REQUEST,
        "a standard request must see the upstream 400 verbatim"
    );
    assert!(
        state.fast_mode_disabled_endpoints().is_empty(),
        "an entitlement-shaped 400 on a non-fast request must not mark the account"
    );
    assert_eq!(
        hits.load(Ordering::SeqCst),
        1,
        "must not rotate/retry a non-fast request off an entitlement-shaped 400"
    );
}

/// AC-3/AC-4: a marked endpoint leaves the pool for fast requests only —
/// affinity or not — standard requests still see it, and an expired mark
/// restores it without a restart.
#[tokio::test]
async fn fast_mode_disabled_filters_fast_routing_until_expiry() {
    let state = test_state_with(vec![
        mk_endpoint("a", "sk-ant-api-a"),
        mk_endpoint("b", "sk-ant-api-b"),
    ]);
    assert!(state.fast_mode_disabled_endpoints().is_empty());
    assert_eq!(state.fast_mode_disabled_remaining_secs(0), None);

    state.note_fast_mode_disabled("a", 0);

    assert_eq!(state.fast_mode_disabled_endpoints(), vec![0]);
    for _ in 0..8 {
        assert_eq!(
            state
                .pick_endpoint_for_client(None, "claude-opus-5", &[], "", true)
                .await,
            Some(1),
            "a fast request must never route to the non-entitled endpoint"
        );
    }
    // Session affinity re-buckets too: the sticky hash only sees candidates.
    assert_eq!(
        state
            .pick_endpoint_for_client(Some("client:sess:1"), "claude-opus-5", &[], "", true)
            .await,
        Some(1),
        "an affinity-pinned fast session must migrate off the non-entitled endpoint"
    );
    // A standard request sees the full pool.
    assert_eq!(
        state.routing_candidates("claude-opus-5", &[]).await.len(),
        2,
        "standard requests must keep using the non-entitled endpoint"
    );
    assert!(
        state
            .fast_mode_disabled_remaining_secs(0)
            .is_some_and(|s| s > FAST_MODE_DISABLED_TTL.as_secs() - 5),
        "/_stats must expose the remaining TTL"
    );
    assert!(!state.pool_cannot_serve("claude-opus-5", true, None));

    // Mark the other account too: the FAST pool is now unservable, the
    // standard pool is untouched.
    state.note_fast_mode_disabled("b", 1);
    assert!(state.pool_cannot_serve("claude-opus-5", true, None));
    assert!(
        !state.pool_cannot_serve("claude-opus-5", false, None),
        "fast-mode marks must not count against a standard request"
    );

    // Force-expire the entries: both endpoints rejoin the fast pool, and the
    // mark counter survives expiry for the metric.
    for expiry in state.fast_mode_disabled.lock().unwrap().values_mut() {
        *expiry = Instant::now();
    }
    assert!(
        state.fast_mode_disabled_endpoints().is_empty(),
        "expired entry must not filter routing"
    );
    assert_eq!(state.fast_mode_disabled_remaining_secs(0), None);
    assert_eq!(
        state.endpoints[0]
            .fast_mode_disabled_total
            .load(std::sync::atomic::Ordering::Relaxed),
        1
    );
}

/// A poisoned fast-mode cache keeps marking and filtering: skipping on `Err`
/// would stop both for the life of the process.
#[tokio::test]
async fn poisoned_fast_mode_disabled_lock_keeps_marking() {
    let state = test_state_with(vec![
        mk_endpoint("a", "sk-ant-api-a"),
        mk_endpoint("b", "sk-ant-api-b"),
    ]);
    poison(&state.fast_mode_disabled);

    state.note_fast_mode_disabled("a", 0);

    assert!(!state.fast_mode_disabled.is_poisoned());
    assert_eq!(state.fast_mode_disabled_endpoints(), vec![0]);
    assert!(state.fast_mode_disabled_remaining_secs(0).is_some());
}

/// Mixed negative caches: one account can't serve the model, the other can't
/// serve fast. A fast request has nowhere to go (upstream error, not a 429);
/// a standard request still has the fast-disabled account.
#[test]
fn pool_cannot_serve_unions_negative_caches() {
    let state = test_state_with(vec![
        mk_endpoint("a", "sk-ant-api-a"),
        mk_endpoint("b", "sk-ant-api-b"),
    ]);
    state.note_model_unsupported("a", 0, "claude-opus-5");
    state.note_fast_mode_disabled("b", 1);
    assert!(state.pool_cannot_serve("claude-opus-5", true, None));
    assert!(!state.pool_cannot_serve("claude-opus-5", false, None));
    assert!(!state.pool_cannot_serve("claude-sonnet-5", true, None));
}

/// An OpenAI-protocol endpoint never accrues a fast-mode
/// mark (it has no org entitlement to reject) and its request translation
/// drops `speed` entirely, so routing a fast request there would silently
/// serve it at standard speed. It must never be a fast-request candidate;
/// standard requests still see it.
#[tokio::test]
async fn fast_mode_excludes_openai_protocol_endpoints() {
    let state = test_state_with(vec![
        make_endpoint("anthropic-only", Protocol::Anthropic),
        make_endpoint("openai-fallback", Protocol::OpenAI),
    ]);
    for _ in 0..8 {
        assert_eq!(
            state
                .pick_endpoint_for_client(None, "claude-opus-5", &[], "", true)
                .await,
            Some(0),
            "a fast request must never route to an OpenAI-protocol endpoint"
        );
    }
    assert_eq!(
        state.routing_candidates("claude-opus-5", &[]).await.len(),
        2,
        "standard requests must still see the OpenAI-protocol endpoint"
    );
}

/// An Anthropic account fast-disabled plus an OpenAI fallback must be
/// treated as pool-exhausted for a fast request (truthful error) — the
/// OpenAI endpoint can't honor `speed:"fast"` and must not count as
/// "eligible" capacity that masks the exhaustion (that mask is exactly what
/// let a fast request silently downgrade through the fallback instead of
/// getting the truthful rejection).
#[test]
fn pool_cannot_serve_excludes_openai_protocol_for_fast_requests() {
    let state = test_state_with(vec![
        mk_endpoint("anthropic-only", "sk-ant-api-a"),
        make_endpoint("openai-fallback", Protocol::OpenAI),
    ]);
    state.note_fast_mode_disabled("anthropic-only", 0);
    assert!(
        state.pool_cannot_serve("claude-opus-5", true, None),
        "an OpenAI fallback must not mask a fully fast-disabled Anthropic pool"
    );
    assert!(
        !state.pool_cannot_serve("claude-opus-5", false, None),
        "the same pool can serve a standard request"
    );
}

/// AC-2 native path: the first entitlement 400 rotates within the request
/// (client sees the 200 from the entitled account), the NEXT fast request
/// skips the non-entitled account outright, and a standard request on the
/// same pool still routes to it.
#[tokio::test]
async fn fast_mode_disabled_rotates_and_next_fast_request_skips_account() {
    use std::sync::atomic::Ordering;
    let (reject_url, reject_hits) =
        spawn_status_then_ok_upstream(usize::MAX, HEAD_400_FAST_MODE, b"{}").await;
    let (ok_url, _h) = spawn_mock_upstream().await;

    let reject = mk_endpoint_at("reject", "sk-ant-api-r", &reject_url);
    let mut healthy = mk_endpoint_at("healthy", "sk-ant-api-h", &ok_url);
    // Priority forces routing to try `reject` first — deterministic without
    // depending on affinity hashing.
    healthy.priority = 1;
    let state = test_state_with(vec![reject, healthy]);
    let addr = serve(build_router(state)).await;

    let client = reqwest::Client::new();
    for _ in 0..2 {
        let resp = client
            .post(format!("http://{addr}/v1/messages"))
            .header("content-type", "application/json")
            .body(FAST_BODY)
            .send()
            .await
            .unwrap();
        assert_eq!(
            resp.status(),
            reqwest::StatusCode::OK,
            "both fast requests must succeed via the entitled account"
        );
    }
    assert_eq!(
        reject_hits.load(Ordering::SeqCst),
        1,
        "the second fast request must skip the non-entitled account, not retry it"
    );

    // AC-6: the mark is visible on /_stats and /metrics.
    let stats: serde_json::Value = client
        .get(format!("http://{addr}/_stats"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let eps = stats["endpoints"].as_array().unwrap();
    assert!(
        eps[0]["fast_mode_disabled_remaining_secs"]
            .as_u64()
            .is_some(),
        "marked account must report remaining secs, got: {}",
        eps[0]
    );
    assert!(
        eps[1]["fast_mode_disabled_remaining_secs"].is_null(),
        "unmarked account must report null, got: {}",
        eps[1]
    );
    let metrics = client
        .get(format!("http://{addr}/metrics"))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(
        metrics.contains("anthropic_fast_mode_disabled_total{account=\"reject\"} 1"),
        "one increment per entitlement 400, got:\n{metrics}"
    );
    assert!(
        metrics.contains("anthropic_fast_mode_disabled_total{account=\"healthy\"} 0"),
        "unmarked account must still emit a zero counter, got:\n{metrics}"
    );

    // A standard request is unaffected by the mark: priority sends it to
    // `reject`, same as before. The entitlement-shaped 400 is
    // gated on `is_fast_mode`, so a standard request that draws it does NOT
    // rotate — it forwards the 400 verbatim (matches
    // `fast_mode_shaped_400_on_standard_request_is_not_marked_or_rotated`).
    let resp = client
        .post(format!("http://{addr}/v1/messages"))
        .header("content-type", "application/json")
        .body(STANDARD_BODY)
        .send()
        .await
        .unwrap();
    assert_eq!(
        reject_hits.load(Ordering::SeqCst),
        2,
        "a standard request must still route to the fast-mode-disabled account"
    );
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::BAD_REQUEST,
        "a standard request must see the entitlement-shaped 400 verbatim, not be rotated off it"
    );
}

/// Panel finding: a rejection on ONE account while the rest of the pool is
/// merely rate-limited is a rate-limited pool. The stashed non-retryable 400
/// must not surface — SDKs would give up on a request that will succeed
/// once the cooldown lifts.
#[tokio::test]
async fn fast_mode_rejection_plus_rate_limited_pool_stays_retryable() {
    let (reject_url, _hits) =
        spawn_status_then_ok_upstream(usize::MAX, HEAD_400_FAST_MODE, b"{}").await;
    let state = test_state_with(vec![
        mk_endpoint_at("reject", "sk-ant-api-r", &reject_url),
        mk_endpoint("limited", "sk-ant-api-l"),
    ]);
    state.endpoints[1]
        .rate_info
        .write()
        .await
        .hard_limited_until = Some(Instant::now() + Duration::from_secs(60));
    let addr = serve(build_router(state)).await;

    let resp = reqwest::Client::new()
        .post(format!("http://{addr}/v1/messages"))
        .header("content-type", "application/json")
        .body(FAST_BODY)
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::TOO_MANY_REQUESTS,
        "one non-entitled account plus a rate-limited rest must stay retryable, not 400"
    );
}

/// Model-shaped twin of `fast_mode_rejection_plus_rate_limited_pool_stays_retryable`:
/// the same `pool_cannot_serve` gate governs the pre-existing LAB-941
/// model-unsupported rejection, not just the new fast-mode one. A rejection on
/// ONE account while the rest of the pool is merely rate-limited (never
/// attempted, so never negative-cached) must stay retryable, not surface the
/// stashed 404 — a deliberate generalisation of LAB-941, not a regression.
#[tokio::test]
async fn model_unsupported_rejection_plus_rate_limited_pool_stays_retryable() {
    let (reject_url, _hits) =
        spawn_status_then_ok_upstream(usize::MAX, HEAD_404_MODEL, b"{}").await;
    let state = test_state_with(vec![
        mk_endpoint_at("reject", "sk-ant-api-r", &reject_url),
        mk_endpoint("limited", "sk-ant-api-l"),
    ]);
    state.endpoints[1]
        .rate_info
        .write()
        .await
        .hard_limited_until = Some(Instant::now() + Duration::from_secs(60));
    let addr = serve(build_router(state)).await;

    let resp = reqwest::Client::new()
        .post(format!("http://{addr}/v1/messages"))
        .header("content-type", "application/json")
        .body(
            r#"{"model":"claude-nope-1","max_tokens":1,"messages":[{"role":"user","content":"hi"}]}"#,
        )
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::TOO_MANY_REQUESTS,
        "one model-unsupported account plus a rate-limited rest must stay retryable, not 404"
    );
}

/// AC-5: when NO eligible account's org has fast mode, the upstream's own 400
/// surfaces — not a synthetic 429 that invites retries. A SECOND fast request
/// hits the warm negative cache (pool empties before any forward, nothing is
/// stashed) and must still get the same 400, replayed, without touching the
/// upstream again.
#[tokio::test]
async fn fast_mode_disabled_everywhere_returns_upstream_400_not_429() {
    use std::sync::atomic::Ordering;
    let (url, hits) = spawn_status_then_ok_upstream(usize::MAX, HEAD_400_FAST_MODE, b"{}").await;
    let state = test_state_with(vec![mk_endpoint_at("only", "sk-ant-api-o", &url)]);
    let addr = serve(build_router(state)).await;

    let client = reqwest::Client::new();
    for pass in [
        "cold cache (real upstream 400)",
        "warm cache (replayed 400)",
    ] {
        let resp = client
            .post(format!("http://{addr}/v1/messages"))
            .header("content-type", "application/json")
            .body(FAST_BODY)
            .send()
            .await
            .unwrap();
        assert_eq!(
            resp.status(),
            reqwest::StatusCode::BAD_REQUEST,
            "{pass}: fast mode disabled everywhere must return 400, not 429"
        );
        let body: serde_json::Value = resp.json().await.unwrap();
        assert_eq!(
            body.pointer("/error/type").and_then(|v| v.as_str()),
            Some("invalid_request_error"),
            "{pass}: envelope type, got: {body}"
        );
        assert_eq!(
            body.pointer("/error/message").and_then(|v| v.as_str()),
            Some(FAST_MODE_NOT_ENABLED_MSG),
            "{pass}: message must be the upstream's own text, got: {body}"
        );
    }
    assert_eq!(
        hits.load(Ordering::SeqCst),
        1,
        "the warm-cache request must not touch the upstream at all"
    );
}

/// Warm-path attribution: the synthesized exhaustion error must name the
/// cause whose removal would unblock the request. The one endpoint that
/// serves the model carries BOTH marks; the other fast-disabled endpoint's
/// allow-list never served it — dropping `speed: "fast"` would not help, so
/// the reply is the model 404, not the fast-mode 400 (a fast-mark filter by
/// `serves_model` alone would still say 400 here).
#[tokio::test]
async fn warm_path_fast_request_names_model_not_fast_when_fast_mark_is_ineligible() {
    let mut haiku_only = mk_endpoint("b", "sk-ant-api-b");
    haiku_only.models = vec!["claude-haiku-*".to_string()];
    let state = test_state_with(vec![mk_endpoint("a", "sk-ant-api-a"), haiku_only]);
    state.note_model_unsupported("a", 0, "claude-opus-5");
    state.note_fast_mode_disabled("a", 0);
    state.note_fast_mode_disabled("b", 1);
    let addr = serve(build_router(state)).await;

    let resp = reqwest::Client::new()
        .post(format!("http://{addr}/v1/messages"))
        .header("content-type", "application/json")
        .body(FAST_BODY)
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::NOT_FOUND,
        "fast mark on an endpoint that never served the model must not claim the rejection"
    );
    let body = resp.text().await.unwrap();
    assert!(
        body.contains("not_found_error") && body.contains("claude-opus-5"),
        "error body must carry the model rejection, got: {body}"
    );
}
