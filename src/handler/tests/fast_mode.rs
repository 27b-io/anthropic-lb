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
