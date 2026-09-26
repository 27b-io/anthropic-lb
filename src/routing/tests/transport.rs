use super::*;

// ── #70: transport circuit-breaker for persistently-dead endpoints ──

/// A persistently-dead endpoint must leave the routing pool after
/// TRANSPORT_FAILURE_THRESHOLD consecutive transport failures, and the
/// session must migrate ONCE to a healthy endpoint instead of paying the
/// affinity tax (two connect stalls) on every request. The dead endpoint
/// sits at priority 0 so routing MUST pick it until the breaker opens —
/// affinity cannot dodge it — making the hit counters deterministic.
#[tokio::test]
async fn dead_endpoint_circuit_breaks_and_session_migrates_once() {
    use std::sync::atomic::Ordering;
    let (dead_url, dead_hits) = spawn_flaky_upstream(usize::MAX, ANTHROPIC_OK_BODY).await;
    let (ok_url, ok_hits) = spawn_flaky_upstream(0, ANTHROPIC_OK_BODY).await;
    let dead = mk_endpoint_at("dead", "sk-ant-api-aaa", &dead_url); // priority 0
    let mut ok = mk_endpoint_at("ok", "sk-ant-api-bbb", &ok_url);
    ok.priority = 1;
    let state = test_state_with(vec![dead, ok]);
    let probe = state.clone();
    let addr = serve(build_router(state)).await;

    let client = reqwest::Client::new();
    for i in 0..4 {
        let resp = client
            .post(format!("http://{addr}/v1/messages"))
            .header("content-type", "application/json")
            .header("x-client-id", "sticky")
            .header("x-session-id", "sticky")
            .body(r#"{"model":"test","max_tokens":1,"messages":[{"role":"user","content":"hi"}]}"#)
            .send()
            .await
            .unwrap();
        assert_eq!(
            resp.status(),
            reqwest::StatusCode::OK,
            "request {i} must succeed via the healthy endpoint"
        );
    }

    // req1: round 0 + round 1 fail (2 hits); req2: round 0 fails (3rd hit,
    // breaker opens). req3/req4 must NOT touch the dead endpoint at all.
    let (d, o) = (
        dead_hits.load(Ordering::SeqCst),
        ok_hits.load(Ordering::SeqCst),
    );
    assert_eq!(
        d, 3,
        "dead endpoint must stop being dialed once the breaker opens (dead_hits={d})"
    );
    assert_eq!(
        o, 4,
        "every request must be served by the healthy endpoint exactly once (ok_hits={o})"
    );

    // Breaker is transport state, NOT rate-limit state.
    let info = probe.endpoints[0].rate_info.read().await;
    assert!(
        info.transport_unhealthy_until.is_some(),
        "breaker must be open on the dead endpoint"
    );
    assert!(
        info.hard_limited_until.is_none(),
        "transport breaker must stay independent of the 429 hard-limit path"
    );
}

/// A recovered endpoint re-enters the pool after the cooldown window, and a
/// successful forward clears the failure counter and the breaker.
#[tokio::test]
async fn circuit_broken_endpoint_reenters_after_cooldown() {
    use std::sync::atomic::Ordering;
    // Dead for exactly 3 connections (the breaker threshold), then healthy.
    let (flaky_url, flaky_hits) = spawn_flaky_upstream(3, ANTHROPIC_OK_BODY).await;
    let (ok_url, ok_hits) = spawn_flaky_upstream(0, ANTHROPIC_OK_BODY).await;
    let flaky = mk_endpoint_at("flaky", "sk-ant-api-aaa", &flaky_url); // priority 0
    let mut ok = mk_endpoint_at("ok", "sk-ant-api-bbb", &ok_url);
    ok.priority = 1;
    let state = Arc::new(AppState {
        endpoints: vec![flaky, ok],
        transport_cooldown: Duration::from_millis(500),
        ..test_state_base()
    });
    let probe = state.clone();
    let addr = serve(build_router(state)).await;

    let client = reqwest::Client::new();
    let send = |i: u32| {
        let client = client.clone();
        async move {
            let resp = client
                    .post(format!("http://{addr}/v1/messages"))
                    .header("content-type", "application/json")
                    .body(r#"{"model":"test","max_tokens":1,"messages":[{"role":"user","content":"hi"}]}"#)
                    .send()
                    .await
                    .unwrap();
            assert_eq!(
                resp.status(),
                reqwest::StatusCode::OK,
                "request {i} must succeed"
            );
        }
    };

    // req1 (2 failures) + req2 (3rd failure → breaker opens) + req3 (skips
    // the broken endpoint entirely).
    for i in 1..=3 {
        send(i).await;
    }
    assert_eq!(flaky_hits.load(Ordering::SeqCst), 3);
    assert_eq!(ok_hits.load(Ordering::SeqCst), 3);

    // Let the cooldown elapse; the endpoint (now healthy) must re-enter at
    // its priority-0 slot and serve the next request itself.
    tokio::time::sleep(Duration::from_millis(700)).await;
    send(4).await;
    assert_eq!(
        flaky_hits.load(Ordering::SeqCst),
        4,
        "recovered endpoint must re-enter the pool after the cooldown"
    );
    assert_eq!(
        ok_hits.load(Ordering::SeqCst),
        3,
        "the fallback endpoint must NOT serve once the recovered endpoint is back"
    );

    // The successful forward must clear the breaker and the counter.
    let info = probe.endpoints[0].rate_info.read().await;
    assert_eq!(
        info.consecutive_transport_failures, 0,
        "failure counter must auto-clear on a successful forward"
    );
    assert!(
        info.transport_unhealthy_until.is_none(),
        "breaker must close on a successful forward"
    );
}

/// The consecutive-failure counter starts a fresh era once the cooldown has
/// elapsed: an expired breaker's failures must not carry over, so re-opening
/// takes a full threshold of NEW evidence.
#[tokio::test]
async fn transport_failure_counter_era_resets_after_cooldown() {
    let state = Arc::new(AppState {
        endpoints: vec![mk_endpoint("a", "sk-ant-api-aaa")],
        transport_cooldown: Duration::ZERO, // breaker expires immediately
        ..test_state_base()
    });
    for _ in 0..TRANSPORT_FAILURE_THRESHOLD {
        state.record_transport_failure(0).await;
    }
    {
        let info = state.endpoints[0].rate_info.read().await;
        assert_eq!(
            info.consecutive_transport_failures,
            TRANSPORT_FAILURE_THRESHOLD
        );
        assert!(
            info.transport_unhealthy_until.is_some(),
            "breaker must open at the threshold"
        );
    }
    // Cooldown (zero) has elapsed → the next failure is the FIRST of a new
    // era, not the fourth of the old one.
    state.record_transport_failure(0).await;
    let info = state.endpoints[0].rate_info.read().await;
    assert_eq!(
        info.consecutive_transport_failures, 1,
        "counter must reset to a fresh era after the cooldown elapses"
    );
    assert!(
        info.transport_unhealthy_until.is_none(),
        "one post-cooldown failure must not re-open the breaker"
    );
}

/// A successful forward clears transport state only — it must not clobber
/// the (independent) 429 hard-limit path.
#[tokio::test]
async fn transport_success_clears_failures_and_leaves_hard_limit_alone() {
    let state = test_state_with(vec![mk_endpoint("a", "sk-ant-api-aaa")]);
    state.record_transport_failure(0).await;
    state.record_transport_failure(0).await;
    {
        let mut info = state.endpoints[0].rate_info.write().await;
        info.hard_limited_until = Some(Instant::now() + Duration::from_secs(3600));
    }
    state.record_transport_success(0).await;
    let info = state.endpoints[0].rate_info.read().await;
    assert_eq!(info.consecutive_transport_failures, 0);
    assert!(info.transport_unhealthy_until.is_none());
    assert!(
        info.hard_limited_until.is_some(),
        "clearing transport health must not clear the 429 hard limit"
    );
}

/// Both protocol branches of `routing_candidates` must exclude a
/// transport-unhealthy endpoint while the breaker is open, and re-admit it
/// once the window has passed.
#[tokio::test]
async fn pick_endpoint_excludes_transport_unhealthy_endpoints() {
    let anthropic = mk_endpoint("anth", "sk-ant-api-aaa");
    let openai = make_endpoint("gw", Protocol::OpenAI);
    let state = test_state_with(vec![anthropic, openai]);
    for ep in &state.endpoints {
        let mut info = ep.rate_info.write().await;
        info.transport_unhealthy_until = Some(Instant::now() + Duration::from_secs(60));
    }
    assert!(
        state.pick_endpoint(None, "", &[]).await.is_none(),
        "both anthropic and openai endpoints must be excluded while unhealthy"
    );
    // Close the OpenAI endpoint's breaker → it must become pickable again.
    {
        let mut info = state.endpoints[1].rate_info.write().await;
        info.transport_unhealthy_until = None;
    }
    assert_eq!(
        state.pick_endpoint(None, "", &[]).await,
        Some(1),
        "a recovered endpoint must be pickable while the other stays excluded"
    );
}

/// The #69 KNOWN GAP: a transport-dead `Protocol::OpenAI` endpoint used to
/// be swallowed to a bare `None` — no transient classification, so the
/// client got a misleading 429. It must exhaust as a retryable 503 exactly
/// like the Anthropic path.
#[tokio::test]
async fn proxy_returns_503_when_openai_endpoint_unreachable() {
    let url = spawn_dead_upstream().await;
    let mut gw = make_endpoint("gw", Protocol::OpenAI);
    gw.base_url = url;
    let state = test_state_with(vec![gw]);
    let addr = serve(build_router(state)).await;
    let resp = reqwest::Client::new()
        .post(format!("http://{addr}/v1/messages"))
        .header("content-type", "application/json")
        .body(r#"{"model":"test","max_tokens":1,"messages":[{"role":"user","content":"hi"}]}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::SERVICE_UNAVAILABLE,
        "a transport-dead OpenAI endpoint must exhaust as a retryable 503, not 429"
    );
    assert!(
        resp.headers().get("retry-after").is_some(),
        "transient exhaustion must carry Retry-After"
    );
}

/// The OpenAI branch must also get #69's round-gated in-place retry: a
/// single-blip OpenAI endpoint recovers to a 200 instead of failing the
/// round (previously: swallowed to `None` → skip → premature 429).
#[tokio::test]
async fn openai_endpoint_rides_out_transient_blip() {
    let (url, _hits) = spawn_flaky_upstream(1, OPENAI_OK_BODY).await;
    let mut gw = make_endpoint("gw", Protocol::OpenAI);
    gw.base_url = url;
    let state = test_state_with(vec![gw]);
    let addr = serve(build_router(state)).await;
    let resp = reqwest::Client::new()
        .post(format!("http://{addr}/v1/messages"))
        .header("content-type", "application/json")
        .body(r#"{"model":"test","max_tokens":1,"messages":[{"role":"user","content":"hi"}]}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::OK,
        "an OpenAI endpoint blip must be retried in place, not fail the request"
    );
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(
        body["type"], "message",
        "response must be translated back to Anthropic format"
    );
}

// ── Task 5: transport-error metric ─────────────────────────────────

/// After a transport failure, `/metrics` must expose a by-kind transport
/// error counter so a flaky egress shows on the dashboard before it becomes
/// client errors. The `{kind=...}` data line only renders once a transport
/// error has been recorded — so this proves the increment, not just a header.
#[tokio::test]
async fn metrics_expose_transport_error_counter() {
    let url = spawn_dead_upstream().await;
    let state = test_state_with(vec![mk_endpoint_at("a", "sk-ant-api-aaa", &url)]);
    let addr = serve(build_router(state)).await;
    let c = reqwest::Client::new();
    let _ = c
        .post(format!("http://{addr}/v1/messages"))
        .header("content-type", "application/json")
        .body(r#"{"model":"test","max_tokens":1,"messages":[{"role":"user","content":"hi"}]}"#)
        .send()
        .await;
    let m = c
        .get(format!("http://{addr}/metrics"))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(
        m.contains("anthropic_upstream_transport_errors_total{kind="),
        "by-kind transport-error counter must appear after a transport failure:\n{m}"
    );
}

/// AC4 (graceful without Redis): a single-instance deployment has no Redis,
/// so `flush_transport_errors` must be a no-op that leaves the local
/// accumulator intact — otherwise local `/metrics` counts would vanish.
#[tokio::test]
async fn flush_transport_errors_noop_without_redis() {
    let state = test_state_with(vec![mk_endpoint("a", "sk-ant-api-aaa")]);
    {
        let mut m = state.upstream_transport_errors.lock().unwrap();
        m.insert("timeout", 3);
        m.insert("connect", 1);
    }
    state.flush_transport_errors().await;
    let m = state.upstream_transport_errors.lock().unwrap();
    assert_eq!(
        m.get("timeout"),
        Some(&3),
        "local accumulator must be retained without redis"
    );
    assert_eq!(m.get("connect"), Some(&1));
}

/// A panicked lock-holder must not wedge the accumulator: recovery clears
/// the poison so the drain, the re-queue-on-redis-failure path, and every
/// `if let Ok` increment/metrics site keep working afterwards.
#[tokio::test]
async fn transport_errors_lock_recovers_and_clears_poison() {
    let state = test_state_with(vec![mk_endpoint("a", "sk-ant-api-aaa")]);
    // Poison the mutex by panicking while holding the guard.
    {
        let state = state.clone();
        std::thread::spawn(move || {
            let _g = state.upstream_transport_errors.lock().unwrap();
            panic!("poison the transport-error mutex");
        })
        .join()
        .unwrap_err();
    }
    assert!(state.upstream_transport_errors.is_poisoned());
    {
        let mut m = state.lock_transport_errors();
        *m.entry("timeout").or_insert(0) += 3;
    }
    assert!(
        !state.upstream_transport_errors.is_poisoned(),
        "recovery must clear the poison, not just bypass it"
    );
    // Plain `lock()` sites (increments, metrics fallback) work again.
    assert_eq!(
        state
            .upstream_transport_errors
            .lock()
            .unwrap()
            .get("timeout"),
        Some(&3)
    );
}

/// AC3 (aggregate exposed): when the sync task has cached a fleet-wide
/// aggregate, `/metrics` must report THOSE counts (cluster-wide), not this
/// replica's local unflushed delta. Proves the Redis view supersedes local.
#[tokio::test]
async fn metrics_prefer_redis_transport_error_aggregate() {
    let state = test_state_with(vec![mk_endpoint("a", "sk-ant-api-aaa")]);
    // A local (unflushed) delta that must be SUPERSEDED by the fleet view.
    state
        .upstream_transport_errors
        .lock()
        .unwrap()
        .insert("timeout", 2);
    // Simulate the sync task having cached a fleet-wide aggregate.
    *state.cluster_info_cache.lock().unwrap() = Some(serde_json::json!({
        "redis_connected": true,
        "replicas_seen": 3,
        "transport_errors": { "timeout": 40, "connect": 5 },
    }));
    let addr = serve(build_router(state)).await;
    let c = reqwest::Client::new();
    let m = c
        .get(format!("http://{addr}/metrics"))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(
        m.contains("anthropic_upstream_transport_errors_total{kind=\"timeout\"} 40"),
        "fleet aggregate (40) must win over local delta (2):\n{m}"
    );
    assert!(
        m.contains("anthropic_upstream_transport_errors_total{kind=\"connect\"} 5"),
        "fleet aggregate must include all kinds:\n{m}"
    );
    assert!(
        !m.contains("anthropic_upstream_transport_errors_total{kind=\"timeout\"} 2"),
        "local delta must not leak once the fleet aggregate is present:\n{m}"
    ); // LAB-4379: the gauge the HELP text names as the scope switch must read 1
       // whenever the fleet total is what is exported.
    assert!(
        m.contains("anthropic_cluster_redis_connected 1"),
        "fleet scope must be advertised by the gauge:\n{m}"
    );
}

/// LAB-466: the `/metrics` local-fallback branch must recover a poisoned
/// `upstream_transport_errors` lock instead of reporting zero. Poisons the
/// mutex directly (not via `lock_transport_errors()`) so this proves the
/// FALLBACK itself recovers, not just the helper in isolation.
#[tokio::test]
async fn metrics_local_fallback_recovers_poisoned_lock() {
    let state = test_state_with(vec![mk_endpoint("a", "sk-ant-api-aaa")]);
    // Seed local counts, then poison the mutex from a panicking thread —
    // bypassing `lock_transport_errors()` so the poison isn't pre-cleared.
    state
        .upstream_transport_errors
        .lock()
        .unwrap()
        .insert("timeout", 40);
    {
        let state = state.clone();
        std::thread::spawn(move || {
            let _g = state.upstream_transport_errors.lock().unwrap();
            panic!("poison the transport-error mutex");
        })
        .join()
        .unwrap_err();
    }
    assert!(state.upstream_transport_errors.is_poisoned());
    // No fleet aggregate cached, so /metrics must take the local fallback path.
    assert!(state.cluster_info_cache.lock().unwrap().is_none());

    let addr = serve(build_router(state)).await;
    let c = reqwest::Client::new();
    let resp = c
        .get(format!("http://{addr}/metrics"))
        .send()
        .await
        .unwrap();
    assert!(
        resp.status().is_success(),
        "metrics endpoint must stay usable after a poisoned lock"
    );
    let m = resp.text().await.unwrap();
    assert!(
        m.contains("anthropic_upstream_transport_errors_total{kind=\"timeout\"} 40"),
        "poisoned local counts must survive the fallback, not be reported as zero:\n{m}"
    );
}
