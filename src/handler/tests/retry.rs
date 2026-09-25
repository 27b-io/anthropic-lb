use super::*;

/// Raw-TCP upstream that RSTs its FIRST connection then serves a valid
/// Anthropic 200 on every later connection — a sub-second egress blip.
async fn spawn_blip_upstream() -> (String, std::sync::Arc<std::sync::atomic::AtomicUsize>) {
    spawn_flaky_upstream(1, ANTHROPIC_OK_BODY).await
}

// ── Task 2: round-gated transient backoff-retry ─────────────────────

/// A transient blip that recovers must surface as 200, not a 429-exhausted.
/// Today (529-only backoff) the single round breaks straight to 429.
#[tokio::test]
async fn proxy_rides_out_transient_upstream_blip() {
    let (url, _hits) = spawn_blip_upstream().await;
    let state = test_state_with(vec![mk_endpoint_at("a", "sk-ant-api-aaa", &url)]);
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
        "a transient blip that recovers must surface as 200, not 429-exhausted"
    );
}

/// Option B: round 0 must RE-TRY the affinity/cache-warm endpoint in place,
/// NOT rotate to a cold endpoint. Two distinct upstreams both blip-then-200;
/// whichever affinity picks serves after the backoff, and the OTHER endpoint
/// must stay at ZERO hits. A flat `push_skip:true` would rotate on round 0,
/// hitting both. (Mutation-test: delete the `retry_round==0` guard → fails.)
#[tokio::test]
async fn transient_blip_retries_warm_endpoint_not_rotates() {
    use std::sync::atomic::Ordering;
    let (url_a, a_hits) = spawn_blip_upstream().await;
    let (url_b, b_hits) = spawn_blip_upstream().await;
    let state = test_state_with(vec![
        mk_endpoint_at("a", "sk-ant-api-aaa", &url_a),
        mk_endpoint_at("b", "sk-ant-api-bbb", &url_b),
    ]);
    let addr = serve(build_router(state)).await;
    let resp = reqwest::Client::new()
        .post(format!("http://{addr}/v1/messages"))
        .header("content-type", "application/json")
        // identity headers → deterministic (non-round-robin) affinity, so
        // round 0 and round 1 pick the SAME endpoint.
        .header("x-client-id", "sticky")
        .header("x-session-id", "sticky")
        .body(r#"{"model":"test","max_tokens":1,"messages":[{"role":"user","content":"hi"}]}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let (a, b) = (a_hits.load(Ordering::SeqCst), b_hits.load(Ordering::SeqCst));
    assert!(
        a == 0 || b == 0,
        "round-0 transient must NOT rotate to a cold endpoint (a_hits={a}, b_hits={b})"
    );
    assert!(
        a >= 2 || b >= 2,
        "the warm endpoint should be retried after the blip (a_hits={a}, b_hits={b})"
    );
}

/// The openai_chat_handler retry loop must ride out a transient blip too —
/// proves it is wired to the same shared round-gated helper as proxy_handler.
#[tokio::test]
async fn openai_chat_rides_out_transient_upstream_blip() {
    let (url, _hits) = spawn_blip_upstream().await;
    let state = test_state_with(vec![mk_endpoint_at("a", "sk-ant-api-aaa", &url)]);
    let addr = serve(build_router(state)).await;
    let resp = reqwest::Client::new()
        .post(format!("http://{addr}/v1/chat/completions"))
        .header("content-type", "application/json")
        .body(r#"{"model":"test","max_tokens":1,"messages":[{"role":"user","content":"hi"}]}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::OK,
        "openai_chat_handler must also ride out a transient blip via the shared helper"
    );
}

// ── Task 3: transient exhaustion returns a retryable 503 ────────────

/// When every endpoint transport-fails through all backoff rounds, the
/// client must get a retryable `503 + Retry-After`, not a `429` (which
/// reads as account rate-limiting). Task 2 still returned 429 here.
#[tokio::test]
async fn proxy_returns_503_when_upstream_unreachable_transiently() {
    let url = spawn_dead_upstream().await;
    let state = test_state_with(vec![mk_endpoint_at("a", "sk-ant-api-aaa", &url)]);
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
        "transient upstream exhaustion must be a retryable 503, not 429"
    );
    assert!(
        resp.headers().get("retry-after").is_some(),
        "503 exhaustion should carry Retry-After so the client times its backoff"
    );
}

// ── GH #97: OpenAI-endpoint 429 hard-limit cooldown + 529 BEBO ───────

/// Values a fronting hop / an LB-aware client sends for every entry in
/// `CLIENT_IDENTITY_HEADERS` — an independent oracle, kept in lockstep by
/// `identity_header_samples_cover_the_production_list`.
const CLIENT_IDENTITY_HEADER_SAMPLES: &[(&str, &str)] = &[
    ("x-forwarded-for", "203.0.113.9, 10.0.0.1"),
    ("x-real-ip", "203.0.113.9"),
    ("forwarded", "for=203.0.113.9;proto=https"),
    ("true-client-ip", "203.0.113.9"),
    ("x-client-id", "geo"),
    ("x-agent-id", "agent-42"),
    ("x-session-id", "sess-7"),
];

#[test]
fn identity_header_samples_cover_the_production_list() {
    let mut prod: Vec<&str> = CLIENT_IDENTITY_HEADERS.to_vec();
    let mut samples: Vec<&str> = CLIENT_IDENTITY_HEADER_SAMPLES
        .iter()
        .map(|(name, _)| *name)
        .collect();
    prod.sort();
    samples.sort();
    assert_eq!(prod, samples, "extend both lists together");
}

/// Send one request carrying every caller-identity header (plus an unrelated
/// custom header) through the proxy to an Anthropic-protocol upstream and
/// return the headers that upstream actually received.
async fn upstream_headers_seen(
    path: &str,
    body: &str,
    forward_caller_identity: bool,
) -> axum::http::HeaderMap {
    let (url, mut seen) = spawn_capturing_upstream(StatusCode::OK, ANTHROPIC_OK_BODY).await;
    let mut ep = make_endpoint("ep", Protocol::Anthropic);
    ep.base_url = url;
    let mut state = test_state_with(vec![ep]);
    Arc::get_mut(&mut state)
        .expect("test fixture should be uniquely owned")
        .forward_caller_identity = forward_caller_identity;
    let addr = serve(build_router(state)).await;

    let mut req = reqwest::Client::new()
        .post(format!("http://{addr}{path}"))
        .header("content-type", "application/json")
        .header("x-custom-trace", "keep-me");
    for &(name, value) in CLIENT_IDENTITY_HEADER_SAMPLES {
        req = req.header(name, value);
    }
    let resp = req.body(body.to_string()).send().await.unwrap();
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::OK,
        "request must reach upstream"
    );
    seen.recv()
        .await
        .expect("upstream must have been hit once")
        .0
}

fn assert_identity_headers_absent(seen: &axum::http::HeaderMap) {
    for &(name, _) in CLIENT_IDENTITY_HEADER_SAMPLES {
        assert!(
            !seen.contains_key(name),
            "{name} must not reach the upstream by default (GH #168)"
        );
    }
    assert_eq!(
        seen.get("x-custom-trace").map(|v| v.to_str().unwrap()),
        Some("keep-me"),
        "unrelated headers must still be forwarded"
    );
}

fn assert_identity_headers_relayed(seen: &axum::http::HeaderMap) {
    for &(name, value) in CLIENT_IDENTITY_HEADER_SAMPLES {
        assert_eq!(
            seen.get(name).map(|v| v.to_str().unwrap()),
            Some(value),
            "{name} must be relayed unchanged when forward_caller_identity = true"
        );
    }
}

/// GH #168 — see `CLIENT_IDENTITY_HEADERS`.
#[tokio::test]
async fn messages_path_strips_caller_identity_headers_upstream_by_default() {
    let seen = upstream_headers_seen(
        "/v1/messages",
        r#"{"model":"test","max_tokens":1,"messages":[{"role":"user","content":"hi"}]}"#,
        false,
    )
    .await;
    assert_identity_headers_absent(&seen);
}

/// Same contract on the OpenAI-compat → Anthropic translation path, which
/// clones the inbound headers independently of `forward_anthropic`.
#[tokio::test]
async fn chat_completions_path_strips_caller_identity_headers_upstream_by_default() {
    let seen = upstream_headers_seen(
        "/v1/chat/completions",
        r#"{"model":"test","messages":[{"role":"user","content":"hi"}]}"#,
        false,
    )
    .await;
    assert_identity_headers_absent(&seen);
}

/// The operator escape hatch relays every entry unchanged.
#[tokio::test]
async fn forward_caller_identity_true_relays_headers_on_messages_path() {
    let seen = upstream_headers_seen(
        "/v1/messages",
        r#"{"model":"test","max_tokens":1,"messages":[{"role":"user","content":"hi"}]}"#,
        true,
    )
    .await;
    assert_identity_headers_relayed(&seen);
}

#[tokio::test]
async fn forward_caller_identity_true_relays_headers_on_chat_completions_path() {
    let seen = upstream_headers_seen(
        "/v1/chat/completions",
        r#"{"model":"test","messages":[{"role":"user","content":"hi"}]}"#,
        true,
    )
    .await;
    assert_identity_headers_relayed(&seen);
}

/// GH #97 regression: a 429 from a `Protocol::OpenAI` endpoint must set a
/// hard-limit cooldown (honouring `retry-after`) so a SUBSEQUENT request
/// skips it at `pick_endpoint` instead of re-hammering an upstream that
/// told us to back off. Previously the 429 only rotated within the current
/// request's retry loop — every new request re-attempted the endpoint.
#[tokio::test]
async fn openai_429_sets_cooldown_and_next_request_skips_endpoint() {
    use std::sync::atomic::Ordering;
    const HEAD_429: &str = "HTTP/1.1 429 Too Many Requests\r\nretry-after: 120\r\ncontent-length: 0\r\nconnection: close\r\n\r\n";
    let (limited_url, limited_hits) =
        spawn_status_then_ok_upstream(usize::MAX, HEAD_429, OPENAI_OK_BODY).await;
    let (healthy_url, healthy_hits) = spawn_flaky_upstream(0, OPENAI_OK_BODY).await;

    let mut limited = make_endpoint("limited", Protocol::OpenAI);
    limited.base_url = limited_url;
    let mut healthy = make_endpoint("healthy", Protocol::OpenAI);
    healthy.base_url = healthy_url;
    // Priority forces routing to prefer `limited` until it leaves the pool —
    // deterministic without depending on affinity hashing.
    healthy.priority = 1;
    let state = test_state_with(vec![limited, healthy]);
    let addr = serve(build_router(state.clone())).await;

    let client = reqwest::Client::new();
    for _ in 0..2 {
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
            "both requests must succeed via the healthy endpoint"
        );
    }

    assert_eq!(
        limited_hits.load(Ordering::SeqCst),
        1,
        "the second request must skip the hard-limited endpoint, not re-hammer it"
    );
    assert!(healthy_hits.load(Ordering::SeqCst) >= 2);

    let info = state.endpoints[0].rate_info.read().await;
    let until = info
        .hard_limited_until
        .expect("a 429 from an OpenAI endpoint must set hard_limited_until");
    // Measured AFTER both round-trips, so the lower bound leaves ~20s of
    // slack for a loaded CI runner while staying far above the 60s default;
    // the upper bound is exact (until = t_429 + 120s, and t_429 < now).
    let cooldown = until.duration_since(Instant::now());
    assert!(
        cooldown > Duration::from_secs(100) && cooldown <= Duration::from_secs(120),
        "cooldown must honour retry-after: 120, not fall back to the 60s default, got {cooldown:?}"
    );
}

/// GH #97: a 529 from an OpenAI endpoint must flag `saw_529` so the retry
/// loop BEBO-retries the pool (long base) instead of exhausting straight to
/// a 429 — aligned with the Anthropic path via `classify_retry_status`.
#[tokio::test]
async fn openai_529_triggers_bebo_backoff_retry() {
    use std::sync::atomic::Ordering;
    const HEAD_529: &str =
        "HTTP/1.1 529 Overloaded\r\ncontent-length: 0\r\nconnection: close\r\n\r\n";
    let (url, hits) = spawn_status_then_ok_upstream(1, HEAD_529, OPENAI_OK_BODY).await;
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
        "a 529 that recovers must be BEBO-retried to a 200, not exhausted to a 429"
    );
    assert_eq!(
        hits.load(Ordering::SeqCst),
        2,
        "exactly one 529 then one successful retry"
    );
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
