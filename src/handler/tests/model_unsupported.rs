use super::*;

// ── LAB-941: model-unsupported detection + negative-cache routing ────

/// Wire-format detection: only genuine "this account can't serve the model"
/// errors match; other 4xx (prompt too long, bad path) must not.
#[test]
fn model_unsupported_error_detection() {
    let anthropic_404 = serde_json::json!({
        "type": "error",
        "error": {"type": "not_found_error", "message": "model: claude-nope-1"}
    });
    assert!(is_model_unsupported_error(
        StatusCode::NOT_FOUND,
        &anthropic_404
    ));
    assert!(
        !is_model_unsupported_error(StatusCode::INTERNAL_SERVER_ERROR, &anthropic_404),
        "status gate: a 5xx is never a model rejection"
    );

    let path_404 = serde_json::json!({
        "type": "error",
        "error": {"type": "not_found_error", "message": "Not Found"}
    });
    assert!(
        !is_model_unsupported_error(StatusCode::NOT_FOUND, &path_404),
        "URL-path 404 lacks the 'model:' prefix and must not match"
    );

    // LiteLLM-style gateway body, observed live from insight-gateway 2026-07-27.
    let litellm_400 = serde_json::json!({
        "error": {
            "message": "/chat/completions: Invalid model name passed in model=claude-opus-5. Call `/v1/models` to view available models for your key.",
            "type": "None", "param": "None", "code": "400"
        }
    });
    assert!(is_model_unsupported_error(
        StatusCode::BAD_REQUEST,
        &litellm_400
    ));

    let openai_code = serde_json::json!({
        "error": {"message": "The model `x` does not exist", "type": "invalid_request_error", "code": "model_not_found"}
    });
    assert!(is_model_unsupported_error(
        StatusCode::NOT_FOUND,
        &openai_code
    ));

    let too_long = serde_json::json!({
        "type": "error",
        "error": {"type": "invalid_request_error", "message": "prompt is too long: 210000 tokens > 200000 maximum"}
    });
    assert!(
        !is_model_unsupported_error(StatusCode::BAD_REQUEST, &too_long),
        "prompt-too-long 400 must not be treated as a model rejection"
    );
}

/// The negative cache removes the (endpoint, model) pair from routing — for
/// that model only, affinity or not — and an expired entry restores it.
#[tokio::test]
async fn model_unsupported_filters_routing_until_expiry() {
    let acct_a = mk_endpoint("a", "sk-ant-api-a");
    let acct_b = mk_endpoint("b", "sk-ant-api-b");
    let state = test_state_with(vec![acct_a, acct_b]);

    state.note_model_unsupported("a", 0, "claude-fable-5");

    for _ in 0..8 {
        assert_eq!(
            state.pick_endpoint(None, "claude-fable-5", &[]).await,
            Some(1),
            "noted model must never route to the rejecting endpoint"
        );
    }
    // Session affinity re-buckets too: the sticky hash only sees candidates.
    assert_eq!(
        state
            .pick_endpoint(Some("client:sess:1"), "claude-fable-5", &[])
            .await,
        Some(1),
        "an affinity-pinned session must migrate off the rejecting endpoint"
    );
    assert!(
        state
            .pick_endpoint(None, "claude-sonnet-5", &[])
            .await
            .is_some(),
        "other models on the same endpoint are unaffected"
    );

    // Force-expire the entry: the endpoint must rejoin the model's pool.
    {
        let mut map = state.unsupported_models.lock().unwrap();
        map.insert((0, "claude-fable-5".to_string()), Instant::now());
    }
    let candidates = state.routing_candidates("claude-fable-5", &[]).await;
    assert_eq!(candidates.len(), 2, "expired entry must not filter routing");
}

/// The learn map is bounded: past UNSUPPORTED_MODEL_MAX distinct pairs, new
/// learns are dropped — model strings are client-supplied input. An EXISTING
/// pair must still refresh its TTL at capacity (refresh doesn't grow the map).
#[test]
fn model_unsupported_map_is_bounded() {
    let state = test_state_with(vec![mk_endpoint("a", "sk-ant-api-a")]);
    for i in 0..(UNSUPPORTED_MODEL_MAX + 50) {
        state.note_model_unsupported("a", 0, &format!("model-{i}"));
    }
    assert_eq!(
        state.unsupported_models.lock().unwrap().len(),
        UNSUPPORTED_MODEL_MAX
    );

    // Shorten one live entry's TTL, then re-note it with the map still full:
    // the refresh must land (expiry back to ~full TTL), not be dropped.
    let key = (0usize, "model-0".to_string());
    {
        let mut map = state.unsupported_models.lock().unwrap();
        map.insert(key.clone(), Instant::now() + Duration::from_secs(1));
    }
    state.note_model_unsupported("a", 0, "model-0");
    {
        let map = state.unsupported_models.lock().unwrap();
        assert_eq!(
            map.len(),
            UNSUPPORTED_MODEL_MAX,
            "refresh must not grow the map"
        );
        let expiry = map
            .get(&key)
            .expect("existing pair must survive a refresh at capacity");
        assert!(
            *expiry > Instant::now() + Duration::from_secs(60),
            "TTL must be refreshed for an existing pair even at capacity"
        );
    }
}

/// LAB-941 native path: an account 404-rejecting the model rotates to the
/// next account within the request, and the negative cache makes the NEXT
/// request skip the rejecting account outright instead of re-pinning it via
/// session affinity.
#[tokio::test]
async fn model_unsupported_rotates_and_next_request_skips_account() {
    use std::sync::atomic::Ordering;
    let (reject_url, reject_hits) =
        spawn_status_then_ok_upstream(usize::MAX, HEAD_404_MODEL, b"{}").await;
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
            .body(r#"{"model":"claude-nope-1","max_tokens":1,"messages":[{"role":"user","content":"hi"}]}"#)
            .send()
            .await
            .unwrap();
        assert_eq!(
            resp.status(),
            reqwest::StatusCode::OK,
            "both requests must succeed via the account that serves the model"
        );
    }
    assert_eq!(
        reject_hits.load(Ordering::SeqCst),
        1,
        "the second request must skip the rejecting account, not retry it"
    );
}

/// When NO account serves the model (nonexistent model), the upstream's own
/// 404 must surface — not a synthetic 429 that invites the client to retry a
/// permanently-failing request. A SECOND request then hits the warm negative
/// cache (the pool empties before any forward runs, nothing is stashed) and
/// must still get a 404, synthesized, without touching the upstream again.
#[tokio::test]
async fn model_unsupported_everywhere_returns_upstream_404_not_429() {
    use std::sync::atomic::Ordering;
    let (url, hits) = spawn_status_then_ok_upstream(usize::MAX, HEAD_404_MODEL, b"{}").await;
    let state = test_state_with(vec![mk_endpoint_at("only", "sk-ant-api-o", &url)]);
    let addr = serve(build_router(state)).await;

    let client = reqwest::Client::new();
    for pass in [
        "cold cache (real upstream 404)",
        "warm cache (synthesized 404)",
    ] {
        let resp = client
            .post(format!("http://{addr}/v1/messages"))
            .header("content-type", "application/json")
            .body(r#"{"model":"claude-nope-1","max_tokens":1,"messages":[{"role":"user","content":"hi"}]}"#)
            .send()
            .await
            .unwrap();
        assert_eq!(
            resp.status(),
            reqwest::StatusCode::NOT_FOUND,
            "{pass}: model unsupported everywhere must return 404, not 429"
        );
        let body = resp.text().await.unwrap();
        assert!(
            body.contains("not_found_error") && body.contains("claude-nope-1"),
            "{pass}: error body must carry the model rejection, got: {body}"
        );
    }
    assert_eq!(
        hits.load(Ordering::SeqCst),
        1,
        "the warm-cache request must not touch the upstream at all"
    );
}

/// Same all-reject scenario through `/v1/chat/completions`: the warm-cache
/// synthesized 404 must carry the OpenAI error shape — `type` is
/// `invalid_request_error` (an OpenAI type, not Anthropic's
/// `not_found_error`) with the specific cause in `code = model_not_found`.
#[tokio::test]
async fn model_unsupported_everywhere_openai_handler_returns_openai_shaped_404() {
    use std::sync::atomic::Ordering;
    let (url, hits) = spawn_status_then_ok_upstream(usize::MAX, HEAD_404_MODEL, b"{}").await;
    let state = test_state_with(vec![mk_endpoint_at("only", "sk-ant-api-o", &url)]);
    let addr = serve(build_router(state)).await;

    let client = reqwest::Client::new();
    // Cold pass populates the cache from the upstream rejection.
    let resp = client
        .post(format!("http://{addr}/v1/chat/completions"))
        .header("content-type", "application/json")
        .body(r#"{"model":"claude-nope-1","messages":[{"role":"user","content":"hi"}]}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::NOT_FOUND);

    // Warm pass: pool empties via the cache, response is synthesized.
    let resp = client
        .post(format!("http://{addr}/v1/chat/completions"))
        .header("content-type", "application/json")
        .body(r#"{"model":"claude-nope-1","messages":[{"role":"user","content":"hi"}]}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::NOT_FOUND,
        "warm cache through the OpenAI handler must return 404, not 429"
    );
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(
        body.pointer("/error/type").and_then(|v| v.as_str()),
        Some("invalid_request_error"),
        "synthesized OpenAI envelope must use an OpenAI error type, got: {body}"
    );
    assert_eq!(
        body.pointer("/error/code").and_then(|v| v.as_str()),
        Some("model_not_found"),
        "synthesized OpenAI envelope must carry code=model_not_found, got: {body}"
    );
    assert!(
        body.pointer("/error/message")
            .and_then(|v| v.as_str())
            .is_some_and(|m| m.contains("claude-nope-1")),
        "message must name the rejected model, got: {body}"
    );
    assert_eq!(
        hits.load(Ordering::SeqCst),
        1,
        "the warm-cache request must not touch the upstream at all"
    );
}

/// LAB-941 incident shape (observed live 2026-07-27): an OpenAI-protocol
/// gateway without the requested model returns 400 "Invalid model name"; the
/// LB must rotate to an account that serves it and route the NEXT request
/// away from the gateway, instead of handing clients the misleading 400.
#[tokio::test]
async fn gateway_invalid_model_rotates_to_serving_account() {
    use std::sync::atomic::Ordering;
    const HEAD_400_LITELLM: &str = "HTTP/1.1 400 Bad Request\r\ncontent-type: application/json\r\nconnection: close\r\n\r\n{\"error\":{\"message\":\"/chat/completions: Invalid model name passed in model=claude-opus-5. Call `/v1/models` to view available models for your key.\",\"type\":\"None\",\"param\":\"None\",\"code\":\"400\"}}";
    let (gw_url, gw_hits) =
        spawn_status_then_ok_upstream(usize::MAX, HEAD_400_LITELLM, OPENAI_OK_BODY).await;
    let (ok_url, _h) = spawn_mock_upstream().await;

    let mut gw = make_endpoint("gw", Protocol::OpenAI);
    gw.base_url = gw_url;
    let mut healthy = mk_endpoint_at("healthy", "sk-ant-api-h", &ok_url);
    // Gateway tried first, deterministically.
    healthy.priority = 1;
    let state = test_state_with(vec![gw, healthy]);
    let addr = serve(build_router(state)).await;

    let client = reqwest::Client::new();
    for _ in 0..2 {
        let resp = client
            .post(format!("http://{addr}/v1/messages"))
            .header("content-type", "application/json")
            .body(r#"{"model":"claude-opus-5","max_tokens":1,"messages":[{"role":"user","content":"hi"}]}"#)
            .send()
            .await
            .unwrap();
        assert_eq!(
            resp.status(),
            reqwest::StatusCode::OK,
            "requests must succeed via the Anthropic account, not fail on the gateway 400"
        );
    }
    assert_eq!(
        gw_hits.load(Ordering::SeqCst),
        1,
        "the second request must skip the gateway for this model"
    );
}
