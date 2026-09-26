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
        &anthropic_404,
        Protocol::Anthropic,
        "claude-nope-1"
    ));
    assert!(
        !is_model_unsupported_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            &anthropic_404,
            Protocol::Anthropic,
            "claude-nope-1"
        ),
        "status gate: a 5xx is never a model rejection"
    );

    let path_404 = serde_json::json!({
        "type": "error",
        "error": {"type": "not_found_error", "message": "Not Found"}
    });
    assert!(
        !is_model_unsupported_error(
            StatusCode::NOT_FOUND,
            &path_404,
            Protocol::Anthropic,
            "claude-nope-1"
        ),
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
        &litellm_400,
        Protocol::OpenAI,
        "claude-opus-5"
    ));

    let openai_code = serde_json::json!({
        "error": {"message": "The model `x` does not exist", "type": "invalid_request_error", "code": "model_not_found"}
    });
    assert!(is_model_unsupported_error(
        StatusCode::NOT_FOUND,
        &openai_code,
        Protocol::OpenAI,
        "x"
    ));

    let too_long = serde_json::json!({
        "type": "error",
        "error": {"type": "invalid_request_error", "message": "prompt is too long: 210000 tokens > 200000 maximum"}
    });
    assert!(
        !is_model_unsupported_error(
            StatusCode::BAD_REQUEST,
            &too_long,
            Protocol::Anthropic,
            "claude-opus-5"
        ),
        "prompt-too-long 400 must not be treated as a model rejection"
    );

    // LAB-5235: on an Anthropic endpoint the free-text phrase is client-seedable
    // — Anthropic echoes an unknown top-level field name into its 400. Only the
    // structured arms may count there.
    let echoed_field = serde_json::json!({
        "type": "error",
        "error": {"type": "invalid_request_error", "message": "invalid model name: Extra inputs are not permitted"}
    });
    assert!(
        !is_model_unsupported_error(
            StatusCode::BAD_REQUEST,
            &echoed_field,
            Protocol::Anthropic,
            "claude-opus-5"
        ),
        "an echoed client field name must not read as a model rejection on an Anthropic endpoint"
    );
    assert!(
        !is_model_unsupported_error(
            StatusCode::BAD_REQUEST,
            &litellm_400,
            Protocol::Anthropic,
            "claude-opus-5"
        ),
        "the free-text arm belongs to OpenAI-protocol gateways only"
    );
    for protocol in [Protocol::Anthropic, Protocol::OpenAI] {
        assert!(
            is_model_unsupported_error(
                StatusCode::NOT_FOUND,
                &anthropic_404,
                protocol,
                "claude-nope-1"
            ) && is_model_unsupported_error(StatusCode::NOT_FOUND, &openai_code, protocol, "x"),
            "structured arms match on every protocol ({protocol:?})"
        );
    }
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

/// The learn map is bounded: past UNSUPPORTED_MODEL_MAX distinct pairs, a new
/// learn evicts the entry nearest expiry — model strings are client-supplied
/// input. An EXISTING pair must still refresh its TTL at capacity, and the
/// refresh evicts nothing (refresh doesn't grow the map).
#[test]
fn model_unsupported_map_is_bounded() {
    let state = test_state_with(vec![mk_endpoint("a", "sk-ant-api-a")]);
    let key = |i: usize| (0usize, format!("model-{i}"));
    let last = UNSUPPORTED_MODEL_MAX + 49;
    for i in 0..=last {
        state.note_model_unsupported("a", 0, &key(i).1);
    }
    {
        let map = state.unsupported_models.lock().unwrap();
        assert_eq!(map.len(), UNSUPPORTED_MODEL_MAX);
        assert!(
            (0..50).all(|i| !map.contains_key(&key(i))) && map.contains_key(&key(50)),
            "the 50 overflow learns must evict the 50 oldest entries, and only those"
        );
    }

    // Give `model-50` the nearest expiry, still live, and shorten the TTL of
    // the pair about to be re-noted. With the map still full, the refresh must
    // land (expiry back to ~full TTL) without evicting `model-50`.
    {
        let mut map = state.unsupported_models.lock().unwrap();
        map.insert(key(50), Instant::now() + Duration::from_secs(30));
        map.insert(key(last), Instant::now() + Duration::from_secs(60));
    }
    state.note_model_unsupported("a", 0, &key(last).1);
    {
        let map = state.unsupported_models.lock().unwrap();
        assert_eq!(
            map.len(),
            UNSUPPORTED_MODEL_MAX,
            "refresh must not shrink or grow the map"
        );
        assert!(
            map.contains_key(&key(50)),
            "a refresh must not evict the entry nearest expiry"
        );
        let expiry = map
            .get(&key(last))
            .expect("existing pair must survive a refresh at capacity");
        assert!(
            *expiry > Instant::now() + Duration::from_secs(120),
            "TTL must be refreshed for an existing pair even at capacity"
        );
    }
}

/// A model name longer than UNSUPPORTED_MODEL_MAX_BYTES is not learned: the
/// entry count is capped, and this caps each entry's size, since the name is
/// client input bounded only by the request body cap.
#[test]
fn model_unsupported_skips_oversized_model_names() {
    let state = test_state_with(vec![mk_endpoint("a", "sk-ant-api-a")]);
    let fits = "m".repeat(UNSUPPORTED_MODEL_MAX_BYTES);
    let oversized = "m".repeat(UNSUPPORTED_MODEL_MAX_BYTES + 1);
    state.note_model_unsupported("a", 0, &fits);
    state.note_model_unsupported("a", 0, &oversized);
    let map = state.unsupported_models.lock().unwrap();
    assert!(map.contains_key(&(0, fits)));
    assert!(!map.contains_key(&(0, oversized)));
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

/// The gateway's 400 for a model it does not serve, as observed live
/// 2026-07-27.
const HEAD_400_LITELLM: &str = "HTTP/1.1 400 Bad Request\r\ncontent-type: application/json\r\nconnection: close\r\n\r\n{\"error\":{\"message\":\"/chat/completions: Invalid model name passed in model=claude-opus-5. Call `/v1/models` to view available models for your key.\",\"type\":\"None\",\"param\":\"None\",\"code\":\"400\"}}";

/// LAB-941 incident shape (observed live 2026-07-27): an OpenAI-protocol
/// gateway without the requested model returns 400 "Invalid model name"; the
/// LB must rotate to an account that serves it and route the NEXT request
/// away from the gateway, instead of handing clients the misleading 400.
#[tokio::test]
async fn gateway_invalid_model_rotates_to_serving_account() {
    use std::sync::atomic::Ordering;
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

/// LAB-5235: Anthropic's 400 for an unknown top-level field, where the field
/// is literally named "invalid model name" — the phrase the LiteLLM arm keys on.
const HEAD_400_ECHOED_FIELD: &str = "HTTP/1.1 400 Bad Request\r\ncontent-type: application/json\r\nconnection: close\r\n\r\n{\"type\":\"error\",\"error\":{\"type\":\"invalid_request_error\",\"message\":\"invalid model name: Extra inputs are not permitted\"}}";

/// Two Anthropic endpoints: `echo` (tried first) always answers with the
/// echoed-field 400, `other` would serve. Returns the state (to inspect the
/// negative cache), the proxy address and both hit counters.
async fn echoed_field_endpoints() -> (
    Arc<AppState>,
    SocketAddr,
    Arc<std::sync::atomic::AtomicUsize>,
    Arc<std::sync::atomic::AtomicUsize>,
) {
    let (echo_url, echo_hits) =
        spawn_status_then_ok_upstream(usize::MAX, HEAD_400_ECHOED_FIELD, b"{}").await;
    let (other_url, other_hits) = spawn_status_then_ok_upstream(0, "", b"{}").await;
    let echo = mk_endpoint_at("echo", "sk-ant-api-e", &echo_url);
    let mut other = mk_endpoint_at("other", "sk-ant-api-x", &other_url);
    other.priority = 1;
    let state = test_state_with(vec![echo, other]);
    let addr = serve(build_router(state.clone())).await;
    (state, addr, echo_hits, other_hits)
}

/// LAB-5235 native path: the echoed-field 400 is the client's own malformed
/// request. It must reach that client as-is — no negative-cache entry (which
/// would deny the model to every other client), no rotation.
#[tokio::test]
async fn echoed_field_name_400_does_not_negative_cache_native() {
    use std::sync::atomic::Ordering;
    let (state, addr, echo_hits, other_hits) = echoed_field_endpoints().await;
    let resp = reqwest::Client::new()
        .post(format!("http://{addr}/v1/messages"))
        .header("content-type", "application/json")
        .body(r#"{"model":"claude-sonnet-5","max_tokens":1,"messages":[{"role":"user","content":"hi"}],"invalid model name":1}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::BAD_REQUEST,
        "the client's own 400 must be forwarded, not rotated away"
    );
    assert_eq!(
        echo_hits.load(Ordering::SeqCst),
        1,
        "the request must reach the echoing account exactly once"
    );
    assert_eq!(
        other_hits.load(Ordering::SeqCst),
        0,
        "an echoed field name must not rotate the request"
    );
    assert!(
        state.unsupported_models.lock().unwrap().is_empty(),
        "an echoed field name must not negative-cache the model"
    );
}

/// LAB-5235 OpenAI-compat path: `/v1/chat/completions` forwards to the same
/// Anthropic accounts, so the same echoed-field 400 must not learn or rotate.
///
/// The mock answers with the echo whatever the body, and the request carries
/// no extra key: the translation copies known fields only, so a client key
/// cannot reach the account here. This pins the classifier's verdict at this
/// call site, not the attack's reachability.
#[tokio::test]
async fn echoed_field_name_400_does_not_negative_cache_openai_compat() {
    use std::sync::atomic::Ordering;
    let (state, addr, echo_hits, other_hits) = echoed_field_endpoints().await;
    let resp = reqwest::Client::new()
        .post(format!("http://{addr}/v1/chat/completions"))
        .header("content-type", "application/json")
        .body(r#"{"model":"claude-sonnet-5","messages":[{"role":"user","content":"hi"}]}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::BAD_REQUEST,
        "the client's own 400 must be forwarded, not rotated away"
    );
    assert_eq!(
        echo_hits.load(Ordering::SeqCst),
        1,
        "the request must reach the echoing account exactly once"
    );
    assert_eq!(
        other_hits.load(Ordering::SeqCst),
        0,
        "an echoed field name must not rotate the request"
    );
    assert!(
        state.unsupported_models.lock().unwrap().is_empty(),
        "an echoed field name must not negative-cache the model"
    );
}

// ── The echo veto and the anchored gateway arm ──

/// Every forward path classifies through `classify_rejection`. An error
/// message that starts with one of the request's own top-level keys is the
/// client's own error, whatever a classifier would read into it; the genuine
/// signals, `model: <id>` included, still classify.
#[test]
fn classify_rejection_vetoes_echoed_request_keys_only() {
    let err =
        |ty: &str, msg: &str| serde_json::json!({"type":"error","error":{"type":ty,"message":msg}});
    let body = |extra: &str| {
        format!(
            r#"{{"model":"claude-opus-5","max_tokens":1,"messages":[{{"role":"user","content":"hi"}}]{extra}}}"#
        )
    };
    let plain = body("");
    let entitlement = err(
        "invalid_request_error",
        "You're out of extra usage. Ask your workspace admin to add more so you can keep going.",
    );
    let litellm = serde_json::json!({"error":{"message":"/chat/completions: Invalid model name passed in model=claude-opus-5. Call `/v1/models` to view available models for your key.","type":"invalid_request_error","param":null,"code":"400"}});
    let genuine: [(
        &str,
        StatusCode,
        serde_json::Value,
        Protocol,
        UpstreamRejection,
    ); 4] = [
        (
            "the Anthropic model 404, whose prefix is the request's own `model` key",
            StatusCode::NOT_FOUND,
            err("not_found_error", "model: claude-opus-5"),
            Protocol::Anthropic,
            UpstreamRejection::ModelUnsupported,
        ),
        (
            "the OpenAI model_not_found code",
            StatusCode::NOT_FOUND,
            serde_json::json!({"error":{"message":"The model `claude-opus-5` does not exist","type":"invalid_request_error","code":"model_not_found"}}),
            Protocol::OpenAI,
            UpstreamRejection::ModelUnsupported,
        ),
        (
            "the gateway's own invalid-model 400",
            StatusCode::BAD_REQUEST,
            litellm.clone(),
            Protocol::OpenAI,
            UpstreamRejection::ModelUnsupported,
        ),
        (
            "the extra-usage 400",
            StatusCode::BAD_REQUEST,
            entitlement.clone(),
            Protocol::Anthropic,
            UpstreamRejection::Entitlement,
        ),
    ];
    for (why, status, e, protocol, want) in genuine {
        assert_eq!(
            classify_rejection(status, &e, protocol, "claude-opus-5", plain.as_bytes()),
            Some(want),
            "{why} must still classify"
        );
    }
    // A bodiless request has no key to echo; the verdict stands.
    assert_eq!(
        classify_rejection(
            StatusCode::BAD_REQUEST,
            &entitlement,
            Protocol::Anthropic,
            "",
            b""
        ),
        Some(UpstreamRejection::Entitlement)
    );

    let echoes: [(&str, &str, serde_json::Value, Protocol); 5] = [
        (
            "a key named with the entitlement anchor",
            r#","You're out of extra usage":1"#,
            err(
                "invalid_request_error",
                "You're out of extra usage: Extra inputs are not permitted",
            ),
            Protocol::Anthropic,
        ),
        (
            "the same key past a tightened anchor",
            r#","You're out of extra usage. x":1"#,
            err(
                "invalid_request_error",
                "You're out of extra usage. x: Extra inputs are not permitted",
            ),
            Protocol::Anthropic,
        ),
        (
            "a key that spells the gateway's own rejection",
            r#","/x: Invalid model name passed in model=claude-opus-5.":1"#,
            serde_json::json!({"error":{"message":"/x: Invalid model name passed in model=claude-opus-5.: Extra inputs are not permitted"}}),
            Protocol::OpenAI,
        ),
        (
            "a key whose name contains a colon",
            r#","You're out of extra usage: yes":1"#,
            err(
                "invalid_request_error",
                "You're out of extra usage: yes: Extra inputs are not permitted",
            ),
            Protocol::Anthropic,
        ),
        (
            "a duplicated key",
            r#","You're out of extra usage":1,"You're out of extra usage":2"#,
            err(
                "invalid_request_error",
                "You're out of extra usage: Extra inputs are not permitted",
            ),
            Protocol::Anthropic,
        ),
    ];
    for (why, extra, e, protocol) in echoes {
        let sent = body(extra);
        assert_eq!(
            classify_rejection(
                StatusCode::BAD_REQUEST,
                &e,
                protocol,
                "claude-opus-5",
                sent.as_bytes()
            ),
            None,
            "{why}: the echo is the client's own error"
        );
    }
    // A body that does not parse cannot rule an echo out, so it forwards.
    assert_eq!(
        classify_rejection(
            StatusCode::BAD_REQUEST,
            &entitlement,
            Protocol::Anthropic,
            "claude-opus-5",
            br#"{"model":"claude-opus-5","#
        ),
        None
    );
}

/// A body with two top-level `model` keys names no single model: the proxy
/// reads the last, a first-key-wins upstream the first. No model arm may then
/// classify, since each would pin the rejection on the model the proxy read.
/// Keys compare decoded, so an escaped duplicate counts too.
#[test]
fn classify_rejection_vetoes_duplicate_model_keys() {
    let arms: [(&str, StatusCode, serde_json::Value, Protocol); 4] = [
        (
            "the Anthropic model 404",
            StatusCode::NOT_FOUND,
            serde_json::json!({"type":"error","error":{"type":"not_found_error","message":"model: gpt-real"}}),
            Protocol::Anthropic,
        ),
        (
            "the model_not_found code",
            StatusCode::NOT_FOUND,
            serde_json::json!({"error":{"message":"The model `gpt-bogus` does not exist","type":"invalid_request_error","code":"model_not_found"}}),
            Protocol::OpenAI,
        ),
        (
            "the model_not_found code with no message",
            StatusCode::NOT_FOUND,
            serde_json::json!({"error":{"code":"model_not_found"}}),
            Protocol::OpenAI,
        ),
        (
            "the gateway's own invalid-model 400",
            StatusCode::BAD_REQUEST,
            serde_json::json!({"error":{"message":"/chat/completions: Invalid model name passed in model=gpt-real. Call `/v1/models` to view available models for your key.","code":"400"}}),
            Protocol::OpenAI,
        ),
    ];
    for (why, status, e, protocol) in &arms {
        for sent in [
            r#"{"model":"gpt-bogus","model":"gpt-real","messages":[]}"#,
            r#"{"model":"gpt-bogus","m\u006fdel":"gpt-real","messages":[]}"#,
        ] {
            assert_eq!(
                classify_rejection(*status, e, *protocol, "gpt-real", sent.as_bytes()),
                None,
                "{why}: {sent}"
            );
        }
        assert_eq!(
            classify_rejection(
                *status,
                e,
                *protocol,
                "gpt-real",
                br#"{"model":"gpt-real","messages":[]}"#
            ),
            Some(UpstreamRejection::ModelUnsupported),
            "{why}: with one `model` key the error still classifies"
        );
    }
}

/// The gateway arm matches its own framing only, at the start of the message
/// and naming the model the request asked for. Provider errors the gateway
/// relays sit later in the same field and can echo client-chosen keys.
#[test]
fn gateway_model_arm_is_anchored_and_bound_to_requested_model() {
    let msg = |m: &str| serde_json::json!({"error":{"message":m,"type":"invalid_request_error","code":"400"}});
    let hit = |m: &str, model: &str| {
        is_model_unsupported_error(StatusCode::BAD_REQUEST, &msg(m), Protocol::OpenAI, model)
    };
    let own = "/chat/completions: Invalid model name passed in model=claude-opus-5. Call `/v1/models` to view available models for your key.";
    assert!(hit(own, "claude-opus-5"));
    assert!(
        !hit(own, "claude-opus-4"),
        "a rejection naming another model must not mark the requested one"
    );
    assert!(
        !hit(own, "claude-opus-5-1"),
        "the bound model must be the whole id, not a prefix of it"
    );
    for relayed in [
        r#"litellm.BadRequestError: AnthropicException - {"type":"error","error":{"type":"invalid_request_error","message":"Invalid model name passed in model=claude-opus-5.: Extra inputs are not permitted"}}"#,
        "litellm.BadRequestError: OpenAIException - Unrecognized request argument supplied: /chat/completions: Invalid model name passed in model=claude-opus-5.",
        "Invalid model name passed in model=claude-opus-5. no route prefix",
    ] {
        assert!(!hit(relayed, "claude-opus-5"), "must not match: {relayed}");
    }
}

/// End to end on an OpenAI-protocol endpoint: a gateway 400 made of the
/// client's own key neither learns nor rotates, whether the key comes back at
/// the start of the message (the echo veto) or relayed mid-message (the
/// anchored arm). The healthy account would serve if the request rotated.
#[tokio::test]
async fn gateway_echo_of_client_key_does_not_negative_cache() {
    use std::sync::atomic::Ordering;
    const HEAD_START_ECHO: &str = "HTTP/1.1 400 Bad Request\r\ncontent-type: application/json\r\nconnection: close\r\n\r\n{\"error\":{\"message\":\"/x: Invalid model name passed in model=claude-opus-5.: Extra inputs are not permitted\",\"type\":\"invalid_request_error\",\"code\":\"400\"}}";
    const HEAD_RELAYED_ECHO: &str = "HTTP/1.1 400 Bad Request\r\ncontent-type: application/json\r\nconnection: close\r\n\r\n{\"error\":{\"message\":\"litellm.BadRequestError: AnthropicException - {\\\"type\\\":\\\"error\\\",\\\"error\\\":{\\\"type\\\":\\\"invalid_request_error\\\",\\\"message\\\":\\\"Invalid model name passed in model=claude-opus-5.: Extra inputs are not permitted\\\"}}\",\"type\":\"invalid_request_error\",\"code\":\"400\"}}";
    for (kind, head, key) in [
        (
            "echo at the start",
            HEAD_START_ECHO,
            "/x: Invalid model name passed in model=claude-opus-5.",
        ),
        (
            "echo relayed mid-message",
            HEAD_RELAYED_ECHO,
            "Invalid model name passed in model=claude-opus-5.",
        ),
    ] {
        let (gw_url, gw_hits) =
            spawn_status_then_ok_upstream(usize::MAX, head, OPENAI_OK_BODY).await;
        let (ok_url, ok_hits) = spawn_flaky_upstream(0, ANTHROPIC_OK_BODY).await;
        let mut gw = make_endpoint("gw", Protocol::OpenAI);
        gw.base_url = gw_url;
        let mut healthy = mk_endpoint_at("healthy", "sk-ant-api-h", &ok_url);
        healthy.priority = 1;
        let state = test_state_with(vec![gw, healthy]);
        let addr = serve(build_router(state.clone())).await;
        let mut req = serde_json::json!({"model":"claude-opus-5","messages":[{"role":"user","content":"hi"}]});
        req[key] = serde_json::json!(1);
        let resp = reqwest::Client::new()
            .post(format!("http://{addr}/v1/chat/completions"))
            .header("content-type", "application/json")
            .body(req.to_string())
            .send()
            .await
            .unwrap();
        assert_eq!(
            resp.status(),
            reqwest::StatusCode::BAD_REQUEST,
            "{kind}: the client's own 400 must be forwarded"
        );
        assert_eq!(
            (
                gw_hits.load(Ordering::SeqCst),
                ok_hits.load(Ordering::SeqCst)
            ),
            (1, 0),
            "{kind}: the request must not rotate"
        );
        assert!(
            state.unsupported_models.lock().unwrap().is_empty(),
            "{kind}: the model must not be negative-cached"
        );
    }
}

// ── Negative-cache hardening: client model strings ──

/// A spray of distinct bogus models fills the map; a genuine rejection that
/// arrives afterwards is still learned, and the next request for that model
/// routes away from the endpoint that rejected it.
#[tokio::test]
async fn bogus_model_spray_cannot_block_a_genuine_learn() {
    use std::sync::atomic::Ordering;
    let (gw_url, gw_hits) =
        spawn_status_then_ok_upstream(usize::MAX, HEAD_400_LITELLM, OPENAI_OK_BODY).await;
    let (ok_url, _h) = spawn_mock_upstream().await;
    let mut gw = make_endpoint("gw", Protocol::OpenAI);
    gw.base_url = gw_url;
    let mut healthy = mk_endpoint_at("healthy", "sk-ant-api-h", &ok_url);
    healthy.priority = 1;
    let state = test_state_with(vec![gw, healthy]);
    for i in 0..UNSUPPORTED_MODEL_MAX {
        state.note_model_unsupported("healthy", 1, &format!("bogus-{i}"));
    }
    let addr = serve(build_router(state.clone())).await;

    let client = reqwest::Client::new();
    for _ in 0..2 {
        let resp = client
            .post(format!("http://{addr}/v1/messages"))
            .header("content-type", "application/json")
            .body(r#"{"model":"claude-opus-5","max_tokens":1,"messages":[{"role":"user","content":"hi"}]}"#)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), reqwest::StatusCode::OK);
    }
    assert_eq!(
        gw_hits.load(Ordering::SeqCst),
        1,
        "the genuine learn must land in a full map, so the second request skips the gateway"
    );
    // One more bogus learn after the genuine one evicts the oldest bogus
    // entry. Evicting the newest instead would let every bogus learn that
    // follows a genuine one push it straight back out.
    state.note_model_unsupported("healthy", 1, "bogus-late");
    let map = state.unsupported_models.lock().unwrap();
    assert_eq!(map.len(), UNSUPPORTED_MODEL_MAX, "the map stays bounded");
    assert!(
        map.contains_key(&(0, "claude-opus-5".to_string())),
        "the genuine learn must survive a later bogus learn"
    );
}

/// A model 404 marks only the model it names. With two `model` keys the proxy
/// reads the last one; an upstream that read the first would reject a model
/// the proxy never asked for, and must not get the requested one cached.
#[tokio::test]
async fn model_404_naming_another_model_does_not_negative_cache() {
    use std::sync::atomic::Ordering;
    assert!(!is_model_unsupported_error(
        StatusCode::NOT_FOUND,
        &serde_json::json!({"type":"error","error":{"type":"not_found_error","message":"model: claude-nope-1"}}),
        Protocol::Anthropic,
        "claude-opus-5"
    ));
    let (url, hits) = spawn_status_then_ok_upstream(usize::MAX, HEAD_404_MODEL, b"{}").await;
    let (other_url, other_hits) = spawn_status_then_ok_upstream(0, "", b"{}").await;
    let rejecting = mk_endpoint_at("rejecting", "sk-ant-api-r", &url);
    let mut other = mk_endpoint_at("other", "sk-ant-api-o", &other_url);
    other.priority = 1;
    let state = test_state_with(vec![rejecting, other]);
    let addr = serve(build_router(state.clone())).await;
    let resp = reqwest::Client::new()
        .post(format!("http://{addr}/v1/messages"))
        .header("content-type", "application/json")
        .body(r#"{"model":"claude-nope-1","model":"claude-opus-5","max_tokens":1,"messages":[{"role":"user","content":"hi"}]}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::NOT_FOUND,
        "the upstream's 404 reaches the caller as-is"
    );
    assert_eq!(
        (
            hits.load(Ordering::SeqCst),
            other_hits.load(Ordering::SeqCst)
        ),
        (1, 0),
        "a 404 for another model must not rotate the request"
    );
    assert!(
        state.unsupported_models.lock().unwrap().is_empty(),
        "the requested model must not be negative-cached"
    );
}
