use super::*;

#[test]
fn oauth_system_prompt_injects_when_missing() {
    let mut body = serde_json::json!({
        "model": "claude-sonnet-4-6",
        "messages": [{"role": "user", "content": "hi"}],
        "max_tokens": 5
    });
    inject_oauth_system_prompt(&mut body);
    let system = body.get("system").unwrap().as_array().unwrap();
    assert_eq!(system.len(), 1);
    assert_eq!(system[0]["text"].as_str().unwrap(), OAUTH_SYSTEM_PROMPT);
}

#[test]
fn oauth_system_prompt_prepends_to_existing_string() {
    let mut body = serde_json::json!({
        "model": "claude-sonnet-4-6",
        "system": "Be helpful.",
        "messages": [{"role": "user", "content": "hi"}],
        "max_tokens": 5
    });
    inject_oauth_system_prompt(&mut body);
    let system = body.get("system").unwrap().as_array().unwrap();
    assert_eq!(system.len(), 2);
    assert_eq!(system[0]["text"].as_str().unwrap(), OAUTH_SYSTEM_PROMPT);
    assert_eq!(system[1]["text"].as_str().unwrap(), "Be helpful.");
}

#[test]
fn oauth_system_prompt_prepends_to_existing_array() {
    let mut body = serde_json::json!({
        "model": "claude-sonnet-4-6",
        "system": [{"type": "text", "text": "Be helpful."}],
        "messages": [{"role": "user", "content": "hi"}],
        "max_tokens": 5
    });
    inject_oauth_system_prompt(&mut body);
    let system = body.get("system").unwrap().as_array().unwrap();
    assert_eq!(system.len(), 2);
    assert_eq!(system[0]["text"].as_str().unwrap(), OAUTH_SYSTEM_PROMPT);
    assert_eq!(system[1]["text"].as_str().unwrap(), "Be helpful.");
}

#[test]
fn oauth_system_prompt_noop_when_already_present() {
    let mut body = serde_json::json!({
        "model": "claude-sonnet-4-6",
        "system": [{"type": "text", "text": OAUTH_SYSTEM_PROMPT}],
        "messages": [{"role": "user", "content": "hi"}],
        "max_tokens": 5
    });
    inject_oauth_system_prompt(&mut body);
    let system = body.get("system").unwrap().as_array().unwrap();
    assert_eq!(system.len(), 1, "should not duplicate");
}

#[test]
fn oauth_system_prompt_noop_when_prompt_is_prefix_string() {
    // CC may send the identity prompt as prefix of a longer system string
    let system_text = format!("{}\n\nYou are an interactive agent.", OAUTH_SYSTEM_PROMPT);
    let mut body = serde_json::json!({
        "model": "claude-sonnet-4-6",
        "system": system_text,
        "messages": [{"role": "user", "content": "hi"}],
        "max_tokens": 5
    });
    inject_oauth_system_prompt(&mut body);
    // Should remain a string, untouched
    assert!(body["system"].is_string(), "should not convert to array");
    assert_eq!(body["system"].as_str().unwrap(), system_text);
}

#[test]
fn oauth_system_prompt_noop_when_prompt_is_prefix_array() {
    // CC may embed the identity prompt as prefix of first block text
    let block_text = format!("{}\n\nYou are an interactive agent.", OAUTH_SYSTEM_PROMPT);
    let mut body = serde_json::json!({
        "model": "claude-sonnet-4-6",
        "system": [{"type": "text", "text": block_text}],
        "messages": [{"role": "user", "content": "hi"}],
        "max_tokens": 5
    });
    inject_oauth_system_prompt(&mut body);
    let system = body.get("system").unwrap().as_array().unwrap();
    assert_eq!(system.len(), 1, "should not prepend duplicate");
    assert_eq!(system[0]["text"].as_str().unwrap(), block_text);
}

#[test]
fn oauth_system_prompt_handles_null_system() {
    let mut body = serde_json::json!({
        "model": "claude-sonnet-4-6",
        "system": null,
        "messages": [{"role": "user", "content": "hi"}],
        "max_tokens": 5
    });
    inject_oauth_system_prompt(&mut body);
    let system = body.get("system").unwrap().as_array().unwrap();
    assert_eq!(system.len(), 1);
    assert_eq!(system[0]["text"].as_str().unwrap(), OAUTH_SYSTEM_PROMPT);
}

/// Every valid-JSON shape that is not an object. One list, one defect
/// class: `serde_json::Value`'s `IndexMut<&str>` auto-vivifies only on
/// `Null` and `Object` and panics on everything else.
const NON_OBJECT_JSON_BODIES: [&str; 5] = ["[1,2,3]", "\"x\"", "7", "true", "null"];

/// The shared injector must never index into a non-object: both of its
/// call sites (`proxy_handler`, `openai_chat_handler`) route through here,
/// so this is the locus that closes the class for all callers.
///
/// Four of the five shapes panicked before this guard existed. `null` did
/// not: it auto-vivified into `{"system":[…]}`. That case therefore pins a
/// deliberate behaviour change, not pre-existing behaviour.
#[test]
fn oauth_system_prompt_leaves_non_object_body_untouched() {
    for raw in NON_OBJECT_JSON_BODIES {
        let mut body: serde_json::Value = serde_json::from_str(raw).unwrap();
        let before = body.clone();
        inject_oauth_system_prompt(&mut body);
        assert_eq!(body, before, "shape {raw} must pass through untouched");
    }
}

/// A valid-JSON non-object body on `/v1/messages` must produce an HTTP
/// response — a 400 in Anthropic's error envelope — rather than a panicked
/// request task and a dropped connection. Drives the real router and pins
/// that the upstream is never contacted. The rejection fires before account
/// selection, so the endpoint's token kind is irrelevant here; the OAuth
/// token only documents which path used to panic.
#[tokio::test]
async fn proxy_rejects_non_object_json_body_with_envelope_400() {
    use std::sync::atomic::Ordering;
    let (url, hits) = spawn_status_then_ok_upstream(0, "", b"{}").await;
    let state = test_state_with(vec![mk_endpoint_at(
        "oauth-acct",
        "sk-ant-oat01-test-token",
        &url,
    )]);
    let addr = serve(build_router(state)).await;
    let client = Client::new();

    for raw in NON_OBJECT_JSON_BODIES {
        let resp = client
            .post(format!("http://{addr}/v1/messages"))
            .header("content-type", "application/json")
            .body(raw)
            .send()
            .await
            .unwrap_or_else(|e| panic!("shape {raw}: no HTTP response: {e}"));
        assert_eq!(
            resp.status(),
            reqwest::StatusCode::BAD_REQUEST,
            "shape {raw}"
        );
        assert_eq!(
            resp.headers()
                .get("content-type")
                .and_then(|v| v.to_str().ok()),
            Some("application/json"),
            "shape {raw}"
        );
        let body: serde_json::Value = resp.json().await.unwrap();
        assert_eq!(body["type"], "error", "shape {raw}: {body}");
        assert_eq!(
            body["error"]["type"], "invalid_request_error",
            "shape {raw}: {body}"
        );
        assert!(body["error"]["message"].is_string(), "shape {raw}: {body}");
    }
    assert_eq!(
        hits.load(Ordering::SeqCst),
        0,
        "a rejected body must never reach the upstream"
    );
}

/// Regression: CC 142+ prepends a billing header as system[0], pushing the
/// CC identity prompt to system[1+]. has_oauth_system_prompt must scan all
/// blocks, not just the first — otherwise inject_oauth_system_prompt
/// re-serializes the body, breaking Anthropic's byte-prefix cache matching.
#[test]
fn oauth_system_prompt_detected_in_later_block() {
    // system[0] is a non-prompt block (billing header), system[1] has the CC prompt
    let mut body = serde_json::json!({
        "model": "claude-sonnet-4-6",
        "system": [
            {"type": "text", "text": "x-anthropic-billing-header: cc_version=2.1.109"},
            {"type": "text", "text": OAUTH_SYSTEM_PROMPT}
        ],
        "messages": [{"role": "user", "content": "hi"}],
        "max_tokens": 5
    });

    // Must detect prompt at system[1] — no re-injection
    assert!(
        has_oauth_system_prompt(&body),
        "should detect CC prompt in system[1]"
    );
    inject_oauth_system_prompt(&mut body);
    let system = body["system"].as_array().unwrap();
    assert_eq!(
        system.len(),
        2,
        "should not prepend — prompt already present"
    );
    assert_eq!(
        system[1]["text"].as_str().unwrap(),
        OAUTH_SYSTEM_PROMPT,
        "CC prompt should remain at system[1]"
    );
}

/// The three identity prompts shipped by Claude Code 2.1.x. Only the first
/// matches `OAUTH_SYSTEM_PROMPT`; the other two must trigger injection, and
/// the sentinel must land AFTER the attribution block, not before it.
const CC_2_1_PERSONAS: [&str; 3] = [
    "You are Claude Code, Anthropic's official CLI for Claude.",
    "You are Claude Code, Anthropic's official CLI for Claude, running within the Claude Agent SDK.",
    "You are a Claude agent, built on Anthropic's Claude Agent SDK.",
];

const CC_ATTRIBUTION_BLOCK: &str =
    "x-anthropic-billing-header: cc_version=2.1.274.15a; cc_entrypoint=sdk-cli;";

/// Regression (LAB-4127): the upstream strips the attribution block only when
/// it is system[0]. Prepending the sentinel displaced it on every OAuth
/// request whose persona did not match the sentinel.
#[test]
fn oauth_system_prompt_inserted_after_leading_attribution_block() {
    for persona in CC_2_1_PERSONAS {
        let mut body = serde_json::json!({
            "model": "claude-sonnet-4-6",
            "system": [
                {"type": "text", "text": CC_ATTRIBUTION_BLOCK},
                {"type": "text", "text": persona, "cache_control": {"type": "ephemeral"}}
            ],
            "messages": [{"role": "user", "content": "hi"}],
            "max_tokens": 5
        });
        inject_oauth_system_prompt(&mut body);
        let system = body["system"].as_array().unwrap();
        assert_eq!(
            system[0]["text"].as_str().unwrap(),
            CC_ATTRIBUTION_BLOCK,
            "attribution block must stay at system[0] for persona {persona:?}"
        );
        if persona.starts_with(OAUTH_SYSTEM_PROMPT) {
            assert_eq!(system.len(), 2, "legacy persona is the sentinel: no-op");
        } else {
            assert_eq!(
                system.len(),
                3,
                "persona {persona:?} must trigger injection"
            );
            assert_eq!(system[1]["text"].as_str().unwrap(), OAUTH_SYSTEM_PROMPT);
            assert_eq!(system[2]["text"].as_str().unwrap(), persona);
            assert!(
                system[2].get("cache_control").is_some(),
                "client's cache_control must move with its block"
            );
        }
    }
}

#[test]
fn oauth_system_prompt_only_leading_attribution_block_is_kept_first() {
    // Attribution block not at index 0: the strip cannot fire anyway, so the
    // sentinel goes to index 0 as before. No reordering of client blocks.
    let mut body = serde_json::json!({
        "model": "claude-sonnet-4-6",
        "system": [
            {"type": "text", "text": CC_2_1_PERSONAS[2]},
            {"type": "text", "text": CC_ATTRIBUTION_BLOCK}
        ],
        "messages": [{"role": "user", "content": "hi"}],
        "max_tokens": 5
    });
    inject_oauth_system_prompt(&mut body);
    let system = body["system"].as_array().unwrap();
    assert_eq!(system.len(), 3);
    assert_eq!(system[0]["text"].as_str().unwrap(), OAUTH_SYSTEM_PROMPT);
    assert_eq!(system[1]["text"].as_str().unwrap(), CC_2_1_PERSONAS[2]);
    assert_eq!(system[2]["text"].as_str().unwrap(), CC_ATTRIBUTION_BLOCK);
}

/// Full proxy roundtrip on an OAuth account: the body the upstream receives
/// keeps the attribution block at system[0] with the sentinel at system[1].
#[tokio::test]
async fn proxy_oauth_account_keeps_attribution_block_first() {
    let seen_body = Arc::new(std::sync::Mutex::new(None::<Vec<u8>>));
    let seen_body_clone = seen_body.clone();
    let mock_app = Router::new().fallback(any(move |req: Request<Body>| {
        let seen_body = seen_body_clone.clone();
        async move {
            let (parts, body) = req.into_parts();
            let body_bytes = axum::body::to_bytes(body, MAX_REQUEST_BODY_BYTES)
                .await
                .unwrap();
            *seen_body.lock().unwrap() = Some(body_bytes.to_vec());
            mock_upstream_handler(Request::from_parts(parts, Body::empty())).await
        }
    }));
    let mock_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mock_addr = mock_listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(mock_listener, mock_app).await.unwrap();
    });

    let oauth_ep = mk_endpoint_at(
        "oauth-acct",
        "sk-ant-oat01-test-token",
        &format!("http://{}", mock_addr),
    );
    let state = Arc::new(AppState {
        endpoints: vec![oauth_ep],
        state_path: PathBuf::from("/tmp/anthropic-lb-oauth-attribution-test.state.json"),
        ..test_state_base()
    });
    let addr = serve(build_router(state)).await;

    let request = serde_json::json!({
        "model": "claude-sonnet-4-6",
        "system": [
            {"type": "text", "text": CC_ATTRIBUTION_BLOCK},
            {"type": "text", "text": CC_2_1_PERSONAS[2], "cache_control": {"type": "ephemeral"}}
        ],
        "messages": [{"role": "user", "content": "hi"}],
        "max_tokens": 5
    });
    let resp = Client::new()
        .post(format!("http://{}/v1/messages", addr))
        .header("content-type", "application/json")
        .json(&request)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    let captured = seen_body.lock().unwrap().clone().expect("upstream body");
    let forwarded: serde_json::Value = serde_json::from_slice(&captured).unwrap();
    let system = forwarded["system"].as_array().unwrap();
    assert_eq!(system.len(), 3);
    assert_eq!(system[0]["text"].as_str().unwrap(), CC_ATTRIBUTION_BLOCK);
    assert_eq!(system[1]["text"].as_str().unwrap(), OAUTH_SYSTEM_PROMPT);
    assert_eq!(system[2]["text"].as_str().unwrap(), CC_2_1_PERSONAS[2]);
}

/// Regression: full proxy roundtrip with OAuth account where the CC prompt
/// is at system[1+]. Verifies the proxy does not re-serialize the body
/// (which would break upstream prompt cache matching).
#[tokio::test]
async fn oauth_system_prompt_no_reserialize_when_in_later_block() {
    use std::sync::Arc as StdArc;

    // Mock upstream that captures the raw request body
    let captured_body: StdArc<tokio::sync::Mutex<Vec<u8>>> =
        StdArc::new(tokio::sync::Mutex::new(Vec::new()));
    let captured = captured_body.clone();
    let mock_app = Router::new().fallback(any(move |req: Request<Body>| {
        let captured = captured.clone();
        async move {
            let body_bytes = axum::body::to_bytes(req.into_body(), 1024 * 1024)
                .await
                .unwrap();
            *captured.lock().await = body_bytes.to_vec();

            let mut resp = axum::Json(serde_json::json!({
                "id": "msg_test",
                "type": "message",
                "content": [{"type": "text", "text": "ok"}],
                "model": "claude-sonnet-4-6",
                "stop_reason": "end_turn",
                "usage": {"input_tokens": 10, "output_tokens": 1}
            }))
            .into_response();
            resp.headers_mut().insert(
                "anthropic-ratelimit-unified-representative-claim",
                HeaderValue::from_static("five_hour"),
            );
            resp.headers_mut().insert(
                "anthropic-ratelimit-unified-5h-utilization",
                HeaderValue::from_static("0.10"),
            );
            let reset = (std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs()
                + 3600)
                .to_string();
            resp.headers_mut().insert(
                "anthropic-ratelimit-unified-5h-reset",
                HeaderValue::from_str(&reset).unwrap(),
            );
            resp
        }
    }));

    let mock_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mock_addr = mock_listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(mock_listener, mock_app).await.unwrap();
    });

    // Build app with an OAuth account, auto_cache off for clean signal
    let accounts = vec![mk_endpoint_at(
        "oauth-acct",
        "sk-ant-oat01-test-token",
        &format!("http://{}", mock_addr),
    )];
    let state = Arc::new(AppState {
        endpoints: accounts,
        state_path: PathBuf::from("/tmp/anthropic-lb-oauth-regression.state.json"),
        auto_cache: false,
        ..test_state_base()
    });

    let app = build_router(state);
    let addr = serve(app).await;

    // Request body: CC prompt at system[1], billing header at system[0]
    let request_body = serde_json::json!({
        "model": "claude-sonnet-4-6",
        "system": [
            {"type": "text", "text": "x-anthropic-billing-header: cc_version=2.1.109"},
            {"type": "text", "text": OAUTH_SYSTEM_PROMPT}
        ],
        "messages": [{"role": "user", "content": "hi"}],
        "max_tokens": 5
    });
    let request_bytes = serde_json::to_vec(&request_body).unwrap();

    let client = Client::new();
    let resp = client
        .post(format!("http://{}/v1/messages", addr))
        .header("content-type", "application/json")
        .body(request_bytes.clone())
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    // The proxy should have forwarded the original body byte-for-byte (no re-serialization).
    // Serde roundtrip can reorder keys or change whitespace — only raw byte comparison
    // catches the cache-breaking re-serialization bug this test guards against.
    let forwarded = captured_body.lock().await;
    assert_eq!(
        forwarded.as_slice(),
        request_bytes.as_slice(),
        "forwarded body must be byte-identical to request (re-serialization breaks upstream cache)"
    );
    let forwarded_body: serde_json::Value = serde_json::from_slice(&forwarded).unwrap();
    let system = forwarded_body["system"].as_array().unwrap();
    assert_eq!(
        system.len(),
        2,
        "proxy must not prepend another CC prompt — it was already at system[1]"
    );
    assert_eq!(
        system[0]["text"].as_str().unwrap(),
        "x-anthropic-billing-header: cc_version=2.1.109",
        "system[0] should be the billing header, untouched"
    );
    assert_eq!(
        system[1]["text"].as_str().unwrap(),
        OAUTH_SYSTEM_PROMPT,
        "system[1] should be the CC prompt, untouched"
    );
}
