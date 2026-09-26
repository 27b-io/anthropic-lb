use super::*;

/// The error describer must surface the real cause + classification, not
/// just reqwest's opaque "error sending request" Display. Uses a genuine
/// connect failure so it exercises the is_connect classifier and the
/// source-chain walk.
#[tokio::test]
async fn describe_reqwest_error_surfaces_cause_and_kind() {
    let err = reqwest::Client::new()
        .get("http://127.0.0.1:1/")
        .timeout(Duration::from_secs(2))
        .send()
        .await
        .expect_err("connect to 127.0.0.1:1 should fail");
    let desc = describe_reqwest_error(&err);
    assert!(
        desc.starts_with("kind="),
        "should classify kind, got: {desc}"
    );
    assert!(
        desc.contains("connect") || desc.contains("cause="),
        "should surface connect classification or root cause, got: {desc}"
    );
}

// ── Integration: HTTP handlers ──────────────────────────────────

#[tokio::test]
async fn proxy_rejects_missing_auth() {
    let (mock_url, _handle) = spawn_mock_upstream().await;
    let (app, _state) = test_app(&mock_url, Some("secret-key".to_string()));

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(
            listener,
            app.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await
        .unwrap();
    });

    let client = Client::new();
    let resp = client
        .post(format!("http://{}/v1/messages", addr))
        .header("content-type", "application/json")
        .body(r#"{"model":"test","max_tokens":1,"messages":[{"role":"user","content":"hi"}]}"#)
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), reqwest::StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn proxy_accepts_valid_auth_and_forwards() {
    let (mock_url, _handle) = spawn_mock_upstream().await;
    let (app, state) = test_app(&mock_url, Some("secret-key".to_string()));

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(
            listener,
            app.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await
        .unwrap();
    });

    let client = Client::new();
    let resp = client
        .post(format!("http://{}/v1/messages", addr))
        .header("content-type", "application/json")
        .header("x-api-key", "secret-key")
        .body(r#"{"model":"test","max_tokens":1,"messages":[{"role":"user","content":"hi"}]}"#)
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    // Verify rate info was updated from mock response headers
    let info = state.endpoints[0].rate_info.read().await;
    assert_eq!(info.utilization, Some(0.25));
    assert_eq!(info.representative_claim.as_deref(), Some("five_hour"));
}

/// A non-streaming upstream that promises N bytes via Content-Length then
/// closes the socket early must surface as a 502, NOT a truncated 200.
/// Regression for the silently-swallowed `unwrap_or_default()` body read.
#[tokio::test]
async fn proxy_returns_502_when_upstream_body_read_fails() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    // Raw TCP mock: send a 200 with content-length far larger than the
    // bytes actually written, then drop the connection mid-body.
    let mock_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mock_addr = mock_listener.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let (mut sock, _) = mock_listener.accept().await.unwrap();
            tokio::spawn(async move {
                let mut buf = [0u8; 8192];
                let _ = sock.read(&mut buf).await; // best-effort drain request
                                                   // Promise 4096 bytes, send a partial JSON prefix, then close.
                let _ = sock
                        .write_all(
                            b"HTTP/1.1 200 OK\r\n\
                              content-type: application/json\r\n\
                              anthropic-ratelimit-unified-5h-utilization: 0.42\r\n\
                              content-length: 4096\r\n\r\n\
                              {\"id\":\"msg_partial\",\"type\":\"message\",\"content\":[{\"type\":\"text\",\"text\":\"par",
                        )
                        .await;
                let _ = sock.flush().await;
                // Drop without sending the remaining promised bytes.
            });
        }
    });

    // Ratelimit reflection is opt-in since LAB-1191; this test asserts the
    // error-arm parity that the opt-in restores, so flip it on.
    let mut state = test_state_with(vec![
        mk_endpoint_at(
            "acct-a",
            "sk-ant-api-test-aaa",
            &format!("http://{}", mock_addr),
        ),
        mk_endpoint_at(
            "acct-b",
            "sk-ant-api-test-bbb",
            &format!("http://{}", mock_addr),
        ),
    ]);
    {
        let s = Arc::get_mut(&mut state).expect("test fixture should be uniquely owned");
        s.proxy_key = Some("secret-key".to_string());
        s.expose_upstream_ratelimit_headers = true;
    }
    let app = build_router(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(
            listener,
            app.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await
        .unwrap();
    });

    let client = Client::new();
    let resp = client
        .post(format!("http://{}/v1/messages", addr))
        .header("content-type", "application/json")
        .header("x-api-key", "secret-key")
        .body(r#"{"model":"test","max_tokens":1,"messages":[{"role":"user","content":"hi"}]}"#)
        .send()
        .await
        .unwrap();

    assert_eq!(
        resp.status(),
        reqwest::StatusCode::BAD_GATEWAY,
        "truncated upstream body must become a 502, not a silent empty 200"
    );
    // With expose_upstream_ratelimit_headers = true, the 502 must still
    // forward the upstream's rate-limit headers (and the budget status) so
    // the client's limit tracking doesn't go blind on the error arm — parity
    // with every success arm.
    assert_eq!(
        resp.headers()
            .get("anthropic-ratelimit-unified-5h-utilization")
            .and_then(|v| v.to_str().ok()),
        Some("0.42"),
        "502 should forward upstream anthropic-ratelimit-* headers when exposed"
    );
    assert!(
        resp.headers().contains_key("x-budget-status"),
        "502 should carry x-budget-status like other response arms"
    );
    let body = resp.text().await.unwrap();
    assert!(
        body.contains("upstream response body read failed"),
        "502 body should carry the upstream read error, got: {body}"
    );
}

#[tokio::test]
async fn proxy_preserves_raw_body_bytes_when_auto_cache_skips() {
    let seen_body = Arc::new(std::sync::Mutex::new(None::<Vec<u8>>));
    let seen_body_clone = seen_body.clone();

    let mock_app = Router::new().fallback(any(move |req: Request<Body>| {
        let seen_body = seen_body_clone.clone();
        async move {
            let body_bytes = axum::body::to_bytes(req.into_body(), MAX_REQUEST_BODY_BYTES)
                .await
                .unwrap();
            *seen_body.lock().unwrap() = Some(body_bytes.to_vec());

            let mut resp = axum::Json(serde_json::json!({
                "id": "msg_test",
                "type": "message",
                "content": [{"type": "text", "text": "ok"}],
            }))
            .into_response();
            let headers = resp.headers_mut();
            headers.insert(
                "anthropic-ratelimit-unified-representative-claim",
                HeaderValue::from_static("five_hour"),
            );
            headers.insert(
                "anthropic-ratelimit-unified-5h-utilization",
                HeaderValue::from_static("0.25"),
            );
            let reset_epoch = (std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs()
                + 3600)
                .to_string();
            headers.insert(
                "anthropic-ratelimit-unified-5h-reset",
                HeaderValue::from_str(&reset_epoch).unwrap(),
            );
            resp
        }
    }));

    let mock_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mock_addr = mock_listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(mock_listener, mock_app).await.unwrap();
    });

    let (app, _state) = test_app(
        &format!("http://{}", mock_addr),
        Some("secret-key".to_string()),
    );

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(
            listener,
            app.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await
        .unwrap();
    });

    let raw_body = "{\n  \"model\" : \"test\",\n  \"messages\" : [{\"role\" : \"user\", \"content\" : [{\"type\":\"text\", \"text\":\"hi\", \"cache_control\":{\"type\":\"ephemeral\"}}]}],\n  \"max_tokens\" : 1\n}";

    let client = Client::new();
    let resp = client
        .post(format!("http://{}/v1/messages", addr))
        .header("content-type", "application/json")
        .header("x-api-key", "secret-key")
        .body(raw_body)
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    let captured = seen_body
        .lock()
        .unwrap()
        .clone()
        .expect("upstream should receive request body");
    assert_eq!(
        String::from_utf8(captured).unwrap(),
        raw_body,
        "request body bytes should remain unchanged when auto-cache does not mutate payload"
    );
}

#[tokio::test]
async fn proxy_preserves_key_order_when_auto_cache_mutates() {
    let seen_body = Arc::new(std::sync::Mutex::new(None::<Vec<u8>>));
    let seen_body_clone = seen_body.clone();

    let mock_app = Router::new().fallback(any(move |req: Request<Body>| {
        let seen_body = seen_body_clone.clone();
        async move {
            let body_bytes = axum::body::to_bytes(req.into_body(), MAX_REQUEST_BODY_BYTES)
                .await
                .unwrap();
            *seen_body.lock().unwrap() = Some(body_bytes.to_vec());

            let mut resp = axum::Json(serde_json::json!({
                "id": "msg_test",
                "type": "message",
                "content": [{"type": "text", "text": "ok"}],
            }))
            .into_response();
            let headers = resp.headers_mut();
            headers.insert(
                "anthropic-ratelimit-unified-representative-claim",
                HeaderValue::from_static("five_hour"),
            );
            headers.insert(
                "anthropic-ratelimit-unified-5h-utilization",
                HeaderValue::from_static("0.25"),
            );
            let reset_epoch = (std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs()
                + 3600)
                .to_string();
            headers.insert(
                "anthropic-ratelimit-unified-5h-reset",
                HeaderValue::from_str(&reset_epoch).unwrap(),
            );
            resp
        }
    }));

    let mock_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mock_addr = mock_listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(mock_listener, mock_app).await.unwrap();
    });

    let (app, _state) = test_app(
        &format!("http://{}", mock_addr),
        Some("secret-key".to_string()),
    );

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(
            listener,
            app.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await
        .unwrap();
    });

    let input_body =
            "{\"model\":\"test\",\"messages\":[{\"role\":\"user\",\"content\":\"hi\"}],\"max_tokens\":1}";
    let expected_body = "{\"model\":\"test\",\"messages\":[{\"role\":\"user\",\"content\":[{\"type\":\"text\",\"text\":\"hi\",\"cache_control\":{\"type\":\"ephemeral\"}}]}],\"max_tokens\":1}";

    let client = Client::new();
    let resp = client
        .post(format!("http://{}/v1/messages", addr))
        .header("content-type", "application/json")
        .header("x-api-key", "secret-key")
        .body(input_body)
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    let captured = seen_body
        .lock()
        .unwrap()
        .clone()
        .expect("upstream should receive request body");
    assert_eq!(
        String::from_utf8(captured).unwrap(),
        expected_body,
        "mutated request body should preserve caller key order while adding cache markers"
    );
}

#[tokio::test]
async fn proxy_oauth_account_preserves_auto_cache_with_system_prompt() {
    // Regression test: when an OAuth account is selected and auto_cache
    // injects breakpoints, oauth_bytes must contain BOTH the OAuth system
    // prompt AND the cache_control markers. Before the fix, the fast-path
    // used raw body_bytes (dropping cache mutations).
    let seen_body = Arc::new(std::sync::Mutex::new(None::<Vec<u8>>));
    let seen_body_clone = seen_body.clone();

    let mock_app = Router::new().fallback(any(move |req: Request<Body>| {
        let seen_body = seen_body_clone.clone();
        async move {
            let body_bytes = axum::body::to_bytes(req.into_body(), MAX_REQUEST_BODY_BYTES)
                .await
                .unwrap();
            *seen_body.lock().unwrap() = Some(body_bytes.to_vec());

            let mut resp = axum::Json(serde_json::json!({
                "id": "msg_test",
                "type": "message",
                "content": [{"type": "text", "text": "ok"}],
            }))
            .into_response();
            let headers = resp.headers_mut();
            headers.insert(
                "anthropic-ratelimit-unified-representative-claim",
                HeaderValue::from_static("five_hour"),
            );
            headers.insert(
                "anthropic-ratelimit-unified-5h-utilization",
                HeaderValue::from_static("0.10"),
            );
            let reset_epoch = (std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs()
                + 3600)
                .to_string();
            headers.insert(
                "anthropic-ratelimit-unified-5h-reset",
                HeaderValue::from_str(&reset_epoch).unwrap(),
            );
            resp
        }
    }));

    let mock_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mock_addr = mock_listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(mock_listener, mock_app).await.unwrap();
    });

    // Single OAuth endpoint — forces all requests through the oauth_body_bytes path
    let mut oauth_ep = mk_endpoint("oauth-acct", "sk-ant-oat01-test-token");
    oauth_ep.base_url = format!("http://{}", mock_addr);
    let accounts = vec![oauth_ep];
    let state = Arc::new(AppState {
        endpoints: accounts,
        state_path: PathBuf::from("/tmp/anthropic-lb-oauth-cache-test.state.json"),
        auto_cache: true, // KEY: this test exercises the auto-cache injection path
        ..test_state_base()
    });

    let app = build_router(state);
    let addr = serve(app).await;

    // Body with NO cache_control and NO system field — auto-cache will inject
    // breakpoints, and inject_oauth_system_prompt will add the CC prompt.
    let client = Client::new();
    let resp = client
            .post(format!("http://{}/v1/messages", addr))
            .header("content-type", "application/json")
            .body(r#"{"model":"claude-sonnet-4-6","messages":[{"role":"user","content":"hi"}],"max_tokens":5}"#)
            .send()
            .await
            .unwrap();

    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    let captured = seen_body
        .lock()
        .unwrap()
        .clone()
        .expect("upstream should receive request body");
    let body: serde_json::Value = serde_json::from_slice(&captured).unwrap();

    // 1. OAuth system prompt must be injected as first system block
    let system = body
        .get("system")
        .expect("system field must be present (injected by OAuth path)");
    let arr = system.as_array().expect("system should be array");
    assert_eq!(
        arr[0]["text"].as_str().unwrap(),
        OAUTH_SYSTEM_PROMPT,
        "first system block must be CC prompt"
    );

    // 2. Auto-cache breakpoints must be present (not dropped by fast-path)
    let messages = body["messages"].as_array().unwrap();
    let last_user = messages
        .iter()
        .rev()
        .find(|m| m["role"] == "user")
        .expect("should have user message");
    let content = last_user["content"].as_array().unwrap();
    assert!(
        content.last().unwrap().get("cache_control").is_some(),
        "auto-cache breakpoint on last user message must survive OAuth path"
    );
}

/// LAB-717: native-path (/v1/messages) streaming — the passthrough stream is
/// scanned incrementally and usage recorded without buffering the response.
#[tokio::test]
async fn anthropic_streaming_records_usage() {
    let mock_app = Router::new().fallback(any(mock_anthropic_streaming_handler));
    let mock_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mock_addr = mock_listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(mock_listener, mock_app).await.unwrap();
    });

    let (app, state) = test_app(&format!("http://{}", mock_addr), None);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(
            listener,
            app.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await
        .unwrap();
    });

    let resp = Client::new()
        .post(format!("http://{}/v1/messages", addr))
        .header("content-type", "application/json")
        .body(r#"{"model":"claude-sonnet-4-6","stream":true,"messages":[{"role":"user","content":"hi"}],"max_tokens":100}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    assert_eq!(
        resp.headers()
            .get("x-should-retry")
            .and_then(|v| v.to_str().ok()),
        Some("false"),
        "streaming responses must reflect x-should-retry exactly"
    );

    let body = resp.text().await.unwrap();
    assert!(
        body.contains("message_stop"),
        "stream should pass through untouched, got: {body:?}"
    );

    assert_eq!(
        poll_streamed_usage(&state).await,
        (10, 5),
        "streamed usage must be recorded from the incremental scan"
    );
}

// ── Integration tests for x-budget-status header ──

#[tokio::test]
async fn response_includes_budget_status_header() {
    let (mock_url, _handle) = spawn_mock_upstream().await;
    let (app, _state) = test_app(&mock_url, Some("test-key".to_string()));

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(
            listener,
            app.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await
        .unwrap();
    });

    let client = Client::new();
    let resp = client
        .post(format!("http://{}/v1/messages", addr))
        .header("content-type", "application/json")
        .header("x-api-key", "test-key")
        .body(r#"{"model":"test","max_tokens":1,"messages":[{"role":"user","content":"hi"}]}"#)
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let budget_status = resp
        .headers()
        .get("x-budget-status")
        .expect("x-budget-status header should be present on proxy response");
    // Mock returns low utilization (0.25) → healthy
    assert_eq!(budget_status.to_str().unwrap(), "healthy");
}

#[tokio::test]
async fn openai_response_includes_budget_status_header() {
    let (mock_url, _handle) = spawn_mock_upstream().await;
    let (app, _state) = test_openai_app(&mock_url, Some("test-key".to_string()));

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(
            listener,
            app.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await
        .unwrap();
    });

    let client = Client::new();
    let resp = client
        .post(format!("http://{}/v1/chat/completions", addr))
        .header("content-type", "application/json")
        .header("x-api-key", "test-key")
        .body(r#"{"model":"test","max_tokens":1,"messages":[{"role":"user","content":"hi"}]}"#)
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let budget_status = resp
        .headers()
        .get("x-budget-status")
        .expect("x-budget-status header should be present on openai-compat response");
    assert_eq!(budget_status.to_str().unwrap(), "healthy");
}

// ── LAB-718: non-streaming requests must not ride the SSE-tuned read_timeout ──
//
// A non-streaming /v1/messages emits zero response bytes until generation
// completes; `client`'s read_timeout (180s, tuned for SSE inter-chunk silence)
// therefore capped generation time and killed the GEO semantic judge's long
// structured-output calls ("operation timed out", 2026-07-24). Routing keys on
// the request body's `stream` flag via `request_wants_stream`.

#[test]
fn request_wants_stream_true_only_for_explicit_stream_true() {
    assert!(request_wants_stream(br#"{"stream":true,"model":"m"}"#));
    assert!(!request_wants_stream(br#"{"stream":false,"model":"m"}"#));
    assert!(
        !request_wants_stream(br#"{"model":"m"}"#),
        "absent flag = Anthropic's non-streaming default"
    );
    assert!(
        !request_wants_stream(br#"{"stream":"true"}"#),
        "non-bool stream is not a stream request"
    );
    assert!(
        !request_wants_stream(b"not json"),
        "unparseable body counts as non-streaming"
    );
    assert!(
        !request_wants_stream(b""),
        "empty body counts as non-streaming"
    );
}

#[test]
fn upstream_client_builder_composes_with_and_without_read_timeout() {
    // Structural guard: both clients build from the shared knob chain; only the
    // streaming client layers read_timeout on top. reqwest doesn't expose its
    // config, so all this can assert is that both builders construct — the
    // load-bearing read_timeout split is pinned by the call sites in main().
    let _streaming = upstream_client_builder()
        .read_timeout(Duration::from_secs(180))
        .build()
        .expect("streaming client builds");
    let _nonstreaming = upstream_client_builder()
        .build()
        .expect("non-streaming client builds");
}
