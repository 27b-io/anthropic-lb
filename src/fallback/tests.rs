use super::*;

// ── Integration: fallback upstream ──────────────────────────────

/// Mock OpenAI-compatible upstream that returns chat completion responses.
async fn mock_openai_upstream_handler(req: Request<Body>) -> Response {
    // Verify Bearer auth
    let auth = req
        .headers()
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    if !auth.starts_with("Bearer ") {
        return (StatusCode::UNAUTHORIZED, "missing bearer auth").into_response();
    }

    let body_bytes = axum::body::to_bytes(req.into_body(), 1_000_000)
        .await
        .unwrap();
    let body: serde_json::Value = serde_json::from_slice(&body_bytes).unwrap();

    let is_streaming = body
        .get("stream")
        .and_then(|s| s.as_bool())
        .unwrap_or(false);

    if is_streaming {
        let sse = [
                "data: {\"id\":\"chatcmpl-fb1\",\"object\":\"chat.completion.chunk\",\"model\":\"gpt-4\",\"choices\":[{\"delta\":{\"role\":\"assistant\",\"content\":\"\"},\"finish_reason\":null}]}\n\n",
                "data: {\"id\":\"chatcmpl-fb1\",\"object\":\"chat.completion.chunk\",\"model\":\"gpt-4\",\"choices\":[{\"delta\":{\"content\":\"Fallback\"},\"finish_reason\":null}]}\n\n",
                "data: {\"id\":\"chatcmpl-fb1\",\"object\":\"chat.completion.chunk\",\"model\":\"gpt-4\",\"choices\":[{\"delta\":{\"content\":\" works\"},\"finish_reason\":null}]}\n\n",
                "data: {\"id\":\"chatcmpl-fb1\",\"object\":\"chat.completion.chunk\",\"model\":\"gpt-4\",\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n",
                "data: [DONE]\n\n",
            ];
        return Response::builder()
            .status(StatusCode::OK)
            .header("content-type", "text/event-stream")
            .body(Body::from(sse.join("")))
            .unwrap();
    }

    axum::Json(serde_json::json!({
        "id": "chatcmpl-fallback",
        "object": "chat.completion",
        "model": "gpt-4",
        "choices": [{
            "index": 0,
            "message": {
                "role": "assistant",
                "content": "Fallback response"
            },
            "finish_reason": "stop"
        }],
        "usage": {
            "prompt_tokens": 10,
            "completion_tokens": 3,
            "total_tokens": 13
        }
    }))
    .into_response()
}

#[tokio::test]
async fn proxy_handler_falls_back_to_upstream() {
    // Start mock OpenAI upstream
    let mock_app = Router::new().fallback(any(mock_openai_upstream_handler));
    let mock_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mock_addr = mock_listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(mock_listener, mock_app).await.unwrap();
    });
    let mock_url = format!("http://{}", mock_addr);

    // Build state with the Anthropic endpoint hard-limited and a
    // priority-100 OpenAI endpoint as the fallback.
    let mut openai = make_endpoint("fallback", Protocol::OpenAI);
    openai.base_url = mock_url.clone();
    openai.priority = 100;
    let state = Arc::new(AppState {
        endpoints: vec![mk_endpoint("acct-a", "sk-ant-api-a"), openai],
        state_path: PathBuf::from("/tmp/anthropic-lb-fallback-test.state.json"),
        auto_cache: false,
        ..test_state_base()
    });

    // Hard-limit the only account
    {
        let mut info = state.endpoints[0].rate_info.write().await;
        info.hard_limited_until = Some(Instant::now() + Duration::from_secs(3600));
    }

    let app = Router::new()
        .fallback(any(proxy_handler))
        .with_state(state.clone());

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

    // Non-streaming request (Anthropic format) → should get Anthropic-format response via fallback
    let resp = client
        .post(format!("http://{}/v1/messages", addr))
        .header("content-type", "application/json")
        .json(&serde_json::json!({
            "model": "claude-sonnet-4-6",
            "messages": [{"role": "user", "content": "Hello"}],
            "max_tokens": 1024
        }))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["type"], "message");
    assert_eq!(body["content"][0]["type"], "text");
    assert_eq!(body["content"][0]["text"], "Fallback response");
    assert_eq!(body["stop_reason"], "end_turn");

    // Verify the OpenAI endpoint got the request
    assert_eq!(state.endpoints[1].requests.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn proxy_handler_fallback_streaming() {
    // Start mock OpenAI upstream
    let mock_app = Router::new().fallback(any(mock_openai_upstream_handler));
    let mock_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mock_addr = mock_listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(mock_listener, mock_app).await.unwrap();
    });
    let mock_url = format!("http://{}", mock_addr);

    let mut openai = make_endpoint("fallback", Protocol::OpenAI);
    openai.base_url = mock_url.clone();
    openai.priority = 100;
    let state = Arc::new(AppState {
        endpoints: vec![mk_endpoint("acct-a", "sk-ant-api-a"), openai],
        state_path: PathBuf::from("/tmp/anthropic-lb-fallback-stream-test.state.json"),
        auto_cache: false,
        ..test_state_base()
    });

    {
        let mut info = state.endpoints[0].rate_info.write().await;
        info.hard_limited_until = Some(Instant::now() + Duration::from_secs(3600));
    }

    let app = Router::new()
        .fallback(any(proxy_handler))
        .with_state(state.clone());

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

    // Streaming request
    let resp = client
        .post(format!("http://{}/v1/messages", addr))
        .header("content-type", "application/json")
        .json(&serde_json::json!({
            "model": "claude-sonnet-4-6",
            "messages": [{"role": "user", "content": "Hello"}],
            "max_tokens": 1024,
            "stream": true
        }))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(
        resp.headers().get("content-type").unwrap(),
        "text/event-stream"
    );

    let body = resp.text().await.unwrap();
    // Should contain Anthropic SSE events (translated from OpenAI)
    assert!(
        body.contains("message_start"),
        "should have message_start event"
    );
    assert!(body.contains("text_delta"), "should have text_delta events");
    assert!(body.contains("Fallback"), "should contain 'Fallback' text");
    assert!(
        body.contains("message_stop"),
        "should have message_stop event"
    );
}

#[tokio::test]
async fn fallback_translated_stream_no_error_frame_after_message_stop() {
    // LAB-710: a transport read failure AFTER the upstream's `[DONE]` must
    // not ship an Anthropic error frame — the translated `message_stop`
    // already terminated the stream from the client's view. Mirror of the
    // passthrough `terminal.completed` guard, one protocol over. The mock
    // drops WITHOUT the 0-length chunked terminator: the proxy's next
    // resp.chunk() errors after message_stop already went downstream.
    let mock_addr = spawn_sse_upstream(
        concat!(
            "data: {\"id\":\"c1\",\"model\":\"gpt-4\",\"choices\":[{\"delta\":{\"role\":\"assistant\",\"content\":\"Hi\"},\"finish_reason\":null}]}\n\n",
            "data: {\"id\":\"c1\",\"model\":\"gpt-4\",\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n",
            "data: [DONE]\n\n",
        ),
        false,
    )
    .await;
    let state = fallback_only_state(mock_addr, "done-then-err-test").await;
    let body = stream_messages(serve(build_router(state)).await).await;

    assert!(
        body.contains("message_stop"),
        "stream completed — must carry the success terminator, got: {body:?}"
    );
    assert!(
        !body.contains("event: error"),
        "no error frame may follow message_stop, got: {body:?}"
    );
}

// ── LAB-4031: single-terminator invariant on the remaining stream loops ──
//
// An SSE stream has exactly one terminator, and an error frame is one.
// LAB-710 enforced that on the two translating loops' transport-error
// arms; these cover the native passthrough and the fallback translate
// branch's end-of-stream.

/// Raw-TCP SSE upstream: answers one request with `body` as a single HTTP
/// chunk, then either closes the chunked body cleanly (`0\r\n\r\n` → the
/// proxy sees `Ok(None)`) or drops the socket without it (→ the proxy's
/// next `resp.chunk()` errors, hyper `IncompleteMessage`).
async fn spawn_sse_upstream(body: &'static str, clean_close: bool) -> SocketAddr {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut buf = vec![0u8; 8192];
        let _ = stream.read(&mut buf).await;
        let head = "HTTP/1.1 200 OK\r\n\
             content-type: text/event-stream\r\n\
             transfer-encoding: chunked\r\n\
             \r\n";
        let _ = stream.write_all(head.as_bytes()).await;
        let chunk = format!("{:x}\r\n{}\r\n", body.len(), body);
        let _ = stream.write_all(chunk.as_bytes()).await;
        if clean_close {
            let _ = stream.write_all(b"0\r\n\r\n").await;
        }
        let _ = stream.shutdown().await;
    });
    addr
}

/// State whose only routable endpoint is an OpenAI-protocol fallback at
/// `mock_addr`, so a `/v1/messages` request takes `try_fallback_upstream`'s
/// translate branch.
async fn fallback_only_state(mock_addr: SocketAddr, state_file: &str) -> Arc<AppState> {
    let mut openai = make_endpoint("fallback", Protocol::OpenAI);
    openai.base_url = format!("http://{}", mock_addr);
    openai.priority = 100;
    let state = Arc::new(AppState {
        endpoints: vec![mk_endpoint("acct-a", "sk-ant-api-a"), openai],
        state_path: PathBuf::from(format!("/tmp/anthropic-lb-{state_file}.state.json")),
        auto_cache: false,
        ..test_state_base()
    });
    let mut info = state.endpoints[0].rate_info.write().await;
    info.hard_limited_until = Some(Instant::now() + Duration::from_secs(3600));
    drop(info);
    state
}

async fn stream_messages(addr: SocketAddr) -> String {
    let resp = Client::new()
        .post(format!("http://{}/v1/messages", addr))
        .header("content-type", "application/json")
        .json(&serde_json::json!({
            "model": "claude-sonnet-4-6",
            "messages": [{"role": "user", "content": "Hello"}],
            "max_tokens": 16,
            "stream": true
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    resp.text().await.unwrap()
}

#[tokio::test]
async fn native_stream_no_error_frame_after_message_stop() {
    // Gap 1: the native /v1/messages passthrough forwarded a complete
    // message (message_stop went downstream verbatim) and then the peer
    // dropped without the chunked terminator. The client already has a
    // complete stream; a trailing `event: error` would make the SDK raise on
    // a request that succeeded.
    let mock_addr = spawn_sse_upstream(
        concat!(
            "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_1\",\"model\":\"claude-sonnet-4-6\",\"usage\":{\"input_tokens\":1,\"output_tokens\":0}}}\n\n",
            "event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\n",
            "event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"Hi\"}}\n\n",
            "event: content_block_stop\ndata: {\"type\":\"content_block_stop\",\"index\":0}\n\n",
            "event: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":1}}\n\n",
            "event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n",
        ),
        false,
    )
    .await;
    let (app, _state) = test_app(&format!("http://{}", mock_addr), None);
    let body = stream_messages(serve(app).await).await;

    assert!(
        body.contains("event: message_stop\n"),
        "upstream's message_stop must be forwarded, got: {body:?}"
    );
    assert!(
        !body.contains("event: error"),
        "no error frame may follow message_stop, got: {body:?}"
    );
}

#[tokio::test]
async fn native_stream_no_second_error_frame_after_inband_error() {
    // The upstream's own `event: error` is a terminator too. When the peer
    // then drops without the chunked terminator, the passthrough must not
    // append a second error frame behind the one it already forwarded.
    let mock_addr = spawn_sse_upstream(
        "event: error\ndata: {\"type\":\"error\",\"error\":{\"type\":\"api_error\",\"message\":\"upstream boom\"}}\n\n",
        false,
    )
    .await;
    let (app, _state) = test_app(&format!("http://{}", mock_addr), None);
    let body = stream_messages(serve(app).await).await;

    assert_eq!(
        body.matches("event: error\n").count(),
        1,
        "exactly one error frame — the upstream's own, got: {body:?}"
    );
    assert!(
        body.contains("upstream boom"),
        "the upstream's error must be forwarded verbatim, got: {body:?}"
    );
}

#[tokio::test]
async fn fallback_translated_stream_clean_eof_mid_message_emits_error_frame() {
    // Gap 2a: upstream body ends cleanly (an intermediary's read timeout,
    // say) after content but before finish_reason / [DONE]. The client is
    // holding message_start + deltas; closing the socket there is a silent
    // truncation. It must get an explicit `event: error` instead.
    let mock_addr = spawn_sse_upstream(
        "data: {\"id\":\"c1\",\"model\":\"gpt-4\",\"choices\":[{\"delta\":{\"role\":\"assistant\",\"content\":\"Hi\"},\"finish_reason\":null}]}\n\n",
        true,
    )
    .await;
    let state = fallback_only_state(mock_addr, "clean-eof-mid-message").await;
    let body = stream_messages(serve(build_router(state)).await).await;

    assert!(
        body.contains("text_delta"),
        "content before the cut must still reach the client, got: {body:?}"
    );
    assert!(
        !body.contains("message_stop"),
        "an unfinished message must not be faked complete, got: {body:?}"
    );
    let err_at = body.find("event: error\n").unwrap_or_else(|| {
        panic!("clean EOF mid-message must terminate with an error frame, got: {body:?}")
    });
    assert!(
        err_at > body.find("text_delta").unwrap(),
        "error frame must be the stream's last event, got: {body:?}"
    );
}

#[tokio::test]
async fn fallback_translated_stream_done_only_emits_error_frame() {
    // Gap 2b: upstream 200s with `data: [DONE]` and nothing else. The
    // translator has no message to stop, so pre-fix the client got a 200
    // with a completely empty SSE body — same shape LAB-710 closed for the
    // in-band-error case, on the no-error path.
    let mock_addr = spawn_sse_upstream("data: [DONE]\n\n", true).await;
    let state = fallback_only_state(mock_addr, "done-only").await;
    let body = stream_messages(serve(build_router(state)).await).await;

    assert!(
        !body.contains("message_start") && !body.contains("message_stop"),
        "no message may be fabricated from an empty stream, got: {body:?}"
    );
    assert!(
        body.contains("event: error\n"),
        "empty upstream stream must terminate with an error frame, got: {body:?}"
    );
}

#[tokio::test]
async fn proxy_handler_no_fallback_returns_429() {
    // With no fallback endpoint, an exhausted pool yields None (→ 429).
    let state = test_state_with(vec![mk_endpoint("a", "sk-ant-api-a")]);

    // Hard-limit the only endpoint
    {
        let mut info = state.endpoints[0].rate_info.write().await;
        info.hard_limited_until = Some(Instant::now() + Duration::from_secs(3600));
    }

    let result = state.pick_endpoint(None, "", &[]).await;
    assert!(
        result.is_none(),
        "should return None when endpoint is hard-limited"
    );
}

// ── Unified endpoint: fallback retry semantics ─────────────────

#[tokio::test]
async fn try_fallback_upstream_rotates_on_429() {
    // Mock upstream returns 429
    let app = Router::new().fallback(any(|| async {
        (StatusCode::TOO_MANY_REQUESTS, "rate limited").into_response()
    }));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    let mut state = test_state_with(vec![]);
    let mut ep = make_endpoint("rl-gw", Protocol::OpenAI);
    ep.base_url = format!("http://{}", addr);
    Arc::get_mut(&mut state).unwrap().endpoints.push(ep);

    let body =
        bytes::Bytes::from_static(br#"{"model":"claude-opus-4-7","messages":[],"max_tokens":1}"#);
    let result = try_fallback_upstream(
        &state,
        &body,
        "req-1",
        "client-1",
        &"127.0.0.1".parse().unwrap(),
        "-",
        "-",
        "claude-opus-4-7",
        0,
        Instant::now(),
        false,
        false,
    )
    .await;
    assert!(
        matches!(
            result,
            ForwardOutcome::Retry {
                push_skip: true,
                transient: false,
                ..
            }
        ),
        "429 must rotate (skip) — an HTTP error is not a transport failure"
    );
}

#[tokio::test]
async fn try_fallback_upstream_rotates_on_500() {
    let app = Router::new().fallback(any(|| async {
        (StatusCode::INTERNAL_SERVER_ERROR, "boom").into_response()
    }));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    let mut state = test_state_with(vec![]);
    let mut ep = make_endpoint("broken", Protocol::OpenAI);
    ep.base_url = format!("http://{}", addr);
    Arc::get_mut(&mut state).unwrap().endpoints.push(ep);

    let body =
        bytes::Bytes::from_static(br#"{"model":"claude-opus-4-7","messages":[],"max_tokens":1}"#);
    let result = try_fallback_upstream(
        &state,
        &body,
        "req-1",
        "client-1",
        &"127.0.0.1".parse().unwrap(),
        "-",
        "-",
        "claude-opus-4-7",
        0,
        Instant::now(),
        false,
        false,
    )
    .await;
    assert!(
        matches!(
            result,
            ForwardOutcome::Retry {
                push_skip: true,
                transient: false,
                ..
            }
        ),
        "500 must rotate (skip) — an HTTP error is not a transport failure"
    );
}

/// Transport send failures on the OpenAI branch must be classified
/// `transient` (round-gated retry) AND feed the circuit breaker — the #69
/// gap where they were swallowed to a bare `None` with no health signal.
#[tokio::test]
async fn try_fallback_upstream_transport_error_is_transient_and_counted() {
    let url = spawn_dead_upstream().await;
    let mut state = test_state_with(vec![]);
    let mut ep = make_endpoint("dead-gw", Protocol::OpenAI);
    ep.base_url = url;
    Arc::get_mut(&mut state).unwrap().endpoints.push(ep);

    let body =
        bytes::Bytes::from_static(br#"{"model":"claude-opus-4-7","messages":[],"max_tokens":1}"#);
    let result = try_fallback_upstream(
        &state,
        &body,
        "req-1",
        "client-1",
        &"127.0.0.1".parse().unwrap(),
        "-",
        "-",
        "claude-opus-4-7",
        0,
        Instant::now(),
        false,
        false,
    )
    .await;
    assert!(
        matches!(
            result,
            ForwardOutcome::Retry {
                push_skip: false,
                transient: true,
                ..
            }
        ),
        "a transport send failure must be transient (round-gated), not a plain skip"
    );
    let info = state.endpoints[0].rate_info.read().await;
    assert_eq!(
        info.transport.consecutive_failures, 1,
        "the transport failure must feed the per-endpoint circuit breaker"
    );
}

/// LAB-712: a non-streaming response from a `Protocol::OpenAI` endpoint must
/// record its `usage.prompt_tokens`/`completion_tokens` into per-client
/// token + budget accounting. Previously this path only bumped `ep.requests`,
/// leaving `pre_request_gate` budget enforcement blind to OpenAI-endpoint spend.
#[tokio::test]
async fn try_fallback_upstream_records_usage_and_budget() {
    // Mock OpenAI upstream: non-streaming chat completion with usage.
    let app = Router::new().fallback(any(|| async {
        axum::Json(serde_json::json!({
            "id": "chatcmpl-1",
            "object": "chat.completion",
            "model": "claude-opus-4-7",
            "choices": [{
                "index": 0,
                "message": {"role": "assistant", "content": "hi"},
                "finish_reason": "stop"
            }],
            "usage": {"prompt_tokens": 60, "completion_tokens": 50, "total_tokens": 110}
        }))
        .into_response()
    }));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    let mut budgets = HashMap::new();
    budgets.insert("client-1".to_string(), 100u64);
    let mut ep = make_endpoint("gw", Protocol::OpenAI);
    ep.base_url = format!("http://{}", addr);
    let state = Arc::new(AppState {
        endpoints: vec![ep],
        client_budgets: budgets,
        ..test_state_base()
    });

    assert!(state.check_budget("client-1").await.is_ok());

    let body =
        bytes::Bytes::from_static(br#"{"model":"claude-opus-4-7","messages":[],"max_tokens":1}"#);
    let result = try_fallback_upstream(
        &state,
        &body,
        "req-1",
        "client-1",
        &"127.0.0.1".parse().unwrap(),
        "-",
        "-",
        "claude-opus-4-7",
        0,
        Instant::now(),
        true,
        false,
    )
    .await;
    assert!(matches!(result, ForwardOutcome::Done(_)));

    // Endpoint + per-client token counters advance by the reported usage.
    assert_eq!(state.endpoints[0].input_tokens.load(Ordering::Relaxed), 60);
    assert_eq!(state.endpoints[0].output_tokens.load(Ordering::Relaxed), 50);
    {
        let map = state.client_usage.lock().unwrap();
        assert_eq!(map.get("client-1").unwrap(), &[60, 50, 0, 0]);
    }

    // Budget sees the spend: 110 tokens against a 100-token budget → gate closes.
    {
        let map = state.budget_usage.lock().unwrap();
        assert_eq!(map.get("client-1").unwrap().1, 110);
    }
    assert!(
        state.check_budget("client-1").await.is_err(),
        "pre_request_gate budget check must see OpenAI-endpoint spend"
    );
}
// ── Unified endpoint: cross-protocol handler routing ───────────

#[tokio::test]
async fn openai_chat_handler_routes_to_unified_anthropic_endpoint() {
    // Mock Anthropic upstream: returns a minimal messages response.
    let mock = Router::new().fallback(any(|| async {
        axum::Json(serde_json::json!({
            "id": "msg_1", "type": "message", "role": "assistant",
            "model": "claude-opus-4-7",
            "content": [{"type": "text", "text": "hi back"}],
            "stop_reason": "end_turn",
            "usage": {"input_tokens": 1, "output_tokens": 2}
        }))
        .into_response()
    }));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mock_addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, mock).await.unwrap();
    });

    let mut state = test_state_with(vec![]); // no legacy accounts
    let mut ep = make_endpoint("unified-anthropic", Protocol::Anthropic);
    ep.base_url = format!("http://{}", mock_addr);
    ep.token = "sk-ant-api-test".to_string();
    Arc::get_mut(&mut state).unwrap().endpoints.push(ep);

    let app = Router::new()
        .route(
            "/v1/chat/completions",
            axum::routing::post(openai_chat_handler),
        )
        .with_state(state);
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

    let resp = reqwest::Client::new()
        .post(format!("http://{}/v1/chat/completions", addr))
        .json(&serde_json::json!({
            "model": "claude-opus-4-7",
            "messages": [{"role": "user", "content": "hi"}],
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert!(
        body["choices"][0]["message"]["content"].is_string(),
        "response must be OpenAI-shaped after round-trip translation"
    );
}
#[tokio::test]
async fn proxy_handler_translates_to_openai_endpoint() {
    // Mock OpenAI upstream: capture the request body to assert it was
    // translated to OpenAI shape, then return an OpenAI-format response.
    let (url, mut rx) = spawn_capturing_upstream(StatusCode::OK, OPENAI_OK_BODY).await;

    let mut state = test_state_with(vec![]);
    let mut ep = make_endpoint("openai-gw", Protocol::OpenAI);
    ep.base_url = url;
    Arc::get_mut(&mut state).unwrap().endpoints.push(ep);

    let proxy_app = Router::new().fallback(any(proxy_handler)).with_state(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let proxy_addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(
            listener,
            proxy_app.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await
        .unwrap();
    });

    let client = reqwest::Client::new();
    let _ = client
        .post(format!("http://{}/v1/messages", proxy_addr))
        .json(&serde_json::json!({
            "model": "claude-opus-4-7",
            "max_tokens": 64,
            "messages": [{"role": "user", "content": "hi"}],
        }))
        .send()
        .await
        .unwrap();

    let received: serde_json::Value =
        serde_json::from_slice(&rx.recv().await.expect("upstream must receive a request").1)
            .unwrap();
    assert!(
        received.get("messages").is_some(),
        "translated request must have OpenAI `messages` field"
    );
    assert_eq!(received["model"], "claude-opus-4-7");
}

/// LAB-716: request translation must run at most ONCE per request, not once
/// per retry attempt. Two OpenAI endpoints; the preferred one 500s so the
/// retry loop rotates to the second — both attempts must reuse one translated
/// body (asserted byte-identical) with a single translate invocation.
/// Relies on `#[tokio::test]`'s current-thread runtime: the handler runs on
/// this test's thread, so the thread-local counter sees exactly this request.
#[tokio::test]
async fn proxy_handler_translates_once_across_endpoint_rotation() {
    // Failing upstream 500s → rotate; healthy upstream serves a canned
    // OpenAI completion. Both capture the raw wire body for comparison.
    let (bad_url, mut bad_rx) =
        spawn_capturing_upstream(StatusCode::INTERNAL_SERVER_ERROR, b"boom").await;
    let (ok_url, mut ok_rx) = spawn_capturing_upstream(StatusCode::OK, OPENAI_OK_BODY).await;

    // Priority 0 = failing endpoint picked first; priority 100 = healthy
    // rotation target (deterministic ordering, same trick as LAB-365).
    let mut state = test_state_with(vec![]);
    let mut bad_ep = make_endpoint("bad-gw", Protocol::OpenAI);
    bad_ep.base_url = bad_url;
    let mut ok_ep = make_endpoint("ok-gw", Protocol::OpenAI);
    ok_ep.base_url = ok_url;
    ok_ep.priority = 100;
    {
        let s = Arc::get_mut(&mut state).unwrap();
        s.endpoints.push(bad_ep);
        s.endpoints.push(ok_ep);
    }

    let proxy_app = Router::new().fallback(any(proxy_handler)).with_state(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let proxy_addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(
            listener,
            proxy_app.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await
        .unwrap();
    });

    let calls_before = TRANSLATE_A2O_CALLS.with(|c| c.get());

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("http://{}/v1/messages", proxy_addr))
        .json(&serde_json::json!({
            "model": "claude-opus-4-7",
            "max_tokens": 64,
            "messages": [{"role": "user", "content": "hi"}],
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "rotation to the healthy endpoint must succeed"
    );

    let calls_after = TRANSLATE_A2O_CALLS.with(|c| c.get());
    assert_eq!(
        calls_after - calls_before,
        1,
        "translation must run exactly once for a 2-endpoint retry, not per attempt"
    );

    // Both attempts must send the SAME wire body (memoized Bytes reused).
    let bad_body = bad_rx
        .recv()
        .await
        .expect("failing endpoint got the request")
        .1;
    let ok_body = ok_rx
        .recv()
        .await
        .expect("healthy endpoint got the rotation")
        .1;
    assert_eq!(
        bad_body, ok_body,
        "rotated attempt must reuse the identical serialized body"
    );
}
