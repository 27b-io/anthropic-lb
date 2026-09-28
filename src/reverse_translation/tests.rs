use super::*;

/// The Anthropic→OpenAI fallback translator gets the same LAB-798 guard as
/// the forward shim: an OpenAI-protocol endpoint can front Claude ≥ 4.7.
#[test]
fn translate_a2o_drops_temperature_for_rejecting_model() {
    let body = serde_json::json!({
        "model": "claude-sonnet-5",
        "messages": [{"role": "user", "content": "Hello"}],
        "max_tokens": 64,
        "temperature": 0.2,
        "top_p": 0.9
    });
    let result = translate_anthropic_request_to_openai(&body).unwrap();
    assert!(result.get("temperature").is_none());
    assert_eq!(result["top_p"], 0.9);
}

#[test]
fn translate_a2o_keeps_default_temperature_for_rejecting_model() {
    // Default 1 passes through (upstream accepts it); ≤ 4.6 models are
    // covered by `translate_anthropic_request_basic`.
    let body = serde_json::json!({
        "model": "claude-fable-5",
        "messages": [{"role": "user", "content": "Hello"}],
        "temperature": 1
    });
    let result = translate_anthropic_request_to_openai(&body).unwrap();
    assert_eq!(result["temperature"], 1);
}

/// LAB-798 third path: `openai_chat_handler` forwards the raw request bytes
/// to a `Protocol::OpenAI` endpoint without translation — the handler must
/// strip a hard-rejected `temperature` from those bytes before forwarding.
#[tokio::test]
async fn openai_passthrough_strips_temperature_for_rejecting_model() {
    let seen_body = Arc::new(std::sync::Mutex::new(None::<Vec<u8>>));
    let seen_body_clone = seen_body.clone();
    let mock_app = Router::new().fallback(any(move |req: Request<Body>| {
        let seen_body = seen_body_clone.clone();
        async move {
            let bytes = axum::body::to_bytes(req.into_body(), MAX_REQUEST_BODY_BYTES)
                .await
                .unwrap();
            *seen_body.lock().unwrap() = Some(bytes.to_vec());
            (
                [("content-type", "application/json")],
                OPENAI_OK_BODY.to_vec(),
            )
        }
    }));
    let mock_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mock_addr = mock_listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(mock_listener, mock_app).await.unwrap();
    });

    let mut gw = make_endpoint("gw", Protocol::OpenAI);
    gw.base_url = format!("http://{mock_addr}");
    let state = test_state_with(vec![gw]);
    let addr = serve(build_router(state)).await;

    let resp = reqwest::Client::new()
        .post(format!("http://{addr}/v1/chat/completions"))
        .header("content-type", "application/json")
        .body(
            r#"{"model":"claude-sonnet-5","max_tokens":8,"temperature":0.2,"top_p":0.9,"messages":[{"role":"user","content":"hi"}]}"#,
        )
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    let captured = seen_body
        .lock()
        .unwrap()
        .clone()
        .expect("gateway should receive request body");
    let forwarded: serde_json::Value = serde_json::from_slice(&captured).unwrap();
    assert!(
        forwarded.get("temperature").is_none(),
        "raw passthrough must not forward a hard-rejected temperature: {forwarded}"
    );
    assert_eq!(
        forwarded["top_p"], 0.9,
        "other params must survive the strip"
    );
}

// ── Reverse translation: Anthropic → OpenAI ──────────────────────

#[test]
fn reverse_map_stop_reasons() {
    assert_eq!(reverse_map_stop_reason("stop"), "end_turn");
    assert_eq!(reverse_map_stop_reason("length"), "max_tokens");
    assert_eq!(reverse_map_stop_reason("tool_calls"), "tool_use");
    assert_eq!(reverse_map_stop_reason("unknown"), "end_turn");
}

#[test]
fn translate_anthropic_request_basic() {
    let body = serde_json::json!({
        "model": "claude-sonnet-4-6",
        "system": "You are helpful.",
        "messages": [
            {"role": "user", "content": "Hello"},
            {"role": "assistant", "content": "Hi there!"},
            {"role": "user", "content": "How are you?"}
        ],
        "max_tokens": 1024,
        "temperature": 0.7,
        "stream": true
    });

    let result = translate_anthropic_request_to_openai(&body).unwrap();
    assert_eq!(result["model"], "claude-sonnet-4-6");
    assert_eq!(result["max_tokens"], 1024);
    assert_eq!(result["temperature"], 0.7);
    assert_eq!(result["stream"], true);

    let msgs = result["messages"].as_array().unwrap();
    assert_eq!(msgs[0]["role"], "system");
    assert_eq!(msgs[0]["content"], "You are helpful.");
    assert_eq!(msgs[1]["role"], "user");
    assert_eq!(msgs[1]["content"], "Hello");
    assert_eq!(msgs[2]["role"], "assistant");
    assert_eq!(msgs[2]["content"], "Hi there!");
}

#[test]
fn translate_anthropic_request_tool_use() {
    let body = serde_json::json!({
        "model": "claude-sonnet-4-6",
        "messages": [
            {
                "role": "assistant",
                "content": [
                    {"type": "text", "text": "Let me search for that."},
                    {
                        "type": "tool_use",
                        "id": "toolu_123",
                        "name": "search",
                        "input": {"query": "test"}
                    }
                ]
            },
            {
                "role": "user",
                "content": [
                    {
                        "type": "tool_result",
                        "tool_use_id": "toolu_123",
                        "content": "Search result"
                    }
                ]
            }
        ],
        "max_tokens": 1024
    });

    let result = translate_anthropic_request_to_openai(&body).unwrap();
    let msgs = result["messages"].as_array().unwrap();

    // Assistant with tool_calls
    assert_eq!(msgs[0]["role"], "assistant");
    assert_eq!(msgs[0]["content"], "Let me search for that.");
    let tc = &msgs[0]["tool_calls"][0];
    assert_eq!(tc["id"], "toolu_123");
    assert_eq!(tc["function"]["name"], "search");

    // Tool result
    assert_eq!(msgs[1]["role"], "tool");
    assert_eq!(msgs[1]["tool_call_id"], "toolu_123");
    assert_eq!(msgs[1]["content"], "Search result");
}

#[test]
fn translate_anthropic_request_tools() {
    let body = serde_json::json!({
        "model": "claude-sonnet-4-6",
        "messages": [{"role": "user", "content": "test"}],
        "max_tokens": 1024,
        "tools": [
            {
                "name": "get_weather",
                "description": "Get weather info",
                "input_schema": {
                    "type": "object",
                    "properties": {"location": {"type": "string"}}
                }
            }
        ],
        "tool_choice": {"type": "auto"}
    });

    let result = translate_anthropic_request_to_openai(&body).unwrap();
    let tools = result["tools"].as_array().unwrap();
    assert_eq!(tools[0]["type"], "function");
    assert_eq!(tools[0]["function"]["name"], "get_weather");
    assert_eq!(result["tool_choice"], "auto");
}

#[test]
fn translate_anthropic_request_tool_result_array_content() {
    // tool_result with array content (structured response) should not be silently dropped
    let body = serde_json::json!({
        "model": "claude-sonnet-4-6",
        "messages": [
            {
                "role": "user",
                "content": [
                    {
                        "type": "tool_result",
                        "tool_use_id": "toolu_456",
                        "content": [
                            {"type": "text", "text": "Result line 1"},
                            {"type": "text", "text": "Result line 2"}
                        ]
                    }
                ]
            }
        ],
        "max_tokens": 1024
    });

    let result = translate_anthropic_request_to_openai(&body).unwrap();
    let msgs = result["messages"].as_array().unwrap();
    assert_eq!(msgs[0]["role"], "tool");
    assert_eq!(msgs[0]["content"], "Result line 1Result line 2");
}

#[test]
fn translate_anthropic_request_image_blocks() {
    // Anthropic image blocks must become OpenAI image_url parts, not be filtered out
    let body = serde_json::json!({
        "model": "claude-sonnet-4-6",
        "messages": [
            {"role": "user", "content": [
                {"type": "text", "text": "Describe this"},
                {"type": "image", "source": {
                    "type": "base64", "media_type": "image/jpeg", "data": "abc123"
                }},
                {"type": "image", "source": {
                    "type": "url", "url": "https://example.com/dog.png"
                }}
            ]}
        ],
        "max_tokens": 1024
    });

    let result = translate_anthropic_request_to_openai(&body).unwrap();
    let content = result["messages"][0]["content"].as_array().unwrap();
    assert_eq!(
        content[0],
        serde_json::json!({"type": "text", "text": "Describe this"})
    );
    assert_eq!(content[1]["type"], "image_url");
    assert_eq!(
        content[1]["image_url"]["url"],
        "data:image/jpeg;base64,abc123"
    );
    assert_eq!(content[2]["type"], "image_url");
    assert_eq!(
        content[2]["image_url"]["url"],
        "https://example.com/dog.png"
    );
}

#[test]
fn translate_anthropic_request_text_only_array_stays_string() {
    // Text-only block arrays keep the plain-string content form (no behavior change)
    let body = serde_json::json!({
        "model": "claude-sonnet-4-6",
        "messages": [
            {"role": "user", "content": [
                {"type": "text", "text": "part one "},
                {"type": "text", "text": "part two"}
            ]}
        ],
        "max_tokens": 1024
    });

    let result = translate_anthropic_request_to_openai(&body).unwrap();
    assert_eq!(result["messages"][0]["content"], "part one part two");
}

#[test]
fn translate_anthropic_request_image_beside_tool_result_survives() {
    // Images sharing a user message with tool_results must survive into the
    // leftover user message, not be text-filtered away
    let body = serde_json::json!({
        "model": "claude-sonnet-4-6",
        "messages": [
            {"role": "user", "content": [
                {"type": "tool_result", "tool_use_id": "toolu_789", "content": "done"},
                {"type": "text", "text": "And this image:"},
                {"type": "image", "source": {
                    "type": "url", "url": "https://example.com/chart.png"
                }}
            ]}
        ],
        "max_tokens": 1024
    });

    let result = translate_anthropic_request_to_openai(&body).unwrap();
    let msgs = result["messages"].as_array().unwrap();
    assert_eq!(msgs[0]["role"], "tool");
    assert_eq!(msgs[0]["content"], "done");
    assert_eq!(msgs[1]["role"], "user");
    let content = msgs[1]["content"].as_array().unwrap();
    assert_eq!(
        content[0],
        serde_json::json!({"type": "text", "text": "And this image:"})
    );
    assert_eq!(content[1]["type"], "image_url");
    assert_eq!(
        content[1]["image_url"]["url"],
        "https://example.com/chart.png"
    );
}

#[test]
fn reverse_sse_no_duplicate_message_stop() {
    let mut ctx = ReverseStreamContext::default();

    // Start message
    translate_openai_sse_to_anthropic(
            "{\"id\":\"chatcmpl-1\",\"model\":\"gpt-4\",\"choices\":[{\"delta\":{\"role\":\"assistant\",\"content\":\"\"},\"finish_reason\":null}]}",
            &mut ctx,
        );
    // Text
    translate_openai_sse_to_anthropic(
            "{\"id\":\"chatcmpl-1\",\"model\":\"gpt-4\",\"choices\":[{\"delta\":{\"content\":\"Hi\"},\"finish_reason\":null}]}",
            &mut ctx,
        );
    // Finish — emits message_stop
    let finish_events = translate_openai_sse_to_anthropic(
            "{\"id\":\"chatcmpl-1\",\"model\":\"gpt-4\",\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}]}",
            &mut ctx,
        );
    let stop_count = finish_events
        .iter()
        .filter(|e| e.contains("message_stop"))
        .count();
    assert_eq!(
        stop_count, 1,
        "finish_reason should emit exactly one message_stop"
    );

    // [DONE] — should NOT emit another message_stop
    let done_events = translate_openai_sse_to_anthropic("[DONE]", &mut ctx);
    assert!(
        done_events.is_empty(),
        "DONE after finish_reason should emit nothing (message_stop already sent)"
    );
}

#[test]
fn reverse_sse_message_stopped_set_by_both_terminator_paths() {
    // LAB-710: `ctx.terminal.completed` gates the transport-error frame — once
    // the client has its `message_stop`, a later read failure must not ship
    // an error frame. Both emit sites must set it: finish_reason (the normal
    // case) and a bare [DONE] with no finish_reason seen.
    let mut ctx = ReverseStreamContext::default();
    translate_openai_sse_to_anthropic(
        "{\"id\":\"c1\",\"model\":\"gpt-4\",\"choices\":[{\"delta\":{\"role\":\"assistant\",\"content\":\"Hi\"},\"finish_reason\":null}]}",
        &mut ctx,
    );
    assert!(!ctx.terminal.completed);
    translate_openai_sse_to_anthropic(
        "{\"id\":\"c1\",\"model\":\"gpt-4\",\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}]}",
        &mut ctx,
    );
    assert!(ctx.terminal.completed, "finish_reason emitted message_stop");

    // message_stop is terminal inside the translator too: an in-band error
    // line (or stray delta) arriving post-completion must emit nothing.
    let after = translate_openai_sse_to_anthropic(
        "{\"error\":{\"message\":\"late\",\"type\":\"server_error\"}}",
        &mut ctx,
    );
    assert!(
        after.is_empty(),
        "no frame may follow message_stop, got: {after:?}"
    );
    assert!(!ctx.terminal.errored);

    let mut ctx = ReverseStreamContext::default();
    translate_openai_sse_to_anthropic(
        "{\"id\":\"c1\",\"model\":\"gpt-4\",\"choices\":[{\"delta\":{\"role\":\"assistant\",\"content\":\"Hi\"},\"finish_reason\":null}]}",
        &mut ctx,
    );
    let done_events = translate_openai_sse_to_anthropic("[DONE]", &mut ctx);
    assert!(done_events[0].contains("message_stop"));
    assert!(ctx.terminal.completed, "bare [DONE] emitted message_stop");
}

#[test]
fn reverse_sse_inband_error_before_message_start() {
    // LAB-710: an in-band OpenAI {"error": {...}} line before any content
    // must emit an Anthropic `event: error` frame — previously it hit the
    // missing-choices early-return and the client got 200 + empty SSE body.
    let mut ctx = ReverseStreamContext::default();
    let events = translate_openai_sse_to_anthropic(
        "{\"error\":{\"message\":\"The server had an error\",\"type\":\"server_error\"}}",
        &mut ctx,
    );
    assert!(ctx.terminal.errored);
    assert_eq!(events.len(), 1);
    assert!(events[0].starts_with("event: error\n"));
    let data_line = events[0].lines().nth(1).unwrap();
    let body: serde_json::Value =
        serde_json::from_str(data_line.strip_prefix("data: ").unwrap()).unwrap();
    assert_eq!(body["type"], "error");
    assert_eq!(body["error"]["type"], "api_error");
    let msg = body["error"]["message"].as_str().unwrap();
    assert!(msg.contains("server_error"));
    assert!(msg.contains("The server had an error"));

    // No fake success terminator: trailing [DONE] after the error emits nothing.
    let after = translate_openai_sse_to_anthropic("[DONE]", &mut ctx);
    assert!(after.is_empty());
}

#[test]
fn reverse_sse_inband_error_mid_message_suppresses_message_stop() {
    // Error arriving after content started: error frame is final — no
    // message_stop may follow it, even if the upstream still sends [DONE].
    let mut ctx = ReverseStreamContext::default();
    translate_openai_sse_to_anthropic(
        "{\"id\":\"chatcmpl-1\",\"model\":\"gpt-4\",\"choices\":[{\"delta\":{\"role\":\"assistant\",\"content\":\"Hi\"},\"finish_reason\":null}]}",
        &mut ctx,
    );
    assert!(ctx.message_started);

    let err_events = translate_openai_sse_to_anthropic(
        "{\"error\":{\"message\":\"overloaded\",\"type\":\"server_error\"}}",
        &mut ctx,
    );
    assert_eq!(err_events.len(), 1);
    assert!(err_events[0].starts_with("event: error\n"));

    let done_events = translate_openai_sse_to_anthropic("[DONE]", &mut ctx);
    assert!(
        done_events.is_empty(),
        "no message_stop may follow an in-band error frame"
    );
}

#[test]
fn translate_anthropic_request_stop_sequences() {
    let body = serde_json::json!({
        "model": "claude-sonnet-4-6",
        "messages": [{"role": "user", "content": "test"}],
        "max_tokens": 1024,
        "stop_sequences": ["END", "STOP"]
    });

    let result = translate_anthropic_request_to_openai(&body).unwrap();
    assert_eq!(result["stop"], serde_json::json!(["END", "STOP"]));
}

#[test]
fn translate_anthropic_request_unsupported_image_source_fails_loudly() {
    // An image source type this translator can't represent (e.g. Anthropic's
    // `file` source) must fail the whole request, not silently drop the image
    // while keeping the surrounding text.
    let body = serde_json::json!({
        "model": "claude-sonnet-4-6",
        "messages": [
            {"role": "user", "content": [
                {"type": "text", "text": "Describe this"},
                {"type": "image", "source": {"type": "file", "file_id": "file_abc"}}
            ]}
        ],
        "max_tokens": 1024
    });

    let err = translate_anthropic_request_to_openai(&body).unwrap_err();
    assert!(
        err.contains("file"),
        "error should name the unsupported source type: {err}"
    );
}

#[test]
fn translate_anthropic_request_malformed_image_source_fails_loudly() {
    // A structurally invalid image source (missing required fields) must also
    // fail loudly rather than being silently dropped.
    let body = serde_json::json!({
        "model": "claude-sonnet-4-6",
        "messages": [
            {"role": "user", "content": [
                {"type": "image", "source": {"type": "base64", "media_type": "image/png"}}
            ]}
        ],
        "max_tokens": 1024
    });

    assert!(translate_anthropic_request_to_openai(&body).is_err());
}

#[test]
fn translate_anthropic_request_unsupported_block_type_fails_loudly() {
    // A content block type this translator can't represent (e.g. `document`)
    // must fail the whole request, not be silently dropped from the message.
    let body = serde_json::json!({
        "model": "claude-sonnet-4-6",
        "messages": [
            {"role": "user", "content": [
                {"type": "text", "text": "Summarize this"},
                {"type": "document", "source": {
                    "type": "base64", "media_type": "application/pdf", "data": "JVBERi0="
                }}
            ]}
        ],
        "max_tokens": 1024
    });

    let err = translate_anthropic_request_to_openai(&body).unwrap_err();
    assert!(
        err.contains("document"),
        "error should name the unsupported block type: {err}"
    );
}

#[test]
fn translate_openai_response_basic() {
    let body = serde_json::json!({
        "id": "chatcmpl-abc123",
        "object": "chat.completion",
        "model": "gpt-4",
        "choices": [{
            "index": 0,
            "message": {
                "role": "assistant",
                "content": "Hello!"
            },
            "finish_reason": "stop"
        }],
        "usage": {
            "prompt_tokens": 10,
            "completion_tokens": 5,
            "total_tokens": 15
        }
    });

    let result = translate_openai_response_to_anthropic(&body);
    assert_eq!(result["id"], "msg_abc123");
    assert_eq!(result["type"], "message");
    assert_eq!(result["model"], "gpt-4");
    assert_eq!(result["stop_reason"], "end_turn");
    assert_eq!(result["content"][0]["type"], "text");
    assert_eq!(result["content"][0]["text"], "Hello!");
    assert_eq!(result["usage"]["input_tokens"], 10);
    assert_eq!(result["usage"]["output_tokens"], 5);
}

#[test]
fn translate_openai_response_tool_calls() {
    let body = serde_json::json!({
        "id": "chatcmpl-xyz",
        "model": "gpt-4",
        "choices": [{
            "message": {
                "role": "assistant",
                "content": null,
                "tool_calls": [{
                    "id": "call_123",
                    "type": "function",
                    "function": {
                        "name": "search",
                        "arguments": "{\"query\":\"test\"}"
                    }
                }]
            },
            "finish_reason": "tool_calls"
        }],
        "usage": {"prompt_tokens": 10, "completion_tokens": 5, "total_tokens": 15}
    });

    let result = translate_openai_response_to_anthropic(&body);
    assert_eq!(result["stop_reason"], "tool_use");
    let blocks = result["content"].as_array().unwrap();
    assert_eq!(blocks[0]["type"], "tool_use");
    assert_eq!(blocks[0]["id"], "call_123");
    assert_eq!(blocks[0]["name"], "search");
    assert_eq!(blocks[0]["input"]["query"], "test");
}

#[test]
fn reverse_sse_basic_text() {
    let mut ctx = ReverseStreamContext::default();

    // First chunk with role
    let events = translate_openai_sse_to_anthropic(
            "{\"id\":\"chatcmpl-1\",\"model\":\"gpt-4\",\"choices\":[{\"delta\":{\"role\":\"assistant\",\"content\":\"\"},\"finish_reason\":null}]}",
            &mut ctx,
        );
    assert!(ctx.message_started);
    assert!(events.iter().any(|e| e.contains("message_start")));

    // Text delta
    let events = translate_openai_sse_to_anthropic(
            "{\"id\":\"chatcmpl-1\",\"model\":\"gpt-4\",\"choices\":[{\"delta\":{\"content\":\"Hello\"},\"finish_reason\":null}]}",
            &mut ctx,
        );
    assert!(events.iter().any(|e| e.contains("text_delta")));
    assert!(events.iter().any(|e| e.contains("Hello")));

    // Finish
    let events = translate_openai_sse_to_anthropic(
            "{\"id\":\"chatcmpl-1\",\"model\":\"gpt-4\",\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}]}",
            &mut ctx,
        );
    assert!(events.iter().any(|e| e.contains("message_delta")));
    assert!(events.iter().any(|e| e.contains("end_turn")));
    assert!(events.iter().any(|e| e.contains("message_stop")));
}

#[test]
fn reverse_sse_tool_use() {
    let mut ctx = ReverseStreamContext::default();

    // Tool call start
    let events = translate_openai_sse_to_anthropic(
            "{\"id\":\"chatcmpl-1\",\"model\":\"gpt-4\",\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"call_1\",\"type\":\"function\",\"function\":{\"name\":\"search\",\"arguments\":\"\"}}]},\"finish_reason\":null}]}",
            &mut ctx,
        );
    assert!(ctx.message_started);
    assert!(ctx.in_tool_use);
    assert!(events.iter().any(|e| e.contains("content_block_start")));
    assert!(events.iter().any(|e| e.contains("tool_use")));
    assert!(events.iter().any(|e| e.contains("search")));

    // Tool arguments delta
    let events = translate_openai_sse_to_anthropic(
            "{\"id\":\"chatcmpl-1\",\"model\":\"gpt-4\",\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0,\"function\":{\"arguments\":\"{\\\"q\\\"\"}}]},\"finish_reason\":null}]}",
            &mut ctx,
        );
    assert!(events.iter().any(|e| e.contains("input_json_delta")));

    // Finish
    let events = translate_openai_sse_to_anthropic(
            "{\"id\":\"chatcmpl-1\",\"model\":\"gpt-4\",\"choices\":[{\"delta\":{},\"finish_reason\":\"tool_calls\"}]}",
            &mut ctx,
        );
    assert!(events.iter().any(|e| e.contains("content_block_stop")));
    assert!(events.iter().any(|e| e.contains("tool_use")));
}

#[test]
fn reverse_sse_done_sentinel() {
    let mut ctx = ReverseStreamContext {
        message_started: true,
        ..ReverseStreamContext::default()
    };
    let events = translate_openai_sse_to_anthropic("[DONE]", &mut ctx);
    assert!(events.iter().any(|e| e.contains("message_stop")));
}

#[tokio::test]
async fn openai_chat_non_streaming() {
    // Spawn a mock that serves /v1/messages with Anthropic format
    let mock_app = Router::new().fallback(any(mock_anthropic_handler));
    let mock_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mock_addr = mock_listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(mock_listener, mock_app).await.unwrap();
    });

    let mock_url = format!("http://{}", mock_addr);
    let (app, _state) = test_openai_app(&mock_url, None);

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
            .body(r#"{"model":"claude-sonnet-4-6","messages":[{"role":"user","content":"Hello"}],"max_tokens":100}"#)
            .send()
            .await
            .unwrap();

    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["object"], "chat.completion");
    assert!(body["id"].as_str().unwrap().starts_with("chatcmpl-"));
    assert_eq!(
        body["choices"][0]["message"]["content"],
        "Hello from Claude"
    );
    assert_eq!(body["choices"][0]["finish_reason"], "stop");
    assert_eq!(body["usage"]["prompt_tokens"], 10);
    assert_eq!(body["usage"]["completion_tokens"], 5);
    assert_eq!(body["usage"]["total_tokens"], 15);
}

/// Mock whose reply IS a fenced code block — exercises the json_mode gate
/// end-to-end (GH #95 / LAB-711).
async fn mock_anthropic_fenced_handler(_req: Request<Body>) -> Response {
    axum::Json(serde_json::json!({
        "id": "msg_fenced",
        "type": "message",
        "content": [{"type": "text", "text": "```json\n{\"a\": 1}\n```"}],
        "model": "claude-sonnet-4-6",
        "stop_reason": "end_turn",
        "usage": {"input_tokens": 10, "output_tokens": 5}
    }))
    .into_response()
}

#[tokio::test]
async fn openai_chat_fence_strip_gated_on_response_format() {
    let mock_app = Router::new().fallback(any(mock_anthropic_fenced_handler));
    let mock_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mock_addr = mock_listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(mock_listener, mock_app).await.unwrap();
    });

    let (app, _state) = test_openai_app(&format!("http://{}", mock_addr), None);
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

    // Without response_format: fenced content passes through verbatim.
    let resp = client
        .post(format!("http://{}/v1/chat/completions", addr))
        .header("content-type", "application/json")
        .body(r#"{"model":"claude-sonnet-4-6","messages":[{"role":"user","content":"Hi"}]}"#)
        .send()
        .await
        .unwrap();
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(
        body["choices"][0]["message"]["content"], "```json\n{\"a\": 1}\n```",
        "non-JSON-mode reply must not be mutated"
    );

    // With response_format json_object: fences stripped.
    let resp = client
        .post(format!("http://{}/v1/chat/completions", addr))
        .header("content-type", "application/json")
        .body(r#"{"model":"claude-sonnet-4-6","messages":[{"role":"user","content":"Hi"}],"response_format":{"type":"json_object"}}"#)
        .send()
        .await
        .unwrap();
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(
        body["choices"][0]["message"]["content"], r#"{"a": 1}"#,
        "JSON-mode reply must have fences stripped"
    );
}

/// Streaming mock whose reply IS a fenced code block, split across three text
/// deltas so a fence spans SSE frame boundaries. Same reply as
/// `mock_anthropic_fenced_handler` for a direct stream/non-stream comparison.
async fn mock_anthropic_streaming_fenced_handler(req: Request<Body>) -> Response {
    let has_auth =
        req.headers().contains_key("x-api-key") || req.headers().contains_key("authorization");
    if !has_auth {
        return (StatusCode::UNAUTHORIZED, "missing auth").into_response();
    }
    let deltas = ["```js", "on\n{\"a\": 1}", "\n```"];
    let mut body = String::from(
        "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_fenced_stream\",\"type\":\"message\",\"role\":\"assistant\",\"model\":\"claude-sonnet-4-6\",\"content\":[],\"stop_reason\":null,\"usage\":{\"input_tokens\":10,\"output_tokens\":0}}}\n\nevent: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\n",
    );
    for d in deltas {
        let payload = serde_json::json!({
            "type": "content_block_delta",
            "index": 0,
            "delta": {"type": "text_delta", "text": d},
        });
        body.push_str(&format!("event: content_block_delta\ndata: {payload}\n\n"));
    }
    body.push_str(
        "event: content_block_stop\ndata: {\"type\":\"content_block_stop\",\"index\":0}\n\n",
    );
    body.push_str("event: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":5}}\n\n");
    body.push_str("event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n");
    Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "text/event-stream")
        .body(Body::from(body))
        .unwrap()
}

/// End-to-end: the streaming path must honour the same json_mode gate as
/// non-streaming, so a fenced reply yields identical assembled content across
/// transports (GH #95 / LAB-711 — closes the handler-plumbing gap that unit
/// tests on translate_sse_event alone cannot cover).
#[tokio::test]
async fn openai_chat_streaming_fence_strip_gated_on_response_format() {
    let mock_app = Router::new().fallback(any(mock_anthropic_streaming_fenced_handler));
    let mock_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mock_addr = mock_listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(mock_listener, mock_app).await.unwrap();
    });

    let (app, _state) = test_openai_app(&format!("http://{}", mock_addr), None);
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

    // Assemble the `content` a client would see from an SSE response body.
    fn assemble(body: &str) -> String {
        let mut content = String::new();
        for line in body.lines() {
            let Some(data) = line.strip_prefix("data: ") else {
                continue;
            };
            if data == "[DONE]" {
                continue;
            }
            if let Ok(v) = serde_json::from_str::<serde_json::Value>(data) {
                if let Some(c) = v["choices"][0]["delta"]["content"].as_str() {
                    content.push_str(c);
                }
            }
        }
        content
    }

    // stream:true without response_format → fenced content passes verbatim.
    let resp = client
        .post(format!("http://{}/v1/chat/completions", addr))
        .header("content-type", "application/json")
        .body(r#"{"model":"claude-sonnet-4-6","messages":[{"role":"user","content":"Hi"}],"stream":true}"#)
        .send()
        .await
        .unwrap();
    let body = resp.text().await.unwrap();
    assert_eq!(
        assemble(&body),
        "```json\n{\"a\": 1}\n```",
        "streaming non-JSON-mode must not mutate content"
    );

    // stream:true with response_format json_object → fences stripped, matching
    // the non-streaming json_object result exactly.
    let resp = client
        .post(format!("http://{}/v1/chat/completions", addr))
        .header("content-type", "application/json")
        .body(r#"{"model":"claude-sonnet-4-6","messages":[{"role":"user","content":"Hi"}],"stream":true,"response_format":{"type":"json_object"}}"#)
        .send()
        .await
        .unwrap();
    let body = resp.text().await.unwrap();
    assert_eq!(
        assemble(&body),
        r#"{"a": 1}"#,
        "streaming JSON-mode must strip fences, matching non-streaming"
    );
}

#[tokio::test]
async fn openai_chat_streaming() {
    let mock_app = Router::new().fallback(any(mock_anthropic_streaming_handler));
    let mock_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mock_addr = mock_listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(mock_listener, mock_app).await.unwrap();
    });

    let mock_url = format!("http://{}", mock_addr);
    let (app, state) = test_openai_app(&mock_url, None);

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
            .body(r#"{"model":"claude-sonnet-4-6","messages":[{"role":"user","content":"Hello"}],"max_tokens":100,"stream":true}"#)
            .send()
            .await
            .unwrap();

    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    assert_eq!(
        resp.headers().get("content-type").unwrap(),
        "text/event-stream"
    );

    let body = resp.text().await.unwrap();

    // Parse SSE events from response
    let mut chunks: Vec<serde_json::Value> = Vec::new();
    let mut got_done = false;
    for line in body.lines() {
        if line == "data: [DONE]" {
            got_done = true;
        } else if let Some(data) = line.strip_prefix("data: ") {
            if let Ok(v) = serde_json::from_str::<serde_json::Value>(data) {
                chunks.push(v);
            }
        }
    }

    assert!(got_done, "should have [DONE] sentinel");
    assert!(
        chunks.len() >= 3,
        "expected at least 3 chunks (role + content + finish), got {}",
        chunks.len()
    );

    // First chunk: role
    assert_eq!(chunks[0]["choices"][0]["delta"]["role"], "assistant");
    assert_eq!(chunks[0]["object"], "chat.completion.chunk");
    assert!(chunks[0]["id"].as_str().unwrap().starts_with("chatcmpl-"));

    // Content chunks
    let content_chunks: Vec<&str> = chunks
        .iter()
        .filter_map(|c| c["choices"][0]["delta"]["content"].as_str())
        .collect();
    assert_eq!(content_chunks.join(""), "Hello world");

    // Last data chunk: finish_reason
    let last = chunks.last().unwrap();
    assert_eq!(last["choices"][0]["finish_reason"], "stop");

    // LAB-717: usage is scanned incrementally (no raw_sse buffering) and
    // recorded by the detached finalize task after the stream closes — poll
    // briefly for it to land. The mock stream carries input=10 / output=5.
    assert_eq!(
        poll_streamed_usage(&state).await,
        (10, 5),
        "streamed usage must be recorded from the incremental scan"
    );
}

#[tokio::test]
async fn openai_chat_rejects_missing_auth() {
    let mock_app = Router::new().fallback(any(mock_anthropic_handler));
    let mock_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mock_addr = mock_listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(mock_listener, mock_app).await.unwrap();
    });

    let mock_url = format!("http://{}", mock_addr);
    let (app, _state) = test_openai_app(&mock_url, Some("secret-key".to_string()));

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
        .body(r#"{"model":"test","messages":[{"role":"user","content":"hi"}],"max_tokens":1}"#)
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), reqwest::StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn openai_chat_accepts_bearer_capitalized() {
    let mock_app = Router::new().fallback(any(mock_anthropic_handler));
    let mock_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mock_addr = mock_listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(mock_listener, mock_app).await.unwrap();
    });

    let mock_url = format!("http://{}", mock_addr);
    let (app, _state) = test_openai_app(&mock_url, Some("secret-key".to_string()));

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
        .header("authorization", "Bearer secret-key")
        .body(r#"{"model":"test","messages":[{"role":"user","content":"hi"}],"max_tokens":1}"#)
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), reqwest::StatusCode::OK);
}

#[tokio::test]
async fn openai_chat_accepts_bearer_lowercase() {
    let mock_app = Router::new().fallback(any(mock_anthropic_handler));
    let mock_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mock_addr = mock_listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(mock_listener, mock_app).await.unwrap();
    });

    let mock_url = format!("http://{}", mock_addr);
    let (app, _state) = test_openai_app(&mock_url, Some("secret-key".to_string()));

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
        .header("authorization", "bearer secret-key")
        .body(r#"{"model":"test","messages":[{"role":"user","content":"hi"}],"max_tokens":1}"#)
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), reqwest::StatusCode::OK);
}

#[tokio::test]
async fn openai_chat_wrong_apikey_valid_bearer() {
    let mock_app = Router::new().fallback(any(mock_anthropic_handler));
    let mock_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mock_addr = mock_listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(mock_listener, mock_app).await.unwrap();
    });

    let mock_url = format!("http://{}", mock_addr);
    let (app, _state) = test_openai_app(&mock_url, Some("secret-key".to_string()));

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
        .header("x-api-key", "wrong-key")
        .header("authorization", "Bearer secret-key")
        .body(r#"{"model":"test","messages":[{"role":"user","content":"hi"}],"max_tokens":1}"#)
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), reqwest::StatusCode::OK);
}

/// Integration test: verify OAuth accounts get the CC system prompt injected
/// in requests sent through the OpenAI-compat endpoint.
#[tokio::test]
async fn openai_compat_injects_oauth_system_prompt() {
    // Mock upstream that captures and validates the request body
    let mock_app = Router::new().fallback(any(|req: Request<Body>| async move {
        let body_bytes = axum::body::to_bytes(req.into_body(), 1024 * 1024)
            .await
            .unwrap();
        let body: serde_json::Value = serde_json::from_slice(&body_bytes).unwrap();

        // Verify the CC system prompt is the first system block
        let system = body.get("system").expect("missing system field");
        let arr = system.as_array().expect("system should be array");
        assert_eq!(
            arr[0]["text"].as_str().unwrap(),
            OAUTH_SYSTEM_PROMPT,
            "first system block must be CC prompt"
        );

        // Return valid Anthropic response
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
        resp
    }));

    let mock_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mock_addr = mock_listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(mock_listener, mock_app).await.unwrap();
    });

    // Build app with an OAuth account
    let accounts = vec![mk_endpoint_at(
        "oauth-acct",
        "sk-ant-oat01-test-token",
        &format!("http://{}", mock_addr),
    )];
    let state = Arc::new(AppState {
        endpoints: accounts,
        state_path: PathBuf::from("/tmp/anthropic-lb-oauth-test.state.json"),
        auto_cache: false, // disable to keep body simple
        ..test_state_base()
    });

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

    let client = Client::new();
    let resp = client
            .post(format!("http://{}/v1/chat/completions", addr))
            .header("content-type", "application/json")
            .body(r#"{"model":"claude-sonnet-4-6","messages":[{"role":"user","content":"hi"}],"max_tokens":5}"#)
            .send()
            .await
            .unwrap();

    assert_eq!(resp.status(), reqwest::StatusCode::OK);
}

/// Test that non-2xx upstream errors are translated to OpenAI error format.
#[tokio::test]
async fn openai_compat_translates_upstream_json_error() {
    // Mock upstream returns 400 with Anthropic-format error
    let mock_app = Router::new().fallback(any(|_req: Request<Body>| async {
            Response::builder()
                .status(StatusCode::BAD_REQUEST)
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"type":"error","error":{"type":"invalid_request_error","message":"max_tokens: must be positive"}}"#,
                ))
                .unwrap()
        }));

    let mock_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mock_addr = mock_listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(mock_listener, mock_app).await.unwrap();
    });

    let (app, _state) = test_openai_app(&format!("http://{}", mock_addr), None);
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
            .body(r#"{"model":"claude-sonnet-4-6","messages":[{"role":"user","content":"hi"}],"max_tokens":5}"#)
            .send()
            .await
            .unwrap();

    assert_eq!(resp.status(), reqwest::StatusCode::BAD_REQUEST);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["error"]["message"], "max_tokens: must be positive");
    assert_eq!(body["error"]["type"], "invalid_request_error");
    assert!(body["error"]["param"].is_null());
}

/// Test that non-JSON upstream errors are wrapped in OpenAI error format.
#[tokio::test]
async fn openai_compat_translates_upstream_raw_error() {
    // Mock upstream returns 422 with plain text (non-retryable, non-JSON)
    let mock_app = Router::new().fallback(any(|_req: Request<Body>| async {
        Response::builder()
            .status(StatusCode::UNPROCESSABLE_ENTITY)
            .header("content-type", "text/plain")
            .body(Body::from("upstream timeout"))
            .unwrap()
    }));

    let mock_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mock_addr = mock_listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(mock_listener, mock_app).await.unwrap();
    });

    let (app, _state) = test_openai_app(&format!("http://{}", mock_addr), None);
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
            .body(r#"{"model":"claude-sonnet-4-6","messages":[{"role":"user","content":"hi"}],"max_tokens":5}"#)
            .send()
            .await
            .unwrap();

    assert_eq!(resp.status(), reqwest::StatusCode::UNPROCESSABLE_ENTITY);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["error"]["message"], "upstream timeout");
    assert_eq!(body["error"]["type"], "api_error");
}

/// Find this test's own `proxied (openai-compat)` line (the only line
/// carrying the client-id marker), then the one line with `message` and
/// that line's `req_id`, and check it names the same `account`. Test states
/// share `instance_id` 0, so the `req_id` alone is not unique across tests;
/// the message narrows it.
fn compat_log_line_for(output: &str, marker: &str, message: &str) -> String {
    let proxied = output
        .lines()
        .find(|l| l.contains(marker) && l.contains("proxied (openai-compat)"))
        .unwrap_or_else(|| panic!("no proxied line for {marker}"));
    let field = |name: &str| {
        proxied
            .split_whitespace()
            .find(|f| f.starts_with(name))
            .unwrap_or_else(|| panic!("proxied line carries {name}"))
            .to_owned()
    };
    let (req_id, account) = (field("req_id="), field("account="));
    let mine: Vec<&str> = output
        .lines()
        .filter(|l| l.contains(message) && l.contains(&req_id))
        .collect();
    assert_eq!(
        mine.len(),
        1,
        "expected one {message:?} line with {req_id}, got:\n{}",
        mine.join("\n")
    );
    assert!(
        mine[0].contains(&account),
        "{message:?} line must name the request's {account}, got: {}",
        mine[0]
    );
    mine[0].to_owned()
}

/// LAB-5313: a 2xx upstream body that isn't valid JSON is not a success.
/// The compat path must return 502 with an OpenAI-shaped error rather than
/// pass the untranslated bytes through under 200, and log the parse error
/// with the request's `req_id` and endpoint.
#[tokio::test]
async fn openai_compat_non_json_2xx_body_returns_502() {
    let buf = log_capture_buf();
    let mock_app = Router::new().fallback(any(|_req: Request<Body>| async {
        Response::builder()
            .status(StatusCode::OK)
            .header("content-type", "application/json")
            .body(Body::from(r#"{"id":"msg_truncated","content":[{"#))
            .unwrap()
    }));
    let mock_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mock_addr = mock_listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(mock_listener, mock_app).await.unwrap();
    });
    let (app, _state) = test_openai_app(&format!("http://{mock_addr}"), None);
    let addr = serve(app).await;

    let marker = "lab5313-non-json-2xx-marker";
    let resp = Client::new()
        .post(format!("http://{addr}/v1/chat/completions"))
        .header("content-type", "application/json")
        .header("x-client-id", marker)
        .body(r#"{"model":"claude-sonnet-4-6","messages":[{"role":"user","content":"hi"}],"max_tokens":5}"#)
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), reqwest::StatusCode::BAD_GATEWAY);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["error"]["type"], "api_error");
    assert_eq!(body["error"]["message"], "invalid upstream response");

    let output = String::from_utf8(buf.lock().unwrap().clone()).unwrap();
    let line = compat_log_line_for(&output, marker, "upstream 2xx body is not valid JSON");
    assert!(
        line.contains(" ERROR ") && line.contains("error="),
        "parse error must be logged with the endpoint and the error, got: {line}"
    );
}

/// LAB-5313: a non-2xx upstream whose error body can't be read (here the
/// connection drops short of `content-length`) logs the read error as a
/// field, then carries on with an empty body.
#[tokio::test]
async fn openai_compat_error_body_read_failure_is_logged() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let buf = log_capture_buf();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mock_addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut req = vec![0u8; 4096];
        let _ = stream.read(&mut req).await;
        // 422 is non-retryable, so the one response reaches the client.
        let _ = stream
            .write_all(b"HTTP/1.1 422 Unprocessable Entity\r\ncontent-length: 100\r\n\r\n{\"type\"")
            .await;
        let _ = stream.shutdown().await;
    });
    let (app, _state) = test_openai_app(&format!("http://{mock_addr}"), None);
    let addr = serve(app).await;

    let marker = "lab5313-error-body-read-marker";
    let resp = Client::new()
        .post(format!("http://{addr}/v1/chat/completions"))
        .header("content-type", "application/json")
        .header("x-client-id", marker)
        .body(r#"{"model":"claude-sonnet-4-6","messages":[{"role":"user","content":"hi"}],"max_tokens":5}"#)
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), reqwest::StatusCode::UNPROCESSABLE_ENTITY);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(
        body["error"]["message"], "",
        "a failed read carries on with the empty body"
    );

    let output = String::from_utf8(buf.lock().unwrap().clone()).unwrap();
    let line = compat_log_line_for(&output, marker, "failed to read upstream error body");
    assert!(
        line.contains(" WARN ") && line.contains("error="),
        "error-body read failure must be logged with the error as a field, got: {line}"
    );
}

// ── Connection resilience: synthetic SSE error on upstream disconnect ──

#[test]
fn anthropic_error_frame_is_well_formed_sse() {
    let bytes = anthropic_error_frame("connection reset by peer");
    let s = std::str::from_utf8(&bytes).expect("frame must be utf8");
    assert!(s.starts_with("event: error\n"), "must use SSE event syntax");
    assert!(s.ends_with("\n\n"), "must terminate with blank line");
    let data_line = s
        .lines()
        .find_map(|l| l.strip_prefix("data: "))
        .expect("must have a data: line");
    let parsed: serde_json::Value =
        serde_json::from_str(data_line).expect("data payload must be valid JSON");
    assert_eq!(parsed["type"], "error");
    // Must be one of Anthropic's documented SSE error types so the
    // SDK doesn't reject the frame and fall back to "socket closed".
    assert_eq!(parsed["error"]["type"], "api_error");
    assert_eq!(parsed["error"]["message"], "connection reset by peer");
}

#[test]
fn openai_error_frame_includes_done_marker() {
    let bytes = openai_error_frame("upstream gone");
    let s = std::str::from_utf8(&bytes).expect("frame must be utf8");
    assert!(
        s.contains("\ndata: [DONE]\n\n"),
        "must terminate with OpenAI [DONE] marker so the client parser closes cleanly: {s:?}"
    );
    let first_data = s
        .lines()
        .find_map(|l| l.strip_prefix("data: "))
        .expect("must have a leading data: line");
    let parsed: serde_json::Value =
        serde_json::from_str(first_data).expect("first data payload must be valid JSON");
    assert_eq!(parsed["error"]["type"], "upstream_error");
    assert_eq!(parsed["error"]["message"], "upstream gone");
}

/// When an upstream Anthropic SSE stream dies mid-flight, the downstream
/// client must receive a synthetic `event: error` frame rather than a bare
/// TCP FIN. Without this guarantee the Claude Code CLI surfaces the
/// uninterpretable "socket connection was closed unexpectedly" error.
#[tokio::test]
async fn streaming_upstream_disconnect_emits_sse_error_to_client() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    // Mock upstream: replies with text/event-stream + one partial SSE
    // chunk, then drops the socket without writing the terminating
    // `message_stop` event or a 0-length chunked terminator.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mock_addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut buf = vec![0u8; 4096];
        let _ = stream.read(&mut buf).await;
        let reset = (std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs()
            + 3600)
            .to_string();
        let head = format!(
            "HTTP/1.1 200 OK\r\n\
                 content-type: text/event-stream\r\n\
                 transfer-encoding: chunked\r\n\
                 anthropic-ratelimit-unified-representative-claim: five_hour\r\n\
                 anthropic-ratelimit-unified-5h-utilization: 0.10\r\n\
                 anthropic-ratelimit-unified-5h-reset: {reset}\r\n\
                 \r\n"
        );
        let _ = stream.write_all(head.as_bytes()).await;
        // One valid chunk, then drop. Chunk = hex-size, CRLF, bytes, CRLF.
        let body = "event: message_start\ndata: {\"type\":\"message_start\"}\n\n";
        let chunk = format!("{:x}\r\n{}\r\n", body.len(), body);
        let _ = stream.write_all(chunk.as_bytes()).await;
        let _ = stream.shutdown().await;
    });

    let upstream_url = format!("http://{}", mock_addr);
    let (app, _state) = test_app(&upstream_url, None);

    let app_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let app_addr = app_listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(
            app_listener,
            app.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await
        .unwrap();
    });

    let client = reqwest::Client::new();
    let resp = client
            .post(format!("http://{}/v1/messages", app_addr))
            .header("content-type", "application/json")
            .header("accept", "text/event-stream")
            .header("x-api-key", "any")
            .body(
                r#"{"model":"claude-sonnet-4-6","stream":true,"messages":[{"role":"user","content":"hi"}]}"#,
            )
            .send()
            .await
            .unwrap();

    assert_eq!(resp.status(), 200);
    let body = resp.bytes().await.unwrap();
    let body_s = std::str::from_utf8(&body).expect("body utf8");

    assert!(
        body_s.contains("event: error\n"),
        "downstream must receive synthetic SSE error frame on upstream disconnect, got: {body_s:?}"
    );
    assert!(
        body_s.contains("\"type\":\"api_error\""),
        "error frame must use Anthropic's documented api_error type, got: {body_s:?}"
    );
}
