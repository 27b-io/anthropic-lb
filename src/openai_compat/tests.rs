use super::*;

// ── Unit: OpenAI request translation ────────────────────────────

#[test]
fn translate_request_extracts_system() {
    let req = serde_json::json!({
        "model": "claude-sonnet-4-6",
        "messages": [
            {"role": "system", "content": "You are helpful"},
            {"role": "user", "content": "Hello"}
        ],
        "max_tokens": 1024
    });
    let result = translate_openai_to_anthropic(&req);
    assert_eq!(result["system"], "You are helpful");
    let msgs = result["messages"].as_array().unwrap();
    assert_eq!(msgs.len(), 1);
    assert_eq!(msgs[0]["role"], "user");
    assert_eq!(msgs[0]["content"], "Hello");
}

#[test]
fn translate_request_multi_system_concat() {
    let req = serde_json::json!({
        "model": "claude-sonnet-4-6",
        "messages": [
            {"role": "system", "content": "Rule 1"},
            {"role": "system", "content": "Rule 2"},
            {"role": "user", "content": "Hello"}
        ],
        "max_tokens": 100
    });
    let result = translate_openai_to_anthropic(&req);
    assert_eq!(result["system"], "Rule 1\n\nRule 2");
}

#[test]
fn translate_request_system_array_content() {
    // OpenAI permits system content as an array of text parts — must not be dropped
    let req = serde_json::json!({
        "model": "claude-sonnet-4-6",
        "messages": [
            {"role": "system", "content": [
                {"type": "text", "text": "You are "},
                {"type": "text", "text": "helpful"}
            ]},
            {"role": "user", "content": "Hello"}
        ],
        "max_tokens": 100
    });
    let result = translate_openai_to_anthropic(&req);
    assert_eq!(result["system"], "You are helpful");
    assert_eq!(result["messages"].as_array().unwrap().len(), 1);
}

#[test]
fn translate_request_image_url_to_image_block() {
    // OpenAI image_url parts must become Anthropic image blocks, not pass through raw
    let req = serde_json::json!({
        "model": "claude-sonnet-4-6",
        "messages": [
            {"role": "user", "content": [
                {"type": "text", "text": "What is this?"},
                {"type": "image_url", "image_url": {"url": "data:image/png;base64,iVBORw0KGgo="}},
                {"type": "image_url", "image_url": {"url": "https://example.com/cat.jpg"}}
            ]}
        ],
        "max_tokens": 100
    });
    let result = translate_openai_to_anthropic(&req);
    let content = result["messages"][0]["content"].as_array().unwrap();
    assert_eq!(
        content[0],
        serde_json::json!({"type": "text", "text": "What is this?"})
    );
    // data: URL → base64 source
    assert_eq!(content[1]["type"], "image");
    assert_eq!(content[1]["source"]["type"], "base64");
    assert_eq!(content[1]["source"]["media_type"], "image/png");
    assert_eq!(content[1]["source"]["data"], "iVBORw0KGgo=");
    // plain URL → url source
    assert_eq!(content[2]["type"], "image");
    assert_eq!(content[2]["source"]["type"], "url");
    assert_eq!(content[2]["source"]["url"], "https://example.com/cat.jpg");
}

#[test]
fn translate_request_no_system() {
    let req = serde_json::json!({
        "model": "claude-sonnet-4-6",
        "messages": [
            {"role": "user", "content": "Hello"}
        ],
        "max_tokens": 100
    });
    let result = translate_openai_to_anthropic(&req);
    assert!(result.get("system").is_none());
}

#[test]
fn translate_request_default_max_tokens() {
    let req = serde_json::json!({
        "model": "claude-sonnet-4-6",
        "messages": [{"role": "user", "content": "Hello"}]
    });
    let result = translate_openai_to_anthropic(&req);
    assert_eq!(result["max_tokens"], 4096);
}

#[test]
fn translate_request_stop_to_stop_sequences() {
    let req = serde_json::json!({
        "model": "claude-sonnet-4-6",
        "messages": [{"role": "user", "content": "Hello"}],
        "max_tokens": 100,
        "stop": ["END", "STOP"]
    });
    let result = translate_openai_to_anthropic(&req);
    let seqs = result["stop_sequences"].as_array().unwrap();
    assert_eq!(seqs.len(), 2);
    assert_eq!(seqs[0], "END");
    assert_eq!(seqs[1], "STOP");
}

#[test]
fn translate_request_passthrough_params() {
    let req = serde_json::json!({
        "model": "claude-sonnet-4-6",
        "messages": [{"role": "user", "content": "Hello"}],
        "max_tokens": 512,
        "temperature": 0.7,
        "top_p": 0.9,
        "stream": true
    });
    let result = translate_openai_to_anthropic(&req);
    assert_eq!(result["model"], "claude-sonnet-4-6");
    assert_eq!(result["max_tokens"], 512);
    assert_eq!(result["temperature"], 0.7);
    assert_eq!(result["top_p"], 0.9);
    assert_eq!(result["stream"], true);
}

#[test]
fn model_rejects_temperature_by_version() {
    // Claude 5 family and ≥ 4.7 hard-reject (LAB-798)
    assert!(model_rejects_temperature("claude-sonnet-5"));
    assert!(model_rejects_temperature("claude-opus-5"));
    assert!(model_rejects_temperature("claude-fable-5"));
    assert!(model_rejects_temperature("claude-fable-5[1m]"));
    assert!(model_rejects_temperature("claude-opus-4-8"));
    assert!(model_rejects_temperature("claude-sonnet-4-7-20260101"));
    // ≤ 4.6 still accepts
    assert!(!model_rejects_temperature("claude-sonnet-4-6"));
    assert!(!model_rejects_temperature("claude-sonnet-4-5-20250929"));
    assert!(!model_rejects_temperature("claude-haiku-4-5-20251001"));
    assert!(!model_rejects_temperature("claude-opus-4-1-20250805"));
    assert!(!model_rejects_temperature("claude-opus-4-20250514"));
    // old-style ids (version before family) and unknown families pass through
    assert!(!model_rejects_temperature("claude-3-5-sonnet-20241022"));
    assert!(!model_rejects_temperature("gpt-4o"));
}

#[test]
fn translate_request_drops_temperature_for_rejecting_model() {
    let req = serde_json::json!({
        "model": "claude-sonnet-5",
        "messages": [{"role": "user", "content": "Hello"}],
        "temperature": 0.2,
        "top_p": 0.9
    });
    let result = translate_openai_to_anthropic(&req);
    // temperature dropped (upstream hard-rejects it); other params untouched
    assert!(result.get("temperature").is_none());
    assert_eq!(result["top_p"], 0.9);
}

#[test]
fn translate_request_keeps_default_temperature_for_rejecting_model() {
    // temperature: 1 is the one value the API still accepts — pass it through
    let req = serde_json::json!({
        "model": "claude-fable-5",
        "messages": [{"role": "user", "content": "Hello"}],
        "temperature": 1
    });
    let result = translate_openai_to_anthropic(&req);
    assert_eq!(result["temperature"], 1);
}

#[test]
fn translate_request_forwards_non_numeric_temperature_unchanged() {
    // Non-numeric junk is not a "confirmed non-default numeric" — forward it
    // so the client gets the same upstream type error as on ≤ 4.6 models
    // instead of the shim silently masking their bug.
    let req = serde_json::json!({
        "model": "claude-sonnet-5",
        "messages": [{"role": "user", "content": "Hello"}],
        "temperature": "0.7"
    });
    let result = translate_openai_to_anthropic(&req);
    assert_eq!(result["temperature"], "0.7");
}

#[test]
fn translate_request_maps_reasoning_effort_to_output_config() {
    for effort in ["low", "medium", "high", "xhigh", "max"] {
        let req = serde_json::json!({
            "model": "claude-opus-5-5",
            "messages": [{"role": "user", "content": "Hello"}],
            "reasoning_effort": effort
        });
        let result = translate_openai_to_anthropic(&req);
        assert_eq!(
            result["output_config"],
            serde_json::json!({"effort": effort})
        );
        // OpenAI-only field never reaches the Anthropic body
        assert!(result.get("reasoning_effort").is_none());
        // adaptive models think on their own; the shim must not synthesise it
        assert!(result.get("thinking").is_none());
    }
}

#[test]
fn translate_request_drops_unmappable_reasoning_effort() {
    // `minimal`/`none` have no Anthropic equivalent; unknown strings, wrong
    // case and non-strings would 400 upstream — drop them all. `null` is
    // treated as absent (no warn), and also yields no `output_config`.
    for effort in [
        serde_json::json!("minimal"),
        serde_json::json!("none"),
        serde_json::json!("ultra"),
        serde_json::json!("HIGH"),
        serde_json::json!(3),
        serde_json::Value::Null,
    ] {
        let req = serde_json::json!({
            "model": "claude-opus-5-5",
            "messages": [{"role": "user", "content": "Hello"}],
            "reasoning_effort": effort
        });
        let result = translate_openai_to_anthropic(&req);
        assert!(
            result.get("output_config").is_none(),
            "effort {effort} must be dropped"
        );
    }
}

#[test]
fn translate_request_without_reasoning_effort_has_no_output_config() {
    let req = serde_json::json!({
        "model": "claude-opus-5-5",
        "messages": [{"role": "user", "content": "Hello"}]
    });
    let result = translate_openai_to_anthropic(&req);
    assert!(result.get("output_config").is_none());
}

#[test]
fn translate_request_strips_name_field() {
    let req = serde_json::json!({
        "model": "claude-sonnet-4-6",
        "messages": [
            {"role": "user", "content": "Hello", "name": "bob"}
        ],
        "max_tokens": 100
    });
    let result = translate_openai_to_anthropic(&req);
    let msgs = result["messages"].as_array().unwrap();
    assert!(msgs[0].get("name").is_none());
}

#[test]
fn translate_request_tools() {
    let req = serde_json::json!({
        "model": "claude-sonnet-4-6",
        "messages": [{"role": "user", "content": "What's the weather?"}],
        "max_tokens": 100,
        "tools": [{
            "type": "function",
            "function": {
                "name": "get_weather",
                "description": "Get the weather",
                "parameters": {
                    "type": "object",
                    "properties": {"location": {"type": "string"}},
                    "required": ["location"]
                }
            }
        }]
    });
    let result = translate_openai_to_anthropic(&req);
    let tools = result["tools"].as_array().unwrap();
    assert_eq!(tools.len(), 1);
    assert_eq!(tools[0]["name"], "get_weather");
    assert_eq!(tools[0]["description"], "Get the weather");
    assert_eq!(tools[0]["input_schema"]["type"], "object");
    assert!(tools[0].get("type").is_none()); // no OpenAI "type":"function" wrapper
}

#[test]
fn translate_request_tool_choice_variants() {
    // auto
    let req = serde_json::json!({
        "model": "claude-sonnet-4-6",
        "messages": [{"role": "user", "content": "Hi"}],
        "tool_choice": "auto"
    });
    let result = translate_openai_to_anthropic(&req);
    assert_eq!(result["tool_choice"]["type"], "auto");

    // required → any
    let req = serde_json::json!({
        "model": "claude-sonnet-4-6",
        "messages": [{"role": "user", "content": "Hi"}],
        "tool_choice": "required"
    });
    let result = translate_openai_to_anthropic(&req);
    assert_eq!(result["tool_choice"]["type"], "any");

    // specific function
    let req = serde_json::json!({
        "model": "claude-sonnet-4-6",
        "messages": [{"role": "user", "content": "Hi"}],
        "tool_choice": {"type": "function", "function": {"name": "search"}}
    });
    let result = translate_openai_to_anthropic(&req);
    assert_eq!(result["tool_choice"]["type"], "tool");
    assert_eq!(result["tool_choice"]["name"], "search");

    // none → omitted
    let req = serde_json::json!({
        "model": "claude-sonnet-4-6",
        "messages": [{"role": "user", "content": "Hi"}],
        "tool_choice": "none"
    });
    let result = translate_openai_to_anthropic(&req);
    assert!(result.get("tool_choice").is_none());
}

#[test]
fn translate_request_tool_result_message() {
    let req = serde_json::json!({
        "model": "claude-sonnet-4-6",
        "messages": [
            {"role": "user", "content": "What's the weather?"},
            {"role": "assistant", "content": null, "tool_calls": [{
                "id": "call_123",
                "type": "function",
                "function": {"name": "get_weather", "arguments": "{\"location\":\"SF\"}"}
            }]},
            {"role": "tool", "tool_call_id": "call_123", "content": "72°F and sunny"}
        ],
        "max_tokens": 100
    });
    let result = translate_openai_to_anthropic(&req);
    let msgs = result["messages"].as_array().unwrap();
    // First message: user text
    assert_eq!(msgs[0]["role"], "user");
    assert_eq!(msgs[0]["content"], "What's the weather?");
    // Second message: assistant with tool_use block
    assert_eq!(msgs[1]["role"], "assistant");
    let blocks = msgs[1]["content"].as_array().unwrap();
    assert_eq!(blocks[0]["type"], "tool_use");
    assert_eq!(blocks[0]["id"], "call_123");
    assert_eq!(blocks[0]["name"], "get_weather");
    assert_eq!(blocks[0]["input"]["location"], "SF");
    // Third message: tool result → user with tool_result
    assert_eq!(msgs[2]["role"], "user");
    let result_blocks = msgs[2]["content"].as_array().unwrap();
    assert_eq!(result_blocks[0]["type"], "tool_result");
    assert_eq!(result_blocks[0]["tool_use_id"], "call_123");
    assert_eq!(result_blocks[0]["content"], "72°F and sunny");
}

#[test]
fn translate_request_assistant_tool_calls_with_text() {
    let req = serde_json::json!({
        "model": "claude-sonnet-4-6",
        "messages": [
            {"role": "assistant", "content": "Let me check.", "tool_calls": [{
                "id": "tc_1",
                "type": "function",
                "function": {"name": "search", "arguments": "{\"q\":\"rust\"}"}
            }]}
        ],
        "max_tokens": 100
    });
    let result = translate_openai_to_anthropic(&req);
    let msgs = result["messages"].as_array().unwrap();
    let blocks = msgs[0]["content"].as_array().unwrap();
    assert_eq!(blocks.len(), 2);
    assert_eq!(blocks[0]["type"], "text");
    assert_eq!(blocks[0]["text"], "Let me check.");
    assert_eq!(blocks[1]["type"], "tool_use");
    assert_eq!(blocks[1]["name"], "search");
}

#[test]
fn translate_request_consecutive_tool_results_merged() {
    // Parallel tool calls produce consecutive role:"tool" messages.
    // Anthropic rejects consecutive same-role messages, so they must merge.
    let req = serde_json::json!({
        "model": "claude-sonnet-4-6",
        "messages": [
            {"role": "user", "content": "Weather in SF and NYC?"},
            {"role": "assistant", "content": null, "tool_calls": [
                {"id": "c1", "type": "function", "function": {"name": "weather", "arguments": "{\"city\":\"SF\"}"}},
                {"id": "c2", "type": "function", "function": {"name": "weather", "arguments": "{\"city\":\"NYC\"}"}}
            ]},
            {"role": "tool", "tool_call_id": "c1", "content": "72F"},
            {"role": "tool", "tool_call_id": "c2", "content": "45F"}
        ],
        "max_tokens": 100
    });
    let result = translate_openai_to_anthropic(&req);
    let msgs = result["messages"].as_array().unwrap();
    // Should be 3 messages: user, assistant, user(merged tool results)
    assert_eq!(msgs.len(), 3);
    assert_eq!(msgs[2]["role"], "user");
    let blocks = msgs[2]["content"].as_array().unwrap();
    assert_eq!(blocks.len(), 2);
    assert_eq!(blocks[0]["tool_use_id"], "c1");
    assert_eq!(blocks[0]["content"], "72F");
    assert_eq!(blocks[1]["tool_use_id"], "c2");
    assert_eq!(blocks[1]["content"], "45F");
}

#[test]
fn translate_request_tool_result_does_not_merge_into_regular_user_msg() {
    // A user message with array-form content should NOT have tool_results
    // appended to it — that would corrupt the original message.
    let req = serde_json::json!({
        "model": "claude-sonnet-4-6",
        "messages": [
            {"role": "user", "content": [{"type": "text", "text": "Hello"}]},
            {"role": "assistant", "content": null, "tool_calls": [{
                "id": "c1", "type": "function",
                "function": {"name": "test", "arguments": "{}"}
            }]},
            {"role": "tool", "tool_call_id": "c1", "content": "done"}
        ],
        "max_tokens": 100
    });
    let result = translate_openai_to_anthropic(&req);
    let msgs = result["messages"].as_array().unwrap();
    // user, assistant, user(tool_result) — three separate messages
    assert_eq!(msgs.len(), 3);
    // First user message should be untouched
    let first_user = msgs[0]["content"].as_array().unwrap();
    assert_eq!(first_user.len(), 1);
    assert_eq!(first_user[0]["type"], "text");
    // Third message is the tool_result
    let tool_msg = msgs[2]["content"].as_array().unwrap();
    assert_eq!(tool_msg[0]["type"], "tool_result");
}

#[test]
fn translate_request_tool_choice_none_removes_tools() {
    let req = serde_json::json!({
        "model": "claude-sonnet-4-6",
        "messages": [{"role": "user", "content": "Hi"}],
        "tools": [{"type": "function", "function": {"name": "search", "description": "Search", "parameters": {"type": "object"}}}],
        "tool_choice": "none"
    });
    let result = translate_openai_to_anthropic(&req);
    assert!(result.get("tools").is_none());
    assert!(result.get("tool_choice").is_none());
}

#[test]
fn translate_request_malformed_arguments_json() {
    let req = serde_json::json!({
        "model": "claude-sonnet-4-6",
        "messages": [
            {"role": "assistant", "content": null, "tool_calls": [{
                "id": "tc_bad",
                "type": "function",
                "function": {"name": "test", "arguments": "not valid json"}
            }]}
        ],
        "max_tokens": 100
    });
    let result = translate_openai_to_anthropic(&req);
    let msgs = result["messages"].as_array().unwrap();
    let blocks = msgs[0]["content"].as_array().unwrap();
    // Should fall back to empty object, not panic
    assert_eq!(blocks[0]["input"], serde_json::json!({}));
}

#[test]
fn translate_request_tool_no_parameters_gets_empty_schema() {
    let req = serde_json::json!({
        "model": "claude-sonnet-4-6",
        "messages": [{"role": "user", "content": "Hi"}],
        "max_tokens": 100,
        "tools": [{"type": "function", "function": {"name": "get_time", "description": "Get current time"}}]
    });
    let result = translate_openai_to_anthropic(&req);
    let tools = result["tools"].as_array().unwrap();
    assert_eq!(tools[0]["name"], "get_time");
    // input_schema must always be present for Anthropic API
    assert_eq!(
        tools[0]["input_schema"],
        serde_json::json!({"type": "object", "properties": {}})
    );
}

#[test]
fn translate_request_tool_null_parameters_gets_empty_schema() {
    let req = serde_json::json!({
        "model": "claude-sonnet-4-6",
        "messages": [{"role": "user", "content": "Hi"}],
        "max_tokens": 100,
        "tools": [{"type": "function", "function": {"name": "ping", "description": "Ping", "parameters": null}}]
    });
    let result = translate_openai_to_anthropic(&req);
    let tools = result["tools"].as_array().unwrap();
    assert_eq!(
        tools[0]["input_schema"],
        serde_json::json!({"type": "object", "properties": {}})
    );
}

#[test]
fn translate_request_tool_result_array_content() {
    let req = serde_json::json!({
        "model": "claude-sonnet-4-6",
        "messages": [
            {"role": "tool", "tool_call_id": "c1", "content": [
                {"type": "text", "text": "Result A"},
                {"type": "text", "text": " Result B"}
            ]}
        ],
        "max_tokens": 100
    });
    let result = translate_openai_to_anthropic(&req);
    let msgs = result["messages"].as_array().unwrap();
    let blocks = msgs[0]["content"].as_array().unwrap();
    assert_eq!(blocks[0]["content"], "Result A Result B");
}

#[test]
fn translate_request_assistant_array_content_with_tool_calls() {
    let req = serde_json::json!({
        "model": "claude-sonnet-4-6",
        "messages": [
            {"role": "assistant", "content": [{"type": "text", "text": "Thinking..."}], "tool_calls": [{
                "id": "tc_1",
                "type": "function",
                "function": {"name": "search", "arguments": "{}"}
            }]}
        ],
        "max_tokens": 100
    });
    let result = translate_openai_to_anthropic(&req);
    let msgs = result["messages"].as_array().unwrap();
    let blocks = msgs[0]["content"].as_array().unwrap();
    assert_eq!(blocks[0]["type"], "text");
    assert_eq!(blocks[0]["text"], "Thinking...");
    assert_eq!(blocks[1]["type"], "tool_use");
}

// ── Unit: OpenAI response translation ───────────────────────────

#[test]
fn translate_response_basic() {
    let resp = serde_json::json!({
        "id": "msg_abc123",
        "type": "message",
        "content": [{"type": "text", "text": "Hello!"}],
        "model": "claude-sonnet-4-6",
        "stop_reason": "end_turn",
        "usage": {"input_tokens": 10, "output_tokens": 5}
    });
    let result = translate_anthropic_to_openai(&resp, false);
    assert_eq!(result["id"], "chatcmpl-msg_abc123");
    assert_eq!(result["object"], "chat.completion");
    assert_eq!(result["choices"][0]["message"]["role"], "assistant");
    assert_eq!(result["choices"][0]["message"]["content"], "Hello!");
    assert_eq!(result["choices"][0]["finish_reason"], "stop");
}

#[test]
fn translate_response_usage_mapping() {
    let resp = serde_json::json!({
        "id": "msg_x",
        "content": [{"type": "text", "text": "ok"}],
        "model": "claude-sonnet-4-6",
        "stop_reason": "end_turn",
        "usage": {"input_tokens": 25, "output_tokens": 15}
    });
    let result = translate_anthropic_to_openai(&resp, false);
    assert_eq!(result["usage"]["prompt_tokens"], 25);
    assert_eq!(result["usage"]["completion_tokens"], 15);
    assert_eq!(result["usage"]["total_tokens"], 40);
}

#[test]
fn translate_response_stop_reason_mapping() {
    assert_eq!(map_stop_reason("end_turn"), "stop");
    assert_eq!(map_stop_reason("max_tokens"), "length");
    assert_eq!(map_stop_reason("stop_sequence"), "stop");
    assert_eq!(map_stop_reason("unknown"), "stop");
}

// ── Unit: JSON fence stripping ──────────────────────────────────

#[test]
fn strip_json_fences_with_lang_tag() {
    let input = "```json\n{\"key\": \"value\"}\n```";
    assert_eq!(strip_json_fences(input), r#"{"key": "value"}"#);
}

#[test]
fn strip_json_fences_no_lang_tag() {
    let input = "```\n{\"key\": \"value\"}\n```";
    assert_eq!(strip_json_fences(input), r#"{"key": "value"}"#);
}

#[test]
fn strip_json_fences_passthrough_plain_json() {
    let input = r#"{"key": "value"}"#;
    assert_eq!(strip_json_fences(input), input);
}

#[test]
fn strip_json_fences_with_whitespace() {
    let input = "  ```json\n{\"a\": 1}\n```  ";
    assert_eq!(strip_json_fences(input), r#"{"a": 1}"#);
}

#[test]
fn translate_response_strips_markdown_fences() {
    // Fence stripping is gated on the request having asked for
    // response_format: json_object (json_mode = true).
    let resp = serde_json::json!({
        "id": "msg_fenced",
        "content": [{"type": "text", "text": "```json\n{\"skipSearch\": true}\n```"}],
        "model": "claude-sonnet-4-6",
        "stop_reason": "end_turn",
        "usage": {"input_tokens": 10, "output_tokens": 5}
    });
    let result = translate_anthropic_to_openai(&resp, true);
    assert_eq!(
        result["choices"][0]["message"]["content"],
        r#"{"skipSearch": true}"#
    );
}

#[test]
fn translate_response_preserves_fences_without_json_mode() {
    // A normal chat reply that IS a fenced code block must pass through
    // verbatim — fences, language tag, and surrounding whitespace intact.
    let text = "```python\nprint(\"hi\")\n```";
    let resp = serde_json::json!({
        "id": "msg_code",
        "content": [{"type": "text", "text": text}],
        "model": "claude-sonnet-4-6",
        "stop_reason": "end_turn",
        "usage": {"input_tokens": 10, "output_tokens": 5}
    });
    let result = translate_anthropic_to_openai(&resp, false);
    assert_eq!(result["choices"][0]["message"]["content"], text);
}

#[test]
fn wants_json_object_detection() {
    assert!(wants_json_object(&serde_json::json!({
        "response_format": {"type": "json_object"}
    })));
    assert!(!wants_json_object(&serde_json::json!({
        "response_format": {"type": "text"}
    })));
    assert!(!wants_json_object(&serde_json::json!({})));
}

#[test]
fn translate_response_tool_use_blocks() {
    let resp = serde_json::json!({
        "id": "msg_tool",
        "type": "message",
        "content": [
            {"type": "tool_use", "id": "toolu_123", "name": "get_weather", "input": {"location": "SF"}}
        ],
        "model": "claude-sonnet-4-6",
        "stop_reason": "tool_use",
        "usage": {"input_tokens": 20, "output_tokens": 15}
    });
    let result = translate_anthropic_to_openai(&resp, false);
    assert_eq!(result["choices"][0]["finish_reason"], "tool_calls");
    assert!(result["choices"][0]["message"]["content"].is_null());
    let tcs = result["choices"][0]["message"]["tool_calls"]
        .as_array()
        .unwrap();
    assert_eq!(tcs.len(), 1);
    assert_eq!(tcs[0]["id"], "toolu_123");
    assert_eq!(tcs[0]["type"], "function");
    assert_eq!(tcs[0]["function"]["name"], "get_weather");
    let args: serde_json::Value =
        serde_json::from_str(tcs[0]["function"]["arguments"].as_str().unwrap()).unwrap();
    assert_eq!(args["location"], "SF");
}

#[test]
fn translate_response_mixed_text_and_tool_use() {
    let resp = serde_json::json!({
        "id": "msg_mixed",
        "type": "message",
        "content": [
            {"type": "text", "text": "Let me check."},
            {"type": "tool_use", "id": "toolu_456", "name": "search", "input": {"q": "rust"}}
        ],
        "model": "claude-sonnet-4-6",
        "stop_reason": "tool_use",
        "usage": {"input_tokens": 10, "output_tokens": 10}
    });
    let result = translate_anthropic_to_openai(&resp, false);
    assert_eq!(result["choices"][0]["message"]["content"], "Let me check.");
    let tcs = result["choices"][0]["message"]["tool_calls"]
        .as_array()
        .unwrap();
    assert_eq!(tcs.len(), 1);
    assert_eq!(tcs[0]["function"]["name"], "search");
}

// ── Unit: SSE event translation ─────────────────────────────────

#[test]
fn translate_sse_message_start() {
    let mut ctx = StreamContext::default();
    let raw = "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_test\",\"model\":\"claude-sonnet-4-6\",\"role\":\"assistant\"}}";
    let result = translate_sse_event(raw, &mut ctx).unwrap();
    assert!(result.starts_with("data: "));
    assert_eq!(ctx.id, "chatcmpl-msg_test");
    assert_eq!(ctx.model, "claude-sonnet-4-6");
    let chunk: serde_json::Value =
        serde_json::from_str(result.strip_prefix("data: ").unwrap().trim()).unwrap();
    assert_eq!(chunk["choices"][0]["delta"]["role"], "assistant");
}

#[test]
fn translate_sse_content_delta() {
    let mut ctx = StreamContext {
        id: "chatcmpl-test".to_string(),
        model: "claude-sonnet-4-6".to_string(),
        ..Default::default()
    };
    let raw = "event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"Hello world\"}}";
    let result = translate_sse_event(raw, &mut ctx).unwrap();
    let chunk: serde_json::Value =
        serde_json::from_str(result.strip_prefix("data: ").unwrap().trim()).unwrap();
    assert_eq!(chunk["choices"][0]["delta"]["content"], "Hello world");
    assert!(chunk["choices"][0]["finish_reason"].is_null());
}

#[test]
fn translate_sse_message_delta() {
    let mut ctx = StreamContext {
        id: "chatcmpl-test".to_string(),
        ..Default::default()
    };
    let raw = "event: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":5}}";
    let result = translate_sse_event(raw, &mut ctx).unwrap();
    let chunk: serde_json::Value =
        serde_json::from_str(result.strip_prefix("data: ").unwrap().trim()).unwrap();
    assert_eq!(chunk["choices"][0]["finish_reason"], "stop");
}

#[test]
fn translate_sse_message_stop() {
    let mut ctx = StreamContext::default();
    let raw = "event: message_stop\ndata: {\"type\":\"message_stop\"}";
    let result = translate_sse_event(raw, &mut ctx).unwrap();
    assert_eq!(result, "data: [DONE]\n\n");
}

#[test]
fn translate_sse_skips_ping() {
    let mut ctx = StreamContext::default();
    let raw = "event: ping\ndata: {\"type\":\"ping\"}";
    assert!(translate_sse_event(raw, &mut ctx).is_none());
}

#[test]
fn translate_sse_tool_use_content_block_start() {
    let mut ctx = StreamContext::default();
    let raw = "event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"tool_use\",\"id\":\"toolu_abc\",\"name\":\"get_weather\",\"input\":{}}}";
    let result = translate_sse_event(raw, &mut ctx).unwrap();
    let chunk: serde_json::Value =
        serde_json::from_str(result.strip_prefix("data: ").unwrap().trim()).unwrap();
    let tc = &chunk["choices"][0]["delta"]["tool_calls"][0];
    assert_eq!(tc["index"], 0);
    assert_eq!(tc["id"], "toolu_abc");
    assert_eq!(tc["function"]["name"], "get_weather");
    assert_eq!(tc["function"]["arguments"], "");
    assert!(ctx.in_tool_use);
    assert_eq!(ctx.tool_call_index, 0);
}

#[test]
fn translate_sse_tool_use_input_json_delta() {
    let mut ctx = StreamContext {
        in_tool_use: true,
        tool_call_index: 0,
        ..Default::default()
    };
    let raw = "event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"input_json_delta\",\"partial_json\":\"{\\\"loc\"}}";
    let result = translate_sse_event(raw, &mut ctx).unwrap();
    let chunk: serde_json::Value =
        serde_json::from_str(result.strip_prefix("data: ").unwrap().trim()).unwrap();
    let tc = &chunk["choices"][0]["delta"]["tool_calls"][0];
    assert_eq!(tc["index"], 0);
    assert_eq!(tc["function"]["arguments"], "{\"loc");
}

#[test]
fn translate_sse_content_block_stop_resets_tool_state() {
    let mut ctx = StreamContext {
        in_tool_use: true,
        tool_call_index: 0,
        ..Default::default()
    };
    let raw = "event: content_block_stop\ndata: {\"type\":\"content_block_stop\",\"index\":0}";
    let result = translate_sse_event(raw, &mut ctx);
    assert!(result.is_none());
    assert!(!ctx.in_tool_use);
}

#[test]
fn translate_sse_tool_use_stop_reason() {
    let mut ctx = StreamContext::default();
    let raw = "event: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"tool_use\"},\"usage\":{\"output_tokens\":10}}";
    let result = translate_sse_event(raw, &mut ctx).unwrap();
    let chunk: serde_json::Value =
        serde_json::from_str(result.strip_prefix("data: ").unwrap().trim()).unwrap();
    assert_eq!(chunk["choices"][0]["finish_reason"], "tool_calls");
}

#[test]
fn translate_sse_inband_error_emits_openai_error_frame() {
    // LAB-710: an Anthropic `event: error` mid-stream must reach the OpenAI
    // client as an error frame, not vanish into the `_ => None` arm.
    let mut ctx = StreamContext::default();
    let raw = "event: error\ndata: {\"type\":\"error\",\"error\":{\"type\":\"overloaded_error\",\"message\":\"Overloaded\"}}";
    let result = translate_sse_event(raw, &mut ctx).unwrap();

    // Flag set → stream loop finalizes as failure and skips the clean [DONE]
    // guard (the error frame carries its own terminator).
    assert!(ctx.terminal.errored);

    // Exactly one [DONE], and it belongs to the error frame itself.
    assert_eq!(result.matches("[DONE]").count(), 1);
    assert!(result.ends_with("data: [DONE]\n\n"));

    let first_event = result.split("\n\n").next().unwrap();
    let chunk: serde_json::Value =
        serde_json::from_str(first_event.strip_prefix("data: ").unwrap()).unwrap();
    assert_eq!(chunk["error"]["type"], "upstream_error");
    let msg = chunk["error"]["message"].as_str().unwrap();
    assert!(msg.contains("overloaded_error"));
    assert!(msg.contains("Overloaded"));
}

#[test]
fn translate_sse_text_block_start_skipped() {
    let mut ctx = StreamContext::default();
    let raw = "event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}";
    let result = translate_sse_event(raw, &mut ctx);
    assert!(result.is_none());
    assert!(!ctx.in_tool_use);
}

#[test]
fn translate_sse_multiple_tool_calls() {
    let mut ctx = StreamContext::default();

    // First tool_use block
    let raw1 = "event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"tool_use\",\"id\":\"toolu_1\",\"name\":\"search\",\"input\":{}}}";
    translate_sse_event(raw1, &mut ctx).unwrap();
    assert_eq!(ctx.tool_call_index, 0);

    // Close first
    let stop1 = "event: content_block_stop\ndata: {\"type\":\"content_block_stop\",\"index\":0}";
    translate_sse_event(stop1, &mut ctx);

    // Second tool_use block
    let raw2 = "event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":1,\"content_block\":{\"type\":\"tool_use\",\"id\":\"toolu_2\",\"name\":\"fetch\",\"input\":{}}}";
    let result = translate_sse_event(raw2, &mut ctx).unwrap();
    assert_eq!(ctx.tool_call_index, 1);
    let chunk: serde_json::Value =
        serde_json::from_str(result.strip_prefix("data: ").unwrap().trim()).unwrap();
    assert_eq!(chunk["choices"][0]["delta"]["tool_calls"][0]["index"], 1);
    assert_eq!(
        chunk["choices"][0]["delta"]["tool_calls"][0]["id"],
        "toolu_2"
    );
}

// ── Regression: streaming ↔ non-streaming content parity (LAB-711) ──
// A fenced reply must yield identical assembled content whether the client
// used stream:true or stream:false, in both default and json_object modes.
// Fences can be split across SSE deltas, so the streaming path buffers text
// and strips the whole message once — the same strip the non-streaming path
// applies. See GH #95 / codex-sol review.

/// Reconstruct the OpenAI `content` a client would assemble from an Anthropic
/// text reply delivered as `text_deltas` SSE frames.
fn reconstruct_stream_content(text_deltas: &[&str], json_mode: bool) -> String {
    let mut ctx = StreamContext {
        json_mode,
        ..Default::default()
    };
    let mut events: Vec<String> = vec![
        "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_x\",\"model\":\"claude-sonnet-4-6\",\"role\":\"assistant\"}}".to_string(),
        "event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}".to_string(),
    ];
    for d in text_deltas {
        let payload = serde_json::json!({
            "type": "content_block_delta",
            "index": 0,
            "delta": {"type": "text_delta", "text": d},
        });
        events.push(format!("event: content_block_delta\ndata: {payload}"));
    }
    events.push(
        "event: content_block_stop\ndata: {\"type\":\"content_block_stop\",\"index\":0}"
            .to_string(),
    );
    events.push("event: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":5}}".to_string());
    events.push("event: message_stop\ndata: {\"type\":\"message_stop\"}".to_string());

    let mut content = String::new();
    for ev in &events {
        let Some(out) = translate_sse_event(ev, &mut ctx) else {
            continue;
        };
        // One translation may carry multiple `data:` frames (buffered content
        // flushed together with the finish chunk).
        for frame in out.split("\n\n") {
            let Some(json) = frame.trim().strip_prefix("data: ") else {
                continue;
            };
            if json == "[DONE]" {
                continue;
            }
            let chunk: serde_json::Value = serde_json::from_str(json).unwrap();
            if let Some(c) = chunk["choices"][0]["delta"]["content"].as_str() {
                content.push_str(c);
            }
        }
    }
    content
}

/// Non-streaming assembled content for the same reply text.
fn nonstream_content(full_text: &str, json_mode: bool) -> String {
    let resp = serde_json::json!({
        "id": "msg_x",
        "content": [{"type": "text", "text": full_text}],
        "model": "claude-sonnet-4-6",
        "stop_reason": "end_turn",
        "usage": {"input_tokens": 1, "output_tokens": 1},
    });
    translate_anthropic_to_openai(&resp, json_mode)["choices"][0]["message"]["content"]
        .as_str()
        .unwrap()
        .to_string()
}

#[test]
fn stream_nonstream_content_parity_json_mode() {
    // Fence deliberately split across three SSE deltas.
    let deltas = ["```jso", "n\n{\"ok\":true}\n``", "`"];
    let full: String = deltas.concat();
    assert_eq!(full, "```json\n{\"ok\":true}\n```");

    let stream_json = reconstruct_stream_content(&deltas, true);
    let nonstream_json = nonstream_content(&full, true);
    // Both transports strip the fence down to raw JSON, and agree.
    assert_eq!(stream_json, r#"{"ok":true}"#);
    assert_eq!(stream_json, nonstream_json);
}

#[test]
fn stream_nonstream_content_parity_default_mode() {
    // Same reply, default (non-JSON) mode: fences preserved on both transports.
    let deltas = ["```jso", "n\n{\"ok\":true}\n``", "`"];
    let full: String = deltas.concat();

    let stream_default = reconstruct_stream_content(&deltas, false);
    let nonstream_default = nonstream_content(&full, false);
    assert_eq!(stream_default, full);
    assert_eq!(stream_default, nonstream_default);
}

#[test]
fn json_mode_stream_buffers_deltas_then_flushes_content_before_finish() {
    let mut ctx = StreamContext {
        json_mode: true,
        ..Default::default()
    };

    let start = "event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}";
    translate_sse_event(start, &mut ctx);

    // The complete fenced JSON reply spans two deltas. Neither partial delta
    // may be forwarded, because fence removal is applied to the whole reply.
    for text in ["```json\n{\"first\":", "true}\n```"] {
        let payload = serde_json::json!({
            "type": "content_block_delta",
            "index": 0,
            "delta": {"type": "text_delta", "text": text},
        });
        let event = format!("event: content_block_delta\ndata: {payload}");
        assert!(translate_sse_event(&event, &mut ctx).is_none());
    }

    let finish = "event: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":1}}";
    let output = translate_sse_event(finish, &mut ctx).unwrap();
    let frames: Vec<_> = output
        .split("\n\n")
        .filter_map(|frame| frame.strip_prefix("data: "))
        .map(|frame| serde_json::from_str::<serde_json::Value>(frame).unwrap())
        .collect();

    // The buffered, fence-stripped content is emitted exactly once and always
    // precedes the OpenAI finish chunk.
    assert_eq!(frames.len(), 2);
    assert_eq!(
        frames[0]["choices"][0]["delta"]["content"],
        r#"{"first":true}"#
    );
    assert!(frames[0]["choices"][0]["finish_reason"].is_null());
    assert_eq!(frames[1]["choices"][0]["delta"], serde_json::json!({}));
    assert_eq!(frames[1]["choices"][0]["finish_reason"], "stop");
    assert_eq!(
        translate_sse_event(
            "event: message_stop\ndata: {\"type\":\"message_stop\"}",
            &mut ctx
        )
        .as_deref(),
        Some("data: [DONE]\n\n")
    );
}

#[test]
fn json_mode_stream_message_stop_safety_net_flushes_buffer() {
    // Abnormal upstream: message_stop arrives without a preceding
    // message_delta. The buffered JSON-mode content must still be flushed
    // (fence-stripped) ahead of [DONE], never silently dropped.
    let mut ctx = StreamContext {
        json_mode: true,
        ..Default::default()
    };
    let payload = serde_json::json!({
        "type": "content_block_delta",
        "index": 0,
        "delta": {"type": "text_delta", "text": "```json\n{\"ok\":true}\n```"},
    });
    assert!(translate_sse_event(
        &format!("event: content_block_delta\ndata: {payload}"),
        &mut ctx
    )
    .is_none());

    let output = translate_sse_event(
        "event: message_stop\ndata: {\"type\":\"message_stop\"}",
        &mut ctx,
    )
    .unwrap();
    let mut frames = output
        .split("\n\n")
        .filter_map(|f| f.strip_prefix("data: "));
    let content: serde_json::Value = serde_json::from_str(frames.next().unwrap()).unwrap();
    assert_eq!(content["choices"][0]["delta"]["content"], r#"{"ok":true}"#);
    assert_eq!(frames.next(), Some("[DONE]"));
    // The stream loop detects the terminator with ends_with("data: [DONE]\n\n")
    // — a combined frame that failed this would earn a second [DONE] from the
    // post-loop guard (LAB-710 panel finding).
    assert!(output.ends_with("data: [DONE]\n\n"));
}
