use crate::*;

// ── Reverse translation: Anthropic → OpenAI (for upstream fallback) ──

/// Map OpenAI finish_reason back to Anthropic stop_reason.
fn reverse_map_stop_reason(reason: &str) -> &'static str {
    match reason {
        "stop" => "end_turn",
        "length" => "max_tokens",
        "tool_calls" => "tool_use",
        _ => "end_turn",
    }
}

/// Translate an Anthropic Messages API request body to OpenAI Chat Completions format.
/// Reverse of `translate_openai_to_anthropic`.
/// Anthropic `image` block → OpenAI `image_url` part. Base64 sources repack as a
/// `data:` URL. Err when the source can't be represented faithfully — missing
/// fields, or a source type this translator doesn't handle (e.g. Anthropic's
/// `file` source) — so the caller fails the request loudly instead of silently
/// dropping the image.
fn anthropic_image_block_to_openai(block: &serde_json::Value) -> Result<serde_json::Value, String> {
    let source = block
        .get("source")
        .ok_or("image block missing \"source\"")?;
    let source_type = source
        .get("type")
        .and_then(|t| t.as_str())
        .ok_or("image source missing \"type\"")?;
    let url = match source_type {
        "url" => source
            .get("url")
            .and_then(|u| u.as_str())
            .ok_or("image url source missing \"url\"")?
            .to_string(),
        "base64" => {
            let media_type = source
                .get("media_type")
                .and_then(|m| m.as_str())
                .ok_or("image base64 source missing \"media_type\"")?;
            let data = source
                .get("data")
                .and_then(|d| d.as_str())
                .ok_or("image base64 source missing \"data\"")?;
            format!("data:{};base64,{}", media_type, data)
        }
        other => {
            return Err(format!(
                "image source type \"{other}\" is not supported by the OpenAI-compat translator"
            ));
        }
    };
    Ok(serde_json::json!({"type": "image_url", "image_url": {"url": url}}))
}

/// Anthropic user content blocks → OpenAI content: a plain string when text-only
/// (the common case, and what OpenAI-compat upstreams handle most reliably), a
/// content-part array when image blocks are present so images survive translation.
/// Err on an unrepresentable image or an unsupported block type (e.g. `document`)
/// rather than dropping content silently.
fn anthropic_user_blocks_to_openai_content(
    blocks: &[&serde_json::Value],
) -> Result<serde_json::Value, String> {
    let mut parts: Vec<serde_json::Value> = Vec::new();
    let mut has_image = false;
    for b in blocks {
        match b.get("type").and_then(|t| t.as_str()) {
            Some("text") => {
                if let Some(t) = b.get("text").and_then(|t| t.as_str()) {
                    parts.push(serde_json::json!({"type": "text", "text": t}));
                }
            }
            Some("image") => {
                parts.push(anthropic_image_block_to_openai(b)?);
                has_image = true;
            }
            Some(other) => {
                return Err(format!(
                    "content block type \"{other}\" is not supported by the OpenAI-compat translator"
                ));
            }
            None => {}
        }
    }
    Ok(if has_image {
        serde_json::Value::Array(parts)
    } else {
        let text: String = parts
            .iter()
            .filter_map(|p| p.get("text").and_then(|t| t.as_str()))
            .collect::<Vec<_>>()
            .join("");
        serde_json::Value::String(text)
    })
}

// Test-only invocation counter for `translate_anthropic_request_to_openai`
// (LAB-716 AC: translation must run at most once per request, not per retry
// attempt). Thread-local: `#[tokio::test]` defaults to a current-thread
// runtime on the test's own thread, so parallel tests can't cross-pollute.
#[cfg(test)]
thread_local! {
    pub(crate) static TRANSLATE_A2O_CALLS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// Err (with a client-facing message) when a message contains an image this
/// translator can't represent faithfully — the caller must surface a real
/// error instead of silently forwarding a request with the image dropped.
pub(crate) fn translate_anthropic_request_to_openai(
    body: &serde_json::Value,
) -> Result<serde_json::Value, String> {
    #[cfg(test)]
    TRANSLATE_A2O_CALLS.with(|c| c.set(c.get() + 1));

    let mut out = serde_json::Map::new();

    if let Some(model) = body.get("model") {
        out.insert("model".to_string(), model.clone());
    }

    let mut messages: Vec<serde_json::Value> = Vec::new();

    // System prompt → system role message(s)
    if let Some(system) = body.get("system") {
        if let Some(s) = system.as_str() {
            if !s.is_empty() {
                messages.push(serde_json::json!({"role": "system", "content": s}));
            }
        } else if let Some(arr) = system.as_array() {
            // Array of system blocks → concatenate text
            let text: String = arr
                .iter()
                .filter_map(|b| b.get("text").and_then(|t| t.as_str()))
                .collect::<Vec<_>>()
                .join("\n\n");
            if !text.is_empty() {
                messages.push(serde_json::json!({"role": "system", "content": text}));
            }
        }
    }

    // Messages
    if let Some(msgs) = body.get("messages").and_then(|m| m.as_array()) {
        for msg in msgs {
            let role = msg.get("role").and_then(|r| r.as_str()).unwrap_or("");
            let content = msg.get("content");

            if role == "assistant" {
                // Check for tool_use blocks in content array
                if let Some(blocks) = content.and_then(|c| c.as_array()) {
                    let has_tool_use = blocks
                        .iter()
                        .any(|b| b.get("type").and_then(|t| t.as_str()) == Some("tool_use"));

                    if has_tool_use {
                        // Extract text blocks as content, tool_use blocks as tool_calls
                        let text: String = blocks
                            .iter()
                            .filter(|b| b.get("type").and_then(|t| t.as_str()) == Some("text"))
                            .filter_map(|b| b.get("text").and_then(|t| t.as_str()))
                            .collect::<Vec<_>>()
                            .join("");

                        let tool_calls: Vec<serde_json::Value> = blocks
                            .iter()
                            .filter(|b| b.get("type").and_then(|t| t.as_str()) == Some("tool_use"))
                            .map(|b| {
                                let id = b.get("id").and_then(|v| v.as_str()).unwrap_or("");
                                let name = b.get("name").and_then(|v| v.as_str()).unwrap_or("");
                                let input =
                                    b.get("input").cloned().unwrap_or(serde_json::json!({}));
                                serde_json::json!({
                                    "id": id,
                                    "type": "function",
                                    "function": {
                                        "name": name,
                                        "arguments": input.to_string(),
                                    }
                                })
                            })
                            .collect();

                        let mut m = serde_json::json!({"role": "assistant"});
                        if text.is_empty() {
                            m["content"] = serde_json::Value::Null;
                        } else {
                            m["content"] = serde_json::Value::String(text);
                        }
                        m["tool_calls"] = serde_json::Value::Array(tool_calls);
                        messages.push(m);
                        continue;
                    }
                }
                // Plain assistant message
                let text = content
                    .and_then(|c| {
                        c.as_str().map(|s| s.to_string()).or_else(|| {
                            c.as_array().map(|arr| {
                                arr.iter()
                                    .filter(|b| {
                                        b.get("type").and_then(|t| t.as_str()) == Some("text")
                                    })
                                    .filter_map(|b| b.get("text").and_then(|t| t.as_str()))
                                    .collect::<Vec<_>>()
                                    .join("")
                            })
                        })
                    })
                    .unwrap_or_default();
                messages.push(serde_json::json!({"role": "assistant", "content": text}));
            } else if role == "user" {
                // Check for tool_result blocks
                if let Some(blocks) = content.and_then(|c| c.as_array()) {
                    let tool_results: Vec<&serde_json::Value> = blocks
                        .iter()
                        .filter(|b| b.get("type").and_then(|t| t.as_str()) == Some("tool_result"))
                        .collect();

                    if !tool_results.is_empty() {
                        // Each tool_result → separate OpenAI tool message
                        for tr in &tool_results {
                            let tool_call_id =
                                tr.get("tool_use_id").and_then(|v| v.as_str()).unwrap_or("");
                            let content_val = tr.get("content");
                            let content_str = content_val
                                .and_then(|c| c.as_str())
                                .map(|s| s.to_string())
                                .or_else(|| {
                                    content_val?.as_array().map(|parts| {
                                        parts
                                            .iter()
                                            .filter_map(|p| p.get("text").and_then(|t| t.as_str()))
                                            .collect::<Vec<_>>()
                                            .join("")
                                    })
                                })
                                .unwrap_or_default();
                            messages.push(serde_json::json!({
                                "role": "tool",
                                "tool_call_id": tool_call_id,
                                "content": content_str,
                            }));
                        }

                        // Also emit any non-tool_result blocks (text + images) as a user message
                        let leftover: Vec<&serde_json::Value> = blocks
                            .iter()
                            .filter(|b| {
                                b.get("type").and_then(|t| t.as_str()) != Some("tool_result")
                            })
                            .collect();
                        let user_content = anthropic_user_blocks_to_openai_content(&leftover)?;
                        let non_empty = match &user_content {
                            serde_json::Value::String(s) => !s.is_empty(),
                            serde_json::Value::Array(a) => !a.is_empty(),
                            _ => false,
                        };
                        if non_empty {
                            messages.push(serde_json::json!({
                                "role": "user",
                                "content": user_content,
                            }));
                        }
                        continue;
                    }
                }
                // Plain user message — string passes through; block arrays keep
                // text AND images (images previously filtered out silently)
                if let Some(c) = content {
                    if let Some(s) = c.as_str() {
                        messages.push(serde_json::json!({"role": "user", "content": s}));
                    } else if let Some(arr) = c.as_array() {
                        let block_refs: Vec<&serde_json::Value> = arr.iter().collect();
                        let user_content = anthropic_user_blocks_to_openai_content(&block_refs)?;
                        messages.push(serde_json::json!({"role": "user", "content": user_content}));
                    }
                }
            }
        }
    }

    out.insert("messages".to_string(), serde_json::Value::Array(messages));

    // max_tokens → max_tokens
    if let Some(mt) = body.get("max_tokens") {
        out.insert("max_tokens".to_string(), mt.clone());
    }

    // Passthrough params — same LAB-798 `temperature` guard as the forward
    // translator: an OpenAI-protocol endpoint can front Claude ≥ 4.7 (empty
    // `models` list serves everything), and the deprecated param 400s there
    // too. Policy in `drops_deprecated_temperature`.
    let model_name = body.get("model").and_then(|m| m.as_str()).unwrap_or("");
    for key in &["temperature", "top_p", "stream"] {
        if let Some(v) = body.get(*key) {
            if *key == "temperature" && drops_deprecated_temperature(model_name, v) {
                continue;
            }
            out.insert(key.to_string(), v.clone());
        }
    }

    // stop_sequences → stop
    if let Some(ss) = body.get("stop_sequences") {
        out.insert("stop".to_string(), ss.clone());
    }

    // tools: Anthropic → OpenAI function format
    if let Some(tools) = body.get("tools").and_then(|t| t.as_array()) {
        let openai_tools: Vec<serde_json::Value> = tools
            .iter()
            .filter_map(|tool| {
                let name = tool.get("name")?.as_str()?;
                let mut func = serde_json::json!({"name": name});
                if let Some(desc) = tool.get("description") {
                    func["description"] = desc.clone();
                }
                if let Some(schema) = tool.get("input_schema") {
                    func["parameters"] = schema.clone();
                }
                Some(serde_json::json!({"type": "function", "function": func}))
            })
            .collect();
        if !openai_tools.is_empty() {
            out.insert("tools".to_string(), serde_json::Value::Array(openai_tools));
        }
    }

    // tool_choice translation
    if let Some(tc) = body.get("tool_choice") {
        let tc_type = tc.get("type").and_then(|t| t.as_str()).unwrap_or("");
        let openai_tc = match tc_type {
            "auto" => Some(serde_json::json!("auto")),
            "any" => Some(serde_json::json!("required")),
            "tool" => tc
                .get("name")
                .and_then(|n| n.as_str())
                .map(|name| serde_json::json!({"type": "function", "function": {"name": name}})),
            _ => None,
        };
        if let Some(otc) = openai_tc {
            out.insert("tool_choice".to_string(), otc);
        }
    }

    Ok(serde_json::Value::Object(out))
}

/// Translate an OpenAI Chat Completions response to Anthropic Messages API format.
/// Reverse of `translate_anthropic_to_openai`.
pub(crate) fn translate_openai_response_to_anthropic(
    body: &serde_json::Value,
) -> serde_json::Value {
    let id = body
        .get("id")
        .and_then(|v| v.as_str())
        .unwrap_or("chatcmpl-unknown")
        .strip_prefix("chatcmpl-")
        .unwrap_or("msg_unknown");
    let model = body
        .get("model")
        .and_then(|v| v.as_str())
        .unwrap_or("unknown");

    let choice = body
        .pointer("/choices/0")
        .cloned()
        .unwrap_or(serde_json::json!({}));
    let message = choice
        .get("message")
        .cloned()
        .unwrap_or(serde_json::json!({}));

    let mut content_blocks: Vec<serde_json::Value> = Vec::new();

    // Text content
    if let Some(text) = message.get("content").and_then(|c| c.as_str()) {
        if !text.is_empty() {
            content_blocks.push(serde_json::json!({"type": "text", "text": text}));
        }
    }

    // Tool calls → tool_use blocks
    if let Some(tool_calls) = message.get("tool_calls").and_then(|t| t.as_array()) {
        for tc in tool_calls {
            let tc_id = tc.get("id").and_then(|v| v.as_str()).unwrap_or("");
            let name = tc
                .pointer("/function/name")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            let args_str = tc
                .pointer("/function/arguments")
                .and_then(|v| v.as_str())
                .unwrap_or("{}");
            let input: serde_json::Value =
                serde_json::from_str(args_str).unwrap_or(serde_json::json!({}));
            content_blocks.push(serde_json::json!({
                "type": "tool_use",
                "id": tc_id,
                "name": name,
                "input": input,
            }));
        }
    }

    if content_blocks.is_empty() {
        content_blocks.push(serde_json::json!({"type": "text", "text": ""}));
    }

    let finish_reason = choice
        .get("finish_reason")
        .and_then(|v| v.as_str())
        .unwrap_or("stop");

    let input_tokens = body
        .pointer("/usage/prompt_tokens")
        .and_then(|v| v.as_u64())
        .unwrap_or(0);
    let output_tokens = body
        .pointer("/usage/completion_tokens")
        .and_then(|v| v.as_u64())
        .unwrap_or(0);

    serde_json::json!({
        "id": format!("msg_{}", id),
        "type": "message",
        "role": "assistant",
        "model": model,
        "content": content_blocks,
        "stop_reason": reverse_map_stop_reason(finish_reason),
        "stop_sequence": null,
        "usage": {
            "input_tokens": input_tokens,
            "output_tokens": output_tokens,
        },
    })
}

/// State tracker for OpenAI SSE → Anthropic SSE translation.
pub(crate) struct ReverseStreamContext {
    id: String,
    model: String,
    pub(crate) message_started: bool,
    block_index: i64,
    in_text_block: bool,
    in_tool_use: bool,
    /// Downstream terminator state. `errored`: an Anthropic `event: error`
    /// frame has been sent — translated from an in-band `{"error": {...}}`
    /// data line, or by the stream loop on a transport failure / premature
    /// EOF. `completed`: `message_stop` has been emitted, from a
    /// finish_reason chunk or a bare upstream `[DONE]`. Both are terminal:
    /// the translator emits nothing once either is set (so no `[DONE]` →
    /// `message_stop` after an error, no error frame after `message_stop`).
    pub(crate) terminal: SseTerminal,
}

impl Default for ReverseStreamContext {
    fn default() -> Self {
        Self {
            id: format!("msg_{}", AppState::now_epoch()),
            model: String::new(),
            message_started: false,
            block_index: -1,
            in_text_block: false,
            in_tool_use: false,
            terminal: SseTerminal::default(),
        }
    }
}

/// Format an Anthropic SSE event.
fn make_anthropic_event(event_type: &str, data: &serde_json::Value) -> String {
    format!("event: {}\ndata: {}\n\n", event_type, data)
}

/// Final-frame Anthropic SSE error event for downstream when an upstream
/// stream dies mid-flight. Without this the client sees a bare TCP FIN
/// ("socket closed unexpectedly"). Emits a single `event: error` frame; the
/// Anthropic SSE protocol has no terminator analogous to OpenAI's `[DONE]`,
/// so the channel may close naturally after this frame.
fn anthropic_error_sse(message: &str) -> String {
    // `error.type` must be one of Anthropic's documented values
    // (overloaded_error, api_error, invalid_request_error, ...). Using a
    // custom type like "upstream_error" risks the SDK rejecting the frame
    // and the client falling back to "socket closed unexpectedly" — the
    // exact failure mode this helper exists to prevent. `api_error` is the
    // documented catch-all; the descriptive detail lives in `message`.
    let body = serde_json::json!({
        "type": "error",
        "error": { "type": "api_error", "message": message }
    });
    make_anthropic_event("error", &body)
}

/// `anthropic_error_sse` as Bytes, for the raw stream-loop send paths.
pub(crate) fn anthropic_error_frame(message: &str) -> bytes::Bytes {
    bytes::Bytes::from(anthropic_error_sse(message))
}

/// Final-frame OpenAI SSE error for downstream. Emits the error JSON
/// followed by `data: [DONE]` (OpenAI's stream terminator) — callers MUST
/// NOT emit an additional `[DONE]` after this frame.
pub(crate) fn openai_error_sse(message: &str) -> String {
    let err = serde_json::json!({
        "error": { "message": message, "type": "upstream_error" }
    });
    format!("data: {err}\n\ndata: [DONE]\n\n")
}

/// `openai_error_sse` as Bytes, for the raw stream-loop send paths.
pub(crate) fn openai_error_frame(message: &str) -> bytes::Bytes {
    bytes::Bytes::from(openai_error_sse(message))
}

/// Translate an OpenAI SSE chunk to Anthropic SSE events.
/// Returns Vec because one OpenAI chunk may produce multiple Anthropic events.
/// `raw` is the raw SSE data line (after stripping "data: " prefix).
pub(crate) fn translate_openai_sse_to_anthropic(
    raw: &str,
    ctx: &mut ReverseStreamContext,
) -> Vec<String> {
    // Both terminators are final. After an in-band upstream error the
    // Anthropic error frame is the last frame; drop whatever else the
    // upstream sends (a trailing [DONE] would otherwise emit a message_stop —
    // a success terminator after an error). Symmetrically, after
    // `message_stop` nothing may follow — an in-band error line or stray
    // delta arriving post-completion would violate the protocol the same way.
    if ctx.terminal.reached() {
        return vec![];
    }

    let trimmed = raw.trim();
    if trimmed == "[DONE]" {
        // Only emit message_stop if we started a message. With no message
        // started this returns nothing and leaves `terminal` unset — the
        // stream loop's end-of-stream guard then terminates explicitly.
        if ctx.message_started {
            ctx.terminal.completed = true;
            return vec![make_anthropic_event(
                "message_stop",
                &serde_json::json!({"type": "message_stop"}),
            )];
        }
        return vec![];
    }

    let parsed: serde_json::Value = match serde_json::from_str(trimmed) {
        Ok(v) => v,
        Err(_) => return vec![],
    };

    // In-band upstream error ({"error": {...}} data line). Without this it
    // would fall through the missing-choices early-return and vanish — a
    // not-yet-started message then ends as a 200 with an empty SSE body.
    if let Some(err) = parsed.get("error").filter(|e| !e.is_null()) {
        ctx.terminal.errored = true;
        let err_type = err
            .get("type")
            .and_then(|v| v.as_str())
            .unwrap_or("upstream_error");
        let err_msg = match err.get("message").and_then(|v| v.as_str()) {
            Some(m) => m.to_string(),
            None => err.to_string(),
        };
        warn!(
            error_type = err_type,
            error_message = err_msg,
            "OpenAI upstream emitted in-band error mid-stream"
        );
        return vec![anthropic_error_sse(&format!("{err_type}: {err_msg}"))];
    }

    let mut events: Vec<String> = Vec::new();

    // Capture model from first chunk
    if let Some(model) = parsed.get("model").and_then(|v| v.as_str()) {
        if ctx.model.is_empty() {
            ctx.model = model.to_string();
        }
    }
    if let Some(id) = parsed.get("id").and_then(|v| v.as_str()) {
        if let Some(stripped) = id.strip_prefix("chatcmpl-") {
            ctx.id = format!("msg_{}", stripped);
        }
    }

    let delta = match parsed.pointer("/choices/0/delta") {
        Some(d) => d,
        None => return vec![],
    };
    let finish_reason = parsed
        .pointer("/choices/0/finish_reason")
        .and_then(|v| v.as_str());

    // Emit message_start on first meaningful chunk
    if !ctx.message_started && (delta.get("role").is_some() || delta.get("content").is_some()) {
        ctx.message_started = true;
        events.push(make_anthropic_event(
            "message_start",
            &serde_json::json!({
                "type": "message_start",
                "message": {
                    "id": ctx.id,
                    "type": "message",
                    "role": "assistant",
                    "model": ctx.model,
                    "content": [],
                    "stop_reason": null,
                    "usage": {"input_tokens": 0, "output_tokens": 0}
                }
            }),
        ));
    }

    // Text content delta
    if let Some(text) = delta.get("content").and_then(|c| c.as_str()) {
        if !text.is_empty() {
            if !ctx.in_text_block {
                // Close tool block if open
                if ctx.in_tool_use {
                    events.push(make_anthropic_event(
                        "content_block_stop",
                        &serde_json::json!({"type": "content_block_stop", "index": ctx.block_index}),
                    ));
                    ctx.in_tool_use = false;
                }
                ctx.block_index += 1;
                ctx.in_text_block = true;
                events.push(make_anthropic_event(
                    "content_block_start",
                    &serde_json::json!({
                        "type": "content_block_start",
                        "index": ctx.block_index,
                        "content_block": {"type": "text", "text": ""}
                    }),
                ));
            }
            events.push(make_anthropic_event(
                "content_block_delta",
                &serde_json::json!({
                    "type": "content_block_delta",
                    "index": ctx.block_index,
                    "delta": {"type": "text_delta", "text": text}
                }),
            ));
        }
    }

    // Tool calls
    if let Some(tool_calls) = delta.get("tool_calls").and_then(|t| t.as_array()) {
        for tc in tool_calls {
            let tool_name = tc.pointer("/function/name").and_then(|n| n.as_str());
            let args = tc
                .pointer("/function/arguments")
                .and_then(|a| a.as_str())
                .unwrap_or("");

            if let Some(name) = tool_name {
                // New tool call — close previous block if any
                if ctx.in_text_block {
                    events.push(make_anthropic_event(
                        "content_block_stop",
                        &serde_json::json!({"type": "content_block_stop", "index": ctx.block_index}),
                    ));
                    ctx.in_text_block = false;
                }
                if ctx.in_tool_use {
                    events.push(make_anthropic_event(
                        "content_block_stop",
                        &serde_json::json!({"type": "content_block_stop", "index": ctx.block_index}),
                    ));
                }

                if !ctx.message_started {
                    ctx.message_started = true;
                    events.push(make_anthropic_event(
                        "message_start",
                        &serde_json::json!({
                            "type": "message_start",
                            "message": {
                                "id": ctx.id,
                                "type": "message",
                                "role": "assistant",
                                "model": ctx.model,
                                "content": [],
                                "stop_reason": null,
                                "usage": {"input_tokens": 0, "output_tokens": 0}
                            }
                        }),
                    ));
                }

                ctx.block_index += 1;
                ctx.in_tool_use = true;
                let id = tc.get("id").and_then(|v| v.as_str()).unwrap_or("");
                events.push(make_anthropic_event(
                    "content_block_start",
                    &serde_json::json!({
                        "type": "content_block_start",
                        "index": ctx.block_index,
                        "content_block": {"type": "tool_use", "id": id, "name": name, "input": {}}
                    }),
                ));
            }

            // Tool arguments delta
            if !args.is_empty() && ctx.in_tool_use {
                events.push(make_anthropic_event(
                    "content_block_delta",
                    &serde_json::json!({
                        "type": "content_block_delta",
                        "index": ctx.block_index,
                        "delta": {"type": "input_json_delta", "partial_json": args}
                    }),
                ));
            }
        }
    }

    // Finish reason → close blocks + message_delta + message_stop
    if let Some(reason) = finish_reason {
        if ctx.in_text_block || ctx.in_tool_use {
            events.push(make_anthropic_event(
                "content_block_stop",
                &serde_json::json!({"type": "content_block_stop", "index": ctx.block_index}),
            ));
            ctx.in_text_block = false;
            ctx.in_tool_use = false;
        }
        events.push(make_anthropic_event(
            "message_delta",
            &serde_json::json!({
                "type": "message_delta",
                "delta": {"stop_reason": reverse_map_stop_reason(reason)},
                "usage": {"output_tokens": 0}
            }),
        ));
        events.push(make_anthropic_event(
            "message_stop",
            &serde_json::json!({"type": "message_stop"}),
        ));
        // terminal's early-return keeps a trailing [DONE] (or anything else)
        // from emitting a duplicate message_stop.
        ctx.terminal.completed = true;
    }

    events
}

/// Forward one OpenAI-compat request to a single Anthropic-protocol `Endpoint`.
/// The caller has already translated the OpenAI request and serialized it once
/// into `anthropic_body_bytes` (plus the OAuth variant); this helper picks the
/// right variant by token prefix, forwards to the endpoint, and translates the
/// Anthropic response back to OpenAI format. Structurally similar to
/// `forward_anthropic`, but intentionally kept separate: it speaks OpenAI on
/// both edges (request body already translated, response translated back).
/// `ep` is the endpoint to forward to; `endpoint_idx` is its index in
/// `state.endpoints`. Both are required: the streaming path spawns a
/// detached 'static task that must re-borrow the endpoint from a cloned
/// Arc<AppState> — a borrowed &Endpoint cannot cross the spawn boundary,
/// so the task captures the Copy `endpoint_idx` and re-indexes.
#[allow(clippy::too_many_arguments)]
async fn forward_openai_compat_anthropic(
    state: &Arc<AppState>,
    parts: &axum::http::request::Parts,
    ep: &Endpoint,
    endpoint_idx: usize,
    // Translated Anthropic-shape bodies, serialized once by the caller
    // (LAB-716); each attempt clones the refcounted `Bytes`.
    anthropic_body_bytes: &bytes::Bytes,
    oauth_anthropic_body_bytes: &bytes::Bytes,
    req_id: &str,
    client_id: &str,
    client_ver: &str,
    client_ip: &std::net::IpAddr,
    agent_id: &str,
    session_id: &str,
    model: &str,
    session_key: Option<&str>,
    is_streaming: bool,
    json_mode: bool,
    request_start: std::time::Instant,
) -> ForwardOutcome {
    let token = ep.token.as_str();
    let passthrough = ep.passthrough;
    let endpoint_name = ep.name.as_str();
    let rate_info = &ep.rate_info;
    let url = format!("{}/v1/messages", ep.base_url);

    let mut headers = parts.headers.clone();
    headers.remove("host");
    if !passthrough {
        headers.remove("authorization");
        headers.remove("x-api-key");
    }
    headers.remove("content-length"); // body size changes after translation
    headers.remove("accept-encoding"); // we need plaintext to translate the response
    if !state.forward_caller_identity {
        strip_client_identity_headers(&mut headers);
    }

    // Inject required Anthropic headers
    headers.insert("content-type", HeaderValue::from_static("application/json"));
    headers.insert("anthropic-version", HeaderValue::from_static("2023-06-01"));

    // Auth injection
    let dropped = inject_account_auth(
        &mut headers,
        token,
        passthrough,
        &state.allowed_client_betas,
    );
    state.record_dropped_beta_flags(client_id, &dropped);
    // No `strip_orphaned_beta_body_fields` here (LAB-1261). That strip exists
    // because a CLIENT can put a beta-paired field in the body; this body is
    // LB-generated by the OpenAI→Anthropic translator, which emits base-schema
    // fields only. Running the strip over our own output would risk deleting a
    // field the translator legitimately added, to fix a pairing that cannot
    // occur on this path.

    // Session registry window (LAB-916). OpenAI-compat callers can't send the
    // `context-1m` beta through translation, but check anyway — the header is
    // forwarded when present. Read from the FILTERED outbound headers so the
    // accounting matches what the upstream actually ran under (PR #116
    // review), same as forward_anthropic.
    let context_window = context_window_for(model, request_has_1m_beta(&headers));

    // Use OAuth variant (with CC system prompt) for OAuth tokens
    let req_body = if token.starts_with(OAUTH_TOKEN_PREFIX) {
        oauth_anthropic_body_bytes
    } else {
        anthropic_body_bytes
    };
    debug!(
        account = endpoint_name,
        model = %model,
        body_len = req_body.len(),
        "openai-compat: upstream request"
    );

    // Same non-streaming read_timeout exemption as forward_anthropic (LAB-718).
    let http_client = if is_streaming {
        &state.client
    } else {
        &state.client_nonstreaming
    };
    let upstream_req = http_client
        .request(reqwest::Method::POST, &url)
        .headers(headers)
        .body(req_body.clone());

    let resp = match upstream_req.send().await {
        Ok(r) => r,
        Err(e) => {
            error!(account = endpoint_name, detail = %describe_reqwest_error(&e), "upstream request failed: {e}");
            // Surface the failure on the dashboard by kind before it becomes a
            // client error. `is_timeout`/`is_connect` are the same classifiers
            // `describe_reqwest_error` uses for the log line above.
            let kind = if e.is_timeout() {
                "timeout"
            } else if e.is_connect() {
                "connect"
            } else {
                "other"
            };
            *state.lock_transport_errors().entry(kind).or_insert(0) += 1;
            // Feed the per-endpoint circuit breaker: enough consecutive
            // failures and this endpoint leaves the routing pool entirely.
            state.record_transport_failure(endpoint_idx).await;
            // Transport-level send failure (ETIMEDOUT/reset/closed/DNS). Mark it
            // `transient`; rotation policy is round-gated and owned by the retry
            // loop (it knows `retry_round`), so `push_skip` stays false here —
            // round 0 retries the SAME affinity/cache-warm endpoint after a
            // backoff rather than rotating to a cold-cache endpoint on every blip.
            return ForwardOutcome::Retry {
                saw_529: false,
                push_skip: false,
                transient: true,
            };
        }
    };

    let status = resp.status();
    ep.requests.fetch_add(1, Ordering::Relaxed);
    // Any HTTP response (even 429/5xx) proves the transport path is alive —
    // clear the circuit-breaker counter.
    state.record_transport_success(endpoint_idx).await;
    state
        .update_rate_info_for(
            rate_info,
            endpoint_name,
            resp.headers(),
            // The OpenAI request shape cannot express `speed`, so a response
            // here always answers a standard-speed request (LAB-2693).
            /* is_fast_mode */
            false,
        )
        .await;

    // Update burn rate (after rate-limit headers are parsed)
    state.update_burn_rate(&ep.burn_rate, client_id);

    // Classify 429 / 529 / other 5xx into a retry decision (shared helper).
    let mut resp = match classify_retry_status(
        state,
        status,
        rate_info,
        endpoint_name,
        resp,
        /* openai_error_shape */ true,
        // The OpenAI request shape cannot express `speed` — never fast.
        None,
    )
    .await
    {
        Ok(resp) => resp,
        Err(outcome) => return outcome,
    };

    // Clear hard limit and burst counter only on a genuine 2xx success.
    // A 4xx (e.g. invalid_request_error, auth failure) is not evidence
    // that the rate-limit window has drained — don't clobber state on
    // client errors.
    let recovered = if status.is_success() {
        let mut info = rate_info.write().await;
        let was = info.hard_limited_until.is_some();
        info.hard_limited_until = None;
        info.consecutive_burst_429s = 0;
        was
    } else {
        false
    };

    // Per-request persistence removed: it re-serialized the whole endpoint pool
    // and did a blocking write on every successful request (a memory + IO
    // amplifier under load). Persist only on the hard-limit RECOVERY transition
    // here; 429 hard-limit entry still persists immediately, and utilization /
    // request counts persist at probe cadence + shutdown.
    if recovered {
        state.save_state().await;
        state.signal_hard_limit_recovery(endpoint_name).await;
    }

    // Capture the routing/utilization snapshot + inject budget status header.
    // The `proxied (openai-compat)` line is deferred to `finalize_stream`/
    // `finalize_non_stream`, which merge it with token usage once known
    // (LAB-3214: one INFO line per request, not two).
    // Named `proxied_ctx` (not `ctx`) — this function's streaming branch
    // already has a local `ctx: StreamContext` for SSE translation.
    let (budget_status, proxied_ctx) = {
        let info = rate_info.read().await;
        let (eff_util, constraint, _adj_5h, _adj_7d) =
            effective_utilization(&info, AppState::now_epoch(), model);
        let proxied_ctx = ProxiedCtx::OpenaiCompat {
            client_ver: client_ver.to_owned(),
            utilization: format!("{eff_util:.2}"),
            util_5h: info
                .utilization_5h
                .map(|v| format!("{v:.2}"))
                .unwrap_or_else(|| "-".to_string()),
            util_7d: info
                .utilization_7d
                .map(|v| format!("{v:.2}"))
                .unwrap_or_else(|| "-".to_string()),
            constraint,
            pin: state.pin_status(client_id, endpoint_idx),
            stream: is_streaming,
        };
        (
            compute_pressure_status(eff_util, client_id, state),
            proxied_ctx,
        )
    };

    // Non-2xx: log error detail, translate to OpenAI error format, return
    if !status.is_success() {
        let error_body = resp.bytes().await.unwrap_or_else(|e| {
            warn!(req_id, account = endpoint_name, error = %e, "openai-compat: failed to read upstream error body");
            bytes::Bytes::new()
        });
        let error_msg = serde_json::from_slice::<serde_json::Value>(&error_body)
            .ok()
            .and_then(|v| {
                v.pointer("/error/message")
                    .and_then(|m| m.as_str())
                    .map(String::from)
            });
        warn!(
            account = endpoint_name,
            model = %model,
            status = status.as_u16(),
            error_message = ?error_msg,
            "openai-compat: upstream error"
        );
        // This branch returns before `finalize_non_stream` — log the merged
        // line here too (zero usage), so an upstream error still gets the
        // routing/utilization snapshot at INFO, same as the old unconditional
        // `proxied (openai-compat)` line did (AC4).
        log_proxied(
            req_id,
            client_id,
            model,
            endpoint_name,
            &client_ip.to_string(),
            agent_id,
            session_id,
            status.as_u16(),
            &proxied_ctx,
            &TokenUsage::default(),
        );

        // Translate Anthropic error to OpenAI error format so clients
        // (LiteLLM, etc.) can parse the actual error message.
        let mut model_unsupported = false;
        let mut entitlement = false;
        let openai_error =
            if let Ok(parsed) = serde_json::from_slice::<serde_json::Value>(&error_body) {
                // Count + trace context-window overflows here too (LAB-916) —
                // this path consumes the same Anthropic windows as the native one.
                if status.as_u16() == 400 {
                    if let Some(msg) = prompt_too_long_message(&parsed) {
                        state.note_prompt_too_long(req_id, model, session_key, msg);
                    }
                }
                // Same model-rejection detection as the native path (LAB-941),
                // and the same entitlement 400 (LAB-4729).
                model_unsupported = is_model_unsupported_error(status, &parsed, ep.protocol);
                entitlement = is_entitlement_exhausted_400(status, &parsed);
                // Anthropic: {"type":"error","error":{"type":"...","message":"..."}}
                let msg = parsed
                    .pointer("/error/message")
                    .and_then(|v| v.as_str())
                    .unwrap_or("unknown upstream error");
                let err_type = parsed
                    .pointer("/error/type")
                    .and_then(|v| v.as_str())
                    .unwrap_or("api_error");
                serde_json::json!({
                    "error": {
                        "message": msg,
                        "type": err_type,
                        "param": null,
                        "code": null
                    }
                })
            } else {
                let raw = String::from_utf8_lossy(&error_body);
                serde_json::json!({
                    "error": {
                        "message": raw.as_ref(),
                        "type": "api_error",
                        "param": null,
                        "code": null
                    }
                })
            };

        let response = Response::builder()
            .status(StatusCode::from_u16(status.as_u16()).unwrap_or(StatusCode::BAD_GATEWAY))
            .header("content-type", "application/json")
            .header("x-budget-status", budget_status)
            .body(Body::from(
                serde_json::to_vec(&openai_error).unwrap_or_default(),
            ))
            .unwrap_or_else(|_| {
                (StatusCode::INTERNAL_SERVER_ERROR, "response build error").into_response()
            });
        if model_unsupported {
            state.note_model_unsupported(endpoint_name, endpoint_idx, model);
            return ForwardOutcome::RetryModelUnsupported(Box::new(response));
        }
        if entitlement {
            state.note_entitlement_400(endpoint_name);
            return ForwardOutcome::RetryEntitlement(Box::new(response));
        }
        return ForwardOutcome::Done(Box::new(response));
    }

    if is_streaming {
        let (tx, rx) = tokio::sync::mpsc::channel::<Result<bytes::Bytes, std::io::Error>>(32);
        let state_clone = state.clone();
        // The detached task can't carry the `ep` borrow across the spawn
        // boundary; capture the Copy index and re-borrow from the owned `Arc`.
        let client_id_clone = client_id.to_owned();
        let acct_name = endpoint_name.to_owned();
        let model_clone = model.to_owned();
        let client_ip_str = client_ip.to_string();
        let agent_clone = agent_id.to_owned();
        let session_clone = session_id.to_owned();
        let req_id_clone = req_id.to_owned();
        let session_key_clone = session_key.map(str::to_owned);
        let status_code = status.as_u16();

        tokio::spawn(async move {
            let mut buffer: Vec<u8> = Vec::new();
            let mut scanner = SseUsageScanner::default();
            // Terminator state: see `StreamContext::terminal`.
            let mut ctx = StreamContext {
                json_mode,
                ..StreamContext::default()
            };

            let mut client_gone = false;

            loop {
                match resp.chunk().await {
                    Ok(Some(chunk)) => {
                        scanner.push(&chunk);
                        buffer.extend_from_slice(&chunk);

                        while let Some(pos) = buffer.windows(2).position(|w| w == b"\n\n") {
                            let event = String::from_utf8_lossy(&buffer[..pos]).into_owned();
                            buffer.drain(..pos + 2);

                            if event.trim().is_empty() {
                                continue;
                            }

                            if let Some(translated) = translate_sse_event(&event, &mut ctx) {
                                // ends_with, not equality: the json_mode flush
                                // and the in-band error frame both append the
                                // terminator to another frame in one string.
                                // The in-band error frame carries its own
                                // [DONE]; `errored` (set by the translator)
                                // already makes it terminal — it is not a
                                // success completion.
                                if translated.ends_with("data: [DONE]\n\n") && !ctx.terminal.errored
                                {
                                    ctx.terminal.completed = true;
                                }
                                if tx.send(Ok(bytes::Bytes::from(translated))).await.is_err() {
                                    client_gone = true;
                                    break;
                                }
                                if ctx.terminal.errored {
                                    // Nothing may follow the error frame.
                                    break;
                                }
                            }
                        }
                        if client_gone || ctx.terminal.errored {
                            break;
                        }
                    }
                    Ok(None) => break,
                    Err(e) => {
                        warn!(req_id = req_id_clone, error = %e, "upstream SSE read failed");
                        // `[DONE]` already went out: the client saw a complete
                        // stream, and a second `[DONE]` (the error frame carries
                        // one) would break strict OpenAI parsers.
                        if ctx.terminal.reached() {
                            debug!(
                                req_id = req_id_clone,
                                "transport error after terminator — error frame suppressed"
                            );
                            break;
                        }
                        // The post-loop "ensure DONE sent" block gates on
                        // !terminal.reached(), so emitting the error frame
                        // here — which already ships [DONE] — cannot race
                        // with a second [DONE] from the post-loop guard.
                        ctx.terminal.errored = true;
                        if tx
                            .send(Ok(openai_error_frame(&format!(
                                "upstream stream interrupted: {e}"
                            ))))
                            .await
                            .is_err()
                        {
                            client_gone = true;
                        }
                        break;
                    }
                }
            }

            // Process any remaining data in buffer (skip once a terminator is
            // out — nothing may follow it)
            if !ctx.terminal.reached() && !buffer.is_empty() {
                let remaining = String::from_utf8_lossy(&buffer).into_owned();
                if !remaining.trim().is_empty() {
                    if let Some(translated) = translate_sse_event(&remaining, &mut ctx) {
                        if translated.ends_with("data: [DONE]\n\n") && !ctx.terminal.errored {
                            ctx.terminal.completed = true;
                        }
                        if tx.send(Ok(bytes::Bytes::from(translated))).await.is_err() {
                            client_gone = true;
                        }
                    }
                }
            }

            // Ensure [DONE] is always sent (skip once any terminator is out —
            // after an error frame it would fake a clean completion)
            if !ctx.terminal.reached()
                && !client_gone
                && tx
                    .send(Ok(bytes::Bytes::from("data: [DONE]\n\n")))
                    .await
                    .is_err()
            {
                client_gone = true;
            }

            // Record usage scanned incrementally from the upstream SSE
            // (LAB-717). The detached task only holds a cloned
            // Arc<AppState>; re-index it.
            let ep = &state_clone.endpoints[endpoint_idx];
            finalize_stream(
                &state_clone,
                ep,
                &req_id_clone,
                &client_id_clone,
                &model_clone,
                &acct_name,
                &client_ip_str,
                &agent_clone,
                &session_clone,
                status_code,
                proxied_ctx,
                scanner,
                request_start,
                client_gone,
                ctx.terminal.errored,
                true,
                session_key_clone.as_deref(),
                context_window,
            )
            .await;
        });

        let response = Response::builder()
            .status(StatusCode::OK)
            .header("content-type", "text/event-stream")
            .header("cache-control", "no-cache")
            .header("connection", "keep-alive")
            .header("x-budget-status", budget_status)
            .body(Body::from_stream(ReceiverStream::new(rx)))
            .unwrap_or_else(|_| {
                (StatusCode::INTERNAL_SERVER_ERROR, "response build error").into_response()
            });
        return ForwardOutcome::Done(Box::new(response));
    }

    // Non-streaming: buffer, translate, return
    let resp_bytes = match resp.bytes().await {
        Ok(b) => b,
        Err(e) => {
            error!("failed to read upstream response: {e}");
            // This branch returns before `finalize_non_stream` — log the
            // merged line here too (no usage: the body never arrived), so
            // the routing/utilization snapshot still lands at INFO (AC4).
            log_proxied(
                req_id,
                client_id,
                model,
                endpoint_name,
                &client_ip.to_string(),
                agent_id,
                session_id,
                status.as_u16(),
                &proxied_ctx,
                &TokenUsage::default(),
            );
            return ForwardOutcome::Done(Box::new(
                (StatusCode::BAD_GATEWAY, "failed to read upstream response").into_response(),
            ));
        }
    };

    let anthropic_resp: serde_json::Value = match serde_json::from_slice(&resp_bytes) {
        Ok(v) => v,
        Err(e) => {
            error!(req_id, account = endpoint_name, error = %e, "openai-compat: upstream 2xx body is not valid JSON");
            // Same as above: malformed upstream body means we never reach
            // `finalize_non_stream`, so log the routing snapshot here.
            log_proxied(
                req_id,
                client_id,
                model,
                endpoint_name,
                &client_ip.to_string(),
                agent_id,
                session_id,
                status.as_u16(),
                &proxied_ctx,
                &TokenUsage::default(),
            );
            // A 2xx we can't translate is not a success: passing the raw
            // bytes through would hand the client an untranslated payload
            // under 200. Same OpenAI error shape as the non-2xx branch.
            let openai_error = serde_json::json!({
                "error": {
                    "message": "invalid upstream response",
                    "type": "api_error",
                    "param": null,
                    "code": null
                }
            });
            let response = Response::builder()
                .status(StatusCode::BAD_GATEWAY)
                .header("content-type", "application/json")
                .header("x-budget-status", budget_status)
                .body(Body::from(
                    serde_json::to_vec(&openai_error).unwrap_or_default(),
                ))
                .unwrap_or_else(|_| {
                    (StatusCode::INTERNAL_SERVER_ERROR, "response build error").into_response()
                });
            return ForwardOutcome::Done(Box::new(response));
        }
    };

    let openai_resp = translate_anthropic_to_openai(&anthropic_resp, json_mode);

    // Extract and record token usage from non-streaming response
    let usage = TokenUsage::from_response_body(&anthropic_resp);
    finalize_non_stream(
        state,
        ep,
        req_id,
        client_id,
        model,
        anthropic_resp.get("model").and_then(|v| v.as_str()),
        endpoint_name,
        &client_ip.to_string(),
        agent_id,
        session_id,
        status.as_u16(),
        &usage,
        request_start.elapsed().as_millis() as u64,
        true,
        session_key,
        context_window,
        Some(proxied_ctx),
    )
    .await;

    let response = Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "application/json")
        .header("x-budget-status", budget_status)
        .body(Body::from(
            serde_json::to_vec(&openai_resp).unwrap_or_default(),
        ))
        .unwrap_or_else(|_| {
            (StatusCode::INTERNAL_SERVER_ERROR, "response build error").into_response()
        });
    ForwardOutcome::Done(Box::new(response))
}

pub(crate) async fn openai_chat_handler(
    State(state): State<Arc<AppState>>,
    axum::extract::ConnectInfo(client_addr): axum::extract::ConnectInfo<SocketAddr>,
    req: Request<Body>,
) -> Response {
    // AC-8: the ONLY client_addr.ip() read in this handler.
    let client_ip = state.resolve_client_ip(client_addr.ip(), req.headers());
    let request_start = Instant::now();

    // IP allowlist check
    if !state.is_ip_allowed(&client_ip) {
        warn!(client = %client_ip, "rejected: IP not in allowlist");
        return (StatusCode::FORBIDDEN, "forbidden").into_response();
    }

    // Proxy auth: accept the credential from either x-api-key or
    // Authorization: Bearer — OpenAI SDKs send only the latter.
    let principal = match state.authenticate_throttled(
        &client_ip,
        client_addr,
        req.headers(),
        true,
        "openai",
    ) {
        Ok(p) => p,
        Err(resp) => return *resp,
    };

    let (parts, body) = req.into_parts();

    let req_id = format!(
        "{:04x}:{}",
        state.instance_id,
        state.next_req_id.fetch_add(1, Ordering::Relaxed)
    );

    // Extract client identification headers
    let rctx = RequestContext::from_request(&state, &client_ip, &parts.headers, principal);
    let affinity_key = rctx.affinity_key(&client_ip, None);
    let affinity = affinity_key.as_deref();
    let RequestContext {
        client_id,
        client_ver,
        agent_id,
        session_id,
    } = rctx;

    // LAB-4395 / GH #199: same identity-only refusal as `proxy_handler`, and
    // for the same reason — ahead of the reservation below.
    if let Some(resp) = state.deny_admin_reader(&req_id, &client_id) {
        return *resp;
    }

    // Admission control (P1-01): same body-memory backstop as proxy_handler.
    let _body_reservation = match reserve_request_body(&state, &parts, &req_id, client_ip) {
        Ok(g) => g,
        Err(resp) => return *resp,
    };

    let mut body_bytes = match read_body_bounded(&state, body, &req_id).await {
        Ok(b) => b,
        Err(resp) => return *resp,
    };

    let mut openai_body: serde_json::Value = match serde_json::from_slice(&body_bytes) {
        Ok(v) => v,
        Err(e) => {
            error!("failed to parse request JSON: {e}");
            return (StatusCode::BAD_REQUEST, "invalid JSON").into_response();
        }
    };

    let is_streaming = openai_body
        .get("stream")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let json_mode = wants_json_object(&openai_body);
    let model = openai_body
        .get("model")
        .and_then(|m| m.as_str())
        .unwrap_or("")
        .to_string();

    // LAB-798: `Protocol::OpenAI` endpoints below forward `body_bytes`
    // verbatim, so a hard-rejected `temperature` must be stripped here,
    // before either wire body is built — the translated Anthropic arm then
    // never sees it either, keeping the warn to once per request. Policy in
    // `drops_deprecated_temperature`.
    if openai_body
        .get("temperature")
        .is_some_and(|v| drops_deprecated_temperature(&model, v))
    {
        if let Some(obj) = openai_body.as_object_mut() {
            obj.remove("temperature");
        }
        match serde_json::to_vec(&openai_body) {
            Ok(b) => body_bytes = bytes::Bytes::from(b),
            // Can't happen for a Value parsed from JSON (string keys only),
            // but forwarding the original bytes would silently resend the
            // rejected param — fail loudly instead, like the serialize arm
            // below.
            Err(e) => {
                error!(req_id, error = %e, "failed to re-serialize request body after temperature strip");
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "body serialization failed",
                )
                    .into_response();
            }
        }
    }

    // Pre-request gate: operator bypass, budget, utilization limit, emergency brake.
    // Note: budget + emergency don't need `model` and could run before body parsing,
    // but those rejections are rare and the JSON parse cost is negligible — not worth
    // splitting the gate for a few microseconds on an almost-never code path.
    if let Err(resp) = state.pre_request_gate(&req_id, &client_id, &model).await {
        return *resp;
    }

    let mut anthropic_body = translate_openai_to_anthropic(&openai_body);

    // LAB-3877: Tier 0 content guard on the OpenAI-compat surface — the same
    // hook as `proxy_handler`. The scanner reads the TRANSLATED body (the
    // Messages shape it understands); the Anthropic arm below forwards that
    // body, the OpenAI arm forwards the original bytes. Unparseable JSON was
    // already rejected above.
    //
    // LAB-4358: `from_openai_body` owns the whole discrimination — it judges
    // readability on the body the client sent (the bytes a `Protocol::OpenAI`
    // upstream receives) and extracts text from the translated one. The
    // hand-maintained role enumeration this used to carry is gone: an OpenAI
    // role translation cannot map is passed through verbatim into the translated
    // `messages`, where `from_body`'s role rule now rejects it on both surfaces
    // at once. Fields translation DROPS — `messages[].name`, the top-level
    // `user` — remain unscanned on both surfaces; that is a coverage question,
    // not a readability one, and is not addressed here.
    #[cfg(feature = "guard")]
    let guard_annotate: Option<usize> = {
        let outcome = guard::ScanInput::from_openai_body(&openai_body, &anthropic_body);
        match state.guard_hook(&req_id, &client_id, &outcome, true) {
            Ok(annotate) => annotate,
            Err(resp) => return *resp,
        }
    };

    if state.auto_cache {
        let inj = inject_cache_breakpoints(&mut anthropic_body);
        if inj.skipped {
            debug!("auto-cache: skipped, existing cache_control found");
        } else if inj.tools || inj.system || inj.messages {
            debug!(
                tools = inj.tools,
                system = inj.system,
                messages = inj.messages,
                "auto-cache: injected breakpoints"
            );
        }
    }

    // Pre-compute OAuth variant with Claude Code system prompt.
    // OAuth tokens (sk-ant-oat*) require this to access sonnet/opus models.
    let mut oauth_anthropic_body = anthropic_body.clone();
    inject_oauth_system_prompt(&mut oauth_anthropic_body);

    // Serialize both variants once (LAB-716): the forward helper used to
    // `to_string()` the JSON on every retry attempt; now each attempt clones
    // a refcounted `Bytes`. Serializing a `Value` can only fail on a
    // non-string map key, which JSON input can't produce — the 500 arm is a
    // formality.
    let (anthropic_body_bytes, oauth_body_bytes) = match (
        serde_json::to_vec(&anthropic_body),
        serde_json::to_vec(&oauth_anthropic_body),
    ) {
        (Ok(a), Ok(o)) => (bytes::Bytes::from(a), bytes::Bytes::from(o)),
        _ => {
            error!(req_id, "failed to serialize translated request body");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                "body serialization failed",
            )
                .into_response();
        }
    };

    // Wrapped as in `proxy_handler`: every `return` inside yields the block's
    // `Response`, so `X-Guard-Findings` is stamped once below.
    let response: Response = async {
        let n = state.endpoints.len();
        let mut last_saw_529 = false;
        let mut last_saw_transient = false;
        // Upstream error from the most recent model-unsupported rejection —
        // returned verbatim if the pool exhausts on nothing but rejections.
        let mut model_unsupported_resp: Option<Response> = None;
        // One-shot entitlement re-send, as in `proxy_handler` (LAB-4729).
        let mut entitlement_resp: Option<(EndpointIdx, Option<Response>)> = None;
        for retry_round in 0..=MAX_529_RETRIES {
            if retry_round > 0 {
                let delay = round_backoff_delay(retry_round, last_saw_529);
                warn!(
                    retry_round = retry_round,
                    delay_ms = delay.as_millis() as u64,
                    saw_529 = last_saw_529,
                    "backoff: retrying all endpoints after transient/overload round"
                );
                tokio::time::sleep(delay).await;
            }
            // Entitlement-refusing endpoint stays skipped across rounds, as in
            // `proxy_handler` (LAB-4729).
            let mut skip: Vec<EndpointIdx> = entitlement_resp.iter().map(|(i, _)| *i).collect();
            let mut saw_529 = false;
            let mut saw_transient = false;
            for _attempt in 0..n {
                // Pick the next endpoint and dispatch by protocol. Both forwards
                // return a `ForwardOutcome` so the shared round-gated policy in
                // `apply_round_outcome` covers both.
                let (outcome, picked_idx): (ForwardOutcome, EndpointIdx) = match state
                    .pick_endpoint_for_client(affinity, &model, &skip, &client_id)
                    .await
                {
                    Some(i) => {
                        let ep = &state.endpoints[i];
                        match ep.protocol {
                            Protocol::Anthropic => {
                                let out = forward_openai_compat_anthropic(
                                    &state,
                                    &parts,
                                    ep,
                                    i,
                                    &anthropic_body_bytes,
                                    &oauth_body_bytes,
                                    &req_id,
                                    &client_id,
                                    &client_ver,
                                    &client_ip,
                                    &agent_id,
                                    &session_id,
                                    &model,
                                    affinity,
                                    is_streaming,
                                    json_mode,
                                    request_start,
                                )
                                .await;
                                (out, i)
                            }
                            Protocol::OpenAI => {
                                // The endpoint is OpenAI-native — forward the
                                // original request body without translation.
                                let out = try_fallback_upstream(
                                    &state,
                                    &body_bytes,
                                    &req_id,
                                    &client_id,
                                    &client_ip,
                                    &agent_id,
                                    &session_id,
                                    &model,
                                    i,
                                    request_start,
                                    false,
                                    is_streaming,
                                )
                                .await;
                                (out, i)
                            }
                        }
                    }
                    // Candidates exhausted mid-round (all skipped / hard-limited /
                    // model-filtered). Break to the round-end logic rather than
                    // returning here, so a transient-only round still reaches the
                    // transient-aware exhaustion status instead of short-circuiting
                    // to a premature 429.
                    None => break,
                };

                match apply_round_outcome(
                    retry_round,
                    outcome,
                    picked_idx,
                    &mut skip,
                    &mut saw_529,
                    &mut saw_transient,
                    &mut model_unsupported_resp,
                    &mut entitlement_resp,
                ) {
                    RetryStep::Return(resp) => return resp,
                    RetryStep::NextAttempt => continue,
                    RetryStep::EndRound => break,
                }
            }
            last_saw_529 = saw_529;
            last_saw_transient = saw_transient;
            if !round_should_continue(retry_round, saw_529, saw_transient) {
                break;
            }
        }

        // Same model-rejection exhaustion rule as `proxy_handler` (LAB-941),
        // in the OpenAI error shape this handler's clients parse.
        if !last_saw_529 && !last_saw_transient {
            if let Some(resp) = entitlement_resp
                .and_then(|(_, r)| r)
                .or(model_unsupported_resp)
            {
                return resp;
            }
            if state.model_unsupported_everywhere(&model) {
                warn!(model, "model unsupported on all eligible endpoints");
                return model_unsupported_response(&model, true);
            }
        }
        exhaustion_response(&state, last_saw_transient, last_saw_529)
    }
    .await;

    #[cfg(feature = "guard")]
    let response = stamp_guard_findings(response, guard_annotate);

    response
}

#[cfg(test)]
mod tests;
