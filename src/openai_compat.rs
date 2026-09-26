use crate::*;

// ── OpenAI compatibility ─────────────────────────────────────────────

/// True when an OpenAI-format request asked for `response_format: json_object`.
/// Single predicate shared by the request-side system nudge and the
/// response-side fence strip so the two can never disagree on what JSON mode is.
pub(crate) fn wants_json_object(body: &serde_json::Value) -> bool {
    body.get("response_format")
        .and_then(|rf| rf.get("type"))
        .and_then(|t| t.as_str())
        == Some("json_object")
}

/// Strip markdown JSON fences from LLM output.
/// Claude sometimes wraps JSON in ```json ... ``` even when told not to.
/// Clients using response_format: json_object (e.g. Vercel AI SDK's generateObject)
/// need raw JSON or their parse step blows up.
/// Only applied when the request asked for JSON mode — a normal chat reply
/// that legitimately is a fenced code block must pass through verbatim.
fn strip_json_fences(s: &str) -> String {
    let trimmed = s.trim();
    if let Some(rest) = trimmed.strip_prefix("```") {
        // Skip language tag on first line (e.g. "json\n")
        let after_tag = match rest.find('\n') {
            Some(pos) => &rest[pos + 1..],
            None => return s.to_string(),
        };
        // Strip closing fence
        if let Some(content) = after_tag.strip_suffix("```") {
            return content.trim().to_string();
        }
    }
    s.to_string()
}

fn map_stop_reason(reason: &str) -> &'static str {
    match reason {
        "end_turn" => "stop",
        "max_tokens" => "length",
        "stop_sequence" => "stop",
        "tool_use" => "tool_calls",
        _ => "stop",
    }
}

pub(crate) struct StreamContext {
    pub(crate) id: String,
    pub(crate) model: String,
    pub(crate) created: u64,
    pub(crate) tool_call_index: i64,
    pub(crate) in_tool_use: bool,
    pub(crate) current_tool_id: String,
    /// Request asked for `response_format: json_object`.
    pub(crate) json_mode: bool,
    /// Text content buffered while `json_mode` is set, flushed fence-stripped
    /// at end-of-message so streaming content matches the non-streaming strip.
    pub(crate) text_buffer: String,
    /// Downstream terminator state. `errored`: the translator surfaced an
    /// in-band `event: error` as an OpenAI error frame (which carries its
    /// own `[DONE]`), or the stream loop synthesised one on a transport
    /// failure; the loop must stop translating and must NOT emit a clean
    /// `[DONE]` afterwards — that would fake a successful completion after
    /// a truncation. `completed`: the clean `[DONE]` went downstream (set by
    /// the loop, which sees every emitted frame; the error frame's own
    /// `[DONE]` does not count).
    pub(crate) terminal: SseTerminal,
}

impl Default for StreamContext {
    fn default() -> Self {
        Self {
            id: format!("chatcmpl-{}", AppState::now_epoch()),
            model: String::new(),
            created: AppState::now_epoch(),
            tool_call_index: -1,
            in_tool_use: false,
            current_tool_id: String::new(),
            json_mode: false,
            text_buffer: String::new(),
            terminal: SseTerminal::default(),
        }
    }
}

impl StreamContext {
    /// In JSON mode, drain the buffered text and strip fences over the whole
    /// message — the streaming equivalent of the non-streaming strip, applied
    /// once so fences split across deltas are handled. Returns None when not in
    /// JSON mode, the buffer is empty, or the strip yields nothing.
    fn take_stripped_json_content(&mut self) -> Option<String> {
        if !self.json_mode || self.text_buffer.is_empty() {
            return None;
        }
        let stripped = strip_json_fences(&self.text_buffer);
        self.text_buffer.clear();
        (!stripped.is_empty()).then_some(stripped)
    }
}

fn make_openai_chunk(
    ctx: &StreamContext,
    delta: serde_json::Value,
    finish_reason: Option<&str>,
) -> String {
    let chunk = serde_json::json!({
        "id": ctx.id,
        "object": "chat.completion.chunk",
        "created": ctx.created,
        "model": ctx.model,
        "choices": [{
            "index": 0,
            "delta": delta,
            "finish_reason": finish_reason,
        }],
    });
    format!("data: {}\n\n", chunk)
}

/// OpenAI `image_url` part → Anthropic `image` block. `data:` URLs unpack into a
/// base64 source; anything else (including malformed data URLs) becomes a url
/// source so upstream rejects it with a real error instead of us dropping it.
fn openai_image_part_to_anthropic(url: &str) -> serde_json::Value {
    if let Some(rest) = url.strip_prefix("data:") {
        if let Some((media_type, data)) = rest.split_once(";base64,") {
            return serde_json::json!({
                "type": "image",
                "source": {"type": "base64", "media_type": media_type, "data": data},
            });
        }
    }
    serde_json::json!({
        "type": "image",
        "source": {"type": "url", "url": url},
    })
}

pub(crate) fn translate_openai_to_anthropic(body: &serde_json::Value) -> serde_json::Value {
    let mut out = serde_json::Map::new();

    // Model
    if let Some(model) = body.get("model") {
        out.insert("model".to_string(), model.clone());
    }

    // Extract system messages, pass through the rest
    let mut system_parts: Vec<String> = Vec::new();
    let mut messages: Vec<serde_json::Value> = Vec::new();

    if let Some(msgs) = body.get("messages").and_then(|m| m.as_array()) {
        for msg in msgs {
            let role = msg.get("role").and_then(|r| r.as_str()).unwrap_or("");
            if role == "system" {
                // content can be a string or an array of text parts
                let content_val = msg.get("content");
                let content_str = content_val
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string())
                    .or_else(|| {
                        content_val?.as_array().map(|parts| {
                            parts
                                .iter()
                                .filter_map(|p| p.get("text").and_then(|t| t.as_str()))
                                .collect::<Vec<_>>()
                                .join("")
                        })
                    });
                if let Some(s) = content_str {
                    system_parts.push(s);
                }
            } else if role == "tool" {
                // OpenAI tool result → Anthropic user message with tool_result block
                // Merge consecutive tool results into a single user message (Anthropic
                // rejects consecutive messages with the same role)
                let tool_call_id = msg
                    .get("tool_call_id")
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                // content can be a string or an array of content parts
                let content_val = msg.get("content");
                let content_str = content_val
                    .and_then(|v| v.as_str())
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
                let block = serde_json::json!({
                    "type": "tool_result",
                    "tool_use_id": tool_call_id,
                    "content": content_str,
                });

                // If last message is a user message with tool_result blocks, append.
                // Only merge into arrays that already contain tool_result blocks —
                // don't corrupt a regular user message that happens to have array content.
                let merged = messages.last_mut().and_then(|last| {
                    if last.get("role")?.as_str()? == "user" {
                        let arr = last.get_mut("content")?.as_array_mut()?;
                        let has_tool_result = arr.iter().any(|el| {
                            el.get("type").and_then(|t| t.as_str()) == Some("tool_result")
                        });
                        if has_tool_result {
                            Some(arr)
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                });
                if let Some(blocks) = merged {
                    blocks.push(block);
                } else {
                    messages.push(serde_json::json!({
                        "role": "user",
                        "content": [block],
                    }));
                }
            } else if role == "assistant" && msg.get("tool_calls").is_some() {
                // Assistant message with tool_calls → Anthropic content blocks
                let mut blocks: Vec<serde_json::Value> = Vec::new();
                // Preserve any text content (string or array form)
                let text_content = msg.get("content");
                let preamble = text_content
                    .and_then(|c| c.as_str())
                    .map(|s| s.to_string())
                    .or_else(|| {
                        text_content?.as_array().map(|parts| {
                            parts
                                .iter()
                                .filter_map(|p| p.get("text").and_then(|t| t.as_str()))
                                .collect::<Vec<_>>()
                                .join("")
                        })
                    })
                    .unwrap_or_default();
                if !preamble.is_empty() {
                    blocks.push(serde_json::json!({"type": "text", "text": preamble}));
                }
                if let Some(tool_calls) = msg.get("tool_calls").and_then(|t| t.as_array()) {
                    for tc in tool_calls {
                        let id = tc.get("id").and_then(|v| v.as_str()).unwrap_or("");
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
                        blocks.push(serde_json::json!({
                            "type": "tool_use",
                            "id": id,
                            "name": name,
                            "input": input,
                        }));
                    }
                }
                messages.push(serde_json::json!({"role": "assistant", "content": blocks}));
            } else {
                // Standard message — strip "name" field, keep role + content.
                // Array content: translate OpenAI image_url parts to Anthropic
                // image blocks (untranslated they 400 the whole request upstream).
                let mut clean = serde_json::Map::new();
                clean.insert(
                    "role".to_string(),
                    serde_json::Value::String(role.to_string()),
                );
                if let Some(content) = msg.get("content") {
                    let translated = if let Some(parts) = content.as_array() {
                        serde_json::Value::Array(
                            parts
                                .iter()
                                .map(|p| {
                                    if p.get("type").and_then(|t| t.as_str()) == Some("image_url") {
                                        let url = p
                                            .pointer("/image_url/url")
                                            .and_then(|u| u.as_str())
                                            .unwrap_or("");
                                        openai_image_part_to_anthropic(url)
                                    } else {
                                        p.clone()
                                    }
                                })
                                .collect(),
                        )
                    } else {
                        content.clone()
                    };
                    clean.insert("content".to_string(), translated);
                }
                messages.push(serde_json::Value::Object(clean));
            }
        }
    }

    // response_format: inject JSON mode instruction into system prompt
    if wants_json_object(body) {
        system_parts.push(
            "You must respond with valid JSON only. No markdown, no code fences, no explanation — just raw JSON.".to_string(),
        );
    }

    if !system_parts.is_empty() {
        out.insert(
            "system".to_string(),
            serde_json::Value::String(system_parts.join("\n\n")),
        );
    }

    out.insert("messages".to_string(), serde_json::Value::Array(messages));

    // max_tokens: try max_tokens, then max_completion_tokens, default 4096
    let max_tokens = body
        .get("max_tokens")
        .or_else(|| body.get("max_completion_tokens"))
        .cloned()
        .unwrap_or(serde_json::json!(4096));
    out.insert("max_tokens".to_string(), max_tokens);

    // Direct passthrough params — except `temperature` on models that
    // hard-reject it as deprecated (Claude ≥ 4.7, LAB-798): forwarding it
    // fails the whole request with a non-retryable 400. Drop policy and
    // rationale live in `drops_deprecated_temperature`.
    let model_name = body.get("model").and_then(|m| m.as_str()).unwrap_or("");
    for key in &["temperature", "top_p", "top_k", "stream"] {
        if let Some(v) = body.get(*key) {
            if *key == "temperature" && drops_deprecated_temperature(model_name, v) {
                continue;
            }
            out.insert(key.to_string(), v.clone());
        }
    }

    // reasoning_effort -> output_config.effort (policy in `translate_reasoning_effort`)
    if let Some(effort) = body
        .get("reasoning_effort")
        .and_then(translate_reasoning_effort)
    {
        out.insert(
            "output_config".to_string(),
            serde_json::json!({"effort": effort}),
        );
    }

    // stop -> stop_sequences
    if let Some(stop) = body.get("stop") {
        let sequences = if stop.is_array() {
            stop.clone()
        } else if let Some(s) = stop.as_str() {
            serde_json::json!([s])
        } else {
            serde_json::json!([])
        };
        out.insert("stop_sequences".to_string(), sequences);
    }

    // tools: OpenAI function definitions → Anthropic tool format
    if let Some(tools) = body.get("tools").and_then(|t| t.as_array()) {
        let anthropic_tools: Vec<serde_json::Value> = tools
            .iter()
            .filter_map(|tool| {
                let func = tool.get("function")?;
                let name = func.get("name")?.as_str()?;
                let mut t = serde_json::json!({"name": name});
                if let Some(desc) = func.get("description") {
                    t["description"] = desc.clone();
                }
                t["input_schema"] = func
                    .get("parameters")
                    .filter(|v| !v.is_null())
                    .cloned()
                    .unwrap_or_else(|| serde_json::json!({"type": "object", "properties": {}}));
                Some(t)
            })
            .collect();
        if !anthropic_tools.is_empty() {
            out.insert(
                "tools".to_string(),
                serde_json::Value::Array(anthropic_tools),
            );
        }
    }

    // tool_choice translation
    if let Some(tc) = body.get("tool_choice") {
        let anthropic_tc = if let Some(s) = tc.as_str() {
            match s {
                "auto" => Some(serde_json::json!({"type": "auto"})),
                "none" => {
                    out.remove("tools");
                    None
                }
                "required" => Some(serde_json::json!({"type": "any"})),
                _ => None,
            }
        } else {
            tc.pointer("/function/name")
                .and_then(|n| n.as_str())
                .map(|name| serde_json::json!({"type": "tool", "name": name}))
        };
        if let Some(atc) = anthropic_tc {
            out.insert("tool_choice".to_string(), atc);
        }
    }

    serde_json::Value::Object(out)
}

pub(crate) fn translate_anthropic_to_openai(
    body: &serde_json::Value,
    json_mode: bool,
) -> serde_json::Value {
    let id = body
        .get("id")
        .and_then(|v| v.as_str())
        .unwrap_or("msg_unknown");
    let model = body
        .get("model")
        .and_then(|v| v.as_str())
        .unwrap_or("unknown");

    let blocks = body.get("content").and_then(|c| c.as_array());

    // Concatenate text content blocks; strip markdown JSON fences only when
    // the request asked for response_format: json_object.
    let content = blocks
        .map(|blocks| {
            let raw = blocks
                .iter()
                .filter(|b| b.get("type").and_then(|t| t.as_str()) == Some("text"))
                .filter_map(|b| b.get("text").and_then(|t| t.as_str()))
                .collect::<Vec<_>>()
                .join("");
            if json_mode {
                strip_json_fences(&raw)
            } else {
                raw
            }
        })
        .unwrap_or_default();

    // Extract tool_use blocks → OpenAI tool_calls
    let tool_calls: Vec<serde_json::Value> = blocks
        .map(|blocks| {
            blocks
                .iter()
                .filter(|b| b.get("type").and_then(|t| t.as_str()) == Some("tool_use"))
                .map(|b| {
                    let tc_id = b.get("id").and_then(|v| v.as_str()).unwrap_or("");
                    let name = b.get("name").and_then(|v| v.as_str()).unwrap_or("");
                    let input = b.get("input").cloned().unwrap_or(serde_json::json!({}));
                    serde_json::json!({
                        "id": tc_id,
                        "type": "function",
                        "function": {
                            "name": name,
                            "arguments": input.to_string(),
                        }
                    })
                })
                .collect()
        })
        .unwrap_or_default();

    let stop_reason = body
        .get("stop_reason")
        .and_then(|v| v.as_str())
        .unwrap_or("end_turn");

    let input_tokens = body
        .pointer("/usage/input_tokens")
        .and_then(|v| v.as_u64())
        .unwrap_or(0);
    let output_tokens = body
        .pointer("/usage/output_tokens")
        .and_then(|v| v.as_u64())
        .unwrap_or(0);

    // Build message: content is null when only tool_calls present (OpenAI convention)
    let mut message = serde_json::json!({"role": "assistant"});
    if !tool_calls.is_empty() {
        message["tool_calls"] = serde_json::Value::Array(tool_calls);
        if content.is_empty() {
            message["content"] = serde_json::Value::Null;
        } else {
            message["content"] = serde_json::Value::String(content);
        }
    } else {
        message["content"] = serde_json::Value::String(content);
    }

    serde_json::json!({
        "id": format!("chatcmpl-{}", id),
        "object": "chat.completion",
        "created": AppState::now_epoch(),
        "model": model,
        "choices": [{
            "index": 0,
            "message": message,
            "finish_reason": map_stop_reason(stop_reason),
        }],
        "usage": {
            "prompt_tokens": input_tokens,
            "completion_tokens": output_tokens,
            "total_tokens": input_tokens + output_tokens,
        },
    })
}

/// Parse a raw SSE event block and translate to OpenAI format.
/// Returns None for events that should be skipped (ping, text content_block_start, etc.).
pub(crate) fn translate_sse_event(raw: &str, ctx: &mut StreamContext) -> Option<String> {
    // Both terminators are final (mirror of the reverse translator): once
    // `[DONE]` or an error frame is out, a stray upstream event must not
    // translate into a second `[DONE]`-carrying frame.
    if ctx.terminal.reached() {
        return None;
    }

    let mut event_type = String::new();
    let mut data = String::new();

    for line in raw.lines() {
        if let Some(val) = line.strip_prefix("event:") {
            event_type = val.trim().to_string();
        } else if let Some(val) = line.strip_prefix("data:") {
            data = val.trim().to_string();
        }
    }

    if data.is_empty() {
        return None;
    }

    let parsed: serde_json::Value = serde_json::from_str(&data).ok()?;

    match event_type.as_str() {
        "message_start" => {
            if let Some(msg) = parsed.get("message") {
                if let Some(id) = msg.get("id").and_then(|v| v.as_str()) {
                    ctx.id = format!("chatcmpl-{}", id);
                }
                if let Some(model) = msg.get("model").and_then(|v| v.as_str()) {
                    ctx.model = model.to_string();
                }
            }
            Some(make_openai_chunk(
                ctx,
                serde_json::json!({"role": "assistant"}),
                None,
            ))
        }
        "content_block_start" => {
            let block = parsed.get("content_block")?;
            let block_type = block.get("type").and_then(|t| t.as_str()).unwrap_or("");
            if block_type == "tool_use" {
                ctx.in_tool_use = true;
                ctx.tool_call_index += 1;
                ctx.current_tool_id = block
                    .get("id")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                let name = block.get("name").and_then(|v| v.as_str()).unwrap_or("");
                Some(make_openai_chunk(
                    ctx,
                    serde_json::json!({
                        "tool_calls": [{
                            "index": ctx.tool_call_index,
                            "id": ctx.current_tool_id,
                            "type": "function",
                            "function": {"name": name, "arguments": ""}
                        }]
                    }),
                    None,
                ))
            } else {
                ctx.in_tool_use = false;
                None
            }
        }
        "content_block_delta" => {
            let delta_type = parsed
                .pointer("/delta/type")
                .and_then(|v| v.as_str())
                .unwrap_or("");

            if ctx.in_tool_use && delta_type == "input_json_delta" {
                let partial = parsed
                    .pointer("/delta/partial_json")
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                if partial.is_empty() {
                    return None;
                }
                Some(make_openai_chunk(
                    ctx,
                    serde_json::json!({
                        "tool_calls": [{
                            "index": ctx.tool_call_index,
                            "function": {"arguments": partial}
                        }]
                    }),
                    None,
                ))
            } else {
                let text = parsed
                    .pointer("/delta/text")
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                if text.is_empty() {
                    return None;
                }
                if ctx.json_mode {
                    // Buffer JSON-mode text; fences are stripped once over the
                    // whole message at message_delta (they may be split across
                    // deltas). Partial JSON is unparseable, so deferring the
                    // emit costs the client nothing.
                    ctx.text_buffer.push_str(text);
                    return None;
                }
                Some(make_openai_chunk(
                    ctx,
                    serde_json::json!({"content": text}),
                    None,
                ))
            }
        }
        "content_block_stop" => {
            if ctx.in_tool_use {
                ctx.in_tool_use = false;
            }
            None
        }
        "message_delta" => {
            let stop_reason = parsed
                .pointer("/delta/stop_reason")
                .and_then(|v| v.as_str())
                .unwrap_or("end_turn");
            let finish = make_openai_chunk(
                ctx,
                serde_json::json!({}),
                Some(map_stop_reason(stop_reason)),
            );
            // Flush buffered JSON-mode content (fence-stripped) as its own chunk
            // before the finish chunk, preserving OpenAI content→finish order.
            match ctx.take_stripped_json_content() {
                Some(content) => {
                    let content_chunk =
                        make_openai_chunk(ctx, serde_json::json!({ "content": content }), None);
                    Some(format!("{content_chunk}{finish}"))
                }
                None => Some(finish),
            }
        }
        "message_stop" => {
            // Safety net: if message_delta never arrived (abnormal upstream),
            // flush buffered JSON-mode content before closing so it is never
            // silently dropped.
            match ctx.take_stripped_json_content() {
                Some(content) => {
                    let content_chunk =
                        make_openai_chunk(ctx, serde_json::json!({ "content": content }), None);
                    Some(format!("{content_chunk}data: [DONE]\n\n"))
                }
                None => Some("data: [DONE]\n\n".to_string()),
            }
        }
        "error" => {
            // In-band upstream failure (e.g. overloaded_error mid-stream).
            // Surface it in the client's protocol instead of dropping it —
            // dropping it made the stream end with a clean [DONE] after a
            // silent truncation. The frame carries its own [DONE];
            // ctx.terminal.errored tells the stream loop to stop and skip
            // the ensure-[DONE] guard.
            ctx.terminal.errored = true;
            let err_type = parsed
                .pointer("/error/type")
                .and_then(|v| v.as_str())
                .unwrap_or("api_error");
            let err_msg = parsed
                .pointer("/error/message")
                .and_then(|v| v.as_str())
                .unwrap_or("upstream error");
            warn!(
                error_type = err_type,
                error_message = err_msg,
                "Anthropic upstream emitted in-band error event mid-stream"
            );
            Some(openai_error_sse(&format!("{err_type}: {err_msg}")))
        }
        _ => None, // ping
    }
}

#[cfg(test)]
mod tests;
