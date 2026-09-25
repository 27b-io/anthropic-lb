use crate::*;

// ── Auto-cache injection ────────────────────────────────────────────

pub(crate) struct CacheInjection {
    pub(crate) tools: bool,
    pub(crate) system: bool,
    pub(crate) messages: bool,
    pub(crate) skipped: bool,
}

/// Inject prompt cache breakpoints into an Anthropic API request body.
///
/// Strategy: up to 3 breakpoints — last tool, last system block, last user message.
/// No-op if any `cache_control` is already present anywhere in the body.
pub(crate) fn inject_cache_breakpoints(body: &mut serde_json::Value) -> CacheInjection {
    let cache_marker = serde_json::json!({"type": "ephemeral"});
    let mut result = CacheInjection {
        tools: false,
        system: false,
        messages: false,
        skipped: false,
    };

    // Bail if any cache_control already present
    if has_existing_cache_control(body) {
        result.skipped = true;
        return result;
    }

    // 1. Tools — add cache_control to last tool
    if let Some(tools) = body.get_mut("tools").and_then(|t| t.as_array_mut()) {
        if let Some(last) = tools.last_mut() {
            if let Some(obj) = last.as_object_mut() {
                obj.insert("cache_control".to_string(), cache_marker.clone());
                result.tools = true;
            }
        }
    }

    // 2. System — string → array conversion, or annotate last block
    if let Some(system) = body.get_mut("system") {
        if let Some(text) = system.as_str().map(String::from) {
            *system = serde_json::json!([{
                "type": "text",
                "text": text,
                "cache_control": cache_marker,
            }]);
            result.system = true;
        } else if let Some(arr) = system.as_array_mut() {
            if let Some(last) = arr.last_mut() {
                if let Some(obj) = last.as_object_mut() {
                    obj.insert("cache_control".to_string(), cache_marker.clone());
                    result.system = true;
                }
            }
        }
    }

    // 3. Messages — find last user message, annotate its content
    if let Some(messages) = body.get_mut("messages").and_then(|m| m.as_array_mut()) {
        if let Some(last_user) = messages
            .iter_mut()
            .rev()
            .find(|m| m.get("role").and_then(|r| r.as_str()) == Some("user"))
        {
            if let Some(content) = last_user.get_mut("content") {
                if let Some(text) = content.as_str().map(String::from) {
                    *content = serde_json::json!([{
                        "type": "text",
                        "text": text,
                        "cache_control": cache_marker,
                    }]);
                    result.messages = true;
                } else if let Some(arr) = content.as_array_mut() {
                    if let Some(last) = arr.last_mut() {
                        if let Some(obj) = last.as_object_mut() {
                            obj.insert("cache_control".to_string(), cache_marker.clone());
                            result.messages = true;
                        }
                    }
                }
            }
        }
    }

    result
}

/// Check if any cache_control key exists in tools, system, or messages.
fn has_existing_cache_control(body: &serde_json::Value) -> bool {
    // Check tools
    if let Some(tools) = body.get("tools").and_then(|t| t.as_array()) {
        for tool in tools {
            if tool.get("cache_control").is_some() {
                return true;
            }
        }
    }
    // Check system
    if let Some(system) = body.get("system") {
        if system.get("cache_control").is_some() {
            return true;
        }
        if let Some(arr) = system.as_array() {
            for block in arr {
                if block.get("cache_control").is_some() {
                    return true;
                }
            }
        }
    }
    // Check messages
    if let Some(messages) = body.get("messages").and_then(|m| m.as_array()) {
        for msg in messages {
            if msg.get("cache_control").is_some() {
                return true;
            }
            if let Some(content) = msg.get("content") {
                if content.get("cache_control").is_some() {
                    return true;
                }
                if let Some(arr) = content.as_array() {
                    for block in arr {
                        if block.get("cache_control").is_some() {
                            return true;
                        }
                    }
                }
            }
        }
    }
    false
}

/// Whether a header name looks sensitive (substring match).
pub(crate) fn is_sensitive_header(name: &str) -> bool {
    SENSITIVE_HEADER_SUBSTRINGS
        .iter()
        .any(|sub| name.contains(sub))
}

/// Return the header value for debug logging, redacting sensitive headers.
pub(crate) fn debug_header_value<'a>(
    name: &axum::http::HeaderName,
    value: &'a HeaderValue,
) -> &'a str {
    if is_sensitive_header(name.as_str()) {
        "<redacted>"
    } else {
        value.to_str().unwrap_or("<binary>")
    }
}

/// Debug: dump all cache_control objects found in the request body.
pub(crate) fn debug_dump_cache_control(body: &serde_json::Value, req_id: &str) {
    let mut count = 0u32;

    if let Some(tools) = body.get("tools").and_then(|t| t.as_array()) {
        for (i, tool) in tools.iter().enumerate() {
            if let Some(cc) = tool.get("cache_control") {
                debug!(req_id, location = format_args!("tools[{i}]"), cache_control = %cc, "body cache_control");
                count += 1;
            }
        }
    }
    if let Some(system) = body.get("system") {
        if let Some(cc) = system.get("cache_control") {
            debug!(req_id, location = "system", cache_control = %cc, "body cache_control");
            count += 1;
        }
        if let Some(arr) = system.as_array() {
            for (i, block) in arr.iter().enumerate() {
                if let Some(cc) = block.get("cache_control") {
                    debug!(req_id, location = format_args!("system[{i}]"), cache_control = %cc, "body cache_control");
                    count += 1;
                }
            }
        }
    }
    if let Some(messages) = body.get("messages").and_then(|m| m.as_array()) {
        for (i, msg) in messages.iter().enumerate() {
            let role = msg.get("role").and_then(|r| r.as_str()).unwrap_or("-");
            if let Some(cc) = msg.get("cache_control") {
                debug!(req_id, location = format_args!("messages[{i}]"), role, cache_control = %cc, "body cache_control");
                count += 1;
            }
            if let Some(content) = msg.get("content") {
                if let Some(arr) = content.as_array() {
                    for (j, block) in arr.iter().enumerate() {
                        if let Some(cc) = block.get("cache_control") {
                            debug!(
                                req_id,
                                location = format_args!("messages[{i}].content[{j}]"),
                                role,
                                cache_control = %cc,
                                "body cache_control"
                            );
                            count += 1;
                        }
                    }
                }
            }
        }
    }
    let msg_count = body
        .get("messages")
        .and_then(|m| m.as_array())
        .map(|a| a.len())
        .unwrap_or(0);
    let tool_count = body
        .get("tools")
        .and_then(|t| t.as_array())
        .map(|a| a.len())
        .unwrap_or(0);
    debug!(
        req_id,
        cache_control_count = count,
        messages = msg_count,
        tools = tool_count,
        "body summary"
    );
}

#[cfg(test)]
mod tests;
