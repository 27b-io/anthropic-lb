use crate::*;

// ── OAuth system prompt injection ──────────────────────────────────

/// Check whether the request body already contains the OAuth system prompt:
/// as a prefix of the `system` string, or as a prefix of ANY block in the
/// `system` array. The whole array is scanned because Claude Code puts its
/// attribution block first and the identity prompt after it.
pub(crate) fn has_oauth_system_prompt(body: &serde_json::Value) -> bool {
    match body.get("system") {
        Some(system) if system.is_string() => system
            .as_str()
            .is_some_and(|s| s.starts_with(OAUTH_SYSTEM_PROMPT)),
        Some(system) if system.is_array() => system
            .as_array()
            .map(|arr| {
                arr.iter().any(|b| {
                    b.get("text")
                        .and_then(|t| t.as_str())
                        .is_some_and(|t| t.starts_with(OAUTH_SYSTEM_PROMPT))
                })
            })
            .unwrap_or(false),
        _ => false,
    }
}

/// Inject the Claude Code system prompt into the `system` array.
///
/// OAuth tokens (sk-ant-oat*) require this exact prompt somewhere in `system`
/// to access sonnet/opus models; it does not have to be first. Haiku works
/// without it, but we inject unconditionally for OAuth accounts to keep
/// things simple.
///
/// - No system field → creates `"system": [{"type":"text","text":"..."}]`
/// - String system → converts to array with CC prompt first, original second
/// - Array system → inserts the CC prompt at index 0, or at index 1 when
///   `system[0]` is a Claude Code attribution block, so the upstream's
///   positional strip of that block still fires
pub(crate) fn inject_oauth_system_prompt(body: &mut serde_json::Value) {
    // Shared mutator: the `body["system"] = …` assignments below are the
    // only `IndexMut<&str>` on a client-controlled `Value` in the request
    // path, and that operator panics on anything but Null/Object. The
    // invariant lives here, with the lines that need it, not with whatever
    // callers happen to pre-validate today (LAB-4314).
    if !body.is_object() || has_oauth_system_prompt(body) {
        return;
    }

    let cc_block = serde_json::json!({"type": "text", "text": OAUTH_SYSTEM_PROMPT});

    match body.get("system") {
        None | Some(&serde_json::Value::Null) => {
            body["system"] = serde_json::json!([cc_block]);
        }
        Some(system) => {
            if let Some(text) = system.as_str() {
                // Convert string to array: CC prompt first, original second
                body["system"] = serde_json::json!([
                    cc_block,
                    {"type": "text", "text": text}
                ]);
            } else if let Some(arr) = system.as_array() {
                // Keep a leading attribution block at index 0 (see ATTRIBUTION_BLOCK_PREFIX).
                let leads_with_attribution = arr
                    .first()
                    .and_then(|b| b["text"].as_str())
                    .is_some_and(|t| t.starts_with(ATTRIBUTION_BLOCK_PREFIX));
                let at = if leads_with_attribution { 1 } else { 0 };
                let mut new_arr = arr.clone();
                new_arr.insert(at, cc_block);
                body["system"] = serde_json::Value::Array(new_arr);
            }
        }
    }
}

#[cfg(test)]
mod tests;
