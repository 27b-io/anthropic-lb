use super::*;

// ── Unit: auto-cache injection ─────────────────────────────────

#[test]
fn inject_cache_no_existing() {
    let mut body = serde_json::json!({
        "model": "claude-sonnet-4-6",
        "max_tokens": 1024,
        "system": "You are a helpful assistant.",
        "tools": [
            {"name": "get_weather", "description": "Gets weather", "input_schema": {"type": "object"}},
            {"name": "search", "description": "Searches", "input_schema": {"type": "object"}}
        ],
        "messages": [
            {"role": "user", "content": "Hello"},
            {"role": "assistant", "content": "Hi there!"},
            {"role": "user", "content": "What's the weather?"}
        ]
    });

    let inj = inject_cache_breakpoints(&mut body);
    assert!(!inj.skipped);
    assert!(inj.tools);
    assert!(inj.system);
    assert!(inj.messages);

    // Last tool should have cache_control
    let tools = body["tools"].as_array().unwrap();
    assert!(tools[0].get("cache_control").is_none());
    assert_eq!(tools[1]["cache_control"]["type"], "ephemeral");

    // System should be converted to array with cache_control
    let system = body["system"].as_array().unwrap();
    assert_eq!(system.len(), 1);
    assert_eq!(system[0]["text"], "You are a helpful assistant.");
    assert_eq!(system[0]["cache_control"]["type"], "ephemeral");

    // Last user message content should be converted to array
    let msgs = body["messages"].as_array().unwrap();
    let last_user = &msgs[2];
    let content = last_user["content"].as_array().unwrap();
    assert_eq!(content[0]["text"], "What's the weather?");
    assert_eq!(content[0]["cache_control"]["type"], "ephemeral");

    // First user message should be untouched
    assert_eq!(msgs[0]["content"], "Hello");
}

#[test]
fn inject_cache_system_array() {
    let mut body = serde_json::json!({
        "model": "claude-sonnet-4-6",
        "system": [
            {"type": "text", "text": "System prompt part 1"},
            {"type": "text", "text": "System prompt part 2"}
        ],
        "messages": [
            {"role": "user", "content": "Hello"}
        ]
    });

    let inj = inject_cache_breakpoints(&mut body);
    assert!(inj.system);

    let system = body["system"].as_array().unwrap();
    assert!(system[0].get("cache_control").is_none());
    assert_eq!(system[1]["cache_control"]["type"], "ephemeral");
}

#[test]
fn inject_cache_already_present() {
    let mut body = serde_json::json!({
        "model": "claude-sonnet-4-6",
        "system": [
            {"type": "text", "text": "Cached system", "cache_control": {"type": "ephemeral"}}
        ],
        "messages": [
            {"role": "user", "content": "Hello"}
        ]
    });

    let inj = inject_cache_breakpoints(&mut body);
    assert!(inj.skipped);
    assert!(!inj.tools);
    assert!(!inj.system);
    assert!(!inj.messages);

    // Verify nothing was modified — messages content is still a string
    assert_eq!(body["messages"][0]["content"], "Hello");
}

#[test]
fn inject_cache_already_present_in_tools() {
    let mut body = serde_json::json!({
        "model": "claude-sonnet-4-6",
        "tools": [
            {"name": "t1", "cache_control": {"type": "ephemeral"}}
        ],
        "messages": [
            {"role": "user", "content": "Hello"}
        ]
    });

    let inj = inject_cache_breakpoints(&mut body);
    assert!(inj.skipped);
}

#[test]
fn inject_cache_already_present_in_message_content() {
    let mut body = serde_json::json!({
        "model": "claude-sonnet-4-6",
        "messages": [
            {"role": "user", "content": [
                {"type": "text", "text": "hi", "cache_control": {"type": "ephemeral"}}
            ]}
        ]
    });

    let inj = inject_cache_breakpoints(&mut body);
    assert!(inj.skipped);
}

#[test]
fn inject_cache_empty_body() {
    let mut body = serde_json::json!({});
    let inj = inject_cache_breakpoints(&mut body);
    assert!(!inj.skipped);
    assert!(!inj.tools);
    assert!(!inj.system);
    assert!(!inj.messages);
}

#[test]
fn inject_cache_messages_string_content() {
    let mut body = serde_json::json!({
        "messages": [
            {"role": "assistant", "content": "I'm an assistant"},
            {"role": "user", "content": "Tell me a joke"}
        ]
    });

    let inj = inject_cache_breakpoints(&mut body);
    assert!(inj.messages);
    assert!(!inj.tools);
    assert!(!inj.system);

    let content = body["messages"][1]["content"].as_array().unwrap();
    assert_eq!(content[0]["type"], "text");
    assert_eq!(content[0]["text"], "Tell me a joke");
    assert_eq!(content[0]["cache_control"]["type"], "ephemeral");

    // Assistant message should be untouched
    assert_eq!(body["messages"][0]["content"], "I'm an assistant");
}

#[test]
fn inject_cache_user_message_array_content() {
    let mut body = serde_json::json!({
        "messages": [
            {"role": "user", "content": [
                {"type": "text", "text": "First part"},
                {"type": "text", "text": "Second part"}
            ]}
        ]
    });

    let inj = inject_cache_breakpoints(&mut body);
    assert!(inj.messages);

    let content = body["messages"][0]["content"].as_array().unwrap();
    assert!(content[0].get("cache_control").is_none());
    assert_eq!(content[1]["cache_control"]["type"], "ephemeral");
}

#[test]
fn inject_cache_no_user_messages() {
    let mut body = serde_json::json!({
        "messages": [
            {"role": "assistant", "content": "Hi"}
        ]
    });

    let inj = inject_cache_breakpoints(&mut body);
    assert!(!inj.messages);
}
