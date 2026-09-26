use super::*;

// ── Unit: token usage extraction ───────────────────────────────

#[test]
fn usage_from_non_streaming_response() {
    let body = serde_json::json!({
        "type": "message",
        "usage": {
            "input_tokens": 100,
            "output_tokens": 50,
            "cache_creation_input_tokens": 20,
            "cache_read_input_tokens": 30,
        }
    });
    let usage = TokenUsage::from_response_body(&body);
    assert_eq!(usage.input_tokens, 100);
    assert_eq!(usage.output_tokens, 50);
    assert_eq!(usage.cache_creation_input_tokens, 20);
    assert_eq!(usage.cache_read_input_tokens, 30);
}

#[test]
fn usage_from_response_no_usage_field() {
    let body = serde_json::json!({"type": "error"});
    let usage = TokenUsage::from_response_body(&body);
    assert!(usage.is_empty());
}

#[test]
fn usage_from_sse_stream() {
    let sse_text = "\
event: message_start\n\
data: {\"type\":\"message_start\",\"message\":{\"usage\":{\"input_tokens\":150,\"cache_creation_input_tokens\":10,\"cache_read_input_tokens\":5}}}\n\
\n\
event: content_block_delta\n\
data: {\"type\":\"content_block_delta\",\"delta\":{\"text\":\"Hello\"}}\n\
\n\
event: message_delta\n\
data: {\"type\":\"message_delta\",\"usage\":{\"output_tokens\":75}}\n\
\n\
event: message_stop\n\
data: {\"type\":\"message_stop\"}\n\n";

    let usage = TokenUsage::from_sse_text(sse_text);
    assert_eq!(usage.input_tokens, 150);
    assert_eq!(usage.output_tokens, 75);
    assert_eq!(usage.cache_creation_input_tokens, 10);
    assert_eq!(usage.cache_read_input_tokens, 5);
}

#[test]
fn usage_from_empty_sse() {
    let usage = TokenUsage::from_sse_text("");
    assert!(usage.is_empty());
}

/// LAB-717: the incremental scanner must produce the same usage regardless of
/// how the stream is fragmented into chunks — including splits mid-line,
/// mid-JSON, and down to one byte per push.
#[test]
fn sse_scanner_chunk_boundaries_do_not_affect_usage() {
    let sse: &[u8] = b"event: message_start\n\
data: {\"type\":\"message_start\",\"message\":{\"usage\":{\"input_tokens\":150,\"cache_creation_input_tokens\":10,\"cache_read_input_tokens\":5}}}\n\
\n\
event: content_block_delta\n\
data: {\"type\":\"content_block_delta\",\"delta\":{\"text\":\"mentions message_delta harmlessly\"}}\n\
\n\
event: message_delta\n\
data: {\"type\":\"message_delta\",\"usage\":{\"output_tokens\":75}}\n\
\n\
event: message_stop\n\
data: {\"type\":\"message_stop\"}\n\n";

    // Every chunk size from pathological (1 byte) to whole-stream.
    for chunk_size in [1, 3, 7, 64, sse.len()] {
        let mut scanner = SseUsageScanner::default();
        for chunk in sse.chunks(chunk_size) {
            scanner.push(chunk);
        }
        scanner.finish();
        assert_eq!(scanner.usage.input_tokens, 150, "chunk_size={chunk_size}");
        assert_eq!(scanner.usage.output_tokens, 75, "chunk_size={chunk_size}");
        assert_eq!(scanner.usage.cache_creation_input_tokens, 10);
        assert_eq!(scanner.usage.cache_read_input_tokens, 5);
        assert_eq!(scanner.bytes_seen, sse.len());
        assert_eq!(scanner.event_count, 4);
    }
}

/// LAB-717: the trailing line is scanned even when the stream ends without a
/// final newline (finish() flushes the carry).
#[test]
fn sse_scanner_flushes_unterminated_final_line() {
    let mut scanner = SseUsageScanner::default();
    scanner.push(b"data: {\"type\":\"message_delta\",\"usage\":{\"output_tokens\":42}}");
    scanner.finish();
    assert_eq!(scanner.usage.output_tokens, 42);
}

/// LAB-717: a single line larger than SSE_SCAN_MAX_LINE must not grow the
/// carry without bound — it is discarded up to its newline, and scanning
/// resumes cleanly on the following lines.
#[test]
fn sse_scanner_discards_oversized_line_and_recovers() {
    let mut scanner = SseUsageScanner::default();
    // Oversized junk line delivered across several pushes, no newline yet.
    let junk = vec![b'x'; SSE_SCAN_MAX_LINE / 2 + 1];
    scanner.push(&junk);
    scanner.push(&junk); // crosses the cap → carry dropped, skip mode
    assert!(
        scanner.carry.is_empty(),
        "carry must not hold oversized line"
    );
    scanner.push(b"more of the same line\n"); // newline ends the skipped line
    scanner.push(b"data: {\"type\":\"message_delta\",\"usage\":{\"output_tokens\":9}}\n");
    scanner.finish();
    assert_eq!(scanner.usage.output_tokens, 9);
}

/// LAB-717: the cap applies uniformly when the oversized line terminates
/// within a single push — both when it completes a cross-chunk carry and
/// when it arrives whole — and the carry allocation must not retain the
/// oversized capacity afterwards.
#[test]
fn sse_scanner_caps_oversized_line_completed_in_one_push() {
    // Whole oversized line (with newline) in one push.
    let mut scanner = SseUsageScanner::default();
    let mut blob = vec![b'x'; SSE_SCAN_MAX_LINE + 1];
    blob.push(b'\n');
    scanner.push(&blob);
    scanner.push(b"data: {\"type\":\"message_delta\",\"usage\":{\"output_tokens\":7}}\n");
    scanner.finish();
    assert_eq!(scanner.usage.output_tokens, 7);
    assert!(scanner.carry.capacity() <= SSE_SCAN_MAX_LINE);

    // Carry just under the cap, then a chunk whose newline completes the
    // combined oversized line.
    let mut scanner = SseUsageScanner::default();
    scanner.push(&vec![b'x'; SSE_SCAN_MAX_LINE]); // fills carry to the cap
    scanner.push(b"y\ndata: {\"type\":\"message_delta\",\"usage\":{\"output_tokens\":8}}\n");
    scanner.finish();
    assert_eq!(scanner.usage.output_tokens, 8);
    assert!(
        scanner.carry.capacity() <= SSE_SCAN_MAX_LINE,
        "oversized merge must not balloon the retained carry allocation"
    );
}

/// LAB-717: diagnostic metadata for stream_end_no_usage is bounded — full
/// event count, but only the first five event types retained.
#[test]
fn sse_scanner_event_preview_bounded_to_five() {
    let mut scanner = SseUsageScanner::default();
    for i in 0..7 {
        scanner.push(format!("event: ev{i}\ndata: {{}}\n\n").as_bytes());
    }
    scanner.finish();
    assert_eq!(scanner.event_count, 7);
    assert_eq!(
        scanner.event_preview,
        vec!["ev0", "ev1", "ev2", "ev3", "ev4"]
    );
    assert!(scanner.usage.is_empty());
}

#[test]
fn sse_scanner_captures_response_model() {
    let sse_text = "\
event: message_start\n\
data: {\"type\":\"message_start\",\"message\":{\"model\":\"claude-sonnet-5\",\"usage\":{\"input_tokens\":150}}}\n\
\n\
event: message_delta\n\
data: {\"type\":\"message_delta\",\"usage\":{\"output_tokens\":75}}\n\n";
    let mut scanner = SseUsageScanner::default();
    scanner.push(sse_text.as_bytes());
    scanner.finish();
    assert_eq!(scanner.model.as_deref(), Some("claude-sonnet-5"));
    assert_eq!(scanner.usage.input_tokens, 150);
}

#[test]
fn inject_auth_non_oauth_token_uses_x_api_key() {
    // The contract is two-way: only the OAuth prefix selects Bearer auth;
    // every other token, API key or not, is sent as x-api-key.
    for token in ["sk-ant-api-test123", "not-an-anthropic-prefix-test123"] {
        let mut headers = axum::http::HeaderMap::new();
        headers.insert("authorization", HeaderValue::from_static("Bearer old"));
        inject_account_auth(&mut headers, token, false, &default_betas());
        assert_eq!(headers.get("x-api-key").unwrap(), token);
        assert!(headers.get("authorization").is_none());
        assert!(headers
            .get("anthropic-dangerous-direct-browser-access")
            .is_none());
    }
}

#[test]
fn inject_auth_oauth_token() {
    let mut headers = axum::http::HeaderMap::new();
    inject_account_auth(&mut headers, "sk-ant-oat-test123", false, &default_betas());
    assert_eq!(
        headers.get("authorization").unwrap(),
        "Bearer sk-ant-oat-test123"
    );
    assert_eq!(
        headers
            .get("anthropic-dangerous-direct-browser-access")
            .unwrap(),
        "true"
    );
    let beta = headers.get("anthropic-beta").unwrap().to_str().unwrap();
    assert!(beta.contains("oauth-2025-04-20"));
    assert!(beta.contains("claude-code-20250219"));
}

#[test]
fn inject_auth_oauth_merges_multi_value_beta() {
    // Allow-listed flags split across TWO header values (LAB-1191: unlisted
    // flags are dropped, so the merge is exercised with allowed ones).
    let mut headers = axum::http::HeaderMap::new();
    headers.append(
        "anthropic-beta",
        HeaderValue::from_static("interleaved-thinking-2025-05-14"),
    );
    headers.append(
        "anthropic-beta",
        HeaderValue::from_static("fine-grained-tool-streaming-2025-05-14"),
    );
    inject_account_auth(&mut headers, "sk-ant-oat-test123", false, &default_betas());
    let beta = headers.get("anthropic-beta").unwrap().to_str().unwrap();
    assert!(
        beta.contains("interleaved-thinking-2025-05-14"),
        "should preserve first header"
    );
    assert!(
        beta.contains("fine-grained-tool-streaming-2025-05-14"),
        "should preserve second header"
    );
    assert!(beta.contains("oauth-2025-04-20"), "should add oauth flag");
    assert!(beta.contains("claude-code-20250219"), "should add cc flag");
}

#[test]
fn inject_auth_passthrough_preserves_headers() {
    let mut headers = axum::http::HeaderMap::new();
    headers.insert(
        "authorization",
        HeaderValue::from_static("Bearer user-token"),
    );
    headers.insert("x-api-key", HeaderValue::from_static("user-key"));
    inject_account_auth(&mut headers, "passthrough", true, &default_betas());
    assert_eq!(headers.get("authorization").unwrap(), "Bearer user-token");
    assert_eq!(headers.get("x-api-key").unwrap(), "user-key");
}

#[test]
fn request_context_trims_whitespace_headers() {
    let state = test_state_with(vec![mk_endpoint("a", "sk-ant-api-x")]);
    let mut headers = axum::http::HeaderMap::new();
    headers.insert("x-agent-id", HeaderValue::from_static("  "));
    headers.insert("x-session-id", HeaderValue::from_static(" \t "));
    let ip: IpAddr = "127.0.0.1".parse().unwrap();
    let rctx = RequestContext::from_request(&state, &ip, &headers, None);
    assert_eq!(rctx.agent_id, "-", "whitespace-only agent_id should be -");
    assert_eq!(
        rctx.session_id, "-",
        "whitespace-only session_id should be -"
    );
    assert!(
        rctx.affinity_key(&ip, None).is_none(),
        "no meaningful identity"
    );
}

fn rctx_for(client_id: &str, agent: &str, session: &str) -> RequestContext {
    RequestContext {
        client_id: client_id.to_string(),
        client_ver: "-".to_string(),
        agent_id: agent.to_string(),
        session_id: session.to_string(),
    }
}

#[test]
fn affinity_key_fp_distinguishes_fanout_under_one_session() {
    // The workflow case: many agents share ONE coarse session-id but have
    // distinct content fingerprints. They MUST get distinct keys so they
    // distribute instead of funneling onto one account.
    let ip: IpAddr = "10.88.0.1".parse().unwrap();
    let rctx = rctx_for("claude:first-steps", "-", "aaee4c00");
    let a = rctx.affinity_key(&ip, Some("fp_agent_a")).unwrap();
    let b = rctx.affinity_key(&ip, Some("fp_agent_b")).unwrap();
    assert_ne!(
        a, b,
        "same session, different fp must yield different keys (distribute the fan-out)"
    );
}

#[test]
fn affinity_key_fp_stable_sticks() {
    // A stable-prefix conversation produces the same fp across turns → same
    // key → stays sticky.
    let ip: IpAddr = "10.88.0.1".parse().unwrap();
    let rctx = rctx_for("claude:first-steps", "-", "aaee4c00");
    let t1 = rctx.affinity_key(&ip, Some("stable_fp")).unwrap();
    let t2 = rctx.affinity_key(&ip, Some("stable_fp")).unwrap();
    assert_eq!(t1, t2, "same fp must yield the same key (sticky)");
}

#[test]
fn affinity_key_without_fp_is_unchanged() {
    // When fp is absent the key must be byte-identical to the legacy
    // header-only form — no mass rehash of existing header-identity traffic.
    let ip: IpAddr = "10.88.0.1".parse().unwrap();
    let rctx = rctx_for("claude:fish", "-", "5f4a96c6");
    assert_eq!(
        rctx.affinity_key(&ip, None).unwrap(),
        "10.88.0.1:claude:fish:-:5f4a96c6"
    );
}

#[test]
fn affinity_key_fp_alone_provides_identity() {
    // Headerless one-shot (no client/agent/session) still gets a key from fp,
    // so it distributes deterministically instead of falling to round-robin.
    let ip: IpAddr = "10.88.0.1".parse().unwrap();
    let rctx = rctx_for("-", "-", "-");
    assert!(
        rctx.affinity_key(&ip, None).is_none(),
        "no identity and no fp → None"
    );
    assert!(
        rctx.affinity_key(&ip, Some("fp_x")).is_some(),
        "fp alone must provide an affinity key"
    );
}
