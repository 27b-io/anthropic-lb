use super::*;

/// `test_state_with(vec![])` with a session-registry cap override (0 = off).
/// Registry unit tests don't route, so no endpoints are needed.
fn test_state_with_session_max(max: usize) -> Arc<AppState> {
    let mut state = test_state_with(vec![]);
    Arc::get_mut(&mut state)
        .expect("test fixture should be uniquely owned")
        .session_registry_max = max;
    state
}

// ── LAB-916: session registry + context-window visibility ───────────

#[test]
fn session_registry_populates_updates_and_caps() {
    let state = test_state_with_session_max(3);
    let rctx = ("cli", "agent-1", "sess-1");
    for (i, key) in ["k1", "k2", "k3"].iter().enumerate() {
        state.record_session(
            key,
            rctx,
            "claude-sonnet-4-5",
            "acct-a",
            1000,
            200_000,
            100 + i as u64,
        );
    }
    // Repeat request on a known key updates in place — no new entry.
    state.record_session(
        "k2",
        rctx,
        "claude-sonnet-4-5",
        "acct-b",
        5000,
        200_000,
        200,
    );
    {
        let map = state.sessions.lock().unwrap();
        assert_eq!(map.len(), 3);
        let e = &map["k2"];
        assert_eq!(e.requests, 2);
        assert_eq!(
            e.last_prompt_tokens, 5000,
            "occupancy tracks the LAST response"
        );
        assert_eq!(e.endpoint, "acct-b", "re-pin updates the endpoint");
        assert_eq!(e.client_id, "cli");
        assert_eq!(e.session_id, "sess-1");
    }
    // A 4th distinct key at the cap evicts the oldest entry (k1, last_seen=100).
    state.record_session("k4", rctx, "claude-sonnet-4-5", "acct-a", 1, 200_000, 300);
    let map = state.sessions.lock().unwrap();
    assert_eq!(map.len(), 3, "cap holds");
    assert!(!map.contains_key("k1"), "oldest entry evicted");
    assert!(map.contains_key("k4"));
}

#[test]
fn session_registry_evicts_by_ttl() {
    let state = test_state_with(vec![]); // defaults: max 1000, ttl 1800s
    let rctx = ("cli", "-", "-");
    state.record_session("old", rctx, "m", "a", 1, 200_000, 1_000);
    // A NEW key arriving past the TTL horizon prunes expired entries.
    state.record_session("new", rctx, "m", "a", 1, 200_000, 1_000 + 1801);
    {
        let map = state.sessions.lock().unwrap();
        assert!(
            !map.contains_key("old"),
            "expired entry pruned on new-key insert"
        );
        assert!(map.contains_key("new"));
    }
    // The /_stats snapshot ALSO filters expired entries before any prune runs.
    assert!(state.sessions_snapshot(1_000 + 1801 + 1801).is_empty());
}

#[test]
fn session_registry_disabled_when_max_zero() {
    let state = test_state_with_session_max(0);
    state.record_session("k", ("c", "-", "-"), "m", "a", 1, 200_000, 1);
    assert!(state.sessions.lock().unwrap().is_empty());
}

#[test]
fn session_tokens_histogram_cumulative_buckets_and_ttl() {
    let state = test_state_with(vec![]);
    let rctx = ("c", "-", "-");
    // One session per interesting band: below the first boundary, exactly ON
    // a boundary (le is inclusive), in the 200k danger zone, and over-window.
    state.record_session("tiny", rctx, "m", "a", 9_000, 200_000, 100);
    state.record_session("edge", rctx, "m", "a", 175_000, 200_000, 100);
    state.record_session("hot", rctx, "m", "a", 190_000, 200_000, 100);
    state.record_session("over", rctx, "m", "a", 324_667, 200_000, 100);
    // Expired entry must not count.
    state.record_session("stale", rctx, "m", "a", 50_000, 200_000, 100);
    state
        .sessions
        .lock()
        .unwrap()
        .get_mut("stale")
        .unwrap()
        .last_seen = 0;

    // now=1900: live entries are exactly at the TTL horizon (1800s, inclusive);
    // stale (last_seen=0, age 1900) is past it.
    let (cum, sum) = state.session_tokens_histogram(1_900);
    let le = |bound: u64| {
        cum[SESSION_TOKENS_BUCKETS
            .iter()
            .position(|b| *b == bound)
            .unwrap()]
    };
    assert_eq!(le(10_000), 1, "tiny only");
    assert_eq!(
        le(150_000),
        1,
        "nothing between 10k and 150k (stale expired)"
    );
    assert_eq!(le(175_000), 2, "boundary value is inclusive");
    assert_eq!(le(200_000), 3);
    assert_eq!(le(300_000), 3, "over-window session past 300k");
    assert_eq!(le(1_000_000), 4);
    assert_eq!(cum[SESSION_TOKENS_BUCKETS.len()], 4, "+Inf == live count");
    assert_eq!(sum, 9_000 + 175_000 + 190_000 + 324_667);

    // Empty registry: all zeros, not an error.
    let empty = test_state_with(vec![]);
    let (cum, sum) = empty.session_tokens_histogram(100);
    assert!(cum.iter().all(|c| *c == 0));
    assert_eq!(sum, 0);
}

#[test]
fn sessions_snapshot_sorts_by_pct_desc_and_caps_top_n() {
    let state = test_state_with(vec![]);
    let rctx = ("c", "-", "-");
    state.record_session("low", rctx, "m", "a", 10_000, 200_000, 100);
    state.record_session("high", rctx, "m", "a", 190_000, 200_000, 100);
    state.record_session("mid", rctx, "m", "a", 100_000, 200_000, 100);
    let pcts: Vec<f64> = state
        .sessions_snapshot(100)
        .iter()
        .map(|s| s["context_window_pct"].as_f64().unwrap())
        .collect();
    assert_eq!(pcts, vec![95.0, 50.0, 5.0], "hottest sessions first");

    let state = test_state_with(vec![]);
    for i in 0..(SESSIONS_STATS_TOP_N + 10) {
        state.record_session(&format!("k{i}"), rctx, "m", "a", i as u64, 200_000, 100);
    }
    assert_eq!(state.sessions_snapshot(100).len(), SESSIONS_STATS_TOP_N);
}

#[test]
fn context_window_mapping() {
    assert_eq!(context_window_for("claude-sonnet-4-5", false), 200_000);
    assert_eq!(context_window_for("claude-sonnet-4-5", true), 1_000_000);
    assert_eq!(
        context_window_for("some-future-model", false),
        200_000,
        "unknown model family falls back to 200k"
    );
    assert_eq!(context_window_for("", false), 200_000);
}

#[test]
fn request_1m_beta_detected_from_comma_joined_header() {
    let mut headers = axum::http::HeaderMap::new();
    headers.insert(
        "anthropic-beta",
        HeaderValue::from_static("oauth-2025-04-20, context-1m-2025-08-07"),
    );
    assert!(request_has_1m_beta(&headers));
    let mut plain = axum::http::HeaderMap::new();
    plain.insert(
        "anthropic-beta",
        HeaderValue::from_static("oauth-2025-04-20"),
    );
    assert!(!request_has_1m_beta(&plain));
    assert!(!request_has_1m_beta(&axum::http::HeaderMap::new()));
}

#[test]
fn window_pct_reports_past_100() {
    assert_eq!(window_pct(100_000, 200_000), 50.0);
    assert_eq!(
        window_pct(250_000, 200_000),
        125.0,
        "over-window sessions must report >100% — that IS the signal"
    );
    assert_eq!(window_pct(1, 0), 0.0);
}

#[test]
fn prompt_too_long_shape_detection_and_parse() {
    let body = serde_json::json!({
        "type": "error",
        "error": {
            "type": "invalid_request_error",
            "message": "prompt is too long: 213462 tokens > 200000 maximum"
        }
    });
    let msg = prompt_too_long_message(&body).expect("matches the prompt-too-long shape");
    assert_eq!(parse_prompt_too_long(msg), Some((213_462, 200_000)));

    let other_400 = serde_json::json!({
        "type": "error",
        "error": {"type": "invalid_request_error", "message": "max_tokens: field required"}
    });
    assert!(prompt_too_long_message(&other_400).is_none());

    let wrong_type = serde_json::json!({
        "type": "error",
        "error": {"type": "authentication_error", "message": "prompt is too long: 1 tokens > 0 maximum"}
    });
    assert!(
        prompt_too_long_message(&wrong_type).is_none(),
        "only invalid_request_error counts"
    );
    assert_eq!(parse_prompt_too_long("prompt is too long"), None);
}

#[test]
fn prompt_too_long_counter_bounds_model_cardinality() {
    let state = test_state_with(vec![]);
    for i in 0..(MAX_PROMPT_TOO_LONG_MODELS + 5) {
        state.note_prompt_too_long(
            "req",
            &format!("model-{i}"),
            None,
            "prompt is too long: 5 tokens > 1 maximum",
        );
    }
    let counts = state.prompt_too_long.lock().unwrap();
    assert_eq!(
        counts.len(),
        MAX_PROMPT_TOO_LONG_MODELS + 1,
        "distinct models capped, overflow buckets into _other"
    );
    assert_eq!(counts.get("_other"), Some(&5));
}

/// Upstream that answers every request with the canned 400.
async fn spawn_prompt_too_long_upstream() -> String {
    spawn_400_upstream(PROMPT_TOO_LONG_BODY).await.0
}

#[tokio::test]
async fn prompt_too_long_400_counted_logged_and_forwarded_unchanged() {
    let url = spawn_prompt_too_long_upstream().await;
    let state = test_state_with(vec![mk_endpoint_at("a", "sk-ant-api-aaa", &url)]);
    let addr = serve(build_router(state.clone())).await;

    let resp = reqwest::Client::new()
        .post(format!("http://{addr}/v1/messages"))
        .header("content-type", "application/json")
        .header("x-claude-code-session-id", "sess-92af31")
        .body(r#"{"model":"claude-sonnet-4-5","max_tokens":1,"messages":[{"role":"user","content":"hi"}]}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::BAD_REQUEST,
        "the 400 must pass through — client errors are not retried"
    );
    let body = resp.bytes().await.unwrap();
    assert_eq!(
        &body[..],
        PROMPT_TOO_LONG_BODY,
        "error body forwarded unchanged"
    );
    assert_eq!(
        state
            .prompt_too_long
            .lock()
            .unwrap()
            .get("claude-sonnet-4-5"),
        Some(&1)
    );

    // And the counter reaches /metrics with the model label.
    let metrics = reqwest::Client::new()
        .get(format!("http://{addr}/metrics"))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(
        metrics.contains(r#"anthropic_prompt_too_long_total{model="claude-sonnet-4-5"} 1"#),
        "metric missing from /metrics: {metrics}"
    );
}

/// 200 upstream body carrying usage — populates the session registry.
/// Prompt occupancy = 150k input + 20k cache_read + 10k cache_creation = 180k (90%).
const ANTHROPIC_USAGE_BODY: &[u8] = br#"{"id":"msg_1","type":"message","role":"assistant","content":[{"type":"text","text":"hi"}],"model":"claude-sonnet-4-5","stop_reason":"end_turn","usage":{"input_tokens":150000,"output_tokens":10,"cache_creation_input_tokens":10000,"cache_read_input_tokens":20000}}"#;

#[tokio::test]
async fn stats_sessions_block_lists_and_redacts() {
    let (url, _hits) = spawn_flaky_upstream(0, ANTHROPIC_USAGE_BODY).await;
    let state = test_state_with(vec![mk_endpoint_at("acct-a", "sk-ant-api-aaa", &url)]);
    let addr = serve(build_router(state.clone())).await;

    let raw_session_id = "0b5e7c1e-secret-session-uuid";
    let resp = reqwest::Client::new()
        .post(format!("http://{addr}/v1/messages"))
        .header("content-type", "application/json")
        .header("x-client-id", "geo-pipeline")
        .header("x-agent-id", "agent-77")
        .header("x-claude-code-session-id", raw_session_id)
        .body(r#"{"model":"claude-sonnet-4-5","max_tokens":1,"messages":[{"role":"user","content":"hi"}]}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    let stats: serde_json::Value = reqwest::Client::new()
        .get(format!("http://{addr}/_stats"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let sessions = stats["sessions"].as_array().unwrap();
    assert_eq!(sessions.len(), 1);
    let s = &sessions[0];
    assert_eq!(s["client_id"], "geo-pipeline");
    assert_eq!(s["model"], "claude-sonnet-4-5");
    assert_eq!(s["endpoint"], "acct-a");
    assert_eq!(
        s["last_prompt_tokens"], 180_000,
        "input + cache_read + cache_creation, NOT output"
    );
    assert_eq!(s["context_window"], 200_000);
    assert_eq!(s["context_window_pct"], 90.0);
    assert_eq!(s["requests"], 1);
    assert_eq!(s["agent"], "agent-77");
    assert_eq!(
        s["session_prefix"], "0b5e7c1e",
        "session id truncated to 8 chars"
    );

    let label = s["session"].as_str().unwrap();
    assert_eq!(
        label.len(),
        16,
        "label is a 16-hex-char hash of the affinity key"
    );
    assert!(label.chars().all(|c| c.is_ascii_hexdigit()));

    // AC3 redaction: no raw client IP or raw session id in the block.
    let text = serde_json::to_string(&stats["sessions"]).unwrap();
    assert!(
        !text.contains(raw_session_id),
        "raw session id must not leak"
    );
    assert!(!text.contains("127.0.0.1"), "raw client IP must not leak");
}
