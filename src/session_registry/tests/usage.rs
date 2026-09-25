use super::*;

#[tokio::test]
async fn record_usage_updates_account_and_client() {
    let state = test_state_with(vec![mk_endpoint("a", "sk-ant-api-x")]);
    let usage = TokenUsage {
        input_tokens: 100,
        output_tokens: 50,
        cache_creation_input_tokens: 20,
        cache_read_input_tokens: 30,
    };
    state
        .record_usage(
            &state.endpoints[0],
            "test-client",
            "claude-sonnet-5",
            &usage,
        )
        .await;

    assert_eq!(state.endpoints[0].input_tokens.load(Ordering::Relaxed), 100);
    assert_eq!(state.endpoints[0].output_tokens.load(Ordering::Relaxed), 50);
    assert_eq!(
        state.endpoints[0]
            .cache_creation_tokens
            .load(Ordering::Relaxed),
        20
    );
    assert_eq!(
        state.endpoints[0].cache_read_tokens.load(Ordering::Relaxed),
        30
    );

    let map = state.client_usage.lock().unwrap();
    let client = map.get("test-client").unwrap();
    assert_eq!(client, &[100, 50, 20, 30]);
}

#[tokio::test]
async fn record_usage_ignores_anonymous() {
    let state = test_state_with(vec![mk_endpoint("a", "sk-ant-api-x")]);
    let usage = TokenUsage {
        input_tokens: 100,
        output_tokens: 50,
        cache_creation_input_tokens: 0,
        cache_read_input_tokens: 0,
    };
    state
        .record_usage(&state.endpoints[0], "-", "claude-sonnet-5", &usage)
        .await;

    // Account gets updated
    assert_eq!(state.endpoints[0].input_tokens.load(Ordering::Relaxed), 100);
    // But no client entry for anonymous
    let map = state.client_usage.lock().unwrap();
    assert!(!map.contains_key("-"));
}

// ── Memory hardening: per-client maps must be bounded ──────────
// These maps are keyed by the user-controlled x-client-id header with no
// eviction; an unbounded set of distinct values would grow them without
// limit (a memory-DoS vector — anthropic-lb#73 audit). Bound new-key inserts.

#[test]
fn client_request_rates_is_bounded() {
    let state = test_state_with(vec![mk_endpoint("a", "sk-ant-api-x")]);
    let br = std::sync::Mutex::new(BurnRate::new());
    for i in 0..10_050 {
        state.update_burn_rate(&br, &format!("c{i}"));
    }
    let n = state.client_request_rates.lock().unwrap().len();
    assert!(
        n <= 10_000,
        "client_request_rates must be bounded against unbounded x-client-id values, got {n}"
    );
}

#[tokio::test]
async fn client_usage_is_bounded() {
    let state = test_state_with(vec![mk_endpoint("a", "sk-ant-api-x")]);
    let usage = TokenUsage {
        input_tokens: 1,
        output_tokens: 1,
        cache_creation_input_tokens: 0,
        cache_read_input_tokens: 0,
    };
    for i in 0..10_050 {
        state
            .record_usage(
                &state.endpoints[0],
                &format!("c{i}"),
                "claude-sonnet-5",
                &usage,
            )
            .await;
    }
    let n = state.client_usage.lock().unwrap().len();
    assert!(
        n <= 10_000,
        "client_usage must be bounded against unbounded x-client-id values, got {n}"
    );
}

// ── LAB-2330: per-(client, model) usage accounting ─────────────

#[tokio::test]
async fn record_usage_tracks_per_model() {
    let state = test_state_with(vec![mk_endpoint("a", "sk-ant-api-x")]);
    let usage = TokenUsage {
        input_tokens: 10,
        output_tokens: 5,
        cache_creation_input_tokens: 2,
        cache_read_input_tokens: 3,
    };
    for model in ["claude-sonnet-5", "claude-sonnet-5", "claude-haiku-4-5"] {
        state
            .record_usage(&state.endpoints[0], "c1", model, &usage)
            .await;
    }

    let map = state.client_model_usage.lock().unwrap();
    assert_eq!(
        map.get(&("c1".to_string(), "claude-sonnet-5".to_string())),
        Some(&[20, 10, 4, 6])
    );
    assert_eq!(
        map.get(&("c1".to_string(), "claude-haiku-4-5".to_string())),
        Some(&[10, 5, 2, 3])
    );
    // The per-client family stays the authoritative total across models.
    assert_eq!(
        state.client_usage.lock().unwrap().get("c1"),
        Some(&[30, 15, 6, 9])
    );
}

#[tokio::test]
async fn record_usage_empty_model_records_unknown() {
    let state = test_state_with(vec![mk_endpoint("a", "sk-ant-api-x")]);
    let usage = TokenUsage {
        input_tokens: 1,
        output_tokens: 1,
        cache_creation_input_tokens: 0,
        cache_read_input_tokens: 0,
    };
    state
        .record_usage(&state.endpoints[0], "c1", "", &usage)
        .await;
    let map = state.client_model_usage.lock().unwrap();
    assert!(map.contains_key(&("c1".to_string(), "unknown".to_string())));
}

#[tokio::test]
async fn record_usage_truncates_model_label() {
    let state = test_state_with(vec![mk_endpoint("a", "sk-ant-api-x")]);
    let usage = TokenUsage {
        input_tokens: 1,
        output_tokens: 0,
        cache_creation_input_tokens: 0,
        cache_read_input_tokens: 0,
    };
    let huge = "m".repeat(500);
    state
        .record_usage(&state.endpoints[0], "c1", &huge, &usage)
        .await;
    // The stored key must have gone through truncate_label (whose exact
    // format is covered by its own unit tests).
    let map = state.client_model_usage.lock().unwrap();
    let (_, model) = map.keys().next().unwrap();
    assert!(
        model.chars().count() < huge.chars().count(),
        "model label must be truncated before becoming a map key"
    );
}

#[tokio::test]
async fn client_model_usage_is_bounded() {
    let state = test_state_with(vec![mk_endpoint("a", "sk-ant-api-x")]);
    let usage = TokenUsage {
        input_tokens: 1,
        output_tokens: 0,
        cache_creation_input_tokens: 0,
        cache_read_input_tokens: 0,
    };
    for i in 0..(MAX_CLIENT_MODEL_LABELS + 50) {
        state
            .record_usage(&state.endpoints[0], "c1", &format!("model-{i}"), &usage)
            .await;
    }
    // Expert-panel finding (LAB-2330): rotating the caller-controlled client
    // id past the cap must NOT mint per-client overflow keys — the bound has
    // to hold on the client axis too.
    for i in 0..50 {
        state
            .record_usage(
                &state.endpoints[0],
                &format!("evil-{i}"),
                "claude-x",
                &usage,
            )
            .await;
    }
    let map = state.client_model_usage.lock().unwrap();
    assert!(
        map.len() <= MAX_CLIENT_MODEL_LABELS + 1,
        "client_model_usage must be hard-bounded, got {}",
        map.len()
    );
    // Overflow tokens are not dropped — they land in the global bucket:
    // 50 c1 overflow models + 50 rotated clients, 1 input token each.
    let other = map
        .get(&("_other".to_string(), "_other".to_string()))
        .unwrap();
    assert_eq!(other[0], 100);
}

/// Regression guard for the `proxied`+`usage` merge (LAB-3214): at the
/// production default filter (`anthropic_lb=info`), a successful
/// non-streaming `/v1/messages` request must produce exactly one INFO line
/// for that request, carrying both routing context and token usage, and no
/// `fingerprint` detail (that's DEBUG-only). Filters the shared capture
/// buffer by a client-id marker unique to this test so concurrently running
/// tests' own log lines can't be mistaken for this request's.
#[tokio::test]
async fn single_info_line_per_proxied_request() {
    let buf = log_capture_buf();

    // Mock upstream that returns usage (mock_anthropic_handler carries
    // `"usage": {"input_tokens": 10, "output_tokens": 5}`), so the merged
    // line's token fields are exercised, not just left at zero.
    let mock_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mock_addr = mock_listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(
            mock_listener,
            Router::new().fallback(any(mock_anthropic_handler)),
        )
        .await
        .unwrap();
    });

    let (app, _state) = test_app(&format!("http://{mock_addr}"), None);
    let app_addr = serve(app).await;

    let marker = "lab3214-single-info-line-marker";
    let client = reqwest::Client::new();
    let resp = client
        .post(format!("http://{app_addr}/v1/messages"))
        .header("content-type", "application/json")
        .header("x-api-key", "any")
        .header("x-client-id", marker)
        .body(r#"{"model":"claude-sonnet-4-6","messages":[{"role":"user","content":"hi"}]}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let output = String::from_utf8(buf.lock().unwrap().clone()).unwrap();
    let my_lines: Vec<&str> = output.lines().filter(|l| l.contains(marker)).collect();
    assert_eq!(
        my_lines.len(),
        1,
        "expected exactly one log line for this request, got:\n{}",
        my_lines.join("\n")
    );
    assert!(
        my_lines[0].contains(" INFO ") && my_lines[0].contains("proxied"),
        "the single line should be the merged INFO `proxied` line, got: {}",
        my_lines[0]
    );
    assert!(
        my_lines[0].contains("input=10") && my_lines[0].contains("output=5"),
        "merged line should carry token usage, got: {}",
        my_lines[0]
    );
    assert!(
        my_lines[0].contains("fp="),
        "merged line should carry the fp content-fingerprint field, got: {}",
        my_lines[0]
    );
    assert!(
        !my_lines[0].contains("fingerprint"),
        "fingerprint detail must not appear for this request at the default (INFO) filter, got: {}",
        my_lines[0]
    );
}

/// Regression guard for a gap the LAB-3214 merge introduced and then fixed:
/// `forward_openai_compat_anthropic` returns early on a non-2xx upstream
/// status, before ever reaching `finalize_non_stream` — the merged `proxied
/// (openai-compat)` line must still be logged from that early-return branch
/// (previously it fired unconditionally, before the status check even ran).
#[tokio::test]
async fn proxied_line_still_logged_on_openai_compat_upstream_error() {
    let buf = log_capture_buf();

    async fn mock_400(_req: Request<Body>) -> Response {
        (
            StatusCode::BAD_REQUEST,
            axum::Json(serde_json::json!({
                "type": "error",
                "error": {"type": "invalid_request_error", "message": "bad request"}
            })),
        )
            .into_response()
    }

    let mock_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mock_addr = mock_listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(mock_listener, Router::new().fallback(any(mock_400)))
            .await
            .unwrap();
    });

    let (app, _state) = test_openai_app(&format!("http://{mock_addr}"), None);
    let app_addr = serve(app).await;

    let marker = "lab3214-compat-error-marker";
    let client = reqwest::Client::new();
    let resp = client
        .post(format!("http://{app_addr}/v1/chat/completions"))
        .header("content-type", "application/json")
        .header("x-api-key", "any")
        .header("x-client-id", marker)
        .body(r#"{"model":"claude-sonnet-4-6","messages":[{"role":"user","content":"hi"}]}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400);

    let output = String::from_utf8(buf.lock().unwrap().clone()).unwrap();
    let my_lines: Vec<&str> = output.lines().filter(|l| l.contains(marker)).collect();
    assert_eq!(
        my_lines.len(),
        1,
        "expected exactly one log line for this errored request, got:\n{}",
        my_lines.join("\n")
    );
    assert!(
        my_lines[0].contains(" INFO ") && my_lines[0].contains("proxied (openai-compat)"),
        "the errored request must still get the merged INFO proxied line, got: {}",
        my_lines[0]
    );
    assert!(
        my_lines[0].contains("status=400"),
        "merged line should carry the upstream status, got: {}",
        my_lines[0]
    );
}
