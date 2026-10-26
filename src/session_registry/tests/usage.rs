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
            &state.caller("test-client"),
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
        .record_usage(
            &state.endpoints[0],
            &state.caller("-"),
            "claude-sonnet-5",
            &usage,
        )
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
                &state.caller(&format!("c{i}")),
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
            .record_usage(&state.endpoints[0], &state.caller("c1"), model, &usage)
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
        .record_usage(&state.endpoints[0], &state.caller("c1"), "", &usage)
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
        .record_usage(&state.endpoints[0], &state.caller("c1"), &huge, &usage)
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
            .record_usage(
                &state.endpoints[0],
                &state.caller("c1"),
                &format!("model-{i}"),
                &usage,
            )
            .await;
    }
    // LAB-2330: rotating the caller-controlled client
    // id past the cap must NOT mint per-client overflow keys — the bound has
    // to hold on the client axis too.
    for i in 0..50 {
        state
            .record_usage(
                &state.endpoints[0],
                &state.caller(&format!("evil-{i}")),
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

// ── LAB-3214: one INFO line per proxied request ─────────────────

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

/// The `proxied` line carries Claude Code's gateway hint headers, so cache
/// analysis can split compaction and subagent calls from main-loop ones. A
/// request without them logs `-` for each.
#[tokio::test]
async fn proxied_line_carries_gateway_hint_headers() {
    let buf = log_capture_buf();

    let mock_addr = serve(Router::new().fallback(any(mock_anthropic_handler))).await;
    let (app, _state) = test_app(&format!("http://{mock_addr}"), None);
    let app_addr = serve(app).await;
    let client = reqwest::Client::new();
    let body = r#"{"model":"claude-sonnet-4-6","messages":[{"role":"user","content":"hi"}]}"#;

    let hinted = "gateway-hints-marker-hinted";
    let resp = client
        .post(format!("http://{app_addr}/v1/messages"))
        .header("content-type", "application/json")
        .header("x-api-key", "any")
        .header("x-client-id", hinted)
        .header("x-claude-code-request-class", "subagent")
        .header("x-claude-code-agent-type", "general-purpose")
        .header("x-claude-code-compaction", "auto")
        .header("x-claude-code-context-compacted", "1")
        .header("x-claude-code-prompt-id", "p-123")
        .body(body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let bare = "gateway-hints-marker-bare";
    let resp = client
        .post(format!("http://{app_addr}/v1/messages"))
        .header("content-type", "application/json")
        .header("x-api-key", "any")
        .header("x-client-id", bare)
        .body(body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let output = String::from_utf8(buf.lock().unwrap().clone()).unwrap();
    let line = |marker: &str| {
        output
            .lines()
            .find(|l| l.contains(marker) && l.contains("proxied"))
            .unwrap_or_else(|| panic!("no proxied line for {marker}:\n{output}"))
            .to_string()
    };
    let hinted_line = line(hinted);
    for field in [
        r#"request_class="subagent""#,
        r#"agent_type="general-purpose""#,
        r#"compaction="auto""#,
        r#"context_compacted="1""#,
        r#"prompt_id="p-123""#,
    ] {
        assert!(
            hinted_line.contains(field),
            "missing {field} in: {hinted_line}"
        );
    }
    let bare_line = line(bare);
    assert!(
        bare_line.contains(r#"request_class="-""#) && bare_line.contains(r#"prompt_id="-""#),
        "absent hints should log -, got: {bare_line}"
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
    assert!(
        my_lines[0].contains(r#"err_type="invalid_request_error""#)
            && my_lines[0].contains(r#"err="bad request""#),
        "the line should name the upstream's error type and message, got: {}",
        my_lines[0]
    );
}

// ── LAB-11037, LAB-11064: a caller-fed log field cannot forge another field ──

/// `unpack_logfmt` reads a line the way VictoriaLogs does, so it reproduces
/// the forge Debug quoting allowed: Go rejects `\u{1}`, the value is read
/// unquoted to the next space, and the rest parses as a second `client_id`.
/// `strict` refuses the same line.
#[test]
fn unpack_logfmt_reads_a_debug_escape_as_that_reader_does() {
    let line = r#"INFO proxied client_id="real-tenant" model="claude-x\u{1} client_id=forged " account="a" status=404"#;
    let fields = unpack_logfmt(line, false);
    assert_eq!(
        field_values(&fields, "client_id"),
        ["real-tenant", "forged"]
    );
    assert_eq!(field_values(&fields, "model"), [r#""claude-x\u{1}"#]);
    assert_eq!(field_values(&fields, "status"), ["404"]);
    let strict = std::panic::catch_unwind(|| unpack_logfmt(line, true));
    assert!(strict.is_err(), "strict must refuse a quote Go cannot read");

    // A bare word after the message is an empty field, which blanks the
    // earlier `client_id` under last-wins.
    let line = r#"INFO proxied client_id="t" model="m" status=200 client_id"#;
    let fields = unpack_logfmt(line, true);
    assert_eq!(field_values(&fields, "client_id"), ["t", ""]);
    assert_eq!(field_values(&fields, "proxied"), Vec::<&str>::new());

    let line = r#"proxied model="claude-x\u0001 client_id=forged \"q\" \\" account="a""#;
    let fields = unpack_logfmt(line, true);
    assert_eq!(field_values(&fields, "client_id"), Vec::<&str>::new());
    assert_eq!(
        field_values(&fields, "model"),
        ["claude-x\u{1} client_id=forged \"q\" \\"]
    );
    assert_eq!(field_values(&fields, "account"), ["a"]);
}

/// In legacy mode `client_id` is the caller's own `x-client-id`, and `ver`,
/// `agent` and `session` come from request headers in every mode. A value
/// carrying ` client_id=` or a `"` must stay inside its own quoted field, on
/// the `proxied` line and on a rejection line of both surfaces. Otherwise a
/// last-field-wins logfmt reader attributes the line to the forged identity.
#[tokio::test]
async fn header_fed_log_fields_cannot_forge_another_field() {
    let buf = log_capture_buf();
    let upstream = format!(
        "http://{}",
        serve(Router::new().fallback(any(mock_anthropic_handler))).await
    );
    let native = serve(test_app(&upstream, None).0).await;
    let openai = serve(test_openai_app(&upstream, None).0).await;
    let ok_body = r#"{"model":"claude-sonnet-4-6","messages":[{"role":"user","content":"hi"}]}"#;
    let client = reqwest::Client::new();

    for (addr, path, proxied, rejected) in [
        (
            native,
            "/v1/messages",
            "proxied",
            "rejected: request body could not be parsed as JSON",
        ),
        (
            openai,
            "/v1/chat/completions",
            "proxied (openai-compat)",
            "rejected: request body is not valid JSON",
        ),
    ] {
        for (body, message, status) in [(ok_body, proxied, 200), ("{not json", rejected, 400)] {
            let marker = format!("lab11037{}-{status}", path.replace('/', "-"));
            let client_id = format!("{marker} client_id=forged");
            let resp = client
                .post(format!("http://{addr}{path}"))
                .header("content-type", "application/json")
                .header("x-api-key", "any")
                .header("x-client-id", &client_id)
                .header("user-agent", r#"claude-cli/"ver=forged"#)
                .header("x-agent-id", "a agent=forged")
                .header("x-session-id", "s session=forged")
                .body(body)
                .send()
                .await
                .unwrap();
            assert_eq!(resp.status().as_u16(), status, "{marker}");

            let output = String::from_utf8(buf.lock().unwrap().clone()).unwrap();
            let lines: Vec<&str> = output
                .lines()
                .filter(|l| l.contains(&marker) && l.contains(message))
                .collect();
            assert_eq!(
                lines.len(),
                1,
                "one `{message}` line for {marker}:\n{output}"
            );
            let fields = unpack_logfmt(lines[0], true);
            assert_eq!(
                field_values(&fields, "client_id"),
                [client_id.as_str()],
                "one client_id on: {}",
                lines[0]
            );
            if status == 200 {
                for (key, sent) in [
                    ("ver", r#""ver=forged"#),
                    ("agent", "a agent=forged"),
                    ("session", "s session=forged"),
                ] {
                    assert_eq!(
                        field_values(&fields, key),
                        [sent],
                        "one {key} on: {}",
                        lines[0]
                    );
                }
            }
        }
    }
}

/// An upstream's SSE `event:` names are upstream text. The
/// `stream_end_no_usage` line carries them as one quoted value, so a name
/// holding ` client_id=forged` reads back inside it and adds no field.
#[tokio::test]
async fn sse_event_names_cannot_forge_another_field() {
    let buf = log_capture_buf();
    let event = "x client_id=forged y";
    let body = format!(
        "event: {event}\ndata: {{}}\n\nevent: message_stop\ndata: {{\"type\":\"message_stop\"}}\n\n"
    );
    let upstream = Router::new().fallback(any(move || {
        let body = body.clone();
        async move { ([("content-type", "text/event-stream")], body) }
    }));
    let upstream = format!("http://{}", serve(upstream).await);
    let addr = serve(test_app(&upstream, None).0).await;
    let marker = "sse-event-name-forge";
    let resp = reqwest::Client::new()
        .post(format!("http://{addr}/v1/messages"))
        .header("x-api-key", "any")
        .header("x-client-id", marker)
        .json(&serde_json::json!({
            "model": "claude-sonnet-4-6",
            "max_tokens": 1,
            "stream": true,
            "messages": [{"role": "user", "content": "hi"}],
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    resp.bytes().await.unwrap();

    // The stream is finalized on a detached task, after the client's end of
    // stream.
    let mut line = None;
    eventually("a stream_end_no_usage line", Duration::from_secs(5), || {
        let output = String::from_utf8(buf.lock().unwrap().clone()).unwrap();
        line = output
            .lines()
            .find(|l| l.contains(marker) && l.contains("stream_end_no_usage"))
            .map(str::to_owned);
        line.is_some()
    })
    .await;
    let line = line.unwrap();
    let fields = unpack_logfmt(&line, true);
    let sent = format!("{event},message_stop");
    assert_eq!(
        field_values(&fields, "sse_events"),
        [sent.as_str()],
        "{line}"
    );
    assert_eq!(field_values(&fields, "client_id"), [marker], "{line}");
}

/// An upstream that answers every request as Anthropic answers an unknown
/// model: a 404 whose message names the request body's own `model`.
async fn model_not_found_upstream() -> String {
    let upstream = Router::new().fallback(any(|body: axum::body::Bytes| async move {
        let request: serde_json::Value = serde_json::from_slice(&body).unwrap_or_default();
        let model = request["model"].as_str().unwrap_or_default();
        let message = format!("model: {model}");
        (
            StatusCode::NOT_FOUND,
            axum::Json(anthropic_error("not_found_error", &message)),
        )
            .into_response()
    }));
    format!("http://{}", serve(upstream).await)
}

/// A `model` from the request body can hold any Unicode. One holding a
/// control (U+0001, U+0085) or format (U+00AD) character before
/// ` client_id=forged` must not put a second `client_id` on any line the
/// request writes, read the way a Go-strconv logfmt reader reads it: the
/// `proxied` line of both surfaces, whose `err` echoes the model as an
/// upstream 404 does, and the exhaustion warn. `model` and `err` read back
/// whole.
#[tokio::test]
async fn body_fed_log_fields_cannot_forge_another_field() {
    let buf = log_capture_buf();
    let upstream = model_not_found_upstream().await;
    let native = serve(test_app(&upstream, None).0).await;
    let openai = serve(test_openai_app(&upstream, None).0).await;
    let client = reqwest::Client::new();

    for (addr, path) in [(native, "/v1/messages"), (openai, "/v1/chat/completions")] {
        for (tag, c) in [("c0", '\u{1}'), ("c1", '\u{85}'), ("cf", '\u{ad}')] {
            let marker = format!("lab11064{}-{tag}", path.replace('/', "-"));
            // The marker rides in the model too, so a line that logs the
            // model but no client_id is checked as well.
            let model = format!("claude-{marker}{c} client_id=forged ");
            let body = serde_json::json!({
                "model": model,
                "max_tokens": 1,
                "messages": [{"role": "user", "content": "hi"}],
            });
            let resp = client
                .post(format!("http://{addr}{path}"))
                .header("x-api-key", "any")
                .header("x-client-id", &marker)
                .json(&body)
                .send()
                .await
                .unwrap();
            assert!(!resp.status().is_success(), "{marker}: {}", resp.status());

            let output = String::from_utf8(buf.lock().unwrap().clone()).unwrap();
            let lines: Vec<&str> = output.lines().filter(|l| l.contains(&marker)).collect();
            assert!(
                lines.iter().any(|l| l.contains("proxied")),
                "a proxied line for {marker}:\n{output}"
            );
            // `err` on the `proxied` line and `error_message` carry it as sent.
            let err = format!("model: {model}");
            for line in lines {
                let fields = unpack_logfmt(line, true);
                let client_ids = field_values(&fields, "client_id");
                assert!(
                    client_ids.is_empty() || client_ids == [marker.as_str()],
                    "no forged client_id on: {line}"
                );
                for (key, sent) in [
                    ("model", model.as_str()),
                    ("err", err.as_str()),
                    ("error_message", err.as_str()),
                ] {
                    let values = field_values(&fields, key);
                    assert!(
                        values.is_empty() || values == [sent],
                        "{key} reads back whole, got {values:?}, sent {sent:?}, on: {line}"
                    );
                }
                if line.contains("proxied") {
                    assert_eq!(client_ids, [marker.as_str()], "{line}");
                    assert_eq!(field_values(&fields, "model"), [model.as_str()], "{line}");
                    assert_eq!(field_values(&fields, "err"), [err.as_str()], "{line}");
                }
            }
        }
    }
}

/// A `model` of 40 KiB is cut to `MAX_LOGGED_VALUE_CHARS` on every line, so it
/// cannot carry the `proxied` line past the log sink's `MAX_LINE_BYTES`, where
/// the sink would cut the fields after it, the token counts among them.
#[tokio::test]
async fn a_long_body_fed_value_cannot_push_a_line_past_the_sink_cut() {
    let buf = log_capture_buf();
    let upstream = model_not_found_upstream().await;
    let addr = serve(test_app(&upstream, None).0).await;
    let marker = "lab11064-long-model";
    let model = format!(
        "claude-{marker} {}\u{1} client_id=forged ",
        "\u{1}".repeat(40 << 10)
    );
    let resp = reqwest::Client::new()
        .post(format!("http://{addr}/v1/messages"))
        .header("x-api-key", "any")
        .header("x-client-id", marker)
        .json(&serde_json::json!({
            "model": model,
            "max_tokens": 1,
            "messages": [{"role": "user", "content": "hi"}],
        }))
        .send()
        .await
        .unwrap();
    assert!(!resp.status().is_success(), "{}", resp.status());

    let output = String::from_utf8(buf.lock().unwrap().clone()).unwrap();
    let lines: Vec<&str> = output.lines().filter(|l| l.contains(marker)).collect();
    assert!(
        lines.iter().any(|l| l.contains("proxied")),
        "a proxied line for {marker}"
    );
    let kept: String = model.chars().take(MAX_LOGGED_VALUE_CHARS).collect();
    let kept = format!("{kept}…");
    for line in lines {
        assert!(line.len() < MAX_LINE_BYTES, "{} bytes", line.len());
        let fields = unpack_logfmt(line, true);
        let client_ids = field_values(&fields, "client_id");
        assert!(
            client_ids.is_empty() || client_ids == [marker],
            "no forged client_id"
        );
        if line.contains("proxied") {
            assert_eq!(field_values(&fields, "model"), [kept.as_str()]);
        }
    }
}

/// Both `proxied` lines with every value at its cap, escaped as wide as it can
/// be: each header-fed value 64 KiB of `"` (two bytes escaped, the widest a
/// header value takes), `model` past `MAX_LOGGED_VALUE_CHARS` and `err_type`
/// and `err` past their caps, all in a character above U+FFFF (ten bytes
/// escaped), and every number at its longest. Each line fits the log sink's
/// `MAX_LINE_BYTES`, so the sink sends it whole, and its token counts read
/// back as given. The header-fed values read back clipped by `truncate_label`.
#[test]
fn a_proxied_line_at_every_cap_fits_the_sink_cut() {
    let buf = log_capture_buf();
    let header = "\"".repeat(64 << 10);
    let wide = |chars: usize| "\u{e0049}".repeat(chars + 1);
    let upstream_err = UpstreamError::from_body(
        400,
        &anthropic_error(&wide(MAX_LABEL_CHARS), &wide(MAX_UPSTREAM_ERR_CHARS)),
    );
    let usage = TokenUsage {
        input_tokens: u64::MAX,
        output_tokens: u64::MAX - 1,
        cache_read_input_tokens: u64::MAX - 2,
        cache_creation_input_tokens: u64::MAX - 3,
    };
    let (utilization, util_5h, util_7d) = ("100.00", "100.00", "100.00");
    let anthropic = ProxiedCtx::Anthropic {
        client_ver: header.clone(),
        utilization: utilization.to_owned(),
        util_5h: util_5h.to_owned(),
        util_7d: util_7d.to_owned(),
        constraint: "7d_sonnet",
        overage: false,
        served_on_overage: false,
        pin: "spilled",
        total: u64::MAX,
        fp: "0123456789ab".to_owned(),
        hints: ClientHints {
            request_class: header.clone(),
            agent_type: header.clone(),
            compaction: header.clone(),
            context_compacted: header.clone(),
            prompt_id: header.clone(),
        },
    };
    let openai = ProxiedCtx::OpenaiCompat {
        client_ver: header.clone(),
        utilization: utilization.to_owned(),
        util_5h: util_5h.to_owned(),
        util_7d: util_7d.to_owned(),
        constraint: "7d_sonnet",
        served_on_overage: false,
        pin: "spilled",
        stream: false,
    };

    for (req_id, ctx) in [
        ("ffff:18446744073709551614", &anthropic),
        ("ffff:18446744073709551615", &openai),
    ] {
        log_proxied(
            req_id,
            &header,
            &wide(MAX_LOGGED_VALUE_CHARS),
            "acct-a",
            "ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff",
            &header,
            &header,
            499,
            ctx,
            &usage,
            upstream_err.as_ref(),
        );
        let output = String::from_utf8(buf.lock().unwrap().clone()).unwrap();
        let line = output
            .lines()
            .find(|l| l.contains(req_id))
            .expect("the proxied line");
        assert!(line.len() <= MAX_LINE_BYTES, "{} bytes", line.len());

        let fields = unpack_logfmt(line, true);
        for (key, sent) in [
            ("input", usage.input_tokens),
            ("output", usage.output_tokens),
            ("cached", usage.cache_read_input_tokens),
            ("cache_write", usage.cache_creation_input_tokens),
        ] {
            assert_eq!(field_values(&fields, key), [sent.to_string()], "{key}");
        }
        let mut header_fed = vec!["client_id", "ver", "agent", "session"];
        if matches!(ctx, ProxiedCtx::Anthropic { .. }) {
            header_fed.extend(["request_class", "agent_type", "compaction"]);
            header_fed.extend(["context_compacted", "prompt_id"]);
        }
        let clipped = truncate_label(&header);
        for key in header_fed {
            assert_eq!(field_values(&fields, key), [clipped.as_str()], "{key}");
        }
    }
}

/// Every header that feeds a `proxied` field, sent at 16 KiB of `"` through
/// the router on both surfaces: well past the log sink's `MAX_LINE_BYTES`
/// together, yet inside hyper's default limit on a request head. The line
/// still fits the cut, and its token counts read back as the upstream reported
/// them.
#[tokio::test]
async fn long_request_headers_cannot_push_a_proxied_line_past_the_sink_cut() {
    let buf = log_capture_buf();
    let upstream = Router::new().fallback(any(|| async {
        axum::Json(serde_json::json!({
            "id": "msg_long_headers",
            "type": "message",
            "role": "assistant",
            "content": [{"type": "text", "text": "hi"}],
            "model": "claude-sonnet-4-6",
            "stop_reason": "end_turn",
            "usage": {
                "input_tokens": 11,
                "output_tokens": 22,
                "cache_read_input_tokens": 33,
                "cache_creation_input_tokens": 44,
            },
        }))
    }));
    let upstream = format!("http://{}", serve(upstream).await);
    let native = serve(test_app(&upstream, None).0).await;
    let openai = serve(test_openai_app(&upstream, None).0).await;
    let pad = "\"".repeat(16 << 10);
    let client = reqwest::Client::new();

    for (addr, path, message) in [
        (native, "/v1/messages", "proxied"),
        (openai, "/v1/chat/completions", "proxied (openai-compat)"),
    ] {
        let marker = format!("long-headers{}", path.replace('/', "-"));
        let mut request = client
            .post(format!("http://{addr}{path}"))
            .header("content-type", "application/json")
            .header("x-api-key", "any")
            .header("x-client-id", format!("{marker}{pad}"))
            .header("user-agent", format!("claude-cli/{pad}"));
        for name in [
            "x-agent-id",
            "x-claude-code-session-id",
            "x-session-id",
            "x-claude-code-request-class",
            "x-claude-code-agent-type",
            "x-claude-code-compaction",
            "x-claude-code-context-compacted",
            "x-claude-code-prompt-id",
        ] {
            request = request.header(name, &pad);
        }
        let resp = request
            .body(r#"{"model":"claude-sonnet-4-6","messages":[{"role":"user","content":"hi"}]}"#)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200, "{marker}");

        let output = String::from_utf8(buf.lock().unwrap().clone()).unwrap();
        let line = output
            .lines()
            .find(|l| l.contains(&marker) && l.contains(message))
            .unwrap_or_else(|| panic!("no `{message}` line for {marker}"));
        assert!(line.len() <= MAX_LINE_BYTES, "{} bytes", line.len());
        let fields = unpack_logfmt(line, true);
        for (key, sent) in [
            ("input", "11"),
            ("output", "22"),
            ("cached", "33"),
            ("cache_write", "44"),
        ] {
            assert_eq!(field_values(&fields, key), [sent], "{key}: {line}");
        }
        let client_id = truncate_label(&format!("{marker}{pad}"));
        assert_eq!(field_values(&fields, "client_id"), [client_id.as_str()]);
    }
}

/// A URI path can hold a raw `"` and, UTF-8 encoded, non-ASCII characters,
/// which Debug would escape as `\u{ad}`. The path on a rejection line reads
/// back whole.
#[tokio::test]
async fn uri_path_on_a_rejection_line_reads_back_whole() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let buf = log_capture_buf();
    let upstream = format!(
        "http://{}",
        serve(Router::new().fallback(any(mock_anthropic_handler))).await
    );
    let addr = serve(test_app(&upstream, None).0).await;
    let marker = "lab11064-path";
    let path = "/v1/messages\u{ad}\"x=y";
    let body = "{not json";
    let request = format!(
        "POST {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\
         x-api-key: any\r\nx-client-id: {marker}\r\n\
         Content-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}",
        body.len()
    );
    let mut sock = tokio::net::TcpStream::connect(addr).await.unwrap();
    sock.write_all(request.as_bytes()).await.unwrap();
    let mut raw = Vec::new();
    tokio::time::timeout(Duration::from_secs(5), sock.read_to_end(&mut raw))
        .await
        .expect("server must respond, not hang")
        .unwrap();
    let raw = String::from_utf8_lossy(&raw);
    assert!(raw.starts_with("HTTP/1.1 400"), "{raw}");

    let output = String::from_utf8(buf.lock().unwrap().clone()).unwrap();
    let line = output
        .lines()
        .find(|l| l.contains(marker) && l.contains("could not be parsed as JSON"))
        .unwrap_or_else(|| panic!("no rejection line for {marker}:\n{output}"));
    let fields = unpack_logfmt(line, true);
    assert_eq!(field_values(&fields, "path"), [path], "{line}");
    assert_eq!(field_values(&fields, "client_id"), [marker], "{line}");
}

// ── LAB-9228: the upstream's reason for a 4xx, on the proxied line ──

/// The `proxied` line for one native `/v1/messages` request tagged `marker`,
/// sent to an upstream that answers every request with `status` and `body`.
async fn native_proxied_line(status: StatusCode, body: serde_json::Value, marker: &str) -> String {
    let buf = log_capture_buf();
    let upstream = Router::new().fallback(any(move || {
        let body = body.clone();
        async move { (status, axum::Json(body)).into_response() }
    }));
    let mock_addr = serve(upstream).await;
    let (app, _state) = test_app(&format!("http://{mock_addr}"), None);
    let app_addr = serve(app).await;
    let resp = reqwest::Client::new()
        .post(format!("http://{app_addr}/v1/messages"))
        .header("content-type", "application/json")
        .header("x-api-key", "any")
        .header("x-client-id", marker)
        .body(r#"{"model":"claude-sonnet-4-6","messages":[{"role":"user","content":"hi"}]}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), status.as_u16());
    let output = String::from_utf8(buf.lock().unwrap().clone()).unwrap();
    output
        .lines()
        .find(|l| l.contains(marker) && l.contains("proxied"))
        .unwrap_or_else(|| panic!("no proxied line for {marker}:\n{output}"))
        .to_string()
}

fn anthropic_error(err_type: &str, message: &str) -> serde_json::Value {
    serde_json::json!({"type": "error", "error": {"type": err_type, "message": message}})
}

/// A forwarded native 4xx names the upstream's error type and message; a 2xx
/// line carries `-` in both fields.
#[tokio::test]
async fn proxied_line_names_upstream_4xx_reason() {
    let line = native_proxied_line(
        StatusCode::BAD_REQUEST,
        anthropic_error("invalid_request_error", "max_tokens: Field required"),
        "upstream-err-native-400",
    )
    .await;
    assert!(
        line.contains("status=400")
            && line.contains(r#"err_type="invalid_request_error""#)
            && line.contains(r#"err="max_tokens: Field required""#),
        "a 4xx line should name the upstream reason, got: {line}"
    );

    let line = native_proxied_line(
        StatusCode::OK,
        serde_json::json!({"type": "message", "usage": {"input_tokens": 1, "output_tokens": 1}}),
        "upstream-err-native-200",
    )
    .await;
    assert!(
        line.contains("status=200")
            && line.contains(r#"err_type="-""#)
            && line.contains(r#"err="-""#),
        "a 2xx line should carry - in both fields, got: {line}"
    );
}

/// A 4xx whose body is not an Anthropic error envelope names nothing.
#[tokio::test]
async fn proxied_line_names_nothing_for_a_non_envelope_4xx() {
    let line = native_proxied_line(
        StatusCode::BAD_REQUEST,
        serde_json::json!({"error": {"type": "invalid_request_error", "message": "no top-level type"}}),
        "upstream-err-native-non-envelope",
    )
    .await;
    assert!(
        line.contains("status=400")
            && line.contains(r#"err_type="-""#)
            && line.contains(r#"err="-""#),
        "a non-envelope 4xx should carry - in both fields, got: {line}"
    );
}

/// `err` is one line and at most 200 characters: a longer message keeps 199
/// characters plus a trailing `…`, and `GoQuoted` escapes its quotes, newlines
/// and returns, so the line stays whole and the value reads back as kept.
#[tokio::test]
async fn proxied_line_err_is_bounded_and_one_line() {
    // 6 characters, 100 times: 600, with a quote, a newline and a return.
    let message = "q\"n\nr\r".repeat(100);
    let line = native_proxied_line(
        StatusCode::BAD_REQUEST,
        anthropic_error("invalid_request_error", &message),
        "upstream-err-native-bounded",
    )
    .await;
    // 33 × 6 + 1 = 199 characters kept, then the cut mark.
    let expected = format!("{}q…", "q\"n\nr\r".repeat(33));
    assert_eq!(expected.chars().count(), 200);
    assert!(
        line.contains(r#"err="q\"n\nr\rq\"n\nr\r"#),
        "err should be escaped, got: {line}"
    );
    let fields = unpack_logfmt(&line, true);
    assert_eq!(field_values(&fields, "err"), [expected.as_str()], "{line}");
}

/// The line a rotated rejection writes (here a 404 for a model the account
/// does not serve) names the reason too; the attempt that then serves it
/// carries `-`.
#[tokio::test]
async fn rotated_rejection_line_names_upstream_reason() {
    let buf = log_capture_buf();
    let (reject_url, _hits) =
        spawn_status_then_ok_upstream(usize::MAX, HEAD_404_MODEL, b"{}").await;
    let (ok_url, _h) = spawn_mock_upstream().await;
    let reject = mk_endpoint_at("reject", "sk-ant-api-r", &reject_url);
    let mut healthy = mk_endpoint_at("healthy", "sk-ant-api-h", &ok_url);
    healthy.priority = 1;
    let addr = serve(build_router(test_state_with(vec![reject, healthy]))).await;

    let marker = "upstream-err-rotated";
    let resp = reqwest::Client::new()
        .post(format!("http://{addr}/v1/messages"))
        .header("content-type", "application/json")
        .header("x-client-id", marker)
        .body(r#"{"model":"claude-nope-1","max_tokens":1,"messages":[{"role":"user","content":"hi"}]}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    let output = String::from_utf8(buf.lock().unwrap().clone()).unwrap();
    let lines: Vec<&str> = output
        .lines()
        .filter(|l| l.contains(marker) && l.contains("proxied"))
        .collect();
    assert_eq!(
        lines.len(),
        2,
        "one line per attempt, got:\n{}",
        lines.join("\n")
    );
    assert!(
        lines[0].contains("status=404")
            && lines[0].contains(r#"err_type="not_found_error""#)
            && lines[0].contains(r#"err="model: claude-nope-1""#),
        "the rotated line should name the upstream reason, got: {}",
        lines[0]
    );
    assert!(
        lines[1].contains("status=200") && lines[1].contains(r#"err_type="-""#),
        "the serving attempt should carry -, got: {}",
        lines[1]
    );
}

/// Only a 4xx with a complete envelope yields a reason. `err_type` is capped
/// by `truncate_label`, and a message of exactly 200 characters is kept whole.
#[test]
fn upstream_error_from_body_gates_and_bounds() {
    let envelope = anthropic_error("invalid_request_error", "m");
    assert!(UpstreamError::from_body(400, &envelope).is_some());
    assert!(UpstreamError::from_body(499, &envelope).is_some());
    assert!(UpstreamError::from_body(200, &envelope).is_none());
    assert!(UpstreamError::from_body(500, &envelope).is_none());
    for partial in [
        serde_json::json!({"type": "error", "error": {"type": "t"}}),
        serde_json::json!({"type": "error", "error": {"message": "m"}}),
        serde_json::json!({"type": "error", "error": {"type": 1, "message": "m"}}),
        serde_json::json!({"type": "message", "error": {"type": "t", "message": "m"}}),
        serde_json::json!("error"),
    ] {
        assert!(
            UpstreamError::from_body(400, &partial).is_none(),
            "{partial}"
        );
    }

    let exact = "é".repeat(200);
    let long_type = format!("{}\n", "t".repeat(100));
    let e = UpstreamError::from_body(400, &anthropic_error(&long_type, &exact)).unwrap();
    assert_eq!(e.message, exact, "a 200-character message is not cut");
    assert_eq!(e.err_type, truncate_label(&format!("{} ", "t".repeat(100))));
}

// ── LAB-7593: estimated stream usage is budget-only, in one write ──

/// The estimate joins the reported usage in the client's budget, and only
/// there: the token counters keep what the upstream reported. With nothing
/// reported, the estimate alone is booked and no counter entry appears.
#[tokio::test]
async fn record_usage_charging_books_estimate_to_budget_only() {
    let state = Arc::new(
        TestPolicy::open()
            .budgets(
                [("c1".to_string(), 1_000_000), ("c2".to_string(), 1_000_000)]
                    .into_iter()
                    .collect(),
            )
            .install(AppState {
                endpoints: vec![mk_endpoint("a", "sk-ant-api-x")],
                ..test_state_base()
            }),
    );
    let usage = TokenUsage {
        input_tokens: 7,
        cache_read_input_tokens: 3,
        ..TokenUsage::default()
    };
    state
        .record_usage_charging(&state.endpoints[0], &state.caller("c1"), "m", &usage, 50)
        .await;
    state
        .record_usage_charging(
            &state.endpoints[0],
            &state.caller("c2"),
            "m",
            &TokenUsage::default(),
            80,
        )
        .await;

    let budget = state.budget_usage.lock().unwrap().clone();
    assert_eq!(budget.get("c1").map(|&(_, used)| used), Some(60));
    assert_eq!(budget.get("c2").map(|&(_, used)| used), Some(80));
    let counters = state.client_usage.lock().unwrap();
    assert_eq!(counters.get("c1"), Some(&[7, 0, 0, 3]));
    assert!(!counters.contains_key("c2"));
    assert_eq!(state.endpoints[0].output_tokens.load(Ordering::Relaxed), 0);
}

// ── LAB-8496: usage served on paid extra usage ─────────────────

#[tokio::test]
async fn record_overage_usage_counts_anonymous_and_skips_empty() {
    let state = test_state_with(vec![mk_endpoint("a", "sk-ant-api-x")]);
    let usage = TokenUsage {
        input_tokens: 4,
        output_tokens: 3,
        cache_creation_input_tokens: 2,
        cache_read_input_tokens: 1,
    };
    // Anonymous traffic is billed too, unlike the per-client families.
    state.record_overage_usage("-", "claude-sonnet-5", &usage);
    state.record_overage_usage("c1", "claude-sonnet-5", &TokenUsage::default());
    let map = state.overage_usage.lock().unwrap();
    assert_eq!(
        map.get(&("-".to_string(), "claude-sonnet-5".to_string())),
        Some(&[4, 3, 2, 1])
    );
    assert_eq!(map.len(), 1, "empty usage must not mint a key");
}
