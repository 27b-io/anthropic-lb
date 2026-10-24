use super::*;

// ── LAB-4190: `speed` on a model without speed support is stripped and re-sent once ──

/// The model-capability 400 as observed live (2026-09-29) on Fable 5.1.
const SPEED_400_FABLE: &[u8] = br#"{"type":"error","error":{"type":"invalid_request_error","message":"'claude-fable-5-1' does not support the `speed` parameter. This feature is only available on supported models."}}"#;

/// The same 400 on Haiku 4.5: it names the RESOLVED id, not the one requested.
const SPEED_400_HAIKU: &[u8] = br#"{"type":"error","error":{"type":"invalid_request_error","message":"'claude-haiku-4-5-20251001' does not support the `speed` parameter. This feature is only available on supported models."}}"#;

const FABLE_FAST: &str = r#"{"model":"claude-fable-5-1","max_tokens":1,"speed":"fast","messages":[{"role":"user","content":"hi"}]}"#;
const FABLE_STANDARD: &str = r#"{"model":"claude-fable-5-1","max_tokens":1,"speed":"standard","messages":[{"role":"user","content":"hi"}]}"#;
const FABLE_FAST_STREAM: &str = r#"{"model":"claude-fable-5-1","max_tokens":1,"stream":true,"speed":"fast","messages":[{"role":"user","content":"hi"}]}"#;
const FABLE_NO_SPEED: &str =
    r#"{"model":"claude-fable-5-1","max_tokens":1,"messages":[{"role":"user","content":"hi"}]}"#;

fn json(raw: &[u8]) -> serde_json::Value {
    serde_json::from_slice(raw).unwrap()
}

#[test]
fn speed_unsupported_classifies_exact_template_only() {
    let sent = FABLE_FAST.as_bytes();
    let classify = |status: StatusCode, err: &serde_json::Value, protocol: Protocol| {
        classify_rejection(status, err, protocol, "claude-fable-5-1", sent, false)
    };
    for (why, raw) in [
        ("Fable", SPEED_400_FABLE),
        ("Haiku, resolved id", SPEED_400_HAIKU),
    ] {
        let err = json(raw);
        assert_eq!(
            classify(StatusCode::BAD_REQUEST, &err, Protocol::Anthropic),
            Some(UpstreamRejection::SpeedUnsupported),
            "{why}"
        );
        for protocol in [Protocol::Anthropic, Protocol::OpenAI] {
            assert!(
                !is_model_unsupported_error(
                    StatusCode::BAD_REQUEST,
                    &err,
                    protocol,
                    "claude-fable-5-1"
                ),
                "{why}: a speed 400 is not a model rejection under {protocol:?}"
            );
        }
    }
    // The org-entitlement 400 keeps its own verdict.
    let not_enabled = serde_json::json!({"type":"error","error":{
        "type":"invalid_request_error","message":FAST_MODE_NOT_ENABLED_MSG}});
    assert_eq!(
        classify(StatusCode::BAD_REQUEST, &not_enabled, Protocol::Anthropic),
        Some(UpstreamRejection::FastModeDisabled)
    );
    // A passthrough endpoint gets no verdict: its caller sees the 400 as a
    // direct client would.
    assert_eq!(
        classify_rejection(
            StatusCode::BAD_REQUEST,
            &json(SPEED_400_FABLE),
            Protocol::Anthropic,
            "claude-fable-5-1",
            sent,
            true
        ),
        None
    );
    // The request-body veto covers this verdict too: a sent body it cannot
    // read as an object forwards the 400.
    assert_eq!(
        classify_rejection(
            StatusCode::BAD_REQUEST,
            &json(SPEED_400_FABLE),
            Protocol::Anthropic,
            "claude-fable-5-1",
            b"[]",
            false
        ),
        None
    );

    let err =
        |ty: &str, msg: &str| serde_json::json!({"type":"error","error":{"type":ty,"message":msg}});
    let suffix = SPEED_UNSUPPORTED_SUFFIX;
    let misses = [
        (
            "a 429",
            StatusCode::TOO_MANY_REQUESTS,
            json(SPEED_400_FABLE),
        ),
        ("a 404", StatusCode::NOT_FOUND, json(SPEED_400_FABLE)),
        (
            "another error type",
            StatusCode::BAD_REQUEST,
            err("not_found_error", &format!("'claude-fable-5-1{suffix}")),
        ),
        (
            "an empty id",
            StatusCode::BAD_REQUEST,
            err("invalid_request_error", &format!("'{suffix}")),
        ),
        (
            "an id holding a quote",
            StatusCode::BAD_REQUEST,
            err("invalid_request_error", &format!("'a' b{suffix}")),
        ),
        (
            "an unquoted id",
            StatusCode::BAD_REQUEST,
            err(
                "invalid_request_error",
                &format!("claude-fable-5-1{suffix}"),
            ),
        ),
        (
            "trailing text",
            StatusCode::BAD_REQUEST,
            err(
                "invalid_request_error",
                &format!("'claude-fable-5-1{suffix} (more)"),
            ),
        ),
        (
            "leading text",
            StatusCode::BAD_REQUEST,
            err(
                "invalid_request_error",
                &format!("x: 'claude-fable-5-1{suffix}"),
            ),
        ),
    ];
    for (why, status, e) in misses {
        assert_eq!(
            classify(status, &e, Protocol::Anthropic),
            None,
            "must not match {why}"
        );
    }

    // A client key spelled as the sentence comes back as an echo. The
    // template does not match it, and the echo veto would stop it if it did.
    let sentence = "'claude-fable-5-1' does not support the `speed` parameter. This feature is only available on supported models.";
    let echo_sent = format!(
        r#"{{"model":"claude-fable-5-1","max_tokens":1,"messages":[],{}:1}}"#,
        serde_json::to_string(sentence).unwrap()
    );
    let echo = err(
        "invalid_request_error",
        &format!("{sentence}: Extra inputs are not permitted"),
    );
    assert_eq!(
        classify_rejection(
            StatusCode::BAD_REQUEST,
            &echo,
            Protocol::Anthropic,
            "claude-fable-5-1",
            echo_sent.as_bytes(),
            false
        ),
        None
    );
}

/// What the speed mock answers a body without a top-level `speed`.
#[derive(Clone, Copy)]
enum Reply {
    /// 200 with unified rate-limit headers (5h utilization 0.42): JSON, or
    /// SSE when the body asks to stream.
    Ok,
    /// A capacity 429 (`retry-after`), which cools the endpoint and rotates.
    RateLimited,
}

type Bodies = Arc<Mutex<Vec<serde_json::Value>>>;

/// Upstream that records every body it receives. A body with a top-level
/// `speed` gets `speed_400` when one is given, the model-capability 400;
/// with `None` the model accepts `speed`. Every other body gets `reply`.
async fn spawn_speed_upstream(speed_400: Option<&'static [u8]>, reply: Reply) -> (String, Bodies) {
    let bodies: Bodies = Arc::new(Mutex::new(Vec::new()));
    let seen = bodies.clone();
    let app = Router::new().fallback(any(move |req: Request<Body>| {
        let seen = seen.clone();
        async move {
            let raw = axum::body::to_bytes(req.into_body(), usize::MAX)
                .await
                .unwrap();
            let body: serde_json::Value = serde_json::from_slice(&raw).unwrap();
            let stream = body.get("stream").and_then(|v| v.as_bool()) == Some(true);
            let has_speed = body.get("speed").is_some();
            seen.lock().unwrap().push(body);
            let json_ct = (axum::http::header::CONTENT_TYPE, "application/json");
            if let (true, Some(err)) = (has_speed, speed_400) {
                return (StatusCode::BAD_REQUEST, [json_ct], err).into_response();
            }
            match reply {
                Reply::RateLimited => (
                    StatusCode::TOO_MANY_REQUESTS,
                    [json_ct, (axum::http::header::RETRY_AFTER, "7")],
                    r#"{"type":"error","error":{"type":"rate_limit_error","message":"rate limited"}}"#,
                )
                    .into_response(),
                Reply::Ok => {
                    let (content_type, payload) = if stream {
                        ("text/event-stream", Body::from(MOCK_ANTHROPIC_SSE))
                    } else {
                        ("application/json", Body::from(ANTHROPIC_OK_BODY))
                    };
                    Response::builder()
                        .status(StatusCode::OK)
                        .header("content-type", content_type)
                        .header(
                            "anthropic-ratelimit-unified-representative-claim",
                            "five_hour",
                        )
                        .header("anthropic-ratelimit-unified-5h-utilization", "0.42")
                        .header(
                            "anthropic-ratelimit-unified-5h-reset",
                            (AppState::now_epoch() + 3600).to_string(),
                        )
                        .body(payload)
                        .unwrap()
                }
            }
        }
    }));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (format!("http://{addr}"), bodies)
}

/// `first` at priority 0 and an always-200 `healthy` at priority 1: any
/// rotation off `first` shows up as a `healthy` hit.
async fn first_then_healthy_pool(first: Endpoint) -> (Arc<AppState>, SocketAddr, Hits) {
    let (ok_url, ok_hits) = spawn_flaky_upstream(0, ANTHROPIC_OK_BODY).await;
    let mut healthy = mk_endpoint_at("healthy", "sk-ant-api-h", &ok_url);
    healthy.priority = 1;
    let state = test_state_with(vec![first, healthy]);
    let addr = serve(build_router(state.clone())).await;
    (state, addr, ok_hits)
}

async fn post(addr: SocketAddr, path: &str, body: &str) -> reqwest::Response {
    reqwest::Client::new()
        .post(format!("http://{addr}{path}"))
        .header("content-type", "application/json")
        .body(body.to_owned())
        .send()
        .await
        .unwrap()
}

async fn metrics(addr: SocketAddr) -> String {
    reqwest::Client::new()
        .get(format!("http://{addr}/metrics"))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap()
}

fn keys(body: &serde_json::Value) -> Vec<String> {
    body.as_object().unwrap().keys().cloned().collect()
}

/// AC-2 / AC-3 / AC-8: the request is re-sent once, to the same endpoint,
/// without `speed` and otherwise unchanged, and the caller gets the re-send's
/// answer. Nothing is learned: a second identical request pays the same two
/// calls, no cache holds an entry, nothing is cooled and nothing rotates.
/// Streaming too, since the 400 arrives as JSON before any SSE byte. Any
/// `speed` value goes, and an OAuth endpoint's body variant is stripped too.
#[tokio::test]
async fn speed_400_resends_once_to_same_endpoint_without_speed() {
    use std::sync::atomic::Ordering;
    let cases = [
        ("fast", "sk-ant-api-f", "/v1/messages", FABLE_FAST),
        ("standard", "sk-ant-api-f", "/v1/messages", FABLE_STANDARD),
        (
            "streaming",
            "sk-ant-api-f",
            "/v1/messages",
            FABLE_FAST_STREAM,
        ),
        ("oauth", "sk-ant-oat01-f", "/v1/messages", FABLE_FAST),
        (
            "count_tokens",
            "sk-ant-api-f",
            "/v1/messages/count_tokens",
            FABLE_FAST,
        ),
    ];
    for (kind, token, path, body) in cases {
        let (url, bodies) = spawn_speed_upstream(Some(SPEED_400_FABLE), Reply::Ok).await;
        let (state, addr, healthy_hits) =
            first_then_healthy_pool(mk_endpoint_at("fable", token, &url)).await;
        for _ in 0..2 {
            let resp = post(addr, path, body).await;
            assert_eq!(resp.status(), reqwest::StatusCode::OK, "{kind}");
            let text = resp.text().await.unwrap();
            if kind == "streaming" {
                assert!(text.contains("message_stop"), "{kind}: {text}");
            }
        }

        let seen = bodies.lock().unwrap().clone();
        assert_eq!(
            seen.len(),
            4,
            "{kind}: two calls per request, every request"
        );
        for pair in seen.chunks(2) {
            let (first, resend) = (&pair[0], &pair[1]);
            assert!(first.get("speed").is_some(), "{kind}: sent as asked first");
            let mut want = keys(first);
            want.retain(|k| k != "speed");
            assert_eq!(keys(resend), want, "{kind}: only `speed` goes");
            for k in &want {
                assert_eq!(first[k], resend[k], "{kind}: `{k}` unchanged");
            }
        }
        assert_eq!(
            healthy_hits.load(Ordering::SeqCst),
            0,
            "{kind}: no rotation"
        );
        assert!(
            state.unsupported_models.lock().unwrap().is_empty(),
            "{kind}"
        );
        assert!(state.fast_mode_disabled_endpoints().is_empty(), "{kind}");
        assert!(
            state.endpoints[0]
                .rate_info
                .read()
                .await
                .hard_limited_until
                .is_none(),
            "{kind}: not cooled"
        );
        let m = metrics(addr).await;
        assert!(
            m.contains(r#"anthropic_speed_unsupported_total{model="claude-fable-5-1"} 2"#),
            "{kind}: one count per strip:\n{m}"
        );
    }
}

/// AC-4 / AC-6: after the strip the request is standard, so the re-send's
/// unified headers are ingested into the account's view. The control is a
/// model that accepts `speed`: one call, `speed` intact, and the fast
/// response's headers left out of the account's view.
#[tokio::test]
async fn stripped_request_is_standard_and_accepted_speed_is_untouched() {
    let (url, _bodies) = spawn_speed_upstream(Some(SPEED_400_FABLE), Reply::Ok).await;
    let state = test_state_with(vec![mk_endpoint_at("fable", "sk-ant-api-f", &url)]);
    let addr = serve(build_router(state.clone())).await;
    assert_eq!(
        post(addr, "/v1/messages", FABLE_FAST).await.status(),
        reqwest::StatusCode::OK
    );
    assert_eq!(
        state.endpoints[0].rate_info.read().await.utilization_5h,
        Some(0.42),
        "the stripped re-send is standard, so its headers are ingested"
    );

    let (url, bodies) = spawn_speed_upstream(None, Reply::Ok).await;
    let state = test_state_with(vec![mk_endpoint_at("opus", "sk-ant-api-o", &url)]);
    let addr = serve(build_router(state.clone())).await;
    assert_eq!(
        post(addr, "/v1/messages", FABLE_FAST).await.status(),
        reqwest::StatusCode::OK
    );
    let seen = bodies.lock().unwrap().clone();
    assert_eq!(seen.len(), 1, "a model that accepts `speed` gets one call");
    assert_eq!(seen[0]["speed"], "fast");
    assert_eq!(
        state.endpoints[0].rate_info.read().await.utilization_5h,
        None,
        "a fast response stays out of the account's standard view"
    );
    assert!(
        !metrics(addr)
            .await
            .contains("anthropic_speed_unsupported_total{"),
        "nothing stripped, nothing counted"
    );
}

/// AC-5: at most one strip. An upstream that still answers the speed 400
/// without `speed` gets it forwarded, after exactly one re-send.
#[tokio::test]
async fn second_speed_400_reaches_caller_unchanged() {
    use std::sync::atomic::Ordering;
    let (url, hits) = spawn_400_upstream(SPEED_400_FABLE).await;
    let (_state, addr, healthy_hits) =
        first_then_healthy_pool(mk_endpoint_at("fable", "sk-ant-api-f", &url)).await;
    let resp = post(addr, "/v1/messages", FABLE_FAST).await;
    assert_eq!(resp.status(), reqwest::StatusCode::BAD_REQUEST);
    assert_eq!(
        resp.json::<serde_json::Value>().await.unwrap(),
        json(SPEED_400_FABLE)
    );
    assert_eq!(
        (
            hits.load(Ordering::SeqCst),
            healthy_hits.load(Ordering::SeqCst)
        ),
        (2, 0)
    );
    assert!(metrics(addr)
        .await
        .contains(r#"anthropic_speed_unsupported_total{model="claude-fable-5-1"} 1"#));
}

/// AC-4 / AC-5: a 429 on the re-send follows the normal policy, cooling the
/// account and rotating, and the next account gets the stripped body. It is
/// marked fast-mode-disabled, so reaching it shows routing now treats the
/// request as standard.
#[tokio::test]
async fn resend_429_rotates_with_the_stripped_body() {
    let (first_url, first_bodies) =
        spawn_speed_upstream(Some(SPEED_400_FABLE), Reply::RateLimited).await;
    let (next_url, mut next_rx) = spawn_capturing_upstream(StatusCode::OK, ANTHROPIC_OK_BODY).await;
    let mut next = mk_endpoint_at("next", "sk-ant-api-n", &next_url);
    next.priority = 1;
    let state = test_state_with(vec![
        mk_endpoint_at("fable", "sk-ant-api-f", &first_url),
        next,
    ]);
    state.note_fast_mode_disabled("next", 1);
    let addr = serve(build_router(state.clone())).await;

    assert_eq!(
        post(addr, "/v1/messages", FABLE_FAST).await.status(),
        reqwest::StatusCode::OK
    );
    assert_eq!(first_bodies.lock().unwrap().len(), 2, "400, then the 429");
    assert!(
        state.endpoints[0]
            .rate_info
            .read()
            .await
            .hard_limited_until
            .is_some(),
        "a standard 429 cools the account"
    );
    let (_, sent) = next_rx.recv().await.unwrap();
    let sent: serde_json::Value = serde_json::from_slice(&sent).unwrap();
    assert!(
        sent.get("speed").is_none(),
        "rotation keeps the strip: {sent}"
    );
    assert!(next_rx.try_recv().is_err(), "one call to the next account");
}

/// AC-5: no strip without a top-level `speed` in the body sent, nor off the
/// Messages schema, nor on a passthrough endpoint: the 400 is forwarded after
/// one call, and nothing is counted.
#[tokio::test]
async fn speed_400_forwarded_when_there_is_nothing_to_strip() {
    use std::sync::atomic::Ordering;
    let cases = [
        ("no speed", "sk-ant-api-f", "/v1/messages", FABLE_NO_SPEED),
        (
            "batches",
            "sk-ant-api-f",
            "/v1/messages/batches",
            r#"{"speed":"fast","requests":[]}"#,
        ),
        ("passthrough", "passthrough", "/v1/messages", FABLE_FAST),
    ];
    for (kind, token, path, body) in cases {
        let (url, hits) = spawn_400_upstream(SPEED_400_FABLE).await;
        let (_state, addr, healthy_hits) =
            first_then_healthy_pool(mk_endpoint_at("fable", token, &url)).await;
        let resp = reqwest::Client::new()
            .post(format!("http://{addr}{path}"))
            .header("content-type", "application/json")
            .header("x-api-key", "sk-ant-api-caller")
            .body(body)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), reqwest::StatusCode::BAD_REQUEST, "{kind}");
        assert_eq!(
            resp.json::<serde_json::Value>().await.unwrap(),
            json(SPEED_400_FABLE),
            "{kind}"
        );
        assert_eq!(
            (
                hits.load(Ordering::SeqCst),
                healthy_hits.load(Ordering::SeqCst)
            ),
            (1, 0),
            "{kind}"
        );
        assert!(
            !metrics(addr)
                .await
                .contains("anthropic_speed_unsupported_total{"),
            "{kind}"
        );
    }
}

/// The model is caller-supplied, so the strip WARN logs it escaped: a raw
/// newline must not start a forged log line (CWE-117).
#[test]
fn speed_unsupported_warn_escapes_the_model() {
    let buf = log_capture_buf();
    let state = test_state_with(vec![mk_endpoint("a", "sk-ant-api-a")]);
    state.note_speed_unsupported("req-lab4190", "x\nFORGED-LAB4190 WARN fake");
    let output = String::from_utf8(buf.lock().unwrap().clone()).unwrap();
    assert!(output.contains(r"\nFORGED-LAB4190"), "{output}");
    assert!(!output.contains("\nFORGED-LAB4190"), "{output}");
}
