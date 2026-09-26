use super::*;

// ── LAB-4729: an "out of extra usage" 400 re-sends once; the account is NOT cooled ──

/// The entitlement 400 as observed live (2026-09-22) — request id replaced.
const ENTITLEMENT_400_BODY: &[u8] = br#"{"type":"error","error":{"type":"invalid_request_error","message":"You're out of extra usage. Ask your workspace admin to add more so you can keep going."},"request_id":"req_test_entitlement"}"#;

/// The anchor message under the wrong `error.type` — the nearest miss, since
/// only the type tells it apart.
const ENTITLEMENT_MSG_AS_AUTH_ERROR: &[u8] = br#"{"type":"error","error":{"type":"authentication_error","message":"You're out of extra usage. Ask your workspace admin to add more so you can keep going."}}"#;

/// A truncated entitlement body. It never parses, so the forward paths never
/// ask the predicate about it — pinned end-to-end, not in the unit test.
const MALFORMED_ENTITLEMENT_400: &[u8] =
    br#"{"type":"error","error":{"type":"invalid_request_error","message":"You're out of extra usage."#;

#[test]
fn entitlement_400_predicate_is_anchored_exact_type_and_400_only() {
    let live: serde_json::Value = serde_json::from_slice(ENTITLEMENT_400_BODY).unwrap();
    assert!(is_entitlement_exhausted_400(StatusCode::BAD_REQUEST, &live));
    // The anchor is the condition, not the plan-specific second sentence.
    let other_plan = serde_json::json!({"type":"error","error":{
        "type":"invalid_request_error",
        "message":"You're out of extra usage. Add more at claude.ai/settings to keep going."}});
    assert!(is_entitlement_exhausted_400(
        StatusCode::BAD_REQUEST,
        &other_plan
    ));

    // The predicate sees every parsed body, 2xx included — the status guard is real.
    for status in [
        StatusCode::TOO_MANY_REQUESTS,
        StatusCode::FORBIDDEN,
        StatusCode::NOT_FOUND,
        StatusCode::OK,
    ] {
        assert!(
            !is_entitlement_exhausted_400(status, &live),
            "only a 400 is the entitlement condition, not {status}"
        );
    }
    let negatives: [(&str, &[u8]); 4] = [
        ("a different invalid_request_error message", PROMPT_TOO_LONG_BODY),
        (
            "the anchor message under authentication_error",
            ENTITLEMENT_MSG_AS_AUTH_ERROR,
        ),
        (
            "the anchor mid-message, not at its start",
            br#"{"type":"error","error":{"type":"invalid_request_error","message":"tools.0: You're out of extra usage is not a valid tool name"}}"#,
        ),
        (
            "a message that merely mentions usage",
            br#"{"type":"error","error":{"type":"invalid_request_error","message":"Invalid usage of tool_choice: extra usage fields are not permitted"}}"#,
        ),
    ];
    for (why, raw) in negatives {
        let body: serde_json::Value = serde_json::from_slice(raw).unwrap();
        assert!(
            !is_entitlement_exhausted_400(StatusCode::BAD_REQUEST, &body),
            "must not match {why}"
        );
    }
}

type Hits = std::sync::Arc<std::sync::atomic::AtomicUsize>;

/// `spent` (priority 0) always answers `body` as a 400; `healthy` at
/// priority 1 is `healthy_url`. Priority, not affinity hashing, forces the
/// first attempt onto `spent`, so hit counts are a clean "did we re-send?"
/// probe — same trick as `two_endpoint_429_then_healthy`.
async fn spent_then(body: &'static [u8], healthy_url: &str) -> (Arc<AppState>, SocketAddr, Hits) {
    let (spent_url, spent_hits) = spawn_400_upstream(body).await;
    let spent = mk_endpoint_at("spent", "sk-ant-api-s", &spent_url);
    let mut healthy = mk_endpoint_at("healthy", "sk-ant-api-h", healthy_url);
    healthy.priority = 1;
    let state = test_state_with(vec![spent, healthy]);
    let addr = serve(build_router(state.clone())).await;
    (state, addr, spent_hits)
}

/// `spent_then` with an always-200 `healthy`.
async fn spent_then_healthy(body: &'static [u8]) -> (Arc<AppState>, SocketAddr, Hits, Hits) {
    let (healthy_url, healthy_hits) = spawn_flaky_upstream(0, ANTHROPIC_OK_BODY).await;
    let (state, addr, spent_hits) = spent_then(body, &healthy_url).await;
    (state, addr, spent_hits, healthy_hits)
}

const MESSAGES_BODY: &str =
    r#"{"model":"claude-opus-5","max_tokens":1,"messages":[{"role":"user","content":"hi"}]}"#;

/// AC-2 / AC-4 / AC-5: the request is re-sent to the next account and THAT
/// response reaches the caller, the event is counted per account — and a
/// streaming request gets the same re-send, because the 400 arrives as a JSON
/// body before any SSE byte. The account is NOT cooled: the refusal is
/// request-class scoped, and cooling on it would let one client sweep the
/// pool (see `note_entitlement_400`). So the second request tries it again.
#[tokio::test]
async fn entitlement_400_resends_once_and_leaves_account_alone() {
    use std::sync::atomic::Ordering;
    for (kind, body) in [
        ("non-streaming", MESSAGES_BODY),
        (
            "streaming",
            r#"{"model":"claude-opus-5","max_tokens":1,"stream":true,"messages":[{"role":"user","content":"hi"}]}"#,
        ),
    ] {
        let (_state, addr, spent_hits, healthy_hits) =
            spent_then_healthy(ENTITLEMENT_400_BODY).await;
        for _ in 0..2 {
            let resp = reqwest::Client::new()
                .post(format!("http://{addr}/v1/messages"))
                .header("content-type", "application/json")
                .body(body)
                .send()
                .await
                .unwrap();
            assert_eq!(
                resp.status(),
                reqwest::StatusCode::OK,
                "{kind}: the re-send's response is what the caller sees"
            );
        }
        // A cooled priority-0 account would be filtered out of the second
        // request, giving (1, 2).
        assert_eq!(
            (
                spent_hits.load(Ordering::SeqCst),
                healthy_hits.load(Ordering::SeqCst)
            ),
            (2, 2),
            "{kind}: each request tries the uncooled account, then re-sends once"
        );
        let m = reqwest::Client::new()
            .get(format!("http://{addr}/metrics"))
            .send()
            .await
            .unwrap()
            .text()
            .await
            .unwrap();
        assert!(
            m.contains(r#"anthropic_entitlement_400_total{account="spent"} 2"#),
            "{kind}: the event must be counted per account on /metrics:\n{m}"
        );
    }
}

/// The refusing account is not cooled and `skip` resets per retry round, so
/// without carrying it across rounds a 529 or transport blip on the re-send
/// target would re-pick the refuser, whose second 400 would end the request
/// before the healthy account got its backoff retry.
#[tokio::test]
async fn entitlement_refuser_stays_skipped_across_retry_rounds() {
    use std::sync::atomic::Ordering;
    const HEAD_529: &str =
        "HTTP/1.1 529 Overloaded\r\ncontent-length: 0\r\nconnection: close\r\n\r\n";
    for kind in ["529 then ok", "transport blip then ok"] {
        let (url, target_hits) = if kind.starts_with("529") {
            spawn_status_then_ok_upstream(1, HEAD_529, ANTHROPIC_OK_BODY).await
        } else {
            spawn_flaky_upstream(1, ANTHROPIC_OK_BODY).await
        };
        let (_state, addr, spent_hits) = spent_then(ENTITLEMENT_400_BODY, &url).await;
        let resp = reqwest::Client::new()
            .post(format!("http://{addr}/v1/messages"))
            .header("content-type", "application/json")
            .body(MESSAGES_BODY)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), reqwest::StatusCode::OK, "{kind}");
        assert_eq!(
            (
                spent_hits.load(Ordering::SeqCst),
                target_hits.load(Ordering::SeqCst)
            ),
            (1, 2),
            "{kind}: the refuser must not be re-picked in the backoff round"
        );
    }
}

/// AC-2 "no loop": one re-send per request. A second entitlement 400 goes to
/// the caller verbatim instead of sweeping the rest of the pool — the refusal
/// is request-class scoped, so every account may give it.
#[tokio::test]
async fn second_entitlement_400_reaches_caller_without_sweeping_pool() {
    use std::sync::atomic::Ordering;
    let (a_url, a_hits) = spawn_400_upstream(ENTITLEMENT_400_BODY).await;
    let (b_url, b_hits) = spawn_400_upstream(ENTITLEMENT_400_BODY).await;
    let (ok_url, ok_hits) = spawn_flaky_upstream(0, ANTHROPIC_OK_BODY).await;
    let a = mk_endpoint_at("spent-a", "sk-ant-api-a", &a_url);
    let mut b = mk_endpoint_at("spent-b", "sk-ant-api-b", &b_url);
    b.priority = 1;
    let mut ok = mk_endpoint_at("healthy", "sk-ant-api-h", &ok_url);
    ok.priority = 2;
    let state = test_state_with(vec![a, b, ok]);
    let addr = serve(build_router(state)).await;

    let resp = reqwest::Client::new()
        .post(format!("http://{addr}/v1/messages"))
        .header("content-type", "application/json")
        .body(MESSAGES_BODY)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::BAD_REQUEST);
    assert_eq!(&resp.bytes().await.unwrap()[..], ENTITLEMENT_400_BODY);
    assert_eq!(
        (
            a_hits.load(Ordering::SeqCst),
            b_hits.load(Ordering::SeqCst),
            ok_hits.load(Ordering::SeqCst)
        ),
        (1, 1, 0),
        "exactly one re-send: the third account must not be tried"
    );
}

/// With nothing else to re-send to, the caller gets the upstream's real 400
/// (it says why, and that more credit fixes it) — not a synthetic 429 that
/// tells it to retry into the same refusal.
#[tokio::test]
async fn entitlement_400_with_no_other_account_returns_upstream_400() {
    let (url, _hits) = spawn_400_upstream(ENTITLEMENT_400_BODY).await;
    let state = test_state_with(vec![mk_endpoint_at("only", "sk-ant-api-o", &url)]);
    let addr = serve(build_router(state)).await;
    let resp = reqwest::Client::new()
        .post(format!("http://{addr}/v1/messages"))
        .header("content-type", "application/json")
        .body(MESSAGES_BODY)
        .send()
        .await
        .unwrap();
    let status = resp.status();
    let body = resp.bytes().await.unwrap();
    assert_eq!(
        status,
        reqwest::StatusCode::BAD_REQUEST,
        "body: {}",
        String::from_utf8_lossy(&body)
    );
    assert_eq!(&body[..], ENTITLEMENT_400_BODY);
}

/// AC-3: every other 400 is the caller's own error — returned byte-for-byte
/// (status, body, upstream `request-id`), not re-sent, nothing counted. The
/// predicate's negatives are unit-tested; these two rows pin the forward path
/// on the nearest miss and on a body that never parses.
#[tokio::test]
async fn non_entitlement_400_passes_through_byte_for_byte() {
    use std::sync::atomic::Ordering;
    for (why, body) in [
        (
            "the anchor message under authentication_error",
            ENTITLEMENT_MSG_AS_AUTH_ERROR,
        ),
        (
            "a malformed (truncated) JSON body",
            MALFORMED_ENTITLEMENT_400,
        ),
    ] {
        let (state, addr, spent_hits, healthy_hits) = spent_then_healthy(body).await;
        let resp = reqwest::Client::new()
            .post(format!("http://{addr}/v1/messages"))
            .header("content-type", "application/json")
            .body(MESSAGES_BODY)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), reqwest::StatusCode::BAD_REQUEST, "{why}");
        assert_eq!(
            resp.headers()
                .get("request-id")
                .and_then(|v| v.to_str().ok()),
            Some("req_upstream_400"),
            "{why}: upstream request-id must reach the caller"
        );
        assert_eq!(&resp.bytes().await.unwrap()[..], body, "{why}");
        assert_eq!(
            (
                spent_hits.load(Ordering::SeqCst),
                healthy_hits.load(Ordering::SeqCst)
            ),
            (1, 0),
            "{why}: a client error must not be re-sent"
        );
        assert!(
            state.entitlement_400.lock().unwrap().is_empty(),
            "{why}: must not be counted"
        );
    }
}

/// The OpenAI-compat handler forwards to the same Anthropic accounts, so the
/// same entitlement 400 re-sends there too (its own `note_entitlement_400`
/// call site, so its own pin).
#[tokio::test]
async fn entitlement_400_resends_on_openai_compat_path() {
    use std::sync::atomic::Ordering;
    let (state, addr, spent_hits, healthy_hits) = spent_then_healthy(ENTITLEMENT_400_BODY).await;
    let resp = reqwest::Client::new()
        .post(format!("http://{addr}/v1/chat/completions"))
        .header("content-type", "application/json")
        .body(r#"{"model":"claude-opus-5","messages":[{"role":"user","content":"hi"}]}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    assert_eq!(
        (
            spent_hits.load(Ordering::SeqCst),
            healthy_hits.load(Ordering::SeqCst)
        ),
        (1, 1)
    );
    assert_eq!(state.entitlement_400.lock().unwrap().get("spent"), Some(&1));
}

/// AC-2 "the second response is what the caller sees": once the re-send gets
/// an answer, the first account's entitlement 400 is no longer the terminal
/// cause. A model rejection surfaces as itself, and a rate-limited re-send
/// target yields the retryable pool-exhaustion 429 — not a non-retryable
/// "add credits" for a pool that is merely cooling.
#[tokio::test]
async fn resend_outcome_supersedes_stashed_entitlement_400() {
    use std::sync::atomic::Ordering;
    for (kind, head, want) in [
        (
            "model-unsupported 404",
            HEAD_404_MODEL,
            reqwest::StatusCode::NOT_FOUND,
        ),
        (
            "rate-limited 429",
            HEAD_429_RETRY_AFTER_7,
            reqwest::StatusCode::TOO_MANY_REQUESTS,
        ),
    ] {
        let (url, target_hits) = spawn_status_then_ok_upstream(usize::MAX, head, b"{}").await;
        let (_state, addr, spent_hits) = spent_then(ENTITLEMENT_400_BODY, &url).await;
        let resp = reqwest::Client::new()
            .post(format!("http://{addr}/v1/messages"))
            .header("content-type", "application/json")
            .body(MESSAGES_BODY)
            .send()
            .await
            .unwrap();
        let status = resp.status();
        let body = resp.text().await.unwrap();
        assert_eq!(status, want, "{kind}: body {body}");
        assert!(
            !body.contains("out of extra usage"),
            "{kind}: the stashed entitlement 400 must not win: {body}"
        );
        if want == reqwest::StatusCode::NOT_FOUND {
            assert!(
                body.contains("not_found_error") && body.contains("claude-nope-1"),
                "{kind}: the re-send target's own error must reach the caller: {body}"
            );
        }
        assert_eq!(
            (
                spent_hits.load(Ordering::SeqCst),
                target_hits.load(Ordering::SeqCst)
            ),
            (1, 1),
            "{kind}"
        );
    }
}

/// A poisoned `entitlement_400` lock: `/metrics` publishes the real count and
/// clears the poison. Driven through the router, not the helper in isolation.
#[tokio::test]
async fn entitlement_400_poisoned_lock_is_recovered_not_zeroed() {
    let state = test_state_with(vec![mk_endpoint("a", "sk-ant-api-aaa")]);
    state.note_entitlement_400("spent");
    {
        let state = state.clone();
        std::thread::spawn(move || {
            let _g = state.entitlement_400.lock().unwrap();
            panic!("deliberate: poison the entitlement-400 counter mutex");
        })
        .join()
        .unwrap_err();
    }
    assert!(state.entitlement_400.is_poisoned());

    let addr = serve(build_router(state.clone())).await;
    let m = reqwest::Client::new()
        .get(format!("http://{addr}/metrics"))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(
        m.contains(r#"anthropic_entitlement_400_total{account="spent"} 1"#),
        "/metrics must publish the real count through a poisoned lock:\n{m}"
    );

    // The render must clear the poison, or the `if let Ok` increment keeps
    // skipping and the series freezes here for the life of the process.
    state.note_entitlement_400("spent");
    assert_eq!(state.entitlement_400.lock().unwrap().get("spent"), Some(&2));
}
