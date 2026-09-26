use super::*;

// ── LAB-1191: token-exfil audit findings (redirects, header reflection,
//    client beta flags) ────────────────────────────────────────────────

/// Raw-TCP upstream that answers every request with a 302 to `target`.
async fn spawn_redirecting_upstream(target: String) -> String {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let (mut sock, _) = listener.accept().await.unwrap();
            let target = target.clone();
            tokio::spawn(async move {
                let mut buf = [0u8; 8192];
                let _ = sock.read(&mut buf).await;
                let resp = format!(
                    "HTTP/1.1 302 Found\r\nlocation: {target}/v1/messages\r\ncontent-length: 0\r\nconnection: close\r\n\r\n"
                );
                let _ = sock.write_all(resp.as_bytes()).await;
                let _ = sock.flush().await;
            });
        }
    });
    format!("http://{}", addr)
}

/// AC-4/5/6: a 3xx from upstream must NOT be followed (the follow-up request
/// would re-send the account credential to the Location host) and must
/// surface as a deliberate 502, not a forwarded 302 or an endless retry.
#[tokio::test]
async fn upstream_redirect_not_followed_and_becomes_502() {
    use std::sync::atomic::Ordering;
    // The redirect target counts every connection it receives — with
    // Policy::none() it must stay at zero.
    let (target_url, target_hits) = spawn_flaky_upstream(0, ANTHROPIC_OK_BODY).await;
    let redirecting = spawn_redirecting_upstream(target_url).await;

    let state = test_state_with(vec![mk_endpoint_at("a", "sk-ant-api-aaa", &redirecting)]);
    let addr = serve(build_router(state)).await;
    let resp = reqwest::Client::new()
        .post(format!("http://{addr}/v1/messages"))
        .header("content-type", "application/json")
        .body(r#"{"model":"test","max_tokens":1,"messages":[{"role":"user","content":"hi"}]}"#)
        .send()
        .await
        .unwrap();

    assert_eq!(
        resp.status(),
        reqwest::StatusCode::BAD_GATEWAY,
        "upstream 3xx must become a deliberate 502"
    );
    let body = resp.text().await.unwrap();
    assert!(body.contains("redirect"), "502 body must say why: {body}");
    assert_eq!(
        target_hits.load(Ordering::SeqCst),
        0,
        "no request may follow the redirect with credentials attached"
    );
}

/// AC-7/AC-10: by default the caller must not see the upstream's rate-limit
/// capacity, cookies, or org identity — only the allow-listed headers.
#[tokio::test]
async fn upstream_headers_stripped_by_default() {
    let (upstream_url, _handle) = spawn_mock_upstream().await;
    let (app, _state) = test_app(&upstream_url, None);
    let addr = serve(app).await;

    let resp = reqwest::Client::new()
        .post(format!("http://{addr}/v1/messages"))
        .header("content-type", "application/json")
        .body(r#"{"model":"test","max_tokens":1,"messages":[{"role":"user","content":"hi"}]}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    for leaked in [
        "anthropic-ratelimit-unified-5h-utilization",
        "anthropic-ratelimit-unified-5h-status",
        "set-cookie",
        "anthropic-organization-id",
    ] {
        assert!(
            !resp.headers().contains_key(leaked),
            "{leaked} must not be reflected by default"
        );
    }
    // Allow-listed headers still flow.
    assert_eq!(
        resp.headers().get("content-type").unwrap(),
        "application/json"
    );
    assert_eq!(
        resp.headers()
            .get("request-id")
            .and_then(|v| v.to_str().ok()),
        Some("req_mock_123"),
        "request-id is allow-listed for SDK error reports"
    );
    assert_eq!(
        resp.headers()
            .get("x-should-retry")
            .and_then(|v| v.to_str().ok()),
        Some("true"),
        "x-should-retry is allow-listed regardless of the ratelimit flag"
    );
    assert!(resp.headers().contains_key("x-budget-status"));
}

/// AC-8/AC-10: expose_upstream_ratelimit_headers = true restores the
/// anthropic-ratelimit-* passthrough (trusted networks) — and ONLY that:
/// cookies and org identity stay stripped.
#[tokio::test]
async fn upstream_ratelimit_headers_reflected_with_flag() {
    let (upstream_url, _handle) = spawn_mock_upstream().await;
    let mut state = test_state_with(vec![mk_endpoint_at(
        "acct-a",
        "sk-ant-api-test-aaa",
        &upstream_url,
    )]);
    Arc::get_mut(&mut state)
        .expect("test fixture should be uniquely owned")
        .expose_upstream_ratelimit_headers = true;
    let addr = serve(build_router(state)).await;

    let resp = reqwest::Client::new()
        .post(format!("http://{addr}/v1/messages"))
        .header("content-type", "application/json")
        .body(r#"{"model":"test","max_tokens":1,"messages":[{"role":"user","content":"hi"}]}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    assert_eq!(
        resp.headers()
            .get("anthropic-ratelimit-unified-5h-utilization")
            .and_then(|v| v.to_str().ok()),
        Some("0.25"),
        "flag must restore anthropic-ratelimit-* passthrough"
    );
    assert!(
        !resp.headers().contains_key("set-cookie"),
        "set-cookie stays stripped even with the ratelimit flag on"
    );
    assert!(
        !resp.headers().contains_key("anthropic-organization-id"),
        "org identity stays stripped even with the ratelimit flag on"
    );
}

#[test]
fn upstream_headers_do_not_synthesize_should_retry() {
    let mut headers = reqwest::header::HeaderMap::new();
    headers.insert(
        "content-type",
        reqwest::header::HeaderValue::from_static("application/json"),
    );
    let response = reflect_upstream_headers(Response::builder(), &headers, false)
        .body(())
        .unwrap();
    assert_eq!(
        response
            .headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok()),
        Some("application/json")
    );
    assert!(!response.headers().contains_key("x-should-retry"));
}

/// Panel follow-up (LAB-1191 AC-5): on the OpenAI-compat surface an upstream
/// 3xx must surface as a 502 in the OPENAI error shape — those clients'
/// parsers cannot read an Anthropic error envelope.
#[tokio::test]
async fn upstream_redirect_becomes_openai_shaped_502_on_chat_completions() {
    use std::sync::atomic::Ordering;
    let (target_url, target_hits) = spawn_flaky_upstream(0, ANTHROPIC_OK_BODY).await;
    let redirecting = spawn_redirecting_upstream(target_url).await;

    let state = test_state_with(vec![mk_endpoint_at("a", "sk-ant-oat01-aaa", &redirecting)]);
    let addr = serve(build_router(state)).await;
    let resp = reqwest::Client::new()
        .post(format!("http://{addr}/v1/chat/completions"))
        .header("content-type", "application/json")
        .body(r#"{"model":"test","max_tokens":1,"messages":[{"role":"user","content":"hi"}]}"#)
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), reqwest::StatusCode::BAD_GATEWAY);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert!(
        body.get("error").and_then(|e| e.get("message")).is_some(),
        "OpenAI-surface 502 must be OpenAI-shaped: {body}"
    );
    assert!(
        body.get("type").is_none(),
        "must not be the Anthropic error envelope: {body}"
    );
    assert_eq!(target_hits.load(Ordering::SeqCst), 0);
}

/// PR #116 review (Kody + CodeRabbit): the 1M-context accounting must read
/// the FILTERED outbound headers. If a custom allow-list strips
/// `context-1m`, the upstream runs the request at 200k — recording the
/// session at 1M would inflate /_stats occupancy.
#[test]
fn stripped_context_1m_flag_is_invisible_to_accounting() {
    let mut headers = axum::http::HeaderMap::new();
    headers.insert(
        "anthropic-beta",
        HeaderValue::from_static("context-1m-2025-08-07"),
    );
    // Custom allow-list that omits context-1m*.
    let restrictive = vec!["oauth-2025-04-20".to_string()];
    let dropped = inject_account_auth(&mut headers, "sk-ant-oat01-test", false, &restrictive);
    assert_eq!(dropped, vec!["context-1m-2025-08-07".to_string()]);
    assert!(
        !request_has_1m_beta(&headers),
        "post-filter headers must not carry the stripped 1M flag"
    );
}

/// Same defect end-to-end: with a restrictive allow-list, a request carrying
/// the 1M beta must be registered in the session registry at the 200k window
/// the upstream actually ran under — not at 1M.
#[tokio::test]
async fn session_registry_window_matches_filtered_beta() {
    // Raw-TCP mock: its canned body carries `usage`, which session
    // registration requires (the axum mock's body has none).
    let (upstream_url, _hits) = spawn_flaky_upstream(0, ANTHROPIC_OK_BODY).await;
    let mut state = test_state_with(vec![mk_endpoint_at(
        "acct-a",
        "sk-ant-oat01-test-aaa",
        &upstream_url,
    )]);
    Arc::get_mut(&mut state)
        .expect("test fixture should be uniquely owned")
        .allowed_client_betas = vec!["oauth-2025-04-20".to_string()];
    let sessions_state = state.clone();
    let addr = serve(build_router(state)).await;

    let resp = reqwest::Client::new()
        .post(format!("http://{addr}/v1/messages"))
        .header("content-type", "application/json")
        .header("x-session-id", "sess-1m-strip")
        .header("anthropic-beta", "context-1m-2025-08-07")
        .body(r#"{"model":"claude-sonnet-4-6","max_tokens":1,"messages":[{"role":"user","content":"hi"}]}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    let windows: Vec<u64> = sessions_state
        .sessions
        .lock()
        .unwrap()
        .values()
        .map(|s| s.context_window)
        .collect();
    assert_eq!(
        windows,
        vec![DEFAULT_CONTEXT_WINDOW],
        "session must be tracked at the window the upstream ran (flag was stripped)"
    );
}
