use super::*;

/// AC-11/AC-13: an unknown client beta flag is dropped from the forwarded
/// header and reported back; the LB's own OAuth flags are still appended.
#[test]
fn oauth_beta_filter_drops_unknown_flags() {
    let mut headers = axum::http::HeaderMap::new();
    headers.insert(
        "anthropic-beta",
        HeaderValue::from_static("evil-feature-2026-01-01,interleaved-thinking-2025-05-14"),
    );
    let dropped = inject_account_auth(&mut headers, "sk-ant-oat01-test", false, &default_betas());
    assert_eq!(dropped, vec!["evil-feature-2026-01-01".to_string()]);
    let sent = headers.get("anthropic-beta").unwrap().to_str().unwrap();
    assert!(
        !sent.contains("evil-feature-2026-01-01"),
        "unknown flag must not be forwarded: {sent}"
    );
    assert!(sent.contains("interleaved-thinking-2025-05-14"));
    for flag in OAUTH_BETA_FLAGS {
        assert!(sent.contains(flag), "required OAuth flag missing: {flag}");
    }
}

/// AC-13: every default-allowed flag survives the filter, including the
/// wildcard-matched 1M-context flag — and 1M detection still fires on the
/// filtered header map.
#[test]
fn oauth_beta_filter_keeps_default_allowed_flags() {
    let client_flags = [
        "oauth-2025-04-20",
        "claude-code-20250219",
        "interleaved-thinking-2025-05-14",
        "fine-grained-tool-streaming-2025-05-14",
        "prompt-caching-2024-07-31",
        "context-1m-2025-08-07",
    ];
    let mut headers = axum::http::HeaderMap::new();
    headers.insert(
        "anthropic-beta",
        HeaderValue::from_str(&client_flags.join(",")).unwrap(),
    );
    let dropped = inject_account_auth(&mut headers, "sk-ant-oat01-test", false, &default_betas());
    assert!(dropped.is_empty(), "nothing should be dropped: {dropped:?}");
    let sent = headers.get("anthropic-beta").unwrap().to_str().unwrap();
    for flag in client_flags {
        assert!(sent.contains(flag), "allowed flag missing: {flag}");
    }
    assert!(
        request_has_1m_beta(&headers),
        "1M-context detection must still fire after filtering"
    );
}

/// Regression (2026-08-01 incident): the full `anthropic-beta` set Claude
/// Code 2.1.220 sends must survive the default allow-list. The first cut of
/// `DEFAULT_CLIENT_BETA_ALLOWLIST` listed only six entries and dropped the
/// rest, which 400'd every Claude Code request through the proxy —
/// `context-management` in particular has a body-side `context_management`
/// object that the LB forwards verbatim, so dropping the header alone is a
/// hard upstream rejection, not a silent feature downgrade.
///
/// This inventory came off `anthropic_beta_flag_dropped_total` on the live
/// fleet. Date suffixes are deliberately concrete: the allow-list wildcards
/// them, so a Claude Code date bump keeps passing while a genuinely new flag
/// family still shows up as a drop. Body-paired families added later
/// (fast-mode, LAB-2669) ride the same array: exact-token forwarding,
/// negative control, date bump.
#[test]
fn oauth_beta_filter_keeps_claude_code_flag_set() {
    let claude_code_flags = [
        "thinking-token-count-2026-05-13",
        "context-management-2025-06-27",
        "mid-conversation-system-2026-04-07",
        "advisor-tool-2026-03-01",
        "effort-2025-11-24",
        "fallback-credit-2026-06-01",
        "extended-cache-ttl-2025-04-11",
        "redact-thinking-2026-02-12",
        "afk-mode-2026-01-31",
        "structured-outputs-2025-12-15",
        // LAB-2669: body-paired (`speed`); not in the 2.1.220 inventory.
        "fast-mode-2026-02-01",
        // LAB-3963: auto-mode classifier pair; `dangerous-tool-use` is
        // body-paired (`safeguards`). Not in the 2.1.220 inventory.
        "auto-mode-classifier-2026-07-16",
        "dangerous-tool-use-2026-09-03",
        // LAB-3964: per-turn family (2.1.278), body-paired — see
        // DEFAULT_CLIENT_BETA_ALLOWLIST.
        "mid-conversation-tool-changes-2026-07-01",
        "per-turn-control-2026-07-01",
        "timing-2026-09-09",
    ];
    // Negative control: the point of the allow-list is that it still rejects.
    // Without this, widening the default to "*" would keep the test green.
    let unlisted = "evil-feature-2026-01-01";
    let mut headers = axum::http::HeaderMap::new();
    headers.insert(
        "anthropic-beta",
        HeaderValue::from_str(&format!("{},{unlisted}", claude_code_flags.join(","))).unwrap(),
    );
    let dropped = inject_account_auth(&mut headers, "sk-ant-oat01-test", false, &default_betas());
    assert_eq!(
        dropped,
        vec![unlisted.to_string()],
        "only the unlisted flag may be dropped"
    );
    // Exact token membership, not substring: `sent.contains(flag)` also passes
    // on a mangled or embedded token (e.g. "no-effort-2025-11-24" contains
    // "effort-2025-11-24"), so it cannot tell a forwarded flag from a
    // corrupted one.
    let sent = headers.get("anthropic-beta").unwrap().to_str().unwrap();
    let tokens: Vec<&str> = sent.split(',').map(str::trim).collect();
    for flag in claude_code_flags {
        assert!(
            tokens.contains(&flag),
            "Claude Code flag not forwarded as an exact token: {flag} (sent: {sent})"
        );
    }
    for flag in OAUTH_BETA_FLAGS {
        assert!(
            tokens.contains(flag),
            "required OAuth flag missing: {flag} (sent: {sent})"
        );
    }
    assert!(
        !tokens.contains(&unlisted),
        "dropped flag must not be forwarded: {sent}"
    );

    // A Claude Code date bump must keep passing — that is the entire reason
    // these entries are wildcarded. Nothing above catches a de-wildcarded
    // entry (`"context-management-*"` narrowed back to the concrete
    // `"context-management-2025-06-27"` satisfies every assertion so far),
    // and that edit re-breaks all primary traffic on Claude Code's next
    // release. Rebuild each family with a different date, through the real
    // filter path.
    let bumped: Vec<String> = claude_code_flags
        .iter()
        .map(|flag| {
            // Strip the trailing `-YYYY-MM-DD`, keeping the family. Validate
            // the suffix shape first so a malformed inventory entry (e.g. a
            // compact `-YYYYMMDD` date) fails naming the entry, instead of
            // mis-stripping and surfacing as a baffling allowlist miss below.
            let parts: Vec<&str> = flag.rsplitn(4, '-').collect();
            assert_eq!(parts.len(), 4, "flag lacks a -YYYY-MM-DD suffix: {flag}");
            // rsplitn yields the components reversed: day, month, year.
            assert!(
                parts[..3]
                    .iter()
                    .zip([2usize, 2, 4])
                    .all(|(p, w)| p.len() == w && p.bytes().all(|b| b.is_ascii_digit())),
                "flag suffix is not numeric YYYY-MM-DD: {flag}"
            );
            let family = parts[3];
            assert_ne!(family, "", "empty family for {flag}");
            assert_ne!(family, *flag, "date-suffix strip failed for {flag}");
            format!("{family}-2099-12-31")
        })
        .collect();
    let mut bumped_headers = axum::http::HeaderMap::new();
    bumped_headers.insert(
        "anthropic-beta",
        HeaderValue::from_str(&bumped.join(",")).unwrap(),
    );
    let bumped_dropped = inject_account_auth(
        &mut bumped_headers,
        "sk-ant-oat01-test",
        false,
        &default_betas(),
    );
    assert!(
        bumped_dropped.is_empty(),
        "a Claude Code date bump must stay allowed (suffix wildcard lost?): {bumped_dropped:?}"
    );
    // `dropped` and the outbound header are separate outputs — an allowed
    // flag silently discarded (neither forwarded nor reported) passes the
    // assertion above. Check the header too, exact-token like the main set.
    let bumped_sent = bumped_headers
        .get("anthropic-beta")
        .unwrap()
        .to_str()
        .unwrap();
    let bumped_tokens: Vec<&str> = bumped_sent.split(',').map(str::trim).collect();
    for flag in &bumped {
        assert!(
            bumped_tokens.contains(&flag.as_str()),
            "bumped flag not forwarded as an exact token: {flag} (sent: {bumped_sent})"
        );
    }
}

/// AC-13: passthrough endpoints return early — caller headers untouched,
/// nothing dropped.
#[test]
fn oauth_beta_filter_passthrough_unchanged() {
    let mut headers = axum::http::HeaderMap::new();
    headers.insert("authorization", HeaderValue::from_static("Bearer caller"));
    headers.insert(
        "anthropic-beta",
        HeaderValue::from_static("anything-goes-2026"),
    );
    let dropped = inject_account_auth(&mut headers, "passthrough", true, &default_betas());
    assert!(dropped.is_empty());
    assert_eq!(
        headers.get("authorization").unwrap(),
        "Bearer caller",
        "passthrough must not touch caller auth"
    );
    assert_eq!(
        headers.get("anthropic-beta").unwrap(),
        "anything-goes-2026",
        "passthrough must not filter caller betas"
    );
}

/// AC-12: dropped flags land in the bounded counter map; past the cap they
/// fold into `_other` instead of growing per-client-controlled cardinality.
#[test]
fn dropped_beta_flag_counter_is_bounded() {
    let state = test_state_with(vec![]);
    for i in 0..(MAX_DROPPED_BETA_FLAGS + 10) {
        state.record_dropped_beta_flags("t", &[format!("flag-{i}")]);
    }
    let map = state.beta_flags_dropped.lock().unwrap();
    assert_eq!(map.len(), MAX_DROPPED_BETA_FLAGS + 1, "cap + _other bucket");
    assert_eq!(map.get("_other"), Some(&10u64));
    drop(map);
    // Existing keys keep counting past the cap.
    state.record_dropped_beta_flags("t", &["flag-0".to_string()]);
    assert_eq!(
        state.beta_flags_dropped.lock().unwrap().get("flag-0"),
        Some(&2u64)
    );
}

/// AC-12/AC-13 end-to-end: a request carrying an unknown beta flag is served,
/// the flag never reaches the upstream, and /metrics reports the drop.
#[tokio::test]
async fn dropped_beta_flag_appears_in_metrics() {
    let (upstream_url, _handle) = spawn_mock_upstream().await;
    // test_state_base already carries the default allow-list.
    let state = test_state_with(vec![mk_endpoint_at(
        "acct-a",
        "sk-ant-oat01-test-aaa",
        &upstream_url,
    )]);
    let addr = serve(build_router(state)).await;

    let resp = reqwest::Client::new()
        .post(format!("http://{addr}/v1/messages"))
        .header("content-type", "application/json")
        .header("anthropic-beta", "totally-unknown-2026-07-30")
        .body(r#"{"model":"test","max_tokens":1,"messages":[{"role":"user","content":"hi"}]}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    let metrics = reqwest::Client::new()
        .get(format!("http://{addr}/metrics"))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(
        metrics
            .contains("anthropic_beta_flag_dropped_total{flag=\"totally-unknown-2026-07-30\"} 1"),
        "metrics must report the dropped flag"
    );
}

/// Body-paired beta families must reach the upstream header AND body together
/// through the real OAuth path (which re-serialises the body) — see
/// `DEFAULT_CLIENT_BETA_ALLOWLIST` for why. Asserts every flag is forwarded
/// as an exact token and `must_contain` survives in the body byte-identical.
async fn assert_oauth_forwards_betas_with_body(flags: &[&str], body: String, must_contain: &str) {
    let (upstream_url, mut seen) =
        spawn_capturing_upstream(StatusCode::OK, ANTHROPIC_OK_BODY).await;
    let state = Arc::new(AppState {
        endpoints: vec![mk_endpoint_at(
            "acct-a",
            "sk-ant-oat01-test-aaa",
            &upstream_url,
        )],
        auto_cache: false,
        ..test_state_base()
    });
    let addr = serve(build_router(state)).await;
    let resp = reqwest::Client::new()
        .post(format!("http://{addr}/v1/messages"))
        .header("content-type", "application/json")
        .header("anthropic-beta", flags.join(","))
        .body(body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    let (headers, bytes) = seen.recv().await.expect("upstream must have been hit once");
    let sent = headers.get("anthropic-beta").unwrap().to_str().unwrap();
    let tokens: Vec<&str> = sent.split(',').map(str::trim).collect();
    for flag in flags {
        assert!(
            tokens.contains(flag),
            "beta not forwarded as an exact token: {flag} (sent: {sent})"
        );
    }
    let raw = std::str::from_utf8(&bytes).unwrap();
    assert!(
        raw.contains(must_contain),
        "body must reach the upstream byte-identical:\n{raw}"
    );
}

/// LAB-3963: auto-mode classifier pair ↔ top-level `safeguards`.
#[tokio::test]
async fn oauth_forwards_auto_mode_classifier_header_and_safeguards_body_together() {
    // Compact JSON, as Claude Code sends it.
    let safeguards = r#"[{"type":"dangerous_tool_use","classifier_context":"rm -rf ./build"}]"#;
    let body = format!(
        r#"{{"model":"test","max_tokens":1,"messages":[{{"role":"user","content":"hi"}}],"safeguards":{safeguards}}}"#
    );
    assert_oauth_forwards_betas_with_body(
        &[
            "auto-mode-classifier-2026-07-16",
            "dangerous-tool-use-2026-09-03",
        ],
        body,
        &format!(r#""safeguards":{safeguards}"#),
    )
    .await;
}

/// LAB-3964: per-turn family ↔ a `role:"system"` entry in `messages` carrying
/// `tool_addition` blocks and `output_config` with `effort` + `timing`. Unlike
/// the top-level `safeguards` key above, this pairing sits INSIDE `messages`,
/// which the auto-cache path mutates — so it gets its own byte-identical check.
#[tokio::test]
async fn oauth_forwards_per_turn_betas_header_and_system_entry_body_together() {
    let system_entry = r#"{"role":"system","content":[{"type":"tool_addition","tool":{"type":"tool_reference","name":"mcp__x__y"}}],"output_config":{"effort":"high","timing":{"type":"now","now":"2026-09-20T10:00:00+10:00"}}}"#;
    let body = format!(
        r#"{{"model":"test","max_tokens":1,"messages":[{{"role":"user","content":"hi"}},{system_entry}]}}"#
    );
    assert_oauth_forwards_betas_with_body(
        &[
            "mid-conversation-tool-changes-2026-07-01",
            "per-turn-control-2026-07-01",
            "timing-2026-09-09",
        ],
        body,
        system_entry,
    )
    .await;
}

/// Panel follow-up (LAB-1191): a client flag that IS one of the required
/// OAUTH_BETA_FLAGS is always forwarded (the merge re-adds it), so it must
/// never be reported as dropped — even under a custom allow-list that
/// omits it. A false drop here would make the diagnostics lie.
#[test]
fn oauth_beta_filter_never_reports_required_flags_as_dropped() {
    let mut headers = axum::http::HeaderMap::new();
    headers.insert(
        "anthropic-beta",
        HeaderValue::from_static("oauth-2025-04-20,claude-code-20250219"),
    );
    // Custom allow-list omitting the OAuth flags entirely.
    let restrictive = vec!["context-1m*".to_string()];
    let dropped = inject_account_auth(&mut headers, "sk-ant-oat01-test", false, &restrictive);
    assert!(
        dropped.is_empty(),
        "required OAuth flags are always sent — reporting them dropped is a lie: {dropped:?}"
    );
    let sent = headers.get("anthropic-beta").unwrap().to_str().unwrap();
    for flag in OAUTH_BETA_FLAGS {
        assert!(sent.contains(flag));
    }
}

/// Panel follow-up (LAB-1191): dropped-flag keys are length-bounded before
/// logging/counting — a multi-kilobyte client "flag" must not be pinned
/// verbatim into every /metrics scrape.
#[test]
fn dropped_beta_flag_keys_are_length_bounded() {
    let state = test_state_with(vec![]);
    let huge = "x".repeat(5000);
    state.record_dropped_beta_flags("t", &[huge]);
    let map = state.beta_flags_dropped.lock().unwrap();
    let key = map.keys().next().unwrap();
    assert!(
        key.len() <= MAX_DROPPED_BETA_FLAG_LEN,
        "key must be truncated, got {} bytes",
        key.len()
    );
}

// ---------------------------------------------------------------------------
// LAB-1261 — header/body coherence for the `anthropic-beta` allow-list.
//
// The 2026-08-01 fleet outage, LAB-2669 (`fast-mode`/`speed`) and LAB-3963
// (`dangerous-tool-use`/`safeguards`) are all one defect: the filter edits the
// header and forwards the body verbatim, so a PAIRED beta the allow-list does
// not carry becomes a hard upstream 400 on every request instead of a feature
// quietly turning off. These tests pin the fix for a flag family the LB has
// never seen — no allow-list entry required.
// ---------------------------------------------------------------------------

/// An OAuth-shaped fixture token, derived from the production discriminator
/// so it cannot drift from the predicate it exists to exercise.
///
/// The shape is load-bearing, not decoration: `inject_account_auth` and
/// `forward_anthropic` both branch on `OAUTH_TOKEN_PREFIX` to decide whether
/// the beta filter runs at all, so a fixture of any other shape would
/// exercise none of the code below.
///
/// No claim is made here about credential scanning. This repo's own rule
/// (`.gitleaks.toml`) requires a 20-character tail, which neither this value
/// nor the `-test` literals elsewhere in this file ever matched.
fn oauth_shaped_fixture_token() -> String {
    format!("{OAUTH_TOKEN_PREFIX}-FIXTURE-DO-NOT-USE")
}

/// 5h utilization the strict upstream reports on every 200. Distinctive
/// enough that reading it back off an endpoint proves it came from here.
const STRICT_UPSTREAM_5H_UTILIZATION: &str = "0.42";

/// Mock upstream modelling the ONE upstream behaviour this ticket is about:
/// Anthropic rejects top-level body fields it does not recognise
/// (`speed: Extra inputs are not permitted`). Returns 400 on any non-base
/// field, 200 otherwise, and records what it was actually sent.
///
/// A mock that always 200s would pass whether or not the body was rewritten,
/// which is the whole assertion — so the strictness is the test.
async fn spawn_strict_anthropic_upstream() -> (String, Arc<Mutex<Vec<serde_json::Value>>>) {
    // Anthropic's rule, restated independently of the production tables so a
    // wrong entry in `BETA_BODY_FIELDS` cannot make this mock agree with the
    // code it is checking: a non-base top-level field is accepted only while
    // the request still declares the beta that owns it.
    const UPSTREAM_PAIRINGS: &[(&str, &str)] = &[("speed", "fast-mode-")];
    // Spelled out rather than imported from `BASE_BODY_FIELDS`: if the mock
    // shares that table, a wrong entry in it makes the mock agree with the
    // code under test and every assertion below stays green.
    const UPSTREAM_BASE_FIELDS: &[&str] = &[
        "model",
        "messages",
        "max_tokens",
        "system",
        "temperature",
        "top_p",
        "top_k",
        "stream",
    ];
    let seen: Arc<Mutex<Vec<serde_json::Value>>> = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&seen);
    let app = Router::new().fallback(any(
        move |headers: axum::http::HeaderMap, body: bytes::Bytes| {
            let sink = Arc::clone(&sink);
            async move {
                let declared: Vec<String> = headers
                    .get_all("anthropic-beta")
                    .iter()
                    .filter_map(|v| v.to_str().ok())
                    .flat_map(|s| s.split(','))
                    .map(|s| s.trim().to_string())
                    .collect();
                let parsed: serde_json::Value =
                    serde_json::from_slice(&body).unwrap_or(serde_json::Value::Null);
                sink.lock().unwrap().push(parsed.clone());
                let extra: Vec<String> = parsed
                    .as_object()
                    .map(|o| {
                        o.keys()
                            .filter(|k| !UPSTREAM_BASE_FIELDS.contains(&k.as_str()))
                            .filter(|k| {
                                !UPSTREAM_PAIRINGS.iter().any(|(field, prefix)| {
                                    field == k
                                        && declared.iter().any(|d| d.starts_with(prefix))
                                })
                            })
                            .cloned()
                            .collect()
                    })
                    .unwrap_or_default();
                if let Some(field) = extra.first() {
                    return (
                        StatusCode::BAD_REQUEST,
                        axum::response::AppendHeaders([("content-type", "application/json")]),
                        format!(
                            r#"{{"type":"error","error":{{"type":"invalid_request_error","message":"{field}: Extra inputs are not permitted"}}}}"#
                        ),
                    )
                        .into_response();
                }
                (
                    StatusCode::OK,
                    axum::response::AppendHeaders([
                        ("content-type", "application/json"),
                        // Standard-window headroom. Only `update_rate_info_for`
                        // reads it, and only when the request was NOT fast —
                        // which makes its arrival the observable proof that the
                        // request drew the standard bucket. See
                        // `stripped_speed_draws_the_standard_rate_bucket`.
                        (
                            "anthropic-ratelimit-unified-5h-utilization",
                            STRICT_UPSTREAM_5H_UTILIZATION,
                        ),
                    ]),
                    r#"{"id":"msg_1","type":"message","role":"assistant","model":"claude-opus-4-7","content":[{"type":"text","text":"ok"}],"stop_reason":"end_turn","usage":{"input_tokens":1,"output_tokens":1}}"#,
                )
                    .into_response()
            }
        },
    ));
    let listener = match tokio::net::TcpListener::bind("127.0.0.1:0").await {
        Ok(l) => l,
        Err(e) => panic!("bind the strict Anthropic mock upstream on 127.0.0.1:0: {e}"),
    };
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (format!("http://{addr}"), seen)
}

/// Drive one request through `forward_anthropic` against the strict upstream.
/// Returns the response status and the body the upstream actually received.
async fn forward_with_betas(
    client_betas: &str,
    body: &'static str,
) -> (StatusCode, serde_json::Value) {
    forward_with_betas_on("/v1/messages", client_betas, body).await
}

async fn forward_with_betas_on(
    path: &'static str,
    client_betas: &str,
    body: &'static str,
) -> (StatusCode, serde_json::Value) {
    let (status, received, _) = forward_with_betas_full(
        path,
        None,
        /* is_fast_mode */ false,
        client_betas,
        body,
    )
    .await;
    (status, received)
}

/// As above, but with the two knobs the fast-mode regression needs: a custom
/// `allowed_client_betas` and the `is_fast_mode` verdict `proxy_handler` would
/// have reached on the pre-filter body. Also hands back the state, so the
/// caller can read what the rate-limit ingest did to the endpoint.
async fn forward_with_betas_full(
    path: &'static str,
    allowed_client_betas: Option<Vec<String>>,
    is_fast_mode: bool,
    client_betas: &str,
    body: &'static str,
) -> (StatusCode, serde_json::Value, Arc<AppState>) {
    let (url, seen) = spawn_strict_anthropic_upstream().await;
    let mut state = test_state_with(vec![]);
    let mut ep = make_endpoint("acct", Protocol::Anthropic);
    ep.base_url = url;
    // `OAUTH_TOKEN_PREFIX` is what arms the beta filter — an API-key endpoint
    // does not filter at all, so it could not exercise this path.
    ep.token = oauth_shaped_fixture_token();
    {
        let state = Arc::get_mut(&mut state).unwrap();
        state.endpoints.push(ep);
        if let Some(allowed) = allowed_client_betas {
            state.allowed_client_betas = allowed;
        }
    }

    let parts = axum::http::Request::builder()
        .method("POST")
        .uri(path)
        .header("anthropic-beta", client_betas)
        .body(())
        .unwrap()
        .into_parts()
        .0;
    let body_bytes = bytes::Bytes::from_static(body.as_bytes());
    let outcome = forward_anthropic(
        &state,
        &parts,
        &body_bytes,
        &body_bytes,
        is_fast_mode,
        &state.endpoints[0],
        0,
        "req-1261",
        "client-1",
        "-",
        &"127.0.0.1".parse().unwrap(),
        "-",
        "-",
        "claude-opus-4-7",
        None,
        None,
        Instant::now(),
    )
    .await;
    let status = match outcome {
        ForwardOutcome::Done(resp) => resp.status(),
        _ => panic!("expected a completed response, got a retry/rotate outcome"),
    };
    let received = seen.lock().unwrap().first().cloned().unwrap_or_default();
    (status, received, state)
}

/// AC-1 + AC-2 + AC-4: a PAIRED beta family the LB has never seen. The header
/// is dropped (allow-list intact) and its orphaned body field goes with it,
/// so the upstream sees a coherent request and answers 200 — where today it
/// answers 400 for every request of that family, fleet-wide.
#[tokio::test]
async fn unknown_paired_beta_degrades_instead_of_400() {
    let (status, sent) = forward_with_betas(
        "totally-unknown-2026-09-01",
        r#"{"model":"claude-opus-4-7","messages":[],"max_tokens":1,"totally_unknown_config":{"mode":"on"}}"#,
    )
    .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "an unknown paired beta must degrade to a working request, not a 400"
    );
    assert!(
        sent.get("totally_unknown_config").is_none(),
        "the orphaned body half must be stripped with its header: {sent}"
    );
    assert!(
        sent.get("max_tokens").is_some() && sent.get("messages").is_some(),
        "base schema fields must survive untouched: {sent}"
    );
}

/// AC-1: the unpaired case — an unknown flag with no body counterpart. The
/// header is still dropped, and the body must not be touched at all.
#[tokio::test]
async fn unknown_unpaired_beta_leaves_body_untouched() {
    let (status, sent) = forward_with_betas(
        "totally-unknown-2026-09-01",
        r#"{"model":"claude-opus-4-7","messages":[],"max_tokens":1,"temperature":1.0,"top_p":0.9}"#,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        sent.get("temperature").and_then(|v| v.as_f64()),
        Some(1.0),
        "no body field may be collateral: {sent}"
    );
    assert_eq!(sent.get("top_p").and_then(|v| v.as_f64()), Some(0.9));
}

/// The over-strip guard, and the reason the surviving set is read off the
/// OUTBOUND header rather than off config: Claude Code sends a dozen betas at
/// once. When one unknown flag arrives alongside allow-listed paired ones,
/// only the orphan's field may go — `speed` belongs to `fast-mode-*`, which
/// survived, so it must survive too. Without this, the fix would silently
/// disable working features on every mixed request.
#[tokio::test]
async fn surviving_paired_beta_keeps_its_body_field() {
    let (status, sent) = forward_with_betas(
        "fast-mode-2026-02-01,totally-unknown-2026-09-01",
        r#"{"model":"claude-opus-4-7","messages":[],"max_tokens":1,"speed":"fast","totally_unknown_config":{}}"#,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        sent.get("speed").and_then(|v| v.as_str()),
        Some("fast"),
        "a body field whose own flag survived must not be stripped: {sent}"
    );
    assert!(
        sent.get("totally_unknown_config").is_none(),
        "the orphaned field must still go: {sent}"
    );
}

/// Stripping `speed` must also unwind the fast-mode verdict `proxy_handler`
/// reached on the pre-filter body, or the accounting bills a request that ran
/// as standard against the fast pool.
///
/// `update_rate_info_for` returns early for a fast request — a fast 200's
/// `anthropic-ratelimit-unified-*` headers describe the fast pool, not the
/// account's 5h/7d windows (LAB-2693). So whether the mock's 5h utilization
/// lands in `rate_info` IS the bucket the request drew, and the control below
/// is what makes the assertion mean anything: the same request with
/// `fast-mode-*` allowed keeps `speed`, stays fast, and ingests nothing.
#[tokio::test]
async fn stripped_speed_draws_the_standard_rate_bucket() {
    const FAST_BODY: &str =
        r#"{"model":"claude-opus-4-7","messages":[],"max_tokens":1,"speed":"fast"}"#;
    let expected: f64 = STRICT_UPSTREAM_5H_UTILIZATION.parse().unwrap();

    // `fast-mode-*` off the allow-list: the header flag is dropped, `speed` is
    // orphaned and goes with it. Only families that HAVE a `BETA_BODY_FIELDS`
    // row may survive, or the strip disables itself — the proxy's own
    // unconditionally re-added `OAUTH_BETA_FLAGS` are exactly that.
    let (status, sent, state) = forward_with_betas_full(
        "/v1/messages",
        /* allowed_client_betas */ Some(vec!["oauth-2025-04-20".to_string()]),
        /* is_fast_mode */ true,
        /* client_betas */ "fast-mode-2026-02-01",
        FAST_BODY,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        sent.get("speed").is_none(),
        "the orphaned `speed` must be stripped once its flag is dropped: {sent}"
    );
    assert_eq!(
        state.endpoints[0].rate_info.read().await.utilization_5h,
        Some(expected),
        "a request that went upstream WITHOUT `speed` ran as standard, so its \
         rate-limit headers are account headroom and must be ingested"
    );

    // Control: same body, same `is_fast_mode`, allow-list carrying the flag.
    let (status, sent, state) = forward_with_betas_full(
        "/v1/messages",
        /* allowed_client_betas */ None,
        /* is_fast_mode */ true,
        /* client_betas */ "fast-mode-2026-02-01",
        FAST_BODY,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        sent.get("speed").and_then(|v| v.as_str()),
        Some("fast"),
        "the default allow-list carries `fast-mode-*`, so `speed` must survive: {sent}"
    );
    assert_eq!(
        state.endpoints[0].rate_info.read().await.utilization_5h,
        None,
        "a genuinely fast request must not ingest fast-pool headers as account headroom"
    );
}

/// The hot path pays nothing. No drop means no parse, no re-serialize, and
/// byte-identical forwarding — which also keeps prompt-cache prefixes intact.
#[test]
fn no_dropped_flag_means_no_body_rewrite() {
    let body = bytes::Bytes::from_static(
        br#"{"model":"claude-opus-4-7","messages":[],"max_tokens":1,"speed":"fast"}"#,
    );
    assert!(
        strip_orphaned_beta_body_fields(&body, "fast-mode-2026-02-01,oauth-2025-04-20", &[])
            .is_none(),
        "nothing dropped → body must be forwarded verbatim"
    );
}

/// Every field of the base schema survives a drop. This is the list the fix
/// trades the flag-family enumeration for, so a typo in it is a silent
/// feature amputation on any request carrying an unknown beta — pin it.
#[test]
fn base_schema_fields_are_never_stripped() {
    let body: serde_json::Value = BASE_BODY_FIELDS
        .iter()
        .map(|f| ((*f).to_string(), serde_json::json!("x")))
        .collect::<serde_json::Map<_, _>>()
        .into();
    let bytes = bytes::Bytes::from(serde_json::to_vec(&body).unwrap());
    assert!(
        strip_orphaned_beta_body_fields(&bytes, "", &["unknown-2026-01-01".to_string()]).is_none(),
        "a body made only of base fields must survive a drop untouched"
    );
}

/// Every known pairing travels with its flag in BOTH directions, and the
/// assertion that makes this non-tautological: each pattern must actually be
/// on the default allow-list. Without it the loop synthesises the surviving
/// flag from the pattern under test, so a row for a family that can never
/// survive — an inert row that reads as protection — still passes.
#[test]
fn known_pairings_travel_together() {
    for (pattern, fields) in BETA_BODY_FIELDS {
        let flag = pattern.replace('*', "2026-01-01");
        assert!(
            beta_flag_allowed(&default_betas(), &flag),
            "{pattern} is not on the default allow-list, so it can never \
             survive the header filter and this mapping is dead code"
        );
        for field in *fields {
            let body = bytes::Bytes::from(
                serde_json::to_vec(&serde_json::json!({
                    "model": "claude-opus-4-7",
                    "messages": [],
                    "max_tokens": 1,
                    *field: "x",
                }))
                .unwrap(),
            );
            // Flag survived → field stays (no strip at all is a valid "stays").
            assert!(
                strip_orphaned_beta_body_fields(
                    &body,
                    &format!("{flag},oauth-2025-04-20"),
                    &["other-2026-01-01".to_string()]
                )
                .is_none(),
                "{field} must survive while {flag} survives"
            );
            // Flag dropped → field goes with it.
            let (rewritten, stripped) = strip_orphaned_beta_body_fields(
                &body,
                "oauth-2025-04-20",
                std::slice::from_ref(&flag),
            )
            .unwrap_or_else(|| panic!("{field} must be stripped when {flag} is dropped"));
            assert_eq!(stripped, vec![(*field).to_string()]);
            let parsed: serde_json::Value = serde_json::from_slice(&rewritten).unwrap();
            assert!(parsed.get(*field).is_none());
        }
    }
}

/// Helly R finding 1, and the invariant the whole keep-side rests on: a
/// surviving flag protects its body field ONLY if a row claims it, so the
/// table has to be total over the allow-list. A family with no row is not
/// neutral — its field gets deleted out from under a caller who was entitled
/// to it. This fails the build rather than waiting for the billing surprise.
#[test]
fn beta_body_field_table_covers_the_allowlist() {
    for family in DEFAULT_CLIENT_BETA_ALLOWLIST {
        assert!(
            BETA_BODY_FIELDS
                .iter()
                .any(|(pattern, _)| pattern == family),
            "{family} is allow-listed but has no BETA_BODY_FIELDS row: a request \
             carrying it plus any unrecognised flag would have that family's \
             top-level body field deleted. Add a row — `&[]` if it owns none."
        );
    }
}

/// Helly R finding 1, the concrete case. `fallback-credit-*` is allow-listed
/// and Claude Code sends it; `fallback_credit_token` is a billing instrument
/// redeemable once within five minutes of a refusal. One unrelated unknown
/// flag alongside it used to delete the token while the credit flag itself
/// sailed through the filter.
#[test]
fn surviving_flag_protects_its_field_against_an_unrelated_drop() {
    let body = bytes::Bytes::from_static(
        br#"{"model":"claude-opus-4-7","messages":[],"max_tokens":1,"fallback_credit_token":"tok_abc"}"#,
    );
    assert!(
        strip_orphaned_beta_body_fields(
            &body,
            "fallback-credit-2026-07-01,oauth-2025-04-20",
            &["totally-made-up-beta-2026-01-01".to_string()],
        )
        .is_none(),
        "an unrelated dropped flag must not cost the caller their fallback credit"
    );
}

/// Helly R finding 1, second half: a custom `allowed_client_betas` can pass a
/// family this proxy has no row for. It may own a top-level field, and the
/// proxy cannot tell that field from an orphan — so it declines to strip at
/// all. Losing the degrade costs a 400 the caller already gets today; deleting
/// `mcp_servers` while keeping the header leaves a dangling tool reference.
#[test]
fn unrecognised_surviving_flag_disables_the_strip() {
    let body = bytes::Bytes::from_static(
        br#"{"model":"claude-opus-4-7","messages":[],"max_tokens":1,"mcp_servers":[{"type":"url","url":"https://x","name":"calc"}]}"#,
    );
    assert!(
        strip_orphaned_beta_body_fields(
            &body,
            "mcp-client-2025-11-20,oauth-2025-04-20",
            &["totally-unknown-2026-09-01".to_string()],
        )
        .is_none(),
        "a surviving flag with no BETA_BODY_FIELDS row must switch the strip off"
    );
}

/// Helly R finding 2: a `serde_json::Value` round-trip rewrote an integer too
/// large for `u64` as a float, silently changing a value inside RETAINED tool
/// history — data corruption on the part of the payload the design promises
/// to preserve. Retained fields are spliced through as their original bytes.
#[test]
fn rewrite_preserves_retained_values_exactly() {
    let body = bytes::Bytes::from_static(
        br#"{"model":"claude-opus-4-7","max_tokens":1,"messages":[{"role":"user","content":[{"type":"text","text":"hi","input":{"record_id":18446744073709551617}}]}],"orphan_field":1}"#,
    );
    let (rewritten, stripped) = strip_orphaned_beta_body_fields(
        &body,
        "oauth-2025-04-20",
        &["totally-unknown-2026-09-01".to_string()],
    )
    .expect("the orphan field must be stripped");
    assert_eq!(stripped, vec!["orphan_field".to_string()]);
    let text = std::str::from_utf8(&rewritten).unwrap();
    assert!(
        text.contains("18446744073709551617"),
        "a retained nested integer must survive byte-for-byte, got: {text}"
    );
    assert!(!text.contains("1.8446744073709552e19"));
    assert!(!text.contains("orphan_field"));
}

/// Panel finding (CRIT), and the sharpest edge on this change: `proxy_handler`
/// is the router's catch-all, so EVERY route reaches `forward_anthropic` —
/// while `BASE_BODY_FIELDS` describes `/v1/messages` alone. Ungated, one
/// unlisted beta flag on a `/v1/messages/batches` POST deleted the entire
/// payload (`{"requests":[…]}` → `{}`) and forwarded it, turning a working
/// request into a 400 AND destroying the caller's data on the way.
///
/// The strip is scoped to the schema it actually knows; everything else
/// forwards byte-for-byte no matter what the beta filter did to the header.
#[tokio::test]
async fn strip_is_scoped_to_the_messages_schema() {
    let (_, sent) = forward_with_betas_on(
        "/v1/messages/batches",
        "totally-unknown-2026-09-01",
        r#"{"requests":[{"custom_id":"a","params":{"model":"claude-opus-4-7"}}]}"#,
    )
    .await;
    assert!(
        sent.get("requests").is_some(),
        "a non-Messages route must forward its body untouched: {sent}"
    );
}

/// The counter is the only alertable signal this mechanism adds, so a caller
/// must not be able to blind it. One request carrying more junk top-level keys
/// than the whole map holds used to fill every slot for the process lifetime,
/// after which real strips landed in `_other` forever and the first-sighting
/// warn never fired again.
#[test]
fn one_request_cannot_exhaust_the_strip_counter() {
    let state = test_state_with(vec![]);
    let junk: Vec<String> = (0..MAX_DROPPED_BETA_FLAGS * 2)
        .map(|i| format!("junk_{i}"))
        .collect();
    state.record_stripped_body_fields("attacker", &junk, &["unknown-2026-01-01".to_string()]);
    let map = state.beta_body_fields_stripped.lock().unwrap();
    let used = map.len();
    assert!(
        used <= MAX_STRIPPED_FIELDS_PER_REQUEST + 1,
        "one request claimed {used} named slots; the per-request cap is \
         {MAX_STRIPPED_FIELDS_PER_REQUEST} plus the shared _other bucket"
    );
    // Helly R finding 3: the removals past the cap still happened, so they are
    // still counted. Discarding them let an ordered payload hide the
    // actionable field behind junk and leave no trace anything else went.
    assert_eq!(
        map.get("_other").copied(),
        Some((MAX_DROPPED_BETA_FLAGS * 2 - MAX_STRIPPED_FIELDS_PER_REQUEST) as u64),
        "every removal past the per-request cap must land in _other"
    );
    drop(map);
    // The genuine signal still gets a slot afterwards.
    state.record_stripped_body_fields("real", &["speed".to_string()], &["fast-mode-x".to_string()]);
    assert!(
        state
            .beta_body_fields_stripped
            .lock()
            .unwrap()
            .contains_key("speed"),
        "a real strip must still be countable after a junk-key request"
    );
}

/// A poisoned counter lock must not turn into a published zero.
///
/// `/metrics` used to read this map with `.lock().ok().unwrap_or_default()`,
/// so one panicking holder emitted an empty series — a zero no alert can tell
/// apart from a real one. Logging and still returning empty would not have
/// fixed it: a `Mutex` poison is permanent, so any site that skips on `Err`
/// would skip forever after one panic. Clearing the poison is the half that
/// keeps it alive.
///
/// Driven through the real router rather than by calling `snapshot_counters`
/// directly, for the same reason `metrics_local_fallback_recovers_poisoned_lock`
/// is: this must prove the `/metrics` RENDER recovers, not just the helper in
/// isolation.
#[tokio::test]
async fn poisoned_counter_lock_is_recovered_not_zeroed() {
    let state = test_state_with(vec![mk_endpoint("a", "sk-ant-api-aaa")]);
    state.record_stripped_body_fields("c1", &["speed".to_string()], &["fast-mode-x".to_string()]);

    // The only way a `Mutex` becomes poisoned: a holder panics. Poison it
    // directly so the recovery under test is the one in the render path.
    {
        let state = state.clone();
        std::thread::spawn(move || {
            let _g = state.beta_body_fields_stripped.lock().unwrap();
            panic!("deliberate: poison the stripped-field counter mutex");
        })
        .join()
        .unwrap_err();
    }
    assert!(state.beta_body_fields_stripped.is_poisoned());

    let addr = serve(build_router(state.clone())).await;
    let body = reqwest::Client::new()
        .get(format!("http://{addr}/metrics"))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(
        body.contains(r#"anthropic_beta_body_field_stripped_total{field="speed"} 1"#),
        "/metrics must publish the real count through a poisoned lock, not a zero \
         indistinguishable from a genuine one:\n{body}"
    );
    // Checked before the next write: the writer clears poison too, so only
    // here does this prove the render cleared it.
    assert!(
        !state.beta_body_fields_stripped.is_poisoned(),
        "the /metrics render must have cleared the poison"
    );

    state.record_stripped_body_fields("c1", &["speed".to_string()], &["fast-mode-x".to_string()]);
    assert_eq!(
        state
            .beta_body_fields_stripped
            .lock()
            .unwrap()
            .get("speed")
            .copied(),
        Some(2),
        "counting must resume once the poison is cleared"
    );
}

/// `client_rejections` has no `snapshot_counters` reader to clear its poison:
/// both its writer and the `/metrics` render skipped on `Err`, so one panicked
/// holder published an empty series and stopped counting for the life of the
/// process. Driven through the real `/metrics` render.
#[tokio::test]
async fn poisoned_client_rejections_lock_is_recovered_not_zeroed() {
    let state = test_state_with(vec![mk_endpoint("a", "sk-ant-api-aaa")]);
    state.note_client_rejection("client-a", "budget");
    poison(&state.client_rejections);

    let addr = serve(build_router(state.clone())).await;
    let body = reqwest::Client::new()
        .get(format!("http://{addr}/metrics"))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(
        body.contains(r#"anthropic_client_rejections_total{client="client-a",reason="budget"} 1"#),
        "/metrics must publish the real count through a poisoned lock:\n{body}"
    );
    assert!(!state.client_rejections.is_poisoned());

    state.note_client_rejection("client-a", "budget");
    assert_eq!(
        state
            .client_rejections
            .lock()
            .unwrap()
            .get(&("client-a".to_string(), "budget"))
            .copied(),
        Some(2),
        "counting must resume once the poison is cleared"
    );
}

/// Stripped field names are JSON object keys — client-controlled bytes with
/// none of the CR/LF guarantee hyper gives header tokens. Unsanitized they
/// reach a plain-text log subscriber, where an embedded newline forges whole
/// log lines, and they become `/metrics` label values.
#[test]
fn stripped_field_names_are_sanitized() {
    assert_eq!(sanitize_metric_key("speed", 64), "speed");
    assert_eq!(
        sanitize_metric_key("context_management", 64),
        "context_management"
    );
    assert_eq!(
        sanitize_metric_key("x\n2026-01-01 WARN forged: client_id=admin", 64),
        "_invalid",
        "a newline-bearing key must not reach a log field verbatim"
    );
    assert_eq!(sanitize_metric_key("", 64), "_invalid");
    // Non-ASCII survives truncation and is then rejected as a whole.
    assert_eq!(sanitize_metric_key("café", 64), "_invalid");
    // Truncation lands on a char boundary rather than splitting the codepoint
    // (which would panic); what is left is a legitimate key.
    assert_eq!(sanitize_metric_key("café", 4), "caf");
}

/// Adversarial-review probe (Helly R, delta pass on `7e2ebb5`), adopted as a
/// permanent regression. `strip_orphaned_beta_body_fields` hand-writes its
/// output object — keys re-escaped, values spliced as raw bytes — and manual
/// JSON emission is exactly where a key containing a quote or a newline, or a
/// value containing `}","model":"…`, gets a chance to break out of the string
/// it was written into. The `\u006dodel` case is the subtle one: serde decodes
/// the key, so an escaped spelling collapses onto a literal `model` already
/// present and the output carries the duplicate — legal JSON, last-wins at the
/// parser, and identical to how the upstream would have read the input.
///
/// The second loop pins the refusal cases: a body that is not a JSON object,
/// has nothing to remove, or does not parse must forward untouched rather than
/// be rewritten into something subtly different.
#[test]
fn raw_object_reemission_survives_hostile_keys_and_values() {
    let dropped = vec!["unknown-beta".to_string()];
    let cases = [
        (r#"{"orphan":1}"#, r#"{}"#),
        (
            r#"{"\u006dodel":"a","orphan":1,"model":"b"}"#,
            r#"{"model":"a","model":"b"}"#,
        ),
        (
            r#"{"messages":[],"orphan":1,"orphan":2}"#,
            r#"{"messages":[]}"#,
        ),
        (
            r#"{"model":"a","evil\"key\n":0,"messages":[ {"x":1,"x":2,"text":"}\",\"model\":\"injected"} ]}"#,
            r#"{"model":"a","messages":[ {"x":1,"x":2,"text":"}\",\"model\":\"injected"} ]}"#,
        ),
        (
            r#"{"metadata":{"n":18446744073709551617,"decimal":0.123456789012345678901,"minus":-0,"exp":1e+009,"text":"\u0061\/b"},"orphan":null}"#,
            r#"{"metadata":{"n":18446744073709551617,"decimal":0.123456789012345678901,"minus":-0,"exp":1e+009,"text":"\u0061\/b"}}"#,
        ),
        (
            r#"{"messages":[],"metadata":null,"stream":false,"orphan":[{"model":"no"}]}"#,
            r#"{"messages":[],"metadata":null,"stream":false}"#,
        ),
    ];
    for (input, expected) in cases {
        let (out, removed) = strip_orphaned_beta_body_fields(
            &bytes::Bytes::copy_from_slice(input.as_bytes()),
            "oauth-2025-04-20",
            &dropped,
        )
        .unwrap();
        assert_eq!(&out[..], expected.as_bytes(), "input: {input}");
        assert!(!removed.is_empty());
        assert!(serde_json::from_slice::<TopLevelObject>(&out).is_ok());
    }
    for input in [
        "{}",
        "[]",
        "null",
        r#"{"messages":[]}"#,
        r#"{"orphan":1,}"#,
        r#"{"orphan":1} {"model":"second"}"#,
        r#"{"messages":"\ud800","orphan":1}"#,
    ] {
        let result = strip_orphaned_beta_body_fields(
            &bytes::Bytes::copy_from_slice(input.as_bytes()),
            "oauth-2025-04-20",
            &dropped,
        );
        // RawValue may preserve a syntactically valid unpaired surrogate value;
        // it must never decode it into a different string.
        if input.contains("ud800") {
            if let Some((out, _)) = result {
                assert!(String::from_utf8_lossy(&out).contains("\\ud800"));
            }
        } else {
            assert!(result.is_none(), "input: {input}");
        }
    }
}

/// Adversarial-review probe (Helly R, delta pass), adopted. Positive controls
/// for the two policy rules, stated as behaviour rather than as the absence of
/// the old defects: a known surviving family keeps its field while an orphan
/// in the same body goes, an unrecognised surviving family disables the strip
/// regardless of where it sits in the header, and the counter totals exactly
/// across batches above and below the per-request cap (0 + 8 + 9 + 100 = 117
/// removals, overflow included).
#[test]
fn surviving_families_and_strip_counter_positive_controls() {
    let input = bytes::Bytes::from_static(br#"{"fallback_credit_token":{"token":"credit","mode":"strict"},"speed":"fast","orphan":1}"#);
    let dropped = vec!["unknown-beta".to_string()];
    let (out, removed) = strip_orphaned_beta_body_fields(
        &input,
        " fallback-credit-2026-07-01,fast-mode-2026-02-01,oauth-2025-04-20 ",
        &dropped,
    )
    .unwrap();
    assert_eq!(removed, vec!["orphan"]);
    assert_eq!(
        &out[..],
        br#"{"fallback_credit_token":{"token":"credit","mode":"strict"},"speed":"fast"}"#
    );
    for flags in [
        "mcp-client-2025-11-20,oauth-2025-04-20",
        "oauth-2025-04-20,mcp-client-2025-11-20",
        "fast-mode-2026-02-01,future-beta",
    ] {
        assert!(strip_orphaned_beta_body_fields(&input, flags, &dropped).is_none());
    }
    let state = test_state_with(vec![]);
    for n in [0, 8, 9, 100] {
        let removed = (0..n).map(|i| format!("key_{i}")).collect::<Vec<_>>();
        state.record_stripped_body_fields("test", &removed, &dropped);
    }
    assert_eq!(
        state
            .beta_body_fields_stripped
            .lock()
            .unwrap()
            .values()
            .sum::<u64>(),
        117
    );
}

/// Adversarial-review probe (Helly R, delta pass), adopted. End-to-end through
/// the router against a no-rewrite control, which is the part the unit tests
/// cannot show: that the bytes reaching the upstream are the client's own.
///
/// It also closes a blind spot the fix itself created — now that a surviving
/// `fallback-credit-*` protects its token, the original defect-asserting input
/// no longer triggers a rewrite at all, so an orphan (`unknown_config`) has to
/// be added deliberately and the strip counter checked, or this would pass by
/// doing nothing.
#[tokio::test]
async fn full_router_rewrite_preserves_client_bytes() {
    let seen = Arc::new(Mutex::new(Vec::<String>::new()));
    let sink = seen.clone();
    let upstream = serve(Router::new().fallback(any(move |body: bytes::Bytes| {
        let sink = sink.clone();
        async move {
            sink.lock()
                .unwrap()
                .push(String::from_utf8(body.to_vec()).unwrap());
            (
                [("content-type", "application/json")],
                r#"{"type":"message","content":[],"usage":{"input_tokens":1,"output_tokens":1}}"#,
            )
        }
    })))
    .await;
    let mut ep = mk_endpoint("acct", &oauth_shaped_fixture_token());
    ep.base_url = format!("http://{upstream}");
    let state = Arc::new(AppState {
        endpoints: vec![ep],
        auto_cache: false,
        ..test_state_base()
    });
    let addr = serve(build_router(state.clone())).await;
    let prefix = r#"{"model":"claude-opus-4-7","system":"You are Claude Code, Anthropic's official CLI for Claude.","messages":[ {"role":"assistant","content":[{"type":"tool_use","id":"t","name":"lookup","input":{"record_id":18446744073709551617,"text":"\u0061"}}]}, {"role":"user","content":[{"type":"tool_result","tool_use_id":"t","content":"found"}]} ],"max_tokens":1,"fallback_credit_token":"credit""#;
    let control = format!("{prefix}}}");
    let orphan = format!("{prefix},\"unknown_config\":{{\"on\":true}}}}");
    let client = Client::new();
    for body in [&control, &orphan] {
        let resp = client
            .post(format!("http://{addr}/v1/messages"))
            .header("content-type", "application/json")
            .header(
                "anthropic-beta",
                "fallback-credit-2026-07-01,totally-made-up-beta-2026-01-01",
            )
            .body(body.clone())
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let _ = resp.bytes().await.unwrap();
    }
    let captured = seen.lock().unwrap();
    assert_eq!(captured[0], control);
    assert_eq!(captured[1], control);
    assert_eq!(
        state
            .beta_body_fields_stripped
            .lock()
            .unwrap()
            .get("unknown_config"),
        Some(&1)
    );
}
