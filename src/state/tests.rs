use super::*;

#[test]
fn content_fingerprint_stable_across_growing_turns() {
    // Same system + same first user, with later turns appended, must yield the
    // SAME fingerprint. Hashing the growing body would change every turn —
    // which is exactly the migration we must avoid for headerless sessions.
    let system = serde_json::json!([{"type":"text","text":"You are Claude Code."}]);
    let turn1 = serde_json::json!({
        "system": system,
        "messages": [{"role":"user","content":"implement feature X"}],
    });
    let turn3 = serde_json::json!({
        "system": system,
        "messages": [
            {"role":"user","content":"implement feature X"},
            {"role":"assistant","content":"ok, on it"},
            {"role":"user","content":"now add tests (this grows the tail)"},
        ],
    });
    let (fp1, _) = content_fingerprints(&turn1);
    let (fp3, _) = content_fingerprints(&turn3);
    assert_eq!(
        fp1, fp3,
        "fingerprint must be stable across a conversation's growing turns"
    );
}

#[test]
fn content_fingerprint_separates_first_user_but_system_only_collides() {
    // Two agents share a system prompt but seed different first tasks. The
    // system+first-user fingerprint must DIFFER (so they route independently);
    // the system-only fingerprint must COLLIDE — demonstrating why first-user
    // must be included (system-only would herd the whole fleet onto one key).
    let system = serde_json::json!([{"type":"text","text":"shared harness system prompt"}]);
    let a = serde_json::json!({"system": system, "messages":[{"role":"user","content":"task A"}]});
    let b = serde_json::json!({"system": system, "messages":[{"role":"user","content":"task B"}]});
    let (fp_a, fps_a) = content_fingerprints(&a);
    let (fp_b, fps_b) = content_fingerprints(&b);
    assert_ne!(
        fp_a, fp_b,
        "different first tasks must produce different fingerprints"
    );
    assert_eq!(
        fps_a, fps_b,
        "system-only fingerprints must collide (why we include the first user turn)"
    );
}

#[test]
fn content_fingerprint_handles_missing_and_empty_fields_without_panic() {
    // This runs on every headerless request, parsing untrusted bodies — it
    // must never panic and must stay deterministic on malformed input.
    let empty = serde_json::json!({});
    let (fp, fps) = content_fingerprints(&empty);
    assert_eq!(fp.len(), 12, "fp must be a well-formed 12-hex digest");
    assert_eq!(fps.len(), 12);
    // Deterministic for identical (degenerate) input.
    assert_eq!(content_fingerprints(&empty), (fp.clone(), fps.clone()));
    // Missing "messages" and a system with no extractable text / wrong types.
    let no_msgs = serde_json::json!({"system": []});
    let weird = serde_json::json!({"system": 42, "messages": "not-an-array"});
    let _ = content_fingerprints(&no_msgs);
    let _ = content_fingerprints(&weird);
    // No user message present -> first-user contributes empty, still stable.
    let no_user =
        serde_json::json!({"system":"S","messages":[{"role":"assistant","content":"hi"}]});
    assert_eq!(
        content_fingerprints(&no_user),
        content_fingerprints(&no_user)
    );
}

#[test]
fn content_fingerprint_handles_string_content_format() {
    // Covers the string (non-array) branch for both system and content. The
    // SDK fleet may send either form; both must yield a usable fingerprint
    // that still separates different first tasks.
    let a = serde_json::json!({"system":"sys", "messages":[{"role":"user","content":"task A"}]});
    let b = serde_json::json!({"system":"sys", "messages":[{"role":"user","content":"task B"}]});
    let (fp_a, fps_a) = content_fingerprints(&a);
    let (fp_b, fps_b) = content_fingerprints(&b);
    assert_ne!(fp_a, fp_b, "string-form first tasks must still separate");
    assert_eq!(fps_a, fps_b, "string-form shared system must still collide");
}

#[test]
fn prefix_breakpoints_capture_hierarchy_and_first_is_turn_stable() {
    let cc = serde_json::json!({"type": "ephemeral"});
    // Turn 1: system has a cache_control breakpoint; the (only) user turn has one.
    let turn1 = serde_json::json!({
        "system": [{"type":"text","text":"STABLE-SYSTEM","cache_control": cc}],
        "messages": [{"role":"user","content":[{"type":"text","text":"U1","cache_control": cc}]}],
    });
    // A later turn: SAME system breakpoint, but the conversation grew and the
    // cache_control moved to a new last user turn.
    let turn3 = serde_json::json!({
        "system": [{"type":"text","text":"STABLE-SYSTEM","cache_control": cc}],
        "messages": [
            {"role":"user","content":[{"type":"text","text":"U1"}]},
            {"role":"assistant","content":[{"type":"text","text":"A1"}]},
            {"role":"user","content":[{"type":"text","text":"U2-grown","cache_control": cc}]},
        ],
    });
    let b1 = prefix_breakpoint_hashes(&turn1);
    let b3 = prefix_breakpoint_hashes(&turn3);
    assert_eq!(b1.len(), 2, "turn1 has 2 cache_control breakpoints");
    assert_eq!(b3.len(), 2, "turn3 has 2 cache_control breakpoints");
    // Positions are monotonically increasing (prefix grows).
    assert!(b1[0].0 < b1[1].0, "breakpoint positions must increase");
    // FIRST breakpoint (the stable system prefix) is identical across turns —
    // this is the turn-stable level. The naive system+first-user hash can't
    // isolate it.
    assert_eq!(
        b1[0].1, b3[0].1,
        "first breakpoint (system) must be turn-stable across growing turns"
    );
    // LAST breakpoint differs (the conversation tail grew).
    assert_ne!(
        b1[1].1, b3[1].1,
        "last breakpoint must change as the conversation grows"
    );
}

#[test]
fn prefix_breakpoints_preserve_block_structure() {
    // Same raw text, different block structure → MUST yield different digests,
    // otherwise the offline analysis would overstate cache reuse. One block
    // "AB" vs two blocks "A","B".
    let cc = serde_json::json!({"type": "ephemeral"});
    let one = serde_json::json!({
        "messages": [{"role":"user","content":[{"type":"text","text":"AB","cache_control": cc}]}],
    });
    let two = serde_json::json!({
        "messages": [{"role":"user","content":[
            {"type":"text","text":"A"},
            {"type":"text","text":"B","cache_control": cc},
        ]}],
    });
    let a = prefix_breakpoint_hashes(&one);
    let b = prefix_breakpoint_hashes(&two);
    assert_eq!(a.len(), 1);
    assert_eq!(b.len(), 1);
    assert_ne!(
        a[0].1, b[0].1,
        "different block structure with identical text must not collide"
    );
}

#[test]
fn prefix_breakpoints_empty_when_no_cache_control() {
    let body = serde_json::json!({
        "system": "plain",
        "messages": [{"role":"user","content":"hi"}],
    });
    assert!(
        prefix_breakpoint_hashes(&body).is_empty(),
        "no cache_control → no breakpoints"
    );
}

// ── P1-01: in-flight request-body memory admission ──────────────────

/// Reservation accounting: reserve adds, over-budget sheds (None), drop releases.
#[test]
fn body_reservation_accounts_and_releases() {
    use std::sync::atomic::Ordering::Relaxed;
    let state = Arc::new(AppState {
        max_inflight_body_bytes: 100,
        ..test_state_base()
    });
    {
        let r1 = state.try_reserve_body(60).expect("60 fits in budget 100");
        assert_eq!(state.inflight_body_bytes.load(Relaxed), 60);
        assert!(
            state.try_reserve_body(60).is_none(),
            "60+60 exceeds budget 100 → must be shed (None)"
        );
        let r2 = state
            .try_reserve_body(40)
            .expect("60+40 == 100 fits exactly");
        assert_eq!(state.inflight_body_bytes.load(Relaxed), 100);
        drop(r2);
        assert_eq!(
            state.inflight_body_bytes.load(Relaxed),
            60,
            "dropping a reservation releases its bytes"
        );
        let _ = &r1;
    }
    assert_eq!(
        state.inflight_body_bytes.load(Relaxed),
        0,
        "all reservations released after scope"
    );
}

/// A request whose body would exceed the in-flight memory budget is shed with
/// a retryable 503 + Retry-After, not buffered (the P1-01 OOM backstop).
#[tokio::test]
async fn body_memory_budget_sheds_oversized_with_503() {
    let (url, _h) = spawn_mock_upstream().await;
    let state = Arc::new(AppState {
        endpoints: vec![mk_endpoint_at("a", "sk-ant-api-aaa", &url)],
        max_inflight_body_bytes: 8, // any real request body exceeds 8 bytes
        ..test_state_base()
    });
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
        reqwest::StatusCode::SERVICE_UNAVAILABLE,
        "a body over the in-flight memory budget must be shed with 503, not buffered"
    );
    assert!(
        resp.headers().get("retry-after").is_some(),
        "memory-pressure 503 must carry Retry-After"
    );
}

/// Control: with a generous budget the same request is served normally — the
/// limiter must not throttle traffic that fits (no false-positive shed).
#[tokio::test]
async fn body_memory_budget_admits_request_that_fits() {
    let (url, _h) = spawn_mock_upstream().await;
    let state = Arc::new(AppState {
        endpoints: vec![mk_endpoint_at("a", "sk-ant-api-aaa", &url)],
        max_inflight_body_bytes: 64 * 1024 * 1024,
        ..test_state_base()
    });
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
        reqwest::StatusCode::OK,
        "a request within the budget must be served, not shed"
    );
}

/// The OpenAI-compat handler shares the same body-memory admission backstop.
#[tokio::test]
async fn openai_handler_body_budget_sheds_with_503() {
    let (url, _h) = spawn_mock_upstream().await;
    let state = Arc::new(AppState {
        endpoints: vec![mk_endpoint_at("a", "sk-ant-api-aaa", &url)],
        max_inflight_body_bytes: 8,
        ..test_state_base()
    });
    let addr = serve(build_router(state)).await;
    let resp = reqwest::Client::new()
            .post(format!("http://{addr}/v1/chat/completions"))
            .header("content-type", "application/json")
            .body(r#"{"model":"claude-sonnet-4-6","messages":[{"role":"user","content":"hi"}],"max_tokens":1}"#)
            .send()
            .await
            .unwrap();
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::SERVICE_UNAVAILABLE,
        "openai-compat path must share the body-memory load-shed"
    );
    assert!(resp.headers().get("retry-after").is_some());
}

/// The body-memory backstop must be observable so the budget can be tuned
/// from measured peak rather than guessed: a gauge for current in-flight body
/// bytes, a gauge for the configured limit, and a counter that increments on
/// each load-shed.
#[tokio::test]
async fn metrics_expose_body_budget_and_shed_counter() {
    let (url, _h) = spawn_mock_upstream().await;
    let state = Arc::new(AppState {
        endpoints: vec![mk_endpoint_at("a", "sk-ant-api-aaa", &url)],
        max_inflight_body_bytes: 8, // force a shed
        ..test_state_base()
    });
    let addr = serve(build_router(state)).await;
    let c = reqwest::Client::new();
    let shed = c
        .post(format!("http://{addr}/v1/messages"))
        .header("content-type", "application/json")
        .body(r#"{"model":"test","max_tokens":1,"messages":[{"role":"user","content":"hi"}]}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(shed.status(), reqwest::StatusCode::SERVICE_UNAVAILABLE);
    let m = c
        .get(format!("http://{addr}/metrics"))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(
        m.contains("anthropic_inflight_body_bytes"),
        "current in-flight body-bytes gauge must be exported:\n{m}"
    );
    assert!(
        m.contains("anthropic_inflight_body_limit_bytes"),
        "the configured budget must be exported as a gauge:\n{m}"
    );
    assert!(
        m.contains("anthropic_body_shed_total 1"),
        "shed counter must increment after a load-shed:\n{m}"
    );
}

/// A stalled upload (partial body, connection held open) must be shed with
/// `408` when `body_read_timeout` elapses, releasing its body-memory
/// reservation and incrementing the timeout counter — otherwise slow-loris
/// bodies pin the P1-01 budget indefinitely.
#[tokio::test]
async fn body_read_timeout_sheds_stalled_body_with_408() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let (url, _h) = spawn_mock_upstream().await;
    let state = Arc::new(AppState {
        endpoints: vec![mk_endpoint_at("a", "sk-ant-api-aaa", &url)],
        max_inflight_body_bytes: 1024 * 1024,
        body_read_timeout: Duration::from_millis(200),
        ..test_state_base()
    });
    let addr = serve(build_router(state.clone())).await;

    let mut sock = tokio::net::TcpStream::connect(addr).await.unwrap();
    sock.write_all(
        b"POST /v1/messages HTTP/1.1\r\n\
              Host: localhost\r\n\
              Content-Type: application/json\r\n\
              Content-Length: 4096\r\n\
              \r\n\
              {\"model\":\"test\"",
    )
    .await
    .unwrap();
    // Send nothing further — the handler must time out rather than wait
    // for the remaining 4080 bytes forever.
    let mut buf = vec![0u8; 1024];
    let n = tokio::time::timeout(Duration::from_secs(5), sock.read(&mut buf))
        .await
        .expect("server must respond within the timeout, not hang")
        .unwrap();
    let resp = String::from_utf8_lossy(&buf[..n]);
    assert!(
        resp.starts_with("HTTP/1.1 408"),
        "stalled body must be shed with 408, got: {resp}"
    );
    assert_eq!(
        state.inflight_body_bytes.load(Ordering::Relaxed),
        0,
        "timing out must release the body-memory reservation"
    );
    assert_eq!(state.body_read_timeout_total.load(Ordering::Relaxed), 1);

    let m = reqwest::Client::new()
        .get(format!("http://{addr}/metrics"))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(
        m.contains("anthropic_body_read_timeout_total 1"),
        "timeout counter must be exported:\n{m}"
    );
}

/// The OpenAI-compat handler shares the same body-read timeout guard.
#[tokio::test]
async fn openai_handler_body_read_timeout_sheds_with_408() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let (url, _h) = spawn_mock_upstream().await;
    let state = Arc::new(AppState {
        endpoints: vec![mk_endpoint_at("a", "sk-ant-api-aaa", &url)],
        max_inflight_body_bytes: 1024 * 1024,
        body_read_timeout: Duration::from_millis(200),
        ..test_state_base()
    });
    let addr = serve(build_router(state.clone())).await;

    let mut sock = tokio::net::TcpStream::connect(addr).await.unwrap();
    sock.write_all(
        b"POST /v1/chat/completions HTTP/1.1\r\n\
              Host: localhost\r\n\
              Content-Type: application/json\r\n\
              Content-Length: 4096\r\n\
              \r\n\
              {\"model\":\"test\"",
    )
    .await
    .unwrap();
    let mut buf = vec![0u8; 1024];
    let n = tokio::time::timeout(Duration::from_secs(5), sock.read(&mut buf))
        .await
        .expect("server must respond within the timeout, not hang")
        .unwrap();
    let resp = String::from_utf8_lossy(&buf[..n]);
    assert!(
        resp.starts_with("HTTP/1.1 408"),
        "openai-compat path must share the body-read timeout, got: {resp}"
    );
    assert_eq!(state.inflight_body_bytes.load(Ordering::Relaxed), 0);
}

// ── Ewma tests ─────────────────────────────────────────────────

#[test]
fn ewma_single_update() {
    let start = Instant::now();
    let mut ewma = Ewma {
        value: 0.0,
        tau: TAU_5M,
        last_update: start,
    };
    // Simulate one request after 60 seconds
    let now = start + Duration::from_secs(60);
    let rate = ewma.update(now);
    // instant_rate = 60/60 = 1.0 req/min
    // alpha = 1 - exp(-60/300) = ~0.1813
    // value = 0.1813 * 1.0 + 0.8187 * 0.0 = ~0.1813
    assert!(
        rate > 0.15 && rate < 0.25,
        "rate should be ~0.18, got {rate}"
    );
}

#[test]
fn ewma_burst() {
    let start = Instant::now();
    let mut ewma = Ewma {
        value: 0.0,
        tau: TAU_5M,
        last_update: start,
    };
    // 10 rapid requests, 100ms apart
    for i in 1..=10 {
        let now = start + Duration::from_millis(i * 100);
        ewma.update(now);
    }
    // instant_rate per update = 60/0.1 = 600 req/min
    // After 10 updates, value should be significantly elevated
    assert!(
        ewma.value() > 1.0,
        "burst rate should be high, got {}",
        ewma.value()
    );
}

#[test]
fn ewma_decay() {
    let start = Instant::now();
    let mut ewma = Ewma {
        value: 100.0,
        tau: TAU_5M,
        last_update: start,
    };
    // One update after 5 minutes (one full tau)
    let now = start + Duration::from_secs(300);
    let rate = ewma.update(now);
    // alpha = 1 - exp(-300/300) = 1 - 1/e ≈ 0.6321
    // instant_rate = 60/300 = 0.2
    // value = 0.6321*0.2 + 0.3679*100 = 0.126 + 36.79 ≈ 36.9
    // The old value decays significantly
    assert!(rate < 50.0, "should decay from 100, got {rate}");
    assert!(rate > 20.0, "should retain some memory, got {rate}");
}

#[test]
fn ewma_stale_reset() {
    let start = Instant::now();
    let mut ewma = Ewma {
        value: 42.0,
        tau: TAU_5M,
        last_update: start,
    };
    // Update after 2 hours (well beyond EWMA_STALE_SECS)
    let now = start + Duration::from_secs(7200);
    let rate = ewma.update(now);
    assert_eq!(rate, 0.0, "stale EWMA should reset to 0");
}

#[test]
fn ewma_zero_elapsed() {
    let start = Instant::now();
    let mut ewma = Ewma {
        value: 10.0,
        tau: TAU_5M,
        last_update: start,
    };
    // Same instant — elapsed clamped to EWMA_MIN_ELAPSED_SECS
    let rate = ewma.update(start);
    assert!(rate.is_finite(), "zero elapsed should not produce NaN/inf");
    assert!(rate > 0.0, "should have a positive value");
}

#[test]
fn ewma_nan_guard() {
    let start = Instant::now();
    let mut ewma = Ewma {
        value: f64::NAN,
        tau: TAU_5M,
        last_update: start,
    };
    let now = start + Duration::from_secs(1);
    let rate = ewma.update(now);
    assert!(
        rate.is_finite(),
        "NaN input should be recovered to finite value"
    );
}

// ── BurnRate tests ─────────────────────────────────────────────

#[test]
fn burn_rate_single_request() {
    let start = Instant::now();
    let mut br = BurnRate {
        rate_5m: Ewma {
            value: 0.0,
            tau: TAU_5M,
            last_update: start,
        },
        rate_1h: Ewma {
            value: 0.0,
            tau: TAU_1H,
            last_update: start,
        },
        rate_6h: Ewma {
            value: 0.0,
            tau: TAU_6H,
            last_update: start,
        },
    };
    let now = start + Duration::from_secs(60);
    br.update(now);
    let (r5m, r1h, r6h) = br.rates();
    // All should be positive after one request
    assert!(r5m > 0.0, "5m rate should be > 0");
    assert!(r1h > 0.0, "1h rate should be > 0");
    assert!(r6h > 0.0, "6h rate should be > 0");
    // Shorter tau → higher alpha → more responsive
    assert!(r5m > r1h, "5m rate should be more responsive than 1h");
    assert!(r1h > r6h, "1h rate should be more responsive than 6h");
}

#[test]
fn burn_rate_burst() {
    let start = Instant::now();
    let mut br = BurnRate {
        rate_5m: Ewma {
            value: 0.0,
            tau: TAU_5M,
            last_update: start,
        },
        rate_1h: Ewma {
            value: 0.0,
            tau: TAU_1H,
            last_update: start,
        },
        rate_6h: Ewma {
            value: 0.0,
            tau: TAU_6H,
            last_update: start,
        },
    };
    // 10 requests, 1 second apart
    for i in 1..=10 {
        br.update(start + Duration::from_secs(i));
    }
    let (r5m, _r1h, r6h) = br.rates();
    // 5m window should spike much higher than 6h window
    assert!(
        r5m > r6h * 2.0,
        "5m should spike much more than 6h: 5m={r5m}, 6h={r6h}"
    );
}

#[test]
fn burn_rate_decay() {
    let start = Instant::now();
    let mut br = BurnRate {
        rate_5m: Ewma {
            value: 60.0,
            tau: TAU_5M,
            last_update: start,
        },
        rate_1h: Ewma {
            value: 60.0,
            tau: TAU_1H,
            last_update: start,
        },
        rate_6h: Ewma {
            value: 60.0,
            tau: TAU_6H,
            last_update: start,
        },
    };
    // One update after 5 minutes of silence
    let now = start + Duration::from_secs(300);
    br.update(now);
    let (r5m, _r1h, r6h) = br.rates();
    // 5m should have decayed much more than 6h
    assert!(
        r5m < r6h,
        "5m should decay faster than 6h: 5m={r5m}, 6h={r6h}"
    );
}

// ── compute_pressure_status tests ──

#[test]
fn pressure_status_healthy() {
    let state = test_state_with(vec![]);
    assert_eq!(compute_pressure_status(0.0, "client1", &state), "healthy");
    assert_eq!(compute_pressure_status(0.50, "client1", &state), "healthy");
    assert_eq!(compute_pressure_status(0.69, "client1", &state), "healthy");
}

#[test]
fn pressure_status_elevated() {
    let state = test_state_with(vec![]);
    assert_eq!(compute_pressure_status(0.70, "client1", &state), "elevated");
    assert_eq!(compute_pressure_status(0.80, "client1", &state), "elevated");
    assert_eq!(compute_pressure_status(0.84, "client1", &state), "elevated");
}

#[test]
fn pressure_status_critical() {
    let state = test_state_with(vec![]);
    assert_eq!(compute_pressure_status(0.85, "client1", &state), "critical");
    assert_eq!(compute_pressure_status(0.90, "client1", &state), "critical");
    assert_eq!(compute_pressure_status(0.94, "client1", &state), "critical");
}

#[test]
fn pressure_status_emergency() {
    let state = test_state_with(vec![]);
    assert_eq!(
        compute_pressure_status(0.95, "client1", &state),
        "emergency"
    );
    assert_eq!(compute_pressure_status(1.0, "client1", &state), "emergency");
}

#[test]
fn pressure_status_operator_always_healthy() {
    let state = Arc::new(AppState {
        state_path: PathBuf::from("/tmp/test.state.json"),
        operators: vec!["ray".to_string()],
        ..test_state_base()
    });
    // Operator always gets "healthy" regardless of utilization
    assert_eq!(compute_pressure_status(0.99, "ray", &state), "healthy");
    assert_eq!(compute_pressure_status(1.0, "ray", &state), "healthy");
    // Non-operator at same utilization gets emergency
    assert_eq!(compute_pressure_status(0.99, "other", &state), "emergency");
}

#[test]
fn pressure_status_upgrade_near_client_limit() {
    let mut limits = HashMap::new();
    limits.insert("gastown".to_string(), 0.85);
    let state = Arc::new(AppState {
        state_path: PathBuf::from("/tmp/test.state.json"),
        client_utilization_limits: limits,
        ..test_state_base()
    });
    // gastown has limit 0.85, 80% of that = 0.68
    // At 0.60, below 0.68 → no upgrade → "healthy"
    assert_eq!(compute_pressure_status(0.60, "gastown", &state), "healthy");
    // At 0.69, above 0.68 → upgrade healthy→elevated
    assert_eq!(compute_pressure_status(0.69, "gastown", &state), "elevated");
    // At 0.70, already elevated, above 0.68 → upgrade elevated→critical
    assert_eq!(compute_pressure_status(0.70, "gastown", &state), "critical");
    // Client without limits: no upgrade
    assert_eq!(compute_pressure_status(0.69, "other", &state), "healthy");
}

#[test]
fn compute_pressure_status_operator_always_healthy() {
    let state = Arc::new(AppState {
        client: Client::new(),
        client_nonstreaming: Client::new(),
        state_path: PathBuf::from("/tmp/test.state.json"),
        operators: vec!["operator-id".to_string()],
        ..test_state_base()
    });

    let status = compute_pressure_status(0.99, "operator-id", &state);
    assert_eq!(
        status, "healthy",
        "operator should always see healthy status"
    );
}

#[test]
fn compute_pressure_status_thresholds() {
    let state = test_state_with(vec![]);

    assert_eq!(compute_pressure_status(0.50, "client", &state), "healthy");
    assert_eq!(compute_pressure_status(0.75, "client", &state), "elevated");
    assert_eq!(compute_pressure_status(0.90, "client", &state), "critical");
    assert_eq!(compute_pressure_status(0.99, "client", &state), "emergency");
}

#[test]
fn compute_pressure_status_limit_proximity_upgrade() {
    let mut limits = HashMap::new();
    limits.insert("client".to_string(), 0.80);
    let state = Arc::new(AppState {
        client: Client::new(),
        client_nonstreaming: Client::new(),
        state_path: PathBuf::from("/tmp/test.state.json"),
        client_utilization_limits: limits,
        ..test_state_base()
    });

    // 0.65 is > 80% of 0.80 limit (80% * 0.80 = 0.64), so should upgrade from healthy to elevated
    let status = compute_pressure_status(0.65, "client", &state);
    assert_eq!(
        status, "elevated",
        "proximity to limit should upgrade status"
    );
}

/// A response carrying a distinct `representative-claim`, with a reset inside
/// the parser's 7d sanity window so the entry actually persists. A literal far
/// future epoch is silently discarded by that cap, which would leave the claim
/// reset-less and the test asserting less than it looks like it asserts.
fn claim_headers(claim: Option<&str>, util: &str, now_epoch: u64) -> reqwest::header::HeaderMap {
    let mut headers = reqwest::header::HeaderMap::new();
    if let Some(c) = claim {
        headers.insert(
            "anthropic-ratelimit-unified-representative-claim",
            HeaderValue::from_str(c).unwrap(),
        );
    }
    headers.insert(
        "anthropic-ratelimit-unified-7d-utilization",
        HeaderValue::from_str(util).unwrap(),
    );
    headers.insert(
        "anthropic-ratelimit-unified-7d-reset",
        HeaderValue::from_str(&(now_epoch + 3600).to_string()).unwrap(),
    );
    headers
}

/// AC-4: claim keys are minted from the upstream response header, never from
/// client input — and even that upstream string cannot mint unbounded series.
/// A hostile upstream (reachable via the redirect-with-credentials path) is the
/// threat model; CWE-770.
#[tokio::test]
async fn claim_keys_cannot_mint_unbounded_series() {
    let state = test_state_with(vec![mk_endpoint("acct-a", "sk-ant-api-test-aaa")]);
    let now_epoch = AppState::now_epoch();

    // Oversized key FIRST, while the map has room: this exercises truncation,
    // not the cap. (Sent after the flood it would be refused outright, and the
    // truncation assertion would pass even with `truncate_label` deleted.)
    let oversized = format!("seven_day_{}", "x".repeat(4096));
    let expected: String = oversized
        .chars()
        .take(MAX_LABEL_CHARS)
        .chain(['…'])
        .collect();
    state
        .update_rate_info(0, &claim_headers(Some(&oversized), "0.50", now_epoch))
        .await;
    {
        let info = state.endpoints[0].rate_info.read().await;
        assert!(
            info.claims_7d.contains_key(&expected),
            "the oversized key must be stored truncated, got {:?}",
            info.claims_7d.keys().collect::<Vec<_>>()
        );
        assert!(
            !info.claims_7d.contains_key(&oversized),
            "the untruncated key must never be retained"
        );
        assert_eq!(
            info.representative_claim.as_deref(),
            Some(expected.as_str()),
            "representative_claim must be truncated too, or refresh_metrics_weights \
             looks up a key that cannot exist"
        );
    }

    // Now flood far past the cap.
    for i in 0..200 {
        let claim = format!("seven_day_junk{i}");
        state
            .update_rate_info(0, &claim_headers(Some(&claim), "0.50", now_epoch))
            .await;
    }

    let info = state.endpoints[0].rate_info.read().await;
    let unreserved = info
        .claims_7d
        .keys()
        .filter(|k| !claim_key_is_reserved(k))
        .count();
    assert!(
        unreserved <= MAX_CLAIMS_PER_ACCOUNT,
        "unreserved claim keys must stay bounded, got {unreserved}"
    );
    assert!(
        info.claims_7d
            .keys()
            .all(|k| k.chars().count() <= MAX_LABEL_CHARS + 1),
        "every claim key must be truncated to a bounded label length"
    );
}

/// AC-4, the failure mode the cap itself creates. `effective_utilization` skips
/// its flat-field fallback whenever `claims_7d` is non-empty, so an account
/// whose map is full of unknown keys with `seven_day` refused would derive NO
/// weekly utilization — routing as though it had no weekly limit, and dropping
/// out of the emergency brake's all-saturated test at 100%.
///
/// Order matters: the flood runs FIRST, so the reserved keys arrive against an
/// already-full map. Seeded the other way round this passes even unfixed.
#[tokio::test]
async fn claim_cap_never_refuses_a_routing_relevant_claim() {
    let state = test_state_with(vec![mk_endpoint("acct-a", "sk-ant-api-test-aaa")]);
    let now_epoch = AppState::now_epoch();

    for i in 0..MAX_CLAIMS_PER_ACCOUNT * 2 {
        let claim = format!("seven_day_junk{i}");
        state
            .update_rate_info(0, &claim_headers(Some(&claim), "0.10", now_epoch))
            .await;
    }

    // No representative-claim header → the parser's `seven_day` default, the
    // key `claim_gates_all_traffic` feeds to the brake.
    state
        .update_rate_info(0, &claim_headers(None, "0.77", now_epoch))
        .await;

    // The Fable band arrives against the same full map.
    let mut band = reqwest::header::HeaderMap::new();
    band.insert(
        "anthropic-ratelimit-unified-7d_oi-utilization",
        HeaderValue::from_static("0.95"),
    );
    band.insert(
        "anthropic-ratelimit-unified-7d_oi-reset",
        HeaderValue::from_str(&(now_epoch + 3600).to_string()).unwrap(),
    );
    band.insert(
        "anthropic-ratelimit-unified-7d_oi-status",
        HeaderValue::from_static("rejected"),
    );
    state.update_rate_info(0, &band).await;

    let info = state.endpoints[0].rate_info.read().await;
    assert_eq!(
        info.claims_7d.get("seven_day").and_then(|c| c.utilization),
        Some(0.77),
        "the all-traffic claim must be admitted against a full map — without it \
         the account derives no weekly utilization and routes as unconstrained"
    );
    assert_eq!(
        info.claims_7d
            .get(FABLE_BAND_CLAIM)
            .and_then(|c| c.status.as_deref()),
        Some("rejected"),
        "the Fable band must be admitted even with the cap full"
    );
    // The point of the two assertions above: routing still sees a weekly figure.
    let (_, window, _, adj_7d) = effective_utilization(&info, now_epoch, "");
    assert!(
        adj_7d.is_some(),
        "a flooded account must still derive a 7d utilization (window={window})"
    );
}

/// AC-4 at the two ingest points that bypass the header parser entirely: the
/// persisted state file and the Redis mirror both assign `claims_7d` whole. A
/// map written by a pre-cap build must not restore unbounded.
#[test]
fn ingested_claims_are_bounded_and_truncated() {
    let mut raw: HashMap<String, ClaimWindowData> = HashMap::new();
    for i in 0..500 {
        raw.insert(format!("seven_day_junk{i}"), ClaimWindowData::default());
    }
    let oversized = format!("seven_day_{}", "y".repeat(4096));
    raw.insert(oversized.clone(), ClaimWindowData::default());
    // Reserved keys, deliberately sorting AFTER the junk so a naive
    // "keep the first N sorted" would drop them.
    for key in ["seven_day", "seven_day_sonnet", FABLE_BAND_CLAIM] {
        raw.insert(key.to_string(), ClaimWindowData::default());
    }

    let bounded = bound_ingested_claims(raw);

    let unreserved = bounded.keys().filter(|k| !claim_key_is_reserved(k)).count();
    assert_eq!(
        unreserved, MAX_CLAIMS_PER_ACCOUNT,
        "unreserved keys must be trimmed to exactly the cap"
    );
    for key in ["seven_day", "seven_day_sonnet", FABLE_BAND_CLAIM] {
        assert!(
            bounded.contains_key(key),
            "reserved key {key} must survive trimming"
        );
    }
    assert!(
        !bounded.contains_key(&oversized),
        "an untruncated key must not survive ingest"
    );
    assert!(
        bounded
            .keys()
            .all(|k| k.chars().count() <= MAX_LABEL_CHARS + 1),
        "every ingested key must be truncated"
    );
}

/// The `claim` label was flagged as a Prometheus exposition-injection vector.
/// It is not: `prom_gauge` puts every label value through `prom_escape`. This
/// asserts that end to end on the rendered body rather than on the helper
/// (`prometheus_label_escaping` already covers the helper), using a worse input
/// than the header path can actually deliver — written straight into
/// `claims_7d` to model a poisoned state file or Redis mirror, since an HTTP
/// `HeaderValue` rejects the newline the attack needs in the first place.
#[tokio::test]
async fn hostile_claim_key_cannot_forge_metric_lines() {
    let (mock_url, _handle) = spawn_mock_upstream().await;
    let (app, state) = test_app(&mock_url, None);
    let now_epoch = AppState::now_epoch();

    // Closes the label, emits a value, opens a forged metric, and trails a
    // backslash to probe escape-swallowing of the closing quote.
    let hostile = "seven_day\"} 1\ninjected_metric{x=\"\\";
    {
        let mut info = state.endpoints[0].rate_info.write().await;
        info.claims_7d.insert(
            hostile.to_string(),
            ClaimWindowData {
                utilization: Some(0.5),
                reset: Some(now_epoch + 3600),
                status: Some("rejected".to_string()),
                last_seen: now_epoch,
            },
        );
    }

    let addr = serve(app).await;
    let body = Client::new()
        .get(format!("http://{}/metrics", addr))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();

    assert!(
        !body.lines().any(|l| l.starts_with("injected_metric")),
        "a claim key must not be able to forge a metric line:\n{body}"
    );
    assert!(
        !body.contains("seven_day\"} 1\ninjected_metric"),
        "the hostile key must never appear unescaped:\n{body}"
    );
    // One sample per series is the real proof: a successful breakout would mint
    // a second line on at least one of them.
    for metric in [
        "anthropic_claim_utilization",
        "anthropic_claim_rate_limit_status",
        "anthropic_claim_reset_seconds",
    ] {
        let n = body
            .lines()
            .filter(|l| l.starts_with(&format!("{metric}{{")))
            .count();
        assert_eq!(
            n, 1,
            "{metric} should emit exactly one sample, got {n}:\n{body}"
        );
    }
}

/// The cap budgets UNRESERVED keys. Counting the whole map instead would let
/// the five reserved keys eat the budget, admitting 27 unknown claims rather
/// than the documented 32 — and would put the live path at odds with
/// `bound_ingested_claims`, which has always counted this way.
#[tokio::test]
async fn claim_cap_budgets_unreserved_keys_only() {
    let state = test_state_with(vec![mk_endpoint("acct-a", "sk-ant-api-test-aaa")]);
    let now_epoch = AppState::now_epoch();

    // All five reserved keys first, so they are present when the cap is tested.
    state
        .update_rate_info(0, &claim_headers(None, "0.10", now_epoch))
        .await; // "seven_day"
    for key in ["seven_day_sonnet", "seven_day_opus", "seven_day_haiku"] {
        state
            .update_rate_info(0, &claim_headers(Some(key), "0.10", now_epoch))
            .await;
    }
    let mut band = reqwest::header::HeaderMap::new();
    band.insert(
        "anthropic-ratelimit-unified-7d_oi-utilization",
        HeaderValue::from_static("0.20"),
    );
    band.insert(
        "anthropic-ratelimit-unified-7d_oi-reset",
        HeaderValue::from_str(&(now_epoch + 3600).to_string()).unwrap(),
    );
    state.update_rate_info(0, &band).await;

    {
        let info = state.endpoints[0].rate_info.read().await;
        assert_eq!(
            info.claims_7d
                .keys()
                .filter(|k| claim_key_is_reserved(k))
                .count(),
            5,
            "fixture should seed all five reserved keys"
        );
    }

    // Exactly the documented unreserved budget.
    for i in 0..MAX_CLAIMS_PER_ACCOUNT {
        let claim = format!("seven_day_junk{i}");
        state
            .update_rate_info(0, &claim_headers(Some(&claim), "0.50", now_epoch))
            .await;
    }

    let info = state.endpoints[0].rate_info.read().await;
    let unreserved = info
        .claims_7d
        .keys()
        .filter(|k| !claim_key_is_reserved(k))
        .count();
    assert_eq!(
        unreserved, MAX_CLAIMS_PER_ACCOUNT,
        "all {MAX_CLAIMS_PER_ACCOUNT} unreserved claims must be admitted even with \
         the reserved set present"
    );
    assert_eq!(
        info.claims_7d.len(),
        MAX_CLAIMS_PER_ACCOUNT + 5,
        "hard bound is the unreserved cap plus the five reserved keys"
    );
}

/// `representative_claim` must be truncated wherever the claim map is ingested
/// whole, not just on the header path: `metrics_gate_weight` looks the key up in
/// `claims_7d`, whose keys `bound_ingested_claims` truncates. An untruncated
/// copy misses its own entry and the routing-weight gauges then report a
/// different claim than the router used. Exercises the persisted-state path; the
/// Redis path applies the identical expression.
#[tokio::test]
async fn ingested_representative_claim_is_truncated_to_match_its_key() {
    let tmp = tempfile::NamedTempFile::new().unwrap();
    let oversized = format!("seven_day_{}", "z".repeat(4096));
    let expected: String = oversized
        .chars()
        .take(MAX_LABEL_CHARS)
        .chain(['…'])
        .collect();
    let now_epoch = AppState::now_epoch();

    // A state file as a pre-truncation build would have written it: raw key,
    // raw representative claim.
    let mut state = test_state_with(vec![]);
    {
        let st = Arc::get_mut(&mut state).unwrap();
        st.state_path = tmp.path().to_path_buf();
        st.endpoints.push(make_endpoint("ep1", Protocol::Anthropic));
    }
    {
        let mut info = state.endpoints[0].rate_info.write().await;
        info.representative_claim = Some(oversized.clone());
        info.claims_7d.insert(
            oversized.clone(),
            ClaimWindowData {
                utilization: Some(0.30),
                reset: Some(now_epoch + 302_400),
                status: Some("allowed".to_string()),
                last_seen: now_epoch,
            },
        );
    }
    state.save_state().await;

    let mut state2 = test_state_with(vec![]);
    {
        let st = Arc::get_mut(&mut state2).unwrap();
        st.state_path = tmp.path().to_path_buf();
        st.endpoints.push(make_endpoint("ep1", Protocol::Anthropic));
    }
    state2.load_state().await;

    let info = state2.endpoints[0].rate_info.read().await;
    assert_eq!(
        info.representative_claim.as_deref(),
        Some(expected.as_str()),
        "the restored representative claim must be truncated"
    );
    let rep = info.representative_claim.as_deref().unwrap();
    assert!(
        info.claims_7d.contains_key(rep),
        "the representative claim must resolve to a key that exists, got {:?} against {:?}",
        rep,
        info.claims_7d.keys().collect::<Vec<_>>()
    );
}

/// LAB-5313: a request-body read failure logs `req_id` and the error as
/// structured fields, not interpolated into the message.
#[tokio::test]
async fn read_body_bounded_logs_read_error_with_req_id() {
    let buf = log_capture_buf();
    let state = test_state_with(vec![]);
    let body = Body::from_stream(tokio_stream::iter([Err::<bytes::Bytes, _>(
        std::io::Error::other("lab5313 client reset"),
    )]));
    let resp = read_body_bounded(&state, body, "lab5313-body-read")
        .await
        .unwrap_err();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);

    let output = String::from_utf8(buf.lock().unwrap().clone()).unwrap();
    let mine: Vec<&str> = output
        .lines()
        .filter(|l| l.contains(r#"req_id="lab5313-body-read""#))
        .collect();
    assert_eq!(
        mine.len(),
        1,
        "expected one line, got:\n{}",
        mine.join("\n")
    );
    assert!(
        mine[0].contains(" ERROR ")
            && mine[0].contains("failed to read request body")
            && mine[0].contains("error=lab5313 client reset"),
        "body read failure must log the error as a field, got: {}",
        mine[0]
    );
}
