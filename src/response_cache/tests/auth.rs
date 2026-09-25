use super::*;

// ── Unit: IP allowlist ──────────────────────────────────────────

#[test]
fn ip_allow_entry_matches_exact_addr() {
    let entry = IpAllowEntry::Addr("10.0.0.1".parse().unwrap());
    assert!(entry.contains(&"10.0.0.1".parse().unwrap()));
    assert!(!entry.contains(&"10.0.0.2".parse().unwrap()));
}

#[test]
fn ip_allow_entry_matches_cidr() {
    let entry = IpAllowEntry::Net("10.0.0.0/24".parse().unwrap());
    assert!(entry.contains(&"10.0.0.1".parse().unwrap()));
    assert!(entry.contains(&"10.0.0.254".parse().unwrap()));
    assert!(!entry.contains(&"10.0.1.1".parse().unwrap()));
}

#[test]
fn empty_allowlist_allows_all() {
    let state = test_state_with(vec![mk_endpoint("a", "sk-ant-api-x")]);
    assert!(state.is_ip_allowed(&"192.168.1.1".parse().unwrap()));
    assert!(state.is_ip_allowed(&"8.8.8.8".parse().unwrap()));
}

#[test]
fn populated_allowlist_blocks_unknown() {
    let state = Arc::new(AppState {
        endpoints: vec![mk_endpoint("a", "sk-ant-api-x")],
        state_path: PathBuf::from("/tmp/test.state.json"),
        allowed_ips: vec![IpAllowEntry::Addr("10.0.0.1".parse().unwrap())],
        ..test_state_base()
    });
    assert!(state.is_ip_allowed(&"10.0.0.1".parse().unwrap()));
    assert!(!state.is_ip_allowed(&"10.0.0.2".parse().unwrap()));
}

// ── Client identity resolution tests ──────────────────────────

#[test]
fn resolve_header_overrides_ip_map() {
    let mut client_names = HashMap::new();
    client_names.insert("10.0.0.1".to_string(), "ray".to_string());
    let state = Arc::new(AppState {
        endpoints: vec![mk_endpoint("a", "sk-ant-api-x")],
        state_path: PathBuf::from("/tmp/test.state.json"),
        client_names,
        ..test_state_base()
    });

    // Header overrides IP mapping (supports multiple clients per IP)
    let mut headers = hyper::HeaderMap::new();
    headers.insert("x-client-id", HeaderValue::from_static("gastown"));
    let ip: IpAddr = "10.0.0.1".parse().unwrap();
    assert_eq!(state.resolve_client_id(&ip, &headers), "gastown");

    // No header → falls back to IP mapping
    let empty_headers = hyper::HeaderMap::new();
    assert_eq!(state.resolve_client_id(&ip, &empty_headers), "ray");
}

#[test]
fn resolve_header_fallback() {
    let state = test_state_with(vec![mk_endpoint("a", "sk-ant-api-x")]);
    let mut headers = hyper::HeaderMap::new();
    headers.insert("x-client-id", HeaderValue::from_static("gastown"));
    let ip: IpAddr = "192.168.1.99".parse().unwrap();
    assert_eq!(state.resolve_client_id(&ip, &headers), "gastown");
}

#[test]
fn resolve_unknown() {
    let state = test_state_with(vec![mk_endpoint("a", "sk-ant-api-x")]);
    let headers = hyper::HeaderMap::new();
    let ip: IpAddr = "192.168.1.99".parse().unwrap();
    assert_eq!(state.resolve_client_id(&ip, &headers), "-");
}

#[test]
fn resolve_multi_client_per_ip() {
    // Multiple clients share the same IP — header differentiates them.
    // Operator is identified by client_id, not by IP.
    let mut client_names = HashMap::new();
    client_names.insert("10.0.0.1".to_string(), "ray".to_string());
    let state = Arc::new(AppState {
        endpoints: vec![mk_endpoint("a", "sk-ant-api-x")],
        state_path: PathBuf::from("/tmp/test.state.json"),
        client_names,
        operators: vec!["ray".to_string()],
        ..test_state_base()
    });

    let ip: IpAddr = "10.0.0.1".parse().unwrap();

    // gastown on same IP as operator → identified as gastown, NOT operator
    let mut headers = hyper::HeaderMap::new();
    headers.insert("x-client-id", HeaderValue::from_static("gastown"));
    assert_eq!(state.resolve_client_id(&ip, &headers), "gastown");
    assert!(!state.is_operator("gastown"));

    // ray on same IP → identified as ray, IS operator
    headers.insert("x-client-id", HeaderValue::from_static("ray"));
    assert_eq!(state.resolve_client_id(&ip, &headers), "ray");
    assert!(state.is_operator("ray"));

    // No header → falls back to IP mapping ("ray")
    let empty = hyper::HeaderMap::new();
    assert_eq!(state.resolve_client_id(&ip, &empty), "ray");
}

// ── Additional comprehensive tests ──────────────────────────────────

#[test]
fn resolve_client_id_prefers_header() {
    let state = test_state_with(vec![mk_endpoint("a", "sk-ant-api-x")]);
    let ip: IpAddr = "192.168.1.100".parse().unwrap();
    let mut headers = hyper::HeaderMap::new();
    headers.insert("x-client-id", HeaderValue::from_static("header-client"));

    let resolved = state.resolve_client_id(&ip, &headers);
    assert_eq!(
        resolved, "header-client",
        "should prefer x-client-id header"
    );
}

#[test]
fn resolve_client_id_falls_back_to_ip_map() {
    let mut client_names = HashMap::new();
    client_names.insert("192.168.1.100".to_string(), "mapped-client".to_string());
    let state = Arc::new(AppState {
        client: Client::new(),
        client_nonstreaming: Client::new(),
        endpoints: vec![mk_endpoint("a", "sk-ant-api-x")],
        state_path: PathBuf::from("/tmp/test.state.json"),
        client_names,
        ..test_state_base()
    });

    let ip: IpAddr = "192.168.1.100".parse().unwrap();
    let headers = hyper::HeaderMap::new();

    let resolved = state.resolve_client_id(&ip, &headers);
    assert_eq!(resolved, "mapped-client", "should fall back to IP mapping");
}

#[test]
fn resolve_client_id_defaults_to_dash() {
    let state = test_state_with(vec![mk_endpoint("a", "sk-ant-api-x")]);
    let ip: IpAddr = "203.0.113.1".parse().unwrap();
    let headers = hyper::HeaderMap::new();

    let resolved = state.resolve_client_id(&ip, &headers);
    assert_eq!(resolved, "-", "should default to dash for unknown clients");
}

#[test]
fn resolve_client_id_ignores_empty_header() {
    let state = test_state_with(vec![mk_endpoint("a", "sk-ant-api-x")]);
    let ip: IpAddr = "192.168.1.100".parse().unwrap();
    let mut headers = hyper::HeaderMap::new();
    headers.insert("x-client-id", HeaderValue::from_static(""));

    let resolved = state.resolve_client_id(&ip, &headers);
    assert_eq!(resolved, "-", "should ignore empty x-client-id header");
}

#[test]
fn resolve_client_id_ignores_dash_header() {
    let state = test_state_with(vec![mk_endpoint("a", "sk-ant-api-x")]);
    let ip: IpAddr = "192.168.1.100".parse().unwrap();
    let mut headers = hyper::HeaderMap::new();
    headers.insert("x-client-id", HeaderValue::from_static("-"));

    let resolved = state.resolve_client_id(&ip, &headers);
    assert_eq!(resolved, "-", "should ignore dash as x-client-id header");
}

#[test]
fn resolve_client_id_ignores_reserved_operator_header() {
    let state = test_state_with(vec![mk_endpoint("a", "sk-ant-api-x")]);
    let ip: IpAddr = "192.168.1.100".parse().unwrap();
    let mut headers = hyper::HeaderMap::new();
    headers.insert("x-client-id", HeaderValue::from_static("_operator"));

    let resolved = state.resolve_client_id(&ip, &headers);
    assert_eq!(
        resolved, "-",
        "a self-asserted _operator identity must not merge into the operator bucket"
    );
}

#[test]
fn resolve_client_id_ignores_reserved_other_header() {
    let state = test_state_with(vec![mk_endpoint("a", "sk-ant-api-x")]);
    let ip: IpAddr = "192.168.1.100".parse().unwrap();
    let mut headers = hyper::HeaderMap::new();
    headers.insert("x-client-id", HeaderValue::from_static("_other"));

    let resolved = state.resolve_client_id(&ip, &headers);
    assert_eq!(
        resolved, "-",
        "a self-asserted _other identity must not merge into the metrics overflow bucket"
    );
}

#[test]
fn ip_allow_entry_ipv6_support() {
    let entry = IpAllowEntry::Addr("::1".parse().unwrap());
    assert!(entry.contains(&"::1".parse().unwrap()));
    assert!(!entry.contains(&"::2".parse().unwrap()));
}

#[test]
fn ip_allow_entry_ipv6_cidr() {
    let entry = IpAllowEntry::Net("2001:db8::/32".parse().unwrap());
    assert!(entry.contains(&"2001:db8::1".parse().unwrap()));
    assert!(entry.contains(&"2001:db8:ffff::1".parse().unwrap()));
    assert!(!entry.contains(&"2001:db9::1".parse().unwrap()));
}

// ── AC-9: real client IP behind a trusted proxy ──

fn state_with_trusted_proxies(cidrs: &[&str]) -> Arc<AppState> {
    Arc::new(AppState {
        trusted_proxies: cidrs
            .iter()
            .map(|s| IpAllowEntry::Net(s.parse().unwrap()))
            .collect(),
        ..test_state_base()
    })
}

#[test]
fn xff_from_an_untrusted_peer_is_ignored_entirely() {
    let state = state_with_trusted_proxies(&["10.0.0.0/8"]);
    let peer: IpAddr = "203.0.113.7".parse().unwrap();
    let resolved = state.resolve_client_ip(peer, &hdrs(&[("x-forwarded-for", "198.51.100.99")]));
    assert_eq!(
        resolved, peer,
        "spoofed XFF from an untrusted peer must not win"
    );
}

#[test]
fn xff_is_honoured_from_a_trusted_peer() {
    let state = state_with_trusted_proxies(&["10.0.0.0/8"]);
    let peer: IpAddr = "10.1.2.3".parse().unwrap();
    let resolved = state.resolve_client_ip(peer, &hdrs(&[("x-forwarded-for", "198.51.100.99")]));
    assert_eq!(resolved, "198.51.100.99".parse::<IpAddr>().unwrap());
}

#[test]
fn xff_chain_picks_the_rightmost_untrusted_hop() {
    // client-spoofed, real client, inner LB — the inner LB is trusted, the
    // real client is the rightmost entry that is not.
    let state = state_with_trusted_proxies(&["10.0.0.0/8"]);
    let peer: IpAddr = "10.1.2.3".parse().unwrap();
    let resolved = state.resolve_client_ip(
        peer,
        &hdrs(&[("x-forwarded-for", "1.2.3.4, 198.51.100.99, 10.9.9.9")]),
    );
    assert_eq!(
        resolved,
        "198.51.100.99".parse::<IpAddr>().unwrap(),
        "must skip trusted hops and stop at the first untrusted one — never walk to attacker-appended entries"
    );
}

#[test]
fn xff_malformed_entries_fall_back_to_the_peer_without_panicking() {
    let state = state_with_trusted_proxies(&["10.0.0.0/8"]);
    let peer: IpAddr = "10.1.2.3".parse().unwrap();
    for garbage in [
        "not-an-ip",
        "198.51.100.99, garbage",
        "",
        "198.51.100.99,,10.0.0.1",
        "[::1]:8080",          // port suffix is not a bare IP
        "198.51.100.99; DROP", // header-injection shaped
    ] {
        let resolved = state.resolve_client_ip(peer, &hdrs(&[("x-forwarded-for", garbage)]));
        assert_eq!(
            resolved, peer,
            "garbage XFF {garbage:?} must resolve to the peer"
        );
    }
    // Header absent entirely: peer.
    assert_eq!(state.resolve_client_ip(peer, &hdrs(&[])), peer);
}

#[test]
fn xff_with_empty_trusted_proxies_is_todays_behaviour_exactly() {
    let state = Arc::new(AppState {
        ..test_state_base()
    });
    let peer: IpAddr = "10.1.2.3".parse().unwrap();
    let resolved = state.resolve_client_ip(peer, &hdrs(&[("x-forwarded-for", "198.51.100.99")]));
    assert_eq!(
        resolved, peer,
        "no trusted_proxies ⇒ the header is never consulted"
    );
}

#[test]
fn xff_all_hops_trusted_falls_back_to_the_peer() {
    let state = state_with_trusted_proxies(&["10.0.0.0/8"]);
    let peer: IpAddr = "10.1.2.3".parse().unwrap();
    let resolved =
        state.resolve_client_ip(peer, &hdrs(&[("x-forwarded-for", "10.0.0.1, 10.0.0.2")]));
    assert_eq!(resolved, peer);
}

#[test]
fn xff_multiple_headers_are_one_logical_list_walked_from_the_right() {
    let state = state_with_trusted_proxies(&["10.0.0.0/8"]);
    let peer: IpAddr = "10.1.2.3".parse().unwrap();
    let mut headers = hyper::HeaderMap::new();
    headers.append("x-forwarded-for", HeaderValue::from_static("1.2.3.4"));
    headers.append(
        "x-forwarded-for",
        HeaderValue::from_static("198.51.100.99, 10.9.9.9"),
    );
    let resolved = state.resolve_client_ip(peer, &headers);
    assert_eq!(resolved, "198.51.100.99".parse::<IpAddr>().unwrap());
}

// ── AC-6: admin surfaces × {unauthenticated, non-operator, operator} ──

/// `[[clients]]` app with an operator and a plain client, both surfaces.
fn admin_matrix_app(upstream_url: &str) -> (Router, Arc<AppState>) {
    let mut acct = mk_endpoint("acct-a", "sk-ant-api-test-aaa");
    acct.base_url = upstream_url.to_string();
    let state = Arc::new(AppState {
        endpoints: vec![acct],
        clients: vec![
            mk_client("ops", "key-ops", &[]),
            mk_client("geo", "key-geo", &[]),
        ],
        operators: vec!["ops".to_string()],
        ..test_state_base()
    });
    (build_router(state.clone()), state)
}

#[tokio::test]
async fn admin_surfaces_gate_by_operator_principal() {
    let (mock_url, _handle) = spawn_mock_upstream().await;
    let (app, _state) = admin_matrix_app(&mock_url);
    let addr = serve(app).await;
    let client = Client::new();

    for path in ["/_stats", "/metrics"] {
        // Unauthenticated → 401.
        let resp = client
            .get(format!("http://{addr}{path}"))
            .send()
            .await
            .unwrap();
        assert_eq!(
            resp.status(),
            reqwest::StatusCode::UNAUTHORIZED,
            "{path} unauthenticated"
        );

        // Authenticated non-operator → 403: a per-client key holder has no
        // business reading other clients' ids and the account names.
        let resp = client
            .get(format!("http://{addr}{path}"))
            .header("x-api-key", "key-geo")
            .send()
            .await
            .unwrap();
        assert_eq!(
            resp.status(),
            reqwest::StatusCode::FORBIDDEN,
            "{path} non-operator"
        );

        // Operator → 200.
        let resp = client
            .get(format!("http://{addr}{path}"))
            .header("x-api-key", "key-ops")
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), reqwest::StatusCode::OK, "{path} operator");
    }
}

/// AC-5: under `allow_unauthenticated` (no credentials configured), both
/// surfaces still serve — this is the lab posture, and it is what keeps the
/// Grafana/vmagent scrape of `/metrics` working there (LAB-925/LAB-927):
/// the scraper presents no credential, and none is required.
#[tokio::test]
async fn admin_surfaces_still_serve_without_credentials_configured() {
    let (mock_url, _handle) = spawn_mock_upstream().await;
    let (app, _state) = test_app(&mock_url, None);
    let addr = serve(app).await;
    let client = Client::new();

    for path in ["/_stats", "/metrics"] {
        let resp = client
            .get(format!("http://{addr}{path}"))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), reqwest::StatusCode::OK, "{path} open mode");
    }
}

/// Legacy `proxy_key`: one shared secret means the key holder IS the
/// operator — a valid key reads both surfaces, a missing one does not.
#[tokio::test]
async fn admin_surfaces_accept_the_legacy_shared_key() {
    let (mock_url, _handle) = spawn_mock_upstream().await;
    let (app, _state) = test_app(&mock_url, Some("legacy-shared-secret-0123456789ab".into()));
    let addr = serve(app).await;
    let client = Client::new();

    for path in ["/_stats", "/metrics"] {
        let resp = client
            .get(format!("http://{addr}{path}"))
            .header("x-api-key", "legacy-shared-secret-0123456789ab")
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), reqwest::StatusCode::OK, "{path} proxy_key");
        let resp = client
            .get(format!("http://{addr}{path}"))
            .send()
            .await
            .unwrap();
        assert_eq!(
            resp.status(),
            reqwest::StatusCode::UNAUTHORIZED,
            "{path} missing key"
        );
    }
}

// ── AC-11/AC-12: failed-auth throttle ──

#[test]
fn auth_throttle_trips_after_the_limit_and_reports_retry_after() {
    let t = AuthThrottle::new(3, Duration::from_secs(60));
    let ip: IpAddr = "203.0.113.7".parse().unwrap();
    for _ in 0..2 {
        t.record_failure(ip);
        assert_eq!(t.check(&ip), None, "under the limit must not throttle");
    }
    t.record_failure(ip);
    let retry = t.check(&ip).expect("limit reached must throttle");
    assert!(
        (1..=60).contains(&retry),
        "retry-after {retry} out of range"
    );
    // A different IP is unaffected.
    assert_eq!(t.check(&"203.0.113.8".parse().unwrap()), None);
}

#[test]
fn auth_throttle_window_expiry_unlocks() {
    let t = AuthThrottle::new(1, Duration::from_millis(30));
    let ip: IpAddr = "203.0.113.7".parse().unwrap();
    t.record_failure(ip);
    assert!(t.check(&ip).is_some());
    std::thread::sleep(Duration::from_millis(40));
    assert_eq!(t.check(&ip), None, "expired window must unlock");
    assert!(
        t.entries.lock().unwrap().is_empty(),
        "expired entry must be removed, not retained"
    );
}

#[test]
fn auth_throttle_zero_limit_disables() {
    let t = AuthThrottle::new(0, Duration::from_secs(60));
    let ip: IpAddr = "203.0.113.7".parse().unwrap();
    for _ in 0..100 {
        t.record_failure(ip);
    }
    assert_eq!(t.check(&ip), None);
    assert!(
        t.entries.lock().unwrap().is_empty(),
        "disabled throttle must not accumulate state"
    );
}

/// AC-12: the map is keyed on an attacker-controlled IP, so it must stay
/// bounded. Among equal-threat (equal-count) entries the oldest window is
/// evicted, and memory stays flat.
#[test]
fn auth_throttle_capacity_evicts_the_least_established_entry() {
    let t = AuthThrottle::with_capacity(1, Duration::from_secs(60), 2);
    let first: IpAddr = "203.0.113.1".parse().unwrap();
    t.record_failure(first);
    std::thread::sleep(Duration::from_millis(5)); // strictly older window_start
    t.record_failure("203.0.113.2".parse().unwrap());
    t.record_failure("203.0.113.3".parse().unwrap()); // over capacity
    let entries = t.entries.lock().unwrap();
    assert_eq!(entries.len(), 2, "capacity must hold");
    assert!(
        !entries.contains_key(&first),
        "among equal-count entries the oldest window is evicted"
    );
}

/// AC-12 hardening: eviction must NOT be an attacker's escape hatch. A burst
/// of fresh single-failure IPs must not evict an established lockout — else a
/// guesser could flush its own throttle and resume. Established lockout
/// (count >= max_failures) is preserved; the fresh count=1 floods evict each
/// other.
#[test]
fn auth_throttle_capacity_preserves_an_established_lockout() {
    let t = AuthThrottle::with_capacity(3, Duration::from_secs(60), 2);
    let locked: IpAddr = "203.0.113.9".parse().unwrap();
    for _ in 0..3 {
        t.record_failure(locked); // count = 3 = max ⇒ actively throttled
    }
    assert!(t.check(&locked).is_some(), "lockout must be active");
    // Flood fresh count=1 IPs well past capacity.
    for i in 0..20u8 {
        t.record_failure(IpAddr::from([198, 51, 100, i]));
    }
    assert!(
        t.entries.lock().unwrap().contains_key(&locked),
        "the established lockout must survive a flood of fresh failures"
    );
    assert!(t.check(&locked).is_some(), "and stay throttled");
}

/// Expired windows are purged before any live entry is evicted, so a table
/// full of stale entries never forces out an active one.
#[test]
fn auth_throttle_capacity_purges_expired_before_evicting() {
    let t = AuthThrottle::with_capacity(1, Duration::from_millis(20), 2);
    t.record_failure("203.0.113.1".parse().unwrap());
    t.record_failure("203.0.113.2".parse().unwrap());
    std::thread::sleep(Duration::from_millis(30)); // both windows expire
    let fresh: IpAddr = "203.0.113.3".parse().unwrap();
    t.record_failure(fresh);
    let entries = t.entries.lock().unwrap();
    assert!(entries.contains_key(&fresh));
    assert!(entries.len() <= 2, "expired entries purged, capacity held");
}

/// A locked-out invalid caller must not deny service to a valid principal
/// sharing its resolved IP (NAT, tailnet proxy, or load balancer). Invalid
/// attempts remain throttled after the valid request, so success is a bypass
/// for that authenticated request rather than a reset attackers can induce.
#[tokio::test]
async fn valid_key_bypasses_a_shared_ip_auth_throttle_without_clearing_it() {
    let (mock_url, _handle) = spawn_mock_upstream().await;
    let mut acct = mk_endpoint("acct-a", "sk-ant-api-test-aaa");
    acct.base_url = mock_url.to_string();
    let state = Arc::new(AppState {
        endpoints: vec![acct],
        clients: vec![
            mk_client("ops", "key-ops", &[]),
            mk_client("geo", "key-geo", &[]),
        ],
        operators: vec!["ops".to_string()],
        auth_throttle: AuthThrottle::new(3, Duration::from_secs(60)),
        ..test_state_base()
    });
    let addr = serve(build_router(state.clone())).await;
    let client = Client::new();

    for _ in 0..3 {
        let resp = client
            .post(format!("http://{addr}/v1/messages"))
            .header("x-api-key", "key-wrong")
            .body("{}")
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), reqwest::StatusCode::UNAUTHORIZED);
    }

    // The next invalid credential is throttled with retry-after.
    let resp = client
        .post(format!("http://{addr}/v1/messages"))
        .header("x-api-key", "key-wrong")
        .body("{}")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::TOO_MANY_REQUESTS);
    let retry: u64 = resp
        .headers()
        .get("retry-after")
        .expect("429 must carry retry-after")
        .to_str()
        .unwrap()
        .parse()
        .expect("retry-after must be whole seconds");
    assert!((1..=60).contains(&retry));

    // A valid credential from the same IP must still reach the handler.
    let resp = client
        .post(format!("http://{addr}/v1/messages"))
        .header("x-api-key", "key-geo")
        .body(r#"{"model":"claude-haiku-4-5","max_tokens":1,"messages":[{"role":"user","content":"hi"}]}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    // Success does not erase a shared IP's attack history: another invalid
    // credential is still throttled, and only failures increment the metric.
    let resp = client
        .post(format!("http://{addr}/v1/messages"))
        .header("x-api-key", "key-wrong")
        .body("{}")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(state.auth_failures.lock().unwrap().values().sum::<u64>(), 5);
}

/// Successful traffic below the limit also leaves the shared IP's failure
/// history intact. This keeps an authenticated neighbour from accidentally
/// defeating the invalid-request throttle for an attacker behind the same NAT.
#[tokio::test]
async fn successful_auth_does_not_clear_shared_ip_failures() {
    let (mock_url, _handle) = spawn_mock_upstream().await;
    let mut acct = mk_endpoint("acct-a", "sk-ant-api-test-aaa");
    acct.base_url = mock_url.to_string();
    let state = Arc::new(AppState {
        endpoints: vec![acct],
        clients: vec![mk_client("geo", "key-geo", &[])],
        auth_throttle: AuthThrottle::new(3, Duration::from_secs(60)),
        ..test_state_base()
    });
    let addr = serve(build_router(state.clone())).await;
    let client = Client::new();

    // Two bad attempts (under the limit of 3).
    for _ in 0..2 {
        let resp = client
            .post(format!("http://{addr}/v1/messages"))
            .header("x-api-key", "key-wrong")
            .body("{}")
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), reqwest::StatusCode::UNAUTHORIZED);
    }
    // A successful auth is served but does not clear the two failures.
    let resp = client
        .post(format!("http://{addr}/v1/messages"))
        .header("x-api-key", "key-geo")
        .body(r#"{"model":"claude-haiku-4-5","max_tokens":1,"messages":[{"role":"user","content":"hi"}]}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    // The third bad request is still the threshold-reaching 401; the next is
    // a 429. If success had cleared the state, both would be 401.
    let resp = client
        .post(format!("http://{addr}/v1/messages"))
        .header("x-api-key", "key-wrong")
        .body("{}")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::UNAUTHORIZED);
    let resp = client
        .post(format!("http://{addr}/v1/messages"))
        .header("x-api-key", "key-wrong")
        .body("{}")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::TOO_MANY_REQUESTS);
}

/// A dual-stack listener delivers IPv4 peers as `::ffff:a.b.c.d`; a v4
/// `trusted_proxies` CIDR must still match after canonicalization, or XFF
/// resolution silently no-ops behind the LB.
#[test]
fn resolve_client_ip_canonicalizes_v4_mapped_peer() {
    let state = state_with_trusted_proxies(&["10.0.0.0/8"]);
    let mapped_peer: IpAddr = "::ffff:10.1.2.3".parse().unwrap();
    let resolved =
        state.resolve_client_ip(mapped_peer, &hdrs(&[("x-forwarded-for", "198.51.100.99")]));
    assert_eq!(
        resolved,
        "198.51.100.99".parse::<IpAddr>().unwrap(),
        "v4-mapped trusted peer must be recognized so XFF is honoured"
    );
}

/// `anthropic_auth_failures_total{route,cred}` is scrape-visible (LAB-1192
/// AC-11, LAB-4720 AC-2): a wrong key on the admin surface and a Bearer-only
/// caller on the native surface (the OpenAI-SDK misconfiguration) land on
/// distinct, fixed-vocabulary series.
#[tokio::test]
async fn metrics_expose_auth_failures_by_route_and_cred() {
    let (mock_url, _handle) = spawn_mock_upstream().await;
    let (app, _state) = admin_matrix_app(&mock_url);
    let addr = serve(app).await;
    let client = Client::new();

    // One failure on the stats route, key in the right header.
    let _ = client
        .get(format!("http://{addr}/_stats"))
        .header("x-api-key", "key-wrong")
        .send()
        .await
        .unwrap();
    // One on the proxy route: a valid key, but in the wrong header.
    let resp = client
        .post(format!("http://{addr}/v1/messages"))
        .header("authorization", "Bearer key-geo")
        .body("{}")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::UNAUTHORIZED);

    let body = client
        .get(format!("http://{addr}/metrics"))
        .header("x-api-key", "key-ops")
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    for line in [
        "anthropic_auth_failures_total{route=\"stats\",cred=\"x-api-key\"} 1",
        "anthropic_auth_failures_total{route=\"proxy\",cred=\"bearer\"} 1",
    ] {
        assert!(body.contains(line), "missing `{line}` in:\n{body}");
    }
}

/// LAB-4720 AC-1: a rejection is attributable beyond the source IP. The
/// header shape separates "no key", "key in the wrong header" and "wrong
/// key"; the fingerprint is 12 hex chars, never a run of the key, and
/// matches the README recipe (`hashlib.blake2s(tag + key).hexdigest()[:12]`);
/// the user-agent is clipped.
#[test]
fn rejected_credential_shape_fingerprint_and_user_agent() {
    assert_eq!(presented_credential(&hdrs(&[])).0, "none");
    assert_eq!(
        presented_credential(&hdrs(&[("x-api-key", "k")])).0,
        "x-api-key"
    );
    assert_eq!(
        presented_credential(&hdrs(&[("authorization", "Bearer k")])).0,
        "bearer"
    );
    assert_eq!(
        presented_credential(&hdrs(&[("authorization", "Basic dXNlcjpwdw==")])).0,
        "auth-other"
    );
    // auth-other is labelled but never fingerprinted — no bare credential
    // to hash, and the README recipe can't reproduce a scheme-prefixed hash.
    assert_eq!(
        presented_credential(&hdrs(&[("authorization", "Basic dXNlcjpwdw==")])).1,
        None
    );
    assert_eq!(
        presented_credential(&hdrs(&[("authorization", "bearer k")])).0,
        "bearer"
    );
    assert_eq!(
        presented_credential(&hdrs(&[("authorization", "Bearer")])).0,
        "auth-other"
    );
    // Both headers: x-api-key is the one compared first, so it names the shape.
    assert_eq!(
        presented_credential(&hdrs(&[("x-api-key", "k"), ("authorization", "bearer k")])).0,
        "x-api-key"
    );

    let key = "stale-client-key-0123456789abcdefghijklmnopqrstuvwxyz";
    let fp = credential_fingerprint(Some(key.as_bytes()));
    assert_eq!(fp, "67baf0920d1c", "must match the README recipe");
    assert!(!key.contains(&fp), "fingerprint leaked a run of the key");
    assert_eq!(credential_fingerprint(None), "-");
    // The README recipe hashes the bare key, so the Bearer scheme must be
    // stripped: `presented_credential` hands back the key alone.
    let bearer = hdrs(&[("authorization", format!("Bearer {key}").as_str())]);
    assert_eq!(presented_credential(&bearer).1, Some(key.as_bytes()));

    assert_eq!(bounded_user_agent(&hdrs(&[])), "-");
    assert_eq!(bounded_user_agent(&hdrs(&[("user-agent", "")])), "-");
    let mut obs_text = hyper::HeaderMap::new();
    obs_text.insert("user-agent", HeaderValue::from_bytes(b"agent\xff").unwrap());
    assert_eq!(bounded_user_agent(&obs_text), "-", "not visible ASCII");
    assert_eq!(
        bounded_user_agent(&hdrs(&[("user-agent", "curl/8.5.0")])),
        "curl/8.5.0"
    );
    let long = "x".repeat(300);
    let ua = bounded_user_agent(&hdrs(&[("user-agent", long.as_str())]));
    assert_eq!(
        ua.chars().count(),
        MAX_LABEL_CHARS + 1,
        "clipped + ellipsis"
    );
}
