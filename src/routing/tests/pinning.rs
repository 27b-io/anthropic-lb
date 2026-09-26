use super::*;

/// `mk_client` with a `preferred_endpoints` pin (LAB-2636).
fn mk_pinned_client(name: &str, key: &str, preferred: &[&str]) -> ClientConfig {
    ClientConfig {
        preferred_endpoints: preferred.iter().map(|s| s.to_string()).collect(),
        ..mk_client(name, key, &[])
    }
}

// ── LAB-2636 / #151: per-client endpoint pinning (preferred_endpoints) ──

/// Two-endpoint pool with a client pinned to "dedicated". `soft_limit` at the
/// production 0.90 so the healthy/spill boundary is exercised for real.
fn pinned_test_state() -> Arc<AppState> {
    Arc::new(AppState {
        endpoints: vec![
            mk_endpoint("general", "sk-ant-api-gen"),
            mk_endpoint("dedicated", "sk-ant-api-ded"),
        ],
        clients: vec![mk_pinned_client("passbolt", "key-pb", &["dedicated"])],
        soft_limit: 0.90,
        ..test_state_base()
    })
}

#[test]
fn client_config_preferred_endpoints_parses_and_defaults_empty() {
    let c = cfg(
        "[[endpoints]]\nname = \"ded\"\ntoken = \"sk-ant-api-x\"\n\n[[clients]]\nname = \"pin\"\nkey = \"k1\"\npreferred_endpoints = [\"ded\"]\n\n[[clients]]\nname = \"plain\"\nkey = \"k2\"\n",
    );
    assert_eq!(c.clients[0].preferred_endpoints, vec!["ded".to_string()]);
    assert!(
        c.clients[1].preferred_endpoints.is_empty(),
        "omitted preferred_endpoints must default to empty (= no pin)"
    );
}

#[test]
fn validate_clients_rejects_unknown_preferred_endpoint() {
    let err = validate_clients(&cfg(
        "[[endpoints]]\nname = \"real\"\ntoken = \"sk-ant-api-x\"\n\n[[clients]]\nname = \"pin\"\nkey = \"k1\"\npreferred_endpoints = [\"typo\"]\n",
    ))
    .unwrap_err();
    assert!(
        err.contains("preferred_endpoints") && err.contains("typo") && err.contains("pin"),
        "error must name the client and the bad entry: {err}"
    );
}

#[test]
fn validate_clients_accepts_known_preferred_endpoint() {
    assert!(validate_clients(&cfg(
        "[[endpoints]]\nname = \"real\"\ntoken = \"sk-ant-api-x\"\n\n[[clients]]\nname = \"pin\"\nkey = \"k1\"\npreferred_endpoints = [\"real\"]\n",
    ))
    .is_ok());
}

/// Pin: the client routes to its preferred endpoint on EVERY pick, even though
/// the general endpoint has far more headroom and attracts unpinned traffic —
/// and even for affinity keys that land unpinned traffic on the general
/// endpoint (the filter runs before affinity/weighted selection).
#[tokio::test]
async fn pinned_client_always_routes_to_preferred_endpoint() {
    let state = pinned_test_state();
    state.endpoints[0].rate_info.write().await.utilization = Some(0.1);
    state.endpoints[1].rate_info.write().await.utilization = Some(0.8);

    let mut unpinned_landed_general = false;
    for i in 0..100 {
        let key = format!("client:sess:{i}");
        if state
            .pick_endpoint_for_client(Some(&key), "claude-opus-5", &[], "other", false)
            .await
            == Some(0)
        {
            unpinned_landed_general = true;
        }
        assert_eq!(
            state
                .pick_endpoint_for_client(Some(&key), "claude-opus-5", &[], "passbolt", false)
                .await,
            Some(1),
            "pinned client must land on 'dedicated' for every affinity key"
        );
    }
    assert!(
        unpinned_landed_general,
        "fixture must attract unpinned traffic to 'general' or the pin assert proves nothing"
    );
}

/// Spill: preferred endpoint over the soft limit (still a candidate, but not
/// healthy) → the full pool applies and the healthy general endpoint serves.
#[tokio::test]
async fn pinned_client_spills_on_soft_limit_overage() {
    let state = pinned_test_state();
    state.endpoints[0].rate_info.write().await.utilization = Some(0.1);
    state.endpoints[1].rate_info.write().await.utilization = Some(0.95);

    assert_eq!(
        state
            .pick_endpoint_for_client(Some("s"), "claude-opus-5", &[], "passbolt", false)
            .await,
        Some(0),
        "soft-limited preferred endpoint must spill to the general pool"
    );
}

/// Spill: preferred endpoint hard-limited (429) → general pool serves.
#[tokio::test]
async fn pinned_client_spills_on_hard_limit() {
    let state = pinned_test_state();
    state.endpoints[0].rate_info.write().await.utilization = Some(0.1);
    {
        let mut info = state.endpoints[1].rate_info.write().await;
        info.utilization = Some(0.1);
        info.hard_limited_until = Some(Instant::now() + Duration::from_secs(3600));
    }

    assert_eq!(
        state
            .pick_endpoint_for_client(Some("s"), "claude-opus-5", &[], "passbolt", false)
            .await,
        Some(0),
        "hard-limited preferred endpoint must spill to the general pool"
    );
}

/// Spill: preferred endpoint transport-circuit-broken → general pool serves.
#[tokio::test]
async fn pinned_client_spills_on_transport_unhealthy() {
    let state = pinned_test_state();
    state.endpoints[0].rate_info.write().await.utilization = Some(0.1);
    {
        let mut info = state.endpoints[1].rate_info.write().await;
        info.utilization = Some(0.1);
        info.transport_unhealthy_until = Some(Instant::now() + Duration::from_secs(3600));
    }

    assert_eq!(
        state
            .pick_endpoint_for_client(Some("s"), "claude-opus-5", &[], "passbolt", false)
            .await,
        Some(0),
        "transport-unhealthy preferred endpoint must spill to the general pool"
    );
}

/// Spill: preferred endpoint doesn't serve the requested model → general pool.
#[tokio::test]
async fn pinned_client_spills_on_model_mismatch() {
    let mut dedicated = mk_endpoint("dedicated", "sk-ant-api-ded");
    dedicated.models = vec!["claude-haiku-*".to_string()];
    let state = Arc::new(AppState {
        endpoints: vec![mk_endpoint("general", "sk-ant-api-gen"), dedicated],
        clients: vec![mk_pinned_client("passbolt", "key-pb", &["dedicated"])],
        soft_limit: 0.90,
        ..test_state_base()
    });

    assert_eq!(
        state
            .pick_endpoint_for_client(Some("s"), "claude-opus-5", &[], "passbolt", false)
            .await,
        Some(0),
        "model not served by the preferred endpoint must spill to the general pool"
    );
    // Sanity: a model the pin DOES serve stays pinned.
    assert_eq!(
        state
            .pick_endpoint_for_client(Some("s"), "claude-haiku-4-5", &[], "passbolt", false)
            .await,
        Some(1)
    );
}

/// AC-11 (LAB-2687): a pinned account whose org lacks fast mode is not a
/// viable preferred candidate for a `speed: "fast"` request — the exclusion
/// runs before the pin filter, so the request spills to the general pool.
#[tokio::test]
async fn pinned_client_fast_request_spills_when_pin_is_fast_mode_disabled() {
    let state = pinned_test_state();
    state.endpoints[0].rate_info.write().await.utilization = Some(0.1);
    state.endpoints[1].rate_info.write().await.utilization = Some(0.1);
    state.note_fast_mode_disabled("dedicated", 1);

    assert_eq!(
        state
            .pick_endpoint_for_client(Some("s"), "claude-opus-5", &[], "passbolt", true)
            .await,
        Some(0),
        "fast request must spill off a fast-mode-disabled pin"
    );
}

/// AC-11 (LAB-2687): the same pin, same client, standard request — the
/// fast-mode mark is irrelevant and the pin holds.
#[tokio::test]
async fn pinned_client_standard_request_keeps_fast_mode_disabled_pin() {
    let state = pinned_test_state();
    state.endpoints[0].rate_info.write().await.utilization = Some(0.1);
    state.endpoints[1].rate_info.write().await.utilization = Some(0.1);
    state.note_fast_mode_disabled("dedicated", 1);

    assert_eq!(
        state
            .pick_endpoint_for_client(Some("s"), "claude-opus-5", &[], "passbolt", false)
            .await,
        Some(1),
        "standard request must stay pinned despite the fast-mode mark"
    );
}

/// Rotation: a preferred endpoint entering the skip list mid-request (5xx
/// rotation) re-applies the rule — remaining preferred endpoints while any is
/// viable, full pool once none is.
#[tokio::test]
async fn pinned_client_rotation_prefers_remaining_preferred_then_spills() {
    let state = Arc::new(AppState {
        endpoints: vec![
            mk_endpoint("general", "sk-ant-api-gen"),
            mk_endpoint("ded-a", "sk-ant-api-a"),
            mk_endpoint("ded-b", "sk-ant-api-b"),
        ],
        clients: vec![mk_pinned_client("passbolt", "key-pb", &["ded-a", "ded-b"])],
        soft_limit: 0.90,
        ..test_state_base()
    });

    // ded-a failed and was skipped → the OTHER preferred endpoint must serve,
    // never the general pool.
    for i in 0..50 {
        let key = format!("client:sess:{i}");
        assert_eq!(
            state
                .pick_endpoint_for_client(Some(&key), "claude-opus-5", &[1], "passbolt", false)
                .await,
            Some(2),
            "with one preferred endpoint skipped, the remaining one must serve"
        );
    }
    // Both preferred endpoints skipped → spill to the general pool.
    assert_eq!(
        state
            .pick_endpoint_for_client(Some("s"), "claude-opus-5", &[1, 2], "passbolt", false)
            .await,
        Some(0),
        "with all preferred endpoints skipped, the general pool must serve"
    );
}

/// Native Anthropic surface end-to-end: the authenticated pinned client is
/// served by its preferred account on every request, despite the general
/// account having far more headroom.
#[tokio::test]
async fn native_surface_pins_authenticated_client_to_preferred_endpoint() {
    let (mock_url, _handle) = spawn_mock_upstream().await;
    let state = Arc::new(AppState {
        endpoints: vec![
            mk_endpoint_at("acct-a", "sk-ant-api-test-aaa", &mock_url),
            mk_endpoint_at("acct-b", "sk-ant-api-test-bbb", &mock_url),
        ],
        clients: vec![mk_pinned_client("passbolt", "key-pb", &["acct-b"])],
        soft_limit: 0.90,
        ..test_state_base()
    });
    state.endpoints[0].rate_info.write().await.utilization = Some(0.05);
    state.endpoints[1].rate_info.write().await.utilization = Some(0.5);
    let addr = serve(build_router(state.clone())).await;
    let client = Client::new();

    for i in 0..10 {
        let resp = client
            .post(format!("http://{addr}/v1/messages"))
            .header("content-type", "application/json")
            .header("x-api-key", "key-pb")
            .header("x-session-id", format!("sess-{i}"))
            .body(r#"{"model":"claude-opus-5","max_tokens":1,"messages":[{"role":"user","content":"hi"}]}"#)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
    }
    assert_eq!(
        state.endpoints[1].requests.load(Ordering::Relaxed),
        10,
        "every request must be served by the preferred account"
    );
    assert_eq!(
        state.endpoints[0].requests.load(Ordering::Relaxed),
        0,
        "the general account must serve none of the pinned client's requests"
    );
}

/// OpenAI-compat surface end-to-end: same pin, authenticated via
/// `Authorization: Bearer`, proving client identity reaches the picker on the
/// second protocol surface too.
#[tokio::test]
async fn openai_compat_surface_pins_authenticated_client_to_preferred_endpoint() {
    let (mock_url, _handle) = spawn_mock_upstream().await;
    let state = Arc::new(AppState {
        endpoints: vec![
            mk_endpoint_at("acct-a", "sk-ant-api-test-aaa", &mock_url),
            mk_endpoint_at("acct-b", "sk-ant-api-test-bbb", &mock_url),
        ],
        clients: vec![mk_pinned_client("passbolt", "key-pb", &["acct-b"])],
        soft_limit: 0.90,
        ..test_state_base()
    });
    state.endpoints[0].rate_info.write().await.utilization = Some(0.05);
    state.endpoints[1].rate_info.write().await.utilization = Some(0.5);
    let addr = serve(build_router(state.clone())).await;
    let client = Client::new();

    for i in 0..10 {
        let resp = client
            .post(format!("http://{addr}/v1/chat/completions"))
            .header("content-type", "application/json")
            .header("authorization", "Bearer key-pb")
            .header("x-session-id", format!("sess-{i}"))
            .body(r#"{"model":"claude-opus-5","messages":[{"role":"user","content":"hi"}]}"#)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
    }
    assert_eq!(
        state.endpoints[1].requests.load(Ordering::Relaxed),
        10,
        "every request must be served by the preferred account"
    );
    assert_eq!(
        state.endpoints[0].requests.load(Ordering::Relaxed),
        0,
        "the general account must serve none of the pinned client's requests"
    );
}

/// Spill: preferred endpoint serving via paid overage reports a LOW gate (the
/// overage window supersedes its exhausted subscription windows) but is
/// priority-demoted — the pin must NOT treat it as viable, or the pinned
/// client would bill paid overage forever while free general-pool capacity
/// sits idle (expert-panel CRIT on the initial LAB-2636 cut).
#[tokio::test]
async fn pinned_client_spills_when_preferred_endpoint_at_paid_overage() {
    let state = pinned_test_state();
    state.endpoints[0].rate_info.write().await.utilization = Some(0.1);
    {
        let mut info = state.endpoints[1].rate_info.write().await;
        // Exhausted subscription covered by overage: raw utilization is
        // irrelevant, compute_routing_weight gates on the (empty) overage
        // window → gate ~0.0 < soft_limit, but priority is demoted by
        // overage_penalty.
        info.utilization_5h = Some(1.0);
        info.overage_in_use = true;
    }

    assert_eq!(
        state
            .pick_endpoint_for_client(Some("s"), "claude-opus-5", &[], "passbolt", false)
            .await,
        Some(0),
        "an overage-covered preferred endpoint must spill to free general-pool capacity"
    );
}
