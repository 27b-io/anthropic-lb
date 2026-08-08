use super::*;

/// The error describer must surface the real cause + classification, not
/// just reqwest's opaque "error sending request" Display. Uses a genuine
/// connect failure so it exercises the is_connect classifier and the
/// source-chain walk.
#[tokio::test]
async fn describe_reqwest_error_surfaces_cause_and_kind() {
    let err = reqwest::Client::new()
        .get("http://127.0.0.1:1/")
        .timeout(Duration::from_secs(2))
        .send()
        .await
        .expect_err("connect to 127.0.0.1:1 should fail");
    let desc = describe_reqwest_error(&err);
    assert!(
        desc.starts_with("kind="),
        "should classify kind, got: {desc}"
    );
    assert!(
        desc.contains("connect") || desc.contains("cause="),
        "should surface connect classification or root cause, got: {desc}"
    );
}

#[test]
fn protocol_deserializes_lowercase_strings() {
    let p: Protocol = serde_json::from_str(r#""anthropic""#).unwrap();
    assert_eq!(p, Protocol::Anthropic);
    let p: Protocol = serde_json::from_str(r#""openai""#).unwrap();
    assert_eq!(p, Protocol::OpenAI);
}

#[test]
fn protocol_default_is_anthropic() {
    assert_eq!(Protocol::default(), Protocol::Anthropic);
    assert_ne!(Protocol::default(), Protocol::OpenAI);
}

#[test]
fn endpoint_config_parses_minimal_anthropic_block() {
    let toml_str = r#"
listen = "0.0.0.0:8080"
upstream = "https://api.anthropic.com"
accounts = []

[[endpoints]]
name = "primary"
token = "sk-ant-test"
"#;
    let cfg: Config = toml::from_str(toml_str).unwrap();
    assert_eq!(cfg.endpoints.len(), 1);
    assert_eq!(cfg.endpoints[0].name, "primary");
    assert_eq!(cfg.endpoints[0].protocol, Protocol::Anthropic);
    assert_eq!(cfg.endpoints[0].base_url, None);
    assert_eq!(cfg.endpoints[0].token, "sk-ant-test");
    assert!(cfg.endpoints[0].models.is_empty());
    assert_eq!(cfg.endpoints[0].priority, 0);
}

#[test]
fn endpoint_config_parses_openai_with_base_url() {
    let toml_str = r#"
listen = "0.0.0.0:8080"
upstream = "https://api.anthropic.com"
accounts = []

[[endpoints]]
name = "gateway"
protocol = "openai"
base_url = "https://gateway.example.com"
token = "sk-test"
priority = 100
models = ["claude-opus-*"]
"#;
    let cfg: Config = toml::from_str(toml_str).unwrap();
    let ep = &cfg.endpoints[0];
    assert_eq!(ep.protocol, Protocol::OpenAI);
    assert_eq!(ep.base_url.as_deref(), Some("https://gateway.example.com"));
    assert_eq!(ep.priority, 100);
    assert_eq!(ep.models, vec!["claude-opus-*".to_string()]);
}

#[test]
fn config_rejects_legacy_accounts_block() {
    let toml_str = r#"
listen = "0.0.0.0:8080"
upstream = "https://api.anthropic.com"

[[accounts]]
name = "primary"
token = "sk-ant-test"
"#;
    let value: toml::Value = toml::from_str(toml_str).unwrap();
    let err = reject_legacy_config_keys(&value).unwrap_err();
    assert!(
        err.contains("accounts"),
        "error must name 'accounts': {err}"
    );
    assert!(
        err.contains("endpoints"),
        "error must mention replacement: {err}"
    );
}

#[test]
fn config_rejects_legacy_upstreams_block() {
    let toml_str = r#"
listen = "0.0.0.0:8080"
upstream = "https://api.anthropic.com"

[[upstreams]]
name = "fallback"
base_url = "https://example.com"
api_key = "key"
"#;
    let value: toml::Value = toml::from_str(toml_str).unwrap();
    let err = reject_legacy_config_keys(&value).unwrap_err();
    assert!(err.contains("upstreams"));
    assert!(err.contains("endpoints"));
}

#[test]
fn config_rejects_fallback_upstream_key() {
    let toml_str = r#"
listen = "0.0.0.0:8080"
upstream = "https://api.anthropic.com"
fallback_upstream = "anything"
"#;
    let value: toml::Value = toml::from_str(toml_str).unwrap();
    let err = reject_legacy_config_keys(&value).unwrap_err();
    assert!(err.contains("fallback_upstream"));
    assert!(err.contains("priority"));
}

#[test]
fn config_accepts_endpoints_only_schema() {
    let toml_str = r#"
listen = "0.0.0.0:8080"
upstream = "https://api.anthropic.com"

[[endpoints]]
name = "primary"
token = "sk-ant-test"
"#;
    let value: toml::Value = toml::from_str(toml_str).unwrap();
    assert!(reject_legacy_config_keys(&value).is_ok());
}

#[test]
fn validate_endpoints_rejects_non_anthropic_host() {
    // LAB-1191 AC-1: a non-canonical host on an anthropic endpoint would send
    // the account token to that host — hard startup error naming the endpoint.
    let endpoints = vec![EndpointConfig {
        name: "primary".to_string(),
        protocol: Protocol::Anthropic,
        base_url: Some("https://staging.anthropic.example".to_string()),
        token: "sk-ant".to_string(),
        models: vec![],
        priority: 0,
        fable_included: None,
        allow_nonstandard_host: None,
    }];
    let err = validate_endpoints(&endpoints).unwrap_err();
    assert!(
        err.contains("primary"),
        "error must name the endpoint: {err}"
    );
    assert!(
        err.contains("allow_nonstandard_host"),
        "error must name the opt-out: {err}"
    );
}

#[test]
fn validate_endpoints_allows_non_anthropic_host_with_opt_in() {
    // LAB-1191 AC-2: explicit opt-in keeps the old warn-only behaviour.
    let endpoints = vec![EndpointConfig {
        name: "staging".to_string(),
        protocol: Protocol::Anthropic,
        base_url: Some("https://staging.anthropic.example".to_string()),
        token: "sk-ant".to_string(),
        models: vec![],
        priority: 0,
        fable_included: None,
        allow_nonstandard_host: Some(true),
    }];
    assert!(validate_endpoints(&endpoints).is_ok());
}

#[test]
fn validate_endpoints_rejects_lookalike_anthropic_host() {
    // LAB-1191 AC-3: exact-host comparison — a canonical-prefix lookalike
    // domain must NOT pass as canonical.
    let endpoints = vec![EndpointConfig {
        name: "evil".to_string(),
        protocol: Protocol::Anthropic,
        base_url: Some("https://api.anthropic.com.evil.example".to_string()),
        token: "sk-ant".to_string(),
        models: vec![],
        priority: 0,
        fable_included: None,
        allow_nonstandard_host: None,
    }];
    let err = validate_endpoints(&endpoints).unwrap_err();
    assert!(err.contains("evil"));
}

#[test]
fn validate_endpoints_accepts_canonical_anthropic_host() {
    // LAB-1191 AC-3: the canonical host boots without opt-in.
    let endpoints = vec![EndpointConfig {
        name: "primary".to_string(),
        protocol: Protocol::Anthropic,
        base_url: Some("https://api.anthropic.com".to_string()),
        token: "sk-ant".to_string(),
        models: vec![],
        priority: 0,
        fable_included: None,
        allow_nonstandard_host: None,
    }];
    assert!(validate_endpoints(&endpoints).is_ok());
}

#[test]
fn validate_endpoints_rejects_http_base_url() {
    let endpoints = vec![EndpointConfig {
        name: "primary".to_string(),
        protocol: Protocol::Anthropic,
        base_url: Some("http://insecure.example".to_string()),
        token: "sk-ant".to_string(),
        models: vec![],
        priority: 0,
        fable_included: None,
        allow_nonstandard_host: None,
    }];
    let err = validate_endpoints(&endpoints).unwrap_err();
    assert!(err.contains("https"), "error must mention https: {err}");
    assert!(err.contains("primary"));
}

#[test]
fn validate_endpoints_requires_base_url_for_openai() {
    let endpoints = vec![EndpointConfig {
        name: "gateway".to_string(),
        protocol: Protocol::OpenAI,
        base_url: None,
        token: "sk-test".to_string(),
        models: vec![],
        priority: 100,
        fable_included: None,
        allow_nonstandard_host: None,
    }];
    let err = validate_endpoints(&endpoints).unwrap_err();
    assert!(err.contains("base_url"));
    assert!(err.contains("gateway"));
}

#[test]
fn validate_endpoints_accepts_well_formed_mix() {
    let endpoints = vec![
        EndpointConfig {
            name: "primary".to_string(),
            protocol: Protocol::Anthropic,
            base_url: None,
            token: "sk-ant".to_string(),
            models: vec![],
            priority: 0,
            fable_included: None,
            allow_nonstandard_host: None,
        },
        EndpointConfig {
            name: "gateway".to_string(),
            protocol: Protocol::OpenAI,
            base_url: Some("https://gateway.example".to_string()),
            token: "sk-test".to_string(),
            models: vec![],
            priority: 100,
            fable_included: None,
            allow_nonstandard_host: None,
        },
    ];
    assert!(validate_endpoints(&endpoints).is_ok());
}

// ── Helpers ──────────────────────────────────────────────────────

/// Build an Anthropic-protocol `Endpoint` with the given name and token.
/// Token prefix drives auth behavior (`sk-ant-oat*` = OAuth,
/// `"passthrough"` = passthrough). Callers that need a non-default field
/// (priority, base_url, models) mutate the returned struct.
fn mk_endpoint(name: &str, token: &str) -> Endpoint {
    Endpoint {
        name: name.to_string(),
        protocol: Protocol::Anthropic,
        base_url: "https://api.anthropic.com".to_string(),
        token: token.to_string(),
        passthrough: token == "passthrough",
        models: vec![],
        priority: 0,
        fable_included: true,
        requests: AtomicU64::new(0),
        rate_info: RwLock::new(RateLimitInfo::default()),
        burn_rate: Mutex::new(BurnRate::new()),
        input_tokens: AtomicU64::new(0),
        output_tokens: AtomicU64::new(0),
        cache_creation_tokens: AtomicU64::new(0),
        cache_read_tokens: AtomicU64::new(0),
        last_routing_weight: AtomicU64::new(0),
        last_routing_share: AtomicU64::new(0),
        last_effective_gate: AtomicU64::new(0),
    }
}

/// Anthropic-protocol `Endpoint` whose `base_url` points at a given
/// upstream — for integration tests that forward to a mock server.
fn mk_endpoint_at(name: &str, token: &str, base_url: &str) -> Endpoint {
    let mut ep = mk_endpoint(name, token);
    ep.base_url = base_url.to_string();
    ep
}

/// Shared fixture for the `Endpoint` pool, parameterized by protocol.
/// Callers that need a non-default field (priority, token, base_url,
/// models) mutate the returned struct.
fn make_endpoint(name: &str, protocol: Protocol) -> Endpoint {
    Endpoint {
        name: name.to_string(),
        protocol,
        base_url: match protocol {
            Protocol::Anthropic => "https://api.anthropic.com".to_string(),
            Protocol::OpenAI => "https://gateway.example".to_string(),
        },
        token: "sk-test".to_string(),
        passthrough: false,
        models: vec![],
        priority: 0,
        fable_included: true,
        requests: AtomicU64::new(0),
        rate_info: RwLock::new(RateLimitInfo::default()),
        burn_rate: Mutex::new(BurnRate::new()),
        input_tokens: AtomicU64::new(0),
        output_tokens: AtomicU64::new(0),
        cache_creation_tokens: AtomicU64::new(0),
        cache_read_tokens: AtomicU64::new(0),
        last_routing_weight: AtomicU64::new(0),
        last_routing_share: AtomicU64::new(0),
        last_effective_gate: AtomicU64::new(0),
    }
}

/// Canonical `AppState` test default — the single place every test fixture
/// derives from. Tests build state via `AppState { <overrides>, ..test_state_base() }`
/// so adding a field to `AppState` is a one-line edit here (plus the
/// config-derived production constructor), not a 49-site shotgun edit.
///
/// Note: `soft_limit` is 1.0 here (open ceiling for routing tests), NOT
/// production's 0.90.
fn test_state_base() -> AppState {
    AppState {
        // Production knob chain (incl. redirect Policy::none — LAB-1191) with
        // a short test timeout layered on top.
        client: upstream_client_builder()
            .timeout(Duration::from_secs(5))
            .build()
            .unwrap(),
        client_nonstreaming: upstream_client_builder()
            .timeout(Duration::from_secs(5))
            .build()
            .unwrap(),
        endpoints: vec![],
        robin: AtomicUsize::new(0),
        routing_strategy: RoutingStrategy::default(),
        cooldown: Duration::from_secs(60),
        transport_cooldown: TRANSPORT_UNHEALTHY_COOLDOWN,
        state_path: PathBuf::from("/tmp/anthropic-lb-test.state.json"),
        proxy_key: None,
        clients: vec![],
        allowed_ips: vec![],
        trusted_proxies: vec![],
        auth_throttle: AuthThrottle::new(
            DEFAULT_AUTH_FAILURE_LIMIT,
            Duration::from_secs(DEFAULT_AUTH_FAILURE_WINDOW_SECS),
        ),
        auth_failures: Mutex::new(HashMap::new()),
        open_admin_warn: Mutex::new(HashMap::new()),
        client_names: HashMap::new(),
        auto_cache: true,
        client_usage: Mutex::new(HashMap::new()),
        shadow_log_tx: None,
        shadow_log_dropped: AtomicU64::new(0),
        client_budgets: HashMap::new(),
        budget_usage: Mutex::new(HashMap::new()),
        client_utilization_limits: HashMap::new(),
        operators: vec![],
        emergency_brake: true,
        emergency_threshold: DEFAULT_EMERGENCY_THRESHOLD,
        client_request_rates: Mutex::new(HashMap::new()),
        soft_limit: 1.0,
        redis: None,
        // true, not production's false: every fixture that overrides `redis`
        // hands in a client that CONNECTED at creation (`fred_test_client`
        // panics if it can't), so the ever-connected gate is open — exactly
        // the post-first-connect state those tests exercise. The LAB-1639
        // startup-outage test overrides this back to false.
        redis_ever_connected: AtomicBool::new(true),
        cluster_info_cache: Mutex::new(None),
        next_req_id: AtomicU64::new(0),
        instance_id: 0,
        probe_interval_secs: 300,
        overage_penalty: 10,
        upstream_transport_errors: Mutex::new(HashMap::new()),
        inflight_body_bytes: AtomicU64::new(0),
        max_inflight_body_bytes: 0,
        body_shed_total: AtomicU64::new(0),
        body_read_timeout: Duration::from_secs(DEFAULT_BODY_READ_TIMEOUT_SECS),
        body_read_timeout_total: AtomicU64::new(0),
        sessions: Mutex::new(HashMap::new()),
        session_registry_max: DEFAULT_SESSION_REGISTRY_MAX,
        session_registry_ttl_secs: DEFAULT_SESSION_REGISTRY_TTL_SECS,
        expose_upstream_ratelimit_headers: false,
        allowed_client_betas: DEFAULT_CLIENT_BETA_ALLOWLIST
            .iter()
            .map(|s| s.to_string())
            .collect(),
        beta_flags_dropped: Mutex::new(HashMap::new()),
        prompt_too_long: Mutex::new(HashMap::new()),
        model_denied: Mutex::new(HashMap::new()),
        unsupported_models: Mutex::new(HashMap::new()),
        response_cache: None,
    }
}

fn test_state_with_strategy(
    endpoints: Vec<Endpoint>,
    routing_strategy: RoutingStrategy,
) -> Arc<AppState> {
    Arc::new(AppState {
        endpoints,
        routing_strategy,
        ..test_state_base()
    })
}

fn test_state_with(endpoints: Vec<Endpoint>) -> Arc<AppState> {
    test_state_with_strategy(endpoints, RoutingStrategy::default())
}

fn test_state_with_soft_limit(endpoints: Vec<Endpoint>, soft_limit: f64) -> Arc<AppState> {
    let mut state = test_state_with(endpoints);
    Arc::get_mut(&mut state)
        .expect("test fixture should be uniquely owned")
        .soft_limit = soft_limit;
    state
}

/// `test_state_with(vec![])` with a session-registry cap override (0 = off).
/// Registry unit tests don't route, so no endpoints are needed.
fn test_state_with_session_max(max: usize) -> Arc<AppState> {
    let mut state = test_state_with(vec![]);
    Arc::get_mut(&mut state)
        .expect("test fixture should be uniquely owned")
        .session_registry_max = max;
    state
}

/// Spawn a mock upstream that returns a canned response with rate-limit headers.
async fn spawn_mock_upstream() -> (String, tokio::task::JoinHandle<()>) {
    let app = Router::new().fallback(any(mock_upstream_handler));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let handle = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (format!("http://{}", addr), handle)
}

async fn mock_upstream_handler(req: Request<Body>) -> Response {
    let has_auth =
        req.headers().contains_key("x-api-key") || req.headers().contains_key("authorization");

    if !has_auth {
        return (StatusCode::UNAUTHORIZED, "missing auth").into_response();
    }

    let mut resp = axum::Json(serde_json::json!({
        "id": "msg_test",
        "type": "message",
        "content": [{"type": "text", "text": "ok"}],
    }))
    .into_response();

    // Inject rate-limit headers the proxy expects
    let headers = resp.headers_mut();
    headers.insert(
        "anthropic-ratelimit-unified-representative-claim",
        HeaderValue::from_static("five_hour"),
    );
    headers.insert(
        "anthropic-ratelimit-unified-5h-utilization",
        HeaderValue::from_static("0.25"),
    );
    // Valid reset 1h in the future so unified derivation includes 5h
    let reset_epoch = (std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
        + 3600)
        .to_string();
    headers.insert(
        "anthropic-ratelimit-unified-5h-reset",
        HeaderValue::from_str(&reset_epoch).unwrap(),
    );
    // Headers a real upstream may attach that must NOT reach the caller by
    // default (LAB-1191 finding 3) + one allow-listed header that must.
    headers.insert(
        "anthropic-ratelimit-unified-5h-status",
        HeaderValue::from_static("allowed"),
    );
    headers.insert("set-cookie", HeaderValue::from_static("upstream=leak"));
    headers.insert(
        "anthropic-organization-id",
        HeaderValue::from_static("org-secret"),
    );
    headers.insert("request-id", HeaderValue::from_static("req_mock_123"));
    resp
}

/// Build the full app router against a given upstream URL. The two
/// Anthropic endpoints both point at `upstream_url` (the mock upstream).
fn test_app_with_strategy(
    upstream_url: &str,
    proxy_key: Option<String>,
    routing_strategy: RoutingStrategy,
) -> (Router, Arc<AppState>) {
    let mut acct_a = mk_endpoint("acct-a", "sk-ant-api-test-aaa");
    acct_a.base_url = upstream_url.to_string();
    let mut acct_b = mk_endpoint("acct-b", "sk-ant-api-test-bbb");
    acct_b.base_url = upstream_url.to_string();
    let endpoints = vec![acct_a, acct_b];

    let state = Arc::new(AppState {
        endpoints,
        routing_strategy,
        proxy_key,
        ..test_state_base()
    });

    (build_router(state.clone()), state)
}

/// Build a router from a pre-configured state. Used by integration tests
/// that need custom AppState (operator, utilization limits, etc.).
fn build_router(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/_stats", axum::routing::get(stats_handler))
        .route("/metrics", axum::routing::get(metrics_handler))
        .route(
            "/v1/chat/completions",
            axum::routing::post(openai_chat_handler),
        )
        .fallback(any(proxy_handler))
        .with_state(state)
}

/// Start a test server and return its address. Spawns the axum server
/// with ConnectInfo support for client IP extraction.
async fn serve(app: Router) -> SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(
            listener,
            app.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await
        .unwrap();
    });
    addr
}

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

// ── Unit: pick_account ──────────────────────────────────────────

#[tokio::test]
async fn pick_prefers_lowest_utilization() {
    // With weighted buckets, the account with more headroom should get
    // a proportionally larger share of traffic
    let state = test_state_with(vec![
        mk_endpoint("high", "sk-ant-api-high"),
        mk_endpoint("low", "sk-ant-api-low"),
    ]);

    // high=0.8 (headroom 0.2), low=0.2 (headroom 0.8) → 80% should go to "low"
    {
        let mut info = state.endpoints[0].rate_info.write().await;
        info.utilization = Some(0.8);
    }
    {
        let mut info = state.endpoints[1].rate_info.write().await;
        info.utilization = Some(0.2);
    }

    let mut counts = [0u32; 2];
    for _ in 0..1000 {
        let idx = state.pick_endpoint(None, "", &[]).await.unwrap();
        counts[idx] += 1;
    }

    // "low" (idx=1) should get ~80% of traffic (±5%)
    let low_pct = counts[1] as f64 / 1000.0;
    assert!(
        (0.75..=0.85).contains(&low_pct),
        "low-util account should get ~80% traffic, got {:.1}%",
        low_pct * 100.0
    );
}

#[tokio::test]
async fn pick_skips_hard_limited() {
    let state = test_state_with(vec![
        mk_endpoint("limited", "sk-ant-api-a"),
        mk_endpoint("available", "sk-ant-api-b"),
    ]);

    // Hard-limit the first account
    {
        let mut info = state.endpoints[0].rate_info.write().await;
        info.utilization = Some(0.1); // great utilization but hard-limited
        info.hard_limited_until = Some(Instant::now() + Duration::from_secs(3600));
    }
    {
        let mut info = state.endpoints[1].rate_info.write().await;
        info.utilization = Some(0.9);
    }

    let idx = state.pick_endpoint(None, "", &[]).await.unwrap();
    assert_eq!(
        idx, 1,
        "should skip hard-limited account despite lower utilization"
    );
}

#[tokio::test]
async fn pick_round_robin_when_no_info() {
    // With no utilization data, all accounts get headroom=0.5 (equal buckets)
    let state = test_state_with(vec![
        mk_endpoint("a", "sk-ant-api-a"),
        mk_endpoint("b", "sk-ant-api-b"),
        mk_endpoint("c", "sk-ant-api-c"),
    ]);

    // Call many times without affinity — Fibonacci scatter should distribute evenly
    let mut counts = [0u32; 3];
    for _ in 0..300 {
        let idx = state.pick_endpoint(None, "", &[]).await.unwrap();
        counts[idx] += 1;
    }

    // Each should get ~33% (±10%)
    for (i, &count) in counts.iter().enumerate() {
        let pct = count as f64 / 300.0;
        assert!(
            (0.23..=0.43).contains(&pct),
            "account {} should get ~33% traffic, got {:.1}%",
            i,
            pct * 100.0
        );
    }
}

#[tokio::test]
async fn pick_returns_none_when_all_limited() {
    let state = test_state_with(vec![
        mk_endpoint("a", "sk-ant-api-a"),
        mk_endpoint("b", "sk-ant-api-b"),
    ]);

    for acct in &state.endpoints {
        let mut info = acct.rate_info.write().await;
        info.hard_limited_until = Some(Instant::now() + Duration::from_secs(3600));
    }

    assert!(state.pick_endpoint(None, "", &[]).await.is_none());
}

#[tokio::test]
async fn pick_recovers_after_hard_limit_expires() {
    // After a hard limit expires with stale data, the account should still be
    // selectable with 0.5 (unknown) utilization instead of being permanently stuck.
    let state = test_state_with(vec![mk_endpoint("recovering", "sk-ant-api-a")]);

    // Simulate mark_hard_limited: set hard_limited_until in the past (expired),
    // poison remaining_tokens to 0, set high utilization from the 429 response.
    {
        let mut info = state.endpoints[0].rate_info.write().await;
        let hard_limit_time = Instant::now() - Duration::from_secs(10);
        info.hard_limited_until = Some(hard_limit_time);
        info.remaining_tokens = Some(0);
        info.remaining_requests = Some(0);
        info.utilization = Some(1.0);
        info.utilization_5h = Some(1.0);
        // last_updated before the hard limit → stale_after_hard_limit = true
        info.last_updated = Some(hard_limit_time - Duration::from_secs(1));
    }

    let result = state.pick_endpoint(None, "", &[]).await;
    assert!(
        result.is_some(),
        "account with expired hard limit should be selectable despite stale high utilization"
    );
}

#[tokio::test]
async fn pick_ignores_stale_rejected_claim_after_hard_limit() {
    // A "rejected" 7d claim from a 429 response should not permanently block the
    // account once the hard limit has expired without fresh data.
    let state = test_state_with(vec![mk_endpoint("recovering", "sk-ant-api-a")]);
    let now_epoch = AppState::now_epoch();

    {
        let mut info = state.endpoints[0].rate_info.write().await;
        let hard_limit_time = Instant::now() - Duration::from_secs(10);
        info.hard_limited_until = Some(hard_limit_time);
        info.last_updated = Some(hard_limit_time - Duration::from_secs(1));
        info.utilization_5h = Some(0.95);
        info.reset_5h = Some(now_epoch + 10000);
        info.claims_7d.insert(
            "seven_day_sonnet".to_string(),
            ClaimWindowData {
                utilization: Some(1.0),
                reset: Some(now_epoch + 100000),
                status: Some("rejected".to_string()),
                ..Default::default()
            },
        );
    }

    let result = state
        .pick_endpoint(Some("test"), "claude-sonnet-4-6", &[])
        .await;
    assert!(
        result.is_some(),
        "stale rejected claim after expired hard limit should not block account"
    );
}

#[tokio::test]
async fn pick_still_skips_fresh_rejected_claim() {
    // If data was refreshed AFTER the hard limit (e.g., by a probe that got fresh
    // "rejected" status), the account should still be skipped.
    let state = test_state_with(vec![
        mk_endpoint("rejected", "sk-ant-api-a"),
        mk_endpoint("available", "sk-ant-api-b"),
    ]);
    let now_epoch = AppState::now_epoch();

    {
        let mut info = state.endpoints[0].rate_info.write().await;
        let hard_limit_time = Instant::now() - Duration::from_secs(300);
        info.hard_limited_until = Some(hard_limit_time);
        // last_updated AFTER the hard limit → data is fresh, not stale
        info.last_updated = Some(Instant::now() - Duration::from_secs(5));
        info.utilization_5h = Some(0.30);
        info.reset_5h = Some(now_epoch + 10000);
        info.claims_7d.insert(
            "seven_day_sonnet".to_string(),
            ClaimWindowData {
                utilization: Some(1.0),
                reset: Some(now_epoch + 100000),
                status: Some("rejected".to_string()),
                ..Default::default()
            },
        );
    }
    {
        let mut info = state.endpoints[1].rate_info.write().await;
        info.utilization = Some(0.5);
    }

    let result = state
        .pick_endpoint(Some("test"), "claude-sonnet-4-6", &[])
        .await;
    assert_eq!(
        result,
        Some(1),
        "fresh rejected claim should still skip the account"
    );
}

#[tokio::test]
async fn pick_ignores_expired_rejected_claim_without_hard_limit() {
    let state = test_state_with(vec![
        mk_endpoint("recovered", "sk-ant-api-a"),
        mk_endpoint("available", "sk-ant-api-b"),
    ]);
    let now_epoch = AppState::now_epoch();

    {
        let mut info = state.endpoints[0].rate_info.write().await;
        info.utilization = Some(0.30);
        info.utilization_5h = Some(0.30);
        info.reset_5h = Some(now_epoch + 10000);
        info.claims_7d.insert(
            "seven_day_sonnet".to_string(),
            ClaimWindowData {
                utilization: Some(1.0),
                reset: Some(now_epoch.saturating_sub(1)),
                status: Some("rejected".to_string()),
                ..Default::default()
            },
        );
    }
    {
        let mut info = state.endpoints[1].rate_info.write().await;
        info.utilization = Some(0.80);
        info.utilization_5h = Some(0.80);
        info.reset_5h = Some(now_epoch + 10000);
    }

    let result = state
        .pick_endpoint(Some("test"), "claude-sonnet-4-6", &[])
        .await;
    assert_eq!(
        result,
        Some(0),
        "expired rejected claim should not block account selection"
    );
}

#[tokio::test]
async fn pick_uses_fresh_data_after_hard_limit_cleared() {
    // After a probe clears hard_limited_until and refreshes data, the normal
    // routing logic should apply (not the 0.5 fallback).
    let state = test_state_with(vec![
        mk_endpoint("low_util", "sk-ant-api-a"),
        mk_endpoint("high_util", "sk-ant-api-b"),
    ]);

    {
        // hard_limited_until is None (cleared by probe), fresh data available
        let mut info = state.endpoints[0].rate_info.write().await;
        info.utilization = Some(0.2);
        info.last_updated = Some(Instant::now());
    }
    {
        let mut info = state.endpoints[1].rate_info.write().await;
        info.utilization = Some(0.8);
        info.last_updated = Some(Instant::now());
    }

    // low_util should get ~80% of traffic (headroom=0.8 vs 0.2)
    let mut counts = [0u32; 2];
    for _ in 0..1000 {
        let idx = state.pick_endpoint(None, "", &[]).await.unwrap();
        counts[idx] += 1;
    }
    let low_pct = counts[0] as f64 / 1000.0;
    assert!(
        low_pct > 0.70,
        "low-util account should get majority of traffic, got {:.1}%",
        low_pct * 100.0
    );
}

#[tokio::test]
async fn mark_hard_limited_detects_burst_429() {
    // Burst 429: x-should-retry=true, no retry-after, no rate-limit headers.
    // Should use short cooldown, NOT poison remaining_tokens/requests.
    let state = test_state_with(vec![mk_endpoint("burst-test", "sk-ant-api-a")]);

    // Pre-set some remaining tokens to verify they aren't poisoned
    {
        let mut info = state.endpoints[0].rate_info.write().await;
        info.remaining_tokens = Some(5000);
        info.remaining_requests = Some(10);
    }

    let mut headers = reqwest::header::HeaderMap::new();
    headers.insert("x-should-retry", HeaderValue::from_static("true"));
    // No retry-after, no anthropic-ratelimit-*, no x-ratelimit-*

    state.mark_hard_limited(0, &headers).await;

    let info = state.endpoints[0].rate_info.read().await;
    assert!(
        info.hard_limited_until.is_some(),
        "burst 429 should still set hard_limited_until"
    );
    assert_eq!(
        info.remaining_tokens,
        Some(5000),
        "burst 429 should NOT poison remaining_tokens"
    );
    assert_eq!(
        info.remaining_requests,
        Some(10),
        "burst 429 should NOT poison remaining_requests"
    );
    assert_eq!(info.consecutive_burst_429s, 1);
}

#[tokio::test]
async fn mark_hard_limited_capacity_429_poisons_state() {
    // Capacity 429: has rate-limit headers → should poison remaining_tokens/requests to 0.
    let state = test_state_with(vec![mk_endpoint("cap-test", "sk-ant-api-a")]);

    {
        let mut info = state.endpoints[0].rate_info.write().await;
        info.remaining_tokens = Some(5000);
        info.remaining_requests = Some(10);
    }

    let mut headers = reqwest::header::HeaderMap::new();
    headers.insert("x-should-retry", HeaderValue::from_static("true"));
    headers.insert(
        "anthropic-ratelimit-unified-5h-utilization",
        HeaderValue::from_static("0.99"),
    );
    // Has rate-limit headers → NOT a burst

    state.mark_hard_limited(0, &headers).await;

    let info = state.endpoints[0].rate_info.read().await;
    assert_eq!(
        info.remaining_tokens,
        Some(0),
        "capacity 429 should poison remaining_tokens to 0"
    );
    assert_eq!(
        info.remaining_requests,
        Some(0),
        "capacity 429 should poison remaining_requests to 0"
    );
    assert_eq!(
        info.consecutive_burst_429s, 0,
        "capacity 429 should reset burst counter"
    );
}

#[tokio::test]
async fn mark_hard_limited_burst_exponential_backoff() {
    // Consecutive burst 429s should produce increasing cooldowns.
    let state = test_state_with(vec![mk_endpoint("backoff-test", "sk-ant-api-a")]);

    let mut headers = reqwest::header::HeaderMap::new();
    headers.insert("x-should-retry", HeaderValue::from_static("true"));

    // Fire 5 consecutive burst 429s
    let mut cooldowns = Vec::new();
    for _ in 0..5 {
        state.mark_hard_limited(0, &headers).await;
        let info = state.endpoints[0].rate_info.read().await;
        let until = info.hard_limited_until.unwrap();
        let remaining = until.duration_since(Instant::now());
        cooldowns.push(remaining.as_secs());
    }

    // Should be roughly 5, 10, 20, 40, 60 (with some timing slack)
    assert!(
        cooldowns[0] <= 6,
        "1st burst should be ~5s, got {}s",
        cooldowns[0]
    );
    assert!(
        (9..=11).contains(&cooldowns[1]),
        "2nd burst should be ~10s, got {}s",
        cooldowns[1]
    );
    assert!(
        (19..=21).contains(&cooldowns[2]),
        "3rd burst should be ~20s, got {}s",
        cooldowns[2]
    );
    assert!(
        (39..=41).contains(&cooldowns[3]),
        "4th burst should be ~40s, got {}s",
        cooldowns[3]
    );
    assert!(
        (59..=61).contains(&cooldowns[4]),
        "5th burst should be ~60s, got {}s",
        cooldowns[4]
    );

    let info = state.endpoints[0].rate_info.read().await;
    assert_eq!(info.consecutive_burst_429s, 5);
}

#[tokio::test]
async fn mark_hard_limited_retry_after_overrides_default() {
    // When retry-after is present, it should be used regardless of x-should-retry.
    let state = test_state_with(vec![mk_endpoint("retry-test", "sk-ant-api-a")]);

    let mut headers = reqwest::header::HeaderMap::new();
    headers.insert("retry-after", HeaderValue::from_static("30"));

    state.mark_hard_limited(0, &headers).await;

    let info = state.endpoints[0].rate_info.read().await;
    let until = info.hard_limited_until.unwrap();
    let remaining = until.duration_since(Instant::now()).as_secs();
    assert!(
        (29..=31).contains(&remaining),
        "retry-after=30 should set ~30s cooldown, got {}s",
        remaining
    );
    assert_eq!(
        info.consecutive_burst_429s, 0,
        "non-burst 429 should reset burst counter"
    );
}

#[tokio::test]
async fn pick_does_not_bias_unknown_accounts() {
    // Unknown accounts get headroom=0.5, known account with 0.1 util gets headroom=0.9
    // Traffic should favor the known account proportionally
    let state = test_state_with(vec![
        mk_endpoint("known", "sk-ant-api-known"),
        mk_endpoint("unknown", "sk-ant-api-unknown"),
    ]);

    {
        let mut info = state.endpoints[0].rate_info.write().await;
        info.utilization = Some(0.1); // headroom = 0.9
    }
    // accounts[1] has no rate info → headroom = 0.5

    // known should get ~64% (0.9 / 1.4), unknown ~36% (0.5 / 1.4)
    let mut counts = [0u32; 2];
    for _ in 0..1000 {
        let idx = state.pick_endpoint(None, "", &[]).await.unwrap();
        counts[idx] += 1;
    }

    let known_pct = counts[0] as f64 / 1000.0;
    assert!(
        (0.57..=0.71).contains(&known_pct),
        "known account should get ~64% traffic, got {:.1}%",
        known_pct * 100.0
    );
}

#[tokio::test]
async fn pick_sticky_same_affinity() {
    // Same affinity key should always return the same account when weights
    // are close enough. AFFINITY_OVERRIDE_RATIO (0.5) compares the picked
    // account's weight to the best — affinity is preserved when the ratio
    // exceeds the threshold, i.e. no single account is 2x better.
    let state = test_state_with(vec![
        mk_endpoint("a", "sk-ant-api-a"),
        mk_endpoint("b", "sk-ant-api-b"),
        mk_endpoint("c", "sk-ant-api-c"),
    ]);

    // Similar utilization → similar weights → ratio stays above 0.5
    {
        let mut info = state.endpoints[0].rate_info.write().await;
        info.utilization = Some(0.40);
    }
    {
        let mut info = state.endpoints[1].rate_info.write().await;
        info.utilization = Some(0.45);
    }
    {
        let mut info = state.endpoints[2].rate_info.write().await;
        info.utilization = Some(0.50);
    }

    let key = "192.168.1.1:client-42:agent-7:session-abc";
    let first = state.pick_endpoint(Some(key), "", &[]).await.unwrap();
    for _ in 0..100 {
        let idx = state.pick_endpoint(Some(key), "", &[]).await.unwrap();
        assert_eq!(
            idx, first,
            "same affinity key must always pick same account"
        );
    }
}

#[tokio::test]
async fn pick_affinity_overridden_by_weight_disparity() {
    // When the affinity-picked account's weight is less than 50% of the best
    // account's weight, affinity should be overridden. This tests the scenario
    // where 5h utilization is similar but 7d utilization is vastly different —
    // the weight formula captures the 7d disparity via waste_risk.
    let state = test_state_with(vec![
        mk_endpoint("low_7d", "sk-ant-api-a"),
        mk_endpoint("high_7d", "sk-ant-api-b"),
    ]);
    let now_epoch = AppState::now_epoch();

    // Both have similar 5h utilization (so gate_5h is similar)
    // but vastly different 7d utilization via claims_7d.
    {
        let mut info = state.endpoints[0].rate_info.write().await;
        info.utilization_5h = Some(0.12);
        info.reset_5h = Some(now_epoch + 10000);
        info.claims_7d.insert(
            "seven_day".to_string(),
            ClaimWindowData {
                utilization: Some(0.30),
                reset: Some(now_epoch + 300000),
                status: None,
                ..Default::default()
            },
        );
    }
    {
        let mut info = state.endpoints[1].rate_info.write().await;
        info.utilization_5h = Some(0.10);
        info.reset_5h = Some(now_epoch + 10000);
        info.claims_7d.insert(
            "seven_day".to_string(),
            ClaimWindowData {
                utilization: Some(0.85),
                reset: Some(now_epoch + 300000),
                status: None,
                ..Default::default()
            },
        );
    }

    // Every affinity key should pick the low-7d account because
    // its weight is much higher (more waste_risk × similar headroom).
    for i in 0..200 {
        let key = format!("sticky-client-{}", i);
        let idx = state
            .pick_endpoint(Some(&key), "claude-opus-4-6", &[])
            .await
            .unwrap();
        assert_eq!(
            idx, 0,
            "client {} picked account {} but should pick 'low_7d' (weight ratio < 0.5)",
            i, idx
        );
    }
}

/// Helper: run `pick_account` with varying affinity keys and assert distribution.
/// `expect_index: None` → both accounts must be seen (affinity preserved).
/// `expect_index: Some(i)` → every pick must equal `i` (override active).
async fn assert_affinity_distribution(
    state: &AppState,
    prefix: &str,
    attempts: usize,
    expect_index: Option<usize>,
    msg: &str,
) {
    let mut saw_0 = false;
    let mut saw_1 = false;
    for i in 0..attempts {
        let key = format!("{}-{}", prefix, i);
        let idx = state
            .pick_endpoint(Some(&key), "claude-opus-4-6", &[])
            .await
            .unwrap();
        match expect_index {
            Some(expected) => {
                assert_eq!(idx, expected, "attempt {}: {}", i, msg);
            }
            None => {
                if idx == 0 {
                    saw_0 = true;
                } else {
                    saw_1 = true;
                }
                if saw_0 && saw_1 {
                    return;
                }
            }
        }
    }
    if expect_index.is_none() {
        assert!(saw_0 && saw_1, "{}", msg);
    }
}

#[tokio::test]
async fn affinity_override_balanced_no_7d_data() {
    // Scenario: balanced 5h, no 7d data → affinity preserved
    // Primary 5h=0.20, Jeff 5h=0.25, no claims_7d
    // Weights: headroom_only → 0.80 vs 0.75, ratio=0.94 > 0.5
    let state = test_state_with(vec![
        mk_endpoint("primary", "sk-ant-api-a"),
        mk_endpoint("jeff", "sk-ant-api-b"),
    ]);
    {
        let mut info = state.endpoints[0].rate_info.write().await;
        info.utilization_5h = Some(0.20);
        info.reset_5h = Some(AppState::now_epoch() + 10000);
    }
    {
        let mut info = state.endpoints[1].rate_info.write().await;
        info.utilization_5h = Some(0.25);
        info.reset_5h = Some(AppState::now_epoch() + 10000);
    }

    assert_affinity_distribution(
        &state,
        "balanced-client",
        500,
        None,
        "balanced accounts should see traffic on both (affinity preserved)",
    )
    .await;
}

#[tokio::test]
async fn affinity_override_moderate_7d_disparity() {
    // Scenario: similar 5h, moderate 7d difference → affinity preserved
    // Primary 5h=0.15, 7d=0.40 vs Jeff 5h=0.15, 7d=0.60
    // waste_risk ratio isn't extreme enough to trigger override
    let state = test_state_with(vec![
        mk_endpoint("primary", "sk-ant-api-a"),
        mk_endpoint("jeff", "sk-ant-api-b"),
    ]);
    let now = AppState::now_epoch();
    set_account_utilization(&state, 0, 0.15, 0.40, now + 10000, now + 300000).await;
    set_account_utilization(&state, 1, 0.15, 0.60, now + 10000, now + 300000).await;

    assert_affinity_distribution(
        &state,
        "moderate-client",
        500,
        None,
        "moderate disparity should preserve affinity on both accounts",
    )
    .await;
}

#[tokio::test]
async fn affinity_override_massive_7d_disparity() {
    // Scenario: egregious disparity — one account nearly spent, other fresh
    // Primary 5h=0.10, 7d=0.10 vs Jeff 5h=0.10, 7d=0.95
    // Primary's waste_risk is enormous, jeff's is near zero
    // Weight ratio far below 0.25 → all traffic overridden to primary
    let state = test_state_with(vec![
        mk_endpoint("primary", "sk-ant-api-a"),
        mk_endpoint("jeff", "sk-ant-api-b"),
    ]);
    let now = AppState::now_epoch();
    set_account_utilization(&state, 0, 0.10, 0.10, now + 10000, now + 300000).await;
    set_account_utilization(&state, 1, 0.10, 0.95, now + 10000, now + 300000).await;

    assert_affinity_distribution(
        &state,
        "production-client",
        200,
        Some(0),
        "routed to jeff despite massive 7d disparity",
    )
    .await;
}

#[tokio::test]
async fn affinity_override_one_exhausted() {
    // Scenario: one account nearly spent on 7d budget
    // Primary 5h=0.10, 7d=0.10 vs Jeff 5h=0.10, 7d=0.90
    // Extreme weight ratio → all traffic to primary
    let state = test_state_with(vec![
        mk_endpoint("primary", "sk-ant-api-a"),
        mk_endpoint("jeff", "sk-ant-api-b"),
    ]);
    let now = AppState::now_epoch();
    set_account_utilization(&state, 0, 0.10, 0.10, now + 10000, now + 300000).await;
    set_account_utilization(&state, 1, 0.10, 0.90, now + 10000, now + 300000).await;

    assert_affinity_distribution(
        &state,
        "exhausted-client",
        200,
        Some(0),
        "routed to nearly-exhausted account",
    )
    .await;
}

#[tokio::test]
async fn affinity_override_both_rough() {
    // Scenario: both accounts in bad shape — affinity preserved
    // Primary 5h=0.85, 7d=0.20 vs Jeff 5h=0.10, 7d=0.90
    // Primary has 5h pressure but 7d budget; Jeff has 5h headroom but 7d exhausted
    // Weights should be close enough to preserve affinity
    let state = test_state_with(vec![
        mk_endpoint("primary", "sk-ant-api-a"),
        mk_endpoint("jeff", "sk-ant-api-b"),
    ]);
    let now = AppState::now_epoch();
    set_account_utilization(&state, 0, 0.85, 0.20, now + 10000, now + 300000).await;
    set_account_utilization(&state, 1, 0.10, 0.90, now + 10000, now + 300000).await;

    assert_affinity_distribution(
        &state,
        "both-rough",
        500,
        None,
        "both-rough scenario should preserve affinity on both accounts",
    )
    .await;
}

#[tokio::test]
async fn affinity_override_preserves_stickiness_with_moderate_disparity() {
    // With 3 candidates whose weights are moderately different (none below
    // 0.25 ratio to the best), the override does NOT fire — all three
    // accounts receive sticky traffic via proportional bucket hashing.
    let state = test_state_with_strategy(
        vec![
            mk_endpoint("primary", "sk-ant-api-a"),
            mk_endpoint("steve", "sk-ant-api-b"),
            mk_endpoint("jeff", "sk-ant-api-c"),
        ],
        RoutingStrategy::StickyWeightedV2,
    );
    let now = AppState::now_epoch();
    // primary is clearly best, steve middling, jeff worst
    set_account_utilization(&state, 0, 0.13, 0.41, now + 10000, now + 300000).await;
    set_account_utilization(&state, 1, 0.15, 0.55, now + 10000, now + 300000).await;
    set_account_utilization(&state, 2, 0.12, 0.79, now + 10000, now + 300000).await;

    let mut saw = [false; 3];
    for i in 0..1000 {
        let key = format!("three-way-client-{}", i);
        let idx = state
            .pick_endpoint(Some(&key), "claude-opus-4-6", &[])
            .await
            .unwrap();
        saw[idx] = true;
        if saw[0] && saw[1] && saw[2] {
            break;
        }
    }
    assert!(
        saw[0] && saw[1] && saw[2],
        "all 3 accounts should receive traffic with moderate disparity"
    );
}

#[tokio::test]
async fn affinity_override_fires_with_three_candidates_egregious_disparity() {
    // When one account is near-exhausted (85% util) and others have plenty
    // of headroom, sessions that hash into the exhausted account's tiny
    // bucket should be overridden to the best account.
    let state = test_state_with_strategy(
        vec![
            mk_endpoint("primary", "sk-ant-api-a"),
            mk_endpoint("jeff", "sk-ant-api-b"),
            mk_endpoint("insight", "sk-ant-api-c"),
        ],
        RoutingStrategy::StickyWeightedV2,
    );
    let now = AppState::now_epoch();
    set_account_utilization(&state, 0, 0.09, 0.09, now + 10000, now + 300000).await;
    set_account_utilization(&state, 1, 0.20, 0.31, now + 10000, now + 300000).await;
    set_account_utilization(&state, 2, 0.85, 0.85, now + 10000, now + 300000).await;

    // Find a key that would naturally hash into insight's bucket.
    // Candidates/weights are static, so compute boundaries once.
    let candidates = state.routing_candidates("claude-opus-4-6", &[]).await;
    let total_weight: f64 = candidates.iter().map(|c| c.weight).sum();
    let mut boundaries: Vec<(usize, f64)> = Vec::new();
    let mut cumulative = 0.0;
    for c in &candidates {
        cumulative += c.weight;
        boundaries.push((c.endpoint, cumulative));
    }

    let mut insight_key = None;
    for i in 0..10000 {
        let key = format!("find-insight-{}", i);
        let target = (stable_affinity_hash(&key) as f64 / u64::MAX as f64) * total_weight;
        for &(idx, boundary) in &boundaries {
            if target < boundary {
                if idx == 2 {
                    insight_key = Some(key);
                }
                break;
            }
        }
        if insight_key.is_some() {
            break;
        }
    }
    let key = insight_key.expect("should find a key that hashes to insight's bucket");

    // With the override, pick_account should redirect away from insight
    let idx = state
        .pick_endpoint(Some(&key), "claude-opus-4-6", &[])
        .await
        .unwrap();
    // New contract: the override migrates to a session-stable replacement
    // re-picked from the healthy remainder (deterministic per session, NOT the
    // global argmax). It must be one of the healthy accounts, never the loaded
    // one — but which healthy account is the session's stable hash, not "best".
    assert_ne!(
        idx, 2,
        "insight (85% util) should be overridden despite affinity hash landing there"
    );
    assert!(
        idx == 0 || idx == 1,
        "override must land on a healthy account (primary or jeff), got {idx}"
    );
}

/// Helper: under StickyWeightedV2, find affinity keys whose *sticky* bucket is
/// `target_idx` (i.e. the sessions that get overridden when that account is the
/// loaded one). Boundaries are static given fixed utilizations.
async fn keys_hashing_to(
    state: &AppState,
    target_idx: usize,
    want: usize,
    scan: usize,
    prefix: &str,
) -> Vec<String> {
    let candidates = state.routing_candidates("claude-opus-4-6", &[]).await;
    let total_weight: f64 = candidates.iter().map(|c| c.weight).sum();
    let mut boundaries: Vec<(usize, f64)> = Vec::new();
    let mut cumulative = 0.0;
    for c in &candidates {
        cumulative += c.weight;
        boundaries.push((c.endpoint, cumulative));
    }
    let mut out = Vec::new();
    for i in 0..scan {
        let key = format!("{}-{}", prefix, i);
        let target = (stable_affinity_hash(&key) as f64 / u64::MAX as f64) * total_weight;
        for &(idx, boundary) in &boundaries {
            if target < boundary {
                if idx == target_idx {
                    out.push(key);
                }
                break;
            }
        }
        if out.len() >= want {
            break;
        }
    }
    out
}

#[tokio::test]
async fn affinity_override_spreads_across_pool_not_single_best() {
    // Regression: when a session's sticky account is too loaded, the override
    // must NOT funnel every such session onto the single global argmax (`best`).
    // That target rotates as utilizations drift, sweeping sessions across the
    // pool and paying a cold-cache `cache_creation` charge on every hop
    // (measured: a swept client ran a 1.18 create:read ratio vs ~0.05 for sticky
    // clients). Overridden sessions must spread across the healthy remainder.
    let state = test_state_with_strategy(
        vec![
            mk_endpoint("loaded", "sk-ant-api-a"),
            mk_endpoint("h1", "sk-ant-api-b"),
            mk_endpoint("h2", "sk-ant-api-c"),
            mk_endpoint("h3", "sk-ant-api-d"),
        ],
        RoutingStrategy::StickyWeightedV2,
    );
    let now = AppState::now_epoch();
    // idx0 is loaded enough to trigger the override (weight << best * 0.25).
    set_account_utilization(&state, 0, 0.80, 0.80, now + 10000, now + 300000).await;
    // Three healthy accounts with a clear single best (h1): under the old
    // `picked = best` code every overridden session would herd onto h1.
    set_account_utilization(&state, 1, 0.08, 0.15, now + 10000, now + 300000).await;
    set_account_utilization(&state, 2, 0.15, 0.30, now + 10000, now + 300000).await;
    set_account_utilization(&state, 3, 0.22, 0.45, now + 10000, now + 300000).await;

    let sessions = keys_hashing_to(&state, 0, 60, 30000, "spread-session").await;
    assert!(
        sessions.len() >= 30,
        "need enough overridden sessions to judge spread, got {}",
        sessions.len()
    );

    let mut destinations = std::collections::HashSet::new();
    for s in &sessions {
        let idx = state
            .pick_endpoint(Some(s), "claude-opus-4-6", &[])
            .await
            .unwrap();
        assert_ne!(idx, 0, "overridden session must leave the loaded account");
        destinations.insert(idx);
    }
    assert!(
            destinations.len() >= 2,
            "overridden sessions herded onto {} account(s) {:?}; expected spread across the healthy pool",
            destinations.len(),
            destinations,
        );
}

#[tokio::test]
async fn affinity_override_destination_independent_of_argmax() {
    // Regression for the cascade: the override must not chase the global argmax.
    // When the healthy argmax flips (utilizations drift), a session that was
    // already overridden must NOT migrate with it. The old behavior (picked =
    // best) moved EVERY overridden session to whichever account was momentarily
    // best, so a single argmax flip migrated ~100% of them — the cache-burning
    // sweep. A session-stable replacement keeps migration low.
    let state = test_state_with_strategy(
        vec![
            mk_endpoint("loaded", "sk-ant-api-a"),
            mk_endpoint("h1", "sk-ant-api-b"),
            mk_endpoint("h2", "sk-ant-api-c"),
            mk_endpoint("h3", "sk-ant-api-d"),
        ],
        RoutingStrategy::StickyWeightedV2,
    );
    let now = AppState::now_epoch();
    set_account_utilization(&state, 0, 0.80, 0.80, now + 10000, now + 300000).await;
    set_account_utilization(&state, 3, 0.15, 0.30, now + 10000, now + 300000).await;
    // Config A: h1 is best.
    set_account_utilization(&state, 1, 0.08, 0.18, now + 10000, now + 300000).await;
    set_account_utilization(&state, 2, 0.22, 0.42, now + 10000, now + 300000).await;

    // The override set (sessions whose sticky bucket is the loaded account) is
    // fixed by idx0's weight, which the later h1/h2 swap leaves untouched.
    let sessions = keys_hashing_to(&state, 0, 60, 40000, "flip-session").await;
    assert!(
        sessions.len() >= 30,
        "need override sessions, got {}",
        sessions.len()
    );
    let mut dest_a = Vec::with_capacity(sessions.len());
    for s in &sessions {
        dest_a.push(
            state
                .pick_endpoint(Some(s), "claude-opus-4-6", &[])
                .await
                .unwrap(),
        );
    }

    // Config B: swap h1 and h2 utilizations so h2 becomes best. The healthy
    // weight *set* is unchanged (symmetric swap) — only the argmax label moves.
    set_account_utilization(&state, 1, 0.22, 0.42, now + 10000, now + 300000).await;
    set_account_utilization(&state, 2, 0.08, 0.18, now + 10000, now + 300000).await;

    let mut migrated = 0usize;
    for (k, s) in sessions.iter().enumerate() {
        let idx = state
            .pick_endpoint(Some(s), "claude-opus-4-6", &[])
            .await
            .unwrap();
        if idx != dest_a[k] {
            migrated += 1;
        }
    }
    let rate = migrated as f64 / sessions.len() as f64;
    assert!(
            rate < 0.40,
            "argmax flip migrated {}/{} ({:.0}%) overridden sessions; destination must not chase the argmax",
            migrated,
            sessions.len(),
            rate * 100.0,
        );
}

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

#[tokio::test]
async fn dynamic_capacity_v1_ignores_replica_local_request_history() {
    let state = test_state_with_strategy(
        vec![
            mk_endpoint("primary", "sk-ant-api-a"),
            mk_endpoint("jeff", "sk-ant-api-b"),
        ],
        RoutingStrategy::DynamicCapacityV1,
    );
    {
        let mut info = state.endpoints[0].rate_info.write().await;
        info.utilization_5h = Some(0.10);
        info.reset_5h = Some(AppState::now_epoch() + 10000);
    }
    {
        let mut info = state.endpoints[1].rate_info.write().await;
        info.utilization_5h = Some(0.10);
        info.reset_5h = Some(AppState::now_epoch() + 10000);
    }

    state.endpoints[1].requests.store(900, Ordering::Relaxed);
    state.endpoints[0].requests.store(100, Ordering::Relaxed);

    let mut primary_count = 0u32;
    let total = 200u32;
    for i in 0..total {
        let key = format!("balance-test-{}", i);
        let idx = state
            .pick_endpoint(Some(&key), "claude-opus-4-6", &[])
            .await
            .unwrap();
        if idx == 0 {
            primary_count += 1;
        }
    }

    assert!(
        (60..=140).contains(&primary_count),
        "dynamic-capacity-v1 should ignore replica-local request skew, got {}/{} to primary",
        primary_count,
        total
    );
}

#[tokio::test]
async fn sticky_weighted_v2_preserves_hash_distribution_under_skewed_history() {
    let state = test_state_with_strategy(
        vec![
            mk_endpoint("primary", "sk-ant-api-a"),
            mk_endpoint("jeff", "sk-ant-api-b"),
        ],
        RoutingStrategy::StickyWeightedV2,
    );
    {
        let mut info = state.endpoints[0].rate_info.write().await;
        info.utilization_5h = Some(0.10);
        info.reset_5h = Some(AppState::now_epoch() + 10000);
    }
    {
        let mut info = state.endpoints[1].rate_info.write().await;
        info.utilization_5h = Some(0.10);
        info.reset_5h = Some(AppState::now_epoch() + 10000);
    }

    state.endpoints[1].requests.store(900, Ordering::Relaxed);
    state.endpoints[0].requests.store(100, Ordering::Relaxed);

    let mut primary_count = 0u32;
    let total = 200u32;
    for i in 0..total {
        let key = format!("balance-test-{}", i);
        let idx = state
            .pick_endpoint(Some(&key), "claude-opus-4-6", &[])
            .await
            .unwrap();
        if idx == 0 {
            primary_count += 1;
        }
    }

    assert!(
        (60..=140).contains(&primary_count),
        "sticky-weighted-v2 should stay near hash distribution, got {}/{} to primary",
        primary_count,
        total
    );
}

#[tokio::test]
async fn pick_unsticky_on_overload() {
    // When a preferred account gets overloaded, sessions should migrate.
    // Hash-based pick assigns ~50% of sessions to each account initially.
    // After overload: primary weight=0.01, backup weight=0.5.
    // Ratio 0.01/0.5 = 0.02 < 0.25 threshold → override fires for all
    // sessions that hashed to primary.
    let state = test_state_with_strategy(
        vec![
            mk_endpoint("primary", "sk-ant-api-a"),
            mk_endpoint("backup", "sk-ant-api-b"),
        ],
        RoutingStrategy::StickyWeightedV2,
    );

    // Start with primary having lots of headroom
    {
        let mut info = state.endpoints[0].rate_info.write().await;
        info.utilization = Some(0.2); // headroom = 0.8
    }
    {
        let mut info = state.endpoints[1].rate_info.write().await;
        info.utilization = Some(0.5); // headroom = 0.5
    }

    // Collect keys that initially pick primary
    let mut primary_keys: Vec<String> = Vec::new();
    for i in 0..500 {
        let key = format!("test-client-{}", i);
        if state.pick_endpoint(Some(&key), "", &[]).await.unwrap() == 0 {
            primary_keys.push(key);
        }
    }
    assert!(
        primary_keys.len() >= 50,
        "should find many keys that pick primary"
    );

    // Now overload primary: util=0.99 (headroom=0.01), backup stays at 0.5 (headroom=0.5)
    {
        let mut info = state.endpoints[0].rate_info.write().await;
        info.utilization = Some(0.99);
    }

    // All sessions that hashed to primary should migrate (egregious disparity)
    let mut migrated = 0usize;
    for key in &primary_keys {
        if state.pick_endpoint(Some(key), "", &[]).await.unwrap() == 1 {
            migrated += 1;
        }
    }

    let migration_pct = migrated as f64 / primary_keys.len() as f64;
    assert!(
        migration_pct > 0.95,
        "at least 95% of clients should migrate, got {:.1}% ({}/{})",
        migration_pct * 100.0,
        migrated,
        primary_keys.len()
    );
}

#[tokio::test]
async fn affinity_sticky_near_equal_weights() {
    // Game theory: two accounts with similar utilization must produce
    // perfectly stable session routing. This is the prompt-cache scenario —
    // bouncing between accounts wastes cache-creation tokens.
    let state = test_state_with(vec![
        mk_endpoint("primary", "sk-ant-api-a"),
        mk_endpoint("jeff", "sk-ant-api-b"),
    ]);
    let now = AppState::now_epoch();
    set_account_utilization(&state, 0, 0.07, 0.73, now + 10000, now + 300000).await;
    set_account_utilization(&state, 1, 0.08, 0.71, now + 10000, now + 300000).await;

    let session = "10.42.0.1:claude:first-steps:-:9e8efc8c-2891-4206-ae10-8bcd5fa7e1f0";
    let first = state
        .pick_endpoint(Some(session), "claude-opus-4-6", &[])
        .await
        .unwrap();

    // Same session, 100 consecutive requests: must ALWAYS pick the same account
    for i in 0..100 {
        let pick = state
            .pick_endpoint(Some(session), "claude-opus-4-6", &[])
            .await
            .unwrap();
        assert_eq!(
            pick, first,
            "request {} routed to account {} instead of {}, session is bouncing",
            i, pick, first
        );
    }
}

#[tokio::test]
async fn affinity_stable_despite_utilization_drift() {
    // Game theory: utilization changes slightly after each response,
    // but sessions must remain stable. Simulates the real scenario where
    // each request nudges the 5h utilization up by a tiny amount.
    // Uses multiple sessions and asserts low aggregate migration rate,
    // avoiding boundary-sensitivity from any single hard-coded key.
    let state = test_state_with(vec![
        mk_endpoint("primary", "sk-ant-api-a"),
        mk_endpoint("jeff", "sk-ant-api-b"),
    ]);
    let now = AppState::now_epoch();
    set_account_utilization(&state, 0, 0.05, 0.70, now + 10000, now + 300000).await;
    set_account_utilization(&state, 1, 0.05, 0.68, now + 10000, now + 300000).await;

    // Record initial picks for 100 distinct sessions
    let num_sessions = 100;
    let sessions: Vec<String> = (0..num_sessions)
        .map(|i| format!("drift-session-{}", i))
        .collect();
    let mut initial_picks = Vec::with_capacity(num_sessions);
    for s in &sessions {
        initial_picks.push(
            state
                .pick_endpoint(Some(s), "claude-opus-4-6", &[])
                .await
                .unwrap(),
        );
    }

    // Simulate 50 requests with drifting utilization
    for i in 0..50 {
        let drift = 0.002 * (i as f64);
        {
            let mut info = state.endpoints[0].rate_info.write().await;
            info.utilization_5h = Some(0.05 + drift);
        }
        {
            let mut info = state.endpoints[1].rate_info.write().await;
            info.utilization_5h = Some(0.05 + drift * 0.8);
        }
    }

    // After drift, check how many sessions migrated
    let mut migrated = 0usize;
    for (j, s) in sessions.iter().enumerate() {
        let pick = state
            .pick_endpoint(Some(s), "claude-opus-4-6", &[])
            .await
            .unwrap();
        if pick != initial_picks[j] {
            migrated += 1;
        }
    }

    // Allow up to 10% migration from boundary effects — small drift shouldn't
    // cause wholesale session migration.
    assert!(
        migrated <= num_sessions / 10,
        "too many sessions migrated after util drift: {}/{} (max {})",
        migrated,
        num_sessions,
        num_sessions / 10,
    );
}

#[tokio::test]
async fn affinity_different_sessions_distribute() {
    // Game theory: different sessions should naturally distribute across
    // accounts via hash, providing cross-account balance without per-session
    // instability. This is the balancing mechanism.
    let state = test_state_with(vec![
        mk_endpoint("primary", "sk-ant-api-a"),
        mk_endpoint("jeff", "sk-ant-api-b"),
    ]);
    let now = AppState::now_epoch();
    set_account_utilization(&state, 0, 0.10, 0.50, now + 10000, now + 300000).await;
    set_account_utilization(&state, 1, 0.10, 0.50, now + 10000, now + 300000).await;

    let mut picks = [0u32; 2];
    for i in 0..500 {
        let session = format!("session-{}", i);
        let idx = state
            .pick_endpoint(Some(&session), "claude-opus-4-6", &[])
            .await
            .unwrap();
        picks[idx] += 1;
    }

    // With equal weights, hash should distribute roughly 50/50 (±15%)
    let primary_pct = picks[0] as f64 / 500.0;
    assert!(
        (0.35..=0.65).contains(&primary_pct),
        "expected ~50/50 distribution, got primary={} jeff={} ({:.0}%)",
        picks[0],
        picks[1],
        primary_pct * 100.0
    );
}

#[tokio::test]
async fn affinity_breaks_on_egregious_disparity() {
    // Game theory: when one account is under extreme pressure,
    // cache locality cost is worth paying to avoid quota exhaustion.
    let state = test_state_with(vec![
        mk_endpoint("healthy", "sk-ant-api-a"),
        mk_endpoint("dying", "sk-ant-api-b"),
    ]);
    let now = AppState::now_epoch();
    // healthy: 5h=0.10, 7d=0.20 (lots of capacity)
    // dying: 5h=0.10, 7d=0.98 (nearly exhausted)
    set_account_utilization(&state, 0, 0.10, 0.20, now + 10000, now + 300000).await;
    set_account_utilization(&state, 1, 0.10, 0.98, now + 10000, now + 300000).await;

    // ALL sessions should go to healthy (dying's weight is negligible)
    let mut healthy_count = 0u32;
    for i in 0..200 {
        let session = format!("session-{}", i);
        if state
            .pick_endpoint(Some(&session), "claude-opus-4-6", &[])
            .await
            .unwrap()
            == 0
        {
            healthy_count += 1;
        }
    }
    assert_eq!(
        healthy_count, 200,
        "all sessions should override to healthy account, got {}/200",
        healthy_count
    );
}

#[tokio::test]
async fn affinity_moderate_disparity_stays_sticky() {
    // Game theory: moderate 7d difference (0.73 vs 0.41) should NOT
    // break stickiness. The cache-creation cost outweighs the routing
    // benefit at this disparity level.
    let state = test_state_with_strategy(
        vec![
            mk_endpoint("primary", "sk-ant-api-a"),
            mk_endpoint("jeff", "sk-ant-api-b"),
        ],
        RoutingStrategy::StickyWeightedV2,
    );
    let now = AppState::now_epoch();
    set_account_utilization(&state, 0, 0.13, 0.41, now + 10000, now + 300000).await;
    set_account_utilization(&state, 1, 0.12, 0.79, now + 10000, now + 300000).await;

    // Both accounts should retain their hashed sessions
    let mut primary_picks = 0u32;
    let mut jeff_picks = 0u32;
    for i in 0..500 {
        let session = format!("moderate-session-{}", i);
        match state
            .pick_endpoint(Some(&session), "claude-opus-4-6", &[])
            .await
            .unwrap()
        {
            0 => primary_picks += 1,
            _ => jeff_picks += 1,
        }
    }
    // Both should get traffic (no one-sided override)
    assert!(
        primary_picks > 100 && jeff_picks > 100,
        "moderate disparity should preserve stickiness on both: primary={}, jeff={}",
        primary_picks,
        jeff_picks
    );
}

#[tokio::test]
async fn pick_proportional_distribution() {
    // Verify distribution matches headroom ratios over many calls
    let state = test_state_with(vec![
        mk_endpoint("a", "sk-ant-api-a"),
        mk_endpoint("b", "sk-ant-api-b"),
        mk_endpoint("c", "sk-ant-api-c"),
    ]);

    // a=0.2 util (headroom 0.8), b=0.5 util (headroom 0.5), c=0.8 util (headroom 0.2)
    // Total headroom = 1.5. Expected: a=53.3%, b=33.3%, c=13.3%
    {
        let mut info = state.endpoints[0].rate_info.write().await;
        info.utilization = Some(0.2);
    }
    {
        let mut info = state.endpoints[1].rate_info.write().await;
        info.utilization = Some(0.5);
    }
    {
        let mut info = state.endpoints[2].rate_info.write().await;
        info.utilization = Some(0.8);
    }

    let mut counts = [0u32; 3];
    let total = 10000u32;
    for _ in 0..total {
        let idx = state.pick_endpoint(None, "", &[]).await.unwrap();
        counts[idx] += 1;
    }

    let pcts: Vec<f64> = counts.iter().map(|&c| c as f64 / total as f64).collect();
    // Expected: ~53.3%, ~33.3%, ~13.3% (±3%)
    assert!(
        (0.50..=0.57).contains(&pcts[0]),
        "account a should get ~53% traffic, got {:.1}%",
        pcts[0] * 100.0
    );
    assert!(
        (0.30..=0.37).contains(&pcts[1]),
        "account b should get ~33% traffic, got {:.1}%",
        pcts[1] * 100.0
    );
    assert!(
        (0.10..=0.17).contains(&pcts[2]),
        "account c should get ~13% traffic, got {:.1}%",
        pcts[2] * 100.0
    );
}

// ── Integration: HTTP handlers ──────────────────────────────────

#[tokio::test]
async fn proxy_rejects_missing_auth() {
    let (mock_url, _handle) = spawn_mock_upstream().await;
    let (app, _state) = test_app(&mock_url, Some("secret-key".to_string()));

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(
            listener,
            app.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await
        .unwrap();
    });

    let client = Client::new();
    let resp = client
        .post(format!("http://{}/v1/messages", addr))
        .header("content-type", "application/json")
        .body(r#"{"model":"test","max_tokens":1,"messages":[{"role":"user","content":"hi"}]}"#)
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), reqwest::StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn proxy_accepts_valid_auth_and_forwards() {
    let (mock_url, _handle) = spawn_mock_upstream().await;
    let (app, state) = test_app(&mock_url, Some("secret-key".to_string()));

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(
            listener,
            app.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await
        .unwrap();
    });

    let client = Client::new();
    let resp = client
        .post(format!("http://{}/v1/messages", addr))
        .header("content-type", "application/json")
        .header("x-api-key", "secret-key")
        .body(r#"{"model":"test","max_tokens":1,"messages":[{"role":"user","content":"hi"}]}"#)
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    // Verify rate info was updated from mock response headers
    let info = state.endpoints[0].rate_info.read().await;
    assert_eq!(info.utilization, Some(0.25));
    assert_eq!(info.representative_claim.as_deref(), Some("five_hour"));
}

/// A non-streaming upstream that promises N bytes via Content-Length then
/// closes the socket early must surface as a 502, NOT a truncated 200.
/// Regression for the silently-swallowed `unwrap_or_default()` body read.
#[tokio::test]
async fn proxy_returns_502_when_upstream_body_read_fails() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    // Raw TCP mock: send a 200 with content-length far larger than the
    // bytes actually written, then drop the connection mid-body.
    let mock_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mock_addr = mock_listener.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let (mut sock, _) = mock_listener.accept().await.unwrap();
            tokio::spawn(async move {
                let mut buf = [0u8; 8192];
                let _ = sock.read(&mut buf).await; // best-effort drain request
                                                   // Promise 4096 bytes, send a partial JSON prefix, then close.
                let _ = sock
                        .write_all(
                            b"HTTP/1.1 200 OK\r\n\
                              content-type: application/json\r\n\
                              anthropic-ratelimit-unified-5h-utilization: 0.42\r\n\
                              content-length: 4096\r\n\r\n\
                              {\"id\":\"msg_partial\",\"type\":\"message\",\"content\":[{\"type\":\"text\",\"text\":\"par",
                        )
                        .await;
                let _ = sock.flush().await;
                // Drop without sending the remaining promised bytes.
            });
        }
    });

    // Ratelimit reflection is opt-in since LAB-1191; this test asserts the
    // error-arm parity that the opt-in restores, so flip it on.
    let mut state = test_state_with(vec![
        mk_endpoint_at(
            "acct-a",
            "sk-ant-api-test-aaa",
            &format!("http://{}", mock_addr),
        ),
        mk_endpoint_at(
            "acct-b",
            "sk-ant-api-test-bbb",
            &format!("http://{}", mock_addr),
        ),
    ]);
    {
        let s = Arc::get_mut(&mut state).expect("test fixture should be uniquely owned");
        s.proxy_key = Some("secret-key".to_string());
        s.expose_upstream_ratelimit_headers = true;
    }
    let app = build_router(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(
            listener,
            app.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await
        .unwrap();
    });

    let client = Client::new();
    let resp = client
        .post(format!("http://{}/v1/messages", addr))
        .header("content-type", "application/json")
        .header("x-api-key", "secret-key")
        .body(r#"{"model":"test","max_tokens":1,"messages":[{"role":"user","content":"hi"}]}"#)
        .send()
        .await
        .unwrap();

    assert_eq!(
        resp.status(),
        reqwest::StatusCode::BAD_GATEWAY,
        "truncated upstream body must become a 502, not a silent empty 200"
    );
    // With expose_upstream_ratelimit_headers = true, the 502 must still
    // forward the upstream's rate-limit headers (and the budget status) so
    // the client's limit tracking doesn't go blind on the error arm — parity
    // with every success arm.
    assert_eq!(
        resp.headers()
            .get("anthropic-ratelimit-unified-5h-utilization")
            .and_then(|v| v.to_str().ok()),
        Some("0.42"),
        "502 should forward upstream anthropic-ratelimit-* headers when exposed"
    );
    assert!(
        resp.headers().contains_key("x-budget-status"),
        "502 should carry x-budget-status like other response arms"
    );
    let body = resp.text().await.unwrap();
    assert!(
        body.contains("upstream response body read failed"),
        "502 body should carry the upstream read error, got: {body}"
    );
}

#[tokio::test]
async fn proxy_preserves_raw_body_bytes_when_auto_cache_skips() {
    let seen_body = Arc::new(std::sync::Mutex::new(None::<Vec<u8>>));
    let seen_body_clone = seen_body.clone();

    let mock_app = Router::new().fallback(any(move |req: Request<Body>| {
        let seen_body = seen_body_clone.clone();
        async move {
            let body_bytes = axum::body::to_bytes(req.into_body(), MAX_REQUEST_BODY_BYTES)
                .await
                .unwrap();
            *seen_body.lock().unwrap() = Some(body_bytes.to_vec());

            let mut resp = axum::Json(serde_json::json!({
                "id": "msg_test",
                "type": "message",
                "content": [{"type": "text", "text": "ok"}],
            }))
            .into_response();
            let headers = resp.headers_mut();
            headers.insert(
                "anthropic-ratelimit-unified-representative-claim",
                HeaderValue::from_static("five_hour"),
            );
            headers.insert(
                "anthropic-ratelimit-unified-5h-utilization",
                HeaderValue::from_static("0.25"),
            );
            let reset_epoch = (std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs()
                + 3600)
                .to_string();
            headers.insert(
                "anthropic-ratelimit-unified-5h-reset",
                HeaderValue::from_str(&reset_epoch).unwrap(),
            );
            resp
        }
    }));

    let mock_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mock_addr = mock_listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(mock_listener, mock_app).await.unwrap();
    });

    let (app, _state) = test_app(
        &format!("http://{}", mock_addr),
        Some("secret-key".to_string()),
    );

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(
            listener,
            app.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await
        .unwrap();
    });

    let raw_body = "{\n  \"model\" : \"test\",\n  \"messages\" : [{\"role\" : \"user\", \"content\" : [{\"type\":\"text\", \"text\":\"hi\", \"cache_control\":{\"type\":\"ephemeral\"}}]}],\n  \"max_tokens\" : 1\n}";

    let client = Client::new();
    let resp = client
        .post(format!("http://{}/v1/messages", addr))
        .header("content-type", "application/json")
        .header("x-api-key", "secret-key")
        .body(raw_body)
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    let captured = seen_body
        .lock()
        .unwrap()
        .clone()
        .expect("upstream should receive request body");
    assert_eq!(
        String::from_utf8(captured).unwrap(),
        raw_body,
        "request body bytes should remain unchanged when auto-cache does not mutate payload"
    );
}

#[tokio::test]
async fn proxy_preserves_key_order_when_auto_cache_mutates() {
    let seen_body = Arc::new(std::sync::Mutex::new(None::<Vec<u8>>));
    let seen_body_clone = seen_body.clone();

    let mock_app = Router::new().fallback(any(move |req: Request<Body>| {
        let seen_body = seen_body_clone.clone();
        async move {
            let body_bytes = axum::body::to_bytes(req.into_body(), MAX_REQUEST_BODY_BYTES)
                .await
                .unwrap();
            *seen_body.lock().unwrap() = Some(body_bytes.to_vec());

            let mut resp = axum::Json(serde_json::json!({
                "id": "msg_test",
                "type": "message",
                "content": [{"type": "text", "text": "ok"}],
            }))
            .into_response();
            let headers = resp.headers_mut();
            headers.insert(
                "anthropic-ratelimit-unified-representative-claim",
                HeaderValue::from_static("five_hour"),
            );
            headers.insert(
                "anthropic-ratelimit-unified-5h-utilization",
                HeaderValue::from_static("0.25"),
            );
            let reset_epoch = (std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs()
                + 3600)
                .to_string();
            headers.insert(
                "anthropic-ratelimit-unified-5h-reset",
                HeaderValue::from_str(&reset_epoch).unwrap(),
            );
            resp
        }
    }));

    let mock_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mock_addr = mock_listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(mock_listener, mock_app).await.unwrap();
    });

    let (app, _state) = test_app(
        &format!("http://{}", mock_addr),
        Some("secret-key".to_string()),
    );

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(
            listener,
            app.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await
        .unwrap();
    });

    let input_body =
            "{\"model\":\"test\",\"messages\":[{\"role\":\"user\",\"content\":\"hi\"}],\"max_tokens\":1}";
    let expected_body = "{\"model\":\"test\",\"messages\":[{\"role\":\"user\",\"content\":[{\"type\":\"text\",\"text\":\"hi\",\"cache_control\":{\"type\":\"ephemeral\"}}]}],\"max_tokens\":1}";

    let client = Client::new();
    let resp = client
        .post(format!("http://{}/v1/messages", addr))
        .header("content-type", "application/json")
        .header("x-api-key", "secret-key")
        .body(input_body)
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    let captured = seen_body
        .lock()
        .unwrap()
        .clone()
        .expect("upstream should receive request body");
    assert_eq!(
        String::from_utf8(captured).unwrap(),
        expected_body,
        "mutated request body should preserve caller key order while adding cache markers"
    );
}

#[tokio::test]
async fn proxy_oauth_account_preserves_auto_cache_with_system_prompt() {
    // Regression test: when an OAuth account is selected and auto_cache
    // injects breakpoints, oauth_bytes must contain BOTH the OAuth system
    // prompt AND the cache_control markers. Before the fix, the fast-path
    // used raw body_bytes (dropping cache mutations).
    let seen_body = Arc::new(std::sync::Mutex::new(None::<Vec<u8>>));
    let seen_body_clone = seen_body.clone();

    let mock_app = Router::new().fallback(any(move |req: Request<Body>| {
        let seen_body = seen_body_clone.clone();
        async move {
            let body_bytes = axum::body::to_bytes(req.into_body(), MAX_REQUEST_BODY_BYTES)
                .await
                .unwrap();
            *seen_body.lock().unwrap() = Some(body_bytes.to_vec());

            let mut resp = axum::Json(serde_json::json!({
                "id": "msg_test",
                "type": "message",
                "content": [{"type": "text", "text": "ok"}],
            }))
            .into_response();
            let headers = resp.headers_mut();
            headers.insert(
                "anthropic-ratelimit-unified-representative-claim",
                HeaderValue::from_static("five_hour"),
            );
            headers.insert(
                "anthropic-ratelimit-unified-5h-utilization",
                HeaderValue::from_static("0.10"),
            );
            let reset_epoch = (std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs()
                + 3600)
                .to_string();
            headers.insert(
                "anthropic-ratelimit-unified-5h-reset",
                HeaderValue::from_str(&reset_epoch).unwrap(),
            );
            resp
        }
    }));

    let mock_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mock_addr = mock_listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(mock_listener, mock_app).await.unwrap();
    });

    // Single OAuth endpoint — forces all requests through the oauth_body_bytes path
    let mut oauth_ep = mk_endpoint("oauth-acct", "sk-ant-oat01-test-token");
    oauth_ep.base_url = format!("http://{}", mock_addr);
    let accounts = vec![oauth_ep];
    let state = Arc::new(AppState {
        endpoints: accounts,
        state_path: PathBuf::from("/tmp/anthropic-lb-oauth-cache-test.state.json"),
        auto_cache: true, // KEY: this test exercises the auto-cache injection path
        ..test_state_base()
    });

    let app = build_router(state);
    let addr = serve(app).await;

    // Body with NO cache_control and NO system field — auto-cache will inject
    // breakpoints, and inject_oauth_system_prompt will add the CC prompt.
    let client = Client::new();
    let resp = client
            .post(format!("http://{}/v1/messages", addr))
            .header("content-type", "application/json")
            .body(r#"{"model":"claude-sonnet-4-6","messages":[{"role":"user","content":"hi"}],"max_tokens":5}"#)
            .send()
            .await
            .unwrap();

    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    let captured = seen_body
        .lock()
        .unwrap()
        .clone()
        .expect("upstream should receive request body");
    let body: serde_json::Value = serde_json::from_slice(&captured).unwrap();

    // 1. OAuth system prompt must be injected as first system block
    let system = body
        .get("system")
        .expect("system field must be present (injected by OAuth path)");
    let arr = system.as_array().expect("system should be array");
    assert_eq!(
        arr[0]["text"].as_str().unwrap(),
        OAUTH_SYSTEM_PROMPT,
        "first system block must be CC prompt"
    );

    // 2. Auto-cache breakpoints must be present (not dropped by fast-path)
    let messages = body["messages"].as_array().unwrap();
    let last_user = messages
        .iter()
        .rev()
        .find(|m| m["role"] == "user")
        .expect("should have user message");
    let content = last_user["content"].as_array().unwrap();
    assert!(
        content.last().unwrap().get("cache_control").is_some(),
        "auto-cache breakpoint on last user message must survive OAuth path"
    );
}

#[tokio::test]
async fn stats_endpoint_returns_account_info() {
    let (mock_url, _handle) = spawn_mock_upstream().await;
    let (app, _state) = test_app(&mock_url, None);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(
            listener,
            app.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await
        .unwrap();
    });

    let client = Client::new();
    let resp = client
        .get(format!("http://{}/_stats", addr))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let body: serde_json::Value = resp.json().await.unwrap();
    let endpoints = body["endpoints"].as_array().unwrap();
    assert_eq!(endpoints.len(), 2);
    assert_eq!(endpoints[0]["name"], "acct-a");
    assert_eq!(endpoints[1]["name"], "acct-b");
    assert_eq!(endpoints[0]["protocol"], "anthropic");
    assert_eq!(body["strategy"], "dynamic-capacity-v1");
    // Legacy `accounts` and `upstreams` arrays are gone from the schema.
    assert!(body.get("accounts").is_none());
    assert!(body.get("upstreams").is_none());
}

#[tokio::test]
async fn stats_endpoint_exposes_endpoints_array() {
    let ep = |name: &str, protocol: Protocol| {
        let mut e = make_endpoint(name, protocol);
        e.priority = 5;
        e
    };
    let state = test_state_with(vec![
        ep("ep-anthropic", Protocol::Anthropic),
        ep("ep-openai", Protocol::OpenAI),
    ]);
    let addr = serve(build_router(state)).await;

    let client = Client::new();
    let resp = client
        .get(format!("http://{}/_stats", addr))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let body: serde_json::Value = resp.json().await.unwrap();
    let endpoints = body["endpoints"].as_array().unwrap();
    assert_eq!(endpoints.len(), 2);
    assert_eq!(endpoints[0]["name"], "ep-anthropic");
    assert_eq!(endpoints[0]["protocol"], "anthropic");
    assert_eq!(endpoints[0]["priority"], 5);
    assert_eq!(endpoints[1]["name"], "ep-openai");
    assert_eq!(endpoints[1]["protocol"], "openai");
}

// ── Unit: OpenAI request translation ────────────────────────────

#[test]
fn translate_request_extracts_system() {
    let req = serde_json::json!({
        "model": "claude-sonnet-4-6",
        "messages": [
            {"role": "system", "content": "You are helpful"},
            {"role": "user", "content": "Hello"}
        ],
        "max_tokens": 1024
    });
    let result = translate_openai_to_anthropic(&req);
    assert_eq!(result["system"], "You are helpful");
    let msgs = result["messages"].as_array().unwrap();
    assert_eq!(msgs.len(), 1);
    assert_eq!(msgs[0]["role"], "user");
    assert_eq!(msgs[0]["content"], "Hello");
}

#[test]
fn translate_request_multi_system_concat() {
    let req = serde_json::json!({
        "model": "claude-sonnet-4-6",
        "messages": [
            {"role": "system", "content": "Rule 1"},
            {"role": "system", "content": "Rule 2"},
            {"role": "user", "content": "Hello"}
        ],
        "max_tokens": 100
    });
    let result = translate_openai_to_anthropic(&req);
    assert_eq!(result["system"], "Rule 1\n\nRule 2");
}

#[test]
fn translate_request_system_array_content() {
    // OpenAI permits system content as an array of text parts — must not be dropped
    let req = serde_json::json!({
        "model": "claude-sonnet-4-6",
        "messages": [
            {"role": "system", "content": [
                {"type": "text", "text": "You are "},
                {"type": "text", "text": "helpful"}
            ]},
            {"role": "user", "content": "Hello"}
        ],
        "max_tokens": 100
    });
    let result = translate_openai_to_anthropic(&req);
    assert_eq!(result["system"], "You are helpful");
    assert_eq!(result["messages"].as_array().unwrap().len(), 1);
}

#[test]
fn translate_request_image_url_to_image_block() {
    // OpenAI image_url parts must become Anthropic image blocks, not pass through raw
    let req = serde_json::json!({
        "model": "claude-sonnet-4-6",
        "messages": [
            {"role": "user", "content": [
                {"type": "text", "text": "What is this?"},
                {"type": "image_url", "image_url": {"url": "data:image/png;base64,iVBORw0KGgo="}},
                {"type": "image_url", "image_url": {"url": "https://example.com/cat.jpg"}}
            ]}
        ],
        "max_tokens": 100
    });
    let result = translate_openai_to_anthropic(&req);
    let content = result["messages"][0]["content"].as_array().unwrap();
    assert_eq!(
        content[0],
        serde_json::json!({"type": "text", "text": "What is this?"})
    );
    // data: URL → base64 source
    assert_eq!(content[1]["type"], "image");
    assert_eq!(content[1]["source"]["type"], "base64");
    assert_eq!(content[1]["source"]["media_type"], "image/png");
    assert_eq!(content[1]["source"]["data"], "iVBORw0KGgo=");
    // plain URL → url source
    assert_eq!(content[2]["type"], "image");
    assert_eq!(content[2]["source"]["type"], "url");
    assert_eq!(content[2]["source"]["url"], "https://example.com/cat.jpg");
}

#[test]
fn translate_request_no_system() {
    let req = serde_json::json!({
        "model": "claude-sonnet-4-6",
        "messages": [
            {"role": "user", "content": "Hello"}
        ],
        "max_tokens": 100
    });
    let result = translate_openai_to_anthropic(&req);
    assert!(result.get("system").is_none());
}

#[test]
fn translate_request_default_max_tokens() {
    let req = serde_json::json!({
        "model": "claude-sonnet-4-6",
        "messages": [{"role": "user", "content": "Hello"}]
    });
    let result = translate_openai_to_anthropic(&req);
    assert_eq!(result["max_tokens"], 4096);
}

#[test]
fn translate_request_stop_to_stop_sequences() {
    let req = serde_json::json!({
        "model": "claude-sonnet-4-6",
        "messages": [{"role": "user", "content": "Hello"}],
        "max_tokens": 100,
        "stop": ["END", "STOP"]
    });
    let result = translate_openai_to_anthropic(&req);
    let seqs = result["stop_sequences"].as_array().unwrap();
    assert_eq!(seqs.len(), 2);
    assert_eq!(seqs[0], "END");
    assert_eq!(seqs[1], "STOP");
}

#[test]
fn translate_request_passthrough_params() {
    let req = serde_json::json!({
        "model": "claude-sonnet-4-6",
        "messages": [{"role": "user", "content": "Hello"}],
        "max_tokens": 512,
        "temperature": 0.7,
        "top_p": 0.9,
        "stream": true
    });
    let result = translate_openai_to_anthropic(&req);
    assert_eq!(result["model"], "claude-sonnet-4-6");
    assert_eq!(result["max_tokens"], 512);
    assert_eq!(result["temperature"], 0.7);
    assert_eq!(result["top_p"], 0.9);
    assert_eq!(result["stream"], true);
}

#[test]
fn translate_request_strips_name_field() {
    let req = serde_json::json!({
        "model": "claude-sonnet-4-6",
        "messages": [
            {"role": "user", "content": "Hello", "name": "bob"}
        ],
        "max_tokens": 100
    });
    let result = translate_openai_to_anthropic(&req);
    let msgs = result["messages"].as_array().unwrap();
    assert!(msgs[0].get("name").is_none());
}

#[test]
fn translate_request_tools() {
    let req = serde_json::json!({
        "model": "claude-sonnet-4-6",
        "messages": [{"role": "user", "content": "What's the weather?"}],
        "max_tokens": 100,
        "tools": [{
            "type": "function",
            "function": {
                "name": "get_weather",
                "description": "Get the weather",
                "parameters": {
                    "type": "object",
                    "properties": {"location": {"type": "string"}},
                    "required": ["location"]
                }
            }
        }]
    });
    let result = translate_openai_to_anthropic(&req);
    let tools = result["tools"].as_array().unwrap();
    assert_eq!(tools.len(), 1);
    assert_eq!(tools[0]["name"], "get_weather");
    assert_eq!(tools[0]["description"], "Get the weather");
    assert_eq!(tools[0]["input_schema"]["type"], "object");
    assert!(tools[0].get("type").is_none()); // no OpenAI "type":"function" wrapper
}

#[test]
fn translate_request_tool_choice_variants() {
    // auto
    let req = serde_json::json!({
        "model": "claude-sonnet-4-6",
        "messages": [{"role": "user", "content": "Hi"}],
        "tool_choice": "auto"
    });
    let result = translate_openai_to_anthropic(&req);
    assert_eq!(result["tool_choice"]["type"], "auto");

    // required → any
    let req = serde_json::json!({
        "model": "claude-sonnet-4-6",
        "messages": [{"role": "user", "content": "Hi"}],
        "tool_choice": "required"
    });
    let result = translate_openai_to_anthropic(&req);
    assert_eq!(result["tool_choice"]["type"], "any");

    // specific function
    let req = serde_json::json!({
        "model": "claude-sonnet-4-6",
        "messages": [{"role": "user", "content": "Hi"}],
        "tool_choice": {"type": "function", "function": {"name": "search"}}
    });
    let result = translate_openai_to_anthropic(&req);
    assert_eq!(result["tool_choice"]["type"], "tool");
    assert_eq!(result["tool_choice"]["name"], "search");

    // none → omitted
    let req = serde_json::json!({
        "model": "claude-sonnet-4-6",
        "messages": [{"role": "user", "content": "Hi"}],
        "tool_choice": "none"
    });
    let result = translate_openai_to_anthropic(&req);
    assert!(result.get("tool_choice").is_none());
}

#[test]
fn translate_request_tool_result_message() {
    let req = serde_json::json!({
        "model": "claude-sonnet-4-6",
        "messages": [
            {"role": "user", "content": "What's the weather?"},
            {"role": "assistant", "content": null, "tool_calls": [{
                "id": "call_123",
                "type": "function",
                "function": {"name": "get_weather", "arguments": "{\"location\":\"SF\"}"}
            }]},
            {"role": "tool", "tool_call_id": "call_123", "content": "72°F and sunny"}
        ],
        "max_tokens": 100
    });
    let result = translate_openai_to_anthropic(&req);
    let msgs = result["messages"].as_array().unwrap();
    // First message: user text
    assert_eq!(msgs[0]["role"], "user");
    assert_eq!(msgs[0]["content"], "What's the weather?");
    // Second message: assistant with tool_use block
    assert_eq!(msgs[1]["role"], "assistant");
    let blocks = msgs[1]["content"].as_array().unwrap();
    assert_eq!(blocks[0]["type"], "tool_use");
    assert_eq!(blocks[0]["id"], "call_123");
    assert_eq!(blocks[0]["name"], "get_weather");
    assert_eq!(blocks[0]["input"]["location"], "SF");
    // Third message: tool result → user with tool_result
    assert_eq!(msgs[2]["role"], "user");
    let result_blocks = msgs[2]["content"].as_array().unwrap();
    assert_eq!(result_blocks[0]["type"], "tool_result");
    assert_eq!(result_blocks[0]["tool_use_id"], "call_123");
    assert_eq!(result_blocks[0]["content"], "72°F and sunny");
}

#[test]
fn translate_request_assistant_tool_calls_with_text() {
    let req = serde_json::json!({
        "model": "claude-sonnet-4-6",
        "messages": [
            {"role": "assistant", "content": "Let me check.", "tool_calls": [{
                "id": "tc_1",
                "type": "function",
                "function": {"name": "search", "arguments": "{\"q\":\"rust\"}"}
            }]}
        ],
        "max_tokens": 100
    });
    let result = translate_openai_to_anthropic(&req);
    let msgs = result["messages"].as_array().unwrap();
    let blocks = msgs[0]["content"].as_array().unwrap();
    assert_eq!(blocks.len(), 2);
    assert_eq!(blocks[0]["type"], "text");
    assert_eq!(blocks[0]["text"], "Let me check.");
    assert_eq!(blocks[1]["type"], "tool_use");
    assert_eq!(blocks[1]["name"], "search");
}

#[test]
fn translate_request_consecutive_tool_results_merged() {
    // Parallel tool calls produce consecutive role:"tool" messages.
    // Anthropic rejects consecutive same-role messages, so they must merge.
    let req = serde_json::json!({
        "model": "claude-sonnet-4-6",
        "messages": [
            {"role": "user", "content": "Weather in SF and NYC?"},
            {"role": "assistant", "content": null, "tool_calls": [
                {"id": "c1", "type": "function", "function": {"name": "weather", "arguments": "{\"city\":\"SF\"}"}},
                {"id": "c2", "type": "function", "function": {"name": "weather", "arguments": "{\"city\":\"NYC\"}"}}
            ]},
            {"role": "tool", "tool_call_id": "c1", "content": "72F"},
            {"role": "tool", "tool_call_id": "c2", "content": "45F"}
        ],
        "max_tokens": 100
    });
    let result = translate_openai_to_anthropic(&req);
    let msgs = result["messages"].as_array().unwrap();
    // Should be 3 messages: user, assistant, user(merged tool results)
    assert_eq!(msgs.len(), 3);
    assert_eq!(msgs[2]["role"], "user");
    let blocks = msgs[2]["content"].as_array().unwrap();
    assert_eq!(blocks.len(), 2);
    assert_eq!(blocks[0]["tool_use_id"], "c1");
    assert_eq!(blocks[0]["content"], "72F");
    assert_eq!(blocks[1]["tool_use_id"], "c2");
    assert_eq!(blocks[1]["content"], "45F");
}

#[test]
fn translate_request_tool_result_does_not_merge_into_regular_user_msg() {
    // A user message with array-form content should NOT have tool_results
    // appended to it — that would corrupt the original message.
    let req = serde_json::json!({
        "model": "claude-sonnet-4-6",
        "messages": [
            {"role": "user", "content": [{"type": "text", "text": "Hello"}]},
            {"role": "assistant", "content": null, "tool_calls": [{
                "id": "c1", "type": "function",
                "function": {"name": "test", "arguments": "{}"}
            }]},
            {"role": "tool", "tool_call_id": "c1", "content": "done"}
        ],
        "max_tokens": 100
    });
    let result = translate_openai_to_anthropic(&req);
    let msgs = result["messages"].as_array().unwrap();
    // user, assistant, user(tool_result) — three separate messages
    assert_eq!(msgs.len(), 3);
    // First user message should be untouched
    let first_user = msgs[0]["content"].as_array().unwrap();
    assert_eq!(first_user.len(), 1);
    assert_eq!(first_user[0]["type"], "text");
    // Third message is the tool_result
    let tool_msg = msgs[2]["content"].as_array().unwrap();
    assert_eq!(tool_msg[0]["type"], "tool_result");
}

#[test]
fn translate_request_tool_choice_none_removes_tools() {
    let req = serde_json::json!({
        "model": "claude-sonnet-4-6",
        "messages": [{"role": "user", "content": "Hi"}],
        "tools": [{"type": "function", "function": {"name": "search", "description": "Search", "parameters": {"type": "object"}}}],
        "tool_choice": "none"
    });
    let result = translate_openai_to_anthropic(&req);
    assert!(result.get("tools").is_none());
    assert!(result.get("tool_choice").is_none());
}

#[test]
fn translate_request_malformed_arguments_json() {
    let req = serde_json::json!({
        "model": "claude-sonnet-4-6",
        "messages": [
            {"role": "assistant", "content": null, "tool_calls": [{
                "id": "tc_bad",
                "type": "function",
                "function": {"name": "test", "arguments": "not valid json"}
            }]}
        ],
        "max_tokens": 100
    });
    let result = translate_openai_to_anthropic(&req);
    let msgs = result["messages"].as_array().unwrap();
    let blocks = msgs[0]["content"].as_array().unwrap();
    // Should fall back to empty object, not panic
    assert_eq!(blocks[0]["input"], serde_json::json!({}));
}

#[test]
fn translate_request_tool_no_parameters_gets_empty_schema() {
    let req = serde_json::json!({
        "model": "claude-sonnet-4-6",
        "messages": [{"role": "user", "content": "Hi"}],
        "max_tokens": 100,
        "tools": [{"type": "function", "function": {"name": "get_time", "description": "Get current time"}}]
    });
    let result = translate_openai_to_anthropic(&req);
    let tools = result["tools"].as_array().unwrap();
    assert_eq!(tools[0]["name"], "get_time");
    // input_schema must always be present for Anthropic API
    assert_eq!(
        tools[0]["input_schema"],
        serde_json::json!({"type": "object", "properties": {}})
    );
}

#[test]
fn translate_request_tool_null_parameters_gets_empty_schema() {
    let req = serde_json::json!({
        "model": "claude-sonnet-4-6",
        "messages": [{"role": "user", "content": "Hi"}],
        "max_tokens": 100,
        "tools": [{"type": "function", "function": {"name": "ping", "description": "Ping", "parameters": null}}]
    });
    let result = translate_openai_to_anthropic(&req);
    let tools = result["tools"].as_array().unwrap();
    assert_eq!(
        tools[0]["input_schema"],
        serde_json::json!({"type": "object", "properties": {}})
    );
}

#[test]
fn translate_request_tool_result_array_content() {
    let req = serde_json::json!({
        "model": "claude-sonnet-4-6",
        "messages": [
            {"role": "tool", "tool_call_id": "c1", "content": [
                {"type": "text", "text": "Result A"},
                {"type": "text", "text": " Result B"}
            ]}
        ],
        "max_tokens": 100
    });
    let result = translate_openai_to_anthropic(&req);
    let msgs = result["messages"].as_array().unwrap();
    let blocks = msgs[0]["content"].as_array().unwrap();
    assert_eq!(blocks[0]["content"], "Result A Result B");
}

#[test]
fn translate_request_assistant_array_content_with_tool_calls() {
    let req = serde_json::json!({
        "model": "claude-sonnet-4-6",
        "messages": [
            {"role": "assistant", "content": [{"type": "text", "text": "Thinking..."}], "tool_calls": [{
                "id": "tc_1",
                "type": "function",
                "function": {"name": "search", "arguments": "{}"}
            }]}
        ],
        "max_tokens": 100
    });
    let result = translate_openai_to_anthropic(&req);
    let msgs = result["messages"].as_array().unwrap();
    let blocks = msgs[0]["content"].as_array().unwrap();
    assert_eq!(blocks[0]["type"], "text");
    assert_eq!(blocks[0]["text"], "Thinking...");
    assert_eq!(blocks[1]["type"], "tool_use");
}

// ── Unit: OpenAI response translation ───────────────────────────

#[test]
fn translate_response_basic() {
    let resp = serde_json::json!({
        "id": "msg_abc123",
        "type": "message",
        "content": [{"type": "text", "text": "Hello!"}],
        "model": "claude-sonnet-4-6",
        "stop_reason": "end_turn",
        "usage": {"input_tokens": 10, "output_tokens": 5}
    });
    let result = translate_anthropic_to_openai(&resp, false);
    assert_eq!(result["id"], "chatcmpl-msg_abc123");
    assert_eq!(result["object"], "chat.completion");
    assert_eq!(result["choices"][0]["message"]["role"], "assistant");
    assert_eq!(result["choices"][0]["message"]["content"], "Hello!");
    assert_eq!(result["choices"][0]["finish_reason"], "stop");
}

#[test]
fn translate_response_usage_mapping() {
    let resp = serde_json::json!({
        "id": "msg_x",
        "content": [{"type": "text", "text": "ok"}],
        "model": "claude-sonnet-4-6",
        "stop_reason": "end_turn",
        "usage": {"input_tokens": 25, "output_tokens": 15}
    });
    let result = translate_anthropic_to_openai(&resp, false);
    assert_eq!(result["usage"]["prompt_tokens"], 25);
    assert_eq!(result["usage"]["completion_tokens"], 15);
    assert_eq!(result["usage"]["total_tokens"], 40);
}

#[test]
fn translate_response_stop_reason_mapping() {
    assert_eq!(map_stop_reason("end_turn"), "stop");
    assert_eq!(map_stop_reason("max_tokens"), "length");
    assert_eq!(map_stop_reason("stop_sequence"), "stop");
    assert_eq!(map_stop_reason("unknown"), "stop");
}

// ── Unit: JSON fence stripping ──────────────────────────────────

#[test]
fn strip_json_fences_with_lang_tag() {
    let input = "```json\n{\"key\": \"value\"}\n```";
    assert_eq!(strip_json_fences(input), r#"{"key": "value"}"#);
}

#[test]
fn strip_json_fences_no_lang_tag() {
    let input = "```\n{\"key\": \"value\"}\n```";
    assert_eq!(strip_json_fences(input), r#"{"key": "value"}"#);
}

#[test]
fn strip_json_fences_passthrough_plain_json() {
    let input = r#"{"key": "value"}"#;
    assert_eq!(strip_json_fences(input), input);
}

#[test]
fn strip_json_fences_with_whitespace() {
    let input = "  ```json\n{\"a\": 1}\n```  ";
    assert_eq!(strip_json_fences(input), r#"{"a": 1}"#);
}

#[test]
fn translate_response_strips_markdown_fences() {
    // Fence stripping is gated on the request having asked for
    // response_format: json_object (json_mode = true).
    let resp = serde_json::json!({
        "id": "msg_fenced",
        "content": [{"type": "text", "text": "```json\n{\"skipSearch\": true}\n```"}],
        "model": "claude-sonnet-4-6",
        "stop_reason": "end_turn",
        "usage": {"input_tokens": 10, "output_tokens": 5}
    });
    let result = translate_anthropic_to_openai(&resp, true);
    assert_eq!(
        result["choices"][0]["message"]["content"],
        r#"{"skipSearch": true}"#
    );
}

#[test]
fn translate_response_preserves_fences_without_json_mode() {
    // A normal chat reply that IS a fenced code block must pass through
    // verbatim — fences, language tag, and surrounding whitespace intact.
    let text = "```python\nprint(\"hi\")\n```";
    let resp = serde_json::json!({
        "id": "msg_code",
        "content": [{"type": "text", "text": text}],
        "model": "claude-sonnet-4-6",
        "stop_reason": "end_turn",
        "usage": {"input_tokens": 10, "output_tokens": 5}
    });
    let result = translate_anthropic_to_openai(&resp, false);
    assert_eq!(result["choices"][0]["message"]["content"], text);
}

#[test]
fn wants_json_object_detection() {
    assert!(wants_json_object(&serde_json::json!({
        "response_format": {"type": "json_object"}
    })));
    assert!(!wants_json_object(&serde_json::json!({
        "response_format": {"type": "text"}
    })));
    assert!(!wants_json_object(&serde_json::json!({})));
}

#[test]
fn translate_response_tool_use_blocks() {
    let resp = serde_json::json!({
        "id": "msg_tool",
        "type": "message",
        "content": [
            {"type": "tool_use", "id": "toolu_123", "name": "get_weather", "input": {"location": "SF"}}
        ],
        "model": "claude-sonnet-4-6",
        "stop_reason": "tool_use",
        "usage": {"input_tokens": 20, "output_tokens": 15}
    });
    let result = translate_anthropic_to_openai(&resp, false);
    assert_eq!(result["choices"][0]["finish_reason"], "tool_calls");
    assert!(result["choices"][0]["message"]["content"].is_null());
    let tcs = result["choices"][0]["message"]["tool_calls"]
        .as_array()
        .unwrap();
    assert_eq!(tcs.len(), 1);
    assert_eq!(tcs[0]["id"], "toolu_123");
    assert_eq!(tcs[0]["type"], "function");
    assert_eq!(tcs[0]["function"]["name"], "get_weather");
    let args: serde_json::Value =
        serde_json::from_str(tcs[0]["function"]["arguments"].as_str().unwrap()).unwrap();
    assert_eq!(args["location"], "SF");
}

#[test]
fn translate_response_mixed_text_and_tool_use() {
    let resp = serde_json::json!({
        "id": "msg_mixed",
        "type": "message",
        "content": [
            {"type": "text", "text": "Let me check."},
            {"type": "tool_use", "id": "toolu_456", "name": "search", "input": {"q": "rust"}}
        ],
        "model": "claude-sonnet-4-6",
        "stop_reason": "tool_use",
        "usage": {"input_tokens": 10, "output_tokens": 10}
    });
    let result = translate_anthropic_to_openai(&resp, false);
    assert_eq!(result["choices"][0]["message"]["content"], "Let me check.");
    let tcs = result["choices"][0]["message"]["tool_calls"]
        .as_array()
        .unwrap();
    assert_eq!(tcs.len(), 1);
    assert_eq!(tcs[0]["function"]["name"], "search");
}

// ── Unit: SSE event translation ─────────────────────────────────

#[test]
fn translate_sse_message_start() {
    let mut ctx = StreamContext::default();
    let raw = "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_test\",\"model\":\"claude-sonnet-4-6\",\"role\":\"assistant\"}}";
    let result = translate_sse_event(raw, &mut ctx).unwrap();
    assert!(result.starts_with("data: "));
    assert_eq!(ctx.id, "chatcmpl-msg_test");
    assert_eq!(ctx.model, "claude-sonnet-4-6");
    let chunk: serde_json::Value =
        serde_json::from_str(result.strip_prefix("data: ").unwrap().trim()).unwrap();
    assert_eq!(chunk["choices"][0]["delta"]["role"], "assistant");
}

#[test]
fn translate_sse_content_delta() {
    let mut ctx = StreamContext {
        id: "chatcmpl-test".to_string(),
        model: "claude-sonnet-4-6".to_string(),
        ..Default::default()
    };
    let raw = "event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"Hello world\"}}";
    let result = translate_sse_event(raw, &mut ctx).unwrap();
    let chunk: serde_json::Value =
        serde_json::from_str(result.strip_prefix("data: ").unwrap().trim()).unwrap();
    assert_eq!(chunk["choices"][0]["delta"]["content"], "Hello world");
    assert!(chunk["choices"][0]["finish_reason"].is_null());
}

#[test]
fn translate_sse_message_delta() {
    let mut ctx = StreamContext {
        id: "chatcmpl-test".to_string(),
        ..Default::default()
    };
    let raw = "event: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":5}}";
    let result = translate_sse_event(raw, &mut ctx).unwrap();
    let chunk: serde_json::Value =
        serde_json::from_str(result.strip_prefix("data: ").unwrap().trim()).unwrap();
    assert_eq!(chunk["choices"][0]["finish_reason"], "stop");
}

#[test]
fn translate_sse_message_stop() {
    let mut ctx = StreamContext::default();
    let raw = "event: message_stop\ndata: {\"type\":\"message_stop\"}";
    let result = translate_sse_event(raw, &mut ctx).unwrap();
    assert_eq!(result, "data: [DONE]\n\n");
}

#[test]
fn translate_sse_skips_ping() {
    let mut ctx = StreamContext::default();
    let raw = "event: ping\ndata: {\"type\":\"ping\"}";
    assert!(translate_sse_event(raw, &mut ctx).is_none());
}

#[test]
fn translate_sse_tool_use_content_block_start() {
    let mut ctx = StreamContext::default();
    let raw = "event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"tool_use\",\"id\":\"toolu_abc\",\"name\":\"get_weather\",\"input\":{}}}";
    let result = translate_sse_event(raw, &mut ctx).unwrap();
    let chunk: serde_json::Value =
        serde_json::from_str(result.strip_prefix("data: ").unwrap().trim()).unwrap();
    let tc = &chunk["choices"][0]["delta"]["tool_calls"][0];
    assert_eq!(tc["index"], 0);
    assert_eq!(tc["id"], "toolu_abc");
    assert_eq!(tc["function"]["name"], "get_weather");
    assert_eq!(tc["function"]["arguments"], "");
    assert!(ctx.in_tool_use);
    assert_eq!(ctx.tool_call_index, 0);
}

#[test]
fn translate_sse_tool_use_input_json_delta() {
    let mut ctx = StreamContext {
        in_tool_use: true,
        tool_call_index: 0,
        ..Default::default()
    };
    let raw = "event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"input_json_delta\",\"partial_json\":\"{\\\"loc\"}}";
    let result = translate_sse_event(raw, &mut ctx).unwrap();
    let chunk: serde_json::Value =
        serde_json::from_str(result.strip_prefix("data: ").unwrap().trim()).unwrap();
    let tc = &chunk["choices"][0]["delta"]["tool_calls"][0];
    assert_eq!(tc["index"], 0);
    assert_eq!(tc["function"]["arguments"], "{\"loc");
}

#[test]
fn translate_sse_content_block_stop_resets_tool_state() {
    let mut ctx = StreamContext {
        in_tool_use: true,
        tool_call_index: 0,
        ..Default::default()
    };
    let raw = "event: content_block_stop\ndata: {\"type\":\"content_block_stop\",\"index\":0}";
    let result = translate_sse_event(raw, &mut ctx);
    assert!(result.is_none());
    assert!(!ctx.in_tool_use);
}

#[test]
fn translate_sse_tool_use_stop_reason() {
    let mut ctx = StreamContext::default();
    let raw = "event: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"tool_use\"},\"usage\":{\"output_tokens\":10}}";
    let result = translate_sse_event(raw, &mut ctx).unwrap();
    let chunk: serde_json::Value =
        serde_json::from_str(result.strip_prefix("data: ").unwrap().trim()).unwrap();
    assert_eq!(chunk["choices"][0]["finish_reason"], "tool_calls");
}

#[test]
fn translate_sse_inband_error_emits_openai_error_frame() {
    // LAB-710: an Anthropic `event: error` mid-stream must reach the OpenAI
    // client as an error frame, not vanish into the `_ => None` arm.
    let mut ctx = StreamContext::default();
    let raw = "event: error\ndata: {\"type\":\"error\",\"error\":{\"type\":\"overloaded_error\",\"message\":\"Overloaded\"}}";
    let result = translate_sse_event(raw, &mut ctx).unwrap();

    // Flag set → stream loop finalizes as failure and skips the clean [DONE]
    // guard (the error frame carries its own terminator).
    assert!(ctx.upstream_error);

    // Exactly one [DONE], and it belongs to the error frame itself.
    assert_eq!(result.matches("[DONE]").count(), 1);
    assert!(result.ends_with("data: [DONE]\n\n"));

    let first_event = result.split("\n\n").next().unwrap();
    let chunk: serde_json::Value =
        serde_json::from_str(first_event.strip_prefix("data: ").unwrap()).unwrap();
    assert_eq!(chunk["error"]["type"], "upstream_error");
    let msg = chunk["error"]["message"].as_str().unwrap();
    assert!(msg.contains("overloaded_error"));
    assert!(msg.contains("Overloaded"));
}

#[test]
fn translate_sse_text_block_start_skipped() {
    let mut ctx = StreamContext::default();
    let raw = "event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}";
    let result = translate_sse_event(raw, &mut ctx);
    assert!(result.is_none());
    assert!(!ctx.in_tool_use);
}

#[test]
fn translate_sse_multiple_tool_calls() {
    let mut ctx = StreamContext::default();

    // First tool_use block
    let raw1 = "event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"tool_use\",\"id\":\"toolu_1\",\"name\":\"search\",\"input\":{}}}";
    translate_sse_event(raw1, &mut ctx).unwrap();
    assert_eq!(ctx.tool_call_index, 0);

    // Close first
    let stop1 = "event: content_block_stop\ndata: {\"type\":\"content_block_stop\",\"index\":0}";
    translate_sse_event(stop1, &mut ctx);

    // Second tool_use block
    let raw2 = "event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":1,\"content_block\":{\"type\":\"tool_use\",\"id\":\"toolu_2\",\"name\":\"fetch\",\"input\":{}}}";
    let result = translate_sse_event(raw2, &mut ctx).unwrap();
    assert_eq!(ctx.tool_call_index, 1);
    let chunk: serde_json::Value =
        serde_json::from_str(result.strip_prefix("data: ").unwrap().trim()).unwrap();
    assert_eq!(chunk["choices"][0]["delta"]["tool_calls"][0]["index"], 1);
    assert_eq!(
        chunk["choices"][0]["delta"]["tool_calls"][0]["id"],
        "toolu_2"
    );
}

// ── Regression: streaming ↔ non-streaming content parity (LAB-711) ──
// A fenced reply must yield identical assembled content whether the client
// used stream:true or stream:false, in both default and json_object modes.
// Fences can be split across SSE deltas, so the streaming path buffers text
// and strips the whole message once — the same strip the non-streaming path
// applies. See GH #95 / codex-sol review.

/// Reconstruct the OpenAI `content` a client would assemble from an Anthropic
/// text reply delivered as `text_deltas` SSE frames.
fn reconstruct_stream_content(text_deltas: &[&str], json_mode: bool) -> String {
    let mut ctx = StreamContext {
        json_mode,
        ..Default::default()
    };
    let mut events: Vec<String> = vec![
        "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_x\",\"model\":\"claude-sonnet-4-6\",\"role\":\"assistant\"}}".to_string(),
        "event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}".to_string(),
    ];
    for d in text_deltas {
        let payload = serde_json::json!({
            "type": "content_block_delta",
            "index": 0,
            "delta": {"type": "text_delta", "text": d},
        });
        events.push(format!("event: content_block_delta\ndata: {payload}"));
    }
    events.push(
        "event: content_block_stop\ndata: {\"type\":\"content_block_stop\",\"index\":0}"
            .to_string(),
    );
    events.push("event: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":5}}".to_string());
    events.push("event: message_stop\ndata: {\"type\":\"message_stop\"}".to_string());

    let mut content = String::new();
    for ev in &events {
        let Some(out) = translate_sse_event(ev, &mut ctx) else {
            continue;
        };
        // One translation may carry multiple `data:` frames (buffered content
        // flushed together with the finish chunk).
        for frame in out.split("\n\n") {
            let Some(json) = frame.trim().strip_prefix("data: ") else {
                continue;
            };
            if json == "[DONE]" {
                continue;
            }
            let chunk: serde_json::Value = serde_json::from_str(json).unwrap();
            if let Some(c) = chunk["choices"][0]["delta"]["content"].as_str() {
                content.push_str(c);
            }
        }
    }
    content
}

/// Non-streaming assembled content for the same reply text.
fn nonstream_content(full_text: &str, json_mode: bool) -> String {
    let resp = serde_json::json!({
        "id": "msg_x",
        "content": [{"type": "text", "text": full_text}],
        "model": "claude-sonnet-4-6",
        "stop_reason": "end_turn",
        "usage": {"input_tokens": 1, "output_tokens": 1},
    });
    translate_anthropic_to_openai(&resp, json_mode)["choices"][0]["message"]["content"]
        .as_str()
        .unwrap()
        .to_string()
}

#[test]
fn stream_nonstream_content_parity_json_mode() {
    // Fence deliberately split across three SSE deltas.
    let deltas = ["```jso", "n\n{\"ok\":true}\n``", "`"];
    let full: String = deltas.concat();
    assert_eq!(full, "```json\n{\"ok\":true}\n```");

    let stream_json = reconstruct_stream_content(&deltas, true);
    let nonstream_json = nonstream_content(&full, true);
    // Both transports strip the fence down to raw JSON, and agree.
    assert_eq!(stream_json, r#"{"ok":true}"#);
    assert_eq!(stream_json, nonstream_json);
}

#[test]
fn stream_nonstream_content_parity_default_mode() {
    // Same reply, default (non-JSON) mode: fences preserved on both transports.
    let deltas = ["```jso", "n\n{\"ok\":true}\n``", "`"];
    let full: String = deltas.concat();

    let stream_default = reconstruct_stream_content(&deltas, false);
    let nonstream_default = nonstream_content(&full, false);
    assert_eq!(stream_default, full);
    assert_eq!(stream_default, nonstream_default);
}

#[test]
fn json_mode_stream_buffers_deltas_then_flushes_content_before_finish() {
    let mut ctx = StreamContext {
        json_mode: true,
        ..Default::default()
    };

    let start = "event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}";
    translate_sse_event(start, &mut ctx);

    // The complete fenced JSON reply spans two deltas. Neither partial delta
    // may be forwarded, because fence removal is applied to the whole reply.
    for text in ["```json\n{\"first\":", "true}\n```"] {
        let payload = serde_json::json!({
            "type": "content_block_delta",
            "index": 0,
            "delta": {"type": "text_delta", "text": text},
        });
        let event = format!("event: content_block_delta\ndata: {payload}");
        assert!(translate_sse_event(&event, &mut ctx).is_none());
    }

    let finish = "event: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":1}}";
    let output = translate_sse_event(finish, &mut ctx).unwrap();
    let frames: Vec<_> = output
        .split("\n\n")
        .filter_map(|frame| frame.strip_prefix("data: "))
        .map(|frame| serde_json::from_str::<serde_json::Value>(frame).unwrap())
        .collect();

    // The buffered, fence-stripped content is emitted exactly once and always
    // precedes the OpenAI finish chunk.
    assert_eq!(frames.len(), 2);
    assert_eq!(
        frames[0]["choices"][0]["delta"]["content"],
        r#"{"first":true}"#
    );
    assert!(frames[0]["choices"][0]["finish_reason"].is_null());
    assert_eq!(frames[1]["choices"][0]["delta"], serde_json::json!({}));
    assert_eq!(frames[1]["choices"][0]["finish_reason"], "stop");
    assert_eq!(
        translate_sse_event(
            "event: message_stop\ndata: {\"type\":\"message_stop\"}",
            &mut ctx
        )
        .as_deref(),
        Some("data: [DONE]\n\n")
    );
}

#[test]
fn json_mode_stream_message_stop_safety_net_flushes_buffer() {
    // Abnormal upstream: message_stop arrives without a preceding
    // message_delta. The buffered JSON-mode content must still be flushed
    // (fence-stripped) ahead of [DONE], never silently dropped.
    let mut ctx = StreamContext {
        json_mode: true,
        ..Default::default()
    };
    let payload = serde_json::json!({
        "type": "content_block_delta",
        "index": 0,
        "delta": {"type": "text_delta", "text": "```json\n{\"ok\":true}\n```"},
    });
    assert!(translate_sse_event(
        &format!("event: content_block_delta\ndata: {payload}"),
        &mut ctx
    )
    .is_none());

    let output = translate_sse_event(
        "event: message_stop\ndata: {\"type\":\"message_stop\"}",
        &mut ctx,
    )
    .unwrap();
    let mut frames = output
        .split("\n\n")
        .filter_map(|f| f.strip_prefix("data: "));
    let content: serde_json::Value = serde_json::from_str(frames.next().unwrap()).unwrap();
    assert_eq!(content["choices"][0]["delta"]["content"], r#"{"ok":true}"#);
    assert_eq!(frames.next(), Some("[DONE]"));
    // The stream loop detects the terminator with ends_with("data: [DONE]\n\n")
    // — a combined frame that failed this would earn a second [DONE] from the
    // post-loop guard (LAB-710 panel finding).
    assert!(output.ends_with("data: [DONE]\n\n"));
}

// ── Reverse translation: Anthropic → OpenAI ──────────────────────

#[test]
fn reverse_map_stop_reasons() {
    assert_eq!(reverse_map_stop_reason("stop"), "end_turn");
    assert_eq!(reverse_map_stop_reason("length"), "max_tokens");
    assert_eq!(reverse_map_stop_reason("tool_calls"), "tool_use");
    assert_eq!(reverse_map_stop_reason("unknown"), "end_turn");
}

#[test]
fn translate_anthropic_request_basic() {
    let body = serde_json::json!({
        "model": "claude-sonnet-4-6",
        "system": "You are helpful.",
        "messages": [
            {"role": "user", "content": "Hello"},
            {"role": "assistant", "content": "Hi there!"},
            {"role": "user", "content": "How are you?"}
        ],
        "max_tokens": 1024,
        "temperature": 0.7,
        "stream": true
    });

    let result = translate_anthropic_request_to_openai(&body).unwrap();
    assert_eq!(result["model"], "claude-sonnet-4-6");
    assert_eq!(result["max_tokens"], 1024);
    assert_eq!(result["temperature"], 0.7);
    assert_eq!(result["stream"], true);

    let msgs = result["messages"].as_array().unwrap();
    assert_eq!(msgs[0]["role"], "system");
    assert_eq!(msgs[0]["content"], "You are helpful.");
    assert_eq!(msgs[1]["role"], "user");
    assert_eq!(msgs[1]["content"], "Hello");
    assert_eq!(msgs[2]["role"], "assistant");
    assert_eq!(msgs[2]["content"], "Hi there!");
}

#[test]
fn translate_anthropic_request_tool_use() {
    let body = serde_json::json!({
        "model": "claude-sonnet-4-6",
        "messages": [
            {
                "role": "assistant",
                "content": [
                    {"type": "text", "text": "Let me search for that."},
                    {
                        "type": "tool_use",
                        "id": "toolu_123",
                        "name": "search",
                        "input": {"query": "test"}
                    }
                ]
            },
            {
                "role": "user",
                "content": [
                    {
                        "type": "tool_result",
                        "tool_use_id": "toolu_123",
                        "content": "Search result"
                    }
                ]
            }
        ],
        "max_tokens": 1024
    });

    let result = translate_anthropic_request_to_openai(&body).unwrap();
    let msgs = result["messages"].as_array().unwrap();

    // Assistant with tool_calls
    assert_eq!(msgs[0]["role"], "assistant");
    assert_eq!(msgs[0]["content"], "Let me search for that.");
    let tc = &msgs[0]["tool_calls"][0];
    assert_eq!(tc["id"], "toolu_123");
    assert_eq!(tc["function"]["name"], "search");

    // Tool result
    assert_eq!(msgs[1]["role"], "tool");
    assert_eq!(msgs[1]["tool_call_id"], "toolu_123");
    assert_eq!(msgs[1]["content"], "Search result");
}

#[test]
fn translate_anthropic_request_tools() {
    let body = serde_json::json!({
        "model": "claude-sonnet-4-6",
        "messages": [{"role": "user", "content": "test"}],
        "max_tokens": 1024,
        "tools": [
            {
                "name": "get_weather",
                "description": "Get weather info",
                "input_schema": {
                    "type": "object",
                    "properties": {"location": {"type": "string"}}
                }
            }
        ],
        "tool_choice": {"type": "auto"}
    });

    let result = translate_anthropic_request_to_openai(&body).unwrap();
    let tools = result["tools"].as_array().unwrap();
    assert_eq!(tools[0]["type"], "function");
    assert_eq!(tools[0]["function"]["name"], "get_weather");
    assert_eq!(result["tool_choice"], "auto");
}

#[test]
fn translate_anthropic_request_tool_result_array_content() {
    // tool_result with array content (structured response) should not be silently dropped
    let body = serde_json::json!({
        "model": "claude-sonnet-4-6",
        "messages": [
            {
                "role": "user",
                "content": [
                    {
                        "type": "tool_result",
                        "tool_use_id": "toolu_456",
                        "content": [
                            {"type": "text", "text": "Result line 1"},
                            {"type": "text", "text": "Result line 2"}
                        ]
                    }
                ]
            }
        ],
        "max_tokens": 1024
    });

    let result = translate_anthropic_request_to_openai(&body).unwrap();
    let msgs = result["messages"].as_array().unwrap();
    assert_eq!(msgs[0]["role"], "tool");
    assert_eq!(msgs[0]["content"], "Result line 1Result line 2");
}

#[test]
fn translate_anthropic_request_image_blocks() {
    // Anthropic image blocks must become OpenAI image_url parts, not be filtered out
    let body = serde_json::json!({
        "model": "claude-sonnet-4-6",
        "messages": [
            {"role": "user", "content": [
                {"type": "text", "text": "Describe this"},
                {"type": "image", "source": {
                    "type": "base64", "media_type": "image/jpeg", "data": "abc123"
                }},
                {"type": "image", "source": {
                    "type": "url", "url": "https://example.com/dog.png"
                }}
            ]}
        ],
        "max_tokens": 1024
    });

    let result = translate_anthropic_request_to_openai(&body).unwrap();
    let content = result["messages"][0]["content"].as_array().unwrap();
    assert_eq!(
        content[0],
        serde_json::json!({"type": "text", "text": "Describe this"})
    );
    assert_eq!(content[1]["type"], "image_url");
    assert_eq!(
        content[1]["image_url"]["url"],
        "data:image/jpeg;base64,abc123"
    );
    assert_eq!(content[2]["type"], "image_url");
    assert_eq!(
        content[2]["image_url"]["url"],
        "https://example.com/dog.png"
    );
}

#[test]
fn translate_anthropic_request_text_only_array_stays_string() {
    // Text-only block arrays keep the plain-string content form (no behavior change)
    let body = serde_json::json!({
        "model": "claude-sonnet-4-6",
        "messages": [
            {"role": "user", "content": [
                {"type": "text", "text": "part one "},
                {"type": "text", "text": "part two"}
            ]}
        ],
        "max_tokens": 1024
    });

    let result = translate_anthropic_request_to_openai(&body).unwrap();
    assert_eq!(result["messages"][0]["content"], "part one part two");
}

#[test]
fn translate_anthropic_request_image_beside_tool_result_survives() {
    // Images sharing a user message with tool_results must survive into the
    // leftover user message, not be text-filtered away
    let body = serde_json::json!({
        "model": "claude-sonnet-4-6",
        "messages": [
            {"role": "user", "content": [
                {"type": "tool_result", "tool_use_id": "toolu_789", "content": "done"},
                {"type": "text", "text": "And this image:"},
                {"type": "image", "source": {
                    "type": "url", "url": "https://example.com/chart.png"
                }}
            ]}
        ],
        "max_tokens": 1024
    });

    let result = translate_anthropic_request_to_openai(&body).unwrap();
    let msgs = result["messages"].as_array().unwrap();
    assert_eq!(msgs[0]["role"], "tool");
    assert_eq!(msgs[0]["content"], "done");
    assert_eq!(msgs[1]["role"], "user");
    let content = msgs[1]["content"].as_array().unwrap();
    assert_eq!(
        content[0],
        serde_json::json!({"type": "text", "text": "And this image:"})
    );
    assert_eq!(content[1]["type"], "image_url");
    assert_eq!(
        content[1]["image_url"]["url"],
        "https://example.com/chart.png"
    );
}

#[test]
fn reverse_sse_no_duplicate_message_stop() {
    let mut ctx = ReverseStreamContext::default();

    // Start message
    translate_openai_sse_to_anthropic(
            "{\"id\":\"chatcmpl-1\",\"model\":\"gpt-4\",\"choices\":[{\"delta\":{\"role\":\"assistant\",\"content\":\"\"},\"finish_reason\":null}]}",
            &mut ctx,
        );
    // Text
    translate_openai_sse_to_anthropic(
            "{\"id\":\"chatcmpl-1\",\"model\":\"gpt-4\",\"choices\":[{\"delta\":{\"content\":\"Hi\"},\"finish_reason\":null}]}",
            &mut ctx,
        );
    // Finish — emits message_stop
    let finish_events = translate_openai_sse_to_anthropic(
            "{\"id\":\"chatcmpl-1\",\"model\":\"gpt-4\",\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}]}",
            &mut ctx,
        );
    let stop_count = finish_events
        .iter()
        .filter(|e| e.contains("message_stop"))
        .count();
    assert_eq!(
        stop_count, 1,
        "finish_reason should emit exactly one message_stop"
    );

    // [DONE] — should NOT emit another message_stop
    let done_events = translate_openai_sse_to_anthropic("[DONE]", &mut ctx);
    assert!(
        done_events.is_empty(),
        "DONE after finish_reason should emit nothing (message_stop already sent)"
    );
}

#[test]
fn reverse_sse_message_stopped_set_by_both_terminator_paths() {
    // LAB-710: `ctx.message_stopped` gates the transport-error frame — once
    // the client has its `message_stop`, a later read failure must not ship
    // an error frame. Both emit sites must set it: finish_reason (the normal
    // case) and a bare [DONE] with no finish_reason seen.
    let mut ctx = ReverseStreamContext::default();
    translate_openai_sse_to_anthropic(
        "{\"id\":\"c1\",\"model\":\"gpt-4\",\"choices\":[{\"delta\":{\"role\":\"assistant\",\"content\":\"Hi\"},\"finish_reason\":null}]}",
        &mut ctx,
    );
    assert!(!ctx.message_stopped);
    translate_openai_sse_to_anthropic(
        "{\"id\":\"c1\",\"model\":\"gpt-4\",\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}]}",
        &mut ctx,
    );
    assert!(ctx.message_stopped, "finish_reason emitted message_stop");

    let mut ctx = ReverseStreamContext::default();
    translate_openai_sse_to_anthropic(
        "{\"id\":\"c1\",\"model\":\"gpt-4\",\"choices\":[{\"delta\":{\"role\":\"assistant\",\"content\":\"Hi\"},\"finish_reason\":null}]}",
        &mut ctx,
    );
    let done_events = translate_openai_sse_to_anthropic("[DONE]", &mut ctx);
    assert!(done_events[0].contains("message_stop"));
    assert!(ctx.message_stopped, "bare [DONE] emitted message_stop");
}

#[test]
fn reverse_sse_inband_error_before_message_start() {
    // LAB-710: an in-band OpenAI {"error": {...}} line before any content
    // must emit an Anthropic `event: error` frame — previously it hit the
    // missing-choices early-return and the client got 200 + empty SSE body.
    let mut ctx = ReverseStreamContext::default();
    let events = translate_openai_sse_to_anthropic(
        "{\"error\":{\"message\":\"The server had an error\",\"type\":\"server_error\"}}",
        &mut ctx,
    );
    assert!(ctx.upstream_error);
    assert_eq!(events.len(), 1);
    assert!(events[0].starts_with("event: error\n"));
    let data_line = events[0].lines().nth(1).unwrap();
    let body: serde_json::Value =
        serde_json::from_str(data_line.strip_prefix("data: ").unwrap()).unwrap();
    assert_eq!(body["type"], "error");
    assert_eq!(body["error"]["type"], "api_error");
    let msg = body["error"]["message"].as_str().unwrap();
    assert!(msg.contains("server_error"));
    assert!(msg.contains("The server had an error"));

    // No fake success terminator: trailing [DONE] after the error emits nothing.
    let after = translate_openai_sse_to_anthropic("[DONE]", &mut ctx);
    assert!(after.is_empty());
}

#[test]
fn reverse_sse_inband_error_mid_message_suppresses_message_stop() {
    // Error arriving after content started: error frame is final — no
    // message_stop may follow it, even if the upstream still sends [DONE].
    let mut ctx = ReverseStreamContext::default();
    translate_openai_sse_to_anthropic(
        "{\"id\":\"chatcmpl-1\",\"model\":\"gpt-4\",\"choices\":[{\"delta\":{\"role\":\"assistant\",\"content\":\"Hi\"},\"finish_reason\":null}]}",
        &mut ctx,
    );
    assert!(ctx.message_started);

    let err_events = translate_openai_sse_to_anthropic(
        "{\"error\":{\"message\":\"overloaded\",\"type\":\"server_error\"}}",
        &mut ctx,
    );
    assert_eq!(err_events.len(), 1);
    assert!(err_events[0].starts_with("event: error\n"));

    let done_events = translate_openai_sse_to_anthropic("[DONE]", &mut ctx);
    assert!(
        done_events.is_empty(),
        "no message_stop may follow an in-band error frame"
    );
}

#[test]
fn translate_anthropic_request_stop_sequences() {
    let body = serde_json::json!({
        "model": "claude-sonnet-4-6",
        "messages": [{"role": "user", "content": "test"}],
        "max_tokens": 1024,
        "stop_sequences": ["END", "STOP"]
    });

    let result = translate_anthropic_request_to_openai(&body).unwrap();
    assert_eq!(result["stop"], serde_json::json!(["END", "STOP"]));
}

#[test]
fn translate_anthropic_request_unsupported_image_source_fails_loudly() {
    // An image source type this translator can't represent (e.g. Anthropic's
    // `file` source) must fail the whole request, not silently drop the image
    // while keeping the surrounding text.
    let body = serde_json::json!({
        "model": "claude-sonnet-4-6",
        "messages": [
            {"role": "user", "content": [
                {"type": "text", "text": "Describe this"},
                {"type": "image", "source": {"type": "file", "file_id": "file_abc"}}
            ]}
        ],
        "max_tokens": 1024
    });

    let err = translate_anthropic_request_to_openai(&body).unwrap_err();
    assert!(
        err.contains("file"),
        "error should name the unsupported source type: {err}"
    );
}

#[test]
fn translate_anthropic_request_malformed_image_source_fails_loudly() {
    // A structurally invalid image source (missing required fields) must also
    // fail loudly rather than being silently dropped.
    let body = serde_json::json!({
        "model": "claude-sonnet-4-6",
        "messages": [
            {"role": "user", "content": [
                {"type": "image", "source": {"type": "base64", "media_type": "image/png"}}
            ]}
        ],
        "max_tokens": 1024
    });

    assert!(translate_anthropic_request_to_openai(&body).is_err());
}

#[test]
fn translate_anthropic_request_unsupported_block_type_fails_loudly() {
    // A content block type this translator can't represent (e.g. `document`)
    // must fail the whole request, not be silently dropped from the message.
    let body = serde_json::json!({
        "model": "claude-sonnet-4-6",
        "messages": [
            {"role": "user", "content": [
                {"type": "text", "text": "Summarize this"},
                {"type": "document", "source": {
                    "type": "base64", "media_type": "application/pdf", "data": "JVBERi0="
                }}
            ]}
        ],
        "max_tokens": 1024
    });

    let err = translate_anthropic_request_to_openai(&body).unwrap_err();
    assert!(
        err.contains("document"),
        "error should name the unsupported block type: {err}"
    );
}

#[test]
fn translate_openai_response_basic() {
    let body = serde_json::json!({
        "id": "chatcmpl-abc123",
        "object": "chat.completion",
        "model": "gpt-4",
        "choices": [{
            "index": 0,
            "message": {
                "role": "assistant",
                "content": "Hello!"
            },
            "finish_reason": "stop"
        }],
        "usage": {
            "prompt_tokens": 10,
            "completion_tokens": 5,
            "total_tokens": 15
        }
    });

    let result = translate_openai_response_to_anthropic(&body);
    assert_eq!(result["id"], "msg_abc123");
    assert_eq!(result["type"], "message");
    assert_eq!(result["model"], "gpt-4");
    assert_eq!(result["stop_reason"], "end_turn");
    assert_eq!(result["content"][0]["type"], "text");
    assert_eq!(result["content"][0]["text"], "Hello!");
    assert_eq!(result["usage"]["input_tokens"], 10);
    assert_eq!(result["usage"]["output_tokens"], 5);
}

#[test]
fn translate_openai_response_tool_calls() {
    let body = serde_json::json!({
        "id": "chatcmpl-xyz",
        "model": "gpt-4",
        "choices": [{
            "message": {
                "role": "assistant",
                "content": null,
                "tool_calls": [{
                    "id": "call_123",
                    "type": "function",
                    "function": {
                        "name": "search",
                        "arguments": "{\"query\":\"test\"}"
                    }
                }]
            },
            "finish_reason": "tool_calls"
        }],
        "usage": {"prompt_tokens": 10, "completion_tokens": 5, "total_tokens": 15}
    });

    let result = translate_openai_response_to_anthropic(&body);
    assert_eq!(result["stop_reason"], "tool_use");
    let blocks = result["content"].as_array().unwrap();
    assert_eq!(blocks[0]["type"], "tool_use");
    assert_eq!(blocks[0]["id"], "call_123");
    assert_eq!(blocks[0]["name"], "search");
    assert_eq!(blocks[0]["input"]["query"], "test");
}

#[test]
fn reverse_sse_basic_text() {
    let mut ctx = ReverseStreamContext::default();

    // First chunk with role
    let events = translate_openai_sse_to_anthropic(
            "{\"id\":\"chatcmpl-1\",\"model\":\"gpt-4\",\"choices\":[{\"delta\":{\"role\":\"assistant\",\"content\":\"\"},\"finish_reason\":null}]}",
            &mut ctx,
        );
    assert!(ctx.message_started);
    assert!(events.iter().any(|e| e.contains("message_start")));

    // Text delta
    let events = translate_openai_sse_to_anthropic(
            "{\"id\":\"chatcmpl-1\",\"model\":\"gpt-4\",\"choices\":[{\"delta\":{\"content\":\"Hello\"},\"finish_reason\":null}]}",
            &mut ctx,
        );
    assert!(events.iter().any(|e| e.contains("text_delta")));
    assert!(events.iter().any(|e| e.contains("Hello")));

    // Finish
    let events = translate_openai_sse_to_anthropic(
            "{\"id\":\"chatcmpl-1\",\"model\":\"gpt-4\",\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}]}",
            &mut ctx,
        );
    assert!(events.iter().any(|e| e.contains("message_delta")));
    assert!(events.iter().any(|e| e.contains("end_turn")));
    assert!(events.iter().any(|e| e.contains("message_stop")));
}

#[test]
fn reverse_sse_tool_use() {
    let mut ctx = ReverseStreamContext::default();

    // Tool call start
    let events = translate_openai_sse_to_anthropic(
            "{\"id\":\"chatcmpl-1\",\"model\":\"gpt-4\",\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"call_1\",\"type\":\"function\",\"function\":{\"name\":\"search\",\"arguments\":\"\"}}]},\"finish_reason\":null}]}",
            &mut ctx,
        );
    assert!(ctx.message_started);
    assert!(ctx.in_tool_use);
    assert!(events.iter().any(|e| e.contains("content_block_start")));
    assert!(events.iter().any(|e| e.contains("tool_use")));
    assert!(events.iter().any(|e| e.contains("search")));

    // Tool arguments delta
    let events = translate_openai_sse_to_anthropic(
            "{\"id\":\"chatcmpl-1\",\"model\":\"gpt-4\",\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0,\"function\":{\"arguments\":\"{\\\"q\\\"\"}}]},\"finish_reason\":null}]}",
            &mut ctx,
        );
    assert!(events.iter().any(|e| e.contains("input_json_delta")));

    // Finish
    let events = translate_openai_sse_to_anthropic(
            "{\"id\":\"chatcmpl-1\",\"model\":\"gpt-4\",\"choices\":[{\"delta\":{},\"finish_reason\":\"tool_calls\"}]}",
            &mut ctx,
        );
    assert!(events.iter().any(|e| e.contains("content_block_stop")));
    assert!(events.iter().any(|e| e.contains("tool_use")));
}

#[test]
fn reverse_sse_done_sentinel() {
    let mut ctx = ReverseStreamContext {
        message_started: true,
        ..ReverseStreamContext::default()
    };
    let events = translate_openai_sse_to_anthropic("[DONE]", &mut ctx);
    assert!(events.iter().any(|e| e.contains("message_stop")));
}

// ── Integration: OpenAI-compat handler ──────────────────────────

/// Mock that returns Anthropic /v1/messages format (non-streaming)
async fn mock_anthropic_handler(req: Request<Body>) -> Response {
    let has_auth =
        req.headers().contains_key("x-api-key") || req.headers().contains_key("authorization");
    if !has_auth {
        return (StatusCode::UNAUTHORIZED, "missing auth").into_response();
    }

    let mut resp = axum::Json(serde_json::json!({
        "id": "msg_integration",
        "type": "message",
        "content": [{"type": "text", "text": "Hello from Claude"}],
        "model": "claude-sonnet-4-6",
        "stop_reason": "end_turn",
        "usage": {"input_tokens": 10, "output_tokens": 5}
    }))
    .into_response();

    let headers = resp.headers_mut();
    headers.insert(
        "anthropic-ratelimit-unified-representative-claim",
        HeaderValue::from_static("five_hour"),
    );
    headers.insert(
        "anthropic-ratelimit-unified-5h-utilization",
        HeaderValue::from_static("0.30"),
    );
    resp
}

/// Mock that returns Anthropic SSE streaming format
async fn mock_anthropic_streaming_handler(req: Request<Body>) -> Response {
    let has_auth =
        req.headers().contains_key("x-api-key") || req.headers().contains_key("authorization");
    if !has_auth {
        return (StatusCode::UNAUTHORIZED, "missing auth").into_response();
    }

    let events = [
            "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_stream\",\"type\":\"message\",\"role\":\"assistant\",\"model\":\"claude-sonnet-4-6\",\"content\":[],\"stop_reason\":null,\"usage\":{\"input_tokens\":10,\"output_tokens\":0}}}\n\n",
            "event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\n",
            "event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"Hello\"}}\n\n",
            "event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\" world\"}}\n\n",
            "event: content_block_stop\ndata: {\"type\":\"content_block_stop\",\"index\":0}\n\n",
            "event: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":5}}\n\n",
            "event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n",
        ];

    let body = events.join("");
    Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "text/event-stream")
        .body(Body::from(body))
        .unwrap()
}

/// Build test app with separate handlers for streaming vs non-streaming.
/// Both Anthropic endpoints point at `upstream_url` (the mock upstream).
fn test_openai_app(upstream_url: &str, proxy_key: Option<String>) -> (Router, Arc<AppState>) {
    let mut acct_a = mk_endpoint("acct-a", "sk-ant-api-test-aaa");
    acct_a.base_url = upstream_url.to_string();
    let mut acct_b = mk_endpoint("acct-b", "sk-ant-api-test-bbb");
    acct_b.base_url = upstream_url.to_string();
    let accounts = vec![acct_a, acct_b];

    let state = Arc::new(AppState {
        endpoints: accounts,
        state_path: PathBuf::from("/tmp/anthropic-lb-openai-test.state.json"),
        proxy_key,
        ..test_state_base()
    });

    let app = Router::new()
        .route(
            "/v1/chat/completions",
            axum::routing::post(openai_chat_handler),
        )
        .with_state(state.clone());

    (app, state)
}

fn test_app(upstream_url: &str, proxy_key: Option<String>) -> (Router, Arc<AppState>) {
    test_app_with_strategy(upstream_url, proxy_key, RoutingStrategy::default())
}

/// Minimal valid Anthropic messages response, served by the raw-TCP mocks.
const ANTHROPIC_OK_BODY: &[u8] = br#"{"id":"msg_1","type":"message","role":"assistant","content":[{"type":"text","text":"hi"}],"model":"test","stop_reason":"end_turn","usage":{"input_tokens":1,"output_tokens":1}}"#;

/// Minimal valid OpenAI chat-completion response, for `Protocol::OpenAI`
/// endpoint mocks (`try_fallback_upstream` translates it back to Anthropic).
const OPENAI_OK_BODY: &[u8] = br#"{"id":"chatcmpl-1","object":"chat.completion","model":"test","choices":[{"index":0,"message":{"role":"assistant","content":"hi"},"finish_reason":"stop"}],"usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}}"#;

/// Raw-TCP upstream that RSTs its first `dead_first` connections (each → a
/// reqwest transport error, i.e. a `transient` `ForwardOutcome`) then serves
/// `body` as an HTTP 200 on every later connection. Returns
/// `(base_url, per_connection_hits)`. `usize::MAX` = dead forever.
async fn spawn_flaky_upstream(
    dead_first: usize,
    body: &'static [u8],
) -> (String, std::sync::Arc<std::sync::atomic::AtomicUsize>) {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let hits = Arc::new(AtomicUsize::new(0));
    let h = hits.clone();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let (mut s, _) = listener.accept().await.unwrap();
            if h.fetch_add(1, Ordering::SeqCst) < dead_first {
                drop(s); // reset before responding → transport error
                continue;
            }
            let mut buf = [0u8; 4096];
            let _ = s.read(&mut buf).await; // drain the request before responding
            let head = format!(
                    "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
                    body.len()
                );
            let _ = s.write_all(head.as_bytes()).await;
            let _ = s.write_all(body).await;
            let _ = s.flush().await;
        }
    });
    (format!("http://{addr}"), hits)
}

/// Raw-TCP upstream that RSTs its FIRST connection then serves a valid
/// Anthropic 200 on every later connection — a sub-second egress blip.
async fn spawn_blip_upstream() -> (String, std::sync::Arc<std::sync::atomic::AtomicUsize>) {
    spawn_flaky_upstream(1, ANTHROPIC_OK_BODY).await
}

// ── Task 2: round-gated transient backoff-retry ─────────────────────

/// A transient blip that recovers must surface as 200, not a 429-exhausted.
/// Today (529-only backoff) the single round breaks straight to 429.
#[tokio::test]
async fn proxy_rides_out_transient_upstream_blip() {
    let (url, _hits) = spawn_blip_upstream().await;
    let state = test_state_with(vec![mk_endpoint_at("a", "sk-ant-api-aaa", &url)]);
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
        "a transient blip that recovers must surface as 200, not 429-exhausted"
    );
}

/// Option B: round 0 must RE-TRY the affinity/cache-warm endpoint in place,
/// NOT rotate to a cold endpoint. Two distinct upstreams both blip-then-200;
/// whichever affinity picks serves after the backoff, and the OTHER endpoint
/// must stay at ZERO hits. A flat `push_skip:true` would rotate on round 0,
/// hitting both. (Mutation-test: delete the `retry_round==0` guard → fails.)
#[tokio::test]
async fn transient_blip_retries_warm_endpoint_not_rotates() {
    use std::sync::atomic::Ordering;
    let (url_a, a_hits) = spawn_blip_upstream().await;
    let (url_b, b_hits) = spawn_blip_upstream().await;
    let state = test_state_with(vec![
        mk_endpoint_at("a", "sk-ant-api-aaa", &url_a),
        mk_endpoint_at("b", "sk-ant-api-bbb", &url_b),
    ]);
    let addr = serve(build_router(state)).await;
    let resp = reqwest::Client::new()
        .post(format!("http://{addr}/v1/messages"))
        .header("content-type", "application/json")
        // identity headers → deterministic (non-round-robin) affinity, so
        // round 0 and round 1 pick the SAME endpoint.
        .header("x-client-id", "sticky")
        .header("x-session-id", "sticky")
        .body(r#"{"model":"test","max_tokens":1,"messages":[{"role":"user","content":"hi"}]}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let (a, b) = (a_hits.load(Ordering::SeqCst), b_hits.load(Ordering::SeqCst));
    assert!(
        a == 0 || b == 0,
        "round-0 transient must NOT rotate to a cold endpoint (a_hits={a}, b_hits={b})"
    );
    assert!(
        a >= 2 || b >= 2,
        "the warm endpoint should be retried after the blip (a_hits={a}, b_hits={b})"
    );
}

/// The openai_chat_handler retry loop must ride out a transient blip too —
/// proves it is wired to the same shared round-gated helper as proxy_handler.
#[tokio::test]
async fn openai_chat_rides_out_transient_upstream_blip() {
    let (url, _hits) = spawn_blip_upstream().await;
    let state = test_state_with(vec![mk_endpoint_at("a", "sk-ant-api-aaa", &url)]);
    let addr = serve(build_router(state)).await;
    let resp = reqwest::Client::new()
        .post(format!("http://{addr}/v1/chat/completions"))
        .header("content-type", "application/json")
        .body(r#"{"model":"test","max_tokens":1,"messages":[{"role":"user","content":"hi"}]}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::OK,
        "openai_chat_handler must also ride out a transient blip via the shared helper"
    );
}

/// Raw-TCP upstream that RSTs EVERY connection — a genuinely-down egress.
async fn spawn_dead_upstream() -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            if let Ok((s, _)) = listener.accept().await {
                drop(s); // reset before responding → transport error, every time
            }
        }
    });
    format!("http://{addr}")
}

// ── Task 3: transient exhaustion returns a retryable 503 ────────────

/// When every endpoint transport-fails through all backoff rounds, the
/// client must get a retryable `503 + Retry-After`, not a `429` (which
/// reads as account rate-limiting). Task 2 still returned 429 here.
#[tokio::test]
async fn proxy_returns_503_when_upstream_unreachable_transiently() {
    let url = spawn_dead_upstream().await;
    let state = test_state_with(vec![mk_endpoint_at("a", "sk-ant-api-aaa", &url)]);
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
        "transient upstream exhaustion must be a retryable 503, not 429"
    );
    assert!(
        resp.headers().get("retry-after").is_some(),
        "503 exhaustion should carry Retry-After so the client times its backoff"
    );
}

// ── #70: transport circuit-breaker for persistently-dead endpoints ──

/// A persistently-dead endpoint must leave the routing pool after
/// TRANSPORT_FAILURE_THRESHOLD consecutive transport failures, and the
/// session must migrate ONCE to a healthy endpoint instead of paying the
/// affinity tax (two connect stalls) on every request. The dead endpoint
/// sits at priority 0 so routing MUST pick it until the breaker opens —
/// affinity cannot dodge it — making the hit counters deterministic.
#[tokio::test]
async fn dead_endpoint_circuit_breaks_and_session_migrates_once() {
    use std::sync::atomic::Ordering;
    let (dead_url, dead_hits) = spawn_flaky_upstream(usize::MAX, ANTHROPIC_OK_BODY).await;
    let (ok_url, ok_hits) = spawn_flaky_upstream(0, ANTHROPIC_OK_BODY).await;
    let dead = mk_endpoint_at("dead", "sk-ant-api-aaa", &dead_url); // priority 0
    let mut ok = mk_endpoint_at("ok", "sk-ant-api-bbb", &ok_url);
    ok.priority = 1;
    let state = test_state_with(vec![dead, ok]);
    let probe = state.clone();
    let addr = serve(build_router(state)).await;

    let client = reqwest::Client::new();
    for i in 0..4 {
        let resp = client
            .post(format!("http://{addr}/v1/messages"))
            .header("content-type", "application/json")
            .header("x-client-id", "sticky")
            .header("x-session-id", "sticky")
            .body(r#"{"model":"test","max_tokens":1,"messages":[{"role":"user","content":"hi"}]}"#)
            .send()
            .await
            .unwrap();
        assert_eq!(
            resp.status(),
            reqwest::StatusCode::OK,
            "request {i} must succeed via the healthy endpoint"
        );
    }

    // req1: round 0 + round 1 fail (2 hits); req2: round 0 fails (3rd hit,
    // breaker opens). req3/req4 must NOT touch the dead endpoint at all.
    let (d, o) = (
        dead_hits.load(Ordering::SeqCst),
        ok_hits.load(Ordering::SeqCst),
    );
    assert_eq!(
        d, 3,
        "dead endpoint must stop being dialed once the breaker opens (dead_hits={d})"
    );
    assert_eq!(
        o, 4,
        "every request must be served by the healthy endpoint exactly once (ok_hits={o})"
    );

    // Breaker is transport state, NOT rate-limit state.
    let info = probe.endpoints[0].rate_info.read().await;
    assert!(
        info.transport_unhealthy_until.is_some(),
        "breaker must be open on the dead endpoint"
    );
    assert!(
        info.hard_limited_until.is_none(),
        "transport breaker must stay independent of the 429 hard-limit path"
    );
}

/// A recovered endpoint re-enters the pool after the cooldown window, and a
/// successful forward clears the failure counter and the breaker.
#[tokio::test]
async fn circuit_broken_endpoint_reenters_after_cooldown() {
    use std::sync::atomic::Ordering;
    // Dead for exactly 3 connections (the breaker threshold), then healthy.
    let (flaky_url, flaky_hits) = spawn_flaky_upstream(3, ANTHROPIC_OK_BODY).await;
    let (ok_url, ok_hits) = spawn_flaky_upstream(0, ANTHROPIC_OK_BODY).await;
    let flaky = mk_endpoint_at("flaky", "sk-ant-api-aaa", &flaky_url); // priority 0
    let mut ok = mk_endpoint_at("ok", "sk-ant-api-bbb", &ok_url);
    ok.priority = 1;
    let state = Arc::new(AppState {
        endpoints: vec![flaky, ok],
        transport_cooldown: Duration::from_millis(500),
        ..test_state_base()
    });
    let probe = state.clone();
    let addr = serve(build_router(state)).await;

    let client = reqwest::Client::new();
    let send = |i: u32| {
        let client = client.clone();
        async move {
            let resp = client
                    .post(format!("http://{addr}/v1/messages"))
                    .header("content-type", "application/json")
                    .body(r#"{"model":"test","max_tokens":1,"messages":[{"role":"user","content":"hi"}]}"#)
                    .send()
                    .await
                    .unwrap();
            assert_eq!(
                resp.status(),
                reqwest::StatusCode::OK,
                "request {i} must succeed"
            );
        }
    };

    // req1 (2 failures) + req2 (3rd failure → breaker opens) + req3 (skips
    // the broken endpoint entirely).
    for i in 1..=3 {
        send(i).await;
    }
    assert_eq!(flaky_hits.load(Ordering::SeqCst), 3);
    assert_eq!(ok_hits.load(Ordering::SeqCst), 3);

    // Let the cooldown elapse; the endpoint (now healthy) must re-enter at
    // its priority-0 slot and serve the next request itself.
    tokio::time::sleep(Duration::from_millis(700)).await;
    send(4).await;
    assert_eq!(
        flaky_hits.load(Ordering::SeqCst),
        4,
        "recovered endpoint must re-enter the pool after the cooldown"
    );
    assert_eq!(
        ok_hits.load(Ordering::SeqCst),
        3,
        "the fallback endpoint must NOT serve once the recovered endpoint is back"
    );

    // The successful forward must clear the breaker and the counter.
    let info = probe.endpoints[0].rate_info.read().await;
    assert_eq!(
        info.consecutive_transport_failures, 0,
        "failure counter must auto-clear on a successful forward"
    );
    assert!(
        info.transport_unhealthy_until.is_none(),
        "breaker must close on a successful forward"
    );
}

/// The consecutive-failure counter starts a fresh era once the cooldown has
/// elapsed: an expired breaker's failures must not carry over, so re-opening
/// takes a full threshold of NEW evidence.
#[tokio::test]
async fn transport_failure_counter_era_resets_after_cooldown() {
    let state = Arc::new(AppState {
        endpoints: vec![mk_endpoint("a", "sk-ant-api-aaa")],
        transport_cooldown: Duration::ZERO, // breaker expires immediately
        ..test_state_base()
    });
    for _ in 0..TRANSPORT_FAILURE_THRESHOLD {
        state.record_transport_failure(0).await;
    }
    {
        let info = state.endpoints[0].rate_info.read().await;
        assert_eq!(
            info.consecutive_transport_failures,
            TRANSPORT_FAILURE_THRESHOLD
        );
        assert!(
            info.transport_unhealthy_until.is_some(),
            "breaker must open at the threshold"
        );
    }
    // Cooldown (zero) has elapsed → the next failure is the FIRST of a new
    // era, not the fourth of the old one.
    state.record_transport_failure(0).await;
    let info = state.endpoints[0].rate_info.read().await;
    assert_eq!(
        info.consecutive_transport_failures, 1,
        "counter must reset to a fresh era after the cooldown elapses"
    );
    assert!(
        info.transport_unhealthy_until.is_none(),
        "one post-cooldown failure must not re-open the breaker"
    );
}

/// A successful forward clears transport state only — it must not clobber
/// the (independent) 429 hard-limit path.
#[tokio::test]
async fn transport_success_clears_failures_and_leaves_hard_limit_alone() {
    let state = test_state_with(vec![mk_endpoint("a", "sk-ant-api-aaa")]);
    state.record_transport_failure(0).await;
    state.record_transport_failure(0).await;
    {
        let mut info = state.endpoints[0].rate_info.write().await;
        info.hard_limited_until = Some(Instant::now() + Duration::from_secs(3600));
    }
    state.record_transport_success(0).await;
    let info = state.endpoints[0].rate_info.read().await;
    assert_eq!(info.consecutive_transport_failures, 0);
    assert!(info.transport_unhealthy_until.is_none());
    assert!(
        info.hard_limited_until.is_some(),
        "clearing transport health must not clear the 429 hard limit"
    );
}

/// Both protocol branches of `routing_candidates` must exclude a
/// transport-unhealthy endpoint while the breaker is open, and re-admit it
/// once the window has passed.
#[tokio::test]
async fn pick_endpoint_excludes_transport_unhealthy_endpoints() {
    let anthropic = mk_endpoint("anth", "sk-ant-api-aaa");
    let openai = make_endpoint("gw", Protocol::OpenAI);
    let state = test_state_with(vec![anthropic, openai]);
    for ep in &state.endpoints {
        let mut info = ep.rate_info.write().await;
        info.transport_unhealthy_until = Some(Instant::now() + Duration::from_secs(60));
    }
    assert!(
        state.pick_endpoint(None, "", &[]).await.is_none(),
        "both anthropic and openai endpoints must be excluded while unhealthy"
    );
    // Close the OpenAI endpoint's breaker → it must become pickable again.
    {
        let mut info = state.endpoints[1].rate_info.write().await;
        info.transport_unhealthy_until = None;
    }
    assert_eq!(
        state.pick_endpoint(None, "", &[]).await,
        Some(1),
        "a recovered endpoint must be pickable while the other stays excluded"
    );
}

/// The #69 KNOWN GAP: a transport-dead `Protocol::OpenAI` endpoint used to
/// be swallowed to a bare `None` — no transient classification, so the
/// client got a misleading 429. It must exhaust as a retryable 503 exactly
/// like the Anthropic path.
#[tokio::test]
async fn proxy_returns_503_when_openai_endpoint_unreachable() {
    let url = spawn_dead_upstream().await;
    let mut gw = make_endpoint("gw", Protocol::OpenAI);
    gw.base_url = url;
    let state = test_state_with(vec![gw]);
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
        "a transport-dead OpenAI endpoint must exhaust as a retryable 503, not 429"
    );
    assert!(
        resp.headers().get("retry-after").is_some(),
        "transient exhaustion must carry Retry-After"
    );
}

/// The OpenAI branch must also get #69's round-gated in-place retry: a
/// single-blip OpenAI endpoint recovers to a 200 instead of failing the
/// round (previously: swallowed to `None` → skip → premature 429).
#[tokio::test]
async fn openai_endpoint_rides_out_transient_blip() {
    let (url, _hits) = spawn_flaky_upstream(1, OPENAI_OK_BODY).await;
    let mut gw = make_endpoint("gw", Protocol::OpenAI);
    gw.base_url = url;
    let state = test_state_with(vec![gw]);
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
        "an OpenAI endpoint blip must be retried in place, not fail the request"
    );
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(
        body["type"], "message",
        "response must be translated back to Anthropic format"
    );
}

// ── GH #97: OpenAI-endpoint 429 hard-limit cooldown + 529 BEBO ───────

/// Raw-TCP upstream that serves `bad_head` (a complete pre-formatted HTTP
/// response head with an empty body) for its first `bad_first` connections,
/// then `ok_body` as a 200 on every later connection. Returns
/// `(base_url, per_connection_hits)`. `usize::MAX` = bad forever.
async fn spawn_status_then_ok_upstream(
    bad_first: usize,
    bad_head: &'static str,
    ok_body: &'static [u8],
) -> (String, std::sync::Arc<std::sync::atomic::AtomicUsize>) {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let hits = Arc::new(AtomicUsize::new(0));
    let h = hits.clone();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let (mut s, _) = listener.accept().await.unwrap();
            let mut buf = [0u8; 4096];
            let _ = s.read(&mut buf).await; // drain the request before responding
            if h.fetch_add(1, Ordering::SeqCst) < bad_first {
                let _ = s.write_all(bad_head.as_bytes()).await;
            } else {
                let head = format!(
                    "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
                    ok_body.len()
                );
                let _ = s.write_all(head.as_bytes()).await;
                let _ = s.write_all(ok_body).await;
            }
            let _ = s.flush().await;
        }
    });
    (format!("http://{addr}"), hits)
}

/// GH #97 regression: a 429 from a `Protocol::OpenAI` endpoint must set a
/// hard-limit cooldown (honouring `retry-after`) so a SUBSEQUENT request
/// skips it at `pick_endpoint` instead of re-hammering an upstream that
/// told us to back off. Previously the 429 only rotated within the current
/// request's retry loop — every new request re-attempted the endpoint.
#[tokio::test]
async fn openai_429_sets_cooldown_and_next_request_skips_endpoint() {
    use std::sync::atomic::Ordering;
    const HEAD_429: &str = "HTTP/1.1 429 Too Many Requests\r\nretry-after: 120\r\ncontent-length: 0\r\nconnection: close\r\n\r\n";
    let (limited_url, limited_hits) =
        spawn_status_then_ok_upstream(usize::MAX, HEAD_429, OPENAI_OK_BODY).await;
    let (healthy_url, healthy_hits) = spawn_flaky_upstream(0, OPENAI_OK_BODY).await;

    let mut limited = make_endpoint("limited", Protocol::OpenAI);
    limited.base_url = limited_url;
    let mut healthy = make_endpoint("healthy", Protocol::OpenAI);
    healthy.base_url = healthy_url;
    // Priority forces routing to prefer `limited` until it leaves the pool —
    // deterministic without depending on affinity hashing.
    healthy.priority = 1;
    let state = test_state_with(vec![limited, healthy]);
    let addr = serve(build_router(state.clone())).await;

    let client = reqwest::Client::new();
    for _ in 0..2 {
        let resp = client
            .post(format!("http://{addr}/v1/messages"))
            .header("content-type", "application/json")
            .body(r#"{"model":"test","max_tokens":1,"messages":[{"role":"user","content":"hi"}]}"#)
            .send()
            .await
            .unwrap();
        assert_eq!(
            resp.status(),
            reqwest::StatusCode::OK,
            "both requests must succeed via the healthy endpoint"
        );
    }

    assert_eq!(
        limited_hits.load(Ordering::SeqCst),
        1,
        "the second request must skip the hard-limited endpoint, not re-hammer it"
    );
    assert!(healthy_hits.load(Ordering::SeqCst) >= 2);

    let info = state.endpoints[0].rate_info.read().await;
    let until = info
        .hard_limited_until
        .expect("a 429 from an OpenAI endpoint must set hard_limited_until");
    // Measured AFTER both round-trips, so the lower bound leaves ~20s of
    // slack for a loaded CI runner while staying far above the 60s default;
    // the upper bound is exact (until = t_429 + 120s, and t_429 < now).
    let cooldown = until.duration_since(Instant::now());
    assert!(
        cooldown > Duration::from_secs(100) && cooldown <= Duration::from_secs(120),
        "cooldown must honour retry-after: 120, not fall back to the 60s default, got {cooldown:?}"
    );
}

/// GH #97: a 529 from an OpenAI endpoint must flag `saw_529` so the retry
/// loop BEBO-retries the pool (long base) instead of exhausting straight to
/// a 429 — aligned with the Anthropic path via `classify_retry_status`.
#[tokio::test]
async fn openai_529_triggers_bebo_backoff_retry() {
    use std::sync::atomic::Ordering;
    const HEAD_529: &str =
        "HTTP/1.1 529 Overloaded\r\ncontent-length: 0\r\nconnection: close\r\n\r\n";
    let (url, hits) = spawn_status_then_ok_upstream(1, HEAD_529, OPENAI_OK_BODY).await;
    let mut gw = make_endpoint("gw", Protocol::OpenAI);
    gw.base_url = url;
    let state = test_state_with(vec![gw]);
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
        "a 529 that recovers must be BEBO-retried to a 200, not exhausted to a 429"
    );
    assert_eq!(
        hits.load(Ordering::SeqCst),
        2,
        "exactly one 529 then one successful retry"
    );
}

// ── LAB-941: model-unsupported detection + negative-cache routing ────

/// Raw 404 with Anthropic's model-not-found envelope (`connection: close`, so
/// the body is EOF-delimited — no content-length needed).
const HEAD_404_MODEL: &str = "HTTP/1.1 404 Not Found\r\ncontent-type: application/json\r\nconnection: close\r\n\r\n{\"type\":\"error\",\"error\":{\"type\":\"not_found_error\",\"message\":\"model: claude-nope-1\"}}";

/// Wire-format detection: only genuine "this account can't serve the model"
/// errors match; other 4xx (prompt too long, bad path) must not.
#[test]
fn model_unsupported_error_detection() {
    let anthropic_404 = serde_json::json!({
        "type": "error",
        "error": {"type": "not_found_error", "message": "model: claude-nope-1"}
    });
    assert!(is_model_unsupported_error(
        StatusCode::NOT_FOUND,
        &anthropic_404
    ));
    assert!(
        !is_model_unsupported_error(StatusCode::INTERNAL_SERVER_ERROR, &anthropic_404),
        "status gate: a 5xx is never a model rejection"
    );

    let path_404 = serde_json::json!({
        "type": "error",
        "error": {"type": "not_found_error", "message": "Not Found"}
    });
    assert!(
        !is_model_unsupported_error(StatusCode::NOT_FOUND, &path_404),
        "URL-path 404 lacks the 'model:' prefix and must not match"
    );

    // LiteLLM-style gateway body, observed live from insight-gateway 2026-07-27.
    let litellm_400 = serde_json::json!({
        "error": {
            "message": "/chat/completions: Invalid model name passed in model=claude-opus-5. Call `/v1/models` to view available models for your key.",
            "type": "None", "param": "None", "code": "400"
        }
    });
    assert!(is_model_unsupported_error(
        StatusCode::BAD_REQUEST,
        &litellm_400
    ));

    let openai_code = serde_json::json!({
        "error": {"message": "The model `x` does not exist", "type": "invalid_request_error", "code": "model_not_found"}
    });
    assert!(is_model_unsupported_error(
        StatusCode::NOT_FOUND,
        &openai_code
    ));

    let too_long = serde_json::json!({
        "type": "error",
        "error": {"type": "invalid_request_error", "message": "prompt is too long: 210000 tokens > 200000 maximum"}
    });
    assert!(
        !is_model_unsupported_error(StatusCode::BAD_REQUEST, &too_long),
        "prompt-too-long 400 must not be treated as a model rejection"
    );
}

/// The negative cache removes the (endpoint, model) pair from routing — for
/// that model only, affinity or not — and an expired entry restores it.
#[tokio::test]
async fn model_unsupported_filters_routing_until_expiry() {
    let acct_a = mk_endpoint("a", "sk-ant-api-a");
    let acct_b = mk_endpoint("b", "sk-ant-api-b");
    let state = test_state_with(vec![acct_a, acct_b]);

    state.note_model_unsupported("a", 0, "claude-fable-5");

    for _ in 0..8 {
        assert_eq!(
            state.pick_endpoint(None, "claude-fable-5", &[]).await,
            Some(1),
            "noted model must never route to the rejecting endpoint"
        );
    }
    // Session affinity re-buckets too: the sticky hash only sees candidates.
    assert_eq!(
        state
            .pick_endpoint(Some("client:sess:1"), "claude-fable-5", &[])
            .await,
        Some(1),
        "an affinity-pinned session must migrate off the rejecting endpoint"
    );
    assert!(
        state
            .pick_endpoint(None, "claude-sonnet-5", &[])
            .await
            .is_some(),
        "other models on the same endpoint are unaffected"
    );

    // Force-expire the entry: the endpoint must rejoin the model's pool.
    {
        let mut map = state.unsupported_models.lock().unwrap();
        map.insert((0, "claude-fable-5".to_string()), Instant::now());
    }
    let candidates = state.routing_candidates("claude-fable-5", &[]).await;
    assert_eq!(candidates.len(), 2, "expired entry must not filter routing");
}

/// The learn map is bounded: past UNSUPPORTED_MODEL_MAX distinct pairs, new
/// learns are dropped — model strings are client-supplied input. An EXISTING
/// pair must still refresh its TTL at capacity (refresh doesn't grow the map).
#[test]
fn model_unsupported_map_is_bounded() {
    let state = test_state_with(vec![mk_endpoint("a", "sk-ant-api-a")]);
    for i in 0..(UNSUPPORTED_MODEL_MAX + 50) {
        state.note_model_unsupported("a", 0, &format!("model-{i}"));
    }
    assert_eq!(
        state.unsupported_models.lock().unwrap().len(),
        UNSUPPORTED_MODEL_MAX
    );

    // Shorten one live entry's TTL, then re-note it with the map still full:
    // the refresh must land (expiry back to ~full TTL), not be dropped.
    let key = (0usize, "model-0".to_string());
    {
        let mut map = state.unsupported_models.lock().unwrap();
        map.insert(key.clone(), Instant::now() + Duration::from_secs(1));
    }
    state.note_model_unsupported("a", 0, "model-0");
    {
        let map = state.unsupported_models.lock().unwrap();
        assert_eq!(
            map.len(),
            UNSUPPORTED_MODEL_MAX,
            "refresh must not grow the map"
        );
        let expiry = map
            .get(&key)
            .expect("existing pair must survive a refresh at capacity");
        assert!(
            *expiry > Instant::now() + Duration::from_secs(60),
            "TTL must be refreshed for an existing pair even at capacity"
        );
    }
}

/// LAB-941 native path: an account 404-rejecting the model rotates to the
/// next account within the request, and the negative cache makes the NEXT
/// request skip the rejecting account outright instead of re-pinning it via
/// session affinity.
#[tokio::test]
async fn model_unsupported_rotates_and_next_request_skips_account() {
    use std::sync::atomic::Ordering;
    let (reject_url, reject_hits) =
        spawn_status_then_ok_upstream(usize::MAX, HEAD_404_MODEL, b"{}").await;
    let (ok_url, _h) = spawn_mock_upstream().await;

    let reject = mk_endpoint_at("reject", "sk-ant-api-r", &reject_url);
    let mut healthy = mk_endpoint_at("healthy", "sk-ant-api-h", &ok_url);
    // Priority forces routing to try `reject` first — deterministic without
    // depending on affinity hashing.
    healthy.priority = 1;
    let state = test_state_with(vec![reject, healthy]);
    let addr = serve(build_router(state)).await;

    let client = reqwest::Client::new();
    for _ in 0..2 {
        let resp = client
            .post(format!("http://{addr}/v1/messages"))
            .header("content-type", "application/json")
            .body(r#"{"model":"claude-nope-1","max_tokens":1,"messages":[{"role":"user","content":"hi"}]}"#)
            .send()
            .await
            .unwrap();
        assert_eq!(
            resp.status(),
            reqwest::StatusCode::OK,
            "both requests must succeed via the account that serves the model"
        );
    }
    assert_eq!(
        reject_hits.load(Ordering::SeqCst),
        1,
        "the second request must skip the rejecting account, not retry it"
    );
}

/// When NO account serves the model (nonexistent model), the upstream's own
/// 404 must surface — not a synthetic 429 that invites the client to retry a
/// permanently-failing request. A SECOND request then hits the warm negative
/// cache (the pool empties before any forward runs, nothing is stashed) and
/// must still get a 404, synthesized, without touching the upstream again.
#[tokio::test]
async fn model_unsupported_everywhere_returns_upstream_404_not_429() {
    use std::sync::atomic::Ordering;
    let (url, hits) = spawn_status_then_ok_upstream(usize::MAX, HEAD_404_MODEL, b"{}").await;
    let state = test_state_with(vec![mk_endpoint_at("only", "sk-ant-api-o", &url)]);
    let addr = serve(build_router(state)).await;

    let client = reqwest::Client::new();
    for pass in [
        "cold cache (real upstream 404)",
        "warm cache (synthesized 404)",
    ] {
        let resp = client
            .post(format!("http://{addr}/v1/messages"))
            .header("content-type", "application/json")
            .body(r#"{"model":"claude-nope-1","max_tokens":1,"messages":[{"role":"user","content":"hi"}]}"#)
            .send()
            .await
            .unwrap();
        assert_eq!(
            resp.status(),
            reqwest::StatusCode::NOT_FOUND,
            "{pass}: model unsupported everywhere must return 404, not 429"
        );
        let body = resp.text().await.unwrap();
        assert!(
            body.contains("not_found_error") && body.contains("claude-nope-1"),
            "{pass}: error body must carry the model rejection, got: {body}"
        );
    }
    assert_eq!(
        hits.load(Ordering::SeqCst),
        1,
        "the warm-cache request must not touch the upstream at all"
    );
}

/// Same all-reject scenario through `/v1/chat/completions`: the warm-cache
/// synthesized 404 must carry the OpenAI error shape — `type` is
/// `invalid_request_error` (an OpenAI type, not Anthropic's
/// `not_found_error`) with the specific cause in `code = model_not_found`.
#[tokio::test]
async fn model_unsupported_everywhere_openai_handler_returns_openai_shaped_404() {
    use std::sync::atomic::Ordering;
    let (url, hits) = spawn_status_then_ok_upstream(usize::MAX, HEAD_404_MODEL, b"{}").await;
    let state = test_state_with(vec![mk_endpoint_at("only", "sk-ant-api-o", &url)]);
    let addr = serve(build_router(state)).await;

    let client = reqwest::Client::new();
    // Cold pass populates the cache from the upstream rejection.
    let resp = client
        .post(format!("http://{addr}/v1/chat/completions"))
        .header("content-type", "application/json")
        .body(r#"{"model":"claude-nope-1","messages":[{"role":"user","content":"hi"}]}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::NOT_FOUND);

    // Warm pass: pool empties via the cache, response is synthesized.
    let resp = client
        .post(format!("http://{addr}/v1/chat/completions"))
        .header("content-type", "application/json")
        .body(r#"{"model":"claude-nope-1","messages":[{"role":"user","content":"hi"}]}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::NOT_FOUND,
        "warm cache through the OpenAI handler must return 404, not 429"
    );
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(
        body.pointer("/error/type").and_then(|v| v.as_str()),
        Some("invalid_request_error"),
        "synthesized OpenAI envelope must use an OpenAI error type, got: {body}"
    );
    assert_eq!(
        body.pointer("/error/code").and_then(|v| v.as_str()),
        Some("model_not_found"),
        "synthesized OpenAI envelope must carry code=model_not_found, got: {body}"
    );
    assert!(
        body.pointer("/error/message")
            .and_then(|v| v.as_str())
            .is_some_and(|m| m.contains("claude-nope-1")),
        "message must name the rejected model, got: {body}"
    );
    assert_eq!(
        hits.load(Ordering::SeqCst),
        1,
        "the warm-cache request must not touch the upstream at all"
    );
}

/// LAB-941 incident shape (observed live 2026-07-27): an OpenAI-protocol
/// gateway without the requested model returns 400 "Invalid model name"; the
/// LB must rotate to an account that serves it and route the NEXT request
/// away from the gateway, instead of handing clients the misleading 400.
#[tokio::test]
async fn gateway_invalid_model_rotates_to_serving_account() {
    use std::sync::atomic::Ordering;
    const HEAD_400_LITELLM: &str = "HTTP/1.1 400 Bad Request\r\ncontent-type: application/json\r\nconnection: close\r\n\r\n{\"error\":{\"message\":\"/chat/completions: Invalid model name passed in model=claude-opus-5. Call `/v1/models` to view available models for your key.\",\"type\":\"None\",\"param\":\"None\",\"code\":\"400\"}}";
    let (gw_url, gw_hits) =
        spawn_status_then_ok_upstream(usize::MAX, HEAD_400_LITELLM, OPENAI_OK_BODY).await;
    let (ok_url, _h) = spawn_mock_upstream().await;

    let mut gw = make_endpoint("gw", Protocol::OpenAI);
    gw.base_url = gw_url;
    let mut healthy = mk_endpoint_at("healthy", "sk-ant-api-h", &ok_url);
    // Gateway tried first, deterministically.
    healthy.priority = 1;
    let state = test_state_with(vec![gw, healthy]);
    let addr = serve(build_router(state)).await;

    let client = reqwest::Client::new();
    for _ in 0..2 {
        let resp = client
            .post(format!("http://{addr}/v1/messages"))
            .header("content-type", "application/json")
            .body(r#"{"model":"claude-opus-5","max_tokens":1,"messages":[{"role":"user","content":"hi"}]}"#)
            .send()
            .await
            .unwrap();
        assert_eq!(
            resp.status(),
            reqwest::StatusCode::OK,
            "requests must succeed via the Anthropic account, not fail on the gateway 400"
        );
    }
    assert_eq!(
        gw_hits.load(Ordering::SeqCst),
        1,
        "the second request must skip the gateway for this model"
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

// ── Task 5: transport-error metric ─────────────────────────────────

/// After a transport failure, `/metrics` must expose a by-kind transport
/// error counter so a flaky egress shows on the dashboard before it becomes
/// client errors. The `{kind=...}` data line only renders once a transport
/// error has been recorded — so this proves the increment, not just a header.
#[tokio::test]
async fn metrics_expose_transport_error_counter() {
    let url = spawn_dead_upstream().await;
    let state = test_state_with(vec![mk_endpoint_at("a", "sk-ant-api-aaa", &url)]);
    let addr = serve(build_router(state)).await;
    let c = reqwest::Client::new();
    let _ = c
        .post(format!("http://{addr}/v1/messages"))
        .header("content-type", "application/json")
        .body(r#"{"model":"test","max_tokens":1,"messages":[{"role":"user","content":"hi"}]}"#)
        .send()
        .await;
    let m = c
        .get(format!("http://{addr}/metrics"))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(
        m.contains("anthropic_upstream_transport_errors_total{kind="),
        "by-kind transport-error counter must appear after a transport failure:\n{m}"
    );
}

/// AC4 (graceful without Redis): a single-instance deployment has no Redis,
/// so `flush_transport_errors` must be a no-op that leaves the local
/// accumulator intact — otherwise local `/metrics` counts would vanish.
#[tokio::test]
async fn flush_transport_errors_noop_without_redis() {
    let state = test_state_with(vec![mk_endpoint("a", "sk-ant-api-aaa")]);
    {
        let mut m = state.upstream_transport_errors.lock().unwrap();
        m.insert("timeout", 3);
        m.insert("connect", 1);
    }
    state.flush_transport_errors().await;
    let m = state.upstream_transport_errors.lock().unwrap();
    assert_eq!(
        m.get("timeout"),
        Some(&3),
        "local accumulator must be retained without redis"
    );
    assert_eq!(m.get("connect"), Some(&1));
}

/// A panicked lock-holder must not wedge the accumulator: recovery clears
/// the poison so the drain, the re-queue-on-redis-failure path, and every
/// `if let Ok` increment/metrics site keep working afterwards.
#[tokio::test]
async fn transport_errors_lock_recovers_and_clears_poison() {
    let state = test_state_with(vec![mk_endpoint("a", "sk-ant-api-aaa")]);
    // Poison the mutex by panicking while holding the guard.
    {
        let state = state.clone();
        std::thread::spawn(move || {
            let _g = state.upstream_transport_errors.lock().unwrap();
            panic!("poison the transport-error mutex");
        })
        .join()
        .unwrap_err();
    }
    assert!(state.upstream_transport_errors.is_poisoned());
    {
        let mut m = state.lock_transport_errors();
        *m.entry("timeout").or_insert(0) += 3;
    }
    assert!(
        !state.upstream_transport_errors.is_poisoned(),
        "recovery must clear the poison, not just bypass it"
    );
    // Plain `lock()` sites (increments, metrics fallback) work again.
    assert_eq!(
        state
            .upstream_transport_errors
            .lock()
            .unwrap()
            .get("timeout"),
        Some(&3)
    );
}

/// AC3 (aggregate exposed): when the sync task has cached a fleet-wide
/// aggregate, `/metrics` must report THOSE counts (cluster-wide), not this
/// replica's local unflushed delta. Proves the Redis view supersedes local.
#[tokio::test]
async fn metrics_prefer_redis_transport_error_aggregate() {
    let state = test_state_with(vec![mk_endpoint("a", "sk-ant-api-aaa")]);
    // A local (unflushed) delta that must be SUPERSEDED by the fleet view.
    state
        .upstream_transport_errors
        .lock()
        .unwrap()
        .insert("timeout", 2);
    // Simulate the sync task having cached a fleet-wide aggregate.
    *state.cluster_info_cache.lock().unwrap() = Some(serde_json::json!({
        "redis_connected": true,
        "replicas_seen": 3,
        "transport_errors": { "timeout": 40, "connect": 5 },
    }));
    let addr = serve(build_router(state)).await;
    let c = reqwest::Client::new();
    let m = c
        .get(format!("http://{addr}/metrics"))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(
        m.contains("anthropic_upstream_transport_errors_total{kind=\"timeout\"} 40"),
        "fleet aggregate (40) must win over local delta (2):\n{m}"
    );
    assert!(
        m.contains("anthropic_upstream_transport_errors_total{kind=\"connect\"} 5"),
        "fleet aggregate must include all kinds:\n{m}"
    );
    assert!(
        !m.contains("anthropic_upstream_transport_errors_total{kind=\"timeout\"} 2"),
        "local delta must not leak once the fleet aggregate is present:\n{m}"
    );
}

/// LAB-466: the `/metrics` local-fallback branch must recover a poisoned
/// `upstream_transport_errors` lock instead of reporting zero. Poisons the
/// mutex directly (not via `lock_transport_errors()`) so this proves the
/// FALLBACK itself recovers, not just the helper in isolation.
#[tokio::test]
async fn metrics_local_fallback_recovers_poisoned_lock() {
    let state = test_state_with(vec![mk_endpoint("a", "sk-ant-api-aaa")]);
    // Seed local counts, then poison the mutex from a panicking thread —
    // bypassing `lock_transport_errors()` so the poison isn't pre-cleared.
    state
        .upstream_transport_errors
        .lock()
        .unwrap()
        .insert("timeout", 40);
    {
        let state = state.clone();
        std::thread::spawn(move || {
            let _g = state.upstream_transport_errors.lock().unwrap();
            panic!("poison the transport-error mutex");
        })
        .join()
        .unwrap_err();
    }
    assert!(state.upstream_transport_errors.is_poisoned());
    // No fleet aggregate cached, so /metrics must take the local fallback path.
    assert!(state.cluster_info_cache.lock().unwrap().is_none());

    let addr = serve(build_router(state)).await;
    let c = reqwest::Client::new();
    let resp = c
        .get(format!("http://{addr}/metrics"))
        .send()
        .await
        .unwrap();
    assert!(
        resp.status().is_success(),
        "metrics endpoint must stay usable after a poisoned lock"
    );
    let m = resp.text().await.unwrap();
    assert!(
        m.contains("anthropic_upstream_transport_errors_total{kind=\"timeout\"} 40"),
        "poisoned local counts must survive the fallback, not be reported as zero:\n{m}"
    );
}

#[tokio::test]
async fn openai_chat_non_streaming() {
    // Spawn a mock that serves /v1/messages with Anthropic format
    let mock_app = Router::new().fallback(any(mock_anthropic_handler));
    let mock_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mock_addr = mock_listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(mock_listener, mock_app).await.unwrap();
    });

    let mock_url = format!("http://{}", mock_addr);
    let (app, _state) = test_openai_app(&mock_url, None);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(
            listener,
            app.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await
        .unwrap();
    });

    let client = Client::new();
    let resp = client
            .post(format!("http://{}/v1/chat/completions", addr))
            .header("content-type", "application/json")
            .body(r#"{"model":"claude-sonnet-4-6","messages":[{"role":"user","content":"Hello"}],"max_tokens":100}"#)
            .send()
            .await
            .unwrap();

    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["object"], "chat.completion");
    assert!(body["id"].as_str().unwrap().starts_with("chatcmpl-"));
    assert_eq!(
        body["choices"][0]["message"]["content"],
        "Hello from Claude"
    );
    assert_eq!(body["choices"][0]["finish_reason"], "stop");
    assert_eq!(body["usage"]["prompt_tokens"], 10);
    assert_eq!(body["usage"]["completion_tokens"], 5);
    assert_eq!(body["usage"]["total_tokens"], 15);
}

/// Mock whose reply IS a fenced code block — exercises the json_mode gate
/// end-to-end (GH #95 / LAB-711).
async fn mock_anthropic_fenced_handler(_req: Request<Body>) -> Response {
    axum::Json(serde_json::json!({
        "id": "msg_fenced",
        "type": "message",
        "content": [{"type": "text", "text": "```json\n{\"a\": 1}\n```"}],
        "model": "claude-sonnet-4-6",
        "stop_reason": "end_turn",
        "usage": {"input_tokens": 10, "output_tokens": 5}
    }))
    .into_response()
}

#[tokio::test]
async fn openai_chat_fence_strip_gated_on_response_format() {
    let mock_app = Router::new().fallback(any(mock_anthropic_fenced_handler));
    let mock_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mock_addr = mock_listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(mock_listener, mock_app).await.unwrap();
    });

    let (app, _state) = test_openai_app(&format!("http://{}", mock_addr), None);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(
            listener,
            app.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await
        .unwrap();
    });

    let client = Client::new();

    // Without response_format: fenced content passes through verbatim.
    let resp = client
        .post(format!("http://{}/v1/chat/completions", addr))
        .header("content-type", "application/json")
        .body(r#"{"model":"claude-sonnet-4-6","messages":[{"role":"user","content":"Hi"}]}"#)
        .send()
        .await
        .unwrap();
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(
        body["choices"][0]["message"]["content"], "```json\n{\"a\": 1}\n```",
        "non-JSON-mode reply must not be mutated"
    );

    // With response_format json_object: fences stripped.
    let resp = client
        .post(format!("http://{}/v1/chat/completions", addr))
        .header("content-type", "application/json")
        .body(r#"{"model":"claude-sonnet-4-6","messages":[{"role":"user","content":"Hi"}],"response_format":{"type":"json_object"}}"#)
        .send()
        .await
        .unwrap();
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(
        body["choices"][0]["message"]["content"], r#"{"a": 1}"#,
        "JSON-mode reply must have fences stripped"
    );
}

/// Streaming mock whose reply IS a fenced code block, split across three text
/// deltas so a fence spans SSE frame boundaries. Same reply as
/// `mock_anthropic_fenced_handler` for a direct stream/non-stream comparison.
async fn mock_anthropic_streaming_fenced_handler(req: Request<Body>) -> Response {
    let has_auth =
        req.headers().contains_key("x-api-key") || req.headers().contains_key("authorization");
    if !has_auth {
        return (StatusCode::UNAUTHORIZED, "missing auth").into_response();
    }
    let deltas = ["```js", "on\n{\"a\": 1}", "\n```"];
    let mut body = String::from(
        "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_fenced_stream\",\"type\":\"message\",\"role\":\"assistant\",\"model\":\"claude-sonnet-4-6\",\"content\":[],\"stop_reason\":null,\"usage\":{\"input_tokens\":10,\"output_tokens\":0}}}\n\nevent: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\n",
    );
    for d in deltas {
        let payload = serde_json::json!({
            "type": "content_block_delta",
            "index": 0,
            "delta": {"type": "text_delta", "text": d},
        });
        body.push_str(&format!("event: content_block_delta\ndata: {payload}\n\n"));
    }
    body.push_str(
        "event: content_block_stop\ndata: {\"type\":\"content_block_stop\",\"index\":0}\n\n",
    );
    body.push_str("event: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":5}}\n\n");
    body.push_str("event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n");
    Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "text/event-stream")
        .body(Body::from(body))
        .unwrap()
}

/// End-to-end: the streaming path must honour the same json_mode gate as
/// non-streaming, so a fenced reply yields identical assembled content across
/// transports (GH #95 / LAB-711 — closes the handler-plumbing gap that unit
/// tests on translate_sse_event alone cannot cover).
#[tokio::test]
async fn openai_chat_streaming_fence_strip_gated_on_response_format() {
    let mock_app = Router::new().fallback(any(mock_anthropic_streaming_fenced_handler));
    let mock_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mock_addr = mock_listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(mock_listener, mock_app).await.unwrap();
    });

    let (app, _state) = test_openai_app(&format!("http://{}", mock_addr), None);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(
            listener,
            app.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await
        .unwrap();
    });

    let client = Client::new();

    // Assemble the `content` a client would see from an SSE response body.
    fn assemble(body: &str) -> String {
        let mut content = String::new();
        for line in body.lines() {
            let Some(data) = line.strip_prefix("data: ") else {
                continue;
            };
            if data == "[DONE]" {
                continue;
            }
            if let Ok(v) = serde_json::from_str::<serde_json::Value>(data) {
                if let Some(c) = v["choices"][0]["delta"]["content"].as_str() {
                    content.push_str(c);
                }
            }
        }
        content
    }

    // stream:true without response_format → fenced content passes verbatim.
    let resp = client
        .post(format!("http://{}/v1/chat/completions", addr))
        .header("content-type", "application/json")
        .body(r#"{"model":"claude-sonnet-4-6","messages":[{"role":"user","content":"Hi"}],"stream":true}"#)
        .send()
        .await
        .unwrap();
    let body = resp.text().await.unwrap();
    assert_eq!(
        assemble(&body),
        "```json\n{\"a\": 1}\n```",
        "streaming non-JSON-mode must not mutate content"
    );

    // stream:true with response_format json_object → fences stripped, matching
    // the non-streaming json_object result exactly.
    let resp = client
        .post(format!("http://{}/v1/chat/completions", addr))
        .header("content-type", "application/json")
        .body(r#"{"model":"claude-sonnet-4-6","messages":[{"role":"user","content":"Hi"}],"stream":true,"response_format":{"type":"json_object"}}"#)
        .send()
        .await
        .unwrap();
    let body = resp.text().await.unwrap();
    assert_eq!(
        assemble(&body),
        r#"{"a": 1}"#,
        "streaming JSON-mode must strip fences, matching non-streaming"
    );
}

#[tokio::test]
async fn openai_chat_streaming() {
    let mock_app = Router::new().fallback(any(mock_anthropic_streaming_handler));
    let mock_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mock_addr = mock_listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(mock_listener, mock_app).await.unwrap();
    });

    let mock_url = format!("http://{}", mock_addr);
    let (app, state) = test_openai_app(&mock_url, None);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(
            listener,
            app.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await
        .unwrap();
    });

    let client = Client::new();
    let resp = client
            .post(format!("http://{}/v1/chat/completions", addr))
            .header("content-type", "application/json")
            .body(r#"{"model":"claude-sonnet-4-6","messages":[{"role":"user","content":"Hello"}],"max_tokens":100,"stream":true}"#)
            .send()
            .await
            .unwrap();

    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    assert_eq!(
        resp.headers().get("content-type").unwrap(),
        "text/event-stream"
    );

    let body = resp.text().await.unwrap();

    // Parse SSE events from response
    let mut chunks: Vec<serde_json::Value> = Vec::new();
    let mut got_done = false;
    for line in body.lines() {
        if line == "data: [DONE]" {
            got_done = true;
        } else if let Some(data) = line.strip_prefix("data: ") {
            if let Ok(v) = serde_json::from_str::<serde_json::Value>(data) {
                chunks.push(v);
            }
        }
    }

    assert!(got_done, "should have [DONE] sentinel");
    assert!(
        chunks.len() >= 3,
        "expected at least 3 chunks (role + content + finish), got {}",
        chunks.len()
    );

    // First chunk: role
    assert_eq!(chunks[0]["choices"][0]["delta"]["role"], "assistant");
    assert_eq!(chunks[0]["object"], "chat.completion.chunk");
    assert!(chunks[0]["id"].as_str().unwrap().starts_with("chatcmpl-"));

    // Content chunks
    let content_chunks: Vec<&str> = chunks
        .iter()
        .filter_map(|c| c["choices"][0]["delta"]["content"].as_str())
        .collect();
    assert_eq!(content_chunks.join(""), "Hello world");

    // Last data chunk: finish_reason
    let last = chunks.last().unwrap();
    assert_eq!(last["choices"][0]["finish_reason"], "stop");

    // LAB-717: usage is scanned incrementally (no raw_sse buffering) and
    // recorded by the detached finalize task after the stream closes — poll
    // briefly for it to land. The mock stream carries input=10 / output=5.
    assert_eq!(
        poll_streamed_usage(&state).await,
        (10, 5),
        "streamed usage must be recorded from the incremental scan"
    );
}

/// Poll endpoint token counters until streamed usage lands (the finalize
/// task is detached, so recording races the client seeing end-of-stream).
async fn poll_streamed_usage(state: &Arc<AppState>) -> (u64, u64) {
    let mut recorded = (0, 0);
    for _ in 0..40 {
        let input: u64 = state
            .endpoints
            .iter()
            .map(|e| e.input_tokens.load(Ordering::Relaxed))
            .sum();
        let output: u64 = state
            .endpoints
            .iter()
            .map(|e| e.output_tokens.load(Ordering::Relaxed))
            .sum();
        recorded = (input, output);
        if input > 0 && output > 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    recorded
}

/// LAB-717: native-path (/v1/messages) streaming — the passthrough stream is
/// scanned incrementally and usage recorded without buffering the response.
#[tokio::test]
async fn anthropic_streaming_records_usage() {
    let mock_app = Router::new().fallback(any(mock_anthropic_streaming_handler));
    let mock_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mock_addr = mock_listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(mock_listener, mock_app).await.unwrap();
    });

    let (app, state) = test_app(&format!("http://{}", mock_addr), None);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(
            listener,
            app.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await
        .unwrap();
    });

    let resp = Client::new()
        .post(format!("http://{}/v1/messages", addr))
        .header("content-type", "application/json")
        .body(r#"{"model":"claude-sonnet-4-6","stream":true,"messages":[{"role":"user","content":"hi"}],"max_tokens":100}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    let body = resp.text().await.unwrap();
    assert!(
        body.contains("message_stop"),
        "stream should pass through untouched, got: {body:?}"
    );

    assert_eq!(
        poll_streamed_usage(&state).await,
        (10, 5),
        "streamed usage must be recorded from the incremental scan"
    );
}

#[tokio::test]
async fn openai_chat_rejects_missing_auth() {
    let mock_app = Router::new().fallback(any(mock_anthropic_handler));
    let mock_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mock_addr = mock_listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(mock_listener, mock_app).await.unwrap();
    });

    let mock_url = format!("http://{}", mock_addr);
    let (app, _state) = test_openai_app(&mock_url, Some("secret-key".to_string()));

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(
            listener,
            app.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await
        .unwrap();
    });

    let client = Client::new();
    let resp = client
        .post(format!("http://{}/v1/chat/completions", addr))
        .header("content-type", "application/json")
        .body(r#"{"model":"test","messages":[{"role":"user","content":"hi"}],"max_tokens":1}"#)
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), reqwest::StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn openai_chat_accepts_bearer_capitalized() {
    let mock_app = Router::new().fallback(any(mock_anthropic_handler));
    let mock_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mock_addr = mock_listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(mock_listener, mock_app).await.unwrap();
    });

    let mock_url = format!("http://{}", mock_addr);
    let (app, _state) = test_openai_app(&mock_url, Some("secret-key".to_string()));

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(
            listener,
            app.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await
        .unwrap();
    });

    let client = Client::new();
    let resp = client
        .post(format!("http://{}/v1/chat/completions", addr))
        .header("content-type", "application/json")
        .header("authorization", "Bearer secret-key")
        .body(r#"{"model":"test","messages":[{"role":"user","content":"hi"}],"max_tokens":1}"#)
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), reqwest::StatusCode::OK);
}

#[tokio::test]
async fn openai_chat_accepts_bearer_lowercase() {
    let mock_app = Router::new().fallback(any(mock_anthropic_handler));
    let mock_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mock_addr = mock_listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(mock_listener, mock_app).await.unwrap();
    });

    let mock_url = format!("http://{}", mock_addr);
    let (app, _state) = test_openai_app(&mock_url, Some("secret-key".to_string()));

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(
            listener,
            app.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await
        .unwrap();
    });

    let client = Client::new();
    let resp = client
        .post(format!("http://{}/v1/chat/completions", addr))
        .header("content-type", "application/json")
        .header("authorization", "bearer secret-key")
        .body(r#"{"model":"test","messages":[{"role":"user","content":"hi"}],"max_tokens":1}"#)
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), reqwest::StatusCode::OK);
}

#[tokio::test]
async fn openai_chat_wrong_apikey_valid_bearer() {
    let mock_app = Router::new().fallback(any(mock_anthropic_handler));
    let mock_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mock_addr = mock_listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(mock_listener, mock_app).await.unwrap();
    });

    let mock_url = format!("http://{}", mock_addr);
    let (app, _state) = test_openai_app(&mock_url, Some("secret-key".to_string()));

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(
            listener,
            app.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await
        .unwrap();
    });

    let client = Client::new();
    let resp = client
        .post(format!("http://{}/v1/chat/completions", addr))
        .header("content-type", "application/json")
        .header("x-api-key", "wrong-key")
        .header("authorization", "Bearer secret-key")
        .body(r#"{"model":"test","messages":[{"role":"user","content":"hi"}],"max_tokens":1}"#)
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), reqwest::StatusCode::OK);
}

// ── Unit: auto-cache injection ─────────────────────────────────

#[test]
fn inject_cache_no_existing() {
    let mut body = serde_json::json!({
        "model": "claude-sonnet-4-6",
        "max_tokens": 1024,
        "system": "You are a helpful assistant.",
        "tools": [
            {"name": "get_weather", "description": "Gets weather", "input_schema": {"type": "object"}},
            {"name": "search", "description": "Searches", "input_schema": {"type": "object"}}
        ],
        "messages": [
            {"role": "user", "content": "Hello"},
            {"role": "assistant", "content": "Hi there!"},
            {"role": "user", "content": "What's the weather?"}
        ]
    });

    let inj = inject_cache_breakpoints(&mut body);
    assert!(!inj.skipped);
    assert!(inj.tools);
    assert!(inj.system);
    assert!(inj.messages);

    // Last tool should have cache_control
    let tools = body["tools"].as_array().unwrap();
    assert!(tools[0].get("cache_control").is_none());
    assert_eq!(tools[1]["cache_control"]["type"], "ephemeral");

    // System should be converted to array with cache_control
    let system = body["system"].as_array().unwrap();
    assert_eq!(system.len(), 1);
    assert_eq!(system[0]["text"], "You are a helpful assistant.");
    assert_eq!(system[0]["cache_control"]["type"], "ephemeral");

    // Last user message content should be converted to array
    let msgs = body["messages"].as_array().unwrap();
    let last_user = &msgs[2];
    let content = last_user["content"].as_array().unwrap();
    assert_eq!(content[0]["text"], "What's the weather?");
    assert_eq!(content[0]["cache_control"]["type"], "ephemeral");

    // First user message should be untouched
    assert_eq!(msgs[0]["content"], "Hello");
}

#[test]
fn inject_cache_system_array() {
    let mut body = serde_json::json!({
        "model": "claude-sonnet-4-6",
        "system": [
            {"type": "text", "text": "System prompt part 1"},
            {"type": "text", "text": "System prompt part 2"}
        ],
        "messages": [
            {"role": "user", "content": "Hello"}
        ]
    });

    let inj = inject_cache_breakpoints(&mut body);
    assert!(inj.system);

    let system = body["system"].as_array().unwrap();
    assert!(system[0].get("cache_control").is_none());
    assert_eq!(system[1]["cache_control"]["type"], "ephemeral");
}

#[test]
fn inject_cache_already_present() {
    let mut body = serde_json::json!({
        "model": "claude-sonnet-4-6",
        "system": [
            {"type": "text", "text": "Cached system", "cache_control": {"type": "ephemeral"}}
        ],
        "messages": [
            {"role": "user", "content": "Hello"}
        ]
    });

    let inj = inject_cache_breakpoints(&mut body);
    assert!(inj.skipped);
    assert!(!inj.tools);
    assert!(!inj.system);
    assert!(!inj.messages);

    // Verify nothing was modified — messages content is still a string
    assert_eq!(body["messages"][0]["content"], "Hello");
}

#[test]
fn inject_cache_already_present_in_tools() {
    let mut body = serde_json::json!({
        "model": "claude-sonnet-4-6",
        "tools": [
            {"name": "t1", "cache_control": {"type": "ephemeral"}}
        ],
        "messages": [
            {"role": "user", "content": "Hello"}
        ]
    });

    let inj = inject_cache_breakpoints(&mut body);
    assert!(inj.skipped);
}

#[test]
fn inject_cache_already_present_in_message_content() {
    let mut body = serde_json::json!({
        "model": "claude-sonnet-4-6",
        "messages": [
            {"role": "user", "content": [
                {"type": "text", "text": "hi", "cache_control": {"type": "ephemeral"}}
            ]}
        ]
    });

    let inj = inject_cache_breakpoints(&mut body);
    assert!(inj.skipped);
}

#[test]
fn inject_cache_empty_body() {
    let mut body = serde_json::json!({});
    let inj = inject_cache_breakpoints(&mut body);
    assert!(!inj.skipped);
    assert!(!inj.tools);
    assert!(!inj.system);
    assert!(!inj.messages);
}

#[test]
fn inject_cache_messages_string_content() {
    let mut body = serde_json::json!({
        "messages": [
            {"role": "assistant", "content": "I'm an assistant"},
            {"role": "user", "content": "Tell me a joke"}
        ]
    });

    let inj = inject_cache_breakpoints(&mut body);
    assert!(inj.messages);
    assert!(!inj.tools);
    assert!(!inj.system);

    let content = body["messages"][1]["content"].as_array().unwrap();
    assert_eq!(content[0]["type"], "text");
    assert_eq!(content[0]["text"], "Tell me a joke");
    assert_eq!(content[0]["cache_control"]["type"], "ephemeral");

    // Assistant message should be untouched
    assert_eq!(body["messages"][0]["content"], "I'm an assistant");
}

#[test]
fn inject_cache_user_message_array_content() {
    let mut body = serde_json::json!({
        "messages": [
            {"role": "user", "content": [
                {"type": "text", "text": "First part"},
                {"type": "text", "text": "Second part"}
            ]}
        ]
    });

    let inj = inject_cache_breakpoints(&mut body);
    assert!(inj.messages);

    let content = body["messages"][0]["content"].as_array().unwrap();
    assert!(content[0].get("cache_control").is_none());
    assert_eq!(content[1]["cache_control"]["type"], "ephemeral");
}

#[test]
fn inject_cache_no_user_messages() {
    let mut body = serde_json::json!({
        "messages": [
            {"role": "assistant", "content": "Hi"}
        ]
    });

    let inj = inject_cache_breakpoints(&mut body);
    assert!(!inj.messages);
}

// ── Unit: token usage extraction ───────────────────────────────

#[test]
fn usage_from_non_streaming_response() {
    let body = serde_json::json!({
        "type": "message",
        "usage": {
            "input_tokens": 100,
            "output_tokens": 50,
            "cache_creation_input_tokens": 20,
            "cache_read_input_tokens": 30,
        }
    });
    let usage = TokenUsage::from_response_body(&body);
    assert_eq!(usage.input_tokens, 100);
    assert_eq!(usage.output_tokens, 50);
    assert_eq!(usage.cache_creation_input_tokens, 20);
    assert_eq!(usage.cache_read_input_tokens, 30);
}

#[test]
fn usage_from_response_no_usage_field() {
    let body = serde_json::json!({"type": "error"});
    let usage = TokenUsage::from_response_body(&body);
    assert!(usage.is_empty());
}

#[test]
fn usage_from_sse_stream() {
    let sse_text = "\
event: message_start\n\
data: {\"type\":\"message_start\",\"message\":{\"usage\":{\"input_tokens\":150,\"cache_creation_input_tokens\":10,\"cache_read_input_tokens\":5}}}\n\
\n\
event: content_block_delta\n\
data: {\"type\":\"content_block_delta\",\"delta\":{\"text\":\"Hello\"}}\n\
\n\
event: message_delta\n\
data: {\"type\":\"message_delta\",\"usage\":{\"output_tokens\":75}}\n\
\n\
event: message_stop\n\
data: {\"type\":\"message_stop\"}\n\n";

    let usage = TokenUsage::from_sse_text(sse_text);
    assert_eq!(usage.input_tokens, 150);
    assert_eq!(usage.output_tokens, 75);
    assert_eq!(usage.cache_creation_input_tokens, 10);
    assert_eq!(usage.cache_read_input_tokens, 5);
}

#[test]
fn usage_from_empty_sse() {
    let usage = TokenUsage::from_sse_text("");
    assert!(usage.is_empty());
}

/// LAB-717: the incremental scanner must produce the same usage regardless of
/// how the stream is fragmented into chunks — including splits mid-line,
/// mid-JSON, and down to one byte per push.
#[test]
fn sse_scanner_chunk_boundaries_do_not_affect_usage() {
    let sse: &[u8] = b"event: message_start\n\
data: {\"type\":\"message_start\",\"message\":{\"usage\":{\"input_tokens\":150,\"cache_creation_input_tokens\":10,\"cache_read_input_tokens\":5}}}\n\
\n\
event: content_block_delta\n\
data: {\"type\":\"content_block_delta\",\"delta\":{\"text\":\"mentions message_delta harmlessly\"}}\n\
\n\
event: message_delta\n\
data: {\"type\":\"message_delta\",\"usage\":{\"output_tokens\":75}}\n\
\n\
event: message_stop\n\
data: {\"type\":\"message_stop\"}\n\n";

    // Every chunk size from pathological (1 byte) to whole-stream.
    for chunk_size in [1, 3, 7, 64, sse.len()] {
        let mut scanner = SseUsageScanner::default();
        for chunk in sse.chunks(chunk_size) {
            scanner.push(chunk);
        }
        scanner.finish();
        assert_eq!(scanner.usage.input_tokens, 150, "chunk_size={chunk_size}");
        assert_eq!(scanner.usage.output_tokens, 75, "chunk_size={chunk_size}");
        assert_eq!(scanner.usage.cache_creation_input_tokens, 10);
        assert_eq!(scanner.usage.cache_read_input_tokens, 5);
        assert_eq!(scanner.bytes_seen, sse.len());
        assert_eq!(scanner.event_count, 4);
    }
}

/// LAB-717: the trailing line is scanned even when the stream ends without a
/// final newline (finish() flushes the carry).
#[test]
fn sse_scanner_flushes_unterminated_final_line() {
    let mut scanner = SseUsageScanner::default();
    scanner.push(b"data: {\"type\":\"message_delta\",\"usage\":{\"output_tokens\":42}}");
    scanner.finish();
    assert_eq!(scanner.usage.output_tokens, 42);
}

/// LAB-717: a single line larger than SSE_SCAN_MAX_LINE must not grow the
/// carry without bound — it is discarded up to its newline, and scanning
/// resumes cleanly on the following lines.
#[test]
fn sse_scanner_discards_oversized_line_and_recovers() {
    let mut scanner = SseUsageScanner::default();
    // Oversized junk line delivered across several pushes, no newline yet.
    let junk = vec![b'x'; SSE_SCAN_MAX_LINE / 2 + 1];
    scanner.push(&junk);
    scanner.push(&junk); // crosses the cap → carry dropped, skip mode
    assert!(
        scanner.carry.is_empty(),
        "carry must not hold oversized line"
    );
    scanner.push(b"more of the same line\n"); // newline ends the skipped line
    scanner.push(b"data: {\"type\":\"message_delta\",\"usage\":{\"output_tokens\":9}}\n");
    scanner.finish();
    assert_eq!(scanner.usage.output_tokens, 9);
}

/// LAB-717: the cap applies uniformly when the oversized line terminates
/// within a single push — both when it completes a cross-chunk carry and
/// when it arrives whole — and the carry allocation must not retain the
/// oversized capacity afterwards.
#[test]
fn sse_scanner_caps_oversized_line_completed_in_one_push() {
    // Whole oversized line (with newline) in one push.
    let mut scanner = SseUsageScanner::default();
    let mut blob = vec![b'x'; SSE_SCAN_MAX_LINE + 1];
    blob.push(b'\n');
    scanner.push(&blob);
    scanner.push(b"data: {\"type\":\"message_delta\",\"usage\":{\"output_tokens\":7}}\n");
    scanner.finish();
    assert_eq!(scanner.usage.output_tokens, 7);
    assert!(scanner.carry.capacity() <= SSE_SCAN_MAX_LINE);

    // Carry just under the cap, then a chunk whose newline completes the
    // combined oversized line.
    let mut scanner = SseUsageScanner::default();
    scanner.push(&vec![b'x'; SSE_SCAN_MAX_LINE]); // fills carry to the cap
    scanner.push(b"y\ndata: {\"type\":\"message_delta\",\"usage\":{\"output_tokens\":8}}\n");
    scanner.finish();
    assert_eq!(scanner.usage.output_tokens, 8);
    assert!(
        scanner.carry.capacity() <= SSE_SCAN_MAX_LINE,
        "oversized merge must not balloon the retained carry allocation"
    );
}

/// LAB-717: diagnostic metadata for stream_end_no_usage is bounded — full
/// event count, but only the first five event types retained.
#[test]
fn sse_scanner_event_preview_bounded_to_five() {
    let mut scanner = SseUsageScanner::default();
    for i in 0..7 {
        scanner.push(format!("event: ev{i}\ndata: {{}}\n\n").as_bytes());
    }
    scanner.finish();
    assert_eq!(scanner.event_count, 7);
    assert_eq!(
        scanner.event_preview,
        vec!["ev0", "ev1", "ev2", "ev3", "ev4"]
    );
    assert!(scanner.usage.is_empty());
}

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
        .record_usage(&state.endpoints[0], "test-client", &usage)
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
    state.record_usage(&state.endpoints[0], "-", &usage).await;

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
            .record_usage(&state.endpoints[0], &format!("c{i}"), &usage)
            .await;
    }
    let n = state.client_usage.lock().unwrap().len();
    assert!(
        n <= 10_000,
        "client_usage must be bounded against unbounded x-client-id values, got {n}"
    );
}

// ── Unit: model-based routing ──────────────────────────────────

#[test]
fn account_serves_model_no_filter() {
    let acct = mk_endpoint("a", "sk-ant-api-x");
    assert!(acct.serves_model("claude-opus-4-6"));
    assert!(acct.serves_model("claude-haiku-4-5"));
    assert!(acct.serves_model(""));
}

#[test]
fn account_serves_model_exact_match() {
    let mut acct = mk_endpoint("a", "sk-ant-api-x");
    acct.models = vec!["claude-sonnet-4-6".to_string()];
    assert!(acct.serves_model("claude-sonnet-4-6"));
    assert!(!acct.serves_model("claude-opus-4-6"));
}

#[test]
fn account_serves_model_prefix_match() {
    let mut acct = mk_endpoint("a", "sk-ant-api-x");
    acct.models = vec!["claude-opus-*".to_string(), "claude-sonnet-*".to_string()];
    assert!(acct.serves_model("claude-opus-4-6"));
    assert!(acct.serves_model("claude-sonnet-4-6"));
    assert!(!acct.serves_model("claude-haiku-4-5"));
}

#[tokio::test]
async fn pick_account_filters_by_model() {
    let mut acct_a = mk_endpoint("opus-only", "sk-ant-api-a");
    acct_a.models = vec!["claude-opus-*".to_string()];

    let acct_b = mk_endpoint("any-model", "sk-ant-api-b");

    let state = test_state_with(vec![acct_a, acct_b]);

    // Requesting opus: both accounts eligible
    let idx = state
        .pick_endpoint(None, "claude-opus-4-6", &[])
        .await
        .unwrap();
    assert!(idx == 0 || idx == 1);

    // Requesting haiku: only acct_b eligible
    let idx = state
        .pick_endpoint(None, "claude-haiku-4-5", &[])
        .await
        .unwrap();
    assert_eq!(idx, 1);
}

#[tokio::test]
async fn soft_limit_excludes_overloaded_accounts() {
    let acct_a = mk_endpoint("healthy", "sk-ant-api-a");
    let acct_b = mk_endpoint("overloaded", "sk-ant-api-b");

    let accounts = vec![acct_a, acct_b];

    // Set utilizations before building state
    let now = AppState::now_epoch();
    {
        let mut info = accounts[0].rate_info.write().await;
        info.utilization = Some(0.30);
        info.utilization_5h = Some(0.30);
        info.reset_5h = Some(now + 10000);
    }
    {
        let mut info = accounts[1].rate_info.write().await;
        info.utilization = Some(0.95);
        info.utilization_5h = Some(0.95);
        info.reset_5h = Some(now + 10000);
    }

    let state = Arc::new(AppState {
        endpoints: accounts,
        soft_limit: 0.90,
        ..test_state_base()
    });

    // Try many affinity keys — all should route to healthy (idx 0)
    for i in 0..20 {
        let key = format!("client-{}", i);
        let idx = state.pick_endpoint(Some(&key), "any", &[]).await.unwrap();
        assert_eq!(
            idx, 0,
            "client '{}' routed to overloaded account despite soft limit",
            key
        );
    }
}

#[tokio::test]
async fn routing_candidates_ignore_unmatched_7d_state() {
    let state = test_state_with(vec![mk_endpoint("acct-a", "sk-ant-api-a")]);
    let now = AppState::now_epoch();

    {
        let mut info = state.endpoints[0].rate_info.write().await;
        info.utilization = Some(0.20);
        info.utilization_5h = Some(0.20);
        info.reset_5h = Some(now + 10000);
        info.claims_7d.insert(
            "seven_day_haiku".to_string(),
            ClaimWindowData {
                utilization: Some(0.95),
                reset: Some(now + 100000),
                status: Some("throttled".to_string()),
                ..Default::default()
            },
        );
        // Derived aggregate 7d fields reflect the unrelated claim, but routing
        // for opus must ignore them when no applicable 7d claim exists.
        info.utilization_7d = Some(0.95);
        info.reset_7d = Some(now + 100000);
        info.status_7d = Some("throttled".to_string());
    }

    let candidates = state.routing_candidates("claude-opus-4-6", &[]).await;
    assert_eq!(candidates.len(), 1, "expected one routing candidate");

    let candidate = &candidates[0];
    assert_eq!(candidate.source, "headroom_only");
    assert!(
        (candidate.gate_7d - 0.0).abs() < f64::EPSILON,
        "unmatched 7d state should not leak into gate_7d: {:?}",
        candidate
    );
    assert!(
        (candidate.weight - 0.8).abs() < 0.0001,
        "weight should remain 5h headroom-only when no 7d claim applies: {:?}",
        candidate
    );
}

// ── Unit: unified endpoint participation in routing ────────────

#[tokio::test]
async fn openai_endpoint_participates_at_configured_priority() {
    let acct = mk_endpoint("anthropic", "sk-ant-api-a");
    {
        let mut info = acct.rate_info.write().await;
        info.utilization = Some(0.0); // healthy
    }
    let mut state = test_state_with(vec![acct]);
    let st = Arc::get_mut(&mut state).unwrap();
    let mut ep = make_endpoint("openai", Protocol::OpenAI);
    ep.priority = 100;
    st.endpoints.push(ep);

    let candidates = state.routing_candidates("claude-opus-4-7", &[]).await;
    let openai_candidate = candidates.iter().find(|c| c.source == "openai");
    assert!(
        openai_candidate.is_some(),
        "openai endpoint must be a candidate"
    );
    let c = openai_candidate.unwrap();
    assert_eq!(c.endpoint, 1, "openai endpoint is at index 1");
    assert_eq!(c.priority, 100);
    assert_eq!(c.weight, 1.0);
    assert_eq!(c.gate, 0.0);
}

#[tokio::test]
async fn openai_endpoint_with_opus_only_allowlist_excludes_sonnet() {
    let mut state = test_state_with(vec![]);
    let st = Arc::get_mut(&mut state).unwrap();
    let mut ep = make_endpoint("opus-gw", Protocol::OpenAI);
    ep.models = vec!["claude-opus-*".to_string()];
    st.endpoints.push(ep);

    let cs_opus = state.routing_candidates("claude-opus-4-7", &[]).await;
    let cs_sonnet = state.routing_candidates("claude-sonnet-4-6", &[]).await;
    assert_eq!(cs_opus.len(), 1, "opus must hit the opus-only endpoint");
    assert_eq!(cs_sonnet.len(), 0, "sonnet must be filtered out");
}

// ── Unit: per-client budget ────────────────────────────────────

#[tokio::test]
async fn budget_check_no_limit_configured() {
    let state = test_state_with(vec![mk_endpoint("a", "sk-ant-api-x")]);
    assert!(state.check_budget("any-client").await.is_ok());
}

#[tokio::test]
async fn budget_check_within_limit() {
    let mut budgets = HashMap::new();
    budgets.insert("client-a".to_string(), 1000u64);
    let state = Arc::new(AppState {
        endpoints: vec![mk_endpoint("a", "sk-ant-api-x")],
        client_budgets: budgets,
        ..test_state_base()
    });

    // Within budget
    assert!(state.check_budget("client-a").await.is_ok());

    // Record some usage
    state.record_budget_usage("client-a", 500).await;
    assert!(state.check_budget("client-a").await.is_ok());

    // Exceed budget
    state.record_budget_usage("client-a", 600).await;
    assert!(state.check_budget("client-a").await.is_err());

    // Unknown client has no budget, always ok
    assert!(state.check_budget("unknown").await.is_ok());
}

// ── Integration: 5xx retry ─────────────────────────────────────

#[tokio::test]
async fn proxy_retries_on_server_error() {
    // Spawn a mock that returns 500 on first request, 200 on second
    let call_count = Arc::new(AtomicU64::new(0));
    let count_clone = call_count.clone();

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mock_addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let (mut stream, _) = listener.accept().await.unwrap();
            let count = count_clone.fetch_add(1, Ordering::Relaxed);
            let response = if count == 0 {
                "HTTP/1.1 500 Internal Server Error\r\ncontent-length: 14\r\n\r\nserver error!!"
            } else {
                "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: 15\r\n\r\n{\"id\":\"test-1\"}"
            };
            use tokio::io::AsyncReadExt;
            use tokio::io::AsyncWriteExt;
            let mut buf = vec![0u8; 4096];
            let _ = stream.read(&mut buf).await;
            let _ = stream.write_all(response.as_bytes()).await;
        }
    });

    let upstream_url = format!("http://{}", mock_addr);
    let (app, _state) = test_app(&upstream_url, None);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let app_addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(
            listener,
            app.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await
        .unwrap();
    });

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("http://{}/v1/messages", app_addr))
        .header("content-type", "application/json")
        .header("x-api-key", "any")
        .body(r#"{"model":"claude-sonnet-4-6","messages":[{"role":"user","content":"hi"}]}"#)
        .send()
        .await
        .unwrap();

    // The first attempt hits 500, second attempt should succeed with 200
    assert_eq!(resp.status(), 200);
    // Two calls to upstream (500 + 200)
    assert_eq!(call_count.load(Ordering::Relaxed), 2);
}

// ── time_adjusted_utilization unit tests ────────────────────────

#[test]
fn time_adjust_no_reset() {
    // No reset timestamp → raw util returned unchanged
    let result = time_adjusted_utilization(Some(0.80), None, None, NEAR_RESET_5H_SECS, 1000000);
    assert_eq!(result, Some(0.80));
}

#[test]
fn time_adjust_outside_threshold() {
    // 5h reset in 2 hours = 7200s, threshold is 3600s → no discount
    let now = 1000000u64;
    let reset = now + 7200;
    let result = time_adjusted_utilization(Some(0.90), Some(reset), None, NEAR_RESET_5H_SECS, now);
    assert_eq!(result, Some(0.90));
}

#[test]
fn time_adjust_inside_threshold() {
    // 5h reset in 30 min = 1800s, threshold 3600s → discount = 1800/3600 = 0.50
    let now = 1000000u64;
    let reset = now + 1800;
    let result = time_adjusted_utilization(Some(0.90), Some(reset), None, NEAR_RESET_5H_SECS, now);
    let expected = 0.90 * 0.50;
    assert!((result.unwrap() - expected).abs() < 1e-10);
}

#[test]
fn time_adjust_at_threshold_boundary() {
    // Reset exactly at threshold boundary (1h = 3600s) → discount = 3600/3600 = 1.0
    let now = 1000000u64;
    let reset = now + 3600;
    let result = time_adjusted_utilization(Some(0.90), Some(reset), None, NEAR_RESET_5H_SECS, now);
    assert_eq!(result, Some(0.90));
}

#[test]
fn time_adjust_near_reset_floor() {
    // Reset in 1 minute = 60s → discount = max(60/3600, 0.05) = 0.05 (floor)
    let now = 1000000u64;
    let reset = now + 60;
    let raw = 60.0 / 3600.0; // 0.0167, below TIME_FRACTION_FLOOR
    assert!(raw < TIME_FRACTION_FLOOR);
    let result = time_adjusted_utilization(Some(0.95), Some(reset), None, NEAR_RESET_5H_SECS, now);
    let expected = 0.95 * TIME_FRACTION_FLOOR;
    assert!((result.unwrap() - expected).abs() < 1e-10);
}

#[test]
fn time_adjust_past_reset() {
    // Reset already happened → None (stale data)
    let now = 1000000u64;
    let reset = now - 100;
    let result = time_adjusted_utilization(Some(0.90), Some(reset), None, NEAR_RESET_5H_SECS, now);
    assert_eq!(result, None);
}

#[test]
fn time_adjust_throttled() {
    // Status=throttled overrides low util → floor at 0.98
    let now = 1000000u64;
    let reset = now + 7200;
    let result = time_adjusted_utilization(
        Some(0.30),
        Some(reset),
        Some("throttled"),
        NEAR_RESET_5H_SECS,
        now,
    );
    assert_eq!(result, Some(THROTTLE_UTIL_FLOOR));
}

#[test]
fn time_adjust_warning() {
    // Status=allowed_warning overrides low util → floor at 0.80
    let now = 1000000u64;
    let reset = now + 7200;
    let result = time_adjusted_utilization(
        Some(0.50),
        Some(reset),
        Some("allowed_warning"),
        NEAR_RESET_5H_SECS,
        now,
    );
    assert_eq!(result, Some(WARNING_UTIL_FLOOR));
}

#[test]
fn time_adjust_warning_already_higher() {
    // Util already above warning floor → util wins
    let now = 1000000u64;
    let reset = now + 7200;
    let result = time_adjusted_utilization(
        Some(0.90),
        Some(reset),
        Some("allowed_warning"),
        NEAR_RESET_5H_SECS,
        now,
    );
    assert_eq!(result, Some(0.90));
}

#[test]
fn time_adjust_none_util() {
    // No utilization data → None
    let result = time_adjusted_utilization(None, Some(1000000), None, NEAR_RESET_5H_SECS, 999000);
    assert_eq!(result, None);
}

#[test]
fn time_adjust_7d_window() {
    // 7d reset in 3 hours = 10800s, threshold 21600s → discount = 10800/21600 = 0.50
    let now = 1000000u64;
    let reset = now + 10800;
    let result = time_adjusted_utilization(Some(0.80), Some(reset), None, NEAR_RESET_7D_SECS, now);
    let expected = 0.80 * 0.50;
    assert!((result.unwrap() - expected).abs() < 1e-10);
}

#[test]
fn time_adjust_throttled_overrides_discount() {
    // Near reset AND throttled → discount applies but floor wins
    let now = 1000000u64;
    let reset = now + 600;
    let result = time_adjusted_utilization(
        Some(0.50),
        Some(reset),
        Some("throttled"),
        NEAR_RESET_5H_SECS,
        now,
    );
    // Discounted: 0.50 * (600/3600) = 0.083, but throttle floor = 0.98
    assert_eq!(result, Some(THROTTLE_UTIL_FLOOR));
}

#[test]
fn time_adjust_status_without_reset() {
    // Status present but no reset → floor applied to raw util
    let result = time_adjusted_utilization(
        Some(0.30),
        None,
        Some("throttled"),
        NEAR_RESET_5H_SECS,
        1000000,
    );
    assert_eq!(result, Some(THROTTLE_UTIL_FLOOR));
}

#[test]
fn time_adjust_rejected() {
    // Status=rejected → floor at 1.0 (fully exhausted, zero bucket share)
    let now = 1000000u64;
    let reset = now + 7200;
    let result = time_adjusted_utilization(
        Some(0.30),
        Some(reset),
        Some("rejected"),
        NEAR_RESET_5H_SECS,
        now,
    );
    assert_eq!(result, Some(REJECTED_UTIL_FLOOR));
}

#[test]
fn time_adjust_rejected_near_reset() {
    // Even near reset, rejected still maps to 1.0 — API is actively refusing
    let now = 1000000u64;
    let reset = now + 60; // 1 minute from reset
    let result = time_adjusted_utilization(
        Some(0.95),
        Some(reset),
        Some("rejected"),
        NEAR_RESET_5H_SECS,
        now,
    );
    assert_eq!(result, Some(REJECTED_UTIL_FLOOR));
}

#[test]
fn time_adjust_unknown_status_gets_warning_floor() {
    // Unknown non-"allowed" status → defensive WARNING_UTIL_FLOOR (Bug #4)
    let now = 1000000u64;
    let reset = now + 7200;
    let result = time_adjusted_utilization(
        Some(0.30),
        Some(reset),
        Some("some_future_status"),
        NEAR_RESET_5H_SECS,
        now,
    );
    assert_eq!(result, Some(WARNING_UTIL_FLOOR));
}

#[test]
fn time_adjust_allowed_status_no_floor() {
    // Explicit "allowed" status → no floor, just raw util
    let now = 1000000u64;
    let reset = now + 7200;
    let result = time_adjusted_utilization(
        Some(0.30),
        Some(reset),
        Some("allowed"),
        NEAR_RESET_5H_SECS,
        now,
    );
    assert_eq!(result, Some(0.30));
}

#[tokio::test]
async fn status_clears_when_header_absent() {
    // Bug #1: stale status should clear when API sends utilization but no status header
    let accounts = vec![mk_endpoint("acct-a", "sk-ant-api-test-aaa")];
    let state = test_state_with(accounts);

    // First response: set throttled status
    let mut headers1 = reqwest::header::HeaderMap::new();
    headers1.insert(
        "anthropic-ratelimit-unified-5h-utilization",
        HeaderValue::from_static("0.30"),
    );
    headers1.insert(
        "anthropic-ratelimit-unified-5h-status",
        HeaderValue::from_static("throttled"),
    );
    headers1.insert(
        "anthropic-ratelimit-unified-5h-reset",
        HeaderValue::from_static("9999999999"),
    );
    state.update_rate_info(0, &headers1).await;
    {
        let info = state.endpoints[0].rate_info.read().await;
        assert_eq!(info.status_5h.as_deref(), Some("throttled"));
    }

    // Second response: utilization header present, NO status header → clears
    let mut headers2 = reqwest::header::HeaderMap::new();
    headers2.insert(
        "anthropic-ratelimit-unified-5h-utilization",
        HeaderValue::from_static("0.25"),
    );
    headers2.insert(
        "anthropic-ratelimit-unified-5h-reset",
        HeaderValue::from_static("9999999999"),
    );
    state.update_rate_info(0, &headers2).await;
    {
        let info = state.endpoints[0].rate_info.read().await;
        assert_eq!(
            info.status_5h, None,
            "status should clear when header absent"
        );
    }
}

#[tokio::test]
async fn status_persists_when_no_util_header() {
    // If neither util nor status header is present for a window, don't clear status
    // (the response might be for a different window entirely)
    let accounts = vec![mk_endpoint("acct-a", "sk-ant-api-test-aaa")];
    let state = test_state_with(accounts);

    // Set throttled status
    let mut headers1 = reqwest::header::HeaderMap::new();
    headers1.insert(
        "anthropic-ratelimit-unified-5h-utilization",
        HeaderValue::from_static("0.30"),
    );
    headers1.insert(
        "anthropic-ratelimit-unified-5h-status",
        HeaderValue::from_static("throttled"),
    );
    state.update_rate_info(0, &headers1).await;

    // Response with no 5h headers at all (maybe only 7d headers)
    let headers2 = reqwest::header::HeaderMap::new();
    state.update_rate_info(0, &headers2).await;
    {
        let info = state.endpoints[0].rate_info.read().await;
        assert_eq!(
            info.status_5h.as_deref(),
            Some("throttled"),
            "status should persist when no util header for that window"
        );
    }
}

#[tokio::test]
async fn pick_returns_none_when_all_rejected() {
    // Bug #5: all-rejected accounts should return None (zero total headroom)
    let now_epoch = AppState::now_epoch();
    let state = test_state_with(vec![
        mk_endpoint("acct-a", "sk-ant-api-test-aaa"),
        mk_endpoint("acct-b", "sk-ant-api-test-bbb"),
    ]);
    for acct in &state.endpoints {
        let mut info = acct.rate_info.write().await;
        info.utilization_5h = Some(0.50);
        info.utilization_7d = Some(0.30);
        info.utilization = Some(0.50);
        info.status_5h = Some("rejected".to_string());
        info.reset_5h = Some(now_epoch + 7200);
        info.reset_7d = Some(now_epoch + 86400);
    }
    let result = state.pick_endpoint(None, "", &[]).await;
    assert_eq!(result, None, "all-rejected should return None");
}

#[tokio::test]
async fn reset_sanity_rejects_far_future() {
    // Bug #6: reset timestamp > block duration from now should be rejected
    let accounts = vec![mk_endpoint("acct-a", "sk-ant-api-test-aaa")];
    let state = test_state_with(accounts);

    let mut headers = reqwest::header::HeaderMap::new();
    // 5h window: reset 10h from now (> 5h max) → should NOT be stored
    headers.insert(
        "anthropic-ratelimit-unified-5h-reset",
        HeaderValue::from_static("9999999999"),
    );
    // 7d window: reset 30d from now (> 7d max) → should NOT be stored
    headers.insert(
        "anthropic-ratelimit-unified-7d-reset",
        HeaderValue::from_static("9999999999"),
    );
    state.update_rate_info(0, &headers).await;
    {
        let info = state.endpoints[0].rate_info.read().await;
        assert_eq!(
            info.reset_5h, None,
            "far-future 5h reset should be rejected"
        );
        assert_eq!(
            info.reset_7d, None,
            "far-future 7d reset should be rejected"
        );
    }
}

// ── pick_account integration tests for time-adjusted routing ────

#[tokio::test]
async fn pick_prefers_near_reset_account() {
    // Account A: 5h=0.95 reset in 10min, 7d=0.30 (7d binding after discount)
    // Account B: 5h=0.60 reset in 3h, 7d=0.50
    // A's 5h gets heavily discounted, 7d=0.30 becomes binding → A has more headroom than B
    let now_epoch = AppState::now_epoch();
    let accounts = vec![
        mk_endpoint("acct-a", "sk-ant-api-test-aaa"),
        mk_endpoint("acct-b", "sk-ant-api-test-bbb"),
    ];
    let state = test_state_with(accounts);
    {
        let mut info = state.endpoints[0].rate_info.write().await;
        info.utilization_5h = Some(0.95);
        info.utilization_7d = Some(0.30);
        info.utilization = Some(0.95);
        info.reset_5h = Some(now_epoch + 600); // 10 min
        info.reset_7d = Some(now_epoch + 86400); // 1 day out
    }
    {
        let mut info = state.endpoints[1].rate_info.write().await;
        info.utilization_5h = Some(0.60);
        info.utilization_7d = Some(0.50);
        info.utilization = Some(0.60);
        info.reset_5h = Some(now_epoch + 10800); // 3 hours
        info.reset_7d = Some(now_epoch + 86400);
    }

    // Run 100 picks without affinity to see distribution
    let mut a_count = 0;
    for _ in 0..100 {
        if let Some(idx) = state.pick_endpoint(None, "", &[]).await {
            if idx == 0 {
                a_count += 1;
            }
        }
    }
    // A's effective = max(adj_5h, adj_7d) = max(0.95*0.167, 0.30) = 0.30
    // B's effective = max(0.60, 0.50) = 0.60 (5h outside discount zone)
    // A headroom=0.70, B headroom=0.40 → A gets ~64% of traffic
    assert!(
        a_count > 50,
        "Account A (near-reset 5h) should get majority: got {a_count}/100"
    );
}

#[tokio::test]
async fn pick_throttled_excludes() {
    // Account A: status=throttled (floor=0.98, above soft_limit=0.90)
    // Account B: healthy
    let now_epoch = AppState::now_epoch();
    let state = Arc::new(AppState {
        endpoints: vec![
            mk_endpoint("acct-a", "sk-ant-api-test-aaa"),
            mk_endpoint("acct-b", "sk-ant-api-test-bbb"),
        ],
        soft_limit: 0.90, // Key: not 1.0 — throttled (0.98) will be excluded
        ..test_state_base()
    });
    {
        let mut info = state.endpoints[0].rate_info.write().await;
        info.utilization_5h = Some(0.30);
        info.utilization_7d = Some(0.20);
        info.utilization = Some(0.30);
        info.status_5h = Some("throttled".to_string());
        info.reset_5h = Some(now_epoch + 7200);
        info.reset_7d = Some(now_epoch + 86400);
    }
    {
        let mut info = state.endpoints[1].rate_info.write().await;
        info.utilization_5h = Some(0.40);
        info.utilization_7d = Some(0.30);
        info.utilization = Some(0.40);
        info.status_5h = Some("allowed".to_string());
        info.reset_5h = Some(now_epoch + 7200);
        info.reset_7d = Some(now_epoch + 86400);
    }

    let mut b_count = 0;
    for _ in 0..100 {
        if let Some(idx) = state.pick_endpoint(None, "", &[]).await {
            if idx == 1 {
                b_count += 1;
            }
        }
    }
    // A is throttled → effective=0.98 → excluded by soft_limit=0.90
    // B gets all traffic
    assert_eq!(b_count, 100, "Throttled account A should be soft-excluded");
}

#[tokio::test]
async fn pick_model_specific_7d_throttled_claim_excludes() {
    let now_epoch = AppState::now_epoch();
    let state = Arc::new(AppState {
        endpoints: vec![
            mk_endpoint("acct-a", "sk-ant-api-test-aaa"),
            mk_endpoint("acct-b", "sk-ant-api-test-bbb"),
        ],
        soft_limit: 0.90,
        ..test_state_base()
    });
    {
        let mut info = state.endpoints[0].rate_info.write().await;
        info.utilization_5h = Some(0.20);
        info.utilization = Some(0.20);
        info.status_5h = Some("allowed".to_string());
        info.reset_5h = Some(now_epoch + 7200);
        info.claims_7d.insert(
            "seven_day_opus".to_string(),
            ClaimWindowData {
                utilization: Some(0.20),
                reset: Some(now_epoch + 86400),
                status: Some("throttled".to_string()),
                ..Default::default()
            },
        );
    }
    {
        let mut info = state.endpoints[1].rate_info.write().await;
        info.utilization_5h = Some(0.40);
        info.utilization = Some(0.40);
        info.status_5h = Some("allowed".to_string());
        info.reset_5h = Some(now_epoch + 7200);
    }

    let mut b_count = 0;
    for i in 0..100 {
        let key = format!("sticky-opus-{i}");
        if let Some(idx) = state
            .pick_endpoint(Some(&key), "claude-opus-4-6", &[])
            .await
        {
            if idx == 1 {
                b_count += 1;
            }
        }
    }

    assert_eq!(
        b_count, 100,
        "model-specific throttled 7d claim should soft-exclude account A"
    );
}

#[tokio::test]
async fn pick_mid_block_unchanged() {
    // Both accounts mid-block (3h remaining on 5h) — outside discount zone
    // Should behave identically to raw utilization
    let now_epoch = AppState::now_epoch();
    let accounts = vec![
        mk_endpoint("acct-a", "sk-ant-api-test-aaa"),
        mk_endpoint("acct-b", "sk-ant-api-test-bbb"),
    ];
    let state = test_state_with(accounts);
    {
        let mut info = state.endpoints[0].rate_info.write().await;
        info.utilization_5h = Some(0.80);
        info.utilization_7d = Some(0.40);
        info.utilization = Some(0.80);
        info.reset_5h = Some(now_epoch + 10800); // 3h out
        info.reset_7d = Some(now_epoch + 86400);
    }
    {
        let mut info = state.endpoints[1].rate_info.write().await;
        info.utilization_5h = Some(0.40);
        info.utilization_7d = Some(0.30);
        info.utilization = Some(0.40);
        info.reset_5h = Some(now_epoch + 10800);
        info.reset_7d = Some(now_epoch + 86400);
    }

    let mut b_count = 0;
    for _ in 0..100 {
        if let Some(idx) = state.pick_endpoint(None, "", &[]).await {
            if idx == 1 {
                b_count += 1;
            }
        }
    }
    // A: effective=max(0.80, 0.40)=0.80, headroom=0.20
    // B: effective=max(0.40, 0.30)=0.40, headroom=0.60
    // B should get ~75% (0.60 / 0.80)
    assert!(
        b_count > 60,
        "Mid-block: B (lower util) should dominate: got {b_count}/100"
    );
    assert!(
        b_count < 90,
        "Mid-block: A should still get some traffic: B got {b_count}/100"
    );
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

// ── Waste risk tests ──────────────────────────────────────────

#[test]
fn waste_risk_normal() {
    let now = 1_000_000u64;
    let reset = now + 302400; // 3.5 days remaining (half of 7d)
    let wr = waste_risk(Some(0.40), Some(reset), now);
    // unused=0.60, remaining_fraction=302400/604800=0.5, wr=0.60/0.5=1.2
    assert!((wr - 1.2).abs() < 0.01, "expected ~1.2, got {wr}");
}

#[test]
fn waste_risk_stale_reset() {
    let now = 1_000_000u64;
    let wr = waste_risk(Some(0.40), Some(now - 100), now);
    assert_eq!(wr, 0.0, "stale reset should return 0");
}

#[test]
fn waste_risk_no_reset() {
    let wr = waste_risk(Some(0.40), None, 1_000_000);
    assert_eq!(wr, 0.0, "no reset should return 0");
}

#[test]
fn waste_risk_under_60s_remaining() {
    let now = 1_000_000u64;
    let wr = waste_risk(Some(0.40), Some(now + 30), now);
    assert_eq!(wr, 0.0, "<60s remaining should return 0");
}

#[test]
fn waste_risk_fully_utilized() {
    let now = 1_000_000u64;
    let wr = waste_risk(Some(1.0), Some(now + 302400), now);
    assert_eq!(wr, 0.0, "fully utilized should return 0");
}

#[test]
fn waste_risk_near_reset_urgency() {
    let now = 1_000_000u64;
    // 2 hours remaining, 50% unused — very urgent
    let wr = waste_risk(Some(0.50), Some(now + 7200), now);
    // unused=0.50, remaining_fraction=7200/604800=0.0119, wr=0.50/0.0119=42→clamped to 10
    assert_eq!(wr, 10.0, "near-reset high-unused should clamp to 10");
}

#[test]
fn waste_risk_clamped() {
    let now = 1_000_000u64;
    // 1 hour remaining, 90% unused
    let wr = waste_risk(Some(0.10), Some(now + 3600), now);
    // unused=0.90, remaining_fraction=3600/604800≈0.00595, wr≈151→clamped to 10
    assert_eq!(wr, 10.0, "should clamp to 10.0");
}

#[test]
fn waste_risk_boundary_at_60s() {
    let now = 1_000_000u64;
    // Exactly 60s remaining — guard is > 60, so should return 0
    assert_eq!(waste_risk(Some(0.40), Some(now + 60), now), 0.0);
    // 61s remaining — just above boundary, should return non-zero
    assert!(waste_risk(Some(0.40), Some(now + 61), now) > 0.0);
}

#[test]
fn waste_risk_util_above_one() {
    let now = 1_000_000u64;
    // API can return util > 1.0; unused should clamp to 0
    let wr = waste_risk(Some(1.05), Some(now + 302400), now);
    assert_eq!(wr, 0.0, "util > 1.0 should produce zero waste risk");
}

// ── compute_routing_weight tests ──────────────────────────────

#[test]
fn routing_weight_basic_5h_only() {
    // No 7d data → headroom_only, weight = headroom = 1 - gate_5h
    let now = 1_000_000u64;
    let info = RateLimitInfo {
        utilization_5h: Some(0.40),
        reset_5h: Some(now + 18000), // 5h window
        status_5h: Some("allowed".to_string()),
        ..Default::default()
    };
    let rw = compute_routing_weight(&info, "claude-sonnet-4-6", now, false)
        .expect("should produce weight");
    assert!(rw.gate_5h > 0.0 && rw.gate_5h < 1.0);
    assert_eq!(rw.source, "headroom_only");
    assert!(rw.wr == 0.0);
    assert!(rw.weight > 0.0);
}

#[test]
fn routing_weight_with_waste_risk() {
    // 7d claim present → waste_risk sourced weight
    let now = 1_000_000u64;
    let mut claims = HashMap::new();
    claims.insert(
        "seven_day_sonnet".to_string(),
        ClaimWindowData {
            utilization: Some(0.40),
            reset: Some(now + 302400), // 3.5 days
            status: Some("allowed".to_string()),
            last_seen: now,
        },
    );
    let info = RateLimitInfo {
        utilization_5h: Some(0.20),
        reset_5h: Some(now + 18000),
        status_5h: Some("allowed".to_string()),
        claims_7d: claims,
        ..Default::default()
    };
    let rw = compute_routing_weight(&info, "claude-sonnet-4-6", now, false)
        .expect("should produce weight");
    assert_eq!(rw.source, "waste_risk");
    assert!(rw.wr > 0.0, "waste_risk should be positive");
    // weight = wr * headroom (both positive)
    assert!(rw.weight > 0.0);
}

#[test]
fn routing_weight_rejected_returns_none() {
    // 7d claim rejected → None (account should be skipped)
    let now = 1_000_000u64;
    let mut claims = HashMap::new();
    claims.insert(
        "seven_day_sonnet".to_string(),
        ClaimWindowData {
            utilization: Some(1.0),
            reset: Some(now + 302400),
            status: Some("rejected".to_string()),
            last_seen: now,
        },
    );
    let info = RateLimitInfo {
        utilization_5h: Some(0.20),
        reset_5h: Some(now + 18000),
        status_5h: Some("allowed".to_string()),
        claims_7d: claims,
        ..Default::default()
    };
    assert!(
        compute_routing_weight(&info, "claude-sonnet-4-6", now, false).is_none(),
        "rejected claim should return None"
    );
}

#[test]
fn routing_weight_expired_rejected_claim_returns_some() {
    let now = 1_000_000u64;
    let mut claims = HashMap::new();
    claims.insert(
        "seven_day_sonnet".to_string(),
        ClaimWindowData {
            utilization: Some(1.0),
            reset: Some(now.saturating_sub(1)),
            status: Some("rejected".to_string()),
            last_seen: now,
        },
    );
    let info = RateLimitInfo {
        utilization_5h: Some(0.20),
        reset_5h: Some(now + 18000),
        status_5h: Some("allowed".to_string()),
        claims_7d: claims,
        ..Default::default()
    };
    assert!(
        compute_routing_weight(&info, "claude-sonnet-4-6", now, false).is_some(),
        "expired rejected claim should not return None"
    );
}

#[test]
fn routing_weight_stale_uses_fallback() {
    // stale_after_hard_limit = true → both gates fallback to 0.5
    let now = 1_000_000u64;
    let info = RateLimitInfo {
        utilization_5h: Some(0.95), // would be high, but stale
        reset_5h: Some(now + 18000),
        status_5h: Some("throttled".to_string()),
        ..Default::default()
    };
    let rw = compute_routing_weight(&info, "claude-sonnet-4-6", now, true)
        .expect("stale should still produce weight");
    assert_eq!(rw.gate_5h, 0.5);
    assert_eq!(rw.gate_7d, 0.5);
    assert_eq!(rw.gate, 0.5);
}

#[test]
fn routing_weight_rejected_but_stale_still_returns_some() {
    // Stale after hard limit + rejected claim → should NOT skip (give it a chance)
    let now = 1_000_000u64;
    let mut claims = HashMap::new();
    claims.insert(
        "seven_day_sonnet".to_string(),
        ClaimWindowData {
            utilization: Some(1.0),
            reset: Some(now + 302400),
            status: Some("rejected".to_string()),
            last_seen: now,
        },
    );
    let info = RateLimitInfo {
        utilization_5h: Some(0.20),
        reset_5h: Some(now + 18000),
        status_5h: Some("allowed".to_string()),
        claims_7d: claims,
        ..Default::default()
    };
    assert!(
        compute_routing_weight(&info, "claude-sonnet-4-6", now, true).is_some(),
        "stale rejected should still return Some (give account a chance)"
    );
}

#[test]
fn routing_weight_publish_ttl_uses_fallback_interval_when_probes_disabled() {
    assert_eq!(AppState::routing_weight_publish_ttl(0), 120);
}

#[test]
fn routing_weight_publish_ttl_doubles_probe_interval() {
    assert_eq!(AppState::routing_weight_publish_ttl(300), 600);
}

#[test]
fn routing_weight_no_data_uses_defaults() {
    // No utilization data at all → gate_5h=0.5 (unknown), headroom=0.5
    let now = 1_000_000u64;
    let info = RateLimitInfo::default();
    let rw = compute_routing_weight(&info, "claude-sonnet-4-6", now, false)
        .expect("should produce weight with defaults");
    assert_eq!(rw.gate_5h, 0.5);
    assert_eq!(rw.source, "headroom_only");
    assert!(rw.weight > 0.0);
}

// ── classify_hard_limit_sync tests ────────────────────────────

#[test]
fn classify_hard_limit_none_is_ignore() {
    let now_instant = Instant::now();
    assert_eq!(
        classify_hard_limit_sync(None, 1_000_000, now_instant),
        HardLimitSync::Ignore
    );
}

#[test]
fn classify_hard_limit_sentinel_is_clear() {
    let now_instant = Instant::now();
    assert_eq!(
        classify_hard_limit_sync(Some(HARD_LIMIT_CLEARED_SENTINEL), 1_000_000, now_instant),
        HardLimitSync::Clear
    );
}

#[test]
fn classify_hard_limit_future_epoch_is_update() {
    let now_instant = Instant::now();
    let now_epoch = 1_000_000u64;
    let future = now_epoch + 300; // 5 min from now
    match classify_hard_limit_sync(Some(future), now_epoch, now_instant) {
        HardLimitSync::Update(until) => {
            let expected = now_instant + Duration::from_secs(300);
            let delta = if until > expected {
                until.duration_since(expected)
            } else {
                expected.duration_since(until)
            };
            assert!(delta < Duration::from_millis(1), "until Instant mismatch");
        }
        other => panic!("expected Update, got {other:?}"),
    }
}

#[test]
fn classify_hard_limit_past_epoch_is_ignore() {
    // Stale non-zero value — e.g. a hard limit that already expired via TTL
    let now_instant = Instant::now();
    let now_epoch = 1_000_000u64;
    assert_eq!(
        classify_hard_limit_sync(Some(now_epoch - 100), now_epoch, now_instant),
        HardLimitSync::Ignore
    );
}

#[test]
fn classify_hard_limit_epoch_equal_now_is_ignore() {
    // Boundary: epoch == now means expired now, not future
    let now_instant = Instant::now();
    let now_epoch = 1_000_000u64;
    assert_eq!(
        classify_hard_limit_sync(Some(now_epoch), now_epoch, now_instant),
        HardLimitSync::Ignore
    );
}

#[test]
fn classify_hard_limit_clamps_far_future() {
    // Corrupt or malicious Redis value must not create a panic-inducing Instant
    // nor a permanent undead hard limit. Clamp to 24h.
    let now_instant = Instant::now();
    let now_epoch = 1_000_000u64;
    match classify_hard_limit_sync(Some(u64::MAX), now_epoch, now_instant) {
        HardLimitSync::Update(until) => {
            let capped = now_instant + Duration::from_secs(86_400);
            let delta = if until > capped {
                until.duration_since(capped)
            } else {
                capped.duration_since(until)
            };
            assert!(delta < Duration::from_millis(1), "expected 24h clamp");
        }
        other => panic!("expected Update (clamped), got {other:?}"),
    }
}

#[tokio::test]
async fn signal_hard_limit_recovery_without_redis_is_noop() {
    // Without Redis, the helper must still refresh metrics + publish weights
    // locally. It must not panic and must not attempt Redis I/O.
    let state = test_state_with(vec![mk_endpoint("a", "sk-ant-api-test")]);
    // Pre-seed a non-zero weight so we can assert refresh_metrics_weights ran
    state.endpoints[0]
        .last_routing_weight
        .store(0u64, Ordering::Relaxed);
    state
        .signal_hard_limit_recovery(&state.endpoints[0].name)
        .await;
    // refresh_metrics_weights always writes a value (even zero) — the point
    // is that the method completed without Redis and without panicking.
    let w = f64::from_bits(
        state.endpoints[0]
            .last_routing_weight
            .load(Ordering::Relaxed),
    );
    assert!(w.is_finite(), "weight atomic must remain valid: {w}");
}

// ── resolve_7d_claim tests ────────────────────────────────────

#[test]
fn resolve_7d_claim_model_specific() {
    let mut claims = HashMap::new();
    claims.insert(
        "seven_day_sonnet".to_string(),
        ClaimWindowData {
            utilization: Some(0.80),
            reset: Some(1000000),
            status: None,
            ..Default::default()
        },
    );
    claims.insert(
        "seven_day".to_string(),
        ClaimWindowData {
            utilization: Some(0.50),
            reset: Some(1000000),
            status: None,
            ..Default::default()
        },
    );
    let info = RateLimitInfo {
        claims_7d: claims,
        ..Default::default()
    };
    let claim = resolve_7d_claim(&info, "claude-sonnet-4-6").unwrap();
    assert!(
        (claim.utilization.unwrap() - 0.80).abs() < 0.001,
        "should pick model-specific claim"
    );
}

#[test]
fn resolve_7d_claim_fallback_general() {
    let mut claims = HashMap::new();
    claims.insert(
        "seven_day".to_string(),
        ClaimWindowData {
            utilization: Some(0.50),
            reset: Some(1000000),
            status: None,
            ..Default::default()
        },
    );
    let info = RateLimitInfo {
        claims_7d: claims,
        ..Default::default()
    };
    let claim = resolve_7d_claim(&info, "claude-sonnet-4-6").unwrap();
    assert!(
        (claim.utilization.unwrap() - 0.50).abs() < 0.001,
        "should fall back to general"
    );
}

#[test]
fn resolve_7d_claim_empty_claims() {
    let info = RateLimitInfo::default();
    assert!(resolve_7d_claim(&info, "claude-sonnet-4-6").is_none());
}

#[test]
fn resolve_7d_claim_empty_model() {
    let mut claims = HashMap::new();
    claims.insert(
        "seven_day".to_string(),
        ClaimWindowData {
            utilization: Some(0.50),
            reset: Some(1000000),
            status: None,
            ..Default::default()
        },
    );
    let info = RateLimitInfo {
        claims_7d: claims,
        ..Default::default()
    };
    assert!(
        resolve_7d_claim(&info, "").is_none(),
        "empty model should return None"
    );
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

// ── Effective utilization tests ────────────────────────────────

#[tokio::test]
async fn effective_util_both_windows() {
    let now_epoch = AppState::now_epoch();
    let mut claims_7d = HashMap::new();
    claims_7d.insert(
        "seven_day".to_string(),
        ClaimWindowData {
            utilization: Some(0.80),
            reset: Some(now_epoch + 100000),
            status: None,
            ..Default::default()
        },
    );
    let info = RateLimitInfo {
        utilization_5h: Some(0.60),
        reset_5h: Some(now_epoch + 10000),
        claims_7d,
        ..Default::default()
    };
    let (util, source, _, _) = effective_utilization(&info, now_epoch, "");
    // 7d at 0.80 (no penalty). Max(0.60, 0.80) = 0.80
    assert_eq!(source, "7d");
    assert!((util - 0.80).abs() < 0.01, "expected ~0.80, got {util}");
}

#[tokio::test]
async fn effective_util_5h_only() {
    let now_epoch = AppState::now_epoch();
    // 7d claim with reset in the past → stale, evicted by time_adjusted_utilization
    let mut claims_7d = HashMap::new();
    claims_7d.insert(
        "seven_day".to_string(),
        ClaimWindowData {
            utilization: Some(0.90),
            reset: Some(now_epoch - 1),
            status: None,
            ..Default::default()
        },
    );
    let info = RateLimitInfo {
        utilization_5h: Some(0.60),
        reset_5h: Some(now_epoch + 10000),
        claims_7d,
        ..Default::default()
    };
    let (util, source, _, _) = effective_utilization(&info, now_epoch, "");
    assert_eq!(source, "5h");
    assert!(
        (util - 0.60).abs() < 0.01,
        "should use 5h value: got {util}"
    );
}

#[tokio::test]
async fn effective_util_7d_only() {
    let now_epoch = AppState::now_epoch();
    let mut claims_7d = HashMap::new();
    claims_7d.insert(
        "seven_day".to_string(),
        ClaimWindowData {
            utilization: Some(0.50),
            reset: Some(now_epoch + 100000),
            status: None,
            ..Default::default()
        },
    );
    let info = RateLimitInfo {
        // 5h stale
        utilization_5h: Some(0.40),
        reset_5h: Some(now_epoch - 1),
        claims_7d,
        ..Default::default()
    };
    let (util, source, _, _) = effective_utilization(&info, now_epoch, "");
    assert_eq!(source, "7d");
    // 0.50 is below CLAIM_PENALTY_THRESHOLD, so no penalty
    assert!(
        (util - 0.50).abs() < 0.01,
        "should use 7d value: got {util}"
    );
}

#[tokio::test]
async fn effective_util_fallback_unified() {
    let now_epoch = AppState::now_epoch();
    // Both 5h and all 7d claims stale → falls through to unified
    let mut claims_7d = HashMap::new();
    claims_7d.insert(
        "seven_day".to_string(),
        ClaimWindowData {
            utilization: Some(0.50),
            reset: Some(now_epoch - 1),
            status: None,
            ..Default::default()
        },
    );
    let info = RateLimitInfo {
        utilization_5h: Some(0.40),
        reset_5h: Some(now_epoch - 1),
        claims_7d,
        utilization: Some(0.65),
        ..Default::default()
    };
    let (util, source, _, _) = effective_utilization(&info, now_epoch, "");
    assert_eq!(source, "unified");
    assert!(
        (util - 0.65).abs() < 0.001,
        "should use unified: got {util}"
    );
}

#[tokio::test]
async fn effective_util_fallback_legacy() {
    let now_epoch = AppState::now_epoch();
    let info = RateLimitInfo {
        remaining_tokens: Some(300_000),
        limit_tokens: Some(1_000_000),
        ..Default::default()
    };
    let (util, source, _, _) = effective_utilization(&info, now_epoch, "");
    assert_eq!(source, "legacy");
    assert!(
        (util - 0.70).abs() < 0.01,
        "should use legacy token ratio: got {util}"
    );
}

#[tokio::test]
async fn effective_util_fallback_unknown() {
    let now_epoch = AppState::now_epoch();
    let info = RateLimitInfo::default();
    let (util, source, _, _) = effective_utilization(&info, now_epoch, "");
    assert_eq!(source, "unknown");
    assert!(
        (util - 0.50).abs() < 0.001,
        "should default to 0.5: got {util}"
    );
}

// ── Per-claim 7d model-specific tests ──────────────────────────

#[tokio::test]
async fn model_specific_routing() {
    // Sonnet 7d at 0.85 (no penalty), Opus 7d at 0.10 → Opus sees low util
    let now_epoch = AppState::now_epoch();
    let mut claims_7d = HashMap::new();
    claims_7d.insert(
        "seven_day_sonnet".to_string(),
        ClaimWindowData {
            utilization: Some(0.85),
            reset: Some(now_epoch + 100000),
            status: None,
            ..Default::default()
        },
    );
    claims_7d.insert(
        "seven_day_opus".to_string(),
        ClaimWindowData {
            utilization: Some(0.10),
            reset: Some(now_epoch + 100000),
            status: None,
            ..Default::default()
        },
    );
    let info = RateLimitInfo {
        utilization_5h: Some(0.30),
        reset_5h: Some(now_epoch + 10000),
        claims_7d,
        ..Default::default()
    };
    let (util_sonnet, _, _, _) = effective_utilization(&info, now_epoch, "claude-sonnet-4-6");
    let (util_opus, _, _, _) = effective_utilization(&info, now_epoch, "claude-opus-4-6");
    // Sonnet 7d at 0.85 (max with 5h 0.30) = 0.85, Opus 7d at 0.10 (max with 5h 0.30) = 0.30
    assert!(
        (util_sonnet - 0.85).abs() < 0.01,
        "sonnet should be 0.85: got {util_sonnet}"
    );
    // Opus at 0.10 → should stay at ~0.30 (max with 5h)
    assert!(util_opus < 0.35, "opus should be low: got {util_opus}");
    // The gap should be large — this is the whole point of per-claim routing
    assert!(
        util_sonnet - util_opus > 0.50,
        "sonnet-opus gap should be >0.50"
    );
}

#[tokio::test]
async fn claim_fallback_general() {
    // Only "seven_day" (general) claim → used for all models
    let now_epoch = AppState::now_epoch();
    let mut claims_7d = HashMap::new();
    claims_7d.insert(
        "seven_day".to_string(),
        ClaimWindowData {
            utilization: Some(0.50),
            reset: Some(now_epoch + 100000),
            status: None,
            ..Default::default()
        },
    );
    let info = RateLimitInfo {
        utilization_5h: Some(0.20),
        reset_5h: Some(now_epoch + 10000),
        claims_7d,
        ..Default::default()
    };
    let (util_sonnet, _, _, _) = effective_utilization(&info, now_epoch, "claude-sonnet-4-6");
    let (util_opus, _, _, _) = effective_utilization(&info, now_epoch, "claude-opus-4-6");
    let (util_haiku, _, _, _) = effective_utilization(&info, now_epoch, "claude-haiku-4-5");
    // All should see the same general claim (0.50, no penalty)
    assert!(
        (util_sonnet - 0.50).abs() < 0.01,
        "sonnet: got {util_sonnet}"
    );
    assert!((util_opus - 0.50).abs() < 0.01, "opus: got {util_opus}");
    assert!((util_haiku - 0.50).abs() < 0.01, "haiku: got {util_haiku}");
}

#[tokio::test]
async fn cross_model_isolation() {
    // Sonnet claim at 0.95 should NOT affect Opus effective_utilization
    let now_epoch = AppState::now_epoch();
    let mut claims_7d = HashMap::new();
    claims_7d.insert(
        "seven_day_sonnet".to_string(),
        ClaimWindowData {
            utilization: Some(0.95),
            reset: Some(now_epoch + 100000),
            status: None,
            ..Default::default()
        },
    );
    // No opus claim and no general claim
    let info = RateLimitInfo {
        utilization_5h: Some(0.20),
        reset_5h: Some(now_epoch + 10000),
        claims_7d,
        ..Default::default()
    };
    let (util_opus, source, _, _) = effective_utilization(&info, now_epoch, "claude-opus-4-6");
    // Opus has no specific claim and no general fallback → only 5h at 0.20
    assert_eq!(source, "5h");
    assert!(
        (util_opus - 0.20).abs() < 0.01,
        "opus should only see 5h: got {util_opus}"
    );

    let (util_sonnet, _, _, _) = effective_utilization(&info, now_epoch, "claude-sonnet-4-6");
    assert!(
        (util_sonnet - 0.95).abs() < 0.01,
        "sonnet should be 0.95: got {util_sonnet}"
    );
}

#[tokio::test]
async fn emergency_brake_worst_case() {
    // Emergency brake (model="") should use max across all claims
    let now_epoch = AppState::now_epoch();
    let mut claims_7d = HashMap::new();
    claims_7d.insert(
        "seven_day_sonnet".to_string(),
        ClaimWindowData {
            utilization: Some(0.95),
            reset: Some(now_epoch + 100000),
            status: None,
            ..Default::default()
        },
    );
    claims_7d.insert(
        "seven_day_opus".to_string(),
        ClaimWindowData {
            utilization: Some(0.10),
            reset: Some(now_epoch + 100000),
            status: None,
            ..Default::default()
        },
    );
    let info = RateLimitInfo {
        utilization_5h: Some(0.30),
        reset_5h: Some(now_epoch + 10000),
        claims_7d,
        ..Default::default()
    };
    let (util, _, _, _) = effective_utilization(&info, now_epoch, "");
    // Should pick sonnet's 0.95 (no penalty now)
    assert!(
        (util - 0.95).abs() < 0.01,
        "emergency brake should use worst claim: got {util}"
    );
}

#[tokio::test]
async fn stale_claim_eviction() {
    // Claim with expired reset should be ignored
    let now_epoch = AppState::now_epoch();
    let mut claims_7d = HashMap::new();
    claims_7d.insert(
        "seven_day_sonnet".to_string(),
        ClaimWindowData {
            utilization: Some(0.95),
            reset: Some(now_epoch - 1), // stale
            status: None,
            ..Default::default()
        },
    );
    claims_7d.insert(
        "seven_day_opus".to_string(),
        ClaimWindowData {
            utilization: Some(0.30),
            reset: Some(now_epoch + 100000), // fresh
            status: None,
            ..Default::default()
        },
    );
    let info = RateLimitInfo {
        utilization_5h: Some(0.20),
        reset_5h: Some(now_epoch + 10000),
        claims_7d,
        ..Default::default()
    };
    // For sonnet model: sonnet claim is stale, no general fallback → only 5h
    let (util_sonnet, source, _, _) = effective_utilization(&info, now_epoch, "claude-sonnet-4-6");
    assert_eq!(source, "5h");
    assert!(
        (util_sonnet - 0.20).abs() < 0.01,
        "stale sonnet should be ignored: got {util_sonnet}"
    );

    // For opus model: opus claim is fresh
    let (util_opus, source, _, _) = effective_utilization(&info, now_epoch, "claude-opus-4-6");
    assert_eq!(source, "7d");
    assert!(
        (util_opus - 0.30).abs() < 0.01,
        "fresh opus should be used: got {util_opus}"
    );
}

#[tokio::test]
async fn model_family_extraction() {
    assert_eq!(model_family("claude-sonnet-4-6"), "sonnet");
    assert_eq!(model_family("claude-opus-4-6"), "opus");
    assert_eq!(model_family("claude-haiku-4-5"), "haiku");
    assert_eq!(model_family("claude-3-5-sonnet"), "sonnet");
    assert_eq!(model_family("claude-fable-5"), "fable");
    assert_eq!(model_family("unknown-model"), "");
}

#[tokio::test]
async fn constraining_claims_fable_pairs_band_with_pool() {
    let mut info = RateLimitInfo::default();
    info.claims_7d.insert(
        "seven_day".to_string(),
        ClaimWindowData {
            utilization: Some(0.30),
            ..Default::default()
        },
    );

    // No band claim yet → primary is the general claim, no pool cap.
    let (primary, cap) = constraining_7d_claims(&info, "claude-fable-5");
    assert_eq!(primary.unwrap().utilization, Some(0.30));
    assert!(cap.is_none(), "no band claim → nothing to pair");

    // Band claim present → (band, Some(pool)).
    info.claims_7d.insert(
        FABLE_BAND_CLAIM.to_string(),
        ClaimWindowData {
            utilization: Some(0.80),
            ..Default::default()
        },
    );
    let (primary, cap) = constraining_7d_claims(&info, "claude-fable-5");
    assert_eq!(primary.unwrap().utilization, Some(0.80));
    assert_eq!(cap.unwrap().utilization, Some(0.30));

    // Non-Fable families never get a pool cap, even with a band claim present.
    let (_, cap) = constraining_7d_claims(&info, "claude-sonnet-4-6");
    assert!(cap.is_none(), "sonnet must keep single-claim semantics");
}

#[tokio::test]
async fn fable_effective_utilization_binds_on_worse_of_band_and_pool() {
    let now_epoch = AppState::now_epoch();
    let mut info = RateLimitInfo::default();
    info.claims_7d.insert(
        "seven_day".to_string(),
        ClaimWindowData {
            utilization: Some(0.90),
            reset: Some(now_epoch + 302400),
            status: None,
            ..Default::default()
        },
    );
    info.claims_7d.insert(
        FABLE_BAND_CLAIM.to_string(),
        ClaimWindowData {
            utilization: Some(0.20),
            reset: Some(now_epoch + 302400),
            status: None,
            ..Default::default()
        },
    );

    // Fable is bound by the drained pool (0.90), not its roomy band (0.20).
    let (util, source, _, _) = effective_utilization(&info, now_epoch, "claude-fable-5");
    assert_eq!(source, "7d");
    assert!((util - 0.90).abs() < 0.01, "pool should bind: got {util}");
}

#[tokio::test]
async fn worst_case_utilization_allowlists_claims() {
    // Model-agnostic worst case (emergency brake path) reads ONLY claims
    // that gate all traffic. Neither the exhausted Fable band nor an
    // unknown future carve-out may drive it — either would brake ALL
    // traffic while regular budgets are healthy.
    let now_epoch = AppState::now_epoch();
    let mut info = RateLimitInfo::default();
    info.claims_7d.insert(
        "seven_day".to_string(),
        ClaimWindowData {
            utilization: Some(0.30),
            reset: Some(now_epoch + 302400),
            status: None,
            ..Default::default()
        },
    );
    info.claims_7d.insert(
        FABLE_BAND_CLAIM.to_string(),
        ClaimWindowData {
            utilization: Some(1.0),
            reset: Some(now_epoch + 302400),
            status: Some("rejected".to_string()),
            ..Default::default()
        },
    );
    info.claims_7d.insert(
        "seven_day_future_carveout_oi".to_string(),
        ClaimWindowData {
            utilization: Some(0.95),
            reset: Some(now_epoch + 302400),
            status: None,
            ..Default::default()
        },
    );

    let (util, source, _, _) = effective_utilization(&info, now_epoch, "");
    assert_eq!(source, "7d");
    assert!(
        (util - 0.30).abs() < 0.01,
        "only allowlisted claims may drive the worst case: got {util}"
    );

    // Sanity on the predicate itself.
    assert!(claim_gates_all_traffic("seven_day"));
    assert!(claim_gates_all_traffic("seven_day_sonnet"));
    assert!(claim_gates_all_traffic("seven_day_opus"));
    assert!(claim_gates_all_traffic("seven_day_haiku"));
    assert!(!claim_gates_all_traffic(FABLE_BAND_CLAIM));
    assert!(!claim_gates_all_traffic("seven_day_future_carveout_oi"));
}

#[tokio::test]
async fn parse_7d_oi_headers_populates_band_claim() {
    // Header shape captured from a live claude-fable-5 response through the
    // LB on 2026-07-21 (LAB-387 verification): the Fable band arrives as the
    // 7d_oi triplet, NOT as a seven_day_fable representative claim.
    let accounts = vec![mk_endpoint("acct-a", "sk-ant-api-test-aaa")];
    let state = test_state_with(accounts);
    let now_epoch = AppState::now_epoch();

    let mut headers = reqwest::header::HeaderMap::new();
    headers.insert(
        "anthropic-ratelimit-unified-representative-claim",
        HeaderValue::from_static("five_hour"),
    );
    headers.insert(
        "anthropic-ratelimit-unified-5h-utilization",
        HeaderValue::from_static("0.09"),
    );
    headers.insert(
        "anthropic-ratelimit-unified-7d-utilization",
        HeaderValue::from_static("0.15"),
    );
    headers.insert(
        "anthropic-ratelimit-unified-7d-reset",
        HeaderValue::from_str(&format!("{}", now_epoch + 302400)).unwrap(),
    );
    headers.insert(
        "anthropic-ratelimit-unified-7d-status",
        HeaderValue::from_static("allowed"),
    );
    headers.insert(
        "anthropic-ratelimit-unified-7d_oi-utilization",
        HeaderValue::from_static("0.26"),
    );
    headers.insert(
        "anthropic-ratelimit-unified-7d_oi-reset",
        HeaderValue::from_str(&format!("{}", now_epoch + 302400)).unwrap(),
    );
    headers.insert(
        "anthropic-ratelimit-unified-7d_oi-status",
        HeaderValue::from_static("allowed"),
    );
    state.update_rate_info(0, &headers).await;

    {
        let info = state.endpoints[0].rate_info.read().await;
        let band = info.claims_7d.get(FABLE_BAND_CLAIM).expect("band claim");
        assert_eq!(band.utilization, Some(0.26));
        assert_eq!(band.reset, Some(now_epoch + 302400));
        assert_eq!(band.status.as_deref(), Some("allowed"));
        let general = info.claims_7d.get("seven_day").expect("general claim");
        assert_eq!(general.utilization, Some(0.15));
    }

    // A later non-Fable response (no 7d_oi triplet — the sonnet shape
    // captured from the same account) must NOT clear the band claim.
    let mut sonnet_headers = reqwest::header::HeaderMap::new();
    sonnet_headers.insert(
        "anthropic-ratelimit-unified-representative-claim",
        HeaderValue::from_static("five_hour"),
    );
    sonnet_headers.insert(
        "anthropic-ratelimit-unified-7d-utilization",
        HeaderValue::from_static("0.16"),
    );
    state.update_rate_info(0, &sonnet_headers).await;

    {
        let info = state.endpoints[0].rate_info.read().await;
        let band = info.claims_7d.get(FABLE_BAND_CLAIM).expect("band persists");
        assert_eq!(band.utilization, Some(0.26), "absence must not clear band");
        assert_eq!(
            info.claims_7d.get("seven_day").unwrap().utilization,
            Some(0.16),
            "general claim refreshed by the non-fable response"
        );
    }
}

#[tokio::test]
async fn band_only_signal_never_drives_brake_fallback() {
    // CodeRabbit finding on PR #87: the flat `info.utilization` derivation
    // included the band, so with 5h absent/stale and no allowlisted 7d
    // claim, a drained band could reach the brake via the raw-unified
    // fallback. A band-only account must resolve to "unknown" (fail-open),
    // not to the band's utilization.
    let accounts = vec![mk_endpoint("acct-a", "sk-ant-api-test-aaa")];
    let state = test_state_with(accounts);
    let now_epoch = AppState::now_epoch();

    // Fable response shape carrying ONLY the band triplet (no 5h/7d data).
    let mut headers = reqwest::header::HeaderMap::new();
    headers.insert(
        "anthropic-ratelimit-unified-7d_oi-utilization",
        HeaderValue::from_static("1.0"),
    );
    headers.insert(
        "anthropic-ratelimit-unified-7d_oi-reset",
        HeaderValue::from_str(&format!("{}", now_epoch + 302400)).unwrap(),
    );
    headers.insert(
        "anthropic-ratelimit-unified-7d_oi-status",
        HeaderValue::from_static("rejected"),
    );
    state.update_rate_info(0, &headers).await;

    let info = state.endpoints[0].rate_info.read().await;
    assert!(
        info.claims_7d.contains_key(FABLE_BAND_CLAIM),
        "band claim parsed"
    );
    assert_eq!(
        info.utilization, None,
        "flat unified utilization must not derive from the band"
    );
    assert_eq!(
        info.utilization_7d, None,
        "flat 7d utilization must not derive from the band"
    );
    let (util, source, _, _) = effective_utilization(&info, now_epoch, "");
    assert_eq!(
        source, "unknown",
        "model-agnostic view of a band-only account is unknown, got {util} from {source}"
    );
}

#[tokio::test]
async fn fable_band_wr_survives_pool_with_missing_reset() {
    // A pool claim with utilization but NO reset yields waste_risk 0.0
    // (missing data, not a real zero) — it must not erase the band's
    // urgency signal via min().
    let now_epoch = AppState::now_epoch();
    let mut info = RateLimitInfo {
        utilization_5h: Some(0.30),
        reset_5h: Some(now_epoch + 10000),
        ..Default::default()
    };
    info.claims_7d.insert(
        FABLE_BAND_CLAIM.to_string(),
        ClaimWindowData {
            utilization: Some(0.20),
            reset: Some(now_epoch + 302400),
            status: None,
            ..Default::default()
        },
    );
    info.claims_7d.insert(
        "seven_day".to_string(),
        ClaimWindowData {
            utilization: Some(0.90),
            reset: None, // stale/missing — waste_risk yields 0.0
            status: None,
            last_seen: now_epoch,
        },
    );

    let rw = compute_routing_weight(&info, "claude-fable-5", now_epoch, false)
        .expect("account routable");
    assert!(
        rw.wr > 0.0,
        "band waste_risk must survive a data-less pool cap: got {}",
        rw.wr
    );
}

#[tokio::test]
async fn extract_client_version_parsing() {
    assert_eq!(
        extract_client_version("claude-cli/2.1.68 (external, cli)"),
        Some("2.1.68")
    );
    assert_eq!(extract_client_version("anthropic-sdk/1.0.0"), Some("1.0.0"));
    assert_eq!(extract_client_version("curl/8.5.0"), Some("8.5.0"));
    assert_eq!(extract_client_version("no-version"), None);
    assert_eq!(extract_client_version("foo/"), None);
}

#[tokio::test]
async fn flat_field_compat_fallback() {
    // When claims_7d is empty, should fall back to flat utilization_7d fields
    let now_epoch = AppState::now_epoch();
    let info = RateLimitInfo {
        utilization_5h: Some(0.30),
        reset_5h: Some(now_epoch + 10000),
        utilization_7d: Some(0.60),
        reset_7d: Some(now_epoch + 100000),
        // claims_7d empty — migration/compat path
        ..Default::default()
    };
    let (util, source, _, _) = effective_utilization(&info, now_epoch, "claude-sonnet-4-6");
    assert_eq!(source, "7d");
    assert!((util - 0.60).abs() < 0.01, "should use flat 7d: got {util}");
}

#[tokio::test]
async fn claim_aware_routing_prefers_low_model_util() {
    // Account A: Sonnet 7d at 0.90 (high), Opus 7d at 0.10 (low)
    // Account B: Sonnet 7d at 0.10 (low), Opus 7d at 0.90 (high)
    // Routing for Sonnet should prefer B, routing for Opus should prefer A
    let acct_a = mk_endpoint("a", "sk-ant-api-a");
    let acct_b = mk_endpoint("b", "sk-ant-api-b");
    let state = test_state_with(vec![acct_a, acct_b]);
    let now = AppState::now_epoch();

    // Set 5h low for both
    set_account_utilization(&state, 0, 0.10, 0.10, now + 10000, now + 100000).await;
    set_account_utilization(&state, 1, 0.10, 0.10, now + 10000, now + 100000).await;

    // Set model-specific 7d utilization
    set_model_utilization(&state, 0, "claude-sonnet-4-6", 0.90, now + 100000).await;
    set_model_utilization(&state, 0, "claude-opus-4-6", 0.10, now + 100000).await;
    set_model_utilization(&state, 1, "claude-sonnet-4-6", 0.10, now + 100000).await;
    set_model_utilization(&state, 1, "claude-opus-4-6", 0.90, now + 100000).await;

    // Run many picks — Sonnet routing should consistently prefer B (index 1)
    let mut sonnet_picks = [0u32; 2];
    let mut opus_picks = [0u32; 2];
    for i in 0..100 {
        let key = format!("client_{}", i);
        if let Some(idx) = state
            .pick_endpoint(Some(&key), "claude-sonnet-4-6", &[])
            .await
        {
            sonnet_picks[idx] += 1;
        }
        if let Some(idx) = state
            .pick_endpoint(Some(&key), "claude-opus-4-6", &[])
            .await
        {
            opus_picks[idx] += 1;
        }
    }
    // B should get most Sonnet traffic (has lower Sonnet util)
    assert!(
        sonnet_picks[1] > sonnet_picks[0],
        "Sonnet should prefer B: A={}, B={}",
        sonnet_picks[0],
        sonnet_picks[1]
    );
    // A should get most Opus traffic (has lower Opus util)
    assert!(
        opus_picks[0] > opus_picks[1],
        "Opus should prefer A: A={}, B={}",
        opus_picks[0],
        opus_picks[1]
    );
}

// ── pick_account waste_risk routing tests ─────────────────────

#[tokio::test]
async fn pick_account_prefers_expiring_quota() {
    // Account A: 7d=0.40, reset in 1 day → high waste_risk
    // Account B: 7d=0.40, reset in 6 days → low waste_risk
    let acct_a = mk_endpoint("a", "sk-ant-api-a");
    let acct_b = mk_endpoint("b", "sk-ant-api-b");
    let state = test_state_with(vec![acct_a, acct_b]);
    let now = AppState::now_epoch();

    // Both 5h at 0.30
    set_account_utilization(&state, 0, 0.30, 0.40, now + 10000, now + 86400).await;
    set_account_utilization(&state, 1, 0.30, 0.40, now + 10000, now + 518400).await;

    // Override claims with different resets
    {
        let mut info = state.endpoints[0].rate_info.write().await;
        info.claims_7d.insert(
            "seven_day_sonnet".to_string(),
            ClaimWindowData {
                utilization: Some(0.40),
                reset: Some(now + 86400),
                status: None,
                ..Default::default()
            },
        );
    }
    {
        let mut info = state.endpoints[1].rate_info.write().await;
        info.claims_7d.insert(
            "seven_day_sonnet".to_string(),
            ClaimWindowData {
                utilization: Some(0.40),
                reset: Some(now + 518400),
                status: None,
                ..Default::default()
            },
        );
    }

    let mut picks = [0u32; 2];
    for i in 0..200 {
        let key = format!("client_{}", i);
        if let Some(idx) = state
            .pick_endpoint(Some(&key), "claude-sonnet-4-6", &[])
            .await
        {
            picks[idx] += 1;
        }
    }
    assert!(
        picks[0] > picks[1] * 2,
        "A (expiring) should get >2x traffic vs B: A={}, B={}",
        picks[0],
        picks[1]
    );
}

#[tokio::test]
async fn pick_fable_prefers_band_headroom() {
    // Two Max accounts, equal 5h and weekly pool; A's Fable band is nearly
    // spent (0.90), B's is roomy (0.20) → Fable traffic should prefer B.
    let acct_a = mk_endpoint("a", "sk-ant-api-a");
    let acct_b = mk_endpoint("b", "sk-ant-api-b");
    let state = test_state_with(vec![acct_a, acct_b]);
    let now = AppState::now_epoch();

    set_account_utilization(&state, 0, 0.30, 0.30, now + 10000, now + 302400).await;
    set_account_utilization(&state, 1, 0.30, 0.30, now + 10000, now + 302400).await;
    set_model_utilization(&state, 0, "claude-fable-5", 0.90, now + 302400).await;
    set_model_utilization(&state, 1, "claude-fable-5", 0.20, now + 302400).await;

    let mut picks = [0u32; 2];
    for i in 0..200 {
        let key = format!("client_{}", i);
        if let Some(idx) = state.pick_endpoint(Some(&key), "claude-fable-5", &[]).await {
            picks[idx] += 1;
        }
    }
    assert!(
        picks[1] > picks[0] * 2,
        "B (roomy fable band) should get >2x traffic vs A: A={}, B={}",
        picks[0],
        picks[1]
    );
}

#[tokio::test]
async fn pick_fable_capped_by_shared_pool() {
    // Fable shares the weekly pool: A's band is barely touched (0.10) but its
    // weekly pool is nearly drained (0.95). B is balanced (0.50/0.50).
    // Without the pool cap A's roomy band would win; with it, B must win.
    let acct_a = mk_endpoint("a", "sk-ant-api-a");
    let acct_b = mk_endpoint("b", "sk-ant-api-b");
    let state = test_state_with(vec![acct_a, acct_b]);
    let now = AppState::now_epoch();

    set_account_utilization(&state, 0, 0.30, 0.95, now + 10000, now + 302400).await;
    set_account_utilization(&state, 1, 0.30, 0.50, now + 10000, now + 302400).await;
    set_model_utilization(&state, 0, "claude-fable-5", 0.10, now + 302400).await;
    set_model_utilization(&state, 1, "claude-fable-5", 0.50, now + 302400).await;

    let mut picks = [0u32; 2];
    for i in 0..200 {
        let key = format!("client_{}", i);
        if let Some(idx) = state.pick_endpoint(Some(&key), "claude-fable-5", &[]).await {
            picks[idx] += 1;
        }
    }
    assert!(
        picks[1] > picks[0] * 2,
        "B should win — A's drained pool caps its roomy band: A={}, B={}",
        picks[0],
        picks[1]
    );
}

#[tokio::test]
async fn fable_band_rejected_skips_for_fable_only() {
    // Exhausted Fable band (rejected, no overage) → account skipped for Fable
    // requests but still fully routable for other families.
    let acct = mk_endpoint("a", "sk-ant-api-a");
    let state = test_state_with(vec![acct]);
    let now = AppState::now_epoch();

    set_account_utilization(&state, 0, 0.30, 0.30, now + 10000, now + 302400).await;
    {
        let mut info = state.endpoints[0].rate_info.write().await;
        info.claims_7d.insert(
            FABLE_BAND_CLAIM.to_string(),
            ClaimWindowData {
                utilization: Some(1.0),
                reset: Some(now + 302400),
                status: Some("rejected".to_string()),
                ..Default::default()
            },
        );
    }

    assert!(
        state
            .routing_candidates("claude-fable-5", &[])
            .await
            .is_empty(),
        "rejected band must skip the account for fable"
    );
    assert_eq!(
        state
            .routing_candidates("claude-sonnet-4-6", &[])
            .await
            .len(),
        1,
        "sonnet routing must be unaffected by the fable band"
    );
}

#[tokio::test]
async fn fable_pool_rejected_skips_fable_despite_roomy_band() {
    // The shared weekly pool is rejected — a roomy band claim must not keep
    // the account routable for Fable.
    let acct = mk_endpoint("a", "sk-ant-api-a");
    let state = test_state_with(vec![acct]);
    let now = AppState::now_epoch();

    {
        let mut info = state.endpoints[0].rate_info.write().await;
        info.utilization_5h = Some(0.30);
        info.reset_5h = Some(now + 10000);
        info.claims_7d.insert(
            "seven_day".to_string(),
            ClaimWindowData {
                utilization: Some(1.0),
                reset: Some(now + 302400),
                status: Some("rejected".to_string()),
                ..Default::default()
            },
        );
        info.claims_7d.insert(
            FABLE_BAND_CLAIM.to_string(),
            ClaimWindowData {
                utilization: Some(0.20),
                reset: Some(now + 302400),
                status: None,
                ..Default::default()
            },
        );
    }

    assert!(
        state
            .routing_candidates("claude-fable-5", &[])
            .await
            .is_empty(),
        "rejected weekly pool must skip the account for fable"
    );
}

#[tokio::test]
async fn fable_included_false_demotes_for_fable_only() {
    // A Pro-plan account (fable_included = false) is paid capacity for Fable
    // from the first token: demoted by overage_penalty for Fable requests,
    // untouched for everything else.
    let mut acct_a = mk_endpoint("a", "sk-ant-api-a");
    acct_a.fable_included = false;
    let acct_b = mk_endpoint("b", "sk-ant-api-b");
    let state = test_state_with(vec![acct_a, acct_b]);
    let now = AppState::now_epoch();

    set_account_utilization(&state, 0, 0.30, 0.30, now + 10000, now + 302400).await;
    set_account_utilization(&state, 1, 0.30, 0.30, now + 10000, now + 302400).await;

    let fable = state.routing_candidates("claude-fable-5", &[]).await;
    let a = fable.iter().find(|c| c.endpoint == 0).unwrap();
    let b = fable.iter().find(|c| c.endpoint == 1).unwrap();
    assert_eq!(
        a.priority, 10,
        "paid-fable account demoted by overage_penalty"
    );
    assert_eq!(b.priority, 0, "included account keeps its tier");

    let sonnet = state.routing_candidates("claude-sonnet-4-6", &[]).await;
    assert!(
        sonnet.iter().all(|c| c.priority == 0),
        "non-fable traffic must not see the demotion"
    );

    // Tier ordering: all Fable traffic lands on the included account.
    for i in 0..50 {
        let key = format!("client_{}", i);
        let idx = state
            .pick_endpoint(Some(&key), "claude-fable-5", &[])
            .await
            .unwrap();
        assert_eq!(idx, 1, "fable must drain included capacity first");
    }
}

#[tokio::test]
async fn pick_account_dampens_by_5h() {
    // Account A: high waste_risk but high 5h → dampened
    // Account B: lower waste_risk but low 5h → more traffic
    let acct_a = mk_endpoint("a", "sk-ant-api-a");
    let acct_b = mk_endpoint("b", "sk-ant-api-b");
    let state = test_state_with(vec![acct_a, acct_b]);
    let now = AppState::now_epoch();

    // A: 5h=0.85, 7d=0.20 (waste_risk ~5.0 with 1.5d remaining)
    set_account_utilization(&state, 0, 0.85, 0.20, now + 10000, now + 129600).await;
    {
        let mut info = state.endpoints[0].rate_info.write().await;
        info.claims_7d.insert(
            "seven_day_sonnet".to_string(),
            ClaimWindowData {
                utilization: Some(0.20),
                reset: Some(now + 129600),
                status: None,
                ..Default::default()
            },
        );
    }

    // B: 5h=0.30, 7d=0.50 (waste_risk ~1.2 with 3.5d remaining)
    set_account_utilization(&state, 1, 0.30, 0.50, now + 10000, now + 302400).await;
    {
        let mut info = state.endpoints[1].rate_info.write().await;
        info.claims_7d.insert(
            "seven_day_sonnet".to_string(),
            ClaimWindowData {
                utilization: Some(0.50),
                reset: Some(now + 302400),
                status: None,
                ..Default::default()
            },
        );
    }

    let mut picks = [0u32; 2];
    for i in 0..500 {
        let key = format!("client_{}", i);
        if let Some(idx) = state
            .pick_endpoint(Some(&key), "claude-sonnet-4-6", &[])
            .await
        {
            picks[idx] += 1;
        }
    }
    // A: wr=0.80/0.2143=3.73, weight=3.73*0.15=0.56
    // B: wr=0.50/0.50=1.0, weight=1.0*0.70=0.70
    // B should get more (~55.6% share)
    assert!(
        picks[1] > picks[0],
        "B (low 5h) should get more traffic: A={}, B={}",
        picks[0],
        picks[1]
    );
}

#[tokio::test]
async fn pick_account_fallback_no_7d_data() {
    // No 7d claims → falls back to headroom-only weighting
    // Uses affinity (sticky) traffic to verify weight-proportional distribution
    // across distinct session keys, exercising the affinity path.
    let acct_a = mk_endpoint("a", "sk-ant-api-a");
    let acct_b = mk_endpoint("b", "sk-ant-api-b");
    let state = test_state_with(vec![acct_a, acct_b]);
    let now = AppState::now_epoch();

    // Set 5h only, no claims_7d
    {
        let mut info = state.endpoints[0].rate_info.write().await;
        info.utilization_5h = Some(0.30);
        info.reset_5h = Some(now + 10000);
        info.utilization = Some(0.30);
        info.claims_7d.clear();
    }
    {
        let mut info = state.endpoints[1].rate_info.write().await;
        info.utilization_5h = Some(0.70);
        info.reset_5h = Some(now + 10000);
        info.utilization = Some(0.70);
        info.claims_7d.clear();
    }

    // Each distinct session key deterministically hashes into the weight-space;
    // 200 different keys should distribute ~70/30 matching A's higher headroom.
    let mut picks = [0u32; 2];
    for i in 0..200 {
        let key = format!("session-{}", i);
        if let Some(idx) = state
            .pick_endpoint(Some(&key), "claude-sonnet-4-6", &[])
            .await
        {
            picks[idx] += 1;
        }
    }

    // Verify the affinity path returns Some for a well-formed session key
    let session = "10.42.0.1:client-test:agent-1:session-fallback";
    assert!(
        state
            .pick_endpoint(Some(session), "claude-sonnet-4-6", &[])
            .await
            .is_some(),
        "affinity pick should return Some when accounts are available"
    );

    // A headroom=0.70, B headroom=0.30. A should get ~70% of 200 = ~140 picks.
    // Require at least 65% (130) to catch regressions while allowing hash variance.
    assert!(
        picks[0] >= 130,
        "expected A to get ~70% but got A={}, B={}",
        picks[0],
        picks[1]
    );
}

#[tokio::test]
async fn emergency_brake_triggers_at_88() {
    // All accounts at 88% raw 7d → brake engages with new threshold
    let now = AppState::now_epoch();
    let acct_a = mk_endpoint("a", "sk-ant-api-a");
    let acct_b = mk_endpoint("b", "sk-ant-api-b");
    let state = test_state_with(vec![acct_a, acct_b]);

    // Verify DEFAULT_EMERGENCY_THRESHOLD is 0.88
    assert!(
        (DEFAULT_EMERGENCY_THRESHOLD - 0.88).abs() < 0.001,
        "default emergency threshold should be 0.88"
    );

    set_account_utilization(&state, 0, 0.89, 0.89, now + 10000, now + 100000).await;
    set_account_utilization(&state, 1, 0.89, 0.89, now + 10000, now + 100000).await;

    // effective_utilization for both should be >= 0.88
    let info0 = state.endpoints[0].rate_info.read().await;
    let (util0, _, _, _) = effective_utilization(&info0, now, "");
    drop(info0);
    assert!(
        util0 >= DEFAULT_EMERGENCY_THRESHOLD,
        "util should be >= threshold: {util0}"
    );
}

#[tokio::test]
async fn emergency_brake_fires_when_only_anthropic_above_threshold_with_openai_present() {
    // 1 anthropic account at utilization 0.95, 1 openai endpoint with stub rate info.
    // A naive "iterate all endpoints" version sees the openai stub at (0.5, "unknown")
    // and forces all_above = false → brake never fires. The correct "skip OpenAI"
    // version excludes it and the brake fires.
    let acct = mk_endpoint("anthropic", "sk-ant");
    {
        let mut info = acct.rate_info.write().await;
        info.utilization = Some(0.95);
        info.utilization_5h = Some(0.95);
    }
    let mut state = test_state_with(vec![acct]);
    let st = Arc::get_mut(&mut state).expect("uniquely owned");
    let mut openai_ep = make_endpoint("openai", Protocol::OpenAI);
    openai_ep.priority = 100;
    st.endpoints.push(openai_ep);
    st.emergency_threshold = 0.88;
    assert!(
        state.is_emergency_brake_active().await,
        "brake must fire: anthropic is above threshold; openai must not vote"
    );
}

#[tokio::test]
async fn probe_endpoint_skips_openai() {
    // The mock upstream injects `5h-utilization: 0.25` headers. If the probe
    // ran, `rate_info.utilization_5h` would become Some(0.25). The OpenAI
    // skip means the endpoint is never contacted and rate_info stays None.
    let (mock_url, _h) = spawn_mock_upstream().await;
    let mut ep = make_endpoint("openai", Protocol::OpenAI);
    ep.base_url = mock_url;
    ep.priority = 100;
    let mut state = test_state_with(vec![]);
    Arc::get_mut(&mut state).unwrap().endpoints.push(ep);

    state.probe_endpoint(0, "claude-haiku-4-5").await;

    // rate_info must be untouched: the OpenAI endpoint was skipped, no HTTP
    // call was made. A naive "probe all endpoints" version would have hit
    // the mock and set utilization_5h to Some(0.25).
    let info = state.endpoints[0].rate_info.read().await;
    assert!(
        info.utilization_5h.is_none(),
        "probe must short-circuit for OpenAI endpoints — rate_info must stay untouched"
    );
    assert_eq!(state.endpoints[0].requests.load(Ordering::Relaxed), 0);
}

#[tokio::test]
async fn pick_account_all_7d_rejected_returns_none() {
    // If all accounts have rejected 7d claims for the model, pick_account returns None
    let acct_a = mk_endpoint("a", "sk-ant-api-a");
    let acct_b = mk_endpoint("b", "sk-ant-api-b");
    let state = test_state_with(vec![acct_a, acct_b]);
    let now = AppState::now_epoch();

    for idx in 0..2 {
        let mut info = state.endpoints[idx].rate_info.write().await;
        info.utilization_5h = Some(0.30);
        info.reset_5h = Some(now + 10000);
        info.claims_7d.insert(
            "seven_day_sonnet".to_string(),
            ClaimWindowData {
                utilization: Some(1.0),
                reset: Some(now + 100000),
                status: Some("rejected".to_string()),
                ..Default::default()
            },
        );
    }

    let result = state
        .pick_endpoint(Some("test"), "claude-sonnet-4-6", &[])
        .await;
    assert!(
        result.is_none(),
        "all-rejected should return None, got {:?}",
        result
    );
}

// ── Enforcement tests ──────────────────────────────────────────

/// Helper: set up account utilization for enforcement tests.
async fn set_account_utilization(
    state: &AppState,
    idx: usize,
    util_5h: f64,
    util_7d: f64,
    reset_5h: u64,
    reset_7d: u64,
) {
    let mut info = state.endpoints[idx].rate_info.write().await;
    info.utilization_5h = Some(util_5h);
    info.utilization_7d = Some(util_7d);
    info.utilization = Some(util_5h.max(util_7d));
    info.reset_5h = Some(reset_5h);
    info.reset_7d = Some(reset_7d);
    // Populate claims_7d with a general "seven_day" entry
    info.claims_7d.insert(
        "seven_day".to_string(),
        ClaimWindowData {
            utilization: Some(util_7d),
            reset: Some(reset_7d),
            status: None,
            ..Default::default()
        },
    );
}

/// Helper: set per-model 7d utilization (e.g. "seven_day_sonnet").
async fn set_model_utilization(
    state: &AppState,
    idx: usize,
    model: &str,
    util_7d: f64,
    reset_7d: u64,
) {
    let family = model_family(model);
    let key = if family.is_empty() {
        "seven_day".to_string()
    } else {
        format!("seven_day_{}", family)
    };
    let mut info = state.endpoints[idx].rate_info.write().await;
    info.claims_7d.insert(
        key,
        ClaimWindowData {
            utilization: Some(util_7d),
            reset: Some(reset_7d),
            status: None,
            ..Default::default()
        },
    );
    // Re-derive flat fields
    info.utilization_7d = info
        .claims_7d
        .values()
        .filter_map(|c| c.utilization)
        .reduce(f64::max);
    info.reset_7d = info.claims_7d.values().filter_map(|c| c.reset).min();
    info.utilization = Some(
        info.utilization_5h
            .unwrap_or(0.0)
            .max(info.utilization_7d.unwrap_or(0.0)),
    );
}

#[tokio::test]
async fn limit_all_below() {
    let now = AppState::now_epoch();
    let mut limits = HashMap::new();
    limits.insert("testclient".to_string(), 0.80);
    let state = Arc::new(AppState {
        endpoints: vec![
            mk_endpoint("a", "sk-ant-api-x"),
            mk_endpoint("b", "sk-ant-api-y"),
        ],
        state_path: PathBuf::from("/tmp/test.state.json"),
        client_utilization_limits: limits,
        ..test_state_base()
    });
    set_account_utilization(&state, 0, 0.50, 0.40, now + 10000, now + 100000).await;
    set_account_utilization(&state, 1, 0.60, 0.50, now + 10000, now + 100000).await;
    assert!(state
        .check_utilization_limit("testclient", "")
        .await
        .is_ok());
}

#[tokio::test]
async fn limit_all_above() {
    let now = AppState::now_epoch();
    let mut limits = HashMap::new();
    limits.insert("testclient".to_string(), 0.50);
    let state = Arc::new(AppState {
        endpoints: vec![
            mk_endpoint("a", "sk-ant-api-x"),
            mk_endpoint("b", "sk-ant-api-y"),
        ],
        state_path: PathBuf::from("/tmp/test.state.json"),
        client_utilization_limits: limits,
        ..test_state_base()
    });
    set_account_utilization(&state, 0, 0.80, 0.70, now + 10000, now + 100000).await;
    set_account_utilization(&state, 1, 0.90, 0.80, now + 10000, now + 100000).await;
    let result = state.check_utilization_limit("testclient", "").await;
    assert!(result.is_err(), "all above limit should return Err");
    let retry = result.unwrap_err();
    assert!(retry >= 60, "retry-after should be >= 60");
    assert!(retry <= 3600, "retry-after should be <= 3600");
}

#[tokio::test]
async fn limit_one_below() {
    let now = AppState::now_epoch();
    let mut limits = HashMap::new();
    limits.insert("testclient".to_string(), 0.70);
    let state = Arc::new(AppState {
        endpoints: vec![
            mk_endpoint("a", "sk-ant-api-x"),
            mk_endpoint("b", "sk-ant-api-y"),
        ],
        state_path: PathBuf::from("/tmp/test.state.json"),
        client_utilization_limits: limits,
        ..test_state_base()
    });
    set_account_utilization(&state, 0, 0.90, 0.80, now + 10000, now + 100000).await;
    set_account_utilization(&state, 1, 0.50, 0.40, now + 10000, now + 100000).await;
    assert!(state
        .check_utilization_limit("testclient", "")
        .await
        .is_ok());
}

#[tokio::test]
async fn limit_no_config() {
    let state = test_state_with(vec![mk_endpoint("a", "sk-ant-api-x")]);
    assert!(state.check_utilization_limit("anyone", "").await.is_ok());
}

#[tokio::test]
async fn limit_operator_bypass() {
    let now = AppState::now_epoch();
    let mut limits = HashMap::new();
    limits.insert("ray".to_string(), 0.10); // very low limit
    let state = Arc::new(AppState {
        endpoints: vec![mk_endpoint("a", "sk-ant-api-x")],
        state_path: PathBuf::from("/tmp/test.state.json"),
        client_utilization_limits: limits,
        operators: vec!["ray".to_string()],
        ..test_state_base()
    });
    set_account_utilization(&state, 0, 0.95, 0.90, now + 10000, now + 100000).await;
    // Operator bypasses everything
    assert!(state.is_operator("ray"));
    assert!(state.pre_request_gate("ray", "").await.is_ok());
    // Non-operator does not bypass
    assert!(!state.is_operator("gastown"));
}

#[tokio::test]
async fn limit_no_compatible_accounts_passes() {
    // When no account serves the requested model, check_utilization_limit
    // should return Ok (let pick_account handle the "no account" error later)
    let mut limits = HashMap::new();
    limits.insert("test-client".to_string(), 0.01); // very low limit
    let mut acct = mk_endpoint("a", "sk-ant-api-x");
    acct.models = vec!["claude-sonnet".to_string()]; // only serves sonnet
    let state = Arc::new(AppState {
        endpoints: vec![acct],
        state_path: PathBuf::from("/tmp/test.state.json"),
        client_utilization_limits: limits,
        ..test_state_base()
    });
    // Request for "claude-opus" — no account serves it → should pass
    assert!(
        state
            .check_utilization_limit("test-client", "claude-opus")
            .await
            .is_ok(),
        "should not 429 when no account serves the requested model"
    );
}

#[tokio::test]
async fn limit_unknown_accounts_fail_open() {
    // Accounts with no rate data (source="unknown") should not trigger the limit gate
    let mut limits = HashMap::new();
    limits.insert("testclient".to_string(), 0.30); // below the 0.5 unknown default
    let state = Arc::new(AppState {
        endpoints: vec![
            mk_endpoint("a", "sk-ant-api-x"),
            mk_endpoint("b", "sk-ant-api-y"),
        ],
        state_path: PathBuf::from("/tmp/test.state.json"),
        client_utilization_limits: limits,
        ..test_state_base()
    });
    // Don't set any utilization — accounts remain "unknown" (0.5)
    // With limit=0.30, unknown 0.5 would appear "above limit" without fail-open
    assert!(
        state
            .check_utilization_limit("testclient", "")
            .await
            .is_ok(),
        "should fail-open when all accounts have unknown utilization"
    );
}

#[tokio::test]
async fn limit_mixed_known_unknown_fails_open() {
    // Known accounts above limit + one unknown account → should NOT 429
    // The unknown account may have capacity; let pick_account route to it
    let now = AppState::now_epoch();
    let mut limits = HashMap::new();
    limits.insert("testclient".to_string(), 0.50);
    let state = Arc::new(AppState {
        endpoints: vec![
            mk_endpoint("a", "sk-ant-api-x"),
            mk_endpoint("b", "sk-ant-api-y"),
            mk_endpoint("c", "sk-ant-api-z"),
        ],
        state_path: PathBuf::from("/tmp/test.state.json"),
        client_utilization_limits: limits,
        ..test_state_base()
    });
    // Two known accounts above limit, one unknown (no data set)
    set_account_utilization(&state, 0, 0.80, 0.70, now + 10000, now + 100000).await;
    set_account_utilization(&state, 1, 0.90, 0.80, now + 10000, now + 100000).await;
    // Account "c" has no rate data → unknown
    assert!(
        state
            .check_utilization_limit("testclient", "")
            .await
            .is_ok(),
        "should fail-open when unknown compatible account may have capacity"
    );
}

#[tokio::test]
async fn emergency_all_above_threshold() {
    let now = AppState::now_epoch();
    let state = test_state_with(vec![
        mk_endpoint("a", "sk-ant-api-x"),
        mk_endpoint("b", "sk-ant-api-y"),
    ]);
    set_account_utilization(&state, 0, 0.96, 0.90, now + 10000, now + 100000).await;
    set_account_utilization(&state, 1, 0.97, 0.95, now + 10000, now + 100000).await;
    assert!(state.is_emergency_brake_active().await);
}

#[tokio::test]
async fn emergency_one_below() {
    let now = AppState::now_epoch();
    let state = test_state_with(vec![
        mk_endpoint("a", "sk-ant-api-x"),
        mk_endpoint("b", "sk-ant-api-y"),
    ]);
    set_account_utilization(&state, 0, 0.96, 0.90, now + 10000, now + 100000).await;
    set_account_utilization(&state, 1, 0.80, 0.70, now + 10000, now + 100000).await;
    assert!(!state.is_emergency_brake_active().await);
}

#[tokio::test]
async fn emergency_operator_bypass() {
    let now = AppState::now_epoch();
    let mut limits = HashMap::new();
    limits.insert("ray".to_string(), 0.10);
    let state = Arc::new(AppState {
        endpoints: vec![mk_endpoint("a", "sk-ant-api-x")],
        state_path: PathBuf::from("/tmp/test.state.json"),
        client_utilization_limits: limits,
        operators: vec!["ray".to_string()],
        ..test_state_base()
    });
    set_account_utilization(&state, 0, 0.98, 0.96, now + 10000, now + 100000).await;
    assert!(state.is_emergency_brake_active().await);
    // Operator bypasses pre_request_gate even during emergency
    assert!(state.pre_request_gate("ray", "").await.is_ok());
    // Non-operator gets blocked
    assert!(state.pre_request_gate("gastown", "").await.is_err());
}

#[tokio::test]
async fn emergency_no_data() {
    // All accounts have default (0.5, "unknown") — brake should NOT activate (fail-open)
    let state = test_state_with(vec![
        mk_endpoint("a", "sk-ant-api-x"),
        mk_endpoint("b", "sk-ant-api-y"),
    ]);
    assert!(
        !state.is_emergency_brake_active().await,
        "brake should fail-open with no data"
    );
}

#[tokio::test]
async fn emergency_stale_data_with_unified() {
    // Stale reset times but valid unified utilization at 0.97
    let state = test_state_with(vec![mk_endpoint("a", "sk-ant-api-x")]);
    {
        let mut info = state.endpoints[0].rate_info.write().await;
        // Resets in the past → stale per-window data
        info.utilization_5h = Some(0.97);
        info.reset_5h = Some(1);
        info.utilization_7d = Some(0.97);
        info.reset_7d = Some(1);
        // But unified utilization is valid
        info.utilization = Some(0.97);
    }
    assert!(
        state.is_emergency_brake_active().await,
        "unified fallback should count"
    );
}

#[tokio::test]
async fn emergency_configurable_threshold() {
    let now = AppState::now_epoch();
    let state = Arc::new(AppState {
        endpoints: vec![mk_endpoint("a", "sk-ant-api-x")],
        state_path: PathBuf::from("/tmp/test.state.json"),
        emergency_threshold: 0.80, // custom low threshold
        ..test_state_base()
    });
    set_account_utilization(&state, 0, 0.85, 0.70, now + 10000, now + 100000).await;
    assert!(
        state.is_emergency_brake_active().await,
        "0.85 should exceed custom 0.80 threshold"
    );
}

// ── Emergency brake: known/unknown interaction tests ──
// These tests document the quota-maximization design intent:
// Unknown accounts (no rate data) return (0.5, "unknown") from effective_utilization.
// The brake does NOT fire when unknown accounts exist because:
//   (a) 0.5 < 0.88 default threshold → all_above = false, OR
//   (b) even if threshold is low enough, any_known = false blocks activation.
// This is intentional: unknown accounts might have capacity, and activating
// the brake blocks ALL non-operator traffic — a blunt instrument that wastes quota.

#[tokio::test]
async fn emergency_mixed_known_above_plus_unknown_preserves_capacity() {
    // Two known accounts above threshold + one unknown (no data).
    // Unknown returns (0.5, "unknown") — its 0.5 < 0.88 breaks the all_above check.
    // Brake stays inactive: the unknown account might have available quota.
    let now = AppState::now_epoch();
    let state = test_state_with(vec![
        mk_endpoint("known-a", "sk-ant-api-a"),
        mk_endpoint("known-b", "sk-ant-api-b"),
        mk_endpoint("unknown-c", "sk-ant-api-c"), // no rate data set
    ]);
    set_account_utilization(&state, 0, 0.96, 0.92, now + 10000, now + 100000).await;
    set_account_utilization(&state, 1, 0.95, 0.91, now + 10000, now + 100000).await;
    // Account 2: no data → effective_utilization returns (0.5, "unknown")
    assert!(
        !state.is_emergency_brake_active().await,
        "brake must not fire: unknown account at 0.5 < 0.88 threshold means potential capacity"
    );
}

#[tokio::test]
async fn emergency_mixed_known_below_plus_unknown_inactive() {
    // Some known above, some known below, plus an unknown. Brake inactive on multiple grounds.
    let now = AppState::now_epoch();
    let state = test_state_with(vec![
        mk_endpoint("high", "sk-ant-api-a"),
        mk_endpoint("low", "sk-ant-api-b"),
        mk_endpoint("unknown", "sk-ant-api-c"),
    ]);
    set_account_utilization(&state, 0, 0.96, 0.92, now + 10000, now + 100000).await;
    set_account_utilization(&state, 1, 0.50, 0.40, now + 10000, now + 100000).await;
    // Account 2: no data
    assert!(
        !state.is_emergency_brake_active().await,
        "brake must not fire: known account below threshold + unknown account"
    );
}

#[tokio::test]
async fn emergency_unknown_with_low_threshold_still_fails_open() {
    // Edge case: threshold set to 0.4, below the unknown default of 0.5.
    // Unknown's 0.5 >= 0.4 so all_above stays true, BUT any_known is false.
    // The any_known guard prevents firing — fail-open even with aggressive threshold.
    let state = Arc::new(AppState {
        endpoints: vec![mk_endpoint("mystery", "sk-ant-api-x")],
        state_path: PathBuf::from("/tmp/test.state.json"),
        emergency_threshold: 0.40, // below unknown's default 0.5
        ..test_state_base()
    });
    // No rate data set — account returns (0.5, "unknown")
    // 0.5 >= 0.4 threshold → all_above is true
    // But any_known is false → brake does NOT fire
    assert!(
        !state.is_emergency_brake_active().await,
        "brake must fail-open: no known accounts even though unknown's 0.5 exceeds 0.40 threshold"
    );
}

#[tokio::test]
async fn emergency_mixed_known_above_low_threshold_plus_unknown_fires() {
    // Converse of above: threshold 0.4, one KNOWN account at 0.6, one unknown at default 0.5.
    // Both 0.6 >= 0.4 and 0.5 >= 0.4 → all_above = true.
    // Known account exists → any_known = true.
    // Brake fires. This is correct because we have real data showing distress.
    let now = AppState::now_epoch();
    let state = Arc::new(AppState {
        endpoints: vec![
            mk_endpoint("known", "sk-ant-api-a"),
            mk_endpoint("unknown", "sk-ant-api-b"),
        ],
        state_path: PathBuf::from("/tmp/test.state.json"),
        emergency_threshold: 0.40,
        ..test_state_base()
    });
    set_account_utilization(&state, 0, 0.60, 0.55, now + 10000, now + 100000).await;
    // Account 1: no data → (0.5, "unknown"), 0.5 >= 0.4 → all_above stays true
    assert!(
        state.is_emergency_brake_active().await,
        "brake should fire: known account above 0.40 threshold + unknown's 0.5 also above"
    );
}

#[tokio::test]
async fn emergency_all_known_at_exact_threshold_fires() {
    // Boundary: accounts at exactly the threshold. The check is `util < threshold`,
    // so util == threshold means NOT below → all_above stays true → brake fires.
    let now = AppState::now_epoch();
    let state = test_state_with(vec![
        mk_endpoint("a", "sk-ant-api-a"),
        mk_endpoint("b", "sk-ant-api-b"),
    ]);
    set_account_utilization(&state, 0, 0.88, 0.88, now + 10000, now + 100000).await;
    set_account_utilization(&state, 1, 0.88, 0.88, now + 10000, now + 100000).await;
    assert!(
        state.is_emergency_brake_active().await,
        "at exactly threshold (0.88): util is NOT < threshold, so brake should fire"
    );
}

#[tokio::test]
async fn emergency_one_known_just_below_threshold_inactive() {
    // Boundary: one account at threshold - epsilon. Just below → all_above = false.
    let now = AppState::now_epoch();
    let state = test_state_with(vec![
        mk_endpoint("a", "sk-ant-api-a"),
        mk_endpoint("b", "sk-ant-api-b"),
    ]);
    set_account_utilization(&state, 0, 0.96, 0.92, now + 10000, now + 100000).await;
    set_account_utilization(&state, 1, 0.879, 0.85, now + 10000, now + 100000).await;
    assert!(
        !state.is_emergency_brake_active().await,
        "0.879 < 0.88 threshold: brake should not fire"
    );
}

#[tokio::test]
async fn emergency_single_known_above_threshold_fires() {
    // Single account fleet, known and above threshold → brake fires.
    let now = AppState::now_epoch();
    let state = test_state_with(vec![mk_endpoint("solo", "sk-ant-api-x")]);
    set_account_utilization(&state, 0, 0.95, 0.92, now + 10000, now + 100000).await;
    assert!(
        state.is_emergency_brake_active().await,
        "single known account above threshold: brake should fire"
    );
}

#[tokio::test]
async fn emergency_single_unknown_account_fails_open() {
    // Single unknown account — both guards prevent activation:
    // 0.5 < 0.88 → all_above = false, AND any_known = false.
    let state = test_state_with(vec![mk_endpoint("solo", "sk-ant-api-x")]);
    assert!(
        !state.is_emergency_brake_active().await,
        "single unknown account: must fail-open"
    );
}

#[tokio::test]
async fn gate_unknown_client_not_operator() {
    let state = Arc::new(AppState {
        endpoints: vec![mk_endpoint("a", "sk-ant-api-x")],
        state_path: PathBuf::from("/tmp/test.state.json"),
        operators: vec!["ray".to_string()],
        ..test_state_base()
    });
    // "-" is not the operator
    assert!(!state.is_operator("-"));
    assert!(!state.is_operator("gastown"));
    assert!(state.is_operator("ray"));
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

// ── Integration tests for x-budget-status header ──

#[tokio::test]
async fn response_includes_budget_status_header() {
    let (mock_url, _handle) = spawn_mock_upstream().await;
    let (app, _state) = test_app(&mock_url, Some("test-key".to_string()));

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(
            listener,
            app.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await
        .unwrap();
    });

    let client = Client::new();
    let resp = client
        .post(format!("http://{}/v1/messages", addr))
        .header("content-type", "application/json")
        .header("x-api-key", "test-key")
        .body(r#"{"model":"test","max_tokens":1,"messages":[{"role":"user","content":"hi"}]}"#)
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let budget_status = resp
        .headers()
        .get("x-budget-status")
        .expect("x-budget-status header should be present on proxy response");
    // Mock returns low utilization (0.25) → healthy
    assert_eq!(budget_status.to_str().unwrap(), "healthy");
}

#[tokio::test]
async fn openai_response_includes_budget_status_header() {
    let (mock_url, _handle) = spawn_mock_upstream().await;
    let (app, _state) = test_openai_app(&mock_url, Some("test-key".to_string()));

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(
            listener,
            app.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await
        .unwrap();
    });

    let client = Client::new();
    let resp = client
        .post(format!("http://{}/v1/chat/completions", addr))
        .header("content-type", "application/json")
        .header("x-api-key", "test-key")
        .body(r#"{"model":"test","max_tokens":1,"messages":[{"role":"user","content":"hi"}]}"#)
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let budget_status = resp
        .headers()
        .get("x-budget-status")
        .expect("x-budget-status header should be present on openai-compat response");
    assert_eq!(budget_status.to_str().unwrap(), "healthy");
}

// ── Stats handler extension tests ──

#[tokio::test]
async fn stats_includes_burn_rate_and_headroom() {
    let (mock_url, _handle) = spawn_mock_upstream().await;
    let (app, _state) = test_app(&mock_url, Some("test-key".to_string()));

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(
            listener,
            app.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await
        .unwrap();
    });

    let client = Client::new();

    // Send a request to populate rate info and burn rate
    let _ = client
        .post(format!("http://{}/v1/messages", addr))
        .header("content-type", "application/json")
        .header("x-api-key", "test-key")
        .body(r#"{"model":"test","max_tokens":1,"messages":[{"role":"user","content":"hi"}]}"#)
        .send()
        .await
        .unwrap();

    // Now check stats
    let resp = client
        .get(format!("http://{}/_stats", addr))
        .header("x-api-key", "test-key")
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let body: serde_json::Value = resp.json().await.unwrap();
    let endpoints = body["endpoints"].as_array().unwrap();
    assert!(!endpoints.is_empty());

    // burn_rate object should exist on every endpoint
    let acct = &endpoints[0];
    assert!(
        acct["burn_rate"].is_object(),
        "burn_rate should be an object"
    );
    assert!(acct["burn_rate"]["last_5m"].is_number());
    assert!(acct["burn_rate"]["last_1h"].is_number());
    assert!(acct["burn_rate"]["last_6h"].is_number());

    // headroom_requests: mock doesn't return remaining_requests or limit_requests,
    // so headroom is null (both inputs absent). That's the expected behavior.
    // The field should still exist in the JSON output.
    assert!(
        acct.get("headroom_requests").is_some(),
        "headroom_requests field should be present"
    );

    // projected_throttle_at should be present (null or string depending on utilization)
    assert!(
        acct["projected_throttle_at"].is_null() || acct["projected_throttle_at"].is_string(),
        "projected_throttle_at should be null or ISO 8601 string"
    );

    // aggregate section
    assert!(
        body["aggregate"].is_object(),
        "aggregate section should exist"
    );
    assert!(
        body["aggregate"].get("total_headroom_requests").is_some(),
        "total_headroom_requests field should be present"
    );
    assert!(body["aggregate"]["consumers"].is_object());
}

#[test]
fn epoch_to_iso8601_known_values() {
    // 2024-01-01T00:00:00Z = 1704067200
    assert_eq!(
        AppState::epoch_to_iso8601(1704067200),
        "2024-01-01T00:00:00Z"
    );
    // Unix epoch
    assert_eq!(AppState::epoch_to_iso8601(0), "1970-01-01T00:00:00Z");
    // 2026-02-14T12:30:45Z = approximate check
    let result = AppState::epoch_to_iso8601(1771157445);
    assert!(
        result.starts_with("2026-02-"),
        "expected 2026-02, got {result}"
    );
    assert!(result.ends_with('Z'));
}

// ── Task 7: Full integration tests ──

#[tokio::test]
async fn request_rejected_by_utilization_limit() {
    let (mock_url, _handle) = spawn_mock_upstream().await;
    let now = AppState::now_epoch();
    let mut limits = HashMap::new();
    limits.insert("-".to_string(), 0.50); // default client gets 0.50 limit
    let state = Arc::new(AppState {
        endpoints: vec![mk_endpoint_at("a", "sk-ant-api-test-aaa", &mock_url)],
        state_path: PathBuf::from("/tmp/test-limit-reject.state.json"),
        proxy_key: Some("key".to_string()),
        auto_cache: false,
        client_utilization_limits: limits,
        ..test_state_base()
    });
    // Set utilization above client's limit (0.80 > 0.50)
    set_account_utilization(&state, 0, 0.80, 0.70, now + 10000, now + 100000).await;

    let app = build_router(state);
    let addr = serve(app).await;

    let client = Client::new();
    let resp = client
        .post(format!("http://{}/v1/messages", addr))
        .header("content-type", "application/json")
        .header("x-api-key", "key")
        .body(r#"{"model":"test","max_tokens":1,"messages":[{"role":"user","content":"hi"}]}"#)
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), reqwest::StatusCode::TOO_MANY_REQUESTS);
    assert!(
        resp.headers().get("retry-after").is_some(),
        "429 from utilization limit should include Retry-After"
    );
}

#[tokio::test]
async fn request_passes_utilization_limit() {
    let (mock_url, _handle) = spawn_mock_upstream().await;
    let now = AppState::now_epoch();
    let mut limits = HashMap::new();
    limits.insert("-".to_string(), 0.90);
    let state = Arc::new(AppState {
        endpoints: vec![mk_endpoint_at("a", "sk-ant-api-test-aaa", &mock_url)],
        state_path: PathBuf::from("/tmp/test-limit-pass.state.json"),
        proxy_key: Some("key".to_string()),
        auto_cache: false,
        client_utilization_limits: limits,
        ..test_state_base()
    });
    // Set utilization below client's limit (0.50 < 0.90)
    set_account_utilization(&state, 0, 0.50, 0.40, now + 10000, now + 100000).await;

    let app = build_router(state);
    let addr = serve(app).await;

    let client = Client::new();
    let resp = client
        .post(format!("http://{}/v1/messages", addr))
        .header("content-type", "application/json")
        .header("x-api-key", "key")
        .body(r#"{"model":"test","max_tokens":1,"messages":[{"role":"user","content":"hi"}]}"#)
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), reqwest::StatusCode::OK);
}

#[tokio::test]
async fn emergency_brake_blocks_non_operator() {
    let (mock_url, _handle) = spawn_mock_upstream().await;
    let now = AppState::now_epoch();
    let state = Arc::new(AppState {
        endpoints: vec![
            mk_endpoint_at("a", "sk-ant-api-test-aaa", &mock_url),
            mk_endpoint_at("b", "sk-ant-api-test-bbb", &mock_url),
        ],
        state_path: PathBuf::from("/tmp/test-emergency-block.state.json"),
        proxy_key: Some("key".to_string()),
        auto_cache: false,
        operators: vec!["ray".to_string()],
        ..test_state_base()
    });
    // All accounts above emergency threshold. 5h=0.96 > emergency threshold (0.88).
    set_account_utilization(&state, 0, 0.96, 0.0, now + 10000, now + 100000).await;
    set_account_utilization(&state, 1, 0.97, 0.0, now + 10000, now + 100000).await;

    let app = build_router(state);
    let addr = serve(app).await;

    let client = Client::new();
    let resp = client
        .post(format!("http://{}/v1/messages", addr))
        .header("content-type", "application/json")
        .header("x-api-key", "key")
        .body(r#"{"model":"test","max_tokens":1,"messages":[{"role":"user","content":"hi"}]}"#)
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), reqwest::StatusCode::TOO_MANY_REQUESTS);
    let body = resp.text().await.unwrap();
    assert!(
        body.contains("emergency"),
        "emergency brake response should mention 'emergency': {body}"
    );
}

#[tokio::test]
async fn emergency_brake_allows_operator() {
    let (mock_url, _handle) = spawn_mock_upstream().await;
    let now = AppState::now_epoch();
    let mut client_names = HashMap::new();
    client_names.insert("127.0.0.1".to_string(), "ray".to_string());
    let state = Arc::new(AppState {
        endpoints: vec![
            mk_endpoint_at("a", "sk-ant-api-test-aaa", &mock_url),
            mk_endpoint_at("b", "sk-ant-api-test-bbb", &mock_url),
        ],
        state_path: PathBuf::from("/tmp/test-emergency-operator.state.json"),
        proxy_key: Some("key".to_string()),
        client_names,
        auto_cache: false,
        operators: vec!["ray".to_string()],
        ..test_state_base()
    });
    // All accounts above emergency threshold — 5h only (avoid claim penalty on 7d)
    set_account_utilization(&state, 0, 0.96, 0.0, now + 10000, now + 100000).await;
    set_account_utilization(&state, 1, 0.97, 0.0, now + 10000, now + 100000).await;

    let app = build_router(state);
    let addr = serve(app).await;

    // Request comes from 127.0.0.1 which maps to "ray" (the operator)
    let client = Client::new();
    let resp = client
        .post(format!("http://{}/v1/messages", addr))
        .header("content-type", "application/json")
        .header("x-api-key", "key")
        .body(r#"{"model":"test","max_tokens":1,"messages":[{"role":"user","content":"hi"}]}"#)
        .send()
        .await
        .unwrap();

    assert_eq!(
        resp.status(),
        reqwest::StatusCode::OK,
        "operator should bypass emergency brake"
    );
}

#[tokio::test]
async fn openai_handler_enforces_utilization_limit() {
    let (mock_url, _handle) = spawn_mock_upstream().await;
    let now = AppState::now_epoch();
    let mut limits = HashMap::new();
    limits.insert("-".to_string(), 0.50);
    let state = Arc::new(AppState {
        endpoints: vec![mk_endpoint_at("a", "sk-ant-api-test-aaa", &mock_url)],
        state_path: PathBuf::from("/tmp/test-openai-limit.state.json"),
        proxy_key: Some("key".to_string()),
        auto_cache: false,
        client_utilization_limits: limits,
        ..test_state_base()
    });
    set_account_utilization(&state, 0, 0.80, 0.70, now + 10000, now + 100000).await;

    let app = build_router(state);
    let addr = serve(app).await;

    let client = Client::new();
    let resp = client
        .post(format!("http://{}/v1/chat/completions", addr))
        .header("content-type", "application/json")
        .header("x-api-key", "key")
        .body(r#"{"model":"test","max_tokens":1,"messages":[{"role":"user","content":"hi"}]}"#)
        .send()
        .await
        .unwrap();

    assert_eq!(
        resp.status(),
        reqwest::StatusCode::TOO_MANY_REQUESTS,
        "OpenAI-compat handler should enforce utilization limits"
    );
}

#[tokio::test]
async fn openai_handler_enforces_emergency_brake() {
    let (mock_url, _handle) = spawn_mock_upstream().await;
    let now = AppState::now_epoch();
    let state = Arc::new(AppState {
        endpoints: vec![mk_endpoint_at("a", "sk-ant-api-test-aaa", &mock_url)],
        state_path: PathBuf::from("/tmp/test-openai-emergency.state.json"),
        proxy_key: Some("key".to_string()),
        auto_cache: false,
        ..test_state_base()
    });
    set_account_utilization(&state, 0, 0.96, 0.0, now + 10000, now + 100000).await;

    let app = build_router(state);
    let addr = serve(app).await;

    let client = Client::new();
    let resp = client
        .post(format!("http://{}/v1/chat/completions", addr))
        .header("content-type", "application/json")
        .header("x-api-key", "key")
        .body(r#"{"model":"test","max_tokens":1,"messages":[{"role":"user","content":"hi"}]}"#)
        .send()
        .await
        .unwrap();

    assert_eq!(
        resp.status(),
        reqwest::StatusCode::TOO_MANY_REQUESTS,
        "OpenAI-compat handler should enforce emergency brake"
    );
}

#[tokio::test]
async fn no_new_config_identical_behavior() {
    // Default config: no operator, no limits, no emergency override
    // Should behave exactly like before the feature was added
    let (mock_url, _handle) = spawn_mock_upstream().await;
    let (app, _state) = test_app(&mock_url, Some("key".to_string()));
    let addr = serve(app).await;

    let client = Client::new();
    let resp = client
        .post(format!("http://{}/v1/messages", addr))
        .header("content-type", "application/json")
        .header("x-api-key", "key")
        .body(r#"{"model":"test","max_tokens":1,"messages":[{"role":"user","content":"hi"}]}"#)
        .send()
        .await
        .unwrap();

    // Default config: no limits, no emergency → request should succeed
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    // Budget status header present even with no config (default healthy)
    assert!(resp.headers().get("x-budget-status").is_some());

    // Stats should still work with aggregate section
    let stats = client
        .get(format!("http://{}/_stats", addr))
        .header("x-api-key", "key")
        .send()
        .await
        .unwrap();
    assert_eq!(stats.status(), reqwest::StatusCode::OK);
    let body: serde_json::Value = stats.json().await.unwrap();
    assert!(body["endpoints"].is_array());
    assert!(body["aggregate"].is_object());
    assert_eq!(body["strategy"], "dynamic-capacity-v1");
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

#[test]
fn status_to_floor_mapping() {
    assert_eq!(status_to_floor(Some("rejected")), REJECTED_UTIL_FLOOR);
    assert_eq!(status_to_floor(Some("throttled")), THROTTLE_UTIL_FLOOR);
    assert_eq!(status_to_floor(Some("allowed_warning")), WARNING_UTIL_FLOOR);
    assert_eq!(status_to_floor(Some("allowed")), 0.0);
    assert_eq!(status_to_floor(None), 0.0);
}

#[test]
fn status_to_floor_unknown_defaults_to_warning() {
    let floor = status_to_floor(Some("unknown_status"));
    assert_eq!(
        floor, WARNING_UTIL_FLOOR,
        "unknown status should map to warning floor"
    );
}

#[test]
fn status_to_ordinal_mapping() {
    assert_eq!(status_to_ordinal(Some("rejected")), 3.0);
    assert_eq!(status_to_ordinal(Some("throttled")), 2.0);
    assert_eq!(status_to_ordinal(Some("allowed_warning")), 1.0);
    assert_eq!(status_to_ordinal(Some("allowed")), 0.0);
    assert_eq!(status_to_ordinal(None), 0.0);
    // Unknown statuses map to 1.0 (warning-level)
    assert_eq!(status_to_ordinal(Some("new_unknown_status")), 1.0);
}

#[test]
fn time_adjusted_utilization_stale_data() {
    let now = 1000000u64;
    let reset_past = 999000u64; // Reset already happened

    let result =
        time_adjusted_utilization(Some(0.50), Some(reset_past), Some("allowed"), 3600.0, now);

    assert_eq!(
        result, None,
        "stale data (reset in past) should return None"
    );
}

#[test]
fn time_adjusted_utilization_near_reset() {
    let now = 1000000u64;
    let reset = now + 1800; // 30 minutes until reset
    let near_reset_threshold = 3600.0; // 1 hour threshold

    let result = time_adjusted_utilization(
        Some(0.80),
        Some(reset),
        Some("allowed"),
        near_reset_threshold,
        now,
    );

    assert!(result.is_some());
    let adjusted = result.unwrap();
    assert!(
        adjusted < 0.80,
        "near-reset utilization should be discounted"
    );
    assert!(
        adjusted >= 0.04,
        "should apply minimum discount floor (0.05)"
    );
}

#[test]
fn time_adjusted_utilization_mid_block() {
    let now = 1000000u64;
    let reset = now + 10800; // 3 hours until reset
    let near_reset_threshold = 3600.0; // 1 hour threshold

    let result = time_adjusted_utilization(
        Some(0.80),
        Some(reset),
        Some("allowed"),
        near_reset_threshold,
        now,
    );

    assert!(result.is_some());
    let adjusted = result.unwrap();
    assert_eq!(adjusted, 0.80, "mid-block utilization should be unchanged");
}

#[test]
fn time_adjusted_utilization_status_floor_minimum() {
    let now = 1000000u64;
    let reset = now + 100; // Very close to reset

    let result = time_adjusted_utilization(
        Some(0.80),
        Some(reset),
        Some("throttled"), // Floor of 0.98
        3600.0,
        now,
    );

    assert!(result.is_some());
    let adjusted = result.unwrap();
    assert_eq!(
        adjusted, THROTTLE_UTIL_FLOOR,
        "status floor should override time discount"
    );
}

#[test]
fn epoch_to_iso8601_leap_year() {
    // 2024-02-29T00:00:00Z (leap day) = 1709164800
    let result = AppState::epoch_to_iso8601(1709164800);
    assert_eq!(
        result, "2024-02-29T00:00:00Z",
        "should handle leap year correctly"
    );
}

#[test]
fn epoch_to_iso8601_edge_of_year() {
    // 2023-12-31T23:59:59Z = 1704067199
    let result = AppState::epoch_to_iso8601(1704067199);
    assert_eq!(
        result, "2023-12-31T23:59:59Z",
        "should handle end of year correctly"
    );
}

#[test]
fn account_serves_model_empty_filter_allows_all() {
    let acct = mk_endpoint("test", "sk-ant-api-x");
    assert!(acct.serves_model("claude-sonnet-4-6"));
    assert!(acct.serves_model("claude-opus-4-6"));
    assert!(acct.serves_model(""));
}

#[test]
fn account_serves_model_prefix_wildcard() {
    let mut acct = mk_endpoint("test", "sk-ant-api-x");
    acct.models = vec!["claude-opus-*".to_string()];

    assert!(acct.serves_model("claude-opus-4-6"));
    assert!(acct.serves_model("claude-opus-future"));
    assert!(!acct.serves_model("claude-sonnet-4-6"));
}

#[test]
fn account_serves_model_multiple_patterns() {
    let mut acct = mk_endpoint("test", "sk-ant-api-x");
    acct.models = vec!["claude-opus-*".to_string(), "claude-sonnet-4-6".to_string()];

    assert!(acct.serves_model("claude-opus-4-6"));
    assert!(acct.serves_model("claude-sonnet-4-6"));
    assert!(!acct.serves_model("claude-sonnet-3-5"));
    assert!(!acct.serves_model("claude-haiku-3-5"));
}

#[tokio::test]
async fn effective_utilization_prefers_most_constrained() {
    // When both windows have data, should return max (most constrained)
    let now = AppState::now_epoch();
    let mut claims_7d = HashMap::new();
    claims_7d.insert(
        "seven_day".to_string(),
        ClaimWindowData {
            utilization: Some(0.60),
            reset: Some(now + 100000),
            status: Some("allowed".to_string()),
            ..Default::default()
        },
    );
    let info = RateLimitInfo {
        utilization_5h: Some(0.80),
        reset_5h: Some(now + 10000),
        status_5h: Some("allowed".to_string()),
        claims_7d,
        ..Default::default()
    };

    let (util, source, _, _) = effective_utilization(&info, now, "");
    assert!(util > 0.60, "should use higher (5h) utilization");
    assert_eq!(source, "5h");
}

#[tokio::test]
async fn effective_utilization_7d_no_penalty() {
    // 7d window should pass through without penalty
    let now = AppState::now_epoch();
    let mut claims_7d = HashMap::new();
    claims_7d.insert(
        "seven_day".to_string(),
        ClaimWindowData {
            utilization: Some(0.85),
            reset: Some(now + 100000),
            status: Some("allowed".to_string()),
            ..Default::default()
        },
    );
    let info = RateLimitInfo {
        utilization_5h: Some(0.50),
        reset_5h: Some(now + 10000),
        status_5h: Some("allowed".to_string()),
        claims_7d,
        ..Default::default()
    };

    let (util, _, _, _) = effective_utilization(&info, now, "");
    assert!(
        (util - 0.85).abs() < 0.01,
        "7d window should pass through at 0.85 without penalty: got {util}"
    );
}

#[tokio::test]
async fn pick_account_rejected_account_gets_no_traffic() {
    let state = test_state_with(vec![
        mk_endpoint("rejected", "sk-ant-api-a"),
        mk_endpoint("healthy", "sk-ant-api-b"),
    ]);

    let now = AppState::now_epoch();
    {
        let mut info = state.endpoints[0].rate_info.write().await;
        info.utilization_5h = Some(0.90);
        info.reset_5h = Some(now + 10000);
        info.status_5h = Some("rejected".to_string()); // Rejected = util floor 1.0
    }
    {
        let mut info = state.endpoints[1].rate_info.write().await;
        info.utilization = Some(0.50);
    }

    // All requests should go to healthy account
    for _ in 0..100 {
        let idx = state.pick_endpoint(None, "", &[]).await.unwrap();
        assert_eq!(idx, 1, "rejected account should receive no traffic");
    }
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

#[tokio::test]
async fn pick_account_all_throttled_uses_all() {
    // When all accounts are throttled (status=throttled, floor=0.98 > soft_limit=0.90),
    // should use all accounts (graceful degradation)
    let state = test_state_with(vec![
        mk_endpoint("a", "sk-ant-api-a"),
        mk_endpoint("b", "sk-ant-api-b"),
    ]);

    let now = AppState::now_epoch();
    for acct in &state.endpoints {
        let mut info = acct.rate_info.write().await;
        info.utilization_5h = Some(0.80);
        info.reset_5h = Some(now + 10000);
        info.status_5h = Some("throttled".to_string()); // Floor 0.98 > soft_limit 0.90
    }

    // Both accounts should receive traffic
    let mut counts = [0u32; 2];
    for _ in 0..1000 {
        let idx = state.pick_endpoint(None, "", &[]).await.unwrap();
        counts[idx] += 1;
    }

    assert!(
        counts[0] > 0,
        "first throttled account should get some traffic"
    );
    assert!(
        counts[1] > 0,
        "second throttled account should get some traffic"
    );
}

// ── Priority tier tests ──────────────────────────────────────────

#[tokio::test]
async fn pick_account_respects_priority_tiers() {
    // Tier 0 accounts with headroom should get ALL traffic — tier 1 gets nothing.
    let mut primary = mk_endpoint("primary", "sk-ant-api-a");
    primary.priority = 0;
    let mut fallback = mk_endpoint("fallback", "sk-ant-api-b");
    fallback.priority = 1;
    let state = test_state_with(vec![primary, fallback]);

    let now = AppState::now_epoch();
    for acct in &state.endpoints {
        let mut info = acct.rate_info.write().await;
        info.utilization_5h = Some(0.30);
        info.reset_5h = Some(now + 10000);
    }

    for _ in 0..100 {
        let idx = state.pick_endpoint(None, "", &[]).await.unwrap();
        assert_eq!(idx, 0, "all traffic should go to tier 0 when healthy");
    }
}

#[tokio::test]
async fn pick_account_falls_through_to_lower_priority() {
    // Tier 0 hard-limited → tier 1 should receive traffic.
    let mut primary = mk_endpoint("primary", "sk-ant-api-a");
    primary.priority = 0;
    let mut fallback = mk_endpoint("fallback", "sk-ant-api-b");
    fallback.priority = 1;
    let state = test_state_with(vec![primary, fallback]);

    // Hard-limit tier 0
    {
        let mut info = state.endpoints[0].rate_info.write().await;
        info.hard_limited_until = Some(Instant::now() + Duration::from_secs(3600));
    }

    let now = AppState::now_epoch();
    {
        let mut info = state.endpoints[1].rate_info.write().await;
        info.utilization_5h = Some(0.30);
        info.reset_5h = Some(now + 10000);
    }

    for _ in 0..100 {
        let idx = state.pick_endpoint(None, "", &[]).await.unwrap();
        assert_eq!(
            idx, 1,
            "tier 1 should get traffic when tier 0 is hard-limited"
        );
    }
}

#[tokio::test]
async fn pick_account_priority_default_zero() {
    // Accounts without explicit priority should behave as tier 0.
    let a = mk_endpoint("a", "sk-ant-api-a");
    let b = mk_endpoint("b", "sk-ant-api-b");
    assert_eq!(a.priority, 0);
    assert_eq!(b.priority, 0);

    let state = test_state_with(vec![a, b]);
    let now = AppState::now_epoch();
    for acct in &state.endpoints {
        let mut info = acct.rate_info.write().await;
        info.utilization_5h = Some(0.30);
        info.reset_5h = Some(now + 10000);
    }

    // Both should receive traffic (same tier)
    let mut counts = [0u32; 2];
    for _ in 0..1000 {
        let idx = state.pick_endpoint(None, "", &[]).await.unwrap();
        counts[idx] += 1;
    }
    assert!(counts[0] > 0, "account a should get traffic");
    assert!(counts[1] > 0, "account b should get traffic");
}

#[tokio::test]
async fn pick_account_priority_soft_limit_stays_in_tier() {
    // Tier 0 accounts above soft_limit but still alive (weight > 0) → tier 0 is
    // degraded-used, NOT skipped. soft_limit is intra-tier load-shedding; it must
    // never cause a jump to a lower-priority (paid) tier while free capacity remains.
    let mut primary_a = mk_endpoint("primary_a", "sk-ant-api-a");
    primary_a.priority = 0;
    let mut primary_b = mk_endpoint("primary_b", "sk-ant-api-b");
    primary_b.priority = 0;
    let mut fallback = mk_endpoint("fallback", "sk-ant-api-c");
    fallback.priority = 1;
    let state = test_state_with_soft_limit(vec![primary_a, primary_b, fallback], 0.90);

    let now = AppState::now_epoch();
    // Tier 0: above soft limit (0.95) but still has headroom — weight > 0.
    for i in 0..2 {
        let mut info = state.endpoints[i].rate_info.write().await;
        info.utilization_5h = Some(0.95);
        info.reset_5h = Some(now + 10000);
    }
    // Tier 1: healthy.
    {
        let mut info = state.endpoints[2].rate_info.write().await;
        info.utilization_5h = Some(0.30);
        info.reset_5h = Some(now + 10000);
    }

    for _ in 0..100 {
        let idx = state.pick_endpoint(None, "", &[]).await.unwrap();
        assert!(
            idx == 0 || idx == 1,
            "tier 0 must be drained (degraded) before tier 1 is touched"
        );
    }
}

#[tokio::test]
async fn pick_account_priority_zero_weight_tier_falls_through() {
    // Tier 0 accounts at zero weight (util 1.0 → gate 1.0) → genuinely exhausted →
    // routing falls through to tier 1.
    let mut primary_a = mk_endpoint("primary_a", "sk-ant-api-a");
    primary_a.priority = 0;
    let mut primary_b = mk_endpoint("primary_b", "sk-ant-api-b");
    primary_b.priority = 0;
    let mut fallback = mk_endpoint("fallback", "sk-ant-api-c");
    fallback.priority = 1;
    let state = test_state_with_soft_limit(vec![primary_a, primary_b, fallback], 0.90);

    let now = AppState::now_epoch();
    // Tier 0: fully exhausted — util 1.0 → gate 1.0 → weight 0.
    for i in 0..2 {
        let mut info = state.endpoints[i].rate_info.write().await;
        info.utilization_5h = Some(1.0);
        info.reset_5h = Some(now + 10000);
    }
    // Tier 1: healthy.
    {
        let mut info = state.endpoints[2].rate_info.write().await;
        info.utilization_5h = Some(0.30);
        info.reset_5h = Some(now + 10000);
    }

    for _ in 0..100 {
        let idx = state.pick_endpoint(None, "", &[]).await.unwrap();
        assert_eq!(idx, 2, "tier 1 used once tier 0 is genuinely zero-weight");
    }
}

#[tokio::test]
async fn pick_account_multiple_tiers_cascade() {
    // Three tiers: 0, 1, 2. Tier 0 and 1 exhausted → tier 2 gets traffic.
    let mut t0 = mk_endpoint("t0", "sk-ant-api-a");
    t0.priority = 0;
    let mut t1 = mk_endpoint("t1", "sk-ant-api-b");
    t1.priority = 1;
    let mut t2 = mk_endpoint("t2", "sk-ant-api-c");
    t2.priority = 2;
    let state = test_state_with(vec![t0, t1, t2]);

    // Hard-limit tiers 0 and 1
    for i in 0..2 {
        let mut info = state.endpoints[i].rate_info.write().await;
        info.hard_limited_until = Some(Instant::now() + Duration::from_secs(3600));
    }

    let now = AppState::now_epoch();
    {
        let mut info = state.endpoints[2].rate_info.write().await;
        info.utilization_5h = Some(0.30);
        info.reset_5h = Some(now + 10000);
    }

    for _ in 0..100 {
        let idx = state.pick_endpoint(None, "", &[]).await.unwrap();
        assert_eq!(
            idx, 2,
            "tier 2 should get traffic when tiers 0 and 1 are exhausted"
        );
    }
}

#[tokio::test]
async fn pick_account_all_tiers_exhausted_returns_none() {
    // All tiers hard-limited → None.
    let mut t0 = mk_endpoint("t0", "sk-ant-api-a");
    t0.priority = 0;
    let mut t1 = mk_endpoint("t1", "sk-ant-api-b");
    t1.priority = 1;
    let state = test_state_with(vec![t0, t1]);

    for acct in &state.endpoints {
        let mut info = acct.rate_info.write().await;
        info.hard_limited_until = Some(Instant::now() + Duration::from_secs(3600));
    }

    assert!(state.pick_endpoint(None, "", &[]).await.is_none());
}

// ── Overage tests ────────────────────────────────────────────────

#[tokio::test]
async fn update_rate_info_parses_overage_headers() {
    let state = test_state_with(vec![mk_endpoint("a", "sk-ant-api-a")]);
    let mut headers = reqwest::header::HeaderMap::new();
    headers.insert(
        "anthropic-ratelimit-unified-overage-in-use",
        "true".parse().unwrap(),
    );
    headers.insert(
        "anthropic-ratelimit-unified-overage-status",
        "allowed".parse().unwrap(),
    );
    headers.insert(
        "anthropic-ratelimit-unified-overage-utilization",
        "0.25".parse().unwrap(),
    );
    let reset = AppState::now_epoch() + 100000;
    headers.insert(
        "anthropic-ratelimit-unified-overage-reset",
        reset.to_string().parse().unwrap(),
    );
    state.update_rate_info(0, &headers).await;

    let info = state.endpoints[0].rate_info.read().await;
    assert!(info.overage_in_use);
    assert_eq!(info.overage_status.as_deref(), Some("allowed"));
    assert_eq!(info.overage_utilization, Some(0.25));
    assert_eq!(info.overage_reset, Some(reset));
}

#[tokio::test]
async fn update_rate_info_overage_absent_resets_to_false() {
    // Corner 1: an account previously in overage whose next response omits the
    // overage-in-use header must drop back to overage_in_use=false.
    let state = test_state_with(vec![mk_endpoint("a", "sk-ant-api-a")]);
    {
        let mut info = state.endpoints[0].rate_info.write().await;
        info.overage_in_use = true;
        info.overage_status = Some("allowed".to_string());
        info.overage_utilization = Some(0.5);
    }
    // Fresh response with no overage headers at all.
    let headers = reqwest::header::HeaderMap::new();
    state.update_rate_info(0, &headers).await;

    let info = state.endpoints[0].rate_info.read().await;
    assert!(!info.overage_in_use, "overage_in_use must reset to false");
    assert_eq!(info.overage_status, None);
    assert_eq!(info.overage_utilization, None);
}

#[test]
fn routing_weight_overage_active_keeps_account_routable() {
    // 5h subscription window rejected/exhausted, but overage is covering it →
    // the account must have non-zero weight, not be dropped.
    let now = AppState::now_epoch();
    let mut info = RateLimitInfo {
        utilization_5h: Some(1.0),
        reset_5h: Some(now + 3000),
        status_5h: Some("rejected".to_string()),
        ..Default::default()
    };
    info.overage_in_use = true;
    info.overage_status = Some("allowed".to_string());
    info.overage_utilization = Some(0.0);
    info.overage_reset = Some(now + 100000);

    let rw = compute_routing_weight(&info, "claude-sonnet-4-6", now, false)
        .expect("overage account must not be skipped");
    assert!(rw.overage_active, "overage_active flag must be set");
    assert!(
        rw.weight > 0.0,
        "overage account with fresh overage budget must have non-zero weight, got {}",
        rw.weight
    );
    assert_eq!(rw.source, "overage");
}

#[test]
fn routing_weight_overage_exhausted_zero_weight() {
    // Overage in use but overage budget itself exhausted (utilization 1.0) → weight 0.
    let now = AppState::now_epoch();
    let mut info = RateLimitInfo {
        utilization_5h: Some(1.0),
        reset_5h: Some(now + 3000),
        status_5h: Some("rejected".to_string()),
        ..Default::default()
    };
    info.overage_in_use = true;
    info.overage_status = Some("allowed".to_string());
    info.overage_utilization = Some(1.0);
    info.overage_reset = Some(now + 100000);

    let rw =
        compute_routing_weight(&info, "claude-sonnet-4-6", now, false).expect("still a candidate");
    assert_eq!(rw.weight, 0.0, "exhausted overage → zero weight");
}

#[tokio::test]
async fn pick_account_overage_demoted_below_free() {
    // Free account (eff. priority 0) must drain before an overage account
    // (eff. priority 0 + overage_penalty 10) receives any traffic.
    let free = mk_endpoint("free", "sk-ant-api-a");
    let overage = mk_endpoint("overage", "sk-ant-api-b");
    let state = test_state_with(vec![free, overage]);

    let now = AppState::now_epoch();
    {
        let mut info = state.endpoints[0].rate_info.write().await;
        info.utilization_5h = Some(0.30);
        info.reset_5h = Some(now + 10000);
    }
    {
        let mut info = state.endpoints[1].rate_info.write().await;
        info.utilization_5h = Some(1.0);
        info.reset_5h = Some(now + 3000);
        info.status_5h = Some("rejected".to_string());
        info.overage_in_use = true;
        info.overage_status = Some("allowed".to_string());
        info.overage_utilization = Some(0.0);
        info.overage_reset = Some(now + 100000);
    }

    for _ in 0..100 {
        let idx = state.pick_endpoint(None, "", &[]).await.unwrap();
        assert_eq!(idx, 0, "free account preferred over overage account");
    }
}

#[tokio::test]
async fn pick_account_overage_used_when_free_exhausted() {
    // Free account at zero weight → the overage account (demoted) is used.
    let free = mk_endpoint("free", "sk-ant-api-a");
    let overage = mk_endpoint("overage", "sk-ant-api-b");
    let state = test_state_with(vec![free, overage]);

    let now = AppState::now_epoch();
    {
        let mut info = state.endpoints[0].rate_info.write().await;
        info.utilization_5h = Some(1.0); // zero weight
        info.reset_5h = Some(now + 10000);
    }
    {
        let mut info = state.endpoints[1].rate_info.write().await;
        info.utilization_5h = Some(1.0);
        info.reset_5h = Some(now + 3000);
        info.status_5h = Some("rejected".to_string());
        info.overage_in_use = true;
        info.overage_status = Some("allowed".to_string());
        info.overage_utilization = Some(0.0);
        info.overage_reset = Some(now + 100000);
    }

    for _ in 0..100 {
        let idx = state.pick_endpoint(None, "", &[]).await.unwrap();
        assert_eq!(idx, 1, "overage account used once free tier is exhausted");
    }
}

#[tokio::test]
async fn pick_endpoint_openai_is_last_resort() {
    // A priority-100 OpenAI endpoint is a routing candidate: a healthy
    // Anthropic endpoint beats it; it is selected only once the lower
    // tier is exhausted.
    let mut openai = make_endpoint("fallback", Protocol::OpenAI);
    openai.priority = 100;
    let state = test_state_with(vec![mk_endpoint("a", "sk-ant-api-a"), openai]);
    let now = AppState::now_epoch();
    {
        let mut info = state.endpoints[0].rate_info.write().await;
        info.utilization_5h = Some(0.30);
        info.reset_5h = Some(now + 10000);
    }

    for _ in 0..50 {
        assert_eq!(
            state.pick_endpoint(None, "", &[]).await,
            Some(0),
            "healthy endpoint beats the priority-100 openai endpoint"
        );
    }

    // Hard-limit the Anthropic endpoint → the openai endpoint is the only
    // remaining candidate.
    {
        let mut info = state.endpoints[0].rate_info.write().await;
        info.hard_limited_until = Some(Instant::now() + Duration::from_secs(3600));
    }
    assert_eq!(
        state.pick_endpoint(None, "", &[]).await,
        Some(1),
        "openai endpoint selected once the lower tier is exhausted"
    );
}

#[test]
fn is_operator_checks_configured_operator() {
    let state = Arc::new(AppState {
        client: Client::new(),
        client_nonstreaming: Client::new(),
        state_path: PathBuf::from("/tmp/test.state.json"),
        operators: vec!["special-operator".to_string()],
        ..test_state_base()
    });

    assert!(state.is_operator("special-operator"));
    assert!(!state.is_operator("regular-client"));
}

#[test]
fn is_operator_returns_false_when_no_operator_configured() {
    let state = test_state_with(vec![]);
    assert!(!state.is_operator("any-client"));
}

#[test]
fn is_operator_supports_multiple_operators() {
    let state = Arc::new(AppState {
        client: Client::new(),
        client_nonstreaming: Client::new(),
        state_path: PathBuf::from("/tmp/test.state.json"),
        operators: vec![
            "ray".to_string(),
            "openclaw".to_string(),
            "claude".to_string(),
        ],
        ..test_state_base()
    });
    assert!(state.is_operator("ray"));
    assert!(state.is_operator("openclaw"));
    assert!(state.is_operator("claude"));
    assert!(!state.is_operator("gastown"));
    assert!(!state.is_operator("-"));
}

// ── Redis distributed state tests ──────────────────────────────────

#[tokio::test]
async fn cluster_info_returns_none_without_redis() {
    let state = test_state_with(vec![mk_endpoint("a", "sk-ant-api-x")]);
    assert!(state.cluster_info().await.is_none());
}

#[tokio::test]
async fn sync_from_redis_noop_without_redis() {
    let state = test_state_with(vec![mk_endpoint("a", "sk-ant-api-x")]);
    // Should not panic or error when redis is None
    state.sync_from_redis().await;
    // Cluster cache should remain None
    assert!(state.cluster_info_cache.lock().unwrap().is_none());
}

/// sync_from_redis / publish_routing_weights build the SyncTarget list over
/// the endpoint pool. With an endpoints-only config (and a mix of
/// Anthropic + OpenAI protocols) neither path may panic.
#[tokio::test]
async fn sync_and_publish_handle_endpoint_pool_without_redis() {
    let mut state = test_state_with(vec![]);
    {
        let st = Arc::get_mut(&mut state).unwrap();
        st.endpoints
            .push(make_endpoint("ep-a", Protocol::Anthropic));
        st.endpoints.push(make_endpoint("ep-oai", Protocol::OpenAI));
    }
    // Both paths must be no-ops (redis is None) and must not panic.
    state.sync_from_redis().await;
    state.publish_routing_weights().await;
    assert!(state.cluster_info_cache.lock().unwrap().is_none());
}

#[tokio::test]
async fn budget_local_fallback_without_redis() {
    let mut budgets = HashMap::new();
    budgets.insert("client-a".to_string(), 100u64);
    let state = Arc::new(AppState {
        endpoints: vec![mk_endpoint("a", "sk-ant-api-x")],
        client_budgets: budgets,
        ..test_state_base()
    });

    // Budget check uses local path when redis is None
    assert!(state.check_budget("client-a").await.is_ok());
    state.record_budget_usage("client-a", 80).await;
    assert!(state.check_budget("client-a").await.is_ok());
    state.record_budget_usage("client-a", 30).await;
    assert!(state.check_budget("client-a").await.is_err());
}

#[test]
fn redis_rate_info_serialization_roundtrip() {
    let mut claims = HashMap::new();
    claims.insert(
        "claude-sonnet-4-6".to_string(),
        ClaimWindowData {
            utilization: Some(0.42),
            reset: Some(1700000000),
            status: Some("active".to_string()),
            ..Default::default()
        },
    );

    let info = RedisRateInfo {
        utilization: Some(0.5),
        utilization_5h: Some(0.3),
        utilization_7d: Some(0.6),
        reset_5h: Some(1700000000),
        reset_7d: Some(1700500000),
        status_5h: Some("active".to_string()),
        status_7d: Some("active".to_string()),
        claims_7d: claims,
        representative_claim: Some("five_hour".to_string()),
        remaining_requests: Some(100),
        remaining_tokens: Some(50000),
        limit_requests: Some(200),
        limit_tokens: Some(100000),
        overage_in_use: false,
        overage_status: None,
        overage_utilization: None,
        overage_reset: None,
        updated_at: 1700000000,
    };

    let json = serde_json::to_string(&info).unwrap();
    let deserialized: RedisRateInfo = serde_json::from_str(&json).unwrap();

    assert_eq!(info.utilization, deserialized.utilization);
    assert_eq!(info.utilization_5h, deserialized.utilization_5h);
    assert_eq!(info.utilization_7d, deserialized.utilization_7d);
    assert_eq!(info.reset_5h, deserialized.reset_5h);
    assert_eq!(info.reset_7d, deserialized.reset_7d);
    assert_eq!(info.updated_at, deserialized.updated_at);
    assert_eq!(info.claims_7d.len(), deserialized.claims_7d.len());
    let claim = deserialized.claims_7d.get("claude-sonnet-4-6").unwrap();
    assert_eq!(claim.utilization, Some(0.42));
    assert_eq!(claim.reset, Some(1700000000));
}

#[test]
fn redis_rate_info_empty_fields() {
    let info = RedisRateInfo {
        utilization: None,
        utilization_5h: None,
        utilization_7d: None,
        reset_5h: None,
        reset_7d: None,
        status_5h: None,
        status_7d: None,
        claims_7d: HashMap::new(),
        representative_claim: None,
        remaining_requests: None,
        remaining_tokens: None,
        limit_requests: None,
        limit_tokens: None,
        overage_in_use: false,
        overage_status: None,
        overage_utilization: None,
        overage_reset: None,
        updated_at: 0,
    };

    let json = serde_json::to_string(&info).unwrap();
    let deserialized: RedisRateInfo = serde_json::from_str(&json).unwrap();
    assert!(deserialized.utilization.is_none());
    assert!(deserialized.claims_7d.is_empty());
    assert_eq!(deserialized.updated_at, 0);
}

#[tokio::test]
async fn hard_limit_unchanged_by_sync_without_redis() {
    let state = test_state_with(vec![mk_endpoint("a", "sk-ant-api-x")]);

    // Set a local hard limit
    {
        let mut info = state.endpoints[0].rate_info.write().await;
        info.hard_limited_until = Some(Instant::now() + Duration::from_secs(30));
    }

    // sync_from_redis should not touch it when redis is None
    state.sync_from_redis().await;

    let info = state.endpoints[0].rate_info.read().await;
    assert!(info.hard_limited_until.is_some());
}

#[tokio::test]
async fn record_budget_usage_skips_zero_tokens() {
    let mut budgets = HashMap::new();
    budgets.insert("client-a".to_string(), 100u64);
    let state = Arc::new(AppState {
        endpoints: vec![mk_endpoint("a", "sk-ant-api-x")],
        client_budgets: budgets,
        ..test_state_base()
    });

    // Recording 0 tokens should be a no-op
    state.record_budget_usage("client-a", 0).await;
    let map = state.budget_usage.lock().unwrap();
    assert!(map.get("client-a").is_none());
}

#[tokio::test]
async fn record_budget_usage_skips_unknown_client() {
    let state = test_state_with(vec![mk_endpoint("a", "sk-ant-api-x")]);
    // No budgets configured — recording should be a no-op
    state.record_budget_usage("unknown-client", 500).await;
    let map = state.budget_usage.lock().unwrap();
    assert!(map.is_empty());
}

// ── Config deserialization (real struct, not toml::Value) ─────

#[test]
fn config_deser_minimal() {
    let toml = r#"
listen = "127.0.0.1:8082"

[[endpoints]]
name = "primary"
token = "sk-ant-api-test"
"#;
    let cfg: Config = toml::from_str(toml).expect("minimal config should deserialize");
    assert_eq!(cfg.listen, "127.0.0.1:8082");
    assert_eq!(cfg.endpoints.len(), 1);
    assert_eq!(cfg.endpoints[0].name, "primary");
    // Optional fields absent
    assert!(cfg.proxy_key.is_none());
    assert!(cfg.redis_url.is_none());
    assert!(cfg.operators.is_empty());
    assert!(cfg.emergency_threshold.is_none());
    assert!(cfg.soft_limit.is_none());
    assert!(cfg.client_budgets.is_empty());
    assert!(cfg.client_utilization_limits.is_empty());
    assert!(cfg.client_names.is_empty());
}

#[test]
fn config_deser_all_optional_fields() {
    let toml = r#"
listen = "0.0.0.0:8082"
strategy = "dynamic-capacity"
rate_limit_cooldown_secs = 120
probe_interval_secs = 600
proxy_key = "secret"
allowed_ips = ["10.0.0.0/8", "192.168.1.1"]
auto_cache = false
shadow_log = "/tmp/shadow.jsonl"
operators = ["ray", "openclaw"]
emergency_threshold = 0.90
soft_limit = 0.85
redis_url = "redis://10.0.0.5:6379"

[client_names]
"10.0.0.1" = "alice"
"10.0.0.2" = "bob"

[client_budgets]
alice = 1000000
bob = 500000

[client_utilization_limits]
alice = 0.95
bob = 0.80

[[endpoints]]
name = "acct-a"
token = "sk-ant-oat01-token1"

[[endpoints]]
name = "acct-b"
token = "sk-ant-api-token2"
models = ["claude-opus-*", "claude-sonnet-4-6"]

[[endpoints]]
name = "openai"
protocol = "openai"
base_url = "https://api.openai.com"
token = "sk-openai-key"
priority = 100
"#;
    let cfg: Config = toml::from_str(toml).expect("full config should deserialize");
    assert_eq!(cfg.proxy_key.as_deref(), Some("secret"));
    assert_eq!(cfg.allowed_ips.as_ref().unwrap().len(), 2);
    assert_eq!(cfg.auto_cache, Some(false));
    assert_eq!(cfg.shadow_log.as_deref(), Some("/tmp/shadow.jsonl"));
    assert_eq!(cfg.operators, vec!["ray", "openclaw"]);
    assert_eq!(cfg.emergency_threshold, Some(0.90));
    assert_eq!(cfg.soft_limit, Some(0.85));
    assert_eq!(cfg.redis_url.as_deref(), Some("redis://10.0.0.5:6379"));
    assert_eq!(cfg.rate_limit_cooldown_secs, Some(120));
    assert_eq!(cfg.probe_interval_secs, Some(600));
    // Maps
    assert_eq!(cfg.client_names.get("10.0.0.1").unwrap(), "alice");
    assert_eq!(*cfg.client_budgets.get("alice").unwrap(), 1000000u64);
    assert_eq!(*cfg.client_utilization_limits.get("alice").unwrap(), 0.95);
    // Endpoints
    assert_eq!(cfg.endpoints.len(), 3);
    assert_eq!(cfg.endpoints[1].models.len(), 2);
    assert_eq!(cfg.endpoints[1].models[0], "claude-opus-*");
    assert_eq!(cfg.endpoints[2].protocol, Protocol::OpenAI);
    assert_eq!(cfg.endpoints[2].priority, 100);
}

#[test]
fn routing_strategy_parses_aliases() {
    assert_eq!(
        RoutingStrategy::parse(None).unwrap(),
        RoutingStrategy::DynamicCapacityV1
    );
    assert_eq!(
        RoutingStrategy::parse(Some("dynamic-capacity")).unwrap(),
        RoutingStrategy::DynamicCapacityV1
    );
    assert_eq!(
        RoutingStrategy::parse(Some("dynamic-capacity-v1")).unwrap(),
        RoutingStrategy::DynamicCapacityV1
    );
    assert_eq!(
        RoutingStrategy::parse(Some("sticky-weighted")).unwrap(),
        RoutingStrategy::StickyWeightedV2
    );
    assert_eq!(
        RoutingStrategy::parse(Some("sticky-weighted-v2")).unwrap(),
        RoutingStrategy::StickyWeightedV2
    );
    assert!(RoutingStrategy::parse(Some("bogus")).is_err());
}

#[test]
fn config_deser_missing_required_field_fails() {
    // Missing `listen` (the sole required top-level key).
    let toml = r#"
[[endpoints]]
name = "test"
token = "sk-ant-api-test"
"#;
    let result = toml::from_str::<Config>(toml);
    assert!(
        result.is_err(),
        "missing listen should fail deserialization"
    );
}

#[test]
fn config_deser_endpoints_default_empty() {
    // `endpoints` defaults to an empty vec at the deserialization layer;
    // the non-empty requirement is enforced in `main()`, not by serde.
    let toml = r#"
listen = "127.0.0.1:8082"
"#;
    let cfg: Config = toml::from_str(toml).expect("config without endpoints deserializes");
    assert!(cfg.endpoints.is_empty());
}

// ── Rate info merge: "most recent wins" ─────────────────────

#[tokio::test]
async fn rate_info_merge_remote_newer_wins() {
    let state = test_state_with(vec![mk_endpoint("a", "sk-ant-api-x")]);
    let now_epoch = AppState::now_epoch();

    // Set local rate info with an older timestamp
    {
        let mut info = state.endpoints[0].rate_info.write().await;
        info.utilization_5h = Some(0.30);
        info.last_updated_epoch = Some(now_epoch - 60); // 60s ago
    }

    // Simulate remote data that's newer (10s ago)
    let remote = RedisRateInfo {
        utilization: Some(0.80),
        utilization_5h: Some(0.75),
        utilization_7d: Some(0.60),
        reset_5h: Some(now_epoch + 3600),
        reset_7d: Some(now_epoch + 86400),
        status_5h: Some("allowed_warning".to_string()),
        status_7d: Some("allowed".to_string()),
        claims_7d: HashMap::new(),
        representative_claim: Some("five_hour".to_string()),
        remaining_requests: Some(50),
        remaining_tokens: Some(25000),
        limit_requests: Some(200),
        limit_tokens: Some(100000),
        overage_in_use: false,
        overage_status: None,
        overage_utilization: None,
        overage_reset: None,
        updated_at: now_epoch - 10, // 10s ago — newer than local
    };

    // Apply same merge logic as sync_from_redis
    {
        let mut info = state.endpoints[0].rate_info.write().await;
        let local_age = info
            .last_updated_epoch
            .map(|epoch| now_epoch.saturating_sub(epoch))
            .unwrap_or(u64::MAX);
        let remote_age = now_epoch.saturating_sub(remote.updated_at);
        assert!(
            remote_age < local_age,
            "remote ({}s) should be newer than local ({}s)",
            remote_age,
            local_age
        );
        // Remote wins — apply
        info.utilization_5h = remote.utilization_5h;
        info.last_updated_epoch = Some(remote.updated_at);
    }

    let info = state.endpoints[0].rate_info.read().await;
    assert_eq!(info.utilization_5h, Some(0.75));
    assert_eq!(info.last_updated_epoch, Some(now_epoch - 10));
}

#[tokio::test]
async fn rate_info_merge_local_newer_preserved() {
    let state = test_state_with(vec![mk_endpoint("a", "sk-ant-api-x")]);
    let now_epoch = AppState::now_epoch();

    // Set local rate info with a recent timestamp (5s ago)
    {
        let mut info = state.endpoints[0].rate_info.write().await;
        info.utilization_5h = Some(0.30);
        info.last_updated_epoch = Some(now_epoch - 5);
    }

    // Remote data is older (120s ago)
    let remote = RedisRateInfo {
        utilization: Some(0.80),
        utilization_5h: Some(0.75),
        utilization_7d: None,
        reset_5h: None,
        reset_7d: None,
        status_5h: None,
        status_7d: None,
        claims_7d: HashMap::new(),
        representative_claim: None,
        remaining_requests: None,
        remaining_tokens: None,
        limit_requests: None,
        limit_tokens: None,
        overage_in_use: false,
        overage_status: None,
        overage_utilization: None,
        overage_reset: None,
        updated_at: now_epoch - 120, // 120s ago — older than local
    };

    // Apply same merge logic as sync_from_redis
    {
        let info = state.endpoints[0].rate_info.read().await;
        let local_age = info
            .last_updated_epoch
            .map(|epoch| now_epoch.saturating_sub(epoch))
            .unwrap_or(u64::MAX);
        let remote_age = now_epoch.saturating_sub(remote.updated_at);
        assert!(
            remote_age >= local_age,
            "local ({}s) should be newer than remote ({}s)",
            local_age,
            remote_age
        );
        // Local wins — do NOT apply remote
    }

    let info = state.endpoints[0].rate_info.read().await;
    assert_eq!(
        info.utilization_5h,
        Some(0.30),
        "local data should be preserved"
    );
}

#[tokio::test]
async fn rate_info_merge_no_local_epoch_remote_wins() {
    let state = test_state_with(vec![mk_endpoint("a", "sk-ant-api-x")]);
    let now_epoch = AppState::now_epoch();

    // Local has no last_updated_epoch (fresh state)
    {
        let info = state.endpoints[0].rate_info.read().await;
        assert!(info.last_updated_epoch.is_none());
    }

    let remote = RedisRateInfo {
        utilization: Some(0.50),
        utilization_5h: Some(0.40),
        utilization_7d: None,
        reset_5h: None,
        reset_7d: None,
        status_5h: None,
        status_7d: None,
        claims_7d: HashMap::new(),
        representative_claim: None,
        remaining_requests: None,
        remaining_tokens: None,
        limit_requests: None,
        limit_tokens: None,
        overage_in_use: false,
        overage_status: None,
        overage_utilization: None,
        overage_reset: None,
        updated_at: now_epoch - 30,
    };

    // When local has no epoch, local_age = u64::MAX, so remote always wins
    {
        let info = state.endpoints[0].rate_info.read().await;
        let local_age = info
            .last_updated_epoch
            .map(|epoch| now_epoch.saturating_sub(epoch))
            .unwrap_or(u64::MAX);
        let remote_age = now_epoch.saturating_sub(remote.updated_at);
        assert!(
            remote_age < local_age,
            "remote should win when local has no epoch"
        );
    }
}

// ── State persistence round-trip ─────────────────────────────

#[tokio::test]
async fn state_roundtrip_preserves_burst_backoff_and_leaves_no_tmp() {
    let dir = tempfile::tempdir().unwrap();
    let state_path = dir.path().join("state.json");
    let state = Arc::new(AppState {
        endpoints: vec![mk_endpoint("primary", "sk-ant-api-aaa")],
        state_path: state_path.clone(),
        ..test_state_base()
    });
    {
        let mut info = state.endpoints[0].rate_info.write().await;
        info.consecutive_burst_429s = 3;
        info.utilization_5h = Some(0.4);
        info.reset_5h = Some(AppState::now_epoch() + 18000);
    }
    state.save_state().await;

    // Atomic write must leave no temp sibling behind in the directory.
    let leftover: Vec<String> = std::fs::read_dir(dir.path())
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .filter(|n| n.contains(".tmp"))
        .collect();
    assert!(
        leftover.is_empty(),
        "atomic save must clean up its temp file(s): {leftover:?}"
    );

    // Restart: a fresh state restores the burst-429 backoff stage (B3-07).
    let restored = Arc::new(AppState {
        endpoints: vec![mk_endpoint("primary", "sk-ant-api-aaa")],
        state_path: state_path.clone(),
        ..test_state_base()
    });
    restored.load_state().await;
    let info = restored.endpoints[0].rate_info.read().await;
    assert_eq!(
        info.consecutive_burst_429s, 3,
        "burst-429 backoff stage must survive a restart (B3-07)"
    );
}

/// Concurrent save_state calls must serialize cleanly: a valid, parseable
/// final file and no temp file left behind (no deadlock, no torn file). The
/// freshness-ordering guarantee itself is structural (the save lock); this
/// guards the concurrent path against corruption/deadlock.
#[tokio::test]
async fn concurrent_save_state_is_serialized_and_clean() {
    let dir = tempfile::tempdir().unwrap();
    let state_path = dir.path().join("state.json");
    let state = Arc::new(AppState {
        endpoints: vec![mk_endpoint("primary", "sk-ant-api-aaa")],
        state_path: state_path.clone(),
        ..test_state_base()
    });
    let mut handles = Vec::new();
    for _ in 0..8 {
        let s = state.clone();
        handles.push(tokio::spawn(async move { s.save_state().await }));
    }
    for h in handles {
        h.await.unwrap();
    }
    let data = tokio::fs::read_to_string(&state_path).await.unwrap();
    serde_json::from_str::<PersistedState>(&data).expect("final state file must be valid JSON");
    let leftover: Vec<String> = std::fs::read_dir(dir.path())
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .filter(|n| n.contains(".tmp"))
        .collect();
    assert!(
        leftover.is_empty(),
        "no temp files may remain after concurrent saves: {leftover:?}"
    );
}

#[tokio::test]
async fn state_persistence_roundtrip() {
    let tmp = tempfile::NamedTempFile::new().unwrap();
    let state_path = PathBuf::from(tmp.path());

    let state = Arc::new(AppState {
        endpoints: vec![
            mk_endpoint("primary", "sk-ant-api-aaa"),
            mk_endpoint("secondary", "sk-ant-api-bbb"),
        ],
        state_path: state_path.clone(),
        ..test_state_base()
    });

    // Set up some state to persist
    let now_epoch = AppState::now_epoch();
    state.endpoints[0].requests.store(42, Ordering::Relaxed);
    state.endpoints[1].requests.store(17, Ordering::Relaxed);
    {
        let mut info = state.endpoints[0].rate_info.write().await;
        info.utilization = Some(0.65);
        info.utilization_5h = Some(0.50);
        info.utilization_7d = Some(0.70);
        info.reset_5h = Some(now_epoch + 18000); // future
        info.reset_7d = Some(now_epoch + 604800);
        info.status_5h = Some("allowed_warning".to_string());
        info.remaining_requests = Some(100);
        info.remaining_tokens = Some(50000);
        info.limit_requests = Some(200);
        info.limit_tokens = Some(100000);
        info.representative_claim = Some("five_hour".to_string());
        // Set a known per-account epoch (older than now — simulates probe from 30s ago)
        info.last_updated_epoch = Some(now_epoch - 30);
        info.claims_7d.insert(
            "seven_day".to_string(),
            ClaimWindowData {
                utilization: Some(0.70),
                reset: Some(now_epoch + 604800),
                status: Some("allowed".to_string()),
                ..Default::default()
            },
        );
    }
    {
        let mut info = state.endpoints[1].rate_info.write().await;
        info.utilization_5h = Some(0.20);
        info.reset_5h = Some(now_epoch + 18000);
        // hard-limit 60s from now
        info.hard_limited_until = Some(Instant::now() + Duration::from_secs(60));
    }

    // Save state
    state.save_state().await;

    // Verify file exists and is valid JSON
    let data = tokio::fs::read_to_string(&state_path).await.unwrap();
    let persisted: PersistedState = serde_json::from_str(&data).unwrap();
    assert_eq!(persisted.endpoints.len(), 2);
    assert_eq!(persisted.endpoints[0].requests_total, 42);
    assert_eq!(persisted.endpoints[1].requests_total, 17);
    assert!(persisted.saved_at > 0);

    // Create a fresh state and load into it
    let state2 = Arc::new(AppState {
        endpoints: vec![
            mk_endpoint("primary", "sk-ant-api-aaa"),
            mk_endpoint("secondary", "sk-ant-api-bbb"),
        ],
        state_path,
        ..test_state_base()
    });

    // Load state
    state2.load_state().await;

    // Verify fields survived the round-trip
    assert_eq!(
        state2.endpoints[0].requests.load(Ordering::Relaxed),
        42,
        "request count should persist"
    );
    assert_eq!(state2.endpoints[1].requests.load(Ordering::Relaxed), 17);

    {
        let info = state2.endpoints[0].rate_info.read().await;
        // Unified is recomputed as max(utilization_5h, utilization_7d)
        assert_eq!(info.utilization, Some(0.70));
        assert_eq!(info.utilization_5h, Some(0.50));
        assert_eq!(info.remaining_requests, Some(100));
        assert_eq!(info.remaining_tokens, Some(50000));
        assert_eq!(info.limit_requests, Some(200));
        assert_eq!(info.limit_tokens, Some(100000));
        assert_eq!(info.representative_claim.as_deref(), Some("five_hour"));
        assert!(
            !info.claims_7d.is_empty(),
            "claims_7d should survive round-trip"
        );
        let claim = info.claims_7d.get("seven_day").unwrap();
        assert_eq!(claim.utilization, Some(0.70));
        // last_updated_epoch should be the per-account value, not saved_at or now()
        assert_eq!(
            info.last_updated_epoch,
            Some(now_epoch - 30),
            "last_updated_epoch should be the per-account persisted value"
        );
    }

    {
        let info = state2.endpoints[1].rate_info.read().await;
        assert_eq!(info.utilization_5h, Some(0.20));
        // Hard limit should have been restored (future epoch)
        assert!(
            info.hard_limited_until.is_some(),
            "hard_limited_until should survive round-trip"
        );
    }
}

#[tokio::test]
async fn load_state_warns_and_starts_clean_on_legacy_accounts_key() {
    // A state file using the legacy `accounts` top-level key must NOT
    // deserialize into the new `endpoints`-keyed PersistedState. load_state
    // logs a warn and starts clean.
    let tmp = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(
        tmp.path(),
        r#"{"accounts":[{"name":"primary","requests_total":42}],"saved_at":0}"#,
    )
    .unwrap();
    let mut state = test_state_with(vec![mk_endpoint("primary", "sk-ant")]);
    Arc::get_mut(&mut state).unwrap().state_path = tmp.path().to_path_buf();
    state.load_state().await; // must not panic
    assert_eq!(
        state.endpoints[0].requests.load(Ordering::Relaxed),
        0,
        "legacy accounts-keyed state file must not load into the new schema"
    );
}

#[tokio::test]
async fn save_load_roundtrip_unified_endpoints() {
    let tmp = tempfile::NamedTempFile::new().unwrap();
    let mut state = test_state_with(vec![]);
    {
        let st = Arc::get_mut(&mut state).unwrap();
        st.state_path = tmp.path().to_path_buf();
        st.endpoints.push(make_endpoint("ep1", Protocol::Anthropic));
    }
    let now_epoch = AppState::now_epoch();
    state.endpoints[0].requests.store(7, Ordering::Relaxed);
    {
        // load_state recomputes `utilization` from the surviving 5h/7d
        // windows, so a bare flat `utilization` would not survive. Set a
        // 5h window with a future reset so it persists and drives util.
        let mut info = state.endpoints[0].rate_info.write().await;
        info.utilization = Some(0.42);
        info.utilization_5h = Some(0.42);
        info.reset_5h = Some(now_epoch + 18000);
    }
    state.save_state().await;

    // Fresh state with the same endpoint name, load from the file.
    let mut state2 = test_state_with(vec![]);
    {
        let st = Arc::get_mut(&mut state2).unwrap();
        st.state_path = tmp.path().to_path_buf();
        st.endpoints.push(make_endpoint("ep1", Protocol::Anthropic));
    }
    state2.load_state().await;
    assert_eq!(state2.endpoints[0].requests.load(Ordering::Relaxed), 7);
    assert_eq!(
        state2.endpoints[0].rate_info.read().await.utilization,
        Some(0.42)
    );
}

// ── Budget day rollover ──────────────────────────────────────

#[tokio::test]
async fn budget_day_rollover_resets_counter() {
    let mut budgets = HashMap::new();
    budgets.insert("client-a".to_string(), 10000u64);
    let state = Arc::new(AppState {
        endpoints: vec![mk_endpoint("a", "sk-ant-api-x")],
        client_budgets: budgets,
        ..test_state_base()
    });

    // Pre-populate with yesterday's usage (high usage that would exceed budget)
    let yesterday = AppState::now_epoch() / 86400 - 1;
    {
        let mut map = state.budget_usage.lock().unwrap();
        map.insert("client-a".to_string(), (yesterday, 9500));
    }

    // Recording usage today should reset the counter (day rollover)
    state.record_budget_usage("client-a", 50).await;

    let map = state.budget_usage.lock().unwrap();
    let (day, used) = map.get("client-a").unwrap();
    let today = AppState::now_epoch() / 86400;
    assert_eq!(*day, today, "day should be today after rollover");
    assert_eq!(*used, 50, "usage should be 50 (reset, not 9550)");
}

#[tokio::test]
async fn budget_check_respects_day_boundary() {
    let mut budgets = HashMap::new();
    budgets.insert("client-a".to_string(), 1000u64);
    let state = Arc::new(AppState {
        endpoints: vec![mk_endpoint("a", "sk-ant-api-x")],
        client_budgets: budgets,
        ..test_state_base()
    });

    // Pre-populate with yesterday's exhausted budget
    let yesterday = AppState::now_epoch() / 86400 - 1;
    {
        let mut map = state.budget_usage.lock().unwrap();
        map.insert("client-a".to_string(), (yesterday, 5000));
    }

    // Budget check for today should pass — yesterday's usage doesn't count
    assert!(
        state.check_budget("client-a").await.is_ok(),
        "yesterday's exhausted budget should not block today"
    );
}

// ── Unit: Prometheus text exposition helpers ──────────────────────

#[test]
fn prometheus_gauge_formats_correctly() {
    let mut buf = String::new();
    prom_gauge(&mut buf, "test_metric", &[("label", "value")], 42.5);
    assert_eq!(buf, "test_metric{label=\"value\"} 42.5\n");
}

#[test]
fn prometheus_gauge_no_labels() {
    let mut buf = String::new();
    prom_gauge(&mut buf, "test_metric", &[], 1.0);
    assert_eq!(buf, "test_metric 1\n");
}

#[test]
fn prometheus_counter_formats_correctly() {
    let mut buf = String::new();
    prom_counter(&mut buf, "test_total", &[("a", "b")], 100);
    assert_eq!(buf, "test_total{a=\"b\"} 100\n");
}

#[test]
fn prometheus_gauge_nan_renders() {
    let mut buf = String::new();
    prom_gauge(&mut buf, "test_metric", &[], f64::NAN);
    assert_eq!(buf, "test_metric NaN\n");
}

#[test]
fn prometheus_gauge_multiple_labels() {
    let mut buf = String::new();
    prom_gauge(&mut buf, "m", &[("a", "1"), ("b", "2")], 0.0);
    assert_eq!(buf, "m{a=\"1\",b=\"2\"} 0\n");
}

#[test]
fn prometheus_label_escaping() {
    let mut buf = String::new();
    prom_gauge(&mut buf, "m", &[("name", "has\"quotes")], 1.0);
    assert_eq!(buf, "m{name=\"has\\\"quotes\"} 1\n");
}

#[test]
fn routing_metrics_present() {
    let acct = mk_endpoint("acct-a", "sk-ant-api-a");
    acct.last_routing_weight
        .store(0.4f64.to_bits(), Ordering::Relaxed);
    acct.last_routing_share
        .store(1.0f64.to_bits(), Ordering::Relaxed);

    let mut buf = String::new();
    append_routing_weight_metrics(
        &mut buf,
        &[acct],
        &[EndpointMetricsSnap {
            name: "acct-a".to_string(),
            ..Default::default()
        }],
    );

    assert!(
        buf.lines().any(
            |line| line.starts_with("anthropic_account_routing_weight{account=\"acct-a\"} 0.4")
        ),
        "missing routing_weight line:
{buf}"
    );
    assert!(
        buf.lines()
            .any(|line| line.starts_with("anthropic_account_routing_share{account=\"acct-a\"} 1")),
        "missing routing_share line:
{buf}"
    );
}
#[test]
fn routing_metrics_zero_weight_for_rejected_claim() {
    let acct = mk_endpoint("acct-a", "sk-ant-api-a");
    acct.last_routing_weight
        .store(0.0f64.to_bits(), Ordering::Relaxed);
    acct.last_routing_share
        .store(0.0f64.to_bits(), Ordering::Relaxed);

    let mut buf = String::new();
    append_routing_weight_metrics(
        &mut buf,
        &[acct],
        &[EndpointMetricsSnap {
            name: "acct-a".to_string(),
            ..Default::default()
        }],
    );

    assert!(
        buf.lines()
            .any(|line| line.starts_with("anthropic_account_routing_weight{account=\"acct-a\"} 0")),
        "rejected claim should zero routing_weight:
{buf}"
    );
    assert!(
        buf.lines()
            .any(|line| line.starts_with("anthropic_account_routing_share{account=\"acct-a\"} 0")),
        "rejected claim should export zero routing_share:
{buf}"
    );
}
#[tokio::test]
async fn passthrough_accounts_participate_in_routing_candidates_and_metrics() {
    let mut state = test_state_with(vec![
        mk_endpoint("passthrough", "passthrough"),
        mk_endpoint("api", "sk-ant-api-b"),
    ]);
    Arc::get_mut(&mut state).unwrap().soft_limit = 1.0;

    let now_epoch = AppState::now_epoch();
    {
        let mut info = state.endpoints[0].rate_info.write().await;
        info.utilization = Some(0.20);
        info.utilization_5h = Some(0.20);
        info.reset_5h = Some(now_epoch + 10000);
    }
    {
        let mut info = state.endpoints[1].rate_info.write().await;
        info.utilization = Some(0.40);
        info.utilization_5h = Some(0.40);
        info.reset_5h = Some(now_epoch + 10000);
    }

    let candidates = state.routing_candidates("claude-sonnet-4-6", &[]).await;
    assert_eq!(
        candidates.len(),
        2,
        "passthrough account should remain routable"
    );

    state.refresh_metrics_weights().await;
    let mut buf = String::new();
    append_routing_weight_metrics(
        &mut buf,
        &state.endpoints,
        &[
            EndpointMetricsSnap {
                name: "passthrough".to_string(),
                passthrough: true,
                utilization: Some(0.20),
                utilization_5h: Some(0.20),
                reset_5h: Some(now_epoch + 10000),
                ..Default::default()
            },
            EndpointMetricsSnap {
                name: "api".to_string(),
                utilization: Some(0.40),
                utilization_5h: Some(0.40),
                reset_5h: Some(now_epoch + 10000),
                ..Default::default()
            },
        ],
    );

    assert!(
        !buf.contains("anthropic_account_routing_weight{account=\"passthrough\"}"),
        "passthrough account should be omitted from routing metrics:
{buf}"
    );
    assert!(
        buf.contains("anthropic_account_routing_weight{account=\"api\"}"),
        "api account should remain in routing metrics:
{buf}"
    );
}

// ── Integration: /metrics endpoint ───────────────────────────────

#[tokio::test]
async fn metrics_endpoint_returns_prometheus_format() {
    let (mock_url, _handle) = spawn_mock_upstream().await;
    let (app, state) = test_app(&mock_url, None);

    // Set some state so metrics are interesting
    {
        let mut info = state.endpoints[0].rate_info.write().await;
        info.utilization_5h = Some(0.42);
        info.utilization_7d = Some(0.35);
        info.remaining_requests = Some(1000);
        info.remaining_tokens = Some(500000);
        info.limit_requests = Some(4000);
        info.limit_tokens = Some(2000000);
    }
    // Set burn rate values (R2.2)
    {
        let mut br = state.endpoints[0].burn_rate.lock().unwrap();
        br.rate_5m.value = 2.5;
        br.rate_1h.value = 1.8;
        br.rate_6h.value = 0.9;
    }
    state.endpoints[0].requests.store(123, Ordering::Relaxed);
    state.endpoints[0]
        .input_tokens
        .store(90000, Ordering::Relaxed);
    state.endpoints[0]
        .output_tokens
        .store(30000, Ordering::Relaxed);
    state.endpoints[0]
        .cache_creation_tokens
        .store(5000, Ordering::Relaxed);
    state.endpoints[0]
        .cache_read_tokens
        .store(15000, Ordering::Relaxed);

    let addr = serve(app).await;
    let client = Client::new();
    let resp = client
        .get(format!("http://{}/metrics", addr))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    // R1.1: Exact Prometheus Content-Type
    let ct = resp
        .headers()
        .get("content-type")
        .unwrap()
        .to_str()
        .unwrap();
    assert_eq!(
        ct, "text/plain; version=0.0.4; charset=utf-8",
        "content-type must match Prometheus exposition format"
    );

    let body = resp.text().await.unwrap();

    // Account utilization
    assert!(
        body.contains("anthropic_account_utilization{account=\"acct-a\",window=\"5h\"} 0.42"),
        "missing 5h util:\n{body}"
    );
    assert!(
        body.contains("anthropic_account_utilization{account=\"acct-a\",window=\"7d\"} 0.35"),
        "missing 7d util:\n{body}"
    );

    // R2.2: Burn rate
    assert!(
        body.contains("anthropic_account_burn_rate{account=\"acct-a\",window=\"5m\"} 2.5"),
        "missing burn_rate 5m:\n{body}"
    );
    assert!(
        body.contains("anthropic_account_burn_rate{account=\"acct-a\",window=\"1h\"} 1.8"),
        "missing burn_rate 1h:\n{body}"
    );
    assert!(
        body.contains("anthropic_account_burn_rate{account=\"acct-a\",window=\"6h\"} 0.9"),
        "missing burn_rate 6h:\n{body}"
    );

    // R2.3: Headroom (remaining_requests=1000 → headroom=1000)
    assert!(
        body.contains("anthropic_account_headroom_requests{account=\"acct-a\"} 1000"),
        "missing headroom_requests:\n{body}"
    );

    // Remaining
    assert!(
        body.contains("anthropic_account_remaining_requests{account=\"acct-a\"} 1000"),
        "missing remaining_requests:\n{body}"
    );

    // R2.5: Limits
    assert!(
        body.contains("anthropic_account_limit_requests{account=\"acct-a\"} 4000"),
        "missing limit_requests:\n{body}"
    );
    assert!(
        body.contains("anthropic_account_limit_tokens{account=\"acct-a\"} 2000000"),
        "missing limit_tokens:\n{body}"
    );

    // Requests total
    assert!(
        body.contains("anthropic_account_requests_total{account=\"acct-a\"} 123"),
        "missing requests_total:\n{body}"
    );

    // R2.7: Hard limited (default = 0)
    assert!(
        body.contains("anthropic_account_hard_limited_remaining_seconds{account=\"acct-a\"} 0"),
        "missing hard_limited_remaining_seconds:\n{body}"
    );

    // R2.8: All 4 token types
    assert!(
        body.contains(
            "anthropic_account_token_usage_total{account=\"acct-a\",type=\"input\"} 90000"
        ),
        "missing input tokens:\n{body}"
    );
    assert!(
        body.contains(
            "anthropic_account_token_usage_total{account=\"acct-a\",type=\"output\"} 30000"
        ),
        "missing output tokens:\n{body}"
    );
    assert!(
        body.contains(
            "anthropic_account_token_usage_total{account=\"acct-a\",type=\"cache_creation\"} 5000"
        ),
        "missing cache_creation tokens:\n{body}"
    );
    assert!(
        body.contains(
            "anthropic_account_token_usage_total{account=\"acct-a\",type=\"cache_read\"} 15000"
        ),
        "missing cache_read tokens:\n{body}"
    );

    // Second account should also be present
    assert!(
        body.contains("anthropic_account_requests_total{account=\"acct-b\"}"),
        "missing acct-b:\n{body}"
    );

    // Meta metric
    assert!(
        body.contains("anthropic_lb_info{strategy=\"dynamic-capacity-v1\"} 1"),
        "missing lb_info:\n{body}"
    );

    // HELP/TYPE headers present
    assert!(
        body.contains("# TYPE anthropic_account_utilization gauge"),
        "missing TYPE header:\n{body}"
    );
}

#[tokio::test]
async fn metrics_endpoint_auth() {
    let (mock_url, _handle) = spawn_mock_upstream().await;
    let (app, _state) = test_app(&mock_url, Some("secret-key".into()));

    let addr = serve(app).await;
    let client = Client::new();

    // Without key → 401
    let resp = client
        .get(format!("http://{}/metrics", addr))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::UNAUTHORIZED);

    // Wrong key → 401
    let resp = client
        .get(format!("http://{}/metrics", addr))
        .header("x-api-key", "wrong-key")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::UNAUTHORIZED);

    // Correct key → 200
    let resp = client
        .get(format!("http://{}/metrics", addr))
        .header("x-api-key", "secret-key")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
}

#[tokio::test]
async fn metrics_omits_null_utilization() {
    let (mock_url, _handle) = spawn_mock_upstream().await;
    let (app, _state) = test_app(&mock_url, None);

    // Don't set any rate_info — defaults are all None
    let addr = serve(app).await;
    let client = Client::new();
    let resp = client
        .get(format!("http://{}/metrics", addr))
        .send()
        .await
        .unwrap();
    let body = resp.text().await.unwrap();

    // Utilization lines should be absent (null data omitted, not emitted as 0 or NaN)
    assert!(
        !body.contains("anthropic_account_utilization{account=\"acct-a\""),
        "utilization should be omitted when null:\n{body}"
    );
    assert!(
        !body.contains("anthropic_account_remaining_requests{account=\"acct-a\""),
        "remaining_requests should be omitted when null:\n{body}"
    );
    assert!(
        !body.contains("anthropic_account_remaining_tokens{account=\"acct-a\""),
        "remaining_tokens should be omitted when null:\n{body}"
    );

    // But requests_total should still be present (counter, always emitted)
    assert!(
        body.contains("anthropic_account_requests_total{account=\"acct-a\"} 0"),
        "requests_total should always be present:\n{body}"
    );
}

#[tokio::test]
async fn metrics_operator_hiding() {
    let (mock_url, _handle) = spawn_mock_upstream().await;
    let accounts = vec![mk_endpoint_at("acct-a", "sk-ant-api-test-aaa", &mock_url)];
    let state = Arc::new(AppState {
        endpoints: accounts,
        operators: vec!["op-alice".to_string(), "op-bob".to_string()],
        ..test_state_base()
    });

    // Seed two operator clients and one regular client with token usage
    {
        let mut usage = state.client_usage.lock().unwrap();
        usage.insert("op-alice".to_string(), [100, 200, 0, 0]);
        usage.insert("op-bob".to_string(), [50, 100, 0, 0]);
        usage.insert("user-charlie".to_string(), [10, 20, 0, 0]);
    }
    {
        let mut rates = state.client_request_rates.lock().unwrap();
        rates.insert(
            "op-alice".to_string(),
            (
                5,
                Ewma {
                    value: 2.0,
                    tau: 60.0,
                    last_update: Instant::now(),
                },
            ),
        );
        rates.insert(
            "op-bob".to_string(),
            (
                3,
                Ewma {
                    value: 1.0,
                    tau: 60.0,
                    last_update: Instant::now(),
                },
            ),
        );
        rates.insert(
            "user-charlie".to_string(),
            (
                10,
                Ewma {
                    value: 5.0,
                    tau: 60.0,
                    last_update: Instant::now(),
                },
            ),
        );
    }

    let app = build_router(state);
    let addr = serve(app).await;
    let client = Client::new();
    let resp = client
        .get(format!("http://{}/metrics", addr))
        .send()
        .await
        .unwrap();
    let body = resp.text().await.unwrap();

    // Operator clients should NOT appear individually
    assert!(
        !body.contains("client=\"op-alice\""),
        "operator op-alice should be hidden:\n{body}"
    );
    assert!(
        !body.contains("client=\"op-bob\""),
        "operator op-bob should be hidden:\n{body}"
    );

    // Single _operator entry with summed values
    assert!(
        body.contains(
            "anthropic_client_token_usage_total{client=\"_operator\",type=\"input\"} 150"
        ),
        "operator input tokens should sum to 150:\n{body}"
    );
    assert!(
        body.contains(
            "anthropic_client_token_usage_total{client=\"_operator\",type=\"output\"} 300"
        ),
        "operator output tokens should sum to 300:\n{body}"
    );
    assert!(
        body.contains("anthropic_client_requests_total{client=\"_operator\"} 8"),
        "operator requests should sum to 8:\n{body}"
    );

    // Regular client should appear normally
    assert!(
        body.contains("client=\"user-charlie\""),
        "non-operator should appear:\n{body}"
    );
}

#[tokio::test]
async fn metrics_help_type_uniqueness() {
    let (mock_url, _handle) = spawn_mock_upstream().await;
    let (app, state) = test_app(&mock_url, None);

    // Set some state so all metric families are populated
    {
        let mut info = state.endpoints[0].rate_info.write().await;
        info.utilization_5h = Some(0.5);
        info.remaining_requests = Some(100);
        info.remaining_tokens = Some(50000);
        info.limit_requests = Some(1000);
        info.limit_tokens = Some(100000);
    }
    state.endpoints[0].requests.store(10, Ordering::Relaxed);

    let addr = serve(app).await;
    let client = Client::new();
    let resp = client
        .get(format!("http://{}/metrics", addr))
        .send()
        .await
        .unwrap();
    let body = resp.text().await.unwrap();

    // Parse all # TYPE lines and verify each metric family appears exactly once
    let mut type_counts: HashMap<String, u32> = HashMap::new();
    for line in body.lines() {
        if line.starts_with("# TYPE ") {
            let family = line
                .strip_prefix("# TYPE ")
                .unwrap()
                .split_whitespace()
                .next()
                .unwrap()
                .to_string();
            *type_counts.entry(family).or_insert(0) += 1;
        }
    }

    for (family, count) in &type_counts {
        assert_eq!(
            *count, 1,
            "# TYPE for {family} appears {count} times, expected exactly once"
        );
    }

    // Also verify HELP lines match TYPE lines
    let mut help_families: Vec<String> = body
        .lines()
        .filter(|l| l.starts_with("# HELP "))
        .map(|l| {
            l.strip_prefix("# HELP ")
                .unwrap()
                .split_whitespace()
                .next()
                .unwrap()
                .to_string()
        })
        .collect();
    let mut type_families: Vec<String> = type_counts.keys().cloned().collect();
    help_families.sort();
    type_families.sort();
    assert_eq!(
        help_families, type_families,
        "HELP and TYPE families should match"
    );
}

#[tokio::test]
async fn metrics_projected_throttle() {
    let (mock_url, _handle) = spawn_mock_upstream().await;
    let (app, state) = test_app(&mock_url, None);

    // acct-a: eff_util >= 0.5 and br_1h >= 0.01 → projected_throttle IS emitted
    {
        let mut info = state.endpoints[0].rate_info.write().await;
        info.utilization_5h = Some(0.7);
        info.remaining_requests = Some(500);
        info.limit_requests = Some(2000);
    }
    {
        let mut br = state.endpoints[0].burn_rate.lock().unwrap();
        br.rate_1h.value = 2.0;
    }

    // acct-b: defaults (no util, no burn rate) → eff_util < 0.5 → NOT emitted

    let addr = serve(app).await;
    let client = Client::new();
    let resp = client
        .get(format!("http://{}/metrics", addr))
        .send()
        .await
        .unwrap();
    let body = resp.text().await.unwrap();

    // acct-a: headroom=500, br_1h=2.0 → (500/2.0)*60 = 15000
    assert!(
        body.contains("anthropic_account_projected_throttle_seconds{account=\"acct-a\"} 15000"),
        "acct-a should have projected_throttle:\n{body}"
    );
    // acct-b should NOT have projected_throttle (eff_util < 0.5)
    assert!(
        !body.contains("anthropic_account_projected_throttle_seconds{account=\"acct-b\"}"),
        "acct-b should NOT have projected_throttle:\n{body}"
    );
}

#[tokio::test]
async fn metrics_client_budgets_and_rpm() {
    let accounts = vec![mk_endpoint("acct-a", "sk-ant-api-test-aaa")];
    let today = AppState::now_epoch() / 86400;

    let mut client_budgets = HashMap::new();
    client_budgets.insert("claude-code".to_string(), 1_000_000u64);

    let mut budget_usage_map = HashMap::new();
    budget_usage_map.insert("claude-code".to_string(), (today, 400_000u64));

    let mut client_rates_map: HashMap<String, (u64, Ewma)> = HashMap::new();
    let mut ewma = Ewma::new(TAU_5M);
    ewma.value = 3.5;
    client_rates_map.insert("claude-code".to_string(), (50, ewma));

    let state = Arc::new(AppState {
        endpoints: accounts,
        client_budgets,
        budget_usage: Mutex::new(budget_usage_map),
        client_request_rates: Mutex::new(client_rates_map),
        ..test_state_base()
    });

    let app = build_router(state);
    let addr = serve(app).await;
    let client = Client::new();
    let resp = client
        .get(format!("http://{}/metrics", addr))
        .send()
        .await
        .unwrap();
    let body = resp.text().await.unwrap();

    // R3.4: Budget metrics
    assert!(
        body.contains("anthropic_client_budget_limit{client=\"claude-code\"} 1000000"),
        "missing budget_limit:\n{body}"
    );
    assert!(
        body.contains("anthropic_client_budget_used{client=\"claude-code\"} 400000"),
        "missing budget_used:\n{body}"
    );
    assert!(
        body.contains("anthropic_client_budget_remaining{client=\"claude-code\"} 600000"),
        "missing budget_remaining:\n{body}"
    );

    // R3.3: Client RPM
    assert!(
        body.contains("anthropic_client_requests_per_minute{client=\"claude-code\"} 3.5"),
        "missing client RPM:\n{body}"
    );
}

#[tokio::test]
async fn metrics_aggregate_headroom_and_share() {
    let (mock_url, _handle) = spawn_mock_upstream().await;
    let (app, state) = test_app(&mock_url, None);

    // Set headroom on both accounts
    {
        let mut info = state.endpoints[0].rate_info.write().await;
        info.remaining_requests = Some(1000);
    }
    {
        let mut info = state.endpoints[1].rate_info.write().await;
        info.remaining_requests = Some(500);
    }

    // Set client request rates for consumer_share
    {
        let mut rates = state.client_request_rates.lock().unwrap();
        let mut ewma_a = Ewma::new(TAU_5M);
        ewma_a.value = 6.0;
        rates.insert("user-a".to_string(), (100, ewma_a));
        let mut ewma_b = Ewma::new(TAU_5M);
        ewma_b.value = 4.0;
        rates.insert("user-b".to_string(), (50, ewma_b));
    }

    let addr = serve(app).await;
    let client = Client::new();
    let resp = client
        .get(format!("http://{}/metrics", addr))
        .send()
        .await
        .unwrap();
    let body = resp.text().await.unwrap();

    // R4.1: Total headroom = 1000 + 500
    assert!(
        body.contains("anthropic_total_headroom_requests 1500"),
        "missing total_headroom:\n{body}"
    );

    // R4.2: Consumer share = rpm / total_rpm
    // user-a: 6.0/10.0 = 0.6, user-b: 4.0/10.0 = 0.4
    assert!(
        body.contains("anthropic_consumer_share{client=\"user-a\"} 0.6"),
        "missing consumer_share user-a:\n{body}"
    );
    assert!(
        body.contains("anthropic_consumer_share{client=\"user-b\"} 0.4"),
        "missing consumer_share user-b:\n{body}"
    );
}

#[tokio::test]
async fn metrics_claim_utilization_and_waste_risk() {
    let (mock_url, _handle) = spawn_mock_upstream().await;
    let (app, state) = test_app(&mock_url, None);

    let now_epoch = AppState::now_epoch();
    // Set reset to 3.5 days from now (half of 7d window)
    let reset_epoch = now_epoch + 302400; // 3.5 * 86400

    {
        let mut info = state.endpoints[0].rate_info.write().await;
        info.claims_7d.insert(
            "claude-sonnet".to_string(),
            ClaimWindowData {
                utilization: Some(0.65),
                reset: Some(reset_epoch),
                status: None,
                ..Default::default()
            },
        );
    }

    let addr = serve(app).await;
    let client = Client::new();
    let resp = client
        .get(format!("http://{}/metrics", addr))
        .send()
        .await
        .unwrap();
    let body = resp.text().await.unwrap();

    // R5.1: Claim utilization
    assert!(
        body.contains(
            "anthropic_claim_utilization{account=\"acct-a\",claim=\"claude-sonnet\"} 0.65"
        ),
        "missing claim_utilization:\n{body}"
    );

    // R5.2: Claim waste risk — should be present and > 0
    // waste_risk(0.65, reset_epoch, now_epoch):
    //   remaining_fraction = 302400 / 604800 = 0.5
    //   unused = 1.0 - 0.65 = 0.35
    //   waste_risk = 0.35 / 0.5 = 0.7
    assert!(
        body.contains("anthropic_claim_waste_risk{account=\"acct-a\",claim=\"claude-sonnet\"} 0.7"),
        "missing claim_waste_risk:\n{body}"
    );

    // acct-b has no claims → no claim metrics for it
    assert!(
        !body.contains("anthropic_claim_utilization{account=\"acct-b\""),
        "acct-b should have no claim_utilization:\n{body}"
    );
}

#[tokio::test]
async fn metrics_reset_seconds_and_account_waste_risk() {
    let (mock_url, _handle) = spawn_mock_upstream().await;
    let (app, state) = test_app(&mock_url, None);

    let now_epoch = AppState::now_epoch();
    let reset_5h = now_epoch + 7200; // 2 hours
    let reset_7d = now_epoch + 302400; // 3.5 days

    {
        let mut info = state.endpoints[0].rate_info.write().await;
        info.reset_5h = Some(reset_5h);
        info.reset_7d = Some(reset_7d);
        info.claims_7d.insert(
            "claude-sonnet".to_string(),
            ClaimWindowData {
                utilization: Some(0.30),
                reset: Some(reset_7d),
                status: None,
                ..Default::default()
            },
        );
    }

    let addr = serve(app).await;
    let client = Client::new();
    let resp = client
        .get(format!("http://{}/metrics", addr))
        .send()
        .await
        .unwrap();
    let body = resp.text().await.unwrap();

    // R6.1: reset_seconds for 5h window — should be close to 7200
    assert!(
        body.contains("# TYPE anthropic_account_reset_seconds gauge"),
        "missing reset_seconds type line:\n{body}"
    );
    // Parse the actual value and check within ±5s tolerance
    let line_5h = body
        .lines()
        .find(|l| {
            l.contains("anthropic_account_reset_seconds")
                && l.contains("acct-a")
                && l.contains("5h")
        })
        .expect("missing reset_seconds 5h for acct-a");
    let val_5h: f64 = line_5h.split_whitespace().last().unwrap().parse().unwrap();
    assert!(
        (val_5h - 7200.0).abs() < 5.0,
        "reset_seconds 5h should be ~7200, got {val_5h}"
    );

    // R6.2: reset_seconds for 7d window — should be close to 302400
    let line_7d = body
        .lines()
        .find(|l| {
            l.contains("anthropic_account_reset_seconds")
                && l.contains("acct-a")
                && l.contains("7d")
        })
        .expect("missing reset_seconds 7d for acct-a");
    let val_7d: f64 = line_7d.split_whitespace().last().unwrap().parse().unwrap();
    assert!(
        (val_7d - 302400.0).abs() < 5.0,
        "reset_seconds 7d should be ~302400, got {val_7d}"
    );

    // R6.3: account_waste_risk — max claim waste_risk for acct-a
    // waste_risk(0.30, reset_7d, now_epoch):
    //   remaining_fraction = 302400 / 604800 = 0.5
    //   unused = 1.0 - 0.30 = 0.70
    //   waste_risk = 0.70 / 0.5 = 1.4
    assert!(
        body.contains("# TYPE anthropic_account_waste_risk gauge"),
        "missing account_waste_risk type line:\n{body}"
    );
    assert!(
        body.contains("anthropic_account_waste_risk{account=\"acct-a\"} 1.4"),
        "missing account_waste_risk for acct-a:\n{body}"
    );

    // R6.4: acct-b has no claims → no account_waste_risk
    assert!(
        !body.contains("anthropic_account_waste_risk{account=\"acct-b\""),
        "acct-b should have no account_waste_risk:\n{body}"
    );

    // R6.5: acct-b has no reset data → no reset_seconds
    assert!(
        !body.contains("anthropic_account_reset_seconds{account=\"acct-b\""),
        "acct-b should have no reset_seconds:\n{body}"
    );
}

#[tokio::test]
async fn metrics_status_gate_and_data_age() {
    let (mock_url, _handle) = spawn_mock_upstream().await;
    let (app, state) = test_app(&mock_url, None);

    let now_epoch = AppState::now_epoch();
    let data_age_secs = 120u64;

    {
        let mut info = state.endpoints[0].rate_info.write().await;
        info.utilization_5h = Some(0.60);
        info.reset_5h = Some(now_epoch + 7200);
        info.status_5h = Some("allowed_warning".to_string());
        info.utilization_7d = Some(0.40);
        info.reset_7d = Some(now_epoch + 302400);
        info.status_7d = Some("throttled".to_string());
        info.last_updated_epoch = Some(now_epoch - data_age_secs);
        info.claims_7d.insert(
            "seven_day".to_string(),
            ClaimWindowData {
                utilization: Some(0.40),
                reset: Some(now_epoch + 302400),
                status: Some("throttled".to_string()),
                ..Default::default()
            },
        );
    }

    // Populate the effective gate atomic
    state.refresh_metrics_weights().await;

    let addr = serve(app).await;
    let client = Client::new();
    let resp = client
        .get(format!("http://{}/metrics", addr))
        .send()
        .await
        .unwrap();
    let body = resp.text().await.unwrap();

    // #49: rate_limit_status — allowed_warning=1 for 5h, throttled=2 for 7d
    assert!(
        body.contains("anthropic_account_rate_limit_status{account=\"acct-a\",window=\"5h\"} 1"),
        "missing status 5h=1 (allowed_warning):\n{body}"
    );
    assert!(
        body.contains("anthropic_account_rate_limit_status{account=\"acct-a\",window=\"7d\"} 2"),
        "missing status 7d=2 (throttled):\n{body}"
    );
    // acct-b has no status data → should still emit 0 (allowed/None)
    assert!(
        body.contains("anthropic_account_rate_limit_status{account=\"acct-b\",window=\"5h\"} 0"),
        "acct-b 5h should be 0 (allowed):\n{body}"
    );

    // #50: effective_gate — should be > 0 for acct-a (has utilization data)
    assert!(
        body.contains("# TYPE anthropic_account_effective_gate gauge"),
        "missing effective_gate TYPE:\n{body}"
    );
    let gate_line = body
        .lines()
        .find(|l| l.contains("anthropic_account_effective_gate") && l.contains("acct-a"))
        .expect("missing effective_gate for acct-a");
    let gate_val: f64 = gate_line
        .split_whitespace()
        .last()
        .unwrap()
        .parse()
        .unwrap();
    assert!(
        gate_val > 0.0,
        "effective_gate for acct-a should be > 0, got {gate_val}"
    );

    // #51: data_age_seconds — should be ~120s for acct-a
    assert!(
        body.contains("# TYPE anthropic_account_data_age_seconds gauge"),
        "missing data_age_seconds TYPE:\n{body}"
    );
    let age_line = body
        .lines()
        .find(|l| l.contains("anthropic_account_data_age_seconds") && l.contains("acct-a"))
        .expect("missing data_age_seconds for acct-a");
    let age_val: f64 = age_line.split_whitespace().last().unwrap().parse().unwrap();
    assert!(
        (age_val - data_age_secs as f64).abs() < 5.0,
        "data_age_seconds should be ~{data_age_secs}, got {age_val}"
    );

    // acct-b has no last_updated_epoch → no data_age line
    assert!(
        !body.contains("anthropic_account_data_age_seconds{account=\"acct-b\""),
        "acct-b should have no data_age_seconds:\n{body}"
    );
}

/// Unit: refresh_metrics_weights() persists routing weights and shares to
/// atomics on each Account, with shares normalized to sum ≈ 1.0 and zeros
/// for rejected accounts and accounts above soft_limit.
#[tokio::test]
async fn refresh_metrics_weights_persists_atomics() {
    let acct_a = mk_endpoint("a", "sk-ant-api-a");
    let acct_b = mk_endpoint("b", "sk-ant-api-b");
    let acct_c = mk_endpoint("c", "sk-ant-api-c");
    let state = test_state_with(vec![acct_a, acct_b, acct_c]);

    let now = AppState::now_epoch();

    // a: 5h=0.20 (healthy), b: 5h=0.30 (healthy)
    for (i, util) in [(0, 0.20), (1, 0.30)].iter() {
        let mut info = state.endpoints[*i].rate_info.write().await;
        info.utilization_5h = Some(*util);
        info.reset_5h = Some(now + 10000);
        info.utilization = Some(*util);
        info.claims_7d.clear();
    }
    // c: status=rejected → status_to_floor → gate=1.0 → weight=0
    {
        let mut info = state.endpoints[2].rate_info.write().await;
        info.utilization_5h = Some(0.10);
        info.reset_5h = Some(now + 10000);
        info.utilization = Some(0.10);
        info.status_5h = Some("rejected".to_string());
        info.claims_7d.clear();
    }

    state.refresh_metrics_weights().await;

    let read_weight = |i: usize| {
        f64::from_bits(
            state.endpoints[i]
                .last_routing_weight
                .load(Ordering::Relaxed),
        )
    };
    let read_share = |i: usize| {
        f64::from_bits(
            state.endpoints[i]
                .last_routing_share
                .load(Ordering::Relaxed),
        )
    };

    // a and b are healthy: positive weight + positive share
    assert!(read_weight(0) > 0.0, "a should have non-zero weight");
    assert!(read_weight(1) > 0.0, "b should have non-zero weight");
    assert!(read_share(0) > 0.0, "a should have non-zero share");
    assert!(read_share(1) > 0.0, "b should have non-zero share");

    // c was rejected (gate=1.0) → must be zeroed
    assert_eq!(read_weight(2), 0.0, "c (rejected) must have zero weight");
    assert_eq!(read_share(2), 0.0, "c (rejected) must have zero share");

    // Shares of healthy accounts sum to ≈ 1.0
    let total_share = read_share(0) + read_share(1) + read_share(2);
    assert!(
        (total_share - 1.0).abs() < 1e-9,
        "shares should sum to 1.0, got {total_share}"
    );

    // Lower-utilization account should win the larger share (a < b)
    assert!(
        read_share(0) > read_share(1),
        "a (lower 5h util) should have larger share than b: a={}, b={}",
        read_share(0),
        read_share(1)
    );
}

/// Unit: refresh_metrics_weights() also populates the unified endpoint
/// pool's gauge atomics. Anthropic endpoints get the headroom computation;
/// OpenAI endpoints get the fixed (gate 0.0, weight 1.0) representative.
#[tokio::test]
async fn refresh_metrics_weights_populates_endpoint_pool() {
    let mut state = test_state_with(vec![]);
    {
        let st = Arc::get_mut(&mut state).unwrap();
        st.endpoints
            .push(make_endpoint("ep-a", Protocol::Anthropic));
        st.endpoints.push(make_endpoint("oai", Protocol::OpenAI));
    }
    let now = AppState::now_epoch();
    {
        let mut info = state.endpoints[0].rate_info.write().await;
        info.utilization_5h = Some(0.20);
        info.reset_5h = Some(now + 10000);
        info.utilization = Some(0.20);
    }

    state.refresh_metrics_weights().await;

    let weight = |i: usize| {
        f64::from_bits(
            state.endpoints[i]
                .last_routing_weight
                .load(Ordering::Relaxed),
        )
    };
    let gate = |i: usize| {
        f64::from_bits(
            state.endpoints[i]
                .last_effective_gate
                .load(Ordering::Relaxed),
        )
    };
    // Anthropic endpoint: positive weight from headroom computation.
    assert!(weight(0) > 0.0, "anthropic endpoint should have weight");
    // OpenAI endpoint: fixed representative — gate 0.0, non-zero weight.
    assert_eq!(gate(1), 0.0, "openai endpoint gate must be the fixed 0.0");
    assert!(weight(1) > 0.0, "openai endpoint should carry weight");
}

/// Regression: when SOME accounts are above soft_limit but at least one
/// is healthy, the soft-limited ones must be zeroed (mirrors pick_account
/// excluding them). When ALL accounts are above soft_limit, none are
/// zeroed — graceful degradation, the dashboard reflects what
/// pick_account would actually still route to.
#[tokio::test]
async fn refresh_metrics_weights_soft_limit_graceful_degradation() {
    let now = AppState::now_epoch();

    // Scenario 1: mixed pool. a=healthy(0.20), b=soft-limited(0.95).
    // soft_limit=0.90 → b should be zeroed, a gets all weight.
    {
        let state = test_state_with_soft_limit(
            vec![
                mk_endpoint("a", "sk-ant-api-a"),
                mk_endpoint("b", "sk-ant-api-b"),
            ],
            0.90,
        );
        for (i, util) in [(0, 0.20), (1, 0.95)].iter() {
            let mut info = state.endpoints[*i].rate_info.write().await;
            info.utilization_5h = Some(*util);
            info.reset_5h = Some(now + 10000);
            info.utilization = Some(*util);
            info.claims_7d.clear();
        }

        state.refresh_metrics_weights().await;

        let w_a = f64::from_bits(
            state.endpoints[0]
                .last_routing_weight
                .load(Ordering::Relaxed),
        );
        let w_b = f64::from_bits(
            state.endpoints[1]
                .last_routing_weight
                .load(Ordering::Relaxed),
        );
        let s_a = f64::from_bits(
            state.endpoints[0]
                .last_routing_share
                .load(Ordering::Relaxed),
        );
        let s_b = f64::from_bits(
            state.endpoints[1]
                .last_routing_share
                .load(Ordering::Relaxed),
        );

        assert!(w_a > 0.0, "healthy a should have non-zero weight");
        assert_eq!(
            w_b, 0.0,
            "soft-limited b should be zeroed when a is healthy"
        );
        assert!(
            (s_a - 1.0).abs() < 1e-9,
            "a should have 100% share, got {s_a}"
        );
        assert_eq!(s_b, 0.0, "b should have zero share");
    }

    // Scenario 2: ENTIRE pool above soft_limit. a=0.95, b=0.92.
    // soft_limit=0.90 → both above. Without graceful degradation the
    // dashboard would go blank — instead BOTH should keep non-zero shares
    // matching what pick_account would still route to.
    {
        let state = test_state_with_soft_limit(
            vec![
                mk_endpoint("a", "sk-ant-api-a"),
                mk_endpoint("b", "sk-ant-api-b"),
            ],
            0.90,
        );
        for (i, util) in [(0, 0.95), (1, 0.92)].iter() {
            let mut info = state.endpoints[*i].rate_info.write().await;
            info.utilization_5h = Some(*util);
            info.reset_5h = Some(now + 10000);
            info.utilization = Some(*util);
            info.claims_7d.clear();
        }

        state.refresh_metrics_weights().await;

        let w_a = f64::from_bits(
            state.endpoints[0]
                .last_routing_weight
                .load(Ordering::Relaxed),
        );
        let w_b = f64::from_bits(
            state.endpoints[1]
                .last_routing_weight
                .load(Ordering::Relaxed),
        );
        let s_a = f64::from_bits(
            state.endpoints[0]
                .last_routing_share
                .load(Ordering::Relaxed),
        );
        let s_b = f64::from_bits(
            state.endpoints[1]
                .last_routing_share
                .load(Ordering::Relaxed),
        );

        assert!(
            w_a > 0.0,
            "degraded a should still have non-zero weight (graceful degradation)"
        );
        assert!(
            w_b > 0.0,
            "degraded b should still have non-zero weight (graceful degradation)"
        );
        assert!(
            (s_a + s_b - 1.0).abs() < 1e-9,
            "shares should sum to 1.0 in degraded pool"
        );
        // b has lower utilization → larger share
        assert!(
            s_b > s_a,
            "b (lower util) should outweigh a in degraded pool: a={s_a}, b={s_b}"
        );
    }
}

#[tokio::test]
async fn metrics_routing_weight_and_share_present() {
    let (mock_url, _handle) = spawn_mock_upstream().await;
    let (app, state) = test_app(&mock_url, None);

    let now_epoch = AppState::now_epoch();

    {
        let mut info = state.endpoints[0].rate_info.write().await;
        info.utilization = Some(0.20);
        info.utilization_5h = Some(0.20);
        info.reset_5h = Some(now_epoch + NEAR_RESET_5H_SECS as u64 + 600);
        info.status_5h = Some("allowed".to_string());
        info.claims_7d.insert(
            "seven_day".to_string(),
            ClaimWindowData {
                utilization: Some(0.50),
                reset: Some(now_epoch + TOTAL_7D_SECS as u64),
                status: Some("allowed".to_string()),
                ..Default::default()
            },
        );
        info.utilization_7d = Some(0.50);
        info.reset_7d = Some(now_epoch + TOTAL_7D_SECS as u64);
        info.status_7d = Some("allowed".to_string());
    }
    {
        let mut info = state.endpoints[1].rate_info.write().await;
        info.utilization = Some(1.0);
        info.utilization_5h = Some(1.0);
        info.reset_5h = Some(now_epoch + NEAR_RESET_5H_SECS as u64 + 600);
        info.status_5h = Some("allowed".to_string());
    }
    state.refresh_metrics_weights().await;

    let addr = serve(app).await;
    let client = Client::new();
    let resp = client
        .get(format!("http://{}/metrics", addr))
        .send()
        .await
        .unwrap();
    let body = resp.text().await.unwrap();

    assert!(
        body.contains("anthropic_account_routing_weight{account=\"acct-a\"} 0.4"),
        "missing routing_weight:\n{body}"
    );
    assert!(
        body.contains("anthropic_account_routing_share{account=\"acct-a\"} 1"),
        "missing routing_share:\n{body}"
    );
    assert!(
        body.contains("# TYPE anthropic_account_routing_weight gauge"),
        "missing routing_weight TYPE header:\n{body}"
    );
    assert!(
        body.contains("# TYPE anthropic_account_routing_share gauge"),
        "missing routing_share TYPE header:\n{body}"
    );
}

/// Integration: GET /metrics emits anthropic_account_routing_weight and
/// anthropic_account_routing_share for non-passthrough accounts only,
/// with the values populated by refresh_metrics_weights().
#[tokio::test]
async fn metrics_routing_weight_and_share() {
    let (mock_url, _handle) = spawn_mock_upstream().await;
    let (app, state) = test_app(&mock_url, None);

    // Default test_app builds 2 non-passthrough accounts: acct-a, acct-b.
    let now = AppState::now_epoch();
    {
        let mut info = state.endpoints[0].rate_info.write().await;
        info.utilization_5h = Some(0.25);
        info.reset_5h = Some(now + 10000);
        info.utilization = Some(0.25);
        info.claims_7d.clear();
    }
    {
        let mut info = state.endpoints[1].rate_info.write().await;
        info.utilization_5h = Some(0.50);
        info.reset_5h = Some(now + 10000);
        info.utilization = Some(0.50);
        info.claims_7d.clear();
    }
    state.refresh_metrics_weights().await;

    let addr = serve(app).await;
    let client = Client::new();
    let resp = client
        .get(format!("http://{}/metrics", addr))
        .send()
        .await
        .unwrap();
    let body = resp.text().await.unwrap();

    // Both metric families are emitted with the right HELP/TYPE headers
    assert!(
        body.contains("# HELP anthropic_account_routing_weight"),
        "missing routing_weight HELP:\n{body}"
    );
    assert!(
        body.contains("# TYPE anthropic_account_routing_weight gauge"),
        "missing routing_weight TYPE:\n{body}"
    );
    assert!(
        body.contains("# HELP anthropic_account_routing_share"),
        "missing routing_share HELP:\n{body}"
    );

    // Both accounts get a routing_weight line labeled by name
    assert!(
        body.contains("anthropic_account_routing_weight{account=\"acct-a\"}"),
        "missing acct-a routing_weight:\n{body}"
    );
    assert!(
        body.contains("anthropic_account_routing_weight{account=\"acct-b\"}"),
        "missing acct-b routing_weight:\n{body}"
    );

    // Shares for the two accounts must sum to ≈ 1.0 (parsed out of the body)
    let parse_share = |account: &str| -> f64 {
        let needle = format!("anthropic_account_routing_share{{account=\"{account}\"}} ");
        let line = body
            .lines()
            .find(|l| l.starts_with(&needle))
            .unwrap_or_else(|| panic!("no share line for {account}"));
        line[needle.len()..]
            .trim()
            .parse::<f64>()
            .expect("parseable share")
    };
    let total = parse_share("acct-a") + parse_share("acct-b");
    assert!(
        (total - 1.0).abs() < 1e-9,
        "shares should sum to 1.0, got {total}\n{body}"
    );

    // acct-a (lower utilization) should get the larger share
    assert!(
        parse_share("acct-a") > parse_share("acct-b"),
        "acct-a should outweigh acct-b\n{body}"
    );
}

#[tokio::test]
async fn routing_metrics_zero_share_for_soft_limited_account_matches_pick_account() {
    let mut state = test_state_with(vec![
        mk_endpoint("healthy", "sk-ant-api-a"),
        mk_endpoint("soft-limited", "sk-ant-api-b"),
    ]);
    Arc::get_mut(&mut state).unwrap().soft_limit = 0.90;
    let now_epoch = AppState::now_epoch();

    {
        let mut info = state.endpoints[0].rate_info.write().await;
        info.utilization = Some(0.30);
        info.utilization_5h = Some(0.30);
        info.reset_5h = Some(now_epoch + 10000);
        info.status_5h = Some("allowed".to_string());
    }
    {
        let mut info = state.endpoints[1].rate_info.write().await;
        info.utilization = Some(0.95);
        info.utilization_5h = Some(0.95);
        info.reset_5h = Some(now_epoch + 10000);
        info.status_5h = Some("allowed".to_string());
    }

    state.refresh_metrics_weights().await;
    let mut buf = String::new();
    append_routing_weight_metrics(
        &mut buf,
        &state.endpoints,
        &[
            EndpointMetricsSnap {
                name: "healthy".to_string(),
                utilization: Some(0.30),
                utilization_5h: Some(0.30),
                reset_5h: Some(now_epoch + 10000),
                status_5h: Some("allowed".to_string()),
                ..Default::default()
            },
            EndpointMetricsSnap {
                name: "soft-limited".to_string(),
                utilization: Some(0.95),
                utilization_5h: Some(0.95),
                reset_5h: Some(now_epoch + 10000),
                status_5h: Some("allowed".to_string()),
                ..Default::default()
            },
        ],
    );

    assert!(
        buf.contains("anthropic_account_routing_share{account=\"healthy\"} 1"),
        "healthy account should receive full routing share:
{buf}"
    );
    assert!(
        buf.contains("anthropic_account_routing_share{account=\"soft-limited\"} 0"),
        "soft-limited account should have zero routing share:
{buf}"
    );
    assert!(
        buf.contains("anthropic_account_routing_weight{account=\"soft-limited\"} 0"),
        "soft-limited account should have zero routing weight:
{buf}"
    );

    for i in 0..20 {
        let key = format!("client-{i}");
        let idx = state.pick_endpoint(Some(&key), "any", &[]).await.unwrap();
        assert_eq!(
            idx, 0,
            "client '{}' routed to soft-limited account despite exported zero share",
            key
        );
    }
}

/// Integration: passthrough accounts must NOT appear in routing_weight or
/// routing_share output (refresh_metrics_weights skips them).
#[tokio::test]
async fn metrics_routing_weight_omits_passthrough() {
    let acct_a = mk_endpoint("a", "sk-ant-api-a");
    let acct_pt = mk_endpoint("pt", "passthrough");
    let state = test_state_with(vec![acct_a, acct_pt]);

    let now = AppState::now_epoch();
    {
        let mut info = state.endpoints[0].rate_info.write().await;
        info.utilization_5h = Some(0.25);
        info.reset_5h = Some(now + 10000);
        info.utilization = Some(0.25);
        info.claims_7d.clear();
    }
    state.refresh_metrics_weights().await;

    let app = Router::new()
        .route("/metrics", axum::routing::get(metrics_handler))
        .with_state(state.clone());
    let addr = serve(app).await;
    let client = Client::new();
    let body = client
        .get(format!("http://{}/metrics", addr))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();

    // a is present
    assert!(
        body.contains("anthropic_account_routing_weight{account=\"a\"}"),
        "missing routing_weight for a:\n{body}"
    );
    // pt (passthrough) must be absent from BOTH families
    assert!(
        !body.contains("anthropic_account_routing_weight{account=\"pt\""),
        "passthrough account must not appear in routing_weight:\n{body}"
    );
    assert!(
        !body.contains("anthropic_account_routing_share{account=\"pt\""),
        "passthrough account must not appear in routing_share:\n{body}"
    );
}

#[test]
fn oauth_system_prompt_injects_when_missing() {
    let mut body = serde_json::json!({
        "model": "claude-sonnet-4-6",
        "messages": [{"role": "user", "content": "hi"}],
        "max_tokens": 5
    });
    inject_oauth_system_prompt(&mut body);
    let system = body.get("system").unwrap().as_array().unwrap();
    assert_eq!(system.len(), 1);
    assert_eq!(system[0]["text"].as_str().unwrap(), OAUTH_SYSTEM_PROMPT);
}

#[test]
fn oauth_system_prompt_prepends_to_existing_string() {
    let mut body = serde_json::json!({
        "model": "claude-sonnet-4-6",
        "system": "Be helpful.",
        "messages": [{"role": "user", "content": "hi"}],
        "max_tokens": 5
    });
    inject_oauth_system_prompt(&mut body);
    let system = body.get("system").unwrap().as_array().unwrap();
    assert_eq!(system.len(), 2);
    assert_eq!(system[0]["text"].as_str().unwrap(), OAUTH_SYSTEM_PROMPT);
    assert_eq!(system[1]["text"].as_str().unwrap(), "Be helpful.");
}

#[test]
fn oauth_system_prompt_prepends_to_existing_array() {
    let mut body = serde_json::json!({
        "model": "claude-sonnet-4-6",
        "system": [{"type": "text", "text": "Be helpful."}],
        "messages": [{"role": "user", "content": "hi"}],
        "max_tokens": 5
    });
    inject_oauth_system_prompt(&mut body);
    let system = body.get("system").unwrap().as_array().unwrap();
    assert_eq!(system.len(), 2);
    assert_eq!(system[0]["text"].as_str().unwrap(), OAUTH_SYSTEM_PROMPT);
    assert_eq!(system[1]["text"].as_str().unwrap(), "Be helpful.");
}

#[test]
fn oauth_system_prompt_noop_when_already_present() {
    let mut body = serde_json::json!({
        "model": "claude-sonnet-4-6",
        "system": [{"type": "text", "text": OAUTH_SYSTEM_PROMPT}],
        "messages": [{"role": "user", "content": "hi"}],
        "max_tokens": 5
    });
    inject_oauth_system_prompt(&mut body);
    let system = body.get("system").unwrap().as_array().unwrap();
    assert_eq!(system.len(), 1, "should not duplicate");
}

#[test]
fn oauth_system_prompt_noop_when_prompt_is_prefix_string() {
    // CC may send the identity prompt as prefix of a longer system string
    let system_text = format!("{}\n\nYou are an interactive agent.", OAUTH_SYSTEM_PROMPT);
    let mut body = serde_json::json!({
        "model": "claude-sonnet-4-6",
        "system": system_text,
        "messages": [{"role": "user", "content": "hi"}],
        "max_tokens": 5
    });
    inject_oauth_system_prompt(&mut body);
    // Should remain a string, untouched
    assert!(body["system"].is_string(), "should not convert to array");
    assert_eq!(body["system"].as_str().unwrap(), system_text);
}

#[test]
fn oauth_system_prompt_noop_when_prompt_is_prefix_array() {
    // CC may embed the identity prompt as prefix of first block text
    let block_text = format!("{}\n\nYou are an interactive agent.", OAUTH_SYSTEM_PROMPT);
    let mut body = serde_json::json!({
        "model": "claude-sonnet-4-6",
        "system": [{"type": "text", "text": block_text}],
        "messages": [{"role": "user", "content": "hi"}],
        "max_tokens": 5
    });
    inject_oauth_system_prompt(&mut body);
    let system = body.get("system").unwrap().as_array().unwrap();
    assert_eq!(system.len(), 1, "should not prepend duplicate");
    assert_eq!(system[0]["text"].as_str().unwrap(), block_text);
}

#[test]
fn oauth_system_prompt_handles_null_system() {
    let mut body = serde_json::json!({
        "model": "claude-sonnet-4-6",
        "system": null,
        "messages": [{"role": "user", "content": "hi"}],
        "max_tokens": 5
    });
    inject_oauth_system_prompt(&mut body);
    let system = body.get("system").unwrap().as_array().unwrap();
    assert_eq!(system.len(), 1);
    assert_eq!(system[0]["text"].as_str().unwrap(), OAUTH_SYSTEM_PROMPT);
}

/// Regression: CC 142+ prepends a billing header as system[0], pushing the
/// CC identity prompt to system[1+]. has_oauth_system_prompt must scan all
/// blocks, not just the first — otherwise inject_oauth_system_prompt
/// re-serializes the body, breaking Anthropic's byte-prefix cache matching.
#[test]
fn oauth_system_prompt_detected_in_later_block() {
    // system[0] is a non-prompt block (billing header), system[1] has the CC prompt
    let mut body = serde_json::json!({
        "model": "claude-sonnet-4-6",
        "system": [
            {"type": "text", "text": "x-anthropic-billing-header: cc_version=2.1.109"},
            {"type": "text", "text": OAUTH_SYSTEM_PROMPT}
        ],
        "messages": [{"role": "user", "content": "hi"}],
        "max_tokens": 5
    });

    // Must detect prompt at system[1] — no re-injection
    assert!(
        has_oauth_system_prompt(&body),
        "should detect CC prompt in system[1]"
    );
    inject_oauth_system_prompt(&mut body);
    let system = body["system"].as_array().unwrap();
    assert_eq!(
        system.len(),
        2,
        "should not prepend — prompt already present"
    );
    assert_eq!(
        system[1]["text"].as_str().unwrap(),
        OAUTH_SYSTEM_PROMPT,
        "CC prompt should remain at system[1]"
    );
}

/// Regression: full proxy roundtrip with OAuth account where the CC prompt
/// is at system[1+]. Verifies the proxy does not re-serialize the body
/// (which would break upstream prompt cache matching).
#[tokio::test]
async fn oauth_system_prompt_no_reserialize_when_in_later_block() {
    use std::sync::Arc as StdArc;

    // Mock upstream that captures the raw request body
    let captured_body: StdArc<tokio::sync::Mutex<Vec<u8>>> =
        StdArc::new(tokio::sync::Mutex::new(Vec::new()));
    let captured = captured_body.clone();
    let mock_app = Router::new().fallback(any(move |req: Request<Body>| {
        let captured = captured.clone();
        async move {
            let body_bytes = axum::body::to_bytes(req.into_body(), 1024 * 1024)
                .await
                .unwrap();
            *captured.lock().await = body_bytes.to_vec();

            let mut resp = axum::Json(serde_json::json!({
                "id": "msg_test",
                "type": "message",
                "content": [{"type": "text", "text": "ok"}],
                "model": "claude-sonnet-4-6",
                "stop_reason": "end_turn",
                "usage": {"input_tokens": 10, "output_tokens": 1}
            }))
            .into_response();
            resp.headers_mut().insert(
                "anthropic-ratelimit-unified-representative-claim",
                HeaderValue::from_static("five_hour"),
            );
            resp.headers_mut().insert(
                "anthropic-ratelimit-unified-5h-utilization",
                HeaderValue::from_static("0.10"),
            );
            let reset = (std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs()
                + 3600)
                .to_string();
            resp.headers_mut().insert(
                "anthropic-ratelimit-unified-5h-reset",
                HeaderValue::from_str(&reset).unwrap(),
            );
            resp
        }
    }));

    let mock_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mock_addr = mock_listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(mock_listener, mock_app).await.unwrap();
    });

    // Build app with an OAuth account, auto_cache off for clean signal
    let accounts = vec![mk_endpoint_at(
        "oauth-acct",
        "sk-ant-oat01-test-token",
        &format!("http://{}", mock_addr),
    )];
    let state = Arc::new(AppState {
        endpoints: accounts,
        state_path: PathBuf::from("/tmp/anthropic-lb-oauth-regression.state.json"),
        auto_cache: false,
        ..test_state_base()
    });

    let app = build_router(state);
    let addr = serve(app).await;

    // Request body: CC prompt at system[1], billing header at system[0]
    let request_body = serde_json::json!({
        "model": "claude-sonnet-4-6",
        "system": [
            {"type": "text", "text": "x-anthropic-billing-header: cc_version=2.1.109"},
            {"type": "text", "text": OAUTH_SYSTEM_PROMPT}
        ],
        "messages": [{"role": "user", "content": "hi"}],
        "max_tokens": 5
    });
    let request_bytes = serde_json::to_vec(&request_body).unwrap();

    let client = Client::new();
    let resp = client
        .post(format!("http://{}/v1/messages", addr))
        .header("content-type", "application/json")
        .body(request_bytes.clone())
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    // The proxy should have forwarded the original body byte-for-byte (no re-serialization).
    // Serde roundtrip can reorder keys or change whitespace — only raw byte comparison
    // catches the cache-breaking re-serialization bug this test guards against.
    let forwarded = captured_body.lock().await;
    assert_eq!(
        forwarded.as_slice(),
        request_bytes.as_slice(),
        "forwarded body must be byte-identical to request (re-serialization breaks upstream cache)"
    );
    let forwarded_body: serde_json::Value = serde_json::from_slice(&forwarded).unwrap();
    let system = forwarded_body["system"].as_array().unwrap();
    assert_eq!(
        system.len(),
        2,
        "proxy must not prepend another CC prompt — it was already at system[1]"
    );
    assert_eq!(
        system[0]["text"].as_str().unwrap(),
        "x-anthropic-billing-header: cc_version=2.1.109",
        "system[0] should be the billing header, untouched"
    );
    assert_eq!(
        system[1]["text"].as_str().unwrap(),
        OAUTH_SYSTEM_PROMPT,
        "system[1] should be the CC prompt, untouched"
    );
}

/// Integration test: verify OAuth accounts get the CC system prompt injected
/// in requests sent through the OpenAI-compat endpoint.
#[tokio::test]
async fn openai_compat_injects_oauth_system_prompt() {
    // Mock upstream that captures and validates the request body
    let mock_app = Router::new().fallback(any(|req: Request<Body>| async move {
        let body_bytes = axum::body::to_bytes(req.into_body(), 1024 * 1024)
            .await
            .unwrap();
        let body: serde_json::Value = serde_json::from_slice(&body_bytes).unwrap();

        // Verify the CC system prompt is the first system block
        let system = body.get("system").expect("missing system field");
        let arr = system.as_array().expect("system should be array");
        assert_eq!(
            arr[0]["text"].as_str().unwrap(),
            OAUTH_SYSTEM_PROMPT,
            "first system block must be CC prompt"
        );

        // Return valid Anthropic response
        let mut resp = axum::Json(serde_json::json!({
            "id": "msg_test",
            "type": "message",
            "content": [{"type": "text", "text": "ok"}],
            "model": "claude-sonnet-4-6",
            "stop_reason": "end_turn",
            "usage": {"input_tokens": 10, "output_tokens": 1}
        }))
        .into_response();
        resp.headers_mut().insert(
            "anthropic-ratelimit-unified-representative-claim",
            HeaderValue::from_static("five_hour"),
        );
        resp.headers_mut().insert(
            "anthropic-ratelimit-unified-5h-utilization",
            HeaderValue::from_static("0.10"),
        );
        resp
    }));

    let mock_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mock_addr = mock_listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(mock_listener, mock_app).await.unwrap();
    });

    // Build app with an OAuth account
    let accounts = vec![mk_endpoint_at(
        "oauth-acct",
        "sk-ant-oat01-test-token",
        &format!("http://{}", mock_addr),
    )];
    let state = Arc::new(AppState {
        endpoints: accounts,
        state_path: PathBuf::from("/tmp/anthropic-lb-oauth-test.state.json"),
        auto_cache: false, // disable to keep body simple
        ..test_state_base()
    });

    let app = Router::new()
        .route(
            "/v1/chat/completions",
            axum::routing::post(openai_chat_handler),
        )
        .with_state(state);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(
            listener,
            app.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await
        .unwrap();
    });

    let client = Client::new();
    let resp = client
            .post(format!("http://{}/v1/chat/completions", addr))
            .header("content-type", "application/json")
            .body(r#"{"model":"claude-sonnet-4-6","messages":[{"role":"user","content":"hi"}],"max_tokens":5}"#)
            .send()
            .await
            .unwrap();

    assert_eq!(resp.status(), reqwest::StatusCode::OK);
}

/// Test that non-2xx upstream errors are translated to OpenAI error format.
#[tokio::test]
async fn openai_compat_translates_upstream_json_error() {
    // Mock upstream returns 400 with Anthropic-format error
    let mock_app = Router::new().fallback(any(|_req: Request<Body>| async {
            Response::builder()
                .status(StatusCode::BAD_REQUEST)
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"type":"error","error":{"type":"invalid_request_error","message":"max_tokens: must be positive"}}"#,
                ))
                .unwrap()
        }));

    let mock_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mock_addr = mock_listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(mock_listener, mock_app).await.unwrap();
    });

    let (app, _state) = test_openai_app(&format!("http://{}", mock_addr), None);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(
            listener,
            app.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await
        .unwrap();
    });

    let client = Client::new();
    let resp = client
            .post(format!("http://{}/v1/chat/completions", addr))
            .header("content-type", "application/json")
            .body(r#"{"model":"claude-sonnet-4-6","messages":[{"role":"user","content":"hi"}],"max_tokens":5}"#)
            .send()
            .await
            .unwrap();

    assert_eq!(resp.status(), reqwest::StatusCode::BAD_REQUEST);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["error"]["message"], "max_tokens: must be positive");
    assert_eq!(body["error"]["type"], "invalid_request_error");
    assert!(body["error"]["param"].is_null());
}

/// Test that non-JSON upstream errors are wrapped in OpenAI error format.
#[tokio::test]
async fn openai_compat_translates_upstream_raw_error() {
    // Mock upstream returns 422 with plain text (non-retryable, non-JSON)
    let mock_app = Router::new().fallback(any(|_req: Request<Body>| async {
        Response::builder()
            .status(StatusCode::UNPROCESSABLE_ENTITY)
            .header("content-type", "text/plain")
            .body(Body::from("upstream timeout"))
            .unwrap()
    }));

    let mock_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mock_addr = mock_listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(mock_listener, mock_app).await.unwrap();
    });

    let (app, _state) = test_openai_app(&format!("http://{}", mock_addr), None);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(
            listener,
            app.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await
        .unwrap();
    });

    let client = Client::new();
    let resp = client
            .post(format!("http://{}/v1/chat/completions", addr))
            .header("content-type", "application/json")
            .body(r#"{"model":"claude-sonnet-4-6","messages":[{"role":"user","content":"hi"}],"max_tokens":5}"#)
            .send()
            .await
            .unwrap();

    assert_eq!(resp.status(), reqwest::StatusCode::UNPROCESSABLE_ENTITY);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["error"]["message"], "upstream timeout");
    assert_eq!(body["error"]["type"], "api_error");
}

#[test]
fn inject_auth_api_key() {
    let mut headers = axum::http::HeaderMap::new();
    headers.insert("authorization", HeaderValue::from_static("Bearer old"));
    inject_account_auth(&mut headers, "sk-ant-api-test123", false, &default_betas());
    assert_eq!(headers.get("x-api-key").unwrap(), "sk-ant-api-test123");
    assert!(headers.get("authorization").is_none());
}

#[test]
fn inject_auth_oauth_token() {
    let mut headers = axum::http::HeaderMap::new();
    inject_account_auth(&mut headers, "sk-ant-oat-test123", false, &default_betas());
    assert_eq!(
        headers.get("authorization").unwrap(),
        "Bearer sk-ant-oat-test123"
    );
    assert_eq!(
        headers
            .get("anthropic-dangerous-direct-browser-access")
            .unwrap(),
        "true"
    );
    let beta = headers.get("anthropic-beta").unwrap().to_str().unwrap();
    assert!(beta.contains("oauth-2025-04-20"));
    assert!(beta.contains("claude-code-20250219"));
}

#[test]
fn inject_auth_oauth_merges_multi_value_beta() {
    // Allow-listed flags split across TWO header values (LAB-1191: unlisted
    // flags are dropped, so the merge is exercised with allowed ones).
    let mut headers = axum::http::HeaderMap::new();
    headers.append(
        "anthropic-beta",
        HeaderValue::from_static("interleaved-thinking-2025-05-14"),
    );
    headers.append(
        "anthropic-beta",
        HeaderValue::from_static("fine-grained-tool-streaming-2025-05-14"),
    );
    inject_account_auth(&mut headers, "sk-ant-oat-test123", false, &default_betas());
    let beta = headers.get("anthropic-beta").unwrap().to_str().unwrap();
    assert!(
        beta.contains("interleaved-thinking-2025-05-14"),
        "should preserve first header"
    );
    assert!(
        beta.contains("fine-grained-tool-streaming-2025-05-14"),
        "should preserve second header"
    );
    assert!(beta.contains("oauth-2025-04-20"), "should add oauth flag");
    assert!(beta.contains("claude-code-20250219"), "should add cc flag");
}

#[test]
fn inject_auth_passthrough_preserves_headers() {
    let mut headers = axum::http::HeaderMap::new();
    headers.insert(
        "authorization",
        HeaderValue::from_static("Bearer user-token"),
    );
    headers.insert("x-api-key", HeaderValue::from_static("user-key"));
    inject_account_auth(&mut headers, "passthrough", true, &default_betas());
    assert_eq!(headers.get("authorization").unwrap(), "Bearer user-token");
    assert_eq!(headers.get("x-api-key").unwrap(), "user-key");
}

#[test]
fn request_context_trims_whitespace_headers() {
    let state = test_state_with(vec![mk_endpoint("a", "sk-ant-api-x")]);
    let mut headers = axum::http::HeaderMap::new();
    headers.insert("x-agent-id", HeaderValue::from_static("  "));
    headers.insert("x-session-id", HeaderValue::from_static(" \t "));
    let ip: IpAddr = "127.0.0.1".parse().unwrap();
    let rctx = RequestContext::from_request(&state, &ip, &headers, None);
    assert_eq!(rctx.agent_id, "-", "whitespace-only agent_id should be -");
    assert_eq!(
        rctx.session_id, "-",
        "whitespace-only session_id should be -"
    );
    assert!(
        rctx.affinity_key(&ip, None).is_none(),
        "no meaningful identity"
    );
}

fn rctx_for(client_id: &str, agent: &str, session: &str) -> RequestContext {
    RequestContext {
        client_id: client_id.to_string(),
        client_ver: "-".to_string(),
        agent_id: agent.to_string(),
        session_id: session.to_string(),
    }
}

#[test]
fn affinity_key_fp_distinguishes_fanout_under_one_session() {
    // The workflow case: many agents share ONE coarse session-id but have
    // distinct content fingerprints. They MUST get distinct keys so they
    // distribute instead of funneling onto one account.
    let ip: IpAddr = "10.88.0.1".parse().unwrap();
    let rctx = rctx_for("claude:first-steps", "-", "aaee4c00");
    let a = rctx.affinity_key(&ip, Some("fp_agent_a")).unwrap();
    let b = rctx.affinity_key(&ip, Some("fp_agent_b")).unwrap();
    assert_ne!(
        a, b,
        "same session, different fp must yield different keys (distribute the fan-out)"
    );
}

#[test]
fn affinity_key_fp_stable_sticks() {
    // A stable-prefix conversation produces the same fp across turns → same
    // key → stays sticky.
    let ip: IpAddr = "10.88.0.1".parse().unwrap();
    let rctx = rctx_for("claude:first-steps", "-", "aaee4c00");
    let t1 = rctx.affinity_key(&ip, Some("stable_fp")).unwrap();
    let t2 = rctx.affinity_key(&ip, Some("stable_fp")).unwrap();
    assert_eq!(t1, t2, "same fp must yield the same key (sticky)");
}

#[test]
fn affinity_key_without_fp_is_unchanged() {
    // When fp is absent the key must be byte-identical to the legacy
    // header-only form — no mass rehash of existing header-identity traffic.
    let ip: IpAddr = "10.88.0.1".parse().unwrap();
    let rctx = rctx_for("claude:fish", "-", "5f4a96c6");
    assert_eq!(
        rctx.affinity_key(&ip, None).unwrap(),
        "10.88.0.1:claude:fish:-:5f4a96c6"
    );
}

#[test]
fn affinity_key_fp_alone_provides_identity() {
    // Headerless one-shot (no client/agent/session) still gets a key from fp,
    // so it distributes deterministically instead of falling to round-robin.
    let ip: IpAddr = "10.88.0.1".parse().unwrap();
    let rctx = rctx_for("-", "-", "-");
    assert!(
        rctx.affinity_key(&ip, None).is_none(),
        "no identity and no fp → None"
    );
    assert!(
        rctx.affinity_key(&ip, Some("fp_x")).is_some(),
        "fp alone must provide an affinity key"
    );
}

// ── Integration: fallback upstream ──────────────────────────────

/// Mock OpenAI-compatible upstream that returns chat completion responses.
async fn mock_openai_upstream_handler(req: Request<Body>) -> Response {
    // Verify Bearer auth
    let auth = req
        .headers()
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    if !auth.starts_with("Bearer ") {
        return (StatusCode::UNAUTHORIZED, "missing bearer auth").into_response();
    }

    let body_bytes = axum::body::to_bytes(req.into_body(), 1_000_000)
        .await
        .unwrap();
    let body: serde_json::Value = serde_json::from_slice(&body_bytes).unwrap();

    let is_streaming = body
        .get("stream")
        .and_then(|s| s.as_bool())
        .unwrap_or(false);

    if is_streaming {
        let sse = [
                "data: {\"id\":\"chatcmpl-fb1\",\"object\":\"chat.completion.chunk\",\"model\":\"gpt-4\",\"choices\":[{\"delta\":{\"role\":\"assistant\",\"content\":\"\"},\"finish_reason\":null}]}\n\n",
                "data: {\"id\":\"chatcmpl-fb1\",\"object\":\"chat.completion.chunk\",\"model\":\"gpt-4\",\"choices\":[{\"delta\":{\"content\":\"Fallback\"},\"finish_reason\":null}]}\n\n",
                "data: {\"id\":\"chatcmpl-fb1\",\"object\":\"chat.completion.chunk\",\"model\":\"gpt-4\",\"choices\":[{\"delta\":{\"content\":\" works\"},\"finish_reason\":null}]}\n\n",
                "data: {\"id\":\"chatcmpl-fb1\",\"object\":\"chat.completion.chunk\",\"model\":\"gpt-4\",\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n",
                "data: [DONE]\n\n",
            ];
        return Response::builder()
            .status(StatusCode::OK)
            .header("content-type", "text/event-stream")
            .body(Body::from(sse.join("")))
            .unwrap();
    }

    axum::Json(serde_json::json!({
        "id": "chatcmpl-fallback",
        "object": "chat.completion",
        "model": "gpt-4",
        "choices": [{
            "index": 0,
            "message": {
                "role": "assistant",
                "content": "Fallback response"
            },
            "finish_reason": "stop"
        }],
        "usage": {
            "prompt_tokens": 10,
            "completion_tokens": 3,
            "total_tokens": 13
        }
    }))
    .into_response()
}

#[tokio::test]
async fn proxy_handler_falls_back_to_upstream() {
    // Start mock OpenAI upstream
    let mock_app = Router::new().fallback(any(mock_openai_upstream_handler));
    let mock_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mock_addr = mock_listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(mock_listener, mock_app).await.unwrap();
    });
    let mock_url = format!("http://{}", mock_addr);

    // Build state with the Anthropic endpoint hard-limited and a
    // priority-100 OpenAI endpoint as the fallback.
    let mut openai = make_endpoint("fallback", Protocol::OpenAI);
    openai.base_url = mock_url.clone();
    openai.priority = 100;
    let state = Arc::new(AppState {
        endpoints: vec![mk_endpoint("acct-a", "sk-ant-api-a"), openai],
        state_path: PathBuf::from("/tmp/anthropic-lb-fallback-test.state.json"),
        auto_cache: false,
        ..test_state_base()
    });

    // Hard-limit the only account
    {
        let mut info = state.endpoints[0].rate_info.write().await;
        info.hard_limited_until = Some(Instant::now() + Duration::from_secs(3600));
    }

    let app = Router::new()
        .fallback(any(proxy_handler))
        .with_state(state.clone());

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(
            listener,
            app.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await
        .unwrap();
    });

    let client = Client::new();

    // Non-streaming request (Anthropic format) → should get Anthropic-format response via fallback
    let resp = client
        .post(format!("http://{}/v1/messages", addr))
        .header("content-type", "application/json")
        .json(&serde_json::json!({
            "model": "claude-sonnet-4-6",
            "messages": [{"role": "user", "content": "Hello"}],
            "max_tokens": 1024
        }))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["type"], "message");
    assert_eq!(body["content"][0]["type"], "text");
    assert_eq!(body["content"][0]["text"], "Fallback response");
    assert_eq!(body["stop_reason"], "end_turn");

    // Verify the OpenAI endpoint got the request
    assert_eq!(state.endpoints[1].requests.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn proxy_handler_fallback_streaming() {
    // Start mock OpenAI upstream
    let mock_app = Router::new().fallback(any(mock_openai_upstream_handler));
    let mock_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mock_addr = mock_listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(mock_listener, mock_app).await.unwrap();
    });
    let mock_url = format!("http://{}", mock_addr);

    let mut openai = make_endpoint("fallback", Protocol::OpenAI);
    openai.base_url = mock_url.clone();
    openai.priority = 100;
    let state = Arc::new(AppState {
        endpoints: vec![mk_endpoint("acct-a", "sk-ant-api-a"), openai],
        state_path: PathBuf::from("/tmp/anthropic-lb-fallback-stream-test.state.json"),
        auto_cache: false,
        ..test_state_base()
    });

    {
        let mut info = state.endpoints[0].rate_info.write().await;
        info.hard_limited_until = Some(Instant::now() + Duration::from_secs(3600));
    }

    let app = Router::new()
        .fallback(any(proxy_handler))
        .with_state(state.clone());

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(
            listener,
            app.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await
        .unwrap();
    });

    let client = Client::new();

    // Streaming request
    let resp = client
        .post(format!("http://{}/v1/messages", addr))
        .header("content-type", "application/json")
        .json(&serde_json::json!({
            "model": "claude-sonnet-4-6",
            "messages": [{"role": "user", "content": "Hello"}],
            "max_tokens": 1024,
            "stream": true
        }))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(
        resp.headers().get("content-type").unwrap(),
        "text/event-stream"
    );

    let body = resp.text().await.unwrap();
    // Should contain Anthropic SSE events (translated from OpenAI)
    assert!(
        body.contains("message_start"),
        "should have message_start event"
    );
    assert!(body.contains("text_delta"), "should have text_delta events");
    assert!(body.contains("Fallback"), "should contain 'Fallback' text");
    assert!(
        body.contains("message_stop"),
        "should have message_stop event"
    );
}

#[tokio::test]
async fn fallback_translated_stream_no_error_frame_after_message_stop() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    // LAB-710: a transport read failure AFTER the upstream's `[DONE]` must
    // not ship an Anthropic error frame — the translated `message_stop`
    // already terminated the stream from the client's view. Mirror of the
    // passthrough `sent_done` guard, one protocol over.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mock_addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut buf = vec![0u8; 8192];
        let _ = stream.read(&mut buf).await;
        let head = "HTTP/1.1 200 OK\r\n\
             content-type: text/event-stream\r\n\
             transfer-encoding: chunked\r\n\
             \r\n";
        let _ = stream.write_all(head.as_bytes()).await;
        let body = concat!(
            "data: {\"id\":\"c1\",\"model\":\"gpt-4\",\"choices\":[{\"delta\":{\"role\":\"assistant\",\"content\":\"Hi\"},\"finish_reason\":null}]}\n\n",
            "data: {\"id\":\"c1\",\"model\":\"gpt-4\",\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n",
            "data: [DONE]\n\n",
        );
        let chunk = format!("{:x}\r\n{}\r\n", body.len(), body);
        let _ = stream.write_all(chunk.as_bytes()).await;
        // Drop WITHOUT the 0-length chunked terminator: the proxy's next
        // resp.chunk() errors after message_stop already went downstream.
        let _ = stream.shutdown().await;
    });

    let mut openai = make_endpoint("fallback", Protocol::OpenAI);
    openai.base_url = format!("http://{}", mock_addr);
    openai.priority = 100;
    let state = Arc::new(AppState {
        endpoints: vec![mk_endpoint("acct-a", "sk-ant-api-a"), openai],
        state_path: PathBuf::from("/tmp/anthropic-lb-done-then-err-test.state.json"),
        auto_cache: false,
        ..test_state_base()
    });
    {
        let mut info = state.endpoints[0].rate_info.write().await;
        info.hard_limited_until = Some(Instant::now() + Duration::from_secs(3600));
    }

    let app = Router::new()
        .fallback(any(proxy_handler))
        .with_state(state.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(
            listener,
            app.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await
        .unwrap();
    });

    let resp = Client::new()
        .post(format!("http://{}/v1/messages", addr))
        .header("content-type", "application/json")
        .json(&serde_json::json!({
            "model": "claude-sonnet-4-6",
            "messages": [{"role": "user", "content": "Hello"}],
            "max_tokens": 1024,
            "stream": true
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = resp.text().await.unwrap();

    assert!(
        body.contains("message_stop"),
        "stream completed — must carry the success terminator, got: {body:?}"
    );
    assert!(
        !body.contains("event: error"),
        "no error frame may follow message_stop, got: {body:?}"
    );
}

#[tokio::test]
async fn proxy_handler_no_fallback_returns_429() {
    // With no fallback endpoint, an exhausted pool yields None (→ 429).
    let state = test_state_with(vec![mk_endpoint("a", "sk-ant-api-a")]);

    // Hard-limit the only endpoint
    {
        let mut info = state.endpoints[0].rate_info.write().await;
        info.hard_limited_until = Some(Instant::now() + Duration::from_secs(3600));
    }

    let result = state.pick_endpoint(None, "", &[]).await;
    assert!(
        result.is_none(),
        "should return None when endpoint is hard-limited"
    );
}

// ── Unified endpoint: fallback retry semantics ─────────────────

#[tokio::test]
async fn try_fallback_upstream_rotates_on_429() {
    // Mock upstream returns 429
    let app = Router::new().fallback(any(|| async {
        (StatusCode::TOO_MANY_REQUESTS, "rate limited").into_response()
    }));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    let mut state = test_state_with(vec![]);
    let mut ep = make_endpoint("rl-gw", Protocol::OpenAI);
    ep.base_url = format!("http://{}", addr);
    Arc::get_mut(&mut state).unwrap().endpoints.push(ep);

    let body = br#"{"model":"claude-opus-4-7","messages":[],"max_tokens":1}"#;
    let result = try_fallback_upstream(
        &state,
        body,
        "req-1",
        "client-1",
        &"127.0.0.1".parse().unwrap(),
        "-",
        "-",
        "claude-opus-4-7",
        0,
        Instant::now(),
        false,
    )
    .await;
    assert!(
        matches!(
            result,
            ForwardOutcome::Retry {
                push_skip: true,
                transient: false,
                ..
            }
        ),
        "429 must rotate (skip) — an HTTP error is not a transport failure"
    );
}

#[tokio::test]
async fn try_fallback_upstream_rotates_on_500() {
    let app = Router::new().fallback(any(|| async {
        (StatusCode::INTERNAL_SERVER_ERROR, "boom").into_response()
    }));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    let mut state = test_state_with(vec![]);
    let mut ep = make_endpoint("broken", Protocol::OpenAI);
    ep.base_url = format!("http://{}", addr);
    Arc::get_mut(&mut state).unwrap().endpoints.push(ep);

    let body = br#"{"model":"claude-opus-4-7","messages":[],"max_tokens":1}"#;
    let result = try_fallback_upstream(
        &state,
        body,
        "req-1",
        "client-1",
        &"127.0.0.1".parse().unwrap(),
        "-",
        "-",
        "claude-opus-4-7",
        0,
        Instant::now(),
        false,
    )
    .await;
    assert!(
        matches!(
            result,
            ForwardOutcome::Retry {
                push_skip: true,
                transient: false,
                ..
            }
        ),
        "500 must rotate (skip) — an HTTP error is not a transport failure"
    );
}

/// Transport send failures on the OpenAI branch must be classified
/// `transient` (round-gated retry) AND feed the circuit breaker — the #69
/// gap where they were swallowed to a bare `None` with no health signal.
#[tokio::test]
async fn try_fallback_upstream_transport_error_is_transient_and_counted() {
    let url = spawn_dead_upstream().await;
    let mut state = test_state_with(vec![]);
    let mut ep = make_endpoint("dead-gw", Protocol::OpenAI);
    ep.base_url = url;
    Arc::get_mut(&mut state).unwrap().endpoints.push(ep);

    let body = br#"{"model":"claude-opus-4-7","messages":[],"max_tokens":1}"#;
    let result = try_fallback_upstream(
        &state,
        body,
        "req-1",
        "client-1",
        &"127.0.0.1".parse().unwrap(),
        "-",
        "-",
        "claude-opus-4-7",
        0,
        Instant::now(),
        false,
    )
    .await;
    assert!(
        matches!(
            result,
            ForwardOutcome::Retry {
                push_skip: false,
                transient: true,
                ..
            }
        ),
        "a transport send failure must be transient (round-gated), not a plain skip"
    );
    let info = state.endpoints[0].rate_info.read().await;
    assert_eq!(
        info.consecutive_transport_failures, 1,
        "the transport failure must feed the per-endpoint circuit breaker"
    );
}

/// LAB-712: a non-streaming response from a `Protocol::OpenAI` endpoint must
/// record its `usage.prompt_tokens`/`completion_tokens` into per-client
/// token + budget accounting. Previously this path only bumped `ep.requests`,
/// leaving `pre_request_gate` budget enforcement blind to OpenAI-endpoint spend.
#[tokio::test]
async fn try_fallback_upstream_records_usage_and_budget() {
    // Mock OpenAI upstream: non-streaming chat completion with usage.
    let app = Router::new().fallback(any(|| async {
        axum::Json(serde_json::json!({
            "id": "chatcmpl-1",
            "object": "chat.completion",
            "model": "claude-opus-4-7",
            "choices": [{
                "index": 0,
                "message": {"role": "assistant", "content": "hi"},
                "finish_reason": "stop"
            }],
            "usage": {"prompt_tokens": 60, "completion_tokens": 50, "total_tokens": 110}
        }))
        .into_response()
    }));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    let mut budgets = HashMap::new();
    budgets.insert("client-1".to_string(), 100u64);
    let mut ep = make_endpoint("gw", Protocol::OpenAI);
    ep.base_url = format!("http://{}", addr);
    let state = Arc::new(AppState {
        endpoints: vec![ep],
        client_budgets: budgets,
        ..test_state_base()
    });

    assert!(state.check_budget("client-1").await.is_ok());

    let body = br#"{"model":"claude-opus-4-7","messages":[],"max_tokens":1}"#;
    let result = try_fallback_upstream(
        &state,
        body,
        "req-1",
        "client-1",
        &"127.0.0.1".parse().unwrap(),
        "-",
        "-",
        "claude-opus-4-7",
        0,
        Instant::now(),
        true,
    )
    .await;
    assert!(matches!(result, ForwardOutcome::Done(_)));

    // Endpoint + per-client token counters advance by the reported usage.
    assert_eq!(state.endpoints[0].input_tokens.load(Ordering::Relaxed), 60);
    assert_eq!(state.endpoints[0].output_tokens.load(Ordering::Relaxed), 50);
    {
        let map = state.client_usage.lock().unwrap();
        assert_eq!(map.get("client-1").unwrap(), &[60, 50, 0, 0]);
    }

    // Budget sees the spend: 110 tokens against a 100-token budget → gate closes.
    {
        let map = state.budget_usage.lock().unwrap();
        assert_eq!(map.get("client-1").unwrap().1, 110);
    }
    assert!(
        state.check_budget("client-1").await.is_err(),
        "pre_request_gate budget check must see OpenAI-endpoint spend"
    );
}
// ── Unified endpoint: cross-protocol handler routing ───────────

#[tokio::test]
async fn openai_chat_handler_routes_to_unified_anthropic_endpoint() {
    // Mock Anthropic upstream: returns a minimal messages response.
    let mock = Router::new().fallback(any(|| async {
        axum::Json(serde_json::json!({
            "id": "msg_1", "type": "message", "role": "assistant",
            "model": "claude-opus-4-7",
            "content": [{"type": "text", "text": "hi back"}],
            "stop_reason": "end_turn",
            "usage": {"input_tokens": 1, "output_tokens": 2}
        }))
        .into_response()
    }));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mock_addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, mock).await.unwrap();
    });

    let mut state = test_state_with(vec![]); // no legacy accounts
    let mut ep = make_endpoint("unified-anthropic", Protocol::Anthropic);
    ep.base_url = format!("http://{}", mock_addr);
    ep.token = "sk-ant-api-test".to_string();
    Arc::get_mut(&mut state).unwrap().endpoints.push(ep);

    let app = Router::new()
        .route(
            "/v1/chat/completions",
            axum::routing::post(openai_chat_handler),
        )
        .with_state(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(
            listener,
            app.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await
        .unwrap();
    });

    let resp = reqwest::Client::new()
        .post(format!("http://{}/v1/chat/completions", addr))
        .json(&serde_json::json!({
            "model": "claude-opus-4-7",
            "messages": [{"role": "user", "content": "hi"}],
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert!(
        body["choices"][0]["message"]["content"].is_string(),
        "response must be OpenAI-shaped after round-trip translation"
    );
}
#[tokio::test]
async fn proxy_handler_translates_to_openai_endpoint() {
    // Mock OpenAI upstream: capture the request body to assert it was
    // translated to OpenAI shape, then return an OpenAI-format response.
    let (tx, mut rx) = tokio::sync::mpsc::channel::<serde_json::Value>(1);
    let app = Router::new().fallback(any(move |req: Request<Body>| {
        let tx = tx.clone();
        async move {
            let bytes = axum::body::to_bytes(req.into_body(), usize::MAX)
                .await
                .unwrap();
            let v: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
            let _ = tx.send(v).await;
            (
                StatusCode::OK,
                axum::Json(serde_json::json!({
                    "id": "chatcmpl-x",
                    "object": "chat.completion",
                    "model": "claude-opus-4-7",
                    "choices": [{
                        "index": 0,
                        "message": {"role": "assistant", "content": "hi back"},
                        "finish_reason": "stop"
                    }],
                    "usage": {"prompt_tokens": 1, "completion_tokens": 2, "total_tokens": 3}
                })),
            )
                .into_response()
        }
    }));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let upstream_addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    let mut state = test_state_with(vec![]);
    let mut ep = make_endpoint("openai-gw", Protocol::OpenAI);
    ep.base_url = format!("http://{}", upstream_addr);
    Arc::get_mut(&mut state).unwrap().endpoints.push(ep);

    let proxy_app = Router::new().fallback(any(proxy_handler)).with_state(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let proxy_addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(
            listener,
            proxy_app.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await
        .unwrap();
    });

    let client = reqwest::Client::new();
    let _ = client
        .post(format!("http://{}/v1/messages", proxy_addr))
        .json(&serde_json::json!({
            "model": "claude-opus-4-7",
            "max_tokens": 64,
            "messages": [{"role": "user", "content": "hi"}],
        }))
        .send()
        .await
        .unwrap();

    let received = rx.recv().await.expect("upstream must receive a request");
    assert!(
        received.get("messages").is_some(),
        "translated request must have OpenAI `messages` field"
    );
    assert_eq!(received["model"], "claude-opus-4-7");
}

// ── Connection resilience: synthetic SSE error on upstream disconnect ──

#[test]
fn anthropic_error_frame_is_well_formed_sse() {
    let bytes = anthropic_error_frame("connection reset by peer");
    let s = std::str::from_utf8(&bytes).expect("frame must be utf8");
    assert!(s.starts_with("event: error\n"), "must use SSE event syntax");
    assert!(s.ends_with("\n\n"), "must terminate with blank line");
    let data_line = s
        .lines()
        .find_map(|l| l.strip_prefix("data: "))
        .expect("must have a data: line");
    let parsed: serde_json::Value =
        serde_json::from_str(data_line).expect("data payload must be valid JSON");
    assert_eq!(parsed["type"], "error");
    // Must be one of Anthropic's documented SSE error types so the
    // SDK doesn't reject the frame and fall back to "socket closed".
    assert_eq!(parsed["error"]["type"], "api_error");
    assert_eq!(parsed["error"]["message"], "connection reset by peer");
}

#[test]
fn openai_error_frame_includes_done_marker() {
    let bytes = openai_error_frame("upstream gone");
    let s = std::str::from_utf8(&bytes).expect("frame must be utf8");
    assert!(
        s.contains("\ndata: [DONE]\n\n"),
        "must terminate with OpenAI [DONE] marker so the client parser closes cleanly: {s:?}"
    );
    let first_data = s
        .lines()
        .find_map(|l| l.strip_prefix("data: "))
        .expect("must have a leading data: line");
    let parsed: serde_json::Value =
        serde_json::from_str(first_data).expect("first data payload must be valid JSON");
    assert_eq!(parsed["error"]["type"], "upstream_error");
    assert_eq!(parsed["error"]["message"], "upstream gone");
}

/// When an upstream Anthropic SSE stream dies mid-flight, the downstream
/// client must receive a synthetic `event: error` frame rather than a bare
/// TCP FIN. Without this guarantee the Claude Code CLI surfaces the
/// uninterpretable "socket connection was closed unexpectedly" error.
#[tokio::test]
async fn streaming_upstream_disconnect_emits_sse_error_to_client() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    // Mock upstream: replies with text/event-stream + one partial SSE
    // chunk, then drops the socket without writing the terminating
    // `message_stop` event or a 0-length chunked terminator.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mock_addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut buf = vec![0u8; 4096];
        let _ = stream.read(&mut buf).await;
        let reset = (std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs()
            + 3600)
            .to_string();
        let head = format!(
            "HTTP/1.1 200 OK\r\n\
                 content-type: text/event-stream\r\n\
                 transfer-encoding: chunked\r\n\
                 anthropic-ratelimit-unified-representative-claim: five_hour\r\n\
                 anthropic-ratelimit-unified-5h-utilization: 0.10\r\n\
                 anthropic-ratelimit-unified-5h-reset: {reset}\r\n\
                 \r\n"
        );
        let _ = stream.write_all(head.as_bytes()).await;
        // One valid chunk, then drop. Chunk = hex-size, CRLF, bytes, CRLF.
        let body = "event: message_start\ndata: {\"type\":\"message_start\"}\n\n";
        let chunk = format!("{:x}\r\n{}\r\n", body.len(), body);
        let _ = stream.write_all(chunk.as_bytes()).await;
        let _ = stream.shutdown().await;
    });

    let upstream_url = format!("http://{}", mock_addr);
    let (app, _state) = test_app(&upstream_url, None);

    let app_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let app_addr = app_listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(
            app_listener,
            app.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await
        .unwrap();
    });

    let client = reqwest::Client::new();
    let resp = client
            .post(format!("http://{}/v1/messages", app_addr))
            .header("content-type", "application/json")
            .header("accept", "text/event-stream")
            .header("x-api-key", "any")
            .body(
                r#"{"model":"claude-sonnet-4-6","stream":true,"messages":[{"role":"user","content":"hi"}]}"#,
            )
            .send()
            .await
            .unwrap();

    assert_eq!(resp.status(), 200);
    let body = resp.bytes().await.unwrap();
    let body_s = std::str::from_utf8(&body).expect("body utf8");

    assert!(
        body_s.contains("event: error\n"),
        "downstream must receive synthetic SSE error frame on upstream disconnect, got: {body_s:?}"
    );
    assert!(
        body_s.contains("\"type\":\"api_error\""),
        "error frame must use Anthropic's documented api_error type, got: {body_s:?}"
    );
}

// ── LAB-718: non-streaming requests must not ride the SSE-tuned read_timeout ──
//
// A non-streaming /v1/messages emits zero response bytes until generation
// completes; `client`'s read_timeout (180s, tuned for SSE inter-chunk silence)
// therefore capped generation time and killed the GEO semantic judge's long
// structured-output calls ("operation timed out", 2026-07-24). Routing keys on
// the request body's `stream` flag via `request_wants_stream`.

#[test]
fn request_wants_stream_true_only_for_explicit_stream_true() {
    assert!(request_wants_stream(br#"{"stream":true,"model":"m"}"#));
    assert!(!request_wants_stream(br#"{"stream":false,"model":"m"}"#));
    assert!(
        !request_wants_stream(br#"{"model":"m"}"#),
        "absent flag = Anthropic's non-streaming default"
    );
    assert!(
        !request_wants_stream(br#"{"stream":"true"}"#),
        "non-bool stream is not a stream request"
    );
    assert!(
        !request_wants_stream(b"not json"),
        "unparseable body counts as non-streaming"
    );
    assert!(
        !request_wants_stream(b""),
        "empty body counts as non-streaming"
    );
}

#[test]
fn upstream_client_builder_composes_with_and_without_read_timeout() {
    // Structural guard: both clients build from the shared knob chain; only the
    // streaming client layers read_timeout on top. reqwest doesn't expose its
    // config, so all this can assert is that both builders construct — the
    // load-bearing read_timeout split is pinned by the call sites in main().
    let _streaming = upstream_client_builder()
        .read_timeout(Duration::from_secs(180))
        .build()
        .expect("streaming client builds");
    let _nonstreaming = upstream_client_builder()
        .build()
        .expect("non-streaming client builds");
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

/// Canned Anthropic context-window-overflow 400, byte-for-byte.
const PROMPT_TOO_LONG_BODY: &[u8] = br#"{"type":"error","error":{"type":"invalid_request_error","message":"prompt is too long: 213462 tokens > 200000 maximum"}}"#;

/// Upstream that answers every request with the canned 400 — the shared
/// canned-status helper with `bad_first = MAX` (never recovers). `bad_head`
/// is a raw pre-formatted response, so the JSON body rides along in it; the
/// content-length is computed here and the leak is one string per test run.
async fn spawn_prompt_too_long_upstream() -> String {
    let raw: &'static str = Box::leak(
        format!(
            "HTTP/1.1 400 Bad Request\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
            PROMPT_TOO_LONG_BODY.len(),
            std::str::from_utf8(PROMPT_TOO_LONG_BODY).unwrap(),
        )
        .into_boxed_str(),
    );
    spawn_status_then_ok_upstream(usize::MAX, raw, ANTHROPIC_OK_BODY)
        .await
        .0
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

// ── LAB-933: opt-in encrypted response cache on /v1/messages ────────

/// In-memory `cachekit::backend::Backend` that records every byte handed to
/// it, so tests can assert on EXACTLY what the storage layer sees (AC4:
/// ciphertext only, digest keys only).
#[derive(Default)]
struct RecordingBackend {
    store: std::sync::Mutex<HashMap<String, Vec<u8>>>,
    writes: std::sync::Mutex<Vec<(String, Vec<u8>)>>,
}

#[async_trait::async_trait]
impl cachekit::backend::Backend for RecordingBackend {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, cachekit::BackendError> {
        Ok(self.store.lock().unwrap().get(key).cloned())
    }
    async fn set(
        &self,
        key: &str,
        value: Vec<u8>,
        _ttl: Option<Duration>,
    ) -> Result<(), cachekit::BackendError> {
        self.writes
            .lock()
            .unwrap()
            .push((key.to_string(), value.clone()));
        self.store.lock().unwrap().insert(key.to_string(), value);
        Ok(())
    }
    async fn delete(&self, key: &str) -> Result<bool, cachekit::BackendError> {
        Ok(self.store.lock().unwrap().remove(key).is_some())
    }
    async fn exists(&self, key: &str) -> Result<bool, cachekit::BackendError> {
        Ok(self.store.lock().unwrap().contains_key(key))
    }
    async fn health(&self) -> Result<cachekit::backend::HealthStatus, cachekit::BackendError> {
        Ok(cachekit::backend::HealthStatus {
            is_healthy: true,
            latency_ms: 0.0,
            backend_type: "recording".into(),
            details: HashMap::new(),
        })
    }
}

/// Backend where every operation fails — the "Redis is down" shape (AC10).
struct ErroringBackend;

#[async_trait::async_trait]
impl cachekit::backend::Backend for ErroringBackend {
    async fn get(&self, _key: &str) -> Result<Option<Vec<u8>>, cachekit::BackendError> {
        Err(cachekit::BackendError::transient("backend down"))
    }
    async fn set(
        &self,
        _key: &str,
        _value: Vec<u8>,
        _ttl: Option<Duration>,
    ) -> Result<(), cachekit::BackendError> {
        Err(cachekit::BackendError::transient("backend down"))
    }
    async fn delete(&self, _key: &str) -> Result<bool, cachekit::BackendError> {
        Err(cachekit::BackendError::transient("backend down"))
    }
    async fn exists(&self, _key: &str) -> Result<bool, cachekit::BackendError> {
        Err(cachekit::BackendError::transient("backend down"))
    }
    async fn health(&self) -> Result<cachekit::backend::HealthStatus, cachekit::BackendError> {
        Err(cachekit::BackendError::transient("backend down"))
    }
}

/// Backend where every operation hangs well past any op timeout (AC10).
struct SlowBackend;

#[async_trait::async_trait]
impl cachekit::backend::Backend for SlowBackend {
    async fn get(&self, _key: &str) -> Result<Option<Vec<u8>>, cachekit::BackendError> {
        tokio::time::sleep(Duration::from_secs(30)).await;
        Ok(None)
    }
    async fn set(
        &self,
        _key: &str,
        _value: Vec<u8>,
        _ttl: Option<Duration>,
    ) -> Result<(), cachekit::BackendError> {
        tokio::time::sleep(Duration::from_secs(30)).await;
        Ok(())
    }
    async fn delete(&self, _key: &str) -> Result<bool, cachekit::BackendError> {
        tokio::time::sleep(Duration::from_secs(30)).await;
        Ok(false)
    }
    async fn exists(&self, _key: &str) -> Result<bool, cachekit::BackendError> {
        tokio::time::sleep(Duration::from_secs(30)).await;
        Ok(false)
    }
    async fn health(&self) -> Result<cachekit::backend::HealthStatus, cachekit::BackendError> {
        Err(cachekit::BackendError::timeout("slow"))
    }
}

const TEST_MASTER_KEY: [u8; 32] = [7u8; 32];

fn test_response_cache(
    backend: cachekit::SharedBackend,
    clients: &[&str],
    op_timeout_ms: u64,
) -> ResponseCache {
    ResponseCache::from_parts(
        backend,
        &clients.iter().map(|s| s.to_string()).collect::<Vec<_>>(),
        &TEST_MASTER_KEY,
        Duration::from_secs(3600),
        Duration::from_millis(op_timeout_ms),
    )
    .unwrap()
}

/// Full app against a counting upstream, with the response cache installed
/// for `clients`. Returns (addr, upstream_hits, state).
async fn serve_cache_app(
    backend: cachekit::SharedBackend,
    clients: &[&str],
) -> (
    SocketAddr,
    std::sync::Arc<std::sync::atomic::AtomicUsize>,
    Arc<AppState>,
) {
    let (url, hits) = spawn_flaky_upstream(0, ANTHROPIC_OK_BODY).await;
    let rc = test_response_cache(backend, clients, 500);
    let state = Arc::new(AppState {
        endpoints: vec![mk_endpoint_at("acct-a", "sk-ant-api-test-aaa", &url)],
        response_cache: Some(rc),
        ..test_state_base()
    });
    let addr = serve(build_router(state.clone())).await;
    (addr, hits, state)
}

const CACHE_TEST_BODY: &str =
    r#"{"model":"test","max_tokens":1,"messages":[{"role":"user","content":"hi"}]}"#;

async fn post_messages(addr: SocketAddr, client_id: &str, body: &str) -> reqwest::Response {
    reqwest::Client::new()
        .post(format!("http://{addr}/v1/messages"))
        .header("content-type", "application/json")
        .header("x-client-id", client_id)
        .body(body.to_string())
        .send()
        .await
        .unwrap()
}

async fn post_count_tokens(addr: SocketAddr, client_id: &str, body: &str) -> reqwest::Response {
    reqwest::Client::new()
        .post(format!("http://{addr}/v1/messages/count_tokens"))
        .header("content-type", "application/json")
        .header("x-client-id", client_id)
        .body(body.to_string())
        .send()
        .await
        .unwrap()
}

// AC6: the key is a full-body canonical digest — same raw text in a
// different structural position must produce a different key.
#[test]
fn response_cache_key_differs_on_nested_structure() {
    let headers = hyper::HeaderMap::new();
    let a: serde_json::Value = serde_json::from_str(
        r#"{"messages":[{"role":"user","content":[{"type":"text","text":"a"},{"type":"text","text":"b"}]}]}"#,
    )
    .unwrap();
    let b: serde_json::Value = serde_json::from_str(
        r#"{"messages":[{"role":"user","content":[{"type":"text","text":"ab"}]}]}"#,
    )
    .unwrap();
    let (fpa, fpsa) = content_fingerprints(&a);
    let (fpb, fpsb) = content_fingerprints(&b);
    let ka = response_cache_key("m", &a, &headers, None, "c", &fpa, &fpsa, "messages");
    let kb = response_cache_key("m", &b, &headers, None, "c", &fpb, &fpsb, "messages");
    assert_ne!(ka, kb, "structural difference must change the key");

    // Model and beta headers are key material too.
    let ka2 = response_cache_key("m2", &a, &headers, None, "c", &fpa, &fpsa, "messages");
    assert_ne!(ka, ka2, "model must change the key");
    let mut beta_headers = hyper::HeaderMap::new();
    beta_headers.insert(
        "anthropic-beta",
        HeaderValue::from_static("context-1m-2025"),
    );
    let ka3 = response_cache_key("m", &a, &beta_headers, None, "c", &fpa, &fpsa, "messages");
    assert_ne!(ka, ka3, "anthropic-beta must change the key");

    // Key format: hex digest only, never content (AC5).
    assert_eq!(ka.len(), 64);
    assert!(ka.chars().all(|c| c.is_ascii_hexdigit()));
}

// AC7 (unit half): identical bodies, different clients → different keys.
#[test]
fn response_cache_key_isolates_clients() {
    let headers = hyper::HeaderMap::new();
    let body: serde_json::Value = serde_json::from_str(CACHE_TEST_BODY).unwrap();
    let (fp, fps) = content_fingerprints(&body);
    let ka = response_cache_key(
        "test", &body, &headers, None, "client-a", &fp, &fps, "messages",
    );
    let kb = response_cache_key(
        "test", &body, &headers, None, "client-b", &fp, &fps, "messages",
    );
    assert_ne!(ka, kb);
}

// LAB-929 AC2/AC7: identical body posted to /v1/messages vs
// /v1/messages/count_tokens must NOT collide — a client that counts before
// sending (same model+messages to both endpoints) must never have one
// surface's cached entry served back as the other's.
#[test]
fn response_cache_key_isolates_surface() {
    let headers = hyper::HeaderMap::new();
    let body: serde_json::Value = serde_json::from_str(CACHE_TEST_BODY).unwrap();
    let (fp, fps) = content_fingerprints(&body);
    let k_messages = response_cache_key("test", &body, &headers, None, "c", &fp, &fps, "messages");
    let k_count_tokens = response_cache_key(
        "test",
        &body,
        &headers,
        None,
        "c",
        &fp,
        &fps,
        "count_tokens",
    );
    assert_ne!(k_messages, k_count_tokens);
}

#[test]
fn response_cache_master_key_validation() {
    assert!(decode_hex_key(&"ab".repeat(32)).is_ok());
    assert!(decode_hex_key("deadbeef").is_err(), "short key must fail");
    assert!(
        decode_hex_key(&"a".repeat(63)).is_err(),
        "odd length must fail"
    );
    assert!(
        decode_hex_key(&"zz".repeat(32)).is_err(),
        "non-hex must fail"
    );
    assert!(decode_hex_key("").is_err());
}

#[test]
fn response_cache_rejects_unknown_client_sentinel() {
    let backend: cachekit::SharedBackend = std::sync::Arc::new(RecordingBackend::default());
    let err = ResponseCache::from_parts(
        backend,
        &["-".to_string()],
        &TEST_MASTER_KEY,
        Duration::from_secs(60),
        Duration::from_millis(100),
    )
    .err()
    .expect("\"-\" must be rejected");
    assert!(err.contains("sentinel"), "got: {err}");
}

// AC1: with no cache configured, repeat requests hit the upstream every time
// and no cache artifacts appear on the response.
#[tokio::test]
async fn response_cache_absent_config_is_inert() {
    let (url, hits) = spawn_flaky_upstream(0, ANTHROPIC_OK_BODY).await;
    let state = test_state_with(vec![mk_endpoint_at("a", "sk-ant-api-aaa", &url)]);
    let addr = serve(build_router(state)).await;
    for _ in 0..2 {
        let resp = post_messages(addr, "someone", CACHE_TEST_BODY).await;
        assert_eq!(resp.status(), reqwest::StatusCode::OK);
        assert!(resp.headers().get("x-alb-cache").is_none());
    }
    assert_eq!(
        hits.load(Ordering::SeqCst),
        2,
        "both requests must reach upstream"
    );
}

// AC2: a client NOT on the allow-list never reads or writes the cache even
// when the cache is configured for someone else.
#[tokio::test]
async fn response_cache_ignores_non_opted_client() {
    let backend = std::sync::Arc::new(RecordingBackend::default());
    let (addr, hits, state) = serve_cache_app(backend.clone(), &["opted-in"]).await;
    for _ in 0..2 {
        let resp = post_messages(addr, "not-opted-in", CACHE_TEST_BODY).await;
        assert_eq!(resp.status(), reqwest::StatusCode::OK);
        assert!(resp.headers().get("x-alb-cache").is_none());
    }
    assert_eq!(hits.load(Ordering::SeqCst), 2);
    assert!(
        backend.writes.lock().unwrap().is_empty(),
        "no cache writes for non-opted client"
    );
    let rc = state.response_cache.as_ref().unwrap();
    assert_eq!(rc.hits.load(Ordering::Relaxed), 0);
    assert_eq!(rc.misses.load(Ordering::Relaxed), 0);
}

// AC9 + AC3 write side: an opted-in replay is served from cache — exactly one
// upstream call, no second budget/usage recording, hit counted, marker header.
#[tokio::test]
async fn response_cache_hit_replays_without_upstream_or_budget() {
    let backend = std::sync::Arc::new(RecordingBackend::default());
    let (addr, hits, state) = serve_cache_app(backend.clone(), &["geo"]).await;

    let first = post_messages(addr, "geo", CACHE_TEST_BODY).await;
    assert_eq!(first.status(), reqwest::StatusCode::OK);
    assert!(first.headers().get("x-alb-cache").is_none());
    let first_body = first.bytes().await.unwrap();

    let budget_after_first = state.budget_usage.lock().unwrap().clone();
    let usage_after_first = state.client_usage.lock().unwrap().clone();
    let upstream_requests_after_first = state.endpoints[0].requests.load(Ordering::Relaxed);

    let second = post_messages(addr, "geo", CACHE_TEST_BODY).await;
    assert_eq!(second.status(), reqwest::StatusCode::OK);
    assert_eq!(
        second
            .headers()
            .get("x-alb-cache")
            .map(|v| v.to_str().unwrap()),
        Some("hit")
    );
    let second_body = second.bytes().await.unwrap();
    assert_eq!(first_body, second_body, "replay must be the original body");

    assert_eq!(
        hits.load(Ordering::SeqCst),
        1,
        "second request must not reach upstream"
    );
    assert_eq!(
        *state.budget_usage.lock().unwrap(),
        budget_after_first,
        "a cache hit must not decrement the daily budget"
    );
    assert_eq!(
        *state.client_usage.lock().unwrap(),
        usage_after_first,
        "a cache hit must not record token usage"
    );
    assert_eq!(
        state.endpoints[0].requests.load(Ordering::Relaxed),
        upstream_requests_after_first,
        "a cache hit must not consume endpoint headroom accounting"
    );

    let rc = state.response_cache.as_ref().unwrap();
    assert_eq!(rc.hits.load(Ordering::Relaxed), 1);
    assert_eq!(rc.misses.load(Ordering::Relaxed), 1);
    assert_eq!(rc.stores.load(Ordering::Relaxed), 1);
}

// AC7 (integration half): a second opted-in client with a byte-identical
// body must MISS — entries are never shared across clients.
#[tokio::test]
async fn response_cache_cross_client_read_misses() {
    let backend = std::sync::Arc::new(RecordingBackend::default());
    let (addr, hits, _state) = serve_cache_app(backend.clone(), &["client-a", "client-b"]).await;

    let r = post_messages(addr, "client-a", CACHE_TEST_BODY).await;
    assert_eq!(r.status(), reqwest::StatusCode::OK);
    let r = post_messages(addr, "client-b", CACHE_TEST_BODY).await;
    assert_eq!(r.status(), reqwest::StatusCode::OK);
    assert!(
        r.headers().get("x-alb-cache").is_none(),
        "cross-client must not hit"
    );
    assert_eq!(
        hits.load(Ordering::SeqCst),
        2,
        "client-b must reach upstream"
    );

    // Each client still hits their OWN entry.
    let r = post_messages(addr, "client-b", CACHE_TEST_BODY).await;
    assert_eq!(
        r.headers().get("x-alb-cache").map(|v| v.to_str().unwrap()),
        Some("hit")
    );
    assert_eq!(hits.load(Ordering::SeqCst), 2);
}

// AC8: streaming requests bypass the cache entirely, even for opted-in clients.
#[tokio::test]
async fn response_cache_streaming_bypasses() {
    let backend = std::sync::Arc::new(RecordingBackend::default());
    let (addr, hits, state) = serve_cache_app(backend.clone(), &["geo"]).await;
    let streaming_body = r#"{"model":"test","max_tokens":1,"stream":true,"messages":[{"role":"user","content":"hi"}]}"#;
    for _ in 0..2 {
        let resp = post_messages(addr, "geo", streaming_body).await;
        assert_eq!(resp.status(), reqwest::StatusCode::OK);
        assert!(resp.headers().get("x-alb-cache").is_none());
    }
    assert_eq!(hits.load(Ordering::SeqCst), 2);
    assert!(backend.writes.lock().unwrap().is_empty());
    let rc = state.response_cache.as_ref().unwrap();
    assert_eq!(
        rc.misses.load(Ordering::Relaxed) + rc.hits.load(Ordering::Relaxed),
        0
    );
}

// AC3: non-2xx responses are never written to the cache.
#[tokio::test]
async fn response_cache_never_stores_non_2xx() {
    // Upstream that always 500s.
    let (url, _hits) = {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            loop {
                let (mut s, _) = listener.accept().await.unwrap();
                let mut buf = [0u8; 4096];
                let _ = s.read(&mut buf).await;
                let body = br#"{"type":"error","error":{"type":"api_error","message":"boom"}}"#;
                let head = format!(
                    "HTTP/1.1 500 Internal Server Error\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
                    body.len()
                );
                let _ = s.write_all(head.as_bytes()).await;
                let _ = s.write_all(body).await;
            }
        });
        (format!("http://{addr}"), ())
    };
    let backend = std::sync::Arc::new(RecordingBackend::default());
    let rc = test_response_cache(backend.clone(), &["geo"], 500);
    let state = Arc::new(AppState {
        endpoints: vec![mk_endpoint_at("acct-a", "sk-ant-api-test-aaa", &url)],
        response_cache: Some(rc),
        ..test_state_base()
    });
    let addr = serve(build_router(state.clone())).await;
    let resp = post_messages(addr, "geo", CACHE_TEST_BODY).await;
    // The LB rotates/retries on upstream 5xx, so a permanently-500ing pool
    // surfaces as an exhaustion status — the invariant under test is only
    // that NOTHING non-2xx is ever written to the cache.
    assert!(!resp.status().is_success());
    assert!(
        backend.writes.lock().unwrap().is_empty(),
        "5xx must never be written to the cache"
    );
    assert_eq!(
        state
            .response_cache
            .as_ref()
            .unwrap()
            .stores
            .load(Ordering::Relaxed),
        0
    );
}

// AC4: nothing handed to the backend may contain the prompt or completion
// plaintext; keys are digests; a wrong-key read fails closed (returns
// nothing) rather than returning plaintext.
#[tokio::test]
async fn response_cache_backend_sees_only_ciphertext() {
    let marker = b"XKCD-CORRECT-HORSE-BATTERY-STAPLE";
    let backend = std::sync::Arc::new(RecordingBackend::default());
    let rc = test_response_cache(backend.clone(), &["geo"], 500);
    let entry = CachedResponse {
        status: 200,
        content_type: "application/json".into(),
        body: format!(
            r#"{{"content":[{{"type":"text","text":"{}"}}]}}"#,
            String::from_utf8_lossy(marker)
        )
        .into_bytes(),
    };
    let key = "a".repeat(64);
    rc.store("geo", &key, &entry, CacheSurface::Messages).await;
    assert_eq!(rc.stores.load(Ordering::Relaxed), 1, "store must succeed");

    let writes = backend.writes.lock().unwrap().clone();
    assert!(!writes.is_empty());
    for (k, v) in &writes {
        assert!(
            !v.windows(marker.len()).any(|w| w == marker),
            "plaintext marker leaked into backend value"
        );
        assert!(
            !k.as_bytes().windows(marker.len()).any(|w| w == marker),
            "plaintext marker leaked into backend key"
        );
    }

    // Right key decrypts.
    let got = rc
        .lookup("geo", &key, CacheSurface::Messages)
        .await
        .expect("right-key read must hit");
    assert_eq!(got.body, entry.body);

    // Wrong master key over the SAME stored bytes: must fail closed.
    let rc_wrong = ResponseCache::from_parts(
        backend.clone(),
        &["geo".to_string()],
        &[9u8; 32],
        Duration::from_secs(3600),
        Duration::from_millis(500),
    )
    .unwrap();
    assert!(
        rc_wrong
            .lookup("geo", &key, CacheSurface::Messages)
            .await
            .is_none(),
        "wrong-key read must not return plaintext"
    );
    assert_eq!(
        rc_wrong.errors.load(Ordering::Relaxed),
        1,
        "wrong-key read counts as error"
    );

    // Cross-tenant decrypt must also fail: same master key, different
    // client_id (tenant) — HKDF gives it a different derived key. Reads go
    // through client-b's cache handle but target client-a's stored bytes.
    let rc_other = ResponseCache::from_parts(
        backend.clone(),
        &["other-client".to_string()],
        &TEST_MASTER_KEY,
        Duration::from_secs(3600),
        Duration::from_millis(500),
    )
    .unwrap();
    assert!(
        rc_other
            .lookup("other-client", &key, CacheSurface::Messages)
            .await
            .is_none(),
        "cross-tenant read must not decrypt"
    );
}

// AC10: a dead backend degrades to normal proxying, never an error response.
#[tokio::test]
async fn response_cache_fails_open_on_backend_error() {
    let backend = std::sync::Arc::new(ErroringBackend);
    let (addr, hits, state) = serve_cache_app(backend, &["geo"]).await;
    for _ in 0..2 {
        let resp = post_messages(addr, "geo", CACHE_TEST_BODY).await;
        assert_eq!(
            resp.status(),
            reqwest::StatusCode::OK,
            "must fail open to upstream"
        );
        assert!(resp.headers().get("x-alb-cache").is_none());
    }
    assert_eq!(hits.load(Ordering::SeqCst), 2);
    let rc = state.response_cache.as_ref().unwrap();
    assert!(
        rc.errors.load(Ordering::Relaxed) >= 2,
        "read+write errors must be counted"
    );
}

// AC10: a HUNG backend is bounded by op_timeout — the request still
// completes promptly with a normal proxied response.
#[tokio::test]
async fn response_cache_fails_open_on_slow_backend() {
    let backend = std::sync::Arc::new(SlowBackend);
    let (url, hits) = spawn_flaky_upstream(0, ANTHROPIC_OK_BODY).await;
    let rc = test_response_cache(backend, &["geo"], 50);
    let state = Arc::new(AppState {
        endpoints: vec![mk_endpoint_at("acct-a", "sk-ant-api-test-aaa", &url)],
        response_cache: Some(rc),
        ..test_state_base()
    });
    let addr = serve(build_router(state.clone())).await;
    let started = std::time::Instant::now();
    let resp = post_messages(addr, "geo", CACHE_TEST_BODY).await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "hung backend must be bounded by op_timeout, took {:?}",
        started.elapsed()
    );
    assert_eq!(hits.load(Ordering::SeqCst), 1);
    assert!(
        state
            .response_cache
            .as_ref()
            .unwrap()
            .errors
            .load(Ordering::Relaxed)
            >= 2
    );
}

// AC12: hit/miss/store/error counters are exposed on /metrics.
#[tokio::test]
async fn response_cache_metrics_exposed() {
    let backend = std::sync::Arc::new(RecordingBackend::default());
    let (addr, _hits, _state) = serve_cache_app(backend, &["geo"]).await;
    post_messages(addr, "geo", CACHE_TEST_BODY).await; // miss + store
    post_messages(addr, "geo", CACHE_TEST_BODY).await; // hit
    let text = reqwest::Client::new()
        .get(format!("http://{addr}/metrics"))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(
        text.contains(r#"anthropic_response_cache_hits_total{surface="messages"} 1"#),
        "{text}"
    );
    assert!(text.contains(r#"anthropic_response_cache_misses_total{surface="messages"} 1"#));
    assert!(text.contains(r#"anthropic_response_cache_stores_total{surface="messages"} 1"#));
    assert!(text.contains(r#"anthropic_response_cache_errors_total{surface="messages"} 0"#));
}

// LAB-929 AC2: replaying an identical /v1/messages/count_tokens request is
// served from cache (one upstream call); a differing body forwards again.
#[tokio::test]
async fn response_cache_count_tokens_hit_replays_without_upstream() {
    let backend = std::sync::Arc::new(RecordingBackend::default());
    let (addr, hits, state) = serve_cache_app(backend, &["geo"]).await;

    let first = post_count_tokens(addr, "geo", CACHE_TEST_BODY).await;
    assert_eq!(first.status(), reqwest::StatusCode::OK);
    assert!(first.headers().get("x-alb-cache").is_none());

    let second = post_count_tokens(addr, "geo", CACHE_TEST_BODY).await;
    assert_eq!(second.status(), reqwest::StatusCode::OK);
    assert_eq!(
        second
            .headers()
            .get("x-alb-cache")
            .map(|v| v.to_str().unwrap()),
        Some("hit")
    );
    assert_eq!(
        hits.load(Ordering::SeqCst),
        1,
        "identical replay must not reach upstream"
    );

    // A differing body (different max_tokens) is a genuine miss.
    let differing =
        r#"{"model":"test","max_tokens":2,"messages":[{"role":"user","content":"hi"}]}"#;
    let third = post_count_tokens(addr, "geo", differing).await;
    assert_eq!(third.status(), reqwest::StatusCode::OK);
    assert!(third.headers().get("x-alb-cache").is_none());
    assert_eq!(
        hits.load(Ordering::SeqCst),
        2,
        "a differing body must forward upstream again"
    );

    let rc = state.response_cache.as_ref().unwrap();
    assert_eq!(rc.count_tokens_hits.load(Ordering::Relaxed), 1);
    assert_eq!(rc.count_tokens_misses.load(Ordering::Relaxed), 2);
    assert_eq!(rc.count_tokens_stores.load(Ordering::Relaxed), 2);
    // The /v1/messages series must stay untouched by count_tokens traffic.
    assert_eq!(rc.hits.load(Ordering::Relaxed), 0);
    assert_eq!(rc.misses.load(Ordering::Relaxed), 0);
}

// LAB-929 AC2/AC7: a client that sends the SAME body to /v1/messages and
// /v1/messages/count_tokens (a common pattern — count before you send) must
// never have one surface's cached entry served back as the other's.
#[tokio::test]
async fn response_cache_messages_and_count_tokens_do_not_cross_serve() {
    let backend = std::sync::Arc::new(RecordingBackend::default());
    let (addr, hits, state) = serve_cache_app(backend, &["geo"]).await;

    let ct = post_count_tokens(addr, "geo", CACHE_TEST_BODY).await;
    assert_eq!(ct.status(), reqwest::StatusCode::OK);
    assert!(ct.headers().get("x-alb-cache").is_none());

    // Byte-identical body to /v1/messages must still miss — not read back
    // the count_tokens entry.
    let msg = post_messages(addr, "geo", CACHE_TEST_BODY).await;
    assert_eq!(msg.status(), reqwest::StatusCode::OK);
    assert!(
        msg.headers().get("x-alb-cache").is_none(),
        "a /v1/messages request must never be served from the count_tokens entry"
    );
    assert_eq!(
        hits.load(Ordering::SeqCst),
        2,
        "both surfaces must independently reach upstream"
    );

    let rc = state.response_cache.as_ref().unwrap();
    assert_eq!(rc.count_tokens_stores.load(Ordering::Relaxed), 1);
    assert_eq!(rc.stores.load(Ordering::Relaxed), 1);
}

// LAB-929 AC4: count_tokens and messages hits/misses/stores/errors are
// separable series on /metrics via the `surface` label, sharing metric names.
#[tokio::test]
async fn response_cache_count_tokens_metrics_exposed() {
    let backend = std::sync::Arc::new(RecordingBackend::default());
    let (addr, _hits, _state) = serve_cache_app(backend, &["geo"]).await;
    post_count_tokens(addr, "geo", CACHE_TEST_BODY).await; // miss + store
    post_count_tokens(addr, "geo", CACHE_TEST_BODY).await; // hit
    post_messages(addr, "geo", CACHE_TEST_BODY).await; // separate surface: miss + store
    let text = reqwest::Client::new()
        .get(format!("http://{addr}/metrics"))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(
        text.contains(r#"anthropic_response_cache_hits_total{surface="count_tokens"} 1"#),
        "{text}"
    );
    assert!(text.contains(r#"anthropic_response_cache_misses_total{surface="count_tokens"} 1"#));
    assert!(text.contains(r#"anthropic_response_cache_stores_total{surface="count_tokens"} 1"#));
    assert!(text.contains(r#"anthropic_response_cache_hits_total{surface="messages"} 0"#));
    assert!(text.contains(r#"anthropic_response_cache_misses_total{surface="messages"} 1"#));
    assert!(text.contains(r#"anthropic_response_cache_stores_total{surface="messages"} 1"#));

    // Panel fix: the exposition format requires all samples of one metric
    // grouped together (no other metric's lines interleaved) — assert both
    // surfaces' `hits_total` samples are adjacent, not separated by
    // misses/stores/errors lines from the metric-major/surface-minor loop.
    let hits_lines: Vec<&str> = text
        .lines()
        .filter(|l| l.starts_with("anthropic_response_cache_hits_total"))
        .collect();
    let all_lines: Vec<&str> = text.lines().collect();
    let first_idx = all_lines.iter().position(|l| *l == hits_lines[0]).unwrap();
    assert_eq!(
        all_lines[first_idx + 1],
        hits_lines[1],
        "hits_total samples for both surfaces must be contiguous, not interleaved with other metrics"
    );
}

// LAB-929 AC5: a dead cache backend fails open on the count_tokens path too
// — inherited from ResponseCache, no new failure handling.
#[tokio::test]
async fn response_cache_count_tokens_fails_open_on_backend_error() {
    let backend = std::sync::Arc::new(ErroringBackend);
    let (addr, hits, state) = serve_cache_app(backend, &["geo"]).await;
    let resp = post_count_tokens(addr, "geo", CACHE_TEST_BODY).await;
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::OK,
        "must fail open to upstream"
    );
    assert!(resp.headers().get("x-alb-cache").is_none());
    assert_eq!(hits.load(Ordering::SeqCst), 1);
    let rc = state.response_cache.as_ref().unwrap();
    assert!(rc.count_tokens_errors.load(Ordering::Relaxed) >= 2);
}

// Metrics stay silent when the cache is not configured (no phantom series).
#[tokio::test]
async fn response_cache_metrics_absent_without_config() {
    let (url, _hits) = spawn_flaky_upstream(0, ANTHROPIC_OK_BODY).await;
    let state = test_state_with(vec![mk_endpoint_at("a", "sk-ant-api-aaa", &url)]);
    let addr = serve(build_router(state)).await;
    let text = reqwest::Client::new()
        .get(format!("http://{addr}/metrics"))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(!text.contains("anthropic_response_cache"));
}

// AC11 (redis): the real fred-backed RedisBackend against a dead port —
// construction succeeds (connectivity is a runtime concern), operations
// fail open within the timeout budget.
#[tokio::test]
async fn response_cache_redis_backend_fails_open_when_down() {
    let cfg = ResponseCacheConfig {
        clients: vec!["geo".to_string()],
        backend: "redis".to_string(),
        master_key: "ab".repeat(32),
        ttl_secs: Some(60),
        op_timeout_ms: Some(200),
        redis_url: Some("redis://127.0.0.1:1".to_string()),
        api_key: None,
        api_url: None,
    };
    let rc = tokio::time::timeout(Duration::from_secs(10), ResponseCache::from_config(&cfg))
        .await
        .expect("from_config must not hang on a dead redis")
        .expect("dead redis must not be a config error")
        .expect("allow-list is non-empty");
    let started = std::time::Instant::now();
    assert!(rc
        .lookup("geo", &"a".repeat(64), CacheSurface::Messages)
        .await
        .is_none());
    assert!(started.elapsed() < Duration::from_secs(5));
    assert!(rc.errors.load(Ordering::Relaxed) >= 1);
}

// AC11 (SaaS): CachekitIO backend constructs through the same config path,
// with and without an api_url override (the override exists for the dev
// environment, which is not on cachekit's built-in host allow-list), and the
// SDK's SSRF guard holds through OUR config path: loopback/private hosts are
// rejected at startup even though the override sets allow_custom_host
// (AC14 talking point).
#[tokio::test]
async fn response_cache_cachekitio_backend_constructs_and_blocks_loopback() {
    let cfg = ResponseCacheConfig {
        clients: vec!["geo".to_string()],
        backend: "cachekitio".to_string(),
        master_key: "ab".repeat(32),
        ttl_secs: Some(60),
        op_timeout_ms: Some(200),
        redis_url: None,
        api_key: Some("ck_test_dummy".to_string()),
        api_url: None,
    };
    assert!(ResponseCache::from_config(&cfg).await.unwrap().is_some());

    // Custom public host (the dev-environment shape) constructs.
    let dev = ResponseCacheConfig {
        api_url: Some("https://api.dev.cachekit.io".to_string()),
        ..cfg.clone()
    };
    assert!(ResponseCache::from_config(&dev).await.unwrap().is_some());

    // Loopback/private hosts fail startup loudly — through the config path.
    let loopback = ResponseCacheConfig {
        api_url: Some("https://127.0.0.1:9".to_string()),
        ..cfg.clone()
    };
    assert!(
        ResponseCache::from_config(&loopback).await.is_err(),
        "loopback api_url must be rejected by cachekit's SSRF guard"
    );

    // RFC1918 private hosts fail too — internal targets stay unreachable.
    let private = ResponseCacheConfig {
        api_url: Some("https://10.0.0.1:443".to_string()),
        ..cfg.clone()
    };
    assert!(
        ResponseCache::from_config(&private).await.is_err(),
        "private-address api_url must be rejected by cachekit's SSRF guard"
    );

    // Plain HTTP fails too.
    let http = ResponseCacheConfig {
        api_url: Some("http://api.dev.cachekit.io".to_string()),
        ..cfg
    };
    assert!(
        ResponseCache::from_config(&http).await.is_err(),
        "non-HTTPS api_url must be rejected"
    );
}

// Config-shape checks: unknown backend and missing per-backend params fail
// startup loudly; an empty allow-list is inert (AC2).
#[tokio::test]
async fn response_cache_config_validation() {
    let base = ResponseCacheConfig {
        clients: vec!["geo".to_string()],
        backend: "redis".to_string(),
        master_key: "ab".repeat(32),
        ttl_secs: None,
        op_timeout_ms: None,
        redis_url: None,
        api_key: None,
        api_url: None,
    };
    assert!(
        ResponseCache::from_config(&base).await.is_err(),
        "backend=redis without redis_url must fail"
    );
    let bad_backend = ResponseCacheConfig {
        backend: "memcached".to_string(),
        ..base.clone()
    };
    assert!(ResponseCache::from_config(&bad_backend).await.is_err());
    let no_key = ResponseCacheConfig {
        backend: "cachekitio".to_string(),
        ..base.clone()
    };
    assert!(
        ResponseCache::from_config(&no_key).await.is_err(),
        "backend=cachekitio without api_key must fail"
    );
    let inert = ResponseCacheConfig {
        clients: vec![],
        ..base
    };
    assert!(ResponseCache::from_config(&inert).await.unwrap().is_none());
}

// AC11 (SaaS, live): full round-trip against the real api.cachekit.io.
// Requires CACHEKIT_API_KEY with write access; deliberately #[ignore]d so CI
// stays hermetic — run locally with `cargo test -- --ignored` to exercise.
#[tokio::test]
#[ignore = "requires CACHEKIT_API_KEY and network access to api.cachekit.io"]
async fn response_cache_cachekitio_live_round_trip() {
    let api_key = match std::env::var("CACHEKIT_API_KEY") {
        Ok(k) if !k.is_empty() => k,
        _ => panic!("set CACHEKIT_API_KEY to run this test"),
    };
    let backend = cachekit::backend::cachekitio::CachekitIO::builder()
        .api_key(api_key)
        .build()
        .unwrap();
    let rc = test_response_cache(std::sync::Arc::new(backend), &["live-test"], 5_000);
    let key = response_cache_key(
        "live",
        &serde_json::from_str::<serde_json::Value>(CACHE_TEST_BODY).unwrap(),
        &hyper::HeaderMap::new(),
        None,
        "live-test",
        "fp",
        "fps",
        "messages",
    );
    let entry = CachedResponse {
        status: 200,
        content_type: "application/json".into(),
        body: b"{\"live\":true}".to_vec(),
    };
    rc.store("live-test", &key, &entry, CacheSurface::Messages)
        .await;
    assert_eq!(rc.stores.load(Ordering::Relaxed), 1, "live store failed");
    let got = rc
        .lookup("live-test", &key, CacheSurface::Messages)
        .await
        .expect("live read-back failed");
    assert_eq!(got.body, entry.body);
}

// Panel fix (bug-hunter MAJ): anthropic-version and the URI query string are
// key material — an SDK upgrade mid-TTL must miss, not replay the old shape.
#[test]
fn response_cache_key_varies_on_version_and_query() {
    let body: serde_json::Value = serde_json::from_str(CACHE_TEST_BODY).unwrap();
    let (fp, fps) = content_fingerprints(&body);
    let plain = hyper::HeaderMap::new();
    let mut versioned = hyper::HeaderMap::new();
    versioned.insert("anthropic-version", HeaderValue::from_static("2023-06-01"));
    let base = response_cache_key("m", &body, &plain, None, "c", &fp, &fps, "messages");
    assert_ne!(
        base,
        response_cache_key("m", &body, &versioned, None, "c", &fp, &fps, "messages"),
        "anthropic-version must change the key"
    );
    assert_ne!(
        base,
        response_cache_key(
            "m",
            &body,
            &plain,
            Some("beta=true"),
            "c",
            &fp,
            &fps,
            "messages"
        ),
        "URI query must change the key"
    );
}

// Panel fix (craftsman MAJ): an upstream answering a stream:false request
// with text/event-stream must pass through untouched — never collected,
// never cached as a bogus non-streaming entry.
#[tokio::test]
async fn response_cache_skips_non_json_content_type() {
    let sse_body: &[u8] = b"event: message_start\ndata: {}\n\n";
    let (url, hits) = {
        use std::sync::atomic::AtomicUsize;
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let hits = Arc::new(AtomicUsize::new(0));
        let h = hits.clone();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            loop {
                let (mut s, _) = listener.accept().await.unwrap();
                h.fetch_add(1, Ordering::SeqCst);
                let mut buf = [0u8; 4096];
                let _ = s.read(&mut buf).await;
                let head = format!(
                    "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
                    sse_body.len()
                );
                let _ = s.write_all(head.as_bytes()).await;
                let _ = s.write_all(sse_body).await;
            }
        });
        (format!("http://{addr}"), hits)
    };
    let backend = std::sync::Arc::new(RecordingBackend::default());
    let rc = test_response_cache(backend.clone(), &["geo"], 500);
    let state = Arc::new(AppState {
        endpoints: vec![mk_endpoint_at("acct-a", "sk-ant-api-test-aaa", &url)],
        response_cache: Some(rc),
        ..test_state_base()
    });
    let addr = serve(build_router(state.clone())).await;
    for _ in 0..2 {
        let resp = post_messages(addr, "geo", CACHE_TEST_BODY).await;
        assert_eq!(resp.status(), reqwest::StatusCode::OK);
        assert!(resp.headers().get("x-alb-cache").is_none());
        assert_eq!(resp.bytes().await.unwrap().as_ref(), sse_body);
    }
    assert_eq!(
        hits.load(Ordering::SeqCst),
        2,
        "SSE responses must never be served from cache"
    );
    assert!(
        backend.writes.lock().unwrap().is_empty(),
        "non-JSON content-type must never be written to the cache"
    );
    assert_eq!(
        state
            .response_cache
            .as_ref()
            .unwrap()
            .stores
            .load(Ordering::Relaxed),
        0
    );
}

// Panel fix (bug-hunter MAJ): bodies over MAX_BODY_BYTES are not stored —
// bounds backend value growth and worst-case L1 memory. The response itself
// is returned intact.
#[tokio::test]
async fn response_cache_skips_oversized_bodies() {
    let backend = std::sync::Arc::new(RecordingBackend::default());
    let rc = test_response_cache(backend.clone(), &["geo"], 500);
    let state = Arc::new(AppState {
        response_cache: Some(rc),
        ..test_state_base()
    });
    let big = vec![b'x'; ResponseCache::MAX_BODY_BYTES + 1];
    let resp = Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "application/json")
        .body(Body::from(big.clone()))
        .unwrap();
    let key_a = "a".repeat(64);
    let out = maybe_cache_store(
        &state,
        Some((key_a.as_str(), CacheSurface::Messages)),
        "geo",
        "rid",
        resp,
    )
    .await;
    assert_eq!(out.status(), StatusCode::OK);
    let out_bytes = axum::body::to_bytes(out.into_body(), usize::MAX)
        .await
        .unwrap();
    assert_eq!(
        out_bytes.len(),
        big.len(),
        "oversized body must be returned intact"
    );
    assert!(backend.writes.lock().unwrap().is_empty());
    assert_eq!(
        state
            .response_cache
            .as_ref()
            .unwrap()
            .stores
            .load(Ordering::Relaxed),
        0
    );

    // At the cap boundary it IS stored.
    let ok_body = vec![b'y'; 1024];
    let resp = Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "application/json")
        .body(Body::from(ok_body))
        .unwrap();
    let key_b = "b".repeat(64);
    let _ = maybe_cache_store(
        &state,
        Some((key_b.as_str(), CacheSurface::Messages)),
        "geo",
        "rid",
        resp,
    )
    .await;
    assert_eq!(
        state
            .response_cache
            .as_ref()
            .unwrap()
            .stores
            .load(Ordering::Relaxed),
        1
    );
}

// ── LAB-1083: per-client authenticated identity + model allow-lists ──
//
// The property under test throughout: with `[[clients]]` configured, a
// caller's identity is what its CREDENTIAL says, never what its headers say.
// Every per-client decision in the proxy keys on `client_id`, so these tests
// mostly prove one thing from several angles — that `client_id` cannot be
// asserted by the caller once a client table exists.

fn mk_client(name: &str, key: &str, models: &[&str]) -> ClientConfig {
    ClientConfig {
        name: name.to_string(),
        key: key.to_string(),
        models: models.iter().map(|s| s.to_string()).collect(),
    }
}

fn state_with_clients(clients: Vec<ClientConfig>) -> Arc<AppState> {
    Arc::new(AppState {
        clients,
        ..test_state_base()
    })
}

fn hdrs(pairs: &[(&str, &str)]) -> hyper::HeaderMap {
    let mut h = hyper::HeaderMap::new();
    for (k, v) in pairs {
        h.insert(
            hyper::header::HeaderName::from_bytes(k.as_bytes()).unwrap(),
            HeaderValue::from_str(v).unwrap(),
        );
    }
    h
}

const TEST_IP: &str = "10.0.0.7";

fn test_ip() -> IpAddr {
    TEST_IP.parse().unwrap()
}

// ── AC-1: distinct key → distinct identity, with no x-client-id ──

#[test]
fn distinct_client_keys_resolve_to_distinct_identities() {
    let state = state_with_clients(vec![
        mk_client("geo", "key-geo", &[]),
        mk_client("radar", "key-radar", &[]),
    ]);
    let ip = test_ip();

    for (key, expected) in [("key-geo", "geo"), ("key-radar", "radar")] {
        let headers = hdrs(&[("x-api-key", key)]);
        let principal = state
            .authenticate(&ip, &headers, false)
            .expect("configured key must authenticate")
            .expect("a [[clients]] match must yield a principal");
        assert_eq!(principal.name, expected);
        // And the identity the rest of the proxy sees follows it — with NO
        // x-client-id header present on either request.
        let rctx = RequestContext::from_request(&state, &ip, &headers, Some(principal));
        assert_eq!(rctx.client_id, expected);
    }
}

// ── AC-2: unknown / missing credential → 401 ──

#[test]
fn unknown_client_key_is_rejected() {
    let state = state_with_clients(vec![mk_client("geo", "key-geo", &[])]);
    let resp = state
        .authenticate(&test_ip(), &hdrs(&[("x-api-key", "key-wrong")]), false)
        .expect_err("unknown key must not authenticate");
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
}

#[test]
fn missing_credential_is_rejected_when_clients_configured() {
    let state = state_with_clients(vec![mk_client("geo", "key-geo", &[])]);
    let resp = state
        .authenticate(&test_ip(), &hyper::HeaderMap::new(), false)
        .expect_err("absent credential must not authenticate");
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
}

/// A near-miss must not authenticate. Guards the constant-time comparison
/// against a length- or prefix-tolerant rewrite.
#[test]
fn client_key_match_is_exact_not_prefix() {
    let state = state_with_clients(vec![mk_client("geo", "key-geo", &[])]);
    for wrong in ["key-ge", "key-geo ", "key-geox", "", "KEY-GEO"] {
        assert!(
            state
                .authenticate(&test_ip(), &hdrs(&[("x-api-key", wrong)]), false)
                .is_err(),
            "'{wrong}' must not authenticate as 'key-geo'"
        );
    }
}

// ── AC-2: bearer accepted on the OpenAI-compat surface only ──

#[test]
fn bearer_credential_accepted_only_where_enabled() {
    let state = state_with_clients(vec![mk_client("geo", "key-geo", &[])]);
    let ip = test_ip();
    let headers = hdrs(&[("authorization", "Bearer key-geo")]);

    let principal = state
        .authenticate(&ip, &headers, true)
        .expect("bearer must authenticate where enabled")
        .expect("principal");
    assert_eq!(principal.name, "geo");

    // The native surface may carry the caller's OWN upstream token in
    // `Authorization` (passthrough endpoints), so bearer is not accepted there.
    assert!(state.authenticate(&ip, &headers, false).is_err());
}

#[test]
fn bearer_scheme_match_is_case_insensitive() {
    let state = state_with_clients(vec![mk_client("geo", "key-geo", &[])]);
    let headers = hdrs(&[("authorization", "bEaReR key-geo")]);
    assert_eq!(
        state
            .authenticate(&test_ip(), &headers, true)
            .unwrap()
            .unwrap()
            .name,
        "geo"
    );
}

// ── AC-4: the credential wins over a spoofed x-client-id ──

#[test]
fn spoofed_x_client_id_is_ignored_under_clients_table() {
    let state = state_with_clients(vec![
        mk_client("alpha", "key-alpha", &[]),
        mk_client("bravo", "key-bravo", &[]),
    ]);
    let ip = test_ip();
    let headers = hdrs(&[("x-api-key", "key-alpha"), ("x-client-id", "bravo")]);

    let principal = state.authenticate(&ip, &headers, false).unwrap().unwrap();
    assert_eq!(principal.name, "alpha");

    let rctx = RequestContext::from_request(&state, &ip, &headers, Some(principal));
    assert_eq!(
        rctx.client_id, "alpha",
        "authenticated principal must win over the x-client-id header"
    );
}

/// The `client_names` IP map is the other client-influenced identity source.
/// It must also lose to the credential.
#[test]
fn client_names_ip_map_is_ignored_under_clients_table() {
    let mut client_names = HashMap::new();
    client_names.insert(TEST_IP.to_string(), "from-ip-map".to_string());
    let state = Arc::new(AppState {
        clients: vec![mk_client("alpha", "key-alpha", &[])],
        client_names,
        ..test_state_base()
    });
    let ip = test_ip();
    let headers = hdrs(&[("x-api-key", "key-alpha")]);
    let principal = state.authenticate(&ip, &headers, false).unwrap().unwrap();
    let rctx = RequestContext::from_request(&state, &ip, &headers, Some(principal));
    assert_eq!(rctx.client_id, "alpha");
}

// ── AC-5: legacy proxy_key path unchanged ──

#[test]
fn legacy_proxy_key_still_authenticates_and_yields_no_principal() {
    let state = Arc::new(AppState {
        proxy_key: Some("shared-secret".to_string()),
        ..test_state_base()
    });
    let ip = test_ip();

    let principal = state
        .authenticate(&ip, &hdrs(&[("x-api-key", "shared-secret")]), false)
        .expect("correct legacy key must authenticate");
    assert!(
        principal.is_none(),
        "legacy path has no principal — identity stays header/IP-derived"
    );

    assert!(state
        .authenticate(&ip, &hdrs(&[("x-api-key", "nope")]), false)
        .is_err());
    assert!(state
        .authenticate(&ip, &hyper::HeaderMap::new(), false)
        .is_err());
}

#[test]
fn legacy_proxy_key_leaves_x_client_id_resolution_intact() {
    let state = Arc::new(AppState {
        proxy_key: Some("shared-secret".to_string()),
        ..test_state_base()
    });
    let ip = test_ip();
    let headers = hdrs(&[("x-api-key", "shared-secret"), ("x-client-id", "gastown")]);
    let principal = state.authenticate(&ip, &headers, false).unwrap();
    let rctx = RequestContext::from_request(&state, &ip, &headers, principal);
    assert_eq!(
        rctx.client_id, "gastown",
        "without [[clients]], x-client-id remains the identity source"
    );
}

#[test]
fn open_proxy_authenticates_every_request() {
    let state = test_state_with(vec![]);
    assert!(state
        .authenticate(&test_ip(), &hyper::HeaderMap::new(), false)
        .unwrap()
        .is_none());
}

// ── AC-7 / AC-8: the allow-list and its shared matcher ──

/// `serves_model` delegates to `model_matches`, so asserting the two agree
/// would be `A == A`. Pin the concrete wildcard semantics instead — including
/// the empty-model ALLOW, which is correct for routing (don't narrow the pool
/// on an unknown model) and is deliberately NOT what the client allow-list
/// does.
#[test]
fn endpoint_model_matcher_semantics() {
    let mut ep = mk_endpoint("a", "sk-ant-api-x");
    ep.models = vec![
        "claude-haiku-*".to_string(),
        "claude-sonnet-4-6".to_string(),
    ];
    assert!(ep.serves_model("claude-haiku-4-5"), "wildcard hit");
    assert!(ep.serves_model("claude-sonnet-4-6"), "exact hit");
    assert!(!ep.serves_model("claude-opus-5"), "miss");
    assert!(
        !ep.serves_model("claude-sonnet-4-6-x"),
        "exact is not a prefix"
    );
    assert!(ep.serves_model(""), "unknown model must not narrow routing");

    ep.models.clear();
    assert!(ep.serves_model("claude-opus-5"), "empty list = all models");
}

#[test]
fn client_allow_list_hit_miss_wildcard_and_empty() {
    let state = state_with_clients(vec![
        mk_client("limited", "k1", &["claude-haiku-*", "claude-sonnet-4-6"]),
        mk_client("unlimited", "k2", &[]),
    ]);

    // exact hit
    assert!(state.client_allows_model("limited", "claude-sonnet-4-6"));
    // wildcard hit
    assert!(state.client_allows_model("limited", "claude-haiku-4-5"));
    // miss
    assert!(!state.client_allows_model("limited", "claude-opus-5"));
    // a near-miss on the exact pattern is still a miss
    assert!(!state.client_allows_model("limited", "claude-sonnet-4-6-extra"));
    // empty list = all models
    assert!(state.client_allows_model("unlimited", "claude-opus-5"));
    // unknown client (legacy path only) = all models
    assert!(state.client_allows_model("-", "claude-opus-5"));
}

/// The allow-list must FAIL CLOSED on an unreadable model.
///
/// `proxy_handler` sets `model = ""` whenever the body does not parse as JSON,
/// and only ever reads a TOP-LEVEL `model` key — so a request to a route that
/// nests it (`/v1/messages/batches` puts it under `requests[].params.model`),
/// or any body the parser rejects, arrives at the gate with no model. If that
/// allowed, a client restricted to haiku would reach opus by sending a body we
/// cannot read. The endpoint matcher's empty-allows-all is right for routing
/// and wrong here; this test is the line between them.
#[test]
fn client_allow_list_denies_an_unreadable_model() {
    let state = state_with_clients(vec![
        mk_client("limited", "k1", &["claude-haiku-*"]),
        mk_client("unlimited", "k2", &[]),
    ]);
    assert!(
        !state.client_allows_model("limited", ""),
        "a client WITH an allow-list must be denied when the model is unknown"
    );
    assert!(
        state.client_allows_model("unlimited", ""),
        "a client with no allow-list is unaffected — nothing to enforce"
    );
}

#[tokio::test]
async fn gate_denies_unreadable_model_for_restricted_client() {
    let state = state_with_clients(vec![mk_client("limited", "k1", &["claude-haiku-*"])]);
    let err = state
        .pre_request_gate("limited", "")
        .await
        .expect_err("unknown model must be denied for a restricted client");
    assert_eq!(err.status(), StatusCode::FORBIDDEN);
    let body = axum::body::to_bytes(err.into_body(), 64 * 1024)
        .await
        .unwrap();
    let text = String::from_utf8_lossy(&body);
    assert!(
        text.contains("no model could be read"),
        "body should explain the empty-model denial, got: {text}"
    );
}

/// End-to-end proof of the same thing: an unparseable body must not smuggle a
/// restricted client past its allow-list.
#[tokio::test]
async fn native_surface_denies_restricted_client_sending_unparseable_body() {
    let (mock_url, _handle) = spawn_mock_upstream().await;
    let (app, _state) = authed_app(
        &mock_url,
        vec![mk_client("limited", "key-limited", &["claude-haiku-*"])],
    );
    let addr = serve(app).await;
    let resp = Client::new()
        .post(format!("http://{addr}/v1/messages"))
        .header("content-type", "application/json")
        .header("x-api-key", "key-limited")
        .body("this is not json")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::FORBIDDEN);
}

/// The model string is caller-controlled and bounded only by the body cap.
/// Untruncated it would be retained for the process lifetime and re-serialized
/// into `/metrics` on every scrape.
#[test]
fn denied_model_label_is_truncated() {
    let state = state_with_clients(vec![mk_client("limited", "k1", &["claude-haiku-*"])]);
    let huge = "z".repeat(100_000);
    state.note_model_denied("limited", &huge);
    let counts = state.model_denied.lock().unwrap();
    let (_, label) = counts.keys().next().unwrap();
    assert!(
        label.chars().count() <= MAX_LABEL_CHARS + 1,
        "label not truncated: {} chars",
        label.chars().count()
    );
}

/// The other half of the same guarantee: the 403 body echoes the denied model
/// through `truncate_label`, so an oversized model field must not be reflected
/// back untruncated.
#[tokio::test]
async fn denied_model_response_body_is_truncated() {
    let state = state_with_clients(vec![mk_client("limited", "k1", &["claude-haiku-*"])]);
    let huge = "z".repeat(100_000);
    let err = state
        .pre_request_gate("limited", &huge)
        .await
        .expect_err("oversized model must be denied");
    let body = axum::body::to_bytes(err.into_body(), 64 * 1024)
        .await
        .unwrap();
    assert!(
        String::from_utf8_lossy(&body).chars().count() < 1_000,
        "403 body must not echo the untruncated model"
    );
}

/// Truncation is char-based: slicing a multi-byte string on a byte boundary
/// would panic.
#[test]
fn truncate_label_handles_multibyte_without_panicking() {
    let s = "é".repeat(200);
    let out = truncate_label(&s);
    assert!(out.chars().count() <= MAX_LABEL_CHARS + 1);
    assert_eq!(truncate_label("short"), "short");
}

// ── AC-9 / AC-10: enforcement in pre_request_gate ──

#[tokio::test]
async fn gate_denies_model_outside_client_allow_list_with_403_naming_both() {
    let state = state_with_clients(vec![mk_client("limited", "k1", &["claude-haiku-*"])]);
    let err = state
        .pre_request_gate("limited", "claude-opus-5")
        .await
        .expect_err("opus must be denied");
    assert_eq!(
        err.status(),
        StatusCode::FORBIDDEN,
        "policy denial is 403, not 429 — 429 means 'retry later', which this never becomes"
    );
    let body = axum::body::to_bytes(err.into_body(), 64 * 1024)
        .await
        .unwrap();
    let text = String::from_utf8_lossy(&body);
    assert!(
        text.contains("limited"),
        "body must name the client: {text}"
    );
    assert!(
        text.contains("claude-opus-5"),
        "body must name the model: {text}"
    );
}

#[tokio::test]
async fn gate_allows_model_inside_client_allow_list() {
    let state = state_with_clients(vec![mk_client("limited", "k1", &["claude-haiku-*"])]);
    assert!(state
        .pre_request_gate("limited", "claude-haiku-4-5")
        .await
        .is_ok());
}

#[tokio::test]
async fn gate_allow_list_bypassed_by_operators() {
    let state = Arc::new(AppState {
        clients: vec![mk_client("limited", "k1", &["claude-haiku-*"])],
        operators: vec!["limited".to_string()],
        ..test_state_base()
    });
    assert!(
        state
            .pre_request_gate("limited", "claude-opus-5")
            .await
            .is_ok(),
        "operators bypass the allow-list like every other gate check"
    );
}

// ── AC-11: denial counter + bounded model cardinality ──

#[tokio::test]
async fn model_denial_increments_counter_per_client_and_model() {
    let state = state_with_clients(vec![mk_client("limited", "k1", &["claude-haiku-*"])]);
    for _ in 0..3 {
        assert!(state
            .pre_request_gate("limited", "claude-opus-5")
            .await
            .is_err());
    }
    assert!(state
        .pre_request_gate("limited", "claude-fable-5")
        .await
        .is_err());

    let counts = state.model_denied.lock().unwrap();
    assert_eq!(
        counts.get(&("limited".to_string(), "claude-opus-5".to_string())),
        Some(&3)
    );
    assert_eq!(
        counts.get(&("limited".to_string(), "claude-fable-5".to_string())),
        Some(&1)
    );
}

/// The model label is caller-controlled — unbounded growth here would be a
/// metrics-cardinality DoS.
#[test]
fn model_denial_labels_are_bounded_by_other_overflow() {
    let state = state_with_clients(vec![mk_client("limited", "k1", &["claude-haiku-*"])]);
    for i in 0..(MAX_MODEL_DENIED_LABELS + 25) {
        state.note_model_denied("limited", &format!("junk-model-{i}"));
    }
    let counts = state.model_denied.lock().unwrap();
    assert!(
        counts.len() <= MAX_MODEL_DENIED_LABELS + 1,
        "label map grew unbounded: {} entries",
        counts.len()
    );
    assert_eq!(
        counts.get(&("limited".to_string(), "_other".to_string())),
        Some(&25),
        "overflow must land in the _other bucket, not be dropped"
    );
}

// ── AC-5 / AC-6: startup validation ──

#[test]
fn config_rejects_proxy_key_and_clients_together() {
    let toml_str = r#"
listen = "0.0.0.0:8080"
proxy_key = "legacy"

[[clients]]
name = "geo"
key = "key-geo"
"#;
    let value: toml::Value = toml::from_str(toml_str).unwrap();
    let err = reject_legacy_config_keys(&value).unwrap_err();
    assert!(
        err.contains("proxy_key"),
        "error must name proxy_key: {err}"
    );
    assert!(err.contains("clients"), "error must name clients: {err}");
}

#[test]
fn config_accepts_proxy_key_alone_and_clients_alone() {
    for toml_str in [
        "listen = \"0.0.0.0:8080\"\nproxy_key = \"legacy\"\n",
        "listen = \"0.0.0.0:8080\"\n\n[[clients]]\nname = \"geo\"\nkey = \"key-geo\"\n",
    ] {
        let value: toml::Value = toml::from_str(toml_str).unwrap();
        assert!(reject_legacy_config_keys(&value).is_ok());
    }
}

#[test]
fn config_parses_clients_table_with_model_allow_list() {
    let toml_str = r#"
listen = "0.0.0.0:8080"

[[clients]]
name = "geo"
key = "key-geo"
models = ["claude-haiku-*", "claude-sonnet-4-6"]

[[clients]]
name = "radar"
key = "key-radar"
"#;
    let cfg: Config = toml::from_str(toml_str).unwrap();
    assert_eq!(cfg.clients.len(), 2);
    assert_eq!(cfg.clients[0].name, "geo");
    assert_eq!(cfg.clients[0].key, "key-geo");
    assert_eq!(cfg.clients[0].models.len(), 2);
    assert!(
        cfg.clients[1].models.is_empty(),
        "omitted models must default to empty (= all allowed)"
    );
}

/// Build a `Config` from a TOML fragment. Goes through the real deserializer,
/// so these tests also pin the config surface, and it beats hand-writing
/// ~30-field `Config` / 7-field `ResponseCacheConfig` literals per case.
fn cfg(fragment: &str) -> Config {
    toml::from_str(&format!("listen = \"127.0.0.1:0\"\n{fragment}"))
        .unwrap_or_else(|e| panic!("test config parse error: {e}\n---\n{fragment}"))
}

const RC_BLOCK: &str = "\n[response_cache]\nbackend = \"redis\"\nredis_url = \"redis://localhost\"\nmaster_key = \"0000000000000000000000000000000000000000000000000000000000000000\"\n";

#[test]
fn validate_clients_rejects_duplicate_names_and_keys() {
    let err = validate_clients(&cfg(
        "[[clients]]\nname = \"geo\"\nkey = \"k1\"\n\n[[clients]]\nname = \"geo\"\nkey = \"k2\"\n",
    ))
    .unwrap_err();
    assert!(err.contains("duplicate name"), "{err}");

    let err = validate_clients(&cfg(
        "[[clients]]\nname = \"geo\"\nkey = \"k1\"\n\n[[clients]]\nname = \"radar\"\nkey = \"k1\"\n",
    ))
    .unwrap_err();
    assert!(err.contains("duplicate key"), "{err}");
    assert!(
        !err.contains("k1"),
        "the error must not echo the credential: {err}"
    );
}

#[test]
fn validate_clients_rejects_bad_names_and_empty_keys() {
    for (fragment, needle) in [
        ("[[clients]]\nname = \"\"\nkey = \"k1\"\n", "name"),
        ("[[clients]]\nname = \"   \"\nkey = \"k1\"\n", "name"),
        ("[[clients]]\nname = \"-\"\nkey = \"k1\"\n", "sentinel"),
        // Reserved: /_stats and /metrics rewrite operator identities to the
        // literal "_operator", so a real tenant with that name would silently
        // merge with the aggregated operator bucket.
        (
            "[[clients]]\nname = \"_operator\"\nkey = \"k1\"\n",
            "_operator",
        ),
        ("[[clients]]\nname = \"geo\"\nkey = \"\"\n", "key"),
        // Untrimmed: stored verbatim, so it would become a client_id matching
        // no client_budgets / operators / response_cache.clients key.
        ("[[clients]]\nname = \" geo\"\nkey = \"k1\"\n", "whitespace"),
        ("[[clients]]\nname = \"geo \"\nkey = \"k1\"\n", "whitespace"),
    ] {
        let err = validate_clients(&cfg(fragment)).unwrap_err();
        assert!(err.contains(needle), "expected '{needle}' in: {err}");
    }
}

#[test]
fn validate_clients_accepts_a_well_formed_table() {
    assert!(validate_clients(&cfg(
        "[[clients]]\nname = \"geo\"\nkey = \"k1\"\nmodels = [\"claude-haiku-*\"]\n\n[[clients]]\nname = \"radar\"\nkey = \"k2\"\n",
    ))
    .is_ok());
}

/// One client registry, not five. Each of these config surfaces keys on a
/// client name and each fails SILENTLY on a typo — a mistyped budget means
/// UNLIMITED spend (`check_budget` returns Ok for an unknown client), a
/// mistyped operator silently gates the caller it meant to exempt, a mistyped
/// cache client silently disables the cache.
#[test]
fn validate_clients_rejects_any_surface_naming_no_configured_client() {
    // NOTE: top-level scalars/arrays must precede the first table header —
    // a bare `operators = [...]` written after `[[clients]]` binds to that
    // table instead and is silently dropped, making the test pass vacuously.
    let base = "\n[[clients]]\nname = \"geo\"\nkey = \"k1\"\n";
    for (fragment, surface) in [
        (
            format!("{base}\n[client_budgets]\ngeo-pipeline = 100\n"),
            "client_budgets",
        ),
        (
            format!("{base}\n[client_utilization_limits]\ngeo-pipeline = 0.5\n"),
            "client_utilization_limits",
        ),
        (
            format!("operators = [\"geo-pipeline\"]\n{base}"),
            "operators",
        ),
        (
            format!("{base}{RC_BLOCK}clients = [\"geo-pipeline\"]\n"),
            "response_cache.clients",
        ),
    ] {
        let err = validate_clients(&cfg(&fragment)).unwrap_err();
        assert!(err.contains(surface), "expected '{surface}' in: {err}");
        assert!(err.contains("geo-pipeline"), "must name the typo: {err}");
    }
}

#[test]
fn validate_clients_accepts_every_surface_naming_a_configured_client() {
    let fragment = format!(
        "operators = [\"geo\"]\n\n[[clients]]\nname = \"geo\"\nkey = \"k1\"\n\n[client_budgets]\ngeo = 100\n\n[client_utilization_limits]\ngeo = 0.5\n{RC_BLOCK}clients = [\"geo\"]\n"
    );
    let parsed = cfg(&fragment);
    // Guard against the vacuous-pass trap above: assert the fragment really
    // populated all four surfaces before asserting validation accepts them.
    assert_eq!(parsed.operators, vec!["geo".to_string()]);
    assert!(parsed.client_budgets.contains_key("geo"));
    assert!(parsed.client_utilization_limits.contains_key("geo"));
    assert_eq!(
        parsed.response_cache.as_ref().unwrap().clients,
        vec!["geo".to_string()]
    );
    assert!(validate_clients(&parsed).is_ok());
}

/// On the legacy path there is no registry to check against, so none of the
/// cross-checks may fire — existing configs must keep booting.
#[test]
fn validate_clients_skips_all_crosschecks_without_a_client_table() {
    let fragment = format!(
        "operators = [\"anything\"]\n\n[client_budgets]\nanything = 100\n{RC_BLOCK}clients = [\"anything\"]\n"
    );
    assert!(validate_clients(&cfg(&fragment)).is_ok());
}

/// `passthrough` forwards the caller's auth headers upstream untouched. Under
/// `[[clients]]` those headers carry the client's PROXY key, so the two modes
/// together would transmit every client key to the upstream.
#[test]
fn validate_clients_rejects_passthrough_endpoint_alongside_clients() {
    let err = validate_clients(&cfg(
        "[[clients]]\nname = \"geo\"\nkey = \"k1\"\n\n[[endpoints]]\nname = \"managed\"\ntoken = \"passthrough\"\n",
    ))
    .unwrap_err();
    assert!(err.contains("passthrough"), "{err}");
    assert!(err.contains("managed"), "must name the endpoint: {err}");
}

/// …but passthrough on the legacy path is untouched.
#[test]
fn validate_clients_allows_passthrough_without_a_client_table() {
    assert!(validate_clients(&cfg(
        "[[endpoints]]\nname = \"managed\"\ntoken = \"passthrough\"\n",
    ))
    .is_ok());
}

// ── AC-4: the response-cache tenant follows the authenticated principal ──

/// #113 derives the cache's per-client encryption key from `client_id`. This
/// asserts the tenant a spoofing caller would land in is its OWN, not the one
/// it named — i.e. the cross-tenant read that ticket accepted is now closed.
#[test]
fn response_cache_tenant_follows_the_authenticated_principal() {
    let state = state_with_clients(vec![
        mk_client("alpha", "key-alpha", &[]),
        mk_client("bravo", "key-bravo", &[]),
    ]);
    let ip = test_ip();
    let headers = hdrs(&[("x-api-key", "key-alpha"), ("x-client-id", "bravo")]);
    let principal = state.authenticate(&ip, &headers, false).unwrap().unwrap();
    let rctx = RequestContext::from_request(&state, &ip, &headers, Some(principal));

    // The cache is keyed by this exact string (`rc.clients.get(&client_id)`),
    // and the HKDF tenant is derived from it.
    assert_eq!(rctx.client_id, "alpha");
    assert_ne!(rctx.client_id, "bravo");
}

// ── Integration: both surfaces, through the real router ──

fn authed_app(upstream_url: &str, clients: Vec<ClientConfig>) -> (Router, Arc<AppState>) {
    let mut acct = mk_endpoint("acct-a", "sk-ant-api-test-aaa");
    acct.base_url = upstream_url.to_string();
    let state = Arc::new(AppState {
        endpoints: vec![acct],
        clients,
        ..test_state_base()
    });
    (build_router(state.clone()), state)
}

/// Native surface: the request authenticates as `limited` while claiming to be
/// `unlimited`. It must be gated as `limited` — 403 — not waved through.
#[tokio::test]
async fn native_surface_gates_spoofing_client_by_its_real_identity() {
    let (mock_url, _handle) = spawn_mock_upstream().await;
    let (app, state) = authed_app(
        &mock_url,
        vec![
            mk_client("limited", "key-limited", &["claude-haiku-*"]),
            mk_client("unlimited", "key-unlimited", &[]),
        ],
    );
    let addr = serve(app).await;
    let client = Client::new();

    let resp = client
        .post(format!("http://{addr}/v1/messages"))
        .header("content-type", "application/json")
        .header("x-api-key", "key-limited")
        .header("x-client-id", "unlimited")
        .body(r#"{"model":"claude-opus-5","max_tokens":1,"messages":[{"role":"user","content":"hi"}]}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::FORBIDDEN);
    assert!(resp.text().await.unwrap().contains("limited"));

    // Same credential, a model it IS allowed: unaffected.
    let resp = client
        .post(format!("http://{addr}/v1/messages"))
        .header("content-type", "application/json")
        .header("x-api-key", "key-limited")
        .body(r#"{"model":"claude-haiku-4-5","max_tokens":1,"messages":[{"role":"user","content":"hi"}]}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    assert_eq!(
        state
            .model_denied
            .lock()
            .unwrap()
            .get(&("limited".to_string(), "claude-opus-5".to_string())),
        Some(&1)
    );
}

/// OpenAI-compat surface: same gate, reached through the other handler —
/// `pre_request_gate` is called from both, so one placement covers both.
#[tokio::test]
async fn openai_surface_enforces_the_same_allow_list() {
    let (mock_url, _handle) = spawn_mock_upstream().await;
    let (app, _state) = authed_app(
        &mock_url,
        vec![mk_client("limited", "key-limited", &["claude-haiku-*"])],
    );
    let addr = serve(app).await;
    let client = Client::new();

    let resp = client
        .post(format!("http://{addr}/v1/chat/completions"))
        .header("content-type", "application/json")
        .header("authorization", "Bearer key-limited")
        .header("x-client-id", "someone-else")
        .body(r#"{"model":"claude-opus-5","messages":[{"role":"user","content":"hi"}],"max_tokens":1}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::FORBIDDEN);

    let resp = client
        .post(format!("http://{addr}/v1/chat/completions"))
        .header("content-type", "application/json")
        .header("authorization", "Bearer key-limited")
        .body(r#"{"model":"claude-haiku-4-5","messages":[{"role":"user","content":"hi"}],"max_tokens":1}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
}

/// AC-2: no site left on the old single-key path.
#[tokio::test]
async fn all_four_auth_sites_reject_an_unknown_key() {
    let (mock_url, _handle) = spawn_mock_upstream().await;
    let (app, _state) = authed_app(&mock_url, vec![mk_client("geo", "key-geo", &[])]);
    let addr = serve(app).await;
    let client = Client::new();

    for path in ["/_stats", "/metrics"] {
        let resp = client
            .get(format!("http://{addr}{path}"))
            .header("x-api-key", "key-wrong")
            .send()
            .await
            .unwrap();
        assert_eq!(
            resp.status(),
            reqwest::StatusCode::UNAUTHORIZED,
            "{path} accepted an unknown key"
        );
        // …and the right one is recognized — but 403, not 200: the admin
        // surfaces are operator-only since LAB-1192 and `geo` is a plain
        // client. The operator 200 path is covered by the AC-6 matrix tests.
        let resp = client
            .get(format!("http://{addr}{path}"))
            .header("x-api-key", "key-geo")
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), reqwest::StatusCode::FORBIDDEN, "{path}");
    }

    let resp = client
        .post(format!("http://{addr}/v1/messages"))
        .header("content-type", "application/json")
        .header("x-api-key", "key-wrong")
        .body(r#"{"model":"claude-haiku-4-5","max_tokens":1,"messages":[{"role":"user","content":"hi"}]}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::UNAUTHORIZED);

    let resp = client
        .post(format!("http://{addr}/v1/chat/completions"))
        .header("content-type", "application/json")
        .header("x-api-key", "key-wrong")
        .body(r#"{"model":"claude-haiku-4-5","messages":[{"role":"user","content":"hi"}],"max_tokens":1}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::UNAUTHORIZED);

    // The bearer branch is reachable only on this surface — cover its reject
    // path too: wrong key, bare scheme, and a valid key without the scheme.
    for auth in ["Bearer key-wrong", "Bearer", "key-geo"] {
        let resp = client
            .post(format!("http://{addr}/v1/chat/completions"))
            .header("content-type", "application/json")
            .header("authorization", auth)
            .body(r#"{"model":"claude-haiku-4-5","messages":[{"role":"user","content":"hi"}],"max_tokens":1}"#)
            .send()
            .await
            .unwrap();
        assert_eq!(
            resp.status(),
            reqwest::StatusCode::UNAUTHORIZED,
            "'{auth}' must not authenticate"
        );
    }
}

/// AC-11: the denial counter reaches /metrics under its documented name.
/// The scrape presents an OPERATOR credential — /metrics is operator-only
/// since LAB-1192.
#[tokio::test]
async fn metrics_exposes_the_model_denial_counter() {
    let (mock_url, _handle) = spawn_mock_upstream().await;
    let mut acct = mk_endpoint("acct-a", "sk-ant-api-test-aaa");
    acct.base_url = mock_url.to_string();
    let state = Arc::new(AppState {
        endpoints: vec![acct],
        clients: vec![
            mk_client("limited", "key-limited", &["claude-haiku-*"]),
            mk_client("ops", "key-ops", &[]),
        ],
        operators: vec!["ops".to_string()],
        ..test_state_base()
    });
    let app = build_router(state.clone());
    assert!(state
        .pre_request_gate("limited", "claude-opus-5")
        .await
        .is_err());

    let addr = serve(app).await;
    let body = Client::new()
        .get(format!("http://{addr}/metrics"))
        .header("x-api-key", "key-ops")
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(
        body.contains(
            "anthropic_client_model_denied_total{client=\"limited\",model=\"claude-opus-5\"} 1"
        ),
        "denial counter missing from /metrics:\n{body}"
    );
}
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

fn default_betas() -> Vec<String> {
    DEFAULT_CLIENT_BETA_ALLOWLIST
        .iter()
        .map(|s| s.to_string())
        .collect()
}

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
/// `DEFAULT_CLIENT_BETA_ALLOWLIST` listed only six entries and dropped these
/// ten, which 400'd every Claude Code request through the proxy —
/// `context-management` in particular has a body-side `context_management`
/// object that the LB forwards verbatim, so dropping the header alone is a
/// hard upstream rejection, not a silent feature downgrade.
///
/// This inventory came off `anthropic_beta_flag_dropped_total` on the live
/// fleet. Date suffixes are deliberately concrete: the allow-list wildcards
/// them, so a Claude Code date bump keeps passing while a genuinely new flag
/// family still shows up as a drop.
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
// ── Real-Redis integration tests (LAB-931) ──────────────────────────
//
// Behavioural coverage for the cross-replica coordination layer against a
// REAL Redis/Valkey backend. These pin the semantics of every coordination
// call site — INCRBY/EXPIRE budgets, SET EX hard-limit propagation, the Lua
// CAS recovery sentinel, the three-phase MGET merge in sync_from_redis,
// SCAN pagination in cluster_info, pipelined HINCRBY transport-error
// flushing, and the SET NX EX probe lock — exactly as they behave today,
// so the redis→fred migration has a baseline to rewrite against.
//
// Opt-in by design: set `ALB_TEST_REDIS_URL` (plain `redis://host:port`,
// no db suffix, no auth) to run them. When unset, every test prints a SKIP
// notice and returns — never a silent pass against nothing. When the env
// var IS set and the backend is unreachable, the tests PANIC, so CI (which
// always sets it — see .github/workflows/ci.yml) can never skip silently.
//
// Isolation: each test owns a dedicated logical DB (the `/N` suffix in the
// connection URL) and flushes it on connect, because ALL `alb:*`
// coordination keys (hard/rate/weight/budget/probe/heartbeat/
// transport_errors) are hardcoded in production code and cannot be
// prefixed per-test. Never point ALB_TEST_REDIS_URL at a Redis holding
// data you care about.
mod redis_integration {
    use super::*;
    use redis::AsyncCommands;

    const TEST_REDIS_ENV: &str = "ALB_TEST_REDIS_URL";

    /// Resolve the opt-in backend URL. None (with a SKIP notice) when the
    /// env var is unset locally; PANICS when unset in CI (`CI` env present),
    /// so no workflow — current or future — can green with this suite
    /// silently skipped. The documented shape (`redis://host:port` — no
    /// auth, no db suffix) is validated here, once: the callers append
    /// `/{db}` for isolation and parse `host:port` for the killable proxy,
    /// both of which silently misbehave on a decorated URL.
    fn test_redis_url() -> Option<String> {
        match std::env::var(TEST_REDIS_ENV) {
            Ok(u) => {
                // Never interpolate the raw value into these messages: a
                // rejected URL may carry credentials (redis://user:pass@…),
                // and the panic lands in CI logs.
                let host_port = u.strip_prefix("redis://").unwrap_or_else(|| {
                    panic!("{TEST_REDIS_ENV} must be a plain redis:// url (value redacted)")
                });
                assert!(
                    !host_port.contains('@') && !host_port.trim_end_matches('/').contains('/'),
                    "{TEST_REDIS_ENV} must be redis://host:port — no auth, no db suffix \
                     (value redacted)"
                );
                Some(u)
            }
            Err(_) if std::env::var("CI").is_ok() => {
                panic!(
                    "CI run without {TEST_REDIS_ENV}: the redis_integration suite would \
                     silently skip — wire a Redis/Valkey service into this workflow"
                );
            }
            Err(_) => {
                eprintln!(
                    "SKIP (redis integration): {TEST_REDIS_ENV} not set — no backend to test against"
                );
                None
            }
        }
    }

    /// Connect to the opt-in test backend, selecting logical DB `db` and
    /// flushing it. Returns None (with a SKIP notice) when the env var is
    /// unset; panics when it is set but the backend is unreachable.
    /// `db` must be unique per test — logical DBs are the isolation unit.
    ///
    /// Returns a PAIR of clients on the same DB: the `redis`-crate connection
    /// is the test's independent fixture/assertion client (deliberately NOT
    /// the client under test), and the fred client is what goes into
    /// `AppState.redis` — the production coordination path being verified.
    async fn redis_test_conn(db: u8) -> Option<(redis::aio::ConnectionManager, RedisClient)> {
        let base = test_redis_url()?;
        let url = format!("{}/{db}", base.trim_end_matches('/'));
        let conn = connect_and_flush(&url).await;
        let fred = fred_test_client(&url).await;
        Some((conn, fred))
    }

    /// Independent (redis-crate) connection WITHOUT flushing — for asserting
    /// on state that must survive, e.g. after a backend recovery.
    async fn connect(url: &str) -> redis::aio::ConnectionManager {
        let client = redis::Client::open(url)
            .unwrap_or_else(|e| panic!("{TEST_REDIS_ENV}: invalid url {url}: {e}"));
        // Short timeouts + a single retry: the failure-path tests kill the
        // backend mid-run and must observe errors in milliseconds, not after
        // the production-scale reconnect backoff.
        let cfg = redis::aio::ConnectionManagerConfig::new()
            .set_response_timeout(Some(Duration::from_secs(1)))
            .set_connection_timeout(Some(Duration::from_secs(2)))
            .set_number_of_retries(1);
        client
            .get_connection_manager_with_config(cfg)
            .await
            .unwrap_or_else(|e| {
                panic!("{TEST_REDIS_ENV} set but backend unreachable at {url}: {e}")
            })
    }

    async fn connect_and_flush(url: &str) -> redis::aio::ConnectionManager {
        let mut conn = connect(url).await;
        let flushed: redis::RedisResult<()> = redis::cmd("FLUSHDB").query_async(&mut conn).await;
        flushed.unwrap_or_else(|e| panic!("FLUSHDB failed on {url}: {e}"));
        conn
    }

    /// The client under test: a fred client configured like `connect` (short
    /// budgets so failure-path tests observe errors in milliseconds) plus a
    /// fast constant reconnect policy so the recovery test can watch
    /// coordination resume without production-scale backoff.
    async fn fred_test_client(url: &str) -> RedisClient {
        let config = RedisConfig::from_url(url)
            .unwrap_or_else(|e| panic!("{TEST_REDIS_ENV}: invalid url {url}: {e}"));
        let perf = PerformanceConfig {
            default_command_timeout: Duration::from_secs(1),
            ..Default::default()
        };
        let conn_config = ConnectionConfig {
            connection_timeout: Duration::from_secs(2),
            internal_command_timeout: Duration::from_secs(2),
            ..Default::default()
        };
        let policy = ReconnectPolicy::new_constant(0, 100);
        let client = RedisClient::new(config, Some(perf), Some(conn_config), Some(policy));
        let _connect_handle = client.init().await.unwrap_or_else(|e| {
            panic!("{TEST_REDIS_ENV} set but backend unreachable at {url}: {e}")
        });
        client
    }

    /// TCP forwarder in front of the real backend that can be killed
    /// mid-test to simulate Redis dying while connections are established.
    /// Killing aborts every live relay and drops the listener, so both
    /// in-flight commands and subsequent reconnect attempts fail.
    async fn spawn_killable_proxy(target: String) -> (String, tokio::sync::oneshot::Sender<()>) {
        spawn_killable_proxy_at("127.0.0.1:0", target).await
    }

    /// Same as `spawn_killable_proxy`, but at a caller-chosen address — used
    /// to REVIVE a killed proxy at its old address so a reconnect policy can
    /// find the backend again.
    async fn spawn_killable_proxy_at(
        bind: &str,
        target: String,
    ) -> (String, tokio::sync::oneshot::Sender<()>) {
        let listener = tokio::net::TcpListener::bind(bind).await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (kill_tx, mut kill_rx) = tokio::sync::oneshot::channel::<()>();
        tokio::spawn(async move {
            let mut relays: Vec<tokio::task::JoinHandle<()>> = Vec::new();
            loop {
                tokio::select! {
                    _ = &mut kill_rx => break,
                    accepted = listener.accept() => {
                        let Ok((mut inbound, _)) = accepted else { break };
                        let target = target.clone();
                        relays.push(tokio::spawn(async move {
                            if let Ok(mut outbound) =
                                tokio::net::TcpStream::connect(&target).await
                            {
                                let _ =
                                    tokio::io::copy_bidirectional(&mut inbound, &mut outbound)
                                        .await;
                            }
                        }));
                    }
                }
            }
            for relay in relays {
                relay.abort();
            }
            // Listener drops here → further connects are refused.
        });
        (format!("127.0.0.1:{}", addr.port()), kill_tx)
    }

    /// fred client (the client under test) routed through a killable proxy.
    /// Same skip/panic contract as `redis_test_conn`. The DB is flushed via
    /// the independent redis-crate client before the fred client connects.
    async fn proxied_conn(db: u8) -> Option<(RedisClient, tokio::sync::oneshot::Sender<()>)> {
        let base = test_redis_url()?;
        let target = base
            .trim_start_matches("redis://")
            .trim_end_matches('/')
            .split('/')
            .next()
            .unwrap()
            .to_string();
        let (proxy_addr, kill) = spawn_killable_proxy(target).await;
        let url = format!("redis://{proxy_addr}/{db}");
        drop(connect_and_flush(&url).await);
        let fred = fred_test_client(&url).await;
        Some((fred, kill))
    }

    async fn kill_proxy(kill: tokio::sync::oneshot::Sender<()>) {
        let _ = kill.send(());
        // Give the aborts a beat to drop sockets before asserting failures.
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    /// Budget keys embed `now_epoch / 86400`, recomputed independently by
    /// the test and each production call site — a UTC-midnight rollover
    /// between the two computations splits the key and fails spuriously.
    /// Park until the day is young enough that the test finishes inside it.
    async fn avoid_utc_midnight() {
        let into_day = AppState::now_epoch() % 86_400;
        if into_day > 86_390 {
            tokio::time::sleep(Duration::from_secs(86_400 - into_day + 1)).await;
        }
    }

    fn state_with_redis(endpoints: Vec<Endpoint>, client: RedisClient) -> Arc<AppState> {
        Arc::new(AppState {
            endpoints,
            redis: Some(client),
            ..test_state_base()
        })
    }

    /// Poll until `check` passes — for asserting on fire-and-forget
    /// tokio::spawn writes (the sentinel CAS).
    async fn eventually<F, Fut>(what: &str, mut check: F)
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = bool>,
    {
        for _ in 0..100 {
            if check().await {
                return;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        panic!("timed out waiting for {what}");
    }

    fn remote_rate_info(updated_at: u64, utilization: f64) -> RedisRateInfo {
        RedisRateInfo {
            utilization: Some(utilization),
            utilization_5h: Some(utilization),
            utilization_7d: None,
            reset_5h: None,
            reset_7d: None,
            status_5h: Some("allowed".into()),
            status_7d: None,
            claims_7d: HashMap::new(),
            representative_claim: None,
            remaining_requests: Some(11),
            remaining_tokens: Some(22),
            limit_requests: None,
            limit_tokens: None,
            overage_in_use: false,
            overage_status: None,
            overage_utilization: None,
            overage_reset: None,
            updated_at,
        }
    }

    /// Mock upstream that counts requests — observable side effect for the
    /// probe-lock tests (probe fired vs probe suppressed).
    async fn spawn_counting_upstream() -> (String, Arc<AtomicUsize>) {
        let counter = Arc::new(AtomicUsize::new(0));
        let hits = counter.clone();
        let app = Router::new().fallback(any(move || {
            let hits = hits.clone();
            async move {
                hits.fetch_add(1, Ordering::SeqCst);
                axum::Json(serde_json::json!({"id": "msg_probe", "type": "message"}))
            }
        }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        (format!("http://{addr}"), counter)
    }

    /// AC2 phase 1: the hard-limit MGET merge against real keys — a remote
    /// future epoch is applied, "most recent wins" holds in both directions,
    /// the clear sentinel clears, and an absent key (MGET None) touches
    /// nothing. Pairs with the pure `classify_hard_limit_*` unit tests.
    #[tokio::test]
    async fn sync_from_redis_merges_hard_limits_with_real_backend() {
        let Some((mut conn, fred)) = redis_test_conn(1).await else {
            return;
        };
        let state = state_with_redis(
            vec![
                make_endpoint("hl-apply", Protocol::Anthropic),
                make_endpoint("hl-clear", Protocol::Anthropic),
                make_endpoint("hl-remote-older", Protocol::Anthropic),
                make_endpoint("hl-remote-newer", Protocol::Anthropic),
                make_endpoint("hl-absent", Protocol::Anthropic),
            ],
            fred,
        );

        let now_epoch = AppState::now_epoch();
        let now = Instant::now();
        state.endpoints[1]
            .rate_info
            .write()
            .await
            .hard_limited_until = Some(now + Duration::from_secs(500));
        state.endpoints[2]
            .rate_info
            .write()
            .await
            .hard_limited_until = Some(now + Duration::from_secs(600));
        state.endpoints[3]
            .rate_info
            .write()
            .await
            .hard_limited_until = Some(now + Duration::from_secs(60));

        let _: () = conn
            .set("alb:hard:hl-apply", now_epoch + 120)
            .await
            .unwrap();
        let _: () = conn
            .set("alb:hard:hl-clear", HARD_LIMIT_CLEARED_SENTINEL)
            .await
            .unwrap();
        let _: () = conn
            .set("alb:hard:hl-remote-older", now_epoch + 60)
            .await
            .unwrap();
        let _: () = conn
            .set("alb:hard:hl-remote-newer", now_epoch + 600)
            .await
            .unwrap();

        state.sync_from_redis().await;

        let after = Instant::now();
        let until = state.endpoints[0]
            .rate_info
            .read()
            .await
            .hard_limited_until
            .expect("remote future epoch must apply a hard limit");
        let secs = until.saturating_duration_since(after).as_secs();
        assert!(
            (115..=121).contains(&secs),
            "hl-apply should be limited ~120s, got {secs}s"
        );

        assert!(
            state.endpoints[1]
                .rate_info
                .read()
                .await
                .hard_limited_until
                .is_none(),
            "clear sentinel must clear the local hard limit"
        );

        let until = state.endpoints[2]
            .rate_info
            .read()
            .await
            .hard_limited_until
            .expect("local hard limit must survive an older remote");
        let secs = until.saturating_duration_since(after).as_secs();
        assert!(
            (595..=601).contains(&secs),
            "older remote (+60s) must not shorten the newer local limit (+600s), got {secs}s"
        );

        let until = state.endpoints[3]
            .rate_info
            .read()
            .await
            .hard_limited_until
            .expect("newer remote must extend the local hard limit");
        let secs = until.saturating_duration_since(after).as_secs();
        assert!(
            (595..=601).contains(&secs),
            "newer remote (+600s) must override the older local limit (+60s), got {secs}s"
        );

        assert!(
            state.endpoints[4]
                .rate_info
                .read()
                .await
                .hard_limited_until
                .is_none(),
            "absent key (MGET None) must not fabricate a hard limit"
        );
    }

    /// AC2 phase 2: the rate-info merge's "most recent wins" comparison in
    /// both directions, plus the absent-key case, against real MGET replies.
    #[tokio::test]
    async fn sync_from_redis_rate_info_most_recent_wins_both_directions() {
        let Some((mut conn, fred)) = redis_test_conn(2).await else {
            return;
        };
        let state = state_with_redis(
            vec![
                make_endpoint("ri-remote-newer", Protocol::Anthropic),
                make_endpoint("ri-remote-older", Protocol::Anthropic),
                make_endpoint("ri-absent", Protocol::Anthropic),
            ],
            fred,
        );
        let now_epoch = AppState::now_epoch();

        {
            let mut info = state.endpoints[0].rate_info.write().await;
            info.utilization = Some(0.10);
            info.last_updated_epoch = Some(now_epoch - 300);
        }
        {
            let mut info = state.endpoints[1].rate_info.write().await;
            info.utilization = Some(0.20);
            info.remaining_tokens = Some(777);
            info.last_updated_epoch = Some(now_epoch);
        }
        {
            let mut info = state.endpoints[2].rate_info.write().await;
            info.utilization = Some(0.30);
        }

        let newer = serde_json::to_string(&remote_rate_info(now_epoch, 0.90)).unwrap();
        let older = serde_json::to_string(&remote_rate_info(now_epoch - 600, 0.80)).unwrap();
        let _: () = conn.set("alb:rate:ri-remote-newer", newer).await.unwrap();
        let _: () = conn.set("alb:rate:ri-remote-older", older).await.unwrap();

        state.sync_from_redis().await;

        {
            let info = state.endpoints[0].rate_info.read().await;
            assert_eq!(info.utilization, Some(0.90), "newer remote must be applied");
            assert_eq!(info.remaining_tokens, Some(22));
            assert_eq!(
                info.last_updated_epoch,
                Some(now_epoch),
                "local epoch must follow the remote updated_at"
            );
        }
        {
            let info = state.endpoints[1].rate_info.read().await;
            assert_eq!(info.utilization, Some(0.20), "older remote must be ignored");
            assert_eq!(info.remaining_tokens, Some(777));
        }
        assert_eq!(
            state.endpoints[2].rate_info.read().await.utilization,
            Some(0.30),
            "absent key must not touch local rate info"
        );
    }

    /// AC2 phase 3: published routing weights land in the gauge atomics;
    /// a two-field CSV (older publisher) leaves the gate untouched;
    /// malformed and absent values touch nothing.
    #[tokio::test]
    async fn sync_from_redis_applies_published_routing_weights() {
        let Some((mut conn, fred)) = redis_test_conn(3).await else {
            return;
        };
        let state = state_with_redis(
            vec![
                make_endpoint("w-full", Protocol::Anthropic),
                make_endpoint("w-nogate", Protocol::Anthropic),
                make_endpoint("w-bad", Protocol::Anthropic),
                make_endpoint("w-absent", Protocol::Anthropic),
            ],
            fred,
        );
        for ep in &state.endpoints {
            ep.last_routing_weight
                .store(7.0f64.to_bits(), Ordering::Relaxed);
            ep.last_routing_share
                .store(7.0f64.to_bits(), Ordering::Relaxed);
            ep.last_effective_gate
                .store(7.0f64.to_bits(), Ordering::Relaxed);
        }
        let _: () = conn.set("alb:weight:w-full", "0.5,0.25,0.9").await.unwrap();
        let _: () = conn.set("alb:weight:w-nogate", "0.5,0.25").await.unwrap();
        let _: () = conn
            .set("alb:weight:w-bad", "not,numbers,here")
            .await
            .unwrap();

        state.sync_from_redis().await;

        let read = |a: &AtomicU64| f64::from_bits(a.load(Ordering::Relaxed));
        assert_eq!(read(&state.endpoints[0].last_routing_weight), 0.5);
        assert_eq!(read(&state.endpoints[0].last_routing_share), 0.25);
        assert_eq!(read(&state.endpoints[0].last_effective_gate), 0.9);

        assert_eq!(read(&state.endpoints[1].last_routing_weight), 0.5);
        assert_eq!(
            read(&state.endpoints[1].last_effective_gate),
            7.0,
            "two-field CSV (older publisher) must leave the gate untouched"
        );

        for idx in [2, 3] {
            assert_eq!(
                read(&state.endpoints[idx].last_routing_weight),
                7.0,
                "malformed/absent weight value must touch nothing (endpoint {idx})"
            );
        }
    }

    /// AC3: the Lua CAS in signal_hard_limit_recovery. Contract: write the
    /// clear sentinel when the key is absent or holds an expired epoch;
    /// never clobber a live (future-epoch) hard limit that a concurrent
    /// mark_hard_limited already wrote. The read side of the sentinel is
    /// covered by the pure `classify_hard_limit_*` tests.
    #[tokio::test]
    async fn recovery_sentinel_cas_clears_stale_but_not_live_hard_limits() {
        let Some((mut conn, fred)) = redis_test_conn(4).await else {
            return;
        };
        let state = state_with_redis(vec![], fred);
        let key = "alb:hard:cas-ep";

        // Absent key → sentinel written, with the sentinel TTL.
        state.signal_hard_limit_recovery("cas-ep").await;
        eventually("sentinel write on absent key", || {
            let mut c = conn.clone();
            async move {
                c.get::<_, Option<u64>>(key).await.unwrap() == Some(HARD_LIMIT_CLEARED_SENTINEL)
            }
        })
        .await;
        let ttl: i64 = redis::cmd("TTL")
            .arg(key)
            .query_async(&mut conn)
            .await
            .unwrap();
        assert!(
            (HARD_LIMIT_SENTINEL_TTL_SECS as i64 - 5..=HARD_LIMIT_SENTINEL_TTL_SECS as i64)
                .contains(&ttl),
            "sentinel must carry its full TTL (~{HARD_LIMIT_SENTINEL_TTL_SECS}s), got {ttl}"
        );

        // Expired epoch → CAS overwrites with the sentinel.
        let _: () = conn.set(key, AppState::now_epoch() - 5).await.unwrap();
        state.signal_hard_limit_recovery("cas-ep").await;
        eventually("sentinel overwrite of expired epoch", || {
            let mut c = conn.clone();
            async move {
                c.get::<_, Option<u64>>(key).await.unwrap() == Some(HARD_LIMIT_CLEARED_SENTINEL)
            }
        })
        .await;

        // Live future epoch (a concurrent mark_hard_limited won the race) →
        // the CAS must refuse, preserving the newer hard limit.
        let live = AppState::now_epoch() + 300;
        let _: () = conn.set(key, live).await.unwrap();
        state.signal_hard_limit_recovery("cas-ep").await;
        // The write is fire-and-forget; give the spawned task time to land
        // before asserting nothing changed.
        tokio::time::sleep(Duration::from_millis(500)).await;
        assert_eq!(
            conn.get::<_, Option<u64>>(key).await.unwrap(),
            Some(live),
            "CAS must not clobber a live hard limit written concurrently"
        );
    }

    /// AC4: INCRBY accumulates across replicas, EXPIRE is set, the shared
    /// counter enforces budgets cluster-wide, and per-day keys make
    /// yesterday's spend invisible today.
    #[tokio::test]
    async fn budget_incrby_accumulates_across_replicas_with_expiry() {
        let Some((mut conn, fred)) = redis_test_conn(5).await else {
            return;
        };
        avoid_utc_midnight().await;
        let budgets: HashMap<String, u64> = [("budget-cli".to_string(), 1000u64)].into();
        let replica_a = Arc::new(AppState {
            client_budgets: budgets.clone(),
            redis: Some(fred.clone()),
            ..test_state_base()
        });
        let replica_b = Arc::new(AppState {
            client_budgets: budgets,
            redis: Some(fred.clone()),
            ..test_state_base()
        });

        replica_a.record_budget_usage("budget-cli", 100).await;
        replica_b.record_budget_usage("budget-cli", 250).await;

        let today = AppState::now_epoch() / 86400;
        let key = format!("alb:budget:budget-cli:{today}");
        assert_eq!(
            conn.get::<_, Option<u64>>(&key).await.unwrap(),
            Some(350),
            "INCRBY must accumulate across replicas"
        );
        let ttl: i64 = redis::cmd("TTL")
            .arg(&key)
            .query_async(&mut conn)
            .await
            .unwrap();
        assert!(
            (BUDGET_TTL_SECS - 100..=BUDGET_TTL_SECS).contains(&ttl),
            "budget key must carry its full 48h EXPIRE (~{BUDGET_TTL_SECS}s), got {ttl}"
        );

        // Cross-replica enforcement: a replica with a lower limit and NO
        // local usage of its own sees the shared counter and refuses.
        let enforcing = Arc::new(AppState {
            client_budgets: [("budget-cli".to_string(), 300u64)].into(),
            redis: Some(fred.clone()),
            ..test_state_base()
        });
        assert_eq!(
            enforcing.check_budget("budget-cli").await,
            Err(0),
            "shared counter (350) must gate a 300 limit on a replica that recorded nothing"
        );
        assert!(
            replica_a.check_budget("budget-cli").await.is_ok(),
            "350 used of 1000 must pass"
        );

        // Day rollover: yesterday's counter lives under a different key and
        // must not gate today (complements budget_day_rollover_resets_counter).
        let _: () = conn
            .set(format!("alb:budget:roll-cli:{}", today - 1), 999_999u64)
            .await
            .unwrap();
        let roll = Arc::new(AppState {
            client_budgets: [("roll-cli".to_string(), 100u64)].into(),
            redis: Some(fred.clone()),
            ..test_state_base()
        });
        assert!(
            roll.check_budget("roll-cli").await.is_ok(),
            "yesterday's counter must not gate today"
        );
    }

    /// AC4 failure path: INCRBY against a poisoned (non-integer) key fails;
    /// record_budget_usage must DELETE the key — unblocking future INCRBYs —
    /// while local state keeps enforcing for as long as the redis read path
    /// errors.
    #[tokio::test]
    async fn budget_incrby_failure_deletes_key_and_local_fallback_enforces() {
        let Some((mut conn, fred)) = redis_test_conn(6).await else {
            return;
        };
        avoid_utc_midnight().await;
        let state = Arc::new(AppState {
            client_budgets: [("poison-cli".to_string(), 100u64)].into(),
            redis: Some(fred),
            ..test_state_base()
        });
        let today = AppState::now_epoch() / 86400;
        let key = format!("alb:budget:poison-cli:{today}");
        let _: () = conn.set(&key, "not-a-number").await.unwrap();

        // While the redis value is unreadable, GET errors at the type layer
        // and check_budget falls back to local state — which enforces.
        state
            .budget_usage
            .lock()
            .unwrap()
            .insert("poison-cli".to_string(), (today, 150));
        assert_eq!(
            state.check_budget("poison-cli").await,
            Err(0),
            "local fallback must enforce while the redis value is unreadable"
        );

        // INCRBY fails on the poisoned key → key deleted, local still updated.
        state.record_budget_usage("poison-cli", 10).await;
        assert!(
            !conn.exists::<_, bool>(&key).await.unwrap(),
            "failed INCRBY must delete the poisoned key"
        );
        assert_eq!(
            state
                .budget_usage
                .lock()
                .unwrap()
                .get("poison-cli")
                .unwrap()
                .1,
            160,
            "local accumulator must survive the redis failure"
        );

        // With the poison gone, the next INCRBY starts clean.
        state.record_budget_usage("poison-cli", 5).await;
        assert_eq!(conn.get::<_, Option<u64>>(&key).await.unwrap(), Some(5));

        // Current contract, pinned deliberately: with redis reachable and the
        // key rebuilt (5 < 100), the shared counter is authoritative and
        // check_budget passes even though local memory says 165. The local
        // count only gates when the redis read path errors.
        assert!(
            state.check_budget("poison-cli").await.is_ok(),
            "deliberate pin: a reachable redis counter (5) is authoritative over larger local state (165)"
        );
    }

    /// AC5: cluster_info's SCAN loop across multiple cursor pages. 500
    /// heartbeat keys against COUNT 100 forces several SCAN round-trips on a
    /// real backend — a pagination bug (e.g. stopping after the first page)
    /// undercounts. Budget MGET aggregation is asserted in the same pass.
    #[tokio::test]
    async fn cluster_info_counts_heartbeats_across_multiple_scan_pages() {
        let Some((mut conn, fred)) = redis_test_conn(7).await else {
            return;
        };
        avoid_utc_midnight().await;
        let mut pipe = redis::pipe();
        for i in 0..500 {
            pipe.cmd("SET")
                .arg(format!("alb:heartbeat:{i}"))
                .arg(1u8)
                .ignore();
        }
        let _: () = pipe.query_async(&mut conn).await.unwrap();

        let today = AppState::now_epoch() / 86400;
        let _: () = conn
            .set(format!("alb:budget:scan-cli:{today}"), 42u64)
            .await
            .unwrap();

        let state = Arc::new(AppState {
            client_budgets: [("scan-cli".to_string(), 1000u64)].into(),
            redis: Some(fred),
            ..test_state_base()
        });
        let info = state.cluster_info().await.expect("cluster_info with redis");
        assert_eq!(
            info["replicas_seen"], 500,
            "SCAN must count all heartbeat keys across cursor pages"
        );
        assert_eq!(info["redis_connected"], true);
        assert_eq!(info["budget_usage"]["scan-cli"]["used"], 42);
        assert_eq!(info["budget_usage"]["scan-cli"]["limit"], 1000);
    }

    /// AC5: pipelined HINCRBY folds deltas from multiple replicas into the
    /// shared hash, drains the local accumulators, and both the write and
    /// the idle tick refresh the TTL.
    #[tokio::test]
    async fn flush_transport_errors_hincrby_accumulates_across_replicas() {
        let Some((mut conn, fred)) = redis_test_conn(8).await else {
            return;
        };
        let replica_a = state_with_redis(vec![], fred.clone());
        let replica_b = state_with_redis(vec![], fred.clone());
        {
            let mut m = replica_a.lock_transport_errors();
            m.insert("connect", 3);
        }
        {
            let mut m = replica_b.lock_transport_errors();
            m.insert("connect", 2);
            m.insert("timeout", 5);
        }
        replica_a.flush_transport_errors().await;
        replica_b.flush_transport_errors().await;

        let map: HashMap<String, u64> = conn.hgetall(TRANSPORT_ERRORS_KEY).await.unwrap();
        assert_eq!(
            map.get("connect"),
            Some(&5),
            "HINCRBY must fold deltas from both replicas"
        );
        assert_eq!(map.get("timeout"), Some(&5));
        assert!(
            replica_a.lock_transport_errors().is_empty(),
            "flush must drain the local accumulator"
        );
        let ttl: i64 = redis::cmd("TTL")
            .arg(TRANSPORT_ERRORS_KEY)
            .query_async(&mut conn)
            .await
            .unwrap();
        assert!(
            (TRANSPORT_ERRORS_TTL_SECS as i64 - 100..=TRANSPORT_ERRORS_TTL_SECS as i64)
                .contains(&ttl),
            "flush must set the full hash TTL (~{TRANSPORT_ERRORS_TTL_SECS}s), got {ttl}"
        );

        // An idle tick (no deltas) must still refresh the TTL so the
        // fleet-wide hash never expires under healthy traffic.
        let _: bool = conn.persist(TRANSPORT_ERRORS_KEY).await.unwrap();
        replica_a.flush_transport_errors().await;
        let ttl: i64 = redis::cmd("TTL")
            .arg(TRANSPORT_ERRORS_KEY)
            .query_async(&mut conn)
            .await
            .unwrap();
        assert!(
            (TRANSPORT_ERRORS_TTL_SECS as i64 - 100..=TRANSPORT_ERRORS_TTL_SECS as i64)
                .contains(&ttl),
            "idle flush must refresh the full TTL (~{TRANSPORT_ERRORS_TTL_SECS}s), got {ttl}"
        );
    }

    /// AC5: HINCRBY failure re-queues the drained deltas locally
    /// (documented at-least-once behaviour) instead of dropping the error
    /// signal.
    #[tokio::test]
    async fn flush_transport_errors_requeues_deltas_when_redis_dies() {
        let Some((fred, kill)) = proxied_conn(9).await else {
            return;
        };
        let state = state_with_redis(vec![], fred);
        {
            let mut m = state.lock_transport_errors();
            m.insert("reset", 4);
        }
        kill_proxy(kill).await;
        state.flush_transport_errors().await;
        assert_eq!(
            state.lock_transport_errors().get("reset"),
            Some(&4),
            "failed flush must re-queue drained deltas for the next tick"
        );
    }

    /// AC6: the SET NX EX probe lock grants one replica per endpoint+model
    /// per interval; a second replica's probe is suppressed while the lock
    /// is held, and a different model probes under its own lock.
    #[tokio::test]
    async fn probe_lock_grants_one_replica_per_endpoint_model() {
        let Some((mut conn, fred)) = redis_test_conn(10).await else {
            return;
        };
        let (mock_url, hits) = spawn_counting_upstream().await;
        let dir = tempfile::tempdir().unwrap();

        let mk_replica = |file: &str, client: RedisClient| {
            Arc::new(AppState {
                endpoints: vec![mk_endpoint_at("probe-ep", "sk-test", &mock_url)],
                redis: Some(client),
                state_path: dir.path().join(file),
                ..test_state_base()
            })
        };
        let replica_a = mk_replica("a.json", fred.clone());
        let replica_b = mk_replica("b.json", fred.clone());

        replica_a.probe_endpoint(0, "claude-sonnet-4-5").await;
        assert_eq!(
            hits.load(Ordering::SeqCst),
            1,
            "first replica must win the lock and probe"
        );

        replica_b.probe_endpoint(0, "claude-sonnet-4-5").await;
        assert_eq!(
            hits.load(Ordering::SeqCst),
            1,
            "second replica must be suppressed by the held lock"
        );
        assert!(
            conn.exists::<_, bool>("alb:probe:probe-ep:claude-sonnet-4-5")
                .await
                .unwrap(),
            "probe lock key must exist while held"
        );

        replica_b.probe_endpoint(0, "claude-opus-4-6").await;
        assert_eq!(
            hits.load(Ordering::SeqCst),
            2,
            "a different model is a different lock and must probe"
        );
    }

    /// AC6: fail-open contract — with Redis down the lock SET errors and the
    /// probe proceeds anyway (a dead coordinator must not stop probing).
    #[tokio::test]
    async fn probe_lock_fails_open_when_redis_is_down() {
        let Some((fred, kill)) = proxied_conn(11).await else {
            return;
        };
        let (mock_url, hits) = spawn_counting_upstream().await;
        let dir = tempfile::tempdir().unwrap();
        let state = Arc::new(AppState {
            endpoints: vec![mk_endpoint_at("failopen-ep", "sk-test", &mock_url)],
            redis: Some(fred),
            state_path: dir.path().join("s.json"),
            ..test_state_base()
        });
        kill_proxy(kill).await;
        // Prove the backend is actually dead before probing — otherwise a
        // silently-regressed kill_proxy would make hits==1 pass via the
        // lock-acquired path instead of the fail-open path.
        let dead = state.redis.clone().unwrap();
        let ping: Result<String, fred::error::RedisError> = dead.ping().await;
        assert!(ping.is_err(), "proxy kill must sever the redis connection");
        state.probe_endpoint(0, "claude-sonnet-4-5").await;
        assert_eq!(
            hits.load(Ordering::SeqCst),
            1,
            "redis down must fail open: probe anyway"
        );
    }

    /// AC7: killing the backend mid-run degrades every coordination path to
    /// local-only — no panics, budgets enforced from local state, cluster
    /// info reports the outage. The `*_without_redis` tests cover the
    /// cold-start absence case; this covers loss of an established backend.
    #[tokio::test]
    async fn backend_death_mid_run_degrades_to_local_only() {
        let Some((fred, kill)) = proxied_conn(12).await else {
            return;
        };
        avoid_utc_midnight().await;
        let state = Arc::new(AppState {
            endpoints: vec![make_endpoint("degrade-ep", Protocol::Anthropic)],
            client_budgets: [("degrade-cli".to_string(), 100u64)].into(),
            redis: Some(fred),
            ..test_state_base()
        });

        // Healthy first: the shared counter works through the proxy.
        state.record_budget_usage("degrade-cli", 50).await;
        assert!(state.check_budget("degrade-cli").await.is_ok());

        kill_proxy(kill).await;

        // Budget: INCRBY and the follow-up DEL both fail; the local
        // accumulator still advances (50+60=110) and check_budget falls back
        // to it, refusing over-limit spend with redis dead.
        state.record_budget_usage("degrade-cli", 60).await;
        assert_eq!(
            state.check_budget("degrade-cli").await,
            Err(0),
            "local fallback must enforce the budget with redis dead"
        );

        // The periodic sync tick and the publish wrappers must return
        // without hanging. (Their redis writes are fire-and-forget
        // tokio::spawn tasks whose failures are swallowed by design — this
        // asserts the synchronous paths, not the spawned writes.)
        state.sync_from_redis().await;
        state.publish_routing_weights().await;
        state.signal_hard_limit_recovery("degrade-ep").await;

        let info = state
            .cluster_info()
            .await
            .expect("cluster_info must still report with redis dead");
        assert_eq!(
            info["redis_connected"], false,
            "cluster_info must surface the outage"
        );
    }

    /// AC5 (LAB-932) — the one deliberate behaviour change of the fred
    /// migration: after a backend outage degrades coordination to local-only,
    /// the backend coming BACK must restore cross-replica coordination
    /// without a process restart. The sibling test above proves graceful
    /// degradation; this proves recovery. Under the old `redis`-crate client
    /// the second half of this test would hang degraded forever.
    #[tokio::test]
    async fn backend_recovery_mid_run_resumes_coordination() {
        let Some(base) = test_redis_url() else {
            return;
        };
        avoid_utc_midnight().await;
        let target = base
            .trim_start_matches("redis://")
            .trim_end_matches('/')
            .split('/')
            .next()
            .unwrap()
            .to_string();
        let (proxy_addr, kill) = spawn_killable_proxy(target.clone()).await;
        let url = format!("redis://{proxy_addr}/13");
        drop(connect_and_flush(&url).await);
        let fred = fred_test_client(&url).await;
        // Independent assertion client, connected DIRECTLY to the backend
        // (not through the killable proxy, and without flushing) so it can
        // verify post-recovery writes actually landed.
        let direct = connect(&format!("{}/13", base.trim_end_matches('/'))).await;

        let state = Arc::new(AppState {
            client_budgets: [("recover-cli".to_string(), 100u64)].into(),
            redis: Some(fred),
            ..test_state_base()
        });

        // Healthy first: the shared counter works through the proxy.
        state.record_budget_usage("recover-cli", 50).await;
        assert!(state.check_budget("recover-cli").await.is_ok());

        kill_proxy(kill).await;

        // Dead: INCRBY and its follow-up DEL both fail; the local accumulator
        // (50+60=110) enforces the 100 budget.
        state.record_budget_usage("recover-cli", 60).await;
        assert_eq!(
            state.check_budget("recover-cli").await,
            Err(0),
            "local fallback must enforce the budget while the backend is dead"
        );

        // Revive the backend at the SAME address. fred's reconnect policy
        // must re-establish the connection on its own.
        let (revived_addr, _revived_kill) = spawn_killable_proxy_at(&proxy_addr, target).await;
        assert_eq!(
            revived_addr, proxy_addr,
            "proxy must revive at its old address"
        );

        // Coordination resumes: the shared counter (still 50 — the
        // dead-window INCRBY failed, and so did its delete-on-failure)
        // becomes authoritative again, flipping check_budget from the local
        // Err(0) back to Ok.
        eventually("coordination to resume after backend recovery", || {
            let s = state.clone();
            async move { s.check_budget("recover-cli").await.is_ok() }
        })
        .await;

        // And writes flow again, verified through the independent direct
        // connection: a fresh INCRBY lands in the real backend.
        state.record_budget_usage("recover-cli", 7).await;
        let today = AppState::now_epoch() / 86400;
        let key = format!("alb:budget:recover-cli:{today}");
        eventually("post-recovery INCRBY to land in the backend", || {
            let mut c = direct.clone();
            let key = key.clone();
            async move { c.get::<_, Option<u64>>(&key).await.ok().flatten() == Some(57) }
        })
        .await;

        let info = state
            .cluster_info()
            .await
            .expect("cluster_info after recovery");
        assert_eq!(
            info["redis_connected"], true,
            "cluster_info must reflect the recovered backend"
        );
    }

    /// LAB-1639: a process that STARTS while the backend is down must come
    /// up serving local-only immediately — client construction must not
    /// block on the unreachable backend — and must attach automatically when
    /// the backend appears, without a restart. The startup analogue of the
    /// mid-run death/recovery pair above. Under the pre-LAB-1639 contract
    /// (fred's default `fail_fast = true` + a blocking `init()`) the single
    /// refused connect at creation pinned the process local-only for its
    /// entire lifetime.
    #[tokio::test]
    async fn backend_down_at_startup_serves_local_only_then_attaches() {
        let Some(base) = test_redis_url() else {
            return;
        };
        avoid_utc_midnight().await;
        let target = base
            .trim_start_matches("redis://")
            .trim_end_matches('/')
            .split('/')
            .next()
            .unwrap()
            .to_string();
        // Flush DB 14 through a DIRECT connection — the proxy is dead at
        // client creation, so the usual flush-through-proxy path cannot run.
        // The same client later verifies that post-attach writes landed.
        let direct = connect_and_flush(&format!("{}/14", base.trim_end_matches('/'))).await;

        // Reserve an address, then kill it BEFORE the client under test
        // exists: nothing is listening when the connection task makes its
        // first attempt — the exact boot-during-outage scenario.
        let (proxy_addr, kill) = spawn_killable_proxy(target.clone()).await;
        kill_proxy(kill).await;
        let url = format!("redis://{proxy_addr}/14");

        // The PRODUCTION constructor (fail_fast=false, background connect),
        // with the harness's short budgets and fast constant reconnect.
        let constructed = std::time::Instant::now();
        let client = start_coordination_redis(
            &url,
            PerformanceConfig {
                default_command_timeout: Duration::from_secs(1),
                ..Default::default()
            },
            ConnectionConfig {
                connection_timeout: Duration::from_secs(2),
                internal_command_timeout: Duration::from_secs(2),
                ..Default::default()
            },
            ReconnectPolicy::new_constant(0, 100),
        )
        .expect("a well-formed url must always yield a client");
        assert!(
            constructed.elapsed() < Duration::from_millis(500),
            "client construction must not block on the unreachable backend"
        );
        assert!(
            !client.is_connected(),
            "never-yet-connected client must read disconnected"
        );

        let state = Arc::new(AppState {
            client_budgets: [("startup-cli".to_string(), 100u64)].into(),
            redis: Some(client.clone()),
            // Production boots with the gate closed; test_state_base opens
            // it for the connected-at-creation fixtures.
            redis_ever_connected: AtomicBool::new(false),
            ..test_state_base()
        });
        state.spawn_redis_connect_watcher();

        // Never-yet-connected: the coordination gate is closed, so request
        // paths and background ticks return at local speed — no buffered
        // fred command burning its 1s timeout, no per-operation warnings.
        assert!(
            state.coordination_redis().is_none(),
            "coordination gate must be closed before the first connect"
        );
        let ops = std::time::Instant::now();
        assert!(state.check_budget("startup-cli").await.is_ok());
        state.record_budget_usage("startup-cli", 50).await;
        state.sync_from_redis().await;
        state.publish_routing_weights().await;
        assert!(
            state.cluster_info().await.is_none(),
            "cluster_info must skip (not stall) while never-yet-connected"
        );
        assert!(
            ops.elapsed() < Duration::from_millis(500),
            "local-only ops must not stall on the dead backend"
        );

        // Local budget accounting still enforces (50 + 60 > 100).
        state.record_budget_usage("startup-cli", 60).await;
        assert_eq!(
            state.check_budget("startup-cli").await,
            Err(0),
            "local fallback must enforce the budget while unconnected"
        );

        // Backend appears (proxy revives at the SAME address): the
        // retry-forever connection task must attach on its own.
        let (revived_addr, _revived_kill) = spawn_killable_proxy_at(&proxy_addr, target).await;
        assert_eq!(
            revived_addr, proxy_addr,
            "proxy must revive at its old address"
        );

        eventually("the first connect to open the coordination gate", || {
            let s = state.clone();
            async move { s.coordination_redis().is_some() }
        })
        .await;

        // Coordination writes begin: a fresh INCRBY lands in the real
        // backend, verified through the independent direct connection.
        // 7, not 117 — the pre-attach 50 and 60 were local-only by design.
        state.record_budget_usage("startup-cli", 7).await;
        let today = AppState::now_epoch() / 86400;
        let key = format!("alb:budget:startup-cli:{today}");
        eventually("the post-attach INCRBY to land in the backend", || {
            let mut c = direct.clone();
            let key = key.clone();
            async move { c.get::<_, Option<u64>>(&key).await.ok().flatten() == Some(7) }
        })
        .await;

        // And the operator surface reflects the attach.
        let info = state
            .cluster_info()
            .await
            .expect("cluster_info after attach");
        assert_eq!(
            info["redis_connected"], true,
            "cluster_info must reflect the attached backend"
        );
    }
}

// ── LAB-1192: exposure controls ─────────────────────────────────
//
// Four mechanisms, one posture: unauthenticated is a boot failure not a
// default (AC-1..3), the admin surfaces answer only to an operator
// principal (AC-4..6), behind a trusted load balancer the proxy resolves
// the real client IP or refuses to guess (AC-7..9), and credential
// guessing is throttled per client IP with bounded state (AC-11..12).

// ── AC-3: startup posture ──

/// 64-hex — what `openssl rand -hex 32` emits, and the documented floor.
const STRONG_KEY: &str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

#[test]
fn exposure_rejects_a_credential_free_config_by_default() {
    let err = validate_exposure(&cfg("")).unwrap_err();
    assert!(
        err.contains("allow_unauthenticated"),
        "error must name the escape hatch: {err}"
    );
    assert!(
        err.contains("[[clients]]"),
        "error must name the credential fix: {err}"
    );
}

#[test]
fn exposure_accepts_the_explicit_escape_hatch() {
    assert!(validate_exposure(&cfg("allow_unauthenticated = true\n")).is_ok());
    // `false` is NOT an escape hatch — omission and explicit false are the
    // same default-deny.
    assert!(validate_exposure(&cfg("allow_unauthenticated = false\n")).is_err());
}

#[test]
fn exposure_accepts_either_credential_form() {
    assert!(validate_exposure(&cfg(&format!("proxy_key = \"{STRONG_KEY}\"\n"))).is_ok());
    assert!(validate_exposure(&cfg(&format!(
        "[[clients]]\nname = \"geo\"\nkey = \"{STRONG_KEY}\"\n"
    )))
    .is_ok());
}

#[test]
fn exposure_rejects_the_flag_alongside_credentials() {
    let err = validate_exposure(&cfg(&format!(
        "allow_unauthenticated = true\nproxy_key = \"{STRONG_KEY}\"\n"
    )))
    .unwrap_err();
    assert!(err.contains("incompatible"), "{err}");
    let err = validate_exposure(&cfg(&format!(
        "allow_unauthenticated = true\n[[clients]]\nname = \"geo\"\nkey = \"{STRONG_KEY}\"\n"
    )))
    .unwrap_err();
    assert!(err.contains("incompatible"), "{err}");
}

/// AC-13: every configured credential form is held to the 32-char floor, and
/// the error carries the generation command.
#[test]
fn exposure_rejects_short_credentials_with_the_generation_command() {
    for fragment in [
        "proxy_key = \"short\"\n".to_string(),
        "[[clients]]\nname = \"geo\"\nkey = \"short\"\n".to_string(),
        // 31 chars — one under the floor, so the boundary is pinned.
        format!("proxy_key = \"{}\"\n", "a".repeat(31)),
    ] {
        let err = validate_exposure(&cfg(&fragment)).unwrap_err();
        assert!(
            err.contains("openssl rand -hex 32"),
            "error must carry the generation command: {err}"
        );
    }
    // 32 exactly passes.
    assert!(validate_exposure(&cfg(&format!("proxy_key = \"{}\"\n", "a".repeat(32)))).is_ok());
}

#[test]
fn exposure_rejects_a_zero_window_with_the_throttle_enabled() {
    let err = validate_exposure(&cfg(&format!(
        "proxy_key = \"{STRONG_KEY}\"\nauth_failure_window_secs = 0\n"
    )))
    .unwrap_err();
    assert!(err.contains("auth_failure_window_secs"), "{err}");
    // Explicitly disabled throttle: a zero window is fine because it is never read.
    assert!(validate_exposure(&cfg(&format!(
        "proxy_key = \"{STRONG_KEY}\"\nauth_failure_limit = 0\nauth_failure_window_secs = 0\n"
    )))
    .is_ok());
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

/// A successful authentication clears the IP's failure state, so a client's
/// own sporadic typos never drift toward a lockout (bug-hunter/security
/// finding: shared-IP false lockout mitigation).
#[test]
fn auth_throttle_clear_resets_failures() {
    let t = AuthThrottle::new(3, Duration::from_secs(60));
    let ip: IpAddr = "203.0.113.7".parse().unwrap();
    t.record_failure(ip);
    t.record_failure(ip);
    t.clear(&ip);
    // Back to zero: two fresh failures still under the limit.
    t.record_failure(ip);
    t.record_failure(ip);
    assert_eq!(t.check(&ip), None, "clear must reset the counter to zero");
}

/// The throttle fires BEFORE the key comparison: once tripped, even the
/// CORRECT credential gets 429 until the window expires — the guessing
/// surface (including its timing) closes entirely. Also pins the counter.
#[tokio::test]
async fn throttled_ip_gets_429_even_with_a_valid_key() {
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

    // Valid credential, throttled IP: 429 with retry-after, before comparison.
    let resp = client
        .post(format!("http://{addr}/v1/messages"))
        .header("x-api-key", "key-geo")
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

    // Counter = 3 rejected credentials + the 1 throttle-429 = 4. The 429 path
    // is counted too, so the metric keeps climbing through a sustained attack
    // instead of plateauing at the limit.
    assert_eq!(state.auth_failures.lock().unwrap().get("proxy"), Some(&4));
}

/// Reset-on-success through the real router: a client that fails a couple of
/// times, then authenticates successfully, is NOT throttled by those earlier
/// failures — its state was cleared. Mitigates shared-IP false lockouts.
#[tokio::test]
async fn successful_auth_clears_prior_failures() {
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
    // A successful auth clears the two failures.
    let resp = client
        .post(format!("http://{addr}/v1/messages"))
        .header("x-api-key", "key-geo")
        .body(r#"{"model":"claude-haiku-4-5","max_tokens":1,"messages":[{"role":"user","content":"hi"}]}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    assert!(
        state
            .auth_throttle
            .check(&"127.0.0.1".parse().unwrap())
            .is_none(),
        "the client's own IP must not be throttled after a success reset it"
    );
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

/// `anthropic_auth_failures_total{route}` is scrape-visible (AC-11).
#[tokio::test]
async fn metrics_expose_auth_failures_by_route() {
    let (mock_url, _handle) = spawn_mock_upstream().await;
    let (app, _state) = admin_matrix_app(&mock_url);
    let addr = serve(app).await;
    let client = Client::new();

    // One failure on the stats route.
    let _ = client
        .get(format!("http://{addr}/_stats"))
        .header("x-api-key", "key-wrong")
        .send()
        .await
        .unwrap();

    let body = client
        .get(format!("http://{addr}/metrics"))
        .header("x-api-key", "key-ops")
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(
        body.contains("anthropic_auth_failures_total{route=\"stats\"} 1"),
        "missing auth-failure counter in:\n{body}"
    );
}
