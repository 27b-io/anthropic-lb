use super::*;

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
        body.contains("anthropic_lb_info{strategy=\"dynamic-capacity-v1\",version=\""),
        "missing lb_info:\n{body}"
    );

    // HELP/TYPE headers present
    assert!(
        body.contains("# TYPE anthropic_account_utilization gauge"),
        "missing TYPE header:\n{body}"
    );
}

/// LAB-4379 AC1: every response the proxy sends lands in the request-duration
/// histogram under a closed `(route, status)` label set, exposed as a declared
/// histogram with cumulative `le` buckets, `_sum` and `_count`.
#[tokio::test]
async fn metrics_request_duration_histogram() {
    let (mock_url, _handle) = spawn_mock_upstream().await;
    let (app, _state) = test_app(&mock_url, None);
    let addr = serve(app).await;
    let client = Client::new();
    for _ in 0..2 {
        let resp = client
            .post(format!("http://{addr}/v1/messages"))
            .header("content-type", "application/json")
            .body(r#"{"model":"test","max_tokens":1,"messages":[{"role":"user","content":"hi"}]}"#)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), reqwest::StatusCode::OK);
    }
    let scrape = || async {
        client
            .get(format!("http://{addr}/metrics"))
            .send()
            .await
            .unwrap()
            .text()
            .await
            .unwrap()
    };

    let body = scrape().await;
    assert!(
        body.contains("# TYPE anthropic_http_request_duration_seconds histogram"),
        "histogram must be declared:\n{body}"
    );
    let series = r#"{route="/v1/messages",status="200""#;
    assert!(
        body.contains(&format!(
            "anthropic_http_request_duration_seconds_bucket{series},le=\"+Inf\"}} 2"
        )),
        "+Inf bucket must count every observation:\n{body}"
    );
    assert!(
        body.contains(&format!(
            "anthropic_http_request_duration_seconds_bucket{series},le=\"600\"}} 2"
        )),
        "buckets must be cumulative (nothing took >600s):\n{body}"
    );
    assert!(
        body.contains(&format!(
            "anthropic_http_request_duration_seconds_count{series}}} 2"
        )),
        "_count must match:\n{body}"
    );
    assert!(
        body.lines().any(|l| l.starts_with(&format!(
            "anthropic_http_request_duration_seconds_sum{series}}} "
        ))),
        "_sum must be emitted:\n{body}"
    );

    // The middleware wraps the admin routes too: the first scrape recorded
    // itself, so the second one sees it.
    let body = scrape().await;
    assert!(
        body.contains(
            "anthropic_http_request_duration_seconds_count{route=\"/metrics\",status=\"200\"} 1"
        ),
        "admin scrape must be timed like any other response:\n{body}"
    );
}

/// LAB-4379 AC1: the `route` label is a closed vocabulary — a caller cannot
/// mint series by varying the path the fallback proxies.
#[test]
fn route_label_is_a_closed_vocabulary() {
    assert_eq!(route_label("/v1/messages"), "/v1/messages");
    assert_eq!(
        route_label("/v1/messages/count_tokens"),
        "/v1/messages/count_tokens"
    );
    assert_eq!(route_label("/v1/chat/completions"), "/v1/chat/completions");
    assert_eq!(route_label("/_stats"), "/_stats");
    assert_eq!(route_label("/metrics"), "/metrics");
    for p in [
        "/",
        "/v1/complete",
        "/v1/messages/",
        "/v1/Messages",
        "/anything/the/caller/chooses",
    ] {
        assert_eq!(route_label(p), "other", "{p}");
    }
}

/// Bucket edges are inclusive upper bounds; overflow lands in `+Inf`; the
/// sum is in seconds.
#[test]
fn request_duration_hist_buckets_and_sum() {
    let mut h = RequestDurationHist::default();
    h.observe(Duration::from_millis(50)); // <= 0.1
    h.observe(Duration::from_millis(100)); // == 0.1, edge is inclusive
    h.observe(Duration::from_secs(3)); // <= 5.0
    h.observe(Duration::from_secs(1000)); // > 600 → +Inf
    assert_eq!(h.buckets[0], 2);
    assert_eq!(REQUEST_DURATION_BUCKETS[5], 5.0);
    assert_eq!(h.buckets[5], 1);
    assert_eq!(h.buckets[REQUEST_DURATION_BUCKETS.len()], 1);
    assert_eq!(h.buckets.iter().sum::<u64>(), 4);
    assert!((h.sum_secs - 1003.15).abs() < 1e-9, "{}", h.sum_secs);
}

/// LAB-4379 AC3/AC4: the exposition identifies the running build and its
/// start time directly, without inferring either from counter resets.
#[tokio::test]
async fn metrics_build_info_and_start_time() {
    let (mock_url, _handle) = spawn_mock_upstream().await;
    let (app, state) = test_app(&mock_url, None);
    let addr = serve(app).await;
    let body = Client::new()
        .get(format!("http://{addr}/metrics"))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();

    let info = body
        .lines()
        .find(|l| l.starts_with("anthropic_lb_info{"))
        .expect("lb_info line");
    assert!(
        info.contains(&format!("version=\"{}\"", env!("CARGO_PKG_VERSION"))),
        "{info}"
    );
    assert!(info.contains("revision=\""), "{info}");

    assert!(
        body.contains("# TYPE process_start_time_seconds gauge"),
        "{body}"
    );
    assert!(
        body.contains(&format!("process_start_time_seconds {}", state.start_epoch)),
        "{body}"
    );
}

/// LAB-4379 AC4: the revision label is the 7-character short commit that
/// `sha-*` image tags use, or `unknown` when the build did not set one.
#[test]
fn build_revision_is_short_sha_or_unknown() {
    assert_eq!(
        build_revision(Some("9c0d22b9aa2d8027cde27f1be250da48118a3e33")),
        "9c0d22b"
    );
    assert_eq!(build_revision(Some("9c0d22b")), "9c0d22b");
    assert_eq!(build_revision(Some("abc")), "abc");
    assert_eq!(build_revision(Some("")), "unknown");
    assert_eq!(build_revision(None), "unknown");
}

/// LAB-4379 AC2: whenever the Redis aggregate is not being reported, the
/// scrape carries this replica's own count and no
/// `anthropic_cluster_redis_connected 1`, which is the gauge the HELP text tells
/// a reader to split `max` from `sum` on. Covers both fallback shapes of the
/// cluster cache: absent (no Redis, or before the first sync) and a failed
/// Redis read (`redis_connected: false`, no `transport_errors`).
/// `metrics_prefer_redis_transport_error_aggregate` covers the connected case.
#[tokio::test]
async fn metrics_transport_errors_scope_without_redis_aggregate() {
    let failed_read = serde_json::json!({
        "redis_connected": false,
        "replicas_seen": 0,
        "budget_usage": {},
    });
    for cache in [None, Some(failed_read)] {
        let label = format!("{cache:?}");
        let (mock_url, _handle) = spawn_mock_upstream().await;
        let (app, state) = test_app(&mock_url, None);
        state.lock_transport_errors().insert("connect", 2);
        *state.lock_cluster_info_cache() = cache;
        let addr = serve(app).await;
        let body = Client::new()
            .get(format!("http://{addr}/metrics"))
            .send()
            .await
            .unwrap()
            .text()
            .await
            .unwrap();

        assert!(
            body.contains("anthropic_upstream_transport_errors_total{kind=\"connect\"} 2"),
            "{label}: local count must be reported:\n{body}"
        );
        assert!(
            !body.contains("anthropic_cluster_redis_connected 1"),
            "{label}: must not advertise the fleet scope:\n{body}"
        );
        let help = body
            .lines()
            .find(|l| l.starts_with("# HELP anthropic_upstream_transport_errors_total "))
            .expect("HELP line");
        assert!(
            help.contains("Where anthropic_cluster_redis_connected is 1"),
            "{help}"
        );
        assert!(help.contains("(aggregate with max)"), "{help}");
        assert!(help.contains("(aggregate with sum)"), "{help}");
    }
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
        let mut usage = state.client_model_usage.lock().unwrap();
        usage.insert(
            ("op-alice".to_string(), "claude-sonnet-5".to_string()),
            [100, 200, 0, 0],
        );
        usage.insert(
            ("op-bob".to_string(), "claude-sonnet-5".to_string()),
            [50, 100, 0, 0],
        );
        usage.insert(
            ("user-charlie".to_string(), "claude-haiku-4-5".to_string()),
            [10, 20, 0, 0],
        );
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

    // LAB-2330: per-model family aggregates operators the same way
    assert!(
        body.contains(
            "anthropic_client_model_token_usage_total{client=\"_operator\",model=\"claude-sonnet-5\",type=\"input\"} 150"
        ),
        "operator per-model input tokens should sum to 150:\n{body}"
    );
    assert!(
        body.contains(
            "anthropic_client_model_token_usage_total{client=\"user-charlie\",model=\"claude-haiku-4-5\",type=\"output\"} 20"
        ),
        "regular client per-model tokens should be emitted:\n{body}"
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

/// LAB-4441 AC-1/AC-3: an account serving via overage with its 7d claim
/// rejected publishes the overage gate (not 1.0), a non-zero routing weight,
/// and the overage window's status and reset.
#[tokio::test]
async fn metrics_overage_account_gate_status_and_reset() {
    let (mock_url, _handle) = spawn_mock_upstream().await;
    let (app, state) = test_app(&mock_url, None);
    let now_epoch = AppState::now_epoch();

    {
        let mut info = state.endpoints[0].rate_info.write().await;
        info.utilization_5h = Some(1.0);
        info.reset_5h = Some(now_epoch + 7200);
        info.status_5h = Some("rejected".to_string());
        info.claims_7d.insert(
            "seven_day".to_string(),
            ClaimWindowData {
                utilization: Some(1.0),
                reset: Some(now_epoch + 302400),
                status: Some("rejected".to_string()),
                ..Default::default()
            },
        );
        info.overage_in_use = true;
        info.overage_status = Some("allowed_warning".to_string());
        info.overage_utilization = Some(0.30);
        info.overage_reset = Some(now_epoch + 86_400);
    }
    state.refresh_metrics_weights().await;

    let addr = serve(app).await;
    let body = Client::new()
        .get(format!("http://{}/metrics", addr))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    let value = |prefix: &str| -> f64 {
        body.lines()
            .find(|l| l.starts_with(prefix))
            .unwrap_or_else(|| panic!("missing {prefix}:\n{body}"))
            .split_whitespace()
            .last()
            .unwrap()
            .parse()
            .unwrap()
    };

    // allowed_warning floor (0.80) over raw overage util 0.30.
    let gate = value("anthropic_account_effective_gate{account=\"acct-a\"}");
    assert_eq!(
        gate, WARNING_UTIL_FLOOR,
        "overage gate, not the rejected 1.0"
    );
    assert!(value("anthropic_account_routing_weight{account=\"acct-a\"}") > 0.0);
    assert_eq!(
        value("anthropic_account_rate_limit_status{account=\"acct-a\",window=\"overage\"}"),
        1.0
    );
    let reset = value("anthropic_account_reset_seconds{account=\"acct-a\",window=\"overage\"}");
    assert!(
        (86_340.0..=86_400.0).contains(&reset),
        "overage reset ~86400, got {reset}"
    );
    // Not in overage → no overage series at all.
    assert!(
        !body.contains("account=\"acct-b\",window=\"overage\""),
        "acct-b must emit no overage series:\n{body}"
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

// ── LAB-4189: direct Fable band visibility ───────────────────────────
//
// Three series make a pool-exhaustion event readable from a scrape alone, rather
// than only from `/_stats` and logs: per-claim status, per-claim reset, and a
// counter for the 429 the caller actually got.

/// AC-1/AC-2: a model carve-out claim gets its own status ordinal and its own
/// reset countdown, labelled by `claim` — neither of which the `window`-labelled
/// account series can express.
#[tokio::test]
async fn metrics_exports_per_claim_status_and_reset() {
    let (mock_url, _handle) = spawn_mock_upstream().await;
    let (app, state) = test_app(&mock_url, None);

    let now_epoch = AppState::now_epoch();
    {
        let mut info = state.endpoints[0].rate_info.write().await;
        // The band is exhausted: utilization 1.0, hard-rejected, resets in 2 days.
        info.claims_7d.insert(
            FABLE_BAND_CLAIM.to_string(),
            ClaimWindowData {
                utilization: Some(1.0),
                reset: Some(now_epoch + 172_800),
                status: Some("rejected".to_string()),
                last_seen: now_epoch,
            },
        );
        // The general claim is healthy at the same instant — this is the
        // divergence a utilization threshold cannot see.
        info.claims_7d.insert(
            "seven_day".to_string(),
            ClaimWindowData {
                utilization: Some(0.40),
                reset: Some(now_epoch + 302_400),
                status: Some("allowed".to_string()),
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
        body.contains("# TYPE anthropic_claim_rate_limit_status gauge"),
        "missing claim status TYPE line:\n{body}"
    );
    assert!(
        body.contains(
            "anthropic_claim_rate_limit_status{account=\"acct-a\",claim=\"seven_day_fable\"} 3"
        ),
        "band claim should report ordinal 3 (rejected):\n{body}"
    );
    assert!(
        body.contains(
            "anthropic_claim_rate_limit_status{account=\"acct-a\",claim=\"seven_day\"} 0"
        ),
        "general claim should report ordinal 0 (allowed) at the same scrape:\n{body}"
    );

    let reset_line = body
        .lines()
        .find(|l| {
            l.starts_with("anthropic_claim_reset_seconds{")
                && l.contains("claim=\"seven_day_fable\"")
        })
        .unwrap_or_else(|| panic!("missing claim reset for the band:\n{body}"));
    let secs: f64 = reset_line.rsplit(' ').next().unwrap().parse().unwrap();
    assert!(
        (secs - 172_800.0).abs() < 5.0,
        "band reset should count down ~172800s, got {secs}"
    );
}

/// AC-2 boundary: an expired reset is omitted, not clamped to zero — mirroring
/// `anthropic_account_reset_seconds`. A stale timestamp means "unknown", and
/// emitting 0 would read as "resets now" on the reset-ordered panel.
#[tokio::test]
async fn metrics_omits_claim_reset_already_in_the_past() {
    let (mock_url, _handle) = spawn_mock_upstream().await;
    let (app, state) = test_app(&mock_url, None);

    let now_epoch = AppState::now_epoch();
    {
        let mut info = state.endpoints[0].rate_info.write().await;
        info.claims_7d.insert(
            FABLE_BAND_CLAIM.to_string(),
            ClaimWindowData {
                utilization: Some(1.0),
                reset: Some(now_epoch - 60),
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
        !body.contains("anthropic_claim_reset_seconds{account=\"acct-a\""),
        "a past-dated reset must not emit a sample:\n{body}"
    );
    // Status still reports — the claim is known-rejected even with a stale reset.
    assert!(
        body.contains(
            "anthropic_claim_rate_limit_status{account=\"acct-a\",claim=\"seven_day_fable\"} 3"
        ),
        "status must survive a stale reset:\n{body}"
    );
}

/// AC-3: both `exhaustion_response` arms increment, under their own `kind`, and
/// both series exist at zero before any exhaustion so a rate() panel has a
/// baseline instead of "No data".
#[tokio::test]
async fn metrics_counts_client_facing_pool_exhaustion() {
    let (mock_url, _handle) = spawn_mock_upstream().await;
    let (app, state) = test_app(&mock_url, None);
    let addr = serve(app).await;
    let scrape = |addr| async move {
        Client::new()
            .get(format!("http://{}/metrics", addr))
            .send()
            .await
            .unwrap()
            .text()
            .await
            .unwrap()
    };

    let before = scrape(addr).await;
    assert!(
        before.contains("anthropic_pool_exhausted_total{kind=\"rate_limited\"} 0")
            && before.contains("anthropic_pool_exhausted_total{kind=\"transient\"} 0"),
        "both kinds must be emitted at zero before any exhaustion:\n{before}"
    );

    // The incident shape: every endpoint gated, nothing transient → 429.
    let resp = exhaustion_response(&state, false, false);
    assert_eq!(resp.status(), StatusCode::TOO_MANY_REQUESTS);
    // A 529 round counts as rate-limited too — same 429 to the caller.
    let _ = exhaustion_response(&state, true, true);
    // Transport-only exhaustion is the other arm → retryable 503.
    let resp = exhaustion_response(&state, true, false);
    assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);

    let after = scrape(addr).await;
    assert!(
        after.contains("anthropic_pool_exhausted_total{kind=\"rate_limited\"} 2"),
        "rate-limit exhaustion (incl. the 529 round) should count 2:\n{after}"
    );
    assert!(
        after.contains("anthropic_pool_exhausted_total{kind=\"transient\"} 1"),
        "transient exhaustion should count 1:\n{after}"
    );
}
