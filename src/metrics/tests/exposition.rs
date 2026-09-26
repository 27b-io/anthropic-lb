use super::*;

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
