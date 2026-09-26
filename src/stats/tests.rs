use super::*;

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
