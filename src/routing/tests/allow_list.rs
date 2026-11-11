use super::*;

// ── allowed_endpoints: a hard per-client allow-list ─────────────────

/// A `[[clients]]` entry restricted to `allowed`, pinned to `preferred`.
fn mk_allowed_client(name: &str, allowed: &[&str], preferred: &[&str]) -> ClientConfig {
    ClientConfig {
        allowed_endpoints: allowed.iter().map(|s| s.to_string()).collect(),
        preferred_endpoints: preferred.iter().map(|s| s.to_string()).collect(),
        ..mk_client(name, &test_key(name), &[])
    }
}

/// The pool every unit test below routes over: `general` (index 0) outside
/// the client's allow-list, `own-a` (1) and `own-b` (2) inside it.
const OWN: [EndpointIdx; 2] = [1, 2];

/// `general`, `own-a`, `own-b` at `soft_limit` 0.90, with the client
/// `restricted` allowed `own-a` and `own-b` and pinned to `preferred`.
fn allow_list_state(preferred: &[&str]) -> Arc<AppState> {
    Arc::new(
        TestPolicy::clients(vec![
            mk_allowed_client("restricted", &["own-a", "own-b"], preferred),
            mk_client("free", &test_key("free"), &[]),
        ])
        .install(AppState {
            endpoints: vec![
                mk_endpoint("general", "sk-ant-api-gen"),
                mk_endpoint("own-a", "sk-ant-api-a"),
                mk_endpoint("own-b", "sk-ant-api-b"),
            ],
            soft_limit: 0.90,
            ..test_state_base()
        }),
    )
}

/// Every pick `client` gets over 50 affinity keys, plus an un-keyed one.
async fn picks(state: &AppState, client: &str, skip: &[EndpointIdx]) -> Vec<Option<EndpointIdx>> {
    let caller = state.caller(client);
    let mut out = Vec::new();
    for key in std::iter::once(None).chain((0..50).map(|i| Some(format!("{client}:sess:{i}")))) {
        out.push(
            state
                .pick_endpoint_for_client(
                    key.as_deref(),
                    "claude-opus-5",
                    skip,
                    &caller,
                    FastRequest::STANDARD,
                    false,
                )
                .await,
        );
    }
    out
}

#[test]
fn client_config_allowed_endpoints_parses_and_defaults_empty() {
    let c = cfg(
        "[[endpoints]]\nname = \"own\"\ntoken = \"sk-ant-api-x\"\n\n[[clients]]\nname = \"restricted\"\nkey = \"k1\"\nallowed_endpoints = [\"own\"]\n\n[[clients]]\nname = \"plain\"\nkey = \"k2\"\n",
    );
    assert_eq!(c.clients[0].allowed_endpoints, vec!["own".to_string()]);
    assert!(
        c.clients[1].allowed_endpoints.is_empty(),
        "omitted allowed_endpoints must default to empty (= no restriction)"
    );
}

#[test]
fn build_rejects_unknown_allowed_endpoint() {
    let Err(err) = ClientPolicy::build(&cfg(&format!(
        "[[endpoints]]\nname = \"real\"\ntoken = \"sk-ant-api-x\"\n\n[[clients]]\nname = \"restricted\"\nkey = \"{STRONG_KEY}\"\nallowed_endpoints = [\"real\", \"typo\"]\n",
    ))) else {
        panic!("an allow-list entry naming no endpoint must be refused");
    };
    assert_eq!(
        err,
        "config: client 'restricted': allowed_endpoints entry 'typo' does not match any configured endpoint"
    );
}

#[test]
fn build_rejects_preferred_endpoint_outside_allow_list() {
    let Err(err) = ClientPolicy::build(&cfg(&format!(
        "[[endpoints]]\nname = \"own\"\ntoken = \"sk-ant-api-x\"\n\n[[endpoints]]\nname = \"other\"\ntoken = \"sk-ant-api-y\"\n\n[[clients]]\nname = \"restricted\"\nkey = \"{STRONG_KEY}\"\nallowed_endpoints = [\"own\"]\npreferred_endpoints = [\"other\"]\n",
    ))) else {
        panic!("a pin outside the allow-list must be refused");
    };
    assert_eq!(
        err,
        "config: client 'restricted': preferred_endpoints entry 'other' is not in its allowed_endpoints"
    );
}

#[test]
fn build_accepts_preferred_endpoint_inside_allow_list() {
    assert!(ClientPolicy::build(&cfg(&format!(
        "[[endpoints]]\nname = \"own\"\ntoken = \"sk-ant-api-x\"\n\n[[endpoints]]\nname = \"other\"\ntoken = \"sk-ant-api-y\"\n\n[[clients]]\nname = \"restricted\"\nkey = \"{STRONG_KEY}\"\nallowed_endpoints = [\"own\", \"other\"]\npreferred_endpoints = [\"own\"]\n",
    )))
    .is_ok());
}

/// Healthy pool: the allow-listed client lands only inside its set, though
/// `general` has far more headroom and takes unrestricted traffic.
#[tokio::test]
async fn allowed_client_routes_only_inside_its_set() {
    let state = allow_list_state(&[]);
    state.endpoints[0].rate_info.write().await.utilization = Some(0.05);
    state.endpoints[1].rate_info.write().await.utilization = Some(0.6);
    state.endpoints[2].rate_info.write().await.utilization = Some(0.6);

    let restricted = picks(&state, "restricted", &[]).await;
    assert!(
        restricted
            .iter()
            .all(|p| p.is_some_and(|i| OWN.contains(&i))),
        "every pick must land in the allow-list: {restricted:?}"
    );
    assert!(
        picks(&state, "free", &[]).await.contains(&Some(0)),
        "fixture must attract unrestricted traffic to 'general' or the assert proves nothing"
    );
}

/// The allowed set in each state that spills a soft pin, with `general`
/// healthy throughout. Hard-limited and transport-unhealthy endpoints are no
/// candidates, so the pick is `None` (the exhaustion refusal); soft-limited
/// and overage-served ones still are, so the pick degrades inside the set.
#[tokio::test]
async fn allowed_client_never_leaves_its_set_whatever_its_state() {
    struct Case {
        name: &'static str,
        set: fn(&mut RateLimitInfo),
        expect_served: bool,
    }
    let cases = [
        Case {
            name: "hard-limited",
            set: |info| {
                info.utilization = Some(0.1);
                info.hard_limited_until = Some(Instant::now() + Duration::from_secs(3600));
            },
            expect_served: false,
        },
        Case {
            name: "above soft_limit",
            set: |info| info.utilization = Some(0.95),
            expect_served: true,
        },
        Case {
            name: "serving via overage",
            set: |info| {
                info.utilization_5h = Some(1.0);
                info.overage_in_use = true;
            },
            expect_served: true,
        },
        Case {
            name: "transport-unhealthy",
            set: |info| {
                info.utilization = Some(0.1);
                info.transport.open_until = Some(Instant::now() + Duration::from_secs(3600));
            },
            expect_served: false,
        },
    ];
    for strategy in [
        RoutingStrategy::DynamicCapacityV1,
        RoutingStrategy::StickyWeightedV2,
    ] {
        for case in &cases {
            let mut state = allow_list_state(&[]);
            Arc::get_mut(&mut state)
                .expect("test fixture should be uniquely owned")
                .routing_strategy = strategy;
            state.endpoints[0].rate_info.write().await.utilization = Some(0.05);
            for i in OWN {
                (case.set)(&mut *state.endpoints[i].rate_info.write().await);
            }
            for pick in picks(&state, "restricted", &[]).await {
                match pick {
                    Some(i) => assert!(
                        case.expect_served && OWN.contains(&i),
                        "{strategy:?}, allowed set {}: picked endpoint {i}",
                        case.name
                    ),
                    None => assert!(
                        !case.expect_served,
                        "{strategy:?}, allowed set {}: a still-routable allowed endpoint must serve",
                        case.name
                    ),
                }
            }
            assert!(
                picks(&state, "free", &[])
                    .await
                    .iter()
                    .all(|p| *p == Some(0)),
                "{strategy:?}, {}: the healthy out-of-set endpoint must serve unrestricted traffic",
                case.name
            );
        }
    }
}

/// Rotation: a retry's re-pick with allowed endpoints in `skip` stays inside
/// the set, and with all of them skipped returns `None`, never `general`.
#[tokio::test]
async fn allowed_client_rotation_stays_inside_its_set() {
    let state = allow_list_state(&[]);
    assert!(
        picks(&state, "restricted", &[1])
            .await
            .iter()
            .all(|p| *p == Some(2)),
        "with own-a skipped, own-b must serve"
    );
    assert!(
        picks(&state, "restricted", &OWN)
            .await
            .iter()
            .all(Option::is_none),
        "with the whole allow-list skipped, nothing may serve"
    );
}

/// `preferred_endpoints` keeps its soft meaning inside the allow-list: the
/// pin holds while viable, and its spill goes to the rest of the allow-list,
/// never to the pool.
#[tokio::test]
async fn preferred_endpoint_spills_inside_the_allow_list_only() {
    let state = allow_list_state(&["own-a"]);
    state.endpoints[0].rate_info.write().await.utilization = Some(0.05);
    state.endpoints[2].rate_info.write().await.utilization = Some(0.05);
    state.endpoints[1].rate_info.write().await.utilization = Some(0.5);
    assert!(
        picks(&state, "restricted", &[])
            .await
            .iter()
            .all(|p| *p == Some(1)),
        "a viable pin holds"
    );

    state.endpoints[1].rate_info.write().await.utilization = Some(0.95);
    assert!(
        picks(&state, "restricted", &[])
            .await
            .iter()
            .all(|p| *p == Some(2)),
        "the pin spills to the rest of the allow-list"
    );
    assert!(
        picks(&state, "restricted", &[2])
            .await
            .iter()
            .all(|p| *p == Some(1)),
        "with the rest skipped, the soft-limited pin degrades rather than leave the set"
    );
}

#[tokio::test]
async fn pin_status_reports_allow_listed_picks() {
    let state = allow_list_state(&["own-a"]);
    let restricted = state.caller("restricted");
    assert_eq!(state.pin_status(&restricted, 1), "allowed");
    assert_eq!(state.pin_status(&restricted, 2), "allowed");
    assert_eq!(state.pin_status(&restricted, 0), "disallowed");
    assert_eq!(state.pin_status(&state.caller("free"), 0), "-");
}

// ── End to end, both surfaces ──

/// `general` at its own upstream, outside the client's allow-list, and
/// `own-a` / `own-b` inside it at upstreams answering `own_a_head` (a full
/// raw response head; `None` = 200) and 200. Returns the state and each
/// upstream's hit counter, in endpoint order.
async fn allow_list_app(own_a_head: Option<&'static str>) -> (Arc<AppState>, [Hits; 3]) {
    let (general_url, general_hits) = spawn_flaky_upstream(0, ANTHROPIC_OK_BODY).await;
    let (own_a_url, own_a_hits) = match own_a_head {
        Some(head) => spawn_status_then_ok_upstream(usize::MAX, head, b"{}").await,
        None => spawn_flaky_upstream(0, ANTHROPIC_OK_BODY).await,
    };
    let (own_b_url, own_b_hits) = spawn_flaky_upstream(0, ANTHROPIC_OK_BODY).await;
    let state = Arc::new(
        TestPolicy::clients(vec![mk_allowed_client(
            "restricted",
            &["own-a", "own-b"],
            &[],
        )])
        .install(AppState {
            endpoints: vec![
                mk_endpoint_at("general", "sk-ant-api-gen", &general_url),
                mk_endpoint_at("own-a", "sk-ant-api-a", &own_a_url),
                mk_endpoint_at("own-b", "sk-ant-api-b", &own_b_url),
            ],
            soft_limit: 0.90,
            ..test_state_base()
        }),
    );
    state.endpoints[0].rate_info.write().await.utilization = Some(0.05);
    (state, [general_hits, own_a_hits, own_b_hits])
}

/// The two forwarding surfaces: path, auth header, body.
fn surfaces() -> [(&'static str, (&'static str, String), &'static str); 2] {
    [
        (
            "/v1/messages",
            ("x-api-key", test_key("restricted")),
            r#"{"model":"claude-opus-5","max_tokens":1,"messages":[{"role":"user","content":"hi"}]}"#,
        ),
        (
            "/v1/chat/completions",
            (
                "authorization",
                format!("Bearer {}", test_key("restricted")),
            ),
            r#"{"model":"claude-opus-5","messages":[{"role":"user","content":"hi"}]}"#,
        ),
    ]
}

/// Every allowed account rate-limited while `general` is healthy: the client
/// gets the existing pool-exhaustion 429, unchanged, and `general`'s
/// upstream sees nothing.
#[tokio::test]
async fn allowed_client_gets_the_exhaustion_refusal_and_never_reaches_the_pool() {
    for (path, (auth, key), body) in surfaces() {
        let (state, hits) = allow_list_app(None).await;
        for i in OWN {
            state.endpoints[i]
                .rate_info
                .write()
                .await
                .hard_limited_until = Some(Instant::now() + Duration::from_secs(3600));
        }
        let addr = serve(build_router(state.clone())).await;
        let resp = Client::new()
            .post(format!("http://{addr}{path}"))
            .header("content-type", "application/json")
            .header(auth, &key)
            .body(body)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 429, "{path}");
        assert!(resp.headers().get("retry-after").is_none(), "{path}");
        let json = parse_wire_error_envelope(resp).await;
        assert_eq!(json["error"]["type"], "rate_limit_error", "{path}");
        assert_eq!(
            json["error"]["message"], "exhausted all endpoints",
            "{path}"
        );
        for (i, h) in hits.iter().enumerate() {
            assert_eq!(
                h.load(Ordering::SeqCst),
                0,
                "{path}: upstream {i} was reached"
            );
        }
    }
}

/// A 5xx and a 429 on the first allowed account each rotate to the other
/// allowed account, never to `general`; both surfaces.
#[tokio::test]
async fn allowed_client_retries_and_rotations_stay_inside_its_set() {
    const HEAD_500: &str = "HTTP/1.1 500 Internal Server Error\r\ncontent-type: application/json\r\nconnection: close\r\n\r\n{\"type\":\"error\",\"error\":{\"type\":\"api_error\",\"message\":\"boom\"}}";
    for head in [HEAD_500, HEAD_429_CAPACITY] {
        for (path, (auth, key), body) in surfaces() {
            let (state, [general, own_a, own_b]) = allow_list_app(Some(head)).await;
            // own-a takes the first pick: own-b is soft-limited, so it is
            // the in-tier healthy set's only member until own-a fails.
            state.endpoints[1].rate_info.write().await.utilization = Some(0.1);
            state.endpoints[2].rate_info.write().await.utilization = Some(0.95);
            let addr = serve(build_router(state.clone())).await;
            let resp = Client::new()
                .post(format!("http://{addr}{path}"))
                .header("content-type", "application/json")
                .header(auth, &key)
                .body(body)
                .send()
                .await
                .unwrap();
            let what = format!("{path}, {}", &head[..32]);
            assert_eq!(resp.status(), 200, "{what}");
            assert!(
                own_a.load(Ordering::SeqCst) >= 1,
                "{what}: own-a was never tried"
            );
            assert_eq!(
                own_b.load(Ordering::SeqCst),
                1,
                "{what}: own-b must serve the retry"
            );
            assert_eq!(
                general.load(Ordering::SeqCst),
                0,
                "{what}: general was reached"
            );
        }
    }
}
