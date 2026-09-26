use super::*;

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
// no db suffix, no auth) to run them. When unset, every test that needs the
// backend prints a SKIP notice and returns — never a silent pass against
// nothing (the killable-proxy harness self-test needs none, so always runs).
// When the env var IS set and the backend is unreachable, the tests PANIC,
// so CI (which always sets it — see .github/workflows/ci.yml) can never skip
// silently.
//
// Isolation: each test owns a dedicated logical DB (allocated in the `Db`
// enum, appended as the `/N` suffix of the connection URL) and flushes it on
// connect, because ALL `alb:*` coordination keys (hard/rate/weight/budget/
// probe/heartbeat/transport_errors) are hardcoded in production code and
// cannot be prefixed per-test. Never point ALB_TEST_REDIS_URL at a Redis
// holding data you care about. The allocation runs past the default 16 DBs,
// so run a throwaway server with the same DB count as CI:
//
//   redis-server --port 16379 --bind 127.0.0.1 --save "" --appendonly no --databases 32 --daemonize yes
//   ALB_TEST_REDIS_URL=redis://127.0.0.1:16379 cargo test redis_integration
mod redis_integration {
    use super::*;
    use redis::AsyncCommands;

    const TEST_REDIS_ENV: &str = "ALB_TEST_REDIS_URL";

    /// The logical-DB allocation (see the module doc). An enum, not constants,
    /// so the compiler rejects a reused number (E0081), and the helpers take
    /// `Db`, so no raw number reaches a URL;
    /// `db_numbers_come_only_from_the_db_allocation` rejects a variant two
    /// tests share. DB 0 is deliberately unused.
    #[repr(u8)]
    enum Db {
        MergeHardLimits = 1,
        RateInfoMostRecent = 2,
        RoutingWeights = 3,
        RecoverySentinelCas = 4,
        BudgetIncrbyAccumulates = 5,
        PoisonedBudgetSelfHeals = 6,
        ClusterInfoScanPages = 7,
        TransportErrorsAccumulate = 8,
        TransportErrorsRequeue = 9,
        ProbeLockOneReplica = 10,
        ProbeLockFailsOpen = 11,
        BackendDeathMidRun = 12,
        BackendRecoveryMidRun = 13,
        BackendDownAtStartup = 14,
        LostIncrbyRevival = 15,
        SeedBudgetMirror = 16,
        TransportErrorsExpireDenied = 17,
        TransportErrorsHincrbyRejected = 18,
    }

    impl Db {
        /// `base` (`redis://host:port`) with this DB selected — the only
        /// place a DB number becomes part of a URL.
        fn url(self, base: &str) -> String {
            format!("{}/{}", base.trim_end_matches('/'), self as u8)
        }
    }

    /// Runs without a backend, so a shared DB fails every `cargo test`, not
    /// just the runs that happen to collide. Bans a `Db` variant named in a
    /// helper or in more than one test fn (repeats in one test are fine):
    /// types can't stop two tests naming one variant.
    #[test]
    fn db_numbers_come_only_from_the_db_allocation() {
        let src = include_str!("real_redis.rs");
        let start = src.find("mod redis_integration {").expect("module present");
        let module = &src[start..];
        let module = &module[..module.find("\n}\n").expect("module end")];
        // Skip this checker, doc included: its own literals would keep
        // `seen > 0` true and `owners` non-empty after a rename.
        let decl = module
            .find("fn db_numbers_come_only_from_the_db_allocation")
            .expect("checker present");
        let from = module[..decl]
            .rfind("\n\n")
            .expect("blank line before checker");
        let to = decl + module[decl..].find("\n    }\n").expect("checker end");
        // One test per variant: the enum only keeps numbers distinct, and a
        // test that copies another's variant flushes its fixtures mid-run.
        let mut owners = std::collections::HashMap::new();
        for (at, _) in module
            .match_indices("Db::")
            .filter(|(at, _)| !(from..to).contains(at))
        {
            let variant = module[at + 4..]
                .split(|c: char| !c.is_ascii_alphanumeric())
                .next()
                .unwrap();
            let sig = module[..at]
                .rfind("\n    fn ")
                .max(module[..at].rfind("\n    async fn "))
                .expect("`Db::` use outside any fn");
            let owner = module[sig..].split('(').next().unwrap().trim();
            let line = src[..start + at].lines().count();
            assert!(
                module[..sig].ends_with("test]"),
                "src/persistence/tests/real_redis.rs:{line}: `Db::{variant}` in helper `{owner}` — take the db from the test"
            );
            let first = *owners.entry(variant).or_insert(owner);
            assert_eq!(
                first, owner,
                "src/persistence/tests/real_redis.rs:{line}: `Db::{variant}` already belongs to `{first}` — add a variant"
            );
        }
        assert!(
            !owners.is_empty(),
            "no `Db::` use matched — the scan has gone vacuous"
        );
    }

    /// Resolve the opt-in backend URL. None (with a SKIP notice) when the
    /// env var is unset locally; PANICS when unset in CI (`CI` env present),
    /// so no workflow — current or future — can green with this suite
    /// silently skipped. The documented shape (`redis://host:port` — no
    /// auth, no db suffix) is validated here, once: `Db::url` appends
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
    /// Take `db` from `Db` — logical DBs are the isolation unit.
    ///
    /// Returns a PAIR of clients on the same DB: the `redis`-crate connection
    /// is the test's independent fixture/assertion client (deliberately NOT
    /// the client under test), and the fred client is what goes into
    /// `AppState.redis` — the production coordination path being verified.
    async fn redis_test_conn(db: Db) -> Option<(redis::aio::ConnectionManager, RedisClient)> {
        let base = test_redis_url()?;
        let url = db.url(&base);
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

    /// Kill handle for a killable proxy: `kill_proxy` sends the proxy the
    /// sender half of an ack channel and waits for its "listener closed" reply.
    type KillSwitch = tokio::sync::oneshot::Sender<tokio::sync::oneshot::Sender<()>>;

    /// TCP forwarder in front of the real backend that can be killed
    /// mid-test to simulate Redis dying while connections are established.
    /// Killing aborts every live relay and drops the listener, so both
    /// in-flight commands and subsequent reconnect attempts fail. The port
    /// itself stays reserved for a revive (see `spawn_killable_proxy_at`).
    async fn spawn_killable_proxy(target: String) -> (String, KillSwitch) {
        spawn_killable_proxy_at("127.0.0.1:0", target).await
    }

    /// Same as `spawn_killable_proxy`, but at a caller-chosen address — used
    /// to REVIVE a killed proxy at its old address so a reconnect policy can
    /// find the backend again.
    ///
    /// The port stays reserved for the rest of the test (LAB-2299): `hold` is
    /// bound but never listens, so connects are still refused while the proxy
    /// is dead, but the kernel will not hand the port to any concurrent
    /// `bind(127.0.0.1:0)` (another test's mock server). Without it the dead
    /// window leaves the port unowned and the revive races on `EADDRINUSE`.
    /// Never listening is also what lets the listener, and every later revive,
    /// bind over `hold`: `SO_REUSEADDR` cannot bind over a LISTEN socket, so a
    /// `listen()` on `hold` would break every revive. A plain
    /// `TcpListener::bind` already sets `SO_REUSEADDR`, so that alone never was
    /// the missing piece. Two sockets on one exact addr:port with only
    /// `SO_REUSEADDR` is Linux behaviour; BSD/macOS reject it, so there `hold`
    /// is skipped and the port goes unreserved, as it did before LAB-2299.
    async fn spawn_killable_proxy_at(bind: &str, target: String) -> (String, KillSwitch) {
        fn reusable_socket(addr: std::net::SocketAddr) -> tokio::net::TcpSocket {
            let socket = tokio::net::TcpSocket::new_v4().unwrap();
            socket.set_reuseaddr(true).unwrap();
            socket
                .bind(addr)
                .unwrap_or_else(|e| panic!("killable proxy: bind {addr}: {e}"));
            socket
        }
        let requested: std::net::SocketAddr = bind
            .parse()
            .unwrap_or_else(|e| panic!("killable proxy: bad bind address {bind}: {e}"));
        let hold = cfg!(target_os = "linux").then(|| reusable_socket(requested));
        let addr = hold.as_ref().map_or(requested, |h| h.local_addr().unwrap());
        let listener = reusable_socket(addr)
            .listen(1024)
            .unwrap_or_else(|e| panic!("killable proxy: listen {addr}: {e}"));
        let addr = listener.local_addr().unwrap();
        let (kill_tx, mut kill_rx): (KillSwitch, _) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            let mut relays = tokio::task::JoinSet::new();
            let dead = loop {
                tokio::select! {
                    dead = &mut kill_rx => break dead,
                    accepted = listener.accept() => {
                        // Panic, not `break`: a proxy that stops accepting is a
                        // broken harness, and a silent exit would park with
                        // `kill_rx` alive — `kill_proxy` would wait forever.
                        // Unwinding drops `relays`, which aborts every relay.
                        let (mut inbound, _) = accepted
                            .unwrap_or_else(|e| panic!("killable proxy: accept on {addr}: {e}"));
                        let target = target.clone();
                        relays.spawn(async move {
                            if let Ok(mut outbound) =
                                tokio::net::TcpStream::connect(&target).await
                            {
                                let _ =
                                    tokio::io::copy_bidirectional(&mut inbound, &mut outbound)
                                        .await;
                            }
                        });
                    }
                }
            };
            relays.abort_all();
            // Further connects are refused from here on — and only now does
            // `kill_proxy` return, so a revive never meets this listener live.
            drop(listener);
            if let Ok(dead) = dead {
                let _ = dead.send(());
            }
            // Moving `hold` into this task IS the reservation: the port stays
            // ours until the test's runtime drops the parked task. Delete this
            // line and `hold` drops when the function returns — the dead window
            // is unowned again and the LAB-2299 race silently comes back.
            let _hold = hold;
            std::future::pending::<()>().await;
        });
        (format!("127.0.0.1:{}", addr.port()), kill_tx)
    }

    /// fred client (the client under test) routed through a killable proxy.
    /// Same skip/panic contract as `redis_test_conn`. The DB is flushed via
    /// the independent redis-crate client before the fred client connects.
    async fn proxied_conn(db: Db) -> Option<(RedisClient, KillSwitch)> {
        let base = test_redis_url()?;
        let target = base
            .trim_start_matches("redis://")
            .trim_end_matches('/')
            .split('/')
            .next()
            .unwrap()
            .to_string();
        let (proxy_addr, kill) = spawn_killable_proxy(target).await;
        let url = db.url(&format!("redis://{proxy_addr}"));
        drop(connect_and_flush(&url).await);
        let fred = fred_test_client(&url).await;
        Some((fred, kill))
    }

    /// Returns only once the proxy's listener is closed, so a same-address
    /// revive can never meet it still in LISTEN (LAB-2299), however starved
    /// the runtime. The fixed sleep alone left that ordering to the scheduler.
    async fn kill_proxy(kill: KillSwitch) {
        let (dead_tx, dead_rx) = tokio::sync::oneshot::channel();
        let _ = kill.send(dead_tx);
        // Err only if the proxy task is already gone — its listener with it.
        let _ = dead_rx.await;
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

    /// LAB-2299: `kill_proxy` must return only after the proxy's listener is
    /// closed — a revive at the same address would otherwise meet it still in
    /// LISTEN, which `SO_REUSEADDR` cannot bind over. 100 blocking tasks queued
    /// ahead of the killed proxy keep the runtime busy well past any fixed
    /// grace period before the proxy task gets to run. No backend needed:
    /// nothing ever connects.
    #[tokio::test]
    async fn killable_proxy_revives_even_when_the_kill_is_starved() {
        let target = "127.0.0.1:1".to_string();
        let (addr, kill) = spawn_killable_proxy(target.clone()).await;
        // Let the proxy task park on its first poll, so the kill below
        // re-queues it BEHIND the busy tasks.
        tokio::task::yield_now().await;
        for _ in 0..100 {
            tokio::spawn(async { std::thread::sleep(Duration::from_millis(5)) });
        }
        kill_proxy(kill).await;
        let (revived, _revived_kill) = spawn_killable_proxy_at(&addr, target).await;
        assert_eq!(revived, addr, "proxy must revive at its old address");
    }

    /// AC2 phase 1: the hard-limit MGET merge against real keys — a remote
    /// future epoch is applied, "most recent wins" holds in both directions,
    /// the clear sentinel clears, and an absent key (MGET None) touches
    /// nothing. Pairs with the pure `classify_hard_limit_*` unit tests.
    #[tokio::test]
    async fn sync_from_redis_merges_hard_limits_with_real_backend() {
        let Some((mut conn, fred)) = redis_test_conn(Db::MergeHardLimits).await else {
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
        let Some((mut conn, fred)) = redis_test_conn(Db::RateInfoMostRecent).await else {
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
        let Some((mut conn, fred)) = redis_test_conn(Db::RoutingWeights).await else {
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
        let Some((mut conn, fred)) = redis_test_conn(Db::RecoverySentinelCas).await else {
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
        let Some((mut conn, fred)) = redis_test_conn(Db::BudgetIncrbyAccumulates).await else {
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

    /// LAB-3217 AC1/AC2: a fresh replica's empty budget mirror is seeded from
    /// the shared `alb:budget:{client}:{today}` counter on its first sync
    /// tick, so `/_stats` `used_today`/`remaining` and the
    /// `anthropic_client_budget_*` gauges equal the fleet total after a
    /// restart — and a later tick whose counter is BEHIND the local mirror
    /// never lowers it (the LAB-1962 floor). Pairs with the pure
    /// `fold_budget_mirror_seeds_floors_and_replaces_stale_day`.
    #[tokio::test]
    async fn sync_from_redis_seeds_budget_mirror_from_shared_counter() {
        let Some((mut conn, fred)) = redis_test_conn(Db::SeedBudgetMirror).await else {
            return;
        };
        avoid_utc_midnight().await;
        let today = AppState::now_epoch() / 86400;
        let key = format!("alb:budget:seed-cli:{today}");
        // The fleet spent this much before the replica under test started.
        let _: () = conn.set(&key, 1_516_491u64).await.unwrap();

        let state = Arc::new(AppState {
            endpoints: vec![make_endpoint("seed-ep", Protocol::Anthropic)],
            client_budgets: [("seed-cli".to_string(), 2_000_000u64)].into(),
            redis: Some(fred),
            ..test_state_base()
        });
        assert!(
            state.budget_usage.lock().unwrap().is_empty(),
            "a fresh replica starts with an empty mirror"
        );

        state.sync_from_redis().await;

        assert_eq!(
            state.budget_usage.lock().unwrap().get("seed-cli").copied(),
            Some((today, 1_516_491)),
            "first sync tick must seed the mirror from the shared counter"
        );

        let app = build_router(state.clone());
        let addr = serve(app).await;
        let client = Client::new();
        let stats: serde_json::Value = serde_json::from_str(
            &client
                .get(format!("http://{addr}/_stats"))
                .send()
                .await
                .unwrap()
                .text()
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(
            stats["client_budgets"]["seed-cli"]["used_today"], 1_516_491,
            "/_stats used_today must equal the shared counter"
        );
        assert_eq!(
            stats["client_budgets"]["seed-cli"]["remaining"], 483_509,
            "/_stats remaining must be limit − shared counter"
        );
        let metrics = client
            .get(format!("http://{addr}/metrics"))
            .send()
            .await
            .unwrap()
            .text()
            .await
            .unwrap();
        assert!(
            metrics.contains("anthropic_client_budget_used{client=\"seed-cli\"} 1516491"),
            "budget_used gauge must equal the shared counter:\n{metrics}"
        );
        assert!(
            metrics.contains("anthropic_client_budget_remaining{client=\"seed-cli\"} 483509"),
            "budget_remaining gauge must be limit − shared counter:\n{metrics}"
        );

        // AC2: a counter that fell BEHIND the mirror (an INCRBY lost while
        // Redis was away) must not lower the enforcement floor.
        let _: () = conn.set(&key, 1_000u64).await.unwrap();
        state.sync_from_redis().await;
        assert_eq!(
            state.budget_usage.lock().unwrap().get("seed-cli").copied(),
            Some((today, 1_516_491)),
            "a lagging shared counter must never lower the local floor"
        );
    }

    /// LAB-1962 Kody amendment: the poisoned-value classifier must match
    /// only server-reported value errors (the verbatim WRONGTYPE /
    /// non-integer frames Redis and Dragonfly emit) and never transport
    /// failures — a transport match would reintroduce the F8 fleet-wide
    /// counter erasure this ticket removed.
    #[test]
    fn budget_value_poisoned_classifies_server_value_errors_only() {
        use fred::error::{RedisError, RedisErrorKind};
        assert!(AppState::budget_value_poisoned(&RedisError::new(
            RedisErrorKind::InvalidArgument,
            "WRONGTYPE Operation against a key holding the wrong kind of value",
        )));
        assert!(AppState::budget_value_poisoned(&RedisError::new(
            RedisErrorKind::Unknown,
            "ERR value is not an integer or out of range",
        )));
        // Transport failures leave a valid counter behind — never delete.
        assert!(!AppState::budget_value_poisoned(&RedisError::new(
            RedisErrorKind::IO,
            "Connection reset by peer (os error 104)",
        )));
        assert!(!AppState::budget_value_poisoned(&RedisError::new(
            RedisErrorKind::Timeout,
            "Request timed out",
        )));
        // Overflow means the counter holds a valid (huge) integer — GET
        // still reads it and check_budget denies; that is real accounting,
        // not poison.
        assert!(!AppState::budget_value_poisoned(&RedisError::new(
            RedisErrorKind::Unknown,
            "ERR increment or decrement would overflow",
        )));
    }

    /// LAB-1962 (panel F8) — reversal of the LAB-931 pin, which deliberately
    /// deferred this: a transport-level INCRBY failure must NOT delete the
    /// shared counter (one replica's failed write must never erase
    /// fleet-wide accounting — see
    /// budget_denies_after_incrby_lost_to_dead_backend_and_revival), and an
    /// absent key with redis reachable falls through to the LOCAL floor
    /// instead of authoritatively allowing. Amended by Kody review on
    /// LAB-1962: a POISONED value (server-reported WRONGTYPE / non-integer)
    /// is the one case that still deletes — such a key fails every INCRBY
    /// and GET for its full 48h TTL, so leaving it wedges cross-replica
    /// accounting for the day. A present under-limit counter remains
    /// authoritative over larger local state — that pin stands.
    #[tokio::test]
    async fn budget_incrby_poisoned_key_self_heals_and_absent_key_uses_local_floor() {
        let Some((mut conn, fred)) = redis_test_conn(Db::PoisonedBudgetSelfHeals).await else {
            return;
        };
        avoid_utc_midnight().await;
        let state = Arc::new(AppState {
            client_budgets: [
                ("poison-cli".to_string(), 100u64),
                ("fresh-cli".to_string(), 100u64),
            ]
            .into(),
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

        // INCRBY fails with a server-reported value error → the poisoned key
        // is DELETED so the shared counter can rebuild (Kody amendment on
        // LAB-1962). Only transport failures preserve the key.
        state.record_budget_usage("poison-cli", 10).await;
        assert_eq!(
            conn.get::<_, Option<String>>(&key).await.unwrap(),
            None,
            "poisoned budget key must self-heal via DEL so the counter can rebuild"
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

        // Absent key (just self-healed) + reachable redis + local over limit
        // → the local floor gates. Under the pre-LAB-1962 contract Ok(None)
        // was an authoritative allow — the enforcement bypass the panel
        // flagged.
        assert_eq!(
            state.check_budget("poison-cli").await,
            Err(0),
            "absent key with redis reachable must fall through to the local floor and deny"
        );

        // Absent key + no local usage (genuine zero) → still allows.
        assert!(
            state.check_budget("fresh-cli").await.is_ok(),
            "absent key with no local usage must still allow"
        );

        // A rebuilt under-limit shared counter is authoritative over larger
        // local state — the surviving half of the LAB-931 pin.
        state.record_budget_usage("poison-cli", 5).await;
        assert_eq!(conn.get::<_, Option<u64>>(&key).await.unwrap(), Some(5));
        assert!(
            state.check_budget("poison-cli").await.is_ok(),
            "deliberate pin: a reachable redis counter (5) is authoritative over larger local state (165)"
        );

        // The Lua guard is the race half of the fix: a DEL landing late
        // (fred reconnect replay, or a second replica healing concurrently)
        // must never erase a valid counter the fleet already rebuilt.
        // Evaluated directly against the rebuilt numeric counter: refuses.
        let refused: i64 = redis::cmd("EVAL")
            .arg(AppState::BUDGET_DEL_IF_POISONED_SCRIPT)
            .arg(1)
            .arg(&key)
            .query_async(&mut conn)
            .await
            .unwrap();
        assert_eq!(refused, 0, "guard must refuse to delete a numeric counter");
        assert_eq!(
            conn.get::<_, Option<u64>>(&key).await.unwrap(),
            Some(5),
            "a late guarded DEL must leave rebuilt accounting intact"
        );

        // tonumber()-gap regression (LAB-1962 review): values Lua's
        // tonumber() accepts but INCRBY rejects ("1.5" was the reported
        // wedge; exponent, hex, padded, and out-of-i64-range forms sit in
        // the same gap) must still be deleted — such a key fails every
        // INCRBY for its full TTL, which is the exact wedge the self-heal
        // exists to clear.
        for gap in ["1.5", "1e3", "0x10", " 1", "9223372036854775808"] {
            let _: () = conn.set(&key, gap).await.unwrap();
            let deleted: i64 = redis::cmd("EVAL")
                .arg(AppState::BUDGET_DEL_IF_POISONED_SCRIPT)
                .arg(1)
                .arg(&key)
                .query_async(&mut conn)
                .await
                .unwrap();
            assert_eq!(deleted, 1, "guard must delete tonumber-gap value {gap:?}");
            assert_eq!(
                conn.get::<_, Option<String>>(&key).await.unwrap(),
                None,
                "tonumber-gap value {gap:?} must not survive the guard"
            );
        }

        // WRONGTYPE self-heal: the guard's INCRBY probe errors on non-string
        // keys too — the removed TYPE branch must stay covered by a test,
        // not just the doc comment's equivalence claim.
        let _: () = conn.rpush(&key, "x").await.unwrap();
        let deleted: i64 = redis::cmd("EVAL")
            .arg(AppState::BUDGET_DEL_IF_POISONED_SCRIPT)
            .arg(1)
            .arg(&key)
            .query_async(&mut conn)
            .await
            .unwrap();
        assert_eq!(deleted, 1, "guard must delete a WRONGTYPE (list) key");
        assert!(
            !conn.exists::<_, bool>(&key).await.unwrap(),
            "WRONGTYPE key must not survive the guard"
        );
    }

    /// LAB-1962 AC3: a kill-proxy-induced INCRBY failure loses the increment
    /// from the shared counter, but the SAME replica keeps denying once over
    /// limit — while the backend is dead (error fallback) AND after it
    /// revives with the key absent (absent-key local floor). Under the
    /// pre-LAB-1962 contract the revival half returned Ok: reachable redis +
    /// absent key was an authoritative allow, so every replica granted
    /// unlimited spend the moment the counter vanished.
    #[tokio::test]
    async fn budget_denies_after_incrby_lost_to_dead_backend_and_revival() {
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
        let url = Db::LostIncrbyRevival.url(&format!("redis://{proxy_addr}"));
        drop(connect_and_flush(&url).await);
        let fred = fred_test_client(&url).await;
        // Independent assertion client, connected DIRECTLY to the backend so
        // it can verify the increment really was lost (not buffered/replayed).
        let mut direct = connect(&Db::LostIncrbyRevival.url(&base)).await;

        let state = Arc::new(AppState {
            client_budgets: [("lost-cli".to_string(), 100u64)].into(),
            redis: Some(fred),
            ..test_state_base()
        });

        kill_proxy(kill).await;

        // The very first INCRBY fails (or is skipped once fred notices the
        // outage) — the shared counter is never created. The local
        // accumulator carries the only record of the spend (150 > 100).
        state.record_budget_usage("lost-cli", 150).await;
        assert_eq!(
            state.check_budget("lost-cli").await,
            Err(0),
            "local fallback must deny while the backend is dead"
        );

        // Revive the backend at the SAME address; fred reconnects on its own.
        let (revived_addr, _revived_kill) = spawn_killable_proxy_at(&proxy_addr, target).await;
        assert_eq!(
            revived_addr, proxy_addr,
            "proxy must revive at its old address"
        );
        eventually("fred to reconnect after revival", || {
            let r = state.redis.clone().unwrap();
            async move { r.ping::<String>().await.is_ok() }
        })
        .await;

        // The dead-window increment is lost for good: no key in the backend.
        let today = AppState::now_epoch() / 86400;
        let key = format!("alb:budget:lost-cli:{today}");
        assert_eq!(
            direct.get::<_, Option<u64>>(&key).await.unwrap(),
            None,
            "the dead-window INCRBY must be lost, not buffered and replayed"
        );

        // The money assertion: redis reachable + absent key must fall
        // through to the local floor (150 >= 100) and DENY.
        assert_eq!(
            state.check_budget("lost-cli").await,
            Err(0),
            "absent key after revival must not bypass the local floor (LAB-1962/F8)"
        );
    }

    /// AC5: cluster_info's SCAN loop across multiple cursor pages. 500
    /// heartbeat keys against COUNT 100 forces several SCAN round-trips on a
    /// real backend — a pagination bug (e.g. stopping after the first page)
    /// undercounts. Budget MGET aggregation is asserted in the same pass.
    #[tokio::test]
    async fn cluster_info_counts_heartbeats_across_multiple_scan_pages() {
        let Some((mut conn, fred)) = redis_test_conn(Db::ClusterInfoScanPages).await else {
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
        let Some((mut conn, fred)) = redis_test_conn(Db::TransportErrorsAccumulate).await else {
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
        let Some((fred, kill)) = proxied_conn(Db::TransportErrorsRequeue).await else {
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

    /// A persistent failure in a command that carries no counts (here
    /// `EXPIRE`, denied by ACL) must not re-send `HINCRBY`s Redis already
    /// applied: the hash holds exactly the failures that happened, however
    /// many ticks the fault lasts.
    #[tokio::test]
    async fn flush_transport_errors_does_not_resend_applied_counts_when_expire_fails() {
        let Some((mut conn, _)) = redis_test_conn(Db::TransportErrorsExpireDenied).await else {
            return;
        };
        let base = test_redis_url().expect("backend url");
        let user = format!("alb-expire-denied-{}", std::process::id());
        let created: redis::RedisResult<()> = redis::cmd("ACL")
            .arg("SETUSER")
            .arg(&user)
            .arg(&["reset", "on", "nopass", "~*", "&*", "+@all", "-expire"])
            .query_async(&mut conn)
            .await;
        created.unwrap_or_else(|e| panic!("ACL SETUSER failed: {e}"));
        let url = Db::TransportErrorsExpireDenied.url(&base).replacen(
            "redis://",
            &format!("redis://{user}:unused@"),
            1,
        );
        let state = state_with_redis(vec![], fred_test_client(&url).await);
        state.lock_transport_errors().insert("connect", 2);
        for _ in 0..3 {
            state.flush_transport_errors().await;
        }
        let deleted: redis::RedisResult<()> = redis::cmd("ACL")
            .arg("DELUSER")
            .arg(&user)
            .query_async(&mut conn)
            .await;
        deleted.unwrap_or_else(|e| panic!("ACL DELUSER failed: {e}"));

        let map: HashMap<String, u64> = conn.hgetall(TRANSPORT_ERRORS_KEY).await.unwrap();
        assert_eq!(
            map.get("connect"),
            Some(&2),
            "an EXPIRE failure must not re-send HINCRBYs Redis applied"
        );
        assert_eq!(
            state.lock_transport_errors().get("connect"),
            None,
            "an applied kind must not be re-queued locally"
        );
    }

    /// A kind whose own `HINCRBY` Redis rejects (a non-integer field) is
    /// re-queued locally; a kind in the same flush whose `HINCRBY` applied is
    /// not re-sent.
    #[tokio::test]
    async fn flush_transport_errors_requeues_only_rejected_kinds() {
        let Some((mut conn, fred)) = redis_test_conn(Db::TransportErrorsHincrbyRejected).await
        else {
            return;
        };
        let _: () = conn
            .hset(TRANSPORT_ERRORS_KEY, "timeout", "not-a-number")
            .await
            .unwrap();
        let state = state_with_redis(vec![], fred);
        {
            let mut m = state.lock_transport_errors();
            m.insert("connect", 2);
            m.insert("timeout", 3);
        }
        state.flush_transport_errors().await;
        state.flush_transport_errors().await;

        let connect: Option<u64> = conn.hget(TRANSPORT_ERRORS_KEY, "connect").await.unwrap();
        assert_eq!(connect, Some(2), "an applied kind must not be re-sent");
        let m = state.lock_transport_errors();
        assert_eq!(
            m.get("timeout"),
            Some(&3),
            "a rejected kind must be re-queued"
        );
        assert_eq!(
            m.get("connect"),
            None,
            "an applied kind must not be re-queued"
        );
    }

    /// AC6: the SET NX EX probe lock grants one replica per endpoint+model
    /// per interval; a second replica's probe is suppressed while the lock
    /// is held, and a different model probes under its own lock.
    #[tokio::test]
    async fn probe_lock_grants_one_replica_per_endpoint_model() {
        let Some((mut conn, fred)) = redis_test_conn(Db::ProbeLockOneReplica).await else {
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
        let Some((fred, kill)) = proxied_conn(Db::ProbeLockFailsOpen).await else {
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
        let Some((fred, kill)) = proxied_conn(Db::BackendDeathMidRun).await else {
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

        // Budget: the INCRBY fails; the local accumulator still advances
        // (50+60=110) and check_budget falls back to it, refusing
        // over-limit spend with redis dead.
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
        let url = Db::BackendRecoveryMidRun.url(&format!("redis://{proxy_addr}"));
        drop(connect_and_flush(&url).await);
        let fred = fred_test_client(&url).await;
        // Independent assertion client, connected DIRECTLY to the backend
        // (not through the killable proxy, and without flushing) so it can
        // verify post-recovery writes actually landed.
        let direct = connect(&Db::BackendRecoveryMidRun.url(&base)).await;

        let state = Arc::new(AppState {
            client_budgets: [("recover-cli".to_string(), 100u64)].into(),
            redis: Some(fred),
            ..test_state_base()
        });

        // Healthy first: the shared counter works through the proxy.
        state.record_budget_usage("recover-cli", 50).await;
        assert!(state.check_budget("recover-cli").await.is_ok());

        kill_proxy(kill).await;

        // Dead: the INCRBY fails; the local accumulator (50+60=110)
        // enforces the 100 budget.
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
        // dead-window INCRBY failed and was lost) becomes authoritative
        // again, flipping check_budget from the local Err(0) back to Ok.
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
        // Flush this test's DB through a DIRECT connection — the proxy is dead at
        // client creation, so the usual flush-through-proxy path cannot run.
        // The same client later verifies that post-attach writes landed.
        let direct = connect_and_flush(&Db::BackendDownAtStartup.url(&base)).await;

        // Reserve an address, then kill it BEFORE the client under test
        // exists: nothing is listening when the connection task makes its
        // first attempt — the exact boot-during-outage scenario.
        let (proxy_addr, kill) = spawn_killable_proxy(target.clone()).await;
        kill_proxy(kill).await;
        let url = Db::BackendDownAtStartup.url(&format!("redis://{proxy_addr}"));

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
