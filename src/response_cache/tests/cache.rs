use super::*;

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

/// Backend that hangs like `SlowBackend` while `stalled`, answers like an
/// empty store otherwise, and counts the calls that reach it.
#[derive(Default)]
struct StallableBackend {
    stalled: AtomicBool,
    /// Latency of every op while not stalled.
    delay_ms: AtomicU64,
    gets: AtomicU64,
    sets: AtomicU64,
    pings: AtomicU64,
}

impl StallableBackend {
    async fn stall_if_set(&self) {
        if self.stalled.load(Ordering::Relaxed) {
            tokio::time::sleep(Duration::from_secs(30)).await;
        }
        tokio::time::sleep(Duration::from_millis(self.delay_ms.load(Ordering::Relaxed))).await;
    }
}

#[async_trait::async_trait]
impl cachekit::backend::Backend for StallableBackend {
    async fn get(&self, _key: &str) -> Result<Option<Vec<u8>>, cachekit::BackendError> {
        self.gets.fetch_add(1, Ordering::Relaxed);
        self.stall_if_set().await;
        Ok(None)
    }
    async fn set(
        &self,
        _key: &str,
        _value: Vec<u8>,
        _ttl: Option<Duration>,
    ) -> Result<(), cachekit::BackendError> {
        self.sets.fetch_add(1, Ordering::Relaxed);
        self.stall_if_set().await;
        Ok(())
    }
    async fn delete(&self, _key: &str) -> Result<bool, cachekit::BackendError> {
        Ok(false)
    }
    async fn exists(&self, _key: &str) -> Result<bool, cachekit::BackendError> {
        Ok(false)
    }
    async fn health(&self) -> Result<cachekit::backend::HealthStatus, cachekit::BackendError> {
        self.pings.fetch_add(1, Ordering::Relaxed);
        self.stall_if_set().await;
        Ok(cachekit::backend::HealthStatus {
            is_healthy: true,
            latency_ms: 0.0,
            backend_type: "stallable".to_string(),
            details: HashMap::new(),
        })
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
    assert_eq!(decode_hex_key(&"ab".repeat(32)).unwrap().len(), 32);
    // cachekit-rs >= 0.9 takes exactly 32 raw bytes. Any other length must
    // fail at config load, naming the field the operator has to fix.
    for bytes in [31, 33] {
        let err =
            decode_hex_key(&"ab".repeat(bytes)).expect_err("a key that is not 32 bytes must fail");
        assert!(err.contains("response_cache.master_key"), "got: {err}");
    }
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

// LAB-10850: cachekit-rs 0.10 binds the namespaced storage key into the
// AES-GCM AAD, where 0.5 bound the bare key, and a 0.5 entry under the same
// namespace fails to authenticate on every read until it expires. An entry a
// pre-upgrade replica wrote must read as a miss, never as an error.
#[tokio::test]
async fn response_cache_pre_upgrade_entry_reads_as_miss() {
    let backend = std::sync::Arc::new(RecordingBackend::default());
    let key = "b".repeat(64);
    let entry = CachedResponse {
        status: 200,
        content_type: "application/json".into(),
        body: b"{}".to_vec(),
    };
    // Exactly what cachekit-rs 0.5 `SecureCache::set` stored: ciphertext
    // whose AAD binds the bare key, under `alb-resp:<key>`.
    let plaintext = cachekit::serializer::serialize(&entry).unwrap();
    let ciphertext = cachekit::EncryptionLayer::new(&TEST_MASTER_KEY, "geo")
        .unwrap()
        .encrypt(&plaintext, &key)
        .unwrap();
    backend
        .store
        .lock()
        .unwrap()
        .insert(format!("alb-resp:{key}"), ciphertext);

    let rc = test_response_cache(backend, &["geo"], 500);
    assert!(
        rc.lookup("geo", &key, CacheSurface::Messages)
            .await
            .is_none()
    );
    assert_eq!(rc.misses.load(Ordering::Relaxed), 1);
    assert_eq!(
        rc.errors.load(Ordering::Relaxed),
        0,
        "a pre-upgrade entry must read as a miss, not an error"
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

    // The exposition format requires all samples of one metric
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

// LAB-10850: a Redis backend parks every op abandoned at op_timeout in
// fred's queue until the connection is back. After `CAP` abandoned ops the
// gate closes: no op reaches the backend, and one PING per interval checks
// Redis. A PING answer reopens it, even when every request is an L1 hit.
#[tokio::test]
async fn response_cache_abandon_gate_closes_and_reopens_on_ping() {
    let probe_interval = Duration::from_millis(500);
    let backend = Arc::new(StallableBackend::default());
    let mut rc = test_response_cache(backend.clone(), &["geo"], 20);
    rc.gate = Some(AbandonGate::new(backend.clone(), probe_interval));
    let surface = CacheSurface::Messages;
    let entry = CachedResponse {
        status: 200,
        content_type: "application/json".to_string(),
        body: b"{}".to_vec(),
    };
    let key = |i: u64| format!("{i:064x}");
    let reached = || {
        (
            backend.gets.load(Ordering::Relaxed),
            backend.sets.load(Ordering::Relaxed),
            backend.pings.load(Ordering::Relaxed),
        )
    };
    let hot = key(999);
    rc.store("geo", &hot, &entry, surface).await;

    backend.stalled.store(true, Ordering::Relaxed);
    for i in 0..AbandonGate::CAP {
        rc.store("geo", &key(i), &entry, surface).await;
    }
    rc.store("geo", &key(100), &entry, surface).await;
    assert!(rc.lookup("geo", &key(101), surface).await.is_none());
    assert_eq!(
        reached(),
        (0, AbandonGate::CAP + 1, 1),
        "a closed gate passes no op, and PINGs once per interval"
    );
    assert_eq!(
        rc.errors_for(surface).load(Ordering::Relaxed),
        AbandonGate::CAP + 2,
        "every refused op still counts as an error"
    );

    tokio::time::sleep(probe_interval).await;
    assert!(
        rc.lookup("geo", &hot, surface).await.is_none(),
        "no L1 hit is served while the gate is closed"
    );
    assert_eq!(reached().2, 2, "the next interval PINGs again");

    backend.stalled.store(false, Ordering::Relaxed);
    tokio::time::sleep(probe_interval).await;
    assert!(
        rc.lookup("geo", &hot, surface).await.is_some(),
        "the PING answer reopens the gate for an L1 hit"
    );
    rc.store("geo", &key(102), &entry, surface).await;
    assert_eq!(reached(), (0, AbandonGate::CAP + 2, 3));
    assert_eq!(rc.stores_for(surface).load(Ordering::Relaxed), 2);
}

// LAB-10850: the cap holds for writes started together, before any has
// timed out, and a write whose caller gives up still holds its place.
#[tokio::test]
async fn response_cache_abandon_gate_caps_concurrent_and_cancelled_writes() {
    let backend = Arc::new(StallableBackend::default());
    backend.stalled.store(true, Ordering::Relaxed);
    let mut rc = test_response_cache(backend.clone(), &["geo"], 200);
    rc.gate = Some(AbandonGate::new(
        backend.clone(),
        AbandonGate::PROBE_INTERVAL,
    ));
    let rc = Arc::new(rc);
    let entry = Arc::new(CachedResponse {
        status: 200,
        content_type: "application/json".to_string(),
        body: b"{}".to_vec(),
    });
    let store = |i: u64| {
        let (rc, entry) = (rc.clone(), entry.clone());
        async move {
            rc.store("geo", &format!("{i:064x}"), &entry, CacheSurface::Messages)
                .await;
        }
    };

    // Cancelled long before op_timeout: fred may still hold the SET.
    assert!(
        tokio::time::timeout(Duration::from_millis(20), store(0))
            .await
            .is_err()
    );
    let mut writes = tokio::task::JoinSet::new();
    for i in 1..=40 {
        writes.spawn(store(i));
    }
    writes.join_all().await;
    assert_eq!(
        backend.sets.load(Ordering::Relaxed),
        AbandonGate::CAP,
        "the cancelled write plus concurrent ones never exceed the cap"
    );
}

// LAB-10850: an answer clears only the ops abandoned before the answered op
// was admitted. A reply that was ready before an outage but read after it
// says nothing about the ops queued since.
#[tokio::test]
async fn response_cache_abandon_gate_late_answer_keeps_newer_abandonments() {
    let backend = Arc::new(StallableBackend::default());
    let gate = AbandonGate::new(backend.clone(), AbandonGate::PROBE_INTERVAL);
    let deadline = || tokio::time::Instant::now() + Duration::from_secs(1);
    let closed = |gate: &AbandonGate| gate.counts().abandoned >= AbandonGate::CAP;

    let old = gate.admit(false, deadline()).await.expect("open gate");
    for _ in 0..AbandonGate::CAP {
        drop(gate.admit(true, deadline()).await.expect("under the cap"));
    }
    assert!(closed(&gate));
    // No probe until the end: only the answers this test hands in count.
    gate.counts().next_probe = std::time::Instant::now() + Duration::from_secs(3600);
    old.answered();
    assert!(
        closed(&gate),
        "a late answer must not forget newer abandonments"
    );
    assert!(gate.admit(true, deadline()).await.is_none());

    // The PING is admitted after every abandonment, so its answer clears them.
    gate.counts().next_probe = std::time::Instant::now();
    gate.probe_slot().expect("probe due").answered();
    assert!(!closed(&gate), "an answer to a newer op reopens the gate");
}

// LAB-10850: cancelled requests close the gate even on a healthy Redis, so
// one tenant could pause the cache for all. Each close PINGs at once, and the
// reply reopens the gate for the very request that sent it.
#[tokio::test]
async fn response_cache_abandon_gate_false_close_reopens_at_once() {
    let backend = Arc::new(StallableBackend::default());
    let mut rc = test_response_cache(backend.clone(), &["geo"], 100);
    rc.gate = Some(AbandonGate::new(
        backend.clone(),
        AbandonGate::PROBE_INTERVAL,
    ));
    let gate = rc.gate.as_ref().unwrap();
    let surface = CacheSurface::Messages;
    let far = tokio::time::Instant::now() + Duration::from_secs(1);
    for round in 1..=2 {
        for _ in 0..AbandonGate::CAP {
            // A request cancelled mid-op, on a live backend.
            drop(gate.admit(true, far).await.expect("gate open"));
        }
        assert!(rc.lookup("geo", &"e".repeat(64), surface).await.is_none());
        assert_eq!(
            (
                backend.pings.load(Ordering::Relaxed),
                rc.misses_for(surface).load(Ordering::Relaxed),
            ),
            (round, round),
            "close {round} PINGs at once and the lookup reaches Redis"
        );
    }
}

// LAB-10850: the probe runs inside the probing op's own op_timeout, so a
// closed gate never stretches a request past it.
#[tokio::test]
async fn response_cache_abandon_gate_probe_shares_the_op_timeout() {
    let budget = Duration::from_millis(100);
    let backend = Arc::new(StallableBackend::default());
    let mut rc = test_response_cache(backend.clone(), &["geo"], budget.as_millis() as u64);
    rc.gate = Some(AbandonGate::new(
        backend.clone(),
        AbandonGate::PROBE_INTERVAL,
    ));
    let gate = rc.gate.as_ref().unwrap();
    let far = tokio::time::Instant::now() + Duration::from_secs(1);
    for _ in 0..AbandonGate::CAP {
        drop(gate.admit(true, far).await.expect("under the cap"));
    }

    // PING and GET take 70 ms each: together they would overrun the budget.
    backend.delay_ms.store(70, Ordering::Relaxed);
    let surface = CacheSurface::Messages;
    let started = std::time::Instant::now();
    assert!(rc.lookup("geo", &"d".repeat(64), surface).await.is_none());
    let elapsed = started.elapsed();
    assert_eq!(backend.pings.load(Ordering::Relaxed), 1);
    assert_eq!(
        rc.misses_for(surface).load(Ordering::Relaxed),
        0,
        "the GET ran past the budget left after the PING ({elapsed:?})"
    );
    assert_eq!(rc.errors_for(surface).load(Ordering::Relaxed), 1);
}

// AC11 (redis): the real fred-backed RedisBackend against a dead port at
// startup — not a config error, but the cache is disabled, so no op queues
// in a fred client that will never connect.
#[tokio::test]
async fn response_cache_redis_backend_unreachable_at_startup_disables_cache() {
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
        .expect("dead redis must not be a config error");
    assert!(rc.is_none(), "a dead redis at startup disables the cache");

    // A dead Redis must not hide a config error that a live one would raise.
    let sentinel = ResponseCacheConfig {
        clients: vec!["-".to_string()],
        ..cfg
    };
    assert!(
        ResponseCache::from_config(&sentinel).await.is_err(),
        "the \"-\" client is refused even while Redis is down"
    );
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
// CACHEKIT_API_URL (cachekit's own env name) targets another cachekit host
// the way `response_cache.api_url` does.
#[tokio::test]
#[ignore = "requires CACHEKIT_API_KEY and network access to api.cachekit.io"]
async fn response_cache_cachekitio_live_round_trip() {
    let api_key = match std::env::var("CACHEKIT_API_KEY") {
        Ok(k) if !k.is_empty() => k,
        _ => panic!("set CACHEKIT_API_KEY to run this test"),
    };
    let mut builder = cachekit::backend::cachekitio::CachekitIO::builder().api_key(api_key);
    if let Ok(url) = std::env::var("CACHEKIT_API_URL") {
        builder = builder.api_url(url).allow_custom_host(true);
    }
    let backend = builder.build().unwrap();
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

// anthropic-version and the URI query string are
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

// An upstream answering a stream:false request
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

// Bodies over MAX_BODY_BYTES are not stored —
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

// ── AC-4: the response-cache tenant follows the authenticated principal ──

/// #113 derives the cache's per-client encryption key from `client_id`. This
/// asserts the tenant a spoofing caller would land in is its OWN, not the one
/// it named — i.e. the cross-tenant read that ticket accepted is now closed.
#[test]
fn response_cache_tenant_follows_the_authenticated_principal() {
    let state = state_with_clients(vec![
        mk_client("alpha", &test_key("key-alpha"), &[]),
        mk_client("bravo", &test_key("key-bravo"), &[]),
    ]);
    let ip = test_ip();
    let headers = hdrs(&[
        ("x-api-key", &test_key("key-alpha")),
        ("x-client-id", "bravo"),
    ]);
    let auth = state.authenticate(&headers, false).unwrap();
    let rctx = RequestContext::from_request(&state, &ip, &headers, auth);

    // The cache is keyed by this exact string (`rc.clients.get(&client_id)`),
    // and the HKDF tenant is derived from it.
    assert_eq!(rctx.caller.name(), "alpha");
    assert_ne!(rctx.caller.name(), "bravo");
}
