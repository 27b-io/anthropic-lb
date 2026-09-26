use super::*;

#[cfg(feature = "guard")]
use crate::guard::AWS_DOCS_EXAMPLE_SECRET_KEY;

/// LAB-4341: the upstream token every guard test's `Endpoint` carries. Not a
/// credential and never was, but it used to be spelled with the real key
/// prefix and a digit-pair version, which is the shape a reader or a scanner
/// learns to skim past. Kept under `sk-ant-api` on purpose, not as decoration:
/// `inject_account_auth` branches on that prefix to choose `x-api-key` over
/// `Bearer`, so a token without it silently changes which header these tests
/// exercise.
#[cfg(feature = "guard")]
const TEST_ENDPOINT_TOKEN: &str = "sk-ant-api-guard-test-token";

/// The two non-`block` policies, keyed `annotate-key` and `off-key`. `off` is
/// not a carbon copy of `annotate`: it returns `Allow` from `Guard::evaluate`'s
/// own early return without reaching the scanners, while `annotate` reaches
/// them and finds nothing to scan. Same bytes on the wire by two routes, so
/// both are pinned — against one shared `AppState` per surface, because
/// `Guard::new()` compiles the bundled rulesets and costs seconds per call.
#[cfg(feature = "guard")]
fn guard_non_block_clients() -> Vec<ClientConfig> {
    vec![
        mk_client("annotate", "annotate-key", &[]),
        ClientConfig {
            guard: crate::guard::GuardPolicy::Off,
            ..mk_client("off", "off-key", &[])
        },
    ]
}

/// A `[[clients]]` entry with the opt-in `block` guard policy, keyed
/// `block-key`.
#[cfg(feature = "guard")]
fn guard_block_client() -> ClientConfig {
    ClientConfig {
        name: "blocked-client".to_string(),
        key: "block-key".to_string(),
        models: vec![],
        preferred_endpoints: vec![],
        guard: crate::guard::GuardPolicy::Block,
    }
}

/// A mock Anthropic upstream that records the raw forwarded body bytes and
/// returns a minimal valid response with rate-limit headers. Returns the
/// listener address and the shared capture buffer.
#[cfg(feature = "guard")]
async fn spawn_guard_body_upstream() -> (String, std::sync::Arc<tokio::sync::Mutex<Vec<u8>>>) {
    let captured: std::sync::Arc<tokio::sync::Mutex<Vec<u8>>> =
        std::sync::Arc::new(tokio::sync::Mutex::new(Vec::new()));
    let cap = captured.clone();
    let mock_app = Router::new().fallback(any(move |req: Request<Body>| {
        let cap = cap.clone();
        async move {
            let body_bytes = axum::body::to_bytes(req.into_body(), 4 * 1024 * 1024)
                .await
                .unwrap();
            *cap.lock().await = body_bytes.to_vec();
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
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, mock_app).await.unwrap();
    });
    (format!("http://{addr}"), captured)
}

/// LAB-3877: with the guard in its default `annotate` (shadow) mode, a request
/// carrying a secret is (a) forwarded upstream byte-for-byte identical to what
/// the client sent — detection must never disturb the prompt-cache prefix — and
/// (b) tagged with the `X-Guard-Findings` header on the response.
#[cfg(feature = "guard")]
#[tokio::test]
async fn guard_annotate_forwards_byte_identical_and_stamps_header() {
    let (upstream, captured) = spawn_guard_body_upstream().await;
    let state = Arc::new(AppState {
        endpoints: vec![mk_endpoint_at("acct", TEST_ENDPOINT_TOKEN, &upstream)],
        state_path: PathBuf::from("/tmp/anthropic-lb-guard-annotate.state.json"),
        auto_cache: false, // clean byte-identity signal
        guard: crate::guard::Guard::new().expect("guard rules"),
        ..test_state_base()
    });
    let app = build_router(state);
    let addr = serve(app).await;

    // Secret (AWS secret key) + PII (email) in the newest user turn.
    let request_body = serde_json::json!({
        "model": "claude-sonnet-4-6",
        "system": [{"type": "text", "text": "you are helpful"}],
        "messages": [{"role": "user", "content":
            format!("deploy key {AWS_DOCS_EXAMPLE_SECRET_KEY} email ops@example.com")}],
        "max_tokens": 5
    });
    let request_bytes = serde_json::to_vec(&request_body).unwrap();

    let resp = Client::new()
        .post(format!("http://{addr}/v1/messages"))
        .header("content-type", "application/json")
        .body(request_bytes.clone())
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200);
    let hdr = resp
        .headers()
        .get("x-guard-findings")
        .expect("annotate must stamp X-Guard-Findings");
    let count: usize = hdr.to_str().unwrap().parse().unwrap();
    assert!(count >= 1, "expected at least one finding, got {count}");

    let forwarded = captured.lock().await;
    assert_eq!(
        forwarded.as_slice(),
        request_bytes.as_slice(),
        "guard must forward the body byte-identically (re-serialization breaks upstream cache)"
    );
}

/// LAB-3877: a client whose policy is `block` gets an HTTP 400 with a
/// `guard_blocked` body when its request carries a secret — and the upstream is
/// never contacted. The error body carries finding offsets only, never the
/// matched secret text.
#[cfg(feature = "guard")]
#[tokio::test]
async fn guard_block_returns_400_with_offsets_and_skips_upstream() {
    let (upstream, captured) = spawn_guard_body_upstream().await;
    let state = Arc::new(AppState {
        endpoints: vec![mk_endpoint_at("acct", TEST_ENDPOINT_TOKEN, &upstream)],
        clients: vec![guard_block_client()],
        state_path: PathBuf::from("/tmp/anthropic-lb-guard-block.state.json"),
        auto_cache: false,
        guard: crate::guard::Guard::new().expect("guard rules"),
        ..test_state_base()
    });
    let app = build_router(state);
    let addr = serve(app).await;

    let secret = AWS_DOCS_EXAMPLE_SECRET_KEY;
    let request_body = serde_json::json!({
        "model": "claude-sonnet-4-6",
        "messages": [{"role": "user",
            "content": format!("aws_secret_access_key = \"{secret}\"")}],
        "max_tokens": 5
    });

    let resp = Client::new()
        .post(format!("http://{addr}/v1/messages"))
        .header("content-type", "application/json")
        .header("x-api-key", "block-key")
        .json(&request_body)
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 400);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["type"], "error");
    assert_eq!(body["error"]["type"], "guard_blocked");
    let findings = body["error"]["findings"]
        .as_array()
        .expect("findings array");
    assert!(!findings.is_empty(), "block body must list findings");
    for f in findings {
        assert!(f.get("scanner").is_some());
        assert!(f.get("detection_type").is_some());
        assert!(f["start"].is_number() && f["end"].is_number());
        assert!(f.get("matched").is_none(), "must not leak matched text");
    }
    // The whole error body must not contain the secret anywhere.
    assert!(
        !body.to_string().contains(secret),
        "block response must never echo the matched secret"
    );
    // Upstream must not have been contacted.
    assert!(
        captured.lock().await.is_empty(),
        "a blocked request must never reach the upstream"
    );
}

/// LAB-3877 (review finding #2): `block` fails closed on a body the guard cannot
/// scan in full. A newest-turn payload padded past the 32 KiB scan limit — even
/// with no secret in the scanned window — is rejected, not forwarded unscanned,
/// so the "pad past the cap, then the secret" bypass is closed.
#[cfg(feature = "guard")]
#[tokio::test]
async fn guard_block_fails_closed_on_oversized_body() {
    let (upstream, captured) = spawn_guard_body_upstream().await;
    let state = Arc::new(AppState {
        endpoints: vec![mk_endpoint_at("acct", TEST_ENDPOINT_TOKEN, &upstream)],
        clients: vec![guard_block_client()],
        state_path: PathBuf::from("/tmp/anthropic-lb-guard-oversized.state.json"),
        auto_cache: false,
        guard: crate::guard::Guard::new().expect("guard rules"),
        ..test_state_base()
    });
    let app = build_router(state);
    let addr = serve(app).await;

    // Newest user turn: >32 KiB of clean padding (the scanned window finds
    // nothing) followed by a secret in the unscanned tail.
    let mut content = "x ".repeat(20_000); // ~40 KiB, well past the 32 KiB cap
    content.push_str(&format!(
        " aws_secret_access_key = \"{AWS_DOCS_EXAMPLE_SECRET_KEY}\""
    ));
    let request_body = serde_json::json!({
        "model": "claude-sonnet-4-6",
        "messages": [{"role": "user", "content": content}],
        "max_tokens": 5
    });

    let resp = Client::new()
        .post(format!("http://{addr}/v1/messages"))
        .header("content-type", "application/json")
        .header("x-api-key", "block-key")
        .json(&request_body)
        .send()
        .await
        .unwrap();

    assert_eq!(
        resp.status(),
        400,
        "oversized body must fail closed under block"
    );
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["error"]["type"], "guard_blocked");
    assert!(
        captured.lock().await.is_empty(),
        "a body that cannot be scanned in full must never reach the upstream under block"
    );
}

/// LAB-3877 (review finding #3): `block` fails closed on an unparseable body — a
/// parse differential vs the upstream must not smuggle content past the scan.
#[cfg(feature = "guard")]
#[tokio::test]
async fn guard_block_fails_closed_on_unparseable_body() {
    let (upstream, captured) = spawn_guard_body_upstream().await;
    let state = Arc::new(AppState {
        endpoints: vec![mk_endpoint_at("acct", TEST_ENDPOINT_TOKEN, &upstream)],
        clients: vec![guard_block_client()],
        state_path: PathBuf::from("/tmp/anthropic-lb-guard-unparseable.state.json"),
        auto_cache: false,
        guard: crate::guard::Guard::new().expect("guard rules"),
        ..test_state_base()
    });
    let app = build_router(state);
    let addr = serve(app).await;

    let resp = Client::new()
        .post(format!("http://{addr}/v1/messages"))
        .header("content-type", "application/json")
        .header("x-api-key", "block-key")
        .body("{ this is not valid json ]]")
        .send()
        .await
        .unwrap();

    assert_eq!(
        resp.status(),
        400,
        "unparseable body must fail closed under block"
    );
    assert!(
        captured.lock().await.is_empty(),
        "an unparseable body must never reach the upstream under block"
    );
}

/// Shadow-mode counterpart: `annotate` (the default) must NOT reject an
/// oversized body — it scans best-effort and forwards byte-identically. Guards
/// against the fail-closed logic leaking into the shadow-mode default.
#[cfg(feature = "guard")]
#[tokio::test]
async fn guard_annotate_forwards_oversized_body() {
    let (upstream, captured) = spawn_guard_body_upstream().await;
    let state = Arc::new(AppState {
        endpoints: vec![mk_endpoint_at("acct", TEST_ENDPOINT_TOKEN, &upstream)],
        state_path: PathBuf::from("/tmp/anthropic-lb-guard-annotate-oversized.state.json"),
        auto_cache: false,
        guard: crate::guard::Guard::new().expect("guard rules"),
        ..test_state_base()
    });
    let app = build_router(state);
    let addr = serve(app).await;

    let content = "x ".repeat(20_000); // ~40 KiB, past the cap, no secret
    let request_body = serde_json::json!({
        "model": "claude-sonnet-4-6",
        "messages": [{"role": "user", "content": content}],
        "max_tokens": 5
    });
    let request_bytes = serde_json::to_vec(&request_body).unwrap();

    let resp = Client::new()
        .post(format!("http://{addr}/v1/messages"))
        .header("content-type", "application/json")
        .body(request_bytes.clone())
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200, "annotate must never reject");
    assert_eq!(
        captured.lock().await.as_slice(),
        request_bytes.as_slice(),
        "annotate forwards the oversized body byte-identically"
    );
}

/// LAB-3877 (review): the block-mode fail-closed rule needs a body to fail on.
/// A bodiless `GET /v1/models` from a block-mode client has nothing to scan and
/// must pass through — not 400 as "unparseable".
#[cfg(feature = "guard")]
#[tokio::test]
async fn guard_block_passes_bodiless_get_through() {
    let (upstream, _captured) = spawn_guard_body_upstream().await;
    let state = Arc::new(AppState {
        endpoints: vec![mk_endpoint_at("acct", TEST_ENDPOINT_TOKEN, &upstream)],
        clients: vec![guard_block_client()],
        state_path: PathBuf::from("/tmp/anthropic-lb-guard-get.state.json"),
        auto_cache: false,
        guard: crate::guard::Guard::new().expect("guard rules"),
        ..test_state_base()
    });
    let app = build_router(state);
    let addr = serve(app).await;

    let resp = Client::new()
        .get(format!("http://{addr}/v1/models"))
        .header("x-api-key", "block-key")
        .send()
        .await
        .unwrap();

    assert_eq!(
        resp.status(),
        200,
        "a bodiless GET is not a scan failure under block"
    );
}

/// LAB-3877 (review): the guard covers `/v1/chat/completions` too — a `block`
/// client cannot route around enforcement via the OpenAI-compat surface. The
/// 400 uses the OpenAI error envelope, still offsets-only, upstream untouched.
#[cfg(feature = "guard")]
#[tokio::test]
async fn guard_block_applies_to_openai_chat_completions() {
    let (upstream, captured) = spawn_guard_body_upstream().await;
    let state = Arc::new(AppState {
        endpoints: vec![mk_endpoint_at("acct", TEST_ENDPOINT_TOKEN, &upstream)],
        clients: vec![guard_block_client()],
        state_path: PathBuf::from("/tmp/anthropic-lb-guard-openai-block.state.json"),
        auto_cache: false,
        guard: crate::guard::Guard::new().expect("guard rules"),
        ..test_state_base()
    });
    let app = build_router(state);
    let addr = serve(app).await;

    let resp = Client::new()
        .post(format!("http://{addr}/v1/chat/completions"))
        .header("content-type", "application/json")
        .header("authorization", "Bearer block-key")
        .json(&serde_json::json!({
            "model": "claude-sonnet-4-6",
            "messages": [{"role": "user",
                "content": format!("aws_secret_access_key = \"{AWS_DOCS_EXAMPLE_SECRET_KEY}\"")}],
            "max_tokens": 5
        }))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 400);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["error"]["code"], "guard_blocked");
    assert!(
        !body["error"]["findings"]
            .as_array()
            .expect("findings array")
            .is_empty(),
        "block body must list findings"
    );
    assert!(
        !body.to_string().contains(AWS_DOCS_EXAMPLE_SECRET_KEY),
        "OpenAI-shape block response must never echo the matched secret"
    );
    assert!(
        captured.lock().await.is_empty(),
        "a blocked chat-completions request must never reach the upstream"
    );
}

/// LAB-3877 (review): an OpenAI message role the translator does not map
/// (legacy `function`) carries content the scanner cannot see but the
/// OpenAI-protocol arm would still forward — under `block` it fails closed.
///
/// LAB-4358 kept the behaviour and deleted the machinery: this used to be a
/// hand-maintained allow-list of OpenAI roles in `openai_chat_handler`. The
/// translator passes an unmapped role through VERBATIM, so the role now lands
/// in the translated `messages` where `ScanInput::from_body`'s own rule rejects
/// it — one rule, both surfaces, nothing to keep in sync with the translator.
#[cfg(feature = "guard")]
#[tokio::test]
async fn guard_block_fails_closed_on_unmapped_openai_role() {
    let (upstream, captured) = spawn_guard_body_upstream().await;
    let state = Arc::new(AppState {
        endpoints: vec![mk_endpoint_at("acct", TEST_ENDPOINT_TOKEN, &upstream)],
        clients: vec![guard_block_client()],
        state_path: PathBuf::from("/tmp/anthropic-lb-guard-openai-role.state.json"),
        auto_cache: false,
        guard: crate::guard::Guard::new().expect("guard rules"),
        ..test_state_base()
    });
    let app = build_router(state);
    let addr = serve(app).await;

    let resp = Client::new()
        .post(format!("http://{addr}/v1/chat/completions"))
        .header("content-type", "application/json")
        .header("authorization", "Bearer block-key")
        .json(&serde_json::json!({
            "model": "claude-sonnet-4-6",
            "messages": [{"role": "function", "name": "env",
                "content": format!("AWS_SECRET_ACCESS_KEY={AWS_DOCS_EXAMPLE_SECRET_KEY}")}],
            "max_tokens": 5
        }))
        .send()
        .await
        .unwrap();

    assert_eq!(
        resp.status(),
        400,
        "unmapped role must fail closed under block"
    );
    // LAB-4322 gave each fail-closed cause its own reason. Pinned here because
    // the whole point of that split is that a client debugging this one is not
    // sent hunting for a JSON syntax error that does not exist. The text is
    // surface-neutral since LAB-4358 — the same rule now rejects `"User"` on
    // `/v1/messages`, and naming OpenAI there would have been a lie.
    let err: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(
        err["error"]["message"],
        crate::guard::REASON_MESSAGE_UNREADABLE,
        "the role reason must not masquerade as a parse failure, got {err}"
    );
    assert!(
        captured.lock().await.is_empty(),
        "an unscannable chat-completions body must never reach the upstream under block"
    );
}

/// LAB-4322: `translate_openai_to_anthropic` reads `messages` through
/// `.as_array()` and then unconditionally writes an array back, so a non-array
/// `messages` becomes an EMPTY array in the document the scanner reads — while
/// a `Protocol::OpenAI` endpoint forwards the client's original bytes, text and
/// all. Under `block` every such shape is unscannable, so none of it reaches an
/// upstream. `null` and absent are both listed: they differ at
/// `body.get("messages")` (`Some(Null)` vs `None`) and a refactor could split
/// them.
#[cfg(feature = "guard")]
#[tokio::test]
async fn guard_block_fails_closed_on_non_array_openai_messages() {
    let secret = AWS_DOCS_EXAMPLE_SECRET_KEY;
    let (upstream, captured) = spawn_guard_body_upstream().await;
    let mut gw = make_endpoint("gw", Protocol::OpenAI);
    gw.base_url = upstream;
    let state = Arc::new(AppState {
        endpoints: vec![gw],
        clients: vec![guard_block_client()],
        state_path: PathBuf::from("/tmp/anthropic-lb-guard-openai-shape.state.json"),
        auto_cache: false,
        guard: crate::guard::Guard::new().expect("guard rules"),
        ..test_state_base()
    });
    let addr = serve(build_router(state)).await;

    let bodies = [
        (
            "string",
            serde_json::json!({"model": "claude-sonnet-4-6", "max_tokens": 5,
                "messages": format!("aws_secret_access_key = \"{secret}\"")}),
        ),
        (
            "object",
            serde_json::json!({"model": "claude-sonnet-4-6", "max_tokens": 5,
                "messages": {"role": "user", "content": format!("key {secret}")}}),
        ),
        (
            "null",
            serde_json::json!({"model": "claude-sonnet-4-6", "max_tokens": 5,
                "messages": serde_json::Value::Null}),
        ),
        (
            "absent",
            serde_json::json!({"model": "claude-sonnet-4-6", "max_tokens": 5}),
        ),
    ];

    for (shape, body) in bodies {
        let resp = Client::new()
            .post(format!("http://{addr}/v1/chat/completions"))
            .header("content-type", "application/json")
            .header("authorization", "Bearer block-key")
            .json(&body)
            .send()
            .await
            .unwrap();

        assert_eq!(
            resp.status(),
            400,
            "{shape} `messages` is unscannable and must fail closed under block"
        );
        let err: serde_json::Value = resp.json().await.unwrap();
        assert_eq!(
            err["error"]["code"], "guard_blocked",
            "{shape}: OpenAI error envelope expected, got {err}"
        );
        assert_eq!(
            err["error"]["message"],
            "request `messages` is missing or not an array and cannot be scanned",
            "{shape}: the shape reason must not masquerade as a parse failure"
        );
        assert!(
            !err.to_string().contains(secret),
            "{shape}: the block response must never echo client text"
        );
        assert!(
            captured.lock().await.is_empty(),
            "{shape}: not one byte may reach a Protocol::OpenAI upstream unscanned"
        );
    }
}

/// LAB-4322 counterpart: the fail-closed widening is gated on `block` only.
/// Under `annotate` and `off` the same unscannable body still reaches the
/// upstream, and the bytes on the wire stay byte-identical to the client's —
/// the prompt-cache raw-prefix invariant. Each policy sends a distinct body so
/// the byte-identity assertion cannot pass against the other's capture.
#[cfg(feature = "guard")]
#[tokio::test]
async fn guard_non_block_forwards_non_array_openai_messages_byte_identically() {
    let (upstream, captured) = spawn_guard_body_upstream().await;
    let mut gw = make_endpoint("gw", Protocol::OpenAI);
    gw.base_url = upstream;
    let state = Arc::new(AppState {
        endpoints: vec![gw],
        clients: vec![
            mk_client("annotate", "annotate-key", &[]),
            ClientConfig {
                guard: crate::guard::GuardPolicy::Off,
                ..mk_client("off", "off-key", &[])
            },
        ],
        state_path: PathBuf::from("/tmp/anthropic-lb-guard-openai-shape-shadow.state.json"),
        auto_cache: false,
        guard: crate::guard::Guard::new().expect("guard rules"),
        ..test_state_base()
    });
    let addr = serve(build_router(state)).await;

    for policy in ["annotate", "off"] {
        let raw = format!(
            r#"{{"model":"claude-sonnet-4-6","max_tokens":5,"messages":"{policy} aws_secret_access_key = \"{AWS_DOCS_EXAMPLE_SECRET_KEY}\""}}"#
        );
        let resp = Client::new()
            .post(format!("http://{addr}/v1/chat/completions"))
            .header("content-type", "application/json")
            .header("authorization", format!("Bearer {policy}-key"))
            .body(raw.clone())
            .send()
            .await
            .unwrap();

        assert_eq!(resp.status(), 200, "{policy} must never reject");
        let body: serde_json::Value = resp.json().await.unwrap();
        assert_eq!(
            body["id"], "msg_test",
            "{policy} must return the upstream's own response"
        );
        assert_eq!(
            captured.lock().await.as_slice(),
            raw.as_bytes(),
            "{policy} forwards the original bytes byte-identically"
        );
    }
}

/// LAB-4341: on the native surface the forwarded document IS the unscanned
/// one, so a `messages` the scanner cannot read as an array puts every
/// character of it on the wire. That the upstream would reject the shape
/// itself is no defence — the bytes have already left.
///
/// Scope: this pins the PRESENT-and-not-an-array shape only. An array that is
/// itself unreadable still forwards; see `guard_messages_wrong_shape`.
#[cfg(feature = "guard")]
#[tokio::test]
async fn guard_block_fails_closed_on_non_array_native_messages() {
    let secret = AWS_DOCS_EXAMPLE_SECRET_KEY;
    let (upstream, captured) = spawn_guard_body_upstream().await;
    let state = Arc::new(AppState {
        endpoints: vec![mk_endpoint_at("acct", TEST_ENDPOINT_TOKEN, &upstream)],
        clients: vec![guard_block_client()],
        state_path: PathBuf::from("/tmp/anthropic-lb-guard-native-shape.state.json"),
        auto_cache: false,
        guard: crate::guard::Guard::new().expect("guard rules"),
        ..test_state_base()
    });
    let addr = serve(build_router(state)).await;

    let bodies = [
        (
            "string",
            serde_json::json!({"model": "claude-sonnet-4-6", "max_tokens": 5,
                "messages": format!("aws_secret_access_key = \"{secret}\"")}),
        ),
        (
            "object",
            serde_json::json!({"model": "claude-sonnet-4-6", "max_tokens": 5,
                "messages": {"role": "user", "content": format!("key {secret}")}}),
        ),
        (
            "null",
            serde_json::json!({"model": "claude-sonnet-4-6", "max_tokens": 5,
                "messages": serde_json::Value::Null}),
        ),
    ];

    for (shape, body) in bodies {
        let resp = Client::new()
            .post(format!("http://{addr}/v1/messages"))
            .header("content-type", "application/json")
            .header("x-api-key", "block-key")
            .json(&body)
            .send()
            .await
            .unwrap();

        assert_eq!(
            resp.status(),
            400,
            "{shape} `messages` is unscannable and must fail closed under block"
        );
        let err: serde_json::Value = resp.json().await.unwrap();
        assert_eq!(
            err["error"]["type"], "guard_blocked",
            "{shape}: native Anthropic error envelope expected, got {err}"
        );
        assert_eq!(
            err["error"]["message"],
            "request `messages` is missing or not an array and cannot be scanned",
            "{shape}: the shape reason must not masquerade as a parse failure"
        );
        assert!(
            !err.to_string().contains(secret),
            "{shape}: the block response must never echo client text"
        );
        assert!(
            captured.lock().await.is_empty(),
            "{shape}: not one byte may reach the upstream unscanned"
        );
    }
}

/// The non-regression half, and the reason the predicate keys on PRESENT-and-
/// wrong-shape rather than on "not an array": `proxy_handler` is the router's
/// `.fallback`, so a JSON body with no `messages` key reaches it routinely and
/// must still forward. Failing those closed takes every non-Messages endpoint
/// offline for `block` clients — this test goes red against exactly that.
///
/// The body carries no request text on purpose. `/v1/complete`'s `prompt` is
/// unscanned user content, and a test asserting that a body WITH content must
/// forward would be pinning a leak as correct.
#[cfg(feature = "guard")]
#[tokio::test]
async fn guard_block_forwards_fallback_json_body_without_messages() {
    let (upstream, captured) = spawn_guard_body_upstream().await;
    let state = Arc::new(AppState {
        endpoints: vec![mk_endpoint_at("acct", TEST_ENDPOINT_TOKEN, &upstream)],
        clients: vec![guard_block_client()],
        state_path: PathBuf::from("/tmp/anthropic-lb-guard-native-no-messages.state.json"),
        auto_cache: false,
        guard: crate::guard::Guard::new().expect("guard rules"),
        ..test_state_base()
    });
    let addr = serve(build_router(state)).await;

    // A non-Messages API shape: valid JSON, no `messages` key, no content.
    let raw = r#"{"model":"claude-2.1","max_tokens_to_sample":5}"#;
    let resp = Client::new()
        .post(format!("http://{addr}/v1/complete"))
        .header("content-type", "application/json")
        .header("x-api-key", "block-key")
        .body(raw)
        .send()
        .await
        .unwrap();

    assert_eq!(
        resp.status(),
        200,
        "a fallback body with no `messages` key must not fail closed under block"
    );
    assert_eq!(
        captured.lock().await.as_slice(),
        raw.as_bytes(),
        "the body must be forwarded byte-identically"
    );
}

/// LAB-4358: every `messages` shape the scanner cannot READ, with the AWS docs
/// example key placed where that unreadability hides it. One row per structural
/// failure; each is driven against BOTH surfaces by the two tests below, so the
/// two can never again disagree about which shapes are readable.
///
/// Every row yields the SAME reason on both surfaces. That is a property of the
/// fix, not a coincidence: readability is judged on the body the client sent, so
/// the translator can no longer turn "this is not a message" into "this role is
/// not one I know" on the way past.
#[cfg(feature = "guard")]
fn guard_unreadable_message_shapes(secret: &str) -> Vec<(String, serde_json::Value, &'static str)> {
    use crate::guard::{REASON_CONTENT_UNREADABLE, REASON_MESSAGE_UNREADABLE};
    let leak = format!("aws_secret_access_key = \"{secret}\"");
    let mut rows: Vec<(String, serde_json::Value, &'static str)> = vec![
        // The array element is not an object at all, so it has no role and the
        // scanner never looked inside it. The filed reproduction.
        (
            "element-not-object",
            serde_json::json!([leak]),
            REASON_MESSAGE_UNREADABLE,
        ),
        (
            "role-absent",
            serde_json::json!([{"content": leak}]),
            REASON_MESSAGE_UNREADABLE,
        ),
        (
            "role-not-a-string",
            serde_json::json!([{"role": 1, "content": leak}]),
            REASON_MESSAGE_UNREADABLE,
        ),
        // Role compare is exact, so `"User"` was not the newest user turn and
        // its content was never scanned. The filed reproduction.
        (
            "role-wrong-case",
            serde_json::json!([{"role": "User", "content": leak}]),
            REASON_MESSAGE_UNREADABLE,
        ),
        // Content is neither the shorthand string nor a block array. The filed
        // reproduction.
        (
            "content-object",
            serde_json::json!([{"role": "user", "content": {"text": leak}}]),
            REASON_CONTENT_UNREADABLE,
        ),
        // A fourth shape reported alongside these but not reproduced at the
        // time: a block that claims `type: text` whose `text` is not a string.
        (
            "text-block-text-not-a-string",
            serde_json::json!([{"role": "user", "content": [
                {"type": "text", "text": {"v": leak}}
            ]}]),
            REASON_CONTENT_UNREADABLE,
        ),
        (
            "content-block-not-object",
            serde_json::json!([{"role": "user", "content": [leak]}]),
            REASON_CONTENT_UNREADABLE,
        ),
        (
            "tool-result-content-object",
            serde_json::json!([{"role": "user", "content": [
                {"type": "tool_result", "tool_use_id": "t1", "content": {"v": leak}}
            ]}]),
            REASON_CONTENT_UNREADABLE,
        ),
        (
            "tool-result-inner-text-not-a-string",
            serde_json::json!([{"role": "user", "content": [
                {"type": "tool_result", "tool_use_id": "t1", "content": [
                    {"type": "text", "text": {"v": leak}}
                ]}
            ]}]),
            REASON_CONTENT_UNREADABLE,
        ),
    ]
    .into_iter()
    .map(|(shape, messages, reason)| (shape.to_string(), messages, reason))
    .collect();
    rows.extend(
        guard_unreadable_block_shapes(secret)
            .into_iter()
            .flat_map(|(shape, blocks)| in_newest_turn(shape, blocks))
            .map(|(shape, messages)| (shape, messages, REASON_CONTENT_UNREADABLE)),
    );
    rows
}

/// LAB-5542: `blocks` placed once as the newest user turn's content and once
/// inside a `tool_result.content` there — the two walks the scanner makes, and
/// the two places a skipped block type used to be forwarded unread.
#[cfg(feature = "guard")]
fn in_newest_turn(shape: &str, blocks: serde_json::Value) -> [(String, serde_json::Value); 2] {
    [
        (
            format!("{shape}/top-level"),
            serde_json::json!([{"role": "user", "content": blocks.clone()}]),
        ),
        (
            format!("{shape}/in-tool-result"),
            serde_json::json!([{"role": "user", "content": [
                {"type": "tool_result", "tool_use_id": "t1", "content": blocks}
            ]}]),
        ),
    ]
}

/// LAB-5542: content blocks the scanner cannot read, each carrying the secret
/// where that unreadability would hide it. Two kinds.
///
/// A block type the scanner does not know that carries a text-bearing field
/// (`text`, `content`, `source`). Skipping those forwarded whatever they held:
/// `{"type":"x","text":"<secret>"}` scanned as nothing.
///
/// A `document` or `search_result` — types the scanner now reads — in a shape
/// it cannot walk. Same rule as a `text` block whose `text` is not a string.
#[cfg(feature = "guard")]
fn guard_unreadable_block_shapes(secret: &str) -> Vec<(&'static str, serde_json::Value)> {
    let leak = format!("aws_secret_access_key = \"{secret}\"");
    let text_source = |data: serde_json::Value| serde_json::json!({"type": "text", "media_type": "text/plain", "data": data});
    let document =
        |source: serde_json::Value| serde_json::json!({"type": "document", "source": source});
    let search_result = |content: serde_json::Value| {
        serde_json::json!({
            "type": "search_result", "source": "https://example.com/a", "title": "A",
            "content": content
        })
    };
    vec![
        (
            "unknown-type-after-text",
            serde_json::json!([{"type": "text", "text": "hello"}, {"type": "x", "text": leak}]),
        ),
        (
            "unknown-type-input-text",
            serde_json::json!([{"type": "input_text", "text": leak}]),
        ),
        (
            "unknown-type-content",
            serde_json::json!([{"type": "x", "content": leak}]),
        ),
        (
            "tool-result-nested",
            serde_json::json!([{"type": "tool_result", "tool_use_id": "t2", "content": [
                {"type": "tool_result", "tool_use_id": "t3", "content": leak}
            ]}]),
        ),
        (
            "unknown-type-source",
            serde_json::json!([{"type": "x", "source": text_source(serde_json::json!(leak))}]),
        ),
        (
            "document-source-absent",
            serde_json::json!([{"type": "document", "title": "t", "data": leak}]),
        ),
        (
            "document-source-null",
            serde_json::json!([document(serde_json::Value::Null)]),
        ),
        (
            "document-source-not-object",
            serde_json::json!([document(serde_json::json!(leak))]),
        ),
        (
            "document-source-type-absent",
            serde_json::json!([document(serde_json::json!({"data": leak}))]),
        ),
        (
            "document-source-type-not-string",
            serde_json::json!([document(serde_json::json!({"type": 1, "data": leak}))]),
        ),
        (
            "document-source-type-unknown",
            serde_json::json!([document(
                serde_json::json!({"type": "markdown", "data": leak})
            )]),
        ),
        (
            "document-content-source-not-string-or-array",
            serde_json::json!([document(
                serde_json::json!({"type": "content", "content": {"v": leak}})
            )]),
        ),
        (
            "document-content-source-element-unknown",
            serde_json::json!([document(serde_json::json!({"type": "content", "content": [
                {"type": "document", "source": text_source(serde_json::json!(leak))}
            ]}))]),
        ),
        (
            "document-content-source-element-untyped",
            serde_json::json!([document(
                serde_json::json!({"type": "content", "content": [leak]})
            )]),
        ),
        (
            "document-content-source-text-not-string",
            serde_json::json!([document(serde_json::json!({"type": "content", "content": [
                {"type": "text", "text": {"v": leak}}
            ]}))]),
        ),
        (
            "document-text-source-data-not-string",
            serde_json::json!([document(text_source(serde_json::json!({"v": leak})))]),
        ),
        (
            "document-title-not-string",
            serde_json::json!([{
                "type": "document", "source": text_source(serde_json::json!("x")),
                "title": {"v": leak}
            }]),
        ),
        (
            "document-context-not-string",
            serde_json::json!([{
                "type": "document", "source": text_source(serde_json::json!("x")),
                "context": [leak]
            }]),
        ),
        (
            "search-result-content-not-array",
            serde_json::json!([search_result(serde_json::json!(leak))]),
        ),
        (
            "search-result-content-element-not-text",
            serde_json::json!([search_result(serde_json::json!([
                {"type": "document", "source": text_source(serde_json::json!(leak))}
            ]))]),
        ),
        (
            "search-result-text-not-string",
            serde_json::json!([search_result(
                serde_json::json!([{"type": "text", "text": {"v": leak}}])
            )]),
        ),
        (
            "search-result-source-not-string",
            serde_json::json!([{
                "type": "search_result", "source": {"v": leak}, "title": "A",
                "content": [{"type": "text", "text": "x"}]
            }]),
        ),
    ]
}

/// LAB-5542: `document` and `search_result` blocks, each carrying the secret in
/// ONE model-visible field, plus a `text` smuggled onto a non-text block. Every row must be scanned and blocked on its
/// findings, at top level and inside `tool_result.content`.
#[cfg(feature = "guard")]
fn guard_scanned_block_shapes(secret: &str) -> Vec<(&'static str, serde_json::Value)> {
    let leak = format!("aws_secret_access_key = \"{secret}\"");
    let text_source =
        |data: &str| serde_json::json!({"type": "text", "media_type": "text/plain", "data": data});
    let pdf = serde_json::json!({"type": "url", "url": "https://example.com/report.pdf"});
    vec![
        (
            "search-result-content-text",
            serde_json::json!([{
                "type": "search_result", "source": "https://example.com/a", "title": "A",
                "content": [{"type": "text", "text": "benign"}, {"type": "text", "text": leak}]
            }]),
        ),
        (
            "search-result-title",
            serde_json::json!([{
                "type": "search_result", "source": "https://example.com/a", "title": leak,
                "content": [{"type": "text", "text": "benign"}]
            }]),
        ),
        (
            "search-result-source",
            serde_json::json!([{
                "type": "search_result", "source": leak, "title": "A",
                "content": [{"type": "text", "text": "benign"}]
            }]),
        ),
        (
            "document-text-source",
            serde_json::json!([{"type": "document", "source": text_source(&leak)}]),
        ),
        (
            "document-content-source-string",
            serde_json::json!([{"type": "document", "source": {"type": "content", "content": leak}}]),
        ),
        (
            "document-content-source-array",
            serde_json::json!([{"type": "document", "source": {"type": "content", "content": [
                {"type": "text", "text": "benign"}, {"type": "text", "text": leak}
            ]}}]),
        ),
        // The image next to the text is skipped; the text beside it is not.
        (
            "document-content-source-mixed",
            serde_json::json!([{"type": "document", "source": {"type": "content", "content": [
                {"type": "image", "source": {"type": "url", "url": "https://example.com/a.png"}},
                {"type": "text", "text": leak}
            ]}}]),
        ),
        // `title` and `context` reach the model whatever the source, including
        // one whose own content the scanner cannot read.
        (
            "document-title",
            serde_json::json!([{"type": "document", "source": pdf, "title": leak}]),
        ),
        (
            "document-context",
            serde_json::json!([{"type": "document", "source": pdf, "context": leak}]),
        ),
        // Translation to an OpenAI upstream joins every `tool_result` part's
        // `text` whatever its type, so this reaches the model there.
        (
            "image-with-text",
            serde_json::json!([{
                "type": "image", "text": leak,
                "source": {"type": "url", "url": "https://example.com/a.png"}
            }]),
        ),
        // Same rule inside a document's content source.
        (
            "document-content-source-image-with-text",
            serde_json::json!([{"type": "document", "source": {"type": "content", "content": [
                {"type": "image", "text": leak, "source": {"type": "url", "url": "https://example.com/a.png"}}
            ]}}]),
        ),
    ]
}

/// LAB-4358: the shapes only the OpenAI wire format can express, one per LOSSY
/// path in `translate_openai_to_anthropic`. These are the rows the shared table
/// structurally cannot carry — an Anthropic body has no `role: "tool"` message
/// and no `image_url` part — and their absence is what hid this class through
/// two prior fixes on this code.
///
/// Each one translates into a readable-but-EMPTY document while the original
/// bytes keep the secret, so judging readability on the translated document
/// reports them CLEAN rather than merely unscanned. That is why
/// `from_openai_body` judges the original.
#[cfg(feature = "guard")]
fn guard_openai_translation_loss_shapes(secret: &str) -> Vec<(&'static str, serde_json::Value)> {
    let leak = format!("aws_secret_access_key = \"{secret}\"");
    vec![
        // `.unwrap_or_default()` in the `tool` arm → `tool_result` content "".
        (
            "tool-content-object",
            serde_json::json!([{"role": "tool", "tool_call_id": "t1", "content": {"v": leak}}]),
        ),
        // The same arm's array branch joins `p.get("text").as_str()`, so a
        // non-string `text` is dropped and the join yields "".
        (
            "tool-content-inner-text-not-a-string",
            serde_json::json!([{"role": "tool", "tool_call_id": "t1", "content": [
                {"type": "text", "text": {"v": leak}}
            ]}]),
        ),
        // `pointer("/image_url/url").unwrap_or("")` → an image block with an
        // empty url, which the scanner skips as an unread block type.
        (
            "image-url-not-an-object",
            serde_json::json!([{"role": "user", "content": [
                {"type": "image_url", "image_url": leak}
            ]}]),
        ),
        // LAB-5542: the `tool` arm keeps each part's `text` and nothing else, so
        // a part's `content` or `source` is dropped — with or without a `text`
        // beside it. A decoy `"text": ""` must not make the part look read.
        (
            "tool-part-search-result",
            serde_json::json!([{"role": "tool", "tool_call_id": "t1", "content": [
                {"type": "search_result", "source": "s", "title": "t",
                 "content": [{"type": "text", "text": leak}]}
            ]}]),
        ),
        (
            "tool-part-search-result-decoy-text",
            serde_json::json!([{"role": "tool", "tool_call_id": "t1", "content": [
                {"type": "search_result", "text": "", "source": "s", "title": "t",
                 "content": [{"type": "text", "text": leak}]}
            ]}]),
        ),
        (
            "tool-part-search-result-title-only",
            serde_json::json!([{"role": "tool", "tool_call_id": "t1", "content": [
                {"type": "search_result", "title": leak}
            ]}]),
        ),
        (
            "tool-part-text-with-source",
            serde_json::json!([{"role": "tool", "tool_call_id": "t1", "content": [
                {"type": "text", "text": "hello",
                 "source": {"type": "text", "media_type": "text/plain", "data": leak}}
            ]}]),
        ),
    ]
}

/// LAB-4358 AC2, native surface: every unreadable `messages` shape fails closed
/// on `/v1/messages` under `block`, with nothing on the wire.
///
/// On this surface the document the scanner reads IS the document forwarded, so
/// before the tri-state each of these returned 200 with the secret in the bytes
/// the proxy sent upstream. That the upstream would itself reject the shape is
/// no defence: the bytes have already left.
#[cfg(feature = "guard")]
#[tokio::test]
async fn guard_block_fails_closed_on_unreadable_native_messages() {
    let secret = AWS_DOCS_EXAMPLE_SECRET_KEY;
    let (upstream, captured) = spawn_guard_body_upstream().await;
    let state = Arc::new(AppState {
        endpoints: vec![mk_endpoint_at("acct", TEST_ENDPOINT_TOKEN, &upstream)],
        clients: vec![guard_block_client()],
        state_path: PathBuf::from("/tmp/anthropic-lb-guard-native-unreadable.state.json"),
        auto_cache: false,
        guard: crate::guard::Guard::new().expect("guard rules"),
        ..test_state_base()
    });
    let addr = serve(build_router(state)).await;

    for (shape, messages, reason) in guard_unreadable_message_shapes(secret) {
        let resp = Client::new()
            .post(format!("http://{addr}/v1/messages"))
            .header("content-type", "application/json")
            .header("x-api-key", "block-key")
            .json(&serde_json::json!({
                "model": "claude-sonnet-4-6", "max_tokens": 5, "messages": messages
            }))
            .send()
            .await
            .unwrap();

        assert_eq!(
            resp.status(),
            400,
            "{shape}: an unreadable `messages` must fail closed under block"
        );
        let err: serde_json::Value = resp.json().await.unwrap();
        assert_eq!(
            err["error"]["type"], "guard_blocked",
            "{shape}: native Anthropic error envelope expected, got {err}"
        );
        assert_eq!(
            err["error"]["message"], reason,
            "{shape}: wrong fail-closed reason"
        );
        assert!(
            !err.to_string().contains(secret),
            "{shape}: the block response must never echo client text"
        );
        assert!(
            captured.lock().await.is_empty(),
            "{shape}: not one byte may reach the upstream unscanned"
        );
    }
}

/// LAB-4358 AC2, OpenAI surface: the same table, same fail-closed outcome, on
/// `/v1/chat/completions` against a `Protocol::OpenAI` endpoint — the arm that
/// forwards the client's ORIGINAL bytes, so an unscannable body reaching it
/// ships every character the scanner could not read.
///
/// Running one table across both surfaces is the actual regression guard here.
/// The bypass this closes existed because each handler derived its own idea of
/// an unreadable `messages`, and `[...]`-wrapping a payload walked between them.
#[cfg(feature = "guard")]
#[tokio::test]
async fn guard_block_fails_closed_on_unreadable_openai_messages() {
    let secret = AWS_DOCS_EXAMPLE_SECRET_KEY;
    let (upstream, captured) = spawn_guard_body_upstream().await;
    let mut gw = make_endpoint("gw", Protocol::OpenAI);
    gw.base_url = upstream;
    let state = Arc::new(AppState {
        endpoints: vec![gw],
        clients: vec![guard_block_client()],
        state_path: PathBuf::from("/tmp/anthropic-lb-guard-openai-unreadable.state.json"),
        auto_cache: false,
        guard: crate::guard::Guard::new().expect("guard rules"),
        ..test_state_base()
    });
    let addr = serve(build_router(state)).await;

    let rows = guard_unreadable_message_shapes(secret)
        .into_iter()
        // The OpenAI-only losses carry no per-surface reason of their own; every
        // one is an unreadable CONTENT shape in the original body.
        .chain(
            guard_openai_translation_loss_shapes(secret)
                .into_iter()
                .map(|(shape, messages)| {
                    (
                        shape.to_string(),
                        messages,
                        crate::guard::REASON_CONTENT_UNREADABLE,
                    )
                }),
        );
    for (shape, messages, reason) in rows {
        let resp = Client::new()
            .post(format!("http://{addr}/v1/chat/completions"))
            .header("content-type", "application/json")
            .header("authorization", "Bearer block-key")
            .json(&serde_json::json!({
                "model": "claude-sonnet-4-6", "max_tokens": 5, "messages": messages
            }))
            .send()
            .await
            .unwrap();

        assert_eq!(
            resp.status(),
            400,
            "{shape}: an unreadable `messages` must fail closed under block"
        );
        let err: serde_json::Value = resp.json().await.unwrap();
        assert_eq!(
            err["error"]["code"], "guard_blocked",
            "{shape}: OpenAI error envelope expected, got {err}"
        );
        assert_eq!(
            err["error"]["message"], reason,
            "{shape}: wrong fail-closed reason"
        );
        assert!(
            !err.to_string().contains(secret),
            "{shape}: the block response must never echo client text"
        );
        assert!(
            captured.lock().await.is_empty(),
            "{shape}: not one byte may reach a Protocol::OpenAI upstream unscanned"
        );
    }
}

/// LAB-5542: `document` and `search_result` content is scanned under `block`.
/// Each row carries the secret in one model-visible field and must be rejected
/// on its findings with nothing on the wire — on `/v1/messages` at top level and
/// inside `tool_result.content`, and on `/v1/chat/completions` as a `user`
/// content part, which translation clones verbatim.
#[cfg(feature = "guard")]
#[tokio::test]
async fn guard_block_scans_document_and_search_result_blocks() {
    let secret = AWS_DOCS_EXAMPLE_SECRET_KEY;
    let (upstream, captured) = spawn_guard_body_upstream().await;
    let state = Arc::new(AppState {
        endpoints: vec![mk_endpoint_at("acct", TEST_ENDPOINT_TOKEN, &upstream)],
        clients: vec![guard_block_client()],
        state_path: PathBuf::from("/tmp/anthropic-lb-guard-document-blocks.state.json"),
        auto_cache: false,
        guard: crate::guard::Guard::new().expect("guard rules"),
        ..test_state_base()
    });
    let native_addr = serve(build_router(state)).await;

    let mut gw = make_endpoint("gw", Protocol::OpenAI);
    gw.base_url = upstream;
    let openai_state = Arc::new(AppState {
        endpoints: vec![gw],
        clients: vec![guard_block_client()],
        state_path: PathBuf::from("/tmp/anthropic-lb-guard-document-blocks-openai.state.json"),
        auto_cache: false,
        guard: crate::guard::Guard::new().expect("guard rules"),
        ..test_state_base()
    });
    let openai_addr = serve(build_router(openai_state)).await;

    let rows = guard_scanned_block_shapes(secret)
        .into_iter()
        .flat_map(|(shape, blocks)| in_newest_turn(shape, blocks));
    for (shape, messages) in rows {
        let body = serde_json::json!({
            "model": "claude-sonnet-4-6", "max_tokens": 5, "messages": messages
        });
        for (surface, addr, path, header, value, kind) in [
            (
                "native",
                native_addr,
                "/v1/messages",
                "x-api-key",
                "block-key",
                "type",
            ),
            (
                "openai",
                openai_addr,
                "/v1/chat/completions",
                "authorization",
                "Bearer block-key",
                "code",
            ),
        ] {
            let resp = Client::new()
                .post(format!("http://{addr}{path}"))
                .header("content-type", "application/json")
                .header(header, value)
                .json(&body)
                .send()
                .await
                .unwrap();
            assert_eq!(
                resp.status(),
                400,
                "{surface}/{shape}: the secret must be found and blocked"
            );
            let err: serde_json::Value = resp.json().await.unwrap();
            assert_eq!(
                err["error"][kind], "guard_blocked",
                "{surface}/{shape}: got {err}"
            );
            assert!(
                err["error"]["findings"]
                    .as_array()
                    .is_some_and(|f| !f.is_empty()),
                "{surface}/{shape}: blocked on findings, not a fail-closed reason: {err}"
            );
            assert!(
                captured.lock().await.is_empty(),
                "{surface}/{shape}: not one byte may reach the upstream"
            );
        }
    }
}

/// LAB-5542: document text counts toward the scan limit like any other text. A
/// newest turn whose document text alone exceeds it fails closed under `block`,
/// so padding a document past the limit cannot hide a secret in its tail.
#[cfg(feature = "guard")]
#[tokio::test]
async fn guard_block_fails_closed_on_oversized_document_text() {
    let (upstream, captured) = spawn_guard_body_upstream().await;
    let state = Arc::new(AppState {
        endpoints: vec![mk_endpoint_at("acct", TEST_ENDPOINT_TOKEN, &upstream)],
        clients: vec![guard_block_client()],
        state_path: PathBuf::from("/tmp/anthropic-lb-guard-oversized-document.state.json"),
        auto_cache: false,
        guard: crate::guard::Guard::new().expect("guard rules"),
        ..test_state_base()
    });
    let addr = serve(build_router(state)).await;

    let data = "x ".repeat(crate::guard::MAX_SCAN_BYTES);
    let resp = Client::new()
        .post(format!("http://{addr}/v1/messages"))
        .header("content-type", "application/json")
        .header("x-api-key", "block-key")
        .json(&serde_json::json!({
            "model": "claude-sonnet-4-6", "max_tokens": 5,
            "messages": [{"role": "user", "content": [
                {"type": "document", "source": {"type": "text", "media_type": "text/plain", "data": data}}
            ]}]
        }))
        .send()
        .await
        .unwrap();

    assert_eq!(
        resp.status(),
        400,
        "oversized document text must fail closed under block"
    );
    let err: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(err["error"]["type"], "guard_blocked");
    assert_eq!(err["error"]["message"], crate::guard::REASON_SCAN_TRUNCATED);
    assert!(
        captured.lock().await.is_empty(),
        "nothing may reach the upstream"
    );
}

/// LAB-4358 AC5: the fail-closed widening is gated on `block`. Under `annotate`
/// AND `off` every row of the table still forwards, and the bytes on the wire
/// are byte-identical to the client's — the prompt-cache raw-prefix invariant
/// that makes this guard safe to run at all. Asserted on both surfaces, on the
/// arms that forward original bytes.
///
/// Every row, not a representative sample: AC5 pins the whole table, and the
/// expensive fixture (`Guard::new()`) is already shared across them.
#[cfg(feature = "guard")]
#[tokio::test]
async fn guard_non_block_forwards_block_rejected_bodies_byte_identically() {
    let secret = AWS_DOCS_EXAMPLE_SECRET_KEY;
    let (upstream, captured) = spawn_guard_body_upstream().await;
    let mut gw = make_endpoint("gw", Protocol::OpenAI);
    gw.base_url = upstream.clone();
    let state = Arc::new(AppState {
        endpoints: vec![mk_endpoint_at("acct", TEST_ENDPOINT_TOKEN, &upstream)],
        clients: guard_non_block_clients(),
        state_path: PathBuf::from("/tmp/anthropic-lb-guard-native-unreadable-shadow.state.json"),
        auto_cache: false,
        guard: crate::guard::Guard::new().expect("guard rules"),
        ..test_state_base()
    });
    let native_addr = serve(build_router(state)).await;

    let openai_state = Arc::new(AppState {
        endpoints: vec![gw],
        clients: guard_non_block_clients(),
        state_path: PathBuf::from("/tmp/anthropic-lb-guard-openai-unreadable-shadow.state.json"),
        auto_cache: false,
        guard: crate::guard::Guard::new().expect("guard rules"),
        ..test_state_base()
    });
    let openai_addr = serve(build_router(openai_state)).await;

    // LAB-5542 widens what `block` rejects to the OpenAI `tool`-arm losses and
    // to bodies that fail on findings in newly scanned blocks; none of them may
    // change a byte under a non-block policy.
    let rows = guard_unreadable_message_shapes(secret)
        .into_iter()
        .map(|(shape, messages, _)| (shape, messages))
        .chain(
            guard_openai_translation_loss_shapes(secret)
                .into_iter()
                .map(|(shape, messages)| (shape.to_string(), messages)),
        )
        .chain(
            guard_scanned_block_shapes(secret)
                .into_iter()
                .flat_map(|(shape, blocks)| in_newest_turn(shape, blocks)),
        );
    for (shape, messages) in rows {
        let body = serde_json::json!({
            "model": "claude-sonnet-4-6", "max_tokens": 5, "messages": messages
        });
        // Compare against the exact bytes reqwest serializes, not a re-encode.
        let raw = serde_json::to_vec(&body).unwrap();

        for (surface, addr, path, header, value, policy) in [
            (
                "native",
                native_addr,
                "/v1/messages",
                "x-api-key",
                "annotate-key",
                "annotate",
            ),
            (
                "native",
                native_addr,
                "/v1/messages",
                "x-api-key",
                "off-key",
                "off",
            ),
            (
                "openai",
                openai_addr,
                "/v1/chat/completions",
                "authorization",
                "Bearer annotate-key",
                "annotate",
            ),
            (
                "openai",
                openai_addr,
                "/v1/chat/completions",
                "authorization",
                "Bearer off-key",
                "off",
            ),
        ] {
            captured.lock().await.clear();
            let resp = Client::new()
                .post(format!("http://{addr}{path}"))
                .header("content-type", "application/json")
                .header(header, value)
                .body(raw.clone())
                .send()
                .await
                .unwrap();
            assert_eq!(
                resp.status(),
                200,
                "{surface}/{policy}/{shape}: a non-block policy must never reject"
            );
            assert_eq!(
                captured.lock().await.as_slice(),
                raw.as_slice(),
                "{surface}/{policy}/{shape}: the body must forward byte-identically"
            );
        }
    }
}

/// LAB-4358 AC3, invariant 1: an image-only turn is NOT a scan failure. The
/// document is readable and genuinely carries no text this scanner reads, so it
/// forwards untouched even under `block` — the deliberate allow the tri-state
/// exists to preserve while everything above it fails closed. Pinned on both
/// surfaces; a fix that closed the bypasses by treating "no text" as "could not
/// read" goes red here.
///
/// LAB-5542 widens the table to the rest of the coverage boundary. The
/// unknown-type rule must not turn any row into a 400: `image` carries a
/// `source` field, and an `image` inside `tool_result.content` is the everyday
/// tool-screenshot shape.
#[cfg(feature = "guard")]
#[tokio::test]
async fn guard_block_forwards_coverage_boundary_blocks() {
    let (upstream, captured) = spawn_guard_body_upstream().await;
    let state = Arc::new(AppState {
        endpoints: vec![mk_endpoint_at("acct", TEST_ENDPOINT_TOKEN, &upstream)],
        clients: vec![guard_block_client()],
        state_path: PathBuf::from("/tmp/anthropic-lb-guard-image-only.state.json"),
        auto_cache: false,
        guard: crate::guard::Guard::new().expect("guard rules"),
        ..test_state_base()
    });
    let native_addr = serve(build_router(state)).await;

    let mut gw = make_endpoint("gw", Protocol::OpenAI);
    gw.base_url = upstream;
    let openai_state = Arc::new(AppState {
        endpoints: vec![gw],
        clients: vec![guard_block_client()],
        state_path: PathBuf::from("/tmp/anthropic-lb-guard-image-only-openai.state.json"),
        auto_cache: false,
        guard: crate::guard::Guard::new().expect("guard rules"),
        ..test_state_base()
    });
    let openai_addr = serve(build_router(openai_state)).await;

    // 1x1 transparent PNG.
    let png = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNk\
               YPhfDwAChwGA60e6kgAAAABJRU5ErkJggg==";
    let image = serde_json::json!(
        {"type": "image", "source": {"type": "base64", "media_type": "image/png", "data": png}}
    );
    let benign_document = |source: serde_json::Value| {
        serde_json::json!([{
            "type": "document", "source": source,
            "title": "Quarterly report", "context": "Shared by the finance team"
        }])
    };

    let mut native_rows: Vec<(String, serde_json::Value)> =
        in_newest_turn("image-only", serde_json::json!([image.clone()])).into();
    for (shape, source) in [
        (
            "document-base64",
            serde_json::json!({"type": "base64", "media_type": "application/pdf", "data": "JVBERi0xLjQK"}),
        ),
        (
            "document-url",
            serde_json::json!({"type": "url", "url": "https://example.com/report.pdf"}),
        ),
        (
            "document-file",
            serde_json::json!({"type": "file", "file_id": "file_011CNha8iCJcU1wXNR6q4V8w"}),
        ),
    ] {
        native_rows.extend(in_newest_turn(shape, benign_document(source)));
    }
    native_rows.extend(in_newest_turn(
        "document-content-source-mixed",
        serde_json::json!([{"type": "document", "source": {"type": "content", "content": [
            image.clone(), {"type": "text", "text": "The chart above shows revenue by quarter."}
        ]}}]),
    ));
    for (shape, block) in [
        (
            "tool-reference",
            serde_json::json!({"type": "tool_reference", "tool_name": "get_weather"}),
        ),
        (
            "browser-state",
            serde_json::json!({"type": "browser_state", "tabs": [
                {"tab_id": 1, "title": "Example Domain", "url": "https://example.com/"}
            ]}),
        ),
    ] {
        native_rows.extend(in_newest_turn(shape, serde_json::json!([block])));
    }

    let openai_rows: Vec<(String, serde_json::Value)> = vec![
        (
            "image-url-only".to_string(),
            serde_json::json!([{"role": "user", "content": [
                {"type": "image_url", "image_url": {"url": format!("data:image/png;base64,{png}")}}
            ]}]),
        ),
        (
            "input-audio".to_string(),
            serde_json::json!([{"role": "user", "content": [
                {"type": "input_audio", "input_audio": {"data": "UklGRiQAAABXQVZF", "format": "wav"}}
            ]}]),
        ),
        (
            "file".to_string(),
            serde_json::json!([{"role": "user", "content": [
                {"type": "file", "file": {"file_id": "file-abc123"}}
            ]}]),
        ),
    ];

    let rows = native_rows
        .into_iter()
        .map(|(shape, messages)| {
            (
                "native",
                shape,
                native_addr,
                "/v1/messages",
                "x-api-key",
                "block-key",
                messages,
            )
        })
        .chain(openai_rows.into_iter().map(|(shape, messages)| {
            (
                "openai",
                shape,
                openai_addr,
                "/v1/chat/completions",
                "authorization",
                "Bearer block-key",
                messages,
            )
        }));
    for (surface, shape, addr, path, header, value, messages) in rows {
        let raw = serde_json::to_vec(&serde_json::json!({
            "model": "claude-sonnet-4-6", "max_tokens": 5, "messages": messages
        }))
        .unwrap();
        captured.lock().await.clear();
        let resp = Client::new()
            .post(format!("http://{addr}{path}"))
            .header("content-type", "application/json")
            .header(header, value)
            .body(raw.clone())
            .send()
            .await
            .unwrap();
        assert_eq!(
            resp.status(),
            200,
            "{surface}/{shape}: nothing unreadable and nothing found, so it must forward under block"
        );
        assert_eq!(
            captured.lock().await.as_slice(),
            raw.as_slice(),
            "{surface}/{shape}: the body must forward byte-identically"
        );
    }
}

/// LAB-4358 AC3, invariant 2: a conversation with no `user` turn yet is
/// readable and empty, not unreadable. Same deliberate allow as the image-only
/// turn, and the case a strict "every message must be a user message the
/// scanner read" rule would break.
#[cfg(feature = "guard")]
#[tokio::test]
async fn guard_block_forwards_conversation_without_user_turn() {
    let (upstream, captured) = spawn_guard_body_upstream().await;
    let state = Arc::new(AppState {
        endpoints: vec![mk_endpoint_at("acct", TEST_ENDPOINT_TOKEN, &upstream)],
        clients: vec![guard_block_client()],
        state_path: PathBuf::from("/tmp/anthropic-lb-guard-no-user-turn.state.json"),
        auto_cache: false,
        guard: crate::guard::Guard::new().expect("guard rules"),
        ..test_state_base()
    });
    let native_addr = serve(build_router(state)).await;

    let mut gw = make_endpoint("gw", Protocol::OpenAI);
    gw.base_url = upstream;
    let openai_state = Arc::new(AppState {
        endpoints: vec![gw],
        clients: vec![guard_block_client()],
        state_path: PathBuf::from("/tmp/anthropic-lb-guard-no-user-turn-openai.state.json"),
        auto_cache: false,
        guard: crate::guard::Guard::new().expect("guard rules"),
        ..test_state_base()
    });
    let openai_addr = serve(build_router(openai_state)).await;

    // An assistant prefill, plus (on the OpenAI surface) a `system` message the
    // translator hoists out of `messages` entirely — the shape that leaves the
    // scanned document with an empty `messages` for an entirely legitimate
    // reason, and so must not be confused with the translator's empty-array
    // rewrite of an unreadable one.
    let native = serde_json::to_vec(&serde_json::json!({
        "model": "claude-sonnet-4-6", "max_tokens": 5,
        "messages": [{"role": "assistant", "content": "Here is my answer:"}]
    }))
    .unwrap();
    let openai = serde_json::to_vec(&serde_json::json!({
        "model": "claude-sonnet-4-6", "max_tokens": 5,
        "messages": [
            {"role": "system", "content": "you are a helpful assistant"},
            {"role": "assistant", "content": "Here is my answer:"}
        ]
    }))
    .unwrap();

    for (surface, addr, path, header, value, raw) in [
        (
            "native",
            native_addr,
            "/v1/messages",
            "x-api-key",
            "block-key",
            &native,
        ),
        (
            "openai",
            openai_addr,
            "/v1/chat/completions",
            "authorization",
            "Bearer block-key",
            &openai,
        ),
    ] {
        captured.lock().await.clear();
        let resp = Client::new()
            .post(format!("http://{addr}{path}"))
            .header("content-type", "application/json")
            .header(header, value)
            .body(raw.clone())
            .send()
            .await
            .unwrap();
        assert_eq!(
            resp.status(),
            200,
            "{surface}: a conversation with no user turn must forward under block"
        );
        assert_eq!(
            captured.lock().await.as_slice(),
            raw.as_slice(),
            "{surface}: the body must forward byte-identically"
        );
    }
}

/// LAB-4358: `guard_hook` against every `ScanOutcome` under every policy,
/// called directly. The tables above pin rejection vs forwarding through the
/// handlers; what only this pins is the fail-closed LOG, emitted under every
/// policy (`would-block` at INFO when the policy does not enforce, `block` at
/// WARN when it does) so shadow mode can size a `block` rollout.
/// `Guard::empty()` suffices: the log fires before `evaluate`, and a
/// scanner-less guard never blocks, so every rejection seen here comes from
/// the fail-closed path.
#[cfg(feature = "guard")]
#[test]
fn guard_hook_logs_fail_closed_cause_under_every_policy() {
    use crate::guard::{
        ScanInput, ScanOutcome, MAX_SCAN_BYTES, REASON_MESSAGE_UNREADABLE, REASON_SCAN_TRUNCATED,
    };
    let buf = log_capture_buf();
    let mut clients = guard_non_block_clients();
    clients.push(guard_block_client());
    let state = AppState {
        clients,
        ..test_state_base()
    };
    let user_turn = |len: usize| {
        ScanInput::from_body(&serde_json::json!({
            "messages": [{"role": "user", "content": "a".repeat(len)}]
        }))
    };
    // (label, outcome, the fail-closed cause it must log, if any)
    let cases = [
        (
            "unscannable",
            ScanOutcome::Unscannable(REASON_MESSAGE_UNREADABLE),
            Some(REASON_MESSAGE_UNREADABLE),
        ),
        (
            "truncated",
            user_turn(MAX_SCAN_BYTES + 1),
            Some(REASON_SCAN_TRUNCATED),
        ),
        ("scannable", user_turn(16), None),
        ("nothing-to-scan", ScanOutcome::NothingToScan, None),
    ];
    for (label, outcome, cause) in &cases {
        for (client, blocks) in [
            ("blocked-client", true),
            ("annotate", false),
            ("off", false),
        ] {
            let req_id = format!("lab4358-hook-{label}-{client}");
            let rejected = state.guard_hook(&req_id, client, outcome, false).is_err();
            assert_eq!(
                rejected,
                blocks && cause.is_some(),
                "{req_id}: reject iff block AND a fail-closed cause"
            );

            let marker = format!("req_id=\"{req_id}\"");
            let output = String::from_utf8(buf.lock().unwrap().clone()).unwrap();
            let lines: Vec<&str> = output
                .lines()
                .filter(|l| l.contains(&marker) && l.contains("guard: unscannable request"))
                .collect();
            let Some(cause) = cause else {
                assert!(
                    lines.is_empty(),
                    "{req_id}: no fail-closed cause, so no line, got {lines:?}"
                );
                continue;
            };
            let (level, verdict) = if blocks {
                (" WARN ", r#"verdict="block""#)
            } else {
                (" INFO ", r#"verdict="would-block""#)
            };
            assert!(
                lines.len() == 1
                    && lines[0].contains(level)
                    && lines[0].contains(verdict)
                    && lines[0].contains(cause),
                "{req_id}: expected one{level}{verdict} line naming the cause, got {lines:?}"
            );
        }
    }
}

/// Shadow-mode counterpart on the OpenAI-compat surface: `annotate` forwards
/// the request and stamps `X-Guard-Findings` on the translated response.
#[cfg(feature = "guard")]
#[tokio::test]
async fn guard_annotate_stamps_header_on_openai_chat_completions() {
    let (upstream, captured) = spawn_guard_body_upstream().await;
    let state = Arc::new(AppState {
        endpoints: vec![mk_endpoint_at("acct", TEST_ENDPOINT_TOKEN, &upstream)],
        state_path: PathBuf::from("/tmp/anthropic-lb-guard-openai-annotate.state.json"),
        auto_cache: false,
        guard: crate::guard::Guard::new().expect("guard rules"),
        ..test_state_base()
    });
    let app = build_router(state);
    let addr = serve(app).await;

    let resp = Client::new()
        .post(format!("http://{addr}/v1/chat/completions"))
        .header("content-type", "application/json")
        .json(&serde_json::json!({
            "model": "claude-sonnet-4-6",
            "messages": [{"role": "user", "content": "email me at ops@example.com"}],
            "max_tokens": 5
        }))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200, "annotate must never reject");
    let count: usize = resp
        .headers()
        .get("x-guard-findings")
        .expect("annotate must stamp X-Guard-Findings")
        .to_str()
        .unwrap()
        .parse()
        .unwrap();
    assert!(count >= 1, "expected at least one finding, got {count}");
    assert!(
        !captured.lock().await.is_empty(),
        "annotate forwards the request upstream"
    );
}
