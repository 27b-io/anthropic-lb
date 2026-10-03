//! A stream that ends before its upstream reports usage is charged to the
//! client's budget conservatively (LAB-7593): the request's `max_tokens` when
//! no `message_delta` arrived, plus a body estimate when no `message_start`
//! did, and nothing when the upstream errored before `message_start`. One
//! test per ending, each over both routes that relay an Anthropic stream:
//! native `/v1/messages` and the OpenAI-compatible `/v1/chat/completions`.

use super::*;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const CLIENT: &str = "budgeted";
const MAX_TOKENS: u64 = 50;
const MESSAGES_BODY: &str = r#"{"model":"claude-sonnet-4-6","max_tokens":50,"stream":true,"messages":[{"role":"user","content":"hi"}]}"#;
const CHAT_BODY: &str = r#"{"model":"claude-sonnet-4-6","max_tokens":50,"stream":true,"messages":[{"role":"user","content":"hi"}]}"#;
const ROUTES: [(&str, &str); 2] = [
    ("/v1/messages", MESSAGES_BODY),
    ("/v1/chat/completions", CHAT_BODY),
];

const MESSAGE_START: &str = "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_1\",\"model\":\"claude-sonnet-4-6\",\"usage\":{\"input_tokens\":7,\"cache_read_input_tokens\":3,\"output_tokens\":1}}}\n\n";
/// `MESSAGE_START`'s input + cache-read tokens.
const SCANNED_INPUT: u64 = 10;
const BLOCK_START: &str = "event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\n";
const DELTA: &str = "event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"marker\"}}\n\n";
const MESSAGE_DELTA: &str = "event: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":5}}\n\n";
const MESSAGE_STOP: &str = "event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n";
const ERROR: &str = "event: error\ndata: {\"type\":\"error\",\"error\":{\"type\":\"overloaded_error\",\"message\":\"Overloaded\"}}\n\n";

/// How the scripted upstream ends after its opening frames.
enum Tail {
    /// Chunked terminator: the upstream ends the response cleanly.
    Close,
    /// `DELTA` repeated until a write fails — until the proxy drops the
    /// upstream, which it does once the client has gone.
    Endless,
}

/// Raw-TCP upstream answering one request with a chunked SSE stream. Returns
/// its URL and the request body it receives, which is what the proxy sends
/// upstream after any rewrite or translation.
async fn spawn_scripted_upstream(
    frames: Vec<&'static str>,
    tail: Tail,
) -> (String, Arc<Mutex<Option<Vec<u8>>>>) {
    let captured = Arc::new(Mutex::new(None));
    let slot = captured.clone();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let (mut s, _) = listener.accept().await.unwrap();
        let mut req = Vec::new();
        let mut buf = vec![0u8; 8192];
        let body_start = loop {
            let n = s.read(&mut buf).await.unwrap();
            assert!(n > 0, "proxy closed before sending its request");
            req.extend_from_slice(&buf[..n]);
            if let Some(i) = req.windows(4).position(|w| w == b"\r\n\r\n") {
                break i + 4;
            }
        };
        let head = String::from_utf8_lossy(&req[..body_start]).to_ascii_lowercase();
        let len: usize = head
            .lines()
            .find_map(|l| l.strip_prefix("content-length:"))
            .expect("proxy sends a content-length")
            .trim()
            .parse()
            .unwrap();
        while req.len() < body_start + len {
            let n = s.read(&mut buf).await.unwrap();
            assert!(n > 0, "proxy closed mid-body");
            req.extend_from_slice(&buf[..n]);
        }
        *slot.lock().unwrap() = Some(req[body_start..body_start + len].to_vec());

        let chunk = |f: &str| format!("{:x}\r\n{f}\r\n", f.len());
        let mut out = String::from(
            "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ntransfer-encoding: chunked\r\n\r\n",
        );
        for f in frames {
            out.push_str(&chunk(f));
        }
        if s.write_all(out.as_bytes()).await.is_err() {
            return;
        }
        match tail {
            Tail::Close => {
                let _ = s.write_all(b"0\r\n\r\n").await;
            }
            Tail::Endless => {
                let delta = chunk(DELTA);
                while s.write_all(delta.as_bytes()).await.is_ok() {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            }
        }
    });
    (format!("http://{addr}"), captured)
}

fn budget_state(url: &str) -> Arc<AppState> {
    Arc::new(AppState {
        endpoints: vec![mk_endpoint_at("acct-a", "sk-ant-api-a", url)],
        auto_cache: false,
        client_budgets: [(CLIENT.to_string(), 10_000_000)].into_iter().collect(),
        client: upstream_client_builder()
            .timeout(Duration::from_secs(60))
            .build()
            .unwrap(),
        ..test_state_base()
    })
}

/// Send a streaming request on a raw socket, read until the relayed content
/// shows up (or the response ends), then drop the connection.
async fn request_then_disconnect(addr: SocketAddr, path: &str, body: &str) {
    let mut s = tokio::net::TcpStream::connect(addr).await.unwrap();
    let req = format!(
        "POST {path} HTTP/1.1\r\nhost: lb\r\ncontent-type: application/json\r\nx-client-id: {CLIENT}\r\ncontent-length: {}\r\n\r\n{body}",
        body.len()
    );
    s.write_all(req.as_bytes()).await.unwrap();
    let mut got = Vec::new();
    let mut buf = vec![0u8; 8192];
    tokio::time::timeout(Duration::from_secs(10), async {
        while !got.windows(6).any(|w| w == b"marker") {
            match s.read(&mut buf).await {
                Ok(0) | Err(_) => break,
                Ok(n) => got.extend_from_slice(&buf[..n]),
            }
        }
    })
    .await
    .expect("relayed content within 10 s");
    assert!(
        got.starts_with(b"HTTP/1.1 200"),
        "client got the stream's head"
    );
}

/// Send a streaming request and read the response to its end.
async fn request_to_end(addr: SocketAddr, path: &str, body: &str) {
    let resp = Client::new()
        .post(format!("http://{addr}{path}"))
        .header("content-type", "application/json")
        .header("x-client-id", CLIENT)
        .body(body.to_owned())
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    resp.text().await.unwrap();
}

/// The client's budget usage once it reaches `expected` (finalization runs
/// in the relay's detached task), or whatever it settled at after 5 s. Then
/// a further pause, so a charge landing after the expected one is caught.
async fn settled_budget(state: &AppState, expected: u64) -> u64 {
    let used = || {
        state
            .budget_usage
            .lock()
            .unwrap()
            .get(CLIENT)
            .map_or(0, |&(_, used)| used)
    };
    let deadline = Instant::now() + Duration::from_secs(5);
    while used() != expected && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    tokio::time::sleep(Duration::from_millis(100)).await;
    used()
}

fn input_estimate(captured: &Mutex<Option<Vec<u8>>>) -> u64 {
    let body = captured
        .lock()
        .unwrap()
        .clone()
        .expect("upstream got the request");
    body.len().div_ceil(4) as u64
}

#[tokio::test]
async fn complete_stream_is_charged_its_reported_usage() {
    for (path, body) in ROUTES {
        let (url, _) = spawn_scripted_upstream(
            vec![
                MESSAGE_START,
                BLOCK_START,
                DELTA,
                MESSAGE_DELTA,
                MESSAGE_STOP,
            ],
            Tail::Close,
        )
        .await;
        let state = budget_state(&url);
        let addr = serve(build_router(state.clone())).await;
        request_to_end(addr, path, body).await;
        assert_eq!(
            settled_budget(&state, SCANNED_INPUT + 5).await,
            SCANNED_INPUT + 5,
            "{path}: a complete stream is charged exactly what the upstream reported"
        );
    }
}

#[tokio::test]
async fn disconnect_after_content_charges_max_tokens_as_output() {
    for (path, body) in ROUTES {
        let (url, _) =
            spawn_scripted_upstream(vec![MESSAGE_START, BLOCK_START, DELTA], Tail::Endless).await;
        let state = budget_state(&url);
        let addr = serve(build_router(state.clone())).await;
        request_then_disconnect(addr, path, body).await;
        let expected = SCANNED_INPUT + MAX_TOKENS;
        assert_eq!(
            settled_budget(&state, expected).await,
            expected,
            "{path}: a client gone before message_delta pays max_tokens for the output"
        );
        // The token counters keep what the upstream reported.
        assert_eq!(
            state.client_usage.lock().unwrap().get(CLIENT),
            Some(&[7, 0, 0, 3]),
            "{path}"
        );
    }
}

#[tokio::test]
async fn disconnect_before_message_start_charges_body_estimate_and_max_tokens() {
    for (path, body) in ROUTES {
        let (url, captured) =
            spawn_scripted_upstream(vec![BLOCK_START, DELTA], Tail::Endless).await;
        let state = budget_state(&url);
        let addr = serve(build_router(state.clone())).await;
        request_then_disconnect(addr, path, body).await;
        let expected = input_estimate(&captured) + MAX_TOKENS;
        assert_eq!(
            settled_budget(&state, expected).await,
            expected,
            "{path}: with no message_start the input is estimated from the body sent upstream"
        );
        assert!(
            !state.client_usage.lock().unwrap().contains_key(CLIENT),
            "{path}: no usage was reported, so the token counters record none"
        );
    }
}

#[tokio::test]
async fn upstream_error_mid_stream_charges_max_tokens_as_output() {
    for (path, body) in ROUTES {
        let (url, _) =
            spawn_scripted_upstream(vec![MESSAGE_START, BLOCK_START, DELTA, ERROR], Tail::Close)
                .await;
        let state = budget_state(&url);
        let addr = serve(build_router(state.clone())).await;
        request_to_end(addr, path, body).await;
        let expected = SCANNED_INPUT + MAX_TOKENS;
        assert_eq!(
            settled_budget(&state, expected).await,
            expected,
            "{path}: a stream the upstream errored before message_delta pays max_tokens"
        );
    }
}

#[tokio::test]
async fn upstream_error_before_message_start_charges_nothing() {
    for (path, body) in ROUTES {
        let (url, _) = spawn_scripted_upstream(vec![ERROR], Tail::Close).await;
        let state = budget_state(&url);
        let addr = serve(build_router(state.clone())).await;
        // The response ends only once its relay task, finalization
        // included, has finished: any charge has landed by then.
        request_to_end(addr, path, body).await;
        assert_eq!(
            settled_budget(&state, 0).await,
            0,
            "{path}: an upstream that errored before generating anything charges nothing"
        );
    }
}
