//! Stalled consumers: a client that keeps its socket open but stops reading
//! must lose its stream within `DOWNSTREAM_SEND_TIMEOUT`, so its relay drops
//! the upstream response instead of pinning it unread. One test per relay
//! route shape, plus a guard that the bound never caps a stream that is
//! being read.

use super::*;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// Log message of the relay's stall exit (`relay_send`).
const STALL_WARN: &str = "client stopped reading the stream";

const MESSAGES_BODY: &str = r#"{"model":"claude-sonnet-4-6","max_tokens":16,"stream":true,"messages":[{"role":"user","content":"hi"}]}"#;
const CHAT_BODY: &str =
    r#"{"model":"claude-sonnet-4-6","stream":true,"messages":[{"role":"user","content":"hi"}]}"#;

/// Anthropic SSE opening: `message_start` carries the input tokens the stall
/// exit must still account for.
const ANTHROPIC_PRELUDE: &str = concat!(
    "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_1\",\"model\":\"claude-sonnet-4-6\",\"usage\":{\"input_tokens\":7,\"output_tokens\":1}}}\n\n",
    "event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\n",
);
const OPENAI_PRELUDE: &str = "data: {\"id\":\"c1\",\"object\":\"chat.completion.chunk\",\"model\":\"gpt-4\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"content\":\"\"},\"finish_reason\":null}]}\n\n";

/// Large deltas, so the kernel buffers between proxy and client fill fast.
fn anthropic_delta(len: usize) -> String {
    format!(
        "event: content_block_delta\ndata: {{\"type\":\"content_block_delta\",\"index\":0,\"delta\":{{\"type\":\"text_delta\",\"text\":\"{}\"}}}}\n\n",
        "x".repeat(len)
    )
}
fn openai_delta(len: usize) -> String {
    format!(
        "data: {{\"id\":\"c1\",\"object\":\"chat.completion.chunk\",\"model\":\"gpt-4\",\"choices\":[{{\"index\":0,\"delta\":{{\"content\":\"{}\"}},\"finish_reason\":null}}]}}\n\n",
        "x".repeat(len)
    )
}

/// Largest single write of the endless upstream (`spawn_endless_sse_upstream`).
const PROBE_WRITE: usize = 16 * 1024;

/// What the endless upstream saw: when its last write landed and when its
/// connection failed.
#[derive(Default)]
struct UpstreamProbe {
    last_write: Mutex<Option<Instant>>,
    closed_at: Mutex<Option<Instant>>,
}

/// Raw-TCP upstream answering one request with a chunked SSE stream that
/// never ends: `prelude`, then `frame` repeated until a write fails, i.e.
/// until the proxy drops the connection.
///
/// Writes go out in `PROBE_WRITE` slices, each stamping `last_write`, so the
/// stamp marks when the proxy stopped reading. A whole multi-MB frame per
/// write stamps only when the kernel takes its last byte: with default TCP
/// buffers that is while the proxy is still reading and scanning megabytes of
/// it, and that work then lands in the stall measurement.
async fn spawn_endless_sse_upstream(
    prelude: &'static str,
    frame: String,
) -> (String, Arc<UpstreamProbe>) {
    let probe = Arc::new(UpstreamProbe::default());
    let p = probe.clone();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let (mut s, _) = listener.accept().await.unwrap();
        let mut buf = vec![0u8; 8192];
        let _ = s.read(&mut buf).await;
        let head = "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ntransfer-encoding: chunked\r\n\r\n";
        let mut out = format!("{head}{:x}\r\n{prelude}\r\n", prelude.len());
        let chunk = format!("{:x}\r\n{frame}\r\n", frame.len());
        loop {
            for piece in out.as_bytes().chunks(PROBE_WRITE) {
                if s.write_all(piece).await.is_err() {
                    *p.closed_at.lock().unwrap() = Some(Instant::now());
                    return;
                }
                *p.last_write.lock().unwrap() = Some(Instant::now());
            }
            out.clone_from(&chunk);
        }
    });
    (format!("http://{addr}"), probe)
}

/// Send a streaming request on a raw socket that is never read. The small
/// receive buffer keeps the bytes needed to back the proxy up modest.
async fn stalled_request(addr: SocketAddr, path: &str, body: &str) -> tokio::net::TcpStream {
    let sock = tokio::net::TcpSocket::new_v4().unwrap();
    sock.set_recv_buffer_size(4096).unwrap();
    let mut s = sock.connect(addr).await.unwrap();
    let req = format!(
        "POST {path} HTTP/1.1\r\nhost: lb\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{body}",
        body.len()
    );
    s.write_all(req.as_bytes()).await.unwrap();
    s
}

/// State with a distinct, non-zero `instance_id`: `req_id` is
/// `{instance_id:04x}:{n}`, and the log capture buffer is process-global,
/// so this is what ties a stall warning to this test's request. The
/// upstream client's total timeout is well past each test's run, so only
/// the stall exit can close the upstream.
fn stall_state(endpoints: Vec<Endpoint>, instance_id: u16) -> Arc<AppState> {
    Arc::new(AppState {
        endpoints,
        instance_id,
        auto_cache: false,
        client: upstream_client_builder()
            .timeout(Duration::from_secs(60))
            .build()
            .unwrap(),
        ..test_state_base()
    })
}

/// Drive one stalled request through the real router and assert the stall
/// exit: the upstream connection closes within the bound (+1 s) of its last
/// accepted write while the client socket is still open, with exactly one
/// stall warning for this request.
async fn assert_stall_drops_upstream(
    state: Arc<AppState>,
    path: &str,
    body: &str,
    probe: Arc<UpstreamProbe>,
) {
    let buf = log_capture_buf();
    let addr = serve(build_router(state.clone())).await;
    let client = stalled_request(addr, path, body).await;

    let deadline = Instant::now() + DOWNSTREAM_SEND_TIMEOUT + Duration::from_secs(5);
    let closed_at = loop {
        if let Some(t) = *probe.closed_at.lock().unwrap() {
            break t;
        }
        assert!(
            Instant::now() < deadline,
            "upstream still open {:?} after the stall bound while the client holds its socket",
            Duration::from_secs(5)
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    let last_write = probe.last_write.lock().unwrap().expect("upstream wrote");
    let held = closed_at.duration_since(last_write);
    assert!(
        held <= DOWNSTREAM_SEND_TIMEOUT + Duration::from_secs(1),
        "upstream held {held:?} after it stopped being read; bound is {DOWNSTREAM_SEND_TIMEOUT:?}"
    );

    // The warning is emitted before the upstream response is dropped, so it
    // is already in the buffer.
    let prefix = format!("{:04x}:", state.instance_id);
    let output = String::from_utf8(buf.lock().unwrap().clone()).unwrap();
    let warns: Vec<&str> = output
        .lines()
        .filter(|l| l.contains(STALL_WARN) && l.contains(&prefix))
        .collect();
    assert_eq!(
        warns.len(),
        1,
        "expected one stall warning for req_id {prefix}*, got:\n{}",
        warns.join("\n")
    );
    assert!(warns[0].contains(" WARN "), "got: {}", warns[0]);

    // Held until here: the upstream went away while the client stayed
    // connected. A client that wakes up drains what was queued and must then
    // see the response aborted — never the chunked terminator that would pass
    // the truncated stream off as complete.
    let mut client = client;
    let mut got = Vec::new();
    let read = tokio::time::timeout(Duration::from_secs(10), async {
        let mut chunk = vec![0u8; 64 * 1024];
        loop {
            match client.read(&mut chunk).await {
                Ok(0) | Err(_) => break,
                Ok(n) => got.extend_from_slice(&chunk[..n]),
            }
        }
    })
    .await;
    assert!(
        read.is_ok(),
        "proxy must close the stalled client's response"
    );
    assert!(
        got.starts_with(b"HTTP/1.1 200"),
        "client got the stream's head"
    );
    assert!(
        !got.ends_with(b"\r\n0\r\n\r\n"),
        "a stalled stream must be aborted, not terminated cleanly"
    );
}

#[tokio::test]
async fn stalled_client_drops_native_anthropic_stream() {
    let (url, probe) = spawn_endless_sse_upstream(ANTHROPIC_PRELUDE, anthropic_delta(8192)).await;
    let state = stall_state(vec![mk_endpoint_at("acct-a", "sk-ant-api-a", &url)], 0x5a01);
    assert_stall_drops_upstream(state.clone(), "/v1/messages", MESSAGES_BODY, probe).await;
    let (input, _) = poll_streamed_usage(&state).await;
    assert_eq!(input, 7, "stall exit must still record message_start usage");
}

#[tokio::test]
async fn stalled_client_drops_translated_openai_stream() {
    let (url, probe) = spawn_endless_sse_upstream(OPENAI_PRELUDE, openai_delta(8192)).await;
    let mut openai = make_endpoint("fallback", Protocol::OpenAI);
    openai.base_url = url;
    let state = stall_state(vec![openai], 0x5a02);
    assert_stall_drops_upstream(state, "/v1/messages", MESSAGES_BODY, probe).await;
}

/// SSE lets one event carry several `data:` lines. The stall lands mid-event
/// here (each ~6 MB event outsizes the ~4.4 MB the path to the client
/// absorbs), and the relay must not go on to send, and wait on, the event's
/// remaining lines.
#[tokio::test]
async fn stalled_client_drops_translated_multi_data_line_stream() {
    let line = openai_delta(64 * 1024);
    let line = line.trim_end_matches('\n');
    let event = format!("{}\n\n", vec![line; 96].join("\n"));
    let (url, probe) = spawn_endless_sse_upstream(OPENAI_PRELUDE, event).await;
    let mut openai = make_endpoint("fallback", Protocol::OpenAI);
    openai.base_url = url;
    let state = stall_state(vec![openai], 0x5a06);
    assert_stall_drops_upstream(state, "/v1/messages", MESSAGES_BODY, probe).await;
}

#[tokio::test]
async fn stalled_client_drops_passthrough_openai_stream() {
    let (url, probe) = spawn_endless_sse_upstream(OPENAI_PRELUDE, openai_delta(8192)).await;
    let mut openai = make_endpoint("fallback", Protocol::OpenAI);
    openai.base_url = url;
    let state = stall_state(vec![openai], 0x5a03);
    assert_stall_drops_upstream(state, "/v1/chat/completions", CHAT_BODY, probe).await;
}

#[tokio::test]
async fn stalled_client_drops_openai_compat_anthropic_stream() {
    let (url, probe) = spawn_endless_sse_upstream(ANTHROPIC_PRELUDE, anthropic_delta(8192)).await;
    let state = stall_state(vec![mk_endpoint_at("acct-a", "sk-ant-api-a", &url)], 0x5a04);
    assert_stall_drops_upstream(state.clone(), "/v1/chat/completions", CHAT_BODY, probe).await;
    let (input, _) = poll_streamed_usage(&state).await;
    assert_eq!(input, 7, "stall exit must still record message_start usage");
}

/// The bound limits one blocked send, never a stream's duration: a client
/// that keeps reading gets a stream lasting over twice the bound complete
/// and byte-identical.
#[tokio::test]
async fn reading_client_gets_stream_longer_than_the_bound() {
    let frames: Vec<String> = (0..25).map(|i| anthropic_delta(i + 1)).collect();
    let mut expected = String::from(ANTHROPIC_PRELUDE);
    for f in &frames {
        expected.push_str(f);
    }
    expected.push_str("event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n");
    let spacing = (DOWNSTREAM_SEND_TIMEOUT * 5) / (2 * frames.len() as u32);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let up = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let (mut s, _) = listener.accept().await.unwrap();
        let mut buf = vec![0u8; 8192];
        let _ = s.read(&mut buf).await;
        let head = "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ntransfer-encoding: chunked\r\n\r\n";
        let mut parts = vec![ANTHROPIC_PRELUDE.to_owned()];
        parts.extend(frames);
        parts.push("event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n".to_owned());
        s.write_all(head.as_bytes()).await.unwrap();
        for p in parts {
            s.write_all(format!("{:x}\r\n{p}\r\n", p.len()).as_bytes())
                .await
                .unwrap();
            tokio::time::sleep(spacing).await;
        }
        s.write_all(b"0\r\n\r\n").await.unwrap();
    });
    let state = stall_state(
        vec![mk_endpoint_at(
            "acct-a",
            "sk-ant-api-a",
            &format!("http://{up}"),
        )],
        0x5a05,
    );
    let addr = serve(build_router(state)).await;

    let started = Instant::now();
    let resp = Client::new()
        .post(format!("http://{addr}/v1/messages"))
        .header("content-type", "application/json")
        .body(MESSAGES_BODY)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = resp.text().await.unwrap();
    assert!(
        started.elapsed() > DOWNSTREAM_SEND_TIMEOUT * 2,
        "stream must outlast twice the bound, took {:?}",
        started.elapsed()
    );
    assert_eq!(
        body, expected,
        "a reading client gets the whole stream verbatim"
    );
}
