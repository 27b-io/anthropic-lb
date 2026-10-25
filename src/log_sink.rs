//! Optional HTTP log sink (`[log_sink]`): every line the proxy writes to stdout
//! is also POSTed, in batches, as JSON lines to a configured collector.
//!
//! For a platform where stdout is not shipped anywhere a log store can read it
//! within seconds. Delivery is best effort by design, because logging must
//! never slow a request: the tracing writer only ever `try_send`s into a
//! bounded queue, and a line that does not fit, or whose batch fails, is
//! counted as dropped and never retried. That keeps memory bounded and keeps a
//! slow or dead collector from holding up anything behind it. The counters are
//! on `/metrics`.

use crate::*;
use std::sync::Weak;
use tokio::sync::mpsc;

/// Lines held in memory awaiting a POST. A line that finds the queue full is
/// dropped.
const QUEUE_LINES: usize = 10_000;
/// ... and the bytes they may hold together. Lines are bounded in count above,
/// but a line can carry request-controlled text (a model name, say) of
/// megabytes, so without this the queue could hold gigabytes while a POST
/// hangs. A line that would cross it is dropped.
const QUEUE_BYTES: u64 = 16 << 20;
/// The longest `_msg` sent; a longer line is cut and marked. Under
/// VictoriaLogs' default 256 KiB line limit even at the worst JSON escaping
/// (6 bytes for a control character), so one line cannot fail a whole batch.
pub(crate) const MAX_LINE_BYTES: usize = 32 << 10;
/// A batch is POSTed once it holds this many lines, ...
const MAX_BATCH_LINES: usize = 1_000;
/// ... or about this many body bytes (the line that crosses it still rides),
/// ...
const MAX_BATCH_BYTES: usize = 1 << 20;
/// ... or this long after its first line arrived, whichever comes first, so a
/// lone line leaves within about a second.
const FLUSH_INTERVAL: Duration = Duration::from_secs(1);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(2);
/// Whole-POST ceiling. A batch that has not been answered by then is dropped.
const POST_TIMEOUT: Duration = Duration::from_secs(5);
const DRAIN_POLL: Duration = Duration::from_millis(50);
/// How long shutdown waits for the queue to empty: a flush interval plus a POST
/// to a healthy collector, short enough not to stall a restart.
pub(crate) const LOG_SINK_SHUTDOWN_DRAIN: Duration = Duration::from_secs(2);

/// Boot check for `[log_sink]`, run by `load_config`; returns the parsed URL.
/// No message quotes the URL, as its path or query may hold a secret.
pub(crate) fn validate_log_sink(config: &LogSinkConfig) -> Result<reqwest::Url, String> {
    let url = reqwest::Url::parse(&config.url)
        .ok()
        .filter(|url| matches!(url.scheme(), "http" | "https"))
        .ok_or_else(|| "log_sink.url: not an http(s) URL".to_string())?;
    if !url.username().is_empty() || url.password().is_some() {
        return Err("log_sink.url: must not carry credentials".to_string());
    }
    Ok(url)
}

/// One stdout line as it is sent: one JSON object per line of the body.
#[derive(Serialize)]
struct Line {
    /// When the line was written, as the fmt layer prints its own timestamp.
    #[serde(rename = "_time")]
    time: String,
    /// The line exactly as written to stdout, minus its newline, cut at
    /// `MAX_LINE_BYTES`.
    #[serde(rename = "_msg")]
    msg: String,
}

impl Line {
    /// What the line holds in memory, as counted against `QUEUE_BYTES`.
    fn bytes(&self) -> u64 {
        (self.time.len() + self.msg.len()) as u64
    }
}

/// `raw` as `_msg`: lossy UTF-8, and past `MAX_LINE_BYTES` cut and marked with
/// the original length, so a reader knows the line was longer.
fn message(raw: &[u8]) -> String {
    if raw.len() <= MAX_LINE_BYTES {
        return String::from_utf8_lossy(raw).into_owned();
    }
    // Back off to a character start, so a valid line is cut between characters
    // rather than mid-way through one (which would end in U+FFFD). A UTF-8
    // character has at most 3 continuation bytes; malformed input loses at most
    // those 3.
    let mut end = MAX_LINE_BYTES;
    while end > MAX_LINE_BYTES - 3 && raw[end] & 0xC0 == 0x80 {
        end -= 1;
    }
    let marker = format!(" …[log sink: cut from {} bytes]", raw.len());
    // Sized up front: growing by push_str would double the capacity, and the
    // queue's byte budget counts length, so it would hold twice what it counts.
    // Sized from the converted text, which malformed input can make up to 3x
    // `end` (each invalid byte becomes a 3-byte U+FFFD).
    let kept = String::from_utf8_lossy(&raw[..end]);
    let mut msg = String::with_capacity(kept.len() + marker.len());
    msg.push_str(&kept);
    msg.push_str(&marker);
    msg
}

pub(crate) struct LogSink {
    tx: mpsc::Sender<Line>,
    /// The collector's `host:port`: what log lines name. Never the URL.
    host: String,
    /// Lines delivered with a 2xx.
    sent: AtomicU64,
    /// Lines that found the queue full, or whose batch failed.
    dropped: AtomicU64,
    /// Lines accepted into the queue and not yet sent or dropped.
    pending: AtomicU64,
    /// Bytes of the lines in the queue, against `max_queued_bytes`. The flush
    /// task gives a batch's bytes back once it has taken the batch out.
    queued_bytes: AtomicU64,
    max_queued_bytes: u64,
}

impl LogSink {
    /// Build the sink and spawn its flush task. Call from inside the runtime.
    pub(crate) fn start(config: &LogSinkConfig) -> Result<Arc<Self>, String> {
        Self::start_with(config, QUEUE_LINES, QUEUE_BYTES, FLUSH_INTERVAL)
    }

    /// `start`, with the queue limits and flush interval a test can shrink.
    fn start_with(
        config: &LogSinkConfig,
        capacity: usize,
        max_queued_bytes: u64,
        flush_interval: Duration,
    ) -> Result<Arc<Self>, String> {
        let url = validate_log_sink(config)?;
        let http = Client::builder()
            .user_agent(concat!("anthropic-lb/", env!("CARGO_PKG_VERSION")))
            // A redirect would carry the logs to a host nobody configured.
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(CONNECT_TIMEOUT)
            .timeout(POST_TIMEOUT)
            .build()
            .map_err(|e| format!("log_sink: HTTP client: {e}"))?;
        let host = url.host_str().unwrap_or_default();
        let host = match url.port_or_known_default() {
            Some(port) => format!("{host}:{port}"),
            None => host.to_string(),
        };
        let (tx, rx) = mpsc::channel(capacity);
        let sink = Arc::new(Self {
            tx,
            host,
            sent: AtomicU64::new(0),
            dropped: AtomicU64::new(0),
            pending: AtomicU64::new(0),
            queued_bytes: AtomicU64::new(0),
            max_queued_bytes,
        });
        // A weak handle: when the last `LogSink` goes, its sender goes with it,
        // the queue closes and the task ends.
        tokio::spawn(flush(rx, Arc::downgrade(&sink), http, url, flush_interval));
        Ok(sink)
    }

    pub(crate) fn host(&self) -> &str {
        &self.host
    }

    pub(crate) fn sent(&self) -> u64 {
        self.sent.load(Ordering::Relaxed)
    }

    pub(crate) fn dropped(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }

    /// Queue one write from the tracing writer, line by line. It runs inside
    /// every event, so it never blocks or awaits: a line that finds the queue
    /// full in lines or bytes, or closed, is dropped and counted.
    pub(crate) fn push(&self, bytes: &[u8]) {
        let mut time: Option<String> = None;
        for raw in bytes.split(|&b| b == b'\n') {
            let raw = raw.strip_suffix(b"\r").unwrap_or(raw);
            if raw.is_empty() {
                continue;
            }
            let line = Line {
                // Every line of one event carries that event's capture time.
                time: time.get_or_insert_with(now_rfc3339).clone(),
                msg: message(raw),
            };
            let size = line.bytes();
            // Reserved before the check, so concurrent pushes cannot all pass
            // it and overshoot together; a refused line gives it back.
            if self.queued_bytes.fetch_add(size, Ordering::Relaxed) + size > self.max_queued_bytes {
                self.queued_bytes.fetch_sub(size, Ordering::Relaxed);
                self.dropped.fetch_add(1, Ordering::Relaxed);
                continue;
            }
            // Counted before the send, not after: the flush task can settle
            // the line before this thread runs again, and `pending` must
            // never dip below zero.
            self.pending.fetch_add(1, Ordering::Relaxed);
            if self.tx.try_send(line).is_err() {
                self.pending.fetch_sub(1, Ordering::Relaxed);
                self.queued_bytes.fetch_sub(size, Ordering::Relaxed);
                self.dropped.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    /// Wait until every queued line is sent or dropped, or `budget` passes.
    /// Returns the lines still undelivered.
    pub(crate) async fn drain(&self, budget: Duration) -> u64 {
        let deadline = tokio::time::Instant::now() + budget;
        loop {
            let pending = self.pending.load(Ordering::Relaxed);
            if pending == 0 || tokio::time::Instant::now() >= deadline {
                return pending;
            }
            tokio::time::sleep(DRAIN_POLL).await;
        }
    }

    /// Account for one POSTed batch of `lines`, and log a change of health.
    fn settle(&self, lines: u64, outcome: Result<(), String>, health: &mut Health) {
        let dropped_before = self.dropped();
        let counter = if outcome.is_ok() {
            &self.sent
        } else {
            &self.dropped
        };
        counter.fetch_add(lines, Ordering::Relaxed);
        self.pending.fetch_sub(lines, Ordering::Relaxed);
        match health.observe(outcome.is_ok(), dropped_before) {
            Some(Transition::Failing) => warn!(
                host = self.host.as_str(),
                reason = outcome.err().unwrap_or_default(),
                "log sink delivery failing: lines are dropped, not retried, until it recovers"
            ),
            Some(Transition::Recovered { dropped }) => info!(
                host = self.host.as_str(),
                dropped, "log sink delivery recovered"
            ),
            None => {}
        }
    }
}

/// Whether the collector is taking batches. The flush task logs only when this
/// changes: its own lines go through the sink too, so a line per failed batch
/// would be noise fed into the very outage it reports.
#[derive(Default)]
struct Health {
    /// While failing: the dropped total when the failure began.
    failing_since: Option<u64>,
}

#[derive(Debug, PartialEq)]
enum Transition {
    Failing,
    /// Lines dropped while it was down, queue overflow included.
    Recovered {
        dropped: u64,
    },
}

impl Health {
    /// One batch's outcome; `dropped` is the dropped total before it.
    fn observe(&mut self, delivered: bool, dropped: u64) -> Option<Transition> {
        match (delivered, self.failing_since) {
            (false, None) => {
                self.failing_since = Some(dropped);
                Some(Transition::Failing)
            }
            (true, Some(since)) => {
                self.failing_since = None;
                Some(Transition::Recovered {
                    dropped: dropped.saturating_sub(since),
                })
            }
            _ => None,
        }
    }
}

/// The queue's one consumer: gather a batch, POST it, settle the counters.
/// POSTs are sequential; lines keep queueing, bounded, while one is in flight.
async fn flush(
    mut rx: mpsc::Receiver<Line>,
    sink: Weak<LogSink>,
    http: Client,
    url: reqwest::Url,
    interval: Duration,
) {
    let mut health = Health::default();
    while let Some(first) = rx.recv().await {
        let deadline = tokio::time::Instant::now() + interval;
        let mut body = Vec::new();
        let mut bytes = first.bytes();
        append_line(&mut body, &first);
        let mut lines = 1;
        while lines < MAX_BATCH_LINES && body.len() < MAX_BATCH_BYTES {
            match tokio::time::timeout_at(deadline, rx.recv()).await {
                Ok(Some(line)) => {
                    bytes += line.bytes();
                    append_line(&mut body, &line);
                    lines += 1;
                }
                // The interval is up, or the queue closed: send what is held.
                Ok(None) | Err(_) => break,
            }
        }
        // The batch is out of the queue: its bytes go back before the POST,
        // which can take up to POST_TIMEOUT, and the batch's own size is
        // bounded by MAX_BATCH_BYTES.
        // A sink already gone still has its last lines posted: the queue keeps
        // what was sent before it closed.
        if let Some(live) = sink.upgrade() {
            live.queued_bytes.fetch_sub(bytes, Ordering::Relaxed);
        }
        let outcome = post(&http, &url, body).await;
        // Once the sink is gone there is no one to count for, but the closed
        // queue is still drained, batch by batch, until `recv` says it is empty.
        if let Some(sink) = sink.upgrade() {
            sink.settle(lines as u64, outcome, &mut health);
        }
    }
}

fn append_line(body: &mut Vec<u8>, line: &Line) {
    serde_json::to_writer(&mut *body, line).expect("two strings always serialise");
    body.push(b'\n');
}

/// POST one batch. The error is a reason safe to log, a status code or the
/// kind of transport failure, never reqwest's message, which quotes the URL.
async fn post(http: &Client, url: &reqwest::Url, body: Vec<u8>) -> Result<(), String> {
    let resp = http
        .post(url.clone())
        // Load-bearing: VictoriaLogs answers 200 to a form-encoded body but ingests none of it.
        .header(axum::http::header::CONTENT_TYPE, "application/x-ndjson")
        .body(body)
        .send()
        .await
        .map_err(|e| {
            let kind = if e.is_timeout() {
                "timeout"
            } else if e.is_connect() {
                "connect"
            } else {
                "request"
            };
            kind.to_string()
        })?;
    if resp.status().is_success() {
        Ok(())
    } else {
        Err(format!("status {}", resp.status().as_u16()))
    }
}

/// Now, as the fmt layer prints it (`2026-10-17T01:02:03.123456Z`), so `_time`
/// and the timestamp inside `_msg` read alike.
fn now_rfc3339() -> String {
    use tracing_subscriber::fmt::time::FormatTime;
    let mut time = String::with_capacity(32);
    // Formatting into a String cannot fail.
    let _ = tracing_subscriber::fmt::time::SystemTime
        .format_time(&mut tracing_subscriber::fmt::format::Writer::new(&mut time));
    time
}

/// The fmt layer that writes stdout, teeing every event to the sink when one is
/// configured. With a sink, or when stdout is not a terminal (a pipe, a
/// container log), it writes plain text: colour escapes inside a log store's
/// `_msg` break text search and field matching, and a logfmt reader then sees
/// each value open with an escape instead of its quote, so reads it unquoted
/// to the next space. On a terminal the default stands, `NO_COLOR` included,
/// so the layer is left alone rather than given `with_ansi(true)`, which would
/// override `NO_COLOR`.
pub(crate) fn stdout_layer<S>(
    sink: Option<Arc<LogSink>>,
) -> tracing_subscriber::fmt::Layer<
    S,
    tracing_subscriber::fmt::format::DefaultFields,
    tracing_subscriber::fmt::format::Format,
    StdoutTee,
>
where
    S: tracing::Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a>,
{
    use std::io::IsTerminal;
    let layer = tracing_subscriber::fmt::layer();
    let layer = if sink.is_some() || !std::io::stdout().is_terminal() {
        layer.with_ansi(false)
    } else {
        layer
    };
    layer.with_writer(StdoutTee { sink })
}

/// The fmt layer's stdout writer: each event goes to stdout byte for byte as
/// the layer formatted it, and to the sink when one is configured.
pub(crate) struct StdoutTee {
    pub(crate) sink: Option<Arc<LogSink>>,
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for StdoutTee {
    type Writer = &'a StdoutTee;

    fn make_writer(&'a self) -> Self::Writer {
        self
    }
}

impl std::io::Write for &StdoutTee {
    /// The fmt layer writes each event with one `write_all`, and this takes
    /// all of it in one call, so one event is one `push`.
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        // The sink first, so a failing stdout (a closed pipe) does not also
        // cost the sink its copy.
        if let Some(sink) = &self.sink {
            sink.push(buf);
        }
        std::io::stdout().lock().write_all(buf)?;
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        std::io::stdout().flush()
    }
}

#[cfg(test)]
mod tests;
