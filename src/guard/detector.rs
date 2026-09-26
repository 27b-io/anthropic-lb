//! Tier 1 ML detector client (LAB-3878).
//!
//! Sends the same newest-turn text Tier 0 scans to a sequence-classification
//! sidecar (a prompt-injection classifier served by Hugging Face
//! text-embeddings-inference) and turns its labels into [`Finding`]s. Like
//! Tier 0 it is detect-only: it reads a copy of the scan text and never touches
//! the body forwarded upstream.
//!
//! Where it runs depends on the client's policy, and that split is the latency
//! budget:
//!
//! - `annotate` classifies **off the request path**, in a spawned task. The
//!   request pays nothing; the verdict reaches logs and metrics only, never
//!   `X-Guard-Findings` (the response has usually left before the verdict
//!   exists). A CPU classifier needs hundreds of milliseconds per 512-token
//!   chunk, so an inline shadow mode would tax every request to measure a
//!   control that blocks none of them.
//! - `block` classifies inline and holds the request at most `timeout_ms`,
//!   however many chunks the input splits into.
//!
//! Failure (timeout, connect error, non-2xx, unreadable response) is fail-closed
//! by default: under `block`, `fail_open = false` rejects with
//! `guard_unavailable` and `fail_open = true` forwards with a `guard_unavailable`
//! finding. A per-detector circuit breaker — the same [`Breaker`] the upstream
//! transport path uses — stops a dead or saturated detector from costing every
//! request a full timeout: once open, requests take their failure path
//! immediately.
//!
//! Wire format: TEI `POST /predict` with a nested-list batch. The FMS Detector
//! API (`/api/v1/text/contents`) is the contract this is meant to converge on;
//! v1 speaks the sidecar's native endpoint instead, and the whole mapping lives
//! in [`predict_request`] and [`flagged_labels`], so a second wire format is one
//! more pair of functions, not a rewrite.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use blake2::{Blake2s256, Digest};
use futures_util::future::join_all;
use serde::Deserialize;
use tracing::{info, warn};

use super::{detections_summary, Finding, GuardPolicy, ScanHistogram, VerdictCounts};
use crate::breaker::Breaker;

pub const DEFAULT_TIMEOUT_MS: u64 = 2000;
pub const DEFAULT_THRESHOLD: f64 = 0.5;
pub const DEFAULT_CHUNK_TOKENS: usize = 512;
pub const DEFAULT_CACHE_SIZE: u64 = 10_000;
pub const DEFAULT_BREAKER_THRESHOLD: u32 = crate::utilization::TRANSPORT_FAILURE_THRESHOLD;
pub const DEFAULT_BREAKER_COOLDOWN_SECS: u64 = 30;

/// Overlap between consecutive chunks, so an injection straddling a boundary
/// is whole in at least one chunk.
const OVERLAP_TOKENS: usize = 32;
/// The proxy has no tokenizer, so chunks are sized in bytes, and a chunk that
/// overflows the model's window is truncated by the sidecar — its tail is
/// never classified. Measured on Prompt Guard 2's tokenizer, a 1024-byte slice
/// is ~285 tokens of English, ~300 of Rust, ~450-520 of JSON, ~270 of CJK and
/// ~760 of base64; at three bytes per token (1536 bytes) JSON reached ~680 and
/// lost a quarter of every chunk. JSON tool results are the likeliest carrier
/// of an indirect injection, so two bytes per token: nothing but encoded blobs
/// overflows, at the cost of ~1.5x the chunks prose would need.
const BYTES_PER_TOKEN: usize = 2;
/// TEI's default `--max-client-batch-size`. Larger inputs split into several
/// batches, issued concurrently.
const MAX_BATCH: usize = 32;
/// Cap on a detector response body. A classifier answer for 32 chunks is a few
/// KiB; anything near this is a misbehaving sidecar, not a verdict.
const MAX_RESPONSE_BYTES: usize = 1024 * 1024;
/// Labels that mean "nothing found". Every other label at or above `threshold`
/// is a finding. Covers Prompt Guard 2 (`BENIGN`/`MALICIOUS`) and ProtectAI's
/// DeBERTa (`SAFE`/`INJECTION`) without a per-model knob.
const BENIGN_LABELS: [&str; 2] = ["BENIGN", "SAFE"];
/// Finding label for "the detector gave no verdict", and the `error.type` of
/// the 503 a `fail_open = false` block client receives.
pub const GUARD_UNAVAILABLE: &str = "guard_unavailable";
pub const REASON_GUARD_UNAVAILABLE: &str =
    "the content guard detector is unavailable and this client requires a verdict";

/// Upper edges (seconds) of `anthropic_guard_detector_duration_seconds`,
/// sized around the measured range: tens of ms for a short input, seconds for a
/// many-chunk tool result on CPU.
const DURATION_BUCKETS: &[f64] = &[0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0];

/// `[guard]` in the operator config.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GuardConfig {
    /// `[guard.detectors.<name>]`. At most one in v1.
    #[serde(default)]
    pub detectors: BTreeMap<String, DetectorConfig>,
}

/// `[guard.detectors.<name>]`. Unknown keys are rejected: a misspelt
/// `fail_open` must not silently leave a security control at its default.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DetectorConfig {
    /// Base URL of the classifier; the client POSTs to `<url>/predict`.
    pub url: String,
    /// Wall-clock ceiling for classifying one request, all chunks included.
    #[serde(default = "default_timeout_ms")]
    pub timeout_ms: u64,
    /// Minimum label score that counts as a finding.
    #[serde(default = "default_threshold")]
    pub threshold: f64,
    /// Under `block`: forward with a `guard_unavailable` finding (true) or
    /// reject with 503 (false) when the detector gives no verdict.
    #[serde(default)]
    pub fail_open: bool,
    /// Chunk size in model tokens (approximated in bytes, see `BYTES_PER_TOKEN`).
    #[serde(default = "default_chunk_tokens")]
    pub chunk_tokens: usize,
    /// Verdict memo entries, keyed by chunk digest. 0 disables the cache.
    #[serde(default = "default_cache_size")]
    pub cache_size: u64,
    /// Consecutive failed requests before the breaker opens.
    #[serde(default = "default_breaker_threshold")]
    pub breaker_threshold: u32,
    /// How long the breaker stays open before one probe is let through.
    #[serde(default = "default_breaker_cooldown_secs")]
    pub breaker_cooldown_secs: u64,
}

fn default_timeout_ms() -> u64 {
    DEFAULT_TIMEOUT_MS
}
fn default_threshold() -> f64 {
    DEFAULT_THRESHOLD
}
fn default_chunk_tokens() -> usize {
    DEFAULT_CHUNK_TOKENS
}
fn default_cache_size() -> u64 {
    DEFAULT_CACHE_SIZE
}
fn default_breaker_threshold() -> u32 {
    DEFAULT_BREAKER_THRESHOLD
}
fn default_breaker_cooldown_secs() -> u64 {
    DEFAULT_BREAKER_COOLDOWN_SECS
}

/// Why a request got no verdict — the `kind` label of
/// `anthropic_guard_detector_errors_total`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorKind {
    Timeout,
    Connect,
    Transport,
    Status,
    Decode,
}

impl ErrorKind {
    const ALL: [ErrorKind; 5] = [
        ErrorKind::Timeout,
        ErrorKind::Connect,
        ErrorKind::Transport,
        ErrorKind::Status,
        ErrorKind::Decode,
    ];

    pub fn label(self) -> &'static str {
        match self {
            ErrorKind::Timeout => "timeout",
            ErrorKind::Connect => "connect",
            ErrorKind::Transport => "transport",
            ErrorKind::Status => "status",
            ErrorKind::Decode => "decode",
        }
    }

    fn from_reqwest(e: &reqwest::Error) -> Self {
        if e.is_timeout() {
            ErrorKind::Timeout
        } else if e.is_connect() {
            ErrorKind::Connect
        } else {
            ErrorKind::Transport
        }
    }
}

/// What one classification produced.
#[derive(Debug)]
pub struct Classified {
    /// Findings from every chunk that was classified (cache hits included).
    pub findings: Vec<Finding>,
    /// Why some chunks got no verdict, if any did. `None` = the whole input
    /// was classified.
    pub unavailable: Option<Unavailable>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Unavailable {
    /// The breaker was open: no call was made.
    CircuitOpen,
    /// A call was made and failed.
    Failed(ErrorKind),
}

impl Unavailable {
    fn label(self) -> &'static str {
        match self {
            Unavailable::CircuitOpen => "circuit_open",
            Unavailable::Failed(kind) => kind.label(),
        }
    }
}

/// The inline (`block`) outcome, for `guard_hook` to turn into a response.
#[derive(Debug)]
pub enum Enforced {
    Allow,
    /// Forward, stamping this many findings: the one `guard_unavailable`
    /// finding a `fail_open = true` client gets when there is no verdict.
    Annotate(usize),
    Block(Vec<Finding>),
    /// No verdict and `fail_open = false`.
    Unavailable,
}

/// Breaker plus the half-open probe marker. One std mutex, never held across
/// an await.
#[derive(Default)]
struct BreakerState {
    breaker: Breaker,
    /// Set while a half-open probe is in flight, to its deadline. A probe whose
    /// future was dropped (client disconnect under `block`) never records an
    /// outcome, so the marker expires instead of wedging the breaker open.
    probe_until: Option<Instant>,
}

enum Admit {
    Call,
    ShortCircuit,
}

pub struct Detector {
    /// Config key, leaked once at startup: it is the `scanner` label on every
    /// finding and metric this detector produces.
    name: &'static str,
    predict_url: reqwest::Url,
    client: reqwest::Client,
    timeout: Duration,
    threshold: f64,
    fail_open: bool,
    chunk_bytes: usize,
    overlap_bytes: usize,
    /// Chunk digest → labels at or above threshold (empty = benign). Holds no
    /// request text.
    cache: moka::sync::Cache<[u8; 32], Arc<[Box<str>]>>,
    breaker: Mutex<BreakerState>,
    breaker_threshold: u32,
    cooldown: Duration,
    errors: [AtomicU64; ErrorKind::ALL.len()],
    short_circuited: AtomicU64,
    cache_hits: AtomicU64,
    cache_misses: AtomicU64,
    duration: ScanHistogram,
    verdicts: VerdictCounts,
}

impl Detector {
    pub fn new(name: &str, cfg: &DetectorConfig) -> Result<Self, String> {
        let bad = |msg: String| format!("guard.detectors.{name}: {msg}");
        if name.is_empty()
            || name.len() > 64
            || !name
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
        {
            return Err(bad(
                "name must be 1-64 characters of [A-Za-z0-9_-]".to_string()
            ));
        }
        let base = reqwest::Url::parse(&cfg.url).map_err(|e| bad(format!("url: {e}")))?;
        if !matches!(base.scheme(), "http" | "https") {
            return Err(bad("url must be http or https".to_string()));
        }
        // Credentials in the URL would ride every request and every log line
        // that ever prints it; the sidecar takes none.
        if !base.username().is_empty() || base.password().is_some() {
            return Err(bad("url must not carry credentials".to_string()));
        }
        let predict_url = base
            .join(&format!("{}/predict", base.path().trim_end_matches('/')))
            .map_err(|e| bad(format!("url: {e}")))?;
        if cfg.timeout_ms == 0 {
            return Err(bad("timeout_ms must be > 0".to_string()));
        }
        if !(cfg.threshold > 0.0 && cfg.threshold <= 1.0) {
            return Err(bad("threshold must be in (0, 1]".to_string()));
        }
        if cfg.chunk_tokens <= 2 * OVERLAP_TOKENS {
            return Err(bad(format!(
                "chunk_tokens must be > {} (twice the {OVERLAP_TOKENS}-token overlap)",
                2 * OVERLAP_TOKENS
            )));
        }
        if cfg.breaker_threshold == 0 {
            return Err(bad("breaker_threshold must be >= 1".to_string()));
        }
        if cfg.breaker_cooldown_secs == 0 {
            return Err(bad("breaker_cooldown_secs must be >= 1".to_string()));
        }
        let timeout = Duration::from_millis(cfg.timeout_ms);
        // The detector sees request content, so it gets its own client: no
        // redirects (a 3xx must not re-send user text to wherever it points),
        // and no connection sharing with the credentialed upstream client.
        let client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(timeout)
            .timeout(timeout)
            .build()
            .map_err(|e| bad(format!("http client: {e}")))?;
        Ok(Self {
            name: Box::leak(name.to_owned().into_boxed_str()),
            predict_url,
            client,
            timeout,
            threshold: cfg.threshold,
            fail_open: cfg.fail_open,
            chunk_bytes: cfg.chunk_tokens * BYTES_PER_TOKEN,
            overlap_bytes: OVERLAP_TOKENS * BYTES_PER_TOKEN,
            cache: moka::sync::Cache::new(cfg.cache_size),
            breaker: Mutex::new(BreakerState::default()),
            breaker_threshold: cfg.breaker_threshold,
            cooldown: Duration::from_secs(cfg.breaker_cooldown_secs),
            errors: Default::default(),
            short_circuited: AtomicU64::new(0),
            cache_hits: AtomicU64::new(0),
            cache_misses: AtomicU64::new(0),
            duration: ScanHistogram::new(DURATION_BUCKETS),
            verdicts: VerdictCounts::default(),
        })
    }

    pub fn name(&self) -> &'static str {
        self.name
    }

    /// Classify `text`, all chunks under one `timeout` deadline.
    pub async fn classify(&self, text: &str) -> Classified {
        let chunks = chunk(text, self.chunk_bytes, self.overlap_bytes);
        let mut findings = Vec::new();
        let mut misses: Vec<(usize, &str, [u8; 32])> = Vec::new();
        for (start, piece) in chunks {
            let digest: [u8; 32] = Blake2s256::digest(piece.as_bytes()).into();
            match self.cache.get(&digest) {
                Some(labels) => {
                    self.cache_hits.fetch_add(1, Ordering::Relaxed);
                    self.push_findings(&mut findings, start, piece.len(), &labels);
                }
                None => {
                    self.cache_misses.fetch_add(1, Ordering::Relaxed);
                    misses.push((start, piece, digest));
                }
            }
        }
        // Fully cached: no call, so nothing for the breaker to learn.
        if misses.is_empty() {
            return Classified {
                findings,
                unavailable: None,
            };
        }
        if let Admit::ShortCircuit = self.admit(Instant::now()) {
            self.short_circuited.fetch_add(1, Ordering::Relaxed);
            return Classified {
                findings,
                unavailable: Some(Unavailable::CircuitOpen),
            };
        }

        let started = Instant::now();
        let batches = misses.chunks(MAX_BATCH).map(|batch| {
            let texts: Vec<&str> = batch.iter().map(|(_, piece, _)| *piece).collect();
            self.predict(texts)
        });
        let outcome = tokio::time::timeout(self.timeout, join_all(batches)).await;
        self.duration.observe(started.elapsed());
        let failure = match outcome {
            // The deadline covers every batch: none of them is a verdict.
            Err(_) => Some(ErrorKind::Timeout),
            Ok(results) => {
                let mut failure = None;
                for (batch, result) in misses.chunks(MAX_BATCH).zip(results) {
                    match result {
                        Ok(per_chunk) => {
                            for ((start, piece, digest), labels) in batch.iter().zip(per_chunk) {
                                self.push_findings(&mut findings, *start, piece.len(), &labels);
                                self.cache.insert(*digest, labels);
                            }
                        }
                        Err(kind) => failure = failure.or(Some(kind)),
                    }
                }
                failure
            }
        };
        self.record_outcome(failure, Instant::now());
        Classified {
            findings,
            unavailable: failure.map(Unavailable::Failed),
        }
    }

    /// One `/predict` call for up to `MAX_BATCH` chunks. Returns, per chunk, the
    /// labels at or above threshold.
    async fn predict(&self, texts: Vec<&str>) -> Result<Vec<Arc<[Box<str>]>>, ErrorKind> {
        let expected = texts.len();
        let resp = self
            .client
            .post(self.predict_url.clone())
            .json(&predict_request(&texts))
            .send()
            .await
            .map_err(|e| ErrorKind::from_reqwest(&e))?;
        if !resp.status().is_success() {
            return Err(ErrorKind::Status);
        }
        let body = read_capped(resp, MAX_RESPONSE_BYTES).await?;
        flagged_labels(&body, expected, self.threshold)
    }

    fn push_findings(&self, out: &mut Vec<Finding>, start: usize, len: usize, labels: &[Box<str>]) {
        out.extend(labels.iter().map(|label| Finding {
            scanner: self.name,
            detection_type: label.to_string(),
            start,
            end: start + len,
        }));
    }

    fn admit(&self, now: Instant) -> Admit {
        let mut s = crate::lock_recovering(&self.breaker, "guard_detector_breaker");
        if s.breaker.is_open(now) {
            return Admit::ShortCircuit;
        }
        if s.breaker.is_half_open(now) {
            if s.probe_until.is_some_and(|deadline| now < deadline) {
                return Admit::ShortCircuit;
            }
            s.probe_until = Some(now + self.timeout);
        }
        Admit::Call
    }

    /// Feed the breaker one request's outcome. After the cooldown the first
    /// outcome decides: a success closes the breaker, a failure re-opens it at
    /// once (a threshold of one) rather than waiting for a fresh run of
    /// `breaker_threshold` failures — the detector has already proved itself
    /// down once, and each extra failure is a request paying `timeout_ms`.
    fn record_outcome(&self, failure: Option<ErrorKind>, now: Instant) {
        if let Some(kind) = failure {
            self.errors[kind as usize].fetch_add(1, Ordering::Relaxed);
        }
        let mut s = crate::lock_recovering(&self.breaker, "guard_detector_breaker");
        s.probe_until = None;
        match failure {
            None => {
                if s.breaker.record_success() {
                    info!(
                        detector = self.name,
                        "detector circuit-breaker CLOSED: detector recovered"
                    );
                }
            }
            Some(_) => {
                let threshold = if s.breaker.is_half_open(now) {
                    1
                } else {
                    self.breaker_threshold
                };
                if s.breaker.record_failure(now, threshold, self.cooldown) {
                    warn!(
                        detector = self.name,
                        consecutive_failures = s.breaker.consecutive_failures,
                        cooldown_secs = self.cooldown.as_secs(),
                        "detector circuit-breaker OPEN: requests take the fail_open path without a call"
                    );
                }
            }
        }
    }

    /// `annotate`: classify in a spawned task and report to logs and metrics.
    /// The request never waits on it.
    pub fn shadow(self: &Arc<Self>, req_id: String, client_id: String, text: String) {
        let detector = Arc::clone(self);
        tokio::spawn(async move {
            let classified = detector.classify(&text).await;
            detector.report(&req_id, &client_id, GuardPolicy::Annotate, classified);
        });
    }

    /// `block`: classify inline and decide.
    pub async fn enforce(&self, req_id: &str, client_id: &str, text: &str) -> Enforced {
        let classified = self.classify(text).await;
        self.report(req_id, client_id, GuardPolicy::Block, classified)
    }

    /// Log and count one classification, and return what `block` would do
    /// with it. Findings from the chunks that did come back are enforced even
    /// when others failed: a partial verdict that flags something is still a
    /// verdict.
    fn report(
        &self,
        req_id: &str,
        client_id: &str,
        policy: GuardPolicy,
        classified: Classified,
    ) -> Enforced {
        let enforcing = policy == GuardPolicy::Block;
        let findings = classified.findings;
        if !findings.is_empty() {
            let verdict = if enforcing { "block" } else { "annotate" };
            self.verdicts.record(client_id, self.name, verdict);
            warn!(
                req_id,
                client_id = %client_id,
                verdict,
                findings = findings.len(),
                detections = %detections_summary(&findings),
                "guard detector"
            );
            return Enforced::Block(findings);
        }
        let Some(unavailable) = classified.unavailable else {
            self.verdicts.record(client_id, self.name, "allow");
            return Enforced::Allow;
        };
        // No verdict. An open breaker already logged its OPEN line and counts
        // every short-circuit, so a per-request line would only flood the log
        // for the length of an outage.
        let outcome = if self.fail_open {
            Enforced::Annotate(1)
        } else {
            Enforced::Unavailable
        };
        if unavailable != Unavailable::CircuitOpen {
            let verdict = match (enforcing, self.fail_open) {
                (_, true) => "annotate",
                (true, false) => "block",
                (false, false) => "would-block",
            };
            if enforcing && !self.fail_open {
                warn!(req_id, client_id = %client_id, verdict, reason = GUARD_UNAVAILABLE, cause = unavailable.label(), "guard detector");
            } else {
                info!(req_id, client_id = %client_id, verdict, reason = GUARD_UNAVAILABLE, cause = unavailable.label(), "guard detector");
            }
        }
        outcome
    }

    // ── /metrics snapshots ──

    pub fn errors_snapshot(&self) -> Vec<(&'static str, u64)> {
        ErrorKind::ALL
            .iter()
            .map(|k| (k.label(), self.errors[*k as usize].load(Ordering::Relaxed)))
            .collect()
    }

    pub fn short_circuited(&self) -> u64 {
        self.short_circuited.load(Ordering::Relaxed)
    }

    /// Open, or cooled down but not yet proven healthy by a probe.
    pub fn circuit_open(&self) -> bool {
        crate::lock_recovering(&self.breaker, "guard_detector_breaker")
            .breaker
            .open_until
            .is_some()
    }

    pub fn cache_snapshot(&self) -> (u64, u64) {
        (
            self.cache_hits.load(Ordering::Relaxed),
            self.cache_misses.load(Ordering::Relaxed),
        )
    }

    pub fn duration_snapshot(&self) -> (Vec<(String, u64)>, f64, u64) {
        self.duration.snapshot()
    }

    pub fn verdicts_snapshot(&self) -> Vec<((String, &'static str, &'static str), u64)> {
        self.verdicts.snapshot()
    }
}

/// Split `text` into chunks of at most `size` bytes, each starting `overlap`
/// bytes before the previous one ended, on UTF-8 boundaries. Returns
/// `(byte_offset, chunk)` pairs. Requires `size > 2 * overlap` (validated at
/// startup) so every step advances by more than one maximal UTF-8 character.
fn chunk(text: &str, size: usize, overlap: usize) -> Vec<(usize, &str)> {
    let mut out = Vec::new();
    let mut start = 0;
    while start < text.len() {
        let end = floor_char_boundary(text, (start + size).min(text.len()));
        out.push((start, &text[start..end]));
        if end == text.len() {
            break;
        }
        start = floor_char_boundary(text, end - overlap);
    }
    out
}

fn floor_char_boundary(s: &str, mut i: usize) -> usize {
    while i > 0 && !s.is_char_boundary(i) {
        i -= 1;
    }
    i
}

/// TEI `/predict` request for a batch: each input is its own one-element list
/// (a two-element list would be read as a sentence pair). `truncate` makes an
/// over-long chunk lose its tail rather than fail the whole batch.
fn predict_request(texts: &[&str]) -> serde_json::Value {
    let inputs: Vec<[&str; 1]> = texts.iter().map(|t| [*t]).collect();
    serde_json::json!({ "inputs": inputs, "truncate": true })
}

#[derive(Deserialize)]
struct Prediction {
    label: String,
    score: f64,
}

/// Map a TEI batch response to, per chunk, the non-benign labels scoring at
/// least `threshold`. A response whose shape or length does not match the
/// request is `Decode` — a verdict for the wrong number of chunks is not a
/// verdict.
fn flagged_labels(
    body: &[u8],
    expected: usize,
    threshold: f64,
) -> Result<Vec<Arc<[Box<str>]>>, ErrorKind> {
    let batch: Vec<Vec<Prediction>> =
        serde_json::from_slice(body).map_err(|_| ErrorKind::Decode)?;
    if batch.len() != expected {
        return Err(ErrorKind::Decode);
    }
    Ok(batch
        .into_iter()
        .map(|predictions| {
            predictions
                .into_iter()
                .filter(|p| {
                    p.score >= threshold
                        && !BENIGN_LABELS
                            .iter()
                            .any(|b| p.label.eq_ignore_ascii_case(b))
                })
                .map(|p| p.label.into_boxed_str())
                .collect()
        })
        .collect())
}

async fn read_capped(mut resp: reqwest::Response, cap: usize) -> Result<Vec<u8>, ErrorKind> {
    let mut body = Vec::new();
    while let Some(bytes) = resp
        .chunk()
        .await
        .map_err(|e| ErrorKind::from_reqwest(&e))?
    {
        if body.len() + bytes.len() > cap {
            return Err(ErrorKind::Decode);
        }
        body.extend_from_slice(&bytes);
    }
    Ok(body)
}

#[cfg(test)]
mod tests;
