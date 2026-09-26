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
//! finding. A circuit breaker — the same [`Breaker`] the upstream transport
//! path uses — stops a dead or saturated detector from costing every request a
//! full timeout: once open, requests take their failure path immediately.
//!
//! The two policies are separate [`Lane`]s, each with its own breaker and
//! in-flight cap. Shadow traffic is every unconfigured client and is free to
//! send, so if it shared admission with `block` it could fill the slots, or
//! time the breaker open, and turn a healthy detector into `503`s for the
//! clients that enforce.
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

use super::{detections_summary, DurationHistogram, Finding, GuardPolicy, VerdictCounts};
use crate::breaker::Breaker;

pub const DEFAULT_TIMEOUT_MS: u64 = 2000;
pub const DEFAULT_THRESHOLD: f64 = 0.5;
pub const DEFAULT_CHUNK_TOKENS: usize = 512;
pub const DEFAULT_CACHE_SIZE: u64 = 10_000;
pub const DEFAULT_BREAKER_THRESHOLD: u32 = crate::utilization::TRANSPORT_FAILURE_THRESHOLD;
pub const DEFAULT_BREAKER_COOLDOWN_SECS: u64 = 30;

/// Bytes of overlap between consecutive chunks, so an injection straddling a
/// boundary is whole in at least one chunk: ~32 tokens of prose, the language
/// an injection is written in.
const OVERLAP_BYTES: usize = 128;
/// `[CLS]` and `[SEP]`, which the sidecar adds to every input.
const SPECIAL_TOKENS: usize = 2;
/// TEI's default `--max-client-batch-size`. A `block` input larger than this
/// splits into several batches, issued concurrently.
const MAX_BATCH: usize = 32;
/// Chunks per `annotate` call, sent one call at a time. The classifier is one
/// FIFO queue shared by both lanes, so this bounds the shadow work a `block`
/// call can find ahead of it: at most `breaker_threshold` shadow calls are in
/// flight, so at most that many chunks. One, because on CPU each chunk ahead
/// costs tens to hundreds of milliseconds (measured: three in-flight batches
/// of eight pushed a short `block` call past a 2 s deadline). Throughput does
/// not suffer: TEI packs queued inputs from concurrent calls into one pass.
const SHADOW_BATCH: usize = 1;
/// Cap on a detector response body. A classifier answer for 32 chunks is a few
/// KiB; anything near this is a misbehaving sidecar, not a verdict.
const MAX_RESPONSE_BYTES: usize = 1024 * 1024;
/// Labels that mean "nothing found". Every other label at or above `threshold`
/// is a finding. Covers Prompt Guard 2 (`BENIGN`/`MALICIOUS`) and ProtectAI's
/// DeBERTa (`SAFE`/`INJECTION`) without a per-model knob. A model that labels
/// its classes anything else (`LABEL_0`) flags everything: loud in shadow mode,
/// and the fail-closed direction under `block`.
const BENIGN_LABELS: [&str; 2] = ["BENIGN", "SAFE"];
/// Labels become finding labels, log fields and cache entries, so a sidecar
/// cannot hand the proxy an arbitrary string for any of them.
const MAX_LABEL_BYTES: usize = 64;
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
    /// Under `block`, the wall-clock ceiling for classifying one request, all
    /// chunks included. Under `annotate`, the ceiling per batch of chunks.
    #[serde(default = "default_timeout_ms")]
    pub timeout_ms: u64,
    /// Minimum label score that counts as a finding.
    #[serde(default = "default_threshold")]
    pub threshold: f64,
    /// Under `block`: forward with a `guard_unavailable` finding (true) or
    /// reject with 503 (false) when the detector gives no verdict.
    #[serde(default)]
    pub fail_open: bool,
    /// The model's input window in tokens; chunks are sized so they always fit
    /// (see `Detector::new`).
    #[serde(default = "default_chunk_tokens")]
    pub chunk_tokens: usize,
    /// Verdict memo entries, keyed by chunk digest. 0 disables the cache.
    #[serde(default = "default_cache_size")]
    pub cache_size: u64,
    /// Consecutive failed requests before a lane's breaker opens. Also each
    /// lane's cap on detector calls in flight at once (see `Detector::admit`).
    #[serde(default = "default_breaker_threshold")]
    pub breaker_threshold: u32,
    /// How long a lane's breaker stays open before one probe is let through.
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
    /// `breaker_threshold` calls were already in flight: no call was made.
    Saturated,
    /// A call was made and failed.
    Failed(ErrorKind),
}

/// The inline (`block`) outcome, for `guard_hook` to turn into a response.
#[derive(Debug)]
pub enum Enforced {
    Allow,
    /// No verdict and `fail_open = true`: forward with one `guard_unavailable`
    /// finding.
    FailOpen,
    Block(Vec<Finding>),
    /// No verdict and `fail_open = false`.
    Unavailable,
}

/// Which admission lane a classification uses — one per policy that calls the
/// detector.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Lane {
    /// `annotate`, in the background.
    Shadow = 0,
    /// `block`, inline.
    Enforce = 1,
}

impl Lane {
    pub const ALL: [Lane; 2] = [Lane::Shadow, Lane::Enforce];

    /// The `lane` metric label: the policy the lane serves.
    pub fn label(self) -> &'static str {
        match self {
            Lane::Shadow => "annotate",
            Lane::Enforce => "block",
        }
    }
}

/// One lane's breaker and admission bookkeeping, plus counts of the requests
/// it turned away without a call.
#[derive(Default)]
struct Gate {
    state: Mutex<GateState>,
    rejected_open: AtomicU64,
    rejected_saturated: AtomicU64,
}

/// Behind one std mutex, never held across an await.
#[derive(Default)]
struct GateState {
    breaker: Breaker,
    /// A half-open probe is in flight. Cleared when its [`Permit`] drops,
    /// which happens whether the probe finished, timed out, or had its future
    /// dropped by a disconnecting `block` client — so the marker can neither
    /// lapse early (letting a second probe in) nor wedge the lane open.
    probe_in_flight: bool,
    /// Bumped on every OPEN and CLOSED transition. A call's outcome only
    /// counts against the era it was admitted in: a slow call admitted while
    /// closed must not close a breaker that has since opened, nor re-open one
    /// a probe has since closed.
    era: u64,
    /// Detector calls admitted and not yet finished.
    in_flight: u32,
}

/// One admitted detector call. Dropping it releases the in-flight slot (and
/// the probe marker, for a probe) on every path out of the call.
struct Permit<'a> {
    gate: &'a Gate,
    era: u64,
    probe: bool,
}

impl Drop for Permit<'_> {
    fn drop(&mut self) {
        let mut s = crate::lock_recovering(&self.gate.state, "guard_detector_gate");
        s.in_flight = s.in_flight.saturating_sub(1);
        if self.probe {
            s.probe_in_flight = false;
        }
    }
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
    /// Chunk digest → labels at or above threshold (empty = benign). Holds no
    /// request text.
    cache: moka::sync::Cache<[u8; 32], Arc<[Box<str>]>>,
    /// Indexed by [`Lane`].
    lanes: [Gate; 2],
    breaker_threshold: u32,
    cooldown: Duration,
    errors: [AtomicU64; ErrorKind::ALL.len()],
    cache_hits: AtomicU64,
    cache_misses: AtomicU64,
    duration: DurationHistogram,
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
        // Chunks are sized in bytes (the proxy has no tokenizer), and Prompt
        // Guard 2's tokenizer yields at most one token per byte: the measured
        // worst case is punctuation, 510 bytes to 512 tokens. So a chunk of
        // `chunk_tokens - 2` bytes always fits the window. Estimating from
        // typical density instead (a couple of bytes per token) let dense
        // content overflow, and the sidecar truncated the tail — which, in a
        // tool result, the attacker writes. Requests are sent with
        // `truncate: false`, so a tokenizer that breaks the bound fails the
        // call instead of dropping text.
        if cfg.chunk_tokens <= SPECIAL_TOKENS + 2 * OVERLAP_BYTES {
            return Err(bad(format!(
                "chunk_tokens must be > {}",
                SPECIAL_TOKENS + 2 * OVERLAP_BYTES
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
            chunk_bytes: cfg.chunk_tokens - SPECIAL_TOKENS,
            cache: moka::sync::Cache::new(cfg.cache_size),
            lanes: Default::default(),
            breaker_threshold: cfg.breaker_threshold,
            cooldown: Duration::from_secs(cfg.breaker_cooldown_secs),
            errors: Default::default(),
            cache_hits: AtomicU64::new(0),
            cache_misses: AtomicU64::new(0),
            duration: DurationHistogram::new(DURATION_BUCKETS),
            verdicts: VerdictCounts::default(),
        })
    }

    pub fn name(&self) -> &'static str {
        self.name
    }

    /// Classify `text` for `client_id` in `lane`. Under `Enforce` every chunk
    /// shares one `timeout` deadline; under `Shadow` each batch has its own.
    pub async fn classify(&self, lane: Lane, client_id: &str, text: &str) -> Classified {
        let chunks = chunk(text, self.chunk_bytes, OVERLAP_BYTES);
        let mut findings = Vec::new();
        let mut misses: Vec<(usize, &str, [u8; 32])> = Vec::new();
        for (start, piece) in chunks {
            let digest = cache_key(client_id, piece);
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
        let permit = match self.admit(lane, Instant::now()) {
            Ok(permit) => permit,
            Err(unavailable) => {
                let gate = self.gate(lane);
                let counter = match unavailable {
                    Unavailable::Saturated => &gate.rejected_saturated,
                    _ => &gate.rejected_open,
                };
                counter.fetch_add(1, Ordering::Relaxed);
                return Classified {
                    findings,
                    unavailable: Some(unavailable),
                };
            }
        };

        let started = Instant::now();
        let failure = match lane {
            // `block` waits on the answer, so every batch goes out at once
            // under one deadline: the request is held at most `timeout_ms`.
            Lane::Enforce => {
                let batches = misses
                    .chunks(MAX_BATCH)
                    .map(|batch| self.predict(texts(batch)));
                match tokio::time::timeout(self.timeout, join_all(batches)).await {
                    // The deadline covers every batch: none of them is a verdict.
                    Err(_) => Some(ErrorKind::Timeout),
                    Ok(results) => {
                        let mut failure = None;
                        for (batch, result) in misses.chunks(MAX_BATCH).zip(results) {
                            match result {
                                Ok(labels) => self.absorb(&mut findings, batch, labels),
                                Err(kind) => failure = failure.or(Some(kind)),
                            }
                        }
                        failure
                    }
                }
            }
            // Nothing waits on `annotate`, so it trickles: one chunk at a time
            // (see `SHADOW_BATCH`), each under its own deadline. A long
            // input takes longer in total, and costs nobody for it.
            Lane::Shadow => {
                let mut failure = None;
                for batch in misses.chunks(SHADOW_BATCH) {
                    match tokio::time::timeout(self.timeout, self.predict(texts(batch))).await {
                        Ok(Ok(labels)) => self.absorb(&mut findings, batch, labels),
                        Ok(Err(kind)) => failure = Some(kind),
                        Err(_) => failure = Some(ErrorKind::Timeout),
                    }
                    if failure.is_some() {
                        break;
                    }
                }
                failure
            }
        };
        self.duration.observe(started.elapsed());
        self.record_outcome(lane, &permit, failure, Instant::now());
        drop(permit);
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
            .map_err(|e| self.call_failed(ErrorKind::from_reqwest(&e), &e))?;
        let status = resp.status();
        if !status.is_success() {
            return Err(self.call_failed(ErrorKind::Status, &status));
        }
        let body = read_capped(resp, MAX_RESPONSE_BYTES)
            .await
            .map_err(|(kind, detail)| self.call_failed(kind, &detail))?;
        flagged_labels(&body, expected, self.threshold)
            .map_err(|detail| self.call_failed(ErrorKind::Decode, &detail))
    }

    /// Log why one detector call failed — the HTTP status, the transport
    /// error, what was wrong with the body — and return its kind. The metric
    /// keeps only the kind; this line is what an operator debugs from. It
    /// never carries request text, and the breaker bounds how often it fires
    /// during an outage.
    fn call_failed(&self, kind: ErrorKind, detail: &dyn std::fmt::Display) -> ErrorKind {
        warn!(
            detector = self.name,
            kind = kind.label(),
            error = %detail,
            "guard detector call failed"
        );
        kind
    }

    /// Record one batch's verdicts: findings for this request, and the cache
    /// for the next.
    fn absorb(
        &self,
        findings: &mut Vec<Finding>,
        batch: &[(usize, &str, [u8; 32])],
        labels: Vec<Arc<[Box<str>]>>,
    ) {
        for ((start, piece, digest), labels) in batch.iter().zip(labels) {
            self.push_findings(findings, *start, piece.len(), &labels);
            self.cache.insert(*digest, labels);
        }
    }

    fn push_findings(&self, out: &mut Vec<Finding>, start: usize, len: usize, labels: &[Box<str>]) {
        out.extend(labels.iter().map(|label| Finding {
            scanner: self.name,
            detection_type: label.to_string(),
            start,
            end: start + len,
        }));
    }

    /// Admit one detector call, or say why not.
    ///
    /// At most `breaker_threshold` calls are in flight at once. Without the
    /// cap, every request that arrives before the first failure lands is
    /// admitted to a hung detector, so a burst pays `timeout_ms` a hundred
    /// times over before the breaker hears about any of it. With it, the
    /// first `breaker_threshold` calls are the only ones that can be caught
    /// out, and their failures are exactly what opens the breaker. The
    /// classifier gains no throughput from concurrent requests (it queues
    /// them), so a request past the cap would mostly have waited in that
    /// queue anyway.
    fn gate(&self, lane: Lane) -> &Gate {
        &self.lanes[lane as usize]
    }

    fn admit(&self, lane: Lane, now: Instant) -> Result<Permit<'_>, Unavailable> {
        let gate = self.gate(lane);
        let mut s = crate::lock_recovering(&gate.state, "guard_detector_gate");
        if s.breaker.is_open(now) {
            return Err(Unavailable::CircuitOpen);
        }
        let probe = s.breaker.is_half_open(now);
        if probe {
            if s.probe_in_flight {
                return Err(Unavailable::CircuitOpen);
            }
            s.probe_in_flight = true;
        } else if s.in_flight >= self.breaker_threshold {
            return Err(Unavailable::Saturated);
        }
        s.in_flight += 1;
        Ok(Permit {
            gate,
            era: s.era,
            probe,
        })
    }

    /// Feed the breaker one call's outcome. After the cooldown the probe's
    /// outcome decides: a success closes the breaker, a failure re-opens it at
    /// once (a threshold of one) rather than waiting for a fresh run of
    /// `breaker_threshold` failures — the detector has already proved itself
    /// down once, and each extra failure is a request paying `timeout_ms`.
    ///
    /// An outcome from an earlier era is counted in the error metric and
    /// otherwise ignored; see `BreakerState::era`.
    fn record_outcome(
        &self,
        lane: Lane,
        permit: &Permit<'_>,
        failure: Option<ErrorKind>,
        now: Instant,
    ) {
        if let Some(kind) = failure {
            self.errors[kind as usize].fetch_add(1, Ordering::Relaxed);
        }
        let mut s = crate::lock_recovering(&permit.gate.state, "guard_detector_gate");
        if permit.era != s.era {
            return;
        }
        match failure {
            None => {
                if s.breaker.record_success() {
                    s.era += 1;
                    info!(
                        detector = self.name,
                        lane = lane.label(),
                        "detector circuit-breaker CLOSED: detector recovered"
                    );
                }
            }
            Some(_) => {
                let threshold = if permit.probe {
                    1
                } else {
                    self.breaker_threshold
                };
                if s.breaker.record_failure(now, threshold, self.cooldown) {
                    s.era += 1;
                    warn!(
                        detector = self.name,
                        lane = lane.label(),
                        consecutive_failures = s.breaker.consecutive_failures,
                        cooldown_secs = self.cooldown.as_secs(),
                        fail_open = self.fail_open,
                        "detector circuit-breaker OPEN: requests get no verdict, without a call, until a probe succeeds"
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
            let classified = detector.classify(Lane::Shadow, &client_id, &text).await;
            detector.report(&req_id, &client_id, GuardPolicy::Annotate, classified);
        });
    }

    /// `block`: classify inline and decide.
    pub async fn enforce(&self, req_id: &str, client_id: &str, text: &str) -> Enforced {
        let classified = self.classify(Lane::Enforce, client_id, text).await;
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
        // No verdict. An open breaker already logged its OPEN line, and both
        // it and saturation count every request they turn away, so a
        // per-request line would only flood the log for the length of an
        // outage or a burst.
        let outcome = if self.fail_open {
            Enforced::FailOpen
        } else {
            Enforced::Unavailable
        };
        if let Unavailable::Failed(kind) = unavailable {
            let verdict = match (enforcing, self.fail_open) {
                (_, true) => "annotate",
                (true, false) => "block",
                (false, false) => "would-block",
            };
            if enforcing && !self.fail_open {
                warn!(req_id, client_id = %client_id, verdict, reason = GUARD_UNAVAILABLE, cause = kind.label(), "guard detector");
            } else {
                info!(req_id, client_id = %client_id, verdict, reason = GUARD_UNAVAILABLE, cause = kind.label(), "guard detector");
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

    /// Requests `lane` turned away without a call: `(breaker open, saturated)`.
    pub fn turned_away(&self, lane: Lane) -> (u64, u64) {
        let gate = self.gate(lane);
        (
            gate.rejected_open.load(Ordering::Relaxed),
            gate.rejected_saturated.load(Ordering::Relaxed),
        )
    }

    /// `lane`'s breaker has opened and no probe has closed it yet: open, or
    /// cooled down and awaiting its probe. Wider than `Breaker::is_open`,
    /// which is false once the cooldown has run out.
    pub fn tripped(&self, lane: Lane) -> bool {
        crate::lock_recovering(&self.gate(lane).state, "guard_detector_gate")
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

fn texts<'a>(batch: &[(usize, &'a str, [u8; 32])]) -> Vec<&'a str> {
    batch.iter().map(|(_, piece, _)| *piece).collect()
}

/// Verdict-cache key for one chunk, namespaced by client. A cache shared across
/// clients would be a timing oracle: under `block` a hit answers a detector
/// round-trip faster than a miss, so one client could test whether another had
/// recently sent a given chunk. Retries and resent turns — what the cache is
/// for — come from the same client anyway. The length prefix keeps
/// `(client, chunk)` pairs from colliding by concatenation.
fn cache_key(client_id: &str, chunk: &str) -> [u8; 32] {
    let mut h = Blake2s256::new();
    h.update((client_id.len() as u64).to_le_bytes());
    h.update(client_id.as_bytes());
    h.update(chunk.as_bytes());
    h.finalize().into()
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
/// (a two-element list would be read as a sentence pair). `truncate: false`
/// overrides a server started with `--auto-truncate`: an over-long chunk fails
/// the call (422) rather than being classified without its tail.
fn predict_request(texts: &[&str]) -> serde_json::Value {
    let inputs: Vec<[&str; 1]> = texts.iter().map(|t| [*t]).collect();
    serde_json::json!({ "inputs": inputs, "truncate": false })
}

#[derive(Deserialize)]
struct Prediction {
    label: String,
    score: f64,
}

/// Map a TEI batch response to, per chunk, the non-benign labels scoring at
/// least `threshold`. A response whose shape or length does not match the
/// request, that labels some input with nothing, or that carries a label
/// outside `MAX_LABEL_BYTES` of `[A-Za-z0-9_-]` is an error — a verdict for the
/// wrong number of chunks is not a verdict. The error says what was wrong,
/// never what the body held.
fn flagged_labels(
    body: &[u8],
    expected: usize,
    threshold: f64,
) -> Result<Vec<Arc<[Box<str>]>>, String> {
    // Category and position only: serde_json's message quotes a wrong-type
    // string, which from a sidecar echoing its input is request text.
    let batch: Vec<Vec<Prediction>> = serde_json::from_slice(body).map_err(|e| {
        format!(
            "not a TEI batch response ({:?} error at line {} column {})",
            e.classify(),
            e.line(),
            e.column()
        )
    })?;
    if batch.len() != expected {
        return Err(format!("{} predictions for {expected} inputs", batch.len()));
    }
    // An input with no predictions at all would otherwise read as benign.
    if batch.iter().any(Vec::is_empty) {
        return Err("an input has no predictions".to_string());
    }
    let valid = |l: &str| {
        !l.is_empty()
            && l.len() <= MAX_LABEL_BYTES
            && l.bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
    };
    if batch.iter().flatten().any(|p| !valid(&p.label)) {
        return Err("a label is empty, too long, or not [A-Za-z0-9_-]".to_string());
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

async fn read_capped(
    mut resp: reqwest::Response,
    cap: usize,
) -> Result<Vec<u8>, (ErrorKind, String)> {
    let mut body = Vec::new();
    while let Some(bytes) = resp
        .chunk()
        .await
        .map_err(|e| (ErrorKind::from_reqwest(&e), e.to_string()))?
    {
        if body.len() + bytes.len() > cap {
            return Err((ErrorKind::Decode, format!("response exceeds {cap} bytes")));
        }
        body.extend_from_slice(&bytes);
    }
    Ok(body)
}

#[cfg(test)]
mod tests;
