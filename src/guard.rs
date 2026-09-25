//! Tier 0 request-content guard (LAB-3877).
//!
//! A synchronous, in-process rules-tier scan over each proxied request body,
//! run after `pre_request_gate` and before endpoint selection. It is
//! **detect-only and read-only**: it borrows the parsed body, never mutates it,
//! so the bytes forwarded upstream stay byte-identical to what the client sent
//! (Anthropic prompt caching matches on raw byte prefixes — see
//! `proxy_handler`). The whole module compiles only behind the `guard` cargo
//! feature; with the feature off, none of this — and none of its dependencies —
//! is built.
//!
//! Shadow mode is the default: the per-client policy ships as `annotate`, so the
//! layer counts and annotates but blocks nothing until false-positive rates have
//! been measured in production. `block` and `off` are opt-in per client.
//!
//! Findings carry **offsets only, never the matched text** — into logs, metrics,
//! and the 400 body alike. The matched secret never leaves the scanner.

use std::sync::atomic::{AtomicU64, Ordering};

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Per-client enforcement policy. Deserialized from `[[clients]].guard` in the
/// operator config; defaults to `annotate` (shadow mode).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum GuardPolicy {
    /// Skip scanning entirely. Also the effective policy for the operator-bypass
    /// client, regardless of config.
    Off,
    /// Scan, annotate, and count — but always allow the request through. The
    /// shadow-mode default.
    #[default]
    Annotate,
    /// Scan and reject a request with any finding (HTTP 400).
    Block,
}

/// A single detection. Offsets are byte offsets into the scanned input; the
/// matched text itself is deliberately absent so it cannot leak into logs, the
/// error body, or metrics.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct Finding {
    /// Scanner that produced this finding (e.g. `"secrets_scanner"`).
    pub scanner: &'static str,
    /// Scanner-specific detection label — a rule id or PII category. Never the
    /// matched value.
    pub detection_type: String,
    /// Byte offset of the match start within the scanned input.
    pub start: usize,
    /// Byte offset one past the match end within the scanned input.
    pub end: usize,
}

/// Outcome of a scan. Scanners themselves only ever produce `Allow` or
/// `Annotate`; `Block` is a policy escalation applied by [`Guard::evaluate`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Verdict {
    /// Nothing flagged.
    Allow,
    /// Findings present; the request proceeds (shadow mode).
    Annotate { findings: Vec<Finding> },
    /// Findings present and policy is `block`; the request is rejected.
    Block {
        findings: Vec<Finding>,
        reason: String,
    },
}

/// Upper bound on the bytes handed to the scanners, so the guard has a hard
/// worst-case latency on the request hot path. Bodies with a larger newest
/// user/tool_result span are scanned only up to this many bytes; the tail is
/// not inspected. Chosen so the combined scanner p99 stays comfortably under
/// the 2 ms budget on the release binary (see `scan_input_capped_under_budget`).
/// A leaked credential in a config/env paste almost always appears within the
/// first 32 KiB, so the coverage cost in shadow mode is small.
///
/// Latency note: the byte cap bounds scan cost for realistic content (prose or
/// code with up to ~100 detections in 32 KiB scans in ~1.2 ms). The PII
/// scanner's cost grows with the number of detections, so *degenerate*
/// content — 32 KiB of almost nothing but PII tokens (~1000 detections) — can
/// take ~14 ms. That is an adversarial p100, not a realistic p99, and it is
/// bounded (input capped here, body capped at the 25 MiB inflight limit); it
/// never leaks and never blocks another request's I/O.
///
// ponytail: fixed cap; scan runs inline on the async worker. If shadow-mode data
// shows finding-dense bodies are common enough that the ~14 ms p100 matters,
// the upgrade path is tokio::task::spawn_blocking for the scan (isolates the CPU
// burst from the executor) and/or a per-deployment cap knob — not removing the
// bound.
pub const MAX_SCAN_BYTES: usize = 32 * 1024;

/// The subset of a request body a scanner is allowed to see: the newest `user`
/// text blocks and the newest `tool_result` blocks. The `system` prompt is
/// deliberately excluded — it is operator-trusted here and a known
/// false-positive surface.
pub struct ScanInput {
    /// Newest user text + tool_result content, joined by newlines and truncated
    /// to [`MAX_SCAN_BYTES`]. Line boundaries between blocks stop a match from
    /// spanning two blocks.
    text: String,
    /// Whether the joined content was truncated to fit the byte cap.
    truncated: bool,
}

impl ScanInput {
    /// The concatenated text to scan.
    pub fn text(&self) -> &str {
        &self.text
    }

    /// Whether the input was truncated to [`MAX_SCAN_BYTES`] (the tail is
    /// unscanned). Surfaced in the guard log so an operator can tell whether the
    /// cap is biting during shadow-mode measurement.
    pub fn truncated(&self) -> bool {
        self.truncated
    }

    /// Build a scan input from a parsed Anthropic Messages body, extracting only
    /// the newest `user` message's text and tool_result blocks. Returns `None`
    /// when the body has no scannable user content.
    ///
    /// "Newest" = the last element of `messages` whose role is `user`. That is
    /// the turn being sent for completion (and, after tool use, the turn that
    /// carries the tool results). `system` is never read.
    pub fn from_body(body: &Value) -> Option<Self> {
        let messages = body.get("messages")?.as_array()?;
        // Last user-role message.
        let last_user = messages
            .iter()
            .rev()
            .find(|m| m.get("role").and_then(Value::as_str) == Some("user"))?;

        let mut segments: Vec<&str> = Vec::new();
        match last_user.get("content") {
            // Shorthand string content is user text.
            Some(Value::String(s)) => segments.push(s),
            Some(Value::Array(blocks)) => {
                for block in blocks {
                    match block.get("type").and_then(Value::as_str) {
                        Some("text") => {
                            if let Some(t) = block.get("text").and_then(Value::as_str) {
                                segments.push(t);
                            }
                        }
                        Some("tool_result") => collect_tool_result(block, &mut segments),
                        _ => {}
                    }
                }
            }
            _ => {}
        }

        if segments.is_empty() {
            return None;
        }

        let mut text = segments.join("\n");
        let truncated = text.len() > MAX_SCAN_BYTES;
        if truncated {
            // Truncate at the largest UTF-8 char boundary <= the cap.
            let mut end = MAX_SCAN_BYTES;
            while end > 0 && !text.is_char_boundary(end) {
                end -= 1;
            }
            text.truncate(end);
        }
        Some(ScanInput { text, truncated })
    }
}

/// A `tool_result` block's `content` is either a string or an array of content
/// blocks (typically `text`). Pull out every text span; ignore image/other.
fn collect_tool_result<'a>(block: &'a Value, out: &mut Vec<&'a str>) {
    match block.get("content") {
        Some(Value::String(s)) => out.push(s),
        Some(Value::Array(inner)) => {
            for b in inner {
                if b.get("type").and_then(Value::as_str) == Some("text") {
                    if let Some(t) = b.get("text").and_then(Value::as_str) {
                        out.push(t);
                    }
                }
            }
        }
        _ => {}
    }
}

/// A compact, deduplicated `scanner:detection_type` summary for the structured
/// `guard` log line — never any matched text. Bounded so a body crafted to
/// produce thousands of distinct detections cannot inflate one log line.
pub fn detections_summary(findings: &[Finding]) -> String {
    const MAX: usize = 20;
    let mut seen: Vec<String> = Vec::new();
    for f in findings {
        let label = format!("{}:{}", f.scanner, f.detection_type);
        if !seen.contains(&label) {
            seen.push(label);
            if seen.len() >= MAX {
                seen.push("…".to_string());
                break;
            }
        }
    }
    seen.join(",")
}

/// A synchronous content scanner. Implementations are built once at startup
/// (rule/regex compilation is not cheap) and shared read-only across requests.
///
/// A scanner reports findings; it does not decide policy. An empty result means
/// nothing was flagged. Enforcement (annotate vs block) is [`Guard::evaluate`]'s
/// job, so scanners never construct a [`Verdict`].
pub trait Scanner: Send + Sync {
    /// Stable label for logs and the `scanner` metric dimension. Also the value
    /// stamped into each [`Finding::scanner`] this scanner produces.
    fn name(&self) -> &'static str;
    /// Scan the input, returning every detection (empty = nothing flagged).
    fn scan(&self, input: &ScanInput) -> Vec<Finding>;
}

/// Secrets/credentials scanner backed by `secrets_scanner` with its bundled
/// (compiled-in) gitleaks/kingfisher ruleset and a hardened proxy config:
/// attacker-supplied inline allow markers are ignored and matched text is
/// redacted before it can be stored anywhere.
pub struct SecretsScanner {
    inner: secrets_scanner::Scanner,
}

impl SecretsScanner {
    pub fn new() -> Result<Self, String> {
        // Hardened for untrusted request bodies: ignore inline `gitleaks:allow`
        // markers a caller could embed to suppress detection, redact matched
        // text, skip context capture, cap output.
        let config = secrets_scanner::ScanConfig::proxy();
        // Fail-closed by construction: if a future change to `proxy()` softened
        // any of those guarantees, refuse to build rather than silently scan
        // untrusted content un-hardened.
        if !config.is_hardened() {
            return Err("secrets_scanner proxy config is not hardened".to_string());
        }
        let inner = secrets_scanner::Scanner::from_bundled()
            .map_err(|e| format!("secrets_scanner rule load failed: {e}"))?
            .with_config(config);
        Ok(Self { inner })
    }
}

impl Scanner for SecretsScanner {
    fn name(&self) -> &'static str {
        "secrets_scanner"
    }

    fn scan(&self, input: &ScanInput) -> Vec<Finding> {
        self.inner
            .scan_content("request-body", input.text())
            .into_iter()
            .map(|f| Finding {
                scanner: self.name(),
                detection_type: f.rule_id,
                start: f.start_offset,
                end: f.end_offset,
            })
            .collect()
    }
}

/// PII scanner backed by `leakguard` (zero-dependency, pure Rust). Detects
/// emails, cards, IPs, JWTs, national ids, and provider API-key shapes.
pub struct LeakGuardScanner {
    inner: leakguard::Redactor,
}

impl LeakGuardScanner {
    pub fn new() -> Self {
        Self {
            inner: leakguard::Redactor::new(),
        }
    }
}

impl Default for LeakGuardScanner {
    fn default() -> Self {
        Self::new()
    }
}

impl Scanner for LeakGuardScanner {
    fn name(&self) -> &'static str {
        "leakguard"
    }

    fn scan(&self, input: &ScanInput) -> Vec<Finding> {
        self.inner
            .find(input.text())
            .into_iter()
            .map(|m| Finding {
                scanner: self.name(),
                detection_type: format!("{:?}", m.kind),
                start: m.start,
                end: m.end,
            })
            .collect()
    }
}

/// Upper edges (seconds) of the `anthropic_guard_scan_duration_seconds`
/// histogram, plus an implicit `+Inf`. Sized around the sub-2ms budget so the
/// interesting quantiles land on real bucket boundaries.
const SCAN_DURATION_BUCKETS: [f64; 9] = [
    0.0001, 0.00025, 0.0005, 0.001, 0.002, 0.005, 0.01, 0.025, 0.05,
];

/// A Prometheus-style histogram over scan durations, lock-free on the hot path.
/// Per-bucket counts are non-cumulative here; `metrics_handler` cumulates them
/// at scrape time into the `le`-labelled series Prometheus expects.
pub struct ScanHistogram {
    /// One counter per bucket in [`SCAN_DURATION_BUCKETS`] plus a trailing
    /// `+Inf` overflow bucket.
    buckets: [AtomicU64; SCAN_DURATION_BUCKETS.len() + 1],
    sum_nanos: AtomicU64,
    count: AtomicU64,
}

impl ScanHistogram {
    fn new() -> Self {
        Self {
            buckets: Default::default(),
            sum_nanos: AtomicU64::new(0),
            count: AtomicU64::new(0),
        }
    }

    fn observe(&self, elapsed: std::time::Duration) {
        let secs = elapsed.as_secs_f64();
        let idx = SCAN_DURATION_BUCKETS
            .iter()
            .position(|&edge| secs <= edge)
            .unwrap_or(SCAN_DURATION_BUCKETS.len());
        self.buckets[idx].fetch_add(1, Ordering::Relaxed);
        self.sum_nanos
            .fetch_add(elapsed.as_nanos() as u64, Ordering::Relaxed);
        self.count.fetch_add(1, Ordering::Relaxed);
    }

    /// Snapshot for `/metrics`: `(le, cumulative_count)` per bucket (the final
    /// entry is `+Inf`), the sum in seconds, and the total observation count.
    pub fn snapshot(&self) -> (Vec<(String, u64)>, f64, u64) {
        let mut cumulative = 0u64;
        let mut series = Vec::with_capacity(self.buckets.len());
        for (i, b) in self.buckets.iter().enumerate() {
            cumulative += b.load(Ordering::Relaxed);
            // f64 Display gives the plain decimal Prometheus wants for these
            // edges (`0.0001`, not `1e-4`); no custom formatter needed.
            let le = if i < SCAN_DURATION_BUCKETS.len() {
                SCAN_DURATION_BUCKETS[i].to_string()
            } else {
                "+Inf".to_string()
            };
            series.push((le, cumulative));
        }
        let sum = self.sum_nanos.load(Ordering::Relaxed) as f64 / 1e9;
        (series, sum, self.count.load(Ordering::Relaxed))
    }
}

/// AWS's published documentation example secret access key (the `EXAMPLEKEY`
/// suffix marks it as such) — not a credential, and allow-listed by secret
/// scanners for exactly that reason. Guard tests need a value that matches a
/// real secret rule so the scanner fires; a synthetic placeholder would not
/// match, and the tests would prove nothing.
#[cfg(test)]
pub(crate) const AWS_DOCS_EXAMPLE_SECRET_KEY: &str = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";

/// Cardinality cap on the `client` dimension of the verdicts counter, so an
/// unbounded stream of distinct client ids cannot grow the map without limit.
/// Overflow folds into `_other`. Mirrors the `beta_flags_dropped` posture.
const MAX_VERDICT_CLIENTS: usize = 256;

/// The assembled guard: the scanners plus their metrics. Constructed once and
/// held in `AppState`.
pub struct Guard {
    scanners: Vec<Box<dyn Scanner>>,
    /// `{(client, scanner, verdict) -> count}` for
    /// `anthropic_guard_verdicts_total`.
    verdicts:
        std::sync::Mutex<std::collections::HashMap<(String, &'static str, &'static str), u64>>,
    scan_hist: ScanHistogram,
}

impl Guard {
    /// Build the guard with the two Tier 0 scanners. Fails if the secrets
    /// ruleset cannot be loaded — a broken guard fails loud at startup rather
    /// than silently scanning nothing.
    pub fn new() -> Result<Self, String> {
        let scanners: Vec<Box<dyn Scanner>> = vec![
            Box::new(SecretsScanner::new()?),
            Box::new(LeakGuardScanner::new()),
        ];
        Ok(Self {
            scanners,
            verdicts: std::sync::Mutex::new(std::collections::HashMap::new()),
            scan_hist: ScanHistogram::new(),
        })
    }

    /// A guard with no scanners — always `Allow`. Test-only, so `AppState`
    /// fixtures that don't exercise the guard don't pay the ruleset-compile
    /// cost on every construction.
    #[cfg(test)]
    pub fn empty() -> Self {
        Self {
            scanners: Vec::new(),
            verdicts: std::sync::Mutex::new(std::collections::HashMap::new()),
            scan_hist: ScanHistogram::new(),
        }
    }

    /// Run the scanners under a client's policy and return the request verdict.
    ///
    /// Records the scan-duration histogram and one verdict counter per scanner.
    /// `Off` and an absent/empty input short-circuit to `Allow` with no scan and
    /// no metrics. Enforcement (`Block`) is applied here from `policy`; the
    /// scanners themselves only report findings.
    pub fn evaluate(
        &self,
        policy: GuardPolicy,
        client_id: &str,
        input: Option<&ScanInput>,
    ) -> Verdict {
        if policy == GuardPolicy::Off {
            return Verdict::Allow;
        }
        let input = match input {
            Some(i) if !i.text.is_empty() => i,
            _ => return Verdict::Allow,
        };

        let start = std::time::Instant::now();
        let mut all: Vec<Finding> = Vec::new();
        // Per-scanner outcome for the metric label, recorded after the timed
        // section so timing captures only scan work.
        let mut per_scanner: Vec<(&'static str, bool)> = Vec::with_capacity(self.scanners.len());
        for scanner in &self.scanners {
            let findings = scanner.scan(input);
            per_scanner.push((scanner.name(), !findings.is_empty()));
            all.extend(findings);
        }
        self.scan_hist.observe(start.elapsed());

        let blocked = policy == GuardPolicy::Block && !all.is_empty();
        // verdict label reflects the request outcome, per scanner: a scanner
        // that fired gets the enforced outcome; one that didn't gets `allow`.
        for (name, hit) in per_scanner {
            let verdict = if !hit {
                "allow"
            } else if blocked {
                "block"
            } else {
                "annotate"
            };
            self.record_verdict(client_id, name, verdict);
        }

        if all.is_empty() {
            Verdict::Allow
        } else if blocked {
            Verdict::Block {
                findings: all,
                reason: "request body contains content flagged by the guard layer".to_string(),
            }
        } else {
            Verdict::Annotate { findings: all }
        }
    }

    fn record_verdict(&self, client_id: &str, scanner: &'static str, verdict: &'static str) {
        let mut map = crate::lock_recovering(&self.verdicts, "guard_verdicts");
        let key = (client_id.to_owned(), scanner, verdict);
        let bounded = if map.len() < MAX_VERDICT_CLIENTS || map.contains_key(&key) {
            key
        } else {
            ("_other".to_owned(), scanner, verdict)
        };
        *map.entry(bounded).or_insert(0) += 1;
    }

    /// Snapshot of the verdicts counter for `/metrics`.
    pub fn verdicts_snapshot(&self) -> Vec<((String, &'static str, &'static str), u64)> {
        crate::lock_recovering(&self.verdicts, "guard_verdicts")
            .iter()
            .map(|(k, v)| (k.clone(), *v))
            .collect()
    }

    /// Snapshot of the scan-duration histogram for `/metrics`.
    pub fn scan_hist_snapshot(&self) -> (Vec<(String, u64)>, f64, u64) {
        self.scan_hist.snapshot()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn input(text: &str) -> ScanInput {
        ScanInput {
            text: text.to_string(),
            truncated: false,
        }
    }

    #[test]
    fn scan_input_extracts_newest_user_and_tool_result_never_system() {
        let body = json!({
            "system": "operator system prompt with a fake token ghp_ZZZ",
            "messages": [
                {"role": "user", "content": "old turn"},
                {"role": "assistant", "content": [{"type": "text", "text": "sure"}]},
                {"role": "user", "content": [
                    {"type": "text", "text": "please read this"},
                    {"type": "tool_result", "content": [
                        {"type": "text", "text": "file contents here"}
                    ]}
                ]}
            ]
        });
        let si = ScanInput::from_body(&body).expect("scannable");
        assert!(si.text().contains("please read this"));
        assert!(si.text().contains("file contents here"));
        // Never the system prompt, never older turns.
        assert!(!si.text().contains("operator system prompt"));
        assert!(!si.text().contains("old turn"));
    }

    #[test]
    fn scan_input_none_without_user_content() {
        let body = json!({"system": "x", "messages": [
            {"role": "assistant", "content": [{"type": "text", "text": "hi"}]}
        ]});
        assert!(ScanInput::from_body(&body).is_none());
    }

    #[test]
    fn scan_input_tool_result_string_content() {
        let body = json!({"messages": [
            {"role": "user", "content": [
                {"type": "tool_result", "content": "plain string result"}
            ]}
        ]});
        let si = ScanInput::from_body(&body).expect("scannable");
        assert_eq!(si.text(), "plain string result");
    }

    #[test]
    fn secrets_scanner_positive_and_negative() {
        let s = SecretsScanner::new().expect("rules");
        // Positive: AWS's documented example secret access key — shaped like
        // the real thing so the rule fires.
        let findings = s.scan(&input(&format!(
            "aws_secret_access_key = \"{}\"",
            AWS_DOCS_EXAMPLE_SECRET_KEY
        )));
        assert!(!findings.is_empty(), "expected a secret finding");
        assert!(findings.iter().all(|f| f.scanner == "secrets_scanner"));
        assert!(findings.iter().all(|f| f.end > f.start));
        // Negative: ordinary prose.
        assert!(s
            .scan(&input("the quick brown fox jumps over the lazy dog"))
            .is_empty());
    }

    #[test]
    fn leakguard_positive_and_negative() {
        let s = LeakGuardScanner::new();
        let findings = s.scan(&input("email me at alice@example.com"));
        assert!(findings.iter().any(|f| f.detection_type == "Email"));
        assert!(findings.iter().all(|f| f.scanner == "leakguard"));
        assert!(s.scan(&input("no personal data here")).is_empty());
    }

    #[test]
    fn policy_dispatch_all_three_modes() {
        let g = Guard::new().expect("guard");
        let dirty = input("contact alice@example.com");

        // off: never scans, always Allow.
        assert_eq!(
            g.evaluate(GuardPolicy::Off, "c", Some(&dirty)),
            Verdict::Allow
        );

        // annotate: findings but request proceeds.
        match g.evaluate(GuardPolicy::Annotate, "c", Some(&dirty)) {
            Verdict::Annotate { findings } => assert!(!findings.is_empty()),
            other => panic!("expected annotate, got {other:?}"),
        }

        // block: findings reject.
        match g.evaluate(GuardPolicy::Block, "c", Some(&dirty)) {
            Verdict::Block { findings, reason } => {
                assert!(!findings.is_empty());
                assert!(!reason.is_empty());
            }
            other => panic!("expected block, got {other:?}"),
        }

        // clean input under block still allows.
        let clean = input("nothing to see here");
        assert_eq!(
            g.evaluate(GuardPolicy::Block, "c", Some(&clean)),
            Verdict::Allow
        );
    }

    #[test]
    fn findings_carry_offsets_never_matched_text() {
        // The Finding type has no field for matched text; serialization proves
        // offsets are all that is emitted.
        let f = Finding {
            scanner: "leakguard",
            detection_type: "Email".to_string(),
            start: 3,
            end: 20,
        };
        let v = serde_json::to_value(&f).unwrap();
        assert_eq!(v["start"], 3);
        assert_eq!(v["end"], 20);
        assert_eq!(v["scanner"], "leakguard");
        assert_eq!(v["detection_type"], "Email");
        assert!(v.get("matched").is_none());
        assert!(v.get("text").is_none());
    }

    #[test]
    fn scan_is_read_only_body_unchanged() {
        // Guard extraction borrows the body; the parsed Value must be untouched.
        let body = json!({"messages": [
            {"role": "user", "content": [
                {"type": "text", "text": format!("token {}", AWS_DOCS_EXAMPLE_SECRET_KEY)}
            ]}
        ]});
        let before = serde_json::to_vec(&body).unwrap();
        let g = Guard::new().expect("guard");
        let si = ScanInput::from_body(&body);
        let _ = g.evaluate(GuardPolicy::Block, "c", si.as_ref());
        let after = serde_json::to_vec(&body).unwrap();
        assert_eq!(before, after, "guard must not mutate the request body");
    }

    /// Build a 200 KB Anthropic body whose newest user turn holds the whole
    /// payload — the worst case for the guard (the entire body is scannable
    /// user content). A detection is embedded roughly every ~300 bytes so the
    /// scanned window (capped at [`MAX_SCAN_BYTES`]) carries ~100 findings: this
    /// exercises the finding-extraction path (allocation, detection_type
    /// formatting, offset math), not just the fast keyword reject. ~100
    /// detections in 32 KiB is a realistic-dense worst case; pathological
    /// all-PII content is a documented ceiling on `MAX_SCAN_BYTES`, not the p99.
    fn body_200kb() -> Value {
        let mut big = String::with_capacity(210_000);
        while big.len() < 200_000 {
            big.push_str(
                "lorem ipsum dolor sit amet consectetur adipiscing elit sed do \
                 eiusmod tempor incididunt ut labore et dolore magna aliqua ut \
                 enim contact ops@example.com and more filler words here too ",
            );
        }
        big.truncate(200_000);
        json!({
            "system": "you are a helpful assistant",
            "messages": [{"role": "user", "content": big}]
        })
    }

    #[test]
    fn scan_input_capped_at_max_scan_bytes() {
        let si = ScanInput::from_body(&body_200kb()).expect("scannable");
        assert!(si.truncated(), "a 200KB user turn must trip the cap");
        assert!(
            si.text().len() <= MAX_SCAN_BYTES,
            "scanned bytes {} exceeded cap {MAX_SCAN_BYTES}",
            si.text().len()
        );
    }

    #[test]
    fn scan_200kb_body_under_budget_p99() {
        // AC: scan time for a 200 KB body is under 2 ms p99 on the release
        // binary. The byte cap bounds the scan regardless of body size, so this
        // holds for any body. Debug builds run the same regexes far slower, so
        // assert the tight 2 ms bound only in release (run
        // `cargo test --release --features guard` for the real measurement) and
        // a loose sanity bound in debug.
        let g = Guard::new().expect("guard");
        // Go through the real extraction path so the cap is part of the
        // measurement, then reuse the (capped) input across iterations to time
        // the scan itself.
        let si = ScanInput::from_body(&body_200kb()).expect("scannable");

        // Guard against the test silently regressing to the fast-reject path:
        // the scanned window must actually produce findings, so the timed loop
        // exercises finding extraction, not just keyword rejection.
        match g.evaluate(GuardPolicy::Annotate, "c", Some(&si)) {
            Verdict::Annotate { findings } => assert!(
                findings.len() >= 50,
                "perf payload must be finding-dense (got {} findings) so the \
                 extraction path is measured",
                findings.len()
            ),
            other => panic!("perf payload produced no findings: {other:?}"),
        }

        let iters = if cfg!(debug_assertions) { 30 } else { 300 };
        let mut samples: Vec<std::time::Duration> = Vec::with_capacity(iters);
        for _ in 0..iters {
            let t = std::time::Instant::now();
            let _ = g.evaluate(GuardPolicy::Annotate, "c", Some(&si));
            samples.push(t.elapsed());
        }
        samples.sort();
        let p99 = samples[((iters as f64 * 0.99) as usize).saturating_sub(1)];

        let budget = if cfg!(debug_assertions) {
            std::time::Duration::from_millis(100)
        } else {
            std::time::Duration::from_millis(2)
        };
        assert!(
            p99 < budget,
            "200KB-body scan p99 {p99:?} exceeded budget {budget:?} (cap {MAX_SCAN_BYTES}B)"
        );
    }

    #[test]
    fn histogram_cumulates_and_counts() {
        let h = ScanHistogram::new();
        h.observe(std::time::Duration::from_micros(50)); // <=0.0001
        h.observe(std::time::Duration::from_micros(300)); // <=0.0005
        h.observe(std::time::Duration::from_millis(3)); // <=0.005
        let (series, sum, count) = h.snapshot();
        assert_eq!(count, 3);
        assert!(sum > 0.0);
        // Cumulative and monotonic non-decreasing; final +Inf bucket holds all.
        assert_eq!(series.last().unwrap().0, "+Inf");
        assert_eq!(series.last().unwrap().1, 3);
        for w in series.windows(2) {
            assert!(w[1].1 >= w[0].1, "buckets must be cumulative");
        }
    }
}
