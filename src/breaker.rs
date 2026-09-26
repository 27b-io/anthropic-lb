//! Consecutive-failure circuit breaker (LAB-3878).
//!
//! One state machine, two callers: the per-endpoint upstream transport breaker
//! (`record_transport_failure` / `record_transport_success`) and the guard's
//! detector client. It was lifted out of `RateLimitInfo` rather than copied so
//! the two cannot drift: a threshold of consecutive failures opens it for a
//! cooldown, any success closes it, and an expired cooldown starts a fresh era
//! so an old breaker's failures never count toward re-opening a new one.
//!
//! The breaker holds state and answers questions; it never logs. Each caller
//! owns its OPEN/CLOSED log line, because the line has to name what left
//! service (an endpoint, a detector) and the caller is the one that knows.

use std::time::{Duration, Instant};

#[derive(Debug, Default)]
pub(crate) struct Breaker {
    /// Failures since the last success or the last fresh era.
    pub(crate) consecutive_failures: u32,
    /// While set and in the future, the breaker is open. Set but in the past
    /// means the cooldown has elapsed and nothing has been recorded since.
    pub(crate) open_until: Option<Instant>,
}

impl Breaker {
    /// Record one failure. Returns `true` exactly when this failure opened the
    /// breaker, so the caller logs the OPEN transition once.
    pub(crate) fn record_failure(
        &mut self,
        now: Instant,
        threshold: u32,
        cooldown: Duration,
    ) -> bool {
        // Cooldown elapsed → fresh era: the expired breaker's failures don't
        // carry over, so re-opening takes a full threshold of new evidence.
        if self.open_until.is_some_and(|until| now >= until) {
            self.open_until = None;
            self.consecutive_failures = 0;
        }
        self.consecutive_failures = self.consecutive_failures.saturating_add(1);
        if self.consecutive_failures >= threshold && self.open_until.is_none() {
            self.open_until = Some(now + cooldown);
            return true;
        }
        false
    }

    /// Record one success. Returns `true` when the breaker was open (or its
    /// cooldown had elapsed unrecorded), so the caller logs the CLOSED
    /// transition once.
    pub(crate) fn record_success(&mut self) -> bool {
        let was_open = self.open_until.is_some();
        self.consecutive_failures = 0;
        self.open_until = None;
        was_open
    }

    /// No failures and not open — the fast-path check that lets a caller skip
    /// taking a write lock on the healthy path.
    pub(crate) fn is_clean(&self) -> bool {
        self.consecutive_failures == 0 && self.open_until.is_none()
    }

    /// Open and still inside the cooldown. Used by the guard detector; the
    /// transport path reads `open_until` directly to log the time remaining.
    #[cfg_attr(not(feature = "guard"), allow(dead_code))]
    pub(crate) fn is_open(&self, now: Instant) -> bool {
        self.open_until.is_some_and(|until| now < until)
    }

    /// The cooldown has elapsed but no outcome has been recorded since: the
    /// next call is a probe. Used by the guard detector only: the transport
    /// breaker re-admits an endpoint on expiry rather than probing it.
    #[cfg_attr(not(feature = "guard"), allow(dead_code))]
    pub(crate) fn is_half_open(&self, now: Instant) -> bool {
        self.open_until.is_some_and(|until| now >= until)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const COOLDOWN: Duration = Duration::from_secs(30);

    #[test]
    fn opens_at_threshold_once() {
        let mut b = Breaker::default();
        let now = Instant::now();
        assert!(!b.record_failure(now, 3, COOLDOWN));
        assert!(!b.record_failure(now, 3, COOLDOWN));
        assert!(b.record_failure(now, 3, COOLDOWN), "third failure opens");
        assert!(b.is_open(now));
        assert!(
            !b.record_failure(now, 3, COOLDOWN),
            "an already-open breaker does not re-report OPEN"
        );
    }

    #[test]
    fn success_closes_and_reports_only_when_open() {
        let mut b = Breaker::default();
        let now = Instant::now();
        b.record_failure(now, 3, COOLDOWN);
        assert!(!b.record_success(), "closed breaker: no CLOSED transition");
        assert!(b.is_clean());
        for _ in 0..3 {
            b.record_failure(now, 3, COOLDOWN);
        }
        assert!(b.record_success(), "open breaker: CLOSED transition");
        assert!(b.is_clean());
    }

    #[test]
    fn expired_cooldown_is_half_open_and_starts_a_fresh_era() {
        let mut b = Breaker::default();
        let t0 = Instant::now();
        for _ in 0..3 {
            b.record_failure(t0, 3, COOLDOWN);
        }
        let later = t0 + COOLDOWN;
        assert!(!b.is_open(later));
        assert!(b.is_half_open(later));
        // Threshold 3: one post-cooldown failure is the first of a new era.
        assert!(!b.record_failure(later, 3, COOLDOWN));
        assert_eq!(b.consecutive_failures, 1);
        assert!(b.open_until.is_none());
    }

    #[test]
    fn threshold_one_reopens_on_a_failed_probe() {
        let mut b = Breaker::default();
        let t0 = Instant::now();
        for _ in 0..3 {
            b.record_failure(t0, 3, COOLDOWN);
        }
        let later = t0 + COOLDOWN;
        assert!(
            b.record_failure(later, 1, COOLDOWN),
            "a failed probe re-opens immediately"
        );
        assert!(b.is_open(later));
    }
}
