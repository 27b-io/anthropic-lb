use crate::*;

// ── Prometheus text exposition helpers ──────────────────────────────────

/// Format a float for Prometheus: no trailing zeros, NaN and Inf handled.
fn prom_fmt(v: f64) -> String {
    if v.is_nan() {
        return "NaN".to_string();
    }
    if v.is_infinite() {
        return if v > 0.0 { "+Inf" } else { "-Inf" }.to_string();
    }
    if v.fract() == 0.0 && v.abs() < 1e15 {
        format!("{}", v as i64)
    } else {
        format!("{}", v)
    }
}

fn prom_escape(s: &str) -> String {
    s.replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
}

fn prom_gauge(buf: &mut String, name: &str, labels: &[(&str, &str)], value: f64) {
    buf.push_str(name);
    if !labels.is_empty() {
        buf.push('{');
        for (i, (k, v)) in labels.iter().enumerate() {
            if i > 0 {
                buf.push(',');
            }
            buf.push_str(k);
            buf.push_str("=\"");
            buf.push_str(&prom_escape(v));
            buf.push('"');
        }
        buf.push('}');
    }
    buf.push(' ');
    buf.push_str(&prom_fmt(value));
    buf.push('\n');
}

fn prom_counter(buf: &mut String, name: &str, labels: &[(&str, &str)], value: u64) {
    buf.push_str(name);
    if !labels.is_empty() {
        buf.push('{');
        for (i, (k, v)) in labels.iter().enumerate() {
            if i > 0 {
                buf.push(',');
            }
            buf.push_str(k);
            buf.push_str("=\"");
            buf.push_str(&prom_escape(v));
            buf.push('"');
        }
        buf.push('}');
    }
    buf.push(' ');
    buf.push_str(&value.to_string());
    buf.push('\n');
}

fn prom_header(buf: &mut String, name: &str, metric_type: &str, help: &str) {
    use std::fmt::Write;
    let _ = writeln!(buf, "# HELP {name} {help}");
    let _ = writeln!(buf, "# TYPE {name} {metric_type}");
}

/// Emit one histogram's `_bucket` / `_sum` / `_count` series for a single
/// label set. `series` is `(le, cumulative_count)` with `+Inf` last — the
/// shape both `RequestDurationHist::snapshot` and the guard's
/// `DurationHistogram::snapshot` produce. The family's `# HELP` / `# TYPE` header
/// is the caller's job: it must appear once even when the family has many
/// label sets.
fn prom_histogram(
    buf: &mut String,
    family: &str,
    labels: &[(&str, &str)],
    series: &[(String, u64)],
    sum: f64,
    count: u64,
) {
    for (le, cumulative) in series {
        let mut with_le = labels.to_vec();
        with_le.push(("le", le.as_str()));
        prom_counter(buf, &format!("{family}_bucket"), &with_le, *cumulative);
    }
    prom_gauge(buf, &format!("{family}_sum"), labels, sum);
    prom_counter(buf, &format!("{family}_count"), labels, count);
}

/// Upper edges (seconds) of `anthropic_http_request_duration_seconds`, plus an
/// implicit `+Inf`. Spans the proxy's own sub-second rejections through the
/// multi-minute non-streaming generations the upstream client budget allows.
const REQUEST_DURATION_BUCKETS: [f64; 12] = [
    0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0, 600.0,
];

/// One `(route, status)` cell of the request-duration histogram. Bucket counts
/// are stored non-cumulative; `snapshot` cumulates them into the `le`-labelled
/// series Prometheus expects. The total count is the sum of all buckets (the
/// cumulated `+Inf` at emit time), so it is not stored separately.
#[derive(Clone, Copy, Default)]
pub(crate) struct RequestDurationHist {
    buckets: [u64; REQUEST_DURATION_BUCKETS.len() + 1],
    sum_secs: f64,
}

impl RequestDurationHist {
    fn observe(&mut self, elapsed: Duration) {
        let secs = elapsed.as_secs_f64();
        let idx = REQUEST_DURATION_BUCKETS
            .iter()
            .position(|&edge| secs <= edge)
            .unwrap_or(REQUEST_DURATION_BUCKETS.len());
        self.buckets[idx] += 1;
        self.sum_secs += secs;
    }

    /// `(le, cumulative_count)` per bucket (last entry `+Inf`), sum in
    /// seconds, and total count — the shape `prom_histogram` emits.
    fn snapshot(&self) -> (Vec<(String, u64)>, f64, u64) {
        let mut cumulative = 0u64;
        let series = self
            .buckets
            .iter()
            .enumerate()
            .map(|(i, n)| {
                cumulative += n;
                let le = REQUEST_DURATION_BUCKETS
                    .get(i)
                    .map_or_else(|| "+Inf".to_string(), |edge| edge.to_string());
                (le, cumulative)
            })
            .collect();
        (series, self.sum_secs, cumulative)
    }
}

/// Collapse a request path onto the closed `route` label vocabulary. Anything
/// the fallback proxies that is not a known Messages path is `other`, so a
/// caller cannot mint series by varying the URL.
fn route_label(path: &str) -> &'static str {
    match path {
        "/v1/messages" => "/v1/messages",
        "/v1/messages/count_tokens" => "/v1/messages/count_tokens",
        "/v1/chat/completions" => "/v1/chat/completions",
        "/_stats" => "/_stats",
        "/metrics" => "/metrics",
        _ => "other",
    }
}

/// Router-wide middleware: time every request from receipt to the moment the
/// handler yields its response — that is when response headers are ready; for
/// a streamed body none of the stream time is included — and record it under
/// `(route, status)`. One site covers every handler, the fallback included.
/// A request whose client disconnects before the response is ready is not
/// recorded: the server drops this future, so nothing after `next.run` runs.
/// The critical section is two additions, so a poisoned mutex holds nothing
/// inconsistent: `lock_recovering` rather than let the whole family vanish
/// from `/metrics`.
pub(crate) async fn record_request_duration(
    State(state): State<Arc<AppState>>,
    req: Request<Body>,
    next: Next,
) -> Response {
    let route = route_label(req.uri().path());
    let start = Instant::now();
    let resp = next.run(req).await;
    lock_recovering(&state.request_durations, "request_durations")
        .entry((route, resp.status().as_u16()))
        .or_default()
        .observe(start.elapsed());
    resp
}

/// Git revision baked in at build time (`ANTHROPIC_LB_GIT_SHA`, which the
/// Dockerfile sets from its `GIT_SHA` build argument). Truncated to the
/// 7-character form `sha-*` image tags use, so `anthropic_lb_info{revision}`
/// compares byte-for-byte against the deployed tag. `unknown` when unset.
fn build_revision(raw: Option<&'static str>) -> &'static str {
    let sha = raw.unwrap_or("");
    if sha.is_empty() {
        return "unknown";
    }
    sha.get(..7).unwrap_or(sha)
}

#[derive(Default, Clone)]
struct ClaimMetricsSnap {
    key: String,
    utilization: Option<f64>,
    waste_risk: f64,
    reset: Option<u64>,
    status: Option<String>,
}

#[allow(dead_code)]
#[derive(Default, Clone)]
struct EndpointMetricsSnap {
    name: String,
    passthrough: bool,
    has_applicable_7d: bool,
    utilization: Option<f64>,
    utilization_5h: Option<f64>,
    utilization_7d: Option<f64>,
    reset_5h: Option<u64>,
    reset_7d: Option<u64>,
    status_5h: Option<String>,
    status_7d: Option<String>,
    stale_after_hard_limit: bool,
    hard_limited_active: bool,
    burn_rate: (f64, f64, f64),
    headroom: Option<u64>,
    remaining_requests: Option<u64>,
    remaining_tokens: Option<u64>,
    limit_requests: Option<u64>,
    limit_tokens: Option<u64>,
    requests_total: u64,
    hard_limited_secs: f64,
    projected_throttle_secs: Option<f64>,
    token_usage: [u64; 4],
    claims: Vec<ClaimMetricsSnap>,
    last_updated_epoch: Option<u64>,
    overage_in_use: bool,
    overage_utilization: Option<f64>,
    overage_status: Option<String>,
    overage_reset: Option<u64>,
    /// Routing-weight gauges, captured from the source struct's atomics at
    /// snap time. Snap-carried so the routing-weight emission is pool-agnostic.
    routing_weight: f64,
    routing_share: f64,
    effective_gate: f64,
}

#[cfg(test)]
fn append_routing_weight_metrics(
    buf: &mut String,
    endpoints: &[Endpoint],
    snaps: &[EndpointMetricsSnap],
) {
    prom_header(
        buf,
        "anthropic_account_routing_weight",
        "gauge",
        "Per-account routing weight (headroom * waste_risk, or plain headroom when overage is in use or no 7d claim)",
    );
    prom_header(
        buf,
        "anthropic_account_routing_share",
        "gauge",
        "Per-account share of total routing weight (0.0-1.0)",
    );
    prom_header(
        buf,
        "anthropic_account_effective_gate",
        "gauge",
        "Effective routing gate: time_adjusted_overage while overage is in use, else max(time_adjusted_5h, time_adjusted_7d), with status floors",
    );

    for (ep, snap) in endpoints.iter().zip(snaps.iter()) {
        if snap.passthrough {
            continue;
        }
        let weight = f64::from_bits(ep.last_routing_weight.load(Ordering::Relaxed));
        let share = f64::from_bits(ep.last_routing_share.load(Ordering::Relaxed));
        let gate = f64::from_bits(ep.last_effective_gate.load(Ordering::Relaxed));
        prom_gauge(
            buf,
            "anthropic_account_routing_weight",
            &[("account", &snap.name)],
            weight,
        );
        prom_gauge(
            buf,
            "anthropic_account_routing_share",
            &[("account", &snap.name)],
            share,
        );
        prom_gauge(
            buf,
            "anthropic_account_effective_gate",
            &[("account", &snap.name)],
            gate,
        );
    }
}

/// Build an `EndpointMetricsSnap` from an endpoint's (name, rate_info,
/// burn_rate, counters, gauge atomics) fields. Callers pass field references
/// rather than an `&Endpoint`.
#[allow(clippy::too_many_arguments)]
async fn build_metrics_snap(
    name: &str,
    passthrough: bool,
    rate_info: &RwLock<RateLimitInfo>,
    burn_rate: &Mutex<BurnRate>,
    requests: &AtomicU64,
    token_counters: [&AtomicU64; 4],
    routing_weight_atomic: &AtomicU64,
    routing_share_atomic: &AtomicU64,
    effective_gate_atomic: &AtomicU64,
    now_epoch: u64,
    total_headroom: &mut Option<u64>,
) -> EndpointMetricsSnap {
    let info = rate_info.read().await;
    let (br_5m, br_1h, br_6h) = {
        let br = lock_burn_rate(burn_rate);
        (br.rate_5m.value, br.rate_1h.value, br.rate_6h.value)
    };

    let headroom: Option<u64> = if let Some(rem) = info.remaining_requests {
        Some(rem)
    } else if let (Some(util), Some(limit)) = (info.utilization, info.limit_requests) {
        Some(((1.0 - util) * limit as f64).max(0.0) as u64)
    } else {
        None
    };
    match (total_headroom.as_mut(), headroom) {
        (Some(total), Some(h)) => *total += h,
        _ => *total_headroom = None,
    }

    let hard_limited_secs = info
        .hard_limited_until
        .and_then(|until| until.checked_duration_since(Instant::now()))
        .map(|d| d.as_secs() as f64)
        .unwrap_or(0.0);
    let hard_limited_active = info
        .hard_limited_until
        .is_some_and(|until| Instant::now() < until);
    let stale_after_hard_limit = info
        .hard_limited_until
        .is_some_and(|until| info.last_updated.is_none_or(|lu| lu <= until));

    let (eff_util, _, _, _) = effective_utilization(&info, now_epoch, "");
    let projected_throttle_secs = if eff_util < 0.5 || br_1h < 0.01 {
        None
    } else {
        headroom.map(|h| {
            if h == 0 {
                0.0
            } else {
                (h as f64 / br_1h) * 60.0
            }
        })
    };

    let claims: Vec<ClaimMetricsSnap> = info
        .claims_7d
        .iter()
        .map(|(k, d)| ClaimMetricsSnap {
            key: k.clone(),
            utilization: d.utilization,
            waste_risk: waste_risk(d.utilization, d.reset, now_epoch),
            reset: d.reset,
            status: d.status.clone(),
        })
        .collect();

    EndpointMetricsSnap {
        name: name.to_string(),
        passthrough,
        has_applicable_7d: !claims.is_empty()
            || info.utilization_7d.is_some()
            || info.reset_7d.is_some()
            || info.status_7d.is_some(),
        utilization: info.utilization,
        utilization_5h: info.utilization_5h,
        utilization_7d: info.utilization_7d,
        reset_5h: info.reset_5h,
        reset_7d: info.reset_7d,
        status_5h: info.status_5h.clone(),
        status_7d: info.status_7d.clone(),
        stale_after_hard_limit,
        hard_limited_active,
        burn_rate: (br_5m, br_1h, br_6h),
        headroom,
        remaining_requests: info.remaining_requests,
        remaining_tokens: info.remaining_tokens,
        limit_requests: info.limit_requests,
        limit_tokens: info.limit_tokens,
        requests_total: requests.load(Ordering::Relaxed),
        hard_limited_secs,
        projected_throttle_secs,
        token_usage: [
            token_counters[0].load(Ordering::Relaxed),
            token_counters[1].load(Ordering::Relaxed),
            token_counters[2].load(Ordering::Relaxed),
            token_counters[3].load(Ordering::Relaxed),
        ],
        claims,
        last_updated_epoch: info.last_updated_epoch,
        overage_in_use: info.overage_in_use,
        overage_utilization: info.overage_utilization,
        overage_status: info.overage_status.clone(),
        overage_reset: info.overage_reset,
        routing_weight: f64::from_bits(routing_weight_atomic.load(Ordering::Relaxed)),
        routing_share: f64::from_bits(routing_share_atomic.load(Ordering::Relaxed)),
        effective_gate: f64::from_bits(effective_gate_atomic.load(Ordering::Relaxed)),
    }
}

/// Lock `mutex`, recovering — and clearing — a poisoned lock, with a `warn!`
/// naming it (`name`) so a panicked holder leaves evidence instead of being
/// silently healed.
///
/// Every std `Mutex` this crate locks directly goes through here (the only bare
/// `.lock()` left is the async `SAVE_LOCK`, which cannot poison; the debug-log
/// writer's `Mutex` is locked by tracing-subscriber, not by this crate). That
/// is sound because each guarded value is either a counter/accumulator store,
/// a map of independent, self-expiring entries (auth throttle windows, the
/// unsupported-model cache, the session registry, log dedup/rate-limit
/// stamps), a single replaced value (cluster-info cache), or the burn-rate
/// state: three independent EWMAs updated one after another, so a panic
/// mid-update leaves at worst a monitoring value torn by one update.
/// A panicking holder can leave one entry stale by at most one update, never
/// break an invariant spanning entries, so the recovered data is still the
/// best answer. Clearing is the half that matters: a `Mutex` poison is
/// permanent, so a site that skips on `Err` (`if let Ok(..)`, `.lock().ok()`)
/// would skip forever after one panic. For `budget_usage` and the auth
/// throttle that skip is fail-OPEN — `check_budget` would grant every request,
/// `AuthThrottle::check` would report "not throttled", and both would stop
/// counting, for the life of the process. Recovery keeps enforcement live
/// without making the fault itself a denial reason: the gate denies only on
/// recovered data.
///
/// Nothing in the guarded critical sections can currently panic, so this is
/// defence against a future edit, not a live incident.
pub(crate) fn lock_recovering<'a, T>(
    mutex: &'a Mutex<T>,
    name: &'static str,
) -> std::sync::MutexGuard<'a, T> {
    match mutex.lock() {
        Ok(g) => g,
        Err(poisoned) => {
            warn!(lock = name, "mutex was poisoned by a panicking holder; recovered its data and cleared the poison");
            mutex.clear_poison();
            poisoned.into_inner()
        }
    }
}

/// Snapshot a `String`-keyed counter map for the `/metrics` render through
/// `lock_recovering`, rather than `.lock().ok().unwrap_or_default()` — that
/// emits a zero indistinguishable from a real one, and leaves the poison in
/// place for every writer.
fn snapshot_counters(
    counters: &Mutex<HashMap<String, u64>>,
    map: &'static str,
) -> Vec<(String, u64)> {
    lock_recovering(counters, map)
        .iter()
        .map(|(k, v)| (k.clone(), *v))
        .collect()
}

/// Lock an account's burn-rate EWMA via `lock_recovering`. Every
/// `burn_rate` site goes through here, so a panicked holder cannot freeze the
/// tracker or pin its `/_stats` and `/metrics` readings at zero.
pub(crate) fn lock_burn_rate(burn_rate: &Mutex<BurnRate>) -> std::sync::MutexGuard<'_, BurnRate> {
    lock_recovering(burn_rate, "burn_rate")
}

pub(crate) async fn metrics_handler(
    State(state): State<Arc<AppState>>,
    axum::extract::ConnectInfo(client_addr): axum::extract::ConnectInfo<SocketAddr>,
    req: Request<Body>,
) -> Response {
    // AC-8: the ONLY client_addr.ip() read in this handler.
    let client_ip = state.resolve_client_ip(client_addr.ip(), req.headers());
    if !state.is_ip_allowed(&client_ip) {
        return (StatusCode::FORBIDDEN, "forbidden").into_response();
    }
    // AC-4: operator principal required, same gate as /_stats — per-account
    // utilisation and budget gauges are pool reconnaissance.
    if let Some(resp) = state.authorize_admin(&client_ip, client_addr, req.headers(), "metrics") {
        return *resp;
    }

    let now_epoch = AppState::now_epoch();
    let today = now_epoch / 86400;

    // ── Phase 1: Gather data (async — acquires locks) ──────────────

    let mut snaps: Vec<EndpointMetricsSnap> = Vec::with_capacity(state.endpoints.len());
    let mut total_headroom: Option<u64> = Some(0);

    // OpenAI endpoints have a stub RateLimitInfo — their gauges read as
    // zero/None, which is the correct representation for an endpoint with no
    // rate-limit data.
    for ep in &state.endpoints {
        snaps.push(
            build_metrics_snap(
                &ep.name,
                ep.passthrough,
                &ep.rate_info,
                &ep.burn_rate,
                &ep.requests,
                [
                    &ep.input_tokens,
                    &ep.output_tokens,
                    &ep.cache_creation_tokens,
                    &ep.cache_read_tokens,
                ],
                &ep.last_routing_weight,
                &ep.last_routing_share,
                &ep.last_effective_gate,
                now_epoch,
                &mut total_headroom,
            )
            .await,
        );
    }

    // Extract global maps once (single lock per map, then drop guard)
    let client_rates: HashMap<String, (u64, f64)> = state
        .lock_client_request_rates()
        .iter()
        .map(|(k, (total, ewma))| (k.clone(), (*total, ewma.value)))
        .collect();
    let client_usage = state.lock_client_usage().clone();
    let client_model_usage: Vec<((String, String), [u64; 4])> = state
        .lock_client_model_usage()
        .iter()
        .map(|(k, v)| (k.clone(), *v))
        .collect();
    let budget_usage = state.lock_budget_usage().clone();
    let cluster_info = state.lock_cluster_info_cache().clone();
    let prompt_too_long = snapshot_counters(&state.prompt_too_long, "prompt_too_long");
    let fast_mode_429 = snapshot_counters(&state.fast_mode_429, "fast_mode_429");
    let entitlement_400 = snapshot_counters(&state.entitlement_400, "entitlement_400");
    let model_denied: Vec<((String, String), u64)> =
        lock_recovering(&state.model_denied, "model_denied")
            .iter()
            .map(|(k, v)| (k.clone(), *v))
            .collect();
    let beta_body_fields_stripped = snapshot_counters(
        &state.beta_body_fields_stripped,
        "beta_body_fields_stripped",
    );
    let beta_flags_dropped = snapshot_counters(&state.beta_flags_dropped, "beta_flags_dropped");
    let client_rejections: Vec<((String, &'static str), u64)> = state
        .lock_client_rejections()
        .iter()
        .map(|(k, v)| (k.clone(), *v))
        .collect();
    let auth_failures: Vec<(AuthFailureKey, u64)> =
        lock_recovering(&state.auth_failures, "auth_failures")
            .iter()
            .map(|(k, v)| (*k, *v))
            .collect();
    let (session_buckets, session_tokens_sum) = state.session_tokens_histogram(now_epoch);
    let mut request_durations: Vec<((&'static str, u16), RequestDurationHist)> =
        lock_recovering(&state.request_durations, "request_durations")
            .iter()
            .map(|(k, v)| (*k, *v))
            .collect();
    // Stable order for the humans who diff two scrapes.
    request_durations.sort_by_key(|(k, _)| *k);

    // ── Phase 2: Serialize (sync — no locks held) ──────────────────

    let mut buf = String::with_capacity(4096);

    // Meta
    prom_header(
        &mut buf,
        "anthropic_lb_info",
        "gauge",
        "Load balancer info, always 1; version is the crate version, revision the short git commit of the build",
    );
    prom_gauge(
        &mut buf,
        "anthropic_lb_info",
        &[
            ("strategy", state.routing_strategy.as_str()),
            ("version", env!("CARGO_PKG_VERSION")),
            (
                "revision",
                build_revision(option_env!("ANTHROPIC_LB_GIT_SHA")),
            ),
        ],
        1.0,
    );

    prom_header(
        &mut buf,
        "process_start_time_seconds",
        "gauge",
        "Start time of the process since unix epoch in seconds",
    );
    prom_gauge(
        &mut buf,
        "process_start_time_seconds",
        &[],
        state.start_epoch as f64,
    );

    // Request latency by (route, status). Per-process, so unlike the
    // Redis-mirrored series below this one aggregates with sum.
    prom_header(
        &mut buf,
        "anthropic_http_request_duration_seconds",
        "histogram",
        "Seconds from request receipt until response headers are sent (streamed body time excluded; a request the client abandons before headers is not recorded) by route and status; per-process, aggregate with sum",
    );
    for ((route, status), hist) in &request_durations {
        let status = status.to_string();
        let (series, sum, count) = hist.snapshot();
        prom_histogram(
            &mut buf,
            "anthropic_http_request_duration_seconds",
            &[("route", route), ("status", &status)],
            &series,
            sum,
            count,
        );
    }

    // Account utilization
    prom_header(
        &mut buf,
        "anthropic_account_utilization",
        "gauge",
        "Account utilization by time window",
    );
    for s in &snaps {
        if let Some(u) = s.utilization_5h {
            prom_gauge(
                &mut buf,
                "anthropic_account_utilization",
                &[("account", &s.name), ("window", "5h")],
                u,
            );
        }
        if let Some(u) = s.utilization_7d {
            prom_gauge(
                &mut buf,
                "anthropic_account_utilization",
                &[("account", &s.name), ("window", "7d")],
                u,
            );
        }
        // Overage window — emitted only while overage is actively serving, so the
        // metric's presence itself signals an account on paid extra usage.
        if s.overage_in_use {
            if let Some(u) = s.overage_utilization {
                prom_gauge(
                    &mut buf,
                    "anthropic_account_utilization",
                    &[("account", &s.name), ("window", "overage")],
                    u,
                );
            }
        }
    }

    // Rate-limit status (ordinal: 0=allowed, 1=allowed_warning, 2=throttled, 3=rejected)
    prom_header(
        &mut buf,
        "anthropic_account_rate_limit_status",
        "gauge",
        "Rate-limit status ordinal (0=allowed, 1=warning, 2=throttled, 3=rejected)",
    );
    for s in &snaps {
        if s.passthrough {
            continue;
        }
        prom_gauge(
            &mut buf,
            "anthropic_account_rate_limit_status",
            &[("account", &s.name), ("window", "5h")],
            status_to_ordinal(s.status_5h.as_deref()),
        );
        prom_gauge(
            &mut buf,
            "anthropic_account_rate_limit_status",
            &[("account", &s.name), ("window", "7d")],
            status_to_ordinal(s.status_7d.as_deref()),
        );
        // Overage window: same presence rule as its utilization series —
        // emitted only while overage is serving (its fields are cleared
        // otherwise), so the status floor that feeds the overage gate is visible.
        if s.overage_in_use {
            prom_gauge(
                &mut buf,
                "anthropic_account_rate_limit_status",
                &[("account", &s.name), ("window", "overage")],
                status_to_ordinal(s.overage_status.as_deref()),
            );
        }
    }

    // Account reset countdowns
    prom_header(
        &mut buf,
        "anthropic_account_reset_seconds",
        "gauge",
        "Seconds until rate-limit window resets",
    );
    for s in &snaps {
        if let Some(r) = s.reset_5h.filter(|&r| r > now_epoch) {
            prom_gauge(
                &mut buf,
                "anthropic_account_reset_seconds",
                &[("account", &s.name), ("window", "5h")],
                (r - now_epoch) as f64,
            );
        }
        if let Some(r) = s.reset_7d.filter(|&r| r > now_epoch) {
            prom_gauge(
                &mut buf,
                "anthropic_account_reset_seconds",
                &[("account", &s.name), ("window", "7d")],
                (r - now_epoch) as f64,
            );
        }
        if let Some(r) = s
            .overage_reset
            .filter(|&r| s.overage_in_use && r > now_epoch)
        {
            prom_gauge(
                &mut buf,
                "anthropic_account_reset_seconds",
                &[("account", &s.name), ("window", "overage")],
                (r - now_epoch) as f64,
            );
        }
    }

    // Account burn rate
    prom_header(
        &mut buf,
        "anthropic_account_burn_rate",
        "gauge",
        "Account burn rate (requests/min) by time window",
    );
    for s in &snaps {
        prom_gauge(
            &mut buf,
            "anthropic_account_burn_rate",
            &[("account", &s.name), ("window", "5m")],
            s.burn_rate.0,
        );
        prom_gauge(
            &mut buf,
            "anthropic_account_burn_rate",
            &[("account", &s.name), ("window", "1h")],
            s.burn_rate.1,
        );
        prom_gauge(
            &mut buf,
            "anthropic_account_burn_rate",
            &[("account", &s.name), ("window", "6h")],
            s.burn_rate.2,
        );
    }

    // Account headroom
    prom_header(
        &mut buf,
        "anthropic_account_headroom_requests",
        "gauge",
        "Available request headroom",
    );
    for s in &snaps {
        if let Some(h) = s.headroom {
            prom_gauge(
                &mut buf,
                "anthropic_account_headroom_requests",
                &[("account", &s.name)],
                h as f64,
            );
        }
    }

    // Account remaining requests
    prom_header(
        &mut buf,
        "anthropic_account_remaining_requests",
        "gauge",
        "Remaining requests in rate-limit window",
    );
    for s in &snaps {
        if let Some(v) = s.remaining_requests {
            prom_gauge(
                &mut buf,
                "anthropic_account_remaining_requests",
                &[("account", &s.name)],
                v as f64,
            );
        }
    }

    // Account remaining tokens
    prom_header(
        &mut buf,
        "anthropic_account_remaining_tokens",
        "gauge",
        "Remaining tokens in rate-limit window",
    );
    for s in &snaps {
        if let Some(v) = s.remaining_tokens {
            prom_gauge(
                &mut buf,
                "anthropic_account_remaining_tokens",
                &[("account", &s.name)],
                v as f64,
            );
        }
    }

    // Account limits
    prom_header(
        &mut buf,
        "anthropic_account_limit_requests",
        "gauge",
        "Request limit",
    );
    for s in &snaps {
        if let Some(v) = s.limit_requests {
            prom_gauge(
                &mut buf,
                "anthropic_account_limit_requests",
                &[("account", &s.name)],
                v as f64,
            );
        }
    }
    prom_header(
        &mut buf,
        "anthropic_account_limit_tokens",
        "gauge",
        "Token limit",
    );
    for s in &snaps {
        if let Some(v) = s.limit_tokens {
            prom_gauge(
                &mut buf,
                "anthropic_account_limit_tokens",
                &[("account", &s.name)],
                v as f64,
            );
        }
    }

    // Account requests total (counter)
    prom_header(
        &mut buf,
        "anthropic_account_requests_total",
        "counter",
        "Total requests routed to account",
    );
    for s in &snaps {
        prom_counter(
            &mut buf,
            "anthropic_account_requests_total",
            &[("account", &s.name)],
            s.requests_total,
        );
    }

    // Account hard limited
    prom_header(
        &mut buf,
        "anthropic_account_hard_limited_remaining_seconds",
        "gauge",
        "Seconds until hard limit expires, 0 if not limited",
    );
    for s in &snaps {
        prom_gauge(
            &mut buf,
            "anthropic_account_hard_limited_remaining_seconds",
            &[("account", &s.name)],
            s.hard_limited_secs,
        );
    }

    // Account projected throttle — omit when unprojectable (R2.10)
    prom_header(
        &mut buf,
        "anthropic_account_projected_throttle_seconds",
        "gauge",
        "Seconds until projected throttle",
    );
    for s in &snaps {
        if let Some(secs) = s.projected_throttle_secs {
            prom_gauge(
                &mut buf,
                "anthropic_account_projected_throttle_seconds",
                &[("account", &s.name)],
                secs,
            );
        }
    }

    // Account token usage (counter — _total suffix per Prometheus convention)
    prom_header(
        &mut buf,
        "anthropic_account_token_usage_total",
        "counter",
        "Token usage by type",
    );
    for s in &snaps {
        let n = &s.name;
        prom_counter(
            &mut buf,
            "anthropic_account_token_usage_total",
            &[("account", n), ("type", "input")],
            s.token_usage[0],
        );
        prom_counter(
            &mut buf,
            "anthropic_account_token_usage_total",
            &[("account", n), ("type", "output")],
            s.token_usage[1],
        );
        prom_counter(
            &mut buf,
            "anthropic_account_token_usage_total",
            &[("account", n), ("type", "cache_creation")],
            s.token_usage[2],
        );
        prom_counter(
            &mut buf,
            "anthropic_account_token_usage_total",
            &[("account", n), ("type", "cache_read")],
            s.token_usage[3],
        );
    }

    // Account passthrough flag
    prom_header(
        &mut buf,
        "anthropic_account_passthrough",
        "gauge",
        "1 if passthrough account",
    );
    for s in &snaps {
        prom_gauge(
            &mut buf,
            "anthropic_account_passthrough",
            &[("account", &s.name)],
            if s.passthrough { 1.0 } else { 0.0 },
        );
    }

    // Account-level waste risk (max across 7d claims).
    // Note: refresh_metrics_weights uses a 3-tier claim selection
    // (representative → seven_day → max) for routing_weight/share.
    // This metric intentionally shows the max to surface the worst-case
    // claim regardless of which one the router currently selects.
    prom_header(
        &mut buf,
        "anthropic_account_waste_risk",
        "gauge",
        "Max waste risk across 7d claims (worst-case urgency signal)",
    );
    for s in &snaps {
        if s.passthrough || s.claims.is_empty() {
            continue;
        }
        let wr = s.claims.iter().map(|c| c.waste_risk).fold(0.0f64, f64::max);
        prom_gauge(
            &mut buf,
            "anthropic_account_waste_risk",
            &[("account", &s.name)],
            wr,
        );
    }

    // Probe data age (seconds since last rate-limit header update)
    prom_header(
        &mut buf,
        "anthropic_account_data_age_seconds",
        "gauge",
        "Seconds since last rate-limit data update from upstream",
    );
    for s in &snaps {
        if s.passthrough {
            continue;
        }
        if let Some(epoch) = s.last_updated_epoch {
            let age = if now_epoch > epoch {
                (now_epoch - epoch) as f64
            } else {
                0.0
            };
            prom_gauge(
                &mut buf,
                "anthropic_account_data_age_seconds",
                &[("account", &s.name)],
                age,
            );
        }
    }

    // Claim metrics
    prom_header(
        &mut buf,
        "anthropic_claim_utilization",
        "gauge",
        "Per-claim utilization",
    );
    for s in &snaps {
        for claim in &s.claims {
            if let Some(u) = claim.utilization {
                prom_gauge(
                    &mut buf,
                    "anthropic_claim_utilization",
                    &[("account", &s.name), ("claim", &claim.key)],
                    u,
                );
            }
        }
    }
    prom_header(
        &mut buf,
        "anthropic_claim_waste_risk",
        "gauge",
        "Per-claim waste risk score",
    );
    for s in &snaps {
        for claim in &s.claims {
            prom_gauge(
                &mut buf,
                "anthropic_claim_waste_risk",
                &[("account", &s.name), ("claim", &claim.key)],
                claim.waste_risk,
            );
        }
    }

    // Per-claim status and reset (LAB-4189). The account-level siblings are
    // labelled `window="5h"|"7d"` only, so a model carve-out had neither. Status
    // is not recoverable from utilization in either direction — see the
    // per-claim section of the README for the why, and read this rather than
    // thresholding the percentage.
    prom_header(
        &mut buf,
        "anthropic_claim_rate_limit_status",
        "gauge",
        "Per-claim rate-limit status ordinal (0=allowed, 1=warning, 2=throttled, 3=rejected)",
    );
    for s in &snaps {
        for claim in &s.claims {
            prom_gauge(
                &mut buf,
                "anthropic_claim_rate_limit_status",
                &[("account", &s.name), ("claim", &claim.key)],
                status_to_ordinal(claim.status.as_deref()),
            );
        }
    }
    prom_header(
        &mut buf,
        "anthropic_claim_reset_seconds",
        "gauge",
        "Seconds until this claim's window resets",
    );
    for s in &snaps {
        for claim in &s.claims {
            // Past-dated resets are omitted rather than clamped to 0, matching
            // `anthropic_account_reset_seconds`: a stale reset is unknown, not
            // "resets now".
            if let Some(r) = claim.reset.filter(|&r| r > now_epoch) {
                prom_gauge(
                    &mut buf,
                    "anthropic_claim_reset_seconds",
                    &[("account", &s.name), ("claim", &claim.key)],
                    (r - now_epoch) as f64,
                );
            }
        }
    }

    // ── Routing weights (refreshed by refresh_metrics_weights per probe cycle) ─────

    prom_header(
        &mut buf,
        "anthropic_account_routing_weight",
        "gauge",
        "Per-account routing weight (headroom * waste_risk, or plain headroom when overage is in use or no 7d claim)",
    );
    prom_header(
        &mut buf,
        "anthropic_account_routing_share",
        "gauge",
        "Per-account share of total routing weight (0.0-1.0)",
    );
    prom_header(
        &mut buf,
        "anthropic_account_effective_gate",
        "gauge",
        "Effective routing gate: time_adjusted_overage while overage is in use, else max(time_adjusted_5h, time_adjusted_7d), with status floors",
    );

    // Snap-carried gauges (captured at snap time).
    for s in &snaps {
        if s.passthrough {
            continue;
        }
        prom_gauge(
            &mut buf,
            "anthropic_account_routing_weight",
            &[("account", &s.name)],
            s.routing_weight,
        );
        prom_gauge(
            &mut buf,
            "anthropic_account_routing_share",
            &[("account", &s.name)],
            s.routing_share,
        );
        prom_gauge(
            &mut buf,
            "anthropic_account_effective_gate",
            &[("account", &s.name)],
            s.effective_gate,
        );
    }

    // ── Aggregate metrics ──────────────────────────────────────────

    prom_header(
        &mut buf,
        "anthropic_total_headroom_requests",
        "gauge",
        "Sum of all account headroom",
    );
    if let Some(h) = total_headroom {
        prom_gauge(&mut buf, "anthropic_total_headroom_requests", &[], h as f64);
    }

    // Consumer share — aggregate operators into single _operator entry
    prom_header(
        &mut buf,
        "anthropic_consumer_share",
        "gauge",
        "Per-consumer fair share of capacity",
    );
    let total_rpm: f64 = client_rates.values().map(|(_, rpm)| rpm).sum();
    if total_rpm > 0.0 {
        let mut operator_rpm = 0.0f64;
        for (client, (_, rpm)) in &client_rates {
            if state.is_operator(client) {
                operator_rpm += rpm;
            } else {
                prom_gauge(
                    &mut buf,
                    "anthropic_consumer_share",
                    &[("client", client)],
                    rpm / total_rpm,
                );
            }
        }
        if operator_rpm > 0.0 {
            prom_gauge(
                &mut buf,
                "anthropic_consumer_share",
                &[("client", "_operator")],
                operator_rpm / total_rpm,
            );
        }
    }

    // ── Client metrics — aggregate operators into single _operator ──

    // Pre-aggregate operator totals
    let mut op_tokens = [0u64; 4];
    let mut op_requests: u64 = 0;
    let mut op_rpm = 0.0f64;
    let mut all_clients: std::collections::HashSet<&String> = client_usage.keys().collect();
    all_clients.extend(client_rates.keys());
    for client in &all_clients {
        if state.is_operator(client) {
            let t = client_usage.get(*client).copied().unwrap_or([0; 4]);
            for i in 0..4 {
                op_tokens[i] += t[i];
            }
            if let Some((total, rpm)) = client_rates.get(*client) {
                op_requests += total;
                op_rpm += rpm;
            }
        }
    }

    prom_header(
        &mut buf,
        "anthropic_client_token_usage_total",
        "counter",
        "Per-client token usage by type",
    );
    let types = ["input", "output", "cache_creation", "cache_read"];
    let mut emitted_operator_token = false;
    for client in &all_clients {
        if state.is_operator(client) {
            if !emitted_operator_token {
                for (i, t) in types.iter().enumerate() {
                    prom_counter(
                        &mut buf,
                        "anthropic_client_token_usage_total",
                        &[("client", "_operator"), ("type", t)],
                        op_tokens[i],
                    );
                }
                emitted_operator_token = true;
            }
        } else {
            let tokens = client_usage.get(*client).copied().unwrap_or([0; 4]);
            for (i, t) in types.iter().enumerate() {
                prom_counter(
                    &mut buf,
                    "anthropic_client_token_usage_total",
                    &[("client", client), ("type", t)],
                    tokens[i],
                );
            }
        }
    }

    // Per-(client, model) usage (LAB-2330). Sibling of the per-client family
    // above — that one stays authoritative for per-client totals; this one
    // adds the model dimension so per-model pricing can be applied downstream.
    // Operators aggregate into `_operator` per model, matching the house
    // pattern. Cardinality is bounded at record time (MAX_CLIENT_MODEL_LABELS).
    prom_header(
        &mut buf,
        "anthropic_client_model_token_usage_total",
        "counter",
        "Per-client token usage by model and type",
    );
    let mut op_model_tokens: HashMap<&str, [u64; 4]> = HashMap::new();
    for ((client, mdl), tokens) in &client_model_usage {
        if state.is_operator(client) {
            let e = op_model_tokens.entry(mdl.as_str()).or_insert([0; 4]);
            for i in 0..4 {
                e[i] += tokens[i];
            }
        } else {
            for (i, t) in types.iter().enumerate() {
                prom_counter(
                    &mut buf,
                    "anthropic_client_model_token_usage_total",
                    &[("client", client), ("model", mdl), ("type", t)],
                    tokens[i],
                );
            }
        }
    }
    for (mdl, tokens) in &op_model_tokens {
        for (i, t) in types.iter().enumerate() {
            prom_counter(
                &mut buf,
                "anthropic_client_model_token_usage_total",
                &[("client", "_operator"), ("model", mdl), ("type", t)],
                tokens[i],
            );
        }
    }

    prom_header(
        &mut buf,
        "anthropic_client_requests_total",
        "counter",
        "Per-client total requests",
    );
    let mut emitted_operator_req = false;
    for client in &all_clients {
        if state.is_operator(client) {
            if !emitted_operator_req {
                prom_counter(
                    &mut buf,
                    "anthropic_client_requests_total",
                    &[("client", "_operator")],
                    op_requests,
                );
                emitted_operator_req = true;
            }
        } else {
            let total = client_rates.get(*client).map(|(t, _)| *t).unwrap_or(0);
            prom_counter(
                &mut buf,
                "anthropic_client_requests_total",
                &[("client", client)],
                total,
            );
        }
    }

    prom_header(
        &mut buf,
        "anthropic_client_requests_per_minute",
        "gauge",
        "Per-client request rate (EWMA)",
    );
    let mut emitted_operator_rpm = false;
    for client in &all_clients {
        if state.is_operator(client) {
            if !emitted_operator_rpm {
                prom_gauge(
                    &mut buf,
                    "anthropic_client_requests_per_minute",
                    &[("client", "_operator")],
                    op_rpm,
                );
                emitted_operator_rpm = true;
            }
        } else {
            let rpm = client_rates.get(*client).map(|(_, r)| *r).unwrap_or(0.0);
            prom_gauge(
                &mut buf,
                "anthropic_client_requests_per_minute",
                &[("client", client)],
                rpm,
            );
        }
    }

    // Client budgets
    if !state.client_budgets.is_empty() {
        prom_header(
            &mut buf,
            "anthropic_client_budget_limit",
            "gauge",
            "Configured daily token limit",
        );
        for (client, &limit) in &state.client_budgets {
            prom_gauge(
                &mut buf,
                "anthropic_client_budget_limit",
                &[("client", client)],
                limit as f64,
            );
        }
        prom_header(
            &mut buf,
            "anthropic_client_budget_used",
            "gauge",
            "Tokens used today",
        );
        for client in state.client_budgets.keys() {
            let used = budget_usage
                .get(client)
                .filter(|(day, _)| *day == today)
                .map(|(_, used)| *used)
                .unwrap_or(0);
            prom_gauge(
                &mut buf,
                "anthropic_client_budget_used",
                &[("client", client)],
                used as f64,
            );
        }
        prom_header(
            &mut buf,
            "anthropic_client_budget_remaining",
            "gauge",
            "Tokens remaining today",
        );
        for (client, &limit) in &state.client_budgets {
            let used = budget_usage
                .get(client)
                .filter(|(day, _)| *day == today)
                .map(|(_, used)| *used)
                .unwrap_or(0);
            prom_gauge(
                &mut buf,
                "anthropic_client_budget_remaining",
                &[("client", client)],
                limit.saturating_sub(used) as f64,
            );
        }
    }

    // Cluster (Redis)
    if let Some(ref ci) = cluster_info {
        prom_header(
            &mut buf,
            "anthropic_cluster_redis_connected",
            "gauge",
            "Whether Redis is connected",
        );
        let connected = ci
            .get("redis_connected")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        prom_gauge(
            &mut buf,
            "anthropic_cluster_redis_connected",
            &[],
            if connected { 1.0 } else { 0.0 },
        );

        prom_header(
            &mut buf,
            "anthropic_cluster_replicas_seen",
            "gauge",
            "Number of cluster replicas",
        );
        let replicas = ci
            .get("replicas_seen")
            .and_then(|v| v.as_u64())
            .unwrap_or(0);
        prom_gauge(
            &mut buf,
            "anthropic_cluster_replicas_seen",
            &[],
            replicas as f64,
        );

        if let Some(bu) = ci.get("budget_usage").and_then(|v| v.as_object()) {
            prom_header(
                &mut buf,
                "anthropic_cluster_budget_limit",
                "gauge",
                "Cluster-wide budget limit",
            );
            for (client, data) in bu {
                if let Some(limit) = data.get("limit").and_then(|v| v.as_u64()) {
                    prom_gauge(
                        &mut buf,
                        "anthropic_cluster_budget_limit",
                        &[("client", client)],
                        limit as f64,
                    );
                }
            }
            prom_header(
                &mut buf,
                "anthropic_cluster_budget_used",
                "gauge",
                "Cluster-wide budget used",
            );
            for (client, data) in bu {
                if let Some(used) = data.get("used").and_then(|v| v.as_u64()) {
                    prom_gauge(
                        &mut buf,
                        "anthropic_cluster_budget_used",
                        &[("client", client)],
                        used as f64,
                    );
                }
            }
        }
    }

    // Shadow log dropped entries counter (always exported for baseline visibility)
    let dropped = state.shadow_log_dropped.load(Ordering::Relaxed);
    prom_header(
        &mut buf,
        "anthropic_shadow_log_dropped_total",
        "counter",
        "Shadow log entries dropped due to channel backpressure",
    );
    prom_gauge(
        &mut buf,
        "anthropic_shadow_log_dropped_total",
        &[],
        dropped as f64,
    );

    // Upstream transport send-failures by kind (timeout/connect/other) — a flaky
    // egress shows here before it cascades into client-visible errors.
    prom_header(
        &mut buf,
        "anthropic_upstream_transport_errors_total",
        "counter",
        "Upstream transport send-failures by kind. Where anthropic_cluster_redis_connected is 1 this is the Redis fleet-wide total, identical on every replica (aggregate with max). Otherwise it is this process's local count (aggregate with sum): without Redis every failure it has seen; with Redis, before the first sync or while unreachable, the failures it has still to flush, which overlap the fleet total only after a flush whose reply was lost",
    );
    // Prefer the Redis fleet-wide aggregate (cached every 5s by the sync task)
    // so multi-replica deployments report a cluster-wide count; fall back to the
    // local accumulator when Redis is absent or the aggregate is unavailable
    // (single-instance, pre-first-sync, or a Redis blip). With Redis that
    // accumulator holds the deltas awaiting a flush, including any re-queued by
    // a flush whose reply was lost (at-least-once), so the fallback is a
    // per-replica count that can overlap the fleet total: the HELP text names
    // the gauge that tells the two scopes apart.
    let transport_errors: Vec<(String, u64)> = cluster_info
        .as_ref()
        .and_then(|ci| ci.get("transport_errors"))
        .and_then(|v| v.as_object())
        .map(|obj| {
            obj.iter()
                .filter_map(|(k, v)| v.as_u64().map(|n| (k.clone(), n)))
                .collect()
        })
        .unwrap_or_else(|| {
            state
                .lock_transport_errors()
                .iter()
                .map(|(k, v)| (k.to_string(), *v))
                .collect()
        });
    for (kind, n) in transport_errors {
        prom_counter(
            &mut buf,
            "anthropic_upstream_transport_errors_total",
            &[("kind", kind.as_str())],
            n,
        );
    }

    // Upstream context-window overflows (LAB-916). Per-replica, in-memory —
    // sessions themselves stay on `/_stats` (per-session Prometheus labels
    // would be a cardinality anti-pattern).
    prom_header(
        &mut buf,
        "anthropic_prompt_too_long_total",
        "counter",
        "Upstream 'prompt is too long' 400 responses by model",
    );
    for (model, n) in &prompt_too_long {
        prom_counter(
            &mut buf,
            "anthropic_prompt_too_long_total",
            &[("model", model.as_str())],
            *n,
        );
    }

    // Fast-mode 429s by account (LAB-2675). A series appears once an account
    // has served a fast-mode 429 — which the proxy forwards rather than
    // cooling the account for — so any sample here means a client is hitting
    // the separate fast bucket. Sustained growth is the trigger to revisit
    // that policy.
    prom_header(
        &mut buf,
        "anthropic_fast_mode_429_total",
        "counter",
        "Upstream 429s on fast-mode requests, forwarded to the caller without cooling the account",
    );
    for (account, n) in &fast_mode_429 {
        prom_counter(
            &mut buf,
            "anthropic_fast_mode_429_total",
            &[("account", account.as_str())],
            *n,
        );
    }

    // Entitlement 400s by account (LAB-4729): an account's extra usage is gone.
    prom_header(
        &mut buf,
        "anthropic_entitlement_400_total",
        "counter",
        "Upstream 'out of extra usage' 400s by account; the first per request is re-sent to another account",
    );
    for (account, n) in &entitlement_400 {
        prom_counter(
            &mut buf,
            "anthropic_entitlement_400_total",
            &[("account", account.as_str())],
            *n,
        );
    }

    // Per-client model-allowlist denials (LAB-1083). A non-zero rate here is
    // either a misconfigured caller or a caller reaching for capacity it was
    // deliberately denied — both worth an alert.
    prom_header(
        &mut buf,
        "anthropic_client_model_denied_total",
        "counter",
        "Requests rejected (403) by the per-client model allow-list",
    );
    for ((client, model), n) in &model_denied {
        prom_counter(
            &mut buf,
            "anthropic_client_model_denied_total",
            &[("client", client.as_str()), ("model", model.as_str())],
            *n,
        );
    }

    // Pre-request-gate 429 rejections (LAB-2551). Per-replica, in-memory —
    // dashboards must rate() before summing across pods. The header is
    // emitted even at zero series so the family is discoverable before the
    // first rejection.
    prom_header(
        &mut buf,
        "anthropic_client_rejections_total",
        "counter",
        "Requests rejected (429) by the pre-request gate, by client and reason (budget/utilization/brake)",
    );
    for ((client, reason), n) in &client_rejections {
        prom_counter(
            &mut buf,
            "anthropic_client_rejections_total",
            &[("client", client.as_str()), ("reason", reason)],
            *n,
        );
    }

    // Client anthropic-beta flags dropped by the allow-list (LAB-1191).
    // Per-replica, in-memory; bounded via `_other` overflow.
    prom_header(
        &mut buf,
        "anthropic_beta_flag_dropped_total",
        "counter",
        "Client anthropic-beta flags dropped by the allow-list",
    );
    for (flag, n) in &beta_flags_dropped {
        prom_counter(
            &mut buf,
            "anthropic_beta_flag_dropped_total",
            &[("flag", flag.as_str())],
            *n,
        );
    }

    // Body fields stripped to keep a request coherent with the filtered
    // header (LAB-1261). Rate > 0 means an unrecognised PAIRED beta family
    // is in live traffic and the allow-list needs a new entry — the alertable
    // half of the pair; the header-drop counter above is routine noise.
    prom_header(
        &mut buf,
        "anthropic_beta_body_field_stripped_total",
        "counter",
        "Top-level body fields stripped after their anthropic-beta header was dropped",
    );
    for (field, n) in &beta_body_fields_stripped {
        prom_counter(
            &mut buf,
            "anthropic_beta_body_field_stripped_total",
            &[("field", field.as_str())],
            *n,
        );
    }

    // Failed authentication attempts (LAB-1192). A non-zero rate on a public
    // ingress is credential scanning — alert on it.
    prom_header(
        &mut buf,
        "anthropic_auth_failures_total",
        "counter",
        "Requests rejected for auth — invalid/missing credential OR throttle 429 — by route \
         and presented-credential shape (cred: none / x-api-key / bearer / auth-other)",
    );
    for ((route, cred), n) in &auth_failures {
        prom_counter(
            &mut buf,
            "anthropic_auth_failures_total",
            &[("route", route), ("cred", cred)],
            *n,
        );
    }

    // Live session-size distribution (LAB-957). A snapshot of the session
    // registry, not an observation stream: values rise AND fall as sessions
    // grow and expire, so this is declared `gauge` — chart instant values,
    // never rate(). Cumulative `le` buckets keep Grafana heatmaps and
    // histogram_quantile() working, and sum across replicas.
    prom_header(
        &mut buf,
        "anthropic_session_tokens_bucket",
        "gauge",
        "Live sessions with last-prompt tokens <= le (cumulative snapshot; instant values, do not rate())",
    );
    for (i, le) in SESSION_TOKENS_BUCKETS.iter().enumerate() {
        prom_gauge(
            &mut buf,
            "anthropic_session_tokens_bucket",
            &[("le", &le.to_string())],
            session_buckets[i] as f64,
        );
    }
    prom_gauge(
        &mut buf,
        "anthropic_session_tokens_bucket",
        &[("le", "+Inf")],
        session_buckets[SESSION_TOKENS_BUCKETS.len()] as f64,
    );
    prom_header(
        &mut buf,
        "anthropic_session_tokens_sum",
        "gauge",
        "Sum of last-prompt tokens across live sessions",
    );
    prom_gauge(
        &mut buf,
        "anthropic_session_tokens_sum",
        &[],
        session_tokens_sum as f64,
    );
    prom_header(
        &mut buf,
        "anthropic_session_tokens_count",
        "gauge",
        "Number of live sessions",
    );
    prom_gauge(
        &mut buf,
        "anthropic_session_tokens_count",
        &[],
        session_buckets[SESSION_TOKENS_BUCKETS.len()] as f64,
    );

    // In-flight request-body memory admission (P1-01): current reserved bytes,
    // the configured ceiling, and the load-shed counter — together these let an
    // operator size `max_inflight_body_mb` from observed peak rather than guess.
    prom_header(
        &mut buf,
        "anthropic_inflight_body_bytes",
        "gauge",
        "Current sum of reserved in-flight request-body bytes",
    );
    prom_gauge(
        &mut buf,
        "anthropic_inflight_body_bytes",
        &[],
        state.inflight_body_bytes.load(Ordering::Relaxed) as f64,
    );
    prom_header(
        &mut buf,
        "anthropic_inflight_body_limit_bytes",
        "gauge",
        "Configured in-flight request-body memory budget in bytes (0 = disabled)",
    );
    prom_gauge(
        &mut buf,
        "anthropic_inflight_body_limit_bytes",
        &[],
        state.max_inflight_body_bytes as f64,
    );
    prom_header(
        &mut buf,
        "anthropic_body_shed_total",
        "counter",
        "Requests load-shed because the in-flight body-memory budget was exhausted",
    );
    prom_counter(
        &mut buf,
        "anthropic_body_shed_total",
        &[],
        state.body_shed_total.load(Ordering::Relaxed),
    );
    prom_header(
        &mut buf,
        "anthropic_body_read_timeout_total",
        "counter",
        "Requests shed with 408 because the body was not received within body_read_timeout_secs",
    );
    prom_counter(
        &mut buf,
        "anthropic_body_read_timeout_total",
        &[],
        state.body_read_timeout_total.load(Ordering::Relaxed),
    );

    // Affinity overrides that migrated a session (GH#156), by the window that
    // bound the sticky account — see the WARN in pick_sticky_weighted_v2.
    prom_header(
        &mut buf,
        "anthropic_affinity_migrations_total",
        "counter",
        "Affinity overrides that migrated a session, by the window that bound the sticky account",
    );
    for bind in AffinityBind::ALL {
        prom_counter(
            &mut buf,
            "anthropic_affinity_migrations_total",
            &[("reason", bind.as_str())],
            state.affinity_migrations[bind as usize].load(Ordering::Relaxed),
        );
    }

    // Client-facing pool exhaustion (LAB-4189): the 429/503 the caller actually
    // received because every endpoint was gated or failed. The per-account
    // gauges describe the pool's state but never say whether a request was
    // turned away because of it. Independent per-replica events, not mirrors of
    // one upstream value — aggregate with `sum by (kind)`.
    prom_header(
        &mut buf,
        "anthropic_pool_exhausted_total",
        "counter",
        "Client-facing pool-exhaustion responses by kind (rate_limited=429, transient=503)",
    );
    for kind in PoolExhaustion::ALL {
        prom_counter(
            &mut buf,
            "anthropic_pool_exhausted_total",
            &[("kind", kind.as_str())],
            state.pool_exhausted[kind as usize].load(Ordering::Relaxed),
        );
    }

    // LAB-933/LAB-929 response cache counters (AC12 / LAB-929 AC4). Emitted
    // only when the cache is configured; `messages` and `count_tokens` are
    // separate series on the same metric names, distinguished by the
    // `surface` label. Looped metric-major (header, then both surfaces'
    // samples, then the next metric) so each family's samples stay
    // contiguous — the exposition format requires all samples of one metric
    // grouped together, and OpenMetrics-strict scrapers reject interleaving.
    if let Some(rc) = &state.response_cache {
        type SurfaceCounter = fn(&ResponseCache, CacheSurface) -> &AtomicU64;
        let series: [(&str, &str, SurfaceCounter); 4] = [
            (
                "anthropic_response_cache_hits_total",
                "Requests served from the response cache (no upstream call, no headroom burned)",
                ResponseCache::hits_for,
            ),
            (
                "anthropic_response_cache_misses_total",
                "Opted-in cacheable requests that proceeded upstream on a cache miss",
                ResponseCache::misses_for,
            ),
            (
                "anthropic_response_cache_stores_total",
                "2xx responses written to the response cache",
                ResponseCache::stores_for,
            ),
            (
                "anthropic_response_cache_errors_total",
                "Response cache operations that failed or timed out (request failed open)",
                ResponseCache::errors_for,
            ),
        ];
        for (name, help, counter_for) in series {
            prom_header(&mut buf, name, "counter", help);
            for surface in [CacheSurface::Messages, CacheSurface::CountTokens] {
                prom_counter(
                    &mut buf,
                    name,
                    &[("surface", surface.label())],
                    counter_for(rc, surface).load(Ordering::Relaxed),
                );
            }
        }
    }

    // LAB-3877: Tier 0 guard metrics. Verdicts by (client, scanner, outcome)
    // and the scan-duration histogram. Both per-replica, in-memory.
    #[cfg(feature = "guard")]
    {
        prom_header(
            &mut buf,
            "anthropic_guard_verdicts_total",
            "counter",
            "Guard scan verdicts by client, scanner, and outcome (allow/annotate/block)",
        );
        // The detector's verdicts share this family under its own `scanner`
        // label, and must be emitted in the same group: the text format does
        // not allow a family's samples to be split by another family's.
        let detector_verdicts = state
            .guard
            .detector()
            .map(|d| d.verdicts_snapshot())
            .unwrap_or_default();
        for ((client, scanner, verdict), n) in state
            .guard
            .verdicts_snapshot()
            .into_iter()
            .chain(detector_verdicts)
        {
            prom_counter(
                &mut buf,
                "anthropic_guard_verdicts_total",
                &[
                    ("client", client.as_str()),
                    ("scanner", scanner),
                    ("verdict", verdict),
                ],
                n,
            );
        }

        let (hist, sum, count) = state.guard.scan_hist_snapshot();
        prom_header(
            &mut buf,
            "anthropic_guard_scan_duration_seconds",
            "histogram",
            "Guard request-body scan duration in seconds",
        );
        prom_histogram(
            &mut buf,
            "anthropic_guard_scan_duration_seconds",
            &[],
            &hist,
            sum,
            count,
        );

        // LAB-3878: Tier 1 detector.
        if let Some(d) = state.guard.detector() {
            let detector = [("detector", d.name())];
            prom_header(
                &mut buf,
                "anthropic_guard_detector_errors_total",
                "counter",
                "Detector requests that got no verdict, by cause (timeout/connect/transport/status/decode)",
            );
            for (kind, n) in d.errors_snapshot() {
                prom_counter(
                    &mut buf,
                    "anthropic_guard_detector_errors_total",
                    &[("detector", d.name()), ("kind", kind)],
                    n,
                );
            }
            prom_header(
                &mut buf,
                "anthropic_guard_detector_circuit_open",
                "gauge",
                "1 while a lane's detector circuit breaker is open or awaiting its recovery probe",
            );
            for lane in guard::detector::Lane::ALL {
                prom_gauge(
                    &mut buf,
                    "anthropic_guard_detector_circuit_open",
                    &[("detector", d.name()), ("lane", lane.label())],
                    if d.tripped(lane) { 1.0 } else { 0.0 },
                );
            }
            prom_header(
                &mut buf,
                "anthropic_guard_detector_short_circuited_total",
                "counter",
                "Requests given no verdict without a detector call, by lane and reason (circuit_open/saturated)",
            );
            for lane in guard::detector::Lane::ALL {
                let (open, saturated) = d.turned_away(lane);
                for (reason, n) in [("circuit_open", open), ("saturated", saturated)] {
                    prom_counter(
                        &mut buf,
                        "anthropic_guard_detector_short_circuited_total",
                        &[
                            ("detector", d.name()),
                            ("lane", lane.label()),
                            ("reason", reason),
                        ],
                        n,
                    );
                }
            }
            let (hits, misses) = d.cache_snapshot();
            prom_header(
                &mut buf,
                "anthropic_guard_detector_cache_lookups_total",
                "counter",
                "Detector verdict-cache lookups per chunk, by result (hit/miss)",
            );
            for (result, n) in [("hit", hits), ("miss", misses)] {
                prom_counter(
                    &mut buf,
                    "anthropic_guard_detector_cache_lookups_total",
                    &[("detector", d.name()), ("result", result)],
                    n,
                );
            }
            let (hist, sum, count) = d.duration_snapshot();
            prom_header(
                &mut buf,
                "anthropic_guard_detector_duration_seconds",
                "histogram",
                "Wall-clock time of detector calls per request, all chunks included",
            );
            prom_histogram(
                &mut buf,
                "anthropic_guard_detector_duration_seconds",
                &detector,
                &hist,
                sum,
                count,
            );
        }
    }

    (
        StatusCode::OK,
        [(
            axum::http::header::CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8",
        )],
        buf,
    )
        .into_response()
}

#[cfg(test)]
mod tests;
