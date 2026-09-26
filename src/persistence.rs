use crate::*;

// ── Persistence ─────────────────────────────────────────────────────

#[derive(Serialize, Deserialize)]
struct PersistedState {
    endpoints: Vec<PersistedEndpoint>,
    #[serde(default)]
    saved_at: u64,
}

#[derive(Serialize, Deserialize)]
struct PersistedEndpoint {
    name: String,
    requests_total: u64,
    utilization: Option<f64>,
    #[serde(default)]
    utilization_7d: Option<f64>,
    #[serde(default)]
    utilization_5h: Option<f64>,
    representative_claim: Option<String>,
    #[serde(default)]
    reset_5h: Option<u64>,
    #[serde(default)]
    reset_7d: Option<u64>,
    #[serde(default)]
    status_5h: Option<String>,
    #[serde(default)]
    status_7d: Option<String>,
    #[serde(default)]
    claims_7d: HashMap<String, ClaimWindowData>,
    remaining_requests: Option<u64>,
    remaining_tokens: Option<u64>,
    limit_requests: Option<u64>,
    limit_tokens: Option<u64>,
    #[serde(default)]
    overage_in_use: bool,
    #[serde(default)]
    overage_status: Option<String>,
    #[serde(default)]
    overage_utilization: Option<f64>,
    #[serde(default)]
    overage_reset: Option<u64>,
    /// Absolute unix timestamp (secs) when hard limit expires
    hard_limited_until_epoch: Option<u64>,
    /// Wall-clock epoch when this account's rate info was last updated.
    /// Used by sync_from_redis "most recent wins" merge after restart.
    #[serde(default)]
    last_updated_epoch: Option<u64>,
    /// Burst-429 backoff stage (consecutive no-Retry-After 429s). Persisted so a
    /// restart mid-escalation doesn't reset exponential backoff to stage 0 (B3-07).
    #[serde(default)]
    consecutive_burst_429s: u32,
}

/// Process-global monotonic nonce for unique state-file temp names, so
/// concurrent save_state calls never share a temp path (which could interleave
/// into a torn file before the atomic rename promotes it).
static STATE_SAVE_NONCE: AtomicU64 = AtomicU64::new(0);

impl AppState {
    pub(crate) fn now_epoch() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs()
    }

    /// Convert epoch seconds to ISO 8601 UTC string (no chrono dependency).
    pub(crate) fn epoch_to_iso8601(epoch: u64) -> String {
        // Days from epoch, accounting for leap years
        let secs_per_day: u64 = 86400;
        let mut remaining = epoch;
        let mut year: u64 = 1970;
        loop {
            let days_in_year = if (year.is_multiple_of(4) && !year.is_multiple_of(100))
                || year.is_multiple_of(400)
            {
                366
            } else {
                365
            };
            let secs_in_year = days_in_year * secs_per_day;
            if remaining < secs_in_year {
                break;
            }
            remaining -= secs_in_year;
            year += 1;
        }
        let is_leap =
            (year.is_multiple_of(4) && !year.is_multiple_of(100)) || year.is_multiple_of(400);
        let days_in_months: [u64; 12] = [
            31,
            if is_leap { 29 } else { 28 },
            31,
            30,
            31,
            30,
            31,
            31,
            30,
            31,
            30,
            31,
        ];
        let mut day_of_year = remaining / secs_per_day;
        remaining %= secs_per_day;
        let mut month: u64 = 1;
        for &dim in &days_in_months {
            if day_of_year < dim {
                break;
            }
            day_of_year -= dim;
            month += 1;
        }
        let day = day_of_year + 1;
        let hour = remaining / 3600;
        remaining %= 3600;
        let minute = remaining / 60;
        let second = remaining % 60;
        format!("{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}Z")
    }

    pub(crate) async fn save_state(&self) {
        // Serialize save executions (not just temp filenames): hold this lock
        // across snapshot + write + rename so the last writer observes the
        // freshest in-memory state and its rename lands last. Without it, two
        // overlapping saves could let an older snapshot's rename finish last and
        // roll back fresher persisted state (e.g. a hard-limit just recorded).
        // Saves are infrequent (probe / hard-limit / recovery / shutdown), so
        // contention is negligible.
        static SAVE_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
        let _save_guard = SAVE_LOCK.lock().await;

        let mut endpoints = Vec::new();
        let now = Instant::now();

        // Build a PersistedEndpoint from an endpoint's (name, requests,
        // rate_info) fields.
        async fn persist_one(
            name: &str,
            requests: &AtomicU64,
            rate_info: &RwLock<RateLimitInfo>,
            now: Instant,
        ) -> PersistedEndpoint {
            let info = rate_info.read().await;
            let hard_until_epoch = info.hard_limited_until.and_then(|until| {
                if until > now {
                    let remaining = until.duration_since(now);
                    Some(AppState::now_epoch() + remaining.as_secs())
                } else {
                    None
                }
            });
            PersistedEndpoint {
                name: name.to_string(),
                requests_total: requests.load(Ordering::Relaxed),
                utilization: info.utilization,
                utilization_7d: info.utilization_7d,
                utilization_5h: info.utilization_5h,
                representative_claim: info.representative_claim.clone(),
                reset_5h: info.reset_5h,
                reset_7d: info.reset_7d,
                status_5h: info.status_5h.clone(),
                status_7d: info.status_7d.clone(),
                claims_7d: info.claims_7d.clone(),
                remaining_requests: info.remaining_requests,
                remaining_tokens: info.remaining_tokens,
                limit_requests: info.limit_requests,
                limit_tokens: info.limit_tokens,
                overage_in_use: info.overage_in_use,
                overage_status: info.overage_status.clone(),
                overage_utilization: info.overage_utilization,
                overage_reset: info.overage_reset,
                hard_limited_until_epoch: hard_until_epoch,
                last_updated_epoch: info.last_updated_epoch,
                consecutive_burst_429s: info.consecutive_burst_429s,
            }
        }

        // Skip OpenAI endpoints — their rate_info is a permanent stub with no
        // state worth persisting.
        for ep in &self.endpoints {
            if ep.protocol == Protocol::OpenAI {
                continue;
            }
            endpoints.push(persist_one(&ep.name, &ep.requests, &ep.rate_info, now).await);
        }

        let state = PersistedState {
            endpoints,
            saved_at: Self::now_epoch(),
        };

        // Atomic write: serialize compactly to a unique temp sibling, then rename
        // it into place. Rename is atomic on the same filesystem, so a crash
        // mid-write leaves the previous complete file intact (not a truncated
        // one) and concurrent writers can't interleave into a torn file. Compact
        // `to_vec` (not `to_string_pretty`) roughly halves the bytes per save.
        match serde_json::to_vec(&state) {
            Ok(json) => {
                let nonce = STATE_SAVE_NONCE.fetch_add(1, Ordering::Relaxed);
                let mut tmp_os = self.state_path.clone().into_os_string();
                tmp_os.push(format!(".{nonce}.tmp"));
                let tmp_path = PathBuf::from(tmp_os);
                if let Err(e) = tokio::fs::write(&tmp_path, &json).await {
                    error!(path = %tmp_path.display(), error = %e, "failed to write temp state");
                    // A failed write may still have created a partial temp file
                    // (open truncates before write_all). Best-effort cleanup, same
                    // as the rename branch, so failed saves don't leak temp files.
                    let _ = tokio::fs::remove_file(&tmp_path).await;
                    return;
                }
                if let Err(e) = tokio::fs::rename(&tmp_path, &self.state_path).await {
                    error!(path = %self.state_path.display(), error = %e, "failed to rename state into place");
                    let _ = tokio::fs::remove_file(&tmp_path).await;
                } else {
                    trace!(path = %self.state_path.display(), "state saved");
                }
            }
            Err(e) => error!(error = %e, "failed to serialize state"),
        }
    }

    /// True if `model`'s 7d claim was refreshed recently enough to skip a probe.
    ///
    /// Pure freshness check used by `probe_endpoint`.
    /// Looks up only the model-specific claim key (e.g. `seven_day_opus`), NOT the
    /// general `seven_day` fallback, so probing one family doesn't suppress another.
    /// An empty `model_family` (unrecognized model) never counts as fresh.
    /// "Recent" means the claim's age is under half the probe interval.
    fn claim_recently_probed(
        info: &RateLimitInfo,
        model: &str,
        probe_interval_secs: u64,
        now_epoch: u64,
    ) -> bool {
        let family = model_family(model);
        if family.is_empty() {
            return false;
        }
        let claim_key = format!("seven_day_{}", family);
        match info.claims_7d.get(&claim_key) {
            Some(claim) => {
                let age = now_epoch.saturating_sub(claim.last_seen);
                age < probe_interval_secs / 2
            }
            None => false,
        }
    }

    /// Fire a minimal request (max_tokens=1) to refresh rate-limit headers for
    /// the endpoint at `idx`. The `model` parameter controls which model is
    /// probed, rotating across families so that per-model 7d utilization claims
    /// get populated for each family. Skips `Protocol::OpenAI` endpoints — they
    /// expose no Anthropic rate-limit headers, so a probe would only burn a
    /// request. One of the three named `match protocol` sites.
    pub(crate) async fn probe_endpoint(&self, idx: usize, model: &str) {
        let ep = &self.endpoints[idx];
        if ep.protocol == Protocol::OpenAI {
            debug!(endpoint = ep.name, "skipping probe for openai endpoint");
            return;
        }
        if ep.passthrough {
            debug!(
                endpoint = ep.name,
                "skipping probe for passthrough endpoint"
            );
            return;
        }

        // Check if hard-limited — don't waste a request
        {
            let info = ep.rate_info.read().await;
            if let Some(until) = info.hard_limited_until {
                if Instant::now() < until {
                    debug!(
                        endpoint = ep.name,
                        "skipping probe, endpoint is hard-limited"
                    );
                    return;
                }
            }
        }

        // Local freshness check: skip if this model's 7d claim was recently refreshed.
        let now_epoch = Self::now_epoch();
        {
            let info = ep.rate_info.read().await;
            if Self::claim_recently_probed(&info, model, self.probe_interval_secs, now_epoch) {
                trace!(
                    endpoint = ep.name,
                    probe_model = model,
                    "probe skipped, model claim is fresh"
                );
                return;
            }
        }

        // Distributed probe lock: one pod per endpoint+model per interval.
        if let Some(redis) = self.coordination_redis() {
            let lock_key = format!("alb:probe:{}:{}", ep.name, model);
            let lock_ttl = self.probe_interval_secs.max(1);
            // SET NX EX: OK reply when acquired, nil (None) when another
            // replica already holds the lock.
            let acquired: Result<Option<String>, fred::error::RedisError> = redis
                .set(
                    lock_key.as_str(),
                    1,
                    Some(Expiration::EX(lock_ttl as i64)),
                    Some(SetOptions::NX),
                    false,
                )
                .await;
            match acquired {
                Ok(Some(_)) => {} // Lock acquired, proceed with probe
                Ok(None) => {
                    trace!(
                        endpoint = ep.name,
                        probe_model = model,
                        "probe skipped, another replica is probing"
                    );
                    return;
                }
                Err(e) => {
                    // Fail-open: if Redis is down, probe anyway
                    trace!(endpoint = ep.name, error = %e, "probe lock failed, probing anyway");
                }
            }
        }

        // Each endpoint carries its own base URL.
        let url = format!("{}/v1/messages", ep.base_url);
        let body = serde_json::json!({
            "model": model,
            "max_tokens": 1,
            "system": [{"type": "text", "text": "You are Claude Code, Anthropic's official CLI for Claude."}],
            "messages": [{"role": "user", "content": "."}]
        });

        // Build headers in a HeaderMap so auth injection can reuse the shared
        // `inject_account_auth` (token-prefix dispatch + OAuth beta-flag merge)
        // instead of a hand-rolled copy.
        let mut headers = axum::http::HeaderMap::new();
        headers.insert("content-type", HeaderValue::from_static("application/json"));
        headers.insert("anthropic-version", HeaderValue::from_static("2023-06-01"));
        headers.insert(
            "anthropic-beta",
            HeaderValue::from_str(&OAUTH_BETA_FLAGS.join(",")).unwrap(),
        );
        headers.insert(
            "user-agent",
            HeaderValue::from_static("claude-cli/2.1.2 (external, cli)"),
        );
        headers.insert("x-app", HeaderValue::from_static("cli"));
        headers.insert(
            "anthropic-dangerous-direct-browser-access",
            HeaderValue::from_static("true"),
        );
        // Probe headers carry only OAUTH_BETA_FLAGS, which the filter never
        // drops (they are unconditionally re-added), so the returned drop
        // list is provably empty — nothing to record.
        let _ = inject_account_auth(
            &mut headers,
            &ep.token,
            ep.passthrough,
            &self.allowed_client_betas,
        );

        let req = self.client.post(&url).headers(headers).json(&body);

        match req.send().await {
            Ok(resp) => {
                let status = resp.status();
                self.update_rate_info_for(
                    &ep.rate_info,
                    &ep.name,
                    resp.headers(),
                    // Probes never request fast mode (PROBE_MODELS, fixed body).
                    /* is_fast_mode */
                    false,
                )
                .await;
                if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
                    self.mark_hard_limited_for(&ep.rate_info, &ep.name, resp.headers())
                        .await;
                } else if status.is_success() {
                    // 2xx only: endpoint is responsive — clear hard limit and
                    // burst counter. 5xx/529 are upstream errors, not recovery.
                    let recovered = {
                        let mut info = ep.rate_info.write().await;
                        let was_hard_limited = info.hard_limited_until.is_some();
                        if was_hard_limited {
                            info.hard_limited_until = None;
                            debug!(
                                endpoint = ep.name,
                                "cleared hard limit after successful probe"
                            );
                        }
                        info.consecutive_burst_429s = 0;
                        was_hard_limited
                    };
                    if recovered {
                        self.signal_hard_limit_recovery(&ep.name).await;
                    }
                }
                // else: 5xx/529 — leave endpoint state untouched.
                self.save_state().await;
                let info = ep.rate_info.read().await;
                let now_epoch = Self::now_epoch();
                let (eff_util, constraint, _adj_5h, _adj_7d) =
                    effective_utilization(&info, now_epoch, model);
                // Only compute routing weight on 2xx — non-success responses leave
                // the endpoint state either mutated (429 → hard-limited) or untouched
                // (5xx), and the pre-response weight no longer reflects reality.
                let rw = if status.is_success() {
                    compute_routing_weight(&info, model, now_epoch, false)
                } else {
                    None
                };
                // DEBUG, not INFO: per-probe completion is high-volume periodic
                // noise (every probe_interval × model-family × endpoint). The
                // routing-weight metrics this computes are still exported via
                // Prometheus; this line is only the human-readable echo.
                debug!(
                    endpoint = ep.name,
                    status = status.as_u16(),
                    probe_model = model,
                    utilization = format_args!("{eff_util:.2}"),
                    util_5h = info
                        .utilization_5h
                        .map(|v| format!("{v:.2}"))
                        .as_deref()
                        .unwrap_or("-"),
                    util_7d = info
                        .utilization_7d
                        .map(|v| format!("{v:.2}"))
                        .as_deref()
                        .unwrap_or("-"),
                    constraint,
                    n_claims_7d = info.claims_7d.len(),
                    gate_5h = rw
                        .as_ref()
                        .map(|r| format!("{:.4}", r.gate_5h))
                        .as_deref()
                        .unwrap_or("-"),
                    gate_7d = rw
                        .as_ref()
                        .map(|r| format!("{:.4}", r.gate_7d))
                        .as_deref()
                        .unwrap_or("-"),
                    waste_risk = rw
                        .as_ref()
                        .map(|r| format!("{:.4}", r.wr))
                        .as_deref()
                        .unwrap_or("-"),
                    weight = rw
                        .as_ref()
                        .map(|r| format!("{:.4}", r.weight))
                        .as_deref()
                        .unwrap_or("-"),
                    weight_source = rw.as_ref().map(|r| r.source).unwrap_or("-"),
                    "probe complete"
                );
            }
            Err(e) => {
                warn!(endpoint = ep.name, error = %e, detail = %describe_reqwest_error(&e), "probe failed");
            }
        }
    }

    pub(crate) async fn load_state(&self) {
        let data = match tokio::fs::read_to_string(&self.state_path).await {
            Ok(d) => d,
            Err(_) => {
                info!(path = %self.state_path.display(), "no persisted state found, starting fresh");
                return;
            }
        };

        let persisted: PersistedState = match serde_json::from_str(&data) {
            Ok(s) => s,
            Err(e) => {
                warn!(error = %e, "failed to parse persisted state (possible legacy 'accounts'-keyed format — that schema was removed); starting fresh");
                return;
            }
        };

        let now_epoch = Self::now_epoch();
        let now_instant = Instant::now();

        for pa in &persisted.endpoints {
            // Match each persisted entry to the endpoint with the same name.
            let restore_target: Option<(&AtomicU64, &RwLock<RateLimitInfo>)> = self
                .endpoints
                .iter()
                .find(|e| e.name == pa.name)
                .map(|e| (&e.requests, &e.rate_info));
            if let Some((requests, rate_info)) = restore_target {
                requests.store(pa.requests_total, Ordering::Relaxed);
                let mut info = rate_info.write().await;
                info.utilization = pa.utilization;
                info.utilization_7d = pa.utilization_7d;
                info.utilization_5h = pa.utilization_5h;
                // Truncated alongside the claim map below: `metrics_gate_weight`
                // looks this key up in `claims_7d`, whose keys
                // `bound_ingested_claims` truncates, so an untruncated copy
                // would miss its own entry (same reason as the header path).
                info.representative_claim = pa.representative_claim.as_deref().map(truncate_label);
                info.reset_5h = pa.reset_5h;
                info.status_5h = pa.status_5h.clone();
                info.overage_in_use = pa.overage_in_use;
                info.overage_status = pa.overage_status.clone();
                info.overage_utilization = pa.overage_utilization;
                info.overage_reset = pa.overage_reset;
                info.consecutive_burst_429s = pa.consecutive_burst_429s;

                // Load claims_7d: either from persisted map or migrate from flat fields
                if !pa.claims_7d.is_empty() {
                    info.claims_7d = bound_ingested_claims(pa.claims_7d.clone());
                } else if let Some(util_7d) = pa.utilization_7d {
                    // Migration: old state file with flat 7d fields only
                    let key = truncate_label(
                        pa.representative_claim
                            .as_deref()
                            .filter(|c| c.starts_with("seven_day"))
                            .unwrap_or("seven_day"),
                    );
                    info.claims_7d.insert(
                        key,
                        ClaimWindowData {
                            utilization: Some(util_7d),
                            reset: pa.reset_7d,
                            status: pa.status_7d.clone(),
                            ..Default::default()
                        },
                    );
                }

                // Evict stale claims (reset in the past)
                info.claims_7d
                    .retain(|_, c| c.reset.is_some_and(|r| r > now_epoch));

                // Derive flat 7d fields from claims_7d — a persisted carve-out
                // claim (Fable band) must not resurrect into the emergency
                // brake's input on boot; see derive_flat_7d_fields.
                (info.utilization_7d, info.reset_7d, info.status_7d) =
                    derive_flat_7d_fields(&info.claims_7d);

                // Invalidate stale 5h data
                if info.reset_5h.is_none_or(|r| r <= now_epoch) {
                    info.utilization_5h = None;
                    info.reset_5h = None;
                    info.status_5h = None;
                }

                // Recompute unified utilization from surviving windows
                let mut max_util: Option<f64> = info.utilization_5h;
                if let Some(u7) = info.utilization_7d {
                    max_util = Some(max_util.map_or(u7, |cur| cur.max(u7)));
                }
                info.utilization = max_util;

                info.remaining_requests = pa.remaining_requests;
                info.remaining_tokens = pa.remaining_tokens;
                info.limit_requests = pa.limit_requests;
                info.limit_tokens = pa.limit_tokens;

                if let Some(until_epoch) = pa.hard_limited_until_epoch {
                    if until_epoch > now_epoch {
                        let remaining_secs = until_epoch - now_epoch;
                        info.hard_limited_until =
                            Some(now_instant + Duration::from_secs(remaining_secs));
                        info!(
                            account = pa.name,
                            remaining_secs, "restored hard limit from persisted state"
                        );
                    }
                }

                info.last_updated = Some(now_instant);
                // Prefer per-account epoch (accurate); fall back to global saved_at
                // (correct for old state files without per-account timestamps).
                info.last_updated_epoch = Some(pa.last_updated_epoch.unwrap_or(persisted.saved_at));
                info!(
                    account = pa.name,
                    utilization = ?pa.utilization,
                    requests = pa.requests_total,
                    "restored account state"
                );
            }
        }
    }
}

#[cfg(test)]
mod tests;
