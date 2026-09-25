use crate::*;

impl AppState {
    pub(crate) fn routing_weight_publish_ttl(probe_interval_secs: u64) -> u64 {
        const FALLBACK_PUBLISH_INTERVAL_SECS: u64 = 60;

        let effective_interval = if probe_interval_secs == 0 {
            FALLBACK_PUBLISH_INTERVAL_SECS
        } else {
            probe_interval_secs
        };

        effective_interval.saturating_mul(2).max(1)
    }

    /// Redis handle for coordination reads/writes, or `None` while the
    /// client has never yet connected (a process that started during a
    /// backend outage — LAB-1639). Before the first connect fred BUFFERS
    /// every command for `REDIS_COMMAND_TIMEOUT` (2s) and each failure would
    /// WARN, so coordination ops skip outright: no stalls, no per-operation
    /// log spam — the startup grace WARN is the one signal. After the first
    /// connect this is permanently `Some`: mid-run drops keep the LAB-932
    /// contract (bounded 2s buffered failures, request paths gated on
    /// `is_connected`). The `is_connected()` arm only covers the moments
    /// between an early first connect and the watcher task storing the flag.
    pub(crate) fn coordination_redis(&self) -> Option<&RedisClient> {
        let redis = self.redis.as_ref()?;
        if self.redis_ever_connected.load(Ordering::Relaxed) || redis.is_connected() {
            Some(redis)
        } else {
            None
        }
    }

    /// Spawn the tasks that observe the coordination client's FIRST connect
    /// (LAB-1639). Call once after construction; no-op without Redis.
    /// One task records the connect — permanently opening the
    /// `coordination_redis` gate — and one emits the single startup WARN if
    /// the backend is still unreachable after `REDIS_STARTUP_GRACE`.
    pub(crate) fn spawn_redis_connect_watcher(self: &Arc<Self>) {
        let Some(client) = self.redis.clone() else {
            return;
        };
        // Belt for the recorder task below: fred's wait_for_connect reads
        // client state THEN subscribes, so a connect landing between those
        // two ops is missed until the next reconnect — with a mid-run drop
        // in between, the gate would wrongly read closed and skip writes
        // LAB-932 buffers. This subscription fires on every successful
        // connection (the first included), so the flag cannot lag reality
        // past one broadcast.
        let state = self.clone();
        client.on_reconnect(move |_server| {
            state.redis_ever_connected.store(true, Ordering::Relaxed);
            Ok(())
        });
        let state = self.clone();
        let waiter = client.clone();
        tokio::spawn(async move {
            // Resolves immediately when already connected. With a
            // retry-forever policy it otherwise resolves only on success:
            // failed attempts broadcast errors, not connect results.
            match waiter.wait_for_connect().await {
                Ok(()) => {
                    state.redis_ever_connected.store(true, Ordering::Relaxed);
                    info!("redis connected for distributed state");
                }
                // Only broadcast when the connection task exits for good
                // (non-retryable config/URL-class errors). Without this arm
                // that exit would be silent and the startup WARN's "until
                // the backend becomes reachable" a lie.
                Err(e) => error!(
                    error = %e,
                    "redis connection task exited before first connect — coordination stays local-only"
                ),
            }
        });
        let state = self.clone();
        tokio::spawn(async move {
            tokio::time::sleep(REDIS_STARTUP_GRACE).await;
            // Double-check is_connected: a connect racing the recorder task
            // above must not produce a false outage WARN.
            if !state.redis_ever_connected.load(Ordering::Relaxed) && !client.is_connected() {
                warn!(
                    "redis unreachable at startup — running local-only until the backend becomes reachable"
                );
            }
        });
    }

    /// Publish precomputed routing weights to Redis so non-probing pods
    /// can set their gauge atomics without recomputing.
    pub(crate) async fn publish_routing_weights(&self) {
        let redis = match self.coordination_redis() {
            Some(r) => r,
            None => return,
        };
        let ttl = Self::routing_weight_publish_ttl(self.probe_interval_secs);
        let publish = |name: &str, weight: &AtomicU64, share: &AtomicU64, gate: &AtomicU64| {
            let w = f64::from_bits(weight.load(Ordering::Relaxed));
            let s = f64::from_bits(share.load(Ordering::Relaxed));
            let g = f64::from_bits(gate.load(Ordering::Relaxed));
            let key = format!("alb:weight:{}", name);
            let val = format!("{w},{s},{g}");
            let conn = redis.clone();
            tokio::spawn(async move {
                if let Err(e) = redis_set_ex(&conn, &key, val, ttl as i64).await {
                    tracing::warn!(error = %e, "redis routing weight publish failed");
                }
            });
        };
        // OpenAI endpoints are skipped — sync_from_redis only reads weights
        // for Anthropic targets.
        for ep in &self.endpoints {
            if ep.protocol == Protocol::OpenAI {
                continue;
            }
            publish(
                &ep.name,
                &ep.last_routing_weight,
                &ep.last_routing_share,
                &ep.last_effective_gate,
            );
        }
    }

    /// Distributed hard-limit recovery: notifies other replicas that the local
    /// hard limit has been cleared. Writes a sentinel (`HARD_LIMIT_CLEARED_SENTINEL`)
    /// to `alb:hard:{account}` so `sync_from_redis` can proactively clear other
    /// replicas' stale `hard_limited_until` Instants — DEL alone leaves them stuck
    /// until their own probe sees recovery.
    ///
    /// The write uses a Lua CAS script: only clears if the current value is absent
    /// or already `<= now_epoch` (i.e. stale/expired). This prevents a TOCTOU race
    /// where `mark_hard_limited` spawns a write of `until_epoch=now+cooldown` at
    /// roughly the same moment, and unordered tokio::spawn tasks reach Redis in
    /// reversed order — without CAS, the stale sentinel would clobber the fresh
    /// hard-limit write and propagate a false "cleared" state across replicas.
    ///
    /// Also refreshes metric gauges and publishes updated routing weights so all
    /// replicas reflect the recovery within the next sync tick.
    /// Broadcast a hard-limit recovery for an endpoint by name. The Redis
    /// sentinel key is derived from the name alone.
    pub(crate) async fn signal_hard_limit_recovery(&self, endpoint_name: &str) {
        if let Some(redis) = self.coordination_redis() {
            let conn = redis.clone();
            let key = format!("alb:hard:{}", endpoint_name);
            let now_epoch = Self::now_epoch();
            // Lua CAS: only write the sentinel if the current value is absent,
            // already the sentinel, or an expired hard-limit (epoch <= now).
            // Rejects a concurrent mark_hard_limited write with epoch > now.
            const RECOVERY_CAS_SCRIPT: &str = r#"
                local current = redis.call('GET', KEYS[1])
                if current == false then
                    return redis.call('SET', KEYS[1], ARGV[1], 'EX', ARGV[2])
                end
                local n = tonumber(current)
                if n == nil or n <= tonumber(ARGV[3]) then
                    return redis.call('SET', KEYS[1], ARGV[1], 'EX', ARGV[2])
                end
                return 0
                "#;
            tokio::spawn(async move {
                // Args travel as decimal strings — byte-identical to the wire
                // encoding the redis crate used, so the stored sentinel still
                // parses as u64 on the sync_from_redis read side.
                let result: Result<RedisValue, fred::error::RedisError> = conn
                    .eval(
                        RECOVERY_CAS_SCRIPT,
                        vec![key],
                        vec![
                            HARD_LIMIT_CLEARED_SENTINEL.to_string(),
                            HARD_LIMIT_SENTINEL_TTL_SECS.to_string(),
                            now_epoch.to_string(),
                        ],
                    )
                    .await;
                if let Err(e) = result {
                    tracing::warn!(error = %e, "redis sentinel write failed for hard-limit clear");
                }
            });
        }
        self.refresh_metrics_weights().await;
        self.publish_routing_weights().await;
    }

    fn pick_weighted_bucket<'a>(
        &self,
        effective: &[&'a RoutingCandidate],
        total_weight: f64,
        affinity_key: Option<&str>,
    ) -> &'a RoutingCandidate {
        let walk_buckets = |target: f64| -> &'a RoutingCandidate {
            let mut picked = effective.last().unwrap();
            let mut cumulative = 0.0;
            for c in effective {
                cumulative += c.weight;
                if target < cumulative {
                    picked = c;
                    break;
                }
            }
            picked
        };

        if let Some(key) = affinity_key {
            let target = (stable_affinity_hash(key) as f64 / u64::MAX as f64) * total_weight;
            walk_buckets(target)
        } else {
            let counter = self.robin.fetch_add(1, Ordering::Relaxed) as u64;
            let position = (counter.wrapping_mul(11400714819323198485) % 10000) as f64;
            let target = position / 10000.0 * total_weight;
            walk_buckets(target)
        }
    }

    fn pick_dynamic_capacity_v1<'a>(
        &self,
        effective: &[&'a RoutingCandidate],
        total_weight: f64,
        affinity_key: Option<&str>,
    ) -> &'a RoutingCandidate {
        let mut picked = self.pick_weighted_bucket(effective, total_weight, affinity_key);

        if affinity_key.is_some() && effective.len() == 2 {
            let other = if picked.endpoint == effective[0].endpoint {
                effective[1]
            } else {
                effective[0]
            };
            let (picked_headroom, bind) = affinity_headroom(picked);
            let (other_headroom, _) = affinity_headroom(other);
            if picked_headroom < other_headroom * LEGACY_AFFINITY_OVERRIDE_RATIO {
                self.affinity_migrations[bind as usize].fetch_add(1, Ordering::Relaxed);
                // Loud on purpose — see the StickyWeightedV2 override below for
                // the cascade rationale. Breaking affinity is a pool-health
                // warning sign, NOT routine — EXCEPT when a status floor bound
                // the sticky account (`Floored`): Anthropic flagged it and we're
                // moving the session onto fresh capacity, which is what the
                // floor exists for. That case logs at INFO (LAB-3295); the
                // counter still records every migration by reason. The field set
                // is identical across levels — only tracing's compile-time level
                // forces the two arms — so it lives once in this local macro.
                macro_rules! log_migration {
                    ($lvl:ident, $msg:literal) => {
                        $lvl!(
                            strategy = RoutingStrategy::DynamicCapacityV1.as_str(),
                            affinity = affinity_key.unwrap_or("-"),
                            reason = bind.as_str(),
                            picked_account = self.endpoint_name(picked.endpoint),
                            picked_headroom = format!("{:.3}", picked_headroom),
                            other_account = self.endpoint_name(other.endpoint),
                            other_headroom = format!("{:.3}", other_headroom),
                            ratio = format!("{:.3}", picked_headroom / other_headroom),
                            $msg
                        )
                    };
                }
                if matches!(bind, AffinityBind::Floored) {
                    log_migration!(info, "affinity migrated: sticky endpoint gate is status-floored, moving session to fresh capacity (routine)");
                } else {
                    log_migration!(warn, "affinity broken: sticky endpoint out of headroom, migrating session (cascade risk)");
                }
                picked = other;
            }
            // NOTE: Request-balance override intentionally disabled. The previous
            // implementation used Account.requests counters which are replica-local
            // and not pool-scoped, so multi-replica deployments could make routing
            // decisions on partial history. Re-enable only after adding shared,
            // pool-scoped counters that are synchronized across replicas.
        }

        picked
    }

    fn pick_sticky_weighted_v2<'a>(
        &self,
        effective: &[&'a RoutingCandidate],
        total_weight: f64,
        affinity_key: Option<&str>,
    ) -> &'a RoutingCandidate {
        let mut picked = self.pick_weighted_bucket(effective, total_weight, affinity_key);

        if let Some(key) = affinity_key {
            let (picked_headroom, bind) = affinity_headroom(picked);
            let (best, best_headroom) = effective
                .iter()
                .map(|c| (*c, affinity_headroom(c).0))
                .max_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal))
                .unwrap();
            if best.endpoint != picked.endpoint
                && picked_headroom < best_headroom * STICKY_WEIGHTED_OVERRIDE_RATIO
            {
                // The sticky account is out of headroom for this session. Do NOT
                // migrate to `best` (the global argmax): that target rotates every
                // request as utilizations drift, so a session chases it across the
                // whole pool and pays a cold-cache `cache_creation` charge on every
                // hop (measured in prod: a swept client ran a 1.18 create:read ratio
                // vs ~0.05 for sticky clients — it created more cache than it ever
                // read back). Instead re-pick over the healthy remainder using a
                // SALTED hash of the same affinity key: deterministic per session
                // (so the cache warms on the replacement and stays there) yet spread
                // across sessions (distinct keys → distinct replacements), and
                // independent of which account is momentarily `best`.
                //
                // "Healthy" = the override would not flee it: the negation of the
                // trigger above. Excluding only `picked` let a 7d-spent account
                // (gate-healthy, big expiring-quota bucket) replace an equally
                // spent one, which answered the caller with Anthropic's
                // entitlement 400 (LAB-4719). The floor also excludes `picked`.
                // Never empty: `best` always clears it, and clearing it means
                // headroom > 0, so gate < 1 and weight > 0. The `best` fallback
                // below is therefore unreachable from routing_candidates; it
                // stays as a guard, and it would still satisfy the floor.
                let floor = best_headroom * STICKY_WEIGHTED_OVERRIDE_RATIO;
                let remaining: Vec<&RoutingCandidate> = effective
                    .iter()
                    .copied()
                    .filter(|c| affinity_headroom(c).0 >= floor)
                    .collect();
                let remaining_weight: f64 = remaining.iter().map(|c| c.weight).sum();
                let replacement = if remaining_weight > 0.0 {
                    // NUL separator can't appear in a header-derived affinity key
                    // (ip:client:agent:session), so the salted key never collides
                    // with a real one.
                    let salted = format!("{key}\u{0}migrate");
                    self.pick_weighted_bucket(&remaining, remaining_weight, Some(&salted))
                } else {
                    best
                };
                self.affinity_migrations[bind as usize].fetch_add(1, Ordering::Relaxed);
                // Loud on purpose. Sustained breaking with reason="loaded" means
                // the pool is the bottleneck — add capacity, don't tune the
                // ratio. reason="spent" means one account's week is nearly gone
                // while others are fresh; expect it to cluster before weekly
                // resets and vanish after. reason="floored" is neither: Anthropic
                // put a status floor on the sticky account and we're moving the
                // session onto fresh capacity — the floor doing its job, not a
                // pool problem. It logs at INFO so the WARN stays a real signal
                // (LAB-3295); the counter records all three. Field set is
                // identical across levels — tracing's compile-time level forces
                // the two arms — so it lives once in this local macro.
                macro_rules! log_migration {
                    ($lvl:ident, $msg:literal) => {
                        $lvl!(
                            strategy = RoutingStrategy::StickyWeightedV2.as_str(),
                            affinity = key,
                            reason = bind.as_str(),
                            picked_account = self.endpoint_name(picked.endpoint),
                            picked_headroom = format!("{:.3}", picked_headroom),
                            replacement_account = self.endpoint_name(replacement.endpoint),
                            replacement_headroom =
                                format!("{:.3}", affinity_headroom(replacement).0),
                            best_account = self.endpoint_name(best.endpoint),
                            ratio = format!("{:.3}", picked_headroom / best_headroom),
                            $msg
                        )
                    };
                }
                if matches!(bind, AffinityBind::Floored) {
                    log_migration!(info, "affinity migrated: sticky endpoint gate is status-floored, moving session to fresh capacity (routine)");
                } else {
                    log_migration!(warn, "affinity broken: sticky endpoint out of headroom, migrating session to stable replacement");
                }
                picked = replacement;
            }
        }

        picked
    }

    /// Select from a pre-filtered candidate slice using the configured routing strategy.
    fn pick_from_candidates(
        &self,
        effective: &[&RoutingCandidate],
        total_weight: f64,
        affinity_key: Option<&str>,
        tier: u32,
    ) -> EndpointIdx {
        let picked = match self.routing_strategy {
            RoutingStrategy::DynamicCapacityV1 => {
                self.pick_dynamic_capacity_v1(effective, total_weight, affinity_key)
            }
            RoutingStrategy::StickyWeightedV2 => {
                self.pick_sticky_weighted_v2(effective, total_weight, affinity_key)
            }
        };

        debug!(
            strategy = self.routing_strategy.as_str(),
            account = self.endpoint_name(picked.endpoint),
            tier = tier,
            gate = format!("{:.3}", picked.gate),
            gate_5h = format!("{:.3}", picked.gate_5h),
            gate_7d = format!("{:.3}", picked.gate_7d),
            waste_risk = format!("{:.3}", picked.wr),
            weight = format!("{:.3}", picked.weight),
            share = format!("{:.0}%", picked.weight / total_weight * 100.0),
            source = picked.source,
            candidates = effective.len(),
            affinity = affinity_key.unwrap_or("-"),
            "pick: selected"
        );
        picked.endpoint
    }

    /// The client's non-empty `preferred_endpoints` list, if it has one.
    /// Resolved through the authenticated `[[clients]]` registry only — on the
    /// legacy `proxy_key` / open path `clients` is empty, so a spoofable
    /// header-derived client id can never acquire another client's pin.
    fn client_preferred_endpoints(&self, client_id: &str) -> Option<&[String]> {
        self.clients
            .iter()
            .find(|c| c.name == client_id)
            .map(|c| c.preferred_endpoints.as_slice())
            .filter(|p| !p.is_empty())
    }

    /// Whether `endpoint_idx` is in a `preferred_endpoints` list. The ONE
    /// membership predicate, shared by the pick-time filter and `pin_status`
    /// — if they ever diverged, the `pin=` log field would lie about what the
    /// picker actually did.
    fn endpoint_is_preferred(&self, preferred: &[String], endpoint_idx: EndpointIdx) -> bool {
        preferred
            .iter()
            .any(|p| *p == self.endpoints[endpoint_idx].name)
    }

    /// `pin` field for the per-request serving log lines: "pinned" when the
    /// serving endpoint is in the client's preferred set, "spilled" when the
    /// client has preferences but was served outside them, "-" for clients
    /// without preferences.
    pub(crate) fn pin_status(&self, client_id: &str, endpoint_idx: EndpointIdx) -> &'static str {
        match self.client_preferred_endpoints(client_id) {
            Some(p) if self.endpoint_is_preferred(p, endpoint_idx) => "pinned",
            Some(_) => "spilled",
            None => "-",
        }
    }

    /// Test-only convenience: pick with no client identity, so no pinning
    /// applies. Keeps the pre-LAB-2636 routing tests byte-identical.
    #[cfg(test)]
    pub(crate) async fn pick_endpoint(
        &self,
        affinity_key: Option<&str>,
        model: &str,
        skip: &[EndpointIdx],
    ) -> Option<EndpointIdx> {
        self.pick_endpoint_for_client(affinity_key, model, skip, "")
            .await
    }

    /// Pick the best available endpoint (account or fallback upstream).
    ///
    /// Per-client pinning (LAB-2636 / #151) runs first: if `client_id` names a
    /// `[[clients]]` entry with `preferred_endpoints` and at least one of them
    /// is a healthy, undemoted candidate (serves the model, not hard-limited,
    /// not transport-unhealthy, not skipped, `gate < soft_limit`, and not
    /// priority-demoted for serving via paid overage / always-paid Fable), the
    /// candidate set is restricted to the preferred endpoints. The filter runs
    /// BEFORE tier/affinity/weighted selection, so a prior affinity landing on
    /// a non-preferred endpoint cannot defeat the pin. Otherwise the full pool
    /// applies unchanged (spill-over). The undemoted requirement matters: an
    /// overage-covered account reports a LOW gate (the overage window
    /// supersedes its exhausted subscription windows), so without it a pinned
    /// client would keep billing paid overage forever instead of spilling to
    /// free general-pool capacity — violating the free-before-paid guarantee
    /// below, which the retain would otherwise bypass.
    ///
    /// Tiers are tried strictly in ascending priority order. Within a tier:
    /// healthy candidates (`gate < soft_limit`) are preferred; if none are healthy
    /// the tier degrades to its soft-limited candidates. Only when a tier has zero
    /// total weight (genuinely exhausted) does routing move to the next tier — so
    /// `soft_limit` is intra-tier load-shedding and never causes a tier jump. This
    /// guarantees free capacity is fully drained before any paid (overage/upstream)
    /// tier is touched.
    pub(crate) async fn pick_endpoint_for_client(
        &self,
        affinity_key: Option<&str>,
        model: &str,
        skip: &[EndpointIdx],
        client_id: &str,
    ) -> Option<EndpointIdx> {
        let mut candidates = self.routing_candidates(model, skip).await;
        if let Some(preferred) = self.client_preferred_endpoints(client_id) {
            let is_preferred =
                |c: &RoutingCandidate| self.endpoint_is_preferred(preferred, c.endpoint);
            // Undemoted = candidate priority equals the configured priority.
            // `routing_candidates` adds `overage_penalty` while an account
            // serves via paid overage (or always-paid Fable) — that demotion
            // is exactly the paid-capacity signal pinning must respect.
            let undemoted =
                |c: &RoutingCandidate| c.priority == self.endpoints[c.endpoint].priority;
            if candidates
                .iter()
                .any(|c| is_preferred(c) && undemoted(c) && c.gate < self.soft_limit)
            {
                candidates.retain(is_preferred);
            } else {
                // Greppable spill marker — the request leaves its dedicated
                // account(s) and will consume shared-pool cache/claims.
                info!(
                    client_id,
                    model, "pick: no preferred endpoint viable — spilling to general pool"
                );
            }
        }
        if candidates.is_empty() {
            debug!("pick: no available endpoints");
            return None;
        }

        // Unique priority tiers, ascending (0 = highest priority).
        let mut tiers: Vec<u32> = candidates.iter().map(|c| c.priority).collect();
        tiers.sort_unstable();
        tiers.dedup();

        for tier in &tiers {
            let tier_candidates: Vec<&RoutingCandidate> =
                candidates.iter().filter(|c| c.priority == *tier).collect();

            // Prefer healthy candidates; fall back to soft-limited ones within the tier.
            let healthy: Vec<&RoutingCandidate> = tier_candidates
                .iter()
                .filter(|c| c.gate < self.soft_limit)
                .copied()
                .collect();

            let (effective, degraded): (Vec<&RoutingCandidate>, bool) = if !healthy.is_empty() {
                (healthy, false)
            } else {
                (tier_candidates.clone(), true)
            };

            let total_weight: f64 = effective.iter().map(|c| c.weight).sum();
            if total_weight <= 0.0 {
                debug!(
                    tier = tier,
                    "pick: tier exhausted (zero weight), trying next"
                );
                continue;
            }

            if degraded {
                debug!(
                    tier = tier,
                    "pick: tier all soft-limited — degrading within tier"
                );
            }
            return Some(self.pick_from_candidates(&effective, total_weight, affinity_key, *tier));
        }

        debug!("pick: all tiers exhausted");
        None
    }

    /// Test-only convenience: parse rate-limit headers into endpoint `idx`.
    #[cfg(test)]
    pub(crate) async fn update_rate_info(&self, idx: usize, headers: &reqwest::header::HeaderMap) {
        let ep = &self.endpoints[idx];
        self.update_rate_info_for(
            &ep.rate_info,
            &ep.name,
            headers,
            /* is_fast_mode */ false,
        )
        .await;
    }

    /// Parse rate-limit headers from a response into the supplied
    /// `RateLimitInfo` lock.
    pub(crate) async fn update_rate_info_for(
        &self,
        rate_info: &RwLock<RateLimitInfo>,
        endpoint_name: &str,
        headers: &reqwest::header::HeaderMap,
        is_fast_mode: bool,
    ) {
        // A fast-mode response's `anthropic-ratelimit-unified-*` headers
        // describe the FAST/paid POOL, not the account's 5h/7d subscription
        // windows. On the observed wire (LAB-2693 / anthropic-lb#163,
        // 2026-09-02) a fast 200 reports `representative-claim: overage`,
        // `overage-in-use: true`, the `overage-*` block, and NO `five_hour`
        // claim — every field is fast-pool, none is account headroom. Ingesting
        // it flipped `overage_in_use` true, and `routing_candidates` then added
        // `overage_penalty`, demoting the serving account out of standard
        // rotation on a single fast request. Fast mode bills a bucket separate
        // from the standard windows (#161), so skip the whole ingest and leave
        // the standard view as the last standard response left it (stale, not
        // corrupted). `last_updated` therefore does not advance on fast-pool
        // data, which keeps the background probe treating the view as due for
        // refresh.
        //
        // This keys on the REQUEST, not the response: a fast-pool `overage` 200
        // is indistinguishable on the wire from a genuine-overage standard 200
        // (see the negative-control test), so the response alone cannot be
        // classified — gating on `overage` markers would suppress real overage
        // tracking on standard traffic. The invariant this relies on: a
        // response to a fast request carries no genuine account 5h/7d claim. It
        // holds today — fast-capable accounts answer from the fast pool, and a
        // fast-disabled org returns 400 rather than a standard 200 (#160 also
        // routes fast requests away from those accounts). If it ever breaks, a
        // fast response could still freeze this account's standard view; the
        // ≤`probe_interval_secs` background probe refreshes the real 5h/7d view
        // regardless of traffic, bounding that staleness.
        if is_fast_mode {
            return;
        }

        let mut info = rate_info.write().await;

        // Debug: log all ratelimit headers
        for (name, value) in headers.iter() {
            let name_str = name.as_str();
            if name_str.contains("ratelimit") || name_str.contains("retry") {
                if let Ok(v) = value.to_str() {
                    tracing::trace!(
                        account = endpoint_name,
                        header = name_str,
                        value = v,
                        "rate-limit header"
                    );
                }
            }
        }

        // Parse representative claim FIRST — determines where 7d data is stored.
        // Model-specific claims (e.g., "seven_day_sonnet") route 7d data to a per-claim
        // entry so different models don't overwrite each other's utilization.
        let rep_claim = headers
            .get("anthropic-ratelimit-unified-representative-claim")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());

        if let Some(ref claim) = rep_claim {
            // Truncated for the same reason as the claim key below, plus one of
            // its own: `refresh_metrics_weights` looks this string up in the
            // (now truncated) claims map, so an untruncated copy would miss its
            // own entry and silently report a different claim than the router used.
            info.representative_claim = Some(truncate_label(claim));
        }

        // Determine the claim key for 7d data storage.
        // If claim starts with "seven_day", use it verbatim (e.g., "seven_day_sonnet").
        // Otherwise default to "seven_day" (general bucket).
        // Truncated because the key outlives the response as a `claim` metric
        // label — see `truncate_label`.
        let claim_key_7d = truncate_label(
            rep_claim
                .as_deref()
                .filter(|c| c.starts_with("seven_day"))
                .unwrap_or("seven_day"),
        );
        let claim_key_7d = claim_key_7d.as_str();

        // Capture 5h utilization (flat — no per-model sub-budgets observed for 5h).
        // Track whether we got utilization for sticky status fix (Bug #1).
        let now_epoch = Self::now_epoch();
        let got_5h_util = if let Some(v) = headers.get("anthropic-ratelimit-unified-5h-utilization")
        {
            if let Ok(s) = v.to_str() {
                info.utilization_5h = s.parse::<f64>().ok().map(|v| v.clamp(0.0, 1.0));
                true
            } else {
                false
            }
        } else {
            false
        };

        // Capture 7d utilization → store in claims_7d[claim_key].
        let got_7d_util = if let Some(v) = headers.get("anthropic-ratelimit-unified-7d-utilization")
        {
            if let Ok(s) = v.to_str() {
                if let Ok(util) = s.parse::<f64>() {
                    // The header parser's only growth point for `claims_7d`
                    // (`bound_ingested_claims` covers the two wholesale assigns).
                    // A refused key reports `false` because it stored nothing —
                    // reporting parsed 7d utilization for data that was dropped
                    // would be a lie to every consumer of that flag.
                    if claim_admitted(&mut info, claim_key_7d, endpoint_name) {
                        let entry = info.claims_7d.entry(claim_key_7d.to_string()).or_default();
                        entry.utilization = Some(util.clamp(0.0, 1.0));
                        entry.last_seen = now_epoch;
                        true
                    } else {
                        false
                    }
                } else {
                    false
                }
            } else {
                false
            }
        } else {
            false
        };

        // Capture per-window reset timestamps (epoch seconds).
        // Sanity-capped: 5h window can't reset >5h out, 7d can't reset >7d out (Bug #6).
        if let Some(v) = headers.get("anthropic-ratelimit-unified-5h-reset") {
            if let Ok(s) = v.to_str() {
                if let Ok(epoch) = s.parse::<u64>() {
                    if epoch <= now_epoch + 18000 {
                        info.reset_5h = Some(epoch);
                    }
                }
            }
        }
        // 7d reset → only update existing claims_7d entries. Creating a placeholder
        // with utilization=None would shadow real fallback entries in resolve_7d_claim
        // (model-specific key takes priority over "seven_day" via or_else).
        if let Some(v) = headers.get("anthropic-ratelimit-unified-7d-reset") {
            if let Ok(s) = v.to_str() {
                if let Ok(epoch) = s.parse::<u64>() {
                    if epoch <= now_epoch + 604800 {
                        if let Some(entry) = info.claims_7d.get_mut(claim_key_7d) {
                            entry.reset = Some(epoch);
                            entry.last_seen = now_epoch;
                        }
                    }
                }
            }
        }

        // Capture per-window status. These signal API-side pressure (burst limits, concurrent
        // request limits, per-model sub-limits) that raw utilization percentages don't reflect.
        // If the API sent utilization for a window but NO status header, clear stale status —
        // absence of the header means pressure has subsided (Bug #1).
        if let Some(v) = headers.get("anthropic-ratelimit-unified-5h-status") {
            if let Ok(s) = v.to_str() {
                info.status_5h = Some(s.to_string());
            }
        } else if got_5h_util {
            info.status_5h = None;
        }
        // 7d status → only update existing claims_7d entries (same shadowing concern).
        if let Some(v) = headers.get("anthropic-ratelimit-unified-7d-status") {
            if let Ok(s) = v.to_str() {
                if let Some(entry) = info.claims_7d.get_mut(claim_key_7d) {
                    entry.status = Some(s.to_string());
                    entry.last_seen = now_epoch;
                }
            }
        } else if got_7d_util {
            // Status absent but util present → pressure subsided for this claim
            if let Some(entry) = info.claims_7d.get_mut(claim_key_7d) {
                entry.status = None;
            }
        }

        // Fable included-band triplet ("7d_oi" = 7d overage-included). Emitted
        // ONLY on Fable responses (verified live 2026-07-21, LAB-387): a sonnet
        // response from the same account carries no 7d_oi headers, and there is
        // no `seven_day_fable` representative claim on the wire. Normalised into
        // the internal FABLE_BAND_CLAIM entry so the standard claims machinery
        // applies. Absence of the triplet (every non-Fable response) must NOT
        // clear the entry — reset-based eviction below handles staleness,
        // exactly like every other claim. Anchored on the utilization header so
        // a partial triplet never creates a utilization-less placeholder (same
        // shadowing rule as the general 7d claim).
        if let Some(v) = headers.get("anthropic-ratelimit-unified-7d_oi-utilization") {
            if let Ok(s) = v.to_str() {
                if let Ok(util) = s.parse::<f64>() {
                    let first_sighting = !info.claims_7d.contains_key(FABLE_BAND_CLAIM);
                    let entry = info
                        .claims_7d
                        .entry(FABLE_BAND_CLAIM.to_string())
                        .or_default();
                    entry.utilization = Some(util.clamp(0.0, 1.0));
                    entry.last_seen = now_epoch;
                    entry.reset = headers
                        .get("anthropic-ratelimit-unified-7d_oi-reset")
                        .and_then(|v| v.to_str().ok())
                        .and_then(|s| s.parse::<u64>().ok())
                        .filter(|&epoch| epoch <= now_epoch + 604800) // 7d sanity cap
                        .or(entry.reset);
                    // Status absent within a present triplet = pressure subsided
                    // (same semantics as the general 7d claim, Bug #1).
                    entry.status = headers
                        .get("anthropic-ratelimit-unified-7d_oi-status")
                        .and_then(|v| v.to_str().ok())
                        .map(|s| s.to_string());
                    if first_sighting {
                        info!(
                            account = endpoint_name,
                            utilization = util,
                            "fable band claim first observed (7d_oi headers) — \
                             fable-aware routing active for this account"
                        );
                    }
                }
            }
        }

        // Overage (paid extra usage) — account-level, covers whichever subscription
        // window is exhausted. `overage-in-use` is always overwritten: header absent
        // or "false" → false (so demotion auto-clears when the window refills).
        info.overage_in_use = headers
            .get("anthropic-ratelimit-unified-overage-in-use")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.eq_ignore_ascii_case("true"))
            .unwrap_or(false);
        if info.overage_in_use {
            info.overage_status = headers
                .get("anthropic-ratelimit-unified-overage-status")
                .and_then(|v| v.to_str().ok())
                .map(|s| s.to_string());
            info.overage_utilization = headers
                .get("anthropic-ratelimit-unified-overage-utilization")
                .and_then(|v| v.to_str().ok())
                .and_then(|s| s.parse::<f64>().ok())
                .map(|v| v.clamp(0.0, 1.0));
            info.overage_reset = headers
                .get("anthropic-ratelimit-unified-overage-reset")
                .and_then(|v| v.to_str().ok())
                .and_then(|s| s.parse::<u64>().ok())
                .filter(|&epoch| epoch <= now_epoch + 2_678_400); // ≤31d sanity cap
        } else {
            // Not in overage — clear stale overage data so routing doesn't read it.
            info.overage_status = None;
            info.overage_utilization = None;
            info.overage_reset = None;
        }

        // Evict stale claims: expired resets (reset <= now) or reset-less entries
        // not seen in >24h. This aligns with load_state() which drops all reset-less
        // entries on boot — the 24h grace period covers the normal case where
        // utilization arrives before reset in the same response cycle.
        const CLAIMS_STALE_SECS: u64 = 86400;
        info.claims_7d.retain(|_, c| {
            if let Some(r) = c.reset {
                r > now_epoch
            } else {
                // No reset — keep only if recently seen
                c.last_seen > 0 && now_epoch.saturating_sub(c.last_seen) < CLAIMS_STALE_SECS
            }
        });

        // Derive flat convenience fields from claims_7d (backward compat for
        // logs/stats; also the model-agnostic fallback input — see
        // derive_flat_7d_fields for why only all-traffic claims participate).
        (info.utilization_7d, info.reset_7d, info.status_7d) =
            derive_flat_7d_fields(&info.claims_7d);

        // Derive unified utilization = max across all windows (5h + all-traffic
        // 7d claims — same allowlist as above; this is the brake's last-resort
        // fallback when both adjusted windows are unavailable).
        // Recompute unconditionally so stale unified values don't survive eviction.
        // Include 5h if reset is absent (no staleness info) or in the future;
        // exclude only when reset is present AND expired (stale data).
        let mut max_util: Option<f64> = info
            .utilization_5h
            .filter(|_| info.reset_5h.is_none_or(|r| r > now_epoch));
        for (key, cd) in info.claims_7d.iter() {
            if !claim_gates_all_traffic(key) {
                continue;
            }
            if let Some(u) = cd.utilization {
                max_util = Some(max_util.map_or(u, |cur| cur.max(u)));
            }
        }
        info.utilization = max_util;

        // Legacy headers (still try them)
        let mut got_legacy = false;
        if let Some(v) = headers.get("x-ratelimit-remaining-requests") {
            if let Ok(s) = v.to_str() {
                info.remaining_requests = s.parse().ok();
                got_legacy = true;
            }
        }
        if let Some(v) = headers.get("x-ratelimit-remaining-tokens") {
            if let Ok(s) = v.to_str() {
                info.remaining_tokens = s.parse().ok();
                got_legacy = true;
            }
        }
        if let Some(v) = headers.get("x-ratelimit-limit-requests") {
            if let Ok(s) = v.to_str() {
                info.limit_requests = s.parse().ok();
                got_legacy = true;
            }
        }
        if let Some(v) = headers.get("x-ratelimit-limit-tokens") {
            if let Ok(s) = v.to_str() {
                info.limit_tokens = s.parse().ok();
                got_legacy = true;
            }
        }

        // Only advance last_updated when we actually parsed rate-limit data.
        // Responses without rate headers (e.g. 5xx) must not clear
        // stale_after_hard_limit, which relies on last_updated <= hard_limited_until.
        let parsed_any = got_5h_util || got_7d_util || rep_claim.is_some() || got_legacy;
        if parsed_any {
            info.last_updated = Some(Instant::now());
            info.last_updated_epoch = Some(Self::now_epoch());
        }

        trace!(
            account = endpoint_name,
            utilization = ?info.utilization,
            util_7d = ?info.utilization_7d,
            util_5h = ?info.utilization_5h,
            reset_5h = ?info.reset_5h,
            reset_7d = ?info.reset_7d,
            status_5h = ?info.status_5h,
            status_7d = ?info.status_7d,
            claim = ?info.representative_claim,
            n_claims_7d = info.claims_7d.len(),
            remaining_requests = ?info.remaining_requests,
            remaining_tokens = ?info.remaining_tokens,
            "rate info updated"
        );

        // Fire-and-forget: publish rate info to Redis for cross-replica sync
        if let Some(redis) = self.coordination_redis() {
            let rate_data = RedisRateInfo {
                utilization: info.utilization,
                utilization_5h: info.utilization_5h,
                utilization_7d: info.utilization_7d,
                reset_5h: info.reset_5h,
                reset_7d: info.reset_7d,
                status_5h: info.status_5h.clone(),
                status_7d: info.status_7d.clone(),
                claims_7d: info.claims_7d.clone(),
                representative_claim: info.representative_claim.clone(),
                remaining_requests: info.remaining_requests,
                remaining_tokens: info.remaining_tokens,
                limit_requests: info.limit_requests,
                limit_tokens: info.limit_tokens,
                overage_in_use: info.overage_in_use,
                overage_status: info.overage_status.clone(),
                overage_utilization: info.overage_utilization,
                overage_reset: info.overage_reset,
                updated_at: Self::now_epoch(),
            };
            // Compute TTL from earliest reset timestamp
            let now_epoch = Self::now_epoch();
            let min_reset = info
                .reset_5h
                .into_iter()
                .chain(info.claims_7d.values().filter_map(|c| c.reset))
                .min();
            let ttl = min_reset
                .map(|r| r.saturating_sub(now_epoch).max(60))
                .unwrap_or(3600); // default 1h if no reset known

            let conn = redis.clone();
            let key = format!("alb:rate:{}", endpoint_name);
            tokio::spawn(async move {
                if let Ok(json) = serde_json::to_string(&rate_data) {
                    if let Err(e) = redis_set_ex(&conn, &key, json, ttl as i64).await {
                        tracing::warn!(error = %e, "redis rate info write failed");
                    }
                }
            });
        }
    }

    /// Test-only convenience: mark endpoint `idx` as hard rate-limited.
    #[cfg(test)]
    async fn mark_hard_limited(&self, idx: usize, headers: &reqwest::header::HeaderMap) {
        let ep = &self.endpoints[idx];
        self.mark_hard_limited_for(&ep.rate_info, &ep.name, headers)
            .await;
    }

    /// Apply a 429 cooldown to the supplied `RateLimitInfo` lock.
    pub(crate) async fn mark_hard_limited_for(
        &self,
        rate_info: &RwLock<RateLimitInfo>,
        endpoint_name: &str,
        headers: &reqwest::header::HeaderMap,
    ) {
        let mut info = rate_info.write().await;

        let raw_retry_after = headers
            .get("retry-after")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());

        // Transient burst 429 (per-minute RPM / concurrency) rather than capacity
        // exhaustion: exponential backoff and don't poison state.
        let is_burst_limit = is_burst_429(headers);

        let cooldown = if is_burst_limit {
            info.consecutive_burst_429s = info.consecutive_burst_429s.saturating_add(1);
            // Exponential backoff: 5s → 10s → 20s → 40s → 60s (cap).
            // RPM windows are typically 60s; 1s was too short and caused thrashing.
            let burst_secs: u64 = match info.consecutive_burst_429s {
                1 => 5,
                2 => 10,
                3 => 20,
                4 => 40,
                _ => 60,
            };
            Duration::from_secs(burst_secs)
        } else {
            info.consecutive_burst_429s = 0;
            if let Some(ref s) = raw_retry_after {
                if let Ok(secs) = s.parse::<f64>() {
                    if secs.is_finite() && secs > 0.0 && secs < 86400.0 {
                        Duration::from_secs_f64(secs)
                    } else {
                        self.cooldown
                    }
                } else {
                    self.cooldown
                }
            } else {
                self.cooldown
            }
        };

        let until = Instant::now() + cooldown;
        info.hard_limited_until = Some(until);
        // Only poison remaining counts when we have actual rate-limit data
        // confirming exhaustion. Burst 429s have no such data.
        if !is_burst_limit {
            info.remaining_requests = Some(0);
            info.remaining_tokens = Some(0);
        }
        info.last_updated = Some(Instant::now());
        info.last_updated_epoch = Some(Self::now_epoch());

        warn!(
            account = endpoint_name,
            cooldown_secs = cooldown.as_secs(),
            retry_after_raw = ?raw_retry_after,
            burst = is_burst_limit,
            consecutive_burst = info.consecutive_burst_429s,
            "account hard rate-limited (429), cooling down"
        );

        // Propagate to Redis for cross-replica awareness
        if let Some(redis) = self.coordination_redis() {
            let conn = redis.clone();
            let key = format!("alb:hard:{}", endpoint_name);
            let until_epoch = Self::now_epoch()
                + cooldown.as_secs()
                + if cooldown.subsec_nanos() > 0 { 1 } else { 0 };
            let ttl = cooldown.as_secs().max(1);
            tokio::spawn(async move {
                if let Err(e) = redis_set_ex(&conn, &key, until_epoch.to_string(), ttl as i64).await
                {
                    tracing::warn!(error = %e, "redis SET EX failed for hard-limit propagation");
                }
            });
        }

        // Drop the write lock explicitly so refresh_metrics_weights can take
        // its read lock without contention.
        drop(info);

        // Hard-limit transitions are sparse, high-impact events. Refresh
        // metric gauges immediately so the dashboard reflects the dropped
        // account within seconds, not after the next probe cycle.
        self.refresh_metrics_weights().await;
        // Republish weights to Redis so non-probing replicas pick up the
        // new routing gauge immediately rather than reading a stale
        // alb:weight:{account} until the next probe cycle.
        self.publish_routing_weights().await;
    }

    /// Record one upstream transport failure (ETIMEDOUT/reset/closed/DNS)
    /// against an endpoint. After TRANSPORT_FAILURE_THRESHOLD consecutive
    /// failures the endpoint is circuit-broken out of routing for
    /// `transport_cooldown` so a stateless affinity recompute stops paying
    /// ~2 connect timeouts per request against a persistently-dead endpoint.
    /// Transport health only — never touches the 429 `hard_limited_until` path.
    pub(crate) async fn record_transport_failure(&self, endpoint_idx: usize) {
        let ep = &self.endpoints[endpoint_idx];
        let mut info = ep.rate_info.write().await;
        let now = Instant::now();
        // Cooldown elapsed → fresh era: the expired breaker's failures don't
        // carry over, so re-opening takes a full threshold of new evidence.
        if info
            .transport_unhealthy_until
            .is_some_and(|until| now >= until)
        {
            info.transport_unhealthy_until = None;
            info.consecutive_transport_failures = 0;
        }
        info.consecutive_transport_failures = info.consecutive_transport_failures.saturating_add(1);
        if info.consecutive_transport_failures >= TRANSPORT_FAILURE_THRESHOLD
            && info.transport_unhealthy_until.is_none()
        {
            info.transport_unhealthy_until = Some(now + self.transport_cooldown);
            warn!(
                endpoint = ep.name,
                consecutive_failures = info.consecutive_transport_failures,
                cooldown_secs = self.transport_cooldown.as_secs(),
                "transport circuit-breaker OPEN: endpoint leaves the routing pool"
            );
        }
    }

    /// Clear transport-failure state after a successful forward (any HTTP
    /// response proves the transport path is alive — even a 429 or 5xx).
    pub(crate) async fn record_transport_success(&self, endpoint_idx: usize) {
        let ep = &self.endpoints[endpoint_idx];
        // Fast path: requests are overwhelmingly healthy-on-healthy; skip the
        // write lock unless there is actually state to clear.
        {
            let info = ep.rate_info.read().await;
            if info.consecutive_transport_failures == 0 && info.transport_unhealthy_until.is_none()
            {
                return;
            }
        }
        let mut info = ep.rate_info.write().await;
        if info.transport_unhealthy_until.is_some() {
            info!(
                endpoint = ep.name,
                "transport circuit-breaker CLOSED: endpoint recovered"
            );
        }
        info.consecutive_transport_failures = 0;
        info.transport_unhealthy_until = None;
    }

    /// Sync shared state from Redis: hard limits + rate info.
    /// Called periodically by background task.
    pub(crate) async fn sync_from_redis(&self) {
        let redis = match self.coordination_redis() {
            Some(r) => r,
            None => return,
        };
        let now_epoch = Self::now_epoch();
        let now_instant = Instant::now();

        // Sync target list over the endpoint pool. OpenAI endpoints are
        // skipped: they carry no Anthropic rate-limit data.
        struct SyncTarget<'a> {
            name: &'a str,
            rate_info: &'a RwLock<RateLimitInfo>,
            weight: &'a AtomicU64,
            share: &'a AtomicU64,
            gate: &'a AtomicU64,
        }
        let mut targets: Vec<SyncTarget<'_>> = Vec::new();
        for e in &self.endpoints {
            if e.protocol == Protocol::OpenAI {
                continue;
            }
            targets.push(SyncTarget {
                name: &e.name,
                rate_info: &e.rate_info,
                weight: &e.last_routing_weight,
                share: &e.last_routing_share,
                gate: &e.last_effective_gate,
            });
        }

        // 1. Sync hard limits (MGET for all targets in one round-trip)
        let hard_keys: Vec<String> = targets
            .iter()
            .map(|t| format!("alb:hard:{}", t.name))
            .collect();

        if let Ok(values) = redis.mget::<Vec<Option<String>>, _>(hard_keys).await {
            for (i, remote) in values.into_iter().enumerate() {
                let remote = remote.and_then(|value| value.parse::<u64>().ok());
                match classify_hard_limit_sync(remote, now_epoch, now_instant) {
                    HardLimitSync::Clear => {
                        // Another replica observed recovery. Clear our local
                        // `hard_limited_until` so pick_account stops excluding
                        // the account. Do NOT reset `consecutive_burst_429s` —
                        // that counter tracks THIS replica's burst-429 backoff
                        // escalation, and resetting it based on another replica's
                        // unrelated success would mask abuse patterns and thrash
                        // the exponential backoff.
                        let mut info = targets[i].rate_info.write().await;
                        if info.hard_limited_until.is_some() {
                            info.hard_limited_until = None;
                            trace!(
                                endpoint = targets[i].name,
                                "synced hard-limit clear sentinel from redis"
                            );
                        }
                    }
                    HardLimitSync::Update(until_instant) => {
                        let mut info = targets[i].rate_info.write().await;
                        let should_update = info
                            .hard_limited_until
                            .is_none_or(|local| until_instant > local);
                        if should_update {
                            info.hard_limited_until = Some(until_instant);
                            trace!(endpoint = targets[i].name, "synced hard-limit from redis");
                        }
                    }
                    HardLimitSync::Ignore => {}
                }
            }
        }

        // 2. Sync rate info (MGET for all targets in one round-trip)
        let rate_keys: Vec<String> = targets
            .iter()
            .map(|t| format!("alb:rate:{}", t.name))
            .collect();

        if let Ok(values) = redis.mget::<Vec<Option<String>>, _>(rate_keys).await {
            for (i, val) in values.iter().enumerate() {
                if let Some(json) = val {
                    if let Ok(remote) = serde_json::from_str::<RedisRateInfo>(json) {
                        let mut info = targets[i].rate_info.write().await;
                        // "Most recent wins": only apply remote data if it's newer
                        // Both ages use wall-clock epoch to avoid mixed-clock-domain bugs
                        let local_age = info
                            .last_updated_epoch
                            .map(|epoch| now_epoch.saturating_sub(epoch))
                            .unwrap_or(u64::MAX);
                        let remote_age = now_epoch.saturating_sub(remote.updated_at);
                        if remote_age < local_age {
                            info.utilization = remote.utilization;
                            info.utilization_5h = remote.utilization_5h;
                            info.utilization_7d = remote.utilization_7d;
                            info.reset_5h = remote.reset_5h;
                            info.reset_7d = remote.reset_7d;
                            info.status_5h = remote.status_5h;
                            info.status_7d = remote.status_7d;
                            info.claims_7d = bound_ingested_claims(remote.claims_7d);
                            // Truncated to match the keys `bound_ingested_claims`
                            // just wrote — see the same pairing in `load_state`.
                            info.representative_claim =
                                remote.representative_claim.as_deref().map(truncate_label);
                            info.remaining_requests = remote.remaining_requests;
                            info.remaining_tokens = remote.remaining_tokens;
                            info.limit_requests = remote.limit_requests;
                            info.limit_tokens = remote.limit_tokens;
                            info.overage_in_use = remote.overage_in_use;
                            info.overage_status = remote.overage_status;
                            info.overage_utilization = remote.overage_utilization;
                            info.overage_reset = remote.overage_reset;
                            info.last_updated = Some(now_instant);
                            info.last_updated_epoch = Some(remote.updated_at);
                            trace!(
                                endpoint = targets[i].name,
                                remote_age,
                                "synced rate info from redis"
                            );
                        }
                    }
                }
            }
        }

        // 3. Sync precomputed routing weights (published by probing pod)
        let weight_keys: Vec<String> = targets
            .iter()
            .map(|t| format!("alb:weight:{}", t.name))
            .collect();
        if let Ok(values) = redis.mget::<Vec<Option<String>>, _>(weight_keys).await {
            for (i, val) in values.iter().enumerate() {
                if let Some(csv) = val {
                    let mut parts = csv.splitn(3, ',');
                    if let (Some(w_str), Some(s_str)) = (parts.next(), parts.next()) {
                        if let (Ok(w), Ok(s)) = (w_str.parse::<f64>(), s_str.parse::<f64>()) {
                            targets[i].weight.store(w.to_bits(), Ordering::Relaxed);
                            targets[i].share.store(s.to_bits(), Ordering::Relaxed);
                            // Gate is optional (backward compat with older publishers)
                            if let Some(Ok(g)) = parts.next().map(|g| g.parse::<f64>()) {
                                targets[i].gate.store(g.to_bits(), Ordering::Relaxed);
                            }
                        }
                    }
                }
            }
        }

        // 4. Flush this replica's transport-error deltas into the shared Redis
        //    hash so the fleet-wide by-kind count is visible cluster-wide.
        //    Drains the local accumulator; the cluster_info refresh below then
        //    reads the freshly-flushed total back via HGETALL.
        self.flush_transport_errors().await;

        // 5. Refresh cluster info cache for /_stats endpoint
        let info = self.cluster_info().await;
        *self.lock_cluster_info_cache() = info;
    }

    /// Lock the transport-error accumulator via `lock_recovering`. Every
    /// `upstream_transport_errors` site goes through here.
    pub(crate) fn lock_transport_errors(
        &self,
    ) -> std::sync::MutexGuard<'_, HashMap<&'static str, u64>> {
        lock_recovering(&self.upstream_transport_errors, "upstream_transport_errors")
    }

    /// Lock the per-client budget accumulator via `lock_recovering`. Every
    /// `budget_usage` site MUST go through here: a bare `if let Ok` would turn
    /// one panicked holder into permanently disabled budgets (fail-open).
    pub(crate) fn lock_budget_usage(
        &self,
    ) -> std::sync::MutexGuard<'_, HashMap<String, (u64, u64)>> {
        lock_recovering(&self.budget_usage, "budget_usage")
    }

    // One `lock_recovering` accessor per multi-site map, so a new site cannot
    // reach for a bare `.lock()` and skip on poison for the process lifetime.

    pub(crate) fn lock_client_rejections(
        &self,
    ) -> std::sync::MutexGuard<'_, HashMap<(String, &'static str), u64>> {
        lock_recovering(&self.client_rejections, "client_rejections")
    }

    pub(crate) fn lock_client_usage(&self) -> std::sync::MutexGuard<'_, HashMap<String, [u64; 4]>> {
        lock_recovering(&self.client_usage, "client_usage")
    }

    pub(crate) fn lock_client_model_usage(
        &self,
    ) -> std::sync::MutexGuard<'_, HashMap<(String, String), [u64; 4]>> {
        lock_recovering(&self.client_model_usage, "client_model_usage")
    }

    pub(crate) fn lock_client_request_rates(
        &self,
    ) -> std::sync::MutexGuard<'_, HashMap<String, (u64, Ewma)>> {
        lock_recovering(&self.client_request_rates, "client_request_rates")
    }

    pub(crate) fn lock_unsupported_models(
        &self,
    ) -> std::sync::MutexGuard<'_, HashMap<(usize, String), Instant>> {
        lock_recovering(&self.unsupported_models, "unsupported_models")
    }

    pub(crate) fn lock_sessions(&self) -> std::sync::MutexGuard<'_, HashMap<String, SessionEntry>> {
        lock_recovering(&self.sessions, "sessions")
    }

    pub(crate) fn lock_cluster_info_cache(
        &self,
    ) -> std::sync::MutexGuard<'_, Option<serde_json::Value>> {
        lock_recovering(&self.cluster_info_cache, "cluster_info_cache")
    }

    /// Log + count client `anthropic-beta` flags dropped by the allow-list
    /// (AC-12: silent stripping is not acceptable — a caller whose feature
    /// vanished must be diagnosable from the logs and
    /// `anthropic_beta_flag_dropped_total{flag}`). Flag names are
    /// client-controlled, so the counter map is capped at
    /// `MAX_DROPPED_BETA_FLAGS` distinct flags (overflow counts as `_other`)
    /// and each key is truncated to `MAX_DROPPED_BETA_FLAG_LEN` bytes —
    /// count-capped but length-unbounded keys would still bloat every
    /// `/metrics` scrape.
    pub(crate) fn record_dropped_beta_flags(&self, client_id: &str, dropped: &[String]) {
        if dropped.is_empty() {
            return;
        }
        let truncated: Vec<&str> = dropped
            .iter()
            .map(|f| {
                let mut end = f.len().min(MAX_DROPPED_BETA_FLAG_LEN);
                while !f.is_char_boundary(end) {
                    end -= 1;
                }
                &f[..end]
            })
            .collect();
        let mut map = lock_recovering(&self.beta_flags_dropped, "beta_flags_dropped");
        // Loud line only on a flag's FIRST sighting — a misconfigured client
        // sends the same unlisted flag at request rate, and the counter
        // already carries the volume. Repeats log at debug for correlation.
        let mut first_seen: Vec<&str> = Vec::new();
        for flag in &truncated {
            if map.contains_key(*flag) || map.len() < MAX_DROPPED_BETA_FLAGS {
                if !map.contains_key(*flag) {
                    first_seen.push(flag);
                }
                *map.entry(flag.to_string()).or_insert(0) += 1;
            } else {
                *map.entry("_other".to_string()).or_insert(0) += 1;
            }
        }
        drop(map);
        if !first_seen.is_empty() {
            warn!(
                client_id,
                flags = %first_seen.join(","),
                "dropped client anthropic-beta flags not on the allow-list \
                 (a configured allowed_client_betas REPLACES the default list — \
                 include the defaults plus the flags to permit)"
            );
        }
        // The repeat subset gets its own debug line even when the same call
        // also carried a first sighting — every dropped flag leaves a log
        // trace on every request it was dropped from (AC-12).
        let repeats: Vec<&str> = truncated
            .iter()
            .filter(|f| !first_seen.contains(f))
            .copied()
            .collect();
        if !repeats.is_empty() {
            debug!(
                client_id,
                flags = %repeats.join(","),
                "dropped client anthropic-beta flags (previously reported)"
            );
        }
    }

    /// Count + log top-level body fields stripped to keep a request coherent
    /// with its filtered `anthropic-beta` header (LAB-1261).
    ///
    /// Same cardinality discipline as `record_dropped_beta_flags`, plus two
    /// guards that sibling does not need. Its keys are HEADER tokens, which
    /// hyper guarantees are free of CR/LF and are bounded in number; these are
    /// JSON object keys from a request body, so they carry arbitrary bytes and
    /// arbitrary count:
    ///
    /// - **Sanitized before they reach the map or a log field.** A key
    ///   containing a newline would otherwise forge whole lines into the
    ///   plain-text log stream.
    /// - **Capped per request.** Without it, one request carrying 50 junk
    ///   top-level keys and one junk beta flag permanently fills all
    ///   `MAX_DROPPED_BETA_FLAGS` slots, after which every genuine paired-beta
    ///   strip lands in `_other` and the first-sighting warn never fires again
    ///   — killing the one alertable signal this whole mechanism adds.
    pub(crate) fn record_stripped_body_fields(
        &self,
        client_id: &str,
        stripped: &[String],
        dropped: &[String],
    ) {
        if stripped.is_empty() {
            return;
        }
        let keys: Vec<String> = stripped
            .iter()
            .take(MAX_STRIPPED_FIELDS_PER_REQUEST)
            .map(|f| sanitize_metric_key(f, MAX_DROPPED_BETA_FLAG_LEN))
            .collect();
        // Removals past the cap still happened, so they are still counted —
        // under `_other`, not discarded. Dropping them outright let an ordered
        // payload hide the actionable field behind eight junk ones and leave
        // no trace that anything else went (Helly R finding 3).
        let over_cap = stripped.len().saturating_sub(keys.len()) as u64;
        let mut map = lock_recovering(&self.beta_body_fields_stripped, "beta_body_fields_stripped");
        if over_cap > 0 {
            *map.entry("_other".to_string()).or_insert(0) += over_cap;
        }
        let mut first_seen: Vec<&str> = Vec::new();
        for field in &keys {
            if map.contains_key(field.as_str()) || map.len() < MAX_DROPPED_BETA_FLAGS {
                if !map.contains_key(field.as_str()) {
                    first_seen.push(field.as_str());
                }
                *map.entry(field.clone()).or_insert(0) += 1;
            } else {
                *map.entry("_other".to_string()).or_insert(0) += 1;
            }
        }
        drop(map);
        if !first_seen.is_empty() {
            // `dropped` is sanitized here too: it is logged raw by the sibling
            // only because a header token cannot carry a newline.
            let context: Vec<String> = dropped
                .iter()
                .take(MAX_STRIPPED_FIELDS_PER_REQUEST)
                .map(|f| sanitize_metric_key(f, MAX_DROPPED_BETA_FLAG_LEN))
                .collect();
            warn!(
                client_id,
                fields = %first_seen.join(","),
                dropped_flags = %context.join(","),
                "stripped top-level body fields orphaned by the anthropic-beta \
                 allow-list — a PAIRED beta family is in live traffic that the \
                 allow-list does not carry; the feature is now off for this \
                 client instead of 400ing. To restore it the family needs BOTH \
                 an allow-list entry AND a row in BETA_BODY_FIELDS naming this \
                 field — the allow-list alone only stops the drop; the row is \
                 what protects the body half"
            );
        }
        // Repeats get their own line even when this call also carried a first
        // sighting, so every strip leaves a trace on every request — same
        // contract as `record_dropped_beta_flags` (AC-12).
        let repeats: Vec<&str> = keys
            .iter()
            .map(String::as_str)
            .filter(|f| !first_seen.contains(f))
            .collect();
        if !repeats.is_empty() {
            debug!(
                client_id,
                fields = %repeats.join(","),
                "stripped orphaned anthropic-beta body fields (previously reported)"
            );
        }
    }

    /// Flush this replica's accumulated transport-error deltas into the shared
    /// Redis hash (`TRANSPORT_ERRORS_KEY`) so the fleet-wide count is visible
    /// cluster-wide. `upstream_transport_errors` is a DELTA accumulator: it is
    /// drained here each tick and its counts folded into Redis via `HINCRBY`,
    /// so the same delta is never pushed twice (no double-counting across
    /// ticks). On Redis failure the drained deltas are returned to the local
    /// map — they retry next tick and stay visible via the local metrics
    /// fallback rather than being lost. No-op without Redis, so single-instance
    /// deployments keep accumulating locally; on an idle tick the TTL is still
    /// refreshed so the fleet-wide hash never expires under healthy traffic.
    pub(crate) async fn flush_transport_errors(&self) {
        let redis = match self.coordination_redis() {
            Some(r) => r,
            None => return,
        };

        // Drain the accumulator atomically: take the deltas AND reset to empty
        // under one lock, so any increment arriving mid-flush belongs to the
        // NEXT tick's delta and cannot be double-flushed.
        let deltas: Vec<(&'static str, u64)> = {
            let mut m = self.lock_transport_errors();
            m.drain().filter(|(_, n)| *n > 0).collect()
        };
        if deltas.is_empty() {
            // No new errors this tick — still refresh the TTL. The hash must
            // only expire once the whole fleet has been down for the TTL
            // window; skipping this would wipe the fleet-wide counter after
            // 48h of perfectly healthy, error-free traffic. EXPIRE on a
            // not-yet-existing key is a no-op, so this is safe pre-first-error.
            let result: Result<(), fred::error::RedisError> = redis
                .expire(TRANSPORT_ERRORS_KEY, TRANSPORT_ERRORS_TTL_SECS as i64)
                .await;
            if let Err(e) = result {
                warn!(error = %e, "redis EXPIRE failed for transport-errors TTL refresh");
            }
            return;
        }

        // One round-trip: HINCRBY every kind, then refresh the TTL. fred
        // pipeline commands resolve immediately when queued; `all()` sends the
        // batch and surfaces the first error, matching the old atomic
        // success-or-requeue contract.
        let pipe = redis.pipeline();
        for (kind, n) in &deltas {
            let _: Result<(), fred::error::RedisError> =
                pipe.hincrby(TRANSPORT_ERRORS_KEY, *kind, *n as i64).await;
        }
        let _: Result<(), fred::error::RedisError> = pipe
            .expire(TRANSPORT_ERRORS_KEY, TRANSPORT_ERRORS_TTL_SECS as i64)
            .await;

        let result: Result<Vec<RedisValue>, fred::error::RedisError> = pipe.all().await;
        if let Err(e) = result {
            // Redis is unreachable (or the pipeline reply was lost) — return the
            // drained deltas to the local accumulator so error signal is not
            // dropped. This is at-least-once: if the connection died AFTER Redis
            // applied some HINCRBYs, re-queuing can over-count by a few next
            // tick. For an error *counter* that bias is correct — a slight
            // over-report beats a silently missed egress fault. (Contrast
            // record_budget_usage, which never retries a failed INCRBY:
            // over-counting a budget would wrongly throttle a client, so the
            // lost increment is covered by the local floor instead —
            // LAB-1962.) The deltas also stay
            // visible via the local metrics fallback until Redis heals.
            warn!(error = %e, "redis HINCRBY failed for transport errors; re-queuing deltas locally");
            // Poison-recovering lock: an `if let Ok` here would silently DROP
            // every drained delta if the mutex got poisoned mid-cycle.
            let mut m = self.lock_transport_errors();
            for (kind, n) in deltas {
                *m.entry(kind).or_insert(0) += n;
            }
        }
    }
    pub(crate) async fn cluster_info(&self) -> Option<serde_json::Value> {
        let redis = self.coordination_redis()?;
        let mut redis_ok = true;

        // Count active replicas via SCAN (non-blocking, unlike KEYS). The
        // cursor is driven manually as a plain command per page — NOT via
        // fred's scan stream. The stream's pages arrive over a channel that
        // `default_command_timeout` does not cover, and abandoning the stream
        // does not stop the scan: `ScanResult`'s Drop impl auto-requests the
        // next page with nobody listening, so every abandoned scan keeps
        // walking the keyspace against a backend that is already slow. A
        // manual cursor loop keeps each page under the ordinary 2s command
        // budget and genuinely ends the scan when this function returns.
        //
        // The count is the *cardinality* of the matched keys, not the sum of
        // page lengths: SCAN guarantees only that a key present for the whole
        // iteration comes back at least once, and is explicitly allowed to
        // return it on more than one page, so summing was wrong by
        // construction. Latent rather than observed (LAB-4554): a scan of the
        // live store returned one page of distinct, independently-refreshed
        // ids, so the reported count was right. It stops being one page as
        // soon as the keyspace outgrows `COUNT`.
        let mut heartbeats: std::collections::HashSet<String> = std::collections::HashSet::new();
        let mut cursor = String::from("0");
        loop {
            let result: Result<(String, Vec<String>), fred::error::RedisError> = redis
                .custom(
                    fred::types::CustomCommand::new_static(
                        "SCAN",
                        fred::types::ClusterHash::FirstKey,
                        false,
                    ),
                    vec![
                        cursor,
                        "MATCH".to_string(),
                        "alb:heartbeat:*".to_string(),
                        "COUNT".to_string(),
                        "100".to_string(),
                    ],
                )
                .await;
            match result {
                Ok((next_cursor, keys)) => {
                    heartbeats.extend(keys);
                    cursor = next_cursor;
                    if cursor == "0" {
                        break;
                    }
                }
                Err(e) => {
                    warn!(error = %e, "redis SCAN failed in cluster_info");
                    redis_ok = false;
                    break;
                }
            }
        }
        let replicas = heartbeats.len() as u64;

        // Aggregate budget usage from Redis (batch MGET). The same fetch
        // re-seeds this replica's local `budget_usage` mirror (LAB-3217) —
        // see fold_budget_mirror for why it must receive THIS `today`.
        let mut redis_budgets = serde_json::Map::new();
        if redis_ok && !self.client_budgets.is_empty() {
            let today = Self::now_epoch() / 86400;
            let client_ids: Vec<&String> = self.client_budgets.keys().collect();
            let budget_keys: Vec<String> = client_ids
                .iter()
                .map(|id| format!("alb:budget:{id}:{today}"))
                .collect();
            match redis.mget::<Vec<Option<u64>>, _>(budget_keys).await {
                Ok(values) => {
                    let used_by_client: Vec<(&str, u64)> = client_ids
                        .iter()
                        .enumerate()
                        .map(|(i, id)| (id.as_str(), values.get(i).copied().flatten().unwrap_or(0)))
                        .collect();
                    for &(client_id, used) in &used_by_client {
                        let limit = self.client_budgets[client_id];
                        redis_budgets.insert(
                            client_id.to_owned(),
                            serde_json::json!({ "limit": limit, "used": used }),
                        );
                    }
                    self.fold_budget_mirror(today, used_by_client);
                }
                Err(e) => {
                    warn!(error = %e, "redis MGET failed for budget aggregation");
                    redis_ok = false;
                }
            }
        }

        // Aggregate upstream transport errors from the shared Redis hash so the
        // fleet-wide by-kind count surfaces on the dashboard/metrics endpoint.
        // Only included when the HGETALL succeeds: its absence tells the metrics
        // handler to fall back to this replica's local accumulator.
        let mut transport_errors: Option<serde_json::Map<String, serde_json::Value>> = None;
        if redis_ok {
            match redis
                .hgetall::<HashMap<String, u64>, _>(TRANSPORT_ERRORS_KEY)
                .await
            {
                Ok(map) => {
                    let mut te = serde_json::Map::new();
                    for (kind, n) in map {
                        te.insert(kind, serde_json::json!(n));
                    }
                    transport_errors = Some(te);
                }
                Err(e) => {
                    warn!(error = %e, "redis HGETALL failed for transport-error aggregation");
                    redis_ok = false;
                }
            }
        }

        let mut out = serde_json::Map::new();
        out.insert("redis_connected".into(), serde_json::json!(redis_ok));
        out.insert("replicas_seen".into(), serde_json::json!(replicas));
        out.insert(
            "budget_usage".into(),
            serde_json::Value::Object(redis_budgets),
        );
        if let Some(te) = transport_errors {
            out.insert("transport_errors".into(), serde_json::Value::Object(te));
        }
        Some(serde_json::Value::Object(out))
    }
}

#[cfg(test)]
mod tests;
