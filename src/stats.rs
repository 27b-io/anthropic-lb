use crate::*;

// ── Stats endpoint ──────────────────────────────────────────────────

/// Build one `/stats` JSON entry from an endpoint's (name, priority,
/// rate_info, burn_rate, counters) fields. When `protocol` is `Some`, a
/// `"protocol"` field is added to the entry.
#[allow(clippy::too_many_arguments)]
async fn build_stats_entry(
    name: &str,
    passthrough: bool,
    priority: u32,
    protocol: Option<&str>,
    rate_info: &RwLock<RateLimitInfo>,
    burn_rate: &Mutex<BurnRate>,
    requests: &AtomicU64,
    token_counters: [&AtomicU64; 4],
    now_epoch: u64,
    total_headroom: &mut Option<u64>,
) -> serde_json::Value {
    let info = rate_info.read().await;
    let hard_limited = match info.hard_limited_until {
        Some(until) if Instant::now() < until => {
            Some(until.duration_since(Instant::now()).as_secs())
        }
        _ => None,
    };

    // Burn rate from EWMA tracker
    let (br_5m, br_1h, br_6h) = {
        let br = lock_burn_rate(burn_rate);
        (br.rate_5m.value, br.rate_1h.value, br.rate_6h.value)
    };

    // Headroom: prefer remaining_requests header, else (1-util)*limit, else null
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

    // Projected throttle time
    let (eff_util, _, _, _) = effective_utilization(&info, now_epoch, "");
    let projected_throttle_at: serde_json::Value = if eff_util < 0.5 || br_1h < 0.01 {
        serde_json::Value::Null
    } else if let Some(headroom_reqs) = headroom {
        if headroom_reqs == 0 {
            // Already at limit — check if hard-limited and report cooldown expiry
            if let Some(hl_secs) = hard_limited {
                serde_json::Value::String(AppState::epoch_to_iso8601(now_epoch + hl_secs))
            } else {
                serde_json::Value::String(AppState::epoch_to_iso8601(now_epoch))
            }
        } else {
            let minutes_remaining = headroom_reqs as f64 / br_1h;
            let secs_remaining = (minutes_remaining * 60.0) as u64;
            let projected_epoch = now_epoch + secs_remaining;
            // If projection is beyond next reset, account will recover → null
            let mut next_reset = info.reset_5h.unwrap_or(u64::MAX);
            for c in info.claims_7d.values() {
                if let Some(r) = c.reset {
                    next_reset = next_reset.min(r);
                }
            }
            if projected_epoch > next_reset && next_reset != u64::MAX {
                serde_json::Value::Null
            } else {
                serde_json::Value::String(AppState::epoch_to_iso8601(projected_epoch))
            }
        }
    } else {
        serde_json::Value::Null
    };

    let mut entry = serde_json::json!({
        "name": name,
        "passthrough": passthrough,
        "priority": priority,
        "requests_total": requests.load(Ordering::Relaxed),
        "utilization": info.utilization,
        "utilization_7d": info.utilization_7d,
        "utilization_5h": info.utilization_5h,
        "representative_claim": info.representative_claim,
        "reset_5h": info.reset_5h,
        "reset_7d": info.reset_7d,
        "status_5h": info.status_5h,
        "status_7d": info.status_7d,
        "overage_in_use": info.overage_in_use,
        "overage_status": info.overage_status,
        "overage_utilization": info.overage_utilization,
        "overage_reset": info.overage_reset,
        "claims_7d": info.claims_7d.iter().map(|(k, v)| {
            (k.clone(), serde_json::json!({
                "utilization": v.utilization,
                "reset": v.reset,
                "status": v.status,
                "waste_risk": waste_risk(v.utilization, v.reset, now_epoch),
            }))
        }).collect::<serde_json::Map<String, serde_json::Value>>(),
        "remaining_requests": info.remaining_requests,
        "remaining_tokens": info.remaining_tokens,
        "limit_requests": info.limit_requests,
        "limit_tokens": info.limit_tokens,
        "hard_limited_remaining_secs": hard_limited,
        "burn_rate": {
            "last_5m": (br_5m * 100.0).round() / 100.0,
            "last_1h": (br_1h * 100.0).round() / 100.0,
            "last_6h": (br_6h * 100.0).round() / 100.0,
        },
        "headroom_requests": headroom,
        "projected_throttle_at": projected_throttle_at,
        "token_usage": {
            "input_tokens": token_counters[0].load(Ordering::Relaxed),
            "output_tokens": token_counters[1].load(Ordering::Relaxed),
            "cache_creation_input_tokens": token_counters[2].load(Ordering::Relaxed),
            "cache_read_input_tokens": token_counters[3].load(Ordering::Relaxed),
        },
    });
    if let Some(p) = protocol {
        entry["protocol"] = serde_json::json!(p);
    }
    entry
}

pub(crate) async fn stats_handler(
    State(state): State<Arc<AppState>>,
    axum::extract::ConnectInfo(client_addr): axum::extract::ConnectInfo<SocketAddr>,
    req: Request<Body>,
) -> Response {
    // AC-8: the ONLY client_addr.ip() read in this handler.
    let client_ip = state.resolve_client_ip(client_addr.ip(), req.headers());
    if !state.is_ip_allowed(&client_ip) {
        return (StatusCode::FORBIDDEN, "forbidden").into_response();
    }
    // AC-4: operator principal required — /_stats discloses other clients'
    // ids, the endpoint account names and pool utilisation.
    if let Some(resp) = state.authorize_admin(&client_ip, client_addr, req.headers(), "stats") {
        return *resp;
    }

    let now_epoch = AppState::now_epoch();
    let mut total_headroom: Option<u64> = Some(0);
    let mut endpoint_stats = Vec::new();
    for ep in &state.endpoints {
        let protocol = match ep.protocol {
            Protocol::Anthropic => "anthropic",
            Protocol::OpenAI => "openai",
        };
        endpoint_stats.push(
            build_stats_entry(
                &ep.name,
                ep.passthrough,
                ep.priority,
                Some(protocol),
                &ep.rate_info,
                &ep.burn_rate,
                &ep.requests,
                [
                    &ep.input_tokens,
                    &ep.output_tokens,
                    &ep.cache_creation_tokens,
                    &ep.cache_read_tokens,
                ],
                now_epoch,
                &mut total_headroom,
            )
            .await,
        );
    }

    // Per-client usage (tokens + request rates)
    let request_rates = state.lock_client_request_rates();
    let client_usage: serde_json::Value = {
        let map = state.lock_client_usage();
        // Collect all client IDs from both token usage and request rates
        let mut all_clients: std::collections::HashSet<&String> = map.keys().collect();
        all_clients.extend(request_rates.keys());

        let obj: serde_json::Map<String, serde_json::Value> = all_clients
            .into_iter()
            .map(|k| {
                // Operator hiding: attribute operator data to a reserved key
                let display_key = if state.is_operator(k) {
                    "_operator".to_string()
                } else {
                    k.clone()
                };
                let tokens = map.get(k).copied().unwrap_or([0; 4]);
                let (req_total, req_per_min) = request_rates
                    .get(k)
                    .map(|(total, ewma)| (*total, ewma.value))
                    .unwrap_or((0, 0.0));
                (
                    display_key,
                    serde_json::json!({
                        "input_tokens": tokens[0],
                        "output_tokens": tokens[1],
                        "cache_creation_input_tokens": tokens[2],
                        "cache_read_input_tokens": tokens[3],
                        "requests_total": req_total,
                        "requests_per_minute": (req_per_min * 100.0).round() / 100.0,
                    }),
                )
            })
            .collect();
        serde_json::Value::Object(obj)
    };

    // Aggregate: total headroom + per-consumer share
    let aggregate = {
        let mut consumers = serde_json::Map::new();
        let mut total_rpm = 0.0_f64;
        for (client, (_, ewma)) in request_rates.iter() {
            let display_key = if state.is_operator(client) {
                "_operator".to_string()
            } else {
                client.clone()
            };
            total_rpm += ewma.value;
            let entry = consumers
                .entry(display_key)
                .or_insert_with(|| serde_json::json!({"requests_per_minute": 0.0, "share": 0.0}));
            if let Some(obj) = entry.as_object_mut() {
                let cur = obj
                    .get("requests_per_minute")
                    .and_then(|v| v.as_f64())
                    .unwrap_or(0.0);
                obj.insert(
                    "requests_per_minute".to_string(),
                    serde_json::json!(cur + ewma.value),
                );
            }
        }
        // Compute shares
        if total_rpm > 0.0 {
            for (_client, val) in consumers.iter_mut() {
                if let Some(obj) = val.as_object_mut() {
                    let rpm = obj
                        .get("requests_per_minute")
                        .and_then(|v| v.as_f64())
                        .unwrap_or(0.0);
                    obj.insert(
                        "requests_per_minute".to_string(),
                        serde_json::json!((rpm * 100.0).round() / 100.0),
                    );
                    obj.insert(
                        "share".to_string(),
                        serde_json::json!(((rpm / total_rpm) * 1000.0).round() / 1000.0),
                    );
                }
            }
        }
        serde_json::json!({
            "total_headroom_requests": total_headroom,
            "consumers": serde_json::Value::Object(consumers),
        })
    };

    // Per-client budget status
    let budgets: serde_json::Value = if state.client_budgets.is_empty() {
        serde_json::json!(null)
    } else {
        let today = AppState::now_epoch() / 86400;
        let usage_map = state.lock_budget_usage();
        let obj: serde_json::Map<String, serde_json::Value> = state
            .client_budgets
            .iter()
            .map(|(client, &limit)| {
                let used = usage_map
                    .get(client)
                    .filter(|(day, _)| *day == today)
                    .map(|(_, used)| *used)
                    .unwrap_or(0);
                (
                    client.clone(),
                    serde_json::json!({
                        "daily_limit": limit,
                        "used_today": used,
                        "remaining": limit.saturating_sub(used),
                    }),
                )
            })
            .collect();
        serde_json::Value::Object(obj)
    };

    // Cluster info (when Redis is available)
    // Read from cache (updated by background sync task) to avoid .await in handler
    let cluster: Option<serde_json::Value> = state.lock_cluster_info_cache().clone();

    let mut response = serde_json::json!({
        "endpoints": endpoint_stats,
        "client_usage": client_usage,
        "client_budgets": budgets,
        "aggregate": aggregate,
        "strategy": state.routing_strategy.as_str(),
        // Live sessions by context-window occupancy, hottest first (LAB-916).
        // Session labels are hashes of the affinity key; raw IPs/session ids
        // never leave the process.
        "sessions": state.sessions_snapshot(now_epoch),
    });
    if let Some(cluster_info) = cluster {
        response["cluster"] = cluster_info;
    }

    axum::Json(response).into_response()
}

#[cfg(test)]
mod tests;
