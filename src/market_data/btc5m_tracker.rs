use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn, error};
use serde::Deserialize;
use serde_json::{json, Value};
use futures_util::{StreamExt, SinkExt};
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use anyhow::{Result, Context};

use crate::config::AppConfig;
use crate::events::bus::{EventBus, MarketEvent, FeedSource, ConnectionStatus, MarketCycleEvent, CycleEventType};
use crate::market_data::state::{apply_price_update, MarketState, Btc5mMarket, MarketPhase};
use crate::market_data::price_validator::{PriceSource, PriceUpdate, PriceRejectReason};
use crate::market_data::clock_sync::PolymarketClock;
use crate::market_data::normalizer::{now_millis, is_ws_control_message, normalize_polymarket_ws_msg};

// Gamma API Structs
#[derive(Debug, Deserialize, Clone)]
pub struct GammaMarket {
    #[serde(rename = "conditionId")]
    pub condition_id: String,
    pub slug: String,
    pub question: String,
    pub active: bool,
    pub closed: bool,
    #[serde(rename = "clobTokenIds")]
    pub clob_token_ids: String,
    #[serde(rename = "volume24hrClob")]
    pub volume_24h: Option<serde_json::Value>,
}

#[derive(Debug, Deserialize)]
struct ClobBook {
    #[serde(default)]
    bids: Vec<ClobLevel>,
    #[serde(default)]
    asks: Vec<ClobLevel>,
}

#[derive(Debug, Deserialize)]
struct ClobLevel {
    price: String,
}

#[derive(Debug, Clone)]
struct TokenMapping {
    yes_token_id: String,
    no_token_id: String,
    yes_outcome: String,
    no_outcome: String,
}

/// Map YES/NO token IDs from Gamma `tokens[]` outcome labels (not array index order).
fn extract_token_ids(market_json: &Value, clob_token_ids_raw: &str) -> Result<TokenMapping> {
    if let Some(tokens) = market_json["tokens"].as_array() {
        let mut yes_token: Option<String> = None;
        let mut no_token: Option<String> = None;
        let mut yes_outcome = String::new();
        let mut no_outcome = String::new();

        for token in tokens {
            let outcome = token["outcome"]
                .as_str()
                .unwrap_or("")
                .to_lowercase();
            let token_id = token["token_id"]
                .as_str()
                .or_else(|| token["tokenId"].as_str())
                .ok_or_else(|| anyhow::anyhow!("missing token_id in tokens entry"))?
                .to_string();

            match outcome.as_str() {
                "yes" | "up" | "higher" | "above" => {
                    yes_token = Some(token_id);
                    yes_outcome = outcome.clone();
                }
                "no" | "down" | "lower" | "below" => {
                    no_token = Some(token_id);
                    no_outcome = outcome.clone();
                }
                other if !other.is_empty() => {
                    tracing::warn!("[token_map] unknown outcome label: {}", other);
                }
                _ => {}
            }
        }

        if let (Some(yes), Some(no)) = (yes_token, no_token) {
            return Ok(TokenMapping {
                yes_token_id: yes,
                no_token_id: no,
                yes_outcome,
                no_outcome,
            });
        }
    }

    // Fallback: clobTokenIds + outcomes arrays (order may match)
    let ids: Vec<String> = serde_json::from_str(clob_token_ids_raw)
        .context("Failed to parse clobTokenIds array string")?;
    if ids.len() < 2 {
        anyhow::bail!("Fewer than 2 token IDs in clobTokenIds");
    }

    let outcomes: Vec<String> = market_json["outcomes"]
        .as_str()
        .and_then(|s| serde_json::from_str(s).ok())
        .or_else(|| {
            market_json["outcomes"].as_array().map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(|s| s.to_string()))
                    .collect()
            })
        })
        .unwrap_or_else(|| vec!["Yes".into(), "No".into()]);

    let mut yes_id = ids[0].clone();
    let mut no_id = ids[1].clone();
    let mut yes_outcome = "yes".to_string();
    let mut no_outcome = "no".to_string();

    for (i, outcome) in outcomes.iter().enumerate() {
        let o = outcome.to_lowercase();
        if let Some(id) = ids.get(i) {
            if matches!(o.as_str(), "yes" | "up" | "higher" | "above") {
                yes_id = id.clone();
                yes_outcome = o;
            } else if matches!(o.as_str(), "no" | "down" | "lower" | "below") {
                no_id = id.clone();
                no_outcome = o;
            }
        }
    }

    Ok(TokenMapping {
        yes_token_id: yes_id,
        no_token_id: no_id,
        yes_outcome,
        no_outcome,
    })
}

fn log_token_mapping(slug: &str, mapping: &TokenMapping) {
    let yes_short = &mapping.yes_token_id[..mapping.yes_token_id.len().min(12)];
    let no_short = &mapping.no_token_id[..mapping.no_token_id.len().min(12)];
    info!(
        "[tracker] market {} token mapping: YES={}... (outcome=\"{}\") NO={}... (outcome=\"{}\")",
        slug, yes_short, mapping.yes_outcome, no_short, mapping.no_outcome
    );
}

async fn fetch_gamma_market_raw(
    http: &reqwest::Client,
    gamma_api: &str,
    slug: &str,
    attempts: u32,
    interval_secs: u64,
) -> Result<(GammaMarket, Value)> {
    let url = format!("{}/markets?slug={}", gamma_api.trim_end_matches('/'), slug);
    for i in 1..=attempts {
        match http.get(&url).send().await {
            Ok(resp) => {
                if resp.status().is_success() {
                    if let Ok(markets) = resp.json::<Vec<Value>>().await {
                        if let Some(v) = markets.into_iter().next() {
                            let gm: GammaMarket = serde_json::from_value(v.clone())
                                .context("Failed to deserialize GammaMarket")?;
                            return Ok((gm, v));
                        }
                    }
                }
            }
            Err(e) => {
                warn!("[tracker] Request error for market {}: {:?}", slug, e);
            }
        }
        info!(
            "[tracker] market {} not yet available, retrying (attempt {}/{})...",
            slug, i, attempts
        );
        tokio::time::sleep(Duration::from_secs(interval_secs)).await;
    }
    anyhow::bail!("Market not found after {} attempts: {}", attempts, slug)
}

async fn fetch_gamma_market(
    http: &reqwest::Client,
    gamma_api: &str,
    slug: &str,
    attempts: u32,
    interval_secs: u64,
) -> Result<(GammaMarket, TokenMapping)> {
    let (gm, raw) = fetch_gamma_market_raw(http, gamma_api, slug, attempts, interval_secs).await?;
    let mapping = extract_token_ids(&raw, &gm.clob_token_ids)?;
    Ok((gm, mapping))
}

async fn fetch_midpoint_price(
    http: &reqwest::Client,
    clob_api: &str,
    token_id: &str,
) -> Result<f64> {
    let url = format!(
        "{}/midpoint?token_id={}",
        clob_api.trim_end_matches('/'),
        token_id
    );
    let response = http
        .get(&url)
        .timeout(Duration::from_secs(3))
        .send()
        .await
        .context("REST midpoint fetch timeout")?;

    if !response.status().is_success() {
        anyhow::bail!("REST midpoint fetch failed: status {}", response.status());
    }

    let raw_body = response.text().await?;
    tracing::debug!("[rest_fallback] raw response: {}", raw_body);

    let body: serde_json::Value =
        serde_json::from_str(&raw_body).context("failed to parse midpoint JSON")?;
    let price_str = body["mid"]
        .as_str()
        .ok_or_else(|| anyhow::anyhow!("missing 'mid' field in response: {:?}", body))?;

    let mid = price_str
        .parse::<f64>()
        .context("failed to parse midpoint price as f64")?;

    if mid == 0.5 {
        tracing::debug!(
            "[rest] mid=0.5 exactly — market may be pre-open or illiquid (token {})",
            token_id
        );
    }

    Ok(mid)
}

async fn fetch_clob_price(
    http: &reqwest::Client,
    clob_api: &str,
    token_id: &str,
) -> Result<(f64, f64)> {
    // Prefer midpoint endpoint (faster, more reliable for fallback polling)
    match fetch_midpoint_price(http, clob_api, token_id).await {
        Ok(mid) => return Ok((mid, 0.0)),
        Err(e) => {
            tracing::debug!(error = %e, "midpoint fetch failed, falling back to book");
        }
    }

    let url = format!("{}/book?token_id={}", clob_api.trim_end_matches('/'), token_id);
    let resp = http
        .get(&url)
        .timeout(Duration::from_secs(3))
        .send()
        .await?
        .json::<ClobBook>()
        .await?;

    let best_bid = resp
        .bids
        .first()
        .and_then(|l| l.price.parse::<f64>().ok())
        .unwrap_or(0.0);
    let best_ask = resp
        .asks
        .first()
        .and_then(|l| l.price.parse::<f64>().ok())
        .unwrap_or(0.0);

    let mid = if best_bid > 0.0 && best_ask > 0.0 {
        (best_bid + best_ask) / 2.0
    } else {
        best_bid.max(best_ask)
    };
    let spread = (best_ask - best_bid).abs();
    Ok((mid, spread))
}

use std::collections::VecDeque;

async fn run_ws_for_market(
    config: Arc<AppConfig>,
    state: Arc<RwLock<MarketState>>,
    bus: EventBus,
    condition_id: String,
    yes_token_id: String,
    no_token_id: String,
    validator: Arc<tokio::sync::Mutex<crate::market_data::price_validator::PriceValidator>>,
    cancel: CancellationToken,
) -> Result<()> {
    let url = &config.feeds.polymarket_ws;
    info!(%url, "Connecting to Polymarket CLOB WS for token pricing");
    let (mut ws, _) = connect_async(url).await?;
    
    bus.publish(MarketEvent::ConnectionStatus(
        FeedSource::Polymarket,
        ConnectionStatus::Connected,
    ));

    let sub_market = json!({
        "auth": {},
        "markets": [condition_id.clone()],
        "type": "subscribe"
    })
    .to_string();

    let sub_book = json!({
        "auth": {},
        "assets_ids": [yes_token_id.clone(), no_token_id.clone()],
        "type": "subscribe"
    })
    .to_string();

    ws.send(Message::Text(sub_market)).await?;
    ws.send(Message::Text(sub_book)).await?;
    info!(
        "Subscribed to Polymarket WS: market channel (condition {}) + book channel (tokens {} / {})",
        condition_id, yes_token_id, no_token_id
    );

    let mut msg_timestamps: VecDeque<u64> = VecDeque::new();
    let mut got_first_valid_msg = false;

    loop {
        let msg_future = ws.next();
        tokio::select! {
            _ = cancel.cancelled() => {
                break;
            }
            res = tokio::time::timeout(Duration::from_secs(15), msg_future) => {
                match res {
                    Ok(Some(Ok(Message::Text(text)))) => {
                        let text = text.trim().to_string();

                        if is_ws_control_message(&text) {
                            tracing::debug!(
                                "[poly_ws] received control message {:?} (ignoring)",
                                text
                            );
                            let now_ms = now_millis();
                            let mut s = state.write().await;
                            s.latency.polymarket_last_msg_ms = now_ms;
                            continue;
                        }

                        tracing::debug!("[poly_ws] raw message received: {:?}", text);

                        let update = match normalize_polymarket_ws_msg(&text) {
                            Ok(Some(u)) => u,
                            Ok(None) => continue,
                            Err(e) => {
                                tracing::warn!(
                                    "[poly_ws] failed to parse JSON message: {} | raw (first 200 chars): {}",
                                    e,
                                    &text[..text.len().min(200)]
                                );
                                continue;
                            }
                        };

                        if !got_first_valid_msg {
                            got_first_valid_msg = true;
                            let mut s = state.write().await;
                            s.latency.fallback_mode = false;
                            info!("[poly_ws] reconnected successfully, resuming live feed");
                        }
                        
                        let now_ms = now_millis();
                        msg_timestamps.push_back(now_ms);
                        while msg_timestamps.front().map(|&t| now_ms - t > 10_000).unwrap_or(false) {
                            msg_timestamps.pop_front();
                        }
                        let polymarket_tick_rate = msg_timestamps.len() as f64 / 10.0;

                        let fallback_mode = {
                            let s = state.read().await;
                            if s.btc5m_market.is_none() {
                                continue;
                            }
                            s.latency.fallback_mode
                        };

                        let (yes_price, no_price) = {
                            let s = state.read().await;
                            let m = s.btc5m_market.as_ref().unwrap();
                            (m.yes_price, m.no_price)
                        };

                        let (new_yes, new_no) = if update.token_id == yes_token_id {
                            (update.price, 1.0 - update.price)
                        } else if update.token_id == no_token_id {
                            (1.0 - update.price, update.price)
                        } else {
                            (yes_price, no_price)
                        };

                        let price_update = PriceUpdate {
                            yes_price: new_yes,
                            no_price: new_no,
                            received_at_ms: now_ms,
                            source: PriceSource::WebSocketLive,
                            sequence_number: update.sequence,
                        };

                        let validation_res = {
                            let mut val = validator.lock().await;
                            val.validate(
                                &price_update,
                                config.data_quality.max_price_jump_pct_per_5s,
                                fallback_mode,
                            )
                        };

                        match validation_res {
                            Ok(()) => {
                                let mut s = state.write().await;
                                s.latency.polymarket_tick_rate = polymarket_tick_rate;
                                s.latency.polymarket_last_msg_ms = now_ms;
                                let mut m_to_publish = None;
                                if let Some(ref mut m) = s.btc5m_market {
                                    let old_yes = m.yes_price;
                                    if apply_price_update(m, &update.token_id, update.price, now_ms)
                                        .unwrap_or(false)
                                    {
                                        if update.spread > 0.0 {
                                            m.spread = update.spread;
                                        }
                                        m.price_source = "WebSocketLive".to_string();
                                        tracing::info!(
                                            "[price_debug] yes_price updated: {:.4} → {:.4} | source: {:?} | token_id: {}",
                                            old_yes,
                                            m.yes_price,
                                            PriceSource::WebSocketLive,
                                            update.token_id
                                        );
                                        m_to_publish = Some(m.clone());
                                    }
                                }
                                if let Some(m) = m_to_publish {
                                    s.last_updated_ms = now_ms;
                                    drop(s);
                                    bus.publish(MarketEvent::PolymarketUpdate(m));
                                }
                            }
                            Err(reject_reason) => {
                                let mut s = state.write().await;
                                match reject_reason {
                                    PriceRejectReason::StaleSnapshot => {
                                        s.latency.stale_snapshot_rejections += 1;
                                        tracing::debug!(
                                            target: "price",
                                            market_id = %condition_id,
                                            reason = ?reject_reason,
                                            "price update REJECTED (stale snapshot)"
                                        );
                                    }
                                    _ => {
                                        s.latency.price_rejections += 1;
                                        tracing::warn!(
                                            target: "price",
                                            market_id = %condition_id,
                                            reason = ?reject_reason,
                                            "price update REJECTED"
                                        );
                                        bus.publish(MarketEvent::PriceRejected {
                                            reason: format!("[price_validator] REJECTED update: {:?}", reject_reason),
                                        });
                                    }
                                }
                            }
                        }
                    }
                    Ok(Some(Ok(Message::Ping(data)))) => {
                        tracing::debug!("[poly_ws] received Ping, sending Pong");
                        let _ = ws.send(Message::Pong(data)).await;
                        let now_ms = now_millis();
                        let mut s = state.write().await;
                        s.latency.polymarket_last_msg_ms = now_ms;
                    }
                    Ok(Some(Ok(Message::Pong(_)))) => {
                        let now_ms = now_millis();
                        let mut s = state.write().await;
                        s.latency.polymarket_last_msg_ms = now_ms;
                    }
                    Ok(Some(Ok(Message::Close(frame)))) => {
                        warn!("[poly_ws] received Close frame: {:?}", frame);
                        break;
                    }
                    Ok(Some(Ok(Message::Binary(_)))) => {
                        tracing::debug!("[poly_ws] received binary message (ignoring)");
                    }
                    Ok(Some(Err(e))) => {
                        error!(error = %e, "WS stream error");
                        break;
                    }
                    Ok(None) => {
                        error!("WS stream ended unexpectedly");
                        break;
                    }
                    Err(_) => {
                        warn!("[poly_ws] no message for 15s — silent connection, forcing reconnect");
                        break;
                    }
                    _ => {}
                }
            }
        }
    }

    bus.publish(MarketEvent::ConnectionStatus(
        FeedSource::Polymarket,
        ConnectionStatus::Disconnected,
    ));
    Ok(())
}

pub async fn run_btc5m_tracker(
    config: Arc<AppConfig>,
    state: Arc<RwLock<MarketState>>,
    bus: EventBus,
    cancel: CancellationToken,
) {
    info!("Starting BTC 5M Market Discovery Tracker");
    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .unwrap_or_default();

    let validator = Arc::new(tokio::sync::Mutex::new(crate::market_data::price_validator::PriceValidator::new()));
    let mut clock = PolymarketClock::new(config.btc5m.clock_sync_interval_secs);
    
    // Initial clock sync
    if let Err(e) = clock.sync(&http, &config.btc5m.clob_api).await {
        error!(error = %e, "Initial clock sync failed");
    } else {
        let offset_ms = (clock.server_time_offset_secs * 1000.0) as i64;
        let mut s = state.write().await;
        s.latency.server_time_offset_ms = offset_ms;
    }

    let mut current_window_ts = clock.current_window_ts();
    let mut active_market: Option<Btc5mMarket> = None;
    let mut ws_cancel: Option<CancellationToken> = None;
    let mut final_window_entered_emitted = false;
    let mut last_rest_fetch = Instant::now();
    let mut critical_alert_sent = false;

    loop {
        if cancel.is_cancelled() {
            break;
        }

        // Periodically sync clock
        if clock.needs_sync() {
            if let Err(e) = clock.sync(&http, &config.btc5m.clob_api).await {
                error!(error = %e, "Clock sync failed");
            } else {
                let offset_ms = (clock.server_time_offset_secs * 1000.0) as i64;
                let mut s = state.write().await;
                s.latency.server_time_offset_ms = offset_ms;
            }
        }

        let secs_left = clock.secs_remaining();
        let server_now = clock.now();
        let is_expired = active_market.as_ref()
            .map(|m| clock.current_window_ts() != m.window_start_unix)
            .unwrap_or(false);

        if is_expired || active_market.is_none() {
            // Settle existing active market
            if let Some(mut m) = active_market.take() {
                m.phase = MarketPhase::Settled;
                info!(slug = %m.slug, "Market settled");
                bus.publish(MarketEvent::PolymarketUpdate(m.clone()));
                bus.publish(MarketEvent::MarketCycleEvent(MarketCycleEvent {
                    event_type: CycleEventType::MarketSettled,
                    market: m,
                    server_time_unix: server_now,
                }));
            }

            // Cancel active pricing WebSocket
            if let Some(token) = ws_cancel.take() {
                token.cancel();
            }

            // Resolve new window_ts
            current_window_ts = clock.current_window_ts();
            let slug = format!("btc-updown-5m-{}", current_window_ts);
            
            // Check if next market has been pre-fetched
            let pre_fetched = {
                let mut s = state.write().await;
                s.next_market.take()
            };

            let gamma_m = match pre_fetched {
                Some(m) if m.slug == slug => {
                    info!(slug = %slug, "Found pre-fetched next market");
                    Some(m)
                }
                _ => {
                    info!(slug = %slug, "Fetching active market metadata");
                    match fetch_gamma_market(
                        &http,
                        &config.btc5m.gamma_api,
                        &slug,
                        config.btc5m.market_retry_attempts,
                        config.btc5m.market_retry_interval_secs,
                    ).await {
                        Ok((gm, mapping)) => {
                            log_token_mapping(&slug, &mapping);
                            Some(Btc5mMarket {
                                slug: gm.slug,
                                condition_id: gm.condition_id,
                                yes_token_id: mapping.yes_token_id,
                                no_token_id: mapping.no_token_id,
                                question: gm.question,
                                window_start_unix: current_window_ts,
                                window_end_unix: current_window_ts + 300,
                                phase: MarketPhase::PreOpen,
                                yes_price: 0.5,
                                no_price: 0.5,
                                spread: 0.01,
                                volume_24h: match gm.volume_24h {
                                    Some(serde_json::Value::Number(n)) => n.as_f64().unwrap_or(0.0),
                                    Some(serde_json::Value::String(s)) => s.parse().unwrap_or(0.0),
                                    _ => 0.0,
                                },
                                time_remaining_secs: secs_left as i64,
                                last_fetched_ms: now_millis(),
                                price_source: "RestSnapshot".to_string(),
                            })
                        }
                        Err(e) => {
                            error!(error = %e, slug = %slug, "Failed to resolve active market metadata");
                            None
                        }
                    }
                }
            };

            if let Some(mut m) = gamma_m {
                let yes_id = m.yes_token_id.clone();
                let no_id = m.no_token_id.clone();

                // REST pricing fetch — YES midpoint drives both sides
                let mut yes_price = 0.5;
                let mut spread = 0.01;
                let start_time = Instant::now();
                match fetch_midpoint_price(&http, &config.btc5m.clob_api, &yes_id).await {
                    Ok(p) => {
                        yes_price = p;
                    }
                    Err(e) => {
                        warn!(error = %e, "CLOB YES midpoint fetch failed, trying book");
                        if let Ok((p, s)) = fetch_clob_price(&http, &config.btc5m.clob_api, &yes_id).await {
                            yes_price = p;
                            spread = s;
                        }
                    }
                }
                let latency_ms = start_time.elapsed().as_millis() as u64;
                {
                    let mut s = state.write().await;
                    s.latency.polymarket_ms = latency_ms;
                }
                bus.publish(MarketEvent::LatencyAlert {
                    source: FeedSource::Polymarket,
                    latency_ms,
                });

                let now_ms = now_millis();
                let old_yes = m.yes_price;
                let _ = apply_price_update(&mut m, &yes_id, yes_price, now_ms);
                m.spread = spread;
                m.price_source = "RestSnapshot".to_string();
                tracing::info!(
                    "[price_debug] yes_price updated: {:.4} → {:.4} | source: {:?} | token_id: {}",
                    old_yes,
                    m.yes_price,
                    PriceSource::RestSnapshot,
                    yes_id
                );
                m.time_remaining_secs = secs_left as i64;
                
                m.phase = if secs_left <= config.btc5m.final_window_secs {
                    MarketPhase::Final
                } else {
                    MarketPhase::Active
                };

                info!(
                    slug = %m.slug,
                    yes = format_args!("{:.2}", m.yes_price),
                    no = format_args!("{:.2}", m.no_price),
                    spread = format_args!("{:.2}", m.spread),
                    phase = %m.phase,
                    yes_token = %m.yes_token_id,
                    no_token = %m.no_token_id,
                    "Market activated: {} YES: {:.2} NO: {:.2} spread: {:.2} time left: {}s phase: {:?}",
                    m.slug, m.yes_price, m.no_price, m.spread, secs_left, m.phase
                );

                // Update state and reset validator for new market
                {
                    let mut s = state.write().await;
                    s.btc5m_market = Some(m.clone());
                    s.last_updated_ms = now_millis();
                    // Reset validator for new market
                    let mut val = validator.lock().await;
                    *val = crate::market_data::price_validator::PriceValidator::new();
                }

                active_market = Some(m.clone());
                last_rest_fetch = Instant::now();
                final_window_entered_emitted = false;

                bus.publish(MarketEvent::PolymarketUpdate(m.clone()));
                bus.publish(MarketEvent::MarketCycleEvent(MarketCycleEvent {
                    event_type: CycleEventType::MarketActivated,
                    market: m.clone(),
                    server_time_unix: server_now,
                }));

                // Spawn pricing WebSocket task with robust reconnect loop
                let new_cancel = CancellationToken::new();
                ws_cancel = Some(new_cancel.clone());
                let state_clone = state.clone();
                let bus_clone = bus.clone();
                let cfg_clone = config.clone();
                let cond_id_clone = m.condition_id.clone();
                let validator_clone = validator.clone();
                let target_window_ts = m.window_start_unix;
                tokio::spawn(async move {
                    let mut backoff_secs: u64 = 1;
                    loop {
                        if new_cancel.is_cancelled() {
                            break;
                        }

                        // Sebelum setiap reconnect attempt
                        let current_ts = {
                            let s = state_clone.read().await;
                            s.btc5m_market.as_ref().map(|m| m.window_start_unix)
                        };
                        if current_ts != Some(target_window_ts) {
                            info!("[poly_ws] market window changed, stopping WS task");
                            break;
                        }

                        // Activate REST fallback immediately while WS is down
                        {
                            let mut s = state_clone.write().await;
                            s.latency.fallback_mode = true;
                        }

                        info!(
                            "[poly_ws] connecting to Polymarket WS for market {}...",
                            cond_id_clone
                        );
                        match run_ws_for_market(
                            cfg_clone.clone(),
                            state_clone.clone(),
                            bus_clone.clone(),
                            cond_id_clone.clone(),
                            yes_id.clone(),
                            no_id.clone(),
                            validator_clone.clone(),
                            new_cancel.clone(),
                        )
                        .await
                        {
                            Ok(_) => {
                                if new_cancel.is_cancelled() {
                                    break;
                                }
                                info!(
                                    "[poly_ws] session ended cleanly, reconnecting in {}s",
                                    backoff_secs
                                );
                            }
                            Err(e) => {
                                warn!(
                                    "[poly_ws] session error: {}, reconnecting in {}s",
                                    e, backoff_secs
                                );
                            }
                        }
                        tokio::time::sleep(Duration::from_secs(backoff_secs)).await;
                        backoff_secs = (backoff_secs * 2).min(30);
                    }
                });
            }
        } else {
            // Update countdown and phase of active market
            if let Some(ref mut m) = active_market {
                m.time_remaining_secs = secs_left as i64;

                // Active -> Final
                if secs_left <= config.btc5m.final_window_secs && m.phase == MarketPhase::Active {
                    m.phase = MarketPhase::Final;
                    info!(slug = %m.slug, "FINAL WINDOW, no new trades");

                    {
                        let mut s = state.write().await;
                        s.btc5m_market = Some(m.clone());
                    }

                    bus.publish(MarketEvent::PolymarketUpdate(m.clone()));
                    if !final_window_entered_emitted {
                        bus.publish(MarketEvent::MarketCycleEvent(MarketCycleEvent {
                            event_type: CycleEventType::FinalWindowEntered,
                            market: m.clone(),
                            server_time_unix: server_now,
                        }));
                        final_window_entered_emitted = true;
                    }
                }

                // Update phase in state
                {
                    let mut s = state.write().await;
                    if let Some(ref mut sm) = s.btc5m_market {
                        sm.time_remaining_secs = secs_left as i64;
                        sm.phase = m.phase;
                    }
                }

                let (fallback_mode, _price_rejections) = {
                    let s = state.read().await;
                    (s.latency.fallback_mode, s.latency.price_rejections)
                };

                // Periodically poll REST CLOB price to keep price fresh & track latency
                let rest_poll_interval = if fallback_mode {
                    Duration::from_secs(2)
                } else {
                    Duration::from_secs(10)
                };

                if m.phase == MarketPhase::Active && last_rest_fetch.elapsed() >= rest_poll_interval {
                    last_rest_fetch = Instant::now();
                    let yes_id = m.yes_token_id.clone();
                    let _no_id = m.no_token_id.clone();
                    let http_clone = http.clone();
                    let clob_api = config.btc5m.clob_api.clone();
                    let state_clone = state.clone();
                    let bus_clone = bus.clone();
                    let market_slug = m.slug.clone();
                    let validator_clone = validator.clone();
                    let config_clone = config.clone();

                    tokio::spawn(async move {
                        let start_time = Instant::now();
                        match fetch_midpoint_price(&http_clone, &clob_api, &yes_id).await {
                            Ok(yes_mid) => {
                                let no_mid = 1.0 - yes_mid;
                                let spread = 0.0;

                                let latency_ms = start_time.elapsed().as_millis() as u64;
                                let now_ms = now_millis();

                                let price_update = PriceUpdate {
                                    yes_price: yes_mid,
                                    no_price: no_mid,
                                    received_at_ms: now_ms,
                                    source: PriceSource::RestSnapshot,
                                    sequence_number: None,
                                };

                                let validation_res = {
                                    let mut val = validator_clone.lock().await;
                                    val.validate(
                                        &price_update,
                                        config_clone.data_quality.max_price_jump_pct_per_5s,
                                        fallback_mode,
                                    )
                                };

                                match validation_res {
                                    Ok(()) => {
                                        let mut s = state_clone.write().await;
                                        let market_update = if let Some(ref mut sm) = s.btc5m_market {
                                            if sm.slug == market_slug {
                                                let old_yes = sm.yes_price;
                                                let _ = apply_price_update(sm, &yes_id, yes_mid, now_ms);
                                                sm.spread = spread;
                                                sm.price_source = "RestSnapshot".to_string();
                                                tracing::info!(
                                                    "[price_debug] yes_price updated: {:.4} → {:.4} | source: {:?} | token_id: {}",
                                                    old_yes,
                                                    sm.yes_price,
                                                    PriceSource::RestSnapshot,
                                                    yes_id
                                                );
                                                Some(sm.clone())
                                            } else {
                                                None
                                            }
                                        } else {
                                            None
                                        };
                                        s.last_updated_ms = now_ms;
                                        s.latency.polymarket_last_msg_ms = now_ms;
                                        s.latency.polymarket_ms = latency_ms;
                                        if let Some(m) = market_update {
                                            tracing::debug!(
                                                "[poly_rest] updated price via REST fallback: yes={:.4} no={:.4}",
                                                m.yes_price, m.no_price
                                            );
                                            bus_clone.publish(MarketEvent::PolymarketUpdate(m));
                                        }
                                    }
                                    Err(reject_reason) => {
                                        let mut s = state_clone.write().await;
                                        match reject_reason {
                                            PriceRejectReason::StaleSnapshot => {
                                                s.latency.stale_snapshot_rejections += 1;
                                                tracing::debug!(
                                                    target: "price",
                                                    market_id = %market_slug,
                                                    reason = ?reject_reason,
                                                    "price update REJECTED (REST poll stale snapshot)"
                                                );
                                            }
                                            _ => {
                                                s.latency.price_rejections += 1;
                                                tracing::warn!(
                                                    target: "price",
                                                    market_id = %market_slug,
                                                    reason = ?reject_reason,
                                                    "price update REJECTED (REST poll)"
                                                );
                                                bus_clone.publish(MarketEvent::PriceRejected {
                                                    reason: format!("[price_validator] REJECTED REST update: {:?}", reject_reason),
                                                });
                                            }
                                        }
                                    }
                                }

                                bus_clone.publish(MarketEvent::LatencyAlert {
                                    source: FeedSource::Polymarket,
                                    latency_ms,
                                });
                            }
                            Err(e) => {
                                warn!(error = %e, slug = %market_slug, "CLOB REST price poll failed");
                            }
                        }
                    });
                }
            }

            // Pre-fetch next market
            let has_next = {
                let s = state.read().await;
                s.next_market.is_some()
            };

            if secs_left <= config.btc5m.pre_fetch_next_secs && !has_next {
                let next_ts = current_window_ts + 300;
                let next_slug = format!("btc-updown-5m-{}", next_ts);
                let http_clone = http.clone();
                let gamma_api = config.btc5m.gamma_api.clone();
                let retry_attempts = config.btc5m.market_retry_attempts;
                let retry_interval = config.btc5m.market_retry_interval_secs;
                let bus_clone = bus.clone();
                let state_clone = state.clone();

                info!(slug = %next_slug, "Pre-fetching next market");
                tokio::spawn(async move {
                    match fetch_gamma_market(
                        &http_clone,
                        &gamma_api,
                        &next_slug,
                        retry_attempts,
                        retry_interval,
                    ).await {
                        Ok((gm, mapping)) => {
                            log_token_mapping(&next_slug, &mapping);
                            let m = Btc5mMarket {
                                slug: gm.slug,
                                condition_id: gm.condition_id,
                                yes_token_id: mapping.yes_token_id,
                                no_token_id: mapping.no_token_id,
                                    question: gm.question,
                                    window_start_unix: next_ts,
                                    window_end_unix: next_ts + 300,
                                    phase: MarketPhase::PreOpen,
                                    yes_price: 0.5,
                                    no_price: 0.5,
                                    spread: 0.01,
                                    volume_24h: match gm.volume_24h {
                                        Some(serde_json::Value::Number(n)) => n.as_f64().unwrap_or(0.0),
                                        Some(serde_json::Value::String(s)) => s.parse().unwrap_or(0.0),
                                        _ => 0.0,
                                    },
                                    time_remaining_secs: 300,
                                    last_fetched_ms: now_millis(),
                                    price_source: "RestSnapshot".to_string(),
                                };
                                info!(slug = %m.slug, "Next market pre-fetched successfully");
                                
                                {
                                    let mut s = state_clone.write().await;
                                    s.next_market = Some(m.clone());
                                }

                                bus_clone.publish(MarketEvent::MarketCycleEvent(MarketCycleEvent {
                                    event_type: CycleEventType::NextMarketPreloaded,
                                    market: m,
                                    server_time_unix: now_millis() as f64 / 1000.0,
                                }));
                        }
                        Err(e) => {
                            warn!(error = %e, slug = %next_slug, "Failed to pre-fetch next market");
                        }
                    }
                });
            }
        }

        // Fallback checks and tick rate calculations
        {
            let mut s = state.write().await;
            let now_ms = now_millis();
            
            // Decrement/update tick rate if silent for > 10s
            let time_since_last_poly = now_ms.saturating_sub(s.latency.polymarket_last_msg_ms);
            if time_since_last_poly > 10_000 {
                s.latency.polymarket_tick_rate = 0.0;
            }
            
            let rate = s.latency.polymarket_tick_rate;
            let current_fallback = s.latency.fallback_mode;
            
            if rate < config.data_quality.ws_min_tick_rate && !current_fallback && time_since_last_poly > 10_000 {
                warn!("[poly_feed] WS tick rate low ({:.2}/s) or silent — activating REST fallback", rate);
                s.latency.fallback_mode = true;
            } else if rate >= 0.5 && current_fallback {
                info!("[poly_feed] WS tick rate recovered ({:.2}/s) — deactivating REST fallback", rate);
                s.latency.fallback_mode = false;
            }

            let feed_down_secs = time_since_last_poly / 1000;
            if feed_down_secs > 60 && s.latency.fallback_mode && !critical_alert_sent {
                bus.publish(MarketEvent::FeedCriticalAlert {
                    feed: "Polymarket".to_string(),
                    down_secs: feed_down_secs,
                });
                critical_alert_sent = true;
            } else if feed_down_secs < 15 {
                critical_alert_sent = false;
            }
        }

        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    if let Some(token) = ws_cancel.take() {
        token.cancel();
    }
}
