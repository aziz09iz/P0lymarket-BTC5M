use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;
use futures_util::{SinkExt, StreamExt};
use reqwest::Client as HttpClient;
use serde_json::{json, Value};
use tokio::sync::RwLock;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use crate::config::AppConfig;
use crate::events::bus::{ConnectionStatus, EventBus, FeedSource, MarketEvent};
use crate::market_data::normalizer::{
    gamma_market_to_state, normalize_gamma_market,
    normalize_polymarket_ws_msg, now_millis, GammaMarket,
};
use crate::market_data::state::MarketState;

const MAX_BACKOFF_SECS: u64 = 30;
/// How often to refresh market state from REST even while WS is connected.
const REST_REFRESH_INTERVAL_SECS: u64 = 30;

// ---------------------------------------------------------------------------
// Public entry point
// ---------------------------------------------------------------------------

/// Entry point for the Polymarket feed task.
///
/// On every connect cycle:
///   1. Fetch active crypto markets from the Gamma REST API.
///   2. Seed `MarketState` and filter to the configured time window.
///   3. Connect to the CLOB WebSocket and subscribe to the market assets.
///   4. Stream updates; fall back to periodic REST refresh if WS is silent.
///
/// Reconnects with exponential back-off on errors.
pub async fn run_polymarket_feed(
    config: Arc<AppConfig>,
    state: Arc<RwLock<MarketState>>,
    bus: EventBus,
    cancel: CancellationToken,
) {
    let http = HttpClient::new();
    let mut backoff_secs = 1u64;

    loop {
        if cancel.is_cancelled() {
            break;
        }

        info!("Starting Polymarket feed cycle");

        // ── Step 1: REST fetch ─────────────────────────────────────────────
        let markets = match fetch_filtered_markets(&http, &config, &state).await {
            Ok(m) => m,
            Err(e) => {
                warn!(error = %e, backoff_secs, "Polymarket REST fetch failed, retrying");
                tokio::select! {
                    _ = tokio::time::sleep(Duration::from_secs(backoff_secs)) => {}
                    _ = cancel.cancelled() => break,
                }
                backoff_secs = (backoff_secs * 2).min(MAX_BACKOFF_SECS);
                continue;
            }
        };

        if markets.is_empty() {
            warn!(
                min = config.feeds.market_filter_min_secs,
                max = config.feeds.market_filter_max_secs,
                "No active Polymarket crypto markets in the time window"
            );
            // Wait and re-check — new markets may open.
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_secs(60)) => {}
                _ = cancel.cancelled() => break,
            }
            continue;
        }

        // ── Step 2: Seed state from REST ───────────────────────────────────
        seed_state(&markets, &state, &bus).await;
        info!(count = markets.len(), "Seeded state from Polymarket REST");

        // ── Step 3: WS + background REST refresh ──────────────────────────
        let result = tokio::select! {
            r = run_ws_with_refresh(&config, &state, &bus, &http, &markets, &cancel) => r,
            _ = cancel.cancelled() => {
                info!("Polymarket feed received cancellation signal");
                break;
            }
        };

        match result {
            Ok(_) => {
                backoff_secs = 1; // reset on clean exit
            }
            Err(e) => {
                warn!(error = %e, backoff_secs, "Polymarket WS error — reconnecting");
                bus.publish(MarketEvent::ConnectionStatus(
                    FeedSource::Polymarket,
                    ConnectionStatus::Reconnecting {
                        delay_secs: backoff_secs,
                    },
                ));
                tokio::select! {
                    _ = tokio::time::sleep(Duration::from_secs(backoff_secs)) => {}
                    _ = cancel.cancelled() => break,
                }
                backoff_secs = (backoff_secs * 2).min(MAX_BACKOFF_SECS);
            }
        }
    }

    info!("Polymarket feed task shut down");
}

// ---------------------------------------------------------------------------
// REST fetch
// ---------------------------------------------------------------------------

async fn fetch_filtered_markets(
    http: &HttpClient,
    config: &AppConfig,
    state: &Arc<RwLock<MarketState>>,
) -> Result<Vec<GammaMarket>> {
    let url = format!("{}/markets?active=true&tag=crypto", config.feeds.polymarket_rest);
    info!(%url, "Fetching Polymarket markets from REST");

    // Measure REST round-trip latency.
    let rest_start = Instant::now();

    let resp = http
        .get(&url)
        .timeout(Duration::from_secs(15))
        .send()
        .await?;

    let rest_latency_ms = rest_start.elapsed().as_millis() as u64;

    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        return Err(anyhow::anyhow!(
            "Gamma REST returned HTTP {status}: {body}"
        ));
    }

    let body: Value = resp.json().await?;

    // Write REST latency to MarketState.
    {
        let mut s = state.write().await;
        s.latency.polymarket_ms = rest_latency_ms;
    }

    info!(latency_ms = rest_latency_ms, "Polymarket REST latency measured");

    // The API returns either an array or `{"data": [...]}`.
    let arr = if body.is_array() {
        body.as_array().cloned().unwrap_or_default()
    } else {
        body["data"]
            .as_array()
            .cloned()
            .unwrap_or_default()
    };

    let min_secs = config.feeds.market_filter_min_secs;
    let max_secs = config.feeds.market_filter_max_secs;
    let keyword = config.feeds.market_keyword_filter.to_lowercase();

    let markets: Vec<GammaMarket> = arr
        .iter()
        .filter_map(normalize_gamma_market)
        .filter(|m| m.time_remaining_secs >= min_secs && m.time_remaining_secs <= max_secs)
        .filter(|m| {
            // BTC keyword filter — match "BTC" or "bitcoin" (case-insensitive).
            if keyword.is_empty() {
                return true;
            }
            let q = m.question.to_lowercase();
            q.contains(&keyword) || q.contains("bitcoin")
        })
        .collect();

    info!(
        total_fetched = arr.len(),
        in_window = markets.len(),
        keyword = %config.feeds.market_keyword_filter,
        "Polymarket REST fetch complete"
    );

    Ok(markets)
}

// ---------------------------------------------------------------------------
// State seed
// ---------------------------------------------------------------------------

async fn seed_state(
    markets: &[GammaMarket],
    state: &Arc<RwLock<MarketState>>,
    bus: &EventBus,
) {
    let mut s = state.write().await;
    for m in markets {
        let ps = gamma_market_to_state(m);
        bus.publish(MarketEvent::PolymarketUpdate(ps.clone()));
        s.polymarkets.insert(m.market_id.clone(), ps);
    }
    s.last_updated_ms = now_millis();
}

// ---------------------------------------------------------------------------
// WS streaming with concurrent REST refresh
// ---------------------------------------------------------------------------

async fn run_ws_with_refresh(
    config: &AppConfig,
    state: &Arc<RwLock<MarketState>>,
    bus: &EventBus,
    http: &HttpClient,
    markets: &[GammaMarket],
    cancel: &CancellationToken,
) -> Result<()> {
    // Build token_id → market_id lookup for WS message routing.
    let mut token_to_market: HashMap<String, (String, bool)> = HashMap::new();
    let mut all_token_ids: Vec<String> = Vec::new();

    for m in markets {
        if !m.yes_token_id.is_empty() {
            token_to_market.insert(
                m.yes_token_id.clone(),
                (m.market_id.clone(), true), // true = YES token
            );
            all_token_ids.push(m.yes_token_id.clone());
        }
        if !m.no_token_id.is_empty() {
            token_to_market.insert(
                m.no_token_id.clone(),
                (m.market_id.clone(), false), // false = NO token
            );
            all_token_ids.push(m.no_token_id.clone());
        }
    }

    // Connect WS
    let url = &config.feeds.polymarket_ws;
    info!(%url, "Connecting to Polymarket CLOB WebSocket");
    let (mut ws, _) = connect_async(url).await?;

    bus.publish(MarketEvent::ConnectionStatus(
        FeedSource::Polymarket,
        ConnectionStatus::Connected,
    ));
    info!("Polymarket WebSocket connected");

    // Subscribe to all asset token IDs
    if !all_token_ids.is_empty() {
        let sub_msg = json!({
            "assets_ids": all_token_ids,
            "type": "subscribe"
        })
        .to_string();
        ws.send(Message::Text(sub_msg)).await?;
        info!(count = all_token_ids.len(), "Subscribed to Polymarket assets");
    }

    let mut rest_refresh = tokio::time::interval(Duration::from_secs(REST_REFRESH_INTERVAL_SECS));
    rest_refresh.tick().await; // skip immediate first tick

    loop {
        tokio::select! {
            msg = ws.next() => {
                match msg {
                    Some(Ok(Message::Text(text))) => {
                        handle_ws_message(
                            &text,
                            &token_to_market,
                            state,
                            bus,
                        ).await;
                    }
                    Some(Ok(Message::Ping(data))) => {
                        ws.send(Message::Pong(data)).await?;
                    }
                    Some(Ok(Message::Close(frame))) => {
                        warn!(?frame, "Polymarket server closed the WebSocket");
                        return Err(anyhow::anyhow!("Polymarket WS closed by server"));
                    }
                    Some(Ok(_)) => {} // Binary / Pong
                    Some(Err(e)) => return Err(e.into()),
                    None => return Err(anyhow::anyhow!("Polymarket WS stream ended")),
                }
            }

            // Periodic REST refresh — keeps time_remaining and volume fresh.
            _ = rest_refresh.tick() => {
                match fetch_filtered_markets(http, config, state).await {
                    Ok(updated) => {
                        // Also evict markets that have left the window.
                        refresh_state_from_rest(&updated, state, bus, config).await;
                    }
                    Err(e) => {
                        warn!(error = %e, "Background REST refresh failed (non-fatal)");
                    }
                }
            }

            _ = cancel.cancelled() => {
                info!("Polymarket WS received cancellation");
                return Ok(());
            }
        }
    }
}

// ---------------------------------------------------------------------------
// WS message handler
// ---------------------------------------------------------------------------

async fn handle_ws_message(
    raw: &str,
    token_to_market: &HashMap<String, (String, bool)>,
    state: &Arc<RwLock<MarketState>>,
    bus: &EventBus,
) {
    let update = match normalize_polymarket_ws_msg(raw) {
        Ok(Some(u)) => u,
        Ok(None) => return, // heartbeat / unknown event type
        Err(e) => {
            error!(error = %e, raw_message = %raw, "Failed to parse Polymarket WS message");
            return;
        }
    };

    let (market_id, is_yes) = match token_to_market.get(&update.token_id) {
        Some(v) => v.clone(),
        None => return, // token not in our tracked set
    };

    let now_ms = now_millis();

    let (updated_state, prev_ms) = {
        let mut s = state.write().await;
        if let Some(ps) = s.polymarkets.get_mut(&market_id) {
            let prev_updated_ms = ps.last_updated_ms;
            if is_yes {
                ps.yes_price = update.price;
                if update.spread > 0.0 {
                    ps.spread = update.spread;
                }
            } else {
                ps.no_price = update.price;
            }
            ps.last_updated_ms = now_ms;
            (Some(ps.clone()), prev_updated_ms)
        } else {
            (None, 0)
        }
    };

    // Update latency outside the borrow (separate write lock acquisition).
    if prev_ms > 0 {
        let mut s = state.write().await;
        s.latency.polymarket_ms = now_ms.saturating_sub(prev_ms);
    }

    if let Some(ps) = updated_state {
        bus.publish(MarketEvent::PolymarketUpdate(ps));
    }
}

// ---------------------------------------------------------------------------
// REST refresh — evict expired markets, update remaining
// ---------------------------------------------------------------------------

async fn refresh_state_from_rest(
    fresh: &[GammaMarket],
    state: &Arc<RwLock<MarketState>>,
    bus: &EventBus,
    config: &AppConfig,
) {
    let fresh_ids: std::collections::HashSet<String> =
        fresh.iter().map(|m| m.market_id.clone()).collect();

    let mut s = state.write().await;

    // Evict markets that have closed or left the filter window.
    s.polymarkets.retain(|id, ps| {
        let in_fresh = fresh_ids.contains(id);
        if !in_fresh && ps.time_remaining_secs < config.feeds.market_filter_min_secs {
            info!(market_id = %id, "Evicting expired Polymarket market");
            false
        } else {
            true
        }
    });

    // Upsert fresh markets.
    for m in fresh {
        let ps = gamma_market_to_state(m);
        bus.publish(MarketEvent::PolymarketUpdate(ps.clone()));
        s.polymarkets.insert(m.market_id.clone(), ps);
    }
    s.last_updated_ms = now_millis();
}
