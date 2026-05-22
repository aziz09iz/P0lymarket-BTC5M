use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use futures_util::{SinkExt, StreamExt};
use tokio::sync::RwLock;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use crate::config::AppConfig;
use crate::events::bus::{ConnectionStatus, EventBus, FeedSource, MarketEvent};
use crate::market_data::normalizer::{normalize_binance_trade, now_millis};
use crate::market_data::state::MarketState;

const MAX_BACKOFF_SECS: u64 = 60;
/// Number of samples in the rolling latency moving average.
const LATENCY_WINDOW: usize = 10;
/// Only fire a latency alert above this threshold (2 seconds).
const LATENCY_ALERT_MS: u64 = 2_000;

/// Entry point for the Binance feed task.
///
/// Connects to Binance WebSocket, parses every trade message, updates shared
/// `MarketState`, and emits events on the bus.
/// Reconnects with exponential back-off on ANY failure — including clean
/// disconnects from the server. The task only exits when cancelled.
pub async fn run_binance_feed(
    config: Arc<AppConfig>,
    state: Arc<RwLock<MarketState>>,
    bus: EventBus,
    cancel: CancellationToken,
) {
    let mut backoff_secs = 1u64;

    loop {
        if cancel.is_cancelled() {
            break;
        }

        match connect_and_stream(&config, &state, &bus, &cancel).await {
            Ok(_) => {
                // Returned Ok means cancellation was requested — exit cleanly.
                if cancel.is_cancelled() {
                    break;
                }
                // Otherwise the server closed the socket — reconnect.
                warn!("Binance WebSocket closed by server — reconnecting in {}s", backoff_secs);
                bus.publish(MarketEvent::ConnectionStatus(
                    FeedSource::Binance,
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
            Err(e) => {
                warn!(
                    error = %e,
                    backoff_secs,
                    "Binance feed error — reconnecting with back-off"
                );
                bus.publish(MarketEvent::ConnectionStatus(
                    FeedSource::Binance,
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

    info!("Binance feed task shut down");
}

async fn connect_and_stream(
    config: &AppConfig,
    state: &Arc<RwLock<MarketState>>,
    bus: &EventBus,
    cancel: &CancellationToken,
) -> Result<()> {
    let url = &config.feeds.binance_ws;
    info!(%url, "Connecting to Binance WebSocket");

    let (mut ws, _) = connect_async(url).await?;

    bus.publish(MarketEvent::ConnectionStatus(
        FeedSource::Binance,
        ConnectionStatus::Connected,
    ));
    info!("Binance WebSocket connected");

    // Rolling window for latency smoothing.
    let mut latency_window: VecDeque<u64> = VecDeque::with_capacity(LATENCY_WINDOW);
    let mut msg_timestamps: VecDeque<u64> = VecDeque::new();

    let ping_interval = Duration::from_secs(config.data_quality.ws_ping_interval_secs);
    let mut ping_timer = tokio::time::interval(ping_interval);
    ping_timer.reset();

    let mut ping_sent_at_ms: Option<u64> = None;

    loop {
        tokio::select! {
            msg = ws.next() => {
                match msg {
                    Some(Ok(Message::Text(text))) => {
                        handle_trade_message(&text, state, bus, config, &mut latency_window, &mut msg_timestamps).await;
                    }
                    Some(Ok(Message::Ping(data))) => {
                        // Respond to keep-alive pings.
                        ws.send(Message::Pong(data)).await?;
                    }
                    Some(Ok(Message::Pong(_))) => {
                        if let Some(sent_at) = ping_sent_at_ms {
                            let latency = now_millis().saturating_sub(sent_at);
                            let mut s = state.write().await;
                            s.latency.binance_ping_ms = latency;
                            ping_sent_at_ms = None;
                        }
                    }
                    Some(Ok(Message::Close(frame))) => {
                        warn!(?frame, "Binance server closed the WebSocket");
                        return Err(anyhow::anyhow!("WebSocket closed by server"));
                    }
                    Some(Ok(_)) => {} // Binary — ignore
                    Some(Err(e)) => return Err(e.into()),
                    None => return Err(anyhow::anyhow!("WebSocket stream ended unexpectedly")),
                }
            }
            _ = ping_timer.tick() => {
                ping_sent_at_ms = Some(now_millis());
                if let Err(e) = ws.send(Message::Ping(vec![])).await {
                    warn!("Failed to send Binance WS ping: {:?}", e);
                }
            }
            _ = cancel.cancelled() => {
                info!("Binance feed received cancellation signal");
                return Ok(());
            }
        }
    }
}

/// Parse one Binance trade message, update state, and emit events.
/// Uses a rolling moving average to smooth latency spikes.
async fn handle_trade_message(
    raw: &str,
    state: &Arc<RwLock<MarketState>>,
    bus: &EventBus,
    config: &AppConfig,
    latency_window: &mut VecDeque<u64>,
    msg_timestamps: &mut VecDeque<u64>,
) {
    let tick = match normalize_binance_trade(raw) {
        Ok(t) => t,
        Err(e) => {
            error!(error = %e, raw_message = %raw, "Failed to parse Binance trade message");
            return;
        }
    };

    let now_ms = now_millis();
    let avg_latency_ms;

    msg_timestamps.push_back(now_ms);
    while msg_timestamps.front().map(|&t| now_ms - t > 5000).unwrap_or(false) {
        msg_timestamps.pop_front();
    }
    let binance_tick_rate = msg_timestamps.len() as f64 / 5.0;

    // Update shared state under write lock.
    {
        let mut s = state.write().await;
        s.btc.push_tick(
            tick.clone(),
            config.state.btc_max_ticks,
            config.state.btc_tick_window_secs,
            config.btc5m.velocity_window_5s,
            config.btc5m.velocity_window_10s,
            config.btc5m.velocity_window_15s,
        );
        
        let raw_latency_ms = now_ms.saturating_sub(tick.event_time_ms);

        if raw_latency_ms < 5000 {
            // Update rolling window.
            if latency_window.len() >= LATENCY_WINDOW {
                latency_window.pop_front();
            }
            latency_window.push_back(raw_latency_ms);
        }

        // Compute smoothed moving average.
        avg_latency_ms = if latency_window.is_empty() {
            raw_latency_ms
        } else {
            latency_window.iter().sum::<u64>() / latency_window.len() as u64
        };

        s.latency.binance_ms = avg_latency_ms;
        s.latency.binance_tick_rate = binance_tick_rate;
        s.latency.binance_last_msg_ms = now_ms;
        s.last_updated_ms = now_ms;
    }

    // Emit events — do this *outside* the lock.
    bus.publish(MarketEvent::BtcTick(tick));

    // Always publish latency to keep Redis stats fresh.
    bus.publish(MarketEvent::LatencyAlert {
        source: FeedSource::Binance,
        latency_ms: avg_latency_ms,
    });
}
