use std::sync::Arc;

use anyhow::Result;
use redis::aio::MultiplexedConnection;
use redis::AsyncCommands;
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use crate::config::AppConfig;
use crate::events::bus::{EventBus, MarketEvent};

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

/// Subscribes to the internal event bus and publishes every event to Redis.
///
/// Redis layout:
///   PUBLISH  polytrade:btc:tick              — each BtcTick JSON
///   PUBLISH  polytrade:market:{id}           — each PolymarketState JSON
///   PUBLISH  polytrade:events                — typed event envelopes for bot
///   SET EX   polytrade:state:market:{id} 60  — latest PolymarketState snapshot
///   SET EX   polytrade:edge:snapshot 90      — latest EdgeSnapshot JSON
///   HSET     polytrade:latency:binance       — latency_ms field (smoothed avg)
///   HSET     polytrade:latency:polymarket    — latency_ms field (REST round-trip)
///   LPUSH    polytrade:paper:trades          — closed paper trade JSON
///
/// Non-fatal: if Redis is unavailable the task logs a warning and exits
/// gracefully — the rest of the system continues running.
pub async fn run_redis_publisher(
    config: Arc<AppConfig>,
    bus: EventBus,
    cancel: CancellationToken,
) {
    if !config.redis.publish_enabled {
        info!("Redis publishing disabled in config");
        return;
    }

    let client = match redis::Client::open(config.redis.url.as_str()) {
        Ok(c) => c,
        Err(e) => {
            error!(error = %e, "Failed to create Redis client — publisher disabled");
            return;
        }
    };

    let mut conn = match client.get_multiplexed_async_connection().await {
        Ok(c) => c,
        Err(e) => {
            warn!(
                error = %e,
                url = %config.redis.url,
                "Cannot connect to Redis — publisher disabled (system continues)"
            );
            return;
        }
    };

    info!(url = %config.redis.url, "Redis publisher connected");

    let mut rx = bus.subscribe();

    loop {
        tokio::select! {
            result = rx.recv() => {
                match result {
                    Ok(event) => {
                        if let Err(e) = dispatch(&mut conn, &event, &config).await {
                            warn!(error = %e, "Redis publish error (non-fatal)");
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        warn!(skipped = n, "Redis publisher lagged — events dropped");
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        info!("Event bus closed — Redis publisher exiting");
                        break;
                    }
                }
            }
            _ = cancel.cancelled() => {
                info!("Redis publisher received cancellation");
                break;
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Dispatch
// ---------------------------------------------------------------------------

async fn dispatch(conn: &mut MultiplexedConnection, event: &MarketEvent, config: &AppConfig) -> Result<()> {
    match event {
        MarketEvent::BtcTick(tick) => {
            let json = serde_json::to_string(tick)?;
            publish(conn, "polytrade:btc:tick", &json).await?;
            // Don't publish every tick to polytrade:events — too noisy for bot.
        }

        MarketEvent::PolymarketUpdate(ps) => {
            let json = serde_json::to_string(ps)?;
            
            // 1. SET polytrade:btc5m:current JSON
            let _: () = redis::cmd("SET")
                .arg("polytrade:btc5m:current")
                .arg(&json)
                .query_async(conn)
                .await?;

            // Scalar key for quick price monitoring (watch / redis-cli get)
            let _: () = redis::cmd("SET")
                .arg("polytrade:btc5m:yes_price")
                .arg(ps.yes_price.to_string())
                .query_async(conn)
                .await?;
            let _: () = redis::cmd("SET")
                .arg("polytrade:btc5m:no_price")
                .arg(ps.no_price.to_string())
                .query_async(conn)
                .await?;

            // 2. SET polytrade:btc5m:phase phase_str
            let phase_str = match ps.phase {
                crate::market_data::state::MarketPhase::PreOpen => "preopen",
                crate::market_data::state::MarketPhase::Active => "active",
                crate::market_data::state::MarketPhase::Final => "final",
                crate::market_data::state::MarketPhase::Settled => "settled",
            };
            let _: () = redis::cmd("SET")
                .arg("polytrade:btc5m:phase")
                .arg(phase_str)
                .query_async(conn)
                .await?;

            // 3. SET polytrade:btc5m:secs_remaining integer
            let _: () = redis::cmd("SET")
                .arg("polytrade:btc5m:secs_remaining")
                .arg(ps.time_remaining_secs)
                .query_async(conn)
                .await?;

            // 4. Backward compatibility: update polytrade:state:market:{slug} and {condition_id}
            // First delete old keys to keep only the active one
            if ps.phase == crate::market_data::state::MarketPhase::Active || ps.phase == crate::market_data::state::MarketPhase::PreOpen {
                let keys: Vec<String> = redis::cmd("KEYS")
                    .arg("polytrade:state:market:*")
                    .query_async(conn)
                    .await
                    .unwrap_or_default();
                for k in keys {
                    let _: () = redis::cmd("DEL")
                        .arg(&k)
                        .query_async(conn)
                        .await
                        .unwrap_or_default();
                }
            }

            // Expiry 60s
            let key_slug = format!("polytrade:state:market:{}", ps.slug);
            let key_cond = format!("polytrade:state:market:{}", ps.condition_id);
            let _: () = redis::cmd("SETEX")
                .arg(&key_slug)
                .arg(60)
                .arg(&json)
                .query_async(conn)
                .await?;
            let _: () = redis::cmd("SETEX")
                .arg(&key_cond)
                .arg(60)
                .arg(&json)
                .query_async(conn)
                .await?;

            // Publish update on channel for real-time subscribers
            let channel = format!("polytrade:market:{}", ps.condition_id);
            publish(conn, &channel, &json).await?;
        }

        MarketEvent::MarketCycleEvent(evt) => {
            let evt_json = serde_json::to_string(evt)?;
            info!(
                event_type = %evt.event_type,
                slug = %evt.market.slug,
                "[tracker] published MarketCycleEvent: {} for {}",
                evt.event_type, evt.market.slug
            );
            
            // Send to polytrade:events envelope for Telegram bot
            let envelope = format!(r#"{{"type":"cycle_event","data":{}}}"#, evt_json);
            publish(conn, "polytrade:events", &envelope).await?;
        }

        MarketEvent::LatencyAlert { source, latency_ms } => {
            let hash_key = format!("polytrade:latency:{source}");
            hset_u64(conn, &hash_key, "latency_ms", *latency_ms).await?;
        }

        MarketEvent::FeedCriticalAlert { feed, down_secs } => {
            let envelope = serde_json::json!({
                "type": "feed_critical",
                "data": {
                    "feed": feed,
                    "down_secs": down_secs,
                    "message": format!("{} feed down {}s+ — REST fallback active", feed, down_secs),
                }
            })
            .to_string();
            publish(conn, "polytrade:events", &envelope).await?;
        }

        MarketEvent::PaperTradeOpen(pos) => {
            if let Ok(data_json) = serde_json::to_string(pos) {
                let envelope = format!(r#"{{"type":"paper_open","data":{}}}"#, data_json);
                publish(conn, "polytrade:events", &envelope).await?;
            }
        }

        MarketEvent::PaperTradeClose(trade) => {
            if let Ok(data_json) = serde_json::to_string(trade) {
                // LPUSH to paper trades list and cap at 1000.
                if trade.is_suspicious {
                    lpush_and_trim(conn, "polytrade:paper:suspicious_trades", &data_json, 1000).await?;
                } else {
                    lpush_and_trim(conn, "polytrade:paper:trades", &data_json, 1000).await?;
                }

                let envelope = format!(r#"{{"type":"paper_close","data":{}}}"#, data_json);
                publish(conn, "polytrade:events", &envelope).await?;
            }
        }

        MarketEvent::EdgeSnapshot {
            market_id,
            question,
            poly_yes_pct,
            poly_no_pct,
            divergence_score,
            expected_repricing,
            edge_pct,
            tradeable,
            direction,
            btc_price,
            btc_trend,
            velocity_trend,
            time_remaining_secs,
            confidence,
            price_velocity,
            volume_delta,
            missing_reason,
            threshold_mode,
            mins_since_last_trade,
            active_min_edge_pct,
            active_min_confidence,
        } => {
            // Write edge snapshot as JSON for the Telegram bot edge monitor.
            let json = serde_json::json!({
                "market_id": market_id,
                "question": question,
                "poly_yes_pct": poly_yes_pct,
                "poly_no_pct": poly_no_pct,
                "divergence_score": divergence_score,
                "expected_repricing": expected_repricing,
                "edge_pct": edge_pct,
                "tradeable": tradeable,
                "direction": direction,
                "btc_price": btc_price,
                "btc_trend": btc_trend,
                "velocity_trend": velocity_trend,
                "time_remaining_secs": time_remaining_secs,
                "confidence": confidence,
                "price_velocity": price_velocity,
                "volume_delta": volume_delta,
                "missing_reason": missing_reason,
                "threshold_mode": threshold_mode,
                "mins_since_last_trade": mins_since_last_trade,
                "active_min_edge_pct": active_min_edge_pct,
                "active_min_confidence": active_min_confidence,
                "ts_ms": crate::market_data::normalizer::now_millis(),
            })
            .to_string();
            set_ex(conn, "polytrade:edge:snapshot", &json, 90).await?;

            // Also publish to event channel for real-time subscribers.
            let envelope = format!(r#"{{"type":"edge_snapshot","data":{}}}"#, json);
            publish(conn, "polytrade:events", &envelope).await?;

            let _ = config;
        }

        // ConnectionStatus — no Redis key needed.
        MarketEvent::ConnectionStatus(_, _) => {}

        MarketEvent::PriceRejected { reason } => {
            let _: () = redis::cmd("INCR")
                .arg("polytrade:stats:price_rejections")
                .query_async(conn)
                .await?;

            let envelope = serde_json::json!({
                "type": "price_rejected",
                "data": {
                    "reason": reason,
                    "ts_ms": crate::market_data::normalizer::now_millis(),
                }
            })
            .to_string();
            publish(conn, "polytrade:events", &envelope).await?;
        }
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Redis command helpers
// ---------------------------------------------------------------------------

async fn publish(conn: &mut MultiplexedConnection, channel: &str, payload: &str) -> Result<()> {
    let _: i64 = conn.publish(channel, payload).await?;
    Ok(())
}

async fn set_ex(
    conn: &mut MultiplexedConnection,
    key: &str,
    value: &str,
    ttl_secs: u64,
) -> Result<()> {
    conn.set_ex::<_, _, ()>(key, value, ttl_secs).await?;
    Ok(())
}

async fn hset_u64(
    conn: &mut MultiplexedConnection,
    key: &str,
    field: &str,
    value: u64,
) -> Result<()> {
    let _: i64 = conn.hset(key, field, value).await?;
    Ok(())
}

async fn lpush_and_trim(
    conn: &mut MultiplexedConnection,
    key: &str,
    value: &str,
    max_len: isize,
) -> Result<()> {
    let _: i64 = conn.lpush(key, value).await?;
    conn.ltrim::<_, ()>(key, 0, max_len - 1).await?;
    Ok(())
}
