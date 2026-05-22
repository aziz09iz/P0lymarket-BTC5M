use std::collections::HashMap;
use std::sync::Arc;

use redis::AsyncCommands;
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::config::AppConfig;
use crate::events::bus::{EventBus, MarketEvent};
use crate::market_data::normalizer::now_millis;
use crate::market_data::state::{prices_valid_for_trading, MarketState};
use crate::paper::simulator::PaperSimulator;
use crate::probability::edge::{self, EdgeScore};
use crate::probability::estimator::{self, ProbabilityInput, ProbabilityEstimate};
use crate::risk::engine::RiskEngine;
use crate::strategy::divergence::MomentumDivergenceStrategy;
use crate::strategy::session::{apply_entry_thresholds, SessionState, ThresholdMode, EntryThresholds};
use crate::strategy::traits::Strategy;

// ---------------------------------------------------------------------------
// Runtime config (read from Redis, falls back to TOML defaults)
// ---------------------------------------------------------------------------

struct RuntimeConfig {
    paused: bool,
    size_usd: f64,
    mode: String, // "paper" or "live"
    min_edge_pct: f64,
    min_confidence: f64,
    max_spread_for_trade: f64,
    consecutive_loss_limit: u32,
    cooldown_after_loss_secs: u64,
}

struct TickSnapshot {
    estimate: ProbabilityEstimate,
    edge_score: EdgeScore,
    missing_reason: String,
    threshold_mode: ThresholdMode,
    thresholds: EntryThresholds,
}

/// Run the strategy engine loop.
pub async fn run_strategy_engine(
    config: Arc<AppConfig>,
    state: Arc<RwLock<MarketState>>,
    bus: EventBus,
    cancel: CancellationToken,
) {
    if !config.paper.enabled {
        info!("Paper trading disabled in config — strategy engine not started");
        return;
    }

    let divergence_config = Arc::new(RwLock::new(config.strategy.divergence.clone()));

    // Build strategies.
    let strategies: Vec<Box<dyn Strategy>> = vec![
        Box::new(MomentumDivergenceStrategy::new(
            divergence_config.clone(),
            config.paper.default_size_usd,
        )),
    ];

    let enabled: Vec<&str> = strategies
        .iter()
        .filter(|s| s.is_enabled())
        .map(|s| s.name())
        .collect();
    info!(strategies = ?enabled, "Strategy engine started");

    let mut simulator = PaperSimulator::new();
    let mut risk = RiskEngine::new(config.risk.clone());
    let mut session = SessionState::new();
    let mut rx = bus.subscribe();
    let mut last_threshold_mode = ThresholdMode::Normal;

    // Try to connect to Redis for config polling.
    let mut redis_conn = try_redis_connect(&config.redis.url).await;

    // Throttle: don't evaluate more than once per 250ms.
    let mut last_eval_ms: u64 = 0;
    const EVAL_INTERVAL_MS: u64 = 250;

    // Config polling: every 1 second.
    let mut last_config_poll_ms: u64 = 0;
    const CONFIG_POLL_INTERVAL_MS: u64 = 1_000;

    let mut runtime = RuntimeConfig {
        paused: false,
        size_usd: config.paper.default_size_usd,
        mode: "paper".to_string(),
        min_edge_pct: config.strategy.divergence.min_edge_pct,
        min_confidence: config.strategy.divergence.min_confidence,
        max_spread_for_trade: config.strategy.divergence.max_spread,
        consecutive_loss_limit: config.risk.consecutive_loss_limit,
        cooldown_after_loss_secs: config.risk.cooldown_after_loss_secs,
    };

    // Edge broadcast throttle.
    let mut last_edge_broadcast_ms: u64 = 0;
    let edge_broadcast_interval_ms = config.edge.edge_broadcast_interval_secs * 1_000;

    loop {
        tokio::select! {
            result = rx.recv() => {
                match result {
                    Ok(MarketEvent::BtcTick(_)) | Ok(MarketEvent::PolymarketUpdate(_)) => {
                        let now = now_millis();

                        // Poll Redis config every 1s.
                        if now.saturating_sub(last_config_poll_ms) >= CONFIG_POLL_INTERVAL_MS {
                            last_config_poll_ms = now;
                            poll_redis_config(&mut redis_conn, &mut runtime, &config, &config.redis.url).await;
                        }

                        // Skip evaluation if paused.
                        if runtime.paused {
                            continue;
                        }

                        if now.saturating_sub(last_eval_ms) < EVAL_INTERVAL_MS {
                            continue;
                        }
                        last_eval_ms = now;

                        // Take a snapshot of state.
                        let snap = state.read().await.clone();

                        session.maybe_reset_session();

                        let (threshold_mode, thresholds) = session.active_thresholds(
                            &config,
                            runtime.min_edge_pct,
                            runtime.min_confidence,
                        );
                        if threshold_mode != last_threshold_mode {
                            let mins = session.mins_since_last_trade();
                            match threshold_mode {
                                ThresholdMode::Relaxed => info!(
                                    "[strategy] threshold mode: RELAXED ({} min no trade) — loosening filters",
                                    mins
                                ),
                                ThresholdMode::Floor => info!(
                                    "[strategy] threshold mode: FLOOR ({} min no trade) — taking best available signal",
                                    mins
                                ),
                                ThresholdMode::Normal => info!(
                                    "[strategy] threshold mode: NORMAL ({} min since last trade)",
                                    mins
                                ),
                            }
                            last_threshold_mode = threshold_mode;
                        }

                        let active_div_config = {
                            let mut active = apply_entry_thresholds(&config.strategy.divergence, &thresholds);
                            active.max_spread = runtime.max_spread_for_trade;
                            *divergence_config.write().await = active.clone();
                            active
                        };

                        let tick_snap = if snap.btc.price == 0.0 || snap.btc5m_market.is_none() {
                            None
                        } else {
                            let market = snap.btc5m_market.as_ref().unwrap();
                            let input = ProbabilityInput {
                                market_id: market.slug.clone(),
                                current_yes_price: market.yes_price,
                                current_no_price: market.no_price,
                                btc_state: snap.btc.clone(),
                                time_remaining_secs: market.time_remaining_secs,
                                spread: market.spread,
                            };
                            let estimate = estimator::compute_estimate(&input, &active_div_config);
                            let direction = if snap.btc.price_velocity > 0.0 {
                                edge::Direction::Yes
                            } else {
                                edge::Direction::No
                            };
                            let edge_score = edge::score_edge(
                                estimate.expected_repricing,
                                direction,
                                market.yes_price,
                                market.no_price,
                                market.spread,
                                market.time_remaining_secs,
                                estimate.confidence,
                                &active_div_config,
                            );
                            let opportunity_score = edge_score.edge_pct * edge_score.confidence
                                * snap.btc.velocity_consistency
                                * (snap.btc.order_flow_ratio - 0.5).abs() * 2.0;

                            session.update_opportunity(opportunity_score);

                            let has_pos = simulator.has_position(&market.slug);
                            let missing_reason = get_missing_reason(&snap, &edge_score, &active_div_config, has_pos, runtime.paused);

                            Some(TickSnapshot {
                                estimate,
                                edge_score,
                                missing_reason,
                                threshold_mode,
                                thresholds,
                            })
                        };

                        if let Some(ref ts) = tick_snap {
                            evaluate_all(
                                &snap,
                                &strategies,
                                &mut simulator,
                                &mut risk,
                                &mut session,
                                &config,
                                &bus,
                                &runtime,
                                ts,
                            ).await;
                        }

                        // Write paper state to Redis.
                        write_paper_state(&mut redis_conn, &simulator, &risk).await;

                        // Broadcast edge snapshot every N seconds.
                        if now.saturating_sub(last_edge_broadcast_ms) >= edge_broadcast_interval_ms {
                            last_edge_broadcast_ms = now;
                            let has_pos = simulator.has_position(&snap.btc5m_market.as_ref().map(|m| m.slug.clone()).unwrap_or_default());
                            broadcast_edge_snapshot(&snap, &bus, &config, &runtime, &session, has_pos, tick_snap.as_ref()).await;
                            write_signal_state(&mut redis_conn, &snap, &config, &runtime, &session, has_pos, tick_snap.as_ref()).await;
                        }
                    }
                    Ok(_) => {} // ignore other events
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                        warn!(skipped = n, "Strategy engine lagged — events dropped");
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                        info!("Event bus closed — strategy engine exiting");
                        break;
                    }
                }
            }
            _ = cancel.cancelled() => {
                info!("Strategy engine received cancellation");
                break;
            }
        }
    }

    // Final stats on shutdown.
    simulator.log_stats();
    info!("Strategy engine shut down");
}

// ---------------------------------------------------------------------------
// Redis config polling
// ---------------------------------------------------------------------------

async fn try_redis_connect(url: &str) -> Option<redis::aio::MultiplexedConnection> {
    let client = redis::Client::open(url).ok()?;
    client.get_multiplexed_async_connection().await.ok()
}

async fn poll_redis_config(
    conn: &mut Option<redis::aio::MultiplexedConnection>,
    runtime: &mut RuntimeConfig,
    config: &AppConfig,
    redis_url: &str,
) {
    if conn.is_none() {
        match try_redis_connect(redis_url).await {
            Some(c) => {
                info!("Redis reconnected in strategy engine");
                *conn = Some(c);
            }
            None => {
                use std::sync::atomic::{AtomicU64, Ordering};
                static LAST_RECONNECT_WARN_MS: AtomicU64 = AtomicU64::new(0);
                let now_ms = now_millis();
                let last_warn = LAST_RECONNECT_WARN_MS.load(Ordering::Relaxed);
                if now_ms.saturating_sub(last_warn) >= 60_000 {
                    if LAST_RECONNECT_WARN_MS.compare_exchange(last_warn, now_ms, Ordering::Relaxed, Ordering::Relaxed).is_ok() {
                        warn!("Redis reconnect failed in strategy engine, retrying in 60s");
                    }
                }
                return;
            }
        }
    }

    let conn = match conn.as_mut() {
        Some(c) => c,
        None => return,
    };

    // Read paused state.
    if let Ok(val) = conn.get::<_, Option<String>>("polytrade:config:paused").await {
        runtime.paused = val.as_deref() == Some("true");
    }

    // Read size override.
    if let Ok(Some(val)) = conn.get::<_, Option<String>>("polytrade:config:size_usd").await {
        if let Ok(size) = val.parse::<f64>() {
            if size >= 0.10 && size <= 100.0 {
                runtime.size_usd = size;
            }
        }
    } else {
        runtime.size_usd = config.paper.default_size_usd;
    }

    // Read mode.
    if let Ok(val) = conn.get::<_, Option<String>>("polytrade:config:mode").await {
        runtime.mode = val.unwrap_or_else(|| "paper".to_string());
    }

    // Read min_edge_pct (saved as e.g. 8.0 for 8%).
    if let Ok(Some(val)) = conn.get::<_, Option<String>>("polytrade:config:min_edge_pct").await {
        if let Ok(val_pct) = val.parse::<f64>() {
            runtime.min_edge_pct = val_pct / 100.0;
        }
    } else {
        runtime.min_edge_pct = config.strategy.divergence.min_edge_pct;
    }

    // Read min_confidence.
    if let Ok(Some(val)) = conn.get::<_, Option<String>>("polytrade:config:min_confidence").await {
        if let Ok(val_conf) = val.parse::<f64>() {
            runtime.min_confidence = val_conf;
        }
    } else {
        runtime.min_confidence = config.strategy.divergence.min_confidence;
    }

    // Read max_spread_for_trade (saved as e.g. 4.0 for 4%).
    if let Ok(Some(val)) = conn.get::<_, Option<String>>("polytrade:config:max_spread_for_trade").await {
        if let Ok(val_spread) = val.parse::<f64>() {
            runtime.max_spread_for_trade = val_spread / 100.0;
        }
    } else {
        runtime.max_spread_for_trade = config.strategy.divergence.max_spread;
    }

    // Read consecutive_loss_limit.
    if let Ok(Some(val)) = conn.get::<_, Option<String>>("polytrade:config:consecutive_loss_limit").await {
        if let Ok(limit) = val.parse::<u32>() {
            runtime.consecutive_loss_limit = limit;
        }
    } else {
        runtime.consecutive_loss_limit = config.risk.consecutive_loss_limit;
    }

    // Read cooldown_after_loss_secs.
    if let Ok(Some(val)) = conn.get::<_, Option<String>>("polytrade:config:cooldown_after_loss_secs").await {
        if let Ok(secs) = val.parse::<u64>() {
            runtime.cooldown_after_loss_secs = secs;
        }
    } else {
        runtime.cooldown_after_loss_secs = config.risk.cooldown_after_loss_secs;
    }
}

// ---------------------------------------------------------------------------
// Write paper state to Redis
// ---------------------------------------------------------------------------

async fn write_paper_state(
    conn_opt: &mut Option<redis::aio::MultiplexedConnection>,
    simulator: &PaperSimulator,
    risk: &RiskEngine,
) {
    let conn = match conn_opt.as_mut() {
        Some(c) => c,
        None => return,
    };

    // Positions snapshot (SET EX 10).
    let positions: Vec<_> = simulator.positions.values().collect();
    if let Ok(json) = serde_json::to_string(&positions) {
        if conn.set_ex::<_, _, ()>("polytrade:paper:positions", &json, 10u64).await.is_err() {
            *conn_opt = None;
            return;
        }
    }

    // Compute detailed stats.
    let closed = &simulator.closed_trades;
    let mut wins_sum = 0.0;
    let mut losses_sum = 0.0;
    let mut best_trade = 0.0;
    let mut worst_trade = 0.0;
    let mut edge_sum = 0.0;
    let mut hold_sum_ms = 0;
    
    let mut yes_trades = 0;
    let mut yes_wins = 0;
    let mut no_trades = 0;
    let mut no_wins = 0;

    let mut valid_win_count = 0;
    let mut valid_loss_count = 0;
    let mut valid_total_pnl = 0.0;
    let mut suspicious_count = 0;

    for trade in closed {
        if trade.is_suspicious {
            suspicious_count += 1;
            continue;
        }
        let pnl = trade.pnl_usd;
        valid_total_pnl += pnl;
        if pnl >= 0.0 {
            wins_sum += pnl;
            valid_win_count += 1;
            if pnl > best_trade {
                best_trade = pnl;
            }
        } else {
            losses_sum += pnl;
            valid_loss_count += 1;
            if pnl < worst_trade {
                worst_trade = pnl;
            }
        }
        edge_sum += trade.edge_pct;
        hold_sum_ms += trade.hold_duration_ms;

        match trade.direction {
            crate::probability::edge::Direction::Yes => {
                yes_trades += 1;
                if pnl >= 0.0 {
                    yes_wins += 1;
                }
            }
            crate::probability::edge::Direction::No => {
                no_trades += 1;
                if pnl >= 0.0 {
                    no_wins += 1;
                }
            }
        }
    }

    let valid_trades_count = valid_win_count + valid_loss_count;
    let avg_win = if valid_win_count > 0 { wins_sum / valid_win_count as f64 } else { 0.0 };
    let avg_loss = if valid_loss_count > 0 { losses_sum / valid_loss_count as f64 } else { 0.0 };
    let edge_avg = if valid_trades_count > 0 { edge_sum / valid_trades_count as f64 } else { 0.0 };
    let avg_hold_secs = if valid_trades_count > 0 { (hold_sum_ms as f64 / valid_trades_count as f64) / 1000.0 } else { 0.0 };

    let res: Result<(), _> = redis::pipe()
        .hset("polytrade:paper:stats", "total_pnl", format!("{:.4}", valid_total_pnl))
        .hset("polytrade:paper:stats", "win_count", valid_win_count)
        .hset("polytrade:paper:stats", "loss_count", valid_loss_count)
        .hset("polytrade:paper:stats", "trade_count", valid_trades_count)
        .hset("polytrade:paper:stats", "suspicious_count", suspicious_count)
        .hset("polytrade:paper:stats", "open_positions", simulator.positions.len())
        .hset("polytrade:paper:stats", "consecutive_losses", risk.consecutive_losses)
        .hset(
            "polytrade:paper:stats",
            "cooldown_until_ms",
            risk.cooldown_until_ms.unwrap_or(0),
        )
        .hset("polytrade:paper:stats", "avg_win", format!("{:.4}", avg_win))
        .hset("polytrade:paper:stats", "avg_loss", format!("{:.4}", avg_loss))
        .hset("polytrade:paper:stats", "best_trade", format!("{:.4}", best_trade))
        .hset("polytrade:paper:stats", "worst_trade", format!("{:.4}", worst_trade))
        .hset("polytrade:paper:stats", "edge_avg", format!("{:.4}", edge_avg))
        .hset("polytrade:paper:stats", "avg_hold_secs", format!("{:.4}", avg_hold_secs))
        .hset("polytrade:paper:stats", "yes_trades", yes_trades)
        .hset("polytrade:paper:stats", "yes_wins", yes_wins)
        .hset("polytrade:paper:stats", "no_trades", no_trades)
        .hset("polytrade:paper:stats", "no_wins", no_wins)
        .query_async(conn)
        .await;
    if res.is_err() {
        *conn_opt = None;
    }
}

// ---------------------------------------------------------------------------
// Edge snapshot broadcast
// ---------------------------------------------------------------------------

fn get_missing_reason(
    state: &MarketState,
    edge_score: &EdgeScore,
    divergence_config: &crate::config::DivergenceConfig,
    has_position: bool,
    paused: bool,
) -> String {
    if paused {
        return "Engine paused".to_string();
    }
    if has_position {
        return "Holding open position".to_string();
    }
    let market = match &state.btc5m_market {
        Some(m) => m,
        None => return "No active market".to_string(),
    };
    if market.phase != crate::market_data::state::MarketPhase::Active {
        return format!("Market phase is {:?}", market.phase);
    }
    if !edge_score.tradeable {
        if let Some(r) = &edge_score.reason {
            if r.contains("conf") {
                let current_conf = edge_score.confidence;
                let needed = divergence_config.min_confidence - current_conf;
                if needed > 0.0 {
                    return format!("conf needs +{:.2} more", needed);
                }
            }
            if r.contains("edge") {
                let current_edge = edge_score.edge_pct;
                let needed = divergence_config.min_edge_pct - current_edge;
                if needed > 0.0 {
                    return format!("edge needs +{:.1}% more", needed * 100.0);
                }
            }
            return r.clone();
        }
        return "Not tradeable".to_string();
    }
    // Check strategy constraints
    let vel_abs = state.btc.price_velocity.abs();
    if vel_abs < divergence_config.min_velocity_abs {
        return format!("vel {:.2}/s < min {:.2}/s", vel_abs, divergence_config.min_velocity_abs);
    }
    if state.btc.microtrend == crate::market_data::state::MicroTrend::Choppy {
        return "microtrend is Choppy".to_string();
    }
    if divergence_config.require_volume_alignment {
        if state.btc.volume_delta.signum() != state.btc.price_velocity.signum() {
            return "Volume delta not aligned with velocity".to_string();
        }
    }

    // Gate 1: Order Flow
    let flow_aligned = if state.btc.price_velocity > 0.0 {
        state.btc.order_flow_ratio > 0.58
    } else {
        state.btc.order_flow_ratio < 0.42
    };
    if !flow_aligned {
        return format!("OFI not aligned ({:.2})", state.btc.order_flow_ratio);
    }

    // Gate 2: Velocity consistency
    if state.btc.velocity_consistency < divergence_config.min_velocity_consistency {
        return format!("consistency {:.2} < {:.2}", state.btc.velocity_consistency, divergence_config.min_velocity_consistency);
    }

    // Gate 3: Deceleration
    let is_strongly_decelerating = 
        state.btc.price_acceleration.signum() != state.btc.price_velocity.signum()
        && state.btc.price_acceleration.abs() > 0.5;
    if is_strongly_decelerating {
        return format!("strongly decelerating ({:.2})", state.btc.price_acceleration);
    }

    "None".to_string()
}

async fn broadcast_edge_snapshot(
    state: &MarketState,
    bus: &EventBus,
    config: &AppConfig,
    runtime: &RuntimeConfig,
    session: &SessionState,
    _has_position: bool,
    tick_snap: Option<&TickSnapshot>,
) {
    let (threshold_mode, thresholds) =
        session.active_thresholds(&config, runtime.min_edge_pct, runtime.min_confidence);

    let ts = match tick_snap {
        Some(val) => val,
        None => {
            bus.publish(MarketEvent::EdgeSnapshot {
                market_id: String::new(),
                question: String::new(),
                poly_yes_pct: 0.0,
                poly_no_pct: 0.0,
                divergence_score: 0.0,
                expected_repricing: 0.0,
                edge_pct: 0.0,
                tradeable: false,
                direction: "—".to_string(),
                btc_price: state.btc.price,
                btc_trend: state.btc.microtrend.to_string(),
                velocity_trend: state.btc.velocity_trend.to_string(),
                time_remaining_secs: 0,
                confidence: 0.0,
                price_velocity: 0.0,
                volume_delta: 0.0,
                order_flow_ratio: 0.5,
                velocity_consistency: 0.5,
                price_acceleration: 0.0,
                missing_reason: "No active market".to_string(),
                threshold_mode: threshold_mode.as_str().to_string(),
                mins_since_last_trade: session.mins_since_last_trade(),
                active_min_edge_pct: thresholds.min_edge_pct,
                active_min_confidence: thresholds.min_confidence,
            });
            return;
        }
    };

    let market = state.btc5m_market.as_ref().unwrap();

    bus.publish(MarketEvent::EdgeSnapshot {
        market_id: market.slug.clone(),
        question: market.question.clone(),
        poly_yes_pct: market.yes_price,
        poly_no_pct: market.no_price,
        divergence_score: ts.estimate.divergence_score,
        expected_repricing: ts.estimate.expected_repricing,
        edge_pct: ts.edge_score.edge_pct,
        tradeable: ts.edge_score.tradeable,
        direction: ts.edge_score.direction.to_string(),
        btc_price: state.btc.price,
        btc_trend: state.btc.microtrend.to_string(),
        velocity_trend: state.btc.velocity_trend.to_string(),
        time_remaining_secs: market.time_remaining_secs,
        confidence: ts.edge_score.confidence,
        price_velocity: state.btc.price_velocity,
        volume_delta: state.btc.volume_delta,
        order_flow_ratio: state.btc.order_flow_ratio,
        velocity_consistency: state.btc.velocity_consistency,
        price_acceleration: state.btc.price_acceleration,
        missing_reason: ts.missing_reason.clone(),
        threshold_mode: ts.threshold_mode.as_str().to_string(),
        mins_since_last_trade: session.mins_since_last_trade(),
        active_min_edge_pct: ts.thresholds.min_edge_pct,
        active_min_confidence: ts.thresholds.min_confidence,
    });
}

async fn write_signal_state(
    conn_opt: &mut Option<redis::aio::MultiplexedConnection>,
    state: &MarketState,
    _config: &AppConfig,
    _runtime: &RuntimeConfig,
    session: &SessionState,
    _has_position: bool,
    tick_snap: Option<&TickSnapshot>,
) {
    let conn = match conn_opt.as_mut() {
        Some(c) => c,
        None => return,
    };

    let ts = match tick_snap {
        Some(val) => val,
        None => return,
    };

    let market = state.btc5m_market.as_ref().unwrap();

    let json = serde_json::json!({
        "market_id": market.slug,
        "question": market.question,
        "poly_yes_pct": market.yes_price,
        "poly_no_pct": market.no_price,
        "divergence_score": ts.estimate.divergence_score,
        "expected_repricing": ts.estimate.expected_repricing,
        "edge_pct": ts.edge_score.edge_pct,
        "tradeable": ts.edge_score.tradeable,
        "direction": ts.edge_score.direction.to_string(),
        "btc_price": state.btc.price,
        "btc_trend": state.btc.microtrend.to_string(),
        "velocity_trend": state.btc.velocity_trend.to_string(),
        "spread": market.spread,
        "time_remaining_secs": market.time_remaining_secs,
        "binance_latency_ms": state.latency.binance_ms,
        "polymarket_latency_ms": state.latency.polymarket_ms,
        "clock_offset_ms": state.latency.server_time_offset_ms,
        "confidence": ts.edge_score.confidence,
        "price_velocity": state.btc.price_velocity,
        "volume_delta": state.btc.volume_delta,
        "order_flow_ratio": state.btc.order_flow_ratio,
        "velocity_consistency": state.btc.velocity_consistency,
        "price_acceleration": state.btc.price_acceleration,
        "missing_reason": ts.missing_reason,
        "binance_ping_ms": state.latency.binance_ping_ms,
        "binance_tick_rate": state.latency.binance_tick_rate,
        "polymarket_tick_rate": state.latency.polymarket_tick_rate,
        "binance_last_msg_ms": state.latency.binance_last_msg_ms,
        "polymarket_last_msg_ms": state.latency.polymarket_last_msg_ms,
        "price_rejections": state.latency.price_rejections,
        "fallback_mode": state.latency.fallback_mode,
        "poly_last_fetched_ms": market.last_fetched_ms,
        "threshold_mode": ts.threshold_mode.as_str(),
        "mins_since_last_trade": session.mins_since_last_trade(),
        "active_min_edge_pct": ts.thresholds.min_edge_pct,
        "active_min_confidence": ts.thresholds.min_confidence,
    });

    if conn.set_ex::<_, _, ()>("polytrade:signal:latest", &json.to_string(), 60u64).await.is_err() {
        *conn_opt = None;
    }
}

// ---------------------------------------------------------------------------
// Evaluation
// ---------------------------------------------------------------------------

async fn evaluate_all(
    state: &MarketState,
    strategies: &[Box<dyn Strategy>],
    simulator: &mut PaperSimulator,
    risk: &mut RiskEngine,
    session: &mut SessionState,
    config: &AppConfig,
    bus: &EventBus,
    runtime: &RuntimeConfig,
    ts: &TickSnapshot,
) {
    // Skip if no BTC data yet.
    if state.btc.price == 0.0 {
        return;
    }

    // Check if market is active.
    let market = match &state.btc5m_market {
        Some(m) => m,
        None => return,
    };

    // Update risk settings dynamically.
    risk.config.consecutive_loss_limit = runtime.consecutive_loss_limit;
    risk.config.cooldown_after_loss_secs = runtime.cooldown_after_loss_secs;

    // Stale price check (warn if stale > price_stale_threshold_ms).
    let elapsed_ms = now_millis() - market.last_fetched_ms;
    if elapsed_ms > config.btc5m.price_stale_threshold_ms {
        use std::sync::atomic::{AtomicU64, Ordering};
        static LAST_STALE_WARN_MS: AtomicU64 = AtomicU64::new(0);
        let last_warn = LAST_STALE_WARN_MS.load(Ordering::Relaxed);
        let now_ms = now_millis();
        if now_ms - last_warn > 10_000 {
            if LAST_STALE_WARN_MS.compare_exchange(last_warn, now_ms, Ordering::Relaxed, Ordering::Relaxed).is_ok() {
                warn!(
                    elapsed_ms = %elapsed_ms,
                    slug = %market.slug,
                    "[tracker] price data stale for {}ms — trading suppressed",
                    elapsed_ms
                );
            }
        }
        return;
    }

    let mut edge_map: HashMap<String, EdgeScore> = HashMap::new();

    if config.strategy.observation_mode {
        info!(
            "[observe] edge={:+.1}% conf={:.2} vel={:.2}/s delta={:.0} time={}s yes={:.3}",
            ts.edge_score.edge_pct * 100.0,
            ts.edge_score.confidence,
            state.btc.price_velocity,
            state.btc.volume_delta,
            market.time_remaining_secs,
            market.yes_price,
        );
    }

    let max_price_age_ms = if state.latency.fallback_mode {
        config.data_quality.max_price_age_fallback_ms
    } else {
        config.data_quality.max_price_age_ms
    };

    // Entry price freshness check
    let price_age_ms = now_millis().saturating_sub(market.last_fetched_ms);
    if price_age_ms > max_price_age_ms {
        use std::sync::atomic::{AtomicU64, Ordering};
        static LAST_FRESHNESS_WARN_MS: AtomicU64 = AtomicU64::new(0);
        let last_warn = LAST_FRESHNESS_WARN_MS.load(Ordering::Relaxed);
        let now_ms = now_millis();
        if now_ms - last_warn > 10_000 {
            if LAST_FRESHNESS_WARN_MS.compare_exchange(last_warn, now_ms, Ordering::Relaxed, Ordering::Relaxed).is_ok() {
                warn!(
                    price_age_ms = %price_age_ms,
                    max_allowed = %max_price_age_ms,
                    fallback = state.latency.fallback_mode,
                    "[strategy] skipping signal — price stale: {}ms old (max {}ms)",
                    price_age_ms, max_price_age_ms
                );
            }
        }

        edge_map.insert(market.slug.clone(), ts.edge_score.clone());
        let closed = simulator.evaluate_exits(
            state,
            &edge_map,
            config,
            bus,
        );
        for trade in &closed {
            risk.record_close(trade);
        }
        return;
    }

    if !prices_valid_for_trading(market) {
        edge_map.insert(market.slug.clone(), ts.edge_score.clone());
        let closed = simulator.evaluate_exits(state, &edge_map, config, bus);
        for trade in &closed {
            risk.record_close(trade);
        }
        return;
    }

    // Suppress new trades if phase is not Active (e.g. PreOpen, Final, Settled)
    if market.phase != crate::market_data::state::MarketPhase::Active {
        // Evaluate exits on open positions.
        edge_map.insert(market.slug.clone(), ts.edge_score.clone());

        let closed = simulator.evaluate_exits(
            state,
            &edge_map,
            config,
            bus,
        );
        for trade in &closed {
            risk.record_close(trade);
        }
        return;
    }

    // Log every evaluation at debug level for observability.
    debug!(
        market_id = %market.slug,
        edge = format_args!("{:+.1}%", ts.edge_score.edge_pct * 100.0),
        direction = %ts.edge_score.direction,
        confidence = format_args!("{:.2}", ts.edge_score.confidence),
        tradeable = ts.edge_score.tradeable,
        "[eval] {}",
        market.question,
    );

    if config.strategy.observation_mode {
        edge_map.insert(market.slug.clone(), ts.edge_score.clone());
        let closed = simulator.evaluate_exits(state, &edge_map, config, bus);
        for trade in &closed {
            risk.record_close(trade);
        }
        return;
    }

    // Floor mode: accept any positive edge signal when configured
    let mut edge_for_strategies = ts.edge_score.clone();
    if ts.threshold_mode == ThresholdMode::Floor
        && config.strategy.floor_entry_if_any_signal
        && edge_for_strategies.edge_pct > ts.thresholds.min_edge_pct
        && !edge_for_strategies.tradeable
    {
        edge_for_strategies.tradeable = true;
        edge_for_strategies.reason = None;
        info!(
            "[strategy] FLOOR mode — forcing tradeable on edge {:+.1}%",
            edge_for_strategies.edge_pct * 100.0
        );
    }

    // 2. Run enabled strategies.
    for strat in strategies {
        if !strat.is_enabled() {
            continue;
        }

        match strat.evaluate(state, &edge_for_strategies, &ts.estimate).await {
            Some(mut signal) => {
                // Override size from Redis config.
                signal.size_usd = runtime.size_usd;

                // Cooldown check: direction cannot be same as last closed trade's direction if last trade was a loss.
                if let Some(last_trade) = simulator.closed_trades.last() {
                    if last_trade.pnl_usd < 0.0 && last_trade.direction == signal.direction {
                        info!(
                            market_id = %signal.market_id,
                            direction = %signal.direction,
                            "[signal] SKIP: direction cooldown active (last trade was loss)",
                        );
                        continue;
                    }
                }

                // Risk check.
                let risk_result = risk.check_allowed(&signal.market_id, simulator);

                // Duplicate position check.
                if simulator.has_position(&signal.market_id) {
                    debug!(
                        market_id = %signal.market_id,
                        "[signal] SKIP: already have open position",
                    );
                    continue;
                }

                if risk_result.allowed {
                    info!(
                        market_id = %signal.market_id,
                        direction = %signal.direction,
                        edge = format_args!("+{:.1}%", signal.edge_pct * 100.0),
                        confidence = format_args!("{:.2}", signal.confidence),
                        entry = format_args!("{:.2}", signal.target_entry_price),
                        target_exit = format_args!("{:.2}", signal.target_exit_price),
                        strategy = %signal.strategy_name,
                        "[signal] ✓ TRADE {}",
                        signal.signal_reason,
                    );
                    simulator.open_position(&signal, bus);
                    session.on_trade_executed();
                } else {
                    info!(
                        market_id = %signal.market_id,
                        direction = %signal.direction,
                        edge = format_args!("+{:.1}%", signal.edge_pct * 100.0),
                        reason = risk_result.reason.as_deref().unwrap_or("unknown"),
                        strategy = %signal.strategy_name,
                        "[signal] ✗ BLOCKED by risk",
                    );
                }
            }
            None => {
                // Log SKIP at info level for significant edges.
                if ts.edge_score.edge_pct > 0.02 {
                    info!(
                        market_id = %market.slug,
                        direction = %ts.edge_score.direction,
                        edge = format_args!("{:+.1}%", ts.edge_score.edge_pct * 100.0),
                        confidence = format_args!("{:.2}", ts.edge_score.confidence),
                        strategy = strat.name(),
                        reason = ts.edge_score.reason.as_deref().unwrap_or("strategy filter"),
                        "[signal] SKIP",
                    );
                }
            }
        }
    }

    edge_map.insert(market.slug.clone(), ts.edge_score.clone());

    // 3. Evaluate exits on open positions.
    let closed = simulator.evaluate_exits(
        state,
        &edge_map,
        config,
        bus,
    );

    // 4. Record closed trades with risk engine.
    for trade in &closed {
        risk.record_close(trade);
    }
}
