mod config;
mod error;
mod events;
mod market_data;
mod paper;
mod probability;
mod risk;
mod storage;
mod strategy;

use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;

use crate::config::AppConfig;
use crate::events::bus::{EventBus, MarketEvent};
use crate::market_data::state::MarketState;

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // ── Load config ────────────────────────────────────────────────────────
    let cfg = AppConfig::load().context("Failed to load config/default.toml")?;

    // ── Init logging ───────────────────────────────────────────────────────
    init_logging(&cfg.logging.level, &cfg.logging.format);

    info!(
        binance_ws = %cfg.feeds.binance_ws,
        polymarket_ws = %cfg.feeds.polymarket_ws,
        redis_url = %cfg.redis.url,
        "PolyTrade 5M — Sprint 1 starting"
    );

    // ── Shared state ───────────────────────────────────────────────────────
    let state: Arc<RwLock<MarketState>> = Arc::new(RwLock::new(MarketState::default()));

    // ── Event bus (capacity 1024) ──────────────────────────────────────────
    let bus = EventBus::new(1_024);

    // ── Cancellation token (shared across all tasks) ───────────────────────
    let cancel = CancellationToken::new();

    // ── Spawn tasks ────────────────────────────────────────────────────────
    let cfg = Arc::new(cfg);

    // ── Initialize Redis config defaults (SET NX — won't overwrite) ─────
    let start_time_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;
    init_redis_config_defaults(&cfg, start_time_ms).await;


    // 1. Binance BTC feed
    let t_binance = {
        let (cfg, state, bus, cancel) =
            (cfg.clone(), state.clone(), bus.clone(), cancel.clone());
        tokio::spawn(async move {
            market_data::binance::run_binance_feed(cfg, state, bus, cancel).await;
        })
    };

    // 2. Polymarket BTC 5M tracker
    let t_poly = {
        let (cfg, state, bus, cancel) =
            (cfg.clone(), state.clone(), bus.clone(), cancel.clone());
        tokio::spawn(async move {
            market_data::btc5m_tracker::run_btc5m_tracker(cfg, state, bus, cancel).await;
        })
    };

    // 3. Redis publisher (subscribes to bus, publishes to Redis)
    let t_redis = {
        let (cfg, bus, cancel) = (cfg.clone(), bus.clone(), cancel.clone());
        tokio::spawn(async move {
            storage::redis::run_redis_publisher(cfg, bus, cancel).await;
        })
    };

    // 4. Health check — log state summary every 10 seconds
    let t_health = {
        let (state, cancel) = (state.clone(), cancel.clone());
        tokio::spawn(async move {
            run_health_check(state, cancel).await;
        })
    };

    // 5. BTC state snapshot publisher — writes polytrade:state:btc to Redis
    let t_btc_snapshot = {
        let (cfg, state, bus, cancel) = (cfg.clone(), state.clone(), bus.clone(), cancel.clone());
        tokio::spawn(async move {
            run_btc_snapshot_publisher(cfg, state, bus, cancel).await;
        })
    };

    // 6. Strategy engine (probability + signals + paper trading)
    let t_strategy = {
        let (cfg, state, bus, cancel) = (cfg.clone(), state.clone(), bus.clone(), cancel.clone());
        tokio::spawn(async move {
            strategy::engine::run_strategy_engine(cfg, state, bus, cancel).await;
        })
    };

    // ── Wait for CTRL+C ────────────────────────────────────────────────────
    match tokio::signal::ctrl_c().await {
        Ok(()) => info!("CTRL+C received — initiating graceful shutdown"),
        Err(e) => error!(error = %e, "Failed to listen for CTRL+C signal"),
    }

    cancel.cancel();

    // Give tasks a moment to wrap up.
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Abort any stragglers.
    t_binance.abort();
    t_poly.abort();
    t_redis.abort();
    t_health.abort();
    t_btc_snapshot.abort();
    t_strategy.abort();

    info!("PolyTrade 5M shut down cleanly");
    Ok(())
}

// ---------------------------------------------------------------------------
// Health check task
// ---------------------------------------------------------------------------

async fn run_health_check(state: Arc<RwLock<MarketState>>, cancel: CancellationToken) {
    let mut interval = tokio::time::interval(Duration::from_secs(10));
    interval.tick().await; // skip immediate first tick

    loop {
        tokio::select! {
            _ = interval.tick() => {
                let s = state.read().await;

                let active_slug = s.btc5m_market.as_ref().map(|m| m.slug.as_str()).unwrap_or("None");
                let active_phase = s.btc5m_market.as_ref().map(|m| format!("{:?}", m.phase)).unwrap_or_else(|| "None".to_string());
                let next_slug = s.next_market.as_ref().map(|m| m.slug.as_str()).unwrap_or("None");
                let btc = &s.btc;
                let lat = &s.latency;

                info!(
                    btc_price = format_args!("${:.0}", btc.price),
                    vel = format_args!("{:+.2}/s", btc.price_velocity),
                    delta = format_args!("{:+.0}", btc.volume_delta),
                    trend = %btc.microtrend,
                    active_market = %active_slug,
                    active_phase = %active_phase,
                    next_market = %next_slug,
                    latency_binance_ms = lat.binance_ms,
                    latency_polymarket_ms = lat.polymarket_ms,
                    "[health]"
                );

                // Also print a compact human-readable line for quick terminal reading.
                println!(
                    "[health] BTC: ${:.0} vel={:+.2}/s delta={:+.0} trend={} | Active: {} ({}) | Next: {} | Latency: binance={}ms poly={}ms",
                    btc.price,
                    btc.price_velocity,
                    btc.volume_delta,
                    btc.microtrend,
                    active_slug,
                    active_phase,
                    next_slug,
                    lat.binance_ms,
                    lat.polymarket_ms,
                );

                // Log active market details at debug level.
                if let Some(ref ps) = s.btc5m_market {
                    tracing::debug!(
                        market_id = %ps.condition_id,
                        slug = %ps.slug,
                        question = %ps.question,
                        yes = ps.yes_price,
                        no = ps.no_price,
                        secs_left = ps.time_remaining_secs,
                        "btc5m_market"
                    );
                }
            }
            _ = cancel.cancelled() => {
                info!("Health check task shut down");
                break;
            }
        }
    }
}

// ---------------------------------------------------------------------------
// BTC state snapshot publisher
// ---------------------------------------------------------------------------
// Publishes `polytrade:state:btc` (SET EX 60) on every BTC tick by
// subscribing to the event bus — keeps Redis state current without the
// Redis publisher task needing access to the full BtcState struct.

async fn run_btc_snapshot_publisher(
    config: Arc<AppConfig>,
    state: Arc<RwLock<MarketState>>,
    bus: EventBus,
    cancel: CancellationToken,
) {
    if !config.redis.publish_enabled {
        return;
    }

    let client = match redis::Client::open(config.redis.url.as_str()) {
        Ok(c) => c,
        Err(e) => {
            warn!(error = %e, "BTC snapshot publisher: Redis client error");
            return;
        }
    };

    let mut conn = match client.get_multiplexed_async_connection().await {
        Ok(c) => c,
        Err(e) => {
            warn!(error = %e, "BTC snapshot publisher: cannot connect to Redis");
            return;
        }
    };

    let mut rx = bus.subscribe();

    loop {
        tokio::select! {
            result = rx.recv() => {
                match result {
                    Ok(MarketEvent::BtcTick(_)) => {
                        // Snapshot the full BtcState on each tick.
                        let btc_state = {
                            let s = state.read().await;
                            s.btc.clone()
                        };
                        match serde_json::to_string(&btc_state) {
                            Ok(json) => {
                                use redis::AsyncCommands;
                                if let Err(e) = conn
                                    .set_ex::<_, _, ()>("polytrade:state:btc", &json, 60u64)
                                    .await
                                {
                                    warn!(error = %e, "Failed to write polytrade:state:btc");
                                }
                            }
                            Err(e) => {
                                warn!(error = %e, "Failed to serialize BtcState");
                            }
                        }
                    }
                    Ok(_) => {} // other event types — ignore
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                        warn!(skipped = n, "BTC snapshot publisher lagged");
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                }
            }
            _ = cancel.cancelled() => break,
        }
    }
}

// ---------------------------------------------------------------------------
// Redis config initialization
// ---------------------------------------------------------------------------

/// Initialize Redis config keys with defaults using SET NX (won't overwrite).
/// This ensures the Telegram bot can read sensible defaults even if the user
/// hasn't set anything yet. Non-fatal: silently skips if Redis is unavailable.
async fn init_redis_config_defaults(config: &AppConfig, start_time_ms: u64) {
    let client = match redis::Client::open(config.redis.url.as_str()) {
        Ok(c) => c,
        Err(_) => return,
    };

    let mut conn = match client.get_multiplexed_async_connection().await {
        Ok(c) => c,
        Err(_) => return,
    };

    use redis::AsyncCommands;

    let defaults: Vec<(&str, String)> = vec![
        ("polytrade:config:mode", "paper".to_string()),
        ("polytrade:config:paused", "false".to_string()),
        (
            "polytrade:config:size_usd",
            format!("{:.2}", config.paper.default_size_usd),
        ),
        (
            "polytrade:config:max_concurrent_positions",
            config.risk.max_concurrent_positions.to_string(),
        ),
        (
            "polytrade:config:max_exposure_usd",
            format!("{:.2}", config.risk.max_exposure_usd),
        ),
        (
            "polytrade:config:consecutive_loss_limit",
            config.risk.consecutive_loss_limit.to_string(),
        ),
        (
            "polytrade:config:cooldown_after_loss_secs",
            config.risk.cooldown_after_loss_secs.to_string(),
        ),
        (
            "polytrade:config:exit_before_final_secs",
            config.strategy.divergence.exit_before_final_secs.to_string(),
        ),
        // Sprint 3.5: risk fine-tuning keys
        (
            "polytrade:config:stop_loss_pct",
            format!("{:.2}", config.strategy.divergence.stop_loss_pct * 100.0),
        ),
        (
            "polytrade:config:profit_target_pct",
            format!("{:.2}", config.strategy.divergence.profit_target_pct * 100.0),
        ),
        (
            "polytrade:config:min_edge_pct",
            format!("{:.2}", config.strategy.divergence.min_edge_pct * 100.0),
        ),
        (
            "polytrade:config:min_trade_interval_secs",
            config.risk.min_trade_interval_secs.to_string(),
        ),
    ];

    for (key, value) in &defaults {
        // SET NX — only set if the key does not already exist.
        let _: Result<bool, _> = conn.set_nx(key, value).await;
    }

    // Write engine state (always overwrite — this is runtime info).
    let engine_state = serde_json::json!({
        "version": env!("CARGO_PKG_VERSION"),
        "start_time_ms": start_time_ms,
        "mode": "paper",
    });
    let _: Result<(), _> = conn
        .set_ex("polytrade:state:engine", &engine_state.to_string(), 86400u64)
        .await;

    info!("Redis config defaults initialized (SET NX)");
}

// ---------------------------------------------------------------------------
// Logging setup
// ---------------------------------------------------------------------------

fn init_logging(level: &str, format: &str) {
    let filter = EnvFilter::try_new(level).unwrap_or_else(|_| EnvFilter::new("info"));

    match format {
        "json" => {
            tracing_subscriber::fmt()
                .with_env_filter(filter)
                .json()
                .init();
        }
        _ => {
            tracing_subscriber::fmt()
                .with_env_filter(filter)
                .pretty()
                .init();
        }
    }
}
