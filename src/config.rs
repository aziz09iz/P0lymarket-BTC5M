use anyhow::Context;
use config::{Config, File};
use serde::Deserialize;

use crate::error::Result;

#[derive(Debug, Deserialize, Clone)]
pub struct AppConfig {
    pub feeds: FeedsConfig,
    pub state: StateConfig,
    pub redis: RedisConfig,
    pub logging: LoggingConfig,
    pub probability: ProbabilityConfig,
    pub edge: EdgeConfig,
    pub strategy: StrategyConfig,
    pub risk: RiskConfig,
    pub paper: PaperConfig,
    pub btc5m: Btc5mConfig,
    pub data_quality: DataQualityConfig,
}

#[derive(Debug, Deserialize, Clone)]
pub struct FeedsConfig {
    pub binance_ws: String,
    pub polymarket_rest: String,
    pub polymarket_ws: String,
    pub market_filter_min_secs: i64,
    pub market_filter_max_secs: i64,
    /// Keyword to filter Polymarket market titles (case-insensitive).
    /// E.g. "BTC" will only include markets with "BTC" or "bitcoin" in title.
    pub market_keyword_filter: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct StateConfig {
    pub btc_tick_window_secs: u64,
    pub btc_max_ticks: usize,
}

#[derive(Debug, Deserialize, Clone)]
pub struct RedisConfig {
    pub url: String,
    pub publish_enabled: bool,
}

#[derive(Debug, Deserialize, Clone)]
pub struct LoggingConfig {
    pub level: String,
    pub format: String,
}

// ---------------------------------------------------------------------------
// Sprint 2 config additions
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize, Clone)]
pub struct ProbabilityConfig {
    pub max_momentum_adjustment: f64,
    pub momentum_weight: f64,
    pub delta_weight: f64,
}

#[derive(Debug, Deserialize, Clone)]
pub struct EdgeConfig {
    pub min_edge_pct: f64,
    pub min_confidence: f64,
    pub max_spread_for_trade: f64,
    pub min_time_remaining_secs: i64,
    pub max_time_remaining_secs: i64,
    /// How often to broadcast the edge snapshot to Redis (seconds).
    pub edge_broadcast_interval_secs: u64,
}

#[derive(Debug, Deserialize, Clone)]
pub struct StrategyConfig {
    /// When true, log all edge signals without executing trades (calibration mode).
    #[serde(default)]
    pub observation_mode: bool,
    #[serde(default = "default_relaxed_mode_after_mins")]
    pub relaxed_mode_after_mins: u64,
    #[serde(default = "default_relaxed_min_edge")]
    pub relaxed_min_edge_pct: f64,
    #[serde(default = "default_relaxed_min_confidence")]
    pub relaxed_min_confidence: f64,
    #[serde(default = "default_relaxed_min_velocity")]
    pub relaxed_min_velocity_abs: f64,
    #[serde(default = "default_floor_mode_after_mins")]
    pub floor_mode_after_mins: u64,
    #[serde(default = "default_floor_min_edge")]
    pub floor_min_edge_pct: f64,
    #[serde(default = "default_floor_min_confidence")]
    pub floor_min_confidence: f64,
    #[serde(default)]
    pub floor_entry_if_any_signal: bool,
    #[serde(default = "default_session_definition_mins")]
    pub session_definition_mins: u64,
    pub divergence: DivergenceConfig,
    pub exhaustion: ExhaustionConfig,
}

#[derive(Debug, Deserialize, Clone)]
pub struct DivergenceConfig {
    pub enabled: bool,
    pub market_uncertainty_min: f64,
    pub market_uncertainty_max: f64,
    pub min_velocity_abs: f64,
    pub velocity_scale: f64,
    pub delta_scale: f64,
    pub min_edge_pct: f64,
    pub min_confidence: f64,
    pub max_expected_repricing: f64,
    pub profit_target_pct: f64,
    pub stop_loss_pct: f64,
    pub exit_before_final_secs: i64,
    pub max_spread: f64,
    pub require_volume_alignment: bool,
    pub min_time_remaining_secs: i64,
    pub max_time_remaining_secs: i64,
}

#[derive(Debug, Deserialize, Clone)]
pub struct ExhaustionConfig {
    pub enabled: bool,
    #[serde(default = "default_exhaustion_min_edge")]
    pub min_edge_pct: f64,
    pub exhaustion_threshold: f64,
    pub velocity_taper_ratio: f64,
    pub profit_target_pct: f64,
    pub stop_loss_pct: f64,
    pub size_multiplier: f64,
    pub min_time_remaining_secs: i64,
    pub max_time_remaining_secs: i64,
}

#[derive(Debug, Deserialize, Clone)]
pub struct RiskConfig {
    pub max_concurrent_positions: usize,
    pub max_exposure_usd: f64,
    pub consecutive_loss_limit: u32,
    pub cooldown_after_loss_secs: u64,
    pub min_trade_interval_secs: u64,
}

#[derive(Debug, Deserialize, Clone)]
pub struct PaperConfig {
    pub enabled: bool,
    pub default_size_usd: f64,
    pub flag_suspicious_trades: bool,
    pub suspicious_pnl_threshold_pct: f64,
    pub suspicious_hold_max_secs: u64,
}

fn default_relaxed_mode_after_mins() -> u64 {
    10
}
fn default_relaxed_min_edge() -> f64 {
    0.025
}
fn default_relaxed_min_confidence() -> f64 {
    0.20
}
fn default_relaxed_min_velocity() -> f64 {
    0.05
}
fn default_floor_mode_after_mins() -> u64 {
    22
}
fn default_floor_min_edge() -> f64 {
    0.015
}
fn default_floor_min_confidence() -> f64 {
    0.15
}
fn default_session_definition_mins() -> u64 {
    30
}

fn default_exhaustion_min_edge() -> f64 {
    0.06
}

#[derive(Debug, Deserialize, Clone)]
pub struct DataQualityConfig {
    pub max_price_jump_pct_per_5s: f64,
    pub max_price_age_ms: u64,
    #[serde(default = "default_max_price_age_fallback")]
    pub max_price_age_fallback_ms: u64,
    pub ws_min_tick_rate: f64,
    pub ws_silence_reconnect_secs: u64,
    pub ws_ping_interval_secs: u64,
}

#[derive(Debug, Deserialize, Clone)]
pub struct Btc5mConfig {
    pub gamma_api: String,
    pub clob_api: String,
    pub clock_sync_interval_secs: u64,
    pub final_window_secs: u64,
    pub pre_fetch_next_secs: u64,
    pub price_stale_threshold_ms: u64,
    pub market_retry_attempts: u32,
    pub market_retry_interval_secs: u64,
    pub velocity_window_10s: u64,
    pub velocity_window_5s: u64,
    pub velocity_window_15s: u64,
}

fn default_max_price_age_fallback() -> u64 {
    8000
}

impl AppConfig {
    pub fn load() -> Result<Self> {
        let cfg = Config::builder()
            .add_source(File::with_name("config/default"))
            .build()
            .context("Failed to build config")?;

        cfg.try_deserialize::<AppConfig>()
            .context("Failed to deserialize config")
    }
}
