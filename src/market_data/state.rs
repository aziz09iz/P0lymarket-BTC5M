use std::collections::VecDeque;

use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Primitive types
// ---------------------------------------------------------------------------

/// Which side initiated the trade (taker perspective).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TradeSide {
    Buy,
    Sell,
}

/// Simple momentum classification derived from price velocity + volume delta.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum MicroTrend {
    Bullish,
    Bearish,
    Choppy,
}

impl std::fmt::Display for MicroTrend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MicroTrend::Bullish => write!(f, "Bullish"),
            MicroTrend::Bearish => write!(f, "Bearish"),
            MicroTrend::Choppy => write!(f, "Choppy"),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
pub enum VelocityTrend {
    Accelerating,
    Stable,
    Tapering,
}

impl std::fmt::Display for VelocityTrend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            VelocityTrend::Accelerating => write!(f, "Accelerating"),
            VelocityTrend::Stable => write!(f, "Stable"),
            VelocityTrend::Tapering => write!(f, "Tapering"),
        }
    }
}

// ---------------------------------------------------------------------------
// Tick
// ---------------------------------------------------------------------------

/// A single BTC trade tick from Binance.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BtcTick {
    pub price: f64,
    pub quantity: f64,
    pub side: TradeSide,
    pub timestamp_ms: u64,
    pub event_time_ms: u64,
}

// ---------------------------------------------------------------------------
// BTC state
// ---------------------------------------------------------------------------

/// Rolling BTC market state maintained in-memory.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BtcState {
    /// Latest trade price.
    pub price: f64,
    /// Price change per second over the 10s window.
    pub price_velocity: f64,
    /// Very short term (5s) velocity.
    pub velocity_5s: f64,
    /// Medium term (15s) velocity.
    pub velocity_15s: f64,
    /// Velocity trend (Accelerating / Stable / Tapering).
    pub velocity_trend: VelocityTrend,
    /// buy_volume − sell_volume over the rolling window.
    pub volume_delta: f64,
    /// Rolling standard deviation of price over the window.
    pub volatility: f64,
    /// Momentum classification.
    pub microtrend: MicroTrend,
    /// Raw tick history (capped at `btc_max_ticks` and `btc_tick_window_secs`).
    pub tick_history: VecDeque<BtcTick>,
}

impl Default for BtcState {
    fn default() -> Self {
        Self {
            price: 0.0,
            price_velocity: 0.0,
            velocity_5s: 0.0,
            velocity_15s: 0.0,
            velocity_trend: VelocityTrend::Stable,
            volume_delta: 0.0,
            volatility: 0.0,
            microtrend: MicroTrend::Choppy,
            tick_history: VecDeque::new(),
        }
    }
}

impl BtcState {
    /// Ingest a new tick, trim the window, and recompute all rolling metrics.
    pub fn push_tick(
        &mut self,
        tick: BtcTick,
        max_ticks: usize,
        window_secs: u64,
        w_5s: u64,
        w_10s: u64,
        w_15s: u64,
    ) {
        self.price = tick.price;
        self.tick_history.push_back(tick);

        // ── Trim by count ──────────────────────────────────────────────────
        while self.tick_history.len() > max_ticks {
            self.tick_history.pop_front();
        }

        // ── Trim by time window ────────────────────────────────────────────
        if let Some(latest) = self.tick_history.back() {
            let cutoff_ms = latest.timestamp_ms.saturating_sub(window_secs * 1_000);
            while self
                .tick_history
                .front()
                .map(|t| t.timestamp_ms < cutoff_ms)
                .unwrap_or(false)
            {
                self.tick_history.pop_front();
            }
        }

        self.compute_metrics(w_5s, w_10s, w_15s);
    }

    fn compute_velocity_helper(ticks: &VecDeque<BtcTick>, window_secs: u64, latest_ms: u64) -> f64 {
        let cutoff_ms = latest_ms.saturating_sub(window_secs * 1000);
        let relevant: Vec<&BtcTick> = ticks
            .iter()
            .filter(|t| t.timestamp_ms >= cutoff_ms)
            .collect();

        if relevant.len() < 2 {
            return 0.0;
        }

        let first = relevant.first().unwrap();
        let last = relevant.last().unwrap();
        let duration_secs = (last.timestamp_ms.saturating_sub(first.timestamp_ms)) as f64 / 1000.0;

        if duration_secs < 1.0 {
            return 0.0;
        }
        (last.price - first.price) / duration_secs
    }

    fn compute_metrics(&mut self, w_5s: u64, w_10s: u64, w_15s: u64) {
        let n = self.tick_history.len();
        if n == 0 {
            return;
        }

        let latest_ms = self.tick_history[n - 1].timestamp_ms;

        // ── Price velocity ($/s) over different windows ───────────────────
        self.velocity_5s = Self::compute_velocity_helper(&self.tick_history, w_5s, latest_ms);
        self.price_velocity = Self::compute_velocity_helper(&self.tick_history, w_10s, latest_ms);
        self.velocity_15s = Self::compute_velocity_helper(&self.tick_history, w_15s, latest_ms);

        // ── Velocity trend classification ──────────────────────────────────
        let abs_5s = self.velocity_5s.abs();
        let abs_15s = self.velocity_15s.abs();
        self.velocity_trend = if abs_5s > abs_15s * 1.3 {
            VelocityTrend::Accelerating
        } else if abs_5s < abs_15s * 0.5 {
            VelocityTrend::Tapering
        } else {
            VelocityTrend::Stable
        };

        // ── Volume delta ───────────────────────────────────────────────────
        let (buy_vol, sell_vol) =
            self.tick_history
                .iter()
                .fold((0.0_f64, 0.0_f64), |(b, s), tick| match tick.side {
                    TradeSide::Buy => (b + tick.quantity, s),
                    TradeSide::Sell => (b, s + tick.quantity),
                });
        self.volume_delta = buy_vol - sell_vol;

        // ── Volatility (std dev of price) ──────────────────────────────────
        let prices: Vec<f64> = self.tick_history.iter().map(|t| t.price).collect();
        let mean = prices.iter().sum::<f64>() / n as f64;
        let variance = prices.iter().map(|p| (p - mean).powi(2)).sum::<f64>() / n as f64;
        self.volatility = variance.sqrt();

        // ── Microtrend ─────────────────────────────────────────────────────
        // Simple: velocity threshold $0.05/s combined with delta sign.
        const VEL_THRESHOLD: f64 = 0.05;
        self.microtrend = if self.price_velocity > VEL_THRESHOLD && self.volume_delta > 0.0 {
            MicroTrend::Bullish
        } else if self.price_velocity < -VEL_THRESHOLD && self.volume_delta < 0.0 {
            MicroTrend::Bearish
        } else {
            MicroTrend::Choppy
        };
    }
}

// ---------------------------------------------------------------------------
// Polymarket state
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PolymarketState {
    pub market_id: String,
    pub question: String,
    pub yes_price: f64,
    pub no_price: f64,
    /// yes_ask − yes_bid (0 if order book data not available)
    pub spread: f64,
    pub volume_24h: f64,
    /// Seconds until market close (negative = already closed)
    pub time_remaining_secs: i64,
    pub last_updated_ms: u64,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum MarketPhase {
    PreOpen,
    Active,
    Final,
    Settled,
}

impl std::fmt::Display for MarketPhase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MarketPhase::PreOpen => write!(f, "PreOpen"),
            MarketPhase::Active => write!(f, "Active"),
            MarketPhase::Final => write!(f, "Final"),
            MarketPhase::Settled => write!(f, "Settled"),
        }
    }
}

/// Apply a CLOB price update for one outcome token; keeps YES + NO summing to 1.0.
/// Returns `Ok(true)` if applied, `Ok(false)` if skipped (out of range / unknown token).
pub fn apply_price_update(
    market: &mut Btc5mMarket,
    asset_id: &str,
    raw_price: f64,
    now_ms: u64,
) -> anyhow::Result<bool> {
    if raw_price < 0.01 || raw_price > 0.99 {
        tracing::warn!(
            "[price] rejected out-of-range price: {:.4} for asset {}",
            raw_price,
            asset_id
        );
        return Ok(false);
    }

    let yes_prefix = &market.yes_token_id[..market.yes_token_id.len().min(8)];
    let no_prefix = &market.no_token_id[..market.no_token_id.len().min(8)];

    if asset_id == market.yes_token_id {
        let old = market.yes_price;
        market.yes_price = raw_price;
        market.no_price = (1.0 - raw_price).max(0.01);
        tracing::debug!("[price] YES updated: {:.4} → {:.4}", old, raw_price);
    } else if asset_id == market.no_token_id {
        let old = market.no_price;
        market.no_price = raw_price;
        market.yes_price = (1.0 - raw_price).max(0.01);
        tracing::debug!("[price] NO updated: {:.4} → {:.4}", old, raw_price);
    } else {
        tracing::warn!(
            "[price] unknown asset_id: {} | known YES={} NO={}",
            asset_id,
            yes_prefix,
            no_prefix
        );
        return Ok(false);
    }

    let sum = market.yes_price + market.no_price;
    if (sum - 1.0).abs() > 0.05 {
        tracing::warn!(
            "[price] sum != 1.0: yes={:.4} no={:.4} sum={:.4} — possible parse error",
            market.yes_price,
            market.no_price,
            sum
        );
    }

    market.last_fetched_ms = now_ms;
    Ok(true)
}

/// Pre-trade sanity checks for YES/NO prices.
pub fn prices_valid_for_trading(market: &Btc5mMarket) -> bool {
    let sum = market.yes_price + market.no_price;
    if (sum - 1.0).abs() > 0.05 {
        tracing::warn!(
            "[strategy] skipping — price sum invalid: yes={:.4} no={:.4} sum={:.4}",
            market.yes_price,
            market.no_price,
            sum
        );
        return false;
    }
    if (market.yes_price - 0.5).abs() < 0.0001 && (market.no_price - 0.5).abs() < 0.0001 {
        tracing::warn!("[strategy] skipping — price stuck at 50/50");
        return false;
    }
    if market.yes_price < 0.02 || market.no_price < 0.02 {
        tracing::warn!(
            "[strategy] skipping — extreme price: yes={:.3} no={:.3}",
            market.yes_price,
            market.no_price
        );
        return false;
    }
    true
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Btc5mMarket {
    pub slug: String,                    // "btc-updown-5m-1748823600"
    pub condition_id: String,            // Polymarket internal ID (from API)
    pub yes_token_id: String,            // CLOB token ID for YES shares
    pub no_token_id: String,             // CLOB token ID for NO shares
    pub question: String,                // Question title
    pub window_start_unix: u64,          // epoch seconds, divisible by 300
    pub window_end_unix: u64,            // window_start + 300
    pub phase: MarketPhase,
    pub yes_price: f64,                  // 0.0–1.0
    pub no_price: f64,
    pub spread: f64,
    pub volume_24h: f64,
    pub time_remaining_secs: i64,        // computed dynamically or stored
    pub last_fetched_ms: u64,
    pub price_source: String,
}

// ---------------------------------------------------------------------------
// Latency tracking (updated by feed tasks)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct LatencyState {
    pub binance_ms: u64,
    pub polymarket_ms: u64,
    pub server_time_offset_ms: i64,
    pub binance_ping_ms: u64,
    pub binance_tick_rate: f64,
    pub polymarket_tick_rate: f64,
    pub binance_last_msg_ms: u64,
    pub polymarket_last_msg_ms: u64,
    pub price_rejections: u64,
    pub fallback_mode: bool,
}

// ---------------------------------------------------------------------------
// Top-level shared state
// ---------------------------------------------------------------------------

/// The single source of truth shared across all tasks via `Arc<RwLock<MarketState>>`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketState {
    pub btc: BtcState,
    pub btc5m_market: Option<Btc5mMarket>,
    pub next_market: Option<Btc5mMarket>,
    pub latency: LatencyState,
    pub last_updated_ms: u64,
}

impl Default for MarketState {
    fn default() -> Self {
        Self {
            btc: BtcState::default(),
            btc5m_market: None,
            next_market: None,
            latency: LatencyState::default(),
            last_updated_ms: 0,
        }
    }
}
