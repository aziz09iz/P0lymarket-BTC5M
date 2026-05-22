use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::market_data::state::MarketState;
use crate::probability::edge::{Direction, EdgeScore};
use crate::probability::estimator::ProbabilityEstimate;

// ---------------------------------------------------------------------------
// Strategy trait
// ---------------------------------------------------------------------------

/// All strategies must implement this trait.
///
/// The engine calls `evaluate()` for each active market on every event-bus tick.
/// Returning `Some(TradeSignal)` means the strategy wants to open a position.
#[async_trait]
pub trait Strategy: Send + Sync {
    /// Human-readable name for logging.
    fn name(&self) -> &str;

    /// Whether this strategy is currently enabled (from config).
    fn is_enabled(&self) -> bool;

    /// Evaluate whether to enter a trade.
    ///
    /// The strategy should NOT check risk limits — that is the risk engine's job.
    /// It should only check market conditions and signal quality.
    async fn evaluate(
        &self,
        state: &MarketState,
        edge: &EdgeScore,
        estimate: &ProbabilityEstimate,
    ) -> Option<TradeSignal>;
}

// ---------------------------------------------------------------------------
// Trade signal
// ---------------------------------------------------------------------------

/// A signal to open a paper position.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TradeSignal {
    pub strategy_name: String,
    pub market_id: String,
    pub direction: Direction,
    /// Suggested entry price (yes_price or no_price depending on direction).
    pub target_entry_price: f64,
    /// Estimated exit probability.
    pub target_exit_price: f64,
    /// Dollar size for the position.
    pub size_usd: f64,
    pub confidence: f64,
    pub edge_pct: f64,
    /// Human-readable explanation of the signal.
    pub signal_reason: String,
    pub signal_at_ms: u64,
    pub profit_target_pct: f64,
    pub stop_loss_pct: f64,
    pub exit_before_final_secs: i64,
    pub price_age_ms: u64,
    pub price_source: String,
}

// ---------------------------------------------------------------------------
// Exit reason
// ---------------------------------------------------------------------------

/// Why a paper position was closed.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ExitReason {
    ProfitTarget,
    StopLoss,
    TimeExit,
    EdgeGone,
}

impl std::fmt::Display for ExitReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ExitReason::ProfitTarget => write!(f, "ProfitTarget"),
            ExitReason::StopLoss => write!(f, "StopLoss"),
            ExitReason::TimeExit => write!(f, "TimeExit"),
            ExitReason::EdgeGone => write!(f, "EdgeGone"),
        }
    }
}
