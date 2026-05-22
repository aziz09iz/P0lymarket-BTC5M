use async_trait::async_trait;

use crate::market_data::state::MarketState;
use crate::probability::edge::EdgeScore;
use crate::probability::estimator::ProbabilityEstimate;
use crate::strategy::traits::{Strategy, TradeSignal};

/// Failed Breakout Trap strategy (SECONDARY — not yet implemented).
///
/// Placeholder that compiles and integrates with the strategy engine
/// but always returns `None`. To be implemented in a future sprint.
pub struct FailedBreakoutStrategy;

impl FailedBreakoutStrategy {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl Strategy for FailedBreakoutStrategy {
    fn name(&self) -> &str {
        "failed_breakout"
    }

    fn is_enabled(&self) -> bool {
        false // disabled until implemented
    }

    async fn evaluate(
        &self,
        _state: &MarketState,
        _edge: &EdgeScore,
        _estimate: &ProbabilityEstimate,
    ) -> Option<TradeSignal> {
        None
    }
}
