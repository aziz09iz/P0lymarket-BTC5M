use async_trait::async_trait;

use crate::config::ExhaustionConfig;
use crate::market_data::normalizer::now_millis;
use crate::market_data::state::MarketState;
use crate::probability::edge::{Direction, EdgeScore};
use crate::probability::estimator::ProbabilityEstimate;
use crate::strategy::traits::{Strategy, TradeSignal};

/// Exhaustion Reversal strategy (Trigger B).
pub struct ExhaustionStrategy {
    config: ExhaustionConfig,
    max_spread: f64,
    default_size_usd: f64,
}

impl ExhaustionStrategy {
    pub fn new(config: ExhaustionConfig, max_spread: f64, default_size_usd: f64) -> Self {
        Self {
            config,
            max_spread,
            default_size_usd,
        }
    }
}

#[async_trait]
impl Strategy for ExhaustionStrategy {
    fn name(&self) -> &str {
        "exhaustion"
    }

    fn is_enabled(&self) -> bool {
        self.config.enabled
    }

    async fn evaluate(
        &self,
        state: &MarketState,
        _edge: &EdgeScore,
        _estimate: &ProbabilityEstimate,
    ) -> Option<TradeSignal> {
        let market = state.btc5m_market.as_ref()?;

        // 1. Time remaining check
        let time_remaining = market.time_remaining_secs;
        if time_remaining < self.config.min_time_remaining_secs
            || time_remaining > self.config.max_time_remaining_secs
        {
            return None;
        }

        // 2. Spread check
        if market.spread > self.max_spread {
            return None;
        }

        // 3. Market overextended check
        let market_overextended = market.yes_price >= self.config.exhaustion_threshold
            || market.no_price >= self.config.exhaustion_threshold;
        if !market_overextended {
            return None;
        }

        // 4. Velocity tapering check
        // Compare last 5s avg vs last 15s avg
        let recent_vel = state.btc.velocity_5s.abs();
        let medium_vel = state.btc.velocity_15s.abs();

        // Check if recent velocity was strong but now tapering.
        // We define "strong medium velocity" as >= 0.05 $/s to avoid noise on flat markets,
        // and velocity is tapering by the configured ratio.
        if medium_vel < 0.05 {
            return None;
        }

        let velocity_tapering = recent_vel < medium_vel * self.config.velocity_taper_ratio;
        if !velocity_tapering {
            return None;
        }

        // 5. Bet opposite direction (mean reversion)
        let direction = if market.yes_price >= self.config.exhaustion_threshold {
            Direction::No // YES overshot -> bet NO
        } else {
            Direction::Yes // NO overshot -> bet YES
        };

        // 6. Determine entry price
        let entry_price = match direction {
            Direction::Yes => market.yes_price,
            Direction::No => market.no_price,
        };

        // 7. Target exit price
        let target_exit = (entry_price + self.config.profit_target_pct).min(0.98);

        // 8. Size calculation (using multiplier)
        let size_usd = self.default_size_usd * self.config.size_multiplier;

        let reason = format!(
            "exhaustion: yes={:.2} no={:.2} recent_vel={:.3} med_vel={:.3} (tapering)",
            market.yes_price,
            market.no_price,
            state.btc.velocity_5s,
            state.btc.velocity_15s,
        );

        Some(TradeSignal {
            strategy_name: self.name().to_string(),
            market_id: market.slug.clone(),
            direction,
            target_entry_price: entry_price,
            target_exit_price: target_exit,
            size_usd,
            confidence: 0.50, // default placeholder or medium confidence
            edge_pct: self.config.profit_target_pct, // for reverse trigger, edge is profit target
            signal_reason: reason,
            signal_at_ms: now_millis(),
            profit_target_pct: self.config.profit_target_pct,
            stop_loss_pct: self.config.stop_loss_pct,
            exit_before_final_secs: 30, // default to 30s final window
            price_age_ms: now_millis().saturating_sub(market.last_fetched_ms),
            price_source: market.price_source.clone(),
        })
    }
}
