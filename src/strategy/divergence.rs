use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::RwLock;

use crate::config::DivergenceConfig;
use crate::market_data::normalizer::now_millis;
use crate::market_data::state::MarketState;
use crate::probability::edge::{Direction, EdgeScore};
use crate::probability::estimator::ProbabilityEstimate;
use crate::strategy::traits::{Strategy, TradeSignal};

/// Primary strategy: BTC-Polymarket Divergence (Trigger A).
pub struct DivergenceStrategy {
    config: Arc<RwLock<DivergenceConfig>>,
    default_size_usd: f64,
}

impl DivergenceStrategy {
    pub fn new(config: Arc<RwLock<DivergenceConfig>>, default_size_usd: f64) -> Self {
        Self {
            config,
            default_size_usd,
        }
    }
}

#[async_trait]
impl Strategy for DivergenceStrategy {
    fn name(&self) -> &str {
        "divergence"
    }

    fn is_enabled(&self) -> bool {
        // block_on equivalent not available — use try_read; config rarely contended
        self.config
            .try_read()
            .map(|c| c.enabled)
            .unwrap_or(true)
    }

    async fn evaluate(
        &self,
        state: &MarketState,
        edge: &EdgeScore,
        estimate: &ProbabilityEstimate,
    ) -> Option<TradeSignal> {
        let market = state.btc5m_market.as_ref()?;
        let cfg = self.config.read().await;

        // 1. Edge must be tradeable.
        if !edge.tradeable {
            return None;
        }

        // 2. BTC velocity must be moving significantly.
        let vel_abs = state.btc.price_velocity.abs();
        if vel_abs < cfg.min_velocity_abs {
            return None;
        }

        // 3. Microtrend must not be Choppy.
        if state.btc.microtrend == crate::market_data::state::MicroTrend::Choppy {
            return None;
        }

        // 4. Volume alignment: delta signum must match velocity signum.
        if cfg.require_volume_alignment {
            if state.btc.volume_delta.signum() != state.btc.price_velocity.signum() {
                return None;
            }
        }

        // 5. Entry price is yes_price or no_price.
        let entry_price = match edge.direction {
            Direction::Yes => market.yes_price,
            Direction::No => market.no_price,
        };

        // 6. Target exit price.
        let target_exit = (entry_price + cfg.profit_target_pct).min(0.98);

        let reason = format!(
            "divergence={:.2} edge={:+.1}% conf={:.2} entry={:.2} velocity={:+.2}/s delta={:+.0}",
            estimate.divergence_score,
            edge.edge_pct * 100.0,
            edge.confidence,
            entry_price,
            state.btc.price_velocity,
            state.btc.volume_delta,
        );

        Some(TradeSignal {
            strategy_name: self.name().to_string(),
            market_id: estimate.market_id.clone(),
            direction: edge.direction.clone(),
            target_entry_price: entry_price,
            target_exit_price: target_exit,
            size_usd: self.default_size_usd,
            confidence: edge.confidence,
            edge_pct: edge.edge_pct,
            signal_reason: reason,
            signal_at_ms: now_millis(),
            profit_target_pct: cfg.profit_target_pct,
            stop_loss_pct: cfg.stop_loss_pct,
            exit_before_final_secs: cfg.exit_before_final_secs,
            price_age_ms: now_millis().saturating_sub(market.last_fetched_ms),
            price_source: market.price_source.clone(),
        })
    }
}
