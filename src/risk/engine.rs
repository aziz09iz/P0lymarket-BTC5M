use std::collections::HashMap;

use tracing::{info, warn};

use crate::config::RiskConfig;
use crate::market_data::normalizer::now_millis;
use crate::paper::simulator::{ClosedTrade, PaperSimulator};

/// Result of a risk check.
#[derive(Debug, Clone)]
pub struct RiskResult {
    pub allowed: bool,
    pub reason: Option<String>,
}

/// Risk engine — validates signals before paper positions are opened.
///
/// In Sprint 2 this only validates (no real capital interaction).
pub struct RiskEngine {
    pub config: RiskConfig,
    pub consecutive_losses: u32,
    pub cooldown_until_ms: Option<u64>,
    pub last_trade_per_market: HashMap<String, u64>,
}

impl RiskEngine {
    pub fn new(config: RiskConfig) -> Self {
        Self {
            config,
            consecutive_losses: 0,
            cooldown_until_ms: None,
            last_trade_per_market: HashMap::new(),
        }
    }

    /// Check whether a new trade is allowed for the given market.
    pub fn check_allowed(
        &self,
        market_id: &str,
        simulator: &PaperSimulator,
    ) -> RiskResult {
        let now = now_millis();

        // 1. Cooldown check (consecutive losses).
        if let Some(cooldown_until) = self.cooldown_until_ms {
            if now < cooldown_until {
                let remaining_s = (cooldown_until - now) / 1000;
                let reason = format!(
                    "cooldown active until +{}s ({} consecutive losses)",
                    remaining_s, self.consecutive_losses,
                );
                warn!("[risk] BLOCKED: {}", reason);
                return RiskResult {
                    allowed: false,
                    reason: Some(reason),
                };
            }
        }

        // 2. Max concurrent positions.
        if simulator.positions.len() >= self.config.max_concurrent_positions {
            let reason = format!(
                "max concurrent positions {} reached",
                self.config.max_concurrent_positions,
            );
            info!("[risk] BLOCKED: {}", reason);
            return RiskResult {
                allowed: false,
                reason: Some(reason),
            };
        }

        // 3. Max dollar exposure.
        let exposure = simulator.total_exposure_usd();
        if exposure >= self.config.max_exposure_usd {
            let reason = format!(
                "max exposure ${:.2} reached (current ${:.2})",
                self.config.max_exposure_usd, exposure,
            );
            info!("[risk] BLOCKED: {}", reason);
            return RiskResult {
                allowed: false,
                reason: Some(reason),
            };
        }

        // 4. Min time between trades on same market.
        if let Some(&last_trade_ms) = self.last_trade_per_market.get(market_id) {
            let elapsed_s = now.saturating_sub(last_trade_ms) / 1000;
            if elapsed_s < self.config.min_trade_interval_secs {
                let reason = format!(
                    "min trade interval: {}s since last trade on {} (min {}s)",
                    elapsed_s, market_id, self.config.min_trade_interval_secs,
                );
                return RiskResult {
                    allowed: false,
                    reason: Some(reason),
                };
            }
        }

        RiskResult {
            allowed: true,
            reason: None,
        }
    }

    /// Record a closed trade — updates consecutive loss counter and cooldown.
    pub fn record_close(&mut self, trade: &ClosedTrade) {
        let now = now_millis();

        // Track last trade time per market.
        self.last_trade_per_market
            .insert(trade.market_id.clone(), now);

        if trade.pnl_usd < 0.0 {
            self.consecutive_losses += 1;
            if self.consecutive_losses >= self.config.consecutive_loss_limit {
                let cooldown_ms = self.config.cooldown_after_loss_secs * 1000;
                self.cooldown_until_ms = Some(now + cooldown_ms);
                warn!(
                    "[risk] {} consecutive losses — cooldown for {}s",
                    self.consecutive_losses,
                    self.config.cooldown_after_loss_secs,
                );
            }
        } else {
            // Win resets the consecutive loss counter.
            self.consecutive_losses = 0;
            self.cooldown_until_ms = None;
        }
    }
}
