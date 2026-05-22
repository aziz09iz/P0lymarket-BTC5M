use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use tracing::info;


use crate::events::bus::{EventBus, MarketEvent};
use crate::market_data::normalizer::now_millis;
use crate::market_data::state::MarketState;
use crate::probability::edge::{Direction, EdgeScore};
use crate::strategy::traits::{ExitReason, TradeSignal};

// ---------------------------------------------------------------------------
// Paper position (open)
// ---------------------------------------------------------------------------

/// An open paper position.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PaperPosition {
    pub market_id: String,
    pub direction: Direction,
    pub entry_price: f64,
    pub size_usd: f64,
    /// size_usd / entry_price
    pub share_qty: f64,
    pub entry_at_ms: u64,
    pub strategy_name: String,
    pub edge_pct: f64,
    pub profit_target_pct: f64,
    pub stop_loss_pct: f64,
    pub exit_before_final_secs: i64,
}

// ---------------------------------------------------------------------------
use crate::config::AppConfig;

// Closed trade
// ---------------------------------------------------------------------------

/// A completed paper trade with PnL.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClosedTrade {
    pub market_id: String,
    pub direction: Direction,
    pub entry_price: f64,
    pub exit_price: f64,
    pub size_usd: f64,
    pub pnl_usd: f64,
    pub pnl_pct: f64,
    pub hold_duration_ms: u64,
    pub exit_reason: ExitReason,
    pub strategy_name: String,
    pub edge_pct: f64,
    pub is_suspicious: bool,
    pub suspicious_reason: Option<String>,
}

pub struct PaperTradeValidator {
    pub max_realistic_pnl_pct: f64,
    pub max_hold_to_profit_ratio: f64,
}

pub enum TradeValidity {
    Valid,
    Suspicious { reason: String },
}

impl PaperTradeValidator {
    pub fn validate_closed_trade(&self, trade: &ClosedTrade) -> TradeValidity {
        let pnl_pct = trade.pnl_pct.abs();
        let hold_secs = trade.hold_duration_ms / 1000;

        // Flag: >50% PnL in under 30 seconds = stale data artifact
        if pnl_pct > self.max_realistic_pnl_pct && hold_secs < self.max_hold_to_profit_ratio as u64 {
            return TradeValidity::Suspicious {
                reason: format!(
                    "+{:.0}% in {}s — likely stale price artifact, not real edge",
                    pnl_pct, hold_secs
                ),
            };
        }

        // Flag: entry price was unusually far from market mid
        if trade.entry_price < 0.05 || trade.entry_price > 0.95 {
            return TradeValidity::Suspicious {
                reason: format!(
                    "entry price {:.3} is extreme — outside normal range",
                    trade.entry_price
                ),
            };
        }

        TradeValidity::Valid
    }
}

// ---------------------------------------------------------------------------
// Simulator
// ---------------------------------------------------------------------------

/// Paper trade simulator — tracks open positions and closed trade history.
pub struct PaperSimulator {
    pub positions: HashMap<String, PaperPosition>,
    pub closed_trades: Vec<ClosedTrade>,
    pub total_pnl_usd: f64,
    pub win_count: u64,
    pub loss_count: u64,
}

impl PaperSimulator {
    pub fn new() -> Self {
        Self {
            positions: HashMap::new(),
            closed_trades: Vec::new(),
            total_pnl_usd: 0.0,
            win_count: 0,
            loss_count: 0,
        }
    }

    /// Check if there is already an open position for this market.
    pub fn has_position(&self, market_id: &str) -> bool {
        self.positions.contains_key(market_id)
    }

    /// Current total exposure in USD across all open positions.
    pub fn total_exposure_usd(&self) -> f64 {
        self.positions.values().map(|p| p.size_usd).sum()
    }

    /// Open a new paper position from a trade signal.
    pub fn open_position(&mut self, signal: &TradeSignal, bus: &EventBus) -> PaperPosition {
        let entry_price = signal.target_entry_price;
        let share_qty = if entry_price > 0.0 {
            signal.size_usd / entry_price
        } else {
            0.0
        };

        let position = PaperPosition {
            market_id: signal.market_id.clone(),
            direction: signal.direction.clone(),
            entry_price,
            size_usd: signal.size_usd,
            share_qty,
            entry_at_ms: now_millis(),
            strategy_name: signal.strategy_name.clone(),
            edge_pct: signal.edge_pct,
            profit_target_pct: signal.profit_target_pct,
            stop_loss_pct: signal.stop_loss_pct,
            exit_before_final_secs: signal.exit_before_final_secs,
        };

        info!(
            market_id = %position.market_id,
            direction = %position.direction,
            entry_price = format_args!("{:.2}", position.entry_price),
            size_usd = format_args!("${:.2}", position.size_usd),
            shares = format_args!("{:.2}", position.share_qty),
            "[paper] OPEN {} {} @ {:.2} size=${:.2} shares={:.2}",
            position.direction,
            position.market_id,
            position.entry_price,
            position.size_usd,
            position.share_qty,
        );

        bus.publish(MarketEvent::PaperTradeOpen(position.clone()));
        self.positions.insert(signal.market_id.clone(), position.clone());
        position
    }

    /// Evaluate exit conditions on all open positions.
    ///
    /// Returns a list of trades that were closed this tick.
    pub fn evaluate_exits(
        &mut self,
        state: &MarketState,
        _edge_map: &HashMap<String, EdgeScore>,
        config: &AppConfig,
        bus: &EventBus,
    ) -> Vec<ClosedTrade> {
        let now = now_millis();
        let mut to_close: Vec<(String, f64, ExitReason)> = Vec::new();

        for (market_id, pos) in &self.positions {
            // Find if this position matches the current active btc5m_market
            let market_opt = state.btc5m_market.as_ref().filter(|m| m.slug == *market_id);

            let market = match market_opt {
                Some(m) => m,
                None => {
                    // Slug changed or market is no longer active -> automatic exit at last known price or entry_price
                    to_close.push((market_id.clone(), pos.entry_price, ExitReason::TimeExit));
                    continue;
                }
            };

            let current_price = match pos.direction {
                Direction::Yes => market.yes_price,
                Direction::No => market.no_price,
            };

            // Check profit target.
            let profit = current_price - pos.entry_price;

            if profit >= pos.profit_target_pct {
                to_close.push((market_id.clone(), current_price, ExitReason::ProfitTarget));
                continue;
            }

            // Check stop loss.
            if profit <= -pos.stop_loss_pct {
                to_close.push((market_id.clone(), current_price, ExitReason::StopLoss));
                continue;
            }

            // Check time exit / final window / settlement
            if market.time_remaining_secs <= pos.exit_before_final_secs
                || market.phase == crate::market_data::state::MarketPhase::Final
                || market.phase == crate::market_data::state::MarketPhase::Settled
            {
                to_close.push((market_id.clone(), current_price, ExitReason::TimeExit));
                continue;
            }

        }

        let mut closed = Vec::new();
        for (market_id, exit_price, reason) in to_close {
            if let Some(pos) = self.positions.remove(&market_id) {
                let pnl_usd = (exit_price - pos.entry_price) * pos.share_qty;
                let pnl_pct = if pos.entry_price > 0.0 {
                    (exit_price - pos.entry_price) / pos.entry_price * 100.0
                } else {
                    0.0
                };
                let display_pnl_pct = pnl_pct;

                let hold_ms = now.saturating_sub(pos.entry_at_ms);

                let mut trade = ClosedTrade {
                    market_id: pos.market_id.clone(),
                    direction: pos.direction.clone(),
                    entry_price: pos.entry_price,
                    exit_price,
                    size_usd: pos.size_usd,
                    pnl_usd,
                    pnl_pct: display_pnl_pct,
                    hold_duration_ms: hold_ms,
                    exit_reason: reason.clone(),
                    strategy_name: pos.strategy_name.clone(),
                    edge_pct: pos.edge_pct,
                    is_suspicious: false,
                    suspicious_reason: None,
                };

                if config.paper.flag_suspicious_trades {
                    let validator = PaperTradeValidator {
                        max_realistic_pnl_pct: config.paper.suspicious_pnl_threshold_pct,
                        max_hold_to_profit_ratio: config.paper.suspicious_hold_max_secs as f64,
                    };
                    match validator.validate_closed_trade(&trade) {
                        TradeValidity::Suspicious { reason } => {
                            trade.is_suspicious = true;
                            trade.suspicious_reason = Some(reason);
                        }
                        TradeValidity::Valid => {}
                    }
                }

                if !trade.is_suspicious {
                    self.total_pnl_usd += pnl_usd;
                    if pnl_usd >= 0.0 {
                        self.win_count += 1;
                    } else {
                        self.loss_count += 1;
                    }

                    info!(
                        "[paper] CLOSE {} {} @ {:.2} pnl={:+.2} ({:+.1}%) reason={} hold={:.0}s",
                        trade.direction,
                        trade.market_id,
                        trade.exit_price,
                        trade.pnl_usd,
                        trade.pnl_pct,
                        trade.exit_reason,
                        hold_ms as f64 / 1000.0,
                    );
                } else {
                    info!(
                        "[paper] CLOSE SUSPICIOUS {} {} @ {:.2} pnl={:+.2} ({:+.1}%) reason={} hold={:.0}s FLAGGED: {}",
                        trade.direction,
                        trade.market_id,
                        trade.exit_price,
                        trade.pnl_usd,
                        trade.pnl_pct,
                        trade.exit_reason,
                        hold_ms as f64 / 1000.0,
                        trade.suspicious_reason.as_deref().unwrap_or("unknown reason"),
                    );
                }

                bus.publish(MarketEvent::PaperTradeClose(trade.clone()));
                self.closed_trades.push(trade.clone());
                closed.push(trade);
            }
        }

        if !closed.is_empty() {
            self.log_stats();
        }

        closed
    }

    /// Log aggregate paper trading stats.
    pub fn log_stats(&self) {
        let total = self.win_count + self.loss_count;
        let winrate = if total > 0 {
            self.win_count as f64 / total as f64 * 100.0
        } else {
            0.0
        };

        info!(
            "[paper:stats] trades={} wins={} losses={} winrate={:.1}% total_pnl={:+.2}",
            total, self.win_count, self.loss_count, winrate, self.total_pnl_usd,
        );
    }
}
