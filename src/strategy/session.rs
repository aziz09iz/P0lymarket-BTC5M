use crate::config::{AppConfig, DivergenceConfig};
use crate::market_data::normalizer::now_millis;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ThresholdMode {
    Normal,
    Relaxed,
    Floor,
}

impl ThresholdMode {
    pub fn as_str(&self) -> &'static str {
        match self {
            ThresholdMode::Normal => "NORMAL",
            ThresholdMode::Relaxed => "RELAXED",
            ThresholdMode::Floor => "FLOOR",
        }
    }

    pub fn emoji(&self) -> &'static str {
        match self {
            ThresholdMode::Normal => "🟢",
            ThresholdMode::Relaxed => "🟡",
            ThresholdMode::Floor => "🔴",
        }
    }
}

#[derive(Debug, Clone)]
pub struct EntryThresholds {
    pub min_edge_pct: f64,
    pub min_confidence: f64,
    pub min_velocity_abs: f64,
    pub market_uncertainty_min: f64,
    pub market_uncertainty_max: f64,
}

#[derive(Debug, Clone)]
pub struct SessionState {
    pub session_start_ms: u64,
    pub last_trade_at_ms: Option<u64>,
    pub trades_this_session: u32,
}

impl SessionState {
    pub fn new() -> Self {
        Self {
            session_start_ms: current_session_start_ms(now_millis()),
            last_trade_at_ms: None,
            trades_this_session: 0,
        }
    }

    pub fn maybe_reset_session(&mut self) {
        let boundary = current_session_start_ms(now_millis());
        if boundary != self.session_start_ms {
            self.session_start_ms = boundary;
            self.last_trade_at_ms = None;
            self.trades_this_session = 0;
            tracing::info!(
                "[strategy] new 30-minute session started — threshold mode reset to NORMAL"
            );
        }
    }

    pub fn mins_since_last_trade(&self) -> u64 {
        let reference = self
            .last_trade_at_ms
            .unwrap_or(self.session_start_ms);
        (now_millis().saturating_sub(reference)) / 60_000
    }

    pub fn on_trade_executed(&mut self) {
        self.last_trade_at_ms = Some(now_millis());
        self.trades_this_session += 1;
        tracing::info!(
            "[strategy] trade executed — {} trades this session, mode → NORMAL",
            self.trades_this_session
        );
    }

    pub fn threshold_mode(&self, config: &AppConfig) -> ThresholdMode {
        let mins = self.mins_since_last_trade();
        if mins >= config.strategy.floor_mode_after_mins {
            ThresholdMode::Floor
        } else if mins >= config.strategy.relaxed_mode_after_mins {
            ThresholdMode::Relaxed
        } else {
            ThresholdMode::Normal
        }
    }

    pub fn active_thresholds(
        &self,
        config: &AppConfig,
        runtime_min_edge: f64,
        runtime_min_confidence: f64,
    ) -> (ThresholdMode, EntryThresholds) {
        let mode = self.threshold_mode(config);
        let div = &config.strategy.divergence;

        let thresholds = match mode {
            ThresholdMode::Normal => EntryThresholds {
                min_edge_pct: runtime_min_edge,
                min_confidence: runtime_min_confidence,
                min_velocity_abs: div.min_velocity_abs,
                market_uncertainty_min: div.market_uncertainty_min,
                market_uncertainty_max: div.market_uncertainty_max,
            },
            ThresholdMode::Relaxed => EntryThresholds {
                min_edge_pct: config.strategy.relaxed_min_edge_pct,
                min_confidence: config.strategy.relaxed_min_confidence,
                min_velocity_abs: config.strategy.relaxed_min_velocity_abs,
                market_uncertainty_min: 0.30,
                market_uncertainty_max: 0.70,
            },
            ThresholdMode::Floor => EntryThresholds {
                min_edge_pct: config.strategy.floor_min_edge_pct,
                min_confidence: config.strategy.floor_min_confidence,
                min_velocity_abs: config.strategy.relaxed_min_velocity_abs,
                market_uncertainty_min: 0.15,
                market_uncertainty_max: 0.85,
            },
        };

        (mode, thresholds)
    }
}

pub fn apply_entry_thresholds(base: &DivergenceConfig, t: &EntryThresholds) -> DivergenceConfig {
    let mut c = base.clone();
    c.min_edge_pct = t.min_edge_pct;
    c.min_confidence = t.min_confidence;
    c.min_velocity_abs = t.min_velocity_abs;
    c.market_uncertainty_min = t.market_uncertainty_min;
    c.market_uncertainty_max = t.market_uncertainty_max;
    c
}

/// Session boundaries at :00 and :30 past each hour (UTC wall clock).
pub fn current_session_start_ms(now_ms: u64) -> u64 {
    let secs = now_ms / 1000;
    let minute_in_hour = (secs / 60) % 60;
    let hour_start_secs = (secs / 3600) * 3600;
    let session_offset_secs = if minute_in_hour < 30 { 0 } else { 30 * 60 };
    (hour_start_secs + session_offset_secs) * 1000
}
