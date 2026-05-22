use serde::{Deserialize, Serialize};

use crate::config::DivergenceConfig;

/// Trade direction for a binary outcome market.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum Direction {
    Yes,
    No,
}

impl std::fmt::Display for Direction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Direction::Yes => write!(f, "YES"),
            Direction::No => write!(f, "NO"),
        }
    }
}

/// Scored edge for a potential trade.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EdgeScore {
    /// Which side has the edge.
    pub direction: Direction,
    /// Expected repricing (what we expect to capture)
    pub edge_pct: f64,
    /// Confidence from the probability estimate.
    pub confidence: f64,
    /// Whether the edge is large enough to consider trading.
    pub tradeable: bool,
    /// If not tradeable, the reason why.
    pub reason: Option<String>,
}

pub fn score_edge(
    expected_repricing: f64,
    direction: Direction,
    yes_price: f64,
    _no_price: f64,
    spread: f64,
    time_remaining_secs: i64,
    confidence: f64,
    config: &DivergenceConfig,
) -> EdgeScore {
    let mut tradeable = true;
    let mut reason: Option<String> = None;

    // Check uncertainty gate: market must still be uncertain
    if yes_price < config.market_uncertainty_min || yes_price > config.market_uncertainty_max {
        tradeable = false;
        reason = Some(format!(
            "YES price {:.1}% not in uncertainty zone [{:.1}%, {:.1}%]",
            yes_price * 100.0,
            config.market_uncertainty_min * 100.0,
            config.market_uncertainty_max * 100.0
        ));
    } else if expected_repricing < config.min_edge_pct {
        tradeable = false;
        reason = Some(format!(
            "edge {:.1}% < min {:.1}%",
            expected_repricing * 100.0,
            config.min_edge_pct * 100.0
        ));
    } else if confidence < config.min_confidence {
        tradeable = false;
        reason = Some(format!(
            "conf {:.2} < min {:.2}",
            confidence, config.min_confidence
        ));
    } else if spread > config.max_spread {
        tradeable = false;
        reason = Some(format!(
            "spread {:.3} > max {:.3}",
            spread, config.max_spread
        ));
    } else if time_remaining_secs < config.min_time_remaining_secs {
        tradeable = false;
        reason = Some(format!(
            "time {}s < min {}s",
            time_remaining_secs, config.min_time_remaining_secs
        ));
    } else if time_remaining_secs > config.max_time_remaining_secs {
        tradeable = false;
        reason = Some(format!(
            "time {}s > max {}s",
            time_remaining_secs, config.max_time_remaining_secs
        ));
    }

    EdgeScore {
        direction,
        edge_pct: expected_repricing,
        confidence,
        tradeable,
        reason,
    }
}
