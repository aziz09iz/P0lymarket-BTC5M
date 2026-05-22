use crate::config::ProbabilityConfig;
use crate::market_data::state::BtcState;

/// Result of the BTC momentum → probability adjustment computation.
#[derive(Debug, Clone)]
pub struct MomentumResult {
    /// Weighted adjustment value (before time decay).
    pub adjustment: f64,
    /// Normalized momentum signal in [-1.0, 1.0].
    pub momentum_signal: f64,
    /// Normalized volume-delta signal in [-1.0, 1.0].
    pub delta_signal: f64,
}

/// Compute a probability adjustment from BTC momentum state.
///
/// The adjustment is a signed value indicating how much to shift the
/// market-implied probability:
///   - Positive → bullish → increase YES prob
///   - Negative → bearish → decrease YES prob
///
/// The magnitude is bounded by `config.max_momentum_adjustment`.
pub fn compute_momentum_adjustment(
    btc: &BtcState,
    config: &ProbabilityConfig,
) -> MomentumResult {
    // Normalize price_velocity into [-1.0, 1.0].
    // ±2.0 $/s is considered the saturation point.
    let momentum_signal = normalize(btc.price_velocity, -2.0, 2.0);

    // Normalize volume_delta into [-1.0, 1.0].
    // ±500_000 units is the saturation point.
    let delta_signal = normalize(btc.volume_delta, -500_000.0, 500_000.0);

    // Weighted combination.
    let raw = momentum_signal * config.momentum_weight
        + delta_signal * config.delta_weight;

    // Scale by max adjustment.
    let adjustment = raw * config.max_momentum_adjustment;

    MomentumResult {
        adjustment,
        momentum_signal,
        delta_signal,
    }
}

/// Normalize `value` from `[min, max]` to `[-1.0, 1.0]`, clamped.
fn normalize(value: f64, min: f64, max: f64) -> f64 {
    if (max - min).abs() < f64::EPSILON {
        return 0.0;
    }
    // Map to [0, 1] then shift to [-1, 1].
    let ratio = (value - min) / (max - min);
    (ratio * 2.0 - 1.0).clamp(-1.0, 1.0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalize_extremes() {
        assert!((normalize(2.0, -2.0, 2.0) - 1.0).abs() < 1e-10);
        assert!((normalize(-2.0, -2.0, 2.0) - (-1.0)).abs() < 1e-10);
        assert!((normalize(0.0, -2.0, 2.0) - 0.0).abs() < 1e-10);
    }

    #[test]
    fn normalize_clamps() {
        assert!((normalize(10.0, -2.0, 2.0) - 1.0).abs() < 1e-10);
        assert!((normalize(-10.0, -2.0, 2.0) - (-1.0)).abs() < 1e-10);
    }
}
