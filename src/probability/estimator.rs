use crate::config::DivergenceConfig;
use crate::market_data::state::BtcState;

/// Input to the probability estimator.
#[derive(Debug, Clone)]
pub struct ProbabilityInput {
    pub market_id: String,
    pub current_yes_price: f64,
    pub current_no_price: f64,
    pub btc_state: BtcState,
    pub time_remaining_secs: i64,
    pub spread: f64,
}

/// Output of the probability estimator.
#[derive(Debug, Clone)]
pub struct ProbabilityEstimate {
    pub market_id: String,
    /// Divergence score (0.0 - 1.0)
    pub divergence_score: f64,
    /// Expected repricing (0.0 - 1.0)
    pub expected_repricing: f64,
    /// Confidence in the estimate (0.0 - 1.0)
    pub confidence: f64,
    /// When this estimate was computed.
    pub computed_at_ms: u64,
    pub velocity_component: f64,
    pub delta_component: f64,
    pub spread_component: f64,
    pub time_component: f64,
}

/// Compute a probability estimate using the new divergence-based model.
pub fn compute_estimate(
    input: &ProbabilityInput,
    config: &DivergenceConfig,
) -> ProbabilityEstimate {
    let now_ms = crate::market_data::normalizer::now_millis();

    let velocity_magnitude = input.btc_state.price_velocity.abs();
    let delta_magnitude = input.btc_state.volume_delta.abs();

    // Normalize to 0-1 scale
    let velocity_score = if config.velocity_scale > 0.0 {
        (velocity_magnitude / config.velocity_scale).min(1.0)
    } else {
        0.0
    };
    let delta_score = if config.delta_scale > 0.0 {
        (delta_magnitude / config.delta_scale).min(1.0)
    } else {
        0.0
    };

    // Combined divergence score
    let divergence_score = velocity_score * 0.55 + delta_score * 0.45;

    // Edge estimate = expected repricing
    let expected_repricing = divergence_score * config.max_expected_repricing;

    // Confidence scoring components
    let vel_comp = velocity_score;
    let delta_aligned = input.btc_state.volume_delta.signum() == input.btc_state.price_velocity.signum();
    let delta_comp = if delta_aligned { delta_score } else { delta_score * 0.5 };

    let spread_comp = if config.max_spread > 0.0 {
        (1.0 - input.spread / config.max_spread).max(0.0)
    } else {
        0.0
    };

    let secs_remaining = input.time_remaining_secs.max(0) as u64;
    let time_comp = if secs_remaining >= 90 && secs_remaining <= 240 {
        1.0
    } else if secs_remaining >= 60 && secs_remaining < 90 {
        0.7
    } else if secs_remaining > 240 && secs_remaining <= 270 {
        0.8
    } else {
        0.3
    };

    let confidence = vel_comp * 0.35
        + delta_comp * 0.35
        + spread_comp * 0.15
        + time_comp * 0.15;

    ProbabilityEstimate {
        market_id: input.market_id.clone(),
        divergence_score,
        expected_repricing,
        confidence,
        computed_at_ms: now_ms,
        velocity_component: vel_comp,
        delta_component: delta_comp,
        spread_component: spread_comp,
        time_component: time_comp,
    }
}
