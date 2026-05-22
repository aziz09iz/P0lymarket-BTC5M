use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum PriceSource {
    WebSocketLive,
    RestSnapshot,
    Interpolated,
}

impl std::fmt::Display for PriceSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PriceSource::WebSocketLive => write!(f, "WebSocketLive"),
            PriceSource::RestSnapshot => write!(f, "RestSnapshot"),
            PriceSource::Interpolated => write!(f, "Interpolated"),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PriceUpdate {
    pub yes_price: f64,
    pub no_price: f64,
    pub received_at_ms: u64,
    pub source: PriceSource,
    pub sequence_number: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PriceRejectReason {
    SuspiciousJump { change: f64, time_ms: u64, allowed: f64 },
    StaleSnapshot,
    OutOfSequence { received: u64, last: u64 },
}

impl std::fmt::Display for PriceRejectReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PriceRejectReason::SuspiciousJump { change, time_ms, allowed } => {
                write!(
                    f,
                    "suspicious jump: change={:.3}, time_ms={}, allowed={:.3}",
                    change, time_ms, allowed
                )
            }
            PriceRejectReason::StaleSnapshot => {
                write!(f, "stale REST snapshot after live WS price received")
            }
            PriceRejectReason::OutOfSequence { received, last } => {
                write!(
                    f,
                    "out of sequence: received={}, last={}",
                    received, last
                )
            }
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PriceValidator {
    pub last_valid_price: Option<PriceUpdate>,
    pub last_valid_at_ms: u64,
}

impl PriceValidator {
    pub fn new() -> Self {
        Self {
            last_valid_price: None,
            last_valid_at_ms: 0,
        }
    }

    /// Returns Err if price update should be rejected
    pub fn validate(
        &mut self,
        update: &PriceUpdate,
        max_price_jump_pct_per_5s: f64,
        fallback_mode: bool,
    ) -> Result<(), PriceRejectReason> {
        if let Some(last) = &self.last_valid_price {
            let yes_change = (update.yes_price - last.yes_price).abs();
            let no_change = (update.no_price - last.no_price).abs();
            let max_change = yes_change.max(no_change);
            let time_delta_ms = update.received_at_ms.saturating_sub(self.last_valid_at_ms);

            // RULE 1: Price jump guard
            // Max allowed price change per time window
            // In a real 5M market, price rarely moves more than 15% in 5 seconds
            let time_factor = (time_delta_ms as f64 / 5000.0).min(1.0);
            let allowed_change = max_price_jump_pct_per_5s * time_factor.max(0.1);

            if max_change > allowed_change && time_delta_ms < 3000 {
                return Err(PriceRejectReason::SuspiciousJump {
                    change: max_change,
                    time_ms: time_delta_ms,
                    allowed: allowed_change,
                });
            }

            // RULE 2: Stale snapshot detection
            // If source is RestSnapshot AND we already have a live WS price,
            // reject the REST snapshot (it's older) unless we are in fallback mode
            // or the time delta is large (> 800 ms).
            if update.source == PriceSource::RestSnapshot
                && last.source == PriceSource::WebSocketLive
                && time_delta_ms < 800
                && !fallback_mode
            {
                return Err(PriceRejectReason::StaleSnapshot);
            }

            // RULE 3: Sequence number regression (if available)
            if let (Some(new_seq), Some(last_seq)) =
                (update.sequence_number, last.sequence_number)
            {
                if new_seq <= last_seq {
                    return Err(PriceRejectReason::OutOfSequence {
                        received: new_seq,
                        last: last_seq,
                    });
                }
            }
        }

        self.last_valid_price = Some(update.clone());
        self.last_valid_at_ms = update.received_at_ms;
        Ok(())
    }
}
