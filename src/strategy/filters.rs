use crate::config::EdgeConfig;
use crate::market_data::normalizer::now_millis;
use crate::market_data::state::{MarketState, MicroTrend, PolymarketState};

/// Result of running all entry filters on a market.
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct FilterResult {
    pub passed: bool,
    pub failed_reason: Option<String>,
}

/// Entry filters that must all pass before a strategy can fire.
///
/// These are shared across strategies — individual strategies may add
/// their own additional checks on top.
#[allow(dead_code)]
pub struct EntryFilter;

#[allow(dead_code)]
impl EntryFilter {
    /// Run all common entry filters.
    ///
    /// Returns `FilterResult { passed: true }` only if every check passes.
    pub fn passes_all(
        state: &MarketState,
        market: &PolymarketState,
        config: &EdgeConfig,
    ) -> FilterResult {
        // 1. Chop filter: microtrend must not be Choppy.
        if state.btc.microtrend == MicroTrend::Choppy {
            return FilterResult {
                passed: false,
                failed_reason: Some("microtrend is Choppy".to_string()),
            };
        }

        // 2. Spread filter.
        if market.spread > config.max_spread_for_trade {
            return FilterResult {
                passed: false,
                failed_reason: Some(format!(
                    "spread {:.3} > max {:.3}",
                    market.spread, config.max_spread_for_trade
                )),
            };
        }

        // 3. Liquidity filter: volume_24h >= $500.
        const MIN_VOLUME_24H: f64 = 500.0;
        if market.volume_24h < MIN_VOLUME_24H {
            return FilterResult {
                passed: false,
                failed_reason: Some(format!(
                    "volume_24h ${:.0} < min ${:.0}",
                    market.volume_24h, MIN_VOLUME_24H
                )),
            };
        }

        // 4. Time window filter.
        if market.time_remaining_secs < config.min_time_remaining_secs {
            return FilterResult {
                passed: false,
                failed_reason: Some(format!(
                    "time {}s < min {}s",
                    market.time_remaining_secs, config.min_time_remaining_secs
                )),
            };
        }
        if market.time_remaining_secs > config.max_time_remaining_secs {
            return FilterResult {
                passed: false,
                failed_reason: Some(format!(
                    "time {}s > max {}s",
                    market.time_remaining_secs, config.max_time_remaining_secs
                )),
            };
        }

        // 5. WS freshness: last_updated_ms must be within 2000ms of now.
        let now = now_millis();
        let staleness_ms = now.saturating_sub(market.last_updated_ms);
        const MAX_STALENESS_MS: u64 = 2_000;
        if staleness_ms > MAX_STALENESS_MS {
            return FilterResult {
                passed: false,
                failed_reason: Some(format!(
                    "data stale: {}ms > {}ms",
                    staleness_ms, MAX_STALENESS_MS
                )),
            };
        }

        FilterResult {
            passed: true,
            failed_reason: None,
        }
    }
}
