use serde::{Deserialize, Serialize};
use tokio::sync::broadcast;

use crate::market_data::state::{BtcTick, PolymarketState, Btc5mMarket, MarketPhase};
use crate::paper::simulator::{ClosedTrade, PaperPosition};

/// Identifies which data feed a message originates from.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FeedSource {
    Binance,
    Polymarket,
}

impl std::fmt::Display for FeedSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FeedSource::Binance => write!(f, "binance"),
            FeedSource::Polymarket => write!(f, "polymarket"),
        }
    }
}

/// Current connection state of a feed.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ConnectionStatus {
    Connected,
    Disconnected,
    Reconnecting { delay_secs: u64 },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketCycleEvent {
    pub event_type: CycleEventType,
    pub market: Btc5mMarket,
    pub server_time_unix: f64,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum CycleEventType {
    MarketActivated,      // phase → Active
    FinalWindowEntered,   // phase → Final (T+270s)
    MarketSettled,        // phase → Settled
    NextMarketPreloaded,  // next market slug resolved
}

impl std::fmt::Display for CycleEventType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CycleEventType::MarketActivated => write!(f, "MarketActivated"),
            CycleEventType::FinalWindowEntered => write!(f, "FinalWindowEntered"),
            CycleEventType::MarketSettled => write!(f, "MarketSettled"),
            CycleEventType::NextMarketPreloaded => write!(f, "NextMarketPreloaded"),
        }
    }
}

/// All events that flow through the internal event bus.
/// Consumers subscribe via `EventBus::subscribe()`.
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub enum MarketEvent {
    BtcTick(BtcTick),
    PolymarketUpdate(Btc5mMarket),
    ConnectionStatus(FeedSource, ConnectionStatus),
    LatencyAlert { source: FeedSource, latency_ms: u64 },
    PriceRejected { reason: String },
    MarketCycleEvent(MarketCycleEvent),
    /// A paper position was opened.
    PaperTradeOpen(PaperPosition),
    /// A paper position was closed.
    PaperTradeClose(ClosedTrade),
    /// Sustained feed outage (e.g. Polymarket WS down > 60s with REST fallback active).
    FeedCriticalAlert { feed: String, down_secs: u64 },
    /// Periodic edge snapshot for the best BTC market (broadcast every N seconds).
    EdgeSnapshot {
        market_id: String,
        question: String,
        poly_yes_pct: f64,
        poly_no_pct: f64,
        divergence_score: f64,
        expected_repricing: f64,
        edge_pct: f64,
        tradeable: bool,
        direction: String,
        btc_price: f64,
        btc_trend: String,
        velocity_trend: String,
        time_remaining_secs: i64,
        confidence: f64,
        price_velocity: f64,
        volume_delta: f64,
        order_flow_ratio: f64,
        velocity_consistency: f64,
        price_acceleration: f64,
        missing_reason: String,
        threshold_mode: String,
        mins_since_last_trade: u64,
        active_min_edge_pct: f64,
        active_min_confidence: f64,
    },
}

/// Thin wrapper around a tokio broadcast channel.
///
/// Cloning the bus gives a new handle to the same underlying channel.
/// Missed events are silently dropped — subscribers that fall behind lose events
/// (they receive `RecvError::Lagged`).
#[derive(Clone)]
pub struct EventBus {
    tx: broadcast::Sender<MarketEvent>,
}

impl EventBus {
    pub fn new(capacity: usize) -> Self {
        let (tx, _) = broadcast::channel(capacity);
        Self { tx }
    }

    /// Publish an event. Returns immediately; no-ops if there are no active receivers.
    pub fn publish(&self, event: MarketEvent) {
        let _ = self.tx.send(event);
    }

    /// Create a new receiver subscribed from this point forward.
    pub fn subscribe(&self) -> broadcast::Receiver<MarketEvent> {
        self.tx.subscribe()
    }
}
