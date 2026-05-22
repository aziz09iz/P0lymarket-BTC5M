use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use serde_json::Value;

use super::state::{BtcTick, PolymarketState, TradeSide};

// ---------------------------------------------------------------------------
// Binance
// ---------------------------------------------------------------------------

/// Parse a raw Binance `btcusdt@trade` WebSocket message into a `BtcTick`.
///
/// Binance stream schema:
/// ```json
/// { "e":"trade", "T":timestamp_ms, "p":"price", "q":"qty", "m":is_buyer_maker }
/// ```
/// `m = true`  → buyer is maker → sell order hit bid  → `TradeSide::Sell`
/// `m = false` → seller is maker → buy order hit ask  → `TradeSide::Buy`
pub fn normalize_binance_trade(raw: &str) -> Result<BtcTick> {
    let v: Value =
        serde_json::from_str(raw).with_context(|| format!("JSON parse failed for: {raw}"))?;

    let price = v["p"]
        .as_str()
        .with_context(|| "Missing field 'p' (price)")?
        .parse::<f64>()
        .context("Cannot parse price as f64")?;

    let quantity = v["q"]
        .as_str()
        .with_context(|| "Missing field 'q' (quantity)")?
        .parse::<f64>()
        .context("Cannot parse quantity as f64")?;

    let is_buyer_maker = v["m"].as_bool().unwrap_or(false);
    let side = if is_buyer_maker {
        TradeSide::Sell
    } else {
        TradeSide::Buy
    };

    let timestamp_ms = v["T"]
        .as_u64()
        .with_context(|| "Missing or invalid field 'T' (trade timestamp)")?;

    let event_time_ms = v["E"]
        .as_u64()
        .with_context(|| "Missing or invalid field 'E' (event timestamp)")?;

    Ok(BtcTick {
        price,
        quantity,
        side,
        timestamp_ms,
        event_time_ms,
    })
}

// ---------------------------------------------------------------------------
// Polymarket — intermediate representation from Gamma REST
// ---------------------------------------------------------------------------

/// Intermediate market data from the Gamma API, before conversion to `PolymarketState`.
#[derive(Debug, Clone)]
pub struct GammaMarket {
    pub market_id: String,
    pub question: String,
    pub yes_price: f64,
    pub no_price: f64,
    pub yes_token_id: String,
    pub no_token_id: String,
    pub spread: f64,
    pub volume_24h: f64,
    pub time_remaining_secs: i64,
}

/// Parse one market object from the Gamma API JSON array.
/// Returns `None` if required fields are missing or malformed.
pub fn normalize_gamma_market(v: &Value) -> Option<GammaMarket> {
    let market_id = v["conditionId"].as_str()?.to_string();
    let question = v["question"]
        .as_str()
        .unwrap_or("Unknown question")
        .to_string();

    // Parse YES / NO token prices, supporting both the newer clobTokenIds/outcomePrices/outcomes format
    // and the fallback tokens array format.
    let mut yes_price = 0.0_f64;
    let mut no_price = 0.0_f64;
    let mut yes_token_id = String::new();
    let mut no_token_id = String::new();
    let mut parsed_successfully = false;

    let parse_string_or_array = |val: &Value| -> Option<Vec<String>> {
        if val.is_array() {
            let arr = val.as_array()?;
            let mut res = Vec::new();
            for item in arr {
                if let Some(s) = item.as_str() {
                    res.push(s.to_string());
                } else {
                    res.push(item.to_string());
                }
            }
            Some(res)
        } else if let Some(s) = val.as_str() {
            serde_json::from_str(s).ok()
        } else {
            None
        }
    };

    if let (Some(clob_token_ids), Some(outcome_prices)) = (
        parse_string_or_array(&v["clobTokenIds"]),
        parse_string_or_array(&v["outcomePrices"]),
    ) {
        let outcomes = parse_string_or_array(&v["outcomes"])
            .unwrap_or_else(|| vec!["Yes".to_string(), "No".to_string()]);

        if outcomes.len() >= 2 {
            for (i, outcome) in outcomes.iter().enumerate() {
                let price = outcome_prices
                    .get(i)
                    .and_then(|p| p.parse::<f64>().ok())
                    .unwrap_or(0.0);
                let token_id = clob_token_ids.get(i).cloned().unwrap_or_default();

                if outcome.eq_ignore_ascii_case("Yes") {
                    yes_price = price;
                    yes_token_id = token_id;
                } else if outcome.eq_ignore_ascii_case("No") {
                    no_price = price;
                    no_token_id = token_id;
                }
            }
            parsed_successfully = true;
        } else if clob_token_ids.len() >= 2 {
            yes_price = outcome_prices
                .get(0)
                .and_then(|p| p.parse::<f64>().ok())
                .unwrap_or(0.0);
            yes_token_id = clob_token_ids.get(0).cloned().unwrap_or_default();
            no_price = outcome_prices
                .get(1)
                .and_then(|p| p.parse::<f64>().ok())
                .unwrap_or(0.0);
            no_token_id = clob_token_ids.get(1).cloned().unwrap_or_default();
            parsed_successfully = true;
        }
    }

    if !parsed_successfully {
        if let Some(tokens) = v["tokens"].as_array() {
            for token in tokens {
                let outcome = token["outcome"].as_str().unwrap_or("");
                let price = token["price"]
                    .as_f64()
                    .or_else(|| token["price"].as_str()?.parse().ok())
                    .unwrap_or(0.0);
                let token_id = token["token_id"].as_str().unwrap_or("").to_string();

                if outcome.eq_ignore_ascii_case("Yes") {
                    yes_price = price;
                    yes_token_id = token_id;
                } else if outcome.eq_ignore_ascii_case("No") {
                    no_price = price;
                    no_token_id = token_id;
                }
            }
        } else {
            return None;
        }
    }

    // Volume — try several field name variants the API may return.
    let volume_24h = v["volume24hr"]
        .as_f64()
        .or_else(|| v["volume24Hr"].as_f64())
        .or_else(|| v["volume"].as_str()?.parse().ok())
        .or_else(|| v["volume"].as_f64())
        .unwrap_or(0.0);

    let end_date = v["endDate"].as_str().unwrap_or("").to_string();
    let time_remaining_secs = compute_time_remaining(&end_date, 0.0);

    // Spread: best_ask - best_bid if available, else 0.
    let yes_ask = v["bestAsk"]
        .as_f64()
        .or_else(|| v["bestAsk"].as_str()?.parse().ok())
        .unwrap_or(yes_price);
    let yes_bid = v["bestBid"]
        .as_f64()
        .or_else(|| v["bestBid"].as_str()?.parse().ok())
        .unwrap_or(yes_price);
    let spread = (yes_ask - yes_bid).abs();

    Some(GammaMarket {
        market_id,
        question,
        yes_price,
        no_price,
        yes_token_id,
        no_token_id,
        spread,
        volume_24h,
        time_remaining_secs,
    })
}

/// Convert a `GammaMarket` into a `PolymarketState` snapshot.
pub fn gamma_market_to_state(m: &GammaMarket) -> PolymarketState {
    PolymarketState {
        market_id: m.market_id.clone(),
        question: m.question.clone(),
        yes_price: m.yes_price,
        no_price: m.no_price,
        spread: m.spread,
        volume_24h: m.volume_24h,
        time_remaining_secs: m.time_remaining_secs,
        last_updated_ms: now_millis(),
    }
}

// ---------------------------------------------------------------------------
// Polymarket — WS message normalizer
// ---------------------------------------------------------------------------

/// What the Polymarket CLOB WS sends per asset:
///
/// Book snapshot: `{"event_type":"book","asset_id":"...","bids":[...],"asks":[...]}`
/// Price change:  `{"event_type":"price_change","asset_id":"...","changes":[...]}`
/// Last trade:    `{"event_type":"last_trade_price","asset_id":"...","price":"0.65"}`
///
/// Known non-JSON control messages from Polymarket CLOB WebSocket.
/// These are keepalives / protocol responses — not errors.
pub fn is_ws_control_message(raw: &str) -> bool {
    match raw.trim() {
        "INVALID OPERATION" | "PONG" | "PING" | "" => true,
        _ => false,
    }
}

/// Returns `(token_id, yes_price, spread)` or `None` if unrecognised / missing data.
pub fn normalize_polymarket_ws_msg(raw: &str) -> Result<Option<WsMarketUpdate>> {
    if is_ws_control_message(raw) {
        return Ok(None);
    }

    let v: Value =
        serde_json::from_str(raw).with_context(|| format!("JSON parse failed for: {raw}"))?;

    let event_type = match v["event_type"].as_str() {
        Some(t) => t,
        None => return Ok(None), // ping / heartbeat / unknown
    };

    let asset_id = match v["asset_id"].as_str() {
        Some(id) => id.to_string(),
        None => return Ok(None),
    };

    let sequence = v["sequence"].as_u64();

    match event_type {
        "book" => {
            if let Some(mid) = extract_mid_from_book(&v["bids"], &v["asks"]) {
                let best_bid = top_of_book(&v["bids"], true);
                let best_ask = top_of_book(&v["asks"], false);
                let spread = (best_ask - best_bid).abs();
                tracing::debug!(
                    "[ws] book snapshot: asset={} mid={:.4}",
                    &asset_id[..asset_id.len().min(8)],
                    mid
                );
                Ok(Some(WsMarketUpdate {
                    token_id: asset_id,
                    price: mid,
                    spread,
                    sequence,
                }))
            } else {
                Ok(None)
            }
        }
        "tick_size_change" | "tick_size" => Ok(None),
        "last_trade_price" => {
            let price = v["price"]
                .as_str()
                .and_then(|s| s.parse::<f64>().ok())
                .or_else(|| v["price"].as_f64());
            Ok(price.map(|p| WsMarketUpdate {
                token_id: asset_id,
                price: p,
                spread: 0.0,
                sequence,
            }))
        }
        "price_change" => {
            // Direct price field (common live format)
            let direct_price = v["price"]
                .as_str()
                .and_then(|s| s.parse::<f64>().ok())
                .or_else(|| v["price"].as_f64());
            if let Some(p) = direct_price {
                return Ok(Some(WsMarketUpdate {
                    token_id: asset_id,
                    price: p,
                    spread: 0.0,
                    sequence,
                }));
            }

            // Legacy: last entry in changes[] array
            let changes = v["changes"].as_array();
            if let Some(entries) = changes {
                if let Some(last) = entries.last() {
                    let price = last["price"]
                        .as_str()
                        .and_then(|s| s.parse::<f64>().ok())
                        .or_else(|| last["price"].as_f64());
                    return Ok(price.map(|p| WsMarketUpdate {
                        token_id: asset_id,
                        price: p,
                        spread: 0.0,
                        sequence,
                    }));
                }
            }
            Ok(None)
        }
        _ => Ok(None),
    }
}

/// Parsed update from a single Polymarket WS message.
pub struct WsMarketUpdate {
    pub token_id: String,
    pub price: f64,
    pub spread: f64,
    pub sequence: Option<u64>,
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Compute seconds remaining until `end_date` (RFC 3339 / ISO 8601).
/// Returns `i64::MAX` if the string is missing or unparseable.
pub fn compute_time_remaining(end_date: &str, server_offset_secs: f64) -> i64 {
    if end_date.is_empty() {
        return i64::MAX;
    }
    match end_date.parse::<DateTime<Utc>>() {
        Ok(dt) => {
            let offset_ms = (server_offset_secs * 1000.0) as i64;
            let now = Utc::now() + chrono::Duration::milliseconds(offset_ms);
            (dt - now).num_seconds()
        }
        Err(_) => i64::MAX,
    }
}

/// Mid price from order book when bid/ask are valid.
fn extract_mid_from_book(bids: &Value, asks: &Value) -> Option<f64> {
    let best_bid = top_of_book(bids, true);
    let best_ask = top_of_book(asks, false);
    if best_bid > 0.0 && best_ask < 1.0 && best_bid < best_ask {
        Some((best_bid + best_ask) / 2.0)
    } else {
        None
    }
}

/// Extract the best price from a Polymarket order book side (JSON array of {price, size}).
fn top_of_book(side: &Value, is_bid: bool) -> f64 {
    let arr = match side.as_array() {
        Some(a) if !a.is_empty() => a,
        _ => return 0.0,
    };

    arr.iter()
        .filter_map(|entry| {
            entry["price"]
                .as_str()
                .and_then(|s| s.parse::<f64>().ok())
                .or_else(|| entry["price"].as_f64())
        })
        .reduce(if is_bid { f64::max } else { f64::min })
        .unwrap_or(0.0)
}

pub fn now_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}
