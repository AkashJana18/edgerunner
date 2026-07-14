use std::collections::BTreeMap;

use anyhow::{Context, Result, bail};
use edgerunner_core::{EventEnvelope, MarketEvent, Price};
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::sync::mpsc;
use tokio_tungstenite::{connect_async, tungstenite::Message};

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct PascalConfig {
    pub ws_url: String,
    pub symbol: String,
    pub market: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct PascalMarket {
    pub symbol: String,
    pub event_description: String,
    pub market_description: String,
    pub market_period: String,
    pub expected_event_start_time_ms: Option<u64>,
    pub tags: Vec<String>,
}

pub struct PascalBookAdapter {
    config: PascalConfig,
}

pub struct PascalMarketClient {
    client: reqwest::Client,
    read_base_url: String,
}

impl PascalBookAdapter {
    pub fn new(config: PascalConfig) -> Self {
        Self { config }
    }

    pub async fn stream_book(self, sender: mpsc::Sender<EventEnvelope>) -> Result<()> {
        let (socket, _) = connect_async(&self.config.ws_url)
            .await
            .context("connect to Pascal websocket")?;
        let (mut write, mut read) = socket.split();
        let mut bids = BTreeMap::new();
        let mut asks = BTreeMap::new();
        write
            .send(Message::Text(
                subscription_message(&self.config.symbol).into(),
            ))
            .await?;

        while let Some(message) = read.next().await {
            let message = message?;
            if !message.is_text() {
                continue;
            }
            let payload: Value = serde_json::from_str(message.to_text()?)?;
            if payload.get("type").and_then(Value::as_str) == Some("error") {
                let message = payload
                    .pointer("/data/message")
                    .and_then(Value::as_str)
                    .unwrap_or("unknown Pascal subscription error");
                bail!("Pascal subscription failed: {message}");
            }
            if payload.get("channel").and_then(Value::as_str) != Some("book") {
                continue;
            }
            if payload.get("symbol").and_then(Value::as_str) != Some(&self.config.symbol) {
                continue;
            }
            let data = payload
                .get("data")
                .context("Pascal book message missing data")?;
            if payload.get("type").and_then(Value::as_str) == Some("snapshot") {
                bids.clear();
                asks.clear();
            }
            apply_levels(&mut bids, data.get("bids"));
            apply_levels(&mut asks, data.get("asks"));
            let Some((&bid, &bid_size)) = bids.last_key_value() else {
                continue;
            };
            let Some((&ask, &ask_size)) = asks.first_key_value() else {
                continue;
            };
            let Some(venue_seq) = parse_u64(payload.get("seq")) else {
                tracing::warn!("dropping Pascal book update without a sequence number");
                continue;
            };
            sender
                .send(EventEnvelope::new(
                    epoch_ns(),
                    MarketEvent::Book {
                        market: self.config.market.clone(),
                        bid,
                        bid_size,
                        ask,
                        ask_size,
                        venue_seq,
                    },
                ))
                .await?;
        }
        bail!("Pascal websocket ended")
    }
}

impl PascalMarketClient {
    pub fn from_ws_url(ws_url: &str) -> Result<Self> {
        let read_base_url = read_base_url(ws_url)?;
        Ok(Self {
            client: reqwest::Client::builder().tcp_nodelay(true).build()?,
            read_base_url,
        })
    }

    pub async fn list_markets(&self) -> Result<Vec<PascalMarket>> {
        let payload: Value = self
            .client
            .get(format!("{}/api/v1/markets", self.read_base_url))
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        let markets = payload
            .get("data")
            .and_then(Value::as_array)
            .context("Pascal market response missing data array")?
            .iter()
            .filter_map(parse_market)
            .collect();
        Ok(markets)
    }
}

fn subscription_message(symbol: &str) -> String {
    json!({
        "type": "subscribe",
        "channels": [{"channel": "book", "symbol": symbol}]
    })
    .to_string()
}

fn read_base_url(ws_url: &str) -> Result<String> {
    let (scheme, remainder) = if let Some(value) = ws_url.strip_prefix("wss://") {
        ("https", value)
    } else if let Some(value) = ws_url.strip_prefix("ws://") {
        ("http", value)
    } else {
        bail!("Pascal websocket URL must use ws:// or wss://");
    };
    let host = remainder.split('/').next().unwrap_or_default();
    if host.is_empty() {
        bail!("Pascal websocket URL is missing a host");
    }
    Ok(format!("{scheme}://{host}"))
}

fn parse_market(value: &Value) -> Option<PascalMarket> {
    let attributes = value.get("display_attributes")?;
    Some(PascalMarket {
        symbol: value.get("symbol")?.as_str()?.to_owned(),
        event_description: attributes.get("event_description")?.as_str()?.to_owned(),
        market_description: attributes
            .get("market_description")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned(),
        market_period: attributes
            .pointer("/game_page/section_description")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned(),
        expected_event_start_time_ms: parse_u64(attributes.get("expected_event_start_time_ms")),
        tags: attributes
            .get("tags")
            .and_then(Value::as_array)
            .map(|values| {
                values
                    .iter()
                    .filter_map(Value::as_str)
                    .map(str::to_owned)
                    .collect()
            })
            .unwrap_or_default(),
    })
}

fn apply_levels(book: &mut BTreeMap<Price, u64>, value: Option<&Value>) {
    let Some(levels) = value.and_then(Value::as_array) else {
        return;
    };
    for level in levels {
        let Some(level) = level.as_array() else {
            continue;
        };
        let Some(price) = level
            .first()
            .and_then(Value::as_str)
            .and_then(|price| Price::from_decimal(price).ok())
        else {
            continue;
        };
        let Some(size) = level
            .get(1)
            .and_then(Value::as_str)
            .and_then(|size| size.parse::<u64>().ok())
        else {
            continue;
        };
        if size == 0 {
            book.remove(&price);
        } else {
            book.insert(price, size);
        }
    }
}

fn parse_u64(value: Option<&Value>) -> Option<u64> {
    value?.as_u64().or_else(|| value?.as_str()?.parse().ok())
}

fn epoch_ns() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
        .min(u64::MAX as u128) as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn applies_and_removes_pascal_l2_levels() {
        let mut book = BTreeMap::new();
        apply_levels(&mut book, Some(&serde_json::json!([["0.540000", "15"]])));
        assert_eq!(book.first_key_value().unwrap().0.micros(), 540_000);
        assert_eq!(*book.first_key_value().unwrap().1, 15);
        apply_levels(&mut book, Some(&serde_json::json!([["0.540000", "0"]])));
        assert!(book.is_empty());
    }

    #[test]
    fn uses_the_current_public_book_subscription_shape() {
        let message: Value = serde_json::from_str(&subscription_message("MATCH.OUTCOME")).unwrap();
        assert_eq!(message["type"], "subscribe");
        assert_eq!(message["channels"][0]["channel"], "book");
        assert_eq!(message["channels"][0]["symbol"], "MATCH.OUTCOME");
    }

    #[test]
    fn derives_the_public_read_api_from_the_websocket_url() {
        assert_eq!(
            read_base_url("wss://data.pascal.trade/ws").unwrap(),
            "https://data.pascal.trade"
        );
    }

    #[test]
    fn parses_public_market_metadata() {
        let market = parse_market(&serde_json::json!({
            "symbol": "MATCH.HOME",
            "display_attributes": {
                "event_description": "Home vs Away",
                "market_description": "Home win",
                "game_page": {"section_description": "Moneyline - Reg Time"},
                "expected_event_start_time_ms": "1784055600000",
                "tags": ["Sports"]
            }
        }))
        .unwrap();
        assert_eq!(market.symbol, "MATCH.HOME");
        assert_eq!(market.market_period, "Moneyline - Reg Time");
        assert_eq!(market.expected_event_start_time_ms, Some(1_784_055_600_000));
    }
}
