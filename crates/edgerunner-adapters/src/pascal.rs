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

pub struct PascalBookAdapter {
    config: PascalConfig,
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
                json!({
                    "method": "subscribe",
                    "params": {"channel": "book", "symbol": self.config.symbol}
                })
                .to_string()
                .into(),
            ))
            .await?;

        while let Some(message) = read.next().await {
            let message = message?;
            if !message.is_text() {
                continue;
            }
            let payload: Value = serde_json::from_str(message.to_text()?)?;
            if payload.get("channel").and_then(Value::as_str) != Some("book") {
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
            sender
                .send(EventEnvelope::new(
                    epoch_ns(),
                    MarketEvent::Book {
                        market: self.config.market.clone(),
                        bid,
                        bid_size,
                        ask,
                        ask_size,
                        venue_seq: parse_u64(payload.get("seq")).unwrap_or_default(),
                    },
                ))
                .await?;
        }
        bail!("Pascal websocket ended")
    }
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
}
