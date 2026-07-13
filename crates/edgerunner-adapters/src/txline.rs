use anyhow::{Context, Result, bail};
use edgerunner_core::{EventEnvelope, MarketEvent, Price};
use futures_util::StreamExt;
use reqwest::header::{ACCEPT, AUTHORIZATION, CACHE_CONTROL, HeaderMap, HeaderValue};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::mpsc;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct TxLineConfig {
    pub origin: String,
    pub api_token: String,
    pub market: String,
    pub fixture_id: u64,
}

pub struct TxLineAdapter {
    client: reqwest::Client,
    config: TxLineConfig,
}

pub struct TxLineProofClient {
    client: reqwest::Client,
    origin: String,
    api_token: String,
}

impl TxLineAdapter {
    pub fn new(config: TxLineConfig) -> Result<Self> {
        let client = api_token_client(&config.api_token)?;
        Ok(Self { client, config })
    }

    pub async fn stream_odds(self, sender: mpsc::Sender<EventEnvelope>) -> Result<()> {
        let client = data_client(&self.client, &self.config.origin, &self.config.api_token).await?;
        let url = format!(
            "{}/api/odds/stream",
            self.config.origin.trim_end_matches('/')
        );
        let response = client
            .get(url)
            .header(ACCEPT, "text/event-stream")
            .header(CACHE_CONTROL, "no-cache")
            .send()
            .await?
            .error_for_status()?;
        let mut stream = response.bytes_stream();
        let mut buffer = Vec::with_capacity(16 * 1024);
        let mut sequence = 0_u64;

        while let Some(chunk) = stream.next().await {
            buffer.extend_from_slice(&chunk?);
            while let Some((index, separator_len)) = find_sse_separator(&buffer) {
                let framed: Vec<u8> = buffer.drain(..index + separator_len).collect();
                let block = std::str::from_utf8(&framed[..index])?;
                let data = block
                    .lines()
                    .filter_map(|line| line.strip_prefix("data:"))
                    .map(str::trim)
                    .collect::<Vec<_>>()
                    .join("\n");
                if data.is_empty() {
                    continue;
                }
                let payload: Value =
                    serde_json::from_str(&data).context("invalid TxLINE SSE JSON")?;
                if !matches_fixture(&payload, self.config.fixture_id) {
                    continue;
                }
                if let Some(probability) = extract_probability(&payload) {
                    let message_id =
                        extract_string(&payload, &["messageId", "MessageId", "message_id"])
                            .filter(|value| !value.trim().is_empty());
                    let proof_ts = extract_i64(&payload, &["ts", "Ts"]);
                    let (Some(message_id), Some(proof_ts)) = (message_id, proof_ts) else {
                        tracing::warn!(
                            fixture_id = self.config.fixture_id,
                            "dropping TxLINE update without message provenance"
                        );
                        continue;
                    };
                    sequence += 1;
                    sender
                        .send(EventEnvelope::new(
                            epoch_ns(),
                            MarketEvent::FairValue {
                                market: self.config.market.clone(),
                                probability,
                                source_seq: extract_u64(&payload, &["seq", "Seq"])
                                    .unwrap_or(sequence),
                                message_id: Some(message_id),
                                proof_ts: Some(proof_ts),
                            },
                        ))
                        .await
                        .context("engine event channel closed")?;
                }
            }
        }
        bail!("TxLINE odds stream ended")
    }
}

impl TxLineProofClient {
    pub fn new(config: &TxLineConfig) -> Result<Self> {
        Ok(Self {
            client: api_token_client(&config.api_token)?,
            origin: config.origin.clone(),
            api_token: config.api_token.clone(),
        })
    }

    pub async fn fetch_odds_proof(&self, message_id: &str, ts: i64) -> Result<Value> {
        let client = data_client(&self.client, &self.origin, &self.api_token).await?;
        let url = format!("{}/api/odds/validation", self.origin.trim_end_matches('/'));
        Ok(client
            .get(url)
            .query(&[("messageId", message_id), ("ts", &ts.to_string())])
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?)
    }
}

fn api_token_client(api_token: &str) -> Result<reqwest::Client> {
    let mut headers = HeaderMap::new();
    headers.insert("x-api-token", HeaderValue::from_str(api_token)?);
    Ok(reqwest::Client::builder()
        .default_headers(headers)
        .tcp_nodelay(true)
        .build()?)
}

async fn data_client(
    client: &reqwest::Client,
    origin: &str,
    api_token: &str,
) -> Result<reqwest::Client> {
    let jwt = client
        .post(format!("{}/auth/guest/start", origin.trim_end_matches('/')))
        .send()
        .await?
        .error_for_status()?
        .json::<GuestAuthResponse>()
        .await?
        .token;
    let mut headers = HeaderMap::new();
    headers.insert(
        AUTHORIZATION,
        HeaderValue::from_str(&format!("Bearer {jwt}"))?,
    );
    headers.insert("x-api-token", HeaderValue::from_str(api_token)?);
    Ok(reqwest::Client::builder()
        .default_headers(headers)
        .tcp_nodelay(true)
        .build()?)
}

#[derive(Deserialize)]
struct GuestAuthResponse {
    token: String,
}

fn find_sse_separator(buffer: &[u8]) -> Option<(usize, usize)> {
    let lf = buffer.windows(2).position(|window| window == b"\n\n");
    let crlf = buffer.windows(4).position(|window| window == b"\r\n\r\n");
    match (lf, crlf) {
        (Some(left), Some(right)) if right < left => Some((right, 4)),
        (Some(left), _) => Some((left, 2)),
        (None, Some(right)) => Some((right, 4)),
        (None, None) => None,
    }
}

fn extract_probability(value: &Value) -> Option<Price> {
    for key in [
        "probability",
        "stablePrice",
        "StablePrice",
        "price",
        "Price",
    ] {
        if let Some(value) = find_key(value, key) {
            let text = value
                .as_str()
                .map(str::to_owned)
                .or_else(|| value.as_number().map(ToString::to_string))?;
            if let Ok(price) = Price::from_decimal(&text) {
                return Some(price);
            }
        }
    }
    None
}

fn extract_u64(value: &Value, keys: &[&str]) -> Option<u64> {
    keys.iter().find_map(|key| {
        let value = find_key(value, key)?;
        value.as_u64().or_else(|| value.as_str()?.parse().ok())
    })
}

fn matches_fixture(value: &Value, fixture_id: u64) -> bool {
    extract_u64(
        value,
        &["fixtureId", "FixtureId", "FixtureID", "fixture_id"],
    )
    .is_some_and(|value| value == fixture_id)
}

fn extract_i64(value: &Value, keys: &[&str]) -> Option<i64> {
    keys.iter().find_map(|key| {
        let value = find_key(value, key)?;
        value.as_i64().or_else(|| value.as_str()?.parse().ok())
    })
}

fn extract_string(value: &Value, keys: &[&str]) -> Option<String> {
    keys.iter()
        .find_map(|key| find_key(value, key)?.as_str().map(str::to_owned))
}

fn find_key<'a>(value: &'a Value, key: &str) -> Option<&'a Value> {
    match value {
        Value::Object(map) => map
            .get(key)
            .or_else(|| map.values().find_map(|nested| find_key(nested, key))),
        Value::Array(values) => values.iter().find_map(|nested| find_key(nested, key)),
        _ => None,
    }
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
    fn extracts_nested_stable_price() {
        let value = serde_json::json!({"data": {"StablePrice": "0.617500"}});
        assert_eq!(extract_probability(&value).unwrap().micros(), 617_500);
    }

    #[test]
    fn keeps_only_the_configured_fixture() {
        let value = serde_json::json!({"data": {"FixtureId": 17588320}});
        assert!(matches_fixture(&value, 17_588_320));
        assert!(!matches_fixture(&value, 17_588_321));
    }

    #[test]
    fn frames_lf_and_crlf_sse_blocks_without_decoding_chunks() {
        assert_eq!(find_sse_separator(b"data: one\n\ndata: two"), Some((9, 2)));
        assert_eq!(
            find_sse_separator(b"data: one\r\n\r\ndata: two"),
            Some((9, 4))
        );
        assert_eq!(find_sse_separator("data: caf\u{e9}".as_bytes()), None);
    }
}
