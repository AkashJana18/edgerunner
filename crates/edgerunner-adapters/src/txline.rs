use anyhow::{Context, Result, bail};
use edgerunner_core::{EventEnvelope, MarketEvent, Price, TxLineMarketSelection};
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
    pub selection: TxLineMarketSelection,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct TxLineFixture {
    pub fixture_id: u64,
    pub participant_1: String,
    pub participant_2: String,
    pub participant_1_is_home: bool,
    pub start_time_ms: u64,
    pub game_state: Option<u64>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct TxLineOddsLine {
    pub super_odds_type: String,
    pub market_parameters: String,
    pub market_period: String,
    pub price_names: Vec<String>,
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

pub struct TxLineDiscoveryClient {
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
        self.emit_snapshot(&client, &sender).await?;
        let url = format!(
            "{}/api/odds/stream",
            self.config.origin.trim_end_matches('/')
        );
        let response = client
            .get(url)
            .query(&[("fixtureId", self.config.fixture_id)])
            .header(ACCEPT, "text/event-stream")
            .header(CACHE_CONTROL, "no-cache")
            .send()
            .await?
            .error_for_status()?;
        let mut stream = response.bytes_stream();
        let mut buffer = Vec::with_capacity(16 * 1024);

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
                if !matches_fixture(&payload, self.config.fixture_id)
                    || !matches_selection(&payload, &self.config.selection)
                {
                    continue;
                }
                if let Some(event) = self.fair_value_event(&payload) {
                    sender
                        .send(event)
                        .await
                        .context("engine event channel closed")?;
                }
            }
        }
        bail!("TxLINE odds stream ended")
    }

    async fn emit_snapshot(
        &self,
        client: &reqwest::Client,
        sender: &mpsc::Sender<EventEnvelope>,
    ) -> Result<()> {
        let url = format!(
            "{}/api/odds/snapshot/{}",
            self.config.origin.trim_end_matches('/'),
            self.config.fixture_id
        );
        let payload: Value = client
            .get(url)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        let Some(snapshot) = payload
            .as_array()
            .context("TxLINE odds snapshot must be an array")?
            .iter()
            .find(|line| {
                matches_fixture(line, self.config.fixture_id)
                    && matches_selection(line, &self.config.selection)
            })
        else {
            tracing::warn!(
                fixture_id = self.config.fixture_id,
                "TxLINE snapshot has no selected market line; waiting for SSE"
            );
            return Ok(());
        };
        if let Some(event) = self.fair_value_event(snapshot) {
            sender
                .send(event)
                .await
                .context("engine event channel closed")?;
        }
        Ok(())
    }

    fn fair_value_event(&self, payload: &Value) -> Option<EventEnvelope> {
        let probability = extract_probability(payload, Some(&self.config.selection))?;
        let message_id = extract_string(payload, &["messageId", "MessageId", "message_id"])
            .filter(|value| !value.trim().is_empty())?;
        let proof_ts = extract_i64(payload, &["ts", "Ts"])?;
        let source_seq = u64::try_from(proof_ts).ok()?;
        let source_time_ns = source_seq.checked_mul(1_000_000);
        let mut event = EventEnvelope::new(
            epoch_ns(),
            MarketEvent::FairValue {
                market: self.config.market.clone(),
                probability,
                source_seq,
                message_id: Some(message_id),
                proof_ts: Some(proof_ts),
            },
        );
        event.source_time_ns = source_time_ns;
        Some(event)
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

impl TxLineDiscoveryClient {
    pub fn new(origin: String, api_token: String) -> Result<Self> {
        Ok(Self {
            client: api_token_client(&api_token)?,
            origin,
            api_token,
        })
    }

    pub async fn fixtures(&self) -> Result<Vec<TxLineFixture>> {
        let client = data_client(&self.client, &self.origin, &self.api_token).await?;
        let url = format!(
            "{}/api/fixtures/snapshot",
            self.origin.trim_end_matches('/')
        );
        let payload: Value = client
            .get(url)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        let fixtures = payload
            .as_array()
            .context("TxLINE fixtures snapshot must be an array")?
            .iter()
            .filter_map(parse_fixture)
            .collect();
        Ok(fixtures)
    }

    pub async fn odds_snapshot(&self, fixture_id: u64) -> Result<Vec<TxLineOddsLine>> {
        let client = data_client(&self.client, &self.origin, &self.api_token).await?;
        let url = format!(
            "{}/api/odds/snapshot/{fixture_id}",
            self.origin.trim_end_matches('/')
        );
        let payload: Value = client
            .get(url)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        let lines = payload
            .as_array()
            .context("TxLINE odds snapshot must be an array")?
            .iter()
            .filter_map(parse_odds_line)
            .collect();
        Ok(lines)
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

fn extract_probability(value: &Value, selection: Option<&TxLineMarketSelection>) -> Option<Price> {
    if let Some(selection) = selection
        && let Some(values) = find_key(value, "Pct").and_then(Value::as_array)
        && let Some(value) = values.get(selection.price_index)
    {
        let text = value
            .as_str()
            .map(str::to_owned)
            .or_else(|| value.as_number().map(ToString::to_string))?;
        return percentage_to_probability(&text);
    }
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

fn percentage_to_probability(value: &str) -> Option<Price> {
    let value = value.trim();
    let (whole, fractional) = value.split_once('.').unwrap_or((value, ""));
    if whole.is_empty()
        || fractional.len() > 4
        || !whole.bytes().all(|byte| byte.is_ascii_digit())
        || !fractional.bytes().all(|byte| byte.is_ascii_digit())
    {
        return None;
    }
    let scale = 10_i64.checked_pow(fractional.len() as u32)?;
    let whole = whole.parse::<i64>().ok()?;
    let fractional = if fractional.is_empty() {
        0
    } else {
        fractional.parse::<i64>().ok()?
    };
    let percentage = whole.checked_mul(scale)?.checked_add(fractional)?;
    let probability_micros = percentage.checked_mul(10_000)?.checked_div(scale)?;
    Price::from_micros(probability_micros).ok()
}

fn matches_selection(value: &Value, selection: &TxLineMarketSelection) -> bool {
    extract_string(value, &["SuperOddsType", "superOddsType"])
        .is_some_and(|value| value == selection.super_odds_type)
        && extract_string(value, &["MarketParameters", "marketParameters"]).unwrap_or_default()
            == selection.market_parameters
        && extract_string(value, &["MarketPeriod", "marketPeriod"]).unwrap_or_default()
            == selection.market_period
}

fn parse_fixture(value: &Value) -> Option<TxLineFixture> {
    Some(TxLineFixture {
        fixture_id: extract_u64(value, &["FixtureId", "fixtureId"])?,
        participant_1: extract_string(value, &["Participant1", "participant1"])?,
        participant_2: extract_string(value, &["Participant2", "participant2"])?,
        participant_1_is_home: find_key(value, "Participant1IsHome")
            .or_else(|| find_key(value, "participant1IsHome"))
            .and_then(Value::as_bool)
            .unwrap_or(true),
        start_time_ms: extract_u64(value, &["StartTime", "startTime"])?,
        game_state: extract_u64(value, &["GameState", "gameState"]),
    })
}

fn parse_odds_line(value: &Value) -> Option<TxLineOddsLine> {
    let price_names = find_key(value, "PriceNames")
        .or_else(|| find_key(value, "priceNames"))?
        .as_array()?
        .iter()
        .filter_map(Value::as_str)
        .map(str::to_owned)
        .collect::<Vec<_>>();
    if price_names.is_empty() {
        return None;
    }
    Some(TxLineOddsLine {
        super_odds_type: extract_string(value, &["SuperOddsType", "superOddsType"])?,
        market_parameters: extract_string(value, &["MarketParameters", "marketParameters"])
            .unwrap_or_default(),
        market_period: extract_string(value, &["MarketPeriod", "marketPeriod"]).unwrap_or_default(),
        price_names,
    })
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
        assert_eq!(extract_probability(&value, None).unwrap().micros(), 617_500);
    }

    #[test]
    fn keeps_only_the_configured_fixture() {
        let value = serde_json::json!({"data": {"FixtureId": 17588320}});
        assert!(matches_fixture(&value, 17_588_320));
        assert!(!matches_fixture(&value, 17_588_321));
    }

    #[test]
    fn converts_txline_pct_to_a_probability_and_selects_the_recorded_price_index() {
        let selection = TxLineMarketSelection {
            super_odds_type: "match_result".into(),
            market_parameters: String::new(),
            market_period: "full_time".into(),
            price_index: 1,
            price_name: "draw".into(),
        };
        let value = serde_json::json!({
            "SuperOddsType": "match_result",
            "MarketPeriod": "full_time",
            "Pct": ["51.000", "24.500", "24.500"]
        });
        assert!(matches_selection(&value, &selection));
        assert_eq!(
            extract_probability(&value, Some(&selection))
                .unwrap()
                .micros(),
            245_000
        );
    }

    #[test]
    fn rejects_pct_values_outside_the_probability_range() {
        assert_eq!(percentage_to_probability("100.000").unwrap(), Price::ONE);
        assert!(percentage_to_probability("100.001").is_none());
        assert!(percentage_to_probability("0.12345").is_none());
    }

    #[test]
    fn preserves_txline_message_provenance_in_a_fair_value_event() {
        let adapter = TxLineAdapter::new(TxLineConfig {
            origin: "https://txline-dev.txodds.com".into(),
            api_token: "test-token".into(),
            market: "fixture.market".into(),
            fixture_id: 42,
            selection: TxLineMarketSelection {
                super_odds_type: "1X2_PARTICIPANT_RESULT".into(),
                market_parameters: String::new(),
                market_period: String::new(),
                price_index: 1,
                price_name: "draw".into(),
            },
        })
        .unwrap();
        let event = adapter
            .fair_value_event(&serde_json::json!({
                "FixtureId": 42,
                "MessageId": "message-1",
                "Ts": 1_784_022_342_409_i64,
                "SuperOddsType": "1X2_PARTICIPANT_RESULT",
                "Pct": ["40.112", "29.789", "30.093"]
            }))
            .unwrap();
        assert_eq!(event.source_time_ns, Some(1_784_022_342_409_000_000));
        match event.event {
            MarketEvent::FairValue {
                probability,
                source_seq,
                message_id,
                proof_ts,
                ..
            } => {
                assert_eq!(probability.micros(), 297_890);
                assert_eq!(source_seq, 1_784_022_342_409);
                assert_eq!(message_id.as_deref(), Some("message-1"));
                assert_eq!(proof_ts, Some(1_784_022_342_409));
            }
            _ => panic!("expected TxLINE fair-value event"),
        }
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
