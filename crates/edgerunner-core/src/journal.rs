use std::{fs::OpenOptions, io::Write, path::Path};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

use crate::{DecisionRecord, EventEnvelope, Fill};

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MarketDataSource {
    Txline,
    Pascal,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct TxLineMarketSelection {
    pub super_odds_type: String,
    pub market_parameters: String,
    pub market_period: String,
    pub price_index: usize,
    pub price_name: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct MarketMapping {
    pub fixture_id: u64,
    pub fixture_label: String,
    pub fixture_start_time_ms: u64,
    pub market: String,
    pub pascal_symbol: String,
    pub txline_selection: TxLineMarketSelection,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "record", rename_all = "snake_case")]
pub enum JournalRecord {
    Run {
        schema: u16,
        run_id: Uuid,
        started_at: DateTime<Utc>,
    },
    Mapping {
        schema: u16,
        mapping: MarketMapping,
    },
    Event {
        schema: u16,
        /// The live adapter that produced this event. This is mandatory for replay.
        source: MarketDataSource,
        event: EventEnvelope,
    },
    Decision {
        schema: u16,
        decision: DecisionRecord,
    },
    Fill {
        schema: u16,
        fill: Fill,
    },
    Proof {
        schema: u16,
        order_id: Uuid,
        message_id: String,
        proof_ts: i64,
        fetched_at: DateTime<Utc>,
        proof: Value,
    },
}

pub struct JournalWriter {
    file: std::fs::File,
}

impl JournalWriter {
    pub fn open(path: impl AsRef<Path>) -> std::io::Result<Self> {
        if let Some(parent) = path.as_ref().parent() {
            std::fs::create_dir_all(parent)?;
        }
        let file = OpenOptions::new().create(true).append(true).open(path)?;
        Ok(Self { file })
    }

    pub fn create(path: impl AsRef<Path>) -> std::io::Result<Self> {
        if let Some(parent) = path.as_ref().parent() {
            std::fs::create_dir_all(parent)?;
        }
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(path)?;
        Ok(Self { file })
    }

    pub fn write(&mut self, record: &JournalRecord) -> std::io::Result<()> {
        serde_json::to_writer(&mut self.file, record)?;
        self.file.write_all(b"\n")?;
        Ok(())
    }
}
