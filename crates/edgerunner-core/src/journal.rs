use std::{fs::OpenOptions, io::Write, path::Path};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

use crate::{DecisionRecord, EventEnvelope, Fill};

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "record", rename_all = "snake_case")]
pub enum JournalRecord {
    Run {
        schema: u16,
        run_id: Uuid,
        started_at: DateTime<Utc>,
    },
    Event {
        schema: u16,
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
