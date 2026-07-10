use std::{fs::File, io::BufRead, io::BufReader, path::Path};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{Engine, JournalRecord, Strategy};

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ReplayReport {
    pub events: u64,
    pub decisions: u64,
    pub fills: u64,
    pub pnl_micros: i64,
    pub decision_checksum: String,
}

pub fn replay<S: Strategy>(
    path: impl AsRef<Path>,
    engine: &mut Engine<S>,
) -> Result<ReplayReport, Box<dyn std::error::Error + Send + Sync>> {
    let file = BufReader::new(File::open(path)?);
    let mut events = 0;
    let mut decisions = 0;
    let mut fills = 0;
    let mut hasher = Sha256::new();
    for line in file.lines() {
        let record: JournalRecord = serde_json::from_str(&line?)?;
        if let JournalRecord::Event { event, .. } = record {
            events += 1;
            for output in engine.process(event) {
                decisions += 1;
                fills += u64::from(output.fill.is_some());
                hasher.update(serde_json::to_vec(&output.decision.intent)?);
            }
        }
    }
    Ok(ReplayReport {
        events,
        decisions,
        fills,
        pnl_micros: engine.state.pnl_micros,
        decision_checksum: format!("{:x}", hasher.finalize()),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        DislocationConfig, DislocationTaker, EngineConfig, EventEnvelope, JournalRecord,
        JournalWriter, MarketEvent, Price,
    };
    use uuid::Uuid;

    #[test]
    fn repeated_replays_produce_identical_intent_checksums() {
        let path = std::env::temp_dir().join(format!("edgerunner-replay-{}.jsonl", Uuid::new_v4()));
        let mut writer = JournalWriter::open(&path).unwrap();
        let market = "FIFA-WC-FRA-BRA".to_owned();
        for event in [
            EventEnvelope::new(
                1_000_000,
                MarketEvent::Book {
                    market: market.clone(),
                    bid: Price::from_micros(540_000).unwrap(),
                    bid_size: 100,
                    ask: Price::from_micros(550_000).unwrap(),
                    ask_size: 100,
                    venue_seq: 1,
                },
            ),
            EventEnvelope::new(
                1_020_000,
                MarketEvent::FairValue {
                    market,
                    probability: Price::from_micros(610_000).unwrap(),
                    source_seq: 1,
                    message_id: Some("message-1".into()),
                    proof_ts: Some(1_020),
                },
            ),
        ] {
            writer
                .write(&JournalRecord::Event { schema: 1, event })
                .unwrap();
        }
        drop(writer);

        let make_engine = || {
            Engine::new(
                DislocationTaker::new(DislocationConfig::default()),
                EngineConfig::default(),
            )
        };
        let mut first = make_engine();
        let mut second = make_engine();
        let first_report = replay(&path, &mut first).unwrap();
        let second_report = replay(&path, &mut second).unwrap();
        assert_eq!(first_report.decisions, 1);
        assert_eq!(first_report.fills, 1);
        assert_eq!(
            first_report.decision_checksum,
            second_report.decision_checksum
        );
        std::fs::remove_file(path).unwrap();
    }
}
