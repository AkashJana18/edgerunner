use std::{fs::File, io::BufRead, io::BufReader, path::Path};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{Engine, JournalRecord, MarketDataSource, MarketEvent, Strategy};

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ReplayReport {
    pub events: u64,
    pub txline_events: u64,
    pub pascal_events: u64,
    pub decisions: u64,
    pub fills: u64,
    pub pnl_micros: i64,
    pub decision_checksum: String,
}

pub fn replay<S: Strategy>(
    path: impl AsRef<Path>,
    engine: &mut Engine<S>,
) -> Result<ReplayReport, Box<dyn std::error::Error + Send + Sync>> {
    replay_with_limit(path, engine, None)
}

pub fn replay_with_limit<S: Strategy>(
    path: impl AsRef<Path>,
    engine: &mut Engine<S>,
    max_events: Option<u64>,
) -> Result<ReplayReport, Box<dyn std::error::Error + Send + Sync>> {
    let file = BufReader::new(File::open(path)?);
    let mut events = 0;
    let mut txline_events = 0;
    let mut pascal_events = 0;
    let mut decisions = 0;
    let mut fills = 0;
    let mut hasher = Sha256::new();
    for (line_number, line) in file.lines().enumerate() {
        let record: JournalRecord = serde_json::from_str(&line?)?;
        if let JournalRecord::Event { source, event, .. } = record {
            validate_recorded_event(source, &event).map_err(|error| {
                format!(
                    "journal line {} is not a recorded TxLINE/Pascal event: {error}",
                    line_number + 1
                )
            })?;
            events += 1;
            match source {
                MarketDataSource::Txline => txline_events += 1,
                MarketDataSource::Pascal => pascal_events += 1,
            }
            for output in engine.process(event) {
                decisions += 1;
                fills += u64::from(output.fill.is_some());
                hasher.update(serde_json::to_vec(&output.decision.intent)?);
            }
            if max_events.is_some_and(|limit| events >= limit) {
                break;
            }
        }
    }
    if events == 0 {
        return Err("journal contains no recorded market events".into());
    }
    if txline_events == 0 || pascal_events == 0 {
        return Err(
            "journal must contain both TxLINE fair-value and Pascal order-book events".into(),
        );
    }
    Ok(ReplayReport {
        events,
        txline_events,
        pascal_events,
        decisions,
        fills,
        pnl_micros: engine.state.pnl_micros,
        decision_checksum: format!("{:x}", hasher.finalize()),
    })
}

fn validate_recorded_event(
    source: MarketDataSource,
    event: &crate::EventEnvelope,
) -> Result<(), String> {
    match (source, &event.event) {
        (
            MarketDataSource::Txline,
            MarketEvent::FairValue {
                message_id: Some(message_id),
                proof_ts: Some(_),
                ..
            },
        ) if !message_id.trim().is_empty() => Ok(()),
        (MarketDataSource::Txline, MarketEvent::FairValue { .. }) => {
            Err("TxLINE fair value is missing message provenance".into())
        }
        (MarketDataSource::Txline, _) => Err("TxLINE source has a non-fair-value event".into()),
        (MarketDataSource::Pascal, MarketEvent::Book { .. }) => Ok(()),
        (MarketDataSource::Pascal, _) => Err("Pascal source has a non-order-book event".into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        DislocationConfig, DislocationTaker, EngineConfig, EventEnvelope, JournalRecord,
        JournalWriter, MarketDataSource, MarketEvent, Price,
    };
    use uuid::Uuid;

    #[test]
    fn repeated_replays_produce_identical_intent_checksums() {
        let path = std::env::temp_dir().join(format!("edgerunner-replay-{}.jsonl", Uuid::new_v4()));
        let mut writer = JournalWriter::open(&path).unwrap();
        let market = "market-a".to_owned();
        for (source, event) in [
            (
                MarketDataSource::Pascal,
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
            ),
            (
                MarketDataSource::Txline,
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
            ),
        ] {
            writer
                .write(&JournalRecord::Event {
                    schema: 2,
                    source,
                    event,
                })
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

    #[test]
    fn rejects_legacy_event_records_without_a_live_source() {
        let path = std::env::temp_dir().join(format!("edgerunner-legacy-{}.jsonl", Uuid::new_v4()));
        std::fs::write(
            &path,
            r#"{"record":"event","schema":1,"event":{"received_ns":1,"event":{"type":"book","market":"legacy","bid":540000,"bid_size":1,"ask":550000,"ask_size":1,"venue_seq":1}}}"#,
        )
        .unwrap();
        let mut engine = Engine::new(
            DislocationTaker::new(DislocationConfig::default()),
            EngineConfig::default(),
        );
        assert!(replay(&path, &mut engine).is_err());
        std::fs::remove_file(path).unwrap();
    }
}
