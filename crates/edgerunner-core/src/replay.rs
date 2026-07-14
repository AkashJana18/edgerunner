use std::{fs::File, io::BufRead, io::BufReader, path::Path};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{Engine, JournalRecord, MarketDataSource, MarketEvent, MarketMapping, Strategy};

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ReplayReport {
    pub events: u64,
    pub txline_events: u64,
    pub pascal_events: u64,
    pub mappings: u64,
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
    let mut mappings = 0;
    let mut active_mapping: Option<MarketMapping> = None;
    let mut decisions = 0;
    let mut fills = 0;
    let mut hasher = Sha256::new();
    for (line_number, line) in file.lines().enumerate() {
        let record: JournalRecord = serde_json::from_str(&line?)?;
        match record {
            JournalRecord::Mapping { mapping, .. } => {
                validate_mapping(&mapping).map_err(|error| {
                    format!(
                        "journal line {} has an invalid market mapping: {error}",
                        line_number + 1
                    )
                })?;
                active_mapping = Some(mapping);
                mappings += 1;
            }
            JournalRecord::Event { source, event, .. } => {
                let mapping = active_mapping.as_ref().ok_or_else(|| {
                    format!(
                        "journal line {} has a market event before its recorded mapping",
                        line_number + 1
                    )
                })?;
                validate_recorded_event(source, &event, mapping).map_err(|error| {
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
            _ => {}
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
    if mappings == 0 {
        return Err("journal contains no recorded market mapping".into());
    }
    Ok(ReplayReport {
        events,
        txline_events,
        pascal_events,
        mappings,
        decisions,
        fills,
        pnl_micros: engine.state.pnl_micros,
        decision_checksum: format!("{:x}", hasher.finalize()),
    })
}

fn validate_recorded_event(
    source: MarketDataSource,
    event: &crate::EventEnvelope,
    mapping: &MarketMapping,
) -> Result<(), String> {
    let event_market = match &event.event {
        MarketEvent::FairValue { market, .. } | MarketEvent::Book { market, .. } => market,
        _ => return Err("recorded data source has an unsupported market event".into()),
    };
    if event_market != &mapping.market {
        return Err("event market does not match the recorded mapping".into());
    }
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

fn validate_mapping(mapping: &MarketMapping) -> Result<(), String> {
    if mapping.fixture_id == 0
        || mapping.market.trim().is_empty()
        || mapping.pascal_symbol.trim().is_empty()
        || mapping.txline_selection.super_odds_type.trim().is_empty()
        || mapping.txline_selection.price_name.trim().is_empty()
    {
        return Err("required mapping fields are empty".into());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        DislocationConfig, DislocationTaker, EngineConfig, EventEnvelope, JournalRecord,
        JournalWriter, MarketDataSource, MarketEvent, MarketMapping, Price, TxLineMarketSelection,
    };
    use uuid::Uuid;

    #[test]
    fn repeated_replays_produce_identical_intent_checksums() {
        let path = std::env::temp_dir().join(format!("edgerunner-replay-{}.jsonl", Uuid::new_v4()));
        let mut writer = JournalWriter::open(&path).unwrap();
        let market = "market-a".to_owned();
        writer
            .write(&JournalRecord::Mapping {
                schema: 2,
                mapping: MarketMapping {
                    fixture_id: 1,
                    fixture_label: "recorded fixture".into(),
                    fixture_start_time_ms: 1_000,
                    market: market.clone(),
                    pascal_symbol: "RECORDED.OUTCOME".into(),
                    txline_selection: TxLineMarketSelection {
                        super_odds_type: "match_result".into(),
                        market_parameters: String::new(),
                        market_period: "full_time".into(),
                        price_index: 0,
                        price_name: "home".into(),
                    },
                },
            })
            .unwrap();
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
