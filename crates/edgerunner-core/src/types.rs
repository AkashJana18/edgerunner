use std::collections::{BTreeMap, VecDeque};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::Price;

pub type MarketId = String;

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Side {
    Bid,
    Ask,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum OrderMode {
    Paper,
    Live,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum FeedStatus {
    Connecting,
    Live,
    Stale,
    Disconnected,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum MarketEvent {
    FairValue {
        market: MarketId,
        probability: Price,
        source_seq: u64,
        message_id: Option<String>,
        proof_ts: Option<i64>,
    },
    Book {
        market: MarketId,
        bid: Price,
        bid_size: u64,
        ask: Price,
        ask_size: u64,
        venue_seq: u64,
    },
    Score {
        market: MarketId,
        phase: String,
        home: u16,
        away: u16,
        danger: bool,
        suspended: bool,
    },
    Feed {
        feed: String,
        status: FeedStatus,
        sequence_gap: bool,
    },
    Timer,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct EventEnvelope {
    pub id: Uuid,
    pub source_time_ns: Option<u64>,
    pub received_time_ns: u64,
    pub event: MarketEvent,
}

impl EventEnvelope {
    pub fn new(received_time_ns: u64, event: MarketEvent) -> Self {
        Self {
            id: Uuid::new_v4(),
            source_time_ns: None,
            received_time_ns,
            event,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct OrderIntent {
    pub id: Uuid,
    pub market: MarketId,
    pub side: Side,
    pub limit_price: Price,
    pub quantity: u64,
    pub expected_edge_micros: i64,
    pub created_time_ns: u64,
    pub source_message_id: Option<String>,
    pub source_proof_ts: Option<i64>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Fill {
    pub order_id: Uuid,
    pub market: MarketId,
    pub side: Side,
    pub price: Price,
    pub quantity: u64,
    pub fee_micros: i64,
    pub acknowledged_time_ns: u64,
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
pub struct MarketState {
    pub market: MarketId,
    pub fair_value: Option<Price>,
    pub best_bid: Option<Price>,
    pub best_ask: Option<Price>,
    pub bid_size: u64,
    pub ask_size: u64,
    pub position: i64,
    pub cash_micros: i64,
    pub fees_micros: i64,
    pub pnl_micros: i64,
    pub phase: String,
    pub score_home: u16,
    pub score_away: u16,
    pub danger: bool,
    pub suspended: bool,
    pub last_fair_time_ns: u64,
    pub last_book_time_ns: u64,
    pub source_seq: u64,
    pub venue_seq: u64,
    pub last_message_id: Option<String>,
    pub last_proof_ts: Option<i64>,
}

impl MarketState {
    pub fn apply(&mut self, envelope: &EventEnvelope) -> bool {
        match &envelope.event {
            MarketEvent::FairValue {
                market,
                probability,
                source_seq,
                message_id,
                proof_ts,
            } => {
                if self.source_seq > 0 && *source_seq <= self.source_seq {
                    return false;
                }
                self.market = market.clone();
                self.fair_value = Some(*probability);
                self.source_seq = *source_seq;
                self.last_message_id = message_id.clone();
                self.last_proof_ts = *proof_ts;
                self.last_fair_time_ns = envelope.received_time_ns;
            }
            MarketEvent::Book {
                market,
                bid,
                bid_size,
                ask,
                ask_size,
                venue_seq,
            } => {
                if self.venue_seq > 0 && *venue_seq <= self.venue_seq {
                    return false;
                }
                self.market = market.clone();
                self.best_bid = Some(*bid);
                self.best_ask = Some(*ask);
                self.bid_size = *bid_size;
                self.ask_size = *ask_size;
                self.venue_seq = *venue_seq;
                self.last_book_time_ns = envelope.received_time_ns;
            }
            MarketEvent::Score {
                market,
                phase,
                home,
                away,
                danger,
                suspended,
            } => {
                self.market = market.clone();
                self.phase = phase.clone();
                self.score_home = *home;
                self.score_away = *away;
                self.danger = *danger;
                self.suspended = *suspended;
            }
            MarketEvent::Feed { .. } | MarketEvent::Timer => {}
        }
        true
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct DecisionRecord {
    pub id: Uuid,
    pub at: DateTime<Utc>,
    pub event_id: Uuid,
    pub market: MarketId,
    pub action: String,
    pub reason: String,
    pub intent: Option<OrderIntent>,
    #[serde(rename = "compute_latency_ns", alias = "decision_latency_ns")]
    pub compute_latency_ns: u64,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct LatencySnapshot {
    pub samples: u64,
    pub p50_us: u64,
    pub p95_us: u64,
    pub p99_us: u64,
    pub max_us: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct EngineSnapshot {
    pub run_id: Uuid,
    pub mode: OrderMode,
    pub running: bool,
    pub killed: bool,
    pub feed_status: BTreeMap<String, FeedStatus>,
    pub markets: Vec<MarketState>,
    pub decisions: VecDeque<DecisionRecord>,
    pub fills: VecDeque<Fill>,
    pub latency: LatencySnapshot,
    pub processed_events: u64,
    pub ignored_events: u64,
    pub rejected_orders: u64,
    pub last_update: DateTime<Utc>,
}
