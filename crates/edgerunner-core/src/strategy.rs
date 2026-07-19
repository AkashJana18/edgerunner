use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::{EventEnvelope, MarketEvent, MarketState, OrderIntent, OrderIntentKind, Side};

pub const ENTRY_EDGE_THRESHOLD: f64 = 0.05;
pub const EXIT_EDGE_THRESHOLD: f64 = 0.01;
pub const ENTRY_EDGE_THRESHOLD_MICROS: i64 = (ENTRY_EDGE_THRESHOLD * 1_000_000.0) as i64;
pub const EXIT_EDGE_THRESHOLD_MICROS: i64 = (EXIT_EDGE_THRESHOLD * 1_000_000.0) as i64;

pub trait Strategy: Send {
    fn name(&self) -> &'static str;
    fn on_event(&mut self, state: &MarketState, event: &EventEnvelope) -> Vec<StrategyEvaluation>;
}

#[derive(Clone, Debug)]
pub struct StrategyEvaluation {
    /// The candidate order evaluated by the strategy, even if it was not eligible to submit.
    pub intent: Option<OrderIntent>,
    pub eligible: bool,
    pub reason: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default)]
pub struct DislocationConfig {
    pub latency_haircut_micros: i64,
    pub taker_fee_rate_micros: i64,
    pub order_size: u64,
}

impl Default for DislocationConfig {
    fn default() -> Self {
        Self {
            latency_haircut_micros: 2_500,
            taker_fee_rate_micros: 1_000,
            order_size: 25,
        }
    }
}

pub struct DislocationTaker {
    config: DislocationConfig,
    last_source_seq: u64,
    last_venue_seq: u64,
}

impl DislocationTaker {
    pub fn new(config: DislocationConfig) -> Self {
        Self {
            config,
            last_source_seq: 0,
            last_venue_seq: 0,
        }
    }

    fn edge_after_costs(&self, gross_edge: i64, price_micros: i64) -> i64 {
        let fee = price_micros
            .saturating_mul(1_000_000 - price_micros)
            .saturating_div(1_000_000)
            .saturating_mul(self.config.taker_fee_rate_micros)
            .saturating_div(1_000_000);
        gross_edge - fee - self.config.latency_haircut_micros
    }
}

impl Strategy for DislocationTaker {
    fn name(&self) -> &'static str {
        "dislocation_taker"
    }

    fn on_event(&mut self, state: &MarketState, event: &EventEnvelope) -> Vec<StrategyEvaluation> {
        if !matches!(
            event.event,
            MarketEvent::FairValue { .. } | MarketEvent::Book { .. }
        ) {
            return Vec::new();
        }
        if state.source_seq == self.last_source_seq && state.venue_seq == self.last_venue_seq {
            return Vec::new();
        }
        self.last_source_seq = state.source_seq;
        self.last_venue_seq = state.venue_seq;

        let (Some(fair), Some(bid), Some(ask)) = (state.fair_value, state.best_bid, state.best_ask)
        else {
            return vec![StrategyEvaluation {
                intent: None,
                eligible: false,
                reason: "waiting for both fair value and order book".into(),
            }];
        };

        let (kind, side, price, edge, visible_size, quantity) = if state.position > 0 {
            let entry_edge = self.edge_after_costs(fair - ask, ask.micros());
            if entry_edge >= ENTRY_EDGE_THRESHOLD_MICROS {
                (
                    OrderIntentKind::Entry,
                    Side::Bid,
                    ask,
                    entry_edge,
                    state.ask_size,
                    self.config.order_size.min(state.ask_size),
                )
            } else {
                let exit_edge = self.edge_after_costs(fair - bid, bid.micros());
                (
                    OrderIntentKind::Exit,
                    Side::Ask,
                    bid,
                    exit_edge,
                    state.bid_size,
                    (state.position as u64).min(state.bid_size),
                )
            }
        } else if state.position < 0 {
            let entry_edge = self.edge_after_costs(bid - fair, bid.micros());
            if entry_edge >= ENTRY_EDGE_THRESHOLD_MICROS {
                (
                    OrderIntentKind::Entry,
                    Side::Ask,
                    bid,
                    entry_edge,
                    state.bid_size,
                    self.config.order_size.min(state.bid_size),
                )
            } else {
                let exit_edge = self.edge_after_costs(ask - fair, ask.micros());
                (
                    OrderIntentKind::Exit,
                    Side::Bid,
                    ask,
                    exit_edge,
                    state.ask_size,
                    state.position.unsigned_abs().min(state.ask_size),
                )
            }
        } else {
            let buy_edge = self.edge_after_costs(fair - ask, ask.micros());
            let sell_edge = self.edge_after_costs(bid - fair, bid.micros());
            if buy_edge >= sell_edge {
                (
                    OrderIntentKind::Entry,
                    Side::Bid,
                    ask,
                    buy_edge,
                    state.ask_size,
                    self.config.order_size.min(state.ask_size),
                )
            } else {
                (
                    OrderIntentKind::Entry,
                    Side::Ask,
                    bid,
                    sell_edge,
                    state.bid_size,
                    self.config.order_size.min(state.bid_size),
                )
            }
        };

        let intent = OrderIntent {
            id: deterministic_order_id(state, kind, side),
            market: state.market.clone(),
            kind,
            side,
            limit_price: price,
            quantity,
            expected_edge_micros: edge,
            created_time_ns: event.received_time_ns,
            source_message_id: state.last_message_id.clone(),
            source_proof_ts: state.last_proof_ts,
        };
        let (eligible, reason) = if visible_size == 0 || quantity == 0 {
            (false, "no visible liquidity at the executable price".into())
        } else {
            match kind {
                OrderIntentKind::Entry if edge >= ENTRY_EDGE_THRESHOLD_MICROS => {
                    (true, "entry edge reached the 5% threshold".into())
                }
                OrderIntentKind::Entry => (
                    false,
                    format!(
                        "net edge {edge} micros is below entry threshold {ENTRY_EDGE_THRESHOLD_MICROS} micros"
                    ),
                ),
                OrderIntentKind::Exit if edge <= EXIT_EDGE_THRESHOLD_MICROS => {
                    (true, "edge mean-reverted to the 1% exit threshold".into())
                }
                OrderIntentKind::Exit => (
                    false,
                    format!(
                        "position remains open while edge {edge} micros is above exit threshold {EXIT_EDGE_THRESHOLD_MICROS} micros"
                    ),
                ),
            }
        };
        vec![StrategyEvaluation {
            intent: Some(intent),
            eligible,
            reason,
        }]
    }
}

fn deterministic_order_id(state: &MarketState, kind: OrderIntentKind, side: Side) -> Uuid {
    let mut hasher = Sha256::new();
    hasher.update(state.market.as_bytes());
    hasher.update(state.source_seq.to_le_bytes());
    hasher.update(state.venue_seq.to_le_bytes());
    hasher.update([match kind {
        OrderIntentKind::Entry => 0,
        OrderIntentKind::Exit => 1,
    }]);
    hasher.update([match side {
        Side::Bid => 0,
        Side::Ask => 1,
    }]);
    let digest = hasher.finalize();
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(&digest[..16]);
    Uuid::from_bytes(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{MarketEvent, Price};

    #[test]
    fn emits_only_for_edge_after_costs() {
        let mut strategy = DislocationTaker::new(DislocationConfig::default());
        let mut state = MarketState {
            market: "ARG-USA".into(),
            fair_value: Some(Price::from_micros(650_000).unwrap()),
            best_bid: Some(Price::from_micros(570_000).unwrap()),
            best_ask: Some(Price::from_micros(580_000).unwrap()),
            ask_size: 10,
            source_seq: 1,
            venue_seq: 1,
            ..Default::default()
        };
        let event = EventEnvelope::new(
            100,
            MarketEvent::FairValue {
                market: state.market.clone(),
                probability: state.fair_value.unwrap(),
                source_seq: 1,
                message_id: None,
                proof_ts: None,
            },
        );
        let evaluations = strategy.on_event(&state, &event);
        assert_eq!(evaluations.len(), 1);
        assert!(evaluations[0].eligible);
        assert_eq!(
            evaluations[0].intent.as_ref().unwrap().kind,
            OrderIntentKind::Entry
        );
        assert_eq!(evaluations[0].intent.as_ref().unwrap().quantity, 10);

        state.fair_value = Some(Price::from_micros(585_000).unwrap());
        state.source_seq = 2;
        let event = EventEnvelope::new(
            200,
            MarketEvent::FairValue {
                market: state.market.clone(),
                probability: state.fair_value.unwrap(),
                source_seq: 2,
                message_id: None,
                proof_ts: None,
            },
        );
        let evaluations = strategy.on_event(&state, &event);
        assert_eq!(evaluations.len(), 1);
        assert!(!evaluations[0].eligible);
        assert!(evaluations[0].reason.contains("below entry threshold"));
    }

    #[test]
    fn exits_an_open_position_only_after_mean_reversion() {
        let mut strategy = DislocationTaker::new(DislocationConfig::default());
        let mut state = MarketState {
            market: "ARG-USA".into(),
            fair_value: Some(Price::from_micros(620_000).unwrap()),
            best_bid: Some(Price::from_micros(605_000).unwrap()),
            best_ask: Some(Price::from_micros(615_000).unwrap()),
            bid_size: 100,
            ask_size: 100,
            position: 25,
            source_seq: 1,
            venue_seq: 1,
            ..Default::default()
        };
        let event = EventEnvelope::new(100, MarketEvent::Timer);
        let event = EventEnvelope {
            event: MarketEvent::Book {
                market: state.market.clone(),
                bid: state.best_bid.unwrap(),
                bid_size: state.bid_size,
                ask: state.best_ask.unwrap(),
                ask_size: state.ask_size,
                venue_seq: 1,
            },
            ..event
        };
        let evaluation = strategy.on_event(&state, &event).pop().unwrap();
        assert!(!evaluation.eligible);
        assert_eq!(evaluation.intent.unwrap().kind, OrderIntentKind::Exit);

        state.best_bid = Some(Price::from_micros(610_000).unwrap());
        state.venue_seq = 2;
        let event = EventEnvelope::new(
            200,
            MarketEvent::Book {
                market: state.market.clone(),
                bid: state.best_bid.unwrap(),
                bid_size: state.bid_size,
                ask: state.best_ask.unwrap(),
                ask_size: state.ask_size,
                venue_seq: 2,
            },
        );
        let evaluation = strategy.on_event(&state, &event).pop().unwrap();
        assert!(evaluation.eligible);
        let intent = evaluation.intent.unwrap();
        assert_eq!(intent.kind, OrderIntentKind::Exit);
        assert_eq!(intent.side, Side::Ask);
        assert_eq!(intent.quantity, 25);
    }

    #[test]
    fn scales_open_positions_while_entry_edge_remains_high() {
        for (position, fair_value, expected_side) in
            [(25, 650_000, Side::Bid), (-25, 350_000, Side::Ask)]
        {
            let mut strategy = DislocationTaker::new(DislocationConfig::default());
            let state = MarketState {
                market: "ARG-USA".into(),
                fair_value: Some(Price::from_micros(fair_value).unwrap()),
                best_bid: Some(Price::from_micros(420_000).unwrap()),
                best_ask: Some(Price::from_micros(580_000).unwrap()),
                bid_size: 100,
                ask_size: 100,
                position,
                source_seq: 1,
                venue_seq: 1,
                ..Default::default()
            };
            let event = EventEnvelope::new(
                100,
                MarketEvent::Book {
                    market: state.market.clone(),
                    bid: state.best_bid.unwrap(),
                    bid_size: state.bid_size,
                    ask: state.best_ask.unwrap(),
                    ask_size: state.ask_size,
                    venue_seq: 1,
                },
            );
            let evaluation = strategy.on_event(&state, &event).pop().unwrap();
            assert!(evaluation.eligible);
            let intent = evaluation.intent.unwrap();
            assert_eq!(intent.kind, OrderIntentKind::Entry);
            assert_eq!(intent.side, expected_side);
            assert_eq!(intent.quantity, 25);
        }
    }
}
