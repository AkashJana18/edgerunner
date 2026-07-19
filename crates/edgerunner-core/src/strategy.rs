use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::{EventEnvelope, MarketEvent, MarketState, OrderIntent, Side};

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
    pub minimum_edge_micros: i64,
    pub latency_haircut_micros: i64,
    pub taker_fee_rate_micros: i64,
    pub order_size: u64,
}

impl Default for DislocationConfig {
    fn default() -> Self {
        Self {
            minimum_edge_micros: 18_000,
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

        let buy_edge = self.edge_after_costs(fair - ask, ask.micros());
        let sell_edge = self.edge_after_costs(bid - fair, bid.micros());
        let (side, price, edge, visible_size) = if buy_edge >= sell_edge {
            (Side::Bid, ask, buy_edge, state.ask_size)
        } else {
            (Side::Ask, bid, sell_edge, state.bid_size)
        };

        let intent = OrderIntent {
            id: deterministic_order_id(state, side),
            market: state.market.clone(),
            side,
            limit_price: price,
            quantity: self.config.order_size.min(visible_size),
            expected_edge_micros: edge,
            created_time_ns: event.received_time_ns,
            source_message_id: state.last_message_id.clone(),
            source_proof_ts: state.last_proof_ts,
        };
        let (eligible, reason) = if visible_size == 0 {
            (false, "no visible liquidity at the executable price".into())
        } else if edge < self.config.minimum_edge_micros {
            (
                false,
                format!(
                    "net edge {edge} micros is below minimum {} micros",
                    self.config.minimum_edge_micros
                ),
            )
        } else {
            (true, "edge survived costs and strategy threshold".into())
        };
        vec![StrategyEvaluation {
            intent: Some(intent),
            eligible,
            reason,
        }]
    }
}

fn deterministic_order_id(state: &MarketState, side: Side) -> Uuid {
    let mut hasher = Sha256::new();
    hasher.update(state.market.as_bytes());
    hasher.update(state.source_seq.to_le_bytes());
    hasher.update(state.venue_seq.to_le_bytes());
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
            fair_value: Some(Price::from_micros(620_000).unwrap()),
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
        assert!(evaluations[0].reason.contains("below minimum"));
    }
}
