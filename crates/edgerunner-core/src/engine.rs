use std::{
    collections::{BTreeMap, VecDeque},
    time::Instant,
};

use chrono::Utc;
use hdrhistogram::Histogram;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    DecisionRecord, EngineSnapshot, EventEnvelope, ExecutionVenue, FeedStatus, Fill,
    LatencySnapshot, MarketState, OrderMode, PaperConfig, PaperVenue, RiskConfig, RiskDecision,
    RiskEngine, Strategy,
};

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default)]
pub struct EngineConfig {
    pub mode: OrderMode,
    pub risk: RiskConfig,
    pub paper: PaperConfig,
    pub decision_history: usize,
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            mode: OrderMode::Paper,
            risk: RiskConfig::default(),
            paper: PaperConfig::default(),
            decision_history: 100,
        }
    }
}

#[derive(Clone, Debug)]
pub struct EngineOutput {
    pub decision: DecisionRecord,
    pub fill: Option<Fill>,
}

pub struct Engine<S: Strategy> {
    pub run_id: Uuid,
    pub state: MarketState,
    pub processed_events: u64,
    pub ignored_events: u64,
    pub rejected_orders: u64,
    pub decisions: VecDeque<DecisionRecord>,
    pub fills: VecDeque<Fill>,
    strategy: S,
    risk: RiskEngine,
    venue: Box<dyn ExecutionVenue>,
    config: EngineConfig,
    latency: Histogram<u64>,
}

impl<S: Strategy> Engine<S> {
    pub fn new(strategy: S, config: EngineConfig) -> Self {
        Self {
            run_id: Uuid::new_v4(),
            state: MarketState::default(),
            processed_events: 0,
            ignored_events: 0,
            rejected_orders: 0,
            decisions: VecDeque::new(),
            fills: VecDeque::new(),
            strategy,
            risk: RiskEngine::new(config.risk.clone()),
            venue: Box::new(PaperVenue::new(config.paper.clone())),
            config,
            latency: Histogram::new_with_bounds(1, 60_000_000, 3).expect("valid histogram bounds"),
        }
    }

    pub fn set_killed(&mut self, killed: bool) {
        self.risk.set_killed(killed);
    }

    pub fn snapshot(
        &self,
        running: bool,
        killed: bool,
        feed_status: BTreeMap<String, FeedStatus>,
    ) -> EngineSnapshot {
        EngineSnapshot {
            run_id: self.run_id,
            mode: self.config.mode,
            running,
            killed,
            feed_status,
            markets: vec![self.state.clone()],
            decisions: self.decisions.clone(),
            fills: self.fills.clone(),
            latency: self.latency_snapshot(),
            processed_events: self.processed_events,
            ignored_events: self.ignored_events,
            rejected_orders: self.rejected_orders,
            last_update: Utc::now(),
        }
    }

    pub fn process(&mut self, event: EventEnvelope) -> Vec<EngineOutput> {
        let started = Instant::now();
        self.processed_events += 1;
        if !self.state.apply(&event) {
            self.ignored_events += 1;
            return Vec::new();
        }
        self.mark_to_market();
        let intents = self.strategy.on_event(&self.state, &event);
        let mut outputs = Vec::with_capacity(intents.len().max(1));

        for intent in intents {
            let (action, reason, fill): (&str, String, Option<Fill>) =
                match self
                    .risk
                    .evaluate(&intent, &self.state, event.received_time_ns)
                {
                    RiskDecision::Approved => {
                        let fill = self
                            .venue
                            .execute(&intent, &self.state, event.received_time_ns);
                        if let Some(ref fill) = fill {
                            self.apply_fill(fill);
                        }
                        ("submitted", "edge survived costs and risk".to_owned(), fill)
                    }
                    RiskDecision::Rejected { reason } => {
                        self.rejected_orders += 1;
                        ("rejected", reason, None)
                    }
                };

            let latency_ns = started.elapsed().as_nanos().min(u64::MAX as u128) as u64;
            let _ = self.latency.record(latency_ns.max(1));
            let decision = DecisionRecord {
                id: Uuid::new_v4(),
                at: Utc::now(),
                event_id: event.id,
                market: intent.market.clone(),
                action: action.to_owned(),
                reason,
                intent: Some(intent),
                decision_latency_ns: latency_ns,
            };
            self.push_decision(decision.clone());
            if let Some(ref fill) = fill {
                self.push_fill(fill.clone());
            }
            outputs.push(EngineOutput { decision, fill });
        }
        outputs
    }

    pub fn latency_snapshot(&self) -> LatencySnapshot {
        if self.latency.is_empty() {
            return LatencySnapshot::default();
        }
        LatencySnapshot {
            samples: self.latency.len(),
            p50_us: self.latency.value_at_quantile(0.50).div_ceil(1_000),
            p95_us: self.latency.value_at_quantile(0.95).div_ceil(1_000),
            p99_us: self.latency.value_at_quantile(0.99).div_ceil(1_000),
            max_us: self.latency.max().div_ceil(1_000),
        }
    }

    fn apply_fill(&mut self, fill: &Fill) {
        let signed_quantity = match fill.side {
            crate::Side::Bid => fill.quantity as i64,
            crate::Side::Ask => -(fill.quantity as i64),
        };
        self.state.position = self.state.position.saturating_add(signed_quantity);
        match fill.side {
            crate::Side::Bid => {
                self.state.ask_size = self.state.ask_size.saturating_sub(fill.quantity)
            }
            crate::Side::Ask => {
                self.state.bid_size = self.state.bid_size.saturating_sub(fill.quantity)
            }
        }
        self.state.cash_micros = self
            .state
            .cash_micros
            .saturating_sub(signed_quantity.saturating_mul(fill.price.micros()))
            .saturating_sub(fill.fee_micros);
        self.state.fees_micros = self.state.fees_micros.saturating_add(fill.fee_micros);
        self.mark_to_market();
    }

    fn mark_to_market(&mut self) {
        let Some(fair) = self.state.fair_value else {
            return;
        };
        self.state.pnl_micros = self
            .state
            .cash_micros
            .saturating_add(self.state.position.saturating_mul(fair.micros()));
    }

    fn push_decision(&mut self, decision: DecisionRecord) {
        self.decisions.push_front(decision);
        self.decisions.truncate(self.config.decision_history);
    }

    fn push_fill(&mut self, fill: Fill) {
        self.fills.push_front(fill);
        self.fills.truncate(self.config.decision_history);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{DislocationConfig, DislocationTaker, MarketEvent, Price};

    #[test]
    fn paper_fill_consumes_visible_depth_and_marks_inventory() {
        let mut engine = Engine::new(
            DislocationTaker::new(DislocationConfig::default()),
            EngineConfig::default(),
        );
        let market = "FIFA-WC-FRA-BRA".to_owned();
        engine.process(EventEnvelope::new(
            1_000_000,
            MarketEvent::Book {
                market: market.clone(),
                bid: Price::from_micros(540_000).unwrap(),
                bid_size: 100,
                ask: Price::from_micros(550_000).unwrap(),
                ask_size: 100,
                venue_seq: 1,
            },
        ));
        let output = engine.process(EventEnvelope::new(
            1_020_000,
            MarketEvent::FairValue {
                market,
                probability: Price::from_micros(610_000).unwrap(),
                source_seq: 1,
                message_id: Some("message-1".into()),
                proof_ts: Some(1_020),
            },
        ));
        assert_eq!(output.len(), 1);
        assert!(output[0].fill.is_some());
        assert_eq!(engine.state.position, 25);
        assert_eq!(engine.state.ask_size, 75);
        assert!(engine.state.pnl_micros > 0);
    }

    #[test]
    fn ignores_duplicate_and_out_of_order_market_data() {
        let mut engine = Engine::new(
            DislocationTaker::new(DislocationConfig::default()),
            EngineConfig::default(),
        );
        let market = "FIFA-WC-FRA-BRA".to_owned();
        for (sequence, probability) in [(2, 610_000), (2, 300_000), (1, 200_000)] {
            engine.process(EventEnvelope::new(
                sequence * 1_000,
                MarketEvent::FairValue {
                    market: market.clone(),
                    probability: Price::from_micros(probability).unwrap(),
                    source_seq: sequence,
                    message_id: None,
                    proof_ts: None,
                },
            ));
        }
        assert_eq!(engine.state.source_seq, 2);
        assert_eq!(engine.state.fair_value.unwrap().micros(), 610_000);
        assert_eq!(engine.ignored_events, 2);
    }
}
