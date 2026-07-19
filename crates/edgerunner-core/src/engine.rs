use std::{
    collections::{BTreeMap, VecDeque},
    time::Instant,
};

use chrono::{DateTime, Utc};
use hdrhistogram::Histogram;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    DecisionRecord, EngineSnapshot, EventEnvelope, ExecutionVenue, FeedStatus, Fill,
    LatencySnapshot, MarketState, NextOrderRequirement, OrderIntent, OrderIntentKind, OrderMode,
    PaperConfig, PaperVenue, PositionLifecycle, PositionStatus, Price, RiskCapacitySnapshot,
    RiskConfig, RiskDecision, RiskEngine, SCALE, Side, Strategy, TradeAction, TradeEvent,
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
    pub trade: Option<TradeEvent>,
}

#[derive(Clone, Debug)]
struct ActivePosition {
    entry_side: Side,
    entry_time_ns: u64,
    remaining_quantity: u64,
    remaining_entry_notional_micros: i64,
    remaining_entry_fee_micros: i64,
    exited_quantity: u64,
    exit_notional_micros: i64,
    realized_pnl_micros: i64,
}

pub struct Engine<S: Strategy> {
    pub run_id: Uuid,
    pub state: MarketState,
    pub processed_events: u64,
    pub ignored_events: u64,
    pub rejected_orders: u64,
    pub decisions: VecDeque<DecisionRecord>,
    pub fills: VecDeque<Fill>,
    pub trades: VecDeque<TradeEvent>,
    strategy: S,
    risk: RiskEngine,
    venue: Box<dyn ExecutionVenue>,
    config: EngineConfig,
    latency: Histogram<u64>,
    active_position: Option<ActivePosition>,
    position_lifecycle: PositionLifecycle,
    last_event_time_ns: u64,
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
            trades: VecDeque::new(),
            strategy,
            risk: RiskEngine::new(config.risk.clone()),
            venue: Box::new(PaperVenue::new(config.paper.clone())),
            config,
            latency: Histogram::new_with_bounds(1, 60_000_000, 3).expect("valid histogram bounds"),
            active_position: None,
            position_lifecycle: PositionLifecycle::default(),
            last_event_time_ns: 0,
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
        let mut position_lifecycle = self.position_lifecycle.clone();
        if position_lifecycle.status == PositionStatus::Open
            && let Some(entry_time) = self
                .active_position
                .as_ref()
                .map(|position| position.entry_time_ns)
        {
            position_lifecycle.holding_time_ns = self.last_event_time_ns.saturating_sub(entry_time);
        }
        EngineSnapshot {
            run_id: self.run_id,
            mode: self.config.mode,
            running,
            killed,
            feed_status,
            markets: vec![self.state.clone()],
            decisions: self.decisions.clone(),
            fills: self.fills.clone(),
            trades: self.trades.clone(),
            position_lifecycle,
            latency: self.latency_snapshot(),
            next_order_requirement: self.next_order_requirement(),
            risk_capacity: self.risk_capacity_snapshot(),
            processed_events: self.processed_events,
            ignored_events: self.ignored_events,
            rejected_orders: self.rejected_orders,
            last_update: Utc::now(),
        }
    }

    fn next_order_requirement(&self) -> Option<NextOrderRequirement> {
        let decision = self.decisions.front()?;
        let intent = decision.intent.as_ref()?;
        let price_micros = intent.limit_price.micros();
        let collateral_micros = if intent.kind == OrderIntentKind::Exit {
            0
        } else {
            let collateral_per_contract = match intent.side {
                Side::Bid => price_micros,
                Side::Ask => SCALE.saturating_sub(price_micros),
            };
            collateral_per_contract.saturating_mul(intent.quantity as i64)
        };
        let fee_micros = self.config.paper.fee_micros(price_micros, intent.quantity);
        Some(NextOrderRequirement {
            side: intent.side,
            quantity: intent.quantity,
            price: intent.limit_price,
            collateral_micros,
            fee_micros,
            required_funds_micros: collateral_micros.saturating_add(fee_micros),
            decision_status: decision.action.clone(),
        })
    }

    fn entry_capacity(&self, side: Side, price: Price) -> RiskCapacitySnapshot {
        let position_limit = self.config.risk.max_position.max(0);
        let position_limit_u64 = position_limit as u64;
        let notional_limit = self.config.risk.max_notional_micros.max(0);
        let notional_position_limit = if price.micros() == 0 {
            position_limit_u64
        } else {
            notional_limit.saturating_div(price.micros()) as u64
        };
        let effective_position_limit = position_limit_u64.min(notional_position_limit);
        let adds_same_direction = self.state.position == 0
            || (self.state.position > 0 && side == Side::Bid)
            || (self.state.position < 0 && side == Side::Ask);
        let remaining_contracts = if adds_same_direction {
            effective_position_limit.saturating_sub(self.state.position.unsigned_abs())
        } else {
            0
        };
        let limiting_gate = if !adds_same_direction {
            "direction"
        } else if notional_position_limit < position_limit_u64 {
            "notional"
        } else {
            "position"
        };
        RiskCapacitySnapshot {
            position_limit,
            notional_limit_micros: notional_limit,
            effective_position_limit,
            remaining_contracts,
            limiting_gate: limiting_gate.into(),
        }
    }

    fn risk_capacity_snapshot(&self) -> RiskCapacitySnapshot {
        let (side, price) = if self.state.position > 0 {
            (Side::Bid, self.state.best_ask.unwrap_or(Price::ZERO))
        } else if self.state.position < 0 {
            (Side::Ask, self.state.best_bid.unwrap_or(Price::ZERO))
        } else if let Some(intent) = self
            .decisions
            .front()
            .and_then(|decision| decision.intent.as_ref())
        {
            match (intent.kind, intent.side) {
                (OrderIntentKind::Entry, side) => (side, intent.limit_price),
                (OrderIntentKind::Exit, Side::Ask) => {
                    (Side::Bid, self.state.best_ask.unwrap_or(Price::ZERO))
                }
                (OrderIntentKind::Exit, Side::Bid) => {
                    (Side::Ask, self.state.best_bid.unwrap_or(Price::ZERO))
                }
            }
        } else {
            let buy_edge = self
                .state
                .fair_value
                .zip(self.state.best_ask)
                .map(|(fair, ask)| fair - ask)
                .unwrap_or_default();
            let sell_edge = self
                .state
                .best_bid
                .zip(self.state.fair_value)
                .map(|(bid, fair)| bid - fair)
                .unwrap_or_default();
            if sell_edge > buy_edge {
                (Side::Ask, self.state.best_bid.unwrap_or(Price::ZERO))
            } else {
                (Side::Bid, self.state.best_ask.unwrap_or(Price::ZERO))
            }
        };
        self.entry_capacity(side, price)
    }

    pub fn process(&mut self, event: EventEnvelope) -> Vec<EngineOutput> {
        let started = Instant::now();
        self.processed_events += 1;
        if !self.state.apply(&event) {
            self.ignored_events += 1;
            return Vec::new();
        }
        self.last_event_time_ns = event.received_time_ns;
        self.mark_to_market();
        let evaluations = self.strategy.on_event(&self.state, &event);
        let mut outputs = Vec::with_capacity(evaluations.len().max(1));

        for mut evaluation in evaluations {
            if evaluation.eligible
                && let Some(intent) = evaluation.intent.as_mut()
                && intent.kind == OrderIntentKind::Entry
            {
                let capacity = self.entry_capacity(intent.side, intent.limit_price);
                let requested_quantity = intent.quantity;
                intent.quantity = intent.quantity.min(capacity.remaining_contracts);
                if intent.quantity == 0 {
                    evaluation.eligible = false;
                    evaluation.reason = format!(
                        "effective risk capacity reached at {} contracts ({})",
                        capacity.effective_position_limit, capacity.limiting_gate
                    );
                } else if intent.quantity < requested_quantity {
                    evaluation.reason = format!(
                        "entry edge reached the 5% threshold; clipped to {} contracts by {} capacity",
                        intent.quantity, capacity.limiting_gate
                    );
                }
            }
            let market = evaluation
                .intent
                .as_ref()
                .map(|intent| intent.market.clone())
                .unwrap_or_else(|| self.state.market.clone());
            let strategy_reason = evaluation.reason.clone();
            let (action, reason, fill, trade): (&str, String, Option<Fill>, Option<TradeEvent>) =
                if !evaluation.eligible {
                    ("skipped", evaluation.reason, None, None)
                } else if let Some(intent) = evaluation.intent.as_ref() {
                    match self
                        .risk
                        .evaluate(intent, &self.state, event.received_time_ns)
                    {
                        RiskDecision::Approved => {
                            let fill =
                                self.venue
                                    .execute(intent, &self.state, event.received_time_ns);
                            let trade = fill.as_ref().map(|fill| self.apply_fill(fill, intent));
                            ("submitted", strategy_reason, fill, trade)
                        }
                        RiskDecision::Rejected { reason } => {
                            self.rejected_orders += 1;
                            ("rejected", reason, None, None)
                        }
                    }
                } else {
                    self.rejected_orders += 1;
                    (
                        "rejected",
                        "eligible evaluation has no order candidate".into(),
                        None,
                        None,
                    )
                };

            let latency_ns = started.elapsed().as_nanos().min(u64::MAX as u128) as u64;
            let _ = self.latency.record(latency_ns.max(1));
            let decision = DecisionRecord {
                id: Uuid::new_v4(),
                at: datetime_from_ns(event.received_time_ns),
                event_id: event.id,
                market,
                action: action.to_owned(),
                reason,
                intent: evaluation.intent,
                compute_latency_ns: latency_ns,
            };
            self.push_decision(decision.clone());
            if let Some(ref fill) = fill {
                self.push_fill(fill.clone());
            }
            if let Some(ref trade) = trade {
                self.push_trade(trade.clone());
            }
            outputs.push(EngineOutput {
                decision,
                fill,
                trade,
            });
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

    fn apply_fill(&mut self, fill: &Fill, intent: &OrderIntent) -> TradeEvent {
        let trade = self.track_position(fill, intent);
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
        trade
    }

    fn track_position(&mut self, fill: &Fill, intent: &OrderIntent) -> TradeEvent {
        let timestamp = datetime_from_ns(fill.acknowledged_time_ns);
        let mut realized_pnl_micros = 0;
        match intent.kind {
            OrderIntentKind::Entry => {
                let entry_notional = fill.price.micros().saturating_mul(fill.quantity as i64);
                if let Some(active) = self.active_position.as_mut() {
                    debug_assert_eq!(active.entry_side, fill.side);
                    active.remaining_quantity =
                        active.remaining_quantity.saturating_add(fill.quantity);
                    active.remaining_entry_notional_micros = active
                        .remaining_entry_notional_micros
                        .saturating_add(entry_notional);
                    active.remaining_entry_fee_micros = active
                        .remaining_entry_fee_micros
                        .saturating_add(fill.fee_micros);
                    let average_entry_price = active
                        .remaining_entry_notional_micros
                        .saturating_div(active.remaining_quantity.max(1) as i64);
                    self.position_lifecycle.entry_price =
                        Price::from_micros(average_entry_price).ok();
                    self.position_lifecycle.status = PositionStatus::Open;
                } else {
                    self.active_position = Some(ActivePosition {
                        entry_side: fill.side,
                        entry_time_ns: fill.acknowledged_time_ns,
                        remaining_quantity: fill.quantity,
                        remaining_entry_notional_micros: entry_notional,
                        remaining_entry_fee_micros: fill.fee_micros,
                        exited_quantity: 0,
                        exit_notional_micros: 0,
                        realized_pnl_micros: 0,
                    });
                    self.position_lifecycle = PositionLifecycle {
                        status: PositionStatus::Open,
                        entry_price: Some(fill.price),
                        exit_price: None,
                        entry_time: Some(timestamp),
                        exit_time: None,
                        holding_time_ns: 0,
                        realized_pnl_micros: 0,
                    };
                }
            }
            OrderIntentKind::Exit => {
                if let Some(active) = self.active_position.as_mut() {
                    let closed_quantity = fill.quantity.min(active.remaining_quantity);
                    let entry_notional = if closed_quantity == active.remaining_quantity {
                        active.remaining_entry_notional_micros
                    } else {
                        active
                            .remaining_entry_notional_micros
                            .saturating_mul(closed_quantity as i64)
                            .saturating_div(active.remaining_quantity as i64)
                    };
                    let entry_fee = if closed_quantity == active.remaining_quantity {
                        active.remaining_entry_fee_micros
                    } else {
                        active
                            .remaining_entry_fee_micros
                            .saturating_mul(closed_quantity as i64)
                            .saturating_div(active.remaining_quantity as i64)
                    };
                    let gross_pnl = match active.entry_side {
                        Side::Bid => fill
                            .price
                            .micros()
                            .saturating_mul(closed_quantity as i64)
                            .saturating_sub(entry_notional),
                        Side::Ask => entry_notional.saturating_sub(
                            fill.price.micros().saturating_mul(closed_quantity as i64),
                        ),
                    };
                    realized_pnl_micros = gross_pnl
                        .saturating_sub(entry_fee)
                        .saturating_sub(fill.fee_micros);
                    active.remaining_quantity =
                        active.remaining_quantity.saturating_sub(closed_quantity);
                    active.remaining_entry_notional_micros = active
                        .remaining_entry_notional_micros
                        .saturating_sub(entry_notional);
                    active.remaining_entry_fee_micros =
                        active.remaining_entry_fee_micros.saturating_sub(entry_fee);
                    active.exited_quantity = active.exited_quantity.saturating_add(closed_quantity);
                    active.exit_notional_micros = active
                        .exit_notional_micros
                        .saturating_add(fill.price.micros().saturating_mul(closed_quantity as i64));
                    active.realized_pnl_micros = active
                        .realized_pnl_micros
                        .saturating_add(realized_pnl_micros);

                    let exit_price_micros = active
                        .exit_notional_micros
                        .saturating_div(active.exited_quantity.max(1) as i64);
                    self.position_lifecycle.exit_price = Price::from_micros(exit_price_micros).ok();
                    self.position_lifecycle.exit_time = Some(timestamp);
                    self.position_lifecycle.holding_time_ns = fill
                        .acknowledged_time_ns
                        .saturating_sub(active.entry_time_ns);
                    self.position_lifecycle.realized_pnl_micros = active.realized_pnl_micros;
                    if active.remaining_quantity == 0 {
                        self.position_lifecycle.status = PositionStatus::Closed;
                        self.active_position = None;
                    }
                }
            }
        }

        TradeEvent {
            order_id: fill.order_id,
            market: fill.market.clone(),
            kind: intent.kind,
            action: match fill.side {
                Side::Bid => TradeAction::Buy,
                Side::Ask => TradeAction::Sell,
            },
            timestamp,
            price: fill.price,
            edge_micros: intent.expected_edge_micros,
            quantity: fill.quantity,
            realized_pnl_micros,
        }
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

    fn push_trade(&mut self, trade: TradeEvent) {
        self.trades.push_front(trade);
        self.trades.truncate(self.config.decision_history);
    }
}

fn datetime_from_ns(timestamp_ns: u64) -> DateTime<Utc> {
    let seconds = (timestamp_ns / 1_000_000_000).min(i64::MAX as u64) as i64;
    let nanos = (timestamp_ns % 1_000_000_000) as u32;
    DateTime::from_timestamp(seconds, nanos).unwrap_or(DateTime::UNIX_EPOCH)
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
        let market = "market-a".to_owned();
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
    fn mean_reversion_exit_closes_position_and_records_realized_pnl() {
        let mut engine = Engine::new(
            DislocationTaker::new(DislocationConfig::default()),
            EngineConfig::default(),
        );
        let market = "market-a".to_owned();
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
        let entry = engine.process(EventEnvelope::new(
            1_020_000,
            MarketEvent::FairValue {
                market: market.clone(),
                probability: Price::from_micros(620_000).unwrap(),
                source_seq: 1,
                message_id: Some("message-1".into()),
                proof_ts: Some(1_020),
            },
        ));
        assert_eq!(entry[0].trade.as_ref().unwrap().action, TradeAction::Buy);
        assert_eq!(engine.position_lifecycle.status, PositionStatus::Open);

        let exit = engine.process(EventEnvelope::new(
            1_040_000,
            MarketEvent::Book {
                market,
                bid: Price::from_micros(610_000).unwrap(),
                bid_size: 100,
                ask: Price::from_micros(620_000).unwrap(),
                ask_size: 100,
                venue_seq: 2,
            },
        ));
        let exit_trade = exit[0].trade.as_ref().unwrap();
        assert_eq!(exit_trade.action, TradeAction::Sell);
        assert_eq!(exit_trade.kind, OrderIntentKind::Exit);
        assert!(exit_trade.realized_pnl_micros > 0);
        assert_eq!(engine.state.position, 0);

        let lifecycle = engine
            .snapshot(true, false, BTreeMap::new())
            .position_lifecycle;
        assert_eq!(lifecycle.status, PositionStatus::Closed);
        assert_eq!(lifecycle.entry_price.unwrap().micros(), 550_000);
        assert_eq!(lifecycle.exit_price.unwrap().micros(), 610_000);
        assert_eq!(lifecycle.holding_time_ns, 20_000);
        assert_eq!(
            lifecycle.realized_pnl_micros,
            exit_trade.realized_pnl_micros
        );
        assert_eq!(lifecycle.realized_pnl_micros, engine.state.pnl_micros);
    }

    #[test]
    fn scales_to_exact_effective_capacity_without_rejections() {
        let mut engine = Engine::new(
            DislocationTaker::new(DislocationConfig::default()),
            EngineConfig::default(),
        );
        let market = "market-a".to_owned();
        engine.process(EventEnvelope::new(
            1_000_000,
            MarketEvent::Book {
                market: market.clone(),
                bid: Price::from_micros(590_000).unwrap(),
                bid_size: 100,
                ask: Price::from_micros(600_000).unwrap(),
                ask_size: 100,
                venue_seq: 1,
            },
        ));
        engine.process(EventEnvelope::new(
            1_020_000,
            MarketEvent::FairValue {
                market: market.clone(),
                probability: Price::from_micros(680_000).unwrap(),
                source_seq: 1,
                message_id: Some("message-1".into()),
                proof_ts: Some(1_020),
            },
        ));

        let mut final_entry_quantity = 0;
        for venue_seq in 2..=7 {
            let output = engine.process(EventEnvelope::new(
                1_020_000 + venue_seq * 20_000,
                MarketEvent::Book {
                    market: market.clone(),
                    bid: Price::from_micros(590_000).unwrap(),
                    bid_size: 100,
                    ask: Price::from_micros(600_000).unwrap(),
                    ask_size: 100,
                    venue_seq,
                },
            ));
            final_entry_quantity = output[0].fill.as_ref().unwrap().quantity;
        }
        assert_eq!(engine.state.position, 166);
        assert_eq!(final_entry_quantity, 16);

        let at_capacity = engine.process(EventEnvelope::new(
            1_180_000,
            MarketEvent::Book {
                market,
                bid: Price::from_micros(590_000).unwrap(),
                bid_size: 100,
                ask: Price::from_micros(600_000).unwrap(),
                ask_size: 100,
                venue_seq: 8,
            },
        ));
        assert_eq!(at_capacity[0].decision.action, "skipped");
        assert!(at_capacity[0].decision.reason.contains("capacity reached"));
        assert!(at_capacity[0].fill.is_none());
        assert_eq!(engine.rejected_orders, 0);

        let capacity = engine.snapshot(true, false, BTreeMap::new()).risk_capacity;
        assert_eq!(capacity.position_limit, 250);
        assert_eq!(capacity.effective_position_limit, 166);
        assert_eq!(capacity.remaining_contracts, 0);
        assert_eq!(capacity.limiting_gate, "notional");
    }

    #[test]
    fn aggregates_multiple_entries_into_one_deterministic_cost_basis() {
        let mut engine = Engine::new(
            DislocationTaker::new(DislocationConfig::default()),
            EngineConfig::default(),
        );
        let market = "market-a".to_owned();
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
        engine.process(EventEnvelope::new(
            1_020_000,
            MarketEvent::FairValue {
                market: market.clone(),
                probability: Price::from_micros(650_000).unwrap(),
                source_seq: 1,
                message_id: Some("message-1".into()),
                proof_ts: Some(1_020),
            },
        ));
        engine.process(EventEnvelope::new(
            1_040_000,
            MarketEvent::Book {
                market: market.clone(),
                bid: Price::from_micros(550_000).unwrap(),
                bid_size: 100,
                ask: Price::from_micros(560_000).unwrap(),
                ask_size: 100,
                venue_seq: 2,
            },
        ));
        assert_eq!(engine.state.position, 50);
        assert_eq!(
            engine.position_lifecycle.entry_price.unwrap().micros(),
            555_000
        );

        let exit = engine.process(EventEnvelope::new(
            1_060_000,
            MarketEvent::Book {
                market,
                bid: Price::from_micros(640_000).unwrap(),
                bid_size: 100,
                ask: Price::from_micros(650_000).unwrap(),
                ask_size: 100,
                venue_seq: 3,
            },
        ));
        let exit_trade = exit[0].trade.as_ref().unwrap();
        assert_eq!(exit_trade.quantity, 50);
        assert!(exit_trade.realized_pnl_micros > 0);
        assert_eq!(engine.state.position, 0);
        assert_eq!(engine.position_lifecycle.status, PositionStatus::Closed);
        assert_eq!(
            engine.position_lifecycle.realized_pnl_micros,
            engine.state.pnl_micros
        );
    }

    #[test]
    fn snapshot_projects_buy_and_sell_collateral_including_fees() {
        let cases = [
            (800_000, Side::Bid, 666_000, 16_650_000, 5_550),
            (200_000, Side::Ask, 644_000, 8_900_000, 5_725),
        ];
        for (fair_value, side, price, collateral, fee) in cases {
            let mut engine = Engine::new(
                DislocationTaker::new(DislocationConfig::default()),
                EngineConfig::default(),
            );
            let market = "market-a".to_owned();
            engine.process(EventEnvelope::new(
                1_000_000,
                MarketEvent::Book {
                    market: market.clone(),
                    bid: Price::from_micros(644_000).unwrap(),
                    bid_size: 100,
                    ask: Price::from_micros(666_000).unwrap(),
                    ask_size: 100,
                    venue_seq: 1,
                },
            ));
            engine.process(EventEnvelope::new(
                1_020_000,
                MarketEvent::FairValue {
                    market,
                    probability: Price::from_micros(fair_value).unwrap(),
                    source_seq: 1,
                    message_id: None,
                    proof_ts: None,
                },
            ));

            let requirement = engine
                .snapshot(true, false, BTreeMap::new())
                .next_order_requirement
                .unwrap();
            assert_eq!(requirement.side, side);
            assert_eq!(requirement.quantity, 25);
            assert_eq!(requirement.price.micros(), price);
            assert_eq!(requirement.collateral_micros, collateral);
            assert_eq!(requirement.fee_micros, fee);
            assert_eq!(requirement.required_funds_micros, collateral + fee);
            assert_eq!(requirement.decision_status, "submitted");
        }
    }

    #[test]
    fn snapshot_without_an_order_requirement_remains_backward_compatible() {
        let engine = Engine::new(
            DislocationTaker::new(DislocationConfig::default()),
            EngineConfig::default(),
        );
        let snapshot = engine.snapshot(true, false, BTreeMap::new());
        assert!(snapshot.next_order_requirement.is_none());

        let mut serialized = serde_json::to_value(snapshot).unwrap();
        serialized
            .as_object_mut()
            .unwrap()
            .remove("next_order_requirement");
        serialized.as_object_mut().unwrap().remove("trades");
        serialized
            .as_object_mut()
            .unwrap()
            .remove("position_lifecycle");
        serialized.as_object_mut().unwrap().remove("risk_capacity");
        let restored: EngineSnapshot = serde_json::from_value(serialized).unwrap();
        assert!(restored.next_order_requirement.is_none());
        assert!(restored.trades.is_empty());
        assert_eq!(restored.position_lifecycle, PositionLifecycle::default());
        assert_eq!(restored.risk_capacity, RiskCapacitySnapshot::default());
    }

    #[test]
    fn ignores_duplicate_and_out_of_order_market_data() {
        let mut engine = Engine::new(
            DislocationTaker::new(DislocationConfig::default()),
            EngineConfig::default(),
        );
        let market = "market-a".to_owned();
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
