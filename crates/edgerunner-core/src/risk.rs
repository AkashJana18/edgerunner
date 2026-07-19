use std::collections::VecDeque;

use serde::{Deserialize, Serialize};

use crate::{MarketState, OrderIntent, Side};

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default)]
pub struct RiskConfig {
    pub max_position: i64,
    pub max_notional_micros: i64,
    pub max_orders_per_minute: usize,
    pub max_drawdown_micros: i64,
    pub feed_stale_after_ms: u64,
}

impl Default for RiskConfig {
    fn default() -> Self {
        Self {
            max_position: 250,
            max_notional_micros: 100_000_000,
            max_orders_per_minute: 60,
            max_drawdown_micros: 15_000_000,
            feed_stale_after_ms: 2_500,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum RiskDecision {
    Approved,
    Rejected { reason: String },
}

pub struct RiskEngine {
    config: RiskConfig,
    order_times_ns: VecDeque<u64>,
    killed: bool,
}

impl RiskEngine {
    pub fn new(config: RiskConfig) -> Self {
        Self {
            config,
            order_times_ns: VecDeque::new(),
            killed: false,
        }
    }

    pub fn set_killed(&mut self, killed: bool) {
        self.killed = killed;
    }

    pub fn evaluate(
        &mut self,
        intent: &OrderIntent,
        state: &MarketState,
        now_ns: u64,
    ) -> RiskDecision {
        let reject = |reason: &str| RiskDecision::Rejected {
            reason: reason.to_owned(),
        };
        if self.killed {
            return reject("kill switch active");
        }
        if state.suspended || state.danger {
            return reject("market safety circuit active");
        }
        let stale_ns = self.config.feed_stale_after_ms.saturating_mul(1_000_000);
        if now_ns.saturating_sub(state.last_fair_time_ns) > stale_ns
            || now_ns.saturating_sub(state.last_book_time_ns) > stale_ns
        {
            return reject("feed is stale");
        }
        if state.pnl_micros < -self.config.max_drawdown_micros {
            return reject("drawdown limit exceeded");
        }
        let delta = match intent.side {
            Side::Bid => intent.quantity as i64,
            Side::Ask => -(intent.quantity as i64),
        };
        let resulting_position = state.position.saturating_add(delta);
        if resulting_position.abs() > self.config.max_position {
            return reject("position limit exceeded");
        }
        let notional = intent
            .limit_price
            .micros()
            .saturating_mul(resulting_position.abs());
        if notional > self.config.max_notional_micros {
            return reject("total notional limit exceeded");
        }

        let cutoff = now_ns.saturating_sub(60_000_000_000);
        while self
            .order_times_ns
            .front()
            .is_some_and(|time| *time < cutoff)
        {
            self.order_times_ns.pop_front();
        }
        if self.order_times_ns.len() >= self.config.max_orders_per_minute {
            return reject("order rate exceeded");
        }
        self.order_times_ns.push_back(now_ns);
        RiskDecision::Approved
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{OrderIntentKind, Price, Side};
    use proptest::prelude::*;
    use uuid::Uuid;

    fn intent() -> OrderIntent {
        OrderIntent {
            id: Uuid::new_v4(),
            market: "m".into(),
            kind: OrderIntentKind::Entry,
            side: Side::Bid,
            limit_price: Price::from_micros(500_000).unwrap(),
            quantity: 10,
            expected_edge_micros: 20_000,
            created_time_ns: 1_000,
            source_message_id: None,
            source_proof_ts: None,
        }
    }

    #[test]
    fn rejects_stale_and_killed_orders() {
        let mut risk = RiskEngine::new(RiskConfig::default());
        let state = MarketState {
            last_fair_time_ns: 1,
            last_book_time_ns: 1,
            ..Default::default()
        };
        assert!(matches!(
            risk.evaluate(&intent(), &state, 3_000_000_000),
            RiskDecision::Rejected { .. }
        ));

        risk.set_killed(true);
        assert_eq!(
            risk.evaluate(&intent(), &state, 1),
            RiskDecision::Rejected {
                reason: "kill switch active".into()
            }
        );
    }

    #[test]
    fn notional_limit_covers_resulting_total_position() {
        let mut risk = RiskEngine::new(RiskConfig::default());
        let state = MarketState {
            position: 170,
            last_fair_time_ns: 1_000,
            last_book_time_ns: 1_000,
            ..Default::default()
        };
        let mut order = intent();
        order.quantity = 25;
        order.limit_price = Price::from_micros(600_000).unwrap();
        assert_eq!(
            risk.evaluate(&order, &state, 1_000),
            RiskDecision::Rejected {
                reason: "total notional limit exceeded".into()
            }
        );
    }

    proptest! {
        #[test]
        fn never_approves_a_position_limit_breach(
            position in -200_i64..=200,
            quantity in 1_u64..=100,
            is_bid in any::<bool>(),
        ) {
            let config = RiskConfig {
                max_position: 100,
                max_notional_micros: i64::MAX,
                ..RiskConfig::default()
            };
            let mut risk = RiskEngine::new(config);
            let state = MarketState {
                position,
                last_fair_time_ns: 10,
                last_book_time_ns: 10,
                ..Default::default()
            };
            let mut order = intent();
            order.side = if is_bid { Side::Bid } else { Side::Ask };
            order.quantity = quantity;
            let delta = if is_bid { quantity as i64 } else { -(quantity as i64) };
            if position.saturating_add(delta).abs() > 100 {
                let rejected = matches!(
                    risk.evaluate(&order, &state, 10),
                    RiskDecision::Rejected { ref reason } if reason == "position limit exceeded"
                );
                prop_assert!(rejected);
            }
        }
    }
}
