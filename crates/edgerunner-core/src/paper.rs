use serde::{Deserialize, Serialize};

use crate::{Fill, MarketState, OrderIntent, OrderMode, Side};

pub trait ExecutionVenue: Send + Sync {
    fn mode(&self) -> OrderMode;
    fn execute(&self, intent: &OrderIntent, state: &MarketState, now_ns: u64) -> Option<Fill>;
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default)]
pub struct PaperConfig {
    pub acknowledgement_delay_ns: u64,
    pub taker_fee_rate_micros: i64,
}

impl Default for PaperConfig {
    fn default() -> Self {
        Self {
            acknowledgement_delay_ns: 2_000_000,
            taker_fee_rate_micros: 1_000,
        }
    }
}

pub struct PaperVenue {
    config: PaperConfig,
}

impl PaperVenue {
    pub fn new(config: PaperConfig) -> Self {
        Self { config }
    }
}

impl ExecutionVenue for PaperVenue {
    fn mode(&self) -> OrderMode {
        OrderMode::Paper
    }

    fn execute(&self, intent: &OrderIntent, state: &MarketState, now_ns: u64) -> Option<Fill> {
        let (price, visible) = match intent.side {
            Side::Bid => (state.best_ask?, state.ask_size),
            Side::Ask => (state.best_bid?, state.bid_size),
        };
        let crosses = match intent.side {
            Side::Bid => intent.limit_price >= price,
            Side::Ask => intent.limit_price <= price,
        };
        if !crosses || visible == 0 {
            return None;
        }
        let quantity = intent.quantity.min(visible);
        let fee_micros = price
            .micros()
            .saturating_mul(1_000_000 - price.micros())
            .saturating_div(1_000_000)
            .saturating_mul(self.config.taker_fee_rate_micros)
            .saturating_div(1_000_000)
            .saturating_mul(quantity as i64);
        Some(Fill {
            order_id: intent.id,
            market: intent.market.clone(),
            side: intent.side,
            price,
            quantity,
            fee_micros,
            acknowledged_time_ns: now_ns.saturating_add(self.config.acknowledgement_delay_ns),
        })
    }
}
