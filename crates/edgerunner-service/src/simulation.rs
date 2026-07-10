use edgerunner_core::{EventEnvelope, MarketEvent, Price};

pub fn events(tick: u64, now_ns: u64) -> Vec<EventEnvelope> {
    let market = "FIFA-WC-FRA-BRA".to_owned();
    let wave = ((tick % 20) as i64 - 10).abs();
    let fair_micros =
        585_000 + (10 - wave) * 2_000 + if tick.is_multiple_of(17) { 31_000 } else { 0 };
    let venue_mid = 584_000 + ((tick * 3 % 13) as i64 - 6) * 1_000;
    let danger = tick.is_multiple_of(37);
    let suspended = tick.is_multiple_of(91);

    let mut events = vec![
        EventEnvelope::new(
            now_ns,
            MarketEvent::Book {
                market: market.clone(),
                bid: Price::from_micros((venue_mid - 6_000).clamp(10_000, 980_000)).unwrap(),
                bid_size: 55 + tick % 40,
                ask: Price::from_micros((venue_mid + 6_000).clamp(20_000, 990_000)).unwrap(),
                ask_size: 45 + (tick * 7) % 50,
                venue_seq: tick,
            },
        ),
        EventEnvelope::new(
            now_ns + 20_000,
            MarketEvent::FairValue {
                market: market.clone(),
                probability: Price::from_micros(fair_micros.clamp(10_000, 990_000)).unwrap(),
                source_seq: tick,
                message_id: None,
                proof_ts: None,
            },
        ),
    ];
    if tick.is_multiple_of(12) || danger || suspended {
        events.push(EventEnvelope::new(
            now_ns + 30_000,
            MarketEvent::Score {
                market,
                phase: if tick > 100 { "H2" } else { "H1" }.into(),
                home: (tick / 71) as u16,
                away: (tick / 113) as u16,
                danger,
                suspended,
            },
        ));
    }
    events
}
