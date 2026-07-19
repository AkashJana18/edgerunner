# Configuration Reference

Pass a TOML file with `--config`. If the flag is omitted, EdgeRunner uses the defaults below. Partial TOML files inherit defaults for omitted sections and fields.

```bash
cargo run -p edgerunner -- serve \
  --live-feeds \
  --config config.example.toml
```

## Engine History

| Field | Default | Meaning |
|---|---:|---|
| `decision_history` | `100` | Maximum recent decisions, fills, and trades retained in dashboard snapshots |

## Strategy

| Field | Default | Meaning |
|---|---:|---|
| `strategy.latency_haircut_micros` | `2500` | Fixed 0.25% edge deduction for latency uncertainty |
| `strategy.order_size` | `25` | Maximum contracts in each entry chunk before depth/risk clipping |
| `strategy.taker_fee_rate_micros` | `1000` | Fee-rate input used when deciding whether edge is executable |

The 5% entry and 1% exit thresholds are fixed strategy constants and are intentionally absent from TOML.

## Risk

| Field | Default | Meaning |
|---|---:|---|
| `risk.max_position` | `250` | Maximum absolute contract position |
| `risk.max_notional_micros` | `100000000` | Maximum total position notional in millionths |
| `risk.max_orders_per_minute` | `60` | Rolling one-minute order cap |
| `risk.max_drawdown_micros` | `15000000` | Maximum permitted loss in millionths before rejection |
| `risk.feed_stale_after_ms` | `2500` | Maximum age of either fair-value or book data |

## Simulation

| Field | Default | Meaning |
|---|---:|---|
| `simulation.acknowledgement_delay_ns` | `2000000` | Deterministic simulated acknowledgement delay (2 ms) |
| `simulation.taker_fee_rate_micros` | `1000` | Fee rate charged to simulated fills |

Keep strategy and simulation fee rates aligned so the strategy evaluates the same fee model that execution charges.

## Complete Example

```toml
decision_history = 100

[strategy]
latency_haircut_micros = 2500
order_size = 25
taker_fee_rate_micros = 1000

[risk]
max_position = 250
max_notional_micros = 100000000
max_orders_per_minute = 60
max_drawdown_micros = 15000000
feed_stale_after_ms = 2500

[simulation]
acknowledgement_delay_ns = 2000000
taker_fee_rate_micros = 1000
```

