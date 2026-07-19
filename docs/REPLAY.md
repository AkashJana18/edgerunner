# Journal and Replay

## Journal Format

EdgeRunner writes schema-versioned newline-delimited JSON. The default path is `data/runs/latest.jsonl`. Opening a service appends to the selected file; it does not erase an existing recording.

Record variants include:

| Record | Purpose |
|---|---|
| `Run` | Service/run metadata and configuration context |
| `Mapping` | Verified TxLINE fixture/outcome to Pascal symbol mapping |
| `Event` | Normalized TxLINE or Pascal input event |
| `Decision` | Strategy action, sizing, edge, latency, and rejection reason |
| `Fill` | Simulated venue execution result |
| `Trade` | Completed position lifecycle and realized PnL |
| `Proof` | Retrieved TxLINE validation proof and provenance |

Treat journals as sensitive operational data: they may contain market identifiers and source provenance. Credentials are not intended to be journaled.

## Record a Live Session

```bash
cargo run -p edgerunner -- serve \
  --live-feeds \
  --journal data/runs/latest.jsonl \
  --config config.example.toml
```

The recording becomes replayable after it contains a valid market mapping and normalized events from both TxLINE and Pascal. TxLINE events must include message ID and proof timestamp provenance.

## Replay from the CLI

```bash
cargo run -p edgerunner -- replay \
  --journal data/runs/latest.jsonl \
  --config config.example.toml
```

Replay reads mapping and event records, then regenerates decisions, fills, completed trades, and PnL through the current engine. Its report includes source counts, decision/fill/trade totals, realized PnL, latency, a decision checksum, and a trade checksum.

Run the same journal with the same binary and configuration twice; both checksums and generated trades should match. A mismatch indicates changed inputs, configuration, or engine behavior.

## Replay from the Dashboard

Set the session to replay and use the replay controls. Available actions are play, pause, reset, seek to an event index, and set speed from `0.25x` through `20x`. These controls change scheduling only; event order and engine calculations remain deterministic.

## Benchmark a Recording

```bash
cargo run --release -p edgerunner -- bench \
  --journal data/runs/latest.jsonl \
  --max-events 100000 \
  --config config.example.toml
```

The benchmark isolates normalized event-to-decision engine work. Do not interpret it as network or end-to-end venue latency.

