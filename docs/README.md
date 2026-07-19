# EdgeRunner Documentation

EdgeRunner consumes live TxLINE fair probabilities and Pascal market data, makes deterministic trading decisions, and simulates execution. The journal can replay the same normalized events through the same engine path.

## Start Here

| Goal | Document |
|---|---|
| Run the application locally | [Getting started](GETTING_STARTED.md) |
| Tune engine, strategy, risk, and simulation values | [Configuration](CONFIGURATION.md) |
| Understand the system boundaries | [Architecture](ARCHITECTURE.md) |
| Understand entries, exits, sizing, and risk | [Strategy and risk](STRATEGY.md) |
| Record and reproduce a run | [Journal and replay](REPLAY.md) |
| Integrate with or inspect the service | [HTTP API](API.md) |
| Present the project | [Demo runbook](DEMO.md) |

## What EdgeRunner Guarantees

- Identical normalized events and configuration take the same engine path in live and replay modes.
- Prices, probabilities, fees, edge, and PnL use fixed-point integer arithmetic.
- Risk approval is required before every simulated order.
- The journal preserves market events, decisions, fills, completed trades, and TxLINE proof data.
- Inactive or incomplete feed configuration never falls back to generated prices.

## Current Boundaries

- TxLINE and Pascal provide real market data; order execution is simulated locally.
- The wallet button connects and displays a Solana wallet, but it does not authorize backend controls or place trades.
- Session `devnet` and `mainnet` labels select operator context; they do not switch real-capital execution on.
- Decision latency measures local engine computation. It excludes network round-trip time and venue acknowledgement.
