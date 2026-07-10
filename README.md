# EdgeRunner

EdgeRunner is a deterministic, low-latency trading engine for sports prediction markets. It consumes TxLINE fair probabilities and a venue L2 order book, evaluates a fixed-point dislocation strategy, applies inline risk gates, and sends approved intents to a paper execution venue.

The repository is intentionally useful without credentials: the default service runs a deterministic World Cup market simulation, writes an event journal, exposes metrics and SSE state, and powers the operator terminal.

## Architecture

```text
TxLINE SSE --------> normalized events --+
                                           +--> single-writer engine --> risk --> execution venue
Pascal L2 WS ------> normalized events --+             |                    |
                                                        +--> journal         +--> fills
                                                        +--> metrics
                                                        +--> SSE dashboard
```

- `edgerunner-core`: fixed-point types, pure strategy contract, risk engine, paper venue, journal, replay, and latency histograms.
- `edgerunner-adapters`: TxLINE SSE and stateful Pascal L2 WebSocket adapters.
- `edgerunner-service`: CLI, simulator, append-only journal worker, Axum API, SSE state, and static UI hosting.
- `web`: responsive React operator terminal.

Detailed design notes are in [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).

## Run Locally

Prerequisites: Rust 1.95+, Node 24+, and npm 11+.

```bash
# terminal 1
cargo run -p edgerunner -- serve

# terminal 2
cd web
npm install
npm run dev
```

Open `http://127.0.0.1:5173`. The API listens on `http://127.0.0.1:8080`.

For a production-style local build:

```bash
cd web && npm run build && cd ..
cargo run --release -p edgerunner -- serve
```

The Rust service then hosts the built UI at `http://127.0.0.1:8080`.

## Commands

```bash
# Simulation plus paper execution
cargo run -p edgerunner -- serve \
  --journal data/runs/latest.jsonl \
  --config config.example.toml

# Deterministically replay a journal
cargo run -p edgerunner -- replay \
  --journal data/runs/latest.jsonl \
  --config config.example.toml

# Benchmark the normalized event-to-decision core
cargo run --release -p edgerunner -- bench \
  --iterations 100000 \
  --config config.example.toml

# Compare HTTP round-trip time from a deployment candidate
cargo run --release -p edgerunner -- probe \
  --url https://txline.txodds.com/api/ \
  --url https://data.pascal.trade/api/v1/time
```

### Live Feeds, Paper Execution

Copy `.env.example` into your secret manager or shell environment. Never commit the activated token.

```bash
export TXLINE_GUEST_JWT=...
export TXLINE_API_TOKEN=...

cargo run -p edgerunner -- serve \
  --live-feeds \
  --market FIFA-WC-FRA-BRA \
  --pascal-symbol YOUR_PASCAL_MARKET_SYMBOL
```

Live-feed mode intentionally cannot place live orders. A Pascal order adapter should be enabled only after private-beta API access, signed-permit integration tests, and explicit non-zero capital limits exist.
For submitted intents carrying TxLINE `messageId` and `ts` provenance, a background worker retrieves `/api/odds/validation` with bounded retries and appends the raw Merkle proof to the same run journal.

## API

- `GET /api/health`: liveness and execution mode.
- `GET /api/ready`: trading readiness; returns 503 when killed or a feed is unavailable.
- `GET /api/config`: effective non-secret strategy, risk, and paper-venue configuration.
- `GET /api/snapshot`: current engine, market, latency, decision, and fill state.
- `GET /api/events`: SSE stream of snapshots.
- `GET /api/metrics`: Prometheus text metrics.
- `POST /api/kill`: activate the risk kill switch.
- `POST /api/resume`: clear the risk kill switch.

Control endpoints require `X-Api-Token`. The local default is `local-demo`; set `EDGERUNNER_CONTROL_TOKEN` for any shared deployment.
The service refuses a non-loopback bind unless `EDGERUNNER_CONTROL_TOKEN` is explicitly set.

## Verification

```bash
cargo fmt --all -- --check
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
cd web && npm run lint && npm run build
```

Latency metrics are separated by responsibility. The displayed decision histogram measures local engine computation. Network RTT and future venue acknowledgement must be reported separately; EdgeRunner does not combine them into a misleading "tick-to-trade" number.
