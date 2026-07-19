# EdgeRunner

EdgeRunner is a deterministic, low-latency trading engine for sports prediction markets. It consumes TxLINE fair probabilities and a venue L2 order book, evaluates a fixed-point dislocation strategy, applies inline risk gates, and sends approved intents to a simulated execution venue.

The service only processes real market data. With a complete TxLINE and Pascal configuration it records the live streams to a journal; without that configuration it starts in an explicit inactive state and never invents prices, books, or score updates.

## Architecture

```text
TxLINE SSE --------> normalized events --+
                                           +--> single-writer engine --> risk --> execution venue
Pascal L2 WS ------> normalized events --+             |                    |
                                                        +--> journal         +--> fills
                                                        +--> metrics
                                                        +--> SSE dashboard
```

- `edgerunner-core`: fixed-point types, pure strategy contract, risk engine, simulated venue, journal, replay, and latency histograms.
- `edgerunner-adapters`: TxLINE SSE and stateful Pascal L2 WebSocket adapters.
- `edgerunner-service`: CLI, live-feed supervision, append-only journal worker, Axum API, SSE state, and static UI hosting.
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
# Live TxLINE SSE + Pascal L2, with simulated execution
cargo run -p edgerunner -- serve \
  --journal data/runs/latest.jsonl \
  --config config.example.toml

# Deterministically replay a journal
cargo run -p edgerunner -- replay \
  --journal data/runs/latest.jsonl \
  --config config.example.toml

# Benchmark a fixed recording from the normalized event-to-decision core
cargo run --release -p edgerunner -- bench \
  --journal data/runs/latest.jsonl \
  --max-events 100000 \
  --config config.example.toml

# Compare HTTP round-trip time from a deployment candidate
cargo run --release -p edgerunner -- probe \
  --url https://txline.txodds.com/api/ \
  --url https://data.pascal.trade/api/v1/time
```

### Live Feeds, Recorded Replay, Simulated Execution

Use TxLINE devnet's free tier to get a real odds feed. TxLINE requires a signed devnet
subscription from the wallet that will own the credentials. The official
[free-tier guide](https://txline-docs.txodds.com/documentation/worldcup) and
[runnable devnet script](https://txline-docs.txodds.com/documentation/examples/devnet-examples)
perform the subscription, activation, and stream check. Keep the resulting API token in your shell
or secret manager, never in the repository.

```bash
# The credentials must be activated against the same devnet wallet that submitted the subscription.
export TXLINE_ORIGIN=https://txline-dev.txodds.com
export TXLINE_API_TOKEN=...

# Start automatic TxLINE fixture/Pascal market discovery.
cargo run -p edgerunner -- serve
```

With only `TXLINE_API_TOKEN`, EdgeRunner fetches TxLINE's live/upcoming fixture snapshot and each
candidate's odds snapshot, then queries Pascal's public market catalogue. It only activates a feed
when both event participants, start time, market period, numeric line (where present), and outcome
match; otherwise it remains in `DISCOVERING` and retries. The TxLINE SSE connection is then filtered
to the recorded fixture and outcome selection. This never falls back to generated market data.

`TXLINE_FIXTURE_ID`, `TXLINE_MARKET`, and `PASCAL_SYMBOL` are optional overrides for a known
fixture, internal market label, or Pascal symbol. `PASCAL_WS_URL` remains optional and defaults to
`wss://data.pascal.trade/ws`.

`PASCAL_WS_URL=wss://data.pascal.trade/ws` is Pascal's public market-data WebSocket and does not
require credentials. If Pascal introduces private-market access for the selected product, add its
credentials to the runtime environment rather than hard-coding them. All current Pascal book events
come from that public WebSocket.

The service loads `.env` at startup, with process environment variables taking precedence. The
dashboard control toggles only between `live` and `inactive`; it never creates a fallback market feed.
Discovery automatically activates matching real workers and opens a new simulated run. The selected
fixture, Pascal symbol, and TxLINE outcome are appended to the journal before market events, so replay
uses the recorded mapping rather than a fresh lookup. You can also override the market label or Pascal
symbol with `--live-feeds --market ... --pascal-symbol ...`.

TxLINE guest JWTs are obtained from `TXLINE_ORIGIN/auth/guest/start` whenever an SSE connection or
proof lookup is opened. Do not persist `TXLINE_GUEST_JWT`; the activated `TXLINE_API_TOKEN` is sent
on every TxLINE request, including guest-token acquisition, SSE, and validation.

Each live journal event records its source (`txline` or `pascal`). TxLINE fair-value records require
the upstream `messageId` and timestamp provenance, while the raw TxLINE validation proof is stored
when an order is submitted. `replay` and `bench` consume those recorded events only and reject
legacy journals without source provenance, incomplete TxLINE events, or journals missing either
feed. This is the historical/replay path; it is deterministic and has no local event generator.

Live-feed mode intentionally cannot place live orders. A Pascal order adapter should be enabled only after private-beta API access, signed-permit integration tests, and explicit non-zero capital limits exist.
For submitted intents carrying TxLINE `messageId` and `ts` provenance, a background worker retrieves `/api/odds/validation` with bounded retries and appends the raw Merkle proof to the same run journal.

## API

- `GET /api/health`: liveness and execution mode.
- `GET /api/ready`: trading readiness; returns 503 when killed or a feed is unavailable.
- `GET /api/config`: effective non-secret strategy, risk, and simulation-venue configuration.
- `GET /api/snapshot`: current engine, market, latency, decision, and fill state.
- `GET /api/events`: SSE stream of snapshots.
- `GET /api/metrics`: Prometheus text metrics.
- `GET /api/session`: current Devnet/Mainnet environment, Live/Replay mode, and replay progress.
- `POST /api/session`: switch environment or run mode. Mainnet is represented as an environment only until a mainnet adapter is configured.
- `POST /api/replay`: play, pause, reset, seek, or change speed for the selected journal.
- `GET /api/feed-mode`: configured live-feed availability and current `live`/`inactive` state.
- `POST /api/feed-mode`: start or stop the real live feeds.
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
