# EdgeRunner

EdgeRunner is a deterministic, low-latency trading engine for sports prediction markets. It consumes TxLINE fair probabilities and a venue L2 order book, evaluates a fixed-point dislocation strategy, applies inline risk gates, and sends approved intents to a simulated execution venue.

The service only processes real market data. With a complete TxLINE and Pascal configuration it records the live streams to a journal; without that configuration it starts in an explicit inactive state and never invents prices, books, or score updates.

## Architecture

```mermaid
flowchart TB
    subgraph SOURCES["Live Market Sources"]
        TX_HTTP["TxLINE Fixtures + Odds API"]
        TX_SSE["TxLINE Fair-Value SSE"]
        PASCAL_API["Pascal Market Catalogue"]
        PASCAL_WS["Pascal L2 WebSocket"]
    end

    subgraph CONTROL["Control Plane"]
        MATCHER["Fixture + Outcome Matcher"]
        SUPERVISOR["Live-Feed Supervisor"]
        REPLAY["Deterministic Replay Controller"]
    end

    TX_HTTP --> MATCHER
    PASCAL_API --> MATCHER
    MATCHER -->|"MarketMapping"| SUPERVISOR

    subgraph INGESTION["Normalized Event Boundary"]
        TX_ADAPTER["TxLINE Adapter<br/>FairValue Events"]
        PASCAL_ADAPTER["Pascal Adapter<br/>Book Events"]
        EVENT_QUEUE["Bounded Event Channel"]
    end

    SUPERVISOR -.->|"starts"| TX_ADAPTER
    SUPERVISOR -.->|"starts"| PASCAL_ADAPTER
    TX_SSE --> TX_ADAPTER
    PASCAL_WS --> PASCAL_ADAPTER
    TX_ADAPTER --> EVENT_QUEUE
    PASCAL_ADAPTER --> EVENT_QUEUE
    REPLAY -->|"recorded events"| EVENT_QUEUE

    subgraph CORE["Deterministic Hot Path · Single Writer · No Network or Disk Await"]
        STATE["MarketState<br/>Sequence + Freshness Checks"]
        STRATEGY["Mean-Reversion Strategy<br/>5% Entry · 1% Exit · Scale to Risk Capacity"]
        RISK["Inline Risk Gates<br/>Kill · Stale · Circuit · Drawdown · Position · Notional · Rate"]
        VENUE["Simulated Execution Venue<br/>Visible Depth + Fees"]
        ACCOUNTING["Position Accounting<br/>Cost Basis · Holding Time · Realized PnL"]
    end

    EVENT_QUEUE --> STATE
    STATE --> STRATEGY
    STRATEGY --> RISK
    RISK -->|"approved intent"| VENUE
    VENUE --> ACCOUNTING
    ACCOUNTING -->|"fill updates"| STATE

    subgraph OUTPUTS["Persistence + Observability"]
        JOURNAL[("Append-Only JSONL Journal<br/>Mapping · Event · Decision · Fill · Trade · Proof")]
        PROOF["TxLINE Validation-Proof Worker"]
        SNAPSHOTS["Snapshot Broadcast"]
        API["Axum API<br/>REST · SSE · Prometheus"]
        DASHBOARD["React Operator Dashboard"]
    end

    MATCHER -->|"mapping"| JOURNAL
    TX_ADAPTER -->|"source event"| JOURNAL
    PASCAL_ADAPTER -->|"source event"| JOURNAL
    STRATEGY -->|"decision"| JOURNAL
    VENUE -->|"fill + trade"| JOURNAL
    RISK -->|"submitted intent provenance"| PROOF
    PROOF -->|"validation proof"| JOURNAL
    JOURNAL --> REPLAY

    STATE --> SNAPSHOTS
    ACCOUNTING --> SNAPSHOTS
    SNAPSHOTS --> API
    API -->|"SSE state"| DASHBOARD
    DASHBOARD -->|"session · replay · kill controls"| API
    API -.-> SUPERVISOR
    API -.-> REPLAY
    API -.-> RISK
```

Live and replay events share the same deterministic engine path; only the event source changes.

- `edgerunner-core`: fixed-point types, pure strategy contract, risk engine, simulated venue, journal, replay, and latency histograms.
- `edgerunner-adapters`: TxLINE SSE and stateful Pascal L2 WebSocket adapters.
- `edgerunner-service`: CLI, live-feed supervision, append-only journal worker, Axum API, SSE state, and static UI hosting.
- `web`: responsive React operator terminal.

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


## API
| Endpoint | Description |
|----------|-------------|
| `GET /api/health` | Service health |
| `GET /api/ready` | Trading readiness |
| `GET /api/snapshot` | Current engine state |
| `GET /api/events` | Server-Sent Events stream |
| `GET /api/metrics` | Prometheus metrics |
| `GET /api/session` | Session information |
| `POST /api/session` | Update session mode |
| `POST /api/replay` | Replay controls |
| `GET /api/feed-mode` | Feed status |
| `POST /api/feed-mode` | Start or stop live feeds |
| `POST /api/kill` | Activate kill switch |
| `POST /api/resume` | Resume execution |
---

## Verification

```bash
cargo fmt --all -- --check
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
cd web && npm run lint && npm run build
```

Latency metrics are separated by responsibility. The displayed decision histogram measures local engine computation. Network RTT and future venue acknowledgement must be reported separately; EdgeRunner does not combine them into a misleading "tick-to-trade" number.
