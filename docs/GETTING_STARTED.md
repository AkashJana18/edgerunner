# Getting Started

## Prerequisites

- Rust 1.95 or newer
- Node.js 24 or newer
- npm 11 or newer
- A TxLINE API token for live fair-value data

Pascal market data is read from its public WebSocket. EdgeRunner obtains a short-lived TxLINE guest JWT from the API token at runtime, so no JWT needs to be stored separately.

## Configure the Environment

From the repository root:

```bash
cp .env.example .env
```

Set at least:

```dotenv
TXLINE_ORIGIN=https://txline-dev.txodds.com
TXLINE_API_TOKEN=your-token
```

The service loads `.env` automatically. Existing process environment variables take precedence.

These values are optional:

| Variable | Purpose |
|---|---|
| `TXLINE_FIXTURE_ID` | Skip automatic TxLINE fixture selection |
| `TXLINE_MARKET` | Select a known internal TxLINE market label |
| `PASCAL_SYMBOL` | Skip automatic Pascal market selection |
| `PASCAL_WS_URL` | Override the default Pascal WebSocket URL |
| `EDGERUNNER_CONTROL_TOKEN` | Protect control endpoints; required for a non-loopback bind |

Do not commit `.env`. Variables prefixed with `VITE_` are visible to browser users, so use the frontend control-token variable only for this local demo and never embed a privileged shared token in a public build.

## Development Mode

Start the backend and frontend in separate terminals:

```bash
# terminal 1, repository root
cargo run -p edgerunner -- serve --live-feeds --config config.example.toml
```

```bash
# terminal 2
cd web
npm install
npm run dev
```

Open `http://127.0.0.1:5173`. Vite proxies `/api` to `http://127.0.0.1:8080` by default.

For a different backend address, set `VITE_API_PROXY_TARGET` before starting Vite. If a local backend uses a custom control token, provide the matching `VITE_EDGERUNNER_CONTROL_TOKEN` at frontend build/start time.

## One-Server Local Build

The Rust service can serve the compiled dashboard from `web/dist`:

```bash
cd web
npm install
npm run build
cd ..
cargo run -p edgerunner -- serve --live-feeds --config config.example.toml
```

Open `http://127.0.0.1:8080`.

## Feed Startup States

With `--live-feeds`, EdgeRunner either uses explicit fixture/symbol overrides or discovers a compatible pair. Discovery compares participants, start time, period, numeric line where applicable, and outcome. The dashboard may remain in `DISCOVERING` until both feeds match and connect.

Without `--live-feeds`, the service starts inactive. It still exposes the dashboard and API but does not fabricate market events.

Useful checks:

```bash
curl -sS http://127.0.0.1:8080/api/health
curl -i -sS http://127.0.0.1:8080/api/ready
curl -sS http://127.0.0.1:8080/api/feed-mode
```

`/api/ready` intentionally returns HTTP 503 when the engine is stopped, killed, inactive, or waiting for usable feeds.

## Common Problems

| Symptom | Check |
|---|---|
| Dashboard cannot reach the API | Confirm the backend is on port 8080 and the Vite proxy target is correct |
| Controls return unauthorized | Match `EDGERUNNER_CONTROL_TOKEN` and `VITE_EDGERUNNER_CONTROL_TOKEN`; loopback defaults to `local-demo` |
| Feed stays in discovery | Verify the token, fixture availability, market outcome, period, line, and Pascal symbol |
| Decisions are rejected as stale | Confirm both feeds are connected and updates arrive within `feed_stale_after_ms` |
| No orders appear | An executable edge must reach 5%, all risk gates must pass, and visible top-of-book size must be available |
| Port is already in use | Stop the previous process or pass `--bind 127.0.0.1:<port>` and update the frontend proxy |

## Verification

```bash
cargo fmt --all -- --check
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
cd web && npm run lint && npm run build
```
