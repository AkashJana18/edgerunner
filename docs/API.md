# HTTP API

The service listens on `127.0.0.1:8080` by default. Responses and request payloads use JSON except for the SSE and Prometheus endpoints.

## Read Endpoints

| Method | Endpoint | Purpose |
|---|---|---|
| `GET` | `/api/health` | Process health |
| `GET` | `/api/ready` | Engine/feed readiness; returns 503 when not ready |
| `GET` | `/api/config` | Active sanitized engine configuration |
| `GET` | `/api/snapshot` | Current market, strategy, position, risk, latency, and journal view |
| `GET` | `/api/events` | Server-Sent Events stream of dashboard snapshots |
| `GET` | `/api/metrics` | Prometheus text metrics |
| `GET` | `/api/session` | Current environment and live/replay session mode |
| `GET` | `/api/feed-mode` | Current live/inactive feed state |

## Control Authentication

All `POST` routes require an `x-api-token` header. Loopback development defaults to `local-demo`. Binding to a non-loopback address requires `EDGERUNNER_CONTROL_TOKEN`.

```bash
curl -sS -X POST \
  -H 'content-type: application/json' \
  -H 'x-api-token: local-demo' \
  -d '{"mode":"live"}' \
  http://127.0.0.1:8080/api/feed-mode
```

## Control Endpoints

| Method | Endpoint | Body | Purpose |
|---|---|---|---|
| `POST` | `/api/session` | `{"environment":"devnet","run_mode":"live"}` | Update operator environment and run mode |
| `POST` | `/api/feed-mode` | `{"mode":"live"}` or `{"mode":"inactive"}` | Start or stop live feed workers |
| `POST` | `/api/kill` | none | Activate the kill switch |
| `POST` | `/api/resume` | none | Clear the kill switch and resume evaluation |
| `POST` | `/api/replay` | action object | Control replay scheduling |

Both session fields are optional. Valid environments are `devnet` and `mainnet`; valid run modes are `live` and `replay`. These labels do not enable real-money execution.

Replay bodies:

```json
{"action":"play"}
```

```json
{"action":"pause"}
```

```json
{"action":"reset"}
```

```json
{"action":"set_speed","speed":2}
```

```json
{"action":"seek","event_index":100}
```

Replay speed must be between `0.25` and `20`.

## Streaming Snapshots

```bash
curl -N http://127.0.0.1:8080/api/events
```

The SSE stream is a dashboard projection, not the canonical record. Slow clients may miss intermediate snapshots; use the journal for complete event history.

## Prometheus Metrics

The metrics endpoint currently exposes:

- `edgerunner_events_total`
- `edgerunner_orders_rejected_total`
- `edgerunner_events_ignored_total`
- `edgerunner_decision_p99_microseconds`
- `edgerunner_position_contracts`
- `edgerunner_pnl_micros`
- `edgerunner_kill_switch`

Decision p99 measures local engine compute time only.

