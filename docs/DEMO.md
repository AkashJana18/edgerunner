# Five-Minute Demo Runbook

## Before the Demo

1. Put a valid `TXLINE_API_TOKEN` in `.env`.
2. Start the backend with `--live-feeds` and the frontend with `npm run dev`.
3. Confirm `/api/ready` is healthy and the dashboard shows both feeds connected.
4. Keep a replayable `data/runs/latest.jsonl` as a fallback if the live event is quiet.

## Demo Flow

### 1. Establish the Problem

“EdgeRunner compares TxLINE's fair probability with an executable Pascal order book. It reacts only when the discrepancy remains profitable after fees and a latency haircut.”

Point to fair value, bid/ask, spread, feed freshness, and the human-readable event name. Mention that the raw market symbol is parsed rather than shown as the primary label.

### 2. Explain the Decision

Show the Decisions view and describe the fixed 5% entry threshold. An approved opportunity enters in configured chunks and continues scaling toward the risk limit while qualifying edge and visible liquidity remain.

Point to local decision p50, p95, and max latency. Clarify that these are independently scaled compute measurements, not network latency.

### 3. Show the Position Lifecycle

Show position status, signed position, remaining capacity, funds required/notional, weighted entry price, and mark-to-market PnL. As the edge mean-reverts to 1% or lower, show the closing order and the Trades view with entry/exit price, quantity, holding time, and realized PnL.

If the live market does not cross both thresholds during the demo, switch to the recorded replay.

### 4. Demonstrate Safety

Activate **Kill engine**. Confirm the kill gate changes state and new intents are rejected. Resume and show the feed freshness, market circuit, position, notional, drawdown, and order-rate gates.

### 5. Demonstrate Determinism

Run the same recording twice:

```bash
cargo run -p edgerunner -- replay \
  --journal data/runs/latest.jsonl \
  --config config.example.toml
```

Compare both the decision checksum and trade checksum. Identical output demonstrates that replay uses the same ordered, fixed-point engine path.

### 6. Close with the Boundary

“The market inputs are real and recorded with TxLINE provenance. Execution is deliberately simulated: EdgeRunner does not place Pascal orders or move wallet funds. The connected wallet is an operator identity display, not execution authorization.”

## Backup Talking Points

- Automatic discovery activates only after TxLINE and Pascal event semantics match.
- The order book is useful because it proves that decisions use executable prices and visible liquidity, not a decorative midpoint.
- Position capacity shows why the engine can scale in without exceeding position or notional limits.
- The journal is append-only and includes events, decisions, fills, completed trades, and proof records.

