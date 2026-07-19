# Strategy and Risk

## Units

EdgeRunner represents probabilities, prices, rates, fees, edge, notional, and PnL as integer millionths. For example, `0.05` is stored as `50,000`. This avoids floating-point drift during replay.

## Executable Edge

The strategy compares TxLINE fair probability with Pascal's executable top of book after costs:

```text
buy edge  = fair probability - ask price - taker fee - latency haircut
sell edge = bid price - fair probability - taker fee - latency haircut
```

The configured fee and latency haircut are included before a trade can qualify.

## Mean-Reversion Lifecycle

The thresholds are fixed strategy constants:

```text
ENTRY_EDGE_THRESHOLD = 5%
EXIT_EDGE_THRESHOLD  = 1%
```

| Position state | Edge | Action |
|---|---:|---|
| Flat | Best executable edge below 5% | Wait |
| Flat | Best executable edge at least 5% | Enter on the stronger side |
| Open | Same-side edge at least 5% | Add another chunk while capacity and depth remain |
| Open | Exit edge above 1% but below entry threshold | Hold |
| Open | Exit edge at or below 1% | Close the position |

Opening one chunk does not stop the strategy from using the remaining limit. It continues scaling in by `order_size` chunks as qualifying events arrive. Each order is clipped by visible depth and the smaller of remaining position and notional capacity.

The default example configuration uses an order size of 25 contracts and a maximum absolute position of 250 contracts.

## Risk Gates

Every intent must pass all applicable gates:

- kill switch is clear;
- market circuit is clear and the event is not dangerous or suspended;
- both feeds are fresh enough;
- maximum drawdown is not breached;
- resulting position stays within `max_position`;
- resulting exposure stays within `max_notional_micros`;
- order rate stays within `max_orders_per_minute`.

Rejections are journaled with the reason. Limits are configured in `config.example.toml`; entry and exit thresholds are not runtime configuration.

## Simulated Fills

The simulated venue fills approved orders at the current top-of-book price, never beyond visible quantity. It applies the configured taker fee and deterministic acknowledgement delay. This is paper execution even when the input feeds are live.

## Position and PnL

Multiple entry fills form a weighted average entry price. The dashboard exposes:

- position status (`OPEN` or `CLOSED`);
- signed contract position and remaining capacity;
- weighted entry price;
- exit price after closure;
- holding time;
- mark-to-market PnL while open;
- realized PnL after the exit.

Completed BUY and SELL records include timestamp, price, edge, quantity, and realized PnL in the trade journal.

