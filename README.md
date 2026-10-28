# nifty-swing-option-buy

The repository/job names stay unchanged. The strategy is now **intraday Nifty
option-premium Renko buying on Dhan**, not the former Definedge straddle/PnF swing
strategy. Version: `premium-renko-levels-1pct-40-20-2026-10-28`.

## Frozen rules

- Lock ATM from Nifty's **09:30 minute open**, rounded to the nearest50 strike.
  Initialization waits for that minute to complete; a restart uses the same
  observation, never the current spot.
- Select the nearest listed expiry **strictly more than6 calendar days away**,
  at that strike for both CE and PE. Freeze both contracts for the day.
- Use **available exact-contract candles from14 calendar days** of history,
  normal session09:15-15:29 only. Missing history is not forward-filled.
- Build **1% option-premium Renko** with the shared library.
- Buy the first observed CE/PE latest close strictly above its **prior40 bricks' high**.
  Exclude the current brick from the channel; use the last brick of a multi-brick
  minute. Only fresh signals at/after09:30 qualify.
- If both latest option levels qualify together, **skip the entire day**.
- Exit the bought option on a close strictly below the **prior20 bricks' low**,
  or at **15:15 IST**. No target, fixed-rupee stop, partial booking, AFT/PCR,
  Supertrend filter or overnight holding.
- **At most one trade per user/day**, including restarts and duplicate job runs.
  Closed or aborted attempts consume the day.
- Last eligible signal minute15:13; an entry must occur in the next minute,
  strictly before15:15. After restart, evaluate the latest fresh levels, even if
  an earlier breakout was missed. No historical breakout/exit replay.

## Preserved jobs and storage

| Existing path/job | New responsibility |
|---|---|
| `straddle/straddle.py` / signal job | Lock09:30 ATM/expiry, form CE/PE charts, update `close`, `high40`, `low20` in Mongo |
| `OptionBuy/option_buy.py` / execution job | Read Mongo levels, decide breakouts/entries/exits, manage one paper trade and net P&L |

All strategy code lives in these **two existing scripts**, with no added shared
module or test/configuration files. The signal job makes no trading decisions.

Mongo database remains `Bots`, with `nifty_swing_<user_name>` and
`orders_nifty_swing_<user_name>`. Signal data uses **one reusable document** in
`supertrend`: `_id = NIFTY_premium_renko_<user_name>` (no date suffix).

| Flat field | Meaning |
|---|---|
| `updated_at`, `strategy_version`, `broker` | Publication timestamp (also identifies session date) and identity |
| `ATM`, `STRIKE`, `expiry`, `dte` |09:30 Nifty spot open, rounded ATM strike and selected expiry |
| `CE_symbol`, `PE_symbol` | Exact dated option symbols |
| `CE_security_id`, `PE_security_id` | Dhan security IDs |
| `CE_lot_size`, `PE_lot_size` | Contract lot sizes |
| `CE_close`, `PE_close` | Latest Renko closes |
| `CE_last_40_high`, `PE_last_40_high` | High of40 prior bricks, excluding current brick |
| `CE_last_20_low`, `PE_last_20_low` | Low of20 prior bricks, excluding current brick |
| `CE_brick_time`, `PE_brick_time` | Latest Renko brick timestamps |
| `CE_data_asof`, `PE_data_asof` | Latest completed source-candle timestamps |
| `skip_day` | Executor-owned marker if both latest options qualify; cleared next day |

Both options refresh together each completed minute. No chart-history arrays or
stored first decision. Levels persist without new bricks; channels remain
`null` until enough bricks exist. Executor compares the flat close/high/low
fields. Entry needs a post09:30 brick and fresh completed source data, but the
brick itself may be older than the latest source minute.

On a new day, publisher re-locks09:30 ATM/expiry and replaces all session fields
on the same document only after both charts succeed. Until then yesterday's
`updated_at` date/freshness blocks execution. No separate `day` field is stored.
Same-day restarts retain the contract lock. Old daily-ID
documents are left untouched; no automatic deletion or production migration.

The daily trade ID is deterministic and atomically inserted. Paper BUY/SELL order
IDs are deterministic too, so a saved fill is reused if a process crashes before
updating the trade. A failed/expired pending entry cannot cause another buy.
Historical trades/orders are not deleted or rewritten. An open legacy/partial
position or non-paper position **blocks the new strategy** with an alert; reconcile
it explicitly before cutover.

## Paper execution and accounting

**Executor is paper only.** Its `live_trading` defaults to `false`; any other value
is rejected. The signal job neither reads nor requires this flag; it places no orders.
There is no real-order branch. Dhan authentication uses the shared token-aware
`login_to_dhan`, never a forced fresh token on every poll.

Only completed one-minute candles form signals. After observing the signal, the
executor records the **current Dhan option LTP** as the paper fill during the next
minute. It does **not** backdate a fill to the historical next-minute open.
The quote receipt time is recorded; a slow BUY response that crosses its entry
minute or15:15 is rejected rather than recorded with an earlier timestamp.
There is no extra synthetic slippage haircut in this forward record: LTP still
does not model bid/ask or executable liquidity, and Dhan's LTP helper does not
provide a quote timestamp. Research results are not promised to reproduce.

`pnl` remains gross. `tamingnifty.charges` supplies itemized `charges`, and
`net_pnl = pnl - charges.total`. An active trade has zero realized gross and its
entry fees; `running_pnl` is the current gross mark, not realized P&L. Closed
records include both executed paper orders' fees; no flat fee, charge schedule or
estimate metadata fields are added.

Stale/missing signals block entry and ordinary channel exits and alert explicitly.
**15:15 closure does not depend on the signal job**; if a quote/API fails the
executor keeps retrying rather than marking the position closed. It stays running
after15:16 while any pending/open position exists. An overdue new-era position is
closed at the next available quote; this is recovery, not an overnight rule.
Neither the exact15:15 fill time nor a maximum monetary loss is guaranteed.

## Configuration and pipeline

Existing job names, image names, push-to-master/workflow-dispatch triggers, both
entrypoint paths and restart steps are preserved. Each image uses its original
job-directory context; build from the repository root:

```powershell
docker build -t nifty-swing-option-buy:test .\OptionBuy
docker build -t straddle-swing-option-buy:test .\straddle
```

Dockerfiles copy **only** their existing script and `requirements.txt`, never
`.env` or other source-directory files. Never bake credentials into either image.

The GitHub workflow is unchanged from the original pipeline. Keep its existing
Azure/ACR/job deployment secrets; **do not add Dhan credentials to GitHub**.
Configure `DHAN_CLIENT_ID`, `DHAN_PIN`, `DHAN_TOTP` directly on **both container
jobs**, for the same account used by the shared token store. Prefer container-job
secret references for sensitive values. Set `live_trading=false` on Option Buy
only; the signal job does not use this flag.
The pipeline updates images only, not job environment variables or secrets.

Both jobs still need `CONNECTION_STRING`, `user_name`, `slack_token` in their
existing environments. The executor also requires an explicit positive integer
`quantity`, validated against the selected contract's lot size; **one lot is now
allowed**, with no even-lot/half-book restriction. `instrument_name`, if supplied,
must be `NIFTY`. No local `.env`, cloud setting or repository secret is modified
by the source rewrite.

Before deploying, ensure the jobs start before09:31, the executor remains
available through15:15 (including its configured timeout), and any legacy open
positions are reconciled. Both container jobs must have the Dhan environment
variables before starting; Definedge-only credentials cannot run this port.

## Validation and release status

```powershell
python -m pip install -r OptionBuy\requirements.txt
python -m compileall -q OptionBuy straddle
```

Both dependency manifests pin `tamingnifty==2.3.2`. Before the fixed-document change,
after simplifying to two
scripts,28 offline checks pass, including exact published levels, strict
breakout comparisons in the executor, updates without new bricks, insufficient
history, candle causality, ATM/expiry, ties, daily cap, restart recovery, EOD,
fees and image/workflow wiring. The check harness was run outside the strategy
repository to respect the no-new-files request; it is not shipped or a pipeline
test suite. The original CI workflow has no added validation steps; run the
compilation command above locally. The subsequent fixed-document/latest-only
change passed offline checks for exact flat Renko levels, atomic new-day refresh,
failed-fetch preservation, latest-level restart behavior, ties, daily cap,
paper entry/exit and fees. The earlier28-check result predates this schema change.

Before simplification, the1%/prior40 first-option entry times matched the research
harness on all five saved pilot sessions. This is not P&L proof: paper fills use
current LTP and the new requested EOD is15:15 rather than the pilot's15:20.

**Paper-only source; deployment readiness must be verified separately.**
Pushing master triggers the unchanged deployment workflow. Configure Dhan
variables on both container jobs before starting the new images.
Docker daemon was unavailable, so actual image builds/Azure deployment are not
validated locally. No strategy loop was started, no broker order was sent, no
token rotated and no production Mongo record changed during implementation.
The new era needs its own forward sample; historical swing results and the
coverage-limited premium study do not validate its profitability.