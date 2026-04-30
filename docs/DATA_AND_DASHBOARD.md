# Data and Dashboard

## SQLite Store

Runtime records live in `data/cointrading.sqlite`. Timestamps are stored in UTC for consistency, and dashboard/Telegram display times are rendered in Korea time (KST).

Tables:

- `signals`: dry-run scalp signals, market regime, fee estimates, and 1/3/5 minute forward scores.
- `orders`: post-only maker order attempts, blocked decisions, dry-run responses, and future live responses.
- `fills`: execution fills and realized fee/PnL records. This table is ready for exchange fill ingestion.
- `fee_snapshots`: maker/taker fee snapshots by symbol.
- `market_regimes`: 15m/1h macro regime snapshots and strategy-router decisions.
- `scalp_cycles`: post-only scalp lifecycle state, including entry waiting, take-profit waiting, reprice, stop, timeout, and realized paper PnL.
- `strategy_evaluations`: latest strategy candidate evaluations by source, execution mode, symbol, regime, side, TP/SL, max hold, sample count, win rate, expectancy, and approval decision.

CSV files remain gitignored and are now treated as compatibility logs. Use `migrate-csv-to-db` to import old rows.

## Commands

```bash
python -m cointrading.cli migrate-csv-to-db
python -m cointrading.cli db-summary
python -m cointrading.cli scalp-report
python -m cointrading.cli maker-once --symbol BTCUSDC
python -m cointrading.cli scalp-engine-step
python -m cointrading.cli market-regime
python -m cointrading.cli market-regime-collect
python -m cointrading.cli strategy-evaluate
python -m cointrading.cli strategy-notify
python -m cointrading.cli dashboard --host 127.0.0.1 --port 8080
```

## Scalp Lifecycle

`scalp-engine-step` is the dry-run/paper state machine:

- no active cycle: evaluate a fresh signal and submit a post-only entry intent.
- entry waiting: paper-fill if the market crosses the passive entry, otherwise timeout or re-quote.
- open cycle: immediately submit a post-only take-profit intent.
- exit waiting: close on take-profit, stop out on risk, reprice after timeout, or force-exit after max hold.

The VM runs this as `cointrading-scalp-engine.timer` every 15 seconds. Live order submission still remains blocked unless both live guards are explicitly changed.

## Strategy Gate

## Macro Regime Router

`market-regime-collect` classifies each active symbol into a larger market regime using 15m and 1h candles:

- `macro_bull`: long trend, pullback-long, and long-only scalping candidates.
- `macro_bear`: short trend, rally-short, and short-only scalping candidates.
- `macro_range`: range/mean-reversion and strict maker scalping candidates.
- `macro_breakout`: reduced-size breakout trend candidates; scalping is blocked.
- `macro_panic`: 신규 진입 금지.

The VM runs this as `cointrading-market-regime.timer` every 5 minutes. When `COINTRADING_MACRO_REGIME_GATE_ENABLED=true`, new scalping cycles are blocked if the latest macro regime routes away from that direction. Missing or stale macro data is not treated as a hard block, so data collection can continue after restarts.

## Strategy Gate

`strategy-evaluate` writes two evaluation sources into SQLite:

- `cycles`: actual post-only/paper lifecycle outcomes grouped by symbol, regime, and side.
- `signal_grid`: a coarse TP/SL/max-hold grid using scored 1/3/5 minute signal returns.

The signal grid compares `maker_post_only`, `taker_momentum`, and `hybrid_taker_entry_maker_exit`. Taker and hybrid rows subtract taker fees plus `COINTRADING_STRATEGY_TAKER_SLIPPAGE_BPS` so tiny targets do not look artificially profitable.

New lifecycle entries are blocked when `COINTRADING_STRATEGY_GATE_ENABLED=true` and the matching execution mode/symbol/regime/side has no `APPROVED` evaluation. If the current fixed TP/SL/max-hold combination is not approved, the gate can select the best approved `signal_grid` candidate for that symbol/regime/side and pass its TP/SL/max-hold settings into the paper lifecycle. This keeps the bot collecting data while preventing weak combinations from continuing into new paper/live cycles.

Approval is based on positive net expectancy after fees/slippage, a minimum sample count, a low win-rate floor, and a break-even win-rate check derived from the observed average win/loss size. This avoids rejecting asymmetric payoff candidates just because their win rate is below 50%.

`strategy-notify` sends a Telegram report when strategy decisions change or when the periodic interval elapses. The VM checks every 15 minutes and defaults to a 6-hour periodic report via `COINTRADING_STRATEGY_NOTIFY_INTERVAL_MINUTES=360`.

## Dashboard

The dashboard is a small HTTP server with tabs for summary, performance, macro regime routing, strategy candidates, lifecycle state, signals, and orders. It uses a server-sent events stream to update the data in place without reloading the whole page.
Signal, strategy, lifecycle, and order tables show the latest 200 rows by default. Add `&limit=500` to the dashboard URL to inspect a longer window; the dashboard caps this at 1000 rows to keep mobile loading reasonable.

Set `COINTRADING_DASHBOARD_AUTH_TOKEN` before exposing it outside the VM.
When the token is set, requests must include either `?token=...` or an `Authorization: Bearer ...` header.

On the VM it runs as `cointrading-dashboard.service`, bound to `0.0.0.0:8080` for mobile/browser access.

```bash
curl -H "Authorization: Bearer $COINTRADING_DASHBOARD_AUTH_TOKEN" http://34.50.6.186:8080/
```
