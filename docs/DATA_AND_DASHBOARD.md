# Data and Dashboard

## SQLite Store

Runtime records live in `data/cointrading.sqlite`. Timestamps are stored in UTC for consistency, and dashboard/Telegram display times are rendered in Korea time (KST).

Tables:

- `signals`: dry-run scalp signals, market regime, fee estimates, and 1/3/5 minute forward scores.
- `orders`: post-only maker order attempts, blocked decisions, dry-run responses, and future live responses.
- `fills`: execution fills and realized fee/PnL records. This table is ready for exchange fill ingestion.
- `fee_snapshots`: maker/taker fee snapshots by symbol.
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

`strategy-evaluate` writes two evaluation sources into SQLite:

- `cycles`: actual post-only/paper lifecycle outcomes grouped by symbol, regime, and side.
- `signal_grid`: a coarse TP/SL/max-hold grid using scored 1/3/5 minute signal returns.

The signal grid compares `maker_post_only`, `taker_momentum`, and `hybrid_taker_entry_maker_exit`. Taker and hybrid rows subtract taker fees plus `COINTRADING_STRATEGY_TAKER_SLIPPAGE_BPS` so tiny targets do not look artificially profitable.

New lifecycle entries are blocked when `COINTRADING_STRATEGY_GATE_ENABLED=true` and the matching execution mode/symbol/regime/side/current TP/SL/max-hold combination does not have an `APPROVED` evaluation. This keeps the bot collecting data while preventing weak combinations from continuing into new paper/live cycles.

`strategy-notify` sends a Telegram report when strategy decisions change or when the periodic interval elapses. The VM checks every 15 minutes and defaults to a 6-hour periodic report via `COINTRADING_STRATEGY_NOTIFY_INTERVAL_MINUTES=360`.

## Dashboard

The dashboard is a small HTTP server with tabs for summary, performance, strategy candidates, lifecycle state, signals, and orders. It uses a server-sent events stream to update the data in place without reloading the whole page.
Signal, strategy, lifecycle, and order tables show the latest 200 rows by default. Add `&limit=500` to the dashboard URL to inspect a longer window; the dashboard caps this at 1000 rows to keep mobile loading reasonable.

Set `COINTRADING_DASHBOARD_AUTH_TOKEN` before exposing it outside the VM.
When the token is set, requests must include either `?token=...` or an `Authorization: Bearer ...` header.

On the VM it runs as `cointrading-dashboard.service`, bound to `0.0.0.0:8080` for mobile/browser access.

```bash
curl -H "Authorization: Bearer $COINTRADING_DASHBOARD_AUTH_TOKEN" http://34.50.6.186:8080/
```
