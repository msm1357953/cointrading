# Data and Dashboard

## SQLite Store

Runtime records live in `data/cointrading.sqlite`. Timestamps are stored in UTC for consistency, and dashboard/Telegram display times are rendered in Korea time (KST).

Tables:

- `signals`: dry-run scalp signals, market regime, fee estimates, and 1/3/5 minute forward scores.
- `orders`: post-only maker order attempts, blocked decisions, dry-run responses, and future live responses.
- `fills`: execution fills and realized fee/PnL records. This table is ready for exchange fill ingestion.
- `fee_snapshots`: maker/taker fee snapshots by symbol.
- `market_regimes`: 15m/1h macro regime snapshots and strategy-router decisions.
- `market_contexts`: mark/index premium, funding, open interest, spread, top-book liquidity, order-book depth, and depth imbalance snapshots.
- `scalp_cycles`: post-only scalp lifecycle state, including entry waiting, take-profit waiting, reprice, stop, timeout, and realized paper PnL.
- `strategy_cycles`: trend, range, and breakout lifecycle state, including entry/open/exit status, TP/SL/max-hold, live order IDs, fills, and realized PnL.
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
python -m cointrading.cli market-context
python -m cointrading.cli market-context-collect
python -m cointrading.cli strategy-evaluate
python -m cointrading.cli strategy-notify
python -m cointrading.cli strategy-engine-step
python -m cointrading.cli live-preflight --notional 25 --symbols ETHUSDC
python -m cointrading.cli live-supervisor --notional 25 --symbols ETHUSDC
python -m cointrading.cli live-supervisor-notify
python -m cointrading.cli vibe-probe-notify
python -m cointrading.cli refine-entry-check
python -m cointrading.cli refine-entry-notify
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

## Rule Strategy Router

`strategy-engine-step` now uses explicit indicator rules before it starts a non-scalping paper cycle:

- `trend_follow`: 15m EMA20/EMA60 alignment, EMA slope, close-vs-EMA60, and RSI confirmation.
- `range_reversion`: 5m Bollinger-band position plus RSI oversold/overbought confirmation in `macro_range`.
- `breakout_reduced`: 5m 20-bar close breakout, RSI confirmation, and volume expansion in `macro_breakout`.

These rules decide whether a strategy is `PASS`, `WATCH`, or `BLOCK`. Only `PASS` setups can start a paper/live strategy lifecycle, and the shared per-symbol lock still allows only one active lifecycle per symbol.

## Macro Regime Router

`market-regime-collect` classifies each active symbol into a larger market regime using 15m and 1h candles:

- `macro_bull`: long trend, pullback-long, and long-only scalping candidates.
- `macro_bear`: short trend, rally-short, and short-only scalping candidates.
- `macro_range`: range/mean-reversion and strict maker scalping candidates.
- `macro_breakout`: reduced-size breakout trend candidates; scalping is blocked.
- `macro_panic`: 신규 진입 금지.

The VM runs this as `cointrading-market-regime.timer` every 5 minutes. When `COINTRADING_MACRO_REGIME_GATE_ENABLED=true`, new scalping cycles are blocked if the latest macro regime routes away from that direction. Missing or stale macro data is not treated as a hard block, so data collection can continue after restarts.

`market-context-collect` runs every minute on the VM and records funding, premium, open interest, spread, top-book liquidity, order-book depth, and imbalance. `live-supervisor` refreshes both market context and macro regime before producing a final go/no-go report.

`live-supervisor-notify` runs on the VM and sends a Telegram alert when a symbol has an approved, macro-aligned, paper-performance-positive candidate and only safety locks remain. It never places orders; it tells the operator to rerun `실전 80` before any manual one-shot enable.

`refine-entry-check` is the current-market gate for second-stage refined strategies. It reads `data/strategy_refine_latest.json`, fetches recent live klines, compares the latest closed-bar features with each historical SURVIVED condition, writes `data/refined_entry_latest.json`, and never places orders. `refine-entry-notify` sends Telegram only when a current READY candidate appears or changes.

The alert gate is deliberately stricter than the strategy-candidate table. A candidate must have at least 20 closed paper lifecycle cycles, positive all-time and recent paper PnL, a paper payoff ratio of at least 1.2, and a recent stop/max-hold exit ratio no higher than 65%. This keeps `signal_grid` approvals from becoming live candidates until the real paper state machine has also shown survivable behavior. The live supervisor only treats `maker_post_only`, `taker_trend`, `maker_range`, and `taker_breakout` as supported live execution modes; experimental `taker_momentum` and hybrid signal-grid rows remain research-only until matching live state machines exist.

`live-preflight` now prints a strategy-by-strategy entry check. `thin_book` is treated as a maker-scalping block only, not as a blanket ban for every possible strategy. Macro trend, range, and breakout candidates are shown as observe/paper candidates until their own live state machines exist.

## Macro Strategy Lifecycle

`strategy-engine-step` manages non-scalping strategy cycles:

- `trend_follow`: market entry, TP/SL/max-hold exit management.
- `range_reversion`: post-only range-edge entry, TP/SL/max-hold exit management.
- `breakout_reduced`: reduced-size market entry, TP/SL/max-hold exit management.

The live path is guarded independently from scalping. It requires all of:

- `COINTRADING_DRY_RUN=false`
- `COINTRADING_LIVE_TRADING_ENABLED=true`
- `COINTRADING_LIVE_STRATEGY_LIFECYCLE_ENABLED=true`
- `COINTRADING_LIVE_ONE_SHOT_ENABLED=true` while `COINTRADING_LIVE_ONE_SHOT_REQUIRED=true`

With default settings it runs in dry-run/paper mode only. The VM runs this as `cointrading-strategy-engine.timer` every minute.

The one-shot guard is consumed after the first live lifecycle starts. This prevents a temporary live enable from accidentally becoming continuous trading.

The first real-money phase also has a simple execution gate in front of live strategy entries:

- `COINTRADING_SIMPLE_TRADE_GATE_ENABLED=true`
- `COINTRADING_SIMPLE_TRADE_GATE_ALLOWED_STRATEGIES=trend_follow`
- `COINTRADING_SIMPLE_TRADE_GATE_DAILY_ENTRY_LIMIT=1`
- `COINTRADING_SIMPLE_TRADE_GATE_COOLDOWN_MINUTES=60`
- `COINTRADING_SIMPLE_TRADE_GATE_MAX_CONSECUTIVE_LOSSES=2`
- `COINTRADING_SIMPLE_TRADE_GATE_APPLY_TO_DRY_RUN=false`

This means the bot may observe many strategies, but the first live phase can only attempt one trend-follow entry per KST day, waits 60 minutes before re-entering the same symbol, and stops after two consecutive live strategy losses. Paper collection is not throttled by this gate unless explicitly enabled.

## Strategy Gate

`strategy-evaluate` writes three evaluation sources into SQLite:

- `cycles`: actual post-only/paper lifecycle outcomes grouped by symbol, regime, and side.
- `strategy_cycles`: actual trend/range/breakout paper lifecycle outcomes grouped by strategy, symbol, side, and TP/SL/hold settings.
- `signal_grid`: a coarse TP/SL/max-hold grid using scored 1/3/5 minute signal returns.

The signal grid compares `maker_post_only`, `taker_momentum`, and `hybrid_taker_entry_maker_exit`. Taker and hybrid rows subtract taker fees plus `COINTRADING_STRATEGY_TAKER_SLIPPAGE_BPS` so tiny targets do not look artificially profitable.

New lifecycle entries are blocked when `COINTRADING_STRATEGY_GATE_ENABLED=true` and the matching execution mode/symbol/regime/side has no `APPROVED` evaluation. If the current fixed TP/SL/max-hold combination is not approved, the gate can select the best approved `signal_grid` candidate for that symbol/regime/side and pass its TP/SL/max-hold settings into the paper lifecycle. This keeps the bot collecting data while preventing weak combinations from continuing into new paper/live cycles.

Actual paper outcomes are stronger than signal-grid estimates. Once observed `cycles` or `strategy_cycles` rows reach `COINTRADING_STRATEGY_EARLY_BLOCK_SAMPLES`, a `BLOCKED` observed-paper evaluation vetoes new entries even if the signal grid still has an approved candidate.

Macro strategy live entries have an extra exact-profile guard. The bot computes the actual TP/SL/hold it would use at entry time, including adaptive exit changes from fresh macro data, and then requires that exact `strategy_cycles` row to be `APPROVED` before any live trend/range/breakout order can proceed. If the exact adaptive profile is still missing but broader observed paper data for the same strategy/symbol/side is `BLOCKED`, even dry-run collection is vetoed until reviewed.

Approval is based on positive net expectancy after fees/slippage, a minimum sample count, a low win-rate floor, and a break-even win-rate check derived from the observed average win/loss size. Defaults are intentionally stricter than the first dry-run scaffold: 100 samples, at least +0.5 bps expectancy, at least 42% win rate, and loss/win width no worse than 1.5. This avoids approving marginal positive rows that cannot survive normal execution noise.

Default paper exits now target positive payoff before frequency: maker scalping defaults to TP 16 bps / SL 4 bps / 300s max hold, while macro strategy defaults are trend 90/30 bps, range 30/15 bps, and breakout 120/40 bps.

`strategy-notify` can send a Telegram report when strategy decisions change or when the periodic interval elapses. It is manual/disabled by default on the VM because this report is a historical signal/paper evaluation, not a current tradable timing alert. Use Telegram `전략` or run the command manually when you want to inspect this layer.

## Automated Research Probe

`vibe-probe-notify` is the automated research gate. It runs a closed-bar probe over Binance public USDC futures candles for the active symbols, scores trend-following, range-reversion, and breakout candidates after taker fee/slippage, writes `data/vibe_probe_latest.json`, and sends Telegram only when an approved research candidate appears or a periodic 6-hour summary is due.

The VM installs `cointrading-vibe-probe-notify.timer`, but it is disabled by default to keep Telegram focused on current-entry alerts. It does not place orders and does not change live flags. Telegram `리서치` reads the latest result without requiring a shell command.

## Dashboard

The dashboard is a small HTTP server organized around the questions needed before live trading:

- `개요`: live/dry-run guard flags, runtime risk mode, active paper cycles, total paper PnL, and approved candidate count.
- `Paper`: combined scalping and macro-strategy paper cycles with entry price, current mark price, target/stop, active-cycle unrealized PnL, realized PnL, and exit reason, plus strategy-vs-scalp performance, average win/loss, payoff ratio, break-even win rate, and exit reason summaries.
- `리서치`: latest automated research-probe result, including approval/watch/block counts, strategy, symbol, sample size, win rate, average bps, profit factor, payoff ratio, and drawdown.
- `전략`: latest strategy candidate evaluations with Korean labels for source, execution mode, regime, side, and decision.
- `시장`: macro regime routing plus market context such as premium, funding, spread, depth, and imbalance.
- `위험`, `신호`, `주문`, `원본요약`: detailed runtime risk text, raw signals, blocked/order attempts, and legacy summaries for debugging.

It uses a server-sent events stream to update the data in place without reloading the whole page. Empty tables show explicit empty-state rows so the page does not look broken when a dataset has not collected enough samples yet.
Signal, strategy, lifecycle, and order tables show the latest 200 rows by default. Add `&limit=500` to the dashboard URL to inspect a longer window; the dashboard caps this at 1000 rows to keep mobile loading reasonable.

Set `COINTRADING_DASHBOARD_AUTH_TOKEN` before exposing it outside the VM.
When the token is set, requests must include either `?token=...` or an `Authorization: Bearer ...` header.

On the VM it runs as `cointrading-dashboard.service`, bound to `0.0.0.0:8080` for mobile/browser access.

```bash
curl -H "Authorization: Bearer $COINTRADING_DASHBOARD_AUTH_TOKEN" http://34.50.6.186:8080/
```
