# Cointrading Status

## Current Scope

- Goal: build a small Python research/trading scaffold for Binance USD-M futures.
- Starting capital assumption: 1000 USDC.
- Default mode: mainnet credentials can exist, but dry-run first. No live trading by default.
- Initial symbols: BTCUSDC and ETHUSDC.
- Dry-run scalping symbols: BTCUSDC, ETHUSDC, SOLUSDC, XRPUSDC, and DOGEUSDC.

## Risk Defaults

- Max account drawdown stop: 10%.
- Daily loss stop: 3%.
- Risk per trade: 0.5% of equity.
- Max notional exposure: 1.5x equity.
- Max leverage setting: 2x for the first live phase.

## Open Decisions

- Final Binance account jurisdiction and futures eligibility are not verified here.
- Real API keys are stored only in gitignored `.env` files locally and on the VM.
- Fees, funding, and symbol filters must be refreshed from Binance before live use.
- Live order placement remains disabled unless both `COINTRADING_DRY_RUN=false` and `COINTRADING_LIVE_TRADING_ENABLED=true`.

## Infrastructure

- GCP project: `seokmin-494312`
- VM: `cointrading-vm`
- Zone: `asia-northeast3-a`
- Static external IP: `34.50.6.186`
- VM project path: `~/cointrading`
- Local project path: `~/coding/cointrading`
- Telegram: notification/client and whitelisted Korean commands added; `계좌` reads a safe Binance futures account summary; `스캘핑 BTCUSDC` reads a dry-run scalping signal and market regime; `보고` reads scored dry-run results for active USDC symbols by horizon, side, and regime; `보고 전체` includes legacy USDT logs; live orders are not exposed over Telegram.
- Binance signed account check from VM succeeded on 2026-04-30.

## Latest Verification

- 2026-04-30: local and VM unit tests passed, 21 tests.
- 2026-04-30: `cointrading-telegram.service`, `cointrading-scalp-collect.timer`, and `cointrading-scalp-score.timer` are active on the VM.
- 2026-04-30: API fee check shows BTCUSDC/ETHUSDC at maker 0 bps and taker 4 bps before BNB discount for this account.
- 2026-04-30: BNB and USDC are funded in the futures wallet, so USDC symbols are the active dry-run universe.
- Telegram and CLI fee/status defaults focus on USDC symbols; `보고 전체` is only for legacy USDT logs.
- 2026-04-30: SQLite store, order/fee tables, Telegram DB-backed reports, a token-protected cloud dashboard service, and a dry-run post-only maker command were added.
- 2026-04-30: SOLUSDC, XRPUSDC, and DOGEUSDC were added to VM dry-run collection for altcoin comparison. First checks showed enough top-book liquidity, but immediate signals were all `quiet_chop`.
- 2026-04-30: Post-only scalp lifecycle state machine was added in dry-run/paper mode. It tracks entry wait, paper fill, immediate take-profit, timeout/reprice, stop-loss, max-hold exit, fills, and paper PnL.
- 2026-04-30: Dashboard and Telegram human-facing order/cycle times now display in Korea time (KST); DB timestamps remain UTC.
- 2026-04-30: Dashboard full-page refresh was replaced with a server-sent events stream so visible data updates in place.
- 2026-04-30: Dashboard was split into summary, performance, lifecycle, signal, and order tabs.
- 2026-04-30: Dashboard signal/lifecycle/order tables now show 200 rows by default and support a bounded `limit` query parameter for longer inspection.
- 2026-04-30: Strategy evaluation/gating was added. SQLite now stores cycle and signal-grid evaluations, the dashboard has a strategy-candidate tab, and new lifecycle entries are blocked unless the matching symbol/regime/side/current TP/SL/max-hold evaluation is approved.
- 2026-04-30: Strategy evaluation now compares maker-post-only, taker-momentum, and hybrid taker-entry/maker-exit candidates with taker slippage assumptions. Telegram strategy reports are sent when candidate decisions change or on a periodic interval.
- 2026-04-30: Strategy gating now uses net expectancy plus observed win/loss payoff balance instead of a hard 50% win-rate cutoff. Approved signal-grid candidates can pass their own TP/SL/max-hold values into the dry-run/paper lifecycle.
- 2026-04-30: Macro regime routing was added. The VM classifies active symbols into bull/bear/range/breakout/panic regimes every 5 minutes, records the allowed strategy set, shows it in Telegram/dashboard, and blocks new scalping cycles when the macro router rejects that direction.
- 2026-04-30: Gemini LLM reporting was added for Telegram risk summaries only. It runs about three times per day, uses SQLite context, and is explicitly excluded from order execution or live-entry decisions.
- 2026-04-30: Gemini reporting was verified on the VM with `gemini-3.1-pro-preview`; the local/VM env key may be named `GEMINI_API_KEY`, `GEMINI_KEY`, or `gemini_key`. A forced Telegram risk summary was sent successfully after increasing the Gemini output budget.
- 2026-04-30: Runtime risk mode was added. It converts recent stop-loss clusters, requote clusters, KST-day realized loss, and BTC stress into NORMAL/CAUTION/DEFENSIVE/HALT; DEFENSIVE/HALT blocks new entries, CAUTION blocks live entries, and the dashboard/Telegram/LLM context now expose this mode. Live scalp entry is also blocked unless `COINTRADING_LIVE_SCALP_LIFECYCLE_ENABLED=true`.
- 2026-04-30: Live lifecycle tests and a minimal live reconciliation path were added. The covered path checks live entry order status, ingests user trades, submits reduce-only take-profit, closes on live take-profit fill, and cancels/replaces the target with reduce-only market exit for stop-loss. Live remains disabled by default.
- 2026-05-01: Exchange-info filter parsing and `live-preflight` were added for the planned 10 USDC first live test. Live order intents must pass tick size, step size, minQty, and minNotional checks before they can be submitted.

## Next Work Packets

1. Run the included backtest on downloaded BTCUSDT/ETHUSDT klines.
2. Run `live-preflight --notional 10` on the VM immediately before any real-money test.
3. Add a one-shot/manual live enable path that caps notional at 10 USDC and automatically disables live mode after the first closed cycle.
4. Add live exchange fill ingestion and open-order reconciliation.
5. Let USDC dry-run scalping collection continue, then inspect Telegram `전략` and the dashboard strategy tab.
6. Review maker/taker/hybrid candidates and only consider live escalation after `APPROVED` rows stay stable across larger sample sizes and their paper lifecycle outcomes are positive.
7. Add a local ML feature dataset from signals, macro regimes, orders, fills, and strategy outcomes; keep Gemini as reporting/monitoring only.
8. Add exact exchange info parsing for tick size and quantity step size before allowing live orders.
9. Before any real-money test, add live fill/order reconciliation and exact TP/SL exit handling, then run a Telegram/manual preflight check with live order notional capped to a tiny amount.
