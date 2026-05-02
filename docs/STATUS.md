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
- 2026-05-01: Exchange-info filter parsing and `live-preflight` were added for the planned tiny first live test. Live order intents must pass tick size, step size, minQty, and minNotional checks before they can be submitted.
- 2026-05-01: Strategy entry routing was separated from the scalping signal. `thin_book` now blocks only maker-scalping entries; macro trend/range/breakout candidates are reported separately as observe/paper candidates until their own live state machines exist. Telegram `진입 ETHUSDC 25` and CLI `live-preflight` show the same strategy-by-strategy entry check.
- 2026-05-01: Macro strategy lifecycle state machines were added for `trend_follow`, `range_reversion`, and `breakout_reduced`. They use the shared SQLite/order/fill records, track entry/open/exit states, handle paper and live reconciliation paths, and require `COINTRADING_LIVE_STRATEGY_LIFECYCLE_ENABLED=true` before live strategy orders can be submitted. The VM deploy now installs `cointrading-strategy-engine.timer`, but live remains off by default.
- 2026-05-01: Telegram strategy notifications were made more decision-oriented. They now say that the report is a signal-log candidate evaluation rather than an order/position report, show live safety flags, group duplicate TP/SL/hold-time variants, and include active macro strategy state machines.
- 2026-05-01: A global per-symbol lifecycle lock was added. Scalp and macro strategy engines now share the same active-symbol guard, so only one lifecycle can open/manage a given symbol at a time.
- 2026-05-01: Market-context collection and live supervision were added. The VM now stores mark/index premium, funding, open interest, spread, top-book/depth liquidity, and imbalance; `live-supervisor`/Telegram `실전` combines fresh market context, macro regime, strategy candidates, paper lifecycle performance, active locks, real Binance orders/positions, min-notional checks, runtime risk, live flags, and one-shot guards into a final go/no-go report.
- 2026-05-01: Explicit rule strategies were added. `trend_follow` now requires 15m EMA/RSI confirmation, `range_reversion` uses 5m RSI plus Bollinger-band position, and `breakout_reduced` uses 5m breakout, RSI, and volume expansion. `strategy-evaluate` now also scores completed trend/range/breakout paper cycles.
- 2026-05-01: Dashboard was reorganized around live-readiness questions. The first screen now shows live guard flags, runtime risk, active paper cycles, paper PnL, and approved candidates; the Paper tab combines scalping and macro strategy paper cycles with entry/target/stop/reason/PnL; market/strategy tables use Korean labels and explicit empty states.
- 2026-05-01: Observed paper performance now vetoes new entries. If completed scalp or macro strategy paper cycles have enough samples and are `BLOCKED`, that real paper result overrides signal-grid approvals. Dashboard performance tables now show average win, average loss, payoff ratio, and break-even win rate.
- 2026-05-01: Default paper strategy thresholds were tightened for payoff. Maker scalping now defaults to TP 16 bps / SL 4 bps / 300s max hold, requires 4 bps minimum edge, and uses stricter imbalance/momentum gates. Strategy evaluation now requires at least 100 samples, +0.5 bps expectancy, 42% win rate, and loss/win width <= 1.5. Macro defaults were moved to higher reward/lower stop settings: trend 90/30 bps, range 30/15 bps, breakout 120/40 bps.
- 2026-05-01: Dashboard active/Paper cycle tables now show current mark price beside entry price and estimate active-cycle unrealized PnL after entry plus market-exit fee assumptions. Overview and Paper summaries also show total active unrealized PnL.
- 2026-05-01: Local and VM runtime envs were prepared for an 80 USDC one-shot candidate check so BTCUSDC can pass the current exchange minimum notional. `COINTRADING_MAX_SINGLE_ORDER_NOTIONAL`, `COINTRADING_POST_ONLY_ORDER_NOTIONAL`, `COINTRADING_STRATEGY_ORDER_NOTIONAL`, and `COINTRADING_LIVE_ONE_SHOT_NOTIONAL` are set to 80, while dry-run/live/one-shot live flags remain locked off.
- 2026-05-01: Live-supervisor candidate Telegram alerts were added. The VM can run `live-supervisor-notify` every minute and sends a message only when an approved, macro-aligned, paper-positive candidate appears with no blockers except dry-run/live/one-shot safety locks. The alert does not place orders.
- 2026-05-01: Live-supervisor paper gates were tightened before any real-money one-shot. A candidate now needs at least 20 closed paper cycles, positive all-time and recent paper PnL, a paper payoff ratio of at least 1.2, and a recent stop/max-hold exit ratio no higher than 65% before it can produce an actionable Telegram alert. Signal-grid approval alone is no longer enough, and research-only execution modes without matching live state machines are blocked.
- 2026-05-01: Telegram and dashboard candidate wording now separates strategy type from order execution. For example, maker-post-only candidates are shown as `전략=메이커 스캘핑` and `주문=지정가 메이커` instead of treating the order type as the strategy name.
- 2026-05-01: A simple live execution gate was added for the first real-money phase. By default it applies to live strategy entries only, allows `trend_follow` only, limits entries to one per KST day, enforces a 60-minute same-symbol cooldown after close, and halts new entries after two consecutive live strategy losses. Paper collection remains unaffected unless `COINTRADING_SIMPLE_TRADE_GATE_APPLY_TO_DRY_RUN=true`.
- 2026-05-01: Telegram trade-event notifications were added separately from candidate evaluation reports. The VM can now send concise entry attempt, entry fill, take-profit, stop-loss, max-hold, cancellation, and periodic position-status reports with entry/current/target/stop prices and estimated unrealized PnL.
- 2026-05-02: Macro strategy exits now support adaptive profiles. `trend_follow`, `range_reversion`, and `breakout_reduced` still have conservative base TP/SL/hold settings, but new cycles can tighten or extend targets based on current ATR and trend strength. This can be disabled with `COINTRADING_STRATEGY_ADAPTIVE_EXITS_ENABLED=false`.
- 2026-05-02: Adaptive exit safety gates were tightened after review. Live macro strategy entries now require exact observed `strategy_cycles` paper approval for the actual TP/SL/hold profile that will be submitted, stale macro rows are ignored when choosing adaptive exits, broad bad observed paper results can veto untested adaptive profiles, and live-supervisor reports now show the actual order plan separately from the candidate row.
- 2026-05-02: Dry-run order submission was hardened. Scalp and macro strategy dry-run paths now synthesize local dry-run order responses without calling the exchange client's `new_order`, so a client/config mismatch cannot leak dry-run paper orders to Binance.
- 2026-05-02: Vibe-Trading was tested as a research/backtest reference, not a live execution engine. A repeatable `vibe-probe` command was added to run a Vibe-style closed-bar probe over Binance public USDC futures candles for trend-following, range-reversion, and breakout strategies with taker fee/slippage, TP/SL/max-hold exits, payoff, profit factor, drawdown, and approval/block decisions.
- 2026-05-02: The research probe is now automatic on the VM through `cointrading-vibe-probe-notify.timer`. It refreshes `data/vibe_probe_latest.json` every 30 minutes, sends Telegram on approved-candidate changes or 6-hour periodic summaries, adds Telegram `리서치`, and exposes a `리서치` dashboard tab. It never places orders.
- 2026-05-02: Research was restructured around a single regime-adaptive meta policy. New `meta-backtest` downloads/caches long-range Binance public USD-M futures candles from `data.binance.vision`, classifies each closed bar into trend/range/breakout/panic/mixed, chooses only one action, simulates conservative taker fee/slippage exits, and writes `data/meta_strategy_latest.json`. Dashboard now has a `메타전략` tab, Telegram `메타`/`리서치` reads the meta report, and `cointrading-meta-backtest-notify.timer` was added for 6-hour research summaries. The default research interval is `1h` because full 15m multi-symbol history is too noisy and too heavy for the VM. The first 2025-01-01 through 2026-04-30 USDC run blocked all five symbols, which means the current meta policy is not live-ready.
- 2026-05-02: A first strategy-mining pass was added. `strategy-mine` searches rule/exit-profile combinations over historical features, selects candidates only from a rolling training window, then scores the next unseen month. Dashboard `메타전략` now includes a strategy-mining table, and Telegram `발굴` reads `data/strategy_mine_latest.json`. The first 2025-01-01 through 2026-04-30 1h run found WATCH candidates but no SURVIVED candidate, so no strategy should be promoted to live yet.
- 2026-05-02: A local-only second-stage strategy refinement pass was added. `strategy-refine` reads `data/strategy_mine_latest.json`, expands only WATCH/SURVIVED candidates around nearby TP/SL/hold/filter values, reruns strict walk-forward, writes `data/strategy_refine_latest.json`, and exposes the result in dashboard `메타전략` plus Telegram `정제`. Heavy historical analysis should run locally; only the generated JSON should be copied to the VM.

## Next Work Packets

1. Improve strategy mining with broader feature families: funding, open-interest change, BTC stress, session/time filters, and volatility compression/expansion.
2. Add a current-market/paper promotion gate for `strategy-refine` SURVIVED candidates before any live test. Historical survival is not enough by itself.
3. Keep live order flags off until at least one candidate is `SURVIVED` and paper lifecycle results confirm it on current market data.
4. Run `strategy-mine` and `meta-backtest-notify --force` on the VM after deploy, then watch Telegram `발굴`/`메타` and dashboard `메타전략`.
5. Run `live-preflight --notional <tiny size> --symbols <symbol>` on the VM immediately before any real-money test.
6. Continue validating live exchange fill ingestion and open-order reconciliation on tiny one-shot tests only after research and paper evidence agree.
