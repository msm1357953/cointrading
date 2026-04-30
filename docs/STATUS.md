# Cointrading Status

## Current Scope

- Goal: build a small Python research/trading scaffold for Binance USD-M futures.
- Starting capital assumption: 1000 USDC.
- Default mode: mainnet credentials can exist, but dry-run first. No live trading by default.
- Initial symbols: BTCUSDC and ETHUSDC.
- Dry-run scalping symbols: BTCUSDC and ETHUSDC.

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
- Telegram and CLI fee/status defaults now focus on BTCUSDC/ETHUSDC; `보고 전체` is only for legacy USDT logs.
- 2026-04-30: SQLite store, order/fee tables, Telegram DB-backed reports, a token-protected cloud dashboard service, and a dry-run post-only maker command were added.

## Next Work Packets

1. Run the included backtest on downloaded BTCUSDT/ETHUSDT klines.
2. Add exchange info parsing for exact tick size and quantity step size.
3. Add exact exchange info parsing for tick size and quantity step size before allowing live orders.
4. Add fill ingestion and open-order reconciliation.
5. Add Telegram daily status reports.
6. Let USDC dry-run scalping collection run at least 4 hours, then inspect `보고`.
7. Use the regime breakdown to disable weak regimes before considering any paper/live order loop.
