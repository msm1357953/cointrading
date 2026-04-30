# Cointrading Status

## Current Scope

- Goal: build a small Python research/trading scaffold for Binance USD-M futures.
- Starting capital assumption: 1000 USDC.
- Default mode: mainnet credentials can exist, but dry-run first. No live trading by default.
- Initial symbols: BTCUSDT and ETHUSDT.
- Dry-run scalping symbols: BTCUSDT, ETHUSDT, BTCUSDC, ETHUSDC.

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

## Infrastructure

- GCP project: `seokmin-494312`
- VM: `cointrading-vm`
- Zone: `asia-northeast3-a`
- Static external IP: `34.50.6.186`
- VM project path: `~/cointrading`
- Local project path: `~/coding/cointrading`
- Telegram: notification/client and whitelisted Korean commands added; `계좌` reads a safe Binance futures account summary; `스캘핑 BTCUSDT` reads a dry-run scalping signal and market regime; `보고` reads scored dry-run results by horizon, side, and regime; live orders are not exposed over Telegram.
- Binance signed account check from VM succeeded on 2026-04-30.

## Latest Verification

- 2026-04-30: local and VM unit tests passed, 17 tests.
- 2026-04-30: `cointrading-telegram.service`, `cointrading-scalp-collect.timer`, and `cointrading-scalp-score.timer` are active on the VM.
- 2026-04-30: current dry-run report has 62 total logs and 14 scored 5 minute samples; 5 minute maker-net is still negative, so live trading remains blocked.
- 2026-04-30: API fee check shows BTCUSDC/ETHUSDC at maker 0 bps and taker 4 bps for this account. Multi-Assets Mode is off and USDC balance is 0, so USDC live trading is not ready yet.

## Next Work Packets

1. Run the included backtest on downloaded BTCUSDT/ETHUSDT klines.
2. Add exchange info parsing for exact tick size and quantity step size.
3. Add paper-trading logs before allowing live orders.
4. Add live order safeguards only after testnet behavior is verified.
5. Add paper-trading logs and Telegram daily status reports.
6. Let dry-run scalping collection run at least 4 hours, then inspect `/scalp_report`.
7. Use the regime breakdown to disable weak regimes before considering any paper/live order loop.
