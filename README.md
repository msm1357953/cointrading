# cointrading

Small Python scaffold for Binance USD-M futures research.

This project starts in testnet/dry-run mode. It is not financial advice and it should not be connected to live trading until the risk gates in `docs/TRADING_PLAN.md` are satisfied.

## Quick Start

```bash
cd ~/coding/cointrading
python3 -m venv .venv
source .venv/bin/activate
pip install -e .
python -m cointrading.cli demo-backtest
```

## Configuration

Copy `.env.example` to `.env` if you want to use environment variables. The code auto-loads the project root `.env` before reading configuration.

Important defaults:

- `COINTRADING_DRY_RUN=true`
- `COINTRADING_TESTNET=true`
- `COINTRADING_INITIAL_EQUITY=1000`
- `COINTRADING_EQUITY_ASSET=USDC`
- `COINTRADING_SCALP_SYMBOLS=BTCUSDC,ETHUSDC,SOLUSDC,XRPUSDC,DOGEUSDC`
- `COINTRADING_MAX_DRAWDOWN_PCT=0.10`
- `COINTRADING_DAILY_LOSS_PCT=0.03`
- `COINTRADING_LIVE_TRADING_ENABLED=false`
- `COINTRADING_POST_ONLY_ORDER_NOTIONAL=25`
- `COINTRADING_SCALP_TAKE_PROFIT_BPS=3`
- `COINTRADING_SCALP_STOP_LOSS_BPS=6`

## Commands

```bash
python -m cointrading.cli demo-backtest
python -m cointrading.cli explain-mdd
python -m cointrading.cli fetch-klines --symbol BTCUSDT --interval 1h --limit 200
python -m cointrading.cli binance-account
python -m cointrading.cli scalp-check --symbol BTCUSDC
python -m cointrading.cli scalp-collect
python -m cointrading.cli scalp-score
python -m cointrading.cli scalp-report
python -m cointrading.cli migrate-csv-to-db
python -m cointrading.cli db-summary
python -m cointrading.cli maker-once --symbol BTCUSDC
python -m cointrading.cli scalp-engine-step
python -m cointrading.cli dashboard --host 127.0.0.1 --port 8080
python -m cointrading.cli fee-status
python -m cointrading.cli vibe-probe --symbols BTCUSDC ETHUSDC --interval 15m --limit 1000
python -m cointrading.cli vibe-probe-notify
python -m cointrading.cli telegram-me
python -m cointrading.cli telegram-updates --limit 5
python -m cointrading.cli telegram-poll --once --timeout 10
```

`fetch-klines` uses Binance public market data and does not need API keys.
Telegram setup details are in `docs/TELEGRAM_CONTROL.md`.

## Live Trading Warning

Live order placement is intentionally guarded by both `COINTRADING_DRY_RUN` and `COINTRADING_LIVE_TRADING_ENABLED`. The scalp lifecycle state machine runs in dry-run/paper mode by default and records entry, take-profit, stop, timeout, and reprice decisions. Do not disable the live guards until exchange reconciliation, symbol filters, and duplicate-order handling are verified.
