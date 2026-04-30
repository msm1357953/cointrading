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
- `COINTRADING_MAX_DRAWDOWN_PCT=0.10`
- `COINTRADING_DAILY_LOSS_PCT=0.03`

## Commands

```bash
python -m cointrading.cli demo-backtest
python -m cointrading.cli explain-mdd
python -m cointrading.cli fetch-klines --symbol BTCUSDT --interval 1h --limit 200
python -m cointrading.cli binance-account
python -m cointrading.cli scalp-check --symbol BTCUSDT
python -m cointrading.cli scalp-collect
python -m cointrading.cli scalp-score
python -m cointrading.cli scalp-report
python -m cointrading.cli fee-status
python -m cointrading.cli telegram-me
python -m cointrading.cli telegram-updates --limit 5
python -m cointrading.cli telegram-poll --once --timeout 10
```

`fetch-klines` uses Binance public market data and does not need API keys.
Telegram setup details are in `docs/TELEGRAM_CONTROL.md`.

## Live Trading Warning

Live order placement is intentionally guarded by `dry_run`. Do not disable it until testnet order behavior, duplicate-order handling, fees, funding, and symbol filters are verified.
