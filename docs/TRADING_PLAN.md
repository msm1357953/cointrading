# Binance Futures Quant Plan

## What Max Drawdown Means

Max drawdown is the deepest fall from a previous equity high.

Example:

- Start: 1000 USDT
- Equity high: 1100 USDT
- Later low: 880 USDT
- Drawdown from high: 20%

It is not just "current loss rate"; it measures how painful the worst peak-to-trough drop was. For a 1000 USDT first account, a 10% max drawdown stop means the system should stop trading near 900 USDT until reviewed.

## First Strategy Family

Start with a simple trend-following strategy:

- Go long when fast moving average is above slow moving average.
- Go short when fast moving average is below slow moving average.
- Stay flat when volatility is too high or there is not enough data.

This is intentionally simple. The first goal is to validate data, costs, position sizing, and risk controls before trying clever alpha.

## Why Python

Python is the practical default for this phase because it has strong tooling for:

- data collection
- backtesting
- statistics
- exchange API integration
- later notebooks and charts

The first version avoids heavy dependencies so the skeleton can run immediately.

## Live Trading Gate

Do not enable live trading until all items are true:

- Signal-grid approval is backed by at least 20 closed paper lifecycle cycles on the same symbol and side.
- All-time and recent paper PnL are positive.
- Paper payoff ratio is at least 1.2, so average wins are meaningfully larger than average losses.
- Recent stop-loss/max-hold exits are not dominating the sample.
- Backtest or paper evaluation includes fees and slippage.
- Daily loss and max drawdown stops are tested.
- API key permissions are restricted.
- Logs prove that duplicate orders are not created after failures.
