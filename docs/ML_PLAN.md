# Machine Learning Plan

## Position

LLM APIs are not required for trading decisions. The first useful machine-learning layer should be local tabular ML trained from the bot's own SQLite data.

Use LLMs only for:

- daily report summarization
- explaining why a regime or strategy changed
- turning metrics into review questions
- risk-alert wording

Do not use an LLM as the order-entry brain.

## First Dataset

Build rows from:

- `signals`: microstructure features, side, micro regime, 1/3/5 minute forward returns
- `market_regimes`: macro regime, trend, volatility, ATR, allowed strategy set
- `strategy_evaluations`: strategy candidate labels
- `scalp_cycles`: paper lifecycle outcome and realized PnL
- `orders` and `fills`: execution intent, fill, fee, and slippage records

Initial labels:

- `horizon_5m_bps > 0`: short-horizon signal direction was right
- `realized_pnl > 0`: paper cycle was profitable
- `max_hold_exit` / `stop_loss`: negative lifecycle outcome classes

## First Models

Start simple:

- logistic regression or random forest for "take / skip"
- gradient boosting later if the feature table is stable
- walk-forward validation split by time, never random shuffle

The model should produce a score used by the strategy gate. It should not place orders directly.

## Promotion Rule

A model can only influence paper trading when all are true:

- enough data exists after the latest feature/schema change
- walk-forward validation is better than the rule baseline
- live order guards remain off
- the model decision is logged with features and score

