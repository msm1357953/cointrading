# Scalping Plan

## Position

Scalping is only useful here if the bot can observe microstructure faster and more consistently than manual trading. It is also where fees hurt the most.

Current observed futures commission:

- Maker round trip: about 4 bps.
- Taker round trip: about 10 bps.

Because of that, the first scalping implementation is a scanner, not a live orderer.

## First Signal

The dry-run signal combines:

- best bid/ask spread
- top book depth imbalance
- top book liquidity
- recent 1 minute momentum
- short realized volatility
- maker-fee edge buffer
- current maker/taker round-trip fee
- latest funding rate
- BNB fee discount status and BNB balance
- USDT vs USDC symbol fee differences

Initial decision rules:

- skip if spread is wider than 1.5 bps
- skip if top book liquidity is too thin
- skip if short realized volatility is above 35 bps
- hard skip if short realized volatility is above 60 bps
- skip if the aligned move does not clear maker round-trip fees by at least 1 bps
- long setup if bid-side depth imbalance and 1m momentum align
- short setup if ask-side depth imbalance and 1m momentum align

The scanner now labels each sample with a market regime:

- scalping possible: aligned long or aligned short
- hard no-trade: wide spread, thin book, high volatility, panic volatility, funding risk
- wait states: low edge after fees, trend without book confirmation, book without momentum, quiet chop

## Commands

```bash
python -m cointrading.cli scalp-check --symbol BTCUSDT
python -m cointrading.cli scalp-check --symbol BTCUSDC
python -m cointrading.cli scalp-check --symbol ETHUSDT
python -m cointrading.cli scalp-check --symbol ETHUSDC
python -m cointrading.cli fee-status
```

Default dry-run collection watches `BTCUSDT`, `ETHUSDT`, `BTCUSDC`, and `ETHUSDC`.
As of the 2026-04-30 API check, BTC/ETH USDC futures return 0 maker fee and 4 bps taker fee for this account, while USDT futures return 2 bps maker and 5 bps taker.
USDC live trading still needs USDC margin or Multi-Assets Mode; otherwise USDC symbols can be monitored but should not be live traded.

If this command is run outside the whitelisted VM, signed commission lookup may fail and the scanner falls back to configured fee defaults.

Telegram:

```text
스캘핑 BTCUSDT
스캘핑 BTCUSDC
스캘핑 ETHUSDT
스캘핑 ETHUSDC
보고
보고 BTCUSDT
수수료
```

Slash commands such as `/scalp BTCUSDT` and `/scalp_report` still work.

## Dry-run Collection

The VM collects BTCUSDT and ETHUSDT signals every minute into:

```text
~/cointrading/data/scalp_signals.csv
```

Each signal is scored after 1, 3, and 5 minutes using the later mid price.
The report breaks down results by direction and market regime, so the next change should come from observed weak regimes rather than manually changing strategy every few minutes.

Decision rule for the next step:

- run the collector for at least 4 hours before making any live-order decision
- prefer 24 hours before using the result seriously
- only consider live post-only orders if 5 minute maker-net expectancy is positive after fees
- keep ignoring taker entries unless taker-net expectancy is clearly positive

CLI:

```bash
python -m cointrading.cli scalp-collect
python -m cointrading.cli scalp-score
python -m cointrading.cli scalp-report
python -m cointrading.cli scalp-report --symbol BTCUSDT
```

## Live Trading Gate

Do not place live scalping orders until:

- the dry-run scanner has produced logs for multiple sessions
- symbol tick size and quantity step size are parsed from exchange info
- post-only limit orders are implemented and tested
- cancel/replace logic is idempotent
- duplicate order protection is tested after API timeouts
- daily loss and kill switch are enforced outside strategy code
