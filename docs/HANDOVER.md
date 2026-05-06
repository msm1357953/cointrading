# Project Handover

Last updated: **2026-05-05** by Claude Opus 4.7 (1M).

This document is the entry point for any future agent (Claude or otherwise)
picking up this project. Read it before doing anything else.

---

## 0. Owner profile

- **Non-developer.** The owner builds and operates this system through AI
  agents. Don't ask them to run shell commands, edit `.env`, install
  packages, or run SQL. **You SSH and execute on their behalf.**
- Goal: passive income → eventual financial independence.
- Korean speaker. **Reply in Korean** unless the conversation is technical
  enough that English code/identifiers are unavoidable.
- Lives in KST. Cares about not having to be awake when signals fire.
- Has been burned in the past by overfit strategies and hyped tooling
  (`Vibe-Trading`, classic TA scalping). They will push back hard against
  anything that smells like marketing or "trust me" claims. Earn trust
  with data and honest negative results.

## 1. What is currently live on the VM

GCP project `seokmin-494312`, VM `cointrading-vm` (asia-northeast3-a, IP
`34.50.6.186`). Code lives at `~/cointrading`, venv at `~/cointrading/.venv`.

**Active systemd timers (the system runs these unattended):**

| Timer | Purpose |
|---|---|
| `cointrading-funding-engine.timer` (1m) | **Strategy 1**: funding-rate mean reversion lifecycle |
| `cointrading-wick-engine.timer` (1m) | **Strategy 2**: 5-minute wick reversion lifecycle |
| `cointrading-ratio-capture.timer` (1h) | Accumulates LS/taker history into the lake |
| `cointrading-market-context.timer` (5m) | Stores funding/OI/depth/spread snapshots |
| `cointrading-market-regime.timer` (5m) | Macro regime classification |
| `cointrading-scalp-collect.timer` (1m) | Legacy signal logging — kept for historical context |
| `cointrading-scalp-score.timer` (1m) | Legacy scoring — light, harmless |
| `cointrading-telegram.service` | Korean command bot |
| `cointrading-dashboard.service` | SSE dashboard |

**Disabled (Phase 1 cleanup, do NOT re-enable without rationale):**
`tactical-paper`, `tactical-live`, `strategy-evaluate`, `tactical-radar-notify`,
`live-supervisor-notify`, `trade-event-notify`, `refine-entry-notify`. The
strategies these served (`scalp_lifecycle`, `strategy_lifecycle`,
`tactical_radar`, `tactical_paper`, `meta_strategy`, `strategy_miner`,
`refined_entry_gate`, `research_probe`) all failed verification. The Python
modules still exist for reference but are NOT executed.

## 2. Two paper-validated strategies

Both are **long-only**, paper by default, backtested out-of-sample, both
share the `strategy_cycles` SQLite table.

### Funding mean reversion — `funding_carry_long`
- Module: [`src/cointrading/funding_lifecycle.py`](../src/cointrading/funding_lifecycle.py)
- Trigger: at each 8h funding settlement (UTC 00/08/16), if the latest
  funding rate ≤ −0.010%, open long.
- Hold: 24h max, with a hard −5% stop loss.
- Universe: BTC/ETH/SOL/XRP/DOGE USDC.
- Out-of-sample backtest: n=80, mean +33 bps after cost (was widened from
  −3% to −5% stop on 2026-05-05 to give 24h reversion room).
- Backtest: [`src/cointrading/research/funding_carry_backtest.py`](../src/cointrading/research/funding_carry_backtest.py)

### Wick reversion — `wick_long`
- Module: [`src/cointrading/wick_lifecycle.py`](../src/cointrading/wick_lifecycle.py)
- Trigger: most recent CLOSED 5-minute candle has `lower_wick_ratio ≥ 0.7`
  AND `(open − low) / open ≥ 1%`. Trigger bar must be fresh
  (≤6 minutes old).
- Hold: 2h max, hard −3% stop.
- 10-min cooldown after close before re-entry on the same symbol.
- Out-of-sample backtest: n=23 in 4 months, mean +73 bps after cost,
  PF 2.13. 4/4 alts (ETH, SOL, XRP, DOGE) positive in IN+OUT. BTC barely
  triggers (≤1% intrabar drop is rare on BTC).
- Backtest: [`src/cointrading/research/wick_scalp_backtest.py`](../src/cointrading/research/wick_scalp_backtest.py)

## 3. Live execution path (DORMANT — code shipped, gates closed)

[`src/cointrading/live_execution.py`](../src/cointrading/live_execution.py)
implements the shared order-flow primitives:

- `submit_live_market_long` — MARKET BUY with `RESULT` response so we get
  `avgPrice` + `executedQty`.
- `submit_protective_stop` — reduce-only `STOP_MARKET` on `MARK_PRICE`.
- `submit_live_market_close` — cancel stop, then reduce-only MARKET sell.
- `query_live_order_status` — poll for stop fills.
- `realized_pnl_from_close` — net of round-trip taker fees.

Both lifecycles call into this when `is_live_armed()` returns True.
Live arming requires **all three** of:

```
COINTRADING_DRY_RUN=false
COINTRADING_LIVE_TRADING_ENABLED=true
COINTRADING_FUNDING_CARRY_LIVE_ENABLED=true   # or _WICK_
```

**Defensive behaviour after live entry:** if the protective-stop submit
fails for any reason, the engine immediately submits a reduce-only market
exit. The position is never naked-long.

Tests covering live flows: [`tests/test_live_execution.py`](../tests/test_live_execution.py)
(160/160 pass as of this commit).

## 4. Live promotion workflow — YOU (the agent) execute it, not the owner

When the live-readiness gate triggers (a 🎯 Telegram alert from
`funding_carry_notify` / `wick_carry_notify` once a strategy clears
≥5 closed paper cycles, non-negative aggregate PnL, ≥40% win rate),
the owner will tell you. **You then do this end-to-end:**

1. **Re-verify** the gate manually by SSHing and querying the DB:
   ```bash
   gcloud compute ssh cointrading-vm --project=seokmin-494312 \
     --zone=asia-northeast3-a --command="
   cd ~/cointrading && .venv/bin/python -c \"
   from cointrading.funding_carry_notify import evaluate_live_ready
   from cointrading.storage import TradingStore
   print(evaluate_live_ready(TradingStore()))
   \""
   ```
2. **Confirm with the owner once.** Show them the n / WR / PnL / per-symbol
   stats so they understand what they're approving. Do NOT auto-arm.
3. After explicit go-ahead, edit the VM's `.env` via SSH (the owner does
   not edit shell or `.env`):
   ```bash
   gcloud compute ssh cointrading-vm ... --command="
   cd ~/cointrading
   sed -i 's/^COINTRADING_DRY_RUN=.*/COINTRADING_DRY_RUN=false/' .env
   sed -i 's/^COINTRADING_LIVE_TRADING_ENABLED=.*/COINTRADING_LIVE_TRADING_ENABLED=true/' .env
   grep -q '^COINTRADING_FUNDING_CARRY_LIVE_ENABLED' .env \
     && sed -i 's/^COINTRADING_FUNDING_CARRY_LIVE_ENABLED=.*/COINTRADING_FUNDING_CARRY_LIVE_ENABLED=true/' .env \
     || echo 'COINTRADING_FUNDING_CARRY_LIVE_ENABLED=true' >> .env
   sudo systemctl restart cointrading-funding-engine.timer
   "
   ```
   (Substitute `WICK_CARRY_LIVE_ENABLED` for the wick strategy.)
4. Run a single `funding-step` / `wick-step` to confirm `is_live_armed()=True`
   and report any new OPEN cycles to the owner.

**Key rule: the owner's job is to say "go". Your job is everything else.**

## 5. Failed hypotheses — DO NOT re-test these

Listed in chronological order. Each was verified honestly with the data
lake and rejected with reasons. Re-attempting them without new data
sources would just waste budget.

| # | Hypothesis | Why it failed |
|---|---|---|
| - | EMA cross / RSI / BB on 1h or 5m | Old `tactical_pullback` etc., 13–20% WR over 5 strategies. Crypto-major OHLC TA is dead alpha. |
| 2 | 1h drop reversion (24h hold) | Forward-return analysis looked good (+89 IN, +282 OUT) but realistic backtest with stop loss was −48 / −53 bps. The screen was misleading. |
| 5a | Top-trader L/S ratio extremes | 25-day window (Binance API limit) — all variants negative IN+OUT. |
| 5b | Smart-money divergence (top vs retail) | Same 25-day limit, mean −10 bps. |
| 5c | Taker buy/sell ratio extremes | Mean −13 bps both directions. |
| 6 | Cross-sectional momentum (long top / short bottom) | 5-symbol universe is too narrow for the academic CSM effect. All grid configs failed. |
| ⭐ | wick + funding combo | n=17/8 too small to deploy as a separate rule but very high effect (+273 bps mean, PF 7.39). Worth revisiting once paper has accumulated more closed cycles. |

## 6. Data lake (research only, local-first)

[`src/cointrading/research/data_lake.py`](../src/cointrading/research/data_lake.py)
populates Parquet files under `data/lake/`. Local has the full history;
the VM accumulates LS/taker data via the hourly `ratio-capture` timer.

| Path | Contents | Source |
|---|---|---|
| `klines/SYMBOL_INTERVAL.parquet` | 5m / 15m / 1h / 4h, 16 months | data.binance.vision |
| `funding/SYMBOL.parquet` | 5 symbols, 16 months | `/fapi/v1/fundingRate` |
| `ls_top_position/SYMBOL_5m.parquet` | Top-trader L/S, 25 days + ongoing | `/futures/data/topLongShortPositionRatio` |
| `ls_global_account/SYMBOL_5m.parquet` | Global L/S, same | `/futures/data/globalLongShortAccountRatio` |
| `taker_volume/SYMBOL_5m.parquet` | Taker buy/sell, same | `/futures/data/takerlongshortRatio` |
| `oi/...` | Open interest — **API blocks bulk history.** Hourly capture runs but only the last ~30 days are reachable. |

The standard analysis interface is `build_aligned_dataset(symbol, ...)`
which merges OHLCV + candle anatomy (wick ratios, intrabar drop) +
higher TFs + funding + LS/taker + OI onto the 5m timeline. Loads
~16 months in ~60ms.

`verify_hypothesis(name=..., trigger=fn, ...)` in
[`src/cointrading/research/verify.py`](../src/cointrading/research/verify.py)
is the standard runner: pass a callable that takes
`(df, i)` and returns whether bar `i` is a trigger. Returns IN / OUT /
FULL stats, per-symbol breakdown, drawdown.

**Always backtest with a real rule (stop loss, cooldown, one position
per symbol) — forward-return analysis is a screen, not a verdict.**

## 7. Telegram surface

Bot token in VM `.env`, chat allowlist in `TELEGRAM_ALLOWED_CHAT_IDS`.

Active commands (Korean):
- `상태` — bot mode + both strategies' summary
- `펀딩` / `펀딩보고` / `펀딩준비` / `펀딩설정`
- `꼬리` / `꼬리보고` / `꼬리준비` / `꼬리설정`
- `장세`, `시장상황 BTCUSDC`, `주문`, `포지션`
- `계좌`, `위험`, `수수료`, `BNB`, `BNB 보충`, `가격`
- `정지` / `재개`

Auto alerts come from `funding_carry_notify.py` and
`wick_carry_notify.py`:
- 📈 OPEN, ✅ CLOSED, 🛑 STOPPED on every cycle event
- 🎯 once per strategy when the live-readiness gate first passes (state
  files at `data/funding_carry_notify_state.json` and `data/wick_carry_notify_state.json`)

## 8. Key environment flags

```
COINTRADING_DRY_RUN=true                       # global paper switch (default true)
COINTRADING_TESTNET=false                      # mainnet API endpoint
COINTRADING_LIVE_TRADING_ENABLED=false         # second of three live gates
COINTRADING_INITIAL_EQUITY=1000                # nominal capital, USDC
COINTRADING_EQUITY_ASSET=USDC

# Strategy 1 — funding (active in paper)
COINTRADING_FUNDING_CARRY_ENABLED=true
COINTRADING_FUNDING_CARRY_NOTIONAL=80
COINTRADING_FUNDING_CARRY_THRESHOLD=0.0001     # |funding| ≥ 0.01%
COINTRADING_FUNDING_CARRY_STOP_LOSS_BPS=500
COINTRADING_FUNDING_CARRY_MAX_HOLD_SECONDS=86400
COINTRADING_FUNDING_CARRY_LIVE_ENABLED=false   # third gate

# Strategy 2 — wick (active in paper)
COINTRADING_WICK_CARRY_ENABLED=true
COINTRADING_WICK_CARRY_NOTIONAL=80
COINTRADING_WICK_CARRY_MIN_WICK_RATIO=0.7
COINTRADING_WICK_CARRY_MIN_DROP_PCT=0.01
COINTRADING_WICK_CARRY_STOP_LOSS_BPS=300
COINTRADING_WICK_CARRY_MAX_HOLD_SECONDS=7200
COINTRADING_WICK_CARRY_LIVE_ENABLED=false      # third gate

# BNB fee-fuel manager (fee discount support, not a strategy)
COINTRADING_BNB_FEE_TOPUP_ENABLED=false
COINTRADING_BNB_FEE_TOPUP_LIVE_ENABLED=false
COINTRADING_BNB_FEE_TOPUP_BEFORE_AUTO_ENTRY=true
COINTRADING_BNB_FEE_TOPUP_REQUIRED_FOR_LIVE=false
COINTRADING_BNB_FEE_TOPUP_SYMBOL=BNBUSDC
COINTRADING_BNB_FEE_TOPUP_MIN_BNB=0.003
COINTRADING_BNB_FEE_TOPUP_TARGET_BNB=0.02
COINTRADING_BNB_FEE_TOPUP_DYNAMIC_TARGET_ENABLED=true
COINTRADING_BNB_FEE_TOPUP_FEE_BUFFER_MULTIPLIER=1.5
COINTRADING_BNB_FEE_TOPUP_MAX_TARGET_BNB=1.0
COINTRADING_BNB_FEE_TOPUP_MIN_QUOTE_USDC=5
COINTRADING_BNB_FEE_TOPUP_MAX_QUOTE_USDC=100
COINTRADING_BNB_FEE_TOPUP_DAILY_QUOTE_LIMIT_USDC=200
```

BNB fee top-up flow: USD-M futures USDC → spot USDC → spot `BNBUSDC`
market buy → USD-M futures BNB. It only executes on mainnet when both top-up
flags are true and `COINTRADING_DRY_RUN=false`. Telegram commands: `BNB` for
status, `BNB 보충` / `BNB보충 15` for manual refill. `consecutive_auto` calls
the same manager before live entry; failure is non-blocking unless
`COINTRADING_BNB_FEE_TOPUP_REQUIRED_FOR_LIVE=true`. With dynamic target on,
the BNB target scales from current futures USDC balance × auto margin pct ×
auto leverage × max trades per day × round-trip taker fee × buffer, so it does
not stay stuck at the small static fallback target.

## 9. Code map

```
src/cointrading/
  config.py                      # all env-driven config
  storage.py                     # SQLite schema + query helpers
  exchange/binance_usdm.py       # public + signed REST client
  exchange_filters.py            # tick / step / minNotional normalisation
  bnb_fee_manager.py             # small BNB top-up for fee discount fuel
  models.py                      # Kline, OrderIntent, etc.

  funding_lifecycle.py           # Strategy 1 engine (paper + live)
  funding_carry_notify.py        # Telegram + state for Strategy 1
  wick_lifecycle.py              # Strategy 2 engine (paper + live)
  wick_carry_notify.py           # Telegram + state for Strategy 2
  live_execution.py              # shared live order-flow primitives

  research/
    data_lake.py                 # Parquet + ratio capture (research only)
    verify.py                    # standard hypothesis runner
    funding_carry.py             # Strategy 1 forward-return screen
    funding_carry_deep.py        # Strategy 1 grid (threshold/horizon/side)
    funding_carry_backtest.py    # Strategy 1 rule backtest
    wick_scalp_backtest.py       # Strategy 2 grid + rule backtest
    drop_reversion[_backtest].py # rejected hypothesis (kept for record)
    csm_backtest.py              # rejected hypothesis (kept for record)

  market_context.py / market_regime.py / risk_state.py / live_guard.py
                                 # generic infra shared by all strategies
  dashboard.py / telegram_bot.py # UI

deploy/                          # systemd units (one .service + .timer per job)
docs/                            # plan docs, this file
tests/                           # 160+ tests covering lifecycle, live exec, telegram, etc.
```

## 10. Things to NOT do

1. **Do not ask the owner to run shell commands** or edit `.env`. SSH
   in yourself.
2. **Do not enable a live gate without explicit owner confirmation in
   the same conversation.**
3. **Do not re-introduce EMA / RSI / MACD / Bollinger triggers** as
   primary signals. They were the dominant cause of the original
   system's losses.
4. **Do not commit `data/lake/` Parquet files** — they're listed in
   `.gitignore` and are rebuildable. Same for `data/binance_history/`.
5. **Do not skip the realistic backtest** even if forward-return looks
   strong. Drop reversion taught us that lesson.
6. **Do not deploy a new strategy without unit tests** for its
   lifecycle (open / time-exit / stop-loss / disabled / cooldown).
7. **Do not push to `main` without running the full test suite.**

## 11. Things to default to doing

1. **Korean responses.** Concise. Show data, not adjectives.
2. **Update STATUS.md** (the project's running log) at meaningful
   milestones with dated entries.
3. **Add to this file** when assumptions or workflows change so the
   next agent doesn't have to re-derive context.
4. **When a hypothesis fails, write WHY** in section 5 above. Future
   agents save time by not re-trying.
5. **Run the full test suite** before any commit.
6. **Use the data lake** for any new analysis. If you need a column
   the lake doesn't have, add it to `data_lake.py` and re-populate
   instead of writing one-off downloaders.
7. **SSH and operate the VM yourself** for any deploy / restart /
   env edit / status check.

---

If anything in this document conflicts with what you observe in the
code or VM, the code/VM is the source of truth. Update this file
afterward.
