"""Telegram alert when N consecutive same-direction 15m bars print.

This is an INFORMATIONAL alert only — no orders, no paper cycle, nothing
in the strategy_cycles table. The owner decides whether to act manually.

Why alert at all when the automated backtest showed this is dead at
retail-taker fees? The 2026-05-06 raw-signal check found:

    N=5 down → next-bar long  : mean +2.4 bps, WR 55% (n=5028)
    N=7 down → next-bar long  : mean −1.2 bps, WR 52% (n=967)

There is a small mean-reversion edge at N=5 that the 13 bps round-trip
cost eats. The owner can sometimes capture it manually with maker
limits. This alert exists for that decision support.

Behaviour:
- Every run: fetch last ~12 closed 15m bars for each watched symbol,
  count the run of same-direction closed bars ending at the most recent
  closed bar, and alert if the run length matches THRESHOLDS (default
  {6, 7}).
- Per-(symbol, run-length) latch via state file: don't re-alert on the
  same trigger bar. Cleared when the pattern breaks.
- No state mutation in DB; pure read-only on price feed.

State file (default: data/consecutive_bar_alert_state.json):

    {
        "BTCUSDC": {
            "last_alerted_bar_open_time_ms": 1777..,
            "last_alerted_n": 7,
            "last_alerted_direction": "down"
        }
    }
"""
from __future__ import annotations

import argparse
import json
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from cointrading.config import TelegramConfig, TradingConfig
from cointrading.exchange.binance_usdm import BinanceAPIError, BinanceUSDMClient
from cointrading.models import Kline
from cointrading.storage import default_db_path
from cointrading.telegram_bot import TelegramClient, TelegramConfigError


logger = logging.getLogger(__name__)

DEFAULT_SYMBOLS = ("BTCUSDC",)
DEFAULT_INTERVAL = "15m"
# N=5 has the strongest raw signal (+2.4 bps gross, WR 55% on 16-month
# 15m sample). N=6 and N=7 fire later when the mean reversion edge has
# already faded — included as confirmation tiers, not primary triggers.
DEFAULT_THRESHOLDS = (5, 6, 7)


def default_state_path() -> Path:
    return default_db_path().parent / "consecutive_bar_alert_state.json"


@dataclass
class RunResult:
    bar: Kline
    n: int
    direction: str  # "up" | "down"


def detect_run(klines: list[Kline]) -> RunResult | None:
    """Walk backward from the most recent CLOSED bar to count the run.

    Caller passes the result of `client.klines(..., limit=L)` where L is
    larger than the maximum threshold we care about. We assume the LAST
    element of the response may still be forming, so the most-recent
    closed bar is the second-to-last entry.
    """
    if len(klines) < 3:
        return None
    closed = klines[:-1]  # drop the partial bar
    last = closed[-1]
    if last.open == 0:
        return None
    last_dir = "up" if last.close > last.open else ("down" if last.close < last.open else "doji")
    if last_dir == "doji":
        return None
    n = 1
    for bar in reversed(closed[:-1]):
        if bar.open == 0:
            break
        bar_dir = "up" if bar.close > bar.open else ("down" if bar.close < bar.open else "doji")
        if bar_dir != last_dir:
            break
        n += 1
    return RunResult(bar=last, n=n, direction=last_dir)


def _load_state(path: Path) -> dict:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text())
    except (OSError, json.JSONDecodeError):
        return {}


def _save_state(path: Path, state: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(state, indent=2, sort_keys=True))


def _format_alert(symbol: str, interval: str, run: RunResult,
                  recent_bars: list[Kline]) -> str:
    arrow = "🔻 음봉" if run.direction == "down" else "🔺 양봉"
    side_hint = "→ 역추세 롱 후보" if run.direction == "down" else "→ 역추세 숏 후보"
    last = run.bar
    pct_change = (last.close - last.open) / last.open * 100 if last.open else 0
    lines = [
        f"⚠️ {symbol} {interval} 연속 {run.n}봉 ({arrow}) {side_hint}",
        f"  최근 종가: {last.close:.2f}  ({pct_change:+.2f}%)",
        "",
        "최근 봉 (오래된 → 최신):",
    ]
    for bar in recent_bars[-min(run.n, 10):]:
        if bar.open <= 0:
            continue
        delta = (bar.close - bar.open) / bar.open * 100
        ts = datetime.fromtimestamp(bar.open_time / 1000, tz=timezone.utc).astimezone()
        lines.append(f"  {ts:%H:%M}  {bar.open:.2f} → {bar.close:.2f}  ({delta:+.2f}%)")
    lines.append("")
    lines.append("ℹ️ 데이터: N=5 down→long mean +2.4 bps WR 55% (raw),")
    lines.append("   N=7 mean −1.2 bps WR 52%. 거래비용 13bps 차감 후 EV−.")
    lines.append("   판단은 너의 몫. 자동 진입 안 함.")
    return "\n".join(lines)


def run_check(
    *,
    symbols: tuple[str, ...] = DEFAULT_SYMBOLS,
    interval: str = DEFAULT_INTERVAL,
    thresholds: tuple[int, ...] = DEFAULT_THRESHOLDS,
    state_path: Path | None = None,
    config: TradingConfig | None = None,
    telegram_config: TelegramConfig | None = None,
) -> dict:
    cfg = config or TradingConfig.from_env()
    tcfg = telegram_config or TelegramConfig.from_env()
    state_path = state_path or default_state_path()
    state = _load_state(state_path)

    client = BinanceUSDMClient(config=cfg)

    tclient: TelegramClient | None = None
    if tcfg.bot_token and tcfg.default_chat_id:
        try:
            tclient = TelegramClient(tcfg)
        except TelegramConfigError as exc:
            logger.warning("consecutive_bar_alert: TelegramClient init failed: %s", exc)

    summary: dict[str, dict] = {}
    sent = 0
    fetch_limit = max(thresholds) + 5

    for symbol in symbols:
        try:
            klines = client.klines(symbol, interval, limit=fetch_limit)
        except BinanceAPIError as exc:
            logger.warning("consecutive_bar_alert: klines failed for %s: %s", symbol, exc)
            summary[symbol] = {"error": str(exc)}
            continue
        if len(klines) < 3:
            summary[symbol] = {"skip": "not enough klines"}
            continue
        run = detect_run(klines)
        if run is None:
            summary[symbol] = {"skip": "doji or no run"}
            continue

        sym_state = state.get(symbol, {})
        prev_bar_ms = sym_state.get("last_alerted_bar_open_time_ms")

        # Reset latch when pattern breaks (different direction or no longer ≥ min threshold)
        min_threshold = min(thresholds)
        if run.n < min_threshold or run.direction != sym_state.get("last_alerted_direction"):
            sym_state.pop("last_alerted_bar_open_time_ms", None)
            sym_state.pop("last_alerted_n", None)
            sym_state.pop("last_alerted_direction", None)

        # Find the highest threshold this run satisfies
        eligible = [t for t in thresholds if run.n >= t]
        if not eligible:
            state[symbol] = sym_state
            summary[symbol] = {"run_n": run.n, "direction": run.direction,
                               "alerted": False, "reason": f"below all thresholds {thresholds}"}
            continue

        target_n = max(eligible)
        already_alerted = (
            prev_bar_ms == run.bar.open_time
            and sym_state.get("last_alerted_n") == target_n
        )
        if already_alerted:
            state[symbol] = sym_state
            summary[symbol] = {"run_n": run.n, "direction": run.direction,
                               "alerted": False, "reason": "already alerted on this bar"}
            continue

        if tclient is not None:
            text = _format_alert(symbol, interval, run, klines[:-1])
            try:
                tclient.send_message(text)
                sent += 1
            except Exception as exc:  # noqa: BLE001 — never block the engine on telegram
                logger.warning("consecutive_bar_alert: send failed for %s: %s", symbol, exc)

        sym_state.update({
            "last_alerted_bar_open_time_ms": run.bar.open_time,
            "last_alerted_n": target_n,
            "last_alerted_direction": run.direction,
        })
        state[symbol] = sym_state
        summary[symbol] = {"run_n": run.n, "direction": run.direction,
                           "alerted": True, "threshold": target_n}

    _save_state(state_path, state)
    return {
        "ts_ms": int(datetime.now(timezone.utc).timestamp() * 1000),
        "telegram_sent": sent,
        "symbols": summary,
    }


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(description="Consecutive same-direction bar alerter")
    p.add_argument("--symbols", nargs="*", default=list(DEFAULT_SYMBOLS))
    p.add_argument("--interval", default=DEFAULT_INTERVAL)
    p.add_argument("--thresholds", nargs="*", type=int, default=list(DEFAULT_THRESHOLDS))
    p.add_argument("--state-path", type=Path, default=default_state_path())
    args = p.parse_args(argv)
    result = run_check(
        symbols=tuple(args.symbols), interval=args.interval,
        thresholds=tuple(args.thresholds), state_path=args.state_path,
    )
    print(json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    main()
