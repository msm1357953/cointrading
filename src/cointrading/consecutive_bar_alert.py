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
# A bar with body / range below this ratio is treated as a doji and is
# transparent inside a run — it doesn't break direction nor count toward
# the directional length.
DEFAULT_DOJI_BODY_RATIO = 0.15
# How many doji bars can sit inside a run before we treat it as broken.
# 1 = a single doji can interrupt 5 down bars and the run still counts.
DEFAULT_MAX_DOJI_PER_RUN = 1


def default_state_path() -> Path:
    return default_db_path().parent / "consecutive_bar_alert_state.json"


@dataclass
class RunResult:
    bar: Kline
    n: int               # directional bars in the run (excludes doji)
    direction: str       # "up" | "down"
    doji_count: int = 0  # how many doji-like bars are nested in the run


def _bar_direction(bar: Kline, doji_body_ratio: float) -> str:
    """Return 'up', 'down', or 'doji'. A small-body bar counts as doji."""
    if bar.open <= 0 or bar.high <= bar.low:
        return "doji"
    rng = bar.high - bar.low
    if rng <= 0:
        return "doji"
    body = abs(bar.close - bar.open)
    if body / rng < doji_body_ratio:
        return "doji"
    return "up" if bar.close > bar.open else "down"


def detect_run(
    klines: list[Kline],
    *,
    doji_body_ratio: float = DEFAULT_DOJI_BODY_RATIO,
    max_doji_per_run: int = DEFAULT_MAX_DOJI_PER_RUN,
) -> RunResult | None:
    """Walk backward from the most recent CLOSED bar to count a directional run.

    Doji-like bars (body / range below `doji_body_ratio`) are treated as
    transparent: they don't break the run and don't add to the directional
    count. Up to `max_doji_per_run` doji are allowed inside the run before
    it's considered broken.

    `klines` is the raw response from `client.klines(..., limit=L)`. We
    assume the last element may still be forming, so the most-recent
    CLOSED bar is the second-to-last entry.
    """
    if len(klines) < 3:
        return None
    closed = klines[:-1]
    if not closed:
        return None

    # Direction = the most recent CLOSED non-doji bar's direction.
    direction: str | None = None
    for bar in reversed(closed):
        d = _bar_direction(bar, doji_body_ratio)
        if d != "doji":
            direction = d
            break
    if direction is None:
        return None

    n_directional = 0
    doji_count = 0
    for bar in reversed(closed):
        d = _bar_direction(bar, doji_body_ratio)
        if d == direction:
            n_directional += 1
        elif d == "doji":
            if doji_count >= max_doji_per_run:
                break
            doji_count += 1
        else:
            break

    if n_directional == 0:
        return None
    return RunResult(bar=closed[-1], n=n_directional,
                     direction=direction, doji_count=doji_count)


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
    title = f"연속 {run.n}봉"
    if run.doji_count > 0:
        title += f" (+ 도지 {run.doji_count})"
    lines = [
        f"⚠️ {symbol} {interval} {title} ({arrow}) {side_hint}",
        f"  최근 종가: {last.close:.2f}  ({pct_change:+.2f}%)",
        "",
        "최근 봉 (오래된 → 최신):",
    ]
    show = recent_bars[-min(run.n + run.doji_count, 10):]
    for bar in show:
        if bar.open <= 0:
            continue
        d = _bar_direction(bar, DEFAULT_DOJI_BODY_RATIO)
        marker = "🔺" if d == "up" else ("🔻" if d == "down" else "·")
        delta = (bar.close - bar.open) / bar.open * 100
        ts = datetime.fromtimestamp(bar.open_time / 1000, tz=timezone.utc).astimezone()
        lines.append(f"  {marker} {ts:%H:%M}  {bar.open:.2f} → {bar.close:.2f}  ({delta:+.2f}%)")
    lines.append("")
    lines.append("ℹ️ 데이터: N=5 down→long mean +2.4 bps WR 55% (raw),")
    lines.append("   N=7 mean −1.2 bps WR 52%. 거래비용 13bps 차감 후 EV−.")
    lines.append("   도지(작은 몸통) 1개까지 끼어도 같은 흐름으로 봄.")
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

    # Lazy import of the auto engine so the alerter does not pull pandas etc.
    # at module import time (the alerter timer fires on the VM independently).
    from cointrading.consecutive_auto_lifecycle import (
        ConsecutiveAutoEngine,
        load_state as load_auto_state,
        save_state as save_auto_state,
    )
    from cointrading.storage import TradingStore as _TradingStore
    auto_state = load_auto_state()
    auto_engine: ConsecutiveAutoEngine | None = None

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

        # Telegram alert (always, regardless of auto mode)
        if tclient is not None:
            text = _format_alert(symbol, interval, run, klines[:-1])
            try:
                tclient.send_message(text)
                sent += 1
            except Exception as exc:  # noqa: BLE001 — never block the engine on telegram
                logger.warning("consecutive_bar_alert: send failed for %s: %s", symbol, exc)

        # Auto-execution: only on the configured auto symbol, when run.n >=
        # consecutive_auto_threshold AND auto mode is on AND safeguards pass.
        auto_action: dict | None = None
        if (
            symbol == cfg.consecutive_auto_symbol
            and run.n >= cfg.consecutive_auto_threshold
            and auto_state.auto_mode
        ):
            if auto_engine is None:
                auto_engine = ConsecutiveAutoEngine(
                    config=cfg, storage=_TradingStore(),
                    client=client,
                )
            outcome = auto_engine.maybe_open(run=run, klines=klines, state=auto_state)
            auto_action = {"action": outcome.action, "detail": outcome.detail,
                           "cycle_id": outcome.cycle_id}
            if outcome.action == "opened" and tclient is not None:
                # Send a separate auto-trade confirmation
                try:
                    tclient.send_message(_format_auto_open(symbol, outcome))
                    sent += 1
                except Exception as exc:  # noqa: BLE001
                    logger.warning("auto_open notify failed: %s", exc)

        sym_state.update({
            "last_alerted_bar_open_time_ms": run.bar.open_time,
            "last_alerted_n": target_n,
            "last_alerted_direction": run.direction,
        })
        state[symbol] = sym_state
        summary[symbol] = {"run_n": run.n, "direction": run.direction,
                           "alerted": True, "threshold": target_n}
        if auto_action is not None:
            summary[symbol]["auto"] = auto_action

    # Always step the auto engine to manage existing OPEN cycles (SL/TP/time)
    if auto_engine is None:
        auto_engine = ConsecutiveAutoEngine(
            config=cfg, storage=_TradingStore(),
            client=client,
        )
    manage_results = auto_engine.manage_open_cycles(auto_state)
    if manage_results and tclient is not None:
        for r in manage_results:
            if r["action"] in ("stopped", "closed_tp", "closed_time"):
                try:
                    tclient.send_message(_format_auto_close(r))
                    sent += 1
                except Exception as exc:  # noqa: BLE001
                    logger.warning("auto_close notify failed: %s", exc)

    save_auto_state(auto_state)
    _save_state(state_path, state)
    return {
        "ts_ms": int(datetime.now(timezone.utc).timestamp() * 1000),
        "telegram_sent": sent,
        "symbols": summary,
        "auto_state": auto_state.to_dict(),
        "manage": manage_results,
    }


def _format_auto_open(symbol: str, outcome) -> str:
    e = outcome.extra
    arrow = "🔺 SHORT" if e.get("side") == "short" else "🔻 LONG"
    sl_bps = e.get("sl_loss_bps", 0) or 0
    tp_bps = e.get("tp_gain_bps", 0) or 0
    rr = (tp_bps / sl_bps) if sl_bps > 0 else 0
    next_close_ms = e.get("next_bar_close_ms")
    bar_close_str = ""
    if next_close_ms:
        from datetime import datetime, timezone
        ts = datetime.fromtimestamp(next_close_ms / 1000, tz=timezone.utc).astimezone()
        bar_close_str = f"  강제청산 (15봉 마감): {ts:%H:%M:%S}\n"
    bnb_line = ""
    bnb = e.get("bnb_topup")
    if bnb:
        if bnb.get("action") == "topped_up":
            bnb_line = (
                f"  BNB 보충: {bnb.get('quote_amount_usdc', 0):.2f} USDC → "
                f"{bnb.get('transferred_bnb', 0):.8f} BNB\n"
            )
        elif bnb.get("action") not in ("sufficient", "disabled"):
            bnb_line = f"  BNB 보충: {bnb.get('action')} ({bnb.get('message')})\n"
    return (
        f"🤖 자동 진입 — {symbol}\n"
        f"  방향: {arrow}\n"
        f"  진입가: {e.get('entry'):.2f}\n"
        f"  SL: {e.get('sl'):.2f}  ({sl_bps:.0f} bps)\n"
        f"  TP: {e.get('tp'):.2f}  ({tp_bps:.0f} bps)  ← 직전봉 시작가\n"
        f"  RR: 1:{rr:.2f}\n"
        f"  노셔널: {e.get('notional'):.0f} USDC ({e.get('leverage')}x ISOLATED)\n"
        f"  자본 기준: {e.get('capital', 0):.0f} USDC\n"
        f"{bnb_line}"
        f"{bar_close_str}"
        f"  근거: {e.get('run_direction')} {e.get('run_n')}봉 연속\n"
        f"  cycle_id: {outcome.cycle_id}"
    )


def _format_auto_close(r: dict) -> str:
    icon = {"stopped": "🛑", "closed_tp": "✅", "closed_time": "⏱"}.get(r["action"], "ℹ️")
    label = {"stopped": "STOP", "closed_tp": "TP 익절", "closed_time": "시간 청산"}.get(r["action"], r["action"])
    pnl = r.get("pnl", 0)
    return (
        f"{icon} 자동 {label}\n"
        f"  exit: {r.get('exit', 0):.2f}\n"
        f"  PnL: {pnl:+.4f} USDC"
    )


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
