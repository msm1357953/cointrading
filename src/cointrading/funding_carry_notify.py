"""Run a funding-carry lifecycle step and notify Telegram on key events.

Sends a Telegram message for each cycle opened, closed (time exit), or
stopped (stop loss) in the step. Also tracks a one-shot 'live readiness'
alert: when accumulated paper evidence first crosses the gate
  (>=5 closed cycles, non-negative aggregate PnL, win rate >= 40%)
the user is notified once. The state file prevents duplicate alerts.

The engine itself stays pure (no Telegram coupling); this module composes
engine.step() with telegram delivery so tests of the engine remain simple.
"""
from __future__ import annotations

import argparse
import json
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from cointrading.config import TelegramConfig, TradingConfig
from cointrading.exchange.binance_usdm import BinanceUSDMClient
from cointrading.funding_lifecycle import (
    FundingCarryEngine,
    STATUS_CLOSED,
    STATUS_OPEN,
    STATUS_STOPPED,
    STRATEGY_NAME,
)
from cointrading.storage import TradingStore, default_db_path
from cointrading.telegram_bot import TelegramClient, TelegramConfigError


logger = logging.getLogger(__name__)


LIVE_READY_MIN_CLOSED = 5
LIVE_READY_MIN_SUM_PNL = 0.0
LIVE_READY_MIN_WIN_RATE = 0.40


def default_state_path() -> Path:
    return default_db_path().parent / "funding_carry_notify_state.json"


@dataclass
class LiveReadyStatus:
    ready: bool
    closed_n: int
    win_n: int
    loss_n: int
    sum_pnl: float
    win_rate: float
    reasons: list[str]


def _load_state(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text())
    except (OSError, json.JSONDecodeError):
        return {}


def _save_state(path: Path, state: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(state, indent=2, sort_keys=True))


def _format_open(opened: dict[str, Any], cfg: TradingConfig) -> str:
    symbol = opened["symbol"]
    fr = float(opened["funding_rate"])
    px = float(opened["entry_price"])
    stop_px = px * (1 - cfg.funding_carry_stop_loss_bps / 10_000.0)
    hold_h = cfg.funding_carry_max_hold_seconds // 3600
    notional = cfg.funding_carry_notional
    return (
        "📈 funding-carry OPEN\n"
        f"  {symbol} long @ {px:.6f}\n"
        f"  funding {fr * 100:+.4f}% (trigger -{cfg.funding_carry_threshold * 100:.4f}%)\n"
        f"  hold {hold_h}h, stop {stop_px:.6f} (-{cfg.funding_carry_stop_loss_bps:.0f} bps)\n"
        f"  notional {notional:.0f} USDC (paper)"
    )


def _format_close(action_row: dict[str, Any], cycle_row: Any) -> str:
    symbol = action_row["symbol"]
    entry = float(action_row["entry"])
    exit_px = float(action_row["exit"])
    ret_bps = (exit_px - entry) / entry * 10_000.0
    realized_pnl = float(cycle_row["realized_pnl"]) if cycle_row["realized_pnl"] is not None else 0.0
    if action_row["action"] == "stopped":
        emoji = "🛑"
        title = "STOPPED"
        reason = "stop_loss"
    else:
        emoji = "✅"
        title = "CLOSED"
        reason = "time_exit (24h)"
    return (
        f"{emoji} funding-carry {title}\n"
        f"  {symbol}: {entry:.6f} → {exit_px:.6f} ({ret_bps:+.1f} bps)\n"
        f"  reason: {reason}\n"
        f"  PnL: {realized_pnl:+.4f} USDC (after fees)"
    )


def evaluate_live_ready(storage: TradingStore) -> LiveReadyStatus:
    """Check if paper evidence has accumulated to allow live consideration."""
    with storage.connect() as connection:
        rows = list(
            connection.execute(
                """
                SELECT status, realized_pnl FROM strategy_cycles
                WHERE strategy=? AND status IN (?, ?)
                """,
                (STRATEGY_NAME, STATUS_CLOSED, STATUS_STOPPED),
            )
        )
    closed_n = len(rows)
    wins = [r for r in rows if r["realized_pnl"] is not None and r["realized_pnl"] > 0]
    losses = [r for r in rows if r["realized_pnl"] is not None and r["realized_pnl"] <= 0]
    sum_pnl = sum(float(r["realized_pnl"] or 0.0) for r in rows)
    win_rate = len(wins) / closed_n if closed_n > 0 else 0.0

    reasons: list[str] = []
    if closed_n < LIVE_READY_MIN_CLOSED:
        reasons.append(f"closed cycles {closed_n}/{LIVE_READY_MIN_CLOSED}")
    if sum_pnl < LIVE_READY_MIN_SUM_PNL:
        reasons.append(f"sum PnL {sum_pnl:+.4f} < {LIVE_READY_MIN_SUM_PNL:+.2f}")
    if win_rate < LIVE_READY_MIN_WIN_RATE:
        reasons.append(f"win rate {win_rate * 100:.0f}% < {LIVE_READY_MIN_WIN_RATE * 100:.0f}%")

    ready = not reasons
    return LiveReadyStatus(
        ready=ready,
        closed_n=closed_n,
        win_n=len(wins),
        loss_n=len(losses),
        sum_pnl=sum_pnl,
        win_rate=win_rate,
        reasons=reasons,
    )


def _format_live_ready(status: LiveReadyStatus) -> str:
    return (
        "🎯 funding-carry: 라이브 검토 가능 시점 도달\n"
        f"  closed cycles: {status.closed_n} (≥{LIVE_READY_MIN_CLOSED} ✓)\n"
        f"  sum PnL: {status.sum_pnl:+.4f} USDC (≥{LIVE_READY_MIN_SUM_PNL:+.2f} ✓)\n"
        f"  win rate: {status.win_rate * 100:.0f}% ({status.win_n}W/{status.loss_n}L) (≥{LIVE_READY_MIN_WIN_RATE * 100:.0f}% ✓)\n\n"
        "라이브 진입 조건 (3개 모두 필요):\n"
        "  1. .env: COINTRADING_DRY_RUN=false\n"
        "  2. .env: COINTRADING_LIVE_TRADING_ENABLED=true\n"
        "  3. .env: COINTRADING_FUNDING_CARRY_LIVE_ENABLED=true\n"
        "  → sudo systemctl restart cointrading-funding-engine.timer"
    )


def run_step_and_notify(*, state_path: Path | None = None) -> dict[str, Any]:
    cfg = TradingConfig.from_env()
    tcfg = TelegramConfig.from_env()
    storage = TradingStore()
    client = BinanceUSDMClient(config=cfg)
    engine = FundingCarryEngine(config=cfg, storage=storage, client=client)

    state_path = state_path or default_state_path()
    state = _load_state(state_path)

    result = engine.step()

    tclient: TelegramClient | None = None
    if tcfg.bot_token and tcfg.default_chat_id:
        try:
            tclient = TelegramClient(tcfg)
        except TelegramConfigError as exc:
            logger.warning("funding_carry_notify: TelegramClient init failed: %s", exc)

    sent = 0
    if tclient is not None:
        for opened in result.opened:
            try:
                tclient.send_message(_format_open(opened, cfg))
                sent += 1
            except Exception as exc:  # noqa: BLE001 — never block the lifecycle on telegram
                logger.warning("funding_carry_notify: send open failed: %s", exc)

        for managed in result.managed:
            if managed.get("action") not in ("stopped", "closed_time"):
                continue
            cycle_row = None
            with storage.connect() as connection:
                row = connection.execute(
                    "SELECT * FROM strategy_cycles WHERE id=?",
                    (managed["id"],),
                ).fetchone()
                cycle_row = row
            if cycle_row is None:
                continue
            try:
                tclient.send_message(_format_close(managed, cycle_row))
                sent += 1
            except Exception as exc:  # noqa: BLE001
                logger.warning("funding_carry_notify: send close failed: %s", exc)

    # Live-ready gate (one-shot)
    status = evaluate_live_ready(storage)
    alert_active = bool(state.get("live_ready_alerted_at_ms"))
    if status.ready and not alert_active and tclient is not None:
        try:
            tclient.send_message(_format_live_ready(status))
            state["live_ready_alerted_at_ms"] = result.ts_ms
            sent += 1
        except Exception as exc:  # noqa: BLE001
            logger.warning("funding_carry_notify: send live-ready failed: %s", exc)
    elif (not status.ready) and alert_active:
        # Conditions deteriorated; clear the latch so the next time it crosses we re-alert.
        state.pop("live_ready_alerted_at_ms", None)
        state["live_ready_lost_at_ms"] = result.ts_ms

    _save_state(state_path, state)

    return {
        "ts_ms": result.ts_ms,
        "opened": len(result.opened),
        "managed_terminal": sum(1 for m in result.managed if m.get("action") in ("stopped", "closed_time")),
        "telegram_sent": sent,
        "live_ready": status.ready,
        "live_ready_reasons": status.reasons,
    }


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(description="funding-carry step + telegram notifications")
    p.add_argument("--state-path", type=Path, default=default_state_path())
    args = p.parse_args(argv)
    result = run_step_and_notify(state_path=args.state_path)
    print(json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    main()
