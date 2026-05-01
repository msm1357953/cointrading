from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

from cointrading.config import TradingConfig
from cointrading.storage import KST, TradingStore, kst_from_ms, now_ms


TERMINAL_STATUSES = ("CLOSED", "STOPPED", "CANCELLED")
EXECUTION_MODE_STRATEGIES = {
    "maker_post_only": "maker_scalp",
    "taker_trend": "trend_follow",
    "maker_range": "range_reversion",
    "taker_breakout": "breakout_reduced",
}


@dataclass(frozen=True)
class ExecutionGateDecision:
    allowed: bool
    reason: str


def evaluate_simple_strategy_gate(
    store: TradingStore,
    config: TradingConfig,
    setup: Any,
    *,
    symbol: str,
    dry_run: bool,
    timestamp_ms: int | None = None,
) -> ExecutionGateDecision:
    if not config.simple_trade_gate_enabled:
        return ExecutionGateDecision(True, "simple trade gate disabled")
    if dry_run and not config.simple_trade_gate_apply_to_dry_run:
        return ExecutionGateDecision(True, "simple trade gate live-only")

    ts = timestamp_ms or now_ms()
    strategy = str(_row_value(setup, "strategy", ""))
    allowed = tuple(item.strip().lower() for item in config.simple_trade_gate_allowed_strategies if item.strip())
    if allowed and strategy not in allowed:
        return ExecutionGateDecision(
            False,
            "simple gate: 허용 전략은 "
            f"{', '.join(allowed)}뿐입니다. 현재 전략={strategy}.",
        )

    loss_limit = int(config.simple_trade_gate_max_consecutive_losses)
    if loss_limit > 0:
        losses = _consecutive_strategy_losses(store, dry_run=dry_run, limit=loss_limit)
        if losses >= loss_limit:
            return ExecutionGateDecision(
                False,
                f"simple gate: 최근 {losses}회 연속 손실이라 신규 진입을 중단합니다.",
            )

    daily_limit = int(config.simple_trade_gate_daily_entry_limit)
    if daily_limit > 0:
        count = _daily_strategy_entry_count(store, timestamp_ms=ts, dry_run=dry_run)
        if count >= daily_limit:
            return ExecutionGateDecision(
                False,
                f"simple gate: 오늘 전략 진입 {count}회로 하루 한도 {daily_limit}회에 도달했습니다.",
            )

    cooldown_ms = max(0, int(config.simple_trade_gate_cooldown_minutes)) * 60_000
    if cooldown_ms > 0:
        recent = _latest_terminal_strategy_cycle(store, symbol=symbol, dry_run=dry_run)
        if recent is not None:
            closed_ms = int(recent["closed_ms"] or recent["updated_ms"])
            remaining_ms = cooldown_ms - (ts - closed_ms)
            if remaining_ms > 0:
                return ExecutionGateDecision(
                    False,
                    "simple gate: "
                    f"{symbol.upper()} 마지막 종료 {kst_from_ms(closed_ms)} 이후 쿨다운 중 "
                    f"({remaining_ms // 60_000 + 1}분 남음).",
                )

    return ExecutionGateDecision(True, "simple trade gate passed")


def simple_strategy_gate_summary(config: TradingConfig) -> str:
    if not config.simple_trade_gate_enabled:
        return "단순 실행게이트 OFF"
    scope = "paper+live" if config.simple_trade_gate_apply_to_dry_run else "live 전용"
    allowed = ", ".join(config.simple_trade_gate_allowed_strategies) or "전체"
    return (
        "단순 실행게이트 ON "
        f"({scope}, 전략={allowed}, "
        f"쿨다운={config.simple_trade_gate_cooldown_minutes}분, "
        f"일한도={config.simple_trade_gate_daily_entry_limit}회, "
        f"연손실정지={config.simple_trade_gate_max_consecutive_losses}회)"
    )


def strategy_name_from_execution_mode(execution_mode: str) -> str:
    return EXECUTION_MODE_STRATEGIES.get(execution_mode, execution_mode)


def _daily_strategy_entry_count(
    store: TradingStore,
    *,
    timestamp_ms: int,
    dry_run: bool,
) -> int:
    start_ms, end_ms = _kst_day_bounds_ms(timestamp_ms)
    with store.connect() as connection:
        row = connection.execute(
            """
            SELECT COUNT(*) AS count
            FROM strategy_cycles
            WHERE dry_run=?
              AND created_ms>=?
              AND created_ms<?
            """,
            (1 if dry_run else 0, start_ms, end_ms),
        ).fetchone()
    return int(row["count"] or 0)


def _latest_terminal_strategy_cycle(
    store: TradingStore,
    *,
    symbol: str,
    dry_run: bool,
):
    with store.connect() as connection:
        return connection.execute(
            f"""
            SELECT *
            FROM strategy_cycles
            WHERE dry_run=?
              AND symbol=?
              AND status IN ({", ".join("?" for _ in TERMINAL_STATUSES)})
            ORDER BY COALESCE(closed_ms, updated_ms) DESC
            LIMIT 1
            """,
            (1 if dry_run else 0, symbol.upper(), *TERMINAL_STATUSES),
        ).fetchone()


def _consecutive_strategy_losses(
    store: TradingStore,
    *,
    dry_run: bool,
    limit: int,
) -> int:
    with store.connect() as connection:
        rows = list(
            connection.execute(
                """
                SELECT realized_pnl
                FROM strategy_cycles
                WHERE dry_run=?
                  AND realized_pnl IS NOT NULL
                  AND status IN ('CLOSED', 'STOPPED')
                ORDER BY COALESCE(closed_ms, updated_ms) DESC
                LIMIT ?
                """,
                (1 if dry_run else 0, limit),
            )
        )
    losses = 0
    for row in rows:
        if float(row["realized_pnl"]) <= 0:
            losses += 1
            continue
        break
    return losses


def _kst_day_bounds_ms(timestamp_ms: int) -> tuple[int, int]:
    current = datetime.fromtimestamp(timestamp_ms / 1000, KST)
    start = current.replace(hour=0, minute=0, second=0, microsecond=0)
    end = start + timedelta(days=1)
    return int(start.timestamp() * 1000), int(end.timestamp() * 1000)


def _row_value(row: Any, key: str, default: Any = None) -> Any:
    if isinstance(row, dict):
        return row.get(key, default)
    try:
        return row[key]
    except (KeyError, IndexError, TypeError):
        return getattr(row, key, default)
