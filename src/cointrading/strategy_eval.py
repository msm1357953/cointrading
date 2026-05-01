from __future__ import annotations

from dataclasses import dataclass
import sqlite3
from typing import Any, Iterable

from cointrading.config import TradingConfig
from cointrading.scalping import ScalpSignal
from cointrading.storage import TradingStore


MAKER_POST_ONLY = "maker_post_only"
TAKER_MOMENTUM = "taker_momentum"
HYBRID_TAKER_ENTRY_MAKER_EXIT = "hybrid_taker_entry_maker_exit"
TAKER_TREND = "taker_trend"
MAKER_RANGE = "maker_range"
TAKER_BREAKOUT = "taker_breakout"
EVALUATION_EXECUTION_MODES = (
    MAKER_POST_ONLY,
    TAKER_MOMENTUM,
    HYBRID_TAKER_ENTRY_MAKER_EXIT,
)

GRID_TAKE_PROFIT_BPS = (3.0, 5.0, 8.0, 12.0, 16.0, 20.0)
GRID_STOP_LOSS_BPS = (4.0, 6.0, 8.0, 10.0)
GRID_MAX_HOLD_SECONDS = (60, 180, 300)


@dataclass(frozen=True)
class StrategyGateDecision:
    allowed: bool
    reason: str
    evaluation_id: int | None = None
    take_profit_bps: float | None = None
    stop_loss_bps: float | None = None
    max_hold_seconds: int | None = None


def evaluate_and_store_strategy(
    store: TradingStore,
    config: TradingConfig,
) -> list[dict[str, Any]]:
    rows = [
        *evaluate_cycle_candidates(store, config),
        *evaluate_strategy_cycle_candidates(store, config),
        *evaluate_signal_grid_candidates(store, config),
    ]
    if rows:
        store.insert_strategy_evaluations(rows)
    return rows


def evaluate_strategy_cycle_candidates(
    store: TradingStore,
    config: TradingConfig,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with store.connect() as connection:
        for row in connection.execute(
            """
            SELECT strategy,
                   execution_mode,
                   symbol,
                   side,
                   take_profit_bps,
                   stop_loss_bps,
                   max_hold_seconds,
                   COUNT(*) AS sample_count,
                   SUM(CASE WHEN realized_pnl > 0 THEN 1 ELSE 0 END) AS win_count,
                   SUM(CASE WHEN realized_pnl <= 0 THEN 1 ELSE 0 END) AS loss_count,
                   AVG((realized_pnl / NULLIF(ABS(quantity) * entry_price, 0)) * 10000.0)
                       AS avg_pnl_bps,
                   SUM((realized_pnl / NULLIF(ABS(quantity) * entry_price, 0)) * 10000.0)
                       AS sum_pnl_bps,
                   AVG(
                       CASE WHEN realized_pnl > 0
                       THEN (realized_pnl / NULLIF(ABS(quantity) * entry_price, 0)) * 10000.0
                       END
                   ) AS avg_win_bps,
                   AVG(
                       CASE WHEN realized_pnl <= 0
                       THEN (realized_pnl / NULLIF(ABS(quantity) * entry_price, 0)) * 10000.0
                       END
                   ) AS avg_loss_bps
            FROM strategy_cycles
            WHERE realized_pnl IS NOT NULL
              AND status IN ('CLOSED', 'STOPPED')
            GROUP BY strategy, execution_mode, symbol, side,
                     take_profit_bps, stop_loss_bps, max_hold_seconds
            ORDER BY avg_pnl_bps DESC
            """
        ):
            rows.append(
                _evaluation_row(
                    source="strategy_cycles",
                    execution_mode=row["execution_mode"],
                    symbol=row["symbol"],
                    regime=row["strategy"],
                    side=row["side"],
                    take_profit_bps=float(row["take_profit_bps"]),
                    stop_loss_bps=float(row["stop_loss_bps"]),
                    max_hold_seconds=int(row["max_hold_seconds"]),
                    sample_count=int(row["sample_count"] or 0),
                    win_count=int(row["win_count"] or 0),
                    loss_count=int(row["loss_count"] or 0),
                    avg_pnl_bps=float(row["avg_pnl_bps"] or 0.0),
                    sum_pnl_bps=float(row["sum_pnl_bps"] or 0.0),
                    avg_win_bps=_optional_float(row["avg_win_bps"]),
                    avg_loss_bps=_optional_float(row["avg_loss_bps"]),
                    config=config,
                )
            )
    return rows


def evaluate_cycle_candidates(
    store: TradingStore,
    config: TradingConfig,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with store.connect() as connection:
        for row in connection.execute(
            """
            SELECT c.symbol,
                   COALESCE(s.regime, 'unknown') AS regime,
                   c.side,
                   COUNT(*) AS sample_count,
                   SUM(CASE WHEN c.realized_pnl > 0 THEN 1 ELSE 0 END) AS win_count,
                   SUM(CASE WHEN c.realized_pnl <= 0 THEN 1 ELSE 0 END) AS loss_count,
                   AVG((c.realized_pnl / NULLIF(c.quantity * c.entry_price, 0)) * 10000.0)
                       AS avg_pnl_bps,
                   SUM((c.realized_pnl / NULLIF(c.quantity * c.entry_price, 0)) * 10000.0)
                       AS sum_pnl_bps,
                   AVG(
                       CASE WHEN c.realized_pnl > 0
                       THEN (c.realized_pnl / NULLIF(c.quantity * c.entry_price, 0)) * 10000.0
                       END
                   ) AS avg_win_bps,
                   AVG(
                       CASE WHEN c.realized_pnl <= 0
                       THEN (c.realized_pnl / NULLIF(c.quantity * c.entry_price, 0)) * 10000.0
                       END
                   ) AS avg_loss_bps
            FROM scalp_cycles c
            LEFT JOIN signals s ON s.id = c.entry_signal_id
            WHERE c.realized_pnl IS NOT NULL
              AND c.status IN ('CLOSED', 'STOPPED')
            GROUP BY c.symbol, regime, c.side
            ORDER BY avg_pnl_bps DESC
            """
        ):
            rows.append(
                _evaluation_row(
                    source="cycles",
                    execution_mode=MAKER_POST_ONLY,
                    symbol=row["symbol"],
                    regime=row["regime"],
                    side=row["side"],
                    take_profit_bps=config.scalp_take_profit_bps,
                    stop_loss_bps=config.scalp_stop_loss_bps,
                    max_hold_seconds=int(config.scalp_max_hold_seconds),
                    sample_count=int(row["sample_count"] or 0),
                    win_count=int(row["win_count"] or 0),
                    loss_count=int(row["loss_count"] or 0),
                    avg_pnl_bps=float(row["avg_pnl_bps"] or 0.0),
                    sum_pnl_bps=float(row["sum_pnl_bps"] or 0.0),
                    avg_win_bps=_optional_float(row["avg_win_bps"]),
                    avg_loss_bps=_optional_float(row["avg_loss_bps"]),
                    config=config,
                )
            )
    return rows


def evaluate_signal_grid_candidates(
    store: TradingStore,
    config: TradingConfig,
) -> list[dict[str, Any]]:
    signals = _grid_signal_rows(store, config.scalp_symbols)
    groups: dict[tuple[str, str, str, str, float, float, int], list[float]] = {}
    for row in signals:
        for max_hold_seconds in GRID_MAX_HOLD_SECONDS:
            horizon_bps = _horizon_bps(row, max_hold_seconds)
            if horizon_bps is None:
                continue
            for execution_mode in EVALUATION_EXECUTION_MODES:
                for take_profit_bps in GRID_TAKE_PROFIT_BPS:
                    for stop_loss_bps in GRID_STOP_LOSS_BPS:
                        key = (
                            execution_mode,
                            row["symbol"],
                            row["regime"],
                            row["side"],
                            take_profit_bps,
                            stop_loss_bps,
                            max_hold_seconds,
                        )
                        groups.setdefault(key, []).append(
                            _coarse_grid_pnl_bps(
                                horizon_bps=horizon_bps,
                                take_profit_bps=take_profit_bps,
                                stop_loss_bps=stop_loss_bps,
                                maker_roundtrip_bps=float(row["maker_roundtrip_bps"]),
                                taker_roundtrip_bps=float(row["taker_roundtrip_bps"]),
                                execution_mode=execution_mode,
                                slippage_bps=config.strategy_taker_slippage_bps,
                            )
                        )

    rows: list[dict[str, Any]] = []
    for (
        execution_mode,
        symbol,
        regime,
        side,
        take_profit_bps,
        stop_loss_bps,
        max_hold_seconds,
    ), values in groups.items():
        rows.append(
            _evaluation_from_values(
                source="signal_grid",
                execution_mode=execution_mode,
                symbol=symbol,
                regime=regime,
                side=side,
                take_profit_bps=take_profit_bps,
                stop_loss_bps=stop_loss_bps,
                max_hold_seconds=max_hold_seconds,
                values=values,
                config=config,
            )
        )
    return sorted(rows, key=lambda item: (item["decision"], -item["avg_pnl_bps"]))


def strategy_gate_decision(
    store: TradingStore,
    signal: ScalpSignal,
    config: TradingConfig,
) -> StrategyGateDecision:
    if not config.strategy_gate_enabled:
        return StrategyGateDecision(
            True,
            "strategy gate disabled",
            take_profit_bps=config.scalp_take_profit_bps,
            stop_loss_bps=config.scalp_stop_loss_bps,
            max_hold_seconds=int(config.scalp_max_hold_seconds),
        )
    if signal.side not in {"long", "short"}:
        return StrategyGateDecision(False, "strategy gate: flat signal")

    sources = ("cycles", "signal_grid")
    fallback: StrategyGateDecision | None = None
    for source in sources:
        row = store.latest_strategy_evaluation(
            symbol=signal.symbol,
            regime=signal.regime,
            side=signal.side,
            take_profit_bps=config.scalp_take_profit_bps,
            stop_loss_bps=config.scalp_stop_loss_bps,
            max_hold_seconds=int(config.scalp_max_hold_seconds),
            execution_mode=config.strategy_execution_mode,
            source=source,
        )
        if row is None:
            continue
        if row["decision"] == "APPROVED":
            return _gate_decision_from_row(row, f"strategy gate approved by {source}")
        if fallback is None or row["decision"] == "SAMPLE_LOW":
            fallback = StrategyGateDecision(
                False,
                f"strategy gate {row['decision']}: {row['reason']}",
                int(row["id"]),
            )
            continue

    candidate = store.latest_strategy_candidate(
        symbol=signal.symbol,
        regime=signal.regime,
        side=signal.side,
        execution_mode=config.strategy_execution_mode,
        decision="APPROVED",
        source="signal_grid",
    )
    if candidate is not None:
        return _gate_decision_from_row(candidate, "strategy gate approved candidate")

    best = store.latest_strategy_candidate(
        symbol=signal.symbol,
        regime=signal.regime,
        side=signal.side,
        execution_mode=config.strategy_execution_mode,
        source="signal_grid",
    )
    if best is not None:
        return StrategyGateDecision(
            False,
            "strategy gate no approved candidate; best "
            f"{best['decision']} TP={float(best['take_profit_bps']):.1f} "
            f"SL={float(best['stop_loss_bps']):.1f} "
            f"H={int(best['max_hold_seconds'])}s "
            f"avg={float(best['avg_pnl_bps']):.3f}bps: {best['reason']}",
            int(best["id"]),
        )
    if fallback is not None:
        return fallback
    return StrategyGateDecision(
        False,
        "strategy gate: no evaluation for "
        f"{config.strategy_execution_mode} {signal.symbol} {signal.regime} {signal.side}",
    )


def strategy_evaluation_text(rows: Iterable[dict[str, Any]], *, limit: int = 20) -> str:
    ranked = sorted(rows, key=lambda item: (item["decision"], -item["avg_pnl_bps"]))
    if not ranked:
        return "전략 평가 결과가 없습니다."
    lines = ["전략 평가"]
    for row in ranked[:limit]:
        lines.append(
            " ".join(
                [
                    f"{row['decision']}",
                    f"{row['source']}",
                    f"{row['execution_mode']}",
                    f"{row['symbol']} {row['regime']} {row['side']}",
                    f"TP={row['take_profit_bps']:.1f}",
                    f"SL={row['stop_loss_bps']:.1f}",
                    f"H={row['max_hold_seconds']}s",
                    f"n={row['sample_count']}",
                    f"승률={row['win_rate']:.1%}",
                    f"평균={row['avg_pnl_bps']:.3f}bps",
                    f"합계={row['sum_pnl_bps']:.3f}bps",
                    f"이유={row['reason']}",
                ]
            )
        )
    return "\n".join(lines)


def _gate_decision_from_row(row: sqlite3.Row, reason: str) -> StrategyGateDecision:
    return StrategyGateDecision(
        True,
        reason,
        int(row["id"]),
        take_profit_bps=float(row["take_profit_bps"]),
        stop_loss_bps=float(row["stop_loss_bps"]),
        max_hold_seconds=int(row["max_hold_seconds"]),
    )


def _grid_signal_rows(
    store: TradingStore,
    symbols: tuple[str, ...],
) -> list[sqlite3.Row]:
    if not symbols:
        return []
    placeholders = ", ".join("?" for _ in symbols)
    with store.connect() as connection:
        return list(
            connection.execute(
                f"""
                SELECT symbol, regime, side, maker_roundtrip_bps, taker_roundtrip_bps,
                       horizon_1m_bps, horizon_3m_bps, horizon_5m_bps
                FROM signals
                WHERE symbol IN ({placeholders})
                  AND side IN ('long', 'short')
                  AND trade_allowed=1
                  AND (
                    horizon_1m_bps IS NOT NULL
                    OR horizon_3m_bps IS NOT NULL
                    OR horizon_5m_bps IS NOT NULL
                  )
                """,
                [symbol.upper() for symbol in symbols],
            )
        )


def _horizon_bps(row: sqlite3.Row, max_hold_seconds: int) -> float | None:
    if max_hold_seconds <= 60:
        value = row["horizon_1m_bps"]
    elif max_hold_seconds <= 180:
        value = row["horizon_3m_bps"]
    else:
        value = row["horizon_5m_bps"]
    return _optional_float(value)


def _coarse_grid_pnl_bps(
    *,
    horizon_bps: float,
    take_profit_bps: float,
    stop_loss_bps: float,
    maker_roundtrip_bps: float,
    taker_roundtrip_bps: float,
    execution_mode: str,
    slippage_bps: float,
) -> float:
    maker_one_way_bps = maker_roundtrip_bps / 2.0
    taker_one_way_bps = taker_roundtrip_bps / 2.0
    if execution_mode == TAKER_MOMENTUM:
        target_cost_bps = taker_roundtrip_bps + (slippage_bps * 2.0)
        stop_cost_bps = target_cost_bps
    elif execution_mode == HYBRID_TAKER_ENTRY_MAKER_EXIT:
        target_cost_bps = taker_one_way_bps + maker_one_way_bps + slippage_bps
        stop_cost_bps = taker_roundtrip_bps + (slippage_bps * 2.0)
    else:
        target_cost_bps = maker_roundtrip_bps
        stop_cost_bps = maker_one_way_bps + taker_one_way_bps
    if horizon_bps >= take_profit_bps:
        return take_profit_bps - target_cost_bps
    if horizon_bps <= -stop_loss_bps:
        return -stop_loss_bps - stop_cost_bps
    return horizon_bps - stop_cost_bps


def _evaluation_from_values(
    *,
    source: str,
    execution_mode: str,
    symbol: str,
    regime: str,
    side: str,
    take_profit_bps: float,
    stop_loss_bps: float,
    max_hold_seconds: int,
    values: list[float],
    config: TradingConfig,
) -> dict[str, Any]:
    wins = [value for value in values if value > 0]
    losses = [value for value in values if value <= 0]
    return _evaluation_row(
        source=source,
        execution_mode=execution_mode,
        symbol=symbol,
        regime=regime,
        side=side,
        take_profit_bps=take_profit_bps,
        stop_loss_bps=stop_loss_bps,
        max_hold_seconds=max_hold_seconds,
        sample_count=len(values),
        win_count=len(wins),
        loss_count=len(losses),
        avg_pnl_bps=sum(values) / len(values) if values else 0.0,
        sum_pnl_bps=sum(values),
        avg_win_bps=sum(wins) / len(wins) if wins else None,
        avg_loss_bps=sum(losses) / len(losses) if losses else None,
        config=config,
    )


def _evaluation_row(
    *,
    source: str,
    execution_mode: str,
    symbol: str,
    regime: str,
    side: str,
    take_profit_bps: float,
    stop_loss_bps: float,
    max_hold_seconds: int,
    sample_count: int,
    win_count: int,
    loss_count: int,
    avg_pnl_bps: float,
    sum_pnl_bps: float,
    avg_win_bps: float | None,
    avg_loss_bps: float | None,
    config: TradingConfig,
) -> dict[str, Any]:
    win_rate = (win_count / sample_count) if sample_count else 0.0
    decision, reason = _classify(
        sample_count=sample_count,
        win_rate=win_rate,
        avg_pnl_bps=avg_pnl_bps,
        avg_win_bps=avg_win_bps,
        avg_loss_bps=avg_loss_bps,
        config=config,
    )
    return {
        "source": source,
        "execution_mode": execution_mode,
        "symbol": symbol.upper(),
        "regime": regime,
        "side": side,
        "take_profit_bps": take_profit_bps,
        "stop_loss_bps": stop_loss_bps,
        "max_hold_seconds": max_hold_seconds,
        "sample_count": sample_count,
        "win_count": win_count,
        "loss_count": loss_count,
        "win_rate": win_rate,
        "avg_pnl_bps": avg_pnl_bps,
        "sum_pnl_bps": sum_pnl_bps,
        "avg_win_bps": avg_win_bps,
        "avg_loss_bps": avg_loss_bps,
        "decision": decision,
        "reason": reason,
    }


def _classify(
    *,
    sample_count: int,
    win_rate: float,
    avg_pnl_bps: float,
    avg_win_bps: float | None,
    avg_loss_bps: float | None,
    config: TradingConfig,
) -> tuple[str, str]:
    if sample_count >= config.strategy_early_block_samples and avg_pnl_bps < 0:
        return "BLOCKED", f"평균손익 {avg_pnl_bps:.3f}bps < 0"
    if sample_count < config.strategy_min_samples:
        return "SAMPLE_LOW", f"표본 {sample_count} < {config.strategy_min_samples}"
    if avg_pnl_bps <= config.strategy_min_expectancy_bps:
        return "BLOCKED", f"기대값 {avg_pnl_bps:.3f}bps <= {config.strategy_min_expectancy_bps:.3f}bps"
    if avg_win_bps and avg_loss_bps and avg_loss_bps < 0:
        required_win_rate = max(
            config.strategy_min_win_rate,
            abs(avg_loss_bps) / (avg_win_bps + abs(avg_loss_bps)),
        )
        if win_rate < required_win_rate:
            return "BLOCKED", f"승률 {win_rate:.1%} < 손익비 필요승률 {required_win_rate:.1%}"
        loss_win_ratio = abs(avg_loss_bps) / avg_win_bps
        if loss_win_ratio > config.strategy_max_loss_win_ratio:
            return "BLOCKED", f"손실/이익폭 {loss_win_ratio:.2f} > {config.strategy_max_loss_win_ratio:.2f}"
    elif win_rate < config.strategy_min_win_rate:
        return "BLOCKED", f"승률 {win_rate:.1%} < 하한 {config.strategy_min_win_rate:.1%}"
    return "APPROVED", "기대값/손익비 기준 통과"


def _optional_float(value: Any) -> float | None:
    if value is None:
        return None
    return float(value)
