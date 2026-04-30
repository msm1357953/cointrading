from __future__ import annotations

from dataclasses import dataclass
import sqlite3
from typing import Any, Iterable

from cointrading.config import TradingConfig
from cointrading.scalping import ScalpSignal
from cointrading.storage import TradingStore


GRID_TAKE_PROFIT_BPS = (3.0, 5.0, 8.0)
GRID_STOP_LOSS_BPS = (4.0, 6.0, 8.0, 10.0)
GRID_MAX_HOLD_SECONDS = (60, 180, 300)


@dataclass(frozen=True)
class StrategyGateDecision:
    allowed: bool
    reason: str
    evaluation_id: int | None = None


def evaluate_and_store_strategy(
    store: TradingStore,
    config: TradingConfig,
) -> list[dict[str, Any]]:
    rows = [
        *evaluate_cycle_candidates(store, config),
        *evaluate_signal_grid_candidates(store, config),
    ]
    if rows:
        store.insert_strategy_evaluations(rows)
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
    groups: dict[tuple[str, str, str, float, float, int], list[float]] = {}
    for row in signals:
        for max_hold_seconds in GRID_MAX_HOLD_SECONDS:
            horizon_bps = _horizon_bps(row, max_hold_seconds)
            if horizon_bps is None:
                continue
            for take_profit_bps in GRID_TAKE_PROFIT_BPS:
                for stop_loss_bps in GRID_STOP_LOSS_BPS:
                    key = (
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
                        )
                    )

    rows: list[dict[str, Any]] = []
    for (
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
        return StrategyGateDecision(True, "strategy gate disabled")
    if signal.side not in {"long", "short"}:
        return StrategyGateDecision(False, "strategy gate: flat signal")

    sources = ("cycles", "signal_grid")
    sample_low: StrategyGateDecision | None = None
    for source in sources:
        row = store.latest_strategy_evaluation(
            symbol=signal.symbol,
            regime=signal.regime,
            side=signal.side,
            take_profit_bps=config.scalp_take_profit_bps,
            stop_loss_bps=config.scalp_stop_loss_bps,
            max_hold_seconds=int(config.scalp_max_hold_seconds),
            source=source,
        )
        if row is None:
            continue
        if row["decision"] == "APPROVED":
            return StrategyGateDecision(
                True,
                f"strategy gate approved by {source}",
                int(row["id"]),
            )
        if row["decision"] == "SAMPLE_LOW":
            sample_low = StrategyGateDecision(
                False,
                f"strategy gate {row['decision']}: {row['reason']}",
                int(row["id"]),
            )
            continue
        return StrategyGateDecision(
            False,
            f"strategy gate {row['decision']}: {row['reason']}",
            int(row["id"]),
        )
    if sample_low is not None:
        return sample_low
    return StrategyGateDecision(
        False,
        f"strategy gate: no evaluation for {signal.symbol} {signal.regime} {signal.side}",
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
) -> float:
    mixed_exit_cost_bps = (maker_roundtrip_bps / 2.0) + (taker_roundtrip_bps / 2.0)
    if horizon_bps >= take_profit_bps:
        return take_profit_bps - maker_roundtrip_bps
    if horizon_bps <= -stop_loss_bps:
        return -stop_loss_bps - mixed_exit_cost_bps
    return horizon_bps - mixed_exit_cost_bps


def _evaluation_from_values(
    *,
    source: str,
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
    if win_rate < config.strategy_min_win_rate:
        return "BLOCKED", f"승률 {win_rate:.1%} < {config.strategy_min_win_rate:.1%}"
    if avg_win_bps and avg_loss_bps and avg_loss_bps < 0:
        loss_win_ratio = abs(avg_loss_bps) / avg_win_bps
        if loss_win_ratio > config.strategy_max_loss_win_ratio:
            return "BLOCKED", f"손실/이익폭 {loss_win_ratio:.2f} > {config.strategy_max_loss_win_ratio:.2f}"
    return "APPROVED", "평가 기준 통과"


def _optional_float(value: Any) -> float | None:
    if value is None:
        return None
    return float(value)
