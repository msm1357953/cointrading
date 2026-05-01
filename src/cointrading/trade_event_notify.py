from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
from typing import Any, Iterable

from cointrading.storage import TradingStore, kst_from_ms, now_ms
from cointrading.strategy_notify import (
    MODE_LABELS,
    REASON_LABELS,
    SIDE_LABELS,
    STATUS_LABELS,
    STRATEGY_LABELS,
)


TERMINAL_EVENT_TYPES = {"take_profit", "stop_loss", "max_hold_exit", "cancelled", "requoted", "stopped", "closed"}


@dataclass(frozen=True)
class TradeEvent:
    source: str
    cycle_id: int
    event_type: str
    event_ms: int
    key: str
    row: Any


@dataclass
class TradeEventNotifyState:
    notified_keys: tuple[str, ...] = ()
    last_summary_ms: int = 0
    initialized_ms: int = 0

    @classmethod
    def load(cls, path: Path, *, timestamp_ms: int | None = None) -> "TradeEventNotifyState":
        ts = timestamp_ms or now_ms()
        if not path.exists():
            return cls(initialized_ms=ts)
        payload = json.loads(path.read_text())
        return cls(
            notified_keys=tuple(str(item) for item in payload.get("notified_keys", [])),
            last_summary_ms=int(payload.get("last_summary_ms", 0)),
            initialized_ms=int(payload.get("initialized_ms", ts)),
        )

    def save(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            json.dumps(
                {
                    "notified_keys": list(self.notified_keys),
                    "last_summary_ms": self.last_summary_ms,
                    "initialized_ms": self.initialized_ms,
                },
                indent=2,
                sort_keys=True,
            )
        )


def default_trade_event_notify_state_path() -> Path:
    return Path(__file__).resolve().parents[2] / "data" / "trade_event_notify_state.json"


def trade_event_notification_decision(
    store: TradingStore,
    state: TradeEventNotifyState,
    *,
    summary_interval_minutes: int,
    force_summary: bool = False,
    timestamp_ms: int | None = None,
    event_limit: int = 200,
    startup_lookback_minutes: int = 10,
) -> tuple[list[TradeEvent], bool]:
    ts = timestamp_ms or now_ms()
    notified = set(state.notified_keys)
    startup_floor = max(0, state.initialized_ms - startup_lookback_minutes * 60_000)
    events: list[TradeEvent] = []
    for source, row in _recent_cycle_rows(store, event_limit):
        event_type = _cycle_event_type(row)
        if event_type is None:
            continue
        event_ms = _cycle_event_ms(row, event_type)
        if event_ms < startup_floor:
            continue
        key = f"{source}:{int(_row_value(row, 'id', 0))}:{event_type}"
        if key in notified:
            continue
        events.append(
            TradeEvent(
                source=source,
                cycle_id=int(_row_value(row, "id", 0)),
                event_type=event_type,
                event_ms=event_ms,
                key=key,
                row=row,
            )
        )
    events.sort(key=lambda item: item.event_ms)
    summary_due = force_summary or (
        summary_interval_minutes > 0
        and (state.last_summary_ms <= 0 or ts - state.last_summary_ms >= summary_interval_minutes * 60_000)
    )
    return events, summary_due


def apply_trade_event_notification_state(
    state: TradeEventNotifyState,
    events: Iterable[TradeEvent],
    *,
    summary_sent: bool,
    timestamp_ms: int | None = None,
    max_keys: int = 1000,
) -> TradeEventNotifyState:
    keys = [*state.notified_keys]
    existing = set(keys)
    for event in events:
        if event.key not in existing:
            keys.append(event.key)
            existing.add(event.key)
    if len(keys) > max_keys:
        keys = keys[-max_keys:]
    state.notified_keys = tuple(keys)
    if summary_sent:
        state.last_summary_ms = timestamp_ms or now_ms()
    return state


def trade_event_notification_text(
    events: Iterable[TradeEvent],
    store: TradingStore,
    *,
    include_summary: bool,
    timestamp_ms: int | None = None,
    event_limit: int = 10,
) -> str:
    ts = timestamp_ms or now_ms()
    events = list(events)
    sections: list[str] = []
    if events:
        lines = ["거래 이벤트", f"확인시각: {kst_from_ms(ts)}"]
        for event in events[:event_limit]:
            lines.extend(_event_lines(event))
        hidden = len(events) - event_limit
        if hidden > 0:
            lines.append(f"- 추가 이벤트 {hidden}개는 대시보드 Paper 탭에서 확인")
        sections.append("\n".join(lines))
    if include_summary:
        sections.append(trade_summary_text(store, timestamp_ms=ts))
    if not sections:
        return "거래 이벤트: 발송할 새 내용이 없습니다."
    return "\n\n".join(sections)


def trade_summary_text(
    store: TradingStore,
    *,
    timestamp_ms: int | None = None,
    active_limit: int = 6,
    recent_limit: int = 200,
) -> str:
    ts = timestamp_ms or now_ms()
    active_rows = _active_cycle_rows(store)
    recent_rows = list(_recent_cycle_rows(store, recent_limit))
    since_ms = ts - 24 * 60 * 60 * 1000
    terminal_rows = []
    for source, row in recent_rows:
        event_type = _cycle_event_type(row)
        if event_type in TERMINAL_EVENT_TYPES and _cycle_event_ms(row, event_type or "") >= since_ms:
            terminal_rows.append((source, row))
    realized = sum(_float_value(row, "realized_pnl") or 0.0 for _, row in terminal_rows)
    take_profit = sum(1 for _, row in terminal_rows if _row_value(row, "reason") == "take_profit")
    stop_loss = sum(1 for _, row in terminal_rows if _row_value(row, "reason") == "stop_loss")
    max_hold = sum(1 for _, row in terminal_rows if _row_value(row, "reason") == "max_hold_exit")
    cancelled = sum(1 for _, row in terminal_rows if str(_row_value(row, "status", "")) in {"CANCELLED", "REQUOTE"})
    unrealized = sum(_unrealized_pnl(row) or 0.0 for _, row in active_rows)

    lines = [
        "상황 보고",
        f"보고시각: {kst_from_ms(ts)}",
        f"현재 진행 중: {len(active_rows)}개 / 추정 미실현손익 {_format_signed(unrealized)} USDC",
        (
            "최근 24시간 종료: "
            f"익절 {take_profit} / 손절 {stop_loss} / 시간종료 {max_hold} / 진입취소 {cancelled} "
            f"/ 실현손익 {_format_signed(realized)} USDC"
        ),
    ]
    if active_rows:
        lines.append("진행 중 상세")
        for source, row in active_rows[:active_limit]:
            lines.append(_active_line(source, row))
        hidden = len(active_rows) - active_limit
        if hidden > 0:
            lines.append(f"- 추가 진행 중 {hidden}개는 대시보드 Paper 탭에서 확인")
    else:
        lines.append("진행 중 상세: 현재 진입 대기/보유/청산 대기 사이클이 없습니다.")
    return "\n".join(lines)


def _recent_cycle_rows(store: TradingStore, limit: int) -> list[tuple[str, Any]]:
    rows: list[tuple[str, Any]] = []
    rows.extend(("strategy", row) for row in store.recent_strategy_cycles(limit))
    rows.extend(("scalp", row) for row in store.recent_scalp_cycles(limit))
    rows.sort(key=lambda item: int(_row_value(item[1], "updated_ms", 0)), reverse=True)
    return rows[: limit * 2]


def _active_cycle_rows(store: TradingStore) -> list[tuple[str, Any]]:
    rows: list[tuple[str, Any]] = []
    rows.extend(("strategy", row) for row in store.active_strategy_cycles())
    rows.extend(("scalp", row) for row in store.active_scalp_cycles())
    rows.sort(key=lambda item: int(_row_value(item[1], "updated_ms", 0)), reverse=True)
    return rows


def _cycle_event_type(row: Any) -> str | None:
    status = str(_row_value(row, "status", "") or "")
    reason = str(_row_value(row, "reason", "") or "")
    opened_ms = _row_value(row, "opened_ms")
    if status == "ENTRY_SUBMITTED":
        return "entry_submitted"
    if status in {"OPEN", "EXIT_SUBMITTED"} and opened_ms is not None:
        return "entry_open"
    if status == "CLOSED":
        if reason == "take_profit":
            return "take_profit"
        return "closed"
    if status == "STOPPED":
        if reason == "stop_loss":
            return "stop_loss"
        if reason == "max_hold_exit":
            return "max_hold_exit"
        return "stopped"
    if status == "CANCELLED":
        return "cancelled"
    if status == "REQUOTE":
        return "requoted"
    return None


def _cycle_event_ms(row: Any, event_type: str) -> int:
    if event_type == "entry_open":
        return int(_row_value(row, "opened_ms") or _row_value(row, "updated_ms", 0))
    if event_type in TERMINAL_EVENT_TYPES:
        return int(_row_value(row, "closed_ms") or _row_value(row, "updated_ms", 0))
    return int(_row_value(row, "updated_ms", 0))


def _event_lines(event: TradeEvent) -> list[str]:
    row = event.row
    label = _event_label(event.event_type)
    status = _status_label(str(_row_value(row, "status", "")))
    reason = _reason_label(str(_row_value(row, "reason", "") or ""))
    symbol = str(_row_value(row, "symbol", ""))
    side = SIDE_LABELS.get(str(_row_value(row, "side", "")), str(_row_value(row, "side", "")))
    lines = [
        "",
        f"- {label}: {_strategy_label(event.source, row)} {symbol} {side}",
        f"  시각: {kst_from_ms(event.event_ms)} / 상태: {status}",
    ]
    if reason:
        lines.append(f"  사유: {reason}")
    lines.append(
        "  가격: "
        f"진입 {_format_price(_float_value(row, 'entry_price'))} / "
        f"현재 {_format_price(_float_value(row, 'last_mid_price'))} / "
        f"목표 {_format_price(_float_value(row, 'target_price'))} / "
        f"손절 {_format_price(_float_value(row, 'stop_price'))}"
    )
    pnl = _float_value(row, "realized_pnl")
    if pnl is not None:
        lines.append(f"  실현손익: {_format_signed(pnl)} USDC")
    else:
        unrealized = _unrealized_pnl(row)
        if unrealized is not None:
            lines.append(f"  추정 미실현손익: {_format_signed(unrealized)} USDC")
    return lines


def _active_line(source: str, row: Any) -> str:
    symbol = str(_row_value(row, "symbol", ""))
    side = SIDE_LABELS.get(str(_row_value(row, "side", "")), str(_row_value(row, "side", "")))
    status = _status_label(str(_row_value(row, "status", "")))
    unrealized = _unrealized_pnl(row)
    pnl = "계산불가" if unrealized is None else f"{_format_signed(unrealized)} USDC"
    return (
        f"- {_strategy_label(source, row)} {symbol} {side} / {status} / "
        f"진입 {_format_price(_float_value(row, 'entry_price'))} / "
        f"현재 {_format_price(_float_value(row, 'last_mid_price'))} / "
        f"목표 {_format_price(_float_value(row, 'target_price'))} / "
        f"손절 {_format_price(_float_value(row, 'stop_price'))} / "
        f"미실현 {pnl}"
    )


def _strategy_label(source: str, row: Any) -> str:
    if source == "scalp":
        return "메이커 스캘핑"
    strategy = str(_row_value(row, "strategy", "") or "")
    execution_mode = str(_row_value(row, "execution_mode", "") or "")
    strategy_label = STRATEGY_LABELS.get(strategy, strategy)
    mode_label = MODE_LABELS.get(execution_mode, execution_mode)
    if mode_label:
        return f"{strategy_label}({mode_label})"
    return strategy_label


def _event_label(event_type: str) -> str:
    return {
        "entry_submitted": "진입 시도",
        "entry_open": "진입 체결",
        "take_profit": "익절",
        "stop_loss": "손절",
        "max_hold_exit": "시간 종료",
        "cancelled": "진입 취소",
        "requoted": "진입 보류",
        "closed": "종료",
        "stopped": "중단",
    }.get(event_type, event_type)


def _status_label(status: str) -> str:
    return STATUS_LABELS.get(status, status)


def _reason_label(reason: str) -> str:
    return REASON_LABELS.get(reason, reason)


def _unrealized_pnl(row: Any) -> float | None:
    entry = _float_value(row, "entry_price")
    current = _float_value(row, "last_mid_price")
    quantity = _float_value(row, "quantity")
    side = str(_row_value(row, "side", "") or "")
    if entry is None or current is None or quantity is None:
        return None
    raw = (current - entry) * quantity
    if side == "short":
        raw *= -1
    return raw


def _float_value(row: Any, key: str) -> float | None:
    value = _row_value(row, key)
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _row_value(row: Any, key: str, default: Any = None) -> Any:
    try:
        keys = row.keys()
    except AttributeError:
        return row.get(key, default)
    if key not in keys:
        return default
    return row[key]


def _format_price(value: float | None) -> str:
    if value is None:
        return "-"
    formatted = f"{value:.8f}".rstrip("0").rstrip(".")
    return formatted or "0"


def _format_signed(value: float) -> str:
    return f"{value:+.6f}"
