from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import dataclass
import hashlib
import json
from pathlib import Path
from typing import Iterable

from cointrading.storage import kst_from_ms, now_ms


@dataclass
class StrategyNotifyState:
    last_signature: str = ""
    last_periodic_ms: int = 0

    @classmethod
    def load(cls, path: Path) -> "StrategyNotifyState":
        if not path.exists():
            return cls()
        payload = json.loads(path.read_text())
        return cls(
            last_signature=str(payload.get("last_signature", "")),
            last_periodic_ms=int(payload.get("last_periodic_ms", 0)),
        )

    def save(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            json.dumps(
                {
                    "last_signature": self.last_signature,
                    "last_periodic_ms": self.last_periodic_ms,
                },
                indent=2,
                sort_keys=True,
            )
        )


def default_strategy_notify_state_path() -> Path:
    return Path(__file__).resolve().parents[2] / "data" / "strategy_notify_state.json"


def strategy_notification_decision(
    rows: Iterable,
    state: StrategyNotifyState,
    *,
    periodic_minutes: int,
    force: bool = False,
    timestamp_ms: int | None = None,
) -> tuple[bool, str, str]:
    rows = list(rows)
    ts = timestamp_ms or now_ms()
    signature = strategy_signature(rows)
    if not rows and not force:
        return False, "평가 결과 없음", signature
    changed = signature != state.last_signature
    periodic_due = (
        periodic_minutes > 0
        and (state.last_periodic_ms <= 0 or ts - state.last_periodic_ms >= periodic_minutes * 60_000)
    )
    if force:
        reason = "수동 발송"
    elif changed:
        reason = "전략 판정 변화"
    elif periodic_due:
        reason = "주기 보고"
    else:
        return False, "발송 조건 없음", signature
    return True, reason, signature


def apply_strategy_notification_state(
    state: StrategyNotifyState,
    *,
    signature: str,
    reason: str,
    timestamp_ms: int | None = None,
) -> StrategyNotifyState:
    ts = timestamp_ms or now_ms()
    state.last_signature = signature
    state.last_periodic_ms = ts
    return state


def strategy_signature(rows: Iterable) -> str:
    parts = []
    for row in rows:
        parts.append(
            "|".join(
                [
                    str(row["source"]),
                    str(row["execution_mode"]),
                    str(row["symbol"]),
                    str(row["regime"]),
                    str(row["side"]),
                    f"{float(row['take_profit_bps']):.3f}",
                    f"{float(row['stop_loss_bps']):.3f}",
                    str(int(row["max_hold_seconds"])),
                    str(row["decision"]),
                ]
            )
        )
    digest = "\n".join(sorted(parts))
    return hashlib.sha256(digest.encode("utf-8")).hexdigest()


def strategy_notification_text(rows: Iterable, *, reason: str, limit: int = 8) -> str:
    rows = list(rows)
    if not rows:
        return f"전략 평가 알림\n사유: {reason}\n평가 결과가 아직 없습니다."

    evaluated_ms = max(int(row["evaluated_ms"]) for row in rows)
    decision_counts = Counter(str(row["decision"]) for row in rows)
    mode_counts: dict[str, Counter[str]] = defaultdict(Counter)
    for row in rows:
        mode_counts[str(row["execution_mode"])][str(row["decision"])] += 1

    approved = [row for row in rows if row["decision"] == "APPROVED"]
    best_rows = sorted(
        rows,
        key=lambda row: (
            _decision_rank(str(row["decision"])),
            -float(row["avg_pnl_bps"]),
            -int(row["sample_count"]),
        ),
    )[:limit]

    lines = [
        "전략 평가 알림",
        f"사유: {reason}",
        f"평가시각: {kst_from_ms(evaluated_ms)}",
        (
            "요약: "
            f"승인 {decision_counts.get('APPROVED', 0)}개, "
            f"차단 {decision_counts.get('BLOCKED', 0)}개, "
            f"표본부족 {decision_counts.get('SAMPLE_LOW', 0)}개"
        ),
    ]
    lines.append("실행방식별")
    for mode in sorted(mode_counts):
        counter = mode_counts[mode]
        lines.append(
            f"- {mode}: 승인 {counter.get('APPROVED', 0)}, "
            f"차단 {counter.get('BLOCKED', 0)}, 표본부족 {counter.get('SAMPLE_LOW', 0)}"
        )

    if approved:
        lines.append("승인 후보")
        for row in approved[:limit]:
            lines.append(_row_line(row))
    else:
        lines.append("승인 후보: 없음. 신규 진입은 계속 차단하는 쪽이 맞습니다.")

    lines.append("상위 후보")
    for row in best_rows:
        lines.append(_row_line(row))
    return "\n".join(lines)


def _decision_rank(decision: str) -> int:
    return {"APPROVED": 0, "SAMPLE_LOW": 1, "BLOCKED": 2}.get(decision, 3)


def _row_line(row) -> str:
    return (
        f"- {row['decision']} {row['execution_mode']} {row['symbol']} "
        f"{row['regime']} {row['side']} TP={float(row['take_profit_bps']):.1f} "
        f"SL={float(row['stop_loss_bps']):.1f} H={int(row['max_hold_seconds'])}s "
        f"n={int(row['sample_count'])} 승률={float(row['win_rate']):.1%} "
        f"평균={float(row['avg_pnl_bps']):.3f}bps"
    )
