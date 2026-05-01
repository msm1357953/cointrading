from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import dataclass
import hashlib
import json
from pathlib import Path
from typing import TYPE_CHECKING, Any, Iterable

from cointrading.storage import kst_from_ms, now_ms

if TYPE_CHECKING:
    from cointrading.config import TradingConfig


MODE_LABELS = {
    "maker_post_only": "지정가 메이커",
    "taker_momentum": "시장가/테이커 추세",
    "hybrid_taker_entry_maker_exit": "테이커 진입+메이커 청산",
}
SOURCE_LABELS = {
    "signal_grid": "초단기 신호 로그",
    "cycles": "상태머신 결과",
}
REGIME_LABELS = {
    "aligned_long": "상승 정렬",
    "aligned_short": "하락 정렬",
    "quiet_chop": "방향성 약함",
    "thin_book": "호가 얇음",
    "panic_volatility": "급변동",
    "invalid_spread": "스프레드 이상",
}
SIDE_LABELS = {
    "long": "롱",
    "short": "숏",
    "flat": "대기",
}
STRATEGY_LABELS = {
    "trend_follow": "추세 추종",
    "range_reversion": "레인지 평균회귀",
    "breakout_reduced": "축소 돌파",
    "maker_scalp": "메이커 스캘핑",
}
STATUS_LABELS = {
    "ENTRY_SUBMITTED": "진입 대기",
    "OPEN": "보유 중",
    "EXIT_SUBMITTED": "청산 대기",
    "CLOSED": "익절 종료",
    "STOPPED": "종료",
}
REASON_LABELS = {
    "strategy exit waiting": "목표가/손절/시간제한 청산 대기",
    "entry waiting": "진입 체결 대기",
    "not filled yet": "아직 미체결",
    "entry filled; strategy position open": "진입 체결 후 보유 중",
    "live strategy position open": "실전 포지션 보유 중",
    "take_profit": "목표가 도달",
    "stop_loss": "손절 조건",
    "max_hold_exit": "최대 보유시간 도달",
}


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


def strategy_notification_text(
    rows: Iterable,
    *,
    reason: str,
    limit: int = 8,
    config: "TradingConfig | None" = None,
    active_strategy_cycles: Iterable | None = None,
) -> str:
    rows = list(rows)
    active_strategy_cycles = list(active_strategy_cycles or [])
    if not rows:
        lines = [
            "전략 후보 평가(신호 로그 기반)",
            f"사유: {reason}",
            "평가 결과가 아직 없습니다.",
        ]
        lines.extend(_safety_lines(config))
        lines.extend(_active_cycle_lines(active_strategy_cycles, limit=limit))
        return "\n".join(lines)

    evaluated_ms = max(int(row["evaluated_ms"]) for row in rows)
    decision_counts = Counter(str(row["decision"]) for row in rows)
    mode_counts: dict[str, Counter[str]] = defaultdict(Counter)
    source_counts: Counter[str] = Counter()
    for row in rows:
        mode_counts[str(row["execution_mode"])][str(row["decision"])] += 1
        source_counts[str(row["source"])] += 1

    approved = sorted(
        [row for row in rows if row["decision"] == "APPROVED"],
        key=lambda row: (-float(row["avg_pnl_bps"]), -int(row["sample_count"])),
    )
    approved_groups = _best_unique_rows(approved)
    reason_counts = Counter(str(row["reason"]) for row in rows if row["decision"] != "APPROVED")

    lines = [
        "전략 후보 평가(신호 로그 기반)",
        f"사유: {reason}",
        f"평가시각: {kst_from_ms(evaluated_ms)}",
        "범위: 신호 로그 기반 후보평가입니다. 실제 주문/포지션 보고가 아닙니다.",
    ]
    lines.extend(_safety_lines(config))
    lines.append(_summary_sentence(decision_counts, mode_counts))
    lines.append(
        "집계: "
        f"승인 {decision_counts.get('APPROVED', 0)}개, "
        f"차단 {decision_counts.get('BLOCKED', 0)}개, "
        f"표본부족 {decision_counts.get('SAMPLE_LOW', 0)}개"
    )
    lines.append(
        "데이터: "
        + ", ".join(
            f"{_source_label(source)} {count}개" for source, count in source_counts.most_common()
        )
    )
    lines.append("실행방식별")
    for mode in sorted(mode_counts):
        lines.append(_mode_summary_line(mode, mode_counts[mode]))

    if approved_groups:
        lines.append("핵심 승인 후보")
        for row, variants in approved_groups[:limit]:
            lines.append(_row_line(row, variants=variants))
        hidden = len(approved) - sum(variants for _, variants in approved_groups[:limit])
        if hidden > 0:
            lines.append(f"- 나머지 승인 파라미터 {hidden}개는 대시보드 전략후보 탭에서 확인")
    else:
        lines.append("핵심 승인 후보: 없음. 신규 진입은 계속 차단하는 쪽이 맞습니다.")

    if reason_counts:
        lines.append("차단 이유 TOP")
        for reason_text, count in reason_counts.most_common(3):
            lines.append(f"- {count}개: {_short_reason(reason_text)}")

    lines.extend(_active_cycle_lines(active_strategy_cycles, limit=limit))
    lines.append("다음 확인: '장세', '진입 ETHUSDC 25', '포지션'을 같이 봐야 실제 진입 판단이 됩니다.")
    return "\n".join(lines)


def _safety_lines(config: "TradingConfig | None") -> list[str]:
    if config is None:
        return []
    live_open = (
        not config.dry_run
        and config.live_trading_enabled
        and config.live_strategy_lifecycle_enabled
    )
    if live_open:
        live_text = "실전 주문 가능 플래그가 켜져 있습니다. 진입 전 프리플라이트 확인 필요."
    else:
        live_text = "실전 주문 잠김"
    return [
        (
            "안전상태: "
            f"dry-run {'ON' if config.dry_run else 'OFF'}, "
            f"live {'ON' if config.live_trading_enabled else 'OFF'}, "
            f"전략 live {'ON' if config.live_strategy_lifecycle_enabled else 'OFF'}"
        ),
        f"실전주문: {live_text}",
    ]


def _summary_sentence(
    decision_counts: Counter[str],
    mode_counts: dict[str, Counter[str]],
) -> str:
    approved_count = decision_counts.get("APPROVED", 0)
    approved_modes = [
        mode for mode, counter in mode_counts.items() if counter.get("APPROVED", 0) > 0
    ]
    if approved_count <= 0:
        return "결론: 지금 승인 후보는 없습니다. 관찰만 하는 구간입니다."
    if approved_modes == ["maker_post_only"]:
        return "결론: 승인 후보는 지정가 메이커뿐입니다. 테이커/하이브리드는 아직 차단입니다."
    labels = ", ".join(_mode_label(mode) for mode in sorted(approved_modes))
    return f"결론: 승인 후보가 {approved_count}개 있습니다. 허용 실행방식: {labels}."


def _mode_summary_line(mode: str, counter: Counter[str]) -> str:
    return (
        f"- {_mode_label(mode)}: "
        f"승인 {counter.get('APPROVED', 0)}, "
        f"차단 {counter.get('BLOCKED', 0)}, "
        f"표본부족 {counter.get('SAMPLE_LOW', 0)}"
    )


def _best_unique_rows(rows: list) -> list[tuple[Any, int]]:
    grouped: dict[tuple[str, str, str, str], list] = defaultdict(list)
    for row in rows:
        grouped[
            (
                str(row["execution_mode"]),
                str(row["symbol"]),
                str(row["regime"]),
                str(row["side"]),
            )
        ].append(row)
    best: list[tuple[Any, int]] = []
    for group_rows in grouped.values():
        ranked = sorted(
            group_rows,
            key=lambda row: (-float(row["avg_pnl_bps"]), -int(row["sample_count"])),
        )
        best.append((ranked[0], len(group_rows)))
    return sorted(
        best,
        key=lambda item: (-float(item[0]["avg_pnl_bps"]), -int(item[0]["sample_count"])),
    )


def _active_cycle_lines(cycles: list, *, limit: int) -> list[str]:
    lines = ["현재 전략 상태머신"]
    if not cycles:
        lines.append("- 진행 중 없음")
        return lines
    for cycle in cycles[:limit]:
        pnl = ""
        if cycle["realized_pnl"] is not None:
            pnl = f" 손익={float(cycle['realized_pnl']):.6f}"
        reason = str(cycle["reason"] or "")
        lines.append(
            f"- {_strategy_label(cycle['strategy'])} {cycle['symbol']} "
            f"{_side_label(cycle['side'])} {_status_label(cycle['status'])} "
            f"{_reason_label(reason)}{pnl}"
        )
    return lines


def _row_line(row, *, variants: int = 1) -> str:
    variant_text = f" / 파라미터 {variants}개" if variants > 1 else ""
    return (
        f"- {row['symbol']} {_side_label(row['side'])} / {_regime_label(row['regime'])} / "
        f"{_mode_label(row['execution_mode'])}{variant_text}: "
        f"TP {float(row['take_profit_bps']):.1f}bps, "
        f"SL {float(row['stop_loss_bps']):.1f}bps, "
        f"보유 {int(row['max_hold_seconds'])}s, "
        f"표본 {int(row['sample_count'])}, 승률 {float(row['win_rate']):.1%}, "
        f"평균 {float(row['avg_pnl_bps']):+.3f}bps"
    )


def _source_label(source: str) -> str:
    return SOURCE_LABELS.get(source, source)


def _mode_label(mode: str) -> str:
    return MODE_LABELS.get(mode, mode)


def _regime_label(regime: str) -> str:
    return REGIME_LABELS.get(regime, regime)


def _side_label(side: str) -> str:
    return SIDE_LABELS.get(side, side)


def _strategy_label(strategy: str) -> str:
    return STRATEGY_LABELS.get(strategy, strategy)


def _status_label(status: str) -> str:
    return STATUS_LABELS.get(status, status)


def _reason_label(reason: str) -> str:
    return REASON_LABELS.get(reason, reason)


def _short_reason(reason: str) -> str:
    reason = reason.strip()
    if len(reason) <= 100:
        return reason
    return f"{reason[:97]}..."
