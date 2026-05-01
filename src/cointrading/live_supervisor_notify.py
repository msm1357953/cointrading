from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
from typing import Iterable

from cointrading.storage import kst_from_ms, now_ms
from cointrading.symbol_supervisor import SupervisorReport, supervisor_report_text


SAFETY_LOCK_PREFIXES = (
    "dry-run이 켜져 있어",
    "live trading 플래그가 꺼져 있습니다.",
    "live 상태머신 플래그가 꺼져 있습니다.",
    "원샷 live 허가가 꺼져 있습니다.",
)


@dataclass
class LiveSupervisorNotifyState:
    last_signature: str = ""
    last_sent_ms: int = 0

    @classmethod
    def load(cls, path: Path) -> "LiveSupervisorNotifyState":
        if not path.exists():
            return cls()
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            return cls()
        return cls(
            last_signature=str(payload.get("last_signature", "")),
            last_sent_ms=int(payload.get("last_sent_ms", 0) or 0),
        )

    def save(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            json.dumps(
                {
                    "last_signature": self.last_signature,
                    "last_sent_ms": self.last_sent_ms,
                },
                ensure_ascii=False,
                sort_keys=True,
            ),
            encoding="utf-8",
        )


def default_live_supervisor_notify_state_path() -> Path:
    return Path(__file__).resolve().parents[2] / "data" / "live_supervisor_notify_state.json"


def actionable_supervisor_reports(reports: Iterable[SupervisorReport]) -> list[SupervisorReport]:
    actionable: list[SupervisorReport] = []
    for report in reports:
        if report.best_candidate is None or report.warnings:
            continue
        if _non_safety_reasons(report):
            continue
        actionable.append(report)
    return actionable


def supervisor_candidate_signature(reports: Iterable[SupervisorReport]) -> str:
    parts = [_report_signature(report) for report in actionable_supervisor_reports(reports)]
    return "\n".join(sorted(parts))


def supervisor_candidate_notification_decision(
    reports: Iterable[SupervisorReport],
    state: LiveSupervisorNotifyState,
    *,
    force: bool = False,
) -> tuple[bool, str, str, list[SupervisorReport]]:
    report_list = list(reports)
    actionable = actionable_supervisor_reports(report_list)
    signature = "\n".join(sorted(_report_signature(report) for report in actionable))
    if force:
        return True, "수동 확인", signature, actionable
    if signature == state.last_signature:
        return False, "변화 없음", signature, actionable
    if actionable:
        return True, "진입 후보 감지", signature, actionable
    if state.last_signature:
        return True, "진입 후보 해제", signature, actionable
    return False, "후보 없음", signature, actionable


def supervisor_candidate_notification_text(
    actionable_reports: Iterable[SupervisorReport],
    *,
    reason: str,
    notional: float,
) -> str:
    reports = list(actionable_reports)
    if not reports:
        title = "실전 진입 후보 해제" if reason == "진입 후보 해제" else "실전 진입 후보 없음"
        return "\n".join(
            [
                title,
                f"사유: {reason}",
                f"확인시각: {kst_from_ms(now_ms())}",
                "현재 안전잠금만 남은 후보가 없습니다.",
                "주문은 실행되지 않았습니다.",
            ]
        )
    return "\n\n".join(
        [
            "\n".join(
                [
                    "실전 진입 후보 감지",
                    f"사유: {reason}",
                    f"확인시각: {kst_from_ms(now_ms())}",
                    f"점검규모: {notional:.2f} USDC",
                    "주문상태: 실행 안 함. dry-run/live/one-shot 안전잠금 유지.",
                    "다음 행동: 텔레그램에서 '실전 80'으로 재확인 후 수동 승인.",
                ]
            ),
            supervisor_report_text(reports),
        ]
    )


def apply_live_supervisor_notify_state(
    state: LiveSupervisorNotifyState,
    *,
    signature: str,
    timestamp_ms: int | None = None,
) -> LiveSupervisorNotifyState:
    state.last_signature = signature
    state.last_sent_ms = timestamp_ms or now_ms()
    return state


def _non_safety_reasons(report: SupervisorReport) -> list[str]:
    return [reason for reason in report.reasons if not _is_safety_lock_reason(reason)]


def _is_safety_lock_reason(reason: str) -> bool:
    return any(reason.startswith(prefix) for prefix in SAFETY_LOCK_PREFIXES)


def _report_signature(report: SupervisorReport) -> str:
    candidate = report.best_candidate or {}
    return "|".join(
        [
            report.symbol,
            str(candidate.get("execution_mode", "")),
            str(candidate.get("regime", "")),
            str(candidate.get("side", "")),
            str(candidate.get("take_profit_bps", "")),
            str(candidate.get("stop_loss_bps", "")),
            str(candidate.get("max_hold_seconds", "")),
        ]
    )
