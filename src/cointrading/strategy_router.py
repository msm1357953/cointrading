from __future__ import annotations

from dataclasses import dataclass
import json
from typing import Any, Iterable

from cointrading.market_regime import (
    MACRO_BEAR,
    MACRO_BREAKOUT,
    MACRO_BULL,
    MACRO_PANIC,
    MACRO_RANGE,
    macro_regime_ko,
    trade_bias_ko,
)
from cointrading.models import SignalSide
from cointrading.risk_state import RuntimeRiskSnapshot, risk_mode_ko
from cointrading.scalping import ScalpSignal
from cointrading.storage import now_ms


SETUP_PASS = "PASS"
SETUP_WATCH = "WATCH"
SETUP_BLOCK = "BLOCK"


@dataclass(frozen=True)
class StrategySetup:
    strategy: str
    execution_mode: str
    status: str
    side: SignalSide
    horizon: str
    live_supported: bool
    reason: str


def evaluate_strategy_setups(
    *,
    scalp_signal: ScalpSignal,
    macro_row: Any | None,
    runtime_risk: RuntimeRiskSnapshot,
    macro_max_age_ms: int,
    current_ms: int | None = None,
) -> list[StrategySetup]:
    ts = current_ms or now_ms()
    macro = _macro_context(macro_row, macro_max_age_ms=macro_max_age_ms, current_ms=ts)
    return [
        _maker_scalp_setup(scalp_signal, macro, runtime_risk),
        _trend_setup(macro, runtime_risk),
        _range_setup(macro, runtime_risk),
        _breakout_setup(macro, runtime_risk),
    ]


def strategy_setups_text(
    setups: Iterable[StrategySetup],
    *,
    symbol: str,
    notional: float | None = None,
    runtime_risk: RuntimeRiskSnapshot | None = None,
) -> str:
    lines = [f"진입 점검: {symbol.upper()}"]
    if notional is not None:
        lines.append(f"요청 규모: {notional:.2f}")
    if runtime_risk is not None:
        lines.append(
            "위험모드: "
            f"{risk_mode_ko(runtime_risk.mode)} / "
            f"신규 {'허용' if runtime_risk.allows_new_entries else '차단'}"
        )
    lines.append("전략별 판단")
    for setup in setups:
        live = "live 가능" if setup.live_supported else "관찰/페이퍼"
        lines.append(
            "- "
            f"{_strategy_ko(setup.strategy)} "
            f"{_status_ko(setup.status)} "
            f"방향={_side_ko(setup.side)} "
            f"기간={setup.horizon} "
            f"실행={setup.execution_mode} "
            f"({live}) - {setup.reason}"
        )
    supported = [item for item in setups if item.live_supported and item.status == SETUP_PASS]
    if supported:
        first = supported[0]
        lines.append(
            "실전 엔진 결론: "
            f"{_strategy_ko(first.strategy)} {first.side}만 주문 후보입니다. "
            "최종 주문 전 exchange filter와 live flag를 다시 봅니다."
        )
    else:
        lines.append(
            "실전 엔진 결론: 지금 자동 주문 후보 없음. "
            "비-스캘핑 후보는 아직 관찰/페이퍼 판단만 합니다."
        )
    return "\n".join(lines)


def has_live_supported_pass(setups: Iterable[StrategySetup]) -> bool:
    return any(setup.live_supported and setup.status == SETUP_PASS for setup in setups)


def _maker_scalp_setup(
    signal: ScalpSignal,
    macro: dict[str, Any],
    runtime_risk: RuntimeRiskSnapshot,
) -> StrategySetup:
    if not runtime_risk.allows_new_entries:
        return StrategySetup(
            "maker_scalp",
            "maker_post_only",
            SETUP_BLOCK,
            "flat",
            "seconds",
            True,
            f"런타임 위험모드 {risk_mode_ko(runtime_risk.mode)}가 신규 진입을 막습니다.",
        )
    if signal.side not in {"long", "short"}:
        return StrategySetup(
            "maker_scalp",
            "maker_post_only",
            SETUP_BLOCK,
            "flat",
            "seconds",
            True,
            _scalp_block_reason(signal),
        )
    allowed, reason = _scalp_allowed_by_macro_context(macro, signal.side)
    if not allowed:
        return StrategySetup(
            "maker_scalp",
            "maker_post_only",
            SETUP_BLOCK,
            signal.side,
            "seconds",
            True,
            reason,
        )
    if not signal.trade_allowed:
        return StrategySetup(
            "maker_scalp",
            "maker_post_only",
            SETUP_BLOCK,
            signal.side,
            "seconds",
            True,
            _scalp_block_reason(signal),
        )
    return StrategySetup(
        "maker_scalp",
        "maker_post_only",
        SETUP_PASS,
        signal.side,
        "seconds",
        True,
        "스프레드/호가/모멘텀/수수료 조건이 스캘핑 기준을 통과했습니다.",
    )


def _trend_setup(macro: dict[str, Any], runtime_risk: RuntimeRiskSnapshot) -> StrategySetup:
    if not runtime_risk.allows_new_entries:
        return _macro_block("trend_follow", "15m-4h", runtime_risk)
    if macro["stale"]:
        return StrategySetup(
            "trend_follow",
            "paper_macro",
            SETUP_BLOCK,
            "flat",
            "15m-4h",
            False,
            "장세 데이터가 없거나 오래되어 추세 판단을 보류합니다.",
        )
    regime = macro["macro_regime"]
    side = macro["trade_bias"]
    if regime in {MACRO_BULL, MACRO_BEAR} and side in {"long", "short"}:
        return StrategySetup(
            "trend_follow",
            "paper_macro",
            SETUP_WATCH,
            side,
            "15m-4h",
            False,
            f"{macro_regime_ko(regime)} / {trade_bias_ko(side)}. thin book은 이 판단의 단독 차단 사유가 아닙니다.",
        )
    if regime == MACRO_BREAKOUT and side in {"long", "short"}:
        return StrategySetup(
            "trend_follow",
            "paper_macro",
            SETUP_WATCH,
            side,
            "15m-4h",
            False,
            "변동성 돌파장은 추세 후보지만 소액 축소와 별도 돌파 엔진이 필요합니다.",
        )
    if regime == MACRO_PANIC:
        return StrategySetup(
            "trend_follow",
            "paper_macro",
            SETUP_BLOCK,
            "flat",
            "15m-4h",
            False,
            "패닉 변동성이라 추세 신규 진입도 막습니다.",
        )
    return StrategySetup(
        "trend_follow",
        "paper_macro",
        SETUP_BLOCK,
        "flat",
        "15m-4h",
        False,
        f"{macro_regime_ko(regime)}라 추세 추종 우위가 약합니다.",
    )


def _range_setup(macro: dict[str, Any], runtime_risk: RuntimeRiskSnapshot) -> StrategySetup:
    if not runtime_risk.allows_new_entries:
        return _macro_block("range_reversion", "5m-1h", runtime_risk)
    if macro["stale"]:
        return StrategySetup(
            "range_reversion",
            "paper_macro",
            SETUP_BLOCK,
            "flat",
            "5m-1h",
            False,
            "장세 데이터가 없거나 오래되어 레인지 판단을 보류합니다.",
        )
    regime = macro["macro_regime"]
    if regime == MACRO_RANGE:
        return StrategySetup(
            "range_reversion",
            "paper_macro",
            SETUP_WATCH,
            "flat",
            "5m-1h",
            False,
            "횡보장 후보입니다. 상단/하단 밴드와 손절폭이 붙기 전에는 자동 주문하지 않습니다.",
        )
    return StrategySetup(
        "range_reversion",
        "paper_macro",
        SETUP_BLOCK,
        "flat",
        "5m-1h",
        False,
        f"{macro_regime_ko(regime)}라 평균회귀 우위가 약합니다.",
    )


def _breakout_setup(macro: dict[str, Any], runtime_risk: RuntimeRiskSnapshot) -> StrategySetup:
    if not runtime_risk.allows_new_entries:
        return _macro_block("breakout_reduced", "5m-1h", runtime_risk)
    if macro["stale"]:
        return StrategySetup(
            "breakout_reduced",
            "paper_macro",
            SETUP_BLOCK,
            "flat",
            "5m-1h",
            False,
            "장세 데이터가 없거나 오래되어 돌파 판단을 보류합니다.",
        )
    regime = macro["macro_regime"]
    side = macro["trade_bias"]
    if regime == MACRO_BREAKOUT and side in {"long", "short"}:
        return StrategySetup(
            "breakout_reduced",
            "paper_macro",
            SETUP_WATCH,
            side,
            "5m-1h",
            False,
            "돌파 후보입니다. 테이커/하이브리드 엔진과 넓은 손절 검증 전에는 live 주문하지 않습니다.",
        )
    return StrategySetup(
        "breakout_reduced",
        "paper_macro",
        SETUP_BLOCK,
        "flat",
        "5m-1h",
        False,
        f"{macro_regime_ko(regime)}라 돌파장 조건이 아닙니다.",
    )


def _macro_block(strategy: str, horizon: str, runtime_risk: RuntimeRiskSnapshot) -> StrategySetup:
    return StrategySetup(
        strategy,
        "paper_macro",
        SETUP_BLOCK,
        "flat",
        horizon,
        False,
        f"런타임 위험모드 {risk_mode_ko(runtime_risk.mode)}가 신규 진입을 막습니다.",
    )


def _macro_context(row: Any | None, *, macro_max_age_ms: int, current_ms: int) -> dict[str, Any]:
    if row is None:
        return {
            "macro_regime": "",
            "trade_bias": "neutral",
            "allowed_strategies": (),
            "stale": True,
        }
    ts = int(_row_value(row, "timestamp_ms", 0) or 0)
    allowed_raw = _row_value(row, "allowed_strategies_json", "[]")
    try:
        allowed = tuple(json.loads(allowed_raw or "[]"))
    except (TypeError, json.JSONDecodeError):
        allowed = tuple()
    return {
        "macro_regime": str(_row_value(row, "macro_regime", "")),
        "trade_bias": str(_row_value(row, "trade_bias", "neutral")),
        "allowed_strategies": allowed,
        "stale": ts <= 0 or current_ms - ts > macro_max_age_ms,
    }


def _scalp_allowed_by_macro_context(macro: dict[str, Any], side: str) -> tuple[bool, str]:
    if macro["stale"]:
        return True, "macro regime unavailable or stale"
    regime = str(macro["macro_regime"])
    if regime == MACRO_PANIC:
        return False, "macro router: panic regime blocks new scalps"
    if regime == MACRO_BREAKOUT:
        return False, "macro router: breakout regime routes away from scalping"
    if regime == MACRO_BULL and side == "short":
        return False, "macro router: bull regime blocks short scalps"
    if regime == MACRO_BEAR and side == "long":
        return False, "macro router: bear regime blocks long scalps"
    return True, f"macro router allows {side} in {regime or 'unknown'}"


def _row_value(row: Any, key: str, default: Any = None) -> Any:
    if isinstance(row, dict):
        return row.get(key, default)
    try:
        return row[key]
    except (KeyError, IndexError, TypeError):
        return getattr(row, key, default)


def _scalp_block_reason(signal: ScalpSignal) -> str:
    if signal.regime == "thin_book" or signal.reason == "book depth too thin":
        return (
            "호가 상단 유동성이 얇아서 메이커 스캘핑만 차단합니다. "
            "추세/스윙 판단은 장세 라우터에서 별도로 봅니다."
        )
    if signal.reason:
        return f"스캘핑 조건 미충족: {signal.reason}"
    return "스캘핑 조건 미충족"


def _strategy_ko(strategy: str) -> str:
    return {
        "maker_scalp": "메이커 스캘핑",
        "trend_follow": "추세 추종",
        "range_reversion": "레인지 평균회귀",
        "breakout_reduced": "축소 돌파",
    }.get(strategy, strategy)


def _status_ko(status: str) -> str:
    return {
        SETUP_PASS: "통과",
        SETUP_WATCH: "관찰",
        SETUP_BLOCK: "차단",
    }.get(status, status)


def _side_ko(side: SignalSide) -> str:
    return {
        "long": "롱",
        "short": "숏",
        "flat": "대기",
    }[side]
