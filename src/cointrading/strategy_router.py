from __future__ import annotations

from dataclasses import dataclass
import json
from typing import Any, Iterable

from cointrading.indicators import TechnicalSnapshot, build_technical_snapshot
from cointrading.market_regime import (
    MACRO_BEAR,
    MACRO_BREAKOUT,
    MACRO_BULL,
    MACRO_PANIC,
    MACRO_RANGE,
    macro_regime_ko,
    trade_bias_ko,
)
from cointrading.models import Kline, SignalSide
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
    klines_5m: list[Kline] | None = None,
    klines_15m: list[Kline] | None = None,
    current_ms: int | None = None,
) -> list[StrategySetup]:
    ts = current_ms or now_ms()
    macro = _macro_context(macro_row, macro_max_age_ms=macro_max_age_ms, current_ms=ts)
    trend_snapshot = build_technical_snapshot(klines_15m or [], interval="15m")
    tactical_klines = klines_5m or klines_15m or []
    tactical_interval = "5m" if klines_5m else "15m"
    tactical_snapshot = build_technical_snapshot(tactical_klines, interval=tactical_interval)
    return [
        _maker_scalp_setup(scalp_signal, macro, runtime_risk),
        _trend_setup(macro, runtime_risk, trend_snapshot),
        _range_setup(macro, runtime_risk, tactical_snapshot),
        _breakout_setup(macro, runtime_risk, tactical_snapshot),
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
        live = "live 엔진 준비" if setup.live_supported else "관찰/페이퍼"
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
            "전략별 상태머신은 준비되어도 조건이 PASS일 때만 진입합니다."
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


def _trend_setup(
    macro: dict[str, Any],
    runtime_risk: RuntimeRiskSnapshot,
    technical: TechnicalSnapshot,
) -> StrategySetup:
    if not runtime_risk.allows_new_entries:
        return _macro_block("trend_follow", "15m-4h", runtime_risk)
    if macro["stale"]:
        return StrategySetup(
            "trend_follow",
            "taker_trend",
            SETUP_BLOCK,
            "flat",
            "15m-4h",
            True,
            "장세 데이터가 없거나 오래되어 추세 판단을 보류합니다.",
        )
    regime = macro["macro_regime"]
    side = macro["trade_bias"]
    if regime not in {MACRO_BULL, MACRO_BEAR}:
        if regime == MACRO_BREAKOUT:
            return StrategySetup(
                "trend_follow",
                "taker_trend",
                SETUP_WATCH,
                "flat",
                "15m-4h",
                True,
                "돌파장은 축소 돌파 전략에서 따로 판단합니다.",
            )
        if regime == MACRO_PANIC:
            return StrategySetup(
                "trend_follow",
                "taker_trend",
                SETUP_BLOCK,
                "flat",
                "15m-4h",
                True,
                "패닉 변동성이라 추세 신규 진입도 막습니다.",
            )
        return StrategySetup(
            "trend_follow",
            "taker_trend",
            SETUP_BLOCK,
            "flat",
            "15m-4h",
            True,
            f"{macro_regime_ko(regime)}라 추세 추종 우위가 약합니다.",
        )
    if not technical.enough:
        return StrategySetup(
            "trend_follow",
            "taker_trend",
            SETUP_WATCH,
            "flat",
            "15m-4h",
            True,
            "15분봉 RSI/EMA 표본이 부족해 추세 진입을 보류합니다.",
        )
    if regime in {MACRO_BULL, MACRO_BEAR} and side in {"long", "short"}:
        allowed, reason = _trend_confirmed(side, technical)
        return StrategySetup(
            "trend_follow",
            "taker_trend",
            SETUP_PASS if allowed else SETUP_WATCH,
            side if allowed else "flat",
            "15m-4h",
            True,
            reason,
        )
    return StrategySetup(
        "trend_follow",
        "taker_trend",
        SETUP_BLOCK,
        "flat",
        "15m-4h",
        True,
        "장세 편향이 중립이라 추세 진입을 보류합니다.",
    )


def _range_setup(
    macro: dict[str, Any],
    runtime_risk: RuntimeRiskSnapshot,
    technical: TechnicalSnapshot,
) -> StrategySetup:
    if not runtime_risk.allows_new_entries:
        return _macro_block("range_reversion", "5m-1h", runtime_risk)
    if macro["stale"]:
        return StrategySetup(
            "range_reversion",
            "maker_range",
            SETUP_BLOCK,
            "flat",
            "5m-1h",
            True,
            "장세 데이터가 없거나 오래되어 레인지 판단을 보류합니다.",
        )
    regime = macro["macro_regime"]
    if regime == MACRO_RANGE:
        if not technical.enough:
            return StrategySetup(
                "range_reversion",
                "maker_range",
                SETUP_WATCH,
                "flat",
                "5m-1h",
                True,
                "5분봉 RSI/볼린저 표본이 부족해 평균회귀 진입을 보류합니다.",
            )
        range_side, range_reason = _range_side_from_technical(technical)
        if range_side in {"long", "short"}:
            return StrategySetup(
                "range_reversion",
                "maker_range",
                SETUP_PASS,
                range_side,
                "5m-1h",
                True,
                range_reason,
            )
        return StrategySetup(
            "range_reversion",
            "maker_range",
            SETUP_WATCH,
            "flat",
            "5m-1h",
            True,
            range_reason or "횡보장이지만 가격이 밴드 중앙부라 평균회귀 진입은 대기합니다.",
        )
    return StrategySetup(
        "range_reversion",
        "maker_range",
        SETUP_BLOCK,
        "flat",
        "5m-1h",
        True,
        f"{macro_regime_ko(regime)}라 평균회귀 우위가 약합니다.",
    )


def _breakout_setup(
    macro: dict[str, Any],
    runtime_risk: RuntimeRiskSnapshot,
    technical: TechnicalSnapshot,
) -> StrategySetup:
    if not runtime_risk.allows_new_entries:
        return _macro_block("breakout_reduced", "5m-1h", runtime_risk)
    if macro["stale"]:
        return StrategySetup(
            "breakout_reduced",
            "taker_breakout",
            SETUP_BLOCK,
            "flat",
            "5m-1h",
            True,
            "장세 데이터가 없거나 오래되어 돌파 판단을 보류합니다.",
        )
    regime = macro["macro_regime"]
    side = macro["trade_bias"]
    if regime == MACRO_BREAKOUT and side in {"long", "short"}:
        if not technical.enough:
            return StrategySetup(
                "breakout_reduced",
                "taker_breakout",
                SETUP_WATCH,
                "flat",
                "5m-1h",
                True,
                "5분봉 RSI/ATR/돌파 표본이 부족해 돌파 진입을 보류합니다.",
            )
        allowed, reason = _breakout_confirmed(side, technical)
        return StrategySetup(
            "breakout_reduced",
            "taker_breakout",
            SETUP_PASS if allowed else SETUP_WATCH,
            side if allowed else "flat",
            "5m-1h",
            True,
            reason,
        )
    return StrategySetup(
        "breakout_reduced",
        "taker_breakout",
        SETUP_BLOCK,
        "flat",
        "5m-1h",
        True,
        f"{macro_regime_ko(regime)}라 돌파장 조건이 아닙니다.",
    )


def _macro_block(strategy: str, horizon: str, runtime_risk: RuntimeRiskSnapshot) -> StrategySetup:
    return StrategySetup(
        strategy,
        _execution_mode_for_strategy(strategy),
        SETUP_BLOCK,
        "flat",
        horizon,
        True,
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


def _range_side_from_bands(klines: list[Kline]) -> tuple[SignalSide, str]:
    rows = klines[-24:]
    if len(rows) < 12:
        return "flat", "레인지 밴드를 계산할 15분봉 표본이 부족합니다."
    highs = [row.high for row in rows]
    lows = [row.low for row in rows]
    upper = max(highs)
    lower = min(lows)
    last = rows[-1].close
    width = upper - lower
    if lower <= 0 or width <= 0:
        return "flat", "레인지 폭이 너무 좁거나 가격 데이터가 이상합니다."
    position = (last - lower) / width
    if position <= 0.25:
        return "long", f"레인지 하단권 position={position:.2f}; 중앙 복귀 롱 후보입니다."
    if position >= 0.75:
        return "short", f"레인지 상단권 position={position:.2f}; 중앙 복귀 숏 후보입니다."
    return "flat", f"레인지 중앙부 position={position:.2f}; 평균회귀 진입 대기입니다."


def _trend_confirmed(side: str, technical: TechnicalSnapshot) -> tuple[bool, str]:
    rsi_value = technical.rsi14 or 50.0
    if side == "long":
        allowed = (
            technical.ema_gap_bps >= 2.0
            and technical.ema_slope_bps >= -5.0
            and technical.ema_slow is not None
            and technical.close >= technical.ema_slow
            and 45.0 <= rsi_value <= 82.0
        )
        reason = (
            f"추세 롱 규칙: EMA20>EMA60, 종가가 EMA60 위, RSI 45~82 필요. "
            f"{technical.short_text()}"
        )
        return allowed, reason if allowed else f"추세 롱 대기: {reason}"
    allowed = (
        technical.ema_gap_bps <= -2.0
        and technical.ema_slope_bps <= 5.0
        and technical.ema_slow is not None
        and technical.close <= technical.ema_slow
        and 18.0 <= rsi_value <= 55.0
    )
    reason = (
        f"추세 숏 규칙: EMA20<EMA60, 종가가 EMA60 아래, RSI 18~55 필요. "
        f"{technical.short_text()}"
    )
    return allowed, reason if allowed else f"추세 숏 대기: {reason}"


def _range_side_from_technical(technical: TechnicalSnapshot) -> tuple[SignalSide, str]:
    rsi_value = technical.rsi14 or 50.0
    position = technical.bollinger_position
    if position is None:
        return "flat", f"볼린저 위치 계산 불가. {technical.short_text()}"
    if position <= 0.22 and rsi_value <= 42.0:
        return (
            "long",
            "평균회귀 롱 규칙: 볼린저 하단권 + RSI 과매도. "
            f"{technical.short_text()}",
        )
    if position >= 0.78 and rsi_value >= 58.0:
        return (
            "short",
            "평균회귀 숏 규칙: 볼린저 상단권 + RSI 과매수. "
            f"{technical.short_text()}",
        )
    return "flat", f"횡보장이지만 RSI/볼린저 위치가 진입권이 아닙니다. {technical.short_text()}"


def _breakout_confirmed(side: str, technical: TechnicalSnapshot) -> tuple[bool, str]:
    rsi_value = technical.rsi14 or 50.0
    if side == "long":
        allowed = technical.high_breakout and rsi_value >= 55.0 and technical.volume_ratio >= 1.10
        reason = (
            "축소 돌파 롱 규칙: 최근 20봉 고점 종가돌파 + RSI 55 이상 + 거래량 1.10배 이상. "
            f"{technical.short_text()}"
        )
        return allowed, reason if allowed else f"돌파 롱 대기: {reason}"
    allowed = technical.low_breakout and rsi_value <= 45.0 and technical.volume_ratio >= 1.10
    reason = (
        "축소 돌파 숏 규칙: 최근 20봉 저점 종가이탈 + RSI 45 이하 + 거래량 1.10배 이상. "
        f"{technical.short_text()}"
    )
    return allowed, reason if allowed else f"돌파 숏 대기: {reason}"


def _execution_mode_for_strategy(strategy: str) -> str:
    return {
        "trend_follow": "taker_trend",
        "range_reversion": "maker_range",
        "breakout_reduced": "taker_breakout",
    }.get(strategy, "paper_macro")


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
