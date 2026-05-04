from __future__ import annotations

from dataclasses import asdict, dataclass
import json
import math
from pathlib import Path
from typing import Any, Iterable

from cointrading.config import TradingConfig
from cointrading.exchange.binance_usdm import BinanceAPIError, BinanceUSDMClient
from cointrading.indicators import TechnicalSnapshot, build_technical_snapshot
from cointrading.models import Kline
from cointrading.storage import kst_from_ms, now_ms


RADAR_READY = "READY"
RADAR_NEAR = "NEAR"
RADAR_WATCH = "WATCH"
RADAR_AVOID = "AVOID"


@dataclass(frozen=True)
class TacticalRadarSignal:
    symbol: str
    decision: str
    scenario: str
    side: str
    current_price: float
    trigger_price: float | None
    stop_price: float | None
    target_price: float | None
    confidence: float
    reason: str
    detail: str
    timestamp_ms: int
    change_2h_bps: float
    pullback_bps: float
    volume_ratio: float
    rsi14: float | None
    bollinger_position: float | None


@dataclass(frozen=True)
class TacticalRadarNotifyState:
    last_signature: str = ""
    last_sent_ms: int = 0

    @classmethod
    def load(cls, path: Path) -> "TacticalRadarNotifyState":
        if not path.exists():
            return cls()
        payload = json.loads(path.read_text(encoding="utf-8"))
        return cls(
            last_signature=str(payload.get("last_signature", "")),
            last_sent_ms=int(payload.get("last_sent_ms", 0) or 0),
        )

    def save(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(asdict(self), ensure_ascii=False, indent=2), encoding="utf-8")


def default_tactical_radar_report_path() -> Path:
    return Path(__file__).resolve().parents[2] / "data" / "tactical_radar_latest.json"


def default_tactical_radar_notify_state_path() -> Path:
    return Path(__file__).resolve().parents[2] / "data" / "tactical_radar_notify_state.json"


def evaluate_tactical_radar(
    client: BinanceUSDMClient,
    *,
    config: TradingConfig,
    symbols: Iterable[str] | None = None,
) -> tuple[list[TacticalRadarSignal], tuple[str, ...]]:
    output: list[TacticalRadarSignal] = []
    warnings: list[str] = []
    active_symbols = [symbol.upper() for symbol in (symbols or config.scalp_symbols)]
    ts = now_ms()
    for symbol in active_symbols:
        try:
            klines_15m = client.klines(symbol=symbol, interval="15m", limit=120)
            klines_1h = client.klines(symbol=symbol, interval="1h", limit=120)
            klines_4h = client.klines(symbol=symbol, interval="4h", limit=120)
        except BinanceAPIError as exc:
            warnings.append(f"{symbol} 캔들 조회 실패: {exc}")
            continue
        signal = evaluate_tactical_symbol(symbol, klines_15m, klines_1h, klines_4h, timestamp_ms=ts)
        output.append(signal)
    return sorted(output, key=_radar_rank_key), tuple(warnings)


def evaluate_tactical_symbol(
    symbol: str,
    klines_15m: list[Kline],
    klines_1h: list[Kline],
    klines_4h: list[Kline] | None = None,
    *,
    timestamp_ms: int | None = None,
) -> TacticalRadarSignal:
    ts = timestamp_ms or now_ms()
    if len(klines_15m) < 80 or len(klines_1h) < 80:
        current = klines_15m[-1].close if klines_15m else 0.0
        return TacticalRadarSignal(
            symbol=symbol,
            decision=RADAR_AVOID,
            scenario="data_low",
            side="flat",
            current_price=current,
            trigger_price=None,
            stop_price=None,
            target_price=None,
            confidence=0.0,
            reason="캔들 표본 부족",
            detail="15m/1h 기술지표를 산출할 표본이 부족합니다.",
            timestamp_ms=ts,
            change_2h_bps=0.0,
            pullback_bps=0.0,
            volume_ratio=0.0,
            rsi14=None,
            bollinger_position=None,
        )

    snap15 = build_technical_snapshot(klines_15m, interval="15m")
    snap1h = build_technical_snapshot(klines_1h, interval="1h")
    closes = [row.close for row in klines_15m]
    current = closes[-1]
    previous = closes[-2]
    change_2h = _return_bps(closes, bars=8)
    recent_high = max(row.high for row in klines_15m[-21:-1])
    recent_low = min(row.low for row in klines_15m[-21:-1])
    pullback_from_high = ((recent_high / current) - 1.0) * 10_000.0 if current > 0 else 0.0
    bounce_from_low = ((current / recent_low) - 1.0) * 10_000.0 if recent_low > 0 else 0.0
    uptrend = _is_uptrend(snap1h)
    downtrend = _is_downtrend(snap1h)
    rsi = snap15.rsi14
    bb = snap15.bollinger_position
    reclaimed_fast = snap15.ema_fast is not None and current >= snap15.ema_fast and current > previous
    rejected_fast = snap15.ema_fast is not None and current <= snap15.ema_fast and current < previous
    impulse_up = (
        uptrend
        and (change_2h >= 35.0 or snap15.high_breakout)
        and snap15.volume_ratio >= 1.15
    )
    impulse_down = (
        downtrend
        and (change_2h <= -35.0 or snap15.low_breakout)
        and snap15.volume_ratio >= 1.15
    )
    overextended_up = (
        uptrend
        and change_2h >= 30.0
        and (
            (rsi is not None and rsi >= 68.0)
            or (bb is not None and bb >= 0.85)
        )
    )
    overextended_down = (
        downtrend
        and change_2h <= -30.0
        and (
            (rsi is not None and rsi <= 32.0)
            or (bb is not None and bb <= 0.15)
        )
    )

    major_signal = _major_level_signal(
        symbol=symbol,
        klines_15m=klines_15m,
        klines_4h=klines_4h or [],
        snap15=snap15,
        snap1h=snap1h,
        current=current,
        previous=previous,
        recent_high=recent_high,
        recent_low=recent_low,
        timestamp_ms=ts,
        change_2h_bps=change_2h,
        pullback_from_high=pullback_from_high,
        bounce_from_low=bounce_from_low,
    )
    if major_signal is not None:
        return major_signal

    failed_breakout = _failed_upside_breakout(klines_15m, recent_high, snap15)
    if failed_breakout:
        return _signal(
            symbol=symbol,
            decision=RADAR_NEAR,
            scenario="failed_breakout_short",
            side="short",
            current=current,
            trigger=current,
            stop=recent_high * 1.0015,
            target=max(current * 0.996, recent_low),
            confidence=0.55,
            reason="상방 돌파 실패 숏 후보",
            detail=(
                "역추세라 실전 진입은 엄격해야 합니다. 고점 돌파 실패와 되밀림은 보이지만 "
                "추세가 강하면 숏은 짧게만 봐야 합니다."
            ),
            snap15=snap15,
            timestamp_ms=ts,
            change_2h_bps=change_2h,
            pullback_bps=pullback_from_high,
        )

    if _pullback_long_ready(snap15, snap1h, pullback_from_high, reclaimed_fast):
        stop = min(recent_low, current * (1.0 - max(snap15.atr_bps, 20.0) / 10_000.0))
        target = max(recent_high, current * 1.004)
        return _signal(
            symbol=symbol,
            decision=RADAR_READY,
            scenario="pullback_long",
            side="long",
            current=current,
            trigger=current,
            stop=stop,
            target=target,
            confidence=0.68,
            reason="상승 추세 눌림 후 재상승 확인",
            detail="추격롱이 아니라 15m 눌림 뒤 단기 EMA 재회복을 확인한 롱 후보입니다.",
            snap15=snap15,
            timestamp_ms=ts,
            change_2h_bps=change_2h,
            pullback_bps=pullback_from_high,
        )

    if _pullback_short_ready(snap15, snap1h, bounce_from_low, rejected_fast):
        stop = max(recent_high, current * (1.0 + max(snap15.atr_bps, 20.0) / 10_000.0))
        target = min(recent_low, current * 0.996)
        return _signal(
            symbol=symbol,
            decision=RADAR_READY,
            scenario="pullback_short",
            side="short",
            current=current,
            trigger=current,
            stop=stop,
            target=target,
            confidence=0.68,
            reason="하락 추세 반등 후 재하락 확인",
            detail="바닥 추격숏이 아니라 15m 반등 뒤 단기 EMA 이탈을 확인한 숏 후보입니다.",
            snap15=snap15,
            timestamp_ms=ts,
            change_2h_bps=change_2h,
            pullback_bps=bounce_from_low,
        )

    if overextended_up:
        trigger = snap15.ema_fast or current * 0.997
        return _signal(
            symbol=symbol,
            decision=RADAR_WATCH,
            scenario="impulse_up_wait_pullback",
            side="long",
            current=current,
            trigger=trigger,
            stop=None,
            target=recent_high,
            confidence=0.45,
            reason="상방 과열, 추격 금지 후 눌림 대기",
            detail="방향은 위지만 RSI/밴드 위치가 높아 지금 추격하면 손절 위치가 나빠집니다.",
            snap15=snap15,
            timestamp_ms=ts,
            change_2h_bps=change_2h,
            pullback_bps=pullback_from_high,
        )

    if overextended_down:
        trigger = snap15.ema_fast or current * 1.003
        return _signal(
            symbol=symbol,
            decision=RADAR_WATCH,
            scenario="impulse_down_wait_bounce",
            side="short",
            current=current,
            trigger=trigger,
            stop=None,
            target=recent_low,
            confidence=0.45,
            reason="하방 과열, 추격 금지 후 반등 대기",
            detail="방향은 아래지만 RSI/밴드 위치가 낮아 지금 추격하면 손절 위치가 나빠집니다.",
            snap15=snap15,
            timestamp_ms=ts,
            change_2h_bps=change_2h,
            pullback_bps=bounce_from_low,
        )

    volatility_signal = _volatility_expansion_signal(
        symbol=symbol,
        snap15=snap15,
        uptrend=uptrend,
        downtrend=downtrend,
        current=current,
        recent_high=recent_high,
        recent_low=recent_low,
        timestamp_ms=ts,
        change_2h_bps=change_2h,
        pullback_from_high=pullback_from_high,
        bounce_from_low=bounce_from_low,
    )
    if volatility_signal is not None:
        return volatility_signal

    range_signal = _range_reversion_signal(
        symbol=symbol,
        snap15=snap15,
        snap1h=snap1h,
        uptrend=uptrend,
        downtrend=downtrend,
        current=current,
        recent_high=recent_high,
        recent_low=recent_low,
        timestamp_ms=ts,
        change_2h_bps=change_2h,
        pullback_from_high=pullback_from_high,
        bounce_from_low=bounce_from_low,
    )
    if range_signal is not None:
        return range_signal

    if uptrend and pullback_from_high >= 15.0 and _not_overextended_for_long(snap15):
        trigger = snap15.ema_fast or current
        return _signal(
            symbol=symbol,
            decision=RADAR_NEAR,
            scenario="pullback_long",
            side="long",
            current=current,
            trigger=trigger,
            stop=recent_low,
            target=recent_high,
            confidence=0.50,
            reason="눌림 진행 중, 지지/재상승 확인 전",
            detail="상승 추세 안의 눌림은 보이지만 아직 재상승 확인이 부족합니다.",
            snap15=snap15,
            timestamp_ms=ts,
            change_2h_bps=change_2h,
            pullback_bps=pullback_from_high,
        )

    if downtrend and bounce_from_low >= 15.0 and _not_overextended_for_short(snap15):
        trigger = snap15.ema_fast or current
        return _signal(
            symbol=symbol,
            decision=RADAR_NEAR,
            scenario="pullback_short",
            side="short",
            current=current,
            trigger=trigger,
            stop=recent_high,
            target=recent_low,
            confidence=0.50,
            reason="반등 진행 중, 재하락 확인 전",
            detail="하락 추세 안의 반등은 보이지만 아직 재하락 확인이 부족합니다.",
            snap15=snap15,
            timestamp_ms=ts,
            change_2h_bps=change_2h,
            pullback_bps=bounce_from_low,
        )

    if impulse_up:
        trigger = snap15.ema_fast or current * 0.997
        return _signal(
            symbol=symbol,
            decision=RADAR_WATCH,
            scenario="impulse_up_wait_pullback",
            side="long",
            current=current,
            trigger=trigger,
            stop=None,
            target=recent_high,
            confidence=0.45,
            reason="상방 임펄스, 추격 금지 후 눌림 대기",
            detail="올라가는 힘은 있지만 지금 바로 추격하면 손절 위치가 멀어집니다.",
            snap15=snap15,
            timestamp_ms=ts,
            change_2h_bps=change_2h,
            pullback_bps=pullback_from_high,
        )

    if impulse_down:
        trigger = snap15.ema_fast or current * 1.003
        return _signal(
            symbol=symbol,
            decision=RADAR_WATCH,
            scenario="impulse_down_wait_bounce",
            side="short",
            current=current,
            trigger=trigger,
            stop=None,
            target=recent_low,
            confidence=0.45,
            reason="하방 임펄스, 추격 금지 후 반등 대기",
            detail="내려가는 힘은 있지만 지금 바로 추격하면 손절 위치가 멀어집니다.",
            snap15=snap15,
            timestamp_ms=ts,
            change_2h_bps=change_2h,
            pullback_bps=bounce_from_low,
        )

    return _signal(
        symbol=symbol,
        decision=RADAR_AVOID,
        scenario="no_tactical_edge",
        side="flat",
        current=current,
        trigger=None,
        stop=None,
        target=None,
        confidence=0.20,
        reason="전술 신호 없음",
        detail="추세, 눌림, 재돌파, 실패돌파 중 어느 쪽도 명확하지 않습니다.",
        snap15=snap15,
        timestamp_ms=ts,
        change_2h_bps=change_2h,
        pullback_bps=pullback_from_high if uptrend else bounce_from_low,
    )


def write_tactical_radar_report(
    path: Path,
    *,
    signals: list[TacticalRadarSignal],
    warnings: Iterable[str] = (),
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "generated_ms": now_ms(),
        "warnings": list(warnings),
        "signals": [asdict(signal) for signal in signals],
    }
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def load_tactical_radar_report(path: Path | None = None) -> dict[str, Any] | None:
    report_path = path or default_tactical_radar_report_path()
    if not report_path.exists():
        return None
    return json.loads(report_path.read_text(encoding="utf-8"))


def tactical_radar_text(
    signals: list[TacticalRadarSignal],
    *,
    warnings: Iterable[str] = (),
    generated_ms: int | None = None,
    limit: int = 8,
) -> str:
    ready = [signal for signal in signals if signal.decision == RADAR_READY]
    near = [signal for signal in signals if signal.decision == RADAR_NEAR]
    watch = [signal for signal in signals if signal.decision == RADAR_WATCH]
    avoid = [signal for signal in signals if signal.decision == RADAR_AVOID]
    lines = ["전술 레이더"]
    lines.append("실전 주문 신호가 아니라, 지금 어떤 전술을 기다릴지 보는 실시간 장세 레이어입니다.")
    if generated_ms:
        lines.append(f"생성시각: {kst_from_ms(generated_ms)}")
    lines.append(
        f"요약: 전술후보 {len(ready)}개, 근접 {len(near)}개, 감시 {len(watch)}개, 관망 {len(avoid)}개"
    )
    if ready:
        lines.append("현재 결론: 전술상 후보가 있습니다. 실전 진입 허가는 live-supervisor/preflight가 따로 필요합니다.")
    elif near:
        lines.append("현재 결론: 근접 후보가 있습니다. 확인 캔들 없이는 추격하지 않습니다.")
    elif watch:
        lines.append("현재 결론: 방향성은 있지만 추격 금지, 눌림/반등 확인 대기입니다.")
    else:
        lines.append("현재 결론: 관망입니다.")
    for warning in list(warnings)[:4]:
        lines.append(f"경고: {warning}")
    lines.append("")
    for signal in signals[:limit]:
        lines.append(_signal_text(signal))
    return "\n".join(lines)


def tactical_radar_report_text(path: Path | None = None, *, limit: int = 8) -> str:
    payload = load_tactical_radar_report(path)
    if payload is None:
        return "전술 레이더 결과가 없습니다. 먼저 tactical-radar를 실행해야 합니다."
    signals = [_signal_from_dict(row) for row in payload.get("signals", []) or []]
    return tactical_radar_text(
        signals,
        warnings=tuple(str(row) for row in payload.get("warnings", []) or []),
        generated_ms=int(payload.get("generated_ms", 0) or 0),
        limit=limit,
    )


def tactical_radar_signature(signals: Iterable[TacticalRadarSignal]) -> str:
    active = [
        (
            signal.symbol,
            signal.decision,
            signal.scenario,
            signal.side,
            round(signal.trigger_price or 0.0, 4),
        )
        for signal in signals
        if signal.decision in {RADAR_READY, RADAR_NEAR, RADAR_WATCH}
    ]
    return json.dumps(active, ensure_ascii=False, sort_keys=True)


def tactical_radar_notification_decision(
    signals: Iterable[TacticalRadarSignal],
    state: TacticalRadarNotifyState,
    *,
    force: bool = False,
    periodic_minutes: int = 30,
) -> tuple[bool, str, str, list[TacticalRadarSignal]]:
    signal_list = list(signals)
    selected = [signal for signal in signal_list if signal.decision in {RADAR_READY, RADAR_NEAR}]
    if not selected:
        selected = [signal for signal in signal_list if signal.decision == RADAR_WATCH]
    signature = tactical_radar_signature(selected)
    if not selected:
        return False, "전술 레이더 활성 후보 없음", signature, []
    if force:
        return True, "강제 전술 레이더 전송", signature, selected
    if signature != state.last_signature and any(
        signal.decision in {RADAR_READY, RADAR_NEAR} for signal in selected
    ):
        return True, "전술 레이더 근접/진입 후보 변화", signature, selected
    interval_ms = max(periodic_minutes, 0) * 60_000
    if interval_ms > 0 and (state.last_sent_ms <= 0 or now_ms() - state.last_sent_ms >= interval_ms):
        return True, "전술 레이더 주기 보고", signature, selected
    return False, "전술 레이더 변화 없음", signature, []


def apply_tactical_radar_notification_state(
    state: TacticalRadarNotifyState,
    *,
    signature: str,
) -> TacticalRadarNotifyState:
    return TacticalRadarNotifyState(last_signature=signature, last_sent_ms=now_ms())


def _signal(
    *,
    symbol: str,
    decision: str,
    scenario: str,
    side: str,
    current: float,
    trigger: float | None,
    stop: float | None,
    target: float | None,
    confidence: float,
    reason: str,
    detail: str,
    snap15: TechnicalSnapshot,
    timestamp_ms: int,
    change_2h_bps: float,
    pullback_bps: float,
) -> TacticalRadarSignal:
    return TacticalRadarSignal(
        symbol=symbol,
        decision=decision,
        scenario=scenario,
        side=side,
        current_price=current,
        trigger_price=trigger,
        stop_price=stop,
        target_price=target,
        confidence=confidence,
        reason=reason,
        detail=detail,
        timestamp_ms=timestamp_ms,
        change_2h_bps=change_2h_bps,
        pullback_bps=pullback_bps,
        volume_ratio=snap15.volume_ratio,
        rsi14=snap15.rsi14,
        bollinger_position=snap15.bollinger_position,
    )


def _signal_text(signal: TacticalRadarSignal) -> str:
    trigger = "n/a" if signal.trigger_price is None else f"{signal.trigger_price:.8g}"
    stop = "n/a" if signal.stop_price is None else f"{signal.stop_price:.8g}"
    target = "n/a" if signal.target_price is None else f"{signal.target_price:.8g}"
    rsi = "n/a" if signal.rsi14 is None else f"{signal.rsi14:.1f}"
    bb = "n/a" if signal.bollinger_position is None else f"{signal.bollinger_position:.2f}"
    return (
        f"- {_decision_ko(signal.decision)} {signal.symbol} {_scenario_ko(signal.scenario)} "
        f"{_side_ko(signal.side)} 현재={signal.current_price:.8g} 트리거={trigger} "
        f"목표={target} 손절={stop} 확신={signal.confidence:.0%}\n"
        f"  이유: {signal.reason}\n"
        f"  지표: 2h={signal.change_2h_bps:+.1f}bps 눌림/반등={signal.pullback_bps:.1f}bps "
        f"RSI={rsi} BB={bb} vol={signal.volume_ratio:.2f}x\n"
        f"  해석: {signal.detail}"
    )


def _signal_from_dict(row: dict[str, Any]) -> TacticalRadarSignal:
    return TacticalRadarSignal(
        symbol=str(row.get("symbol", "")),
        decision=str(row.get("decision", "")),
        scenario=str(row.get("scenario", "")),
        side=str(row.get("side", "")),
        current_price=float(row.get("current_price", 0.0) or 0.0),
        trigger_price=_optional_float(row.get("trigger_price")),
        stop_price=_optional_float(row.get("stop_price")),
        target_price=_optional_float(row.get("target_price")),
        confidence=float(row.get("confidence", 0.0) or 0.0),
        reason=str(row.get("reason", "")),
        detail=str(row.get("detail", "")),
        timestamp_ms=int(row.get("timestamp_ms", 0) or 0),
        change_2h_bps=float(row.get("change_2h_bps", 0.0) or 0.0),
        pullback_bps=float(row.get("pullback_bps", 0.0) or 0.0),
        volume_ratio=float(row.get("volume_ratio", 0.0) or 0.0),
        rsi14=_optional_float(row.get("rsi14")),
        bollinger_position=_optional_float(row.get("bollinger_position")),
    )


def _optional_float(value: Any) -> float | None:
    if value is None:
        return None
    return float(value)


def _major_level_signal(
    *,
    symbol: str,
    klines_15m: list[Kline],
    klines_4h: list[Kline],
    snap15: TechnicalSnapshot,
    snap1h: TechnicalSnapshot,
    current: float,
    previous: float,
    recent_high: float,
    recent_low: float,
    timestamp_ms: int,
    change_2h_bps: float,
    pullback_from_high: float,
    bounce_from_low: float,
) -> TacticalRadarSignal | None:
    closed_4h = _closed_rows(klines_4h)
    if len(closed_4h) < 32:
        return None

    snap4h = build_technical_snapshot(closed_4h, interval="4h")
    latest = closed_4h[-1]
    prior = closed_4h[-21:-1]
    high_level, high_source, high_touches = _upper_breakout_level(prior, latest.close)
    low_level, low_source, low_touches = _lower_breakout_level(prior, latest.close)

    if high_level > 0 and latest.close >= high_level * 1.0005 and current > high_level:
        return _major_breakout_long_signal(
            symbol=symbol,
            klines_15m=klines_15m,
            snap15=snap15,
            snap1h=snap1h,
            snap4h=snap4h,
            current=current,
            previous=previous,
            level=high_level,
            level_source=high_source,
            touches=high_touches,
            recent_high=recent_high,
            recent_low=recent_low,
            timestamp_ms=timestamp_ms,
            change_2h_bps=change_2h_bps,
            pullback_from_high=pullback_from_high,
        )

    if low_level > 0 and latest.close <= low_level * 0.9995 and current < low_level:
        return _major_breakout_short_signal(
            symbol=symbol,
            klines_15m=klines_15m,
            snap15=snap15,
            snap1h=snap1h,
            snap4h=snap4h,
            current=current,
            previous=previous,
            level=low_level,
            level_source=low_source,
            touches=low_touches,
            recent_high=recent_high,
            recent_low=recent_low,
            timestamp_ms=timestamp_ms,
            change_2h_bps=change_2h_bps,
            bounce_from_low=bounce_from_low,
        )
    return None


def _major_breakout_long_signal(
    *,
    symbol: str,
    klines_15m: list[Kline],
    snap15: TechnicalSnapshot,
    snap1h: TechnicalSnapshot,
    snap4h: TechnicalSnapshot,
    current: float,
    previous: float,
    level: float,
    level_source: str,
    touches: int,
    recent_high: float,
    recent_low: float,
    timestamp_ms: int,
    change_2h_bps: float,
    pullback_from_high: float,
) -> TacticalRadarSignal:
    stop = level * (1.0 - _level_stop_buffer_bps(snap15) / 10_000.0)
    risk_bps = _price_risk_bps("long", current, stop)
    extension_bps = ((current / level) - 1.0) * 10_000.0 if level > 0 else 0.0
    target = _reward_target("long", current, risk_bps, min_reward_bps=80.0, reward_risk=1.45)
    retested = _recent_retest_long(klines_15m, level) and current > previous
    trend_ok = _is_uptrend(snap1h) or (snap4h.ema_slow is not None and current >= snap4h.ema_slow)

    if retested and trend_ok and risk_bps <= 220.0:
        return _signal(
            symbol=symbol,
            decision=RADAR_READY,
            scenario="breakout_retest_long",
            side="long",
            current=current,
            trigger=current,
            stop=stop,
            target=target,
            confidence=0.72,
            reason="상방 돌파 후 레벨 재테스트 롱 후보",
            detail=(
                f"{level_source} {level:.8g} 상향 돌파 뒤 되눌림/재회복을 확인했습니다. "
                f"최근 4h 레벨 근접 횟수={touches}, 추격폭={extension_bps:.1f}bps입니다."
            ),
            snap15=snap15,
            timestamp_ms=timestamp_ms,
            change_2h_bps=change_2h_bps,
            pullback_bps=pullback_from_high,
        )

    if trend_ok and extension_bps <= 140.0 and risk_bps <= 220.0 and _not_extreme_chase_long(snap15):
        return _signal(
            symbol=symbol,
            decision=RADAR_READY,
            scenario="key_level_breakout_long",
            side="long",
            current=current,
            trigger=current,
            stop=stop,
            target=target,
            confidence=0.66,
            reason="주요 레벨 상방 돌파 롱 후보",
            detail=(
                f"{level_source} {level:.8g} 위에서 4h 돌파가 확인됐습니다. "
                f"추격폭={extension_bps:.1f}bps, 손절거리={risk_bps:.1f}bps라 아직 허용권입니다."
            ),
            snap15=snap15,
            timestamp_ms=timestamp_ms,
            change_2h_bps=change_2h_bps,
            pullback_bps=pullback_from_high,
        )

    trigger = level * 1.001
    return _signal(
        symbol=symbol,
        decision=RADAR_WATCH,
        scenario="key_level_breakout_wait_retest",
        side="long",
        current=current,
        trigger=trigger,
        stop=stop,
        target=max(recent_high, target),
        confidence=0.48,
        reason="상방 큰 돌파는 확인, 추격폭/위험거리 때문에 재테스트 대기",
        detail=(
            f"{level_source} {level:.8g} 돌파는 맞지만 현재 진입 손절거리={risk_bps:.1f}bps, "
            f"추격폭={extension_bps:.1f}bps입니다. 레벨 근처 재확인을 기다립니다."
        ),
        snap15=snap15,
        timestamp_ms=timestamp_ms,
        change_2h_bps=change_2h_bps,
        pullback_bps=pullback_from_high,
    )


def _major_breakout_short_signal(
    *,
    symbol: str,
    klines_15m: list[Kline],
    snap15: TechnicalSnapshot,
    snap1h: TechnicalSnapshot,
    snap4h: TechnicalSnapshot,
    current: float,
    previous: float,
    level: float,
    level_source: str,
    touches: int,
    recent_high: float,
    recent_low: float,
    timestamp_ms: int,
    change_2h_bps: float,
    bounce_from_low: float,
) -> TacticalRadarSignal:
    stop = level * (1.0 + _level_stop_buffer_bps(snap15) / 10_000.0)
    risk_bps = _price_risk_bps("short", current, stop)
    extension_bps = ((level / current) - 1.0) * 10_000.0 if current > 0 else 0.0
    target = _reward_target("short", current, risk_bps, min_reward_bps=80.0, reward_risk=1.45)
    retested = _recent_retest_short(klines_15m, level) and current < previous
    trend_ok = _is_downtrend(snap1h) or (snap4h.ema_slow is not None and current <= snap4h.ema_slow)

    if retested and trend_ok and risk_bps <= 220.0:
        return _signal(
            symbol=symbol,
            decision=RADAR_READY,
            scenario="breakout_retest_short",
            side="short",
            current=current,
            trigger=current,
            stop=stop,
            target=target,
            confidence=0.72,
            reason="하방 이탈 후 레벨 재테스트 숏 후보",
            detail=(
                f"{level_source} {level:.8g} 하향 이탈 뒤 반등/재이탈을 확인했습니다. "
                f"최근 4h 레벨 근접 횟수={touches}, 추격폭={extension_bps:.1f}bps입니다."
            ),
            snap15=snap15,
            timestamp_ms=timestamp_ms,
            change_2h_bps=change_2h_bps,
            pullback_bps=bounce_from_low,
        )

    if trend_ok and extension_bps <= 140.0 and risk_bps <= 220.0 and _not_extreme_chase_short(snap15):
        return _signal(
            symbol=symbol,
            decision=RADAR_READY,
            scenario="key_level_breakout_short",
            side="short",
            current=current,
            trigger=current,
            stop=stop,
            target=target,
            confidence=0.66,
            reason="주요 레벨 하방 이탈 숏 후보",
            detail=(
                f"{level_source} {level:.8g} 아래에서 4h 이탈이 확인됐습니다. "
                f"추격폭={extension_bps:.1f}bps, 손절거리={risk_bps:.1f}bps라 아직 허용권입니다."
            ),
            snap15=snap15,
            timestamp_ms=timestamp_ms,
            change_2h_bps=change_2h_bps,
            pullback_bps=bounce_from_low,
        )

    trigger = level * 0.999
    return _signal(
        symbol=symbol,
        decision=RADAR_WATCH,
        scenario="key_level_breakout_wait_retest",
        side="short",
        current=current,
        trigger=trigger,
        stop=stop,
        target=min(recent_low, target),
        confidence=0.48,
        reason="하방 큰 이탈은 확인, 추격폭/위험거리 때문에 재테스트 대기",
        detail=(
            f"{level_source} {level:.8g} 이탈은 맞지만 현재 진입 손절거리={risk_bps:.1f}bps, "
            f"추격폭={extension_bps:.1f}bps입니다. 레벨 근처 재확인을 기다립니다."
        ),
        snap15=snap15,
        timestamp_ms=timestamp_ms,
        change_2h_bps=change_2h_bps,
        pullback_bps=bounce_from_low,
    )


def _volatility_expansion_signal(
    *,
    symbol: str,
    snap15: TechnicalSnapshot,
    uptrend: bool,
    downtrend: bool,
    current: float,
    recent_high: float,
    recent_low: float,
    timestamp_ms: int,
    change_2h_bps: float,
    pullback_from_high: float,
    bounce_from_low: float,
) -> TacticalRadarSignal | None:
    if snap15.volume_ratio < 1.35 or snap15.bollinger_width_bps < 35.0:
        return None
    if uptrend and snap15.high_breakout and change_2h_bps >= 25.0 and _not_extreme_chase_long(snap15):
        stop = min(recent_low, current * (1.0 - max(snap15.atr_bps, 25.0) / 10_000.0))
        risk_bps = _price_risk_bps("long", current, stop)
        return _signal(
            symbol=symbol,
            decision=RADAR_READY,
            scenario="volatility_expansion_long",
            side="long",
            current=current,
            trigger=current,
            stop=stop,
            target=_reward_target("long", current, risk_bps, min_reward_bps=60.0, reward_risk=1.35),
            confidence=0.58,
            reason="상방 변동성 확장 롱 후보",
            detail="15m 고점 돌파, 거래량 증가, 밴드 확장이 동시에 나온 모멘텀 paper 후보입니다.",
            snap15=snap15,
            timestamp_ms=timestamp_ms,
            change_2h_bps=change_2h_bps,
            pullback_bps=pullback_from_high,
        )
    if downtrend and snap15.low_breakout and change_2h_bps <= -25.0 and _not_extreme_chase_short(snap15):
        stop = max(recent_high, current * (1.0 + max(snap15.atr_bps, 25.0) / 10_000.0))
        risk_bps = _price_risk_bps("short", current, stop)
        return _signal(
            symbol=symbol,
            decision=RADAR_READY,
            scenario="volatility_expansion_short",
            side="short",
            current=current,
            trigger=current,
            stop=stop,
            target=_reward_target("short", current, risk_bps, min_reward_bps=60.0, reward_risk=1.35),
            confidence=0.58,
            reason="하방 변동성 확장 숏 후보",
            detail="15m 저점 이탈, 거래량 증가, 밴드 확장이 동시에 나온 모멘텀 paper 후보입니다.",
            snap15=snap15,
            timestamp_ms=timestamp_ms,
            change_2h_bps=change_2h_bps,
            pullback_bps=bounce_from_low,
        )
    return None


def _range_reversion_signal(
    *,
    symbol: str,
    snap15: TechnicalSnapshot,
    snap1h: TechnicalSnapshot,
    uptrend: bool,
    downtrend: bool,
    current: float,
    recent_high: float,
    recent_low: float,
    timestamp_ms: int,
    change_2h_bps: float,
    pullback_from_high: float,
    bounce_from_low: float,
) -> TacticalRadarSignal | None:
    if uptrend or downtrend or current <= 0 or recent_low <= 0:
        return None
    range_width_bps = ((recent_high / recent_low) - 1.0) * 10_000.0
    range_like = abs(snap1h.ema_gap_bps) <= 12.0 and 35.0 <= range_width_bps <= 220.0
    if not range_like:
        return None

    bb = snap15.bollinger_position
    rsi = snap15.rsi14
    near_low = current <= recent_low * 1.0025
    near_high = current >= recent_high * 0.9975
    if near_low and ((bb is not None and bb <= 0.12) or (rsi is not None and rsi <= 30.0)):
        stop = current * (1.0 - max(20.0, snap15.atr_bps * 0.9) / 10_000.0)
        risk_bps = _price_risk_bps("long", current, stop)
        target = min(recent_high, _reward_target("long", current, risk_bps, min_reward_bps=35.0, reward_risk=1.25))
        if target > current:
            return _signal(
                symbol=symbol,
                decision=RADAR_READY,
                scenario="range_reversion_long",
                side="long",
                current=current,
                trigger=current,
                stop=stop,
                target=target,
                confidence=0.54,
                reason="횡보 하단 반등 롱 후보",
                detail="1h 추세가 약하고 15m 가격이 박스 하단/과매도에 붙은 mean-reversion paper 후보입니다.",
                snap15=snap15,
                timestamp_ms=timestamp_ms,
                change_2h_bps=change_2h_bps,
                pullback_bps=pullback_from_high,
            )

    if near_high and ((bb is not None and bb >= 0.88) or (rsi is not None and rsi >= 70.0)):
        stop = current * (1.0 + max(20.0, snap15.atr_bps * 0.9) / 10_000.0)
        risk_bps = _price_risk_bps("short", current, stop)
        target = max(recent_low, _reward_target("short", current, risk_bps, min_reward_bps=35.0, reward_risk=1.25))
        if target < current:
            return _signal(
                symbol=symbol,
                decision=RADAR_READY,
                scenario="range_reversion_short",
                side="short",
                current=current,
                trigger=current,
                stop=stop,
                target=target,
                confidence=0.54,
                reason="횡보 상단 되돌림 숏 후보",
                detail="1h 추세가 약하고 15m 가격이 박스 상단/과매수에 붙은 mean-reversion paper 후보입니다.",
                snap15=snap15,
                timestamp_ms=timestamp_ms,
                change_2h_bps=change_2h_bps,
                pullback_bps=bounce_from_low,
            )
    return None


def _closed_rows(klines: list[Kline]) -> list[Kline]:
    if len(klines) <= 2:
        return klines
    return klines[:-1]


def _upper_breakout_level(rows: list[Kline], latest_close: float) -> tuple[float, str, int]:
    raw_level = max(row.high for row in rows)
    level = raw_level
    source = "20개 4h 고점"
    round_level = _round_level_below(latest_close)
    if (
        round_level is not None
        and latest_close > round_level
        and raw_level <= round_level
        and raw_level >= round_level * 0.990
    ):
        level = round_level
        source = "심리적 라운드 레벨"
    return level, source, _upper_touch_count(rows, level)


def _lower_breakout_level(rows: list[Kline], latest_close: float) -> tuple[float, str, int]:
    raw_level = min(row.low for row in rows)
    level = raw_level
    source = "20개 4h 저점"
    round_level = _round_level_above(latest_close)
    if (
        round_level is not None
        and latest_close < round_level
        and raw_level >= round_level
        and raw_level <= round_level * 1.010
    ):
        level = round_level
        source = "심리적 라운드 레벨"
    return level, source, _lower_touch_count(rows, level)


def _round_level_below(price: float) -> float | None:
    step = _psychological_step(price)
    if step <= 0:
        return None
    level = math.floor(price / step) * step
    return level if level > 0 else None


def _round_level_above(price: float) -> float | None:
    step = _psychological_step(price)
    if step <= 0:
        return None
    level = math.ceil(price / step) * step
    return level if level > 0 else None


def _psychological_step(price: float) -> float:
    if price >= 50_000:
        return 1_000.0
    if price >= 10_000:
        return 500.0
    if price >= 1_000:
        return 50.0
    if price >= 100:
        return 5.0
    if price >= 10:
        return 0.5
    if price >= 1:
        return 0.05
    return 0.005


def _upper_touch_count(rows: list[Kline], level: float, *, tolerance_bps: float = 100.0) -> int:
    if level <= 0:
        return 0
    threshold = level * (1.0 - tolerance_bps / 10_000.0)
    return sum(1 for row in rows if threshold <= row.high <= level * 1.002)


def _lower_touch_count(rows: list[Kline], level: float, *, tolerance_bps: float = 100.0) -> int:
    if level <= 0:
        return 0
    threshold = level * (1.0 + tolerance_bps / 10_000.0)
    return sum(1 for row in rows if level * 0.998 <= row.low <= threshold)


def _level_stop_buffer_bps(snapshot: TechnicalSnapshot) -> float:
    return min(max(snapshot.atr_bps * 1.15, 35.0), 95.0)


def _recent_retest_long(klines: list[Kline], level: float) -> bool:
    if level <= 0:
        return False
    recent = klines[-8:]
    touched = any(row.low <= level * 1.0025 for row in recent)
    reclaimed = klines[-1].close >= level * 1.0005
    return touched and reclaimed


def _recent_retest_short(klines: list[Kline], level: float) -> bool:
    if level <= 0:
        return False
    recent = klines[-8:]
    touched = any(row.high >= level * 0.9975 for row in recent)
    rejected = klines[-1].close <= level * 0.9995
    return touched and rejected


def _price_risk_bps(side: str, current: float, stop: float) -> float:
    if current <= 0 or stop <= 0:
        return 9999.0
    if side == "long":
        return ((current / stop) - 1.0) * 10_000.0
    if side == "short":
        return ((stop / current) - 1.0) * 10_000.0
    return 9999.0


def _reward_target(
    side: str,
    current: float,
    risk_bps: float,
    *,
    min_reward_bps: float,
    reward_risk: float,
) -> float:
    reward_bps = max(risk_bps * reward_risk, min_reward_bps)
    if side == "long":
        return current * (1.0 + reward_bps / 10_000.0)
    return current * (1.0 - reward_bps / 10_000.0)


def _not_extreme_chase_long(snapshot: TechnicalSnapshot) -> bool:
    rsi_ok = snapshot.rsi14 is None or snapshot.rsi14 <= 82.0
    bb_ok = snapshot.bollinger_position is None or snapshot.bollinger_position <= 1.15
    return rsi_ok and bb_ok


def _not_extreme_chase_short(snapshot: TechnicalSnapshot) -> bool:
    rsi_ok = snapshot.rsi14 is None or snapshot.rsi14 >= 18.0
    bb_ok = snapshot.bollinger_position is None or snapshot.bollinger_position >= -0.15
    return rsi_ok and bb_ok


def _is_uptrend(snapshot: TechnicalSnapshot) -> bool:
    return (
        snapshot.enough
        and snapshot.ema_slow is not None
        and snapshot.close >= snapshot.ema_slow
        and snapshot.ema_gap_bps >= 8.0
        and snapshot.ema_slope_bps >= -4.0
    )


def _is_downtrend(snapshot: TechnicalSnapshot) -> bool:
    return (
        snapshot.enough
        and snapshot.ema_slow is not None
        and snapshot.close <= snapshot.ema_slow
        and snapshot.ema_gap_bps <= -8.0
        and snapshot.ema_slope_bps <= 4.0
    )


def _pullback_long_ready(
    snap15: TechnicalSnapshot,
    snap1h: TechnicalSnapshot,
    pullback_bps: float,
    reclaimed_fast: bool,
) -> bool:
    rsi_ok = snap15.rsi14 is not None and 43.0 <= snap15.rsi14 <= 64.0
    bb_ok = snap15.bollinger_position is not None and 0.30 <= snap15.bollinger_position <= 0.72
    return _is_uptrend(snap1h) and 15.0 <= pullback_bps <= 120.0 and reclaimed_fast and rsi_ok and bb_ok


def _pullback_short_ready(
    snap15: TechnicalSnapshot,
    snap1h: TechnicalSnapshot,
    bounce_bps: float,
    rejected_fast: bool,
) -> bool:
    rsi_ok = snap15.rsi14 is not None and 36.0 <= snap15.rsi14 <= 57.0
    bb_ok = snap15.bollinger_position is not None and 0.28 <= snap15.bollinger_position <= 0.70
    return _is_downtrend(snap1h) and 15.0 <= bounce_bps <= 120.0 and rejected_fast and rsi_ok and bb_ok


def _not_overextended_for_long(snapshot: TechnicalSnapshot) -> bool:
    rsi_ok = snapshot.rsi14 is None or snapshot.rsi14 <= 68.0
    bb_ok = snapshot.bollinger_position is None or snapshot.bollinger_position <= 0.85
    return rsi_ok and bb_ok


def _not_overextended_for_short(snapshot: TechnicalSnapshot) -> bool:
    rsi_ok = snapshot.rsi14 is None or snapshot.rsi14 >= 32.0
    bb_ok = snapshot.bollinger_position is None or snapshot.bollinger_position >= 0.15
    return rsi_ok and bb_ok


def _failed_upside_breakout(
    klines: list[Kline],
    recent_high: float,
    snapshot: TechnicalSnapshot,
) -> bool:
    latest = klines[-1]
    body_top = max(latest.open, latest.close)
    upper_wick_bps = ((latest.high / body_top) - 1.0) * 10_000.0 if body_top > 0 else 0.0
    failed_level = latest.high > recent_high and latest.close < recent_high
    overbought = snapshot.rsi14 is not None and snapshot.rsi14 >= 66.0
    return failed_level and upper_wick_bps >= 18.0 and snapshot.volume_ratio >= 1.2 and overbought


def _return_bps(values: list[float], *, bars: int) -> float:
    if len(values) <= bars:
        return 0.0
    previous = values[-bars - 1]
    if previous <= 0:
        return 0.0
    return ((values[-1] / previous) - 1.0) * 10_000.0


def _radar_rank_key(signal: TacticalRadarSignal) -> tuple[int, float, float]:
    rank = {RADAR_READY: 0, RADAR_NEAR: 1, RADAR_WATCH: 2, RADAR_AVOID: 3}.get(signal.decision, 4)
    return (rank, -signal.confidence, -abs(signal.change_2h_bps))


def _decision_ko(decision: str) -> str:
    return {
        RADAR_READY: "전술후보",
        RADAR_NEAR: "근접",
        RADAR_WATCH: "감시",
        RADAR_AVOID: "관망",
    }.get(decision, decision)


def _scenario_ko(scenario: str) -> str:
    return {
        "pullback_long": "눌림 롱",
        "pullback_short": "반등 숏",
        "key_level_breakout_long": "주요레벨 상방돌파 롱",
        "key_level_breakout_short": "주요레벨 하방이탈 숏",
        "key_level_breakout_wait_retest": "주요레벨 돌파 재테스트대기",
        "breakout_retest_long": "상방돌파 재테스트 롱",
        "breakout_retest_short": "하방이탈 재테스트 숏",
        "volatility_expansion_long": "상방 변동성확장 롱",
        "volatility_expansion_short": "하방 변동성확장 숏",
        "range_reversion_long": "횡보하단 반등 롱",
        "range_reversion_short": "횡보상단 되돌림 숏",
        "impulse_up_wait_pullback": "상방 임펄스 눌림대기",
        "impulse_down_wait_bounce": "하방 임펄스 반등대기",
        "failed_breakout_short": "상방 실패돌파 숏",
        "no_tactical_edge": "전술 없음",
        "data_low": "표본부족",
    }.get(scenario, scenario)


def _side_ko(side: str) -> str:
    return {"long": "롱", "short": "숏", "flat": "관망"}.get(side, side)
