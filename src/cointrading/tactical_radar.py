from __future__ import annotations

from dataclasses import asdict, dataclass
import json
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
        except BinanceAPIError as exc:
            warnings.append(f"{symbol} 캔들 조회 실패: {exc}")
            continue
        signal = evaluate_tactical_symbol(symbol, klines_15m, klines_1h, timestamp_ms=ts)
        output.append(signal)
    return sorted(output, key=_radar_rank_key), tuple(warnings)


def evaluate_tactical_symbol(
    symbol: str,
    klines_15m: list[Kline],
    klines_1h: list[Kline],
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
        f"요약: 진입가능 {len(ready)}개, 근접 {len(near)}개, 감시 {len(watch)}개, 관망 {len(avoid)}개"
    )
    if ready:
        lines.append("현재 결론: 조건부 진입 후보가 있습니다. live-supervisor/preflight는 별도입니다.")
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
        RADAR_READY: "진입가능",
        RADAR_NEAR: "근접",
        RADAR_WATCH: "감시",
        RADAR_AVOID: "관망",
    }.get(decision, decision)


def _scenario_ko(scenario: str) -> str:
    return {
        "pullback_long": "눌림 롱",
        "pullback_short": "반등 숏",
        "impulse_up_wait_pullback": "상방 임펄스 눌림대기",
        "impulse_down_wait_bounce": "하방 임펄스 반등대기",
        "failed_breakout_short": "상방 실패돌파 숏",
        "no_tactical_edge": "전술 없음",
        "data_low": "표본부족",
    }.get(scenario, scenario)


def _side_ko(side: str) -> str:
    return {"long": "롱", "short": "숏", "flat": "관망"}.get(side, side)
