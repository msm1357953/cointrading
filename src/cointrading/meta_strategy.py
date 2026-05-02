from __future__ import annotations

from dataclasses import asdict, dataclass
import json
import math
from pathlib import Path
import statistics
from typing import Any, Iterable

from cointrading.config import TradingConfig
from cointrading.historical_data import HistoricalKlineResult
from cointrading.indicators import ema_series
from cointrading.models import Kline, SignalSide
from cointrading.storage import kst_from_ms, now_ms


@dataclass(frozen=True)
class MarketFeatureSnapshot:
    symbol: str
    timestamp_ms: int
    close: float
    ema_gap_bps: float
    ema_slope_bps: float
    trend_1h_bps: float
    trend_4h_bps: float
    trend_24h_bps: float
    rsi14: float | None
    bollinger_position: float | None
    bollinger_width_bps: float
    atr_bps: float
    realized_vol_bps: float
    volume_ratio: float
    high_breakout: bool
    low_breakout: bool


@dataclass(frozen=True)
class MetaDecision:
    timestamp_ms: int
    symbol: str
    regime: str
    action: str
    side: SignalSide
    take_profit_bps: float
    stop_loss_bps: float
    max_hold_bars: int
    confidence: float
    reason: str
    features: MarketFeatureSnapshot


@dataclass(frozen=True)
class MetaTrade:
    symbol: str
    regime: str
    action: str
    side: SignalSide
    entry_time_ms: int
    exit_time_ms: int
    entry_price: float
    exit_price: float
    notional: float
    pnl: float
    pnl_bps: float
    reason: str
    hold_bars: int
    take_profit_bps: float
    stop_loss_bps: float


@dataclass(frozen=True)
class MetaActionSummary:
    key: str
    count: int
    win_rate: float
    avg_pnl_bps: float
    sum_pnl: float
    profit_factor: float


@dataclass(frozen=True)
class MetaBacktestResult:
    symbol: str
    interval: str
    start_ms: int
    end_ms: int
    sample_bars: int
    source_files: int
    missing_files: int
    trade_count: int
    win_rate: float
    avg_pnl_bps: float
    sum_pnl: float
    sum_pnl_bps: float
    max_drawdown_pct: float
    profit_factor: float
    payoff_ratio: float
    max_consecutive_loss: int
    decision: str
    reason: str
    action_summaries: list[MetaActionSummary]
    regime_summaries: list[MetaActionSummary]
    recent_trades: list[MetaTrade]
    recent_decisions: list[MetaDecision]


@dataclass
class MetaNotifyState:
    last_signature: str = ""
    last_sent_ms: int = 0

    @classmethod
    def load(cls, path: Path) -> "MetaNotifyState":
        if not path.exists():
            return cls()
        payload = json.loads(path.read_text(encoding="utf-8"))
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
                indent=2,
                sort_keys=True,
            ),
            encoding="utf-8",
        )


def default_meta_report_path() -> Path:
    return Path(__file__).resolve().parents[2] / "data" / "meta_strategy_latest.json"


def default_meta_notify_state_path() -> Path:
    return Path(__file__).resolve().parents[2] / "data" / "meta_strategy_notify_state.json"


def run_meta_backtest(
    *,
    history: HistoricalKlineResult,
    config: TradingConfig,
    notional: float | None = None,
) -> MetaBacktestResult:
    klines = history.klines
    order_notional = notional if notional is not None else config.strategy_order_notional
    trades, decisions = backtest_meta_policy(
        symbol=history.symbol,
        interval=history.interval,
        klines=klines,
        config=config,
        notional=order_notional,
    )
    return summarize_meta_result(
        symbol=history.symbol,
        interval=history.interval,
        klines=klines,
        trades=trades,
        decisions=decisions,
        config=config,
        source_files=len(history.source_files),
        missing_files=len(history.missing_urls),
    )


def backtest_meta_policy(
    *,
    symbol: str,
    interval: str,
    klines: list[Kline],
    config: TradingConfig,
    notional: float,
) -> tuple[list[MetaTrade], list[MetaDecision]]:
    if len(klines) < 120:
        return [], []
    interval_seconds = _interval_seconds(interval)
    closes = [row.close for row in klines]
    fast = ema_series(closes, 20)
    slow = ema_series(closes, 80)
    trades: list[MetaTrade] = []
    decisions: list[MetaDecision] = []
    active: dict[str, Any] | None = None
    cooldown_bars = max(4, round(3600 / interval_seconds))
    cooldown_until_index = 0
    index = 100

    while index < len(klines):
        if active is None:
            if index < cooldown_until_index:
                index += 1
                continue
            if index + 1 >= len(klines):
                break
            decision = decide_meta_action(
                symbol=symbol,
                interval_seconds=interval_seconds,
                klines=klines,
                index=index,
                config=config,
                closes=closes,
                fast_ema=fast,
                slow_ema=slow,
            )
            if decision.side not in {"long", "short"}:
                index += 1
                continue
            decisions.append(decision)
            entry_bar = klines[index + 1]
            entry_price = _slipped_entry_price(entry_bar.open, decision.side, config.slippage_bps)
            active = {
                "decision": decision,
                "entry_index": index + 1,
                "entry_time_ms": entry_bar.open_time,
                "entry_price": entry_price,
                "quantity": notional / entry_price if entry_price > 0 else 0.0,
            }
            index += 1
            continue

        decision = active["decision"]
        side = str(decision.side)
        entry_index = int(active["entry_index"])
        entry_price = float(active["entry_price"])
        quantity = float(active["quantity"])
        bar = klines[index]
        hold_bars = index - entry_index
        target_price = _target_price(entry_price, side, decision.take_profit_bps)
        stop_price = _stop_price(entry_price, side, decision.stop_loss_bps)
        exit_price = 0.0
        reason = ""

        if side == "long":
            stop_hit = bar.low <= stop_price
            target_hit = bar.high >= target_price
        else:
            stop_hit = bar.high >= stop_price
            target_hit = bar.low <= target_price

        if stop_hit:
            exit_price = _slipped_exit_price(stop_price, side, config.slippage_bps)
            reason = "stop_loss"
        elif target_hit:
            exit_price = _slipped_exit_price(target_price, side, config.slippage_bps)
            reason = "take_profit"
        elif hold_bars >= decision.max_hold_bars:
            exit_price = _slipped_exit_price(bar.close, side, config.slippage_bps)
            reason = "max_hold"
        else:
            current_decision = decide_meta_action(
                symbol=symbol,
                interval_seconds=interval_seconds,
                klines=klines,
                index=index,
                config=config,
                closes=closes,
                fast_ema=fast,
                slow_ema=slow,
            )
            if current_decision.regime == "panic":
                exit_price = _slipped_exit_price(bar.close, side, config.slippage_bps)
                reason = "panic_exit"
            elif current_decision.side in {"long", "short"} and current_decision.side != side:
                exit_price = _slipped_exit_price(bar.close, side, config.slippage_bps)
                reason = "policy_flip"

        if reason:
            trades.append(
                _build_trade(
                    decision=decision,
                    entry_time_ms=int(active["entry_time_ms"]),
                    exit_time_ms=bar.close_time,
                    entry_price=entry_price,
                    exit_price=exit_price,
                    quantity=quantity,
                    notional=notional,
                    fee_rate=config.taker_fee_rate,
                    reason=reason,
                    hold_bars=hold_bars,
                )
            )
            active = None
            cooldown_until_index = index + cooldown_bars
        index += 1

    if active is not None and klines:
        decision = active["decision"]
        final_bar = klines[-1]
        trades.append(
            _build_trade(
                decision=decision,
                entry_time_ms=int(active["entry_time_ms"]),
                exit_time_ms=final_bar.close_time,
                entry_price=float(active["entry_price"]),
                exit_price=_slipped_exit_price(final_bar.close, decision.side, config.slippage_bps),
                quantity=float(active["quantity"]),
                notional=notional,
                fee_rate=config.taker_fee_rate,
                reason="end_of_sample",
                hold_bars=max(len(klines) - 1 - int(active["entry_index"]), 0),
            )
        )
    return trades, decisions[-100:]


def decide_meta_action(
    *,
    symbol: str,
    interval_seconds: int,
    klines: list[Kline],
    index: int,
    config: TradingConfig,
    closes: list[float] | None = None,
    fast_ema: list[float] | None = None,
    slow_ema: list[float] | None = None,
) -> MetaDecision:
    features = _features_at(
        symbol=symbol,
        interval_seconds=interval_seconds,
        klines=klines,
        index=index,
        closes=closes,
        fast_ema=fast_ema,
        slow_ema=slow_ema,
    )
    if index < 100:
        return _decision(features, "warmup", "no_trade", "flat", config, interval_seconds, 0.0, "지표 준비 중")

    if features.atr_bps >= 220.0 or features.realized_vol_bps >= 120.0:
        return _decision(
            features,
            "panic",
            "no_trade",
            "flat",
            config,
            interval_seconds,
            0.0,
            "변동성 급등 구간은 신규 진입 금지",
        )

    if features.high_breakout and features.volume_ratio >= 1.2 and features.trend_4h_bps > 12.0:
        if (
            features.volume_ratio < 1.45
            or features.trend_24h_bps < 20.0
            or features.bollinger_position is None
            or features.bollinger_position < 0.88
        ):
            return _decision(
                features,
                "breakout_up",
                "no_trade",
                "flat",
                config,
                interval_seconds,
                0.35,
                "돌파지만 거래량/위치/상위추세 확인 부족",
            )
        return _decision(
            features,
            "breakout_up",
            "breakout_long",
            "long",
            config,
            interval_seconds,
            0.68,
            "거래량 동반 상방 돌파",
        )
    if features.low_breakout and features.volume_ratio >= 1.2 and features.trend_4h_bps < -12.0:
        if (
            features.volume_ratio < 1.45
            or features.trend_24h_bps > -20.0
            or features.bollinger_position is None
            or features.bollinger_position > 0.12
        ):
            return _decision(
                features,
                "breakout_down",
                "no_trade",
                "flat",
                config,
                interval_seconds,
                0.35,
                "돌파지만 거래량/위치/상위추세 확인 부족",
            )
        return _decision(
            features,
            "breakout_down",
            "breakout_short",
            "short",
            config,
            interval_seconds,
            0.68,
            "거래량 동반 하방 돌파",
        )

    if (
        features.ema_gap_bps > 8.0
        and features.ema_slope_bps > 1.5
        and features.trend_4h_bps > 20.0
        and features.trend_24h_bps > 35.0
        and features.rsi14 is not None
        and 44.0 <= features.rsi14 <= 68.0
        and features.bollinger_position is not None
        and 0.35 <= features.bollinger_position <= 0.90
    ):
        return _decision(
            features,
            "trend_up",
            "trend_long",
            "long",
            config,
            interval_seconds,
            0.74,
            "상승 추세 정렬",
        )
    if (
        features.ema_gap_bps < -8.0
        and features.ema_slope_bps < -1.5
        and features.trend_4h_bps < -20.0
        and features.trend_24h_bps < -35.0
        and features.rsi14 is not None
        and 32.0 <= features.rsi14 <= 56.0
        and features.bollinger_position is not None
        and 0.10 <= features.bollinger_position <= 0.65
    ):
        return _decision(
            features,
            "trend_down",
            "trend_short",
            "short",
            config,
            interval_seconds,
            0.74,
            "하락 추세 정렬",
        )

    range_like = (
        abs(features.trend_4h_bps) <= 28.0
        and abs(features.ema_gap_bps) <= 18.0
        and 20.0 <= features.bollinger_width_bps <= 180.0
    )
    if range_like and features.rsi14 is not None and features.bollinger_position is not None:
        if features.rsi14 <= 34.0 and features.bollinger_position <= 0.18:
            return _decision(
                features,
                "range",
                "range_long",
                "long",
                config,
                interval_seconds,
                0.62,
                "횡보 하단 되돌림",
            )
        if features.rsi14 >= 66.0 and features.bollinger_position >= 0.82:
            return _decision(
                features,
                "range",
                "range_short",
                "short",
                config,
                interval_seconds,
                0.62,
                "횡보 상단 되돌림",
            )
        return _decision(
            features,
            "range",
            "no_trade",
            "flat",
            config,
            interval_seconds,
            0.35,
            "횡보지만 상하단 진입 위치 아님",
        )

    return _decision(
        features,
        "mixed",
        "no_trade",
        "flat",
        config,
        interval_seconds,
        0.25,
        "추세/횡보/돌파 조건이 불명확",
    )


def summarize_meta_result(
    *,
    symbol: str,
    interval: str,
    klines: list[Kline],
    trades: list[MetaTrade],
    decisions: list[MetaDecision],
    config: TradingConfig,
    source_files: int,
    missing_files: int,
) -> MetaBacktestResult:
    pnls = [trade.pnl for trade in trades]
    pnl_bps_values = [trade.pnl_bps for trade in trades]
    wins = [value for value in pnls if value > 0]
    losses = [value for value in pnls if value <= 0]
    win_rate = len(wins) / len(trades) if trades else 0.0
    avg_pnl_bps = statistics.fmean(pnl_bps_values) if pnl_bps_values else 0.0
    avg_win = statistics.fmean(wins) if wins else 0.0
    avg_loss_abs = abs(statistics.fmean(losses)) if losses else 0.0
    payoff_ratio = avg_win / avg_loss_abs if avg_loss_abs > 0 else (999.0 if wins else 0.0)
    gross_profit = sum(wins)
    gross_loss = abs(sum(losses))
    profit_factor = gross_profit / gross_loss if gross_loss > 0 else (999.0 if wins else 0.0)
    equity = config.initial_equity
    peak = config.initial_equity
    max_drawdown = 0.0
    max_consecutive_loss = 0
    current_loss_streak = 0
    for pnl in pnls:
        equity += pnl
        peak = max(peak, equity)
        if peak > 0:
            max_drawdown = max(max_drawdown, (peak - equity) / peak)
        if pnl <= 0:
            current_loss_streak += 1
            max_consecutive_loss = max(max_consecutive_loss, current_loss_streak)
        else:
            current_loss_streak = 0
    decision, reason = _meta_result_decision(
        trade_count=len(trades),
        avg_pnl_bps=avg_pnl_bps,
        profit_factor=profit_factor,
        payoff_ratio=payoff_ratio,
        max_drawdown=max_drawdown,
        max_consecutive_loss=max_consecutive_loss,
    )
    return MetaBacktestResult(
        symbol=symbol,
        interval=interval,
        start_ms=klines[0].open_time if klines else 0,
        end_ms=klines[-1].close_time if klines else 0,
        sample_bars=len(klines),
        source_files=source_files,
        missing_files=missing_files,
        trade_count=len(trades),
        win_rate=win_rate,
        avg_pnl_bps=avg_pnl_bps,
        sum_pnl=sum(pnls),
        sum_pnl_bps=sum(pnl_bps_values),
        max_drawdown_pct=max_drawdown,
        profit_factor=profit_factor,
        payoff_ratio=payoff_ratio,
        max_consecutive_loss=max_consecutive_loss,
        decision=decision,
        reason=reason,
        action_summaries=_summaries_by_key(trades, lambda trade: trade.action),
        regime_summaries=_summaries_by_key(trades, lambda trade: trade.regime),
        recent_trades=trades[-100:],
        recent_decisions=decisions[-30:],
    )


def write_meta_report(
    path: Path,
    *,
    results: list[MetaBacktestResult],
    symbols: Iterable[str],
    interval: str,
    start_date: str,
    end_date: str,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "generated_ms": now_ms(),
        "symbols": [symbol.upper() for symbol in symbols],
        "interval": interval,
        "start_date": start_date,
        "end_date": end_date,
        "results": [asdict(result) for result in results],
    }
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def load_meta_report(path: Path | None = None) -> dict[str, Any] | None:
    report_path = path or default_meta_report_path()
    if not report_path.exists():
        return None
    return json.loads(report_path.read_text(encoding="utf-8"))


def meta_report_text(path: Path | None = None, *, limit: int = 8) -> str:
    payload = load_meta_report(path)
    if payload is None:
        return "메타전략 백테스트 결과가 아직 없습니다. 먼저 meta-backtest를 실행해야 합니다."
    results = [_meta_result_from_dict(row) for row in payload.get("results", [])]
    return meta_results_text(results, generated_ms=int(payload.get("generated_ms", 0) or 0), limit=limit)


def meta_results_text(
    results: list[MetaBacktestResult],
    *,
    generated_ms: int | None = None,
    limit: int = 8,
) -> str:
    lines = [
        "상황판단형 메타전략 백테스트",
        "한 정책: 장세 판단 → 행동 선택 → TP/SL/보유시간 적용. 실주문 없음.",
    ]
    if generated_ms:
        lines.append(f"생성시각: {kst_from_ms(generated_ms)}")
    if not results:
        lines.append("결과 없음")
        return "\n".join(lines)
    ready = [row for row in results if row.decision == "PAPER_READY"]
    observe = [row for row in results if row.decision == "OBSERVE"]
    blocked = [row for row in results if row.decision == "BLOCKED"]
    lines.append(f"요약: paper 후보 {len(ready)}개, 관찰 {len(observe)}개, 차단 {len(blocked)}개")
    ranked = sorted(results, key=lambda row: row.avg_pnl_bps, reverse=True)
    best = ranked[0]
    lines.append(
        "상위 결과: "
        f"{best.symbol} {best.decision} 평균={best.avg_pnl_bps:+.2f}bps "
        f"PF={best.profit_factor:.2f} MDD={best.max_drawdown_pct:.1%}"
    )
    lines.append("")
    lines.append("심볼별")
    for row in ranked[:limit]:
        lines.append(_meta_result_line(row))
        for action in row.action_summaries[:3]:
            lines.append(
                f"  · {meta_action_ko(action.key)} n={action.count} "
                f"승률={action.win_rate:.1%} 평균={action.avg_pnl_bps:+.2f}bps "
                f"PF={action.profit_factor:.2f}"
            )
    return "\n".join(lines)


def meta_notification_decision(
    results: list[MetaBacktestResult],
    state: MetaNotifyState,
    *,
    periodic_minutes: int,
    force: bool = False,
    current_ms: int | None = None,
) -> tuple[bool, str, str]:
    ts = current_ms or now_ms()
    signature = _meta_signature(results)
    if force:
        return True, "수동 강제 실행", signature
    if signature != state.last_signature:
        return True, "메타전략 판정 변화", signature
    interval_ms = max(periodic_minutes, 1) * 60_000
    if ts - state.last_sent_ms >= interval_ms:
        return True, "주기 요약", signature
    return False, "변화 없음", signature


def apply_meta_notification_state(
    state: MetaNotifyState,
    *,
    signature: str,
    timestamp_ms: int | None = None,
) -> MetaNotifyState:
    state.last_signature = signature
    state.last_sent_ms = timestamp_ms or now_ms()
    return state


def meta_action_ko(action: str) -> str:
    return {
        "trend_long": "추세 롱",
        "trend_short": "추세 숏",
        "range_long": "횡보 하단 롱",
        "range_short": "횡보 상단 숏",
        "breakout_long": "상방 돌파 롱",
        "breakout_short": "하방 돌파 숏",
        "no_trade": "관망",
    }.get(action, action)


def meta_regime_ko(regime: str) -> str:
    return {
        "trend_up": "상승 추세",
        "trend_down": "하락 추세",
        "range": "횡보",
        "breakout_up": "상방 돌파",
        "breakout_down": "하방 돌파",
        "panic": "변동성 급등",
        "mixed": "혼합/애매",
        "warmup": "준비",
    }.get(regime, regime)


def _features_at(
    *,
    symbol: str,
    interval_seconds: int,
    klines: list[Kline],
    index: int,
    closes: list[float] | None,
    fast_ema: list[float] | None,
    slow_ema: list[float] | None,
) -> MarketFeatureSnapshot:
    closes = closes or [row.close for row in klines]
    row = klines[index]
    bars_1h = max(1, round(3600 / interval_seconds))
    bars_4h = bars_1h * 4
    bars_24h = bars_1h * 24
    fast = fast_ema or ema_series(closes, 20)
    slow = slow_ema or ema_series(closes, 80)
    ema_gap = ((fast[index] / slow[index]) - 1.0) * 10_000.0 if slow[index] > 0 else 0.0
    ema_slope = _return_bps(fast, index, bars_1h)
    bb_pos, bb_width = _bollinger_at(closes, index, period=20)
    return MarketFeatureSnapshot(
        symbol=symbol,
        timestamp_ms=row.close_time,
        close=row.close,
        ema_gap_bps=ema_gap,
        ema_slope_bps=ema_slope,
        trend_1h_bps=_return_bps(closes, index, bars_1h),
        trend_4h_bps=_return_bps(closes, index, bars_4h),
        trend_24h_bps=_return_bps(closes, index, bars_24h),
        rsi14=_rsi_at(closes, index, period=14),
        bollinger_position=bb_pos,
        bollinger_width_bps=bb_width,
        atr_bps=_atr_bps_at(klines, index, lookback=14),
        realized_vol_bps=_realized_vol_bps_at(closes, index, lookback=20),
        volume_ratio=_volume_ratio_at(klines, index, lookback=20),
        high_breakout=_high_breakout_at(klines, index, lookback=48),
        low_breakout=_low_breakout_at(klines, index, lookback=48),
    )


def _decision(
    features: MarketFeatureSnapshot,
    regime: str,
    action: str,
    side: SignalSide,
    config: TradingConfig,
    interval_seconds: int,
    confidence: float,
    reason: str,
) -> MetaDecision:
    tp, sl, hold_seconds = _profile_for_action(action, config, features)
    return MetaDecision(
        timestamp_ms=features.timestamp_ms,
        symbol=features.symbol,
        regime=regime,
        action=action,
        side=side,
        take_profit_bps=tp,
        stop_loss_bps=sl,
        max_hold_bars=max(1, math.ceil(hold_seconds / interval_seconds)),
        confidence=confidence,
        reason=reason,
        features=features,
    )


def _profile_for_action(
    action: str,
    config: TradingConfig,
    features: MarketFeatureSnapshot,
) -> tuple[float, float, float]:
    atr = max(features.atr_bps, 1.0)
    if action.startswith("trend"):
        return (
            max(config.trend_take_profit_bps, atr * 3.6),
            max(config.trend_stop_loss_bps, atr * 1.4),
            config.trend_max_hold_seconds,
        )
    if action.startswith("range"):
        return (
            max(config.range_take_profit_bps, atr * 1.3),
            max(config.range_stop_loss_bps, atr * 0.9),
            config.range_max_hold_seconds,
        )
    if action.startswith("breakout"):
        return (
            max(config.breakout_take_profit_bps, atr * 4.5),
            max(config.breakout_stop_loss_bps, atr * 1.7),
            config.breakout_max_hold_seconds,
        )
    return 0.0, 0.0, 1.0


def _build_trade(
    *,
    decision: MetaDecision,
    entry_time_ms: int,
    exit_time_ms: int,
    entry_price: float,
    exit_price: float,
    quantity: float,
    notional: float,
    fee_rate: float,
    reason: str,
    hold_bars: int,
) -> MetaTrade:
    side = decision.side
    gross = (
        quantity * (exit_price - entry_price)
        if side == "long"
        else quantity * (entry_price - exit_price)
    )
    entry_fee = notional * fee_rate
    exit_fee = abs(quantity * exit_price) * fee_rate
    pnl = gross - entry_fee - exit_fee
    pnl_bps = (pnl / notional) * 10_000.0 if notional > 0 else 0.0
    return MetaTrade(
        symbol=decision.symbol,
        regime=decision.regime,
        action=decision.action,
        side=side,
        entry_time_ms=entry_time_ms,
        exit_time_ms=exit_time_ms,
        entry_price=entry_price,
        exit_price=exit_price,
        notional=notional,
        pnl=pnl,
        pnl_bps=pnl_bps,
        reason=reason,
        hold_bars=hold_bars,
        take_profit_bps=decision.take_profit_bps,
        stop_loss_bps=decision.stop_loss_bps,
    )


def _summaries_by_key(
    trades: list[MetaTrade],
    key_fn,
) -> list[MetaActionSummary]:
    groups: dict[str, list[MetaTrade]] = {}
    for trade in trades:
        groups.setdefault(str(key_fn(trade)), []).append(trade)
    summaries = [_summary_for_group(key, rows) for key, rows in groups.items()]
    return sorted(summaries, key=lambda item: item.avg_pnl_bps, reverse=True)


def _summary_for_group(key: str, trades: list[MetaTrade]) -> MetaActionSummary:
    pnls = [trade.pnl for trade in trades]
    pnl_bps_values = [trade.pnl_bps for trade in trades]
    wins = [value for value in pnls if value > 0]
    losses = [value for value in pnls if value <= 0]
    gross_loss = abs(sum(losses))
    profit_factor = sum(wins) / gross_loss if gross_loss > 0 else (999.0 if wins else 0.0)
    return MetaActionSummary(
        key=key,
        count=len(trades),
        win_rate=len(wins) / len(trades) if trades else 0.0,
        avg_pnl_bps=statistics.fmean(pnl_bps_values) if pnl_bps_values else 0.0,
        sum_pnl=sum(pnls),
        profit_factor=profit_factor,
    )


def _meta_result_decision(
    *,
    trade_count: int,
    avg_pnl_bps: float,
    profit_factor: float,
    payoff_ratio: float,
    max_drawdown: float,
    max_consecutive_loss: int,
) -> tuple[str, str]:
    if trade_count < 30:
        return "OBSERVE", "거래 표본 30회 미만"
    if (
        avg_pnl_bps >= 1.0
        and profit_factor >= 1.15
        and payoff_ratio >= 0.85
        and max_drawdown <= 0.12
        and max_consecutive_loss < 10
    ):
        return "PAPER_READY", "메타정책을 paper 장기 관찰 후보로 승격 가능"
    reasons = []
    if avg_pnl_bps < 0:
        reasons.append("평균 손익 음수")
    elif avg_pnl_bps < 1.0:
        reasons.append("평균 수익폭 부족")
    if profit_factor < 1.15:
        reasons.append("PF 1.15 미만")
    if payoff_ratio < 0.85:
        reasons.append("손익비 부족")
    if max_drawdown > 0.12:
        reasons.append("드로다운 과다")
    if max_consecutive_loss >= 10:
        reasons.append("연속 손실 과다")
    return "BLOCKED", ", ".join(reasons) or "기준 미달"


def _meta_result_line(row: MetaBacktestResult) -> str:
    return (
        "- "
        f"{row.symbol} {row.decision} n={row.trade_count} "
        f"승률={row.win_rate:.1%} 평균={row.avg_pnl_bps:+.2f}bps "
        f"PF={row.profit_factor:.2f} 손익비={row.payoff_ratio:.2f} "
        f"MDD={row.max_drawdown_pct:.1%} 연손={row.max_consecutive_loss} "
        f"표본봉={row.sample_bars} - {row.reason}"
    )


def _meta_signature(results: list[MetaBacktestResult]) -> str:
    rows = [
        {
            "symbol": row.symbol,
            "decision": row.decision,
            "avg_pnl_bps": round(row.avg_pnl_bps, 2),
            "trade_count": row.trade_count,
            "best_action": row.action_summaries[0].key if row.action_summaries else "",
        }
        for row in sorted(results, key=lambda item: item.symbol)
    ]
    return json.dumps(rows, ensure_ascii=False, sort_keys=True)


def _meta_result_from_dict(row: dict[str, Any]) -> MetaBacktestResult:
    return MetaBacktestResult(
        symbol=str(row.get("symbol", "")),
        interval=str(row.get("interval", "")),
        start_ms=int(row.get("start_ms", 0) or 0),
        end_ms=int(row.get("end_ms", 0) or 0),
        sample_bars=int(row.get("sample_bars", 0) or 0),
        source_files=int(row.get("source_files", 0) or 0),
        missing_files=int(row.get("missing_files", 0) or 0),
        trade_count=int(row.get("trade_count", 0) or 0),
        win_rate=float(row.get("win_rate", 0.0) or 0.0),
        avg_pnl_bps=float(row.get("avg_pnl_bps", 0.0) or 0.0),
        sum_pnl=float(row.get("sum_pnl", 0.0) or 0.0),
        sum_pnl_bps=float(row.get("sum_pnl_bps", 0.0) or 0.0),
        max_drawdown_pct=float(row.get("max_drawdown_pct", 0.0) or 0.0),
        profit_factor=float(row.get("profit_factor", 0.0) or 0.0),
        payoff_ratio=float(row.get("payoff_ratio", 0.0) or 0.0),
        max_consecutive_loss=int(row.get("max_consecutive_loss", 0) or 0),
        decision=str(row.get("decision", "")),
        reason=str(row.get("reason", "")),
        action_summaries=[
            _action_summary_from_dict(item) for item in row.get("action_summaries", []) or []
        ],
        regime_summaries=[
            _action_summary_from_dict(item) for item in row.get("regime_summaries", []) or []
        ],
        recent_trades=[],
        recent_decisions=[],
    )


def _action_summary_from_dict(row: dict[str, Any]) -> MetaActionSummary:
    return MetaActionSummary(
        key=str(row.get("key", "")),
        count=int(row.get("count", 0) or 0),
        win_rate=float(row.get("win_rate", 0.0) or 0.0),
        avg_pnl_bps=float(row.get("avg_pnl_bps", 0.0) or 0.0),
        sum_pnl=float(row.get("sum_pnl", 0.0) or 0.0),
        profit_factor=float(row.get("profit_factor", 0.0) or 0.0),
    )


def _interval_seconds(interval: str) -> int:
    unit = interval[-1]
    value = int(interval[:-1])
    if unit == "m":
        return value * 60
    if unit == "h":
        return value * 3600
    if unit == "d":
        return value * 86_400
    raise ValueError(f"unsupported interval: {interval}")


def _return_bps(values: list[float], index: int, lookback: int) -> float:
    if index < lookback or values[index - lookback] <= 0:
        return 0.0
    return ((values[index] / values[index - lookback]) - 1.0) * 10_000.0


def _rsi_at(values: list[float], index: int, *, period: int = 14) -> float | None:
    if index < period:
        return None
    gains = 0.0
    losses = 0.0
    for previous, current in zip(values[index - period : index], values[index - period + 1 : index + 1]):
        change = current - previous
        if change >= 0:
            gains += change
        else:
            losses += abs(change)
    avg_gain = gains / period
    avg_loss = losses / period
    if avg_loss == 0:
        return 100.0 if avg_gain > 0 else 50.0
    rs = avg_gain / avg_loss
    return 100.0 - (100.0 / (1.0 + rs))


def _bollinger_at(
    values: list[float],
    index: int,
    *,
    period: int = 20,
    width: float = 2.0,
) -> tuple[float | None, float]:
    if index + 1 < period:
        return None, 0.0
    window = values[index - period + 1 : index + 1]
    mid = sum(window) / len(window)
    deviation = statistics.pstdev(window)
    upper = mid + (deviation * width)
    lower = mid - (deviation * width)
    band_width = upper - lower
    close = values[index]
    width_bps = (band_width / close) * 10_000.0 if close > 0 else 0.0
    if band_width <= 0:
        return None, width_bps
    return (close - lower) / band_width, width_bps


def _atr_bps_at(klines: list[Kline], index: int, *, lookback: int = 14) -> float:
    if index < 1:
        return 0.0
    start = max(1, index - lookback + 1)
    true_ranges = []
    for row_index in range(start, index + 1):
        row = klines[row_index]
        previous_close = klines[row_index - 1].close
        true_ranges.append(
            max(row.high - row.low, abs(row.high - previous_close), abs(row.low - previous_close))
        )
    close = klines[index].close
    return (sum(true_ranges) / len(true_ranges) / close) * 10_000.0 if close > 0 and true_ranges else 0.0


def _realized_vol_bps_at(values: list[float], index: int, *, lookback: int = 20) -> float:
    if index < lookback:
        return 0.0
    returns = [
        ((values[pos] / values[pos - 1]) - 1.0) * 10_000.0
        for pos in range(index - lookback + 1, index + 1)
        if values[pos - 1] > 0
    ]
    if len(returns) < 2:
        return 0.0
    return statistics.pstdev(returns)


def _volume_ratio_at(klines: list[Kline], index: int, *, lookback: int = 20) -> float:
    if index < 2:
        return 0.0
    start = max(0, index - lookback)
    rows = klines[start:index]
    average = sum(row.volume for row in rows) / len(rows) if rows else 0.0
    return klines[index].volume / average if average > 0 else 0.0


def _high_breakout_at(klines: list[Kline], index: int, *, lookback: int = 48) -> bool:
    if index <= lookback:
        return False
    prior = klines[index - lookback : index]
    return klines[index].close > max(row.high for row in prior)


def _low_breakout_at(klines: list[Kline], index: int, *, lookback: int = 48) -> bool:
    if index <= lookback:
        return False
    prior = klines[index - lookback : index]
    return klines[index].close < min(row.low for row in prior)


def _target_price(entry_price: float, side: str, bps: float) -> float:
    ratio = bps / 10_000.0
    return entry_price * (1.0 + ratio) if side == "long" else entry_price * (1.0 - ratio)


def _stop_price(entry_price: float, side: str, bps: float) -> float:
    ratio = bps / 10_000.0
    return entry_price * (1.0 - ratio) if side == "long" else entry_price * (1.0 + ratio)


def _slipped_entry_price(price: float, side: str, slippage_bps: float) -> float:
    ratio = slippage_bps / 10_000.0
    return price * (1.0 + ratio) if side == "long" else price * (1.0 - ratio)


def _slipped_exit_price(price: float, side: str, slippage_bps: float) -> float:
    ratio = slippage_bps / 10_000.0
    return price * (1.0 - ratio) if side == "long" else price * (1.0 + ratio)
