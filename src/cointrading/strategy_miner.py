from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import json
from pathlib import Path
import statistics
from typing import Any, Callable, Iterable

from cointrading.config import TradingConfig
from cointrading.historical_data import HistoricalKlineResult
from cointrading.meta_strategy import (
    MarketFeatureSnapshot,
    MetaTrade,
    _build_trade,
    _features_at,
    _interval_seconds,
)
from cointrading.indicators import ema_series
from cointrading.models import Kline, SignalSide
from cointrading.storage import kst_from_ms, now_ms


@dataclass(frozen=True)
class RuleCondition:
    min_ema_gap_bps: float | None = None
    max_ema_gap_bps: float | None = None
    min_ema_slope_bps: float | None = None
    max_ema_slope_bps: float | None = None
    min_trend_4h_bps: float | None = None
    max_trend_4h_bps: float | None = None
    min_trend_24h_bps: float | None = None
    max_trend_24h_bps: float | None = None
    min_rsi14: float | None = None
    max_rsi14: float | None = None
    min_bollinger_position: float | None = None
    max_bollinger_position: float | None = None
    min_volume_ratio: float | None = None
    max_atr_bps: float | None = None
    require_high_breakout: bool = False
    require_low_breakout: bool = False


@dataclass(frozen=True)
class CandidateRule:
    rule_id: str
    action: str
    side: SignalSide
    condition: RuleCondition
    take_profit_bps: float
    stop_loss_bps: float
    max_hold_bars: int


@dataclass(frozen=True)
class TradeSummary:
    count: int
    win_rate: float
    avg_pnl_bps: float
    sum_pnl: float
    profit_factor: float
    payoff_ratio: float
    max_drawdown_pct: float
    max_consecutive_loss: int


@dataclass(frozen=True)
class MinedStrategyResult:
    symbol: str
    interval: str
    rule_id: str
    action: str
    side: SignalSide
    condition: RuleCondition
    take_profit_bps: float
    stop_loss_bps: float
    max_hold_bars: int
    full_summary: TradeSummary
    selected_windows: int
    positive_test_windows: int
    test_summary: TradeSummary
    decision: str
    reason: str


def default_strategy_mine_report_path() -> Path:
    return Path(__file__).resolve().parents[2] / "data" / "strategy_mine_latest.json"


def mine_history_for_strategies(
    *,
    history: HistoricalKlineResult,
    config: TradingConfig,
    notional: float | None = None,
    train_months: int = 6,
    test_months: int = 1,
    top_limit: int = 30,
) -> list[MinedStrategyResult]:
    if len(history.klines) < 300:
        return []
    order_notional = notional if notional is not None else config.strategy_order_notional
    features = _feature_series(history.symbol, history.interval, history.klines)
    candidates = default_candidate_rules(history.interval, config)
    windows = _walk_forward_windows(history.klines, train_months=train_months, test_months=test_months)
    results = [
        _evaluate_candidate_walk_forward(
            history=history,
            features=features,
            rule=rule,
            config=config,
            notional=order_notional,
            windows=windows,
        )
        for rule in candidates
    ]
    ranked = _dedupe_similar_results(sorted(results, key=_result_rank_key))
    return ranked[:top_limit]


def default_candidate_rules(interval: str, config: TradingConfig) -> list[CandidateRule]:
    interval_seconds = _interval_seconds(interval)
    bars_per_hour = max(1, round(3600 / interval_seconds))
    rules: list[CandidateRule] = []

    def hold_bars(hours: int) -> int:
        return max(1, hours * bars_per_hour)

    trend_profiles = [
        (config.trend_take_profit_bps, config.trend_stop_loss_bps, hold_bars(24)),
        (140.0, 45.0, hold_bars(36)),
        (200.0, 70.0, hold_bars(48)),
    ]
    breakout_profiles = [
        (config.breakout_take_profit_bps, config.breakout_stop_loss_bps, hold_bars(12)),
        (180.0, 60.0, hold_bars(24)),
        (260.0, 90.0, hold_bars(36)),
    ]
    range_profiles = [
        (config.range_take_profit_bps, config.range_stop_loss_bps, hold_bars(8)),
        (50.0, 25.0, hold_bars(12)),
        (80.0, 40.0, hold_bars(18)),
    ]

    for profile_index, (tp, sl, hold) in enumerate(breakout_profiles):
        for volume in (1.3, 1.6, 2.0):
            for trend_4h in (0.0, 25.0):
                for trend_24h in (0.0, 60.0):
                    for bb_min in (0.80, 0.90):
                        rules.append(
                            CandidateRule(
                                rule_id=(
                                    f"breakout_long_p{profile_index}_v{volume}_"
                                    f"t4{trend_4h}_t24{trend_24h}_bb{bb_min}"
                                ),
                                action="breakout_long",
                                side="long",
                                condition=RuleCondition(
                                    min_trend_4h_bps=trend_4h,
                                    min_trend_24h_bps=trend_24h,
                                    min_bollinger_position=bb_min,
                                    min_volume_ratio=volume,
                                    max_atr_bps=260.0,
                                    require_high_breakout=True,
                                ),
                                take_profit_bps=tp,
                                stop_loss_bps=sl,
                                max_hold_bars=hold,
                            )
                        )
                        rules.append(
                            CandidateRule(
                                rule_id=(
                                    f"breakout_short_p{profile_index}_v{volume}_"
                                    f"t4{trend_4h}_t24{trend_24h}_bb{1.0 - bb_min:.2f}"
                                ),
                                action="breakout_short",
                                side="short",
                                condition=RuleCondition(
                                    max_trend_4h_bps=-trend_4h,
                                    max_trend_24h_bps=-trend_24h,
                                    max_bollinger_position=1.0 - bb_min,
                                    min_volume_ratio=volume,
                                    max_atr_bps=260.0,
                                    require_low_breakout=True,
                                ),
                                take_profit_bps=tp,
                                stop_loss_bps=sl,
                                max_hold_bars=hold,
                            )
                        )

    for profile_index, (tp, sl, hold) in enumerate(trend_profiles):
        for ema_gap in (8.0, 20.0):
            for slope in (1.0, 4.0):
                for trend_4h in (20.0, 50.0):
                    for trend_24h in (35.0, 90.0):
                        rules.append(
                            CandidateRule(
                                rule_id=(
                                    f"trend_long_p{profile_index}_ema{ema_gap}_"
                                    f"s{slope}_t4{trend_4h}_t24{trend_24h}"
                                ),
                                action="trend_long",
                                side="long",
                                condition=RuleCondition(
                                    min_ema_gap_bps=ema_gap,
                                    min_ema_slope_bps=slope,
                                    min_trend_4h_bps=trend_4h,
                                    min_trend_24h_bps=trend_24h,
                                    min_rsi14=40.0,
                                    max_rsi14=72.0,
                                    min_bollinger_position=0.25,
                                    max_bollinger_position=0.92,
                                    max_atr_bps=220.0,
                                ),
                                take_profit_bps=tp,
                                stop_loss_bps=sl,
                                max_hold_bars=hold,
                            )
                        )
                        rules.append(
                            CandidateRule(
                                rule_id=(
                                    f"trend_short_p{profile_index}_ema{ema_gap}_"
                                    f"s{slope}_t4{trend_4h}_t24{trend_24h}"
                                ),
                                action="trend_short",
                                side="short",
                                condition=RuleCondition(
                                    max_ema_gap_bps=-ema_gap,
                                    max_ema_slope_bps=-slope,
                                    max_trend_4h_bps=-trend_4h,
                                    max_trend_24h_bps=-trend_24h,
                                    min_rsi14=28.0,
                                    max_rsi14=60.0,
                                    min_bollinger_position=0.08,
                                    max_bollinger_position=0.75,
                                    max_atr_bps=220.0,
                                ),
                                take_profit_bps=tp,
                                stop_loss_bps=sl,
                                max_hold_bars=hold,
                            )
                        )

    for profile_index, (tp, sl, hold) in enumerate(range_profiles):
        for max_trend in (18.0, 30.0):
            for rsi_edge, bb_edge in ((30.0, 0.15), (35.0, 0.25)):
                rules.append(
                    CandidateRule(
                        rule_id=f"range_long_p{profile_index}_t{max_trend}_r{rsi_edge}_bb{bb_edge}",
                        action="range_long",
                        side="long",
                        condition=RuleCondition(
                            min_ema_gap_bps=-max_trend,
                            max_ema_gap_bps=max_trend,
                            min_trend_4h_bps=-max_trend,
                            max_trend_4h_bps=max_trend,
                            max_rsi14=rsi_edge,
                            max_bollinger_position=bb_edge,
                            max_atr_bps=160.0,
                        ),
                        take_profit_bps=tp,
                        stop_loss_bps=sl,
                        max_hold_bars=hold,
                    )
                )
                rules.append(
                    CandidateRule(
                        rule_id=f"range_short_p{profile_index}_t{max_trend}_r{100-rsi_edge}_bb{1-bb_edge:.2f}",
                        action="range_short",
                        side="short",
                        condition=RuleCondition(
                            min_ema_gap_bps=-max_trend,
                            max_ema_gap_bps=max_trend,
                            min_trend_4h_bps=-max_trend,
                            max_trend_4h_bps=max_trend,
                            min_rsi14=100.0 - rsi_edge,
                            min_bollinger_position=1.0 - bb_edge,
                            max_atr_bps=160.0,
                        ),
                        take_profit_bps=tp,
                        stop_loss_bps=sl,
                        max_hold_bars=hold,
                    )
                )
    return rules


def strategy_mine_text(results: list[MinedStrategyResult], *, generated_ms: int | None = None, limit: int = 10) -> str:
    lines = [
        "전략 발굴 리포트",
        "과거 구간에서 고른 규칙이 다음 구간에서도 먹혔는지 walk-forward로 본 결과입니다.",
    ]
    if generated_ms:
        lines.append(f"생성시각: {kst_from_ms(generated_ms)}")
    if not results:
        lines.append("결과 없음")
        return "\n".join(lines)
    survived = [row for row in results if row.decision == "SURVIVED"]
    watch = [row for row in results if row.decision == "WATCH"]
    rejected = [row for row in results if row.decision == "REJECTED"]
    lines.append(f"요약: 생존 {len(survived)}개, 관찰 {len(watch)}개, 탈락 {len(rejected)}개")
    lines.append("")
    for row in results[:limit]:
        test = row.test_summary
        full = row.full_summary
        lines.append(
            "- "
            f"{row.decision} {row.symbol} {strategy_action_ko(row.action)} "
            f"TP={row.take_profit_bps:.0f}/SL={row.stop_loss_bps:.0f}/H={row.max_hold_bars}봉 "
            f"WF={row.positive_test_windows}/{row.selected_windows} "
            f"테스트 n={test.count} 평균={test.avg_pnl_bps:+.2f}bps PF={test.profit_factor:.2f} "
            f"전체 평균={full.avg_pnl_bps:+.2f}bps - {row.reason}"
        )
    return "\n".join(lines)


def write_strategy_mine_report(
    path: Path,
    *,
    results: list[MinedStrategyResult],
    symbols: Iterable[str],
    interval: str,
    start_date: str,
    end_date: str,
    train_months: int,
    test_months: int,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "generated_ms": now_ms(),
        "symbols": [symbol.upper() for symbol in symbols],
        "interval": interval,
        "start_date": start_date,
        "end_date": end_date,
        "train_months": train_months,
        "test_months": test_months,
        "results": [asdict(row) for row in results],
    }
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def load_strategy_mine_report(path: Path | None = None) -> dict[str, Any] | None:
    report_path = path or default_strategy_mine_report_path()
    if not report_path.exists():
        return None
    return json.loads(report_path.read_text(encoding="utf-8"))


def strategy_mine_report_text(path: Path | None = None, *, limit: int = 10) -> str:
    payload = load_strategy_mine_report(path)
    if payload is None:
        return "전략 발굴 결과가 아직 없습니다. 먼저 strategy-mine을 실행해야 합니다."
    results = [_result_from_dict(row) for row in payload.get("results", []) or []]
    return strategy_mine_text(results, generated_ms=int(payload.get("generated_ms", 0) or 0), limit=limit)


def strategy_action_ko(action: str) -> str:
    return {
        "trend_long": "추세 롱",
        "trend_short": "추세 숏",
        "breakout_long": "상방 돌파 롱",
        "breakout_short": "하방 돌파 숏",
        "range_long": "횡보 하단 롱",
        "range_short": "횡보 상단 숏",
    }.get(action, action)


def _evaluate_candidate_walk_forward(
    *,
    history: HistoricalKlineResult,
    features: list[MarketFeatureSnapshot | None],
    rule: CandidateRule,
    config: TradingConfig,
    notional: float,
    windows: list[tuple[int, int, int, int]],
) -> MinedStrategyResult:
    full_trades = _backtest_rule(
        symbol=history.symbol,
        klines=history.klines,
        features=features,
        rule=rule,
        config=config,
        notional=notional,
        start_ms=history.klines[0].open_time,
        end_ms=history.klines[-1].close_time,
    )
    selected_windows = 0
    positive_test_windows = 0
    all_test_trades: list[MetaTrade] = []
    for train_start, train_end, test_start, test_end in windows:
        train_trades = _backtest_rule(
            symbol=history.symbol,
            klines=history.klines,
            features=features,
            rule=rule,
            config=config,
            notional=notional,
            start_ms=train_start,
            end_ms=train_end,
        )
        train_summary = _summarize_trades(train_trades, config.initial_equity)
        if not _passes_train_filter(train_summary):
            continue
        selected_windows += 1
        test_trades = _backtest_rule(
            symbol=history.symbol,
            klines=history.klines,
            features=features,
            rule=rule,
            config=config,
            notional=notional,
            start_ms=test_start,
            end_ms=test_end,
        )
        test_summary = _summarize_trades(test_trades, config.initial_equity)
        if test_summary.count > 0 and test_summary.avg_pnl_bps > 0.0:
            positive_test_windows += 1
        all_test_trades.extend(test_trades)
    full_summary = _summarize_trades(full_trades, config.initial_equity)
    test_summary = _summarize_trades(all_test_trades, config.initial_equity)
    decision, reason = _mined_decision(
        full_summary=full_summary,
        test_summary=test_summary,
        selected_windows=selected_windows,
        positive_test_windows=positive_test_windows,
    )
    return MinedStrategyResult(
        symbol=history.symbol,
        interval=history.interval,
        rule_id=rule.rule_id,
        action=rule.action,
        side=rule.side,
        condition=rule.condition,
        take_profit_bps=rule.take_profit_bps,
        stop_loss_bps=rule.stop_loss_bps,
        max_hold_bars=rule.max_hold_bars,
        full_summary=full_summary,
        selected_windows=selected_windows,
        positive_test_windows=positive_test_windows,
        test_summary=test_summary,
        decision=decision,
        reason=reason,
    )


def _backtest_rule(
    *,
    symbol: str,
    klines: list[Kline],
    features: list[MarketFeatureSnapshot | None],
    rule: CandidateRule,
    config: TradingConfig,
    notional: float,
    start_ms: int,
    end_ms: int,
) -> list[MetaTrade]:
    trades: list[MetaTrade] = []
    active: dict[str, Any] | None = None
    cooldown_until = 0
    for index, bar in enumerate(klines):
        if bar.open_time < start_ms:
            continue
        if bar.open_time > end_ms:
            break
        if active is None:
            if index < cooldown_until or index + 1 >= len(klines):
                continue
            feature = features[index]
            if feature is None or not _matches(rule.condition, feature):
                continue
            entry_bar = klines[index + 1]
            entry_price = _slipped_entry_price(entry_bar.open, rule.side, config.slippage_bps)
            active = {
                "entry_index": index + 1,
                "entry_time_ms": entry_bar.open_time,
                "entry_price": entry_price,
                "quantity": notional / entry_price if entry_price > 0 else 0.0,
            }
            continue
        side = rule.side
        entry_index = int(active["entry_index"])
        entry_price = float(active["entry_price"])
        quantity = float(active["quantity"])
        hold_bars = index - entry_index
        target_price = _target_price(entry_price, side, rule.take_profit_bps)
        stop_price = _stop_price(entry_price, side, rule.stop_loss_bps)
        reason = ""
        exit_price = 0.0
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
        elif hold_bars >= rule.max_hold_bars:
            exit_price = _slipped_exit_price(bar.close, side, config.slippage_bps)
            reason = "max_hold"
        if reason:
            trades.append(
                _build_trade(
                    decision=_decision_like(symbol, rule, int(active["entry_time_ms"])),
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
            cooldown_until = index + 4
    return trades


def _feature_series(
    symbol: str,
    interval: str,
    klines: list[Kline],
) -> list[MarketFeatureSnapshot | None]:
    interval_seconds = _interval_seconds(interval)
    closes = [row.close for row in klines]
    fast = ema_series(closes, 20)
    slow = ema_series(closes, 80)
    output: list[MarketFeatureSnapshot | None] = []
    for index in range(len(klines)):
        if index < 100:
            output.append(None)
            continue
        output.append(
            _features_at(
                symbol=symbol,
                interval_seconds=interval_seconds,
                klines=klines,
                index=index,
                closes=closes,
                fast_ema=fast,
                slow_ema=slow,
            )
        )
    return output


def _matches(condition: RuleCondition, feature: MarketFeatureSnapshot) -> bool:
    if condition.require_high_breakout and not feature.high_breakout:
        return False
    if condition.require_low_breakout and not feature.low_breakout:
        return False
    checks: list[tuple[float | None, float, Callable[[float, float], bool]]] = [
        (condition.min_ema_gap_bps, feature.ema_gap_bps, lambda value, bound: value >= bound),
        (condition.max_ema_gap_bps, feature.ema_gap_bps, lambda value, bound: value <= bound),
        (condition.min_ema_slope_bps, feature.ema_slope_bps, lambda value, bound: value >= bound),
        (condition.max_ema_slope_bps, feature.ema_slope_bps, lambda value, bound: value <= bound),
        (condition.min_trend_4h_bps, feature.trend_4h_bps, lambda value, bound: value >= bound),
        (condition.max_trend_4h_bps, feature.trend_4h_bps, lambda value, bound: value <= bound),
        (condition.min_trend_24h_bps, feature.trend_24h_bps, lambda value, bound: value >= bound),
        (condition.max_trend_24h_bps, feature.trend_24h_bps, lambda value, bound: value <= bound),
        (condition.min_volume_ratio, feature.volume_ratio, lambda value, bound: value >= bound),
        (condition.max_atr_bps, feature.atr_bps, lambda value, bound: value <= bound),
    ]
    for bound, value, predicate in checks:
        if bound is not None and not predicate(value, bound):
            return False
    if not _optional_between(feature.rsi14, condition.min_rsi14, condition.max_rsi14):
        return False
    if not _optional_between(
        feature.bollinger_position,
        condition.min_bollinger_position,
        condition.max_bollinger_position,
    ):
        return False
    return True


def _optional_between(value: float | None, minimum: float | None, maximum: float | None) -> bool:
    if minimum is None and maximum is None:
        return True
    if value is None:
        return False
    if minimum is not None and value < minimum:
        return False
    if maximum is not None and value > maximum:
        return False
    return True


def _passes_train_filter(summary: TradeSummary) -> bool:
    return (
        summary.count >= 12
        and summary.avg_pnl_bps >= 2.0
        and summary.profit_factor >= 1.12
        and summary.max_drawdown_pct <= 0.08
    )


def _mined_decision(
    *,
    full_summary: TradeSummary,
    test_summary: TradeSummary,
    selected_windows: int,
    positive_test_windows: int,
) -> tuple[str, str]:
    positive_rate = positive_test_windows / selected_windows if selected_windows else 0.0
    if (
        selected_windows >= 3
        and test_summary.count >= 20
        and test_summary.avg_pnl_bps > 0.0
        and test_summary.profit_factor >= 1.05
        and positive_rate >= 0.50
        and full_summary.avg_pnl_bps > 0.0
    ):
        return "SURVIVED", "walk-forward 테스트에서도 양수"
    if selected_windows >= 2 and test_summary.count >= 10 and test_summary.avg_pnl_bps > 0.0:
        return "WATCH", "후보 가능성은 있으나 견고성 부족"
    reasons = []
    if selected_windows == 0:
        reasons.append("훈련 구간 통과 없음")
    elif positive_rate < 0.50:
        reasons.append("다음 구간 양수 비율 부족")
    if test_summary.avg_pnl_bps <= 0:
        reasons.append("walk-forward 평균 손익 음수")
    if test_summary.profit_factor < 1.05:
        reasons.append("walk-forward PF 부족")
    if full_summary.avg_pnl_bps <= 0:
        reasons.append("전체 구간 평균 손익 음수")
    return "REJECTED", ", ".join(reasons) or "기준 미달"


def _summarize_trades(trades: list[MetaTrade], initial_equity: float) -> TradeSummary:
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
    equity = initial_equity
    peak = initial_equity
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
    return TradeSummary(
        count=len(trades),
        win_rate=win_rate,
        avg_pnl_bps=avg_pnl_bps,
        sum_pnl=sum(pnls),
        profit_factor=profit_factor,
        payoff_ratio=payoff_ratio,
        max_drawdown_pct=max_drawdown,
        max_consecutive_loss=max_consecutive_loss,
    )


@dataclass(frozen=True)
class _DecisionLike:
    symbol: str
    regime: str
    action: str
    side: SignalSide
    take_profit_bps: float
    stop_loss_bps: float


def _decision_like(symbol: str, rule: CandidateRule, timestamp_ms: int) -> _DecisionLike:
    del timestamp_ms
    return _DecisionLike(
        symbol=symbol,
        regime="mined",
        action=rule.action,
        side=rule.side,
        take_profit_bps=rule.take_profit_bps,
        stop_loss_bps=rule.stop_loss_bps,
    )


def _walk_forward_windows(
    klines: list[Kline],
    *,
    train_months: int,
    test_months: int,
) -> list[tuple[int, int, int, int]]:
    if not klines:
        return []
    start_month = _month_start(_date_from_ms(klines[0].open_time))
    final_month = _month_start(_date_from_ms(klines[-1].open_time))
    windows = []
    current = start_month
    while True:
        train_start = current
        train_end_month = _add_months(train_start, train_months)
        test_end_month = _add_months(train_end_month, test_months)
        if train_end_month > final_month:
            break
        train_start_ms = _ms_from_datetime(train_start)
        train_end_ms = _ms_from_datetime(train_end_month) - 1
        test_start_ms = _ms_from_datetime(train_end_month)
        test_end_ms = _ms_from_datetime(test_end_month) - 1
        windows.append((train_start_ms, train_end_ms, test_start_ms, test_end_ms))
        current = _add_months(current, test_months)
    return windows


def _date_from_ms(timestamp_ms: int) -> datetime:
    return datetime.fromtimestamp(timestamp_ms / 1000.0, tz=timezone.utc)


def _month_start(value: datetime) -> datetime:
    return datetime(value.year, value.month, 1, tzinfo=timezone.utc)


def _add_months(value: datetime, months: int) -> datetime:
    year = value.year + ((value.month - 1 + months) // 12)
    month = ((value.month - 1 + months) % 12) + 1
    return datetime(year, month, 1, tzinfo=timezone.utc)


def _ms_from_datetime(value: datetime) -> int:
    return int(value.timestamp() * 1000)


def _result_rank_key(row: MinedStrategyResult) -> tuple[int, float, float, int]:
    decision_rank = {"SURVIVED": 0, "WATCH": 1, "REJECTED": 2}.get(row.decision, 3)
    positive_rate = row.positive_test_windows / row.selected_windows if row.selected_windows else 0.0
    return (
        decision_rank,
        -row.test_summary.avg_pnl_bps,
        -positive_rate,
        -row.test_summary.count,
    )


def _dedupe_similar_results(results: list[MinedStrategyResult]) -> list[MinedStrategyResult]:
    seen = set()
    output = []
    for row in results:
        key = (
            row.symbol,
            row.action,
            row.side,
            round(row.take_profit_bps, 3),
            round(row.stop_loss_bps, 3),
            row.max_hold_bars,
            row.selected_windows,
            row.positive_test_windows,
            row.test_summary.count,
            round(row.test_summary.avg_pnl_bps, 2),
        )
        if key in seen:
            continue
        seen.add(key)
        output.append(row)
    return output


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


def _result_from_dict(row: dict[str, Any]) -> MinedStrategyResult:
    return MinedStrategyResult(
        symbol=str(row.get("symbol", "")),
        interval=str(row.get("interval", "")),
        rule_id=str(row.get("rule_id", "")),
        action=str(row.get("action", "")),
        side=str(row.get("side", "flat")),  # type: ignore[arg-type]
        condition=RuleCondition(**(row.get("condition", {}) or {})),
        take_profit_bps=float(row.get("take_profit_bps", 0.0) or 0.0),
        stop_loss_bps=float(row.get("stop_loss_bps", 0.0) or 0.0),
        max_hold_bars=int(row.get("max_hold_bars", 0) or 0),
        full_summary=_summary_from_dict(row.get("full_summary", {}) or {}),
        selected_windows=int(row.get("selected_windows", 0) or 0),
        positive_test_windows=int(row.get("positive_test_windows", 0) or 0),
        test_summary=_summary_from_dict(row.get("test_summary", {}) or {}),
        decision=str(row.get("decision", "")),
        reason=str(row.get("reason", "")),
    )


def _summary_from_dict(row: dict[str, Any]) -> TradeSummary:
    return TradeSummary(
        count=int(row.get("count", 0) or 0),
        win_rate=float(row.get("win_rate", 0.0) or 0.0),
        avg_pnl_bps=float(row.get("avg_pnl_bps", 0.0) or 0.0),
        sum_pnl=float(row.get("sum_pnl", 0.0) or 0.0),
        profit_factor=float(row.get("profit_factor", 0.0) or 0.0),
        payoff_ratio=float(row.get("payoff_ratio", 0.0) or 0.0),
        max_drawdown_pct=float(row.get("max_drawdown_pct", 0.0) or 0.0),
        max_consecutive_loss=int(row.get("max_consecutive_loss", 0) or 0),
    )
