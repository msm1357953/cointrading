from __future__ import annotations

from dataclasses import asdict, dataclass
import json
from pathlib import Path
import statistics
from typing import Callable, Iterable

from cointrading.config import TradingConfig
from cointrading.exchange.binance_usdm import BinanceUSDMClient
from cointrading.indicators import bollinger_position, ema_series, rsi
from cointrading.models import Kline, SignalSide
from cointrading.storage import kst_from_ms, now_ms


@dataclass(frozen=True)
class ProbeStrategy:
    name: str
    label: str
    take_profit_bps: float
    stop_loss_bps: float
    max_hold_bars: int
    signal_fn: Callable[[list[Kline], int], SignalSide]


@dataclass(frozen=True)
class ProbeTrade:
    strategy: str
    symbol: str
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


@dataclass(frozen=True)
class ProbeResult:
    strategy: str
    strategy_label: str
    symbol: str
    interval: str
    sample_bars: int
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


def default_probe_report_path() -> Path:
    return Path(__file__).resolve().parents[2] / "data" / "vibe_probe_latest.json"


def default_probe_strategies(config: TradingConfig) -> tuple[ProbeStrategy, ...]:
    return (
        ProbeStrategy(
            name="trend_follow",
            label="추세추종",
            take_profit_bps=config.trend_take_profit_bps,
            stop_loss_bps=config.trend_stop_loss_bps,
            max_hold_bars=96,
            signal_fn=_trend_signal,
        ),
        ProbeStrategy(
            name="range_reversion",
            label="횡보 평균회귀",
            take_profit_bps=config.range_take_profit_bps,
            stop_loss_bps=config.range_stop_loss_bps,
            max_hold_bars=24,
            signal_fn=_range_signal,
        ),
        ProbeStrategy(
            name="breakout_reduced",
            label="돌파 추종",
            take_profit_bps=config.breakout_take_profit_bps,
            stop_loss_bps=config.breakout_stop_loss_bps,
            max_hold_bars=48,
            signal_fn=_breakout_signal,
        ),
    )


def run_vibe_style_probe(
    *,
    symbols: Iterable[str],
    interval: str = "15m",
    limit: int = 1000,
    notional: float | None = None,
    config: TradingConfig | None = None,
    client: BinanceUSDMClient | None = None,
) -> tuple[list[ProbeResult], list[ProbeTrade]]:
    config = config or TradingConfig.from_env()
    client = client or BinanceUSDMClient(config=config)
    order_notional = notional if notional is not None else config.strategy_order_notional
    results: list[ProbeResult] = []
    trades: list[ProbeTrade] = []
    for raw_symbol in symbols:
        symbol = raw_symbol.upper()
        klines = client.klines(symbol=symbol, interval=interval, limit=limit)
        for strategy in default_probe_strategies(config):
            strategy_trades = backtest_probe_strategy(
                symbol=symbol,
                interval=interval,
                klines=klines,
                strategy=strategy,
                config=config,
                notional=order_notional,
            )
            trades.extend(strategy_trades)
            results.append(
                summarize_probe_result(
                    symbol=symbol,
                    interval=interval,
                    sample_bars=len(klines),
                    strategy=strategy,
                    trades=strategy_trades,
                    initial_equity=config.initial_equity,
                )
            )
    return results, trades


def backtest_probe_strategy(
    *,
    symbol: str,
    interval: str,
    klines: list[Kline],
    strategy: ProbeStrategy,
    config: TradingConfig,
    notional: float,
) -> list[ProbeTrade]:
    del interval
    trades: list[ProbeTrade] = []
    active: dict[str, float | int | str] | None = None
    index = 0
    while index < len(klines):
        if active is None:
            side = strategy.signal_fn(klines, index)
            if side not in {"long", "short"} or index + 1 >= len(klines):
                index += 1
                continue
            entry_bar = klines[index + 1]
            entry_price = _slipped_entry_price(entry_bar.open, side, config.slippage_bps)
            active = {
                "side": side,
                "entry_index": index + 1,
                "entry_time_ms": entry_bar.open_time,
                "entry_price": entry_price,
                "quantity": notional / entry_price if entry_price > 0 else 0.0,
            }
            index += 2
            continue

        side = str(active["side"])
        entry_index = int(active["entry_index"])
        entry_price = float(active["entry_price"])
        quantity = float(active["quantity"])
        bar = klines[index]
        hold_bars = index - entry_index
        exit_price = 0.0
        reason = ""
        target_price = _target_price(entry_price, side, strategy.take_profit_bps)
        stop_price = _stop_price(entry_price, side, strategy.stop_loss_bps)

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
        elif hold_bars >= strategy.max_hold_bars:
            exit_price = _slipped_exit_price(bar.close, side, config.slippage_bps)
            reason = "max_hold"
        else:
            next_signal = strategy.signal_fn(klines, index)
            if next_signal in {"long", "short"} and next_signal != side:
                exit_price = _slipped_exit_price(bar.close, side, config.slippage_bps)
                reason = "signal_flip"

        if reason:
            trades.append(
                _build_trade(
                    strategy=strategy,
                    symbol=symbol,
                    side=side,
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
        index += 1
    if active is not None and klines:
        final_bar = klines[-1]
        side = str(active["side"])
        entry_index = int(active["entry_index"])
        entry_price = float(active["entry_price"])
        quantity = float(active["quantity"])
        trades.append(
            _build_trade(
                strategy=strategy,
                symbol=symbol,
                side=side,
                entry_time_ms=int(active["entry_time_ms"]),
                exit_time_ms=final_bar.close_time,
                entry_price=entry_price,
                exit_price=_slipped_exit_price(final_bar.close, side, config.slippage_bps),
                quantity=quantity,
                notional=notional,
                fee_rate=config.taker_fee_rate,
                reason="end_of_sample",
                hold_bars=max(len(klines) - 1 - entry_index, 0),
            )
        )
    return trades


def summarize_probe_result(
    *,
    symbol: str,
    interval: str,
    sample_bars: int,
    strategy: ProbeStrategy,
    trades: list[ProbeTrade],
    initial_equity: float,
) -> ProbeResult:
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

    decision, reason = _probe_decision(
        trade_count=len(trades),
        avg_pnl_bps=avg_pnl_bps,
        profit_factor=profit_factor,
        payoff_ratio=payoff_ratio,
        max_drawdown=max_drawdown,
        max_consecutive_loss=max_consecutive_loss,
    )
    return ProbeResult(
        strategy=strategy.name,
        strategy_label=strategy.label,
        symbol=symbol,
        interval=interval,
        sample_bars=sample_bars,
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
    )


def vibe_probe_text(results: list[ProbeResult], *, limit: int = 12) -> str:
    lines = [
        "Vibe 스타일 리서치 프로브",
        "실주문 없음: 공개 캔들 기반 폐쇄형 백테스트입니다.",
    ]
    if not results:
        lines.append("결과 없음")
        return "\n".join(lines)
    generated = kst_from_ms(now_ms())
    lines.append(f"생성시각: {generated}")
    approved = [row for row in results if row.decision == "APPROVED"]
    blocked = [row for row in results if row.decision == "BLOCKED"]
    watch = [row for row in results if row.decision == "WATCH"]
    lines.append(
        f"요약: 승인 {len(approved)}개, 관찰 {len(watch)}개, 차단 {len(blocked)}개"
    )
    if not approved:
        lines.append("결론: 지금 결과만 보면 실전 진입 근거가 없습니다.")
    else:
        best = sorted(approved, key=lambda row: row.avg_pnl_bps, reverse=True)[0]
        lines.append(
            "결론: 후보 있음 - "
            f"{best.symbol} {best.strategy_label} 평균 {best.avg_pnl_bps:+.2f}bps"
        )
    lines.append("")
    lines.append("상위/하위 결과")
    ranked = sorted(results, key=lambda row: row.avg_pnl_bps, reverse=True)
    for row in ranked[:limit]:
        lines.append(_probe_result_line(row))
    return "\n".join(lines)


def write_probe_report(
    path: Path,
    *,
    results: list[ProbeResult],
    trades: list[ProbeTrade],
    symbols: Iterable[str],
    interval: str,
    limit: int,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "generated_ms": now_ms(),
        "symbols": [symbol.upper() for symbol in symbols],
        "interval": interval,
        "limit": limit,
        "results": [asdict(row) for row in results],
        "recent_trades": [asdict(row) for row in trades[-200:]],
    }
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _probe_result_line(row: ProbeResult) -> str:
    return (
        "- "
        f"{row.decision} {row.symbol} {row.strategy_label} "
        f"n={row.trade_count} 승률={row.win_rate:.1%} "
        f"평균={row.avg_pnl_bps:+.2f}bps 합계={row.sum_pnl:+.4f} "
        f"PF={row.profit_factor:.2f} 손익비={row.payoff_ratio:.2f} "
        f"MDD={row.max_drawdown_pct:.1%} 연손={row.max_consecutive_loss} "
        f"- {row.reason}"
    )


def _build_trade(
    *,
    strategy: ProbeStrategy,
    symbol: str,
    side: str,
    entry_time_ms: int,
    exit_time_ms: int,
    entry_price: float,
    exit_price: float,
    quantity: float,
    notional: float,
    fee_rate: float,
    reason: str,
    hold_bars: int,
) -> ProbeTrade:
    gross = (
        quantity * (exit_price - entry_price)
        if side == "long"
        else quantity * (entry_price - exit_price)
    )
    entry_fee = notional * fee_rate
    exit_fee = abs(quantity * exit_price) * fee_rate
    pnl = gross - entry_fee - exit_fee
    pnl_bps = (pnl / notional) * 10_000.0 if notional > 0 else 0.0
    return ProbeTrade(
        strategy=strategy.name,
        symbol=symbol,
        side=side,  # type: ignore[arg-type]
        entry_time_ms=entry_time_ms,
        exit_time_ms=exit_time_ms,
        entry_price=entry_price,
        exit_price=exit_price,
        notional=notional,
        pnl=pnl,
        pnl_bps=pnl_bps,
        reason=reason,
        hold_bars=hold_bars,
    )


def _probe_decision(
    *,
    trade_count: int,
    avg_pnl_bps: float,
    profit_factor: float,
    payoff_ratio: float,
    max_drawdown: float,
    max_consecutive_loss: int,
) -> tuple[str, str]:
    if trade_count < 20:
        return "WATCH", "표본 20회 미만"
    if avg_pnl_bps >= 2.0 and profit_factor >= 1.2 and payoff_ratio >= 0.9 and max_drawdown <= 0.08:
        return "APPROVED", "평균수익/PF/드로다운 기준 통과"
    reasons = []
    if avg_pnl_bps < 0:
        reasons.append("평균 손익 음수")
    elif avg_pnl_bps < 2.0:
        reasons.append("평균 수익폭 부족")
    if profit_factor < 1.2:
        reasons.append("PF 1.2 미만")
    if payoff_ratio < 0.9:
        reasons.append("손익비 부족")
    if max_drawdown > 0.08:
        reasons.append("드로다운 과다")
    if max_consecutive_loss >= 8:
        reasons.append("연속 손실 과다")
    return "BLOCKED", ", ".join(reasons) or "승인 기준 미달"


def _trend_signal(klines: list[Kline], index: int) -> SignalSide:
    if index < 80:
        return "flat"
    closes = [row.close for row in klines[: index + 1]]
    fast = ema_series(closes, 20)[-1]
    slow = ema_series(closes, 80)[-1]
    base = closes[-17]
    momentum = ((closes[-1] / base) - 1.0) if base > 0 else 0.0
    if fast > slow and momentum > 0.001:
        return "long"
    if fast < slow and momentum < -0.001:
        return "short"
    return "flat"


def _range_signal(klines: list[Kline], index: int) -> SignalSide:
    if index < 96:
        return "flat"
    closes = [row.close for row in klines[: index + 1]]
    close = closes[-1]
    mean_96 = sum(closes[-96:]) / 96
    if mean_96 <= 0 or abs((close / mean_96) - 1.0) >= 0.012:
        return "flat"
    rsi14 = rsi(closes, period=14)
    bb_pos, _ = bollinger_position(closes, period=20, width=2.0)
    if rsi14 is None or bb_pos is None:
        return "flat"
    if rsi14 < 30 and bb_pos < 0.2:
        return "long"
    if rsi14 > 70 and bb_pos > 0.8:
        return "short"
    return "flat"


def _breakout_signal(klines: list[Kline], index: int) -> SignalSide:
    if index < 49:
        return "flat"
    window = klines[index - 48 : index]
    current = klines[index]
    high = max(row.high for row in window)
    low = min(row.low for row in window)
    avg_volume = sum(row.volume for row in window) / len(window)
    volume_ratio = current.volume / avg_volume if avg_volume > 0 else 0.0
    if current.close > high and volume_ratio > 1.15:
        return "long"
    if current.close < low and volume_ratio > 1.15:
        return "short"
    return "flat"


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
