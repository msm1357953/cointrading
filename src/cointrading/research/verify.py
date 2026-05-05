"""Standard hypothesis verification interface backed by the data lake.

Usage::

    from cointrading.research.verify import verify_hypothesis, Report

    def my_trigger(df, i):
        return df.lower_wick_ratio[i] >= 0.7 and df.intrabar_drop_pct[i] >= 0.01

    report = verify_hypothesis(
        name="wick_07_drop1",
        trigger=my_trigger,
        symbols=("BTCUSDC","ETHUSDC","SOLUSDC","XRPUSDC","DOGEUSDC"),
        base_interval="5m",
        start="2025-01-01", end="2026-04-30", split="2026-01-01",
        hold_bars=24, stop_loss_bps=300, take_profit_bps=None,
        cooldown_bars=2, cost_bps_per_leg=6.5,
    )
    print(report.to_text())

A trigger is a callable ``(df: DataFrame, i: int) -> bool``. It receives the
aligned dataset (with funding/OI/wick features merged) and the row index.
The function should return True when the i-th bar is a valid entry.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Callable

import pandas as pd

from cointrading.historical_data import parse_yyyy_mm_dd
from cointrading.research.data_lake import build_aligned_dataset


TriggerFn = Callable[[pd.DataFrame, int], bool]


@dataclass
class Trade:
    symbol: str
    entry_time_ms: int
    exit_time_ms: int
    entry_price: float
    exit_price: float
    pnl_bps_after_cost: float
    exit_reason: str


@dataclass
class PeriodStats:
    label: str
    n: int
    wins: int
    losses: int
    mean_bps: float
    sum_bps: float
    win_rate: float
    avg_win: float
    avg_loss: float
    profit_factor: float
    payoff: float
    max_dd_bps: float
    exit_reasons: dict[str, int]


@dataclass
class Report:
    name: str
    config: dict
    trades: list[Trade] = field(default_factory=list)
    by_symbol: dict[str, list[Trade]] = field(default_factory=dict)
    full: PeriodStats | None = None
    in_sample: PeriodStats | None = None
    out_sample: PeriodStats | None = None
    in_sample_by_symbol: dict[str, PeriodStats] = field(default_factory=dict)
    out_sample_by_symbol: dict[str, PeriodStats] = field(default_factory=dict)

    def to_text(self) -> str:
        lines = [f"=== {self.name} ===", f"  config: {self.config}"]
        for label, ps in [("FULL", self.full), ("IN-SAMPLE", self.in_sample),
                          ("OUT-OF-SAMPLE", self.out_sample)]:
            if ps is None:
                continue
            lines.append("")
            lines.append(f"  --- {label} ---")
            lines.append(f"    n={ps.n} WR={ps.win_rate*100:.1f}% mean={ps.mean_bps:+.1f} "
                         f"PF={ps.profit_factor:.2f} payoff={ps.payoff:.2f} "
                         f"sum={ps.sum_bps:+.1f} maxDD={ps.max_dd_bps:+.1f} "
                         f"exits={ps.exit_reasons}")
        lines.append("")
        lines.append("  --- per symbol IN/OUT ---")
        symbols = sorted(set(self.in_sample_by_symbol) | set(self.out_sample_by_symbol))
        for s in symbols:
            ips = self.in_sample_by_symbol.get(s)
            ops = self.out_sample_by_symbol.get(s)
            in_str = (f"n={ips.n} mean={ips.mean_bps:+.1f}" if ips and ips.n else "n=0")
            out_str = (f"n={ops.n} mean={ops.mean_bps:+.1f}" if ops and ops.n else "n=0")
            lines.append(f"    {s:<10} IN: {in_str:<25}  OUT: {out_str}")
        return "\n".join(lines)


def _stats(label: str, trades: list[Trade]) -> PeriodStats:
    if not trades:
        return PeriodStats(label=label, n=0, wins=0, losses=0, mean_bps=0.0,
                           sum_bps=0.0, win_rate=0.0, avg_win=0.0, avg_loss=0.0,
                           profit_factor=0.0, payoff=0.0, max_dd_bps=0.0,
                           exit_reasons={})
    wins = [t for t in trades if t.pnl_bps_after_cost > 0]
    losses = [t for t in trades if t.pnl_bps_after_cost <= 0]
    sum_pnl = sum(t.pnl_bps_after_cost for t in trades)
    avg_win = sum(t.pnl_bps_after_cost for t in wins) / len(wins) if wins else 0.0
    avg_loss = sum(abs(t.pnl_bps_after_cost) for t in losses) / len(losses) if losses else 0.0
    sum_w = sum(t.pnl_bps_after_cost for t in wins)
    sum_l = sum(abs(t.pnl_bps_after_cost) for t in losses)
    pf = sum_w / sum_l if sum_l > 0 else float("inf")
    payoff = avg_win / avg_loss if avg_loss > 0 else float("inf")
    eq = peak = max_dd = 0.0
    for t in sorted(trades, key=lambda x: x.exit_time_ms):
        eq += t.pnl_bps_after_cost
        peak = max(peak, eq)
        max_dd = min(max_dd, eq - peak)
    by_reason: dict[str, int] = {}
    for t in trades:
        by_reason[t.exit_reason] = by_reason.get(t.exit_reason, 0) + 1
    return PeriodStats(
        label=label, n=len(trades), wins=len(wins), losses=len(losses),
        mean_bps=sum_pnl / len(trades), sum_bps=sum_pnl,
        win_rate=len(wins) / len(trades),
        avg_win=avg_win, avg_loss=avg_loss,
        profit_factor=pf, payoff=payoff,
        max_dd_bps=max_dd, exit_reasons=by_reason,
    )


def _simulate(
    df: pd.DataFrame,
    trigger: TriggerFn,
    *,
    symbol: str,
    hold_bars: int,
    stop_loss_bps: float | None,
    take_profit_bps: float | None,
    cooldown_bars: int,
    cost_bps_per_leg: float,
) -> list[Trade]:
    if df.empty or len(df) < hold_bars + 2:
        return []
    closes = df["close"].to_numpy()
    open_times = df["open_time"].to_numpy()
    n = len(df)
    trades: list[Trade] = []
    in_pos_until = -1
    for i in range(n - 1):
        if i <= in_pos_until + cooldown_bars:
            continue
        try:
            if not trigger(df, i):
                continue
        except (KeyError, IndexError, ValueError):
            continue
        entry = closes[i]
        if entry <= 0 or pd.isna(entry):
            continue
        time_exit_idx = min(i + hold_bars, n - 1)
        exit_idx = time_exit_idx
        exit_price = closes[exit_idx]
        exit_reason = "time"
        for j in range(i + 1, time_exit_idx + 1):
            ret_bps = (closes[j] - entry) / entry * 10_000.0
            if stop_loss_bps is not None and ret_bps <= -stop_loss_bps:
                exit_price = closes[j]
                exit_idx = j
                exit_reason = "stop"
                break
            if take_profit_bps is not None and ret_bps >= take_profit_bps:
                exit_price = closes[j]
                exit_idx = j
                exit_reason = "tp"
                break
        pnl_bps = (exit_price - entry) / entry * 10_000.0
        trades.append(Trade(
            symbol=symbol,
            entry_time_ms=int(open_times[i]),
            exit_time_ms=int(open_times[exit_idx]),
            entry_price=float(entry),
            exit_price=float(exit_price),
            pnl_bps_after_cost=float(pnl_bps - 2 * cost_bps_per_leg),
            exit_reason=exit_reason,
        ))
        in_pos_until = exit_idx
    return trades


def verify_hypothesis(
    *,
    name: str,
    trigger: TriggerFn,
    symbols: tuple[str, ...] = ("BTCUSDC", "ETHUSDC", "SOLUSDC", "XRPUSDC", "DOGEUSDC"),
    base_interval: str = "5m",
    start: str = "2025-01-01",
    end: str | None = None,
    split: str = "2026-01-01",
    hold_bars: int = 24,
    stop_loss_bps: float | None = 300.0,
    take_profit_bps: float | None = None,
    cooldown_bars: int = 2,
    cost_bps_per_leg: float = 6.5,
    higher_intervals: tuple[str, ...] = ("1h", "4h"),
    include_funding: bool = True,
    include_oi: bool = False,
) -> Report:
    end_d = parse_yyyy_mm_dd(end) if end else (date.today())
    split_d = parse_yyyy_mm_dd(split)
    split_ms = int(datetime(split_d.year, split_d.month, split_d.day, tzinfo=timezone.utc).timestamp() * 1000)

    all_trades: list[Trade] = []
    by_symbol: dict[str, list[Trade]] = {}
    for symbol in symbols:
        df = build_aligned_dataset(
            symbol, base_interval=base_interval, start=start, end=end_d,
            include_funding=include_funding, include_oi=include_oi,
            higher_intervals=higher_intervals,
        )
        trades = _simulate(
            df, trigger, symbol=symbol,
            hold_bars=hold_bars,
            stop_loss_bps=stop_loss_bps,
            take_profit_bps=take_profit_bps,
            cooldown_bars=cooldown_bars,
            cost_bps_per_leg=cost_bps_per_leg,
        )
        by_symbol[symbol] = trades
        all_trades.extend(trades)

    in_trades = [t for t in all_trades if t.entry_time_ms < split_ms]
    out_trades = [t for t in all_trades if t.entry_time_ms >= split_ms]

    in_by_symbol = {s: _stats(f"IN-{s}", [t for t in trs if t.entry_time_ms < split_ms])
                    for s, trs in by_symbol.items()}
    out_by_symbol = {s: _stats(f"OUT-{s}", [t for t in trs if t.entry_time_ms >= split_ms])
                     for s, trs in by_symbol.items()}

    return Report(
        name=name,
        config={
            "base_interval": base_interval,
            "hold_bars": hold_bars, "hold_minutes": hold_bars * {"5m": 5, "15m": 15, "1h": 60, "4h": 240}.get(base_interval, 5),
            "stop_loss_bps": stop_loss_bps, "take_profit_bps": take_profit_bps,
            "cooldown_bars": cooldown_bars, "cost_bps_per_leg": cost_bps_per_leg,
            "start": start, "end": str(end_d), "split": split,
            "symbols": list(symbols),
        },
        trades=all_trades, by_symbol=by_symbol,
        full=_stats("FULL", all_trades),
        in_sample=_stats("IN", in_trades),
        out_sample=_stats("OUT", out_trades),
        in_sample_by_symbol=in_by_symbol,
        out_sample_by_symbol=out_by_symbol,
    )
