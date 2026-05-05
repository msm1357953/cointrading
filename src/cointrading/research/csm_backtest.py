"""Cross-sectional momentum backtest on 5 USDC majors.

Hypothesis (academic consensus, e.g., Jegadeesh & Titman 1993, applied to crypto):
    Rank symbols by trailing return over a lookback window. Long the top
    performer, short the bottom performer (or top-N / bottom-N). Hold for
    a fixed horizon. Rebalance.

For our small 5-symbol universe we test:
    long-1 / short-1: hold the strongest, short the weakest.

Costs: 13 bps round-trip per leg (taker), so ~26 bps for paired long+short
or 13 bps if we run long-only top-1.
"""
from __future__ import annotations

import argparse
import math
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone

import pandas as pd

from cointrading.historical_data import parse_yyyy_mm_dd
from cointrading.research.data_lake import load_klines


DEFAULT_SYMBOLS = ("BTCUSDC", "ETHUSDC", "SOLUSDC", "XRPUSDC", "DOGEUSDC")


@dataclass
class CSMTrade:
    rebalance_time_ms: int
    long_symbol: str | None
    short_symbol: str | None
    long_entry: float
    long_exit: float
    short_entry: float
    short_exit: float
    long_ret_bps: float
    short_ret_bps: float
    pnl_bps_after_cost: float


def build_returns_panel(
    symbols: tuple[str, ...], interval: str, start: date, end: date
) -> pd.DataFrame:
    """Wide DataFrame indexed by open_time with one column per symbol's close."""
    frames = []
    for s in symbols:
        df = load_klines(s, interval, start=start, end=end)[["open_time", "close"]].copy()
        df = df.rename(columns={"close": s}).set_index("open_time")
        frames.append(df)
    panel = pd.concat(frames, axis=1).sort_index()
    return panel


def simulate_csm(
    panel: pd.DataFrame,
    *,
    lookback_bars: int,
    hold_bars: int,
    cost_bps_per_leg: float,
    long_only: bool,
) -> list[CSMTrade]:
    n = len(panel)
    if n < lookback_bars + hold_bars + 1:
        return []
    trades: list[CSMTrade] = []
    i = lookback_bars
    while i < n - hold_bars:
        prices_now = panel.iloc[i]
        prices_past = panel.iloc[i - lookback_bars]
        rets = (prices_now / prices_past - 1.0).dropna()
        if len(rets) < 2:
            i += hold_bars
            continue
        long_sym = rets.idxmax()
        short_sym = rets.idxmin() if not long_only else None

        rebalance_t = int(panel.index[i])
        exit_idx = min(i + hold_bars, n - 1)

        long_entry = float(panel[long_sym].iloc[i])
        long_exit = float(panel[long_sym].iloc[exit_idx])
        long_ret = (long_exit - long_entry) / long_entry * 10_000.0 if long_entry > 0 else 0.0

        if short_sym is None:
            short_entry = short_exit = 0.0
            short_ret = 0.0
            cost = 2 * cost_bps_per_leg
        else:
            short_entry = float(panel[short_sym].iloc[i])
            short_exit = float(panel[short_sym].iloc[exit_idx])
            short_ret = (short_entry - short_exit) / short_entry * 10_000.0 if short_entry > 0 else 0.0
            cost = 4 * cost_bps_per_leg  # entry+exit on both sides

        gross = long_ret + short_ret
        pnl = gross - cost
        trades.append(CSMTrade(
            rebalance_time_ms=rebalance_t,
            long_symbol=long_sym, short_symbol=short_sym,
            long_entry=long_entry, long_exit=long_exit,
            short_entry=short_entry, short_exit=short_exit,
            long_ret_bps=long_ret, short_ret_bps=short_ret,
            pnl_bps_after_cost=pnl,
        ))
        i = exit_idx
    return trades


def stats_block(label: str, trades: list[CSMTrade]) -> str:
    if not trades:
        return f"  {label}: n=0"
    pnls = [t.pnl_bps_after_cost for t in trades]
    n = len(pnls)
    wins = [p for p in pnls if p > 0]
    losses = [p for p in pnls if p <= 0]
    sum_p = sum(pnls)
    mean_p = sum_p / n
    wr = len(wins) / n * 100
    sum_w = sum(wins)
    sum_l = sum(abs(p) for p in losses)
    pf = sum_w / sum_l if sum_l > 0 else float("inf")
    eq = peak = max_dd = 0.0
    for p in pnls:
        eq += p
        peak = max(peak, eq)
        max_dd = min(max_dd, eq - peak)
    return (
        f"  {label}: n={n:>4} WR={wr:5.1f}% mean={mean_p:+7.1f} "
        f"sum={sum_p:+9.1f} PF={pf:.2f} maxDD={max_dd:+9.1f}"
    )


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(description="Cross-sectional momentum backtest")
    p.add_argument("--symbols", nargs="*", default=list(DEFAULT_SYMBOLS))
    p.add_argument("--interval", default="4h")
    p.add_argument("--lookback-bars", type=int, default=42)  # ~7 days at 4h
    p.add_argument("--hold-bars", type=int, default=42)
    p.add_argument("--cost-bps", type=float, default=6.5)
    p.add_argument("--long-only", action="store_true")
    p.add_argument("--start", default="2025-01-01")
    p.add_argument("--end", default="2026-04-30")
    p.add_argument("--split", default="2026-01-01")
    args = p.parse_args(argv)

    start = parse_yyyy_mm_dd(args.start)
    end = parse_yyyy_mm_dd(args.end)
    split = parse_yyyy_mm_dd(args.split)
    bar_min = {"5m": 5, "15m": 15, "1h": 60, "4h": 240}.get(args.interval, 60)
    print(
        f"=== CSM backtest ===\n"
        f"  symbols     : {args.symbols}\n"
        f"  interval    : {args.interval}  (bar={bar_min}min)\n"
        f"  lookback    : {args.lookback_bars} bars (~{args.lookback_bars * bar_min / 60:.1f}h)\n"
        f"  hold        : {args.hold_bars} bars (~{args.hold_bars * bar_min / 60:.1f}h)\n"
        f"  cost/leg    : {args.cost_bps} bps\n"
        f"  long_only   : {args.long_only}\n"
        f"  range/split : {start} ~ {end} split={split}"
    )

    panel = build_returns_panel(tuple(args.symbols), args.interval, start, end)
    trades = simulate_csm(
        panel,
        lookback_bars=args.lookback_bars,
        hold_bars=args.hold_bars,
        cost_bps_per_leg=args.cost_bps,
        long_only=args.long_only,
    )

    split_ms = int(datetime(split.year, split.month, split.day, tzinfo=timezone.utc).timestamp() * 1000)
    in_t = [t for t in trades if t.rebalance_time_ms < split_ms]
    out_t = [t for t in trades if t.rebalance_time_ms >= split_ms]

    print()
    print(stats_block("FULL", trades))
    print(stats_block("IN  ", in_t))
    print(stats_block("OUT ", out_t))

    # Symbol selection frequency
    if trades:
        print("\nLong selection counts:")
        from collections import Counter
        c = Counter(t.long_symbol for t in trades)
        for s, n in sorted(c.items(), key=lambda x: -x[1]):
            print(f"  {s:<10} {n:>3}")
        if not args.long_only:
            print("Short selection counts:")
            c = Counter(t.short_symbol for t in trades)
            for s, n in sorted(c.items(), key=lambda x: -x[1]):
                print(f"  {s:<10} {n:>3}")


if __name__ == "__main__":
    main()
