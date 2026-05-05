"""Deeper validation of the funding-rate mean-reversion hypothesis.

Goes beyond the quintile analysis in funding_carry.py to answer the questions
that determine whether a *live* rule is feasible:

  1. ABSOLUTE THRESHOLDS — quintile cuts depend on the sample's distribution.
     A live rule needs concrete cuts like "funding >= +0.05% -> short".
     This sweeps a grid of absolute thresholds and reports forward returns
     after costs.

  2. SIDE ASYMMETRY — does the long side (low funding -> rebound up) and the
     short side (high funding -> mean revert down) both work? If only one
     side carries the edge we should only trade that side.

  3. HORIZON SENSITIVITY — funding settles every 8 hours. We should know
     which hold time captures the move: 8h, 16h, 24h, 36h, or 48h.

The script reuses fetch + alignment helpers from funding_carry.
"""
from __future__ import annotations

import argparse
import json
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from cointrading.historical_data import (
    HistoricalDataError,
    load_binance_vision_klines,
    parse_yyyy_mm_dd,
)
from cointrading.research.funding_carry import (
    DEFAULT_SYMBOLS,
    align_funding_to_kline,
    compute_forward_returns,
    fetch_funding_rate_history,
    mean,
    stdev,
    t_stat,
)


# Absolute funding rate cuts (in fraction, not percent). 0.0001 = 0.01%.
DEFAULT_THRESHOLDS = (0.0001, 0.0002, 0.0003, 0.0005, 0.0008, 0.0010, 0.0015)
DEFAULT_HORIZONS_HOURS = (8, 16, 24, 36, 48)
DEFAULT_COST_BPS = 13.0


@dataclass
class SidePoint:
    n: int
    mean_bps: float
    median_bps: float
    t: float
    std_bps: float


def _summarize(returns: list[float]) -> SidePoint:
    if not returns:
        return SidePoint(n=0, mean_bps=float("nan"), median_bps=float("nan"), t=float("nan"), std_bps=float("nan"))
    s = sorted(returns)
    median = s[len(s) // 2] if len(s) % 2 else 0.5 * (s[len(s) // 2 - 1] + s[len(s) // 2])
    return SidePoint(n=len(returns), mean_bps=mean(returns), median_bps=median, t=t_stat(returns), std_bps=stdev(returns))


@dataclass
class SymbolPaired:
    """Per-symbol paired (funding, forward_returns_dict) observations."""
    symbol: str
    rows: list[tuple[float, dict[int, float], int]] = field(default_factory=list)
    # rows: (funding_rate, {horizon_h: ret_bps}, funding_time_ms)


def load_symbol_paired(symbol: str, start: date, end: date, horizons: list[int]) -> SymbolPaired:
    klines_result = load_binance_vision_klines(
        symbol=symbol, interval="1h", start_date=start, end_date=end
    )
    if not klines_result.klines:
        raise HistoricalDataError(f"no klines for {symbol}")
    funding = fetch_funding_rate_history(symbol=symbol, start=start, end=end, verbose=False)
    fwd = compute_forward_returns(klines_result.klines, horizons)
    paired = align_funding_to_kline(funding, fwd)
    rows: list[tuple[float, dict[int, float], int]] = []
    for obs, _, fwd_h in paired:
        rows.append((obs.funding_rate, fwd_h, obs.funding_time_ms))
    return SymbolPaired(symbol=symbol, rows=rows)


def filter_period(sp: SymbolPaired, start_ms: int, end_ms: int) -> SymbolPaired:
    out = SymbolPaired(symbol=sp.symbol, rows=[r for r in sp.rows if start_ms <= r[2] < end_ms])
    return out


def threshold_side_returns(
    sp: SymbolPaired, threshold: float, horizon_h: int
) -> tuple[list[float], list[float]]:
    """Return (long_side_returns_bps, short_side_returns_bps) at a given absolute threshold."""
    long_rets: list[float] = []
    short_rets: list[float] = []
    for funding, fwd_h, _ in sp.rows:
        if horizon_h not in fwd_h:
            continue
        ret = fwd_h[horizon_h]
        if funding <= -threshold:
            # negative funding -> shorts overpay -> price rebounds up -> we go long
            long_rets.append(ret)
        elif funding >= threshold:
            # positive funding -> longs overpay -> price retreats -> we go short
            # flip sign: positive value = profitable short
            short_rets.append(-ret)
    return long_rets, short_rets


def horizon_scan_at_threshold(
    sp: SymbolPaired, threshold: float, horizons: list[int]
) -> dict[int, dict[str, SidePoint]]:
    out: dict[int, dict[str, SidePoint]] = {}
    for h in horizons:
        l, s = threshold_side_returns(sp, threshold, h)
        out[h] = {"long": _summarize(l), "short": _summarize(s)}
    return out


def threshold_scan_at_horizon(
    sp: SymbolPaired, thresholds: list[float], horizon_h: int
) -> dict[float, dict[str, SidePoint]]:
    out: dict[float, dict[str, SidePoint]] = {}
    for t in thresholds:
        l, s = threshold_side_returns(sp, t, horizon_h)
        out[t] = {"long": _summarize(l), "short": _summarize(s)}
    return out


def _print_threshold_table(
    label: str,
    per_symbol: dict[str, dict[float, dict[str, SidePoint]]],
    thresholds: list[float],
    cost_bps: float,
    side: str,
) -> None:
    print(f"\n  --- {label}  side={side}  (cost {cost_bps} bps subtracted; positive = profitable) ---")
    header = ["symbol"] + [f">=|{t*100:.3f}%|" for t in thresholds]
    print("  " + "  ".join(f"{h:>14}" for h in header))
    for symbol, grid in per_symbol.items():
        cells = [symbol]
        for t in thresholds:
            sp = grid[t][side]
            if sp.n == 0:
                cells.append("    -    ")
            else:
                cells.append(f"{sp.mean_bps - cost_bps:+7.1f} (n={sp.n})")
        print("  " + "  ".join(f"{c:>14}" for c in cells))


def _print_horizon_table(
    label: str,
    per_symbol: dict[str, dict[int, dict[str, SidePoint]]],
    horizons: list[int],
    cost_bps: float,
    side: str,
) -> None:
    print(f"\n  --- {label}  side={side}  (cost {cost_bps} bps subtracted) ---")
    header = ["symbol"] + [f"{h}h" for h in horizons]
    print("  " + "  ".join(f"{h:>16}" for h in header))
    for symbol, grid in per_symbol.items():
        cells = [symbol]
        for h in horizons:
            sp = grid[h][side]
            if sp.n == 0:
                cells.append("    -    ")
            else:
                cells.append(f"{sp.mean_bps - cost_bps:+7.1f} t={sp.t:+.1f} n={sp.n}")
        print("  " + "  ".join(f"{c:>16}" for c in cells))


def _aggregate_side(
    per_symbol: dict[str, dict[float, dict[str, SidePoint]]],
    threshold: float,
    side: str,
) -> SidePoint:
    """Pool returns across symbols for the given (threshold, side) cut."""
    total = []
    # Re-summarize from per-symbol n weighted by mean — but we don't have raw values here.
    # Instead approximate equal-weight mean of mean_bps.
    means = []
    ns = []
    for symbol, grid in per_symbol.items():
        sp = grid[threshold][side]
        if sp.n > 0:
            means.append(sp.mean_bps)
            ns.append(sp.n)
    if not means:
        return SidePoint(n=0, mean_bps=float("nan"), median_bps=float("nan"), t=float("nan"), std_bps=float("nan"))
    pooled_mean = sum(m * n for m, n in zip(means, ns)) / sum(ns)
    return SidePoint(n=sum(ns), mean_bps=pooled_mean, median_bps=float("nan"), t=float("nan"), std_bps=float("nan"))


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(description="Deep validation of funding mean-reversion")
    p.add_argument("--symbols", nargs="*", default=list(DEFAULT_SYMBOLS))
    p.add_argument("--start", default="2025-01-01")
    p.add_argument("--end", default=None)
    p.add_argument("--split", default="2026-01-01")
    p.add_argument("--horizons", nargs="*", type=int, default=list(DEFAULT_HORIZONS_HOURS))
    p.add_argument("--thresholds", nargs="*", type=float, default=list(DEFAULT_THRESHOLDS))
    p.add_argument("--focus-horizon", type=int, default=24)
    p.add_argument("--focus-threshold", type=float, default=0.0005)
    p.add_argument("--cost-bps", type=float, default=DEFAULT_COST_BPS)
    p.add_argument("--out", default="data/funding_carry_deep_latest.json")
    args = p.parse_args(argv)

    start = parse_yyyy_mm_dd(args.start)
    end = parse_yyyy_mm_dd(args.end) if args.end else (date.today() - timedelta(days=1))
    split = parse_yyyy_mm_dd(args.split)
    horizons = list(args.horizons)
    if args.focus_horizon not in horizons:
        horizons.append(args.focus_horizon)
        horizons = sorted(set(horizons))

    print("=== Funding Mean-Reversion DEEP Validation ===")
    print(f"  Symbols       : {', '.join(args.symbols)}")
    print(f"  Range         : {start} ~ {end}")
    print(f"  Split         : {split}")
    print(f"  Thresholds    : {[f'{t*100:.3f}%' for t in args.thresholds]}")
    print(f"  Horizons      : {horizons}")
    print(f"  Focus         : threshold={args.focus_threshold*100:.3f}%  horizon={args.focus_horizon}h")
    print(f"  Cost/leg bps  : {args.cost_bps}")

    # Load all symbols once
    all_paired: dict[str, SymbolPaired] = {}
    for symbol in args.symbols:
        print(f"  loading {symbol}...")
        try:
            all_paired[symbol] = load_symbol_paired(symbol, start, end, horizons)
        except HistoricalDataError as e:
            print(f"  [error] {symbol}: {e}")

    split_ms = int(datetime(split.year, split.month, split.day, tzinfo=timezone.utc).timestamp() * 1000)
    end_ms = int(datetime(end.year, end.month, end.day, 23, 59, 59, tzinfo=timezone.utc).timestamp() * 1000) + 1

    in_paired = {s: filter_period(p, 0, split_ms) for s, p in all_paired.items()}
    out_paired = {s: filter_period(p, split_ms, end_ms) for s, p in all_paired.items()}

    # ----- 1. Threshold scan at focus horizon -----
    def grid_threshold(period_paired: dict[str, SymbolPaired]) -> dict[str, dict[float, dict[str, SidePoint]]]:
        return {
            sym: threshold_scan_at_horizon(p, args.thresholds, args.focus_horizon)
            for sym, p in period_paired.items()
        }

    full_threshold_grid = grid_threshold(all_paired)
    in_threshold_grid = grid_threshold(in_paired)
    out_threshold_grid = grid_threshold(out_paired)

    print(f"\n========== 1. ABSOLUTE THRESHOLD SCAN  (horizon={args.focus_horizon}h) ==========")
    for side in ("long", "short"):
        _print_threshold_table("FULL", full_threshold_grid, args.thresholds, args.cost_bps, side)
        _print_threshold_table("IN-SAMPLE", in_threshold_grid, args.thresholds, args.cost_bps, side)
        _print_threshold_table("OUT-OF-SAMPLE", out_threshold_grid, args.thresholds, args.cost_bps, side)

    # Pooled view: alive count per threshold/side
    print("\n  --- ALIVE COUNT after cost (per side) ---")
    print(f"  {'side':<6} {'thr':>8}  FULL/IN/OUT  (symbols positive after cost)")
    for side in ("long", "short"):
        for t in args.thresholds:
            f_alive = sum(1 for s in args.symbols if full_threshold_grid.get(s, {}).get(t, {}).get(side) and full_threshold_grid[s][t][side].n > 0 and full_threshold_grid[s][t][side].mean_bps - args.cost_bps > 0)
            i_alive = sum(1 for s in args.symbols if in_threshold_grid.get(s, {}).get(t, {}).get(side) and in_threshold_grid[s][t][side].n > 0 and in_threshold_grid[s][t][side].mean_bps - args.cost_bps > 0)
            o_alive = sum(1 for s in args.symbols if out_threshold_grid.get(s, {}).get(t, {}).get(side) and out_threshold_grid[s][t][side].n > 0 and out_threshold_grid[s][t][side].mean_bps - args.cost_bps > 0)
            n = len(args.symbols)
            print(f"  {side:<6} {t*100:>7.3f}%  {f_alive}/{n}  {i_alive}/{n}  {o_alive}/{n}")

    # ----- 2. Horizon scan at focus threshold -----
    def grid_horizon(period_paired: dict[str, SymbolPaired]) -> dict[str, dict[int, dict[str, SidePoint]]]:
        return {
            sym: horizon_scan_at_threshold(p, args.focus_threshold, horizons)
            for sym, p in period_paired.items()
        }

    full_horizon_grid = grid_horizon(all_paired)
    in_horizon_grid = grid_horizon(in_paired)
    out_horizon_grid = grid_horizon(out_paired)

    print(f"\n========== 2. HORIZON SCAN  (threshold={args.focus_threshold*100:.3f}%) ==========")
    for side in ("long", "short"):
        _print_horizon_table("FULL", full_horizon_grid, horizons, args.cost_bps, side)
        _print_horizon_table("IN-SAMPLE", in_horizon_grid, horizons, args.cost_bps, side)
        _print_horizon_table("OUT-OF-SAMPLE", out_horizon_grid, horizons, args.cost_bps, side)

    # ----- 3. Side asymmetry summary at focus (thr, h) -----
    print(f"\n========== 3. SIDE ASYMMETRY  (threshold={args.focus_threshold*100:.3f}%, horizon={args.focus_horizon}h) ==========")
    print(f"  {'symbol':<10} {'period':<8} {'long_n':>7} {'long_mean':>11} {'long_t':>8} {'short_n':>9} {'short_mean':>12} {'short_t':>8}")
    for symbol in args.symbols:
        for label, grid in [("FULL", full_horizon_grid), ("IN", in_horizon_grid), ("OUT", out_horizon_grid)]:
            entry = grid.get(symbol, {}).get(args.focus_horizon)
            if not entry:
                continue
            l = entry["long"]
            s = entry["short"]
            print(
                f"  {symbol:<10} {label:<8} "
                f"{l.n:>7} {l.mean_bps - args.cost_bps:>+10.1f} {l.t:>+7.2f}  "
                f"{s.n:>9} {s.mean_bps - args.cost_bps:>+11.1f} {s.t:>+7.2f}"
            )

    # ----- Save -----
    payload = {
        "generated_ms": int(datetime.now(timezone.utc).timestamp() * 1000),
        "symbols": args.symbols,
        "start": str(start),
        "end": str(end),
        "split": str(split),
        "thresholds": args.thresholds,
        "horizons": horizons,
        "focus_horizon": args.focus_horizon,
        "focus_threshold": args.focus_threshold,
        "cost_bps_per_leg": args.cost_bps,
    }
    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    Path(args.out).write_text(json.dumps(payload, indent=2))
    print(f"\n  saved: {args.out}")


if __name__ == "__main__":
    main()
