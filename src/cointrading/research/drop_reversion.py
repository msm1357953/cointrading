"""Verify whether short-term price drops mean-revert ('꼬리 잡기').

Hypothesis:
    When the close price drops by at least X% over the last Y hours,
    the next Z hours show a positive average return (bounce).

Causal story: a sharp drop is often driven by liquidation cascades or
panic flows, which create a temporary price dislocation that mean
reverts as liquidity returns.

Distinct from the funding-rate hypothesis: that one looks at position
imbalance (funding); this one looks at realized price action only.
The two should be largely uncorrelated, which means they could be
combined later as separate alphas.

Output: per-(drop_window, drop_threshold) cell, the mean forward
return at multiple horizons, after a 13 bps round-trip cost, split
into FULL / IN-SAMPLE / OUT-OF-SAMPLE and aggregated across the 5
USDC symbols.
"""
from __future__ import annotations

import argparse
import json
import math
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from cointrading.historical_data import (
    HistoricalDataError,
    load_binance_vision_klines,
    parse_yyyy_mm_dd,
)
from cointrading.models import Kline


DEFAULT_SYMBOLS = ("BTCUSDC", "ETHUSDC", "SOLUSDC", "XRPUSDC", "DOGEUSDC")
DEFAULT_DROP_WINDOWS = (1, 4, 8)
DEFAULT_DROP_THRESHOLDS = (0.02, 0.03, 0.05, 0.07)  # 2%, 3%, 5%, 7%
DEFAULT_FORWARD_HORIZONS = (1, 4, 8, 24)
DEFAULT_COST_BPS = 13.0  # round-trip taker + slippage


@dataclass
class Event:
    symbol: str
    open_time_ms: int
    drop_pct: float
    forward_returns_bps: dict[int, float]  # horizon_h -> bps


def find_events(
    klines: list[Kline],
    drop_window_h: int,
    drop_threshold: float,
    forward_horizons: tuple[int, ...],
    cooldown_hours: int,
) -> list[Event]:
    if len(klines) < drop_window_h + max(forward_horizons) + 1:
        return []
    events: list[Event] = []
    last_trigger_idx = -10**9

    for i in range(drop_window_h, len(klines)):
        if i - last_trigger_idx < cooldown_hours:
            continue
        prior = klines[i - drop_window_h].close
        now = klines[i].close
        if prior <= 0:
            continue
        drop_pct = (now - prior) / prior  # negative for a drop
        if drop_pct > -drop_threshold:
            continue

        fwd: dict[int, float] = {}
        for h in forward_horizons:
            j = i + h
            if j >= len(klines):
                continue
            entry = klines[i].close
            exit_close = klines[j].close
            if entry <= 0:
                continue
            fwd[h] = (exit_close - entry) / entry * 10_000.0
        events.append(Event(
            symbol=klines[i].open_time and klines[i].open_time and "_" or "_",
            open_time_ms=klines[i].open_time,
            drop_pct=drop_pct,
            forward_returns_bps=fwd,
        ))
        last_trigger_idx = i
    return events


def mean(values: list[float]) -> float:
    return sum(values) / len(values) if values else float("nan")


def stdev(values: list[float]) -> float:
    if len(values) < 2:
        return float("nan")
    m = mean(values)
    return math.sqrt(sum((v - m) ** 2 for v in values) / (len(values) - 1))


def t_stat(values: list[float]) -> float:
    if len(values) < 2:
        return float("nan")
    s = stdev(values)
    if s == 0 or math.isnan(s):
        return float("nan")
    return mean(values) / (s / math.sqrt(len(values)))


@dataclass
class CellSummary:
    n: int
    mean_bps_after_cost: float
    median_bps_after_cost: float
    t: float


def summarize(values: list[float], cost_bps: float) -> CellSummary:
    if not values:
        return CellSummary(n=0, mean_bps_after_cost=float("nan"),
                           median_bps_after_cost=float("nan"), t=float("nan"))
    s = sorted(values)
    median = s[len(s) // 2] if len(s) % 2 else 0.5 * (s[len(s) // 2 - 1] + s[len(s) // 2])
    return CellSummary(
        n=len(values),
        mean_bps_after_cost=mean(values) - cost_bps,
        median_bps_after_cost=median - cost_bps,
        t=t_stat(values),
    )


def filter_events_period(events: list[tuple[str, Event]], start_ms: int, end_ms: int) -> list[tuple[str, Event]]:
    return [(s, e) for s, e in events if start_ms <= e.open_time_ms < end_ms]


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(description="Verify short-term drop mean-reversion (꼬리 잡기)")
    p.add_argument("--symbols", nargs="*", default=list(DEFAULT_SYMBOLS))
    p.add_argument("--start", default="2025-01-01")
    p.add_argument("--end", default=None)
    p.add_argument("--split", default="2026-01-01")
    p.add_argument("--drop-windows", nargs="*", type=int, default=list(DEFAULT_DROP_WINDOWS))
    p.add_argument("--drop-thresholds", nargs="*", type=float, default=list(DEFAULT_DROP_THRESHOLDS))
    p.add_argument("--horizons", nargs="*", type=int, default=list(DEFAULT_FORWARD_HORIZONS))
    p.add_argument("--cost-bps", type=float, default=DEFAULT_COST_BPS)
    p.add_argument("--cooldown-multiplier", type=int, default=2,
                   help="cooldown after a trigger = drop_window * this multiplier (hours)")
    p.add_argument("--out", default="data/drop_reversion_latest.json")
    args = p.parse_args(argv)

    start = parse_yyyy_mm_dd(args.start)
    end = parse_yyyy_mm_dd(args.end) if args.end else (date.today() - timedelta(days=1))
    split = parse_yyyy_mm_dd(args.split)

    print("=== Drop-reversion (꼬리 잡기) verification ===")
    print(f"  Symbols       : {', '.join(args.symbols)}")
    print(f"  Range         : {start} ~ {end}  split @ {split}")
    print(f"  Drop windows  : {args.drop_windows} hours")
    print(f"  Drop thresholds: {[f'{t * 100:.1f}%' for t in args.drop_thresholds]}")
    print(f"  Horizons      : {args.horizons} hours")
    print(f"  Cost bps RT   : {args.cost_bps}")

    # Load all klines first
    klines_per_symbol: dict[str, list[Kline]] = {}
    for symbol in args.symbols:
        try:
            res = load_binance_vision_klines(
                symbol=symbol, interval="1h", start_date=start, end_date=end,
            )
            klines_per_symbol[symbol] = res.klines
            print(f"  {symbol}: {len(res.klines)} 1h klines")
        except HistoricalDataError as exc:
            print(f"  [error] {symbol}: {exc}")

    split_ms = int(datetime(split.year, split.month, split.day, tzinfo=timezone.utc).timestamp() * 1000)

    # Run per (drop_window, drop_threshold) configuration
    results: list[dict] = []
    for drop_w in args.drop_windows:
        cooldown = drop_w * args.cooldown_multiplier
        for drop_t in args.drop_thresholds:
            # Per symbol per period: collect forward returns by horizon
            cells: dict[str, dict[int, list[float]]] = {
                "FULL": {h: [] for h in args.horizons},
                "IN":   {h: [] for h in args.horizons},
                "OUT":  {h: [] for h in args.horizons},
            }
            per_symbol_n: dict[str, dict[str, int]] = {}
            for symbol, klines in klines_per_symbol.items():
                events = find_events(
                    klines=klines,
                    drop_window_h=drop_w,
                    drop_threshold=drop_t,
                    forward_horizons=tuple(args.horizons),
                    cooldown_hours=cooldown,
                )
                per_symbol_n[symbol] = {"FULL": 0, "IN": 0, "OUT": 0}
                for e in events:
                    period = "IN" if e.open_time_ms < split_ms else "OUT"
                    per_symbol_n[symbol]["FULL"] += 1
                    per_symbol_n[symbol][period] += 1
                    for h in args.horizons:
                        if h not in e.forward_returns_bps:
                            continue
                        cells["FULL"][h].append(e.forward_returns_bps[h])
                        cells[period][h].append(e.forward_returns_bps[h])

            cell_summary = {
                period: {h: summarize(cells[period][h], args.cost_bps) for h in args.horizons}
                for period in ("FULL", "IN", "OUT")
            }
            results.append({
                "drop_window_h": drop_w,
                "drop_threshold": drop_t,
                "cooldown_h": cooldown,
                "per_symbol_n": per_symbol_n,
                "summaries": cell_summary,
            })

    # Print formatted tables
    print("\n========== AGGREGATE (5심볼 합산, after-cost bps) ==========")
    for r in results:
        print(f"\n--- drop ≤ -{r['drop_threshold'] * 100:.0f}% in {r['drop_window_h']}h "
              f"(cooldown {r['cooldown_h']}h) ---")
        cells_header = ["period"]
        for h in args.horizons:
            cells_header.append(f"h={h}h")
        print("  " + " | ".join(f"{c:<22}" for c in cells_header))
        for period in ("FULL", "IN", "OUT"):
            cells = [period]
            for h in args.horizons:
                s = r["summaries"][period][h]
                if s.n == 0:
                    cells.append("        -")
                else:
                    cells.append(f"{s.mean_bps_after_cost:+7.1f} t={s.t:+.2f} n={s.n}")
            print("  " + " | ".join(f"{c:<22}" for c in cells))

    # Verdict — pick best cell across all (window, threshold, horizon) for OUT
    print("\n========== VERDICT (OUT-of-sample, after-cost) ==========")
    out_rows: list[tuple[str, float, int, CellSummary]] = []
    for r in results:
        for h in args.horizons:
            s = r["summaries"]["OUT"][h]
            label = f"drop ≤ -{r['drop_threshold'] * 100:.0f}% in {r['drop_window_h']}h, hold {h}h"
            out_rows.append((label, r["drop_threshold"], h, s))
    out_rows.sort(key=lambda x: (x[3].n > 5 and not math.isnan(x[3].mean_bps_after_cost), x[3].mean_bps_after_cost), reverse=True)
    print("\nTop 10 OUT-of-sample cells (n>=10):")
    print(f"  {'label':<55} {'mean':>9} {'t':>7} {'n':>5}")
    shown = 0
    for label, _, _, s in out_rows:
        if s.n < 10:
            continue
        print(f"  {label:<55} {s.mean_bps_after_cost:>+9.1f} {s.t:>+7.2f} {s.n:>5}")
        shown += 1
        if shown >= 10:
            break

    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "generated_ms": int(datetime.now(timezone.utc).timestamp() * 1000),
        "config": {
            "symbols": args.symbols,
            "start": str(start), "end": str(end), "split": str(split),
            "drop_windows": args.drop_windows,
            "drop_thresholds": args.drop_thresholds,
            "horizons": args.horizons,
            "cost_bps": args.cost_bps,
        },
        "results": [
            {
                "drop_window_h": r["drop_window_h"],
                "drop_threshold": r["drop_threshold"],
                "per_symbol_n": r["per_symbol_n"],
                "summaries": {
                    period: {
                        str(h): {
                            "n": s.n,
                            "mean_bps_after_cost": s.mean_bps_after_cost,
                            "t": s.t,
                        }
                        for h, s in horizons.items()
                    }
                    for period, horizons in r["summaries"].items()
                },
            }
            for r in results
        ],
    }
    Path(args.out).write_text(json.dumps(payload, indent=2, default=str))
    print(f"\n  saved: {args.out}")


if __name__ == "__main__":
    main()
