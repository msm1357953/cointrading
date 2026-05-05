"""Verify whether extreme funding rates predict mean-reverting price moves.

Hypothesis:
    Funding rate top quintile (high positive)  -> next N-hour return < 0
    Funding rate bottom quintile (high negative) -> next N-hour return > 0

Causal story: a high funding rate means longs pay shorts a large carry, which
historically marks crowded long positioning that is vulnerable to liquidation.
A symmetric story applies to crowded shorts.

This module is read-only data analysis. It does not place orders or modify
lifecycle state. All output goes to stdout plus a JSON file under data/.
"""
from __future__ import annotations

import argparse
import json
import math
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from cointrading.historical_data import (
    HistoricalDataError,
    default_history_dir,
    load_binance_vision_klines,
    parse_yyyy_mm_dd,
)
from cointrading.models import Kline


FUNDING_API_URL = "https://fapi.binance.com/fapi/v1/fundingRate"
DEFAULT_SYMBOLS = ("BTCUSDC", "ETHUSDC", "SOLUSDC", "XRPUSDC", "DOGEUSDC")
DEFAULT_HORIZONS_HOURS = (1, 4, 8, 24)
DEFAULT_QUANTILES = 5
DEFAULT_COST_BPS = 13.0  # round-trip taker + slippage estimate per leg


# ---------- Funding rate fetch + cache ----------


@dataclass(frozen=True)
class FundingObservation:
    symbol: str
    funding_time_ms: int
    funding_rate: float


def funding_cache_path(symbol: str, history_dir: Path | None = None) -> Path:
    root = history_dir or default_history_dir()
    return root / "funding" / f"{symbol.upper()}_funding.jsonl"


def fetch_funding_rate_history(
    *,
    symbol: str,
    start: date,
    end: date,
    history_dir: Path | None = None,
    timeout: float = 30.0,
    verbose: bool = True,
) -> list[FundingObservation]:
    cache_path = funding_cache_path(symbol, history_dir)
    cache_path.parent.mkdir(parents=True, exist_ok=True)

    cached: dict[int, FundingObservation] = {}
    if cache_path.exists():
        with cache_path.open() as f:
            for line in f:
                if not line.strip():
                    continue
                obj = json.loads(line)
                cached[obj["fundingTime"]] = FundingObservation(
                    symbol=obj["symbol"],
                    funding_time_ms=obj["fundingTime"],
                    funding_rate=float(obj["fundingRate"]),
                )

    start_ms = int(datetime(start.year, start.month, start.day, tzinfo=timezone.utc).timestamp() * 1000)
    end_ms = int(datetime(end.year, end.month, end.day, 23, 59, 59, tzinfo=timezone.utc).timestamp() * 1000)

    have_in_range = sorted(t for t in cached if start_ms <= t <= end_ms)
    cursor = have_in_range[-1] + 1 if have_in_range else start_ms

    new_records = 0
    while cursor < end_ms:
        url = f"{FUNDING_API_URL}?symbol={symbol}&startTime={cursor}&endTime={end_ms}&limit=1000"
        request = Request(url, headers={"User-Agent": "cointrading-research/0.1"})
        try:
            with urlopen(request, timeout=timeout) as response:
                payload = json.loads(response.read())
        except (HTTPError, URLError) as exc:
            raise HistoricalDataError(f"funding fetch failed: {url}") from exc
        if not payload:
            break
        for record in payload:
            t = int(record["fundingTime"])
            if t not in cached:
                cached[t] = FundingObservation(
                    symbol=record["symbol"],
                    funding_time_ms=t,
                    funding_rate=float(record["fundingRate"]),
                )
                new_records += 1
        last_t = int(payload[-1]["fundingTime"])
        if last_t <= cursor:
            break
        cursor = last_t + 1
        if len(payload) < 1000:
            break

    if new_records > 0:
        if verbose:
            print(f"  [{symbol}] fetched {new_records} new funding records (cache total {len(cached)})")
        with cache_path.open("w") as f:
            for t in sorted(cached.keys()):
                obs = cached[t]
                f.write(json.dumps({
                    "symbol": obs.symbol,
                    "fundingTime": obs.funding_time_ms,
                    "fundingRate": obs.funding_rate,
                }) + "\n")
    elif verbose:
        in_range = sum(1 for t in cached if start_ms <= t <= end_ms)
        print(f"  [{symbol}] cached funding records in range: {in_range}")

    return [cached[t] for t in sorted(cached.keys()) if start_ms <= t <= end_ms]


# ---------- Forward returns + alignment ----------


def compute_forward_returns(
    klines: list[Kline], horizons_hours: list[int]
) -> dict[int, dict[int, float]]:
    """Return dict[open_time_ms] -> dict[horizon_h] -> return_bps, using close prices."""
    by_time = {k.open_time: k for k in klines}
    times = sorted(by_time.keys())
    out: dict[int, dict[int, float]] = {}
    hour_ms = 3600 * 1000
    for t in times:
        entry = by_time[t].close
        if entry == 0:
            continue
        out[t] = {}
        for h in horizons_hours:
            target = t + h * hour_ms
            if target in by_time:
                exit_close = by_time[target].close
                out[t][h] = (exit_close - entry) / entry * 10000.0
    return out


def align_funding_to_kline(
    funding_obs: list[FundingObservation],
    fwd_returns: dict[int, dict[int, float]],
    max_align_gap_ms: int = 3600 * 1000,
) -> list[tuple[FundingObservation, int, dict[int, float]]]:
    """Pair each funding event with the next 1h kline's forward returns."""
    sorted_open_times = sorted(fwd_returns.keys())
    paired: list[tuple[FundingObservation, int, dict[int, float]]] = []
    for obs in funding_obs:
        lo, end_idx = 0, len(sorted_open_times)
        while lo < end_idx:
            mid = (lo + end_idx) // 2
            if sorted_open_times[mid] < obs.funding_time_ms:
                lo = mid + 1
            else:
                end_idx = mid
        if lo >= len(sorted_open_times):
            continue
        next_open = sorted_open_times[lo]
        if next_open - obs.funding_time_ms > max_align_gap_ms:
            continue
        paired.append((obs, next_open, fwd_returns[next_open]))
    return paired


# ---------- Stats helpers (stdlib only) ----------


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


def rank_bucket(rank: int, n_total: int, n_buckets: int) -> int:
    """Map sorted rank (0..n_total-1) to bucket index, handling ties stably."""
    if n_total == 0:
        return 0
    return min(rank * n_buckets // n_total, n_buckets - 1)


# ---------- Per-symbol analysis ----------


@dataclass
class SymbolAnalysis:
    symbol: str
    n_obs: int
    bounds: list[tuple[float, float]]
    buckets: dict[int, dict[int, list[float]]]  # bucket_idx -> horizon_h -> returns


def analyze_symbol(
    *,
    symbol: str,
    start: date,
    end: date,
    horizons: list[int],
    n_quantiles: int,
) -> SymbolAnalysis:
    klines_result = load_binance_vision_klines(
        symbol=symbol, interval="1h", start_date=start, end_date=end
    )
    if not klines_result.klines:
        raise HistoricalDataError(f"no klines for {symbol} in {start}..{end}")
    funding = fetch_funding_rate_history(symbol=symbol, start=start, end=end)

    fwd = compute_forward_returns(klines_result.klines, horizons)
    paired = align_funding_to_kline(funding, fwd)

    if not paired:
        raise HistoricalDataError(f"no aligned funding observations for {symbol}")

    sorted_paired = sorted(paired, key=lambda p: p[0].funding_rate)
    n_total = len(sorted_paired)
    buckets: dict[int, dict[int, list[float]]] = {
        i: {h: [] for h in horizons} for i in range(n_quantiles)
    }
    bucket_rates: dict[int, list[float]] = {i: [] for i in range(n_quantiles)}
    for rank, (obs, _, fwd_h) in enumerate(sorted_paired):
        b = rank_bucket(rank, n_total, n_quantiles)
        bucket_rates[b].append(obs.funding_rate)
        for h in horizons:
            if h in fwd_h:
                buckets[b][h].append(fwd_h[h])
    bounds = [
        (min(bucket_rates[i]), max(bucket_rates[i])) if bucket_rates[i] else (float("nan"), float("nan"))
        for i in range(n_quantiles)
    ]
    return SymbolAnalysis(symbol=symbol, n_obs=len(paired), bounds=bounds, buckets=buckets)


# ---------- Reporting ----------


def print_symbol_table(r: SymbolAnalysis, horizons: list[int]) -> None:
    print(f"\n  === {r.symbol}  (n={r.n_obs}) ===")
    header = ["bucket", "fund% range"] + [f"{h}h bps" for h in horizons] + [f"{h}h n" for h in horizons]
    widths = [8, 18] + [9] * len(horizons) + [6] * len(horizons)
    print("  " + " ".join(f"{h:>{w}}" for h, w in zip(header, widths)))
    for i in range(len(r.bounds)):
        lo, hi = r.bounds[i]
        cells = [f"Q{i+1}", f"{lo*100:+.4f}~{hi*100:+.4f}"]
        for h in horizons:
            rets = r.buckets[i][h]
            cells.append(f"{mean(rets):+.2f}" if rets else "-")
        for h in horizons:
            cells.append(str(len(r.buckets[i][h])))
        print("  " + " ".join(f"{c:>{w}}" for c, w in zip(cells, widths)))


def assess_alive(
    results: list[SymbolAnalysis], horizons: list[int]
) -> list[dict]:
    out: list[dict] = []
    for r in results:
        last = len(r.bounds) - 1
        for h in horizons:
            q5 = r.buckets[last][h]
            q1 = r.buckets[0][h]
            if not q5 or not q1:
                continue
            q5_mean = mean(q5)
            q1_mean = mean(q1)
            spread = q1_mean - q5_mean
            out.append({
                "symbol": r.symbol,
                "horizon_h": h,
                "n_q1": len(q1),
                "n_q5": len(q5),
                "q1_bps": q1_mean,
                "q5_bps": q5_mean,
                "q1_t": t_stat(q1),
                "q5_t": t_stat(q5),
                "spread_bps": spread,
            })
    return out


def print_verdict(label: str, summary: list[dict], cost_bps: float) -> None:
    print(f"\n  --- {label} Q1−Q5 spread (mean reversion ALIVE if positive after cost) ---")
    print(f"  {'symbol':<10} {'h':<4} {'n_q1':>5} {'n_q5':>5} {'q1 bps':>9} (t) {'q5 bps':>9} (t) {'spread':>9} {'aft cost':>10}")
    for s in summary:
        cost_per_leg = cost_bps  # one round-trip per leg of long/short
        spread_after = s["spread_bps"] - 2 * cost_per_leg
        q1_t = s.get("q1_t", float("nan"))
        q5_t = s.get("q5_t", float("nan"))
        print(
            f"  {s['symbol']:<10} {s['horizon_h']}h{'':<2} "
            f"{s['n_q1']:>5} {s['n_q5']:>5} "
            f"{s['q1_bps']:>+9.2f} ({q1_t:+.2f}) "
            f"{s['q5_bps']:>+9.2f} ({q5_t:+.2f}) "
            f"{s['spread_bps']:>+9.2f} {spread_after:>+10.2f}"
        )


# ---------- CLI ----------


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(description="Funding rate mean reversion verification")
    p.add_argument("--symbols", nargs="*", default=list(DEFAULT_SYMBOLS))
    p.add_argument("--start", default="2025-01-01")
    p.add_argument("--end", default=None, help="default: yesterday UTC")
    p.add_argument("--split", default="2026-01-01")
    p.add_argument("--horizons", nargs="*", type=int, default=list(DEFAULT_HORIZONS_HOURS))
    p.add_argument("--quantiles", type=int, default=DEFAULT_QUANTILES)
    p.add_argument("--cost-bps", type=float, default=DEFAULT_COST_BPS)
    p.add_argument("--out", default="data/funding_carry_latest.json")
    args = p.parse_args(argv)

    start = parse_yyyy_mm_dd(args.start)
    end = parse_yyyy_mm_dd(args.end) if args.end else (date.today() - timedelta(days=1))
    split = parse_yyyy_mm_dd(args.split)

    print(f"=== Funding Rate Mean Reversion Verification ===")
    print(f"  Symbols       : {', '.join(args.symbols)}")
    print(f"  Range         : {start}  ..  {end}")
    print(f"  Split         : {split}  (in-sample < split, out-of-sample >=)")
    print(f"  Horizons      : {args.horizons} hours")
    print(f"  Quantiles     : {args.quantiles}")
    print(f"  Cost bps/leg  : {args.cost_bps} (round-trip per side; spread cost = 2x)")

    def run_block(label: str, sub_start: date, sub_end: date) -> tuple[list[SymbolAnalysis], list[dict]]:
        print(f"\n--- {label}  ({sub_start} ~ {sub_end}) ---")
        results: list[SymbolAnalysis] = []
        for symbol in args.symbols:
            try:
                r = analyze_symbol(
                    symbol=symbol,
                    start=sub_start,
                    end=sub_end,
                    horizons=args.horizons,
                    n_quantiles=args.quantiles,
                )
                results.append(r)
                print_symbol_table(r, args.horizons)
            except HistoricalDataError as e:
                print(f"  [error] {symbol}: {e}")
        summary = assess_alive(results, args.horizons)
        return results, summary

    full_results, full_summary = run_block("FULL SAMPLE", start, end)
    in_results, in_summary = run_block("IN-SAMPLE", start, split)
    out_results, out_summary = run_block("OUT-OF-SAMPLE", split, end)

    print("\n=========================== VERDICTS ===========================")
    print_verdict("FULL", full_summary, args.cost_bps)
    print_verdict("IN-SAMPLE", in_summary, args.cost_bps)
    print_verdict("OUT-OF-SAMPLE", out_summary, args.cost_bps)

    print("\n--- ALIVE COUNT (post-cost spread > 0) ---")
    n_symbols = len(args.symbols)
    cost_total = 2 * args.cost_bps
    for h in args.horizons:
        full_alive = sum(1 for s in full_summary if s["horizon_h"] == h and s["spread_bps"] - cost_total > 0)
        in_alive = sum(1 for s in in_summary if s["horizon_h"] == h and s["spread_bps"] - cost_total > 0)
        out_alive = sum(1 for s in out_summary if s["horizon_h"] == h and s["spread_bps"] - cost_total > 0)
        print(f"  {h}h: FULL {full_alive}/{n_symbols}  IN {in_alive}/{n_symbols}  OUT {out_alive}/{n_symbols}")

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "generated_ms": int(datetime.now(timezone.utc).timestamp() * 1000),
        "symbols": args.symbols,
        "start": str(start),
        "end": str(end),
        "split": str(split),
        "horizons_hours": args.horizons,
        "quantiles": args.quantiles,
        "cost_bps_per_leg": args.cost_bps,
        "full_summary": full_summary,
        "in_sample_summary": in_summary,
        "out_of_sample_summary": out_summary,
    }
    out_path.write_text(json.dumps(payload, indent=2))
    print(f"\n  saved: {out_path}")


if __name__ == "__main__":
    main()
