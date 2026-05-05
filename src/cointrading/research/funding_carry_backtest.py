"""End-to-end backtest of the funding mean-reversion rule.

This simulates the actual trading rule we plan to deploy:

  ENTRY:
    At each funding settlement (UTC 00:00, 08:00, 16:00), for each symbol,
    if funding_rate <= -threshold, open a long position.
    Notional fixed (e.g., 80 USDC equivalent).

  EXIT:
    Close after `hold_hours` from entry, OR
    Hit stop_loss_bps (early stop), OR
    Hit take_profit_bps (early profit, if enabled).

  POSITION OVERLAP:
    By default, allow at most one open position per symbol (skip new signals
    if symbol already has a position). Optionally allow overlap.

The output is a per-trade ledger plus aggregate stats (total PnL, win rate,
payoff, drawdown, profit factor) per symbol and combined.

This is research/backtest only — no orders submitted.
"""
from __future__ import annotations

import argparse
import json
from dataclasses import dataclass, field, asdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from cointrading.historical_data import (
    HistoricalDataError,
    load_binance_vision_klines,
    parse_yyyy_mm_dd,
)
from cointrading.models import Kline
from cointrading.research.funding_carry import (
    DEFAULT_SYMBOLS,
    fetch_funding_rate_history,
    FundingObservation,
)


@dataclass
class Trade:
    symbol: str
    entry_time_ms: int
    exit_time_ms: int
    entry_price: float
    exit_price: float
    pnl_bps: float           # before cost
    pnl_bps_after_cost: float
    exit_reason: str         # "time" | "stop" | "tp"
    funding_rate: float


@dataclass
class BacktestResult:
    symbol: str
    trades: list[Trade] = field(default_factory=list)


def find_kline_at_or_after(klines_by_time: dict[int, Kline], sorted_times: list[int], t_ms: int) -> Kline | None:
    lo, hi = 0, len(sorted_times)
    while lo < hi:
        mid = (lo + hi) // 2
        if sorted_times[mid] < t_ms:
            lo = mid + 1
        else:
            hi = mid
    if lo >= len(sorted_times):
        return None
    return klines_by_time[sorted_times[lo]]


def simulate_symbol(
    *,
    symbol: str,
    klines: list[Kline],
    funding: list[FundingObservation],
    threshold: float,
    hold_hours: int,
    cost_bps_per_leg: float,
    stop_loss_bps: float | None,
    take_profit_bps: float | None,
) -> BacktestResult:
    by_time = {k.open_time: k for k in klines}
    times = sorted(by_time.keys())
    hour_ms = 3600 * 1000

    result = BacktestResult(symbol=symbol)
    in_position_until_ms = 0  # next allowed entry time

    for fobs in funding:
        if fobs.funding_rate > -threshold:
            continue
        if fobs.funding_time_ms < in_position_until_ms:
            continue
        # Entry at first kline open >= funding time
        entry_kline = find_kline_at_or_after(by_time, times, fobs.funding_time_ms)
        if entry_kline is None:
            continue
        if entry_kline.open_time - fobs.funding_time_ms > hour_ms:
            continue
        entry_price = entry_kline.close
        if entry_price == 0:
            continue
        time_exit_ms = entry_kline.open_time + hold_hours * hour_ms

        exit_price = entry_price
        exit_time_ms = time_exit_ms
        exit_reason = "time"

        # Walk forward kline-by-kline to check stops
        idx = times.index(entry_kline.open_time) + 1
        while idx < len(times) and times[idx] < time_exit_ms:
            kt = times[idx]
            k = by_time[kt]
            ret_bps = (k.close - entry_price) / entry_price * 10000.0
            if stop_loss_bps is not None and ret_bps <= -stop_loss_bps:
                exit_price = k.close
                exit_time_ms = kt
                exit_reason = "stop"
                break
            if take_profit_bps is not None and ret_bps >= take_profit_bps:
                exit_price = k.close
                exit_time_ms = kt
                exit_reason = "tp"
                break
            idx += 1
        else:
            # natural time exit: find kline at time_exit_ms
            exit_kline = find_kline_at_or_after(by_time, times, time_exit_ms)
            if exit_kline is None:
                continue
            exit_price = exit_kline.close
            exit_time_ms = exit_kline.open_time

        pnl_bps = (exit_price - entry_price) / entry_price * 10000.0
        pnl_after_cost = pnl_bps - 2 * cost_bps_per_leg  # round-trip = entry + exit fees
        result.trades.append(
            Trade(
                symbol=symbol,
                entry_time_ms=entry_kline.open_time,
                exit_time_ms=exit_time_ms,
                entry_price=entry_price,
                exit_price=exit_price,
                pnl_bps=pnl_bps,
                pnl_bps_after_cost=pnl_after_cost,
                exit_reason=exit_reason,
                funding_rate=fobs.funding_rate,
            )
        )
        in_position_until_ms = exit_time_ms

    return result


def aggregate_stats(trades: list[Trade]) -> dict:
    if not trades:
        return {"n": 0}
    wins = [t for t in trades if t.pnl_bps_after_cost > 0]
    losses = [t for t in trades if t.pnl_bps_after_cost <= 0]
    sum_pnl = sum(t.pnl_bps_after_cost for t in trades)
    sum_wins = sum(t.pnl_bps_after_cost for t in wins)
    sum_losses = sum(abs(t.pnl_bps_after_cost) for t in losses)
    avg_win = sum_wins / len(wins) if wins else 0.0
    avg_loss = sum_losses / len(losses) if losses else 0.0
    payoff = avg_win / avg_loss if avg_loss > 0 else float("inf")
    profit_factor = sum_wins / sum_losses if sum_losses > 0 else float("inf")

    # Drawdown on running cumulative PnL bps (treat each trade as 1 unit notional)
    eq = 0.0
    peak = 0.0
    max_dd = 0.0
    for t in sorted(trades, key=lambda x: x.exit_time_ms):
        eq += t.pnl_bps_after_cost
        if eq > peak:
            peak = eq
        max_dd = min(max_dd, eq - peak)

    by_reason: dict[str, int] = {}
    for t in trades:
        by_reason[t.exit_reason] = by_reason.get(t.exit_reason, 0) + 1

    return {
        "n": len(trades),
        "win_n": len(wins),
        "loss_n": len(losses),
        "win_rate": len(wins) / len(trades),
        "avg_win_bps": avg_win,
        "avg_loss_bps": avg_loss,
        "payoff": payoff,
        "profit_factor": profit_factor,
        "sum_bps": sum_pnl,
        "mean_bps": sum_pnl / len(trades),
        "max_dd_bps": max_dd,
        "exit_reasons": by_reason,
    }


def filter_trades_period(trades: list[Trade], start_ms: int, end_ms: int) -> list[Trade]:
    return [t for t in trades if start_ms <= t.entry_time_ms < end_ms]


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(description="Backtest funding mean-reversion rule")
    p.add_argument("--symbols", nargs="*", default=list(DEFAULT_SYMBOLS))
    p.add_argument("--start", default="2025-01-01")
    p.add_argument("--end", default=None)
    p.add_argument("--split", default="2026-01-01")
    p.add_argument("--threshold", type=float, default=0.0001, help="absolute |funding| (e.g., 0.0001=0.01%)")
    p.add_argument("--hold-hours", type=int, default=24)
    p.add_argument("--cost-bps", type=float, default=13.0)
    p.add_argument("--stop-bps", type=float, default=300.0, help="stop loss in bps; 0 to disable")
    p.add_argument("--tp-bps", type=float, default=0.0, help="take profit in bps; 0 to disable")
    p.add_argument("--out", default="data/funding_carry_backtest.json")
    args = p.parse_args(argv)

    start = parse_yyyy_mm_dd(args.start)
    end = parse_yyyy_mm_dd(args.end) if args.end else (date.today() - timedelta(days=1))
    split = parse_yyyy_mm_dd(args.split)
    stop_loss = args.stop_bps if args.stop_bps > 0 else None
    take_profit = args.tp_bps if args.tp_bps > 0 else None

    print("=== Funding Mean-Reversion BACKTEST ===")
    print(f"  Symbols    : {', '.join(args.symbols)}")
    print(f"  Range      : {start} ~ {end}")
    print(f"  Split      : {split}")
    print(f"  Threshold  : funding <= -{args.threshold*100:.3f}%")
    print(f"  Hold       : {args.hold_hours}h")
    print(f"  Cost       : {args.cost_bps} bps/leg (round-trip {2*args.cost_bps} bps)")
    print(f"  Stop loss  : {stop_loss} bps")
    print(f"  Take profit: {take_profit} bps")
    print(f"  Position   : long-only, no overlap (one position per symbol)")

    all_trades: list[Trade] = []
    per_symbol_trades: dict[str, list[Trade]] = {}
    for symbol in args.symbols:
        try:
            klines_result = load_binance_vision_klines(
                symbol=symbol, interval="1h", start_date=start, end_date=end
            )
            funding = fetch_funding_rate_history(symbol=symbol, start=start, end=end, verbose=False)
        except HistoricalDataError as e:
            print(f"  [error] {symbol}: {e}")
            continue
        result = simulate_symbol(
            symbol=symbol,
            klines=klines_result.klines,
            funding=funding,
            threshold=args.threshold,
            hold_hours=args.hold_hours,
            cost_bps_per_leg=args.cost_bps,
            stop_loss_bps=stop_loss,
            take_profit_bps=take_profit,
        )
        per_symbol_trades[symbol] = result.trades
        all_trades.extend(result.trades)

    split_ms = int(datetime(split.year, split.month, split.day, tzinfo=timezone.utc).timestamp() * 1000)
    end_ms = int(datetime(end.year, end.month, end.day, 23, 59, 59, tzinfo=timezone.utc).timestamp() * 1000) + 1

    def fmt(stats: dict) -> str:
        if stats.get("n", 0) == 0:
            return "n=0"
        return (
            f"n={stats['n']:>4}  WR={stats['win_rate']*100:5.1f}%  "
            f"avgW={stats['avg_win_bps']:+7.1f}  avgL={stats['avg_loss_bps']:+7.1f}  "
            f"payoff={stats['payoff']:.2f}  PF={stats['profit_factor']:.2f}  "
            f"sum={stats['sum_bps']:+8.1f} bps  mean={stats['mean_bps']:+6.1f}  "
            f"maxDD={stats['max_dd_bps']:+7.1f}  reasons={stats['exit_reasons']}"
        )

    print(f"\n=== PER-SYMBOL STATS (after-cost bps) ===")
    print("\n--- FULL ---")
    for symbol in args.symbols:
        trades = per_symbol_trades.get(symbol, [])
        print(f"  {symbol:<10}  {fmt(aggregate_stats(trades))}")
    print(f"  {'COMBINED':<10}  {fmt(aggregate_stats(all_trades))}")

    print("\n--- IN-SAMPLE ---")
    for symbol in args.symbols:
        trades = filter_trades_period(per_symbol_trades.get(symbol, []), 0, split_ms)
        print(f"  {symbol:<10}  {fmt(aggregate_stats(trades))}")
    in_all = [t for t in all_trades if t.entry_time_ms < split_ms]
    print(f"  {'COMBINED':<10}  {fmt(aggregate_stats(in_all))}")

    print("\n--- OUT-OF-SAMPLE ---")
    for symbol in args.symbols:
        trades = filter_trades_period(per_symbol_trades.get(symbol, []), split_ms, end_ms)
        print(f"  {symbol:<10}  {fmt(aggregate_stats(trades))}")
    out_all = [t for t in all_trades if split_ms <= t.entry_time_ms < end_ms]
    print(f"  {'COMBINED':<10}  {fmt(aggregate_stats(out_all))}")

    # Equity curve description (last 5 trades)
    if all_trades:
        ordered = sorted(all_trades, key=lambda x: x.exit_time_ms)
        eq = 0.0
        print("\n--- LAST 5 TRADES (any symbol) ---")
        for t in ordered[-5:]:
            entry_iso = datetime.fromtimestamp(t.entry_time_ms / 1000, tz=timezone.utc).isoformat()
            print(
                f"  {t.symbol:<10} entry={entry_iso}  fund={t.funding_rate*100:+.4f}%  "
                f"px {t.entry_price:.4f}->{t.exit_price:.4f}  "
                f"pnl={t.pnl_bps_after_cost:+7.1f} bps  reason={t.exit_reason}"
            )

    # Save full ledger
    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "config": {
            "symbols": args.symbols,
            "start": str(start),
            "end": str(end),
            "split": str(split),
            "threshold": args.threshold,
            "hold_hours": args.hold_hours,
            "cost_bps_per_leg": args.cost_bps,
            "stop_loss_bps": stop_loss,
            "take_profit_bps": take_profit,
        },
        "trades": [asdict(t) for t in all_trades],
        "stats_full": {s: aggregate_stats(per_symbol_trades.get(s, [])) for s in args.symbols},
        "stats_in_sample": {s: aggregate_stats(filter_trades_period(per_symbol_trades.get(s, []), 0, split_ms)) for s in args.symbols},
        "stats_out_of_sample": {s: aggregate_stats(filter_trades_period(per_symbol_trades.get(s, []), split_ms, end_ms)) for s in args.symbols},
        "stats_combined_full": aggregate_stats(all_trades),
        "stats_combined_in": aggregate_stats(in_all),
        "stats_combined_out": aggregate_stats(out_all),
    }
    Path(args.out).write_text(json.dumps(payload, indent=2, default=str))
    print(f"\n  saved: {args.out}")


if __name__ == "__main__":
    main()
