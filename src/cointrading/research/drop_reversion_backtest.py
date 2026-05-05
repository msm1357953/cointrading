"""End-to-end backtest of the drop-reversion rule with stop loss.

Rule under test:
    ENTRY: when the close price has dropped by at least DROP_THRESHOLD over
           the last DROP_WINDOW hours, open a long at the current close.
           Cooldown of `cooldown_h` after each exit before re-entry on the
           same symbol.
    EXIT : (a) STOPPED if mark <= entry * (1 - stop_loss_bps/10000)
           (b) CLOSED  at hold_hours from entry if no stop hit.
           (c) TP optional.

Reuses the load_binance_vision_klines cache. Read-only research; no orders.
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


DEFAULT_SYMBOLS = ("BTCUSDC", "ETHUSDC", "SOLUSDC", "XRPUSDC", "DOGEUSDC")


@dataclass
class Trade:
    symbol: str
    entry_time_ms: int
    exit_time_ms: int
    entry_price: float
    exit_price: float
    pnl_bps: float
    pnl_bps_after_cost: float
    exit_reason: str
    drop_pct: float


def simulate_symbol(
    *,
    symbol: str,
    klines: list[Kline],
    drop_window_h: int,
    drop_threshold: float,
    hold_hours: int,
    cost_bps_per_leg: float,
    stop_loss_bps: float | None,
    take_profit_bps: float | None,
    cooldown_h: int,
) -> list[Trade]:
    if len(klines) < drop_window_h + hold_hours + 2:
        return []
    trades: list[Trade] = []
    n = len(klines)
    in_pos_until_idx = -1

    for i in range(drop_window_h, n - 1):
        if i <= in_pos_until_idx + cooldown_h:
            continue
        prior = klines[i - drop_window_h].close
        now = klines[i].close
        if prior <= 0:
            continue
        drop_pct = (now - prior) / prior
        if drop_pct > -drop_threshold:
            continue

        entry_idx = i
        entry_price = klines[entry_idx].close
        if entry_price <= 0:
            continue
        time_exit_idx = entry_idx + hold_hours
        time_exit_idx = min(time_exit_idx, n - 1)

        exit_price = entry_price
        exit_idx = time_exit_idx
        exit_reason = "time"

        for j in range(entry_idx + 1, time_exit_idx + 1):
            k = klines[j]
            ret_bps = (k.close - entry_price) / entry_price * 10000.0
            if stop_loss_bps is not None and ret_bps <= -stop_loss_bps:
                exit_price = k.close
                exit_idx = j
                exit_reason = "stop"
                break
            if take_profit_bps is not None and ret_bps >= take_profit_bps:
                exit_price = k.close
                exit_idx = j
                exit_reason = "tp"
                break
        else:
            exit_price = klines[time_exit_idx].close

        pnl_bps = (exit_price - entry_price) / entry_price * 10000.0
        pnl_after_cost = pnl_bps - 2 * cost_bps_per_leg
        trades.append(Trade(
            symbol=symbol,
            entry_time_ms=klines[entry_idx].open_time,
            exit_time_ms=klines[exit_idx].open_time,
            entry_price=entry_price,
            exit_price=exit_price,
            pnl_bps=pnl_bps,
            pnl_bps_after_cost=pnl_after_cost,
            exit_reason=exit_reason,
            drop_pct=drop_pct,
        ))
        in_pos_until_idx = exit_idx

    return trades


def aggregate_stats(trades: list[Trade]) -> dict:
    if not trades:
        return {"n": 0}
    wins = [t for t in trades if t.pnl_bps_after_cost > 0]
    losses = [t for t in trades if t.pnl_bps_after_cost <= 0]
    sum_pnl = sum(t.pnl_bps_after_cost for t in trades)
    avg_win = (sum(t.pnl_bps_after_cost for t in wins) / len(wins)) if wins else 0.0
    avg_loss = (sum(abs(t.pnl_bps_after_cost) for t in losses) / len(losses)) if losses else 0.0
    payoff = avg_win / avg_loss if avg_loss > 0 else float("inf")
    sum_wins = sum(t.pnl_bps_after_cost for t in wins)
    sum_losses = sum(abs(t.pnl_bps_after_cost) for t in losses)
    pf = sum_wins / sum_losses if sum_losses > 0 else float("inf")

    eq = 0.0
    peak = 0.0
    max_dd = 0.0
    for t in sorted(trades, key=lambda x: x.exit_time_ms):
        eq += t.pnl_bps_after_cost
        peak = max(peak, eq)
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
        "profit_factor": pf,
        "sum_bps": sum_pnl,
        "mean_bps": sum_pnl / len(trades),
        "max_dd_bps": max_dd,
        "exit_reasons": by_reason,
    }


def filter_period(trades: list[Trade], start_ms: int, end_ms: int) -> list[Trade]:
    return [t for t in trades if start_ms <= t.entry_time_ms < end_ms]


def fmt(stats: dict) -> str:
    if stats.get("n", 0) == 0:
        return "n=0"
    return (
        f"n={stats['n']:>4}  WR={stats['win_rate'] * 100:5.1f}%  "
        f"avgW=+{stats['avg_win_bps']:6.1f}  avgL=-{stats['avg_loss_bps']:6.1f}  "
        f"payoff={stats['payoff']:.2f}  PF={stats['profit_factor']:.2f}  "
        f"sum={stats['sum_bps']:+8.1f}  mean={stats['mean_bps']:+6.1f}  "
        f"maxDD={stats['max_dd_bps']:+8.1f}  "
        f"reasons={stats['exit_reasons']}"
    )


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(description="Drop-reversion rule backtest")
    p.add_argument("--symbols", nargs="*", default=list(DEFAULT_SYMBOLS))
    p.add_argument("--start", default="2025-01-01")
    p.add_argument("--end", default=None)
    p.add_argument("--split", default="2026-01-01")
    p.add_argument("--drop-window", type=int, default=1)
    p.add_argument("--drop-threshold", type=float, default=0.02)
    p.add_argument("--hold-hours", type=int, default=24)
    p.add_argument("--cost-bps", type=float, default=13.0)
    p.add_argument("--stop-bps", type=float, default=500.0)
    p.add_argument("--tp-bps", type=float, default=0.0)
    p.add_argument("--cooldown-multiplier", type=int, default=1,
                   help="cooldown after exit = drop_window * this multiplier (hours)")
    args = p.parse_args(argv)

    start = parse_yyyy_mm_dd(args.start)
    end = parse_yyyy_mm_dd(args.end) if args.end else (date.today() - timedelta(days=1))
    split = parse_yyyy_mm_dd(args.split)
    cooldown = args.drop_window * args.cooldown_multiplier
    sl = args.stop_bps if args.stop_bps > 0 else None
    tp = args.tp_bps if args.tp_bps > 0 else None

    print(
        f"trigger: drop ≤ -{args.drop_threshold * 100:.1f}% in {args.drop_window}h | "
        f"hold {args.hold_hours}h | "
        f"SL={'-' + str(int(sl)) + 'bps' if sl else 'none'} | "
        f"TP={'+' + str(int(tp)) + 'bps' if tp else 'none'} | "
        f"cooldown {cooldown}h | cost {args.cost_bps} bps/leg"
    )

    all_trades: list[Trade] = []
    per_symbol: dict[str, list[Trade]] = {}
    for symbol in args.symbols:
        try:
            klines_res = load_binance_vision_klines(
                symbol=symbol, interval="1h", start_date=start, end_date=end,
            )
        except HistoricalDataError as exc:
            print(f"  [error] {symbol}: {exc}")
            continue
        trades = simulate_symbol(
            symbol=symbol,
            klines=klines_res.klines,
            drop_window_h=args.drop_window,
            drop_threshold=args.drop_threshold,
            hold_hours=args.hold_hours,
            cost_bps_per_leg=args.cost_bps,
            stop_loss_bps=sl,
            take_profit_bps=tp,
            cooldown_h=cooldown,
        )
        per_symbol[symbol] = trades
        all_trades.extend(trades)

    split_ms = int(datetime(split.year, split.month, split.day, tzinfo=timezone.utc).timestamp() * 1000)
    end_ms = int(datetime(end.year, end.month, end.day, 23, 59, 59, tzinfo=timezone.utc).timestamp() * 1000) + 1

    print("\n--- FULL ---")
    for sym in args.symbols:
        print(f"  {sym:<10}  {fmt(aggregate_stats(per_symbol.get(sym, [])))}")
    print(f"  {'COMBINED':<10}  {fmt(aggregate_stats(all_trades))}")

    print("\n--- IN-SAMPLE ---")
    for sym in args.symbols:
        print(f"  {sym:<10}  {fmt(aggregate_stats(filter_period(per_symbol.get(sym, []), 0, split_ms)))}")
    in_all = [t for t in all_trades if t.entry_time_ms < split_ms]
    print(f"  {'COMBINED':<10}  {fmt(aggregate_stats(in_all))}")

    print("\n--- OUT-OF-SAMPLE ---")
    for sym in args.symbols:
        print(f"  {sym:<10}  {fmt(aggregate_stats(filter_period(per_symbol.get(sym, []), split_ms, end_ms)))}")
    out_all = [t for t in all_trades if split_ms <= t.entry_time_ms < end_ms]
    print(f"  {'COMBINED':<10}  {fmt(aggregate_stats(out_all))}")


if __name__ == "__main__":
    main()
