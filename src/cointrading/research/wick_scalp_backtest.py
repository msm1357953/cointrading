"""End-to-end backtest for short-term wick reversion ('꼬리 잡기').

Trigger (real wick scalping, not generic pullback):
    On 5-minute candles, a "long lower wick" = the candle's range is
    dominated by the recovery from the low. We require:
        lower_wick_ratio = lower_wick / range >= W
        drop_pct = (open - low) / open >= D
    This catches the kind of bar where price spiked down inside the
    bar and then recovered — the canonical 'long lower shadow'.

Entry: long at the close of the trigger bar.
Exit:  earlier of (stop_loss, time_exit, take_profit if set).

Costs: 13 bps round-trip taker + slippage (one-leg 6.5 bps assumed).
This is realistic for retail USDC futures without maker rebates.

Data: 5-minute klines from data.binance.vision (cached locally).
"""
from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone

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
    wick_ratio: float


def detect_wick(kline: Kline, *, min_wick_ratio: float, min_drop_pct: float) -> tuple[bool, float, float]:
    """Return (triggered, drop_pct, wick_ratio)."""
    o, h, l, c = kline.open, kline.high, kline.low, kline.close
    rng = h - l
    if rng <= 0 or o <= 0:
        return False, 0.0, 0.0
    lower_wick = (min(o, c) - l)  # body bottom - low
    if lower_wick < 0:
        return False, 0.0, 0.0
    wick_ratio = lower_wick / rng
    drop_pct = (o - l) / o  # peak-to-trough drop within the bar
    triggered = wick_ratio >= min_wick_ratio and drop_pct >= min_drop_pct
    return triggered, drop_pct, wick_ratio


def simulate_symbol(
    *,
    symbol: str,
    klines: list[Kline],
    min_wick_ratio: float,
    min_drop_pct: float,
    hold_bars: int,
    cost_bps_per_leg: float,
    stop_loss_bps: float | None,
    take_profit_bps: float | None,
    cooldown_bars: int,
) -> list[Trade]:
    if len(klines) < hold_bars + 2:
        return []
    trades: list[Trade] = []
    n = len(klines)
    in_pos_until_idx = -1

    for i in range(n - 1):
        if i <= in_pos_until_idx + cooldown_bars:
            continue
        triggered, drop_pct, wick_ratio = detect_wick(
            klines[i], min_wick_ratio=min_wick_ratio, min_drop_pct=min_drop_pct,
        )
        if not triggered:
            continue
        entry_price = klines[i].close
        if entry_price <= 0:
            continue
        time_exit_idx = min(i + hold_bars, n - 1)
        exit_idx = time_exit_idx
        exit_price = klines[time_exit_idx].close
        exit_reason = "time"

        for j in range(i + 1, time_exit_idx + 1):
            k = klines[j]
            ret_bps = (k.close - entry_price) / entry_price * 10_000.0
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

        pnl_bps = (exit_price - entry_price) / entry_price * 10_000.0
        pnl_after_cost = pnl_bps - 2 * cost_bps_per_leg
        trades.append(Trade(
            symbol=symbol,
            entry_time_ms=klines[i].open_time,
            exit_time_ms=klines[exit_idx].open_time,
            entry_price=entry_price,
            exit_price=exit_price,
            pnl_bps=pnl_bps,
            pnl_bps_after_cost=pnl_after_cost,
            exit_reason=exit_reason,
            drop_pct=drop_pct,
            wick_ratio=wick_ratio,
        ))
        in_pos_until_idx = exit_idx

    return trades


def aggregate_stats(trades: list[Trade]) -> dict:
    if not trades:
        return {"n": 0}
    wins = [t for t in trades if t.pnl_bps_after_cost > 0]
    losses = [t for t in trades if t.pnl_bps_after_cost <= 0]
    sum_pnl = sum(t.pnl_bps_after_cost for t in trades)
    avg_win = sum(t.pnl_bps_after_cost for t in wins) / len(wins) if wins else 0.0
    avg_loss = sum(abs(t.pnl_bps_after_cost) for t in losses) / len(losses) if losses else 0.0
    payoff = avg_win / avg_loss if avg_loss > 0 else float("inf")
    sum_w = sum(t.pnl_bps_after_cost for t in wins)
    sum_l = sum(abs(t.pnl_bps_after_cost) for t in losses)
    pf = sum_w / sum_l if sum_l > 0 else float("inf")
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
        f"n={stats['n']:>5}  WR={stats['win_rate']*100:5.1f}%  "
        f"payoff={stats['payoff']:.2f}  PF={stats['profit_factor']:.2f}  "
        f"mean={stats['mean_bps']:+6.1f}  sum={stats['sum_bps']:+9.1f}  "
        f"maxDD={stats['max_dd_bps']:+9.1f}  reasons={stats['exit_reasons']}"
    )


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(description="Wick reversion backtest (꼬리 잡기)")
    p.add_argument("--symbols", nargs="*", default=list(DEFAULT_SYMBOLS))
    p.add_argument("--start", default="2025-01-01")
    p.add_argument("--end", default=None)
    p.add_argument("--split", default="2026-01-01")
    p.add_argument("--interval", default="5m", help="5m or 15m")
    p.add_argument("--wick-ratio", type=float, default=0.5,
                   help="lower_wick / range minimum (e.g., 0.5 = wick is half the bar)")
    p.add_argument("--drop-pct", type=float, default=0.005,
                   help="(open-low)/open minimum (e.g., 0.005 = 0.5%% intrabar drop)")
    p.add_argument("--hold-bars", type=int, default=6,
                   help="hold for this many bars (5m * 6 = 30 min)")
    p.add_argument("--cooldown-bars", type=int, default=2)
    p.add_argument("--cost-bps", type=float, default=6.5,
                   help="per-leg cost (taker fee + slippage)")
    p.add_argument("--stop-bps", type=float, default=50.0)
    p.add_argument("--tp-bps", type=float, default=0.0)
    args = p.parse_args(argv)

    start = parse_yyyy_mm_dd(args.start)
    end = parse_yyyy_mm_dd(args.end) if args.end else (date.today() - timedelta(days=1))
    split = parse_yyyy_mm_dd(args.split)
    sl = args.stop_bps if args.stop_bps > 0 else None
    tp = args.tp_bps if args.tp_bps > 0 else None
    bar_minutes = 5 if args.interval == "5m" else 15

    print(
        f"trigger: lower_wick_ratio>={args.wick_ratio}, intrabar drop>={args.drop_pct*100:.2f}% "
        f"({args.interval} candles)"
    )
    print(
        f"  hold {args.hold_bars} bars ({args.hold_bars * bar_minutes} min) | "
        f"SL={'-' + str(int(sl)) + 'bps' if sl else 'none'} | "
        f"TP={'+' + str(int(tp)) + 'bps' if tp else 'none'} | "
        f"cooldown {args.cooldown_bars} bars | cost {args.cost_bps} bps/leg"
    )

    all_trades: list[Trade] = []
    per_symbol: dict[str, list[Trade]] = {}
    for symbol in args.symbols:
        try:
            print(f"  loading {symbol} {args.interval}...", end=" ", flush=True)
            klines_res = load_binance_vision_klines(
                symbol=symbol, interval=args.interval, start_date=start, end_date=end,
            )
            print(f"{len(klines_res.klines)} bars")
        except HistoricalDataError as exc:
            print(f"[error] {exc}")
            continue
        trades = simulate_symbol(
            symbol=symbol,
            klines=klines_res.klines,
            min_wick_ratio=args.wick_ratio,
            min_drop_pct=args.drop_pct,
            hold_bars=args.hold_bars,
            cost_bps_per_leg=args.cost_bps,
            stop_loss_bps=sl,
            take_profit_bps=tp,
            cooldown_bars=args.cooldown_bars,
        )
        per_symbol[symbol] = trades
        all_trades.extend(trades)

    split_ms = int(datetime(split.year, split.month, split.day, tzinfo=timezone.utc).timestamp() * 1000)
    end_ms = int(datetime(end.year, end.month, end.day, 23, 59, 59, tzinfo=timezone.utc).timestamp() * 1000) + 1

    print("\n--- FULL ---")
    for s in args.symbols:
        print(f"  {s:<10}  {fmt(aggregate_stats(per_symbol.get(s, [])))}")
    print(f"  {'COMBINED':<10}  {fmt(aggregate_stats(all_trades))}")

    print("\n--- IN-SAMPLE ---")
    for s in args.symbols:
        print(f"  {s:<10}  {fmt(aggregate_stats(filter_period(per_symbol.get(s, []), 0, split_ms)))}")
    in_all = [t for t in all_trades if t.entry_time_ms < split_ms]
    print(f"  {'COMBINED':<10}  {fmt(aggregate_stats(in_all))}")

    print("\n--- OUT-OF-SAMPLE ---")
    for s in args.symbols:
        print(f"  {s:<10}  {fmt(aggregate_stats(filter_period(per_symbol.get(s, []), split_ms, end_ms)))}")
    out_all = [t for t in all_trades if split_ms <= t.entry_time_ms < end_ms]
    print(f"  {'COMBINED':<10}  {fmt(aggregate_stats(out_all))}")


if __name__ == "__main__":
    main()
