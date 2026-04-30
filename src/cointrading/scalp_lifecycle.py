from __future__ import annotations

from dataclasses import dataclass
import sqlite3
import time

from cointrading.config import TradingConfig
from cointrading.execution import place_post_only_maker
from cointrading.exchange.binance_usdm import BinanceAPIError, BinanceUSDMClient
from cointrading.models import OrderIntent
from cointrading.scalping import ScalpSignal
from cointrading.storage import TradingStore, now_ms


@dataclass(frozen=True)
class ScalpLifecycleResult:
    symbol: str
    action: str
    detail: str
    cycle_id: int | None = None


def start_cycle_from_signal(
    client: BinanceUSDMClient,
    store: TradingStore,
    signal: ScalpSignal,
    config: TradingConfig,
    *,
    signal_id: int,
    timestamp_ms: int | None = None,
) -> ScalpLifecycleResult:
    if store.active_scalp_cycle(signal.symbol) is not None:
        return ScalpLifecycleResult(signal.symbol, "skip", "active cycle already exists")

    ts = timestamp_ms or now_ms()
    result = place_post_only_maker(client, store, signal, config, signal_id=signal_id)
    intent = result.decision.intent
    if not result.decision.allowed or intent is None or result.order_id is None:
        return ScalpLifecycleResult(
            signal.symbol,
            "blocked",
            result.decision.reason,
            result.order_id,
        )

    assert intent.price is not None
    target_price = _take_profit_price(signal.side, intent.price, config.scalp_take_profit_bps)
    stop_price = _stop_price(signal.side, intent.price, config.scalp_stop_loss_bps)
    cycle_id = store.insert_scalp_cycle(
        symbol=signal.symbol,
        side=signal.side,
        status="ENTRY_SUBMITTED",
        reason="entry submitted",
        entry_signal_id=signal_id,
        entry_order_id=result.order_id,
        quantity=abs(intent.quantity),
        entry_price=intent.price,
        target_price=target_price,
        stop_price=stop_price,
        maker_one_way_bps=signal.maker_roundtrip_bps / 2.0,
        taker_one_way_bps=signal.taker_roundtrip_bps / 2.0,
        entry_deadline_ms=ts + _seconds_ms(config.scalp_entry_timeout_seconds),
        timestamp_ms=ts,
    )
    return ScalpLifecycleResult(
        signal.symbol,
        "entry_submitted",
        f"entry={intent.price:.8f} tp={target_price:.8f} stop={stop_price:.8f}",
        cycle_id,
    )


def manage_cycle(
    client: BinanceUSDMClient,
    store: TradingStore,
    cycle: sqlite3.Row,
    config: TradingConfig,
    *,
    bid: float,
    ask: float,
    timestamp_ms: int | None = None,
) -> ScalpLifecycleResult:
    ts = timestamp_ms or now_ms()
    mid = (bid + ask) / 2.0
    cycle_id = int(cycle["id"])
    status = str(cycle["status"])
    side = str(cycle["side"])

    if not config.dry_run:
        store.update_scalp_cycle(
            cycle_id,
            reason="live exchange reconciliation is required before lifecycle automation",
            last_mid_price=mid,
            timestamp_ms=ts,
        )
        return ScalpLifecycleResult(
            str(cycle["symbol"]),
            "blocked",
            "live lifecycle reconciliation is not enabled",
            cycle_id,
        )

    if status == "ENTRY_SUBMITTED":
        return _manage_entry_submitted(client, store, cycle, config, bid, ask, mid, ts)
    if status in {"OPEN", "EXIT_SUBMITTED"}:
        return _manage_open_cycle(client, store, cycle, config, bid, ask, mid, ts)
    return ScalpLifecycleResult(str(cycle["symbol"]), "skip", f"inactive status {status}", cycle_id)


def _manage_entry_submitted(
    client: BinanceUSDMClient,
    store: TradingStore,
    cycle: sqlite3.Row,
    config: TradingConfig,
    bid: float,
    ask: float,
    mid: float,
    timestamp_ms: int,
) -> ScalpLifecycleResult:
    cycle_id = int(cycle["id"])
    symbol = str(cycle["symbol"])
    side = str(cycle["side"])
    entry_price = float(cycle["entry_price"])
    quantity = float(cycle["quantity"])

    if _entry_filled(side, bid, ask, entry_price):
        entry_side = "BUY" if side == "long" else "SELL"
        store.record_fill(
            order_id=cycle["entry_order_id"],
            symbol=symbol,
            side=entry_side,
            price=entry_price,
            quantity=quantity,
            commission=_commission(entry_price, quantity, float(cycle["maker_one_way_bps"])),
            commission_asset=config.equity_asset,
            raw={"paper": True, "role": "entry"},
            timestamp_ms=timestamp_ms,
        )
        exit_order_id = _submit_take_profit(
            client,
            store,
            cycle,
            config,
            timestamp_ms=timestamp_ms,
        )
        store.update_scalp_cycle(
            cycle_id,
            status="EXIT_SUBMITTED",
            reason="entry filled; take-profit submitted",
            exit_order_id=exit_order_id,
            opened_ms=timestamp_ms,
            exit_deadline_ms=timestamp_ms + _seconds_ms(config.scalp_exit_reprice_seconds),
            max_hold_deadline_ms=timestamp_ms + _seconds_ms(config.scalp_max_hold_seconds),
            last_mid_price=mid,
            timestamp_ms=timestamp_ms,
        )
        return ScalpLifecycleResult(symbol, "entry_filled", "take-profit submitted", cycle_id)

    if timestamp_ms >= int(cycle["entry_deadline_ms"]):
        store.update_scalp_cycle(
            cycle_id,
            status="CANCELLED",
            reason="entry timeout",
            last_mid_price=mid,
            timestamp_ms=timestamp_ms,
        )
        return ScalpLifecycleResult(symbol, "entry_cancelled", "entry timeout", cycle_id)

    if _entry_moved_away(side, entry_price, mid, config.scalp_requote_bps):
        store.update_scalp_cycle(
            cycle_id,
            status="REQUOTE",
            reason="entry moved away; wait for fresh quote",
            last_mid_price=mid,
            timestamp_ms=timestamp_ms,
        )
        return ScalpLifecycleResult(symbol, "entry_requote", "entry moved away", cycle_id)

    store.update_scalp_cycle(
        cycle_id,
        reason="entry waiting",
        last_mid_price=mid,
        timestamp_ms=timestamp_ms,
    )
    return ScalpLifecycleResult(symbol, "entry_waiting", "not filled yet", cycle_id)


def _manage_open_cycle(
    client: BinanceUSDMClient,
    store: TradingStore,
    cycle: sqlite3.Row,
    config: TradingConfig,
    bid: float,
    ask: float,
    mid: float,
    timestamp_ms: int,
) -> ScalpLifecycleResult:
    cycle_id = int(cycle["id"])
    symbol = str(cycle["symbol"])
    side = str(cycle["side"])
    target_price = float(cycle["target_price"])
    stop_price = float(cycle["stop_price"])

    if _target_filled(side, bid, ask, target_price):
        return _close_cycle(
            client,
            store,
            cycle,
            config,
            exit_price=target_price,
            fee_bps=float(cycle["maker_one_way_bps"]),
            reason="take_profit",
            order_type="LIMIT",
            timestamp_ms=timestamp_ms,
        )

    if _stop_triggered(side, mid, stop_price):
        return _close_cycle(
            client,
            store,
            cycle,
            config,
            exit_price=mid,
            fee_bps=float(cycle["taker_one_way_bps"]),
            reason="stop_loss",
            order_type="MARKET",
            timestamp_ms=timestamp_ms,
        )

    max_hold_deadline = cycle["max_hold_deadline_ms"]
    if max_hold_deadline is not None and timestamp_ms >= int(max_hold_deadline):
        return _close_cycle(
            client,
            store,
            cycle,
            config,
            exit_price=mid,
            fee_bps=float(cycle["taker_one_way_bps"]),
            reason="max_hold_exit",
            order_type="MARKET",
            timestamp_ms=timestamp_ms,
        )

    exit_deadline = cycle["exit_deadline_ms"]
    if exit_deadline is not None and timestamp_ms >= int(exit_deadline):
        new_target = _repriced_exit_price(side, bid, ask, float(cycle["entry_price"]), config)
        order_id = _submit_take_profit(
            client,
            store,
            cycle,
            config,
            price=new_target,
            timestamp_ms=timestamp_ms,
        )
        store.update_scalp_cycle(
            cycle_id,
            status="EXIT_SUBMITTED",
            reason="take-profit repriced",
            exit_order_id=order_id,
            target_price=new_target,
            exit_deadline_ms=timestamp_ms + _seconds_ms(config.scalp_exit_reprice_seconds),
            last_mid_price=mid,
            reprice_count=int(cycle["reprice_count"]) + 1,
            timestamp_ms=timestamp_ms,
        )
        return ScalpLifecycleResult(symbol, "exit_repriced", f"target={new_target:.8f}", cycle_id)

    store.update_scalp_cycle(
        cycle_id,
        reason="exit waiting",
        last_mid_price=mid,
        timestamp_ms=timestamp_ms,
    )
    return ScalpLifecycleResult(symbol, "exit_waiting", "target not hit", cycle_id)


def _submit_take_profit(
    client: BinanceUSDMClient,
    store: TradingStore,
    cycle: sqlite3.Row,
    config: TradingConfig,
    *,
    price: float | None = None,
    timestamp_ms: int | None = None,
) -> int:
    target = price if price is not None else float(cycle["target_price"])
    intent = OrderIntent(
        symbol=str(cycle["symbol"]),
        side="SELL" if str(cycle["side"]) == "long" else "BUY",
        quantity=float(cycle["quantity"]),
        order_type="LIMIT",
        price=target,
        time_in_force="GTX",
        reduce_only=True,
        client_order_id=_client_order_id("tp", str(cycle["symbol"])),
    )
    response = client.new_order(intent)
    return store.insert_order_attempt(
        intent,
        status="DRY_RUN" if config.dry_run else str(response.get("status", "SUBMITTED")),
        dry_run=config.dry_run,
        reason="take-profit post-only",
        response=response,
        signal_id=cycle["entry_signal_id"],
        timestamp_ms=timestamp_ms,
    )


def _close_cycle(
    client: BinanceUSDMClient,
    store: TradingStore,
    cycle: sqlite3.Row,
    config: TradingConfig,
    *,
    exit_price: float,
    fee_bps: float,
    reason: str,
    order_type: str,
    timestamp_ms: int,
) -> ScalpLifecycleResult:
    symbol = str(cycle["symbol"])
    side = str(cycle["side"])
    quantity = float(cycle["quantity"])
    if reason == "take_profit" and cycle["exit_order_id"] is not None:
        order_id = int(cycle["exit_order_id"])
    else:
        intent = OrderIntent(
            symbol=symbol,
            side="SELL" if side == "long" else "BUY",
            quantity=quantity,
            order_type=order_type,
            price=None if order_type == "MARKET" else exit_price,
            time_in_force="GTX" if order_type == "LIMIT" else None,
            reduce_only=True,
            client_order_id=_client_order_id(reason, symbol),
        )
        try:
            response = client.new_order(intent)
        except BinanceAPIError as exc:
            store.update_scalp_cycle(
                int(cycle["id"]),
                status="EXIT_SUBMITTED",
                reason=f"exit error: {exc}",
                timestamp_ms=timestamp_ms,
            )
            return ScalpLifecycleResult(symbol, "exit_error", str(exc), int(cycle["id"]))

        order_id = store.insert_order_attempt(
            intent,
            status="DRY_RUN" if config.dry_run else str(response.get("status", "SUBMITTED")),
            dry_run=config.dry_run,
            reason=reason,
            response=response,
            signal_id=cycle["entry_signal_id"],
            timestamp_ms=timestamp_ms,
        )
    gross_pnl = _pnl(side, float(cycle["entry_price"]), exit_price, quantity)
    entry_fee = _commission(
        float(cycle["entry_price"]),
        quantity,
        float(cycle["maker_one_way_bps"]),
    )
    exit_fee = _commission(exit_price, quantity, fee_bps)
    realized_pnl = gross_pnl - entry_fee - exit_fee
    store.record_fill(
        order_id=order_id,
        symbol=symbol,
        side="SELL" if side == "long" else "BUY",
        price=exit_price,
        quantity=quantity,
        commission=exit_fee,
        commission_asset=config.equity_asset,
        realized_pnl=realized_pnl,
        raw={"paper": True, "role": "exit", "reason": reason},
        timestamp_ms=timestamp_ms,
    )
    status = "CLOSED" if reason == "take_profit" else "STOPPED"
    store.update_scalp_cycle(
        int(cycle["id"]),
        status=status,
        reason=reason,
        exit_order_id=order_id,
        closed_ms=timestamp_ms,
        last_mid_price=exit_price,
        realized_pnl=realized_pnl,
        timestamp_ms=timestamp_ms,
    )
    return ScalpLifecycleResult(symbol, reason, f"pnl={realized_pnl:.6f}", int(cycle["id"]))


def _entry_filled(side: str, bid: float, ask: float, entry_price: float) -> bool:
    if side == "long":
        return ask <= entry_price
    return bid >= entry_price


def _target_filled(side: str, bid: float, ask: float, target_price: float) -> bool:
    if side == "long":
        return bid >= target_price
    return ask <= target_price


def _stop_triggered(side: str, mid: float, stop_price: float) -> bool:
    if side == "long":
        return mid <= stop_price
    return mid >= stop_price


def _entry_moved_away(side: str, entry_price: float, mid: float, threshold_bps: float) -> bool:
    if entry_price <= 0:
        return False
    if side == "long":
        moved_bps = ((mid / entry_price) - 1.0) * 10_000.0
    else:
        moved_bps = ((entry_price / mid) - 1.0) * 10_000.0 if mid > 0 else 0.0
    return moved_bps >= threshold_bps


def _take_profit_price(side: str, entry_price: float, take_profit_bps: float) -> float:
    multiplier = 1.0 + (take_profit_bps / 10_000.0)
    if side == "long":
        return entry_price * multiplier
    return entry_price / multiplier


def _stop_price(side: str, entry_price: float, stop_loss_bps: float) -> float:
    multiplier = 1.0 + (stop_loss_bps / 10_000.0)
    if side == "long":
        return entry_price / multiplier
    return entry_price * multiplier


def _repriced_exit_price(
    side: str,
    bid: float,
    ask: float,
    entry_price: float,
    config: TradingConfig,
) -> float:
    min_profit = config.scalp_min_exit_bps / 10_000.0
    if side == "long":
        return max(ask, entry_price * (1.0 + min_profit))
    return min(bid, entry_price / (1.0 + min_profit))


def _pnl(side: str, entry_price: float, exit_price: float, quantity: float) -> float:
    if side == "long":
        return (exit_price - entry_price) * quantity
    return (entry_price - exit_price) * quantity


def _commission(price: float, quantity: float, fee_bps: float) -> float:
    return price * quantity * (fee_bps / 10_000.0)


def _seconds_ms(seconds: float) -> int:
    return int(seconds * 1000)


def _client_order_id(prefix: str, symbol: str) -> str:
    cleaned = "".join(ch for ch in prefix.lower() if ch.isalnum())
    return f"ct{cleaned}{symbol.lower()}{int(time.time() * 1000)}"[:36]
