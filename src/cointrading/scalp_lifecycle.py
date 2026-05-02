from __future__ import annotations

from dataclasses import dataclass
import json
import sqlite3
import time

from cointrading.config import TradingConfig
from cointrading.execution import build_post_only_intent, place_post_only_maker, submit_order
from cointrading.exchange.binance_usdm import BinanceAPIError, BinanceUSDMClient
from cointrading.live_guard import consume_live_one_shot, validate_live_one_shot
from cointrading.market_regime import scalp_allowed_by_macro
from cointrading.models import OrderIntent
from cointrading.risk_state import evaluate_runtime_risk, risk_mode_ko
from cointrading.scalping import ScalpSignal
from cointrading.storage import TradingStore, now_ms
from cointrading.strategy_eval import strategy_gate_decision


@dataclass(frozen=True)
class ScalpLifecycleResult:
    symbol: str
    action: str
    detail: str
    cycle_id: int | None = None


@dataclass(frozen=True)
class LiveFillSummary:
    quantity: float
    avg_price: float
    commission: float
    commission_asset: str
    realized_pnl: float | None
    trades: tuple[dict, ...]


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
    if signal.symbol.upper() in store.active_cycle_symbols():
        return ScalpLifecycleResult(
            signal.symbol,
            "skip",
            "another strategy cycle is already active for this symbol",
        )

    ts = timestamp_ms or now_ms()
    if not config.dry_run and not config.live_scalp_lifecycle_enabled:
        reason = "live scalp lifecycle is disabled; entry blocked until reconciliation is ready"
        order_id = _insert_blocked_attempt(
            store,
            signal,
            signal_id,
            ts,
            reason,
            dry_run=config.dry_run,
        )
        return ScalpLifecycleResult(signal.symbol, "blocked", reason, order_id)

    runtime_risk = evaluate_runtime_risk(store, config, symbol=signal.symbol, current_ms=ts)
    if not runtime_risk.allows_new_entries:
        reason = f"runtime risk: {risk_mode_ko(runtime_risk.mode)} - {runtime_risk.reasons[0]}"
        order_id = _insert_blocked_attempt(
            store,
            signal,
            signal_id,
            ts,
            reason,
            dry_run=config.dry_run,
        )
        return ScalpLifecycleResult(signal.symbol, "blocked", reason, order_id)

    macro_allowed, macro_reason = _macro_gate_decision(store, signal, config, ts)
    if not macro_allowed:
        order_id = _insert_blocked_attempt(
            store,
            signal,
            signal_id,
            ts,
            macro_reason,
            dry_run=config.dry_run,
        )
        return ScalpLifecycleResult(signal.symbol, "blocked", macro_reason, order_id)

    gate = strategy_gate_decision(store, signal, config)
    if not gate.allowed:
        order_id = _insert_blocked_attempt(
            store,
            signal,
            signal_id,
            ts,
            gate.reason,
            dry_run=config.dry_run,
        )
        return ScalpLifecycleResult(signal.symbol, "blocked", gate.reason, order_id)

    decision = build_post_only_intent(signal, config)
    if decision.intent is not None and not config.dry_run:
        price = decision.intent.price or signal.mid_price
        guard = validate_live_one_shot(
            config,
            symbol=signal.symbol,
            strategy="maker_scalp",
            notional=abs(decision.intent.quantity) * price,
        )
        if not guard.allowed:
            order_id = _insert_blocked_attempt(
                store,
                signal,
                signal_id,
                ts,
                guard.reason,
                dry_run=False,
            )
            return ScalpLifecycleResult(signal.symbol, "blocked", guard.reason, order_id)

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
    take_profit_bps = gate.take_profit_bps or config.scalp_take_profit_bps
    stop_loss_bps = gate.stop_loss_bps or config.scalp_stop_loss_bps
    max_hold_seconds = gate.max_hold_seconds or int(config.scalp_max_hold_seconds)
    target_price = _take_profit_price(signal.side, intent.price, take_profit_bps)
    stop_price = _stop_price(signal.side, intent.price, stop_loss_bps)
    cycle_id = store.insert_scalp_cycle(
        symbol=signal.symbol,
        side=signal.side,
        status="ENTRY_SUBMITTED",
        reason=f"entry submitted; {gate.reason}",
        entry_signal_id=signal_id,
        entry_order_id=result.order_id,
        quantity=abs(intent.quantity),
        entry_price=intent.price,
        target_price=target_price,
        stop_price=stop_price,
        maker_one_way_bps=signal.maker_roundtrip_bps / 2.0,
        taker_one_way_bps=signal.taker_roundtrip_bps / 2.0,
        entry_deadline_ms=ts + _seconds_ms(config.scalp_entry_timeout_seconds),
        strategy_evaluation_id=gate.evaluation_id,
        strategy_take_profit_bps=take_profit_bps,
        strategy_stop_loss_bps=stop_loss_bps,
        strategy_max_hold_seconds=max_hold_seconds,
        timestamp_ms=ts,
    )
    if not config.dry_run:
        consume_live_one_shot(
            symbol=signal.symbol,
            strategy="maker_scalp",
            notional=abs(intent.quantity) * intent.price,
            cycle_id=cycle_id,
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
        if not config.live_scalp_lifecycle_enabled:
            store.update_scalp_cycle(
                cycle_id,
                reason="live lifecycle reconciliation is not enabled",
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
            return _manage_live_entry_submitted(client, store, cycle, config, mid, ts)
        if status in {"OPEN", "EXIT_SUBMITTED"}:
            return _manage_live_open_cycle(client, store, cycle, config, bid, ask, mid, ts)
        return ScalpLifecycleResult(
            str(cycle["symbol"]),
            "skip",
            f"inactive status {status}",
            cycle_id,
        )

    if status == "ENTRY_SUBMITTED":
        return _manage_entry_submitted(client, store, cycle, config, bid, ask, mid, ts)
    if status in {"OPEN", "EXIT_SUBMITTED"}:
        return _manage_open_cycle(client, store, cycle, config, bid, ask, mid, ts)
    return ScalpLifecycleResult(str(cycle["symbol"]), "skip", f"inactive status {status}", cycle_id)


def _manage_live_entry_submitted(
    client: BinanceUSDMClient,
    store: TradingStore,
    cycle: sqlite3.Row,
    config: TradingConfig,
    mid: float,
    timestamp_ms: int,
) -> ScalpLifecycleResult:
    cycle_id = int(cycle["id"])
    symbol = str(cycle["symbol"])
    side = str(cycle["side"])
    local_order = store.order_by_id(int(cycle["entry_order_id"]))
    if local_order is None:
        store.update_scalp_cycle(
            cycle_id,
            reason="entry order row missing",
            last_mid_price=mid,
            timestamp_ms=timestamp_ms,
        )
        return ScalpLifecycleResult(symbol, "blocked", "entry order row missing", cycle_id)

    try:
        status_payload = _live_order_status(client, local_order)
    except BinanceAPIError as exc:
        store.update_scalp_cycle(
            cycle_id,
            reason=f"entry status error: {exc}",
            last_mid_price=mid,
            timestamp_ms=timestamp_ms,
        )
        return ScalpLifecycleResult(symbol, "entry_status_error", str(exc), cycle_id)

    exchange_status = _order_status_value(status_payload)
    store.update_order_attempt(
        int(local_order["id"]),
        status=exchange_status or str(local_order["status"]),
        response=status_payload,
    )

    if exchange_status == "FILLED":
        summary = _live_fill_summary(client, local_order, status_payload, config)
        if summary.quantity <= 0 or summary.avg_price <= 0:
            store.update_scalp_cycle(
                cycle_id,
                reason="entry filled but fill quantity/price is missing",
                last_mid_price=mid,
                timestamp_ms=timestamp_ms,
            )
            return ScalpLifecycleResult(symbol, "entry_status_error", "missing fill", cycle_id)
        _record_live_fills(
            store,
            local_order_id=int(local_order["id"]),
            symbol=symbol,
            side="BUY" if side == "long" else "SELL",
            summary=summary,
            role="entry",
            timestamp_ms=timestamp_ms,
        )
        take_profit_bps = _cycle_take_profit_bps(cycle, config)
        stop_loss_bps = _cycle_stop_loss_bps(cycle, config)
        target_price = _take_profit_price(side, summary.avg_price, take_profit_bps)
        stop_price = _stop_price(side, summary.avg_price, stop_loss_bps)
        exit_order_id = _submit_take_profit(
            client,
            store,
            cycle,
            config,
            price=target_price,
            quantity=summary.quantity,
            timestamp_ms=timestamp_ms,
        )
        store.update_scalp_cycle(
            cycle_id,
            status="EXIT_SUBMITTED",
            reason="entry filled; live take-profit submitted",
            exit_order_id=exit_order_id,
            quantity=summary.quantity,
            entry_price=summary.avg_price,
            target_price=target_price,
            stop_price=stop_price,
            opened_ms=timestamp_ms,
            exit_deadline_ms=timestamp_ms + _seconds_ms(config.scalp_exit_reprice_seconds),
            max_hold_deadline_ms=timestamp_ms + _seconds_ms(_cycle_max_hold_seconds(cycle, config)),
            last_mid_price=mid,
            timestamp_ms=timestamp_ms,
        )
        return ScalpLifecycleResult(symbol, "entry_filled", "live take-profit submitted", cycle_id)

    if exchange_status in {"CANCELED", "EXPIRED", "REJECTED"}:
        reason = f"entry order {exchange_status.lower()}"
        store.update_scalp_cycle(
            cycle_id,
            status="CANCELLED",
            reason=reason,
            last_mid_price=mid,
            timestamp_ms=timestamp_ms,
        )
        return ScalpLifecycleResult(symbol, "entry_cancelled", reason, cycle_id)

    if timestamp_ms >= int(cycle["entry_deadline_ms"]):
        cancel_payload = _cancel_live_order(client, store, local_order)
        reason = "entry timeout; live order cancelled"
        if cancel_payload:
            reason = f"{reason} ({_order_status_value(cancel_payload).lower()})"
        store.update_scalp_cycle(
            cycle_id,
            status="CANCELLED",
            reason=reason,
            last_mid_price=mid,
            timestamp_ms=timestamp_ms,
        )
        return ScalpLifecycleResult(symbol, "entry_cancelled", reason, cycle_id)

    store.update_scalp_cycle(
        cycle_id,
        reason=f"entry live waiting ({exchange_status or 'UNKNOWN'})",
        last_mid_price=mid,
        timestamp_ms=timestamp_ms,
    )
    return ScalpLifecycleResult(symbol, "entry_waiting", "live order not filled yet", cycle_id)


def _manage_live_open_cycle(
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

    exit_order_id = cycle["exit_order_id"]
    if exit_order_id is not None:
        local_exit_order = store.order_by_id(int(exit_order_id))
        if local_exit_order is not None:
            try:
                status_payload = _live_order_status(client, local_exit_order)
            except BinanceAPIError as exc:
                store.update_scalp_cycle(
                    cycle_id,
                    reason=f"exit status error: {exc}",
                    last_mid_price=mid,
                    timestamp_ms=timestamp_ms,
                )
                return ScalpLifecycleResult(symbol, "exit_status_error", str(exc), cycle_id)
            exchange_status = _order_status_value(status_payload)
            store.update_order_attempt(
                int(local_exit_order["id"]),
                status=exchange_status or str(local_exit_order["status"]),
                response=status_payload,
            )
            if exchange_status == "FILLED":
                close_reason = _live_exit_reason(local_exit_order)
                return _close_live_cycle(
                    client,
                    store,
                    cycle,
                    config,
                    local_order=local_exit_order,
                    status_payload=status_payload,
                    reason=close_reason,
                    timestamp_ms=timestamp_ms,
                )

    if _stop_triggered(side, mid, stop_price):
        return _submit_live_market_exit(
            client,
            store,
            cycle,
            config,
            reason="stop_loss",
            timestamp_ms=timestamp_ms,
        )

    max_hold_deadline = cycle["max_hold_deadline_ms"]
    if max_hold_deadline is not None and timestamp_ms >= int(max_hold_deadline):
        return _submit_live_market_exit(
            client,
            store,
            cycle,
            config,
            reason="max_hold_exit",
            timestamp_ms=timestamp_ms,
        )

    exit_deadline = cycle["exit_deadline_ms"]
    if exit_deadline is not None and timestamp_ms >= int(exit_deadline):
        if exit_order_id is not None:
            local_exit_order = store.order_by_id(int(exit_order_id))
            if local_exit_order is not None:
                _cancel_live_order(client, store, local_exit_order)
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
            reason="live take-profit repriced",
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
        reason="live exit waiting",
        last_mid_price=mid,
        timestamp_ms=timestamp_ms,
    )
    return ScalpLifecycleResult(symbol, "exit_waiting", "live target not filled", cycle_id)


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
            max_hold_deadline_ms=timestamp_ms + _seconds_ms(_cycle_max_hold_seconds(cycle, config)),
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
    quantity: float | None = None,
    timestamp_ms: int | None = None,
) -> int:
    target = price if price is not None else float(cycle["target_price"])
    intent = OrderIntent(
        symbol=str(cycle["symbol"]),
        side="SELL" if str(cycle["side"]) == "long" else "BUY",
        quantity=quantity if quantity is not None else float(cycle["quantity"]),
        order_type="LIMIT",
        price=target,
        time_in_force="GTX",
        reduce_only=True,
        client_order_id=_client_order_id("tp", str(cycle["symbol"])),
    )
    response = submit_order(client, intent, config)
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
            response = submit_order(client, intent, config)
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


def _submit_live_market_exit(
    client: BinanceUSDMClient,
    store: TradingStore,
    cycle: sqlite3.Row,
    config: TradingConfig,
    *,
    reason: str,
    timestamp_ms: int,
) -> ScalpLifecycleResult:
    symbol = str(cycle["symbol"])
    side = str(cycle["side"])
    cycle_id = int(cycle["id"])
    if cycle["exit_order_id"] is not None:
        local_exit_order = store.order_by_id(int(cycle["exit_order_id"]))
        if local_exit_order is not None:
            _cancel_live_order(client, store, local_exit_order)

    intent = OrderIntent(
        symbol=symbol,
        side="SELL" if side == "long" else "BUY",
        quantity=float(cycle["quantity"]),
        order_type="MARKET",
        reduce_only=True,
        client_order_id=_client_order_id(reason, symbol),
    )
    try:
        response = client.new_order(intent)
    except BinanceAPIError as exc:
        store.update_scalp_cycle(
            cycle_id,
            status="EXIT_SUBMITTED",
            reason=f"{reason} submit error: {exc}",
            timestamp_ms=timestamp_ms,
        )
        return ScalpLifecycleResult(symbol, "exit_error", str(exc), cycle_id)

    local_order_id = store.insert_order_attempt(
        intent,
        status=str(response.get("status", "SUBMITTED")),
        dry_run=False,
        reason=reason,
        response=response,
        signal_id=cycle["entry_signal_id"],
        timestamp_ms=timestamp_ms,
    )
    local_order = store.order_by_id(local_order_id)
    assert local_order is not None
    if _order_status_value(response) == "FILLED":
        return _close_live_cycle(
            client,
            store,
            cycle,
            config,
            local_order=local_order,
            status_payload=response,
            reason=reason,
            timestamp_ms=timestamp_ms,
        )
    store.update_scalp_cycle(
        cycle_id,
        status="EXIT_SUBMITTED",
        reason=f"{reason} live market exit submitted",
        exit_order_id=local_order_id,
        timestamp_ms=timestamp_ms,
    )
    return ScalpLifecycleResult(symbol, "exit_submitted", reason, cycle_id)


def _close_live_cycle(
    client: BinanceUSDMClient,
    store: TradingStore,
    cycle: sqlite3.Row,
    config: TradingConfig,
    *,
    local_order: sqlite3.Row,
    status_payload: dict,
    reason: str,
    timestamp_ms: int,
) -> ScalpLifecycleResult:
    symbol = str(cycle["symbol"])
    side = str(cycle["side"])
    summary = _live_fill_summary(client, local_order, status_payload, config)
    if summary.quantity <= 0 or summary.avg_price <= 0:
        store.update_scalp_cycle(
            int(cycle["id"]),
            reason=f"{reason} filled but fill quantity/price is missing",
            timestamp_ms=timestamp_ms,
        )
        return ScalpLifecycleResult(symbol, "exit_status_error", "missing fill", int(cycle["id"]))

    _record_live_fills(
        store,
        local_order_id=int(local_order["id"]),
        symbol=symbol,
        side="SELL" if side == "long" else "BUY",
        summary=summary,
        role="exit",
        timestamp_ms=timestamp_ms,
    )
    realized_pnl = _live_realized_pnl(store, cycle, summary, config)
    status = "CLOSED" if reason == "take_profit" else "STOPPED"
    store.update_scalp_cycle(
        int(cycle["id"]),
        status=status,
        reason=reason,
        exit_order_id=int(local_order["id"]),
        closed_ms=timestamp_ms,
        last_mid_price=summary.avg_price,
        realized_pnl=realized_pnl,
        timestamp_ms=timestamp_ms,
    )
    store.update_order_attempt(
        int(local_order["id"]),
        status=_order_status_value(status_payload) or "FILLED",
        reason=reason,
        response=status_payload,
    )
    return ScalpLifecycleResult(symbol, reason, f"pnl={realized_pnl:.6f}", int(cycle["id"]))


def _live_realized_pnl(
    store: TradingStore,
    cycle: sqlite3.Row,
    exit_summary: LiveFillSummary,
    config: TradingConfig,
) -> float:
    exit_commission = (
        exit_summary.commission if exit_summary.commission_asset == config.equity_asset else 0.0
    )
    if exit_summary.realized_pnl is not None:
        return exit_summary.realized_pnl - exit_commission

    entry_commission = _local_order_commission(
        store,
        int(cycle["entry_order_id"]),
        config.equity_asset,
    )
    gross = _pnl(
        str(cycle["side"]),
        float(cycle["entry_price"]),
        exit_summary.avg_price,
        exit_summary.quantity,
    )
    return gross - entry_commission - exit_commission


def _live_exit_reason(local_order: sqlite3.Row) -> str:
    reason = str(local_order["reason"] or "")
    if reason in {"stop_loss", "max_hold_exit"}:
        return reason
    return "take_profit"


def _live_order_status(client: BinanceUSDMClient, local_order: sqlite3.Row) -> dict:
    order_id, client_order_id = _exchange_order_refs(local_order)
    return client.order_status(
        symbol=str(local_order["symbol"]),
        order_id=order_id,
        orig_client_order_id=client_order_id,
    )


def _cancel_live_order(
    client: BinanceUSDMClient,
    store: TradingStore,
    local_order: sqlite3.Row,
) -> dict | None:
    try:
        order_id, client_order_id = _exchange_order_refs(local_order)
        response = client.cancel_order(
            symbol=str(local_order["symbol"]),
            order_id=order_id,
            orig_client_order_id=client_order_id,
        )
    except BinanceAPIError:
        return None
    store.update_order_attempt(
        int(local_order["id"]),
        status=_order_status_value(response) or "CANCELED",
        response=response,
    )
    return response


def _exchange_order_refs(local_order: sqlite3.Row) -> tuple[int | None, str | None]:
    payload = _order_response(local_order)
    order_id = _int_or_none(payload.get("orderId"))
    client_order_id = str(payload.get("clientOrderId") or local_order["client_order_id"] or "")
    return order_id, client_order_id or None


def _order_response(local_order: sqlite3.Row) -> dict:
    raw = local_order["response_json"]
    if not raw:
        return {}
    try:
        payload = json.loads(str(raw))
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}


def _live_fill_summary(
    client: BinanceUSDMClient,
    local_order: sqlite3.Row,
    status_payload: dict,
    config: TradingConfig,
) -> LiveFillSummary:
    order_id, _ = _exchange_order_refs(local_order)
    trades: list[dict] = []
    if order_id is not None:
        try:
            trades = list(
                client.account_trades(
                    symbol=str(local_order["symbol"]),
                    order_id=order_id,
                    limit=50,
                )
            )
        except BinanceAPIError:
            trades = []
    if trades:
        return _summary_from_trades(trades, config)
    return _summary_from_order_status(status_payload, config)


def _summary_from_trades(trades: list[dict], config: TradingConfig) -> LiveFillSummary:
    quantity = 0.0
    quote = 0.0
    commission = 0.0
    commission_assets: set[str] = set()
    realized_values: list[float] = []
    for trade in trades:
        price = _float_or_zero(trade.get("price"))
        qty = _float_or_zero(trade.get("qty"))
        quantity += qty
        quote += price * qty
        commission += _float_or_zero(trade.get("commission"))
        asset = str(trade.get("commissionAsset") or config.equity_asset)
        commission_assets.add(asset)
        if "realizedPnl" in trade and trade.get("realizedPnl") not in {None, ""}:
            realized_values.append(float(trade["realizedPnl"]))
    avg_price = quote / quantity if quantity > 0 else 0.0
    commission_asset = next(iter(commission_assets)) if len(commission_assets) == 1 else "MIXED"
    realized_pnl = sum(realized_values) if realized_values else None
    return LiveFillSummary(
        quantity=quantity,
        avg_price=avg_price,
        commission=commission,
        commission_asset=commission_asset,
        realized_pnl=realized_pnl,
        trades=tuple(trades),
    )


def _summary_from_order_status(status_payload: dict, config: TradingConfig) -> LiveFillSummary:
    quantity = _float_or_zero(
        status_payload.get("executedQty")
        or status_payload.get("cumQty")
        or status_payload.get("origQty")
    )
    avg_price = _float_or_zero(status_payload.get("avgPrice"))
    if avg_price <= 0 and quantity > 0:
        quote = _float_or_zero(status_payload.get("cumQuote"))
        avg_price = quote / quantity if quote > 0 else 0.0
    realized_pnl = None
    if status_payload.get("realizedPnl") not in {None, ""}:
        realized_pnl = float(status_payload["realizedPnl"])
    return LiveFillSummary(
        quantity=quantity,
        avg_price=avg_price,
        commission=0.0,
        commission_asset=config.equity_asset,
        realized_pnl=realized_pnl,
        trades=(),
    )


def _record_live_fills(
    store: TradingStore,
    *,
    local_order_id: int,
    symbol: str,
    side: str,
    summary: LiveFillSummary,
    role: str,
    timestamp_ms: int,
) -> None:
    if summary.trades:
        for trade in summary.trades:
            price = _float_or_zero(trade.get("price"))
            quantity = _float_or_zero(trade.get("qty"))
            realized = None
            if trade.get("realizedPnl") not in {None, ""}:
                realized = float(trade["realizedPnl"])
            store.record_fill(
                order_id=local_order_id,
                exchange_trade_id=str(trade.get("id") or ""),
                symbol=symbol,
                side=side,
                price=price,
                quantity=quantity,
                commission=_float_or_zero(trade.get("commission")),
                commission_asset=str(trade.get("commissionAsset") or summary.commission_asset),
                realized_pnl=realized,
                raw={"live": True, "role": role, "trade": trade},
                timestamp_ms=timestamp_ms,
            )
        return
    store.record_fill(
        order_id=local_order_id,
        symbol=symbol,
        side=side,
        price=summary.avg_price,
        quantity=summary.quantity,
        commission=summary.commission,
        commission_asset=summary.commission_asset,
        realized_pnl=summary.realized_pnl,
        raw={"live": True, "role": role, "source": "order_status"},
        timestamp_ms=timestamp_ms,
    )


def _local_order_commission(store: TradingStore, order_id: int, asset: str) -> float:
    with store.connect() as connection:
        row = connection.execute(
            """
            SELECT SUM(commission) AS commission
            FROM fills
            WHERE order_id=? AND commission_asset=?
            """,
            (order_id, asset),
        ).fetchone()
    return float(row["commission"] or 0.0)


def _order_status_value(payload: dict) -> str:
    return str(payload.get("status") or "").upper()


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


def _cycle_max_hold_seconds(cycle: sqlite3.Row, config: TradingConfig) -> int:
    value = cycle["strategy_max_hold_seconds"]
    if value is None:
        return int(config.scalp_max_hold_seconds)
    return int(value)


def _cycle_take_profit_bps(cycle: sqlite3.Row, config: TradingConfig) -> float:
    value = cycle["strategy_take_profit_bps"]
    if value is None:
        return float(config.scalp_take_profit_bps)
    return float(value)


def _cycle_stop_loss_bps(cycle: sqlite3.Row, config: TradingConfig) -> float:
    value = cycle["strategy_stop_loss_bps"]
    if value is None:
        return float(config.scalp_stop_loss_bps)
    return float(value)


def _macro_gate_decision(
    store: TradingStore,
    signal: ScalpSignal,
    config: TradingConfig,
    timestamp_ms: int,
) -> tuple[bool, str]:
    if not config.macro_regime_gate_enabled:
        return True, "macro regime gate disabled"
    row = store.latest_market_regime(signal.symbol)
    return scalp_allowed_by_macro(
        row,
        signal.side,
        max_age_ms=int(config.macro_regime_max_age_minutes) * 60_000,
        current_ms=timestamp_ms,
    )


def _insert_blocked_attempt(
    store: TradingStore,
    signal: ScalpSignal,
    signal_id: int,
    timestamp_ms: int,
    reason: str,
    *,
    dry_run: bool,
) -> int:
    return store.insert_order_attempt(
        OrderIntent(
            symbol=signal.symbol,
            side="BUY" if signal.side != "short" else "SELL",
            quantity=0.0,
            order_type="LIMIT",
            price=signal.mid_price,
            time_in_force="GTX",
        ),
        status="BLOCKED",
        dry_run=dry_run,
        reason=reason,
        signal_id=signal_id,
        timestamp_ms=timestamp_ms,
    )


def _pnl(side: str, entry_price: float, exit_price: float, quantity: float) -> float:
    if side == "long":
        return (exit_price - entry_price) * quantity
    return (entry_price - exit_price) * quantity


def _commission(price: float, quantity: float, fee_bps: float) -> float:
    return price * quantity * (fee_bps / 10_000.0)


def _float_or_zero(value) -> float:
    if value is None or str(value).strip() == "":
        return 0.0
    return float(value)


def _int_or_none(value) -> int | None:
    if value is None or str(value).strip() == "":
        return None
    return int(value)


def _seconds_ms(seconds: float) -> int:
    return int(seconds * 1000)


def _client_order_id(prefix: str, symbol: str) -> str:
    cleaned = "".join(ch for ch in prefix.lower() if ch.isalnum())
    return f"ct{cleaned}{symbol.lower()}{int(time.time() * 1000)}"[:36]
