from __future__ import annotations

from dataclasses import asdict, dataclass
import sqlite3

from cointrading.config import TradingConfig
from cointrading.execution_gate import evaluate_simple_strategy_gate
from cointrading.exchange.binance_usdm import BinanceAPIError, BinanceUSDMClient
from cointrading.exchange_filters import SymbolFilters
from cointrading.live_guard import consume_live_one_shot, validate_live_one_shot
from cointrading.models import OrderIntent, OrderSide
from cointrading.risk import RiskManager
from cointrading.risk_state import evaluate_runtime_risk, risk_mode_ko
from cointrading.scalp_lifecycle import (
    _cancel_live_order,
    _client_order_id,
    _commission,
    _entry_filled,
    _live_fill_summary,
    _live_order_status,
    _local_order_commission,
    _order_status_value,
    _pnl,
    _record_live_fills,
    _seconds_ms,
    _stop_price,
    _stop_triggered,
    _take_profit_price,
    _target_filled,
)
from cointrading.storage import TradingStore, now_ms
from cointrading.strategy_eval import observed_evaluation_veto
from cointrading.strategy_router import SETUP_PASS, StrategySetup


@dataclass(frozen=True)
class StrategyLifecycleResult:
    strategy: str
    symbol: str
    action: str
    detail: str
    cycle_id: int | None = None


@dataclass(frozen=True)
class StrategyPlan:
    strategy: str
    execution_mode: str
    symbol: str
    side: str
    entry_order_type: str
    entry_price: float
    quantity: float
    take_profit_bps: float
    stop_loss_bps: float
    max_hold_seconds: int
    maker_one_way_bps: float
    taker_one_way_bps: float
    exit_profile: str
    exit_reason: str


def start_strategy_cycle_from_setup(
    client: BinanceUSDMClient,
    store: TradingStore,
    setup: StrategySetup,
    config: TradingConfig,
    *,
    symbol: str,
    bid: float,
    ask: float,
    timestamp_ms: int | None = None,
) -> StrategyLifecycleResult:
    ts = timestamp_ms or now_ms()
    symbol = symbol.upper()
    if setup.strategy == "maker_scalp":
        return StrategyLifecycleResult(setup.strategy, symbol, "skip", "maker scalp uses scalp engine")
    if not config.strategy_lifecycle_enabled:
        return StrategyLifecycleResult(
            setup.strategy,
            symbol,
            "blocked",
            "strategy lifecycle is disabled",
        )
    if setup.status != SETUP_PASS or setup.side not in {"long", "short"}:
        return StrategyLifecycleResult(setup.strategy, symbol, "blocked", setup.reason)
    if symbol in store.active_cycle_symbols():
        return StrategyLifecycleResult(
            setup.strategy,
            symbol,
            "skip",
            "another cycle is already active for this symbol",
        )
    if not config.dry_run and not config.live_strategy_lifecycle_enabled:
        return _blocked_order_attempt(
            store,
            setup,
            symbol,
            "live strategy lifecycle is disabled",
            config,
            ts,
        )
    if not config.dry_run and not config.live_trading_enabled:
        return _blocked_order_attempt(
            store,
            setup,
            symbol,
            "live trading flag is disabled",
            config,
            ts,
        )

    runtime_risk = evaluate_runtime_risk(store, config, symbol=symbol, current_ms=ts)
    if not runtime_risk.allows_new_entries:
        reason = f"runtime risk: {risk_mode_ko(runtime_risk.mode)} - {runtime_risk.reasons[0]}"
        return _blocked_order_attempt(store, setup, symbol, reason, config, ts)

    gate = evaluate_simple_strategy_gate(
        store,
        config,
        setup,
        symbol=symbol,
        dry_run=config.dry_run,
        timestamp_ms=ts,
    )
    if not gate.allowed:
        return _blocked_order_attempt(store, setup, symbol, gate.reason, config, ts)

    mid = (bid + ask) / 2.0
    plan = strategy_plan_from_setup(
        setup,
        config,
        symbol=symbol,
        bid=bid,
        ask=ask,
        macro_row=store.latest_market_regime(symbol),
    )
    if plan is None:
        return StrategyLifecycleResult(setup.strategy, symbol, "blocked", "no strategy plan")

    observed_row = store.latest_strategy_evaluation(
        symbol=symbol,
        regime=setup.strategy,
        side=setup.side,
        take_profit_bps=plan.take_profit_bps,
        stop_loss_bps=plan.stop_loss_bps,
        max_hold_seconds=plan.max_hold_seconds,
        execution_mode=plan.execution_mode,
        source="strategy_cycles",
    )
    veto = observed_evaluation_veto(observed_row, config)
    if veto is not None:
        return _blocked_order_attempt(store, setup, symbol, veto.reason, config, ts)

    risk_decision = RiskManager(config).validate_new_notional(
        config.initial_equity,
        plan.quantity * mid,
    )
    if not risk_decision.allowed:
        return _blocked_order_attempt(store, setup, symbol, risk_decision.reason, config, ts)

    intent = _entry_intent(plan)
    normalized_intent, filter_reason = _normalize_intent(client, intent, config)
    if normalized_intent is None:
        return _blocked_order_attempt(store, setup, symbol, filter_reason, config, ts)
    if not config.dry_run:
        guard = validate_live_one_shot(
            config,
            symbol=symbol,
            strategy=setup.strategy,
            notional=abs(normalized_intent.quantity) * mid,
        )
        if not guard.allowed:
            return _blocked_order_attempt(store, setup, symbol, guard.reason, config, ts)

    try:
        response = client.new_order(normalized_intent)
    except BinanceAPIError as exc:
        order_id = store.insert_order_attempt(
            normalized_intent,
            status="ERROR",
            dry_run=config.dry_run,
            reason=str(exc),
            response=None,
            timestamp_ms=ts,
        )
        return StrategyLifecycleResult(setup.strategy, symbol, "error", str(exc), order_id)

    status = "DRY_RUN" if config.dry_run else str(response.get("status", "SUBMITTED"))
    order_id = store.insert_order_attempt(
        normalized_intent,
        status=status,
        dry_run=config.dry_run,
        reason=f"{setup.strategy} entry; {filter_reason}",
        response=response,
        timestamp_ms=ts,
    )
    entry_price = normalized_intent.price if normalized_intent.price is not None else mid
    cycle_id = store.insert_strategy_cycle(
        strategy=setup.strategy,
        execution_mode=setup.execution_mode,
        symbol=symbol,
        side=setup.side,
        status="ENTRY_SUBMITTED",
        reason=f"entry submitted; {setup.reason}; exit profile {plan.exit_profile}: {plan.exit_reason}",
        entry_order_id=order_id,
        quantity=normalized_intent.quantity,
        entry_price=entry_price,
        target_price=_take_profit_price(setup.side, entry_price, plan.take_profit_bps),
        stop_price=_stop_price(setup.side, entry_price, plan.stop_loss_bps),
        entry_order_type=plan.entry_order_type,
        take_profit_bps=plan.take_profit_bps,
        stop_loss_bps=plan.stop_loss_bps,
        max_hold_seconds=plan.max_hold_seconds,
        maker_one_way_bps=plan.maker_one_way_bps,
        taker_one_way_bps=plan.taker_one_way_bps,
        entry_deadline_ms=ts + _seconds_ms(config.strategy_entry_timeout_seconds),
        dry_run=config.dry_run,
        last_mid_price=mid,
        setup=asdict(setup),
        timestamp_ms=ts,
    )
    if not config.dry_run:
        consume_live_one_shot(
            symbol=symbol,
            strategy=setup.strategy,
            notional=abs(normalized_intent.quantity) * entry_price,
            cycle_id=cycle_id,
        )
    return StrategyLifecycleResult(
        setup.strategy,
        symbol,
        "entry_submitted",
        (
            f"{plan.entry_order_type} entry={entry_price:.8f} "
            f"TP={plan.take_profit_bps:.1f} SL={plan.stop_loss_bps:.1f} "
            f"H={plan.max_hold_seconds}s profile={plan.exit_profile}"
        ),
        cycle_id,
    )


def manage_strategy_cycle(
    client: BinanceUSDMClient,
    store: TradingStore,
    cycle: sqlite3.Row,
    config: TradingConfig,
    *,
    bid: float,
    ask: float,
    timestamp_ms: int | None = None,
) -> StrategyLifecycleResult:
    ts = timestamp_ms or now_ms()
    mid = (bid + ask) / 2.0
    if not config.dry_run:
        if not config.live_strategy_lifecycle_enabled:
            store.update_strategy_cycle(
                int(cycle["id"]),
                reason="live strategy lifecycle is not enabled",
                last_mid_price=mid,
                timestamp_ms=ts,
            )
            return _result(cycle, "blocked", "live strategy lifecycle is not enabled")
        if str(cycle["status"]) == "ENTRY_SUBMITTED":
            return _manage_live_entry(client, store, cycle, config, mid, ts)
        if str(cycle["status"]) in {"OPEN", "EXIT_SUBMITTED"}:
            return _manage_live_open(client, store, cycle, config, bid, ask, mid, ts)
        return _result(cycle, "skip", f"inactive status {cycle['status']}")

    if str(cycle["status"]) == "ENTRY_SUBMITTED":
        return _manage_paper_entry(client, store, cycle, config, bid, ask, mid, ts)
    if str(cycle["status"]) in {"OPEN", "EXIT_SUBMITTED"}:
        return _manage_paper_open(client, store, cycle, config, bid, ask, mid, ts)
    return _result(cycle, "skip", f"inactive status {cycle['status']}")


def strategy_plan_from_setup(
    setup: StrategySetup,
    config: TradingConfig,
    *,
    symbol: str,
    bid: float,
    ask: float,
    macro_row=None,
) -> StrategyPlan | None:
    if setup.side not in {"long", "short"}:
        return None
    mid = (bid + ask) / 2.0
    if mid <= 0:
        return None
    if setup.strategy == "trend_follow":
        entry_type = "MARKET"
        tp = config.trend_take_profit_bps
        sl = config.trend_stop_loss_bps
        hold = int(config.trend_max_hold_seconds)
        mode = "taker_trend"
    elif setup.strategy == "range_reversion":
        entry_type = "LIMIT"
        tp = config.range_take_profit_bps
        sl = config.range_stop_loss_bps
        hold = int(config.range_max_hold_seconds)
        mode = "maker_range"
    elif setup.strategy == "breakout_reduced":
        entry_type = "MARKET"
        tp = config.breakout_take_profit_bps
        sl = config.breakout_stop_loss_bps
        hold = int(config.breakout_max_hold_seconds)
        mode = "taker_breakout"
    else:
        return None

    tp, sl, hold, profile, profile_reason = _adaptive_exit_profile(
        setup.strategy,
        config,
        base_take_profit_bps=tp,
        base_stop_loss_bps=sl,
        base_max_hold_seconds=hold,
        macro_row=macro_row,
    )

    notional = min(config.strategy_order_notional, config.max_single_order_notional)
    entry_price = _strategy_entry_price(setup.side, entry_type, bid, ask, mid)
    return StrategyPlan(
        strategy=setup.strategy,
        execution_mode=mode,
        symbol=symbol.upper(),
        side=setup.side,
        entry_order_type=entry_type,
        entry_price=entry_price,
        quantity=notional / entry_price,
        take_profit_bps=tp,
        stop_loss_bps=sl,
        max_hold_seconds=hold,
        maker_one_way_bps=config.maker_fee_rate * 10_000.0,
        taker_one_way_bps=config.taker_fee_rate * 10_000.0,
        exit_profile=profile,
        exit_reason=profile_reason,
    )


def _adaptive_exit_profile(
    strategy: str,
    config: TradingConfig,
    *,
    base_take_profit_bps: float,
    base_stop_loss_bps: float,
    base_max_hold_seconds: int,
    macro_row,
) -> tuple[float, float, int, str, str]:
    if not config.strategy_adaptive_exits_enabled:
        return (
            float(base_take_profit_bps),
            float(base_stop_loss_bps),
            int(base_max_hold_seconds),
            "fixed",
            "adaptive exits disabled",
        )
    if macro_row is None:
        return (
            float(base_take_profit_bps),
            float(base_stop_loss_bps),
            int(base_max_hold_seconds),
            "base",
            "macro data unavailable",
        )

    atr = max(0.0, _row_float(macro_row, "atr_bps"))
    trend_1h = abs(_row_float(macro_row, "trend_1h_bps"))
    trend_4h = abs(_row_float(macro_row, "trend_4h_bps"))
    trend_strength = max(trend_1h, trend_4h * 0.5)
    if atr <= 0 and trend_strength <= 0:
        return (
            float(base_take_profit_bps),
            float(base_stop_loss_bps),
            int(base_max_hold_seconds),
            "base",
            "macro volatility/trend unavailable",
        )

    if strategy == "trend_follow":
        if atr >= 70.0 or trend_strength >= 70.0:
            tp = _round_bps(_clamp(max(base_take_profit_bps, atr * 1.8, trend_strength * 1.2), base_take_profit_bps, 180.0))
            sl = _round_bps(_clamp(max(base_stop_loss_bps, atr * 0.55), base_stop_loss_bps, 60.0))
            hold = max(int(base_max_hold_seconds), 21_600)
            return tp, sl, hold, "trend_runner", _profile_reason(atr, trend_strength)
        if (0 < atr < 30.0) or trend_strength < 25.0:
            tp = _round_bps(_clamp(min(base_take_profit_bps, max(45.0, atr * 2.0 if atr > 0 else 60.0)), 45.0, base_take_profit_bps))
            sl = _round_bps(_clamp(min(base_stop_loss_bps, max(18.0, atr * 0.75 if atr > 0 else 25.0)), 18.0, base_stop_loss_bps))
            hold = min(int(base_max_hold_seconds), 7_200)
            return tp, sl, hold, "trend_tight", _profile_reason(atr, trend_strength)

    if strategy == "range_reversion":
        if 0 < atr < 20.0:
            tp = _round_bps(min(base_take_profit_bps, 20.0))
            sl = _round_bps(min(base_stop_loss_bps, 10.0))
            hold = min(int(base_max_hold_seconds), 1_800)
            return tp, sl, hold, "range_tight", _profile_reason(atr, trend_strength)
        if atr >= 45.0:
            tp = _round_bps(_clamp(max(base_take_profit_bps, atr * 0.8), base_take_profit_bps, 50.0))
            sl = _round_bps(_clamp(max(base_stop_loss_bps, atr * 0.4), base_stop_loss_bps, 25.0))
            hold = max(int(base_max_hold_seconds), 5_400)
            return tp, sl, hold, "range_wide", _profile_reason(atr, trend_strength)

    if strategy == "breakout_reduced":
        if atr >= 120.0 or trend_strength >= 100.0:
            tp = _round_bps(_clamp(max(base_take_profit_bps, atr * 1.6, trend_strength * 1.3), base_take_profit_bps, 240.0))
            sl = _round_bps(_clamp(max(base_stop_loss_bps, atr * 0.55), base_stop_loss_bps, 80.0))
            hold = max(int(base_max_hold_seconds), 10_800)
            return tp, sl, hold, "breakout_runner", _profile_reason(atr, trend_strength)
        if 0 < atr < 80.0:
            tp = _round_bps(min(base_take_profit_bps, 90.0))
            sl = _round_bps(min(base_stop_loss_bps, 35.0))
            hold = min(int(base_max_hold_seconds), 5_400)
            return tp, sl, hold, "breakout_quick", _profile_reason(atr, trend_strength)

    return (
        _round_bps(base_take_profit_bps),
        _round_bps(base_stop_loss_bps),
        int(base_max_hold_seconds),
        "base",
        _profile_reason(atr, trend_strength),
    )


def _manage_paper_entry(
    client: BinanceUSDMClient,
    store: TradingStore,
    cycle: sqlite3.Row,
    config: TradingConfig,
    bid: float,
    ask: float,
    mid: float,
    timestamp_ms: int,
) -> StrategyLifecycleResult:
    fill_price = _paper_entry_fill_price(cycle, bid, ask, mid)
    if fill_price is not None:
        quantity = float(cycle["quantity"])
        entry_side = "BUY" if str(cycle["side"]) == "long" else "SELL"
        store.record_fill(
            order_id=cycle["entry_order_id"],
            symbol=str(cycle["symbol"]),
            side=entry_side,
            price=fill_price,
            quantity=quantity,
            commission=_commission(fill_price, quantity, _entry_fee_bps(cycle)),
            commission_asset=config.equity_asset,
            raw={"paper": True, "role": "strategy_entry", "strategy": cycle["strategy"]},
            timestamp_ms=timestamp_ms,
        )
        store.update_strategy_cycle(
            int(cycle["id"]),
            status="OPEN",
            reason="entry filled; strategy position open",
            entry_price=fill_price,
            target_price=_take_profit_price(str(cycle["side"]), fill_price, float(cycle["take_profit_bps"])),
            stop_price=_stop_price(str(cycle["side"]), fill_price, float(cycle["stop_loss_bps"])),
            opened_ms=timestamp_ms,
            max_hold_deadline_ms=timestamp_ms + _seconds_ms(float(cycle["max_hold_seconds"])),
            last_mid_price=mid,
            timestamp_ms=timestamp_ms,
        )
        return _result(cycle, "entry_filled", "strategy position open")

    if timestamp_ms >= int(cycle["entry_deadline_ms"]):
        store.update_strategy_cycle(
            int(cycle["id"]),
            status="CANCELLED",
            reason="entry timeout",
            last_mid_price=mid,
            timestamp_ms=timestamp_ms,
        )
        return _result(cycle, "entry_cancelled", "entry timeout")

    store.update_strategy_cycle(
        int(cycle["id"]),
        reason="entry waiting",
        last_mid_price=mid,
        timestamp_ms=timestamp_ms,
    )
    return _result(cycle, "entry_waiting", "not filled yet")


def _manage_paper_open(
    client: BinanceUSDMClient,
    store: TradingStore,
    cycle: sqlite3.Row,
    config: TradingConfig,
    bid: float,
    ask: float,
    mid: float,
    timestamp_ms: int,
) -> StrategyLifecycleResult:
    side = str(cycle["side"])
    target = float(cycle["target_price"])
    stop = float(cycle["stop_price"])
    if _target_filled(side, bid, ask, target):
        return _close_paper_cycle(store, cycle, config, exit_price=target, reason="take_profit", timestamp_ms=timestamp_ms)
    if _stop_triggered(side, mid, stop):
        return _close_paper_cycle(store, cycle, config, exit_price=mid, reason="stop_loss", timestamp_ms=timestamp_ms)
    deadline = cycle["max_hold_deadline_ms"]
    if deadline is not None and timestamp_ms >= int(deadline):
        return _close_paper_cycle(store, cycle, config, exit_price=mid, reason="max_hold_exit", timestamp_ms=timestamp_ms)
    store.update_strategy_cycle(
        int(cycle["id"]),
        reason="strategy exit waiting",
        last_mid_price=mid,
        timestamp_ms=timestamp_ms,
    )
    return _result(cycle, "exit_waiting", "target/stop not hit")


def _manage_live_entry(
    client: BinanceUSDMClient,
    store: TradingStore,
    cycle: sqlite3.Row,
    config: TradingConfig,
    mid: float,
    timestamp_ms: int,
) -> StrategyLifecycleResult:
    local_order = store.order_by_id(int(cycle["entry_order_id"]))
    if local_order is None:
        store.update_strategy_cycle(int(cycle["id"]), reason="entry order row missing", timestamp_ms=timestamp_ms)
        return _result(cycle, "blocked", "entry order row missing")
    try:
        payload = _live_order_status(client, local_order)
    except BinanceAPIError as exc:
        store.update_strategy_cycle(
            int(cycle["id"]),
            reason=f"entry status error: {exc}",
            last_mid_price=mid,
            timestamp_ms=timestamp_ms,
        )
        return _result(cycle, "entry_status_error", str(exc))
    exchange_status = _order_status_value(payload)
    store.update_order_attempt(
        int(local_order["id"]),
        status=exchange_status or str(local_order["status"]),
        response=payload,
    )
    if exchange_status == "FILLED":
        summary = _live_fill_summary(client, local_order, payload, config)
        if summary.quantity <= 0 or summary.avg_price <= 0:
            store.update_strategy_cycle(int(cycle["id"]), reason="entry fill missing", timestamp_ms=timestamp_ms)
            return _result(cycle, "entry_status_error", "entry fill missing")
        _record_live_fills(
            store,
            local_order_id=int(local_order["id"]),
            symbol=str(cycle["symbol"]),
            side="BUY" if str(cycle["side"]) == "long" else "SELL",
            summary=summary,
            role="strategy_entry",
            timestamp_ms=timestamp_ms,
        )
        store.update_strategy_cycle(
            int(cycle["id"]),
            status="OPEN",
            reason="live entry filled; strategy position open",
            quantity=summary.quantity,
            entry_price=summary.avg_price,
            target_price=_take_profit_price(str(cycle["side"]), summary.avg_price, float(cycle["take_profit_bps"])),
            stop_price=_stop_price(str(cycle["side"]), summary.avg_price, float(cycle["stop_loss_bps"])),
            opened_ms=timestamp_ms,
            max_hold_deadline_ms=timestamp_ms + _seconds_ms(float(cycle["max_hold_seconds"])),
            last_mid_price=mid,
            timestamp_ms=timestamp_ms,
        )
        return _result(cycle, "entry_filled", "live strategy position open")
    if exchange_status in {"CANCELED", "EXPIRED", "REJECTED"}:
        reason = f"entry order {exchange_status.lower()}"
        store.update_strategy_cycle(
            int(cycle["id"]),
            status="CANCELLED",
            reason=reason,
            last_mid_price=mid,
            timestamp_ms=timestamp_ms,
        )
        return _result(cycle, "entry_cancelled", reason)
    if timestamp_ms >= int(cycle["entry_deadline_ms"]):
        _cancel_live_order(client, store, local_order)
        store.update_strategy_cycle(
            int(cycle["id"]),
            status="CANCELLED",
            reason="entry timeout; live order cancelled",
            last_mid_price=mid,
            timestamp_ms=timestamp_ms,
        )
        return _result(cycle, "entry_cancelled", "entry timeout; live order cancelled")
    store.update_strategy_cycle(
        int(cycle["id"]),
        reason=f"entry live waiting ({exchange_status or 'UNKNOWN'})",
        last_mid_price=mid,
        timestamp_ms=timestamp_ms,
    )
    return _result(cycle, "entry_waiting", "live order not filled yet")


def _manage_live_open(
    client: BinanceUSDMClient,
    store: TradingStore,
    cycle: sqlite3.Row,
    config: TradingConfig,
    bid: float,
    ask: float,
    mid: float,
    timestamp_ms: int,
) -> StrategyLifecycleResult:
    if str(cycle["status"]) == "EXIT_SUBMITTED" and cycle["exit_order_id"] is not None:
        local_order = store.order_by_id(int(cycle["exit_order_id"]))
        if local_order is not None:
            try:
                payload = _live_order_status(client, local_order)
            except BinanceAPIError as exc:
                store.update_strategy_cycle(
                    int(cycle["id"]),
                    reason=f"exit status error: {exc}",
                    timestamp_ms=timestamp_ms,
                )
                return _result(cycle, "exit_status_error", str(exc))
            exchange_status = _order_status_value(payload)
            store.update_order_attempt(
                int(local_order["id"]),
                status=exchange_status or str(local_order["status"]),
                response=payload,
            )
            if exchange_status == "FILLED":
                return _close_live_cycle(client, store, cycle, config, local_order, payload, timestamp_ms)

    side = str(cycle["side"])
    reason = ""
    if _target_filled(side, bid, ask, float(cycle["target_price"])):
        reason = "take_profit"
    elif _stop_triggered(side, mid, float(cycle["stop_price"])):
        reason = "stop_loss"
    elif cycle["max_hold_deadline_ms"] is not None and timestamp_ms >= int(cycle["max_hold_deadline_ms"]):
        reason = "max_hold_exit"
    if reason:
        return _submit_live_exit(client, store, cycle, config, reason=reason, timestamp_ms=timestamp_ms)

    store.update_strategy_cycle(
        int(cycle["id"]),
        reason="live strategy exit waiting",
        last_mid_price=mid,
        timestamp_ms=timestamp_ms,
    )
    return _result(cycle, "exit_waiting", "live target/stop not hit")


def _submit_live_exit(
    client: BinanceUSDMClient,
    store: TradingStore,
    cycle: sqlite3.Row,
    config: TradingConfig,
    *,
    reason: str,
    timestamp_ms: int,
) -> StrategyLifecycleResult:
    intent = OrderIntent(
        symbol=str(cycle["symbol"]),
        side="SELL" if str(cycle["side"]) == "long" else "BUY",
        quantity=float(cycle["quantity"]),
        order_type="MARKET",
        reduce_only=True,
        client_order_id=_client_order_id(reason, str(cycle["symbol"])),
    )
    normalized, filter_reason = _normalize_intent(client, intent, config)
    if normalized is None:
        store.update_strategy_cycle(
            int(cycle["id"]),
            status="EXIT_SUBMITTED",
            reason=f"{reason} blocked: {filter_reason}",
            timestamp_ms=timestamp_ms,
        )
        return _result(cycle, "exit_error", filter_reason)
    try:
        response = client.new_order(normalized)
    except BinanceAPIError as exc:
        store.update_strategy_cycle(
            int(cycle["id"]),
            status="EXIT_SUBMITTED",
            reason=f"{reason} submit error: {exc}",
            timestamp_ms=timestamp_ms,
        )
        return _result(cycle, "exit_error", str(exc))
    order_id = store.insert_order_attempt(
        normalized,
        status=str(response.get("status", "SUBMITTED")),
        dry_run=False,
        reason=reason,
        response=response,
        timestamp_ms=timestamp_ms,
    )
    local_order = store.order_by_id(order_id)
    assert local_order is not None
    if _order_status_value(response) == "FILLED":
        return _close_live_cycle(client, store, cycle, config, local_order, response, timestamp_ms)
    store.update_strategy_cycle(
        int(cycle["id"]),
        status="EXIT_SUBMITTED",
        reason=f"{reason} live market exit submitted",
        exit_order_id=order_id,
        timestamp_ms=timestamp_ms,
    )
    return _result(cycle, "exit_submitted", reason)


def _close_live_cycle(
    client: BinanceUSDMClient,
    store: TradingStore,
    cycle: sqlite3.Row,
    config: TradingConfig,
    local_order: sqlite3.Row,
    payload: dict,
    timestamp_ms: int,
) -> StrategyLifecycleResult:
    reason = str(local_order["reason"] or "take_profit")
    summary = _live_fill_summary(client, local_order, payload, config)
    if summary.quantity <= 0 or summary.avg_price <= 0:
        store.update_strategy_cycle(int(cycle["id"]), reason=f"{reason} fill missing", timestamp_ms=timestamp_ms)
        return _result(cycle, "exit_status_error", "exit fill missing")
    _record_live_fills(
        store,
        local_order_id=int(local_order["id"]),
        symbol=str(cycle["symbol"]),
        side="SELL" if str(cycle["side"]) == "long" else "BUY",
        summary=summary,
        role="strategy_exit",
        timestamp_ms=timestamp_ms,
    )
    realized = _live_realized_pnl(store, cycle, summary, config)
    status = "CLOSED" if reason == "take_profit" else "STOPPED"
    store.update_strategy_cycle(
        int(cycle["id"]),
        status=status,
        reason=reason,
        exit_order_id=int(local_order["id"]),
        closed_ms=timestamp_ms,
        last_mid_price=summary.avg_price,
        realized_pnl=realized,
        timestamp_ms=timestamp_ms,
    )
    return _result(cycle, reason, f"pnl={realized:.6f}")


def _close_paper_cycle(
    store: TradingStore,
    cycle: sqlite3.Row,
    config: TradingConfig,
    *,
    exit_price: float,
    reason: str,
    timestamp_ms: int,
) -> StrategyLifecycleResult:
    quantity = float(cycle["quantity"])
    side = str(cycle["side"])
    order_intent = OrderIntent(
        symbol=str(cycle["symbol"]),
        side="SELL" if side == "long" else "BUY",
        quantity=quantity,
        order_type="MARKET",
        reduce_only=True,
        client_order_id=_client_order_id(reason, str(cycle["symbol"])),
    )
    order_id = store.insert_order_attempt(
        order_intent,
        status="DRY_RUN",
        dry_run=True,
        reason=f"strategy {reason}",
        response={"dryRun": True},
        timestamp_ms=timestamp_ms,
    )
    fee_bps = float(cycle["taker_one_way_bps"])
    gross = _pnl(side, float(cycle["entry_price"]), exit_price, quantity)
    entry_fee = _commission(float(cycle["entry_price"]), quantity, _entry_fee_bps(cycle))
    exit_fee = _commission(exit_price, quantity, fee_bps)
    realized = gross - entry_fee - exit_fee
    store.record_fill(
        order_id=order_id,
        symbol=str(cycle["symbol"]),
        side="SELL" if side == "long" else "BUY",
        price=exit_price,
        quantity=quantity,
        commission=exit_fee,
        commission_asset=config.equity_asset,
        realized_pnl=realized,
        raw={"paper": True, "role": "strategy_exit", "strategy": cycle["strategy"], "reason": reason},
        timestamp_ms=timestamp_ms,
    )
    status = "CLOSED" if reason == "take_profit" else "STOPPED"
    store.update_strategy_cycle(
        int(cycle["id"]),
        status=status,
        reason=reason,
        exit_order_id=order_id,
        closed_ms=timestamp_ms,
        last_mid_price=exit_price,
        realized_pnl=realized,
        timestamp_ms=timestamp_ms,
    )
    return _result(cycle, reason, f"pnl={realized:.6f}")


def _entry_intent(plan: StrategyPlan) -> OrderIntent:
    side: OrderSide = "BUY" if plan.side == "long" else "SELL"
    return OrderIntent(
        symbol=plan.symbol,
        side=side,
        quantity=plan.quantity,
        order_type=plan.entry_order_type,
        price=plan.entry_price if plan.entry_order_type == "LIMIT" else None,
        time_in_force="GTX" if plan.entry_order_type == "LIMIT" else None,
        reduce_only=False,
        client_order_id=_client_order_id(plan.strategy, plan.symbol),
    )


def _normalize_intent(
    client: BinanceUSDMClient,
    intent: OrderIntent,
    config: TradingConfig,
) -> tuple[OrderIntent | None, str]:
    try:
        filters = SymbolFilters.from_exchange_info(client.exchange_info(intent.symbol), intent.symbol)
        normalized, reason = filters.normalize_intent(intent)
    except (AttributeError, BinanceAPIError, ValueError) as exc:
        if config.dry_run:
            return intent, f"dry-run without exchange filters: {exc}"
        return None, f"exchange filter error: {exc}"
    if normalized is not None and normalized.price is None:
        try:
            ticker = client.book_ticker(intent.symbol)
            mid = (float(ticker["bidPrice"]) + float(ticker["askPrice"])) / 2.0
        except (AttributeError, BinanceAPIError, KeyError, ValueError) as exc:
            if not config.dry_run:
                return None, f"market notional check failed: {exc}"
            return normalized, reason
        notional = normalized.quantity * mid
        min_notional = float(filters.min_order_notional_at(mid))
        if min_notional > 0 and notional < min_notional:
            return None, f"market notional {notional:.4f} below minimum {min_notional:.4f}"
    return normalized, reason


def _blocked_order_attempt(
    store: TradingStore,
    setup: StrategySetup,
    symbol: str,
    reason: str,
    config: TradingConfig,
    timestamp_ms: int,
) -> StrategyLifecycleResult:
    order_id = store.insert_order_attempt(
        OrderIntent(
            symbol=symbol,
            side="BUY" if setup.side != "short" else "SELL",
            quantity=0.0,
            order_type="MARKET",
        ),
        status="BLOCKED",
        dry_run=config.dry_run,
        reason=f"{setup.strategy}: {reason}",
        timestamp_ms=timestamp_ms,
    )
    return StrategyLifecycleResult(setup.strategy, symbol, "blocked", reason, order_id)


def _strategy_entry_price(side: str, entry_type: str, bid: float, ask: float, mid: float) -> float:
    if entry_type == "MARKET":
        return ask if side == "long" else bid
    return bid if side == "long" else ask


def _row_float(row, key: str) -> float:
    try:
        keys = row.keys()
    except AttributeError:
        value = row.get(key, 0.0)
    else:
        value = row[key] if key in keys else 0.0
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _clamp(value: float, minimum: float, maximum: float) -> float:
    return max(minimum, min(maximum, value))


def _round_bps(value: float) -> float:
    return round(float(value), 1)


def _profile_reason(atr_bps: float, trend_strength_bps: float) -> str:
    return f"ATR={atr_bps:.1f}bps trend={trend_strength_bps:.1f}bps"


def _paper_entry_fill_price(cycle: sqlite3.Row, bid: float, ask: float, mid: float) -> float | None:
    if str(cycle["entry_order_type"]) == "MARKET":
        return ask if str(cycle["side"]) == "long" else bid
    entry_price = float(cycle["entry_price"])
    if _entry_filled(str(cycle["side"]), bid, ask, entry_price):
        return entry_price
    return None


def _entry_fee_bps(cycle: sqlite3.Row) -> float:
    if str(cycle["entry_order_type"]) == "LIMIT":
        return float(cycle["maker_one_way_bps"])
    return float(cycle["taker_one_way_bps"])


def _live_realized_pnl(
    store: TradingStore,
    cycle: sqlite3.Row,
    exit_summary,
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


def _result(cycle: sqlite3.Row, action: str, detail: str) -> StrategyLifecycleResult:
    return StrategyLifecycleResult(
        str(cycle["strategy"]),
        str(cycle["symbol"]),
        action,
        detail,
        int(cycle["id"]),
    )
