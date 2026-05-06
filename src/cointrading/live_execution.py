"""Shared live execution helpers for funding/wick paper-validated strategies.

Both lifecycles (funding_carry_long, wick_long) follow the same live model:

  1. open: MARKET BUY at trigger time, request RESULT response so we have
     avgPrice + executedQty in the new_order response.
  2. protective stop: reduce-only STOP_MARKET at entry * (1 - SL/10000),
     trigger=MARK_PRICE. Submitted right after entry fills.
  3. manage: each step poll the protective stop's status. If FILLED,
     close the cycle from that fill price + commissions.
  4. time exit: when the max_hold deadline passes, cancel the protective
     stop, then submit a reduce-only MARKET sell.

This module is the only place that talks to the live exchange for these
two strategies; both lifecycle modules call into it only when their
`is_live_armed()` check passes.

Defensive choices:
  - ALWAYS request RESULT on entry MARKET orders (we need avgPrice).
  - If protective stop submission fails, immediately submit a reduce-only
    market exit so we are never naked-long without a stop.
  - Cancel-then-replace is the only safe order modification primitive on
    Binance USDM, so all stop adjustments go cancel → submit.
"""
from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import Any

from cointrading.config import TradingConfig
from cointrading.exchange.binance_usdm import BinanceAPIError, BinanceUSDMClient
from cointrading.exchange_filters import SymbolFilters
from cointrading.models import OrderIntent
from cointrading.storage import TradingStore


logger = logging.getLogger(__name__)


def client_order_id(prefix: str, symbol: str) -> str:
    """Stable-ish client order id for tracing. Binance limits to 36 chars."""
    return f"{prefix}{symbol.lower()}{int(time.time() * 1000)}"[:36]


def normalize_market_intent(
    client: BinanceUSDMClient, intent: OrderIntent, config: TradingConfig
) -> tuple[OrderIntent | None, str]:
    """Apply tick/step/minNotional filters. Returns (intent_or_None, reason)."""
    try:
        filters = SymbolFilters.from_exchange_info(
            client.exchange_info(intent.symbol), intent.symbol
        )
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


@dataclass
class LiveEntryResult:
    success: bool
    order_id: int | None
    avg_price: float
    executed_qty: float
    detail: str
    response: dict | None = None


def submit_live_market_long(
    *,
    client: BinanceUSDMClient,
    store: TradingStore,
    config: TradingConfig,
    symbol: str,
    quantity: float,
    strategy_label: str,
    timestamp_ms: int,
) -> LiveEntryResult:
    """Submit MARKET BUY with RESULT response. Records order in DB regardless."""
    intent = OrderIntent(
        symbol=symbol,
        side="BUY",
        quantity=quantity,
        order_type="MARKET",
        response_type="RESULT",
        reduce_only=False,
        client_order_id=client_order_id(strategy_label, symbol),
    )
    normalized, reason = normalize_market_intent(client, intent, config)
    if normalized is None:
        order_id = store.insert_order_attempt(
            intent, status="BLOCKED", dry_run=False, reason=reason,
            timestamp_ms=timestamp_ms,
        )
        return LiveEntryResult(False, order_id, 0.0, 0.0, reason)

    try:
        response = client.new_order(normalized)
    except BinanceAPIError as exc:
        detail = f"market entry failed: {exc}"
        order_id = store.insert_order_attempt(
            normalized, status="ERROR", dry_run=False, reason=detail,
            timestamp_ms=timestamp_ms,
        )
        return LiveEntryResult(False, order_id, 0.0, 0.0, detail)

    avg_price = float(response.get("avgPrice") or 0.0)
    executed_qty = float(response.get("executedQty") or 0.0)
    status = str(response.get("status", "SUBMITTED"))
    order_id = store.insert_order_attempt(
        normalized, status=status, dry_run=False, reason="live_entry",
        response=response, timestamp_ms=timestamp_ms,
    )
    if status not in ("FILLED", "PARTIALLY_FILLED") or avg_price <= 0 or executed_qty <= 0:
        return LiveEntryResult(False, order_id, avg_price, executed_qty,
                               f"unexpected entry status: {status}")
    return LiveEntryResult(True, order_id, avg_price, executed_qty,
                           "live entry filled", response=response)


@dataclass
class StopSubmitResult:
    order_id: int | None
    detail: str


def submit_protective_stop(
    *,
    client: BinanceUSDMClient,
    store: TradingStore,
    config: TradingConfig,
    symbol: str,
    quantity: float,
    stop_price: float,
    strategy_label: str,
    timestamp_ms: int,
) -> StopSubmitResult:
    intent = OrderIntent(
        symbol=symbol,
        side="SELL",
        quantity=quantity,
        order_type="STOP_MARKET",
        stop_price=stop_price,
        working_type="MARK_PRICE",
        reduce_only=True,
        client_order_id=client_order_id(f"stop_{strategy_label}", symbol),
    )
    normalized, reason = normalize_market_intent(client, intent, config)
    if normalized is None:
        store.insert_order_attempt(
            intent, status="BLOCKED", dry_run=False, reason=reason,
            timestamp_ms=timestamp_ms,
        )
        return StopSubmitResult(None, f"protective stop blocked: {reason}")
    try:
        response = client.new_order(normalized)
    except BinanceAPIError as exc:
        detail = f"protective stop submit failed: {exc}"
        store.insert_order_attempt(
            normalized, status="ERROR", dry_run=False, reason=detail,
            timestamp_ms=timestamp_ms,
        )
        return StopSubmitResult(None, detail)
    order_id = store.insert_order_attempt(
        normalized, status=str(response.get("status", "SUBMITTED")),
        dry_run=False, reason="protective_stop", response=response,
        timestamp_ms=timestamp_ms,
    )
    return StopSubmitResult(order_id, "exchange protective stop set")


def submit_take_profit_market(
    *,
    client: BinanceUSDMClient,
    store: TradingStore,
    config: TradingConfig,
    symbol: str,
    side: str,         # the cycle side: "long" or "short"
    quantity: float,
    tp_price: float,
    strategy_label: str,
    timestamp_ms: int,
) -> StopSubmitResult:
    """Reduce-only TAKE_PROFIT_MARKET on MARK_PRICE. Side is opposite to the
    cycle side (long cycle → SELL TP; short cycle → BUY TP)."""
    order_side = "SELL" if side == "long" else "BUY"
    intent = OrderIntent(
        symbol=symbol,
        side=order_side,
        quantity=quantity,
        order_type="TAKE_PROFIT_MARKET",
        stop_price=tp_price,
        working_type="MARK_PRICE",
        reduce_only=True,
        client_order_id=client_order_id(f"tp_{strategy_label}", symbol),
    )
    normalized, reason = normalize_market_intent(client, intent, config)
    if normalized is None:
        store.insert_order_attempt(
            intent, status="BLOCKED", dry_run=False, reason=reason,
            timestamp_ms=timestamp_ms,
        )
        return StopSubmitResult(None, f"take-profit blocked: {reason}")
    try:
        response = client.new_order(normalized)
    except BinanceAPIError as exc:
        detail = f"take-profit submit failed: {exc}"
        store.insert_order_attempt(
            normalized, status="ERROR", dry_run=False, reason=detail,
            timestamp_ms=timestamp_ms,
        )
        return StopSubmitResult(None, detail)
    order_id = store.insert_order_attempt(
        normalized, status=str(response.get("status", "SUBMITTED")),
        dry_run=False, reason="take_profit", response=response,
        timestamp_ms=timestamp_ms,
    )
    return StopSubmitResult(order_id, "take-profit set")


def cancel_local_order(
    *, client: BinanceUSDMClient, store: TradingStore, local_order_id: int,
) -> tuple[bool, str]:
    """Cancel an order by its local DB id. Returns (cancelled_or_already_gone, detail)."""
    local_order = store.order_by_id(int(local_order_id))
    if local_order is None:
        return True, "no local order record"
    response_json = local_order["response_json"]
    exchange_order_id: int | None = None
    if response_json:
        try:
            import json as _json
            exchange_order_id = int(_json.loads(response_json).get("orderId"))
        except Exception:  # noqa: BLE001
            exchange_order_id = None
    try:
        client.cancel_order(
            symbol=str(local_order["symbol"]),
            order_id=exchange_order_id,
            orig_client_order_id=str(local_order["client_order_id"]) if not exchange_order_id else None,
        )
        return True, "cancelled"
    except BinanceAPIError as exc:
        msg = str(exc)
        if "Unknown order" in msg or "-2011" in msg:
            return True, "already gone"
        return False, f"cancel failed: {exc}"


def cancel_protective_stop(
    *,
    client: BinanceUSDMClient,
    store: TradingStore,
    cycle: Any,
) -> tuple[bool, str]:
    """Cancel the protective STOP referenced by cycle.exit_order_id."""
    if cycle["exit_order_id"] is None:
        return True, "no protective stop"
    return cancel_local_order(client=client, store=store,
                              local_order_id=int(cycle["exit_order_id"]))


def submit_live_market_close(
    *,
    client: BinanceUSDMClient,
    store: TradingStore,
    config: TradingConfig,
    cycle: Any,
    reason: str,
    timestamp_ms: int,
) -> LiveEntryResult:
    """Cancel protective stop (if any), then submit reduce-only MARKET close."""
    if cycle["exit_order_id"] is not None:
        ok, detail = cancel_protective_stop(client=client, store=store, cycle=cycle)
        if not ok:
            return LiveEntryResult(False, None, 0.0, 0.0, detail)

    symbol = str(cycle["symbol"])
    quantity = float(cycle["quantity"])
    intent = OrderIntent(
        symbol=symbol,
        side="SELL",
        quantity=quantity,
        order_type="MARKET",
        response_type="RESULT",
        reduce_only=True,
        client_order_id=client_order_id(reason, symbol),
    )
    normalized, norm_reason = normalize_market_intent(client, intent, config)
    if normalized is None:
        store.insert_order_attempt(
            intent, status="BLOCKED", dry_run=False, reason=norm_reason,
            timestamp_ms=timestamp_ms,
        )
        return LiveEntryResult(False, None, 0.0, 0.0, norm_reason)
    try:
        response = client.new_order(normalized)
    except BinanceAPIError as exc:
        detail = f"market close failed: {exc}"
        store.insert_order_attempt(
            normalized, status="ERROR", dry_run=False, reason=detail,
            timestamp_ms=timestamp_ms,
        )
        return LiveEntryResult(False, None, 0.0, 0.0, detail)
    avg_price = float(response.get("avgPrice") or 0.0)
    executed_qty = float(response.get("executedQty") or 0.0)
    status = str(response.get("status", "SUBMITTED"))
    order_id = store.insert_order_attempt(
        normalized, status=status, dry_run=False, reason=reason,
        response=response, timestamp_ms=timestamp_ms,
    )
    return LiveEntryResult(
        status in ("FILLED", "PARTIALLY_FILLED"),
        order_id, avg_price, executed_qty,
        f"market close {status}",
        response=response,
    )


def query_live_order_status(
    *,
    client: BinanceUSDMClient,
    store: TradingStore,
    order_id: int,
) -> dict | None:
    """Pull current status from the exchange for the given local order_id."""
    local = store.order_by_id(order_id)
    if local is None:
        return None
    response_json = local["response_json"]
    exchange_id: int | None = None
    if response_json:
        try:
            import json as _json
            exchange_id = int(_json.loads(response_json).get("orderId"))
        except Exception:  # noqa: BLE001
            exchange_id = None
    try:
        return client.order_status(
            symbol=str(local["symbol"]),
            order_id=exchange_id,
            orig_client_order_id=str(local["client_order_id"]) if not exchange_id else None,
        )
    except BinanceAPIError as exc:
        logger.warning("query_live_order_status %s failed: %s", order_id, exc)
        return None


def realized_pnl_from_close(
    *,
    cycle: Any,
    avg_exit_price: float,
    executed_qty: float,
    config: TradingConfig,
) -> float:
    """Net realized PnL using the actual exit price and round-trip taker fees.
    A more precise version would use commissions from /fapi/v1/userTrades, but
    for a first live phase we approximate with config fees.
    """
    entry_price = float(cycle["entry_price"])
    qty = executed_qty if executed_qty > 0 else float(cycle["quantity"])
    side = str(cycle["side"])
    if side == "long":
        gross = (avg_exit_price - entry_price) * qty
    else:
        gross = (entry_price - avg_exit_price) * qty
    notional_in = entry_price * qty
    notional_out = avg_exit_price * qty
    fees = (notional_in + notional_out) * config.taker_fee_rate
    return gross - fees
