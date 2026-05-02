from __future__ import annotations

from dataclasses import dataclass
import time

from cointrading.config import TradingConfig
from cointrading.exchange.binance_usdm import BinanceAPIError, BinanceUSDMClient
from cointrading.exchange_filters import SymbolFilters
from cointrading.models import OrderIntent, OrderSide
from cointrading.risk import RiskManager
from cointrading.scalping import ScalpSignal
from cointrading.storage import TradingStore


@dataclass(frozen=True)
class ExecutionDecision:
    allowed: bool
    reason: str
    intent: OrderIntent | None = None


@dataclass(frozen=True)
class ExecutionResult:
    decision: ExecutionDecision
    order_id: int | None = None
    response: dict | None = None


def dry_run_order_response(intent: OrderIntent) -> dict:
    params: dict[str, object] = {
        "symbol": intent.symbol,
        "side": intent.side,
        "type": intent.order_type,
        "quantity": intent.quantity,
    }
    if intent.price is not None:
        params["price"] = intent.price
    if intent.time_in_force:
        params["timeInForce"] = intent.time_in_force
    if intent.reduce_only:
        params["reduceOnly"] = True
    if intent.client_order_id:
        params["newClientOrderId"] = intent.client_order_id
    return {
        "dryRun": True,
        "endpoint": "/fapi/v1/order",
        "params": params,
    }


def submit_order(
    client: BinanceUSDMClient,
    intent: OrderIntent,
    config: TradingConfig,
) -> dict:
    if config.dry_run:
        return dry_run_order_response(intent)
    return client.new_order(intent)


def build_post_only_intent(
    signal: ScalpSignal,
    config: TradingConfig,
    *,
    notional: float | None = None,
) -> ExecutionDecision:
    if signal.side not in {"long", "short"}:
        return ExecutionDecision(False, "signal is flat")
    if not signal.trade_allowed:
        return ExecutionDecision(False, f"signal blocked: {signal.reason}")
    if signal.edge_after_maker_bps < config.min_live_edge_bps:
        return ExecutionDecision(
            False,
            f"edge {signal.edge_after_maker_bps:.3f}bps below live gate",
        )

    order_notional = min(
        notional if notional is not None else config.post_only_order_notional,
        config.max_single_order_notional,
    )
    risk_decision = RiskManager(config).validate_new_notional(config.initial_equity, order_notional)
    if not risk_decision.allowed:
        return ExecutionDecision(False, risk_decision.reason)

    price = _passive_price(signal)
    if price <= 0:
        return ExecutionDecision(False, "passive price must be positive")
    quantity = order_notional / price
    side: OrderSide = "BUY" if signal.side == "long" else "SELL"
    intent = OrderIntent(
        symbol=signal.symbol,
        side=side,
        quantity=quantity,
        order_type="LIMIT",
        price=price,
        time_in_force="GTX",
        reduce_only=False,
        client_order_id=_client_order_id(signal.symbol),
    )
    return ExecutionDecision(True, "post-only maker intent ready", intent)


def place_post_only_maker(
    client: BinanceUSDMClient,
    store: TradingStore,
    signal: ScalpSignal,
    config: TradingConfig,
    *,
    signal_id: int | None = None,
) -> ExecutionResult:
    decision = build_post_only_intent(signal, config)
    if decision.intent is None:
        order_id = store.insert_order_attempt(
            OrderIntent(
                symbol=signal.symbol,
                side="BUY" if signal.side != "short" else "SELL",
                quantity=0.0,
                order_type="LIMIT",
                price=signal.mid_price,
                time_in_force="GTX",
            ),
            status="BLOCKED",
            dry_run=True,
            reason=decision.reason,
            signal_id=signal_id,
        )
        return ExecutionResult(decision, order_id=order_id)

    if not config.dry_run and not config.live_trading_enabled:
        order_id = store.insert_order_attempt(
            decision.intent,
            status="BLOCKED",
            dry_run=False,
            reason="live trading flag is disabled",
            signal_id=signal_id,
        )
        return ExecutionResult(
            ExecutionDecision(False, "live trading flag is disabled", decision.intent),
            order_id=order_id,
        )

    if not config.dry_run:
        try:
            filters = SymbolFilters.from_exchange_info(
                client.exchange_info(decision.intent.symbol),
                decision.intent.symbol,
            )
            normalized_intent, filter_reason = filters.normalize_intent(decision.intent)
        except (BinanceAPIError, ValueError) as exc:
            normalized_intent = None
            filter_reason = f"exchange filter error: {exc}"
        if normalized_intent is None:
            order_id = store.insert_order_attempt(
                decision.intent,
                status="BLOCKED",
                dry_run=False,
                reason=filter_reason,
                signal_id=signal_id,
            )
            return ExecutionResult(
                ExecutionDecision(False, filter_reason, decision.intent),
                order_id=order_id,
            )
        decision = ExecutionDecision(
            True,
            f"{decision.reason}; {filter_reason}",
            normalized_intent,
        )

    try:
        response = submit_order(client, decision.intent, config)
    except BinanceAPIError as exc:
        order_id = store.insert_order_attempt(
            decision.intent,
            status="ERROR",
            dry_run=config.dry_run,
            reason=str(exc),
            signal_id=signal_id,
        )
        return ExecutionResult(
            ExecutionDecision(False, str(exc), decision.intent),
            order_id=order_id,
        )

    status = "DRY_RUN" if config.dry_run else str(response.get("status", "SUBMITTED"))
    order_id = store.insert_order_attempt(
        decision.intent,
        status=status,
        dry_run=config.dry_run,
        reason=decision.reason,
        response=response,
        signal_id=signal_id,
    )
    return ExecutionResult(decision, order_id=order_id, response=response)


def _passive_price(signal: ScalpSignal) -> float:
    half_spread = signal.mid_price * (signal.spread_bps / 20_000.0)
    if signal.side == "long":
        return signal.mid_price - half_spread
    return signal.mid_price + half_spread


def _client_order_id(symbol: str) -> str:
    return f"ct{symbol.lower()}{int(time.time() * 1000)}"[:36]
