from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR, getcontext
from typing import Any

from cointrading.models import OrderIntent


getcontext().prec = 28


@dataclass(frozen=True)
class SymbolFilters:
    symbol: str
    tick_size: Decimal
    step_size: Decimal
    min_qty: Decimal
    max_qty: Decimal
    min_notional: Decimal

    @classmethod
    def from_exchange_info(cls, payload: dict[str, Any], symbol: str) -> "SymbolFilters":
        symbol = symbol.upper()
        symbols = payload.get("symbols", [])
        row = next((item for item in symbols if item.get("symbol") == symbol), None)
        if row is None:
            raise ValueError(f"{symbol} not found in exchangeInfo")
        filters = {item.get("filterType"): item for item in row.get("filters", [])}
        price_filter = filters.get("PRICE_FILTER") or {}
        lot_filter = filters.get("LOT_SIZE") or {}
        min_notional_filter = filters.get("MIN_NOTIONAL") or {}
        min_notional_raw = (
            min_notional_filter.get("notional")
            or min_notional_filter.get("minNotional")
            or "0"
        )
        return cls(
            symbol=symbol,
            tick_size=_decimal(price_filter.get("tickSize"), "0"),
            step_size=_decimal(lot_filter.get("stepSize"), "0"),
            min_qty=_decimal(lot_filter.get("minQty"), "0"),
            max_qty=_decimal(lot_filter.get("maxQty"), "0"),
            min_notional=_decimal(min_notional_raw, "0"),
        )

    def normalize_intent(self, intent: OrderIntent) -> tuple[OrderIntent | None, str]:
        quantity = self.floor_quantity(intent.quantity)
        if quantity <= 0:
            return None, "quantity rounds to zero"
        if self.min_qty > 0 and quantity < self.min_qty:
            return None, f"quantity {quantity} below minQty {self.min_qty}"
        if self.max_qty > 0 and quantity > self.max_qty:
            return None, f"quantity {quantity} above maxQty {self.max_qty}"

        price = intent.price
        if price is not None:
            price = self.post_only_price(intent.side, float(price))
            if price <= 0:
                return None, "price rounds to zero"
            notional = Decimal(str(price)) * Decimal(str(quantity))
        else:
            notional = Decimal("0")
        stop_price = intent.stop_price
        if stop_price is not None:
            stop_price = self.post_only_price(intent.side, float(stop_price))
            if stop_price <= 0:
                return None, "stop price rounds to zero"
        if price is not None and self.min_notional > 0 and notional < self.min_notional:
            return None, f"notional {notional} below minNotional {self.min_notional}"

        return (
            OrderIntent(
                symbol=intent.symbol,
                side=intent.side,
                quantity=float(quantity),
                order_type=intent.order_type,
                price=price,
                stop_price=stop_price,
                time_in_force=intent.time_in_force,
                working_type=intent.working_type,
                reduce_only=intent.reduce_only,
                client_order_id=intent.client_order_id,
            ),
            "exchange filters ok",
        )

    def floor_quantity(self, quantity: float) -> Decimal:
        return _quantize(Decimal(str(quantity)), self.step_size, ROUND_FLOOR)

    def post_only_price(self, side: str, price: float) -> float:
        if self.tick_size <= 0:
            return price
        rounding = ROUND_FLOOR if side == "BUY" else ROUND_CEILING
        return float(_quantize(Decimal(str(price)), self.tick_size, rounding))

    def min_order_notional_at(self, price: float) -> Decimal:
        min_qty_notional = self.min_qty * Decimal(str(price))
        return max(self.min_notional, min_qty_notional)


def _quantize(value: Decimal, step: Decimal, rounding: str) -> Decimal:
    if step <= 0:
        return value
    units = (value / step).to_integral_value(rounding=rounding)
    return units * step


def _decimal(value: Any, default: str) -> Decimal:
    if value is None or str(value).strip() == "":
        return Decimal(default)
    return Decimal(str(value))
