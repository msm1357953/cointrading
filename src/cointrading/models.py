from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal


SignalSide = Literal["long", "short", "flat"]
OrderSide = Literal["BUY", "SELL"]


@dataclass(frozen=True)
class Kline:
    open_time: int
    open: float
    high: float
    low: float
    close: float
    volume: float
    close_time: int

    @classmethod
    def from_binance(cls, row: list[Any]) -> "Kline":
        return cls(
            open_time=int(row[0]),
            open=float(row[1]),
            high=float(row[2]),
            low=float(row[3]),
            close=float(row[4]),
            volume=float(row[5]),
            close_time=int(row[6]),
        )


@dataclass(frozen=True)
class Signal:
    symbol: str
    side: SignalSide
    reason: str


@dataclass(frozen=True)
class OrderIntent:
    symbol: str
    side: OrderSide
    quantity: float
    order_type: str = "MARKET"
    price: float | None = None
    stop_price: float | None = None
    time_in_force: str | None = None
    working_type: str | None = None
    reduce_only: bool = False
    client_order_id: str | None = None


@dataclass
class Position:
    symbol: str
    quantity: float = 0.0
    entry_price: float = 0.0

    @property
    def side(self) -> SignalSide:
        if self.quantity > 0:
            return "long"
        if self.quantity < 0:
            return "short"
        return "flat"

    def unrealized_pnl(self, mark_price: float) -> float:
        if self.quantity == 0:
            return 0.0
        return self.quantity * (mark_price - self.entry_price)

    def notional(self, mark_price: float) -> float:
        return abs(self.quantity) * mark_price


@dataclass(frozen=True)
class Trade:
    symbol: str
    side: SignalSide
    quantity: float
    entry_price: float
    exit_price: float
    pnl: float
    fee: float
    entry_time: int
    exit_time: int


@dataclass(frozen=True)
class BacktestMetrics:
    final_equity: float
    total_return_pct: float
    max_drawdown_pct: float
    trade_count: int
    win_rate_pct: float
    total_fees: float


@dataclass
class BacktestResult:
    metrics: BacktestMetrics
    trades: list[Trade] = field(default_factory=list)
    equity_curve: list[tuple[int, float]] = field(default_factory=list)
