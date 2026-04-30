from __future__ import annotations

from dataclasses import dataclass

from cointrading.indicators import realized_volatility, sma
from cointrading.models import Kline, Signal


@dataclass(frozen=True)
class MovingAverageCrossStrategy:
    symbol: str
    fast_window: int = 20
    slow_window: int = 60
    volatility_window: int = 24
    max_volatility: float = 0.025

    def generate(self, history: list[Kline]) -> Signal:
        closes = [item.close for item in history]
        fast = sma(closes, self.fast_window)
        slow = sma(closes, self.slow_window)
        vol = realized_volatility(closes, self.volatility_window)

        if fast is None or slow is None or vol is None:
            return Signal(self.symbol, "flat", "not enough history")
        if vol > self.max_volatility:
            return Signal(self.symbol, "flat", "volatility filter")
        if fast > slow:
            return Signal(self.symbol, "long", "fast above slow")
        if fast < slow:
            return Signal(self.symbol, "short", "fast below slow")
        return Signal(self.symbol, "flat", "moving averages tied")
