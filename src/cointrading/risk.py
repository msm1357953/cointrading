from __future__ import annotations

from dataclasses import dataclass
import math

from cointrading.config import TradingConfig


def drawdown_pct(peak_equity: float, current_equity: float) -> float:
    if peak_equity <= 0:
        return 0.0
    return max(0.0, (peak_equity - current_equity) / peak_equity)


@dataclass(frozen=True)
class RiskDecision:
    allowed: bool
    reason: str


@dataclass
class RiskManager:
    config: TradingConfig
    peak_equity: float | None = None
    day_start_equity: float | None = None
    halted: bool = False

    def update_equity(self, equity: float) -> RiskDecision:
        if self.peak_equity is None:
            self.peak_equity = equity
        if self.day_start_equity is None:
            self.day_start_equity = equity

        self.peak_equity = max(self.peak_equity, equity)
        dd = drawdown_pct(self.peak_equity, equity)
        daily_loss = drawdown_pct(self.day_start_equity, equity)

        if dd >= self.config.max_drawdown_pct:
            self.halted = True
            return RiskDecision(False, f"max drawdown hit: {dd:.2%}")
        if daily_loss >= self.config.daily_loss_pct:
            self.halted = True
            return RiskDecision(False, f"daily loss hit: {daily_loss:.2%}")
        if self.halted:
            return RiskDecision(False, "risk manager halted")
        return RiskDecision(True, "risk ok")

    def reset_daily_anchor(self, equity: float) -> None:
        self.day_start_equity = equity

    def max_position_quantity(
        self,
        equity: float,
        entry_price: float,
        stop_distance_pct: float,
        current_notional: float = 0.0,
    ) -> float:
        if equity <= 0:
            return 0.0
        if entry_price <= 0:
            raise ValueError("entry_price must be positive")
        if stop_distance_pct <= 0:
            raise ValueError("stop_distance_pct must be positive")

        risk_budget = equity * self.config.risk_per_trade_pct
        risk_sized_qty = risk_budget / (entry_price * stop_distance_pct)
        max_total_notional = equity * self.config.max_notional_multiplier
        available_notional = max(0.0, max_total_notional - current_notional)
        notional_capped_qty = available_notional / entry_price
        return max(0.0, min(risk_sized_qty, notional_capped_qty))

    def validate_new_notional(self, equity: float, notional: float) -> RiskDecision:
        if self.halted:
            return RiskDecision(False, "risk manager halted")
        if equity <= 0:
            return RiskDecision(False, "equity must be positive")
        if notional < 0 or not math.isfinite(notional):
            return RiskDecision(False, "notional must be finite and non-negative")
        max_notional = equity * self.config.max_notional_multiplier
        if notional > max_notional:
            return RiskDecision(
                False,
                f"notional {notional:.2f} exceeds cap {max_notional:.2f}",
            )
        return RiskDecision(True, "notional ok")
