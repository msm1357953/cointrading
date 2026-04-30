from __future__ import annotations

from dataclasses import dataclass

from cointrading.config import TradingConfig
from cointrading.models import (
    BacktestMetrics,
    BacktestResult,
    Kline,
    Position,
    SignalSide,
    Trade,
)
from cointrading.risk import RiskManager, drawdown_pct
from cointrading.strategies import MovingAverageCrossStrategy


@dataclass
class Backtester:
    config: TradingConfig
    strategy: MovingAverageCrossStrategy
    stop_distance_pct: float = 0.02

    def run(self, klines: list[Kline]) -> BacktestResult:
        if not klines:
            raise ValueError("klines cannot be empty")

        equity_cash = self.config.initial_equity
        position = Position(symbol=self.strategy.symbol)
        entry_time = 0
        trades: list[Trade] = []
        equity_curve: list[tuple[int, float]] = []
        risk = RiskManager(self.config)
        peak_equity = equity_cash
        max_dd = 0.0
        total_fees = 0.0

        for index, kline in enumerate(klines):
            mark_equity = equity_cash + position.unrealized_pnl(kline.close)
            risk_decision = risk.update_equity(mark_equity)
            peak_equity = max(peak_equity, mark_equity)
            max_dd = max(max_dd, drawdown_pct(peak_equity, mark_equity))
            equity_curve.append((kline.close_time, mark_equity))

            if not risk_decision.allowed:
                if position.quantity != 0:
                    fee = self._fee(position.quantity, kline.close)
                    pnl = position.unrealized_pnl(kline.close)
                    equity_cash += pnl - fee
                    total_fees += fee
                    trades.append(
                        Trade(
                            symbol=position.symbol,
                            side=position.side,
                            quantity=abs(position.quantity),
                            entry_price=position.entry_price,
                            exit_price=kline.close,
                            pnl=pnl,
                            fee=fee,
                            entry_time=entry_time,
                            exit_time=kline.close_time,
                        )
                    )
                    position = Position(symbol=self.strategy.symbol)
                continue

            signal = self.strategy.generate(klines[: index + 1])
            target_side = signal.side
            current_side = position.side
            if target_side == current_side:
                continue

            if position.quantity != 0:
                exit_price = self._apply_slippage(kline.close, self._exit_side(position))
                fee = self._fee(position.quantity, exit_price)
                pnl = position.unrealized_pnl(exit_price)
                equity_cash += pnl - fee
                total_fees += fee
                trades.append(
                    Trade(
                        symbol=position.symbol,
                        side=current_side,
                        quantity=abs(position.quantity),
                        entry_price=position.entry_price,
                        exit_price=exit_price,
                        pnl=pnl,
                        fee=fee,
                        entry_time=entry_time,
                        exit_time=kline.close_time,
                    )
                )
                position = Position(symbol=self.strategy.symbol)

            if target_side in {"long", "short"}:
                current_notional = position.notional(kline.close)
                qty = risk.max_position_quantity(
                    equity=equity_cash,
                    entry_price=kline.close,
                    stop_distance_pct=self.stop_distance_pct,
                    current_notional=current_notional,
                )
                if qty <= 0:
                    continue
                notional_decision = risk.validate_new_notional(equity_cash, qty * kline.close)
                if not notional_decision.allowed:
                    continue
                signed_qty = qty if target_side == "long" else -qty
                entry_price = self._apply_slippage(kline.close, self._entry_side(target_side))
                fee = self._fee(signed_qty, entry_price)
                equity_cash -= fee
                total_fees += fee
                position = Position(
                    symbol=self.strategy.symbol,
                    quantity=signed_qty,
                    entry_price=entry_price,
                )
                entry_time = kline.close_time

        final_price = klines[-1].close
        final_equity = equity_cash + position.unrealized_pnl(final_price)
        peak_equity = max(peak_equity, final_equity)
        max_dd = max(max_dd, drawdown_pct(peak_equity, final_equity))
        wins = [trade for trade in trades if trade.pnl - trade.fee > 0]
        trade_count = len(trades)
        metrics = BacktestMetrics(
            final_equity=final_equity,
            total_return_pct=(final_equity / self.config.initial_equity) - 1.0,
            max_drawdown_pct=max_dd,
            trade_count=trade_count,
            win_rate_pct=(len(wins) / trade_count) if trade_count else 0.0,
            total_fees=total_fees,
        )
        return BacktestResult(metrics=metrics, trades=trades, equity_curve=equity_curve)

    def _fee(self, quantity: float, price: float) -> float:
        return abs(quantity) * price * self.config.taker_fee_rate

    def _apply_slippage(self, price: float, order_side: str) -> float:
        slippage = self.config.slippage_bps / 10_000.0
        if order_side == "BUY":
            return price * (1.0 + slippage)
        if order_side == "SELL":
            return price * (1.0 - slippage)
        raise ValueError(f"unknown order side: {order_side}")

    @staticmethod
    def _entry_side(signal_side: SignalSide) -> str:
        if signal_side == "long":
            return "BUY"
        if signal_side == "short":
            return "SELL"
        raise ValueError("flat has no entry side")

    @staticmethod
    def _exit_side(position: Position) -> str:
        if position.quantity > 0:
            return "SELL"
        if position.quantity < 0:
            return "BUY"
        raise ValueError("flat position has no exit side")
