import unittest

from cointrading.config import TradingConfig
from cointrading.risk import RiskManager, drawdown_pct


class RiskTests(unittest.TestCase):
    def test_drawdown_pct_uses_peak_to_trough(self) -> None:
        self.assertEqual(drawdown_pct(1100, 880), 0.2)

    def test_risk_manager_halts_on_max_drawdown(self) -> None:
        manager = RiskManager(TradingConfig(initial_equity=1000, max_drawdown_pct=0.10))
        self.assertTrue(manager.update_equity(1000).allowed)
        self.assertTrue(manager.update_equity(1200).allowed)
        decision = manager.update_equity(1000)
        self.assertFalse(decision.allowed)
        self.assertIn("max drawdown", decision.reason)

    def test_position_size_respects_notional_cap(self) -> None:
        manager = RiskManager(
            TradingConfig(
                initial_equity=1000,
                risk_per_trade_pct=0.02,
                max_notional_multiplier=1.0,
            )
        )
        qty = manager.max_position_quantity(
            equity=1000,
            entry_price=50_000,
            stop_distance_pct=0.01,
        )
        self.assertEqual(qty, 0.02)


if __name__ == "__main__":
    unittest.main()
