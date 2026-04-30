import unittest

from cointrading.backtest import Backtester
from cointrading.config import TradingConfig
from cointrading.cli import _demo_klines
from cointrading.strategies import MovingAverageCrossStrategy


class BacktestTests(unittest.TestCase):
    def test_demo_backtest_runs(self) -> None:
        klines = list(_demo_klines())
        strategy = MovingAverageCrossStrategy(symbol="BTCUSDT")
        result = Backtester(TradingConfig(), strategy).run(klines)
        self.assertGreater(result.metrics.final_equity, 0)
        self.assertEqual(len(result.equity_curve), len(klines))
        self.assertGreaterEqual(result.metrics.max_drawdown_pct, 0)


if __name__ == "__main__":
    unittest.main()
