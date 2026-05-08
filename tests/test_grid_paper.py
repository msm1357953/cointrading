import tempfile
import unittest
from dataclasses import replace
from pathlib import Path

from cointrading.config import TradingConfig
from cointrading.grid_paper import GridPaperEngine, STRATEGY_NAME
from cointrading.models import Kline
from cointrading.storage import TradingStore


def _klines(start: float, step: float, n: int, interval_ms: int) -> list[Kline]:
    rows = []
    t0 = 1_000_000
    price = start
    for i in range(n):
        o = price
        c = price + step
        h = max(o, c) + 30
        l = min(o, c) - 30
        rows.append(
            Kline(
                open_time=t0 + i * interval_ms,
                open=o,
                high=h,
                low=l,
                close=c,
                volume=10.0,
                close_time=t0 + (i + 1) * interval_ms - 1,
            )
        )
        price = c
    return rows


class FakeGridPaperClient:
    def __init__(self) -> None:
        self.book = {"bidPrice": "80000.0", "askPrice": "80000.5"}

    def book_ticker(self, symbol):
        return self.book

    def klines(self, symbol, interval, limit=500):
        if interval == "5m":
            return _klines(79_000, 2.0, min(limit, 300), 300_000)
        if interval == "15m":
            return _klines(79_000, 5.0, min(limit, 96), 900_000)
        if interval == "1h":
            return _klines(78_000, 20.0, min(limit, 10), 3_600_000)
        return _klines(79_000, 1.0, min(limit, 10), 60_000)


def _cfg(**overrides) -> TradingConfig:
    base = replace(
        TradingConfig(),
        grid_symbol="BTCUSDC",
        grid_gap_min_usdc=50.0,
        grid_gap_max_usdc=150.0,
        grid_take_profit_min_usdc=30.0,
        grid_take_profit_max_usdc=80.0,
        grid_max_layers=2,
        grid_paper_enabled=True,
        grid_paper_notional=25.0,
        grid_paper_max_active_cycles=4,
        grid_position_filter_enabled=False,
        grid_liquidity_filter_enabled=False,
        orderflow_guard_enabled=False,
        maker_fee_rate=0.0,
    )
    return replace(base, **overrides)


class GridPaperTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.store = TradingStore(Path(self.tmp.name) / "db.sqlite")
        self.client = FakeGridPaperClient()
        self.cfg = _cfg()

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def _engine(self) -> GridPaperEngine:
        return GridPaperEngine(config=self.cfg, storage=self.store, client=self.client)

    def test_regular_grid_paper_opens_virtual_layers(self) -> None:
        result = self._engine().step()

        self.assertEqual(len(result.opened), 4)
        cycles = self.store.recent_strategy_cycles(limit=10)
        self.assertEqual(len(cycles), 4)
        self.assertTrue(all(row["strategy"] == STRATEGY_NAME for row in cycles))
        self.assertEqual({row["status"] for row in cycles}, {"ENTRY_SUBMITTED"})
        self.assertEqual({row["side"] for row in cycles}, {"long", "short"})

    def test_regular_grid_paper_fills_and_closes(self) -> None:
        self.cfg = _cfg(grid_max_layers=1, grid_paper_max_active_cycles=2)
        self._engine().step()

        self.client.book = {"bidPrice": "79950.0", "askPrice": "79950.5"}
        filled = self._engine().step()
        self.assertTrue(any(item["action"] == "entry_filled" for item in filled.managed))

        self.client.book = {"bidPrice": "79985.0", "askPrice": "79985.5"}
        closed = self._engine().step()
        self.assertTrue(any(item["action"] == "take_profit" for item in closed.managed))
        closed_cycles = [
            row for row in self.store.recent_strategy_cycles(limit=10)
            if row["status"] == "CLOSED"
        ]
        self.assertEqual(len(closed_cycles), 1)
        self.assertGreater(float(closed_cycles[0]["realized_pnl"]), 0.0)


if __name__ == "__main__":
    unittest.main()
