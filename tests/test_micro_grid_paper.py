import tempfile
import unittest
from dataclasses import replace
from pathlib import Path

from cointrading.config import TradingConfig
from cointrading.micro_grid_paper import MicroGridPaperEngine, STRATEGY_NAME
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


class FakeMarketClient:
    def __init__(self) -> None:
        self.book = {"bidPrice": "80000.0", "askPrice": "80000.5"}

    def book_ticker(self, symbol):
        return self.book

    def klines(self, symbol, interval, limit=500):
        if interval == "5m":
            return _klines(79_000, 2.0, min(limit, 300), 300_000)
        if interval == "15m":
            return _klines(79_000, 10.0, min(limit, 96), 900_000)
        if interval == "1h":
            return _klines(78_000, 20.0, min(limit, 10), 3_600_000)
        return _klines(79_000, 1.0, min(limit, 10), 60_000)


def _cfg(**overrides) -> TradingConfig:
    base = replace(
        TradingConfig(),
        micro_grid_paper_enabled=True,
        micro_grid_paper_symbol="BTCUSDC",
        micro_grid_paper_notional=25.0,
        micro_grid_paper_gaps_usdc=(5.0, 10.0),
        micro_grid_paper_take_profits_usdc=(5.0, 8.0),
        micro_grid_paper_stop_gap_multiple=4.0,
        micro_grid_paper_max_active_cycles=8,
        maker_fee_rate=0.0,
        orderflow_guard_enabled=False,
    )
    return replace(base, **overrides)


class MicroGridPaperTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.store = TradingStore(Path(self.tmp.name) / "db.sqlite")
        self.client = FakeMarketClient()
        self.cfg = _cfg()

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def _engine(self) -> MicroGridPaperEngine:
        return MicroGridPaperEngine(config=self.cfg, storage=self.store, client=self.client)

    def test_step_submits_virtual_entries_without_exchange_orders(self) -> None:
        result = self._engine().step()

        self.assertEqual(len(result.opened), 4)
        cycles = self.store.recent_strategy_cycles(limit=10)
        self.assertEqual(len(cycles), 4)
        self.assertTrue(all(row["strategy"] == STRATEGY_NAME for row in cycles))
        self.assertEqual({row["status"] for row in cycles}, {"ENTRY_SUBMITTED"})
        self.assertEqual({row["side"] for row in cycles}, {"long", "short"})

    def test_virtual_entry_can_fill_and_close_at_take_profit(self) -> None:
        self.cfg = _cfg(
            micro_grid_paper_gaps_usdc=(5.0,),
            micro_grid_paper_take_profits_usdc=(5.0,),
            micro_grid_paper_max_active_cycles=2,
        )
        self._engine().step()

        self.client.book = {"bidPrice": "79995.0", "askPrice": "79995.5"}
        filled = self._engine().step()
        self.assertTrue(any(item["action"] == "entry_filled" for item in filled.managed))

        self.client.book = {"bidPrice": "80000.5", "askPrice": "80001.0"}
        closed = self._engine().step()
        self.assertTrue(any(item["action"] == "take_profit" for item in closed.managed))
        cycles = self.store.recent_strategy_cycles(limit=10)
        closed_cycles = [row for row in cycles if row["status"] == "CLOSED"]
        self.assertEqual(len(closed_cycles), 1)
        self.assertGreater(float(closed_cycles[0]["realized_pnl"]), 0.0)


if __name__ == "__main__":
    unittest.main()
