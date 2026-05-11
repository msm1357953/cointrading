import tempfile
import unittest
from dataclasses import replace
import json
from pathlib import Path
from types import SimpleNamespace

from cointrading.config import TradingConfig
from cointrading.grid_paper import GridPaperEngine, STRATEGY_NAME
from cointrading.grid_lifecycle import GridState, MODE_AUTO, MODE_SHORT, save_state
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
            return _klines(79_000, 90.0, min(limit, 96), 900_000)
        if interval == "1h":
            return _klines(78_000, 250.0, min(limit, 10), 3_600_000)
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
        self.state = Path(self.tmp.name) / "grid_state.json"
        save_state(GridState(mode=MODE_AUTO), self.state)
        self.client = FakeGridPaperClient()
        self.cfg = _cfg()

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def _engine(self) -> GridPaperEngine:
        return GridPaperEngine(
            config=self.cfg,
            storage=self.store,
            client=self.client,
            state_path=self.state,
        )

    def test_regular_grid_paper_opens_auto_side_only(self) -> None:
        result = self._engine().step()

        self.assertEqual(len(result.opened), 2)
        cycles = self.store.recent_strategy_cycles(limit=10)
        self.assertEqual(len(cycles), 2)
        self.assertTrue(all(row["strategy"] == STRATEGY_NAME for row in cycles))
        self.assertEqual({row["status"] for row in cycles}, {"ENTRY_SUBMITTED"})
        self.assertEqual({row["side"] for row in cycles}, {"long"})

    def test_regular_grid_paper_follows_manual_short_mode(self) -> None:
        save_state(GridState(mode=MODE_SHORT), self.state)

        result = self._engine().step()

        self.assertEqual(len(result.opened), 2)
        cycles = self.store.recent_strategy_cycles(limit=10)
        self.assertEqual({row["side"] for row in cycles}, {"short"})

    def test_regular_grid_paper_allows_unconfirmed_orderflow_danger(self) -> None:
        self.cfg = _cfg(orderflow_guard_enabled=True, grid_orderflow_confirmations=3)
        save_state(GridState(mode=MODE_AUTO, orderflow_long_danger_count=1), self.state)
        market = SimpleNamespace(
            orderflow_long_status="DANGER",
            orderflow_short_status="NORMAL",
            orderflow_reason="롱 DANGER 관찰중",
            range_position_15m=0.5,
            orderflow_bid_depth_010=500_000.0,
            orderflow_ask_depth_010=500_000.0,
        )

        self.assertEqual(self._engine()._side_block_reason("long", market), "")

    def test_regular_grid_paper_blocks_confirmed_orderflow_danger(self) -> None:
        self.cfg = _cfg(orderflow_guard_enabled=True, grid_orderflow_confirmations=3)
        save_state(GridState(mode=MODE_AUTO, orderflow_long_danger_count=3), self.state)
        market = SimpleNamespace(
            orderflow_long_status="DANGER",
            orderflow_short_status="NORMAL",
            orderflow_reason="롱 DANGER 확정",
            range_position_15m=0.5,
            orderflow_bid_depth_010=500_000.0,
            orderflow_ask_depth_010=500_000.0,
        )

        self.assertIn("confirmed 3/3", self._engine()._side_block_reason("long", market))

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

    def test_unfilled_layers_reanchor_when_price_moves(self) -> None:
        self.cfg = _cfg(grid_max_layers=1, grid_paper_max_active_cycles=2)
        self._engine().step()
        first_long = [
            row for row in self.store.recent_strategy_cycles(limit=10)
            if row["strategy"] == STRATEGY_NAME and row["side"] == "long"
        ][0]

        self.client.book = {"bidPrice": "80100.0", "askPrice": "80100.5"}
        result = self._engine().step()

        self.assertTrue(any(item["action"] == "entry_reanchored" for item in result.managed))
        latest_long = [
            row for row in self.store.recent_strategy_cycles(limit=10)
            if row["strategy"] == STRATEGY_NAME and row["side"] == "long"
        ][0]
        self.assertGreater(float(latest_long["entry_price"]), float(first_long["entry_price"]))
        self.assertEqual(int(latest_long["reprice_count"]), 1)

    def test_multiple_filled_paper_layers_share_basket_average_target(self) -> None:
        self.cfg = _cfg(grid_max_layers=2, grid_paper_max_active_cycles=2)
        self._engine().step()

        self.client.book = {"bidPrice": "79900.0", "askPrice": "79900.5"}
        result = self._engine().step()

        self.assertTrue(any(item["action"] == "paper_basket_tp_synced" for item in result.managed))
        opens = [
            row for row in self.store.recent_strategy_cycles(limit=10)
            if row["strategy"] == STRATEGY_NAME and row["side"] == "long" and row["status"] == "OPEN"
        ]
        self.assertEqual(len(opens), 2)
        targets = {round(float(row["target_price"]), 2) for row in opens}
        self.assertEqual(len(targets), 1)
        stops = {round(float(row["stop_price"]), 2) for row in opens}
        self.assertEqual(len(stops), 1)
        basket_entries = {
            round(float(json.loads(row["setup_json"])["basket_avg_entry"]), 2)
            for row in opens
        }
        basket_stops = {
            round(float(json.loads(row["setup_json"])["basket_stop_price"]), 2)
            for row in opens
        }
        self.assertEqual(stops, basket_stops)
        self.assertEqual(len(basket_entries), 1)
        avg_entry = basket_entries.pop()
        layer_entries = sorted(float(row["entry_price"]) for row in opens)
        self.assertGreater(avg_entry, layer_entries[0])
        self.assertLess(avg_entry, layer_entries[-1])


if __name__ == "__main__":
    unittest.main()
