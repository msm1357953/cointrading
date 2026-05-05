"""Tests for the shared live execution module + LIVE path of funding/wick."""
import json
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path

from cointrading.config import TradingConfig
from cointrading.funding_lifecycle import (
    FundingCarryEngine,
    STATUS_CLOSED as F_CLOSED,
    STATUS_OPEN as F_OPEN,
    STATUS_STOPPED as F_STOPPED,
    STRATEGY_NAME as F_NAME,
)
from cointrading.live_execution import (
    realized_pnl_from_close,
    submit_live_market_long,
    submit_protective_stop,
)
from cointrading.models import Kline
from cointrading.storage import TradingStore
from cointrading.wick_lifecycle import (
    STATUS_CLOSED as W_CLOSED,
    STRATEGY_NAME as W_NAME,
    WickReversionEngine,
)


class FakeLiveClient:
    """Stub Binance USDM client for live-path tests.

    - exchange_info: returns very loose filters so any qty passes.
    - book_ticker: configurable mid via set_price().
    - new_order: records the intent, returns FILLED with avgPrice = current mid.
    - order_status: returns the canned status for the order_id.
    - cancel_order: records the cancel.
    """

    def __init__(self) -> None:
        self.next_order_id = 100
        self.orders: dict[int, dict] = {}
        self.canned_status: dict[int, dict] = {}  # order_id -> status response
        self.cancels: list[int] = []
        self.book = {"bidPrice": "100.0", "askPrice": "100.04"}
        self.entry_price_override: float | None = None
        # tracking by client_order_id for query lookups
        self.last_intent_by_cid: dict[str, dict] = {}

        # 5m kline triggers for wick tests
        self.kline_responses: dict[str, list[Kline]] = {}
        # funding for funding tests
        self.funding_responses: dict[str, list[dict]] = {}

    def set_price(self, bid: float, ask: float) -> None:
        self.book = {"bidPrice": str(bid), "askPrice": str(ask)}

    def book_ticker(self, symbol: str):
        return self.book

    def exchange_info(self, symbol: str | None = None):
        sym = symbol or "BTCUSDC"
        return {
            "symbols": [
                {
                    "symbol": sym,
                    "filters": [
                        {"filterType": "PRICE_FILTER", "tickSize": "0.0001"},
                        {"filterType": "LOT_SIZE", "minQty": "0.0001", "maxQty": "10000", "stepSize": "0.0001"},
                        {"filterType": "MIN_NOTIONAL", "notional": "1"},
                    ],
                }
            ]
        }

    def new_order(self, intent):
        order_id = self.next_order_id
        self.next_order_id += 1
        if intent.order_type == "MARKET":
            avg = self.entry_price_override or float(self.book["askPrice"])
            response = {
                "orderId": order_id,
                "symbol": intent.symbol,
                "status": "FILLED",
                "avgPrice": f"{avg:.8f}",
                "executedQty": f"{intent.quantity:.8f}",
            }
        else:  # STOP_MARKET
            response = {
                "orderId": order_id,
                "symbol": intent.symbol,
                "status": "NEW",
                "stopPrice": f"{intent.stop_price:.8f}",
            }
        self.orders[order_id] = response
        if intent.client_order_id:
            self.last_intent_by_cid[intent.client_order_id] = response
        return response

    def order_status(self, *, symbol: str, order_id: int | None = None,
                     orig_client_order_id: str | None = None):
        if order_id is not None and order_id in self.canned_status:
            return self.canned_status[order_id]
        if order_id is not None and order_id in self.orders:
            return self.orders[order_id]
        return {"status": "UNKNOWN"}

    def cancel_order(self, *, symbol: str, order_id=None, orig_client_order_id=None):
        if order_id is not None:
            self.cancels.append(order_id)
            return {"orderId": order_id, "status": "CANCELED"}
        return {"status": "CANCELED"}

    def funding_rate(self, symbol: str, limit: int = 1):
        return list(self.funding_responses.get(symbol, []))

    def klines(self, symbol: str, interval: str, limit: int = 500):
        return list(self.kline_responses.get(symbol, []))


class LiveExecutionUnitTests(unittest.TestCase):
    """Direct tests on submit_* helpers."""

    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.store = TradingStore(Path(self.tmp.name) / "test.sqlite")
        self.client = FakeLiveClient()
        self.cfg = replace(TradingConfig(), dry_run=False, live_trading_enabled=True)

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def test_market_long_filled(self) -> None:
        self.client.entry_price_override = 100.04
        result = submit_live_market_long(
            client=self.client, store=self.store, config=self.cfg,
            symbol="BTCUSDC", quantity=0.8, strategy_label="fc", timestamp_ms=1000,
        )
        self.assertTrue(result.success)
        self.assertEqual(result.avg_price, 100.04)
        self.assertEqual(result.executed_qty, 0.8)

    def test_protective_stop_records_order(self) -> None:
        result = submit_protective_stop(
            client=self.client, store=self.store, config=self.cfg,
            symbol="BTCUSDC", quantity=0.8, stop_price=97.0,
            strategy_label="fc", timestamp_ms=2000,
        )
        self.assertIsNotNone(result.order_id)
        order = self.store.order_by_id(int(result.order_id))
        self.assertEqual(order["order_type"], "STOP_MARKET")
        self.assertEqual(order["reduce_only"], 1)

    def test_realized_pnl_long(self) -> None:
        # Mock cycle row
        class C(dict):
            def __getitem__(self, k): return super().__getitem__(k)
        cycle = C(side="long", entry_price=100.0, quantity=0.8)
        # Win case: entry 100, exit 103, qty 0.8 -> gross = 0.8*3 = 2.4
        # fees = (100*0.8 + 103*0.8) * 0.0005 = 162.4 * 0.0005 = 0.0812
        cfg = replace(self.cfg, taker_fee_rate=0.0005)
        pnl = realized_pnl_from_close(cycle=cycle, avg_exit_price=103.0, executed_qty=0.8, config=cfg)
        self.assertAlmostEqual(pnl, 2.4 - 0.0812, places=4)


class FundingLiveLifecycleTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.store = TradingStore(Path(self.tmp.name) / "t.sqlite")
        self.client = FakeLiveClient()
        self.now_ms = 1_777_968_000_000
        self.cfg = replace(
            TradingConfig(),
            dry_run=False,  # ARMED
            live_trading_enabled=True,
            funding_carry_enabled=True,
            funding_carry_live_enabled=True,
            funding_carry_symbols=("BTCUSDC",),
            funding_carry_threshold=0.0001,
            funding_carry_notional=80.0,
            funding_carry_stop_loss_bps=300.0,
            funding_carry_max_hold_seconds=86_400,
            funding_carry_check_window_minutes=60,
            taker_fee_rate=0.0004,
        )

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def _engine(self) -> FundingCarryEngine:
        return FundingCarryEngine(
            config=self.cfg, storage=self.store, client=self.client,
            now_ms_fn=lambda: self.now_ms,
        )

    def test_live_open_submits_entry_and_protective_stop(self) -> None:
        self.client.funding_responses["BTCUSDC"] = [
            {"fundingRate": "-0.0005", "fundingTime": self.now_ms - 60_000}
        ]
        self.client.set_price(100.0, 100.04)
        self.client.entry_price_override = 100.04

        engine = self._engine()
        self.assertTrue(engine.is_live_armed())
        result = engine.step()

        self.assertEqual(len(result.opened), 1)
        self.assertEqual(result.opened[0]["mode"], "LIVE")

        cycle = self.store.active_strategy_cycle(F_NAME, "BTCUSDC")
        self.assertIsNotNone(cycle)
        self.assertEqual(cycle["dry_run"], 0)
        self.assertAlmostEqual(cycle["entry_price"], 100.04, places=4)
        self.assertAlmostEqual(cycle["stop_price"], 100.04 * 0.97, places=4)
        # protective stop is referenced
        self.assertIsNotNone(cycle["exit_order_id"])

    def test_live_protective_stop_filled_closes_cycle(self) -> None:
        self.client.funding_responses["BTCUSDC"] = [
            {"fundingRate": "-0.0005", "fundingTime": self.now_ms - 60_000}
        ]
        self.client.set_price(100.0, 100.04)
        self.client.entry_price_override = 100.04
        engine = self._engine()
        engine.step()
        cycle = self.store.active_strategy_cycle(F_NAME, "BTCUSDC")
        stop_local_id = int(cycle["exit_order_id"])
        # Find exchange order id for the stop
        stop_local = self.store.order_by_id(stop_local_id)
        stop_exchange_id = int(json.loads(stop_local["response_json"])["orderId"])

        # Simulate the exchange filling the protective stop (-3% drop)
        self.client.canned_status[stop_exchange_id] = {
            "orderId": stop_exchange_id, "status": "FILLED",
            "avgPrice": "97.0388",  # 100.04 * 0.97
            "executedQty": str(cycle["quantity"]),
        }
        # Time advances slightly
        self.now_ms += 60_000
        result = engine.step()

        actions = [m["action"] for m in result.managed]
        self.assertIn("stopped", actions)

        cycle = self.store.recent_strategy_cycles(limit=1)[0]
        self.assertEqual(cycle["status"], F_STOPPED)
        self.assertEqual(cycle["reason"], "exchange_stop_filled")
        self.assertLess(cycle["realized_pnl"], 0)

    def test_live_time_exit_cancels_stop_and_market_closes(self) -> None:
        self.client.funding_responses["BTCUSDC"] = [
            {"fundingRate": "-0.0005", "fundingTime": self.now_ms - 60_000}
        ]
        self.client.set_price(100.0, 100.04)
        self.client.entry_price_override = 100.04
        engine = self._engine()
        engine.step()
        # Advance past 24h max hold
        self.now_ms += 25 * 3600 * 1000
        # Higher exit price
        self.client.set_price(101.0, 101.04)
        self.client.entry_price_override = 101.04  # market close fills here

        # Make protective stop status say it's still NEW
        cycle = self.store.recent_strategy_cycles(limit=1)[0]
        stop_local = self.store.order_by_id(int(cycle["exit_order_id"]))
        stop_exch_id = int(json.loads(stop_local["response_json"])["orderId"])
        self.client.canned_status[stop_exch_id] = {"orderId": stop_exch_id, "status": "NEW"}

        result = engine.step()
        actions = [m["action"] for m in result.managed]
        self.assertIn("closed_time", actions)
        self.assertIn(stop_exch_id, self.client.cancels, "stop should be cancelled before market close")

        cycle = self.store.recent_strategy_cycles(limit=1)[0]
        self.assertEqual(cycle["status"], F_CLOSED)
        self.assertEqual(cycle["reason"], "time_exit")
        self.assertGreater(cycle["realized_pnl"], 0)


class WickLiveLifecycleTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.store = TradingStore(Path(self.tmp.name) / "t.sqlite")
        self.client = FakeLiveClient()
        self.now_ms = 1_777_968_000_000
        self.cfg = replace(
            TradingConfig(),
            dry_run=False,
            live_trading_enabled=True,
            wick_carry_enabled=True,
            wick_carry_live_enabled=True,
            wick_carry_symbols=("BTCUSDC",),
            wick_carry_min_wick_ratio=0.5,
            wick_carry_min_drop_pct=0.01,
            wick_carry_notional=80.0,
            wick_carry_stop_loss_bps=300.0,
            wick_carry_max_hold_seconds=7_200,
            wick_carry_cooldown_seconds=600,
            wick_carry_freshness_seconds=360,
            taker_fee_rate=0.0004,
        )

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def _engine(self) -> WickReversionEngine:
        return WickReversionEngine(
            config=self.cfg, storage=self.store, client=self.client,
            now_ms_fn=lambda: self.now_ms,
        )

    def test_wick_live_open_fills_and_protects(self) -> None:
        # Make a fresh trigger bar
        close_t = self.now_ms - 60_000
        open_t = close_t - 5 * 60_000 + 1
        trigger = Kline(open_time=open_t, open=100.0, high=100.5,
                        low=98.0, close=100.0, volume=10.0, close_time=close_t)
        partial = Kline(open_time=open_t + 5 * 60_000, open=100.0, high=100.0,
                        low=100.0, close=100.0, volume=1.0,
                        close_time=open_t + 10 * 60_000 - 1)
        self.client.kline_responses["BTCUSDC"] = [trigger, partial]
        self.client.set_price(100.0, 100.04)
        self.client.entry_price_override = 100.04

        engine = self._engine()
        result = engine.step()
        self.assertEqual(len(result.opened), 1)
        self.assertEqual(result.opened[0]["mode"], "LIVE")

        cycle = self.store.active_strategy_cycle(W_NAME, "BTCUSDC")
        self.assertIsNotNone(cycle)
        self.assertEqual(cycle["dry_run"], 0)
        self.assertIsNotNone(cycle["exit_order_id"])


if __name__ == "__main__":
    unittest.main()
