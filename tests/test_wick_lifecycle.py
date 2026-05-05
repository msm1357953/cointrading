import tempfile
import unittest
from dataclasses import replace
from pathlib import Path

from cointrading.config import TradingConfig
from cointrading.models import Kline
from cointrading.storage import TradingStore
from cointrading.wick_lifecycle import (
    STATUS_CLOSED,
    STATUS_OPEN,
    STATUS_STOPPED,
    STRATEGY_NAME,
    WickReversionEngine,
    detect_wick,
)


def _kline(open_time, *, o, h, l, c, v=10.0):
    return Kline(open_time=open_time, open=o, high=h, low=l, close=c,
                 volume=v, close_time=open_time + 5 * 60_000 - 1)


class FakeWickClient:
    def __init__(self) -> None:
        self.kline_responses: dict[str, list[Kline]] = {}
        self.book_responses: dict[str, dict] = {}

    def klines(self, symbol: str, interval: str, limit: int = 500):
        return list(self.kline_responses.get(symbol, []))

    def book_ticker(self, symbol: str):
        return self.book_responses.get(symbol, {"bidPrice": "0", "askPrice": "0"})


def _config_for_test() -> TradingConfig:
    return replace(
        TradingConfig(),
        dry_run=True,
        wick_carry_enabled=True,
        wick_carry_symbols=("BTCUSDC", "ETHUSDC"),
        wick_carry_min_wick_ratio=0.7,
        wick_carry_min_drop_pct=0.01,
        wick_carry_notional=80.0,
        wick_carry_stop_loss_bps=300.0,
        wick_carry_max_hold_seconds=7_200,
        wick_carry_cooldown_seconds=600,
        wick_carry_freshness_seconds=360,
        taker_fee_rate=0.0004,
        maker_fee_rate=0.0002,
    )


class DetectWickTests(unittest.TestCase):
    def test_long_lower_wick_triggers(self) -> None:
        # open=100, high=101, low=98 (3% drop), close=100.5
        # body_low = 100, lower_wick = 100-98 = 2, range = 3, ratio = 0.667 (just below)
        k = _kline(0, o=100, h=101, l=98, c=100.5)
        triggered, ratio, drop = detect_wick(k, min_wick_ratio=0.5, min_drop_pct=0.01)
        self.assertTrue(triggered)
        self.assertAlmostEqual(ratio, 2/3, places=3)
        self.assertAlmostEqual(drop, 0.02, places=4)

    def test_short_lower_wick_does_not_trigger(self) -> None:
        # Mostly body, small wick
        k = _kline(0, o=100, h=101, l=99.8, c=100.9)
        triggered, ratio, drop = detect_wick(k, min_wick_ratio=0.7, min_drop_pct=0.01)
        self.assertFalse(triggered)

    def test_zero_range_does_not_crash(self) -> None:
        k = _kline(0, o=100, h=100, l=100, c=100)
        triggered, ratio, drop = detect_wick(k, min_wick_ratio=0.7, min_drop_pct=0.01)
        self.assertFalse(triggered)
        self.assertEqual(ratio, 0.0)


class WickReversionEngineTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmpdir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tmpdir.name) / "test.sqlite"
        self.storage = TradingStore(self.db_path)
        self.client = FakeWickClient()
        self.now_ms = 1_777_968_000_000

    def tearDown(self) -> None:
        self.tmpdir.cleanup()

    def _engine(self, *, config: TradingConfig | None = None) -> WickReversionEngine:
        return WickReversionEngine(
            config=config or _config_for_test(),
            storage=self.storage,
            client=self.client,
            now_ms_fn=lambda: self.now_ms,
        )

    def _set_klines(self, symbol: str, *, trigger_open_ms: int, o, h, l, c) -> None:
        # The engine takes the SECOND-to-last kline as the trigger bar.
        # Provide [trigger, partial] so trigger is at index -2.
        trigger = _kline(trigger_open_ms, o=o, h=h, l=l, c=c)
        partial = _kline(trigger_open_ms + 5 * 60_000, o=c, h=c, l=c, c=c)
        self.client.kline_responses[symbol] = [trigger, partial]

    def test_does_not_open_when_disabled(self) -> None:
        cfg = replace(_config_for_test(), wick_carry_enabled=False)
        self._set_klines("BTCUSDC", trigger_open_ms=self.now_ms - 60_000, o=100, h=101, l=98, c=100.5)
        self.client.book_responses["BTCUSDC"] = {"bidPrice": "100", "askPrice": "100.04"}
        engine = self._engine(config=cfg)
        result = engine.step()
        self.assertEqual(result.opened, [])
        self.assertEqual(result.skipped, [{"reason": "wick_carry_disabled"}])

    def test_opens_on_long_lower_wick(self) -> None:
        # Trigger bar closed 60s ago (within freshness)
        # close_time = trigger_open + 5min - 1ms. Want now - close_time = 60s.
        # close_time = now - 60_000 -> trigger_open = close_time - 5min + 1
        close_time_ms = self.now_ms - 60_000
        trigger_open = close_time_ms - 5 * 60_000 + 1
        self._set_klines("BTCUSDC", trigger_open_ms=trigger_open,
                         o=100.0, h=100.5, l=98.0, c=100.0)
        self.client.book_responses["BTCUSDC"] = {"bidPrice": "100.00", "askPrice": "100.04"}
        # ETH: weak signal
        self._set_klines("ETHUSDC", trigger_open_ms=trigger_open,
                         o=2000, h=2010, l=1999, c=2008)
        self.client.book_responses["ETHUSDC"] = {"bidPrice": "2000", "askPrice": "2000.4"}

        engine = self._engine()
        result = engine.step()

        self.assertEqual(len(result.opened), 1)
        self.assertEqual(result.opened[0]["symbol"], "BTCUSDC")
        cycle = self.storage.active_strategy_cycle(STRATEGY_NAME, "BTCUSDC")
        self.assertIsNotNone(cycle)
        self.assertEqual(cycle["status"], STATUS_OPEN)
        self.assertEqual(cycle["dry_run"], 1)
        self.assertAlmostEqual(cycle["entry_price"], 100.02, places=4)
        self.assertAlmostEqual(cycle["stop_price"], 100.02 * 0.97, places=4)

    def test_skips_when_bar_not_fresh(self) -> None:
        # Trigger bar closed 30 minutes ago (way past freshness=6min)
        close_time_ms = self.now_ms - 30 * 60_000
        trigger_open = close_time_ms - 5 * 60_000 + 1
        self._set_klines("BTCUSDC", trigger_open_ms=trigger_open,
                         o=100.0, h=100.5, l=98.0, c=100.0)
        self.client.book_responses["BTCUSDC"] = {"bidPrice": "100", "askPrice": "100.04"}
        engine = self._engine()
        result = engine.step()
        self.assertEqual(result.opened, [])
        not_fresh = [s for s in result.skipped if s.get("reason") == "bar_not_fresh"]
        self.assertEqual(len(not_fresh), 1)

    def test_cooldown_blocks_immediate_re_entry(self) -> None:
        # Insert a freshly-closed cycle directly via storage to simulate cooldown
        self.storage.insert_strategy_cycle(
            strategy=STRATEGY_NAME, execution_mode="taker_market", symbol="BTCUSDC",
            side="long", status=STATUS_CLOSED, quantity=1.0, entry_price=100.0,
            target_price=105.0, stop_price=97.0, entry_order_type="MARKET",
            take_profit_bps=0.0, stop_loss_bps=300.0, max_hold_seconds=7200,
            maker_one_way_bps=2.0, taker_one_way_bps=4.0,
            entry_deadline_ms=self.now_ms - 1000, dry_run=True,
        )
        # Mark closed_ms to "30 sec ago" via an update
        cycle = self.storage.recent_strategy_cycles(limit=1)[0]
        self.storage.update_strategy_cycle(
            int(cycle["id"]), closed_ms=self.now_ms - 30_000, realized_pnl=1.0,
        )

        # Now the wick triggers, but cooldown should block
        close_time_ms = self.now_ms - 60_000
        trigger_open = close_time_ms - 5 * 60_000 + 1
        self._set_klines("BTCUSDC", trigger_open_ms=trigger_open,
                         o=100.0, h=100.5, l=98.0, c=100.0)
        self.client.book_responses["BTCUSDC"] = {"bidPrice": "100", "askPrice": "100.04"}

        engine = self._engine()
        result = engine.step()
        self.assertEqual(result.opened, [])
        cooldown = [s for s in result.skipped if s.get("reason") == "cooldown"]
        self.assertEqual(len(cooldown), 1)

    def test_time_exit_closes_cycle(self) -> None:
        close_time_ms = self.now_ms - 60_000
        trigger_open = close_time_ms - 5 * 60_000 + 1
        self._set_klines("BTCUSDC", trigger_open_ms=trigger_open,
                         o=100.0, h=100.5, l=98.0, c=100.0)
        self.client.book_responses["BTCUSDC"] = {"bidPrice": "100.00", "askPrice": "100.04"}

        engine = self._engine()
        engine.step()
        # Advance past 2h max hold
        self.now_ms += 3 * 3600 * 1000
        # Slightly higher exit
        self.client.book_responses["BTCUSDC"] = {"bidPrice": "101.00", "askPrice": "101.04"}
        result = engine.step()

        actions = [m["action"] for m in result.managed]
        self.assertIn("closed_time", actions)
        cycle = self.storage.recent_strategy_cycles(limit=1)[0]
        self.assertEqual(cycle["status"], STATUS_CLOSED)
        self.assertEqual(cycle["reason"], "time_exit")
        self.assertGreater(cycle["realized_pnl"], 0)

    def test_stop_loss_closes_cycle(self) -> None:
        close_time_ms = self.now_ms - 60_000
        trigger_open = close_time_ms - 5 * 60_000 + 1
        self._set_klines("BTCUSDC", trigger_open_ms=trigger_open,
                         o=100.0, h=100.5, l=98.0, c=100.0)
        self.client.book_responses["BTCUSDC"] = {"bidPrice": "100.00", "askPrice": "100.04"}

        engine = self._engine()
        engine.step()
        # Drop below stop (entry 100.02, stop 97.0194)
        self.client.book_responses["BTCUSDC"] = {"bidPrice": "96.50", "askPrice": "96.55"}
        self.now_ms += 60_000
        result = engine.step()

        actions = [m["action"] for m in result.managed]
        self.assertIn("stopped", actions)
        cycle = self.storage.recent_strategy_cycles(limit=1)[0]
        self.assertEqual(cycle["status"], STATUS_STOPPED)
        self.assertEqual(cycle["reason"], "stop_loss")
        self.assertLess(cycle["realized_pnl"], 0)


if __name__ == "__main__":
    unittest.main()
