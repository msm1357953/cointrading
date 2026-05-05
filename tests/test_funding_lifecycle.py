import tempfile
import unittest
from dataclasses import replace
from pathlib import Path

from cointrading.config import TradingConfig
from cointrading.funding_lifecycle import (
    FundingCarryEngine,
    STATUS_CLOSED,
    STATUS_OPEN,
    STATUS_STOPPED,
    STRATEGY_NAME,
)
from cointrading.storage import TradingStore


class FakeFundingClient:
    """Lightweight Binance USDM client stub for the engine."""

    def __init__(self) -> None:
        self.funding_responses: dict[str, list[dict]] = {}
        self.book_responses: dict[str, dict] = {}

    def funding_rate(self, symbol: str, limit: int = 1):  # noqa: ARG002
        return list(self.funding_responses.get(symbol, []))

    def book_ticker(self, symbol: str):
        return self.book_responses.get(symbol, {"bidPrice": "0", "askPrice": "0"})


def _config_for_test() -> TradingConfig:
    base = TradingConfig()
    return replace(
        base,
        dry_run=True,
        funding_carry_enabled=True,
        funding_carry_symbols=("BTCUSDC", "ETHUSDC"),
        funding_carry_threshold=0.0001,
        funding_carry_notional=80.0,
        funding_carry_stop_loss_bps=300.0,
        funding_carry_max_hold_seconds=86_400,
        funding_carry_check_window_minutes=60,
        taker_fee_rate=0.0004,
        maker_fee_rate=0.0002,
    )


class FundingCarryEngineTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmpdir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tmpdir.name) / "test.sqlite"
        self.storage = TradingStore(self.db_path)
        self.client = FakeFundingClient()
        self.now_ms = 1_777_968_000_000  # fixed clock; ~2026-05-05 UTC

    def tearDown(self) -> None:
        self.tmpdir.cleanup()

    def _engine(self, *, config: TradingConfig | None = None) -> FundingCarryEngine:
        return FundingCarryEngine(
            config=config or _config_for_test(),
            storage=self.storage,
            client=self.client,
            now_ms_fn=lambda: self.now_ms,
        )

    def test_does_not_open_when_disabled(self) -> None:
        cfg = replace(_config_for_test(), funding_carry_enabled=False)
        self.client.funding_responses["BTCUSDC"] = [
            {"fundingRate": "-0.0005", "fundingTime": self.now_ms - 60_000}
        ]
        self.client.book_responses["BTCUSDC"] = {"bidPrice": "100", "askPrice": "100.04"}
        engine = self._engine(config=cfg)
        result = engine.step()
        self.assertEqual(result.opened, [])
        self.assertEqual(result.skipped, [{"reason": "funding_carry_disabled"}])

    def test_opens_on_negative_funding_within_window(self) -> None:
        self.client.funding_responses["BTCUSDC"] = [
            {"fundingRate": "-0.0005", "fundingTime": self.now_ms - 5 * 60_000}
        ]
        self.client.book_responses["BTCUSDC"] = {"bidPrice": "100.00", "askPrice": "100.04"}
        # ETHUSDC funding above threshold -> should NOT open
        self.client.funding_responses["ETHUSDC"] = [
            {"fundingRate": "0.0001", "fundingTime": self.now_ms - 5 * 60_000}
        ]
        self.client.book_responses["ETHUSDC"] = {"bidPrice": "2000", "askPrice": "2000.40"}

        engine = self._engine()
        result = engine.step()

        self.assertEqual(len(result.opened), 1)
        opened = result.opened[0]
        self.assertEqual(opened["symbol"], "BTCUSDC")
        self.assertAlmostEqual(opened["funding_rate"], -0.0005)

        cycle = self.storage.active_strategy_cycle(STRATEGY_NAME, "BTCUSDC")
        self.assertIsNotNone(cycle)
        self.assertEqual(cycle["status"], STATUS_OPEN)
        self.assertEqual(cycle["side"], "long")
        self.assertEqual(cycle["dry_run"], 1)
        self.assertAlmostEqual(cycle["entry_price"], 100.02, places=4)
        # stop = 100.02 * (1 - 0.03) = 97.0194
        self.assertAlmostEqual(cycle["stop_price"], 100.02 * 0.97, places=4)
        # quantity = 80 / 100.02
        self.assertAlmostEqual(cycle["quantity"], 80.0 / 100.02, places=8)

        # No second open for same symbol on next step (already_open)
        result2 = engine.step()
        self.assertEqual(result2.opened, [])
        already = [s for s in result2.skipped if s.get("reason") == "already_open"]
        self.assertTrue(any(s["symbol"] == "BTCUSDC" for s in already))

    def test_skips_outside_check_window(self) -> None:
        # funding event 2 hours ago (window is 60 minutes)
        self.client.funding_responses["BTCUSDC"] = [
            {"fundingRate": "-0.0010", "fundingTime": self.now_ms - 2 * 60 * 60_000}
        ]
        self.client.book_responses["BTCUSDC"] = {"bidPrice": "100", "askPrice": "100.04"}
        engine = self._engine()
        result = engine.step()
        self.assertEqual(result.opened, [])
        outside = [s for s in result.skipped if s.get("reason") == "outside_window"]
        self.assertEqual(len(outside), 1)

    def test_skips_when_funding_above_threshold(self) -> None:
        self.client.funding_responses["BTCUSDC"] = [
            {"fundingRate": "0.0050", "fundingTime": self.now_ms - 60_000}
        ]
        self.client.book_responses["BTCUSDC"] = {"bidPrice": "100", "askPrice": "100.04"}
        engine = self._engine()
        result = engine.step()
        self.assertEqual(result.opened, [])
        rejected = [s for s in result.skipped if s.get("reason") == "rate_above_threshold"]
        self.assertEqual(len(rejected), 1)

    def test_time_exit_closes_cycle(self) -> None:
        self.client.funding_responses["BTCUSDC"] = [
            {"fundingRate": "-0.0005", "fundingTime": self.now_ms - 60_000}
        ]
        self.client.book_responses["BTCUSDC"] = {"bidPrice": "100.00", "askPrice": "100.04"}

        engine = self._engine()
        engine.step()  # opens cycle
        # advance past max hold
        self.now_ms += 25 * 60 * 60 * 1000
        # price slightly higher
        self.client.book_responses["BTCUSDC"] = {"bidPrice": "101.00", "askPrice": "101.04"}
        result = engine.step()

        actions = [m["action"] for m in result.managed]
        self.assertIn("closed_time", actions)

        cycle = self.storage.recent_strategy_cycles(limit=1)[0]
        self.assertEqual(cycle["status"], STATUS_CLOSED)
        self.assertEqual(cycle["reason"], "time_exit")
        self.assertGreater(cycle["realized_pnl"], 0)  # +1% gross - ~8 bps fee = positive

    def test_stop_loss_closes_cycle(self) -> None:
        self.client.funding_responses["BTCUSDC"] = [
            {"fundingRate": "-0.0005", "fundingTime": self.now_ms - 60_000}
        ]
        self.client.book_responses["BTCUSDC"] = {"bidPrice": "100.00", "askPrice": "100.04"}

        engine = self._engine()
        engine.step()  # opens cycle at mid 100.02, stop 97.0194

        # Move price below stop
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
