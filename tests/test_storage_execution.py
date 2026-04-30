import tempfile
import unittest
from pathlib import Path

from cointrading.config import TradingConfig
from cointrading.execution import build_post_only_intent
from cointrading.models import OrderIntent
from cointrading.scalping import ScalpSignal
from cointrading.storage import TradingStore


def _signal(side="long", trade_allowed=True, maker_cost=0.0):
    return ScalpSignal(
        symbol="BTCUSDC",
        side=side,
        reason="bid imbalance with positive momentum",
        regime="aligned_long",
        trade_allowed=trade_allowed,
        mid_price=100.0,
        spread_bps=1.0,
        imbalance=0.5,
        momentum_bps=5.0,
        realized_vol_bps=3.0,
        maker_roundtrip_bps=maker_cost,
        taker_roundtrip_bps=7.2,
        edge_after_maker_bps=5.0 - maker_cost,
        book_bid_notional=100_000.0,
        book_ask_notional=100_000.0,
        book_depth_notional=200_000.0,
        bnb_fee_discount_enabled=True,
        bnb_fee_discount_active=True,
        latest_funding_rate=0.0,
    )


class StorageExecutionTests(unittest.TestCase):
    def test_store_records_signals_orders_and_fees(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            store = TradingStore(Path(directory) / "cointrading.sqlite")
            signal_id = store.insert_signal(_signal(), timestamp_ms=1)
            intent = OrderIntent(
                symbol="BTCUSDC",
                side="BUY",
                quantity=0.25,
                order_type="LIMIT",
                price=99.99,
                time_in_force="GTX",
            )
            order_id = store.insert_order_attempt(
                intent,
                status="DRY_RUN",
                dry_run=True,
                reason="test",
                signal_id=signal_id,
            )
            store.record_fee_snapshot(
                "BTCUSDC",
                0.0,
                3.6,
                bnb_fee_discount_enabled=True,
                bnb_fee_discount_active=True,
            )
            counts = store.summary_counts()
            self.assertEqual(counts["signals"], 1)
            self.assertEqual(counts["orders"], 1)
            self.assertEqual(counts["fee_snapshots"], 1)
            self.assertEqual(store.recent_orders()[0]["id"], order_id)

    def test_post_only_intent_uses_gtx_and_passive_price(self) -> None:
        decision = build_post_only_intent(
            _signal(),
            TradingConfig(post_only_order_notional=25, max_single_order_notional=50),
        )
        self.assertTrue(decision.allowed)
        assert decision.intent is not None
        self.assertEqual(decision.intent.time_in_force, "GTX")
        self.assertEqual(decision.intent.order_type, "LIMIT")
        self.assertEqual(decision.intent.side, "BUY")
        self.assertLess(decision.intent.price, 100.0)

    def test_post_only_intent_blocks_bad_signal(self) -> None:
        decision = build_post_only_intent(_signal(side="flat"), TradingConfig())
        self.assertFalse(decision.allowed)
        self.assertIsNone(decision.intent)


if __name__ == "__main__":
    unittest.main()
