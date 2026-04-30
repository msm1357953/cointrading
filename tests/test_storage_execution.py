import tempfile
import unittest
from pathlib import Path

from cointrading.config import TradingConfig
from cointrading.execution import build_post_only_intent
from cointrading.models import OrderIntent
from cointrading.scalp_lifecycle import manage_cycle, start_cycle_from_signal
from cointrading.scalping import ScalpSignal
from cointrading.storage import TradingStore


class FakeOrderClient:
    def new_order(self, intent):
        return {"dryRun": True, "params": {"symbol": intent.symbol, "side": intent.side}}


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

    def test_scalp_lifecycle_enters_submits_target_and_closes(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            store = TradingStore(Path(directory) / "cointrading.sqlite")
            client = FakeOrderClient()
            config = TradingConfig(
                post_only_order_notional=25,
                max_single_order_notional=50,
                scalp_take_profit_bps=3,
                scalp_stop_loss_bps=6,
                scalp_entry_timeout_seconds=45,
                scalp_exit_reprice_seconds=45,
                scalp_max_hold_seconds=180,
            )
            signal_id = store.insert_signal(_signal(maker_cost=0), timestamp_ms=1)
            start = start_cycle_from_signal(
                client,
                store,
                _signal(maker_cost=0),
                config,
                signal_id=signal_id,
                timestamp_ms=1_000,
            )
            self.assertEqual(start.action, "entry_submitted")
            cycle = store.active_scalp_cycle("BTCUSDC")
            assert cycle is not None
            self.assertEqual(cycle["status"], "ENTRY_SUBMITTED")

            filled = manage_cycle(
                client,
                store,
                cycle,
                config,
                bid=99.98,
                ask=99.99,
                timestamp_ms=2_000,
            )
            self.assertEqual(filled.action, "entry_filled")
            cycle = store.active_scalp_cycle("BTCUSDC")
            assert cycle is not None
            self.assertEqual(cycle["status"], "EXIT_SUBMITTED")

            closed = manage_cycle(
                client,
                store,
                cycle,
                config,
                bid=float(cycle["target_price"]) + 0.01,
                ask=float(cycle["target_price"]) + 0.02,
                timestamp_ms=3_000,
            )
            self.assertEqual(closed.action, "take_profit")
            self.assertIsNone(store.active_scalp_cycle("BTCUSDC"))
            counts = store.summary_counts()
            self.assertEqual(counts["scalp_cycles"], 1)
            self.assertEqual(counts["fills"], 2)


if __name__ == "__main__":
    unittest.main()
