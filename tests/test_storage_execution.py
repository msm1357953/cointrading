import tempfile
import unittest
from pathlib import Path

from cointrading.config import TradingConfig
from cointrading.execution import build_post_only_intent
from cointrading.market_regime import MarketRegimeSnapshot
from cointrading.models import OrderIntent
from cointrading.scalp_lifecycle import manage_cycle, start_cycle_from_signal
from cointrading.scalping import ScalpSignal
from cointrading.storage import TradingStore, kst_from_ms


class FakeOrderClient:
    def new_order(self, intent):
        return {"dryRun": True, "params": {"symbol": intent.symbol, "side": intent.side}}


class FailingOrderClient:
    def new_order(self, intent):
        raise AssertionError("live order should not be submitted")


class FakeLiveOrderClient:
    def __init__(self) -> None:
        self.next_order_id = 1
        self.orders = {}
        self.new_order_intents = []
        self.cancelled_order_ids = []

    def new_order(self, intent):
        self.new_order_intents.append(intent)
        order_id = self.next_order_id
        self.next_order_id += 1
        status = "FILLED" if intent.order_type == "MARKET" else "NEW"
        avg_price = intent.price if intent.price is not None else 99.0
        response = {
            "orderId": order_id,
            "clientOrderId": intent.client_order_id,
            "symbol": intent.symbol,
            "side": intent.side,
            "status": status,
            "executedQty": f"{intent.quantity:.8f}" if status == "FILLED" else "0",
            "avgPrice": f"{avg_price:.8f}" if status == "FILLED" else "0",
        }
        self.orders[order_id] = response
        return response

    def order_status(self, *, symbol, order_id=None, orig_client_order_id=None):
        assert order_id is not None
        row = dict(self.orders[int(order_id)])
        if int(order_id) == 1:
            row["status"] = "FILLED"
            row["executedQty"] = "0.25000000"
            row["avgPrice"] = "100.00000000"
            self.orders[int(order_id)] = row
        return row

    def account_trades(self, *, symbol, order_id=None, limit=50):
        if int(order_id) == 1:
            return [
                {
                    "id": 101,
                    "orderId": 1,
                    "price": "100.00000000",
                    "qty": "0.25000000",
                    "commission": "0.00000000",
                    "commissionAsset": "USDC",
                    "realizedPnl": "0",
                }
            ]
        if int(order_id) == 3:
            return [
                {
                    "id": 103,
                    "orderId": 3,
                    "price": "99.00000000",
                    "qty": "0.25000000",
                    "commission": "0.01000000",
                    "commissionAsset": "USDC",
                    "realizedPnl": "-0.25000000",
                }
            ]
        if int(order_id) == 2 and self.orders[int(order_id)]["status"] == "FILLED":
            return [
                {
                    "id": 102,
                    "orderId": 2,
                    "price": "100.10000000",
                    "qty": "0.25000000",
                    "commission": "0.00000000",
                    "commissionAsset": "USDC",
                    "realizedPnl": "0.02500000",
                }
            ]
        return []

    def cancel_order(self, *, symbol, order_id=None, orig_client_order_id=None):
        assert order_id is not None
        self.cancelled_order_ids.append(int(order_id))
        self.orders[int(order_id)] = {**self.orders[int(order_id)], "status": "CANCELED"}
        return self.orders[int(order_id)]


def _signal(
    side="long",
    trade_allowed=True,
    maker_cost=0.0,
    symbol="BTCUSDC",
    regime="aligned_long",
):
    return ScalpSignal(
        symbol=symbol,
        side=side,
        reason="bid imbalance with positive momentum",
        regime=regime,
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
    def test_kst_display_time(self) -> None:
        self.assertEqual(kst_from_ms(0), "1970-01-01 09:00:00 KST")

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
                strategy_gate_enabled=False,
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

    def test_lifecycle_uses_approved_strategy_candidate_parameters(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            store = TradingStore(Path(directory) / "cointrading.sqlite")
            client = FakeOrderClient()
            config = TradingConfig(
                scalp_take_profit_bps=3,
                scalp_stop_loss_bps=6,
                scalp_max_hold_seconds=180,
                strategy_gate_enabled=True,
            )
            store.insert_strategy_evaluations(
                [
                    {
                        "source": "signal_grid",
                        "execution_mode": "maker_post_only",
                        "symbol": "DOGEUSDC",
                        "regime": "aligned_short",
                        "side": "short",
                        "take_profit_bps": 20.0,
                        "stop_loss_bps": 4.0,
                        "max_hold_seconds": 300,
                        "sample_count": 50,
                        "win_count": 23,
                        "loss_count": 27,
                        "win_rate": 23 / 50,
                        "avg_pnl_bps": 2.5,
                        "sum_pnl_bps": 125.0,
                        "avg_win_bps": 20.0,
                        "avg_loss_bps": -6.0,
                        "decision": "APPROVED",
                        "reason": "test candidate",
                    }
                ],
                timestamp_ms=10_000,
            )
            signal = _signal(side="short", symbol="DOGEUSDC", regime="aligned_short")
            signal_id = store.insert_signal(signal, timestamp_ms=11_000)

            start = start_cycle_from_signal(
                client,
                store,
                signal,
                config,
                signal_id=signal_id,
                timestamp_ms=12_000,
            )

            self.assertEqual(start.action, "entry_submitted")
            cycle = store.active_scalp_cycle("DOGEUSDC")
            assert cycle is not None
            self.assertEqual(float(cycle["strategy_take_profit_bps"]), 20.0)
            self.assertEqual(float(cycle["strategy_stop_loss_bps"]), 4.0)
            self.assertEqual(int(cycle["strategy_max_hold_seconds"]), 300)

            filled = manage_cycle(
                client,
                store,
                cycle,
                config,
                bid=float(cycle["entry_price"]) + 0.01,
                ask=float(cycle["entry_price"]) + 0.02,
                timestamp_ms=13_000,
            )

            self.assertEqual(filled.action, "entry_filled")
            cycle = store.active_scalp_cycle("DOGEUSDC")
            assert cycle is not None
            self.assertEqual(int(cycle["max_hold_deadline_ms"]), 313_000)

    def test_lifecycle_blocks_scalp_against_macro_router(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            store = TradingStore(Path(directory) / "cointrading.sqlite")
            config = TradingConfig(strategy_gate_enabled=False, macro_regime_gate_enabled=True)
            store.insert_market_regime(
                MarketRegimeSnapshot(
                    symbol="BTCUSDC",
                    macro_regime="macro_bear",
                    trade_bias="short",
                    allowed_strategies=("trend_short_15m_1h",),
                    blocked_reason="",
                    last_price=100.0,
                    trend_1h_bps=-50.0,
                    trend_4h_bps=-120.0,
                    realized_vol_bps=20.0,
                    atr_bps=30.0,
                    timestamp_ms=1_000,
                )
            )
            signal = _signal(side="long")
            signal_id = store.insert_signal(signal, timestamp_ms=2_000)

            result = start_cycle_from_signal(
                FakeOrderClient(),
                store,
                signal,
                config,
                signal_id=signal_id,
                timestamp_ms=3_000,
            )

            self.assertEqual(result.action, "blocked")
            self.assertIn("bear regime blocks long", result.detail)

    def test_lifecycle_blocks_live_without_reconciliation_switch(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            store = TradingStore(Path(directory) / "cointrading.sqlite")
            config = TradingConfig(
                dry_run=False,
                live_trading_enabled=True,
                live_scalp_lifecycle_enabled=False,
                runtime_risk_enabled=False,
                strategy_gate_enabled=False,
            )
            signal = _signal()
            signal_id = store.insert_signal(signal, timestamp_ms=2_000)

            result = start_cycle_from_signal(
                FailingOrderClient(),
                store,
                signal,
                config,
                signal_id=signal_id,
                timestamp_ms=3_000,
            )

            self.assertEqual(result.action, "blocked")
            self.assertIn("live scalp lifecycle is disabled", result.detail)

    def test_live_entry_fill_reconciles_and_submits_reduce_only_target(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            store = TradingStore(Path(directory) / "cointrading.sqlite")
            client = FakeLiveOrderClient()
            config = TradingConfig(
                dry_run=False,
                live_trading_enabled=True,
                live_scalp_lifecycle_enabled=True,
                runtime_risk_enabled=False,
                strategy_gate_enabled=False,
                scalp_take_profit_bps=10,
                scalp_stop_loss_bps=5,
            )
            signal = _signal()
            signal_id = store.insert_signal(signal, timestamp_ms=2_000)

            start = start_cycle_from_signal(
                client,
                store,
                signal,
                config,
                signal_id=signal_id,
                timestamp_ms=3_000,
            )
            self.assertEqual(start.action, "entry_submitted")

            cycle = store.active_scalp_cycle("BTCUSDC")
            assert cycle is not None
            filled = manage_cycle(
                client,
                store,
                cycle,
                config,
                bid=99.9,
                ask=100.1,
                timestamp_ms=4_000,
            )

            self.assertEqual(filled.action, "entry_filled")
            cycle = store.active_scalp_cycle("BTCUSDC")
            assert cycle is not None
            self.assertEqual(cycle["status"], "EXIT_SUBMITTED")
            self.assertAlmostEqual(float(cycle["entry_price"]), 100.0)
            self.assertAlmostEqual(float(cycle["target_price"]), 100.1)
            self.assertAlmostEqual(float(cycle["stop_price"]), 99.9500249875)
            target_intent = client.new_order_intents[-1]
            self.assertTrue(target_intent.reduce_only)
            self.assertEqual(target_intent.order_type, "LIMIT")
            self.assertEqual(target_intent.time_in_force, "GTX")
            self.assertEqual(store.summary_counts()["fills"], 1)

    def test_live_stop_loss_cancels_target_and_records_market_exit(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            store = TradingStore(Path(directory) / "cointrading.sqlite")
            client = FakeLiveOrderClient()
            config = TradingConfig(
                dry_run=False,
                live_trading_enabled=True,
                live_scalp_lifecycle_enabled=True,
                runtime_risk_enabled=False,
                strategy_gate_enabled=False,
                scalp_take_profit_bps=10,
                scalp_stop_loss_bps=5,
            )
            signal = _signal()
            signal_id = store.insert_signal(signal, timestamp_ms=2_000)
            start_cycle_from_signal(
                client,
                store,
                signal,
                config,
                signal_id=signal_id,
                timestamp_ms=3_000,
            )
            cycle = store.active_scalp_cycle("BTCUSDC")
            assert cycle is not None
            manage_cycle(
                client,
                store,
                cycle,
                config,
                bid=99.9,
                ask=100.1,
                timestamp_ms=4_000,
            )
            cycle = store.active_scalp_cycle("BTCUSDC")
            assert cycle is not None
            exit_order_id = int(cycle["exit_order_id"])

            stopped = manage_cycle(
                client,
                store,
                cycle,
                config,
                bid=99.8,
                ask=99.9,
                timestamp_ms=5_000,
            )

            self.assertEqual(stopped.action, "stop_loss")
            self.assertIn(exit_order_id, client.cancelled_order_ids)
            stop_intent = client.new_order_intents[-1]
            self.assertTrue(stop_intent.reduce_only)
            self.assertEqual(stop_intent.order_type, "MARKET")
            self.assertIsNone(store.active_scalp_cycle("BTCUSDC"))
            recent = store.recent_scalp_cycles(limit=1)[0]
            self.assertEqual(recent["status"], "STOPPED")
            self.assertEqual(recent["reason"], "stop_loss")
            self.assertLess(float(recent["realized_pnl"]), 0)
            self.assertEqual(store.summary_counts()["fills"], 2)

    def test_live_take_profit_fill_closes_cycle(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            store = TradingStore(Path(directory) / "cointrading.sqlite")
            client = FakeLiveOrderClient()
            config = TradingConfig(
                dry_run=False,
                live_trading_enabled=True,
                live_scalp_lifecycle_enabled=True,
                runtime_risk_enabled=False,
                strategy_gate_enabled=False,
                scalp_take_profit_bps=10,
                scalp_stop_loss_bps=5,
            )
            signal = _signal()
            signal_id = store.insert_signal(signal, timestamp_ms=2_000)
            start_cycle_from_signal(
                client,
                store,
                signal,
                config,
                signal_id=signal_id,
                timestamp_ms=3_000,
            )
            cycle = store.active_scalp_cycle("BTCUSDC")
            assert cycle is not None
            manage_cycle(
                client,
                store,
                cycle,
                config,
                bid=99.9,
                ask=100.1,
                timestamp_ms=4_000,
            )
            cycle = store.active_scalp_cycle("BTCUSDC")
            assert cycle is not None
            exit_order_id = int(cycle["exit_order_id"])
            client.orders[2] = {
                **client.orders[2],
                "status": "FILLED",
                "executedQty": "0.25000000",
                "avgPrice": "100.10000000",
            }

            closed = manage_cycle(
                client,
                store,
                cycle,
                config,
                bid=100.1,
                ask=100.2,
                timestamp_ms=5_000,
            )

            self.assertEqual(closed.action, "take_profit")
            self.assertIsNone(store.active_scalp_cycle("BTCUSDC"))
            recent = store.recent_scalp_cycles(limit=1)[0]
            self.assertEqual(recent["status"], "CLOSED")
            self.assertEqual(recent["reason"], "take_profit")
            self.assertEqual(int(recent["exit_order_id"]), exit_order_id)
            self.assertGreater(float(recent["realized_pnl"]), 0)
            self.assertEqual(store.summary_counts()["fills"], 2)


if __name__ == "__main__":
    unittest.main()
