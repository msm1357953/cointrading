import tempfile
import unittest
from pathlib import Path

from cointrading.config import TradingConfig
from cointrading.strategy_lifecycle import (
    manage_strategy_cycle,
    start_strategy_cycle_from_setup,
)
from cointrading.strategy_router import SETUP_PASS, StrategySetup
from cointrading.storage import TradingStore


class FakeStrategyClient:
    def __init__(self) -> None:
        self.next_order_id = 1
        self.orders = {}
        self.new_order_intents = []

    def exchange_info(self, symbol=None):
        symbol = symbol or "ETHUSDC"
        return {
            "symbols": [
                {
                    "symbol": symbol,
                    "filters": [
                        {"filterType": "PRICE_FILTER", "tickSize": "0.01"},
                        {
                            "filterType": "LOT_SIZE",
                            "minQty": "0.001",
                            "maxQty": "1000",
                            "stepSize": "0.001",
                        },
                        {"filterType": "MIN_NOTIONAL", "notional": "20"},
                    ],
                }
            ]
        }

    def book_ticker(self, symbol):
        return {"bidPrice": "99.90", "askPrice": "100.10"}

    def new_order(self, intent):
        self.new_order_intents.append(intent)
        order_id = self.next_order_id
        self.next_order_id += 1
        status = "FILLED" if intent.order_type == "MARKET" else "NEW"
        avg_price = intent.price if intent.price is not None else 100.0
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
                    "id": 201,
                    "orderId": 1,
                    "price": "100.00000000",
                    "qty": "0.25000000",
                    "commission": "0.01000000",
                    "commissionAsset": "USDC",
                    "realizedPnl": "0",
                }
            ]
        if int(order_id) == 2 and self.orders[int(order_id)]["status"] == "FILLED":
            return [
                {
                    "id": 202,
                    "orderId": 2,
                    "price": "101.00000000",
                    "qty": "0.25000000",
                    "commission": "0.01000000",
                    "commissionAsset": "USDC",
                    "realizedPnl": "0.25000000",
                }
            ]
        return []

    def cancel_order(self, *, symbol, order_id=None, orig_client_order_id=None):
        assert order_id is not None
        self.orders[int(order_id)] = {**self.orders[int(order_id)], "status": "CANCELED"}
        return self.orders[int(order_id)]


def _setup(strategy="trend_follow", side="long") -> StrategySetup:
    return StrategySetup(
        strategy=strategy,
        execution_mode="taker_trend" if strategy == "trend_follow" else "maker_range",
        status=SETUP_PASS,
        side=side,
        horizon="15m-4h",
        live_supported=True,
        reason="test setup",
    )


class StrategyLifecycleTests(unittest.TestCase):
    def test_strategy_entry_skips_when_scalp_cycle_active_for_symbol(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            store = TradingStore(Path(directory) / "cointrading.sqlite")
            store.insert_scalp_cycle(
                symbol="ETHUSDC",
                side="long",
                status="OPEN",
                quantity=0.25,
                entry_price=100.0,
                target_price=100.1,
                stop_price=99.5,
                maker_one_way_bps=0.0,
                taker_one_way_bps=3.6,
                entry_deadline_ms=60_000,
            )
            client = FakeStrategyClient()

            result = start_strategy_cycle_from_setup(
                client,
                store,
                _setup(),
                TradingConfig(
                    strategy_lifecycle_enabled=True,
                    runtime_risk_enabled=False,
                ),
                symbol="ETHUSDC",
                bid=99.9,
                ask=100.0,
                timestamp_ms=1_000,
            )

            self.assertEqual(result.action, "skip")
            self.assertIn("already active for this symbol", result.detail)
            self.assertEqual(client.new_order_intents, [])
            self.assertIsNone(store.active_strategy_cycle("trend_follow", "ETHUSDC"))

    def test_strategy_entry_skips_when_different_strategy_active_for_symbol(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            store = TradingStore(Path(directory) / "cointrading.sqlite")
            store.insert_strategy_cycle(
                strategy="range_reversion",
                execution_mode="maker_range",
                symbol="ETHUSDC",
                side="short",
                status="OPEN",
                quantity=0.25,
                entry_price=100.0,
                target_price=99.0,
                stop_price=101.0,
                entry_order_type="LIMIT",
                take_profit_bps=20,
                stop_loss_bps=25,
                max_hold_seconds=3_600,
                maker_one_way_bps=0.0,
                taker_one_way_bps=3.6,
                entry_deadline_ms=60_000,
                dry_run=True,
            )
            client = FakeStrategyClient()

            result = start_strategy_cycle_from_setup(
                client,
                store,
                _setup(strategy="trend_follow", side="long"),
                TradingConfig(
                    strategy_lifecycle_enabled=True,
                    runtime_risk_enabled=False,
                ),
                symbol="ETHUSDC",
                bid=99.9,
                ask=100.0,
                timestamp_ms=1_000,
            )

            self.assertEqual(result.action, "skip")
            self.assertIn("already active for this symbol", result.detail)
            self.assertEqual(client.new_order_intents, [])
            self.assertIsNone(store.active_strategy_cycle("trend_follow", "ETHUSDC"))

    def test_strategy_entry_blocks_when_observed_paper_result_is_bad(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            store = TradingStore(Path(directory) / "cointrading.sqlite")
            client = FakeStrategyClient()
            config = TradingConfig(
                strategy_lifecycle_enabled=True,
                runtime_risk_enabled=False,
                strategy_early_block_samples=3,
            )
            store.insert_strategy_evaluations(
                [
                    {
                        "source": "strategy_cycles",
                        "execution_mode": "taker_trend",
                        "symbol": "ETHUSDC",
                        "regime": "trend_follow",
                        "side": "long",
                        "take_profit_bps": config.trend_take_profit_bps,
                        "stop_loss_bps": config.trend_stop_loss_bps,
                        "max_hold_seconds": int(config.trend_max_hold_seconds),
                        "sample_count": 3,
                        "win_count": 1,
                        "loss_count": 2,
                        "win_rate": 1 / 3,
                        "avg_pnl_bps": -12.0,
                        "sum_pnl_bps": -36.0,
                        "avg_win_bps": 20.0,
                        "avg_loss_bps": -28.0,
                        "decision": "BLOCKED",
                        "reason": "평균손익 -12.000bps < 0",
                    }
                ],
                timestamp_ms=10_000,
            )

            result = start_strategy_cycle_from_setup(
                client,
                store,
                _setup(),
                config,
                symbol="ETHUSDC",
                bid=99.9,
                ask=100.0,
                timestamp_ms=11_000,
            )

            self.assertEqual(result.action, "blocked")
            self.assertIn("observed paper veto", result.detail)
            self.assertEqual(client.new_order_intents, [])
            self.assertIsNone(store.active_strategy_cycle("trend_follow", "ETHUSDC"))

    def test_trend_strategy_paper_cycle_opens_and_closes(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            store = TradingStore(Path(directory) / "cointrading.sqlite")
            client = FakeStrategyClient()
            config = TradingConfig(
                strategy_order_notional=25,
                max_single_order_notional=25,
                trend_take_profit_bps=100,
                trend_stop_loss_bps=50,
                strategy_lifecycle_enabled=True,
                runtime_risk_enabled=False,
            )

            start = start_strategy_cycle_from_setup(
                client,
                store,
                _setup(),
                config,
                symbol="ETHUSDC",
                bid=99.9,
                ask=100.0,
                timestamp_ms=1_000,
            )
            self.assertEqual(start.action, "entry_submitted")
            cycle = store.active_strategy_cycle("trend_follow", "ETHUSDC")
            assert cycle is not None

            opened = manage_strategy_cycle(
                client,
                store,
                cycle,
                config,
                bid=99.9,
                ask=100.0,
                timestamp_ms=2_000,
            )
            self.assertEqual(opened.action, "entry_filled")
            cycle = store.active_strategy_cycle("trend_follow", "ETHUSDC")
            assert cycle is not None

            closed = manage_strategy_cycle(
                client,
                store,
                cycle,
                config,
                bid=101.1,
                ask=101.2,
                timestamp_ms=3_000,
            )
            self.assertEqual(closed.action, "take_profit")
            self.assertIsNone(store.active_strategy_cycle("trend_follow", "ETHUSDC"))
            self.assertEqual(store.summary_counts()["strategy_cycles"], 1)
            self.assertEqual(store.summary_counts()["fills"], 2)

    def test_live_strategy_lifecycle_requires_explicit_switch(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            store = TradingStore(Path(directory) / "cointrading.sqlite")
            config = TradingConfig(
                dry_run=False,
                live_trading_enabled=True,
                live_strategy_lifecycle_enabled=False,
                runtime_risk_enabled=False,
            )

            result = start_strategy_cycle_from_setup(
                FakeStrategyClient(),
                store,
                _setup(),
                config,
                symbol="ETHUSDC",
                bid=99.9,
                ask=100.0,
                timestamp_ms=1_000,
            )

            self.assertEqual(result.action, "blocked")
            self.assertIn("live strategy lifecycle is disabled", result.detail)

    def test_live_strategy_lifecycle_blocks_non_simple_gate_strategy(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            store = TradingStore(Path(directory) / "cointrading.sqlite")
            client = FakeStrategyClient()
            config = TradingConfig(
                dry_run=False,
                live_trading_enabled=True,
                live_strategy_lifecycle_enabled=True,
                live_one_shot_required=False,
                runtime_risk_enabled=False,
            )

            result = start_strategy_cycle_from_setup(
                client,
                store,
                _setup(strategy="range_reversion", side="long"),
                config,
                symbol="ETHUSDC",
                bid=99.9,
                ask=100.0,
                timestamp_ms=1_000,
            )

            self.assertEqual(result.action, "blocked")
            self.assertIn("허용 전략은 trend_follow뿐", result.detail)
            self.assertEqual(client.new_order_intents, [])

    def test_live_strategy_entry_and_take_profit_reconcile(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            store = TradingStore(Path(directory) / "cointrading.sqlite")
            client = FakeStrategyClient()
            config = TradingConfig(
                dry_run=False,
                live_trading_enabled=True,
                live_strategy_lifecycle_enabled=True,
                live_one_shot_required=False,
                runtime_risk_enabled=False,
                strategy_order_notional=25,
                max_single_order_notional=25,
                trend_take_profit_bps=100,
                trend_stop_loss_bps=50,
            )

            start_strategy_cycle_from_setup(
                client,
                store,
                _setup(),
                config,
                symbol="ETHUSDC",
                bid=99.9,
                ask=100.0,
                timestamp_ms=1_000,
            )
            cycle = store.active_strategy_cycle("trend_follow", "ETHUSDC")
            assert cycle is not None
            opened = manage_strategy_cycle(
                client,
                store,
                cycle,
                config,
                bid=99.9,
                ask=100.0,
                timestamp_ms=2_000,
            )
            self.assertEqual(opened.action, "entry_filled")
            cycle = store.active_strategy_cycle("trend_follow", "ETHUSDC")
            assert cycle is not None
            take_profit = manage_strategy_cycle(
                client,
                store,
                cycle,
                config,
                bid=101.0,
                ask=101.1,
                timestamp_ms=3_000,
            )

            self.assertEqual(take_profit.action, "take_profit")
            self.assertTrue(client.new_order_intents[-1].reduce_only)
            self.assertEqual(client.new_order_intents[-1].order_type, "MARKET")
            self.assertIsNone(store.active_strategy_cycle("trend_follow", "ETHUSDC"))
            recent = store.recent_strategy_cycles(limit=1)[0]
            self.assertEqual(recent["status"], "CLOSED")
            self.assertGreater(float(recent["realized_pnl"]), 0)


if __name__ == "__main__":
    unittest.main()
