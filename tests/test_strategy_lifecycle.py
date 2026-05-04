import tempfile
import unittest
from pathlib import Path

from cointrading.config import TradingConfig
from cointrading.market_regime import MarketRegimeSnapshot
from cointrading.strategy_lifecycle import (
    manage_strategy_cycle,
    strategy_plan_from_setup,
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
        order_id = int(order_id)
        if order_id == 1:
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
        if order_id > 1 and self.orders[order_id]["status"] == "FILLED":
            return [
                {
                    "id": 202,
                    "orderId": order_id,
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


class FailingStrategyClient:
    def new_order(self, intent):
        raise AssertionError("dry-run order should not be submitted to the exchange client")


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


def _macro_row(*, atr_bps=40.0, trend_1h_bps=40.0, trend_4h_bps=60.0):
    return {
        "atr_bps": atr_bps,
        "trend_1h_bps": trend_1h_bps,
        "trend_4h_bps": trend_4h_bps,
    }


def _insert_macro(
    store: TradingStore,
    *,
    symbol="ETHUSDC",
    timestamp_ms=1_000,
    atr_bps=40.0,
    trend_1h_bps=40.0,
    trend_4h_bps=60.0,
) -> None:
    store.insert_market_regime(
        MarketRegimeSnapshot(
            symbol=symbol,
            macro_regime="macro_bull",
            trade_bias="long",
            allowed_strategies=("trend_long_15m_1h",),
            blocked_reason="",
            last_price=100.0,
            trend_1h_bps=trend_1h_bps,
            trend_4h_bps=trend_4h_bps,
            realized_vol_bps=10.0,
            atr_bps=atr_bps,
            timestamp_ms=timestamp_ms,
        )
    )


def _insert_strategy_evaluation(
    store: TradingStore,
    config: TradingConfig,
    *,
    symbol="ETHUSDC",
    strategy="trend_follow",
    side="long",
    execution_mode="taker_trend",
    take_profit_bps=None,
    stop_loss_bps=None,
    max_hold_seconds=None,
    decision="APPROVED",
    sample_count=30,
    avg_pnl_bps=5.0,
    timestamp_ms=10_000,
) -> None:
    tp = config.trend_take_profit_bps if take_profit_bps is None else take_profit_bps
    sl = config.trend_stop_loss_bps if stop_loss_bps is None else stop_loss_bps
    hold = int(config.trend_max_hold_seconds if max_hold_seconds is None else max_hold_seconds)
    win_count = max(0, int(sample_count * 2 / 3))
    loss_count = max(0, sample_count - win_count)
    store.insert_strategy_evaluations(
        [
            {
                "source": "strategy_cycles",
                "execution_mode": execution_mode,
                "symbol": symbol,
                "regime": strategy,
                "side": side,
                "take_profit_bps": tp,
                "stop_loss_bps": sl,
                "max_hold_seconds": hold,
                "sample_count": sample_count,
                "win_count": win_count,
                "loss_count": loss_count,
                "win_rate": win_count / sample_count if sample_count else 0.0,
                "avg_pnl_bps": avg_pnl_bps,
                "sum_pnl_bps": avg_pnl_bps * sample_count,
                "avg_win_bps": 12.0 if win_count else None,
                "avg_loss_bps": -6.0 if loss_count else None,
                "decision": decision,
                "reason": "test evaluation",
            }
        ],
        timestamp_ms=timestamp_ms,
    )


class StrategyLifecycleTests(unittest.TestCase):
    def test_trend_plan_extends_target_when_trend_and_atr_are_strong(self) -> None:
        plan = strategy_plan_from_setup(
            _setup(strategy="trend_follow", side="long"),
            TradingConfig(
                trend_take_profit_bps=90,
                trend_stop_loss_bps=30,
                trend_max_hold_seconds=14_400,
            ),
            symbol="ETHUSDC",
            bid=99.9,
            ask=100.1,
            macro_row=_macro_row(atr_bps=80, trend_1h_bps=95, trend_4h_bps=130),
        )

        assert plan is not None
        self.assertEqual(plan.exit_profile, "trend_runner")
        self.assertGreater(plan.take_profit_bps, 90)
        self.assertGreaterEqual(plan.stop_loss_bps, 30)
        self.assertGreater(plan.max_hold_seconds, 14_400)

    def test_trend_plan_can_tighten_target_when_trend_is_weak(self) -> None:
        plan = strategy_plan_from_setup(
            _setup(strategy="trend_follow", side="long"),
            TradingConfig(
                trend_take_profit_bps=90,
                trend_stop_loss_bps=30,
                trend_max_hold_seconds=14_400,
            ),
            symbol="ETHUSDC",
            bid=99.9,
            ask=100.1,
            macro_row=_macro_row(atr_bps=18, trend_1h_bps=15, trend_4h_bps=20),
        )

        assert plan is not None
        self.assertEqual(plan.exit_profile, "trend_tight")
        self.assertLess(plan.take_profit_bps, 90)
        self.assertLessEqual(plan.stop_loss_bps, 30)
        self.assertLess(plan.max_hold_seconds, 14_400)

    def test_adaptive_exit_can_be_disabled(self) -> None:
        plan = strategy_plan_from_setup(
            _setup(strategy="breakout_reduced", side="long"),
            TradingConfig(
                breakout_take_profit_bps=120,
                breakout_stop_loss_bps=40,
                breakout_max_hold_seconds=7_200,
                strategy_adaptive_exits_enabled=False,
            ),
            symbol="ETHUSDC",
            bid=99.9,
            ask=100.1,
            macro_row=_macro_row(atr_bps=180, trend_1h_bps=160, trend_4h_bps=210),
        )

        assert plan is not None
        self.assertEqual(plan.exit_profile, "fixed")
        self.assertEqual(plan.take_profit_bps, 120)
        self.assertEqual(plan.stop_loss_bps, 40)
        self.assertEqual(plan.max_hold_seconds, 7_200)

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

    def test_adaptive_plan_is_blocked_by_broad_bad_observed_paper_result(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            store = TradingStore(Path(directory) / "cointrading.sqlite")
            client = FakeStrategyClient()
            config = TradingConfig(
                strategy_lifecycle_enabled=True,
                runtime_risk_enabled=False,
                strategy_early_block_samples=3,
            )
            _insert_macro(
                store,
                atr_bps=18.0,
                trend_1h_bps=15.0,
                trend_4h_bps=20.0,
                timestamp_ms=1_000,
            )
            _insert_strategy_evaluation(
                store,
                config,
                decision="BLOCKED",
                sample_count=3,
                avg_pnl_bps=-12.0,
                timestamp_ms=2_000,
            )

            result = start_strategy_cycle_from_setup(
                client,
                store,
                _setup(),
                config,
                symbol="ETHUSDC",
                bid=99.9,
                ask=100.0,
                timestamp_ms=3_000,
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

    def test_tactical_cycle_moves_stop_to_breakeven_after_one_r(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            store = TradingStore(Path(directory) / "cointrading.sqlite")
            cycle_id = store.insert_strategy_cycle(
                strategy="tactical_breakout_retest_long",
                execution_mode="paper_tactical",
                symbol="ETHUSDC",
                side="long",
                status="OPEN",
                quantity=0.25,
                entry_price=100.0,
                target_price=110.0,
                stop_price=98.0,
                entry_order_type="MARKET",
                take_profit_bps=1000.0,
                stop_loss_bps=200.0,
                max_hold_seconds=14_400,
                maker_one_way_bps=0.0,
                taker_one_way_bps=5.0,
                entry_deadline_ms=1_000,
                dry_run=True,
                opened_ms=1_000,
                max_hold_deadline_ms=15_000,
                timestamp_ms=1_000,
            )
            cycle = store.active_strategy_cycle("tactical_breakout_retest_long", "ETHUSDC")
            assert cycle is not None

            result = manage_strategy_cycle(
                FakeStrategyClient(),
                store,
                cycle,
                TradingConfig(taker_fee_rate=0.0005),
                bid=102.0,
                ask=102.1,
                timestamp_ms=2_000,
            )

            self.assertEqual(result.action, "exit_waiting")
            updated = store.active_strategy_cycle("tactical_breakout_retest_long", "ETHUSDC")
            assert updated is not None
            self.assertGreater(float(updated["stop_price"]), 100.0)
            self.assertEqual(int(updated["id"]), cycle_id)

    def test_dry_run_strategy_entry_does_not_call_exchange_new_order(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            store = TradingStore(Path(directory) / "cointrading.sqlite")
            config = TradingConfig(
                strategy_order_notional=25,
                max_single_order_notional=25,
                strategy_lifecycle_enabled=True,
                runtime_risk_enabled=False,
                strategy_gate_enabled=False,
            )

            start = start_strategy_cycle_from_setup(
                FailingStrategyClient(),
                store,
                _setup(),
                config,
                symbol="ETHUSDC",
                bid=99.9,
                ask=100.0,
                timestamp_ms=1_000,
            )

            self.assertEqual(start.action, "entry_submitted")
            self.assertIsNotNone(store.active_strategy_cycle("trend_follow", "ETHUSDC"))

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

    def test_live_strategy_entry_requires_exact_paper_approved_exit_profile(self) -> None:
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
            )

            result = start_strategy_cycle_from_setup(
                client,
                store,
                _setup(),
                config,
                symbol="ETHUSDC",
                bid=99.9,
                ask=100.0,
                timestamp_ms=1_000,
            )

            self.assertEqual(result.action, "blocked")
            self.assertIn("live requires exact paper approval", result.detail)
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
            _insert_strategy_evaluation(store, config)

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
            protective_intent = client.new_order_intents[-1]
            self.assertEqual(protective_intent.order_type, "STOP_MARKET")
            self.assertTrue(protective_intent.reduce_only)
            self.assertIsNotNone(protective_intent.stop_price)
            self.assertEqual(protective_intent.working_type, "MARK_PRICE")
            self.assertIsNotNone(cycle["exit_order_id"])
            protective_order_id = int(cycle["exit_order_id"])
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
            self.assertEqual(client.orders[protective_order_id]["status"], "CANCELED")
            self.assertTrue(client.new_order_intents[-1].reduce_only)
            self.assertEqual(client.new_order_intents[-1].order_type, "MARKET")
            self.assertIsNone(store.active_strategy_cycle("trend_follow", "ETHUSDC"))
            recent = store.recent_strategy_cycles(limit=1)[0]
            self.assertEqual(recent["status"], "CLOSED")
            self.assertGreater(float(recent["realized_pnl"]), 0)


if __name__ == "__main__":
    unittest.main()
