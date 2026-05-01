import tempfile
import unittest
from pathlib import Path

from cointrading.config import TradingConfig
from cointrading.scalp_lifecycle import start_cycle_from_signal
from cointrading.scalping import ScalpSignal
from cointrading.storage import TradingStore
from cointrading.strategy_eval import (
    MAKER_POST_ONLY,
    TAKER_TREND,
    TAKER_MOMENTUM,
    evaluate_and_store_strategy,
    strategy_gate_decision,
)


class FakeOrderClient:
    def new_order(self, intent):
        return {"dryRun": True, "params": {"symbol": intent.symbol, "side": intent.side}}


def _signal(symbol="BTCUSDC", side="long", regime="aligned_long", horizon=5.0):
    return ScalpSignal(
        symbol=symbol,
        side=side,
        reason="test signal",
        regime=regime,
        trade_allowed=True,
        mid_price=100.0,
        spread_bps=1.0,
        imbalance=0.5,
        momentum_bps=5.0,
        realized_vol_bps=3.0,
        maker_roundtrip_bps=0.0,
        taker_roundtrip_bps=7.2,
        edge_after_maker_bps=horizon,
        book_bid_notional=100_000.0,
        book_ask_notional=100_000.0,
        book_depth_notional=200_000.0,
        bnb_fee_discount_enabled=True,
        bnb_fee_discount_active=True,
        latest_funding_rate=0.0,
    )


class StrategyEvaluationTests(unittest.TestCase):
    def test_strategy_evaluation_blocks_negative_cycle_group(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            store = TradingStore(Path(directory) / "cointrading.sqlite")
            config = TradingConfig(strategy_early_block_samples=2, strategy_min_samples=3)
            for index, pnl in enumerate([-0.03, -0.02]):
                signal_id = store.insert_signal(_signal(), timestamp_ms=1_000 + index)
                cycle_id = store.insert_scalp_cycle(
                    symbol="BTCUSDC",
                    side="long",
                    status="STOPPED",
                    quantity=1.0,
                    entry_price=100.0,
                    target_price=100.03,
                    stop_price=99.94,
                    maker_one_way_bps=0.0,
                    taker_one_way_bps=3.6,
                    entry_deadline_ms=2_000,
                    entry_signal_id=signal_id,
                    timestamp_ms=2_000 + index,
                )
                store.update_scalp_cycle(cycle_id, realized_pnl=pnl, timestamp_ms=3_000 + index)

            rows = evaluate_and_store_strategy(store, config)
            cycle_rows = [row for row in rows if row["source"] == "cycles"]

            self.assertEqual(cycle_rows[0]["decision"], "BLOCKED")
            self.assertEqual(cycle_rows[0]["execution_mode"], MAKER_POST_ONLY)
            self.assertIn("평균손익", cycle_rows[0]["reason"])
            self.assertGreater(store.summary_counts()["strategy_evaluations"], 0)

    def test_strategy_evaluation_includes_macro_strategy_cycles(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            store = TradingStore(Path(directory) / "cointrading.sqlite")
            config = TradingConfig(strategy_min_samples=2, strategy_min_win_rate=0.50)
            for index, pnl in enumerate([0.10, 0.12]):
                cycle_id = store.insert_strategy_cycle(
                    strategy="trend_follow",
                    execution_mode=TAKER_TREND,
                    symbol="ETHUSDC",
                    side="long",
                    status="CLOSED",
                    quantity=0.25,
                    entry_price=100.0,
                    target_price=101.0,
                    stop_price=99.5,
                    entry_order_type="MARKET",
                    take_profit_bps=80,
                    stop_loss_bps=40,
                    max_hold_seconds=14_400,
                    maker_one_way_bps=0.0,
                    taker_one_way_bps=3.6,
                    entry_deadline_ms=2_000,
                    dry_run=True,
                    timestamp_ms=2_000 + index,
                )
                store.update_strategy_cycle(
                    cycle_id,
                    status="CLOSED",
                    realized_pnl=pnl,
                    timestamp_ms=3_000 + index,
                )

            rows = evaluate_and_store_strategy(store, config)
            strategy_rows = [row for row in rows if row["source"] == "strategy_cycles"]

            self.assertEqual(strategy_rows[0]["execution_mode"], TAKER_TREND)
            self.assertEqual(strategy_rows[0]["regime"], "trend_follow")
            self.assertEqual(strategy_rows[0]["decision"], "APPROVED")

    def test_strategy_gate_requires_approved_evaluation(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            store = TradingStore(Path(directory) / "cointrading.sqlite")
            config = TradingConfig(strategy_gate_enabled=True)
            signal = _signal()

            blocked = strategy_gate_decision(store, signal, config)
            self.assertFalse(blocked.allowed)
            self.assertIn("no evaluation", blocked.reason)

            store.insert_strategy_evaluations(
                [
                    {
                        "source": "cycles",
                        "execution_mode": MAKER_POST_ONLY,
                        "symbol": "BTCUSDC",
                        "regime": "aligned_long",
                        "side": "long",
                        "take_profit_bps": config.scalp_take_profit_bps,
                        "stop_loss_bps": config.scalp_stop_loss_bps,
                        "max_hold_seconds": int(config.scalp_max_hold_seconds),
                        "sample_count": 30,
                        "win_count": 20,
                        "loss_count": 10,
                        "win_rate": 20 / 30,
                        "avg_pnl_bps": 1.2,
                        "sum_pnl_bps": 36.0,
                        "avg_win_bps": 4.0,
                        "avg_loss_bps": -2.0,
                        "decision": "APPROVED",
                        "reason": "평가 기준 통과",
                    }
                ],
                timestamp_ms=10_000,
            )
            allowed = strategy_gate_decision(store, signal, config)
            self.assertTrue(allowed.allowed)

    def test_strategy_gate_can_select_approved_candidate_parameters(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            store = TradingStore(Path(directory) / "cointrading.sqlite")
            config = TradingConfig(
                strategy_gate_enabled=True,
                scalp_take_profit_bps=3,
                scalp_stop_loss_bps=6,
                scalp_max_hold_seconds=180,
            )
            signal = _signal(symbol="DOGEUSDC", side="short", regime="aligned_short")
            store.insert_strategy_evaluations(
                [
                    {
                        "source": "signal_grid",
                        "execution_mode": MAKER_POST_ONLY,
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

            allowed = strategy_gate_decision(store, signal, config)

            self.assertTrue(allowed.allowed)
            self.assertEqual(allowed.take_profit_bps, 20.0)
            self.assertEqual(allowed.stop_loss_bps, 4.0)
            self.assertEqual(allowed.max_hold_seconds, 300)

    def test_signal_grid_can_approve_positive_candidate(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            store = TradingStore(Path(directory) / "cointrading.sqlite")
            config = TradingConfig(strategy_min_samples=2, strategy_min_win_rate=0.50)
            for index in range(2):
                signal_id = store.insert_signal(_signal(), timestamp_ms=1_000 + index)
                store.update_signal_scores(
                    signal_id,
                    {
                        "horizon_1m_bps": 6.0,
                        "horizon_3m_bps": 6.0,
                        "horizon_5m_bps": 6.0,
                    },
                )

            rows = evaluate_and_store_strategy(store, config)
            approved = [
                row
                for row in rows
                if row["source"] == "signal_grid"
                and row["execution_mode"] == MAKER_POST_ONLY
                and row["take_profit_bps"] == 3.0
                and row["stop_loss_bps"] == 4.0
                and row["max_hold_seconds"] == 60
            ]

            self.assertEqual(approved[0]["decision"], "APPROVED")

    def test_signal_grid_can_approve_asymmetric_payoff_below_half_win_rate(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            store = TradingStore(Path(directory) / "cointrading.sqlite")
            config = TradingConfig(strategy_min_samples=7, strategy_min_win_rate=0.40)
            horizons = [20.0, 20.0, 20.0, -4.0, -4.0, -4.0, -4.0]
            for index, horizon in enumerate(horizons):
                signal_id = store.insert_signal(_signal(horizon=horizon), timestamp_ms=1_000 + index)
                store.update_signal_scores(
                    signal_id,
                    {
                        "horizon_1m_bps": horizon,
                        "horizon_3m_bps": horizon,
                        "horizon_5m_bps": horizon,
                    },
                )

            rows = evaluate_and_store_strategy(store, config)
            approved = [
                row
                for row in rows
                if row["source"] == "signal_grid"
                and row["execution_mode"] == MAKER_POST_ONLY
                and row["take_profit_bps"] == 20.0
                and row["stop_loss_bps"] == 4.0
                and row["max_hold_seconds"] == 60
            ]

            self.assertEqual(approved[0]["decision"], "APPROVED")
            self.assertLess(approved[0]["win_rate"], 0.50)
            self.assertGreater(approved[0]["avg_pnl_bps"], 0.0)

    def test_signal_grid_evaluates_taker_modes_with_extra_cost(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            store = TradingStore(Path(directory) / "cointrading.sqlite")
            config = TradingConfig(
                strategy_min_samples=2,
                strategy_min_win_rate=0.50,
                strategy_taker_slippage_bps=1.0,
            )
            for index in range(2):
                signal_id = store.insert_signal(_signal(horizon=20.0), timestamp_ms=1_000 + index)
                store.update_signal_scores(
                    signal_id,
                    {
                        "horizon_1m_bps": 20.0,
                        "horizon_3m_bps": 20.0,
                        "horizon_5m_bps": 20.0,
                    },
                )

            rows = evaluate_and_store_strategy(store, config)
            taker_rows = [
                row
                for row in rows
                if row["source"] == "signal_grid"
                and row["execution_mode"] == TAKER_MOMENTUM
                and row["take_profit_bps"] == 12.0
                and row["stop_loss_bps"] == 4.0
                and row["max_hold_seconds"] == 60
            ]

            self.assertEqual(taker_rows[0]["decision"], "APPROVED")
            self.assertLess(taker_rows[0]["avg_pnl_bps"], 12.0)

    def test_start_cycle_records_strategy_gate_block(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            store = TradingStore(Path(directory) / "cointrading.sqlite")
            config = TradingConfig(strategy_gate_enabled=True)
            signal = _signal()
            signal_id = store.insert_signal(signal, timestamp_ms=1_000)

            result = start_cycle_from_signal(
                FakeOrderClient(),
                store,
                signal,
                config,
                signal_id=signal_id,
                timestamp_ms=2_000,
            )

            self.assertEqual(result.action, "blocked")
            self.assertIn("strategy gate", result.detail)
            self.assertEqual(store.recent_orders()[0]["status"], "BLOCKED")


if __name__ == "__main__":
    unittest.main()
