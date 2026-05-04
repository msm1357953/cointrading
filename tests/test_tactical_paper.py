from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from cointrading.config import TradingConfig
from cointrading.storage import TradingStore
from cointrading.tactical_paper import (
    run_tactical_paper_step,
    start_tactical_live_cycle_from_signal,
    start_tactical_paper_cycle_from_signal,
)
from cointrading.tactical_radar import RADAR_NEAR, RADAR_READY, TacticalRadarSignal


class FakeLiveTacticalClient:
    def __init__(self) -> None:
        self.orders = []

    def _signed_request(self, method, path, params=None):
        if path == "/fapi/v1/openOrders":
            return []
        raise AssertionError(path)

    def account_info(self):
        return {"positions": []}

    def exchange_info(self, symbol=None):
        symbol = symbol or "SOLUSDC"
        return {
            "symbols": [
                {
                    "symbol": symbol,
                    "filters": [
                        {"filterType": "PRICE_FILTER", "tickSize": "0.01"},
                        {"filterType": "LOT_SIZE", "minQty": "0.01", "maxQty": "100000", "stepSize": "0.01"},
                        {"filterType": "MIN_NOTIONAL", "notional": "5"},
                    ],
                }
            ]
        }

    def book_ticker(self, symbol):
        return {"bidPrice": "99.95", "askPrice": "100.05"}

    def new_order(self, intent):
        self.orders.append(intent)
        return {
            "orderId": 101,
            "symbol": intent.symbol,
            "side": intent.side,
            "status": "NEW",
            "executedQty": "0",
            "avgPrice": "0",
        }

    def klines(self, symbol: str, interval: str, limit: int = 120):
        return []


class TacticalPaperTests(unittest.TestCase):
    def test_ready_signal_starts_dry_run_strategy_cycle(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = TradingStore(Path(tmp) / "trading.db")
            config = TradingConfig(
                dry_run=False,
                equity_asset="USDC",
                taker_fee_rate=0.0004,
                maker_fee_rate=0.0,
            )
            signal = _signal(decision=RADAR_READY)

            result = start_tactical_paper_cycle_from_signal(
                store,
                signal,
                config,
                bid=99.95,
                ask=100.05,
                notional=80.0,
                timestamp_ms=1_000,
            )

            self.assertEqual(result.action, "entry_submitted")
            cycle = store.recent_strategy_cycles(limit=1)[0]
            self.assertEqual(cycle["strategy"], "tactical_pullback_long")
            self.assertEqual(cycle["execution_mode"], "paper_tactical")
            self.assertEqual(cycle["symbol"], "SOLUSDC")
            self.assertEqual(cycle["side"], "long")
            self.assertEqual(cycle["status"], "ENTRY_SUBMITTED")
            self.assertEqual(cycle["dry_run"], 1)
            self.assertGreater(float(cycle["take_profit_bps"]), 0.0)
            self.assertGreater(float(cycle["stop_loss_bps"]), 0.0)
            order = store.recent_orders(limit=1)[0]
            self.assertEqual(order["status"], "DRY_RUN")
            self.assertEqual(order["dry_run"], 1)

    def test_near_signal_does_not_start_cycle(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = TradingStore(Path(tmp) / "trading.db")
            result = start_tactical_paper_cycle_from_signal(
                store,
                _signal(decision=RADAR_NEAR),
                TradingConfig(),
                bid=99.95,
                ask=100.05,
                notional=80.0,
                timestamp_ms=1_000,
            )

            self.assertEqual(result.action, "skip")
            self.assertEqual(store.recent_strategy_cycles(limit=1), [])

    def test_ready_signal_starts_live_cycle_when_flags_are_enabled(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = TradingStore(Path(tmp) / "trading.db")
            client = FakeLiveTacticalClient()
            config = TradingConfig(
                dry_run=False,
                live_trading_enabled=True,
                live_strategy_lifecycle_enabled=True,
                live_one_shot_required=False,
                equity_asset="USDC",
                taker_fee_rate=0.0004,
                maker_fee_rate=0.0,
                max_single_order_notional=80.0,
                tactical_live_min_closed_cycles=0,
            )

            result = start_tactical_live_cycle_from_signal(
                client,
                store,
                _signal(decision=RADAR_READY),
                config,
                notional=80.0,
                timestamp_ms=1_000,
            )

            self.assertEqual(result.action, "entry_submitted")
            self.assertEqual(len(client.orders), 1)
            cycle = store.recent_strategy_cycles(limit=1)[0]
            self.assertEqual(cycle["strategy"], "tactical_pullback_long")
            self.assertEqual(cycle["execution_mode"], "live_tactical")
            self.assertEqual(cycle["status"], "ENTRY_SUBMITTED")
            self.assertEqual(cycle["dry_run"], 0)
            order = store.recent_orders(limit=1)[0]
            self.assertEqual(order["dry_run"], 0)

    def test_live_scenario_allowlist_blocks_unapproved_ready_signal(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = TradingStore(Path(tmp) / "trading.db")
            client = FakeLiveTacticalClient()
            config = TradingConfig(
                dry_run=False,
                live_trading_enabled=True,
                live_strategy_lifecycle_enabled=True,
                live_one_shot_required=False,
                tactical_live_scenarios=("key_level_breakout_long",),
            )

            result = start_tactical_live_cycle_from_signal(
                client,
                store,
                _signal(decision=RADAR_READY),
                config,
                notional=80.0,
                timestamp_ms=1_000,
            )

            self.assertEqual(result.action, "blocked")
            self.assertIn("live 허용 전술", result.detail)
            self.assertEqual(client.orders, [])
            self.assertEqual(store.recent_strategy_cycles(limit=1), [])

    def test_live_cycle_requires_paper_evidence_by_default(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = TradingStore(Path(tmp) / "trading.db")
            client = FakeLiveTacticalClient()
            config = TradingConfig(
                dry_run=False,
                live_trading_enabled=True,
                live_strategy_lifecycle_enabled=True,
                live_one_shot_required=False,
            )

            result = start_tactical_live_cycle_from_signal(
                client,
                store,
                _signal(decision=RADAR_READY),
                config,
                notional=80.0,
                timestamp_ms=1_000,
            )

            self.assertEqual(result.action, "blocked")
            self.assertIn("paper 근거 부족", result.detail)
            self.assertEqual(client.orders, [])
            self.assertEqual(store.recent_strategy_cycles(limit=1), [])

    def test_live_cycle_can_use_early_positive_paper_evidence_when_enabled(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = TradingStore(Path(tmp) / "trading.db")
            for index in range(5):
                cycle_id = store.insert_strategy_cycle(
                    strategy="tactical_pullback_long",
                    execution_mode="paper_tactical",
                    symbol="SOLUSDC",
                    side="long",
                    status="OPEN",
                    quantity=1.0,
                    entry_price=100.0,
                    target_price=101.0,
                    stop_price=99.0,
                    entry_order_type="MARKET",
                    take_profit_bps=100.0,
                    stop_loss_bps=100.0,
                    max_hold_seconds=1800,
                    maker_one_way_bps=0.0,
                    taker_one_way_bps=4.0,
                    entry_deadline_ms=2_000,
                    dry_run=True,
                    timestamp_ms=1_000 + index,
                )
                store.update_strategy_cycle(
                    cycle_id,
                    status="CLOSED",
                    reason="take_profit",
                    realized_pnl=0.10,
                    closed_ms=2_000 + index,
                    timestamp_ms=2_000 + index,
                )
            client = FakeLiveTacticalClient()
            config = TradingConfig(
                dry_run=False,
                live_trading_enabled=True,
                live_strategy_lifecycle_enabled=True,
                live_one_shot_required=False,
                tactical_live_early_evidence_enabled=True,
            )

            result = start_tactical_live_cycle_from_signal(
                client,
                store,
                _signal(decision=RADAR_READY),
                config,
                notional=80.0,
                timestamp_ms=10_000,
            )

            self.assertEqual(result.action, "entry_submitted")
            self.assertEqual(len(client.orders), 1)

    def test_live_cycle_is_not_managed_by_paper_step(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = TradingStore(Path(tmp) / "trading.db")
            store.insert_strategy_cycle(
                strategy="tactical_pullback_long",
                execution_mode="live_tactical",
                symbol="SOLUSDC",
                side="long",
                status="ENTRY_SUBMITTED",
                quantity=1.0,
                entry_price=100.0,
                target_price=101.0,
                stop_price=99.0,
                entry_order_type="MARKET",
                take_profit_bps=100.0,
                stop_loss_bps=100.0,
                max_hold_seconds=1800,
                maker_one_way_bps=0.0,
                taker_one_way_bps=4.0,
                entry_deadline_ms=2_000,
                dry_run=False,
                timestamp_ms=1_000,
            )

            results, _ = run_tactical_paper_step(
                FakeLiveTacticalClient(),
                store,
                TradingConfig(),
                symbols=[],
                timestamp_ms=2_000,
            )

            self.assertEqual(results[0].action, "wait")
            cycle = store.recent_strategy_cycles(limit=1)[0]
            self.assertEqual(cycle["status"], "ENTRY_SUBMITTED")
            self.assertEqual(cycle["dry_run"], 0)


def _signal(*, decision: str) -> TacticalRadarSignal:
    return TacticalRadarSignal(
        symbol="SOLUSDC",
        decision=decision,
        scenario="pullback_long",
        side="long",
        current_price=100.0,
        trigger_price=100.0,
        stop_price=99.2,
        target_price=101.5,
        confidence=0.68,
        reason="상승 추세 눌림 후 재상승 확인",
        detail="test",
        timestamp_ms=1_000,
        change_2h_bps=80.0,
        pullback_bps=35.0,
        volume_ratio=1.2,
        rsi14=52.0,
        bollinger_position=0.5,
    )


if __name__ == "__main__":
    unittest.main()
