from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from cointrading.config import TradingConfig
from cointrading.storage import TradingStore
from cointrading.tactical_paper import start_tactical_paper_cycle_from_signal
from cointrading.tactical_radar import RADAR_NEAR, RADAR_READY, TacticalRadarSignal


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
