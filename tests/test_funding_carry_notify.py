import tempfile
import unittest
from pathlib import Path

from cointrading.funding_carry_notify import (
    LIVE_READY_MIN_CLOSED,
    LIVE_READY_MIN_WIN_RATE,
    evaluate_live_ready,
)
from cointrading.funding_lifecycle import STATUS_CLOSED, STATUS_STOPPED, STRATEGY_NAME
from cointrading.storage import TradingStore, now_ms


def _insert_closed_cycle(storage: TradingStore, *, status: str, realized_pnl: float) -> None:
    cycle_id = storage.insert_strategy_cycle(
        strategy=STRATEGY_NAME,
        execution_mode="taker_market",
        symbol="SOLUSDC",
        side="long",
        status=status,
        quantity=1.0,
        entry_price=100.0,
        target_price=105.0,
        stop_price=97.0,
        entry_order_type="MARKET",
        take_profit_bps=0.0,
        stop_loss_bps=300.0,
        max_hold_seconds=86_400,
        maker_one_way_bps=2.0,
        taker_one_way_bps=4.0,
        entry_deadline_ms=now_ms(),
        dry_run=True,
        reason="test",
    )
    storage.update_strategy_cycle(
        cycle_id,
        status=status,
        closed_ms=now_ms(),
        realized_pnl=realized_pnl,
    )


class EvaluateLiveReadyTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmpdir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tmpdir.name) / "test.sqlite"
        self.storage = TradingStore(self.db_path)

    def tearDown(self) -> None:
        self.tmpdir.cleanup()

    def test_not_ready_when_no_cycles(self) -> None:
        status = evaluate_live_ready(self.storage)
        self.assertFalse(status.ready)
        self.assertEqual(status.closed_n, 0)
        self.assertTrue(any("closed cycles" in r for r in status.reasons))

    def test_not_ready_when_too_few_cycles(self) -> None:
        for _ in range(LIVE_READY_MIN_CLOSED - 1):
            _insert_closed_cycle(self.storage, status=STATUS_CLOSED, realized_pnl=1.0)
        status = evaluate_live_ready(self.storage)
        self.assertFalse(status.ready)
        self.assertTrue(any("closed cycles" in r for r in status.reasons))

    def test_not_ready_when_negative_pnl(self) -> None:
        for _ in range(LIVE_READY_MIN_CLOSED):
            _insert_closed_cycle(self.storage, status=STATUS_STOPPED, realized_pnl=-1.0)
        status = evaluate_live_ready(self.storage)
        self.assertFalse(status.ready)
        self.assertTrue(any("sum PnL" in r for r in status.reasons))

    def test_ready_when_all_conditions_met(self) -> None:
        # 5 cycles: 3 wins +2.0, 2 losses -1.0 -> sum = +4.0, WR = 60%
        for _ in range(3):
            _insert_closed_cycle(self.storage, status=STATUS_CLOSED, realized_pnl=2.0)
        for _ in range(2):
            _insert_closed_cycle(self.storage, status=STATUS_STOPPED, realized_pnl=-1.0)
        status = evaluate_live_ready(self.storage)
        self.assertTrue(status.ready, msg=f"reasons={status.reasons}")
        self.assertEqual(status.closed_n, 5)
        self.assertEqual(status.win_n, 3)
        self.assertEqual(status.loss_n, 2)
        self.assertAlmostEqual(status.sum_pnl, 4.0)
        self.assertAlmostEqual(status.win_rate, 0.6)

    def test_not_ready_when_low_win_rate(self) -> None:
        # 5 cycles, 1 win, 4 losses, but big win = positive sum, low WR
        _insert_closed_cycle(self.storage, status=STATUS_CLOSED, realized_pnl=10.0)
        for _ in range(4):
            _insert_closed_cycle(self.storage, status=STATUS_STOPPED, realized_pnl=-1.0)
        status = evaluate_live_ready(self.storage)
        self.assertFalse(status.ready)
        self.assertTrue(any("win rate" in r for r in status.reasons))


if __name__ == "__main__":
    unittest.main()
