import tempfile
import unittest
from dataclasses import replace
from pathlib import Path

from cointrading.config import TradingConfig
from cointrading.orderflow_guard import (
    OrderflowWindow,
    load_latest_snapshot,
    orderflow_guard_text,
    write_snapshot,
)


def _cfg(**overrides) -> TradingConfig:
    base = replace(
        TradingConfig(),
        orderflow_guard_enabled=True,
        orderflow_guard_symbol="BTCUSDC",
        orderflow_guard_window_seconds=5.0,
        orderflow_guard_stale_seconds=10.0,
        orderflow_guard_spread_danger_bps=1.5,
        orderflow_guard_depth_drop_caution=0.35,
        orderflow_guard_depth_drop_danger=0.50,
        orderflow_guard_imbalance_caution=0.80,
        orderflow_guard_imbalance_danger=0.65,
        orderflow_guard_taker_ratio_caution=0.60,
        orderflow_guard_taker_ratio_danger=0.70,
        orderflow_guard_min_trade_notional_usdc=5000.0,
        orderflow_guard_velocity_caution_bps=4.0,
        orderflow_guard_velocity_danger_bps=8.0,
    )
    return replace(base, **overrides)


class OrderflowGuardTests(unittest.TestCase):
    def test_normal_snapshot_when_book_and_flow_are_balanced(self) -> None:
        cfg = _cfg()
        window = OrderflowWindow(window_seconds=5.0)
        for i in range(5):
            ts = 1_000_000 + i * 1000
            window.add_depth(
                event_ms=ts,
                bids=[(100.0, 100.0), (99.95, 100.0), (99.91, 100.0)],
                asks=[(100.01, 100.0), (100.06, 100.0), (100.1, 100.0)],
            )
            window.add_trade(event_ms=ts, price=100.0, quantity=10.0, buyer_is_maker=False)
            window.add_trade(event_ms=ts, price=100.0, quantity=10.0, buyer_is_maker=True)

        snapshot = window.to_snapshot(symbol="BTCUSDC", config=cfg, now_ms=1_005_000)

        self.assertEqual(snapshot["status"], "NORMAL")
        self.assertEqual(snapshot["long_status"], "NORMAL")
        self.assertEqual(snapshot["short_status"], "NORMAL")
        self.assertGreater(snapshot["bid_depth_010"], 0)

    def test_long_danger_when_bid_depth_collapses_and_sell_flow_dominates(self) -> None:
        cfg = _cfg()
        window = OrderflowWindow(window_seconds=5.0)
        window.add_depth(
            event_ms=1_000_000,
            bids=[(100.0, 200.0), (99.95, 200.0)],
            asks=[(100.01, 200.0), (100.06, 200.0)],
        )
        window.add_depth(
            event_ms=1_004_000,
            bids=[(100.0, 10.0), (99.95, 10.0)],
            asks=[(100.01, 200.0), (100.06, 200.0)],
        )
        window.add_trade(event_ms=1_004_100, price=100.0, quantity=80.0, buyer_is_maker=True)

        snapshot = window.to_snapshot(symbol="BTCUSDC", config=cfg, now_ms=1_005_000)

        self.assertEqual(snapshot["long_status"], "DANGER")
        self.assertIn(snapshot["short_status"], {"NORMAL", "CAUTION"})
        self.assertIn("롱 DANGER", snapshot["reason"])

    def test_loader_marks_stale_snapshot_as_danger_for_both_sides(self) -> None:
        cfg = _cfg()
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "orderflow.json"
            write_snapshot(
                path,
                {
                    "symbol": "BTCUSDC",
                    "updated_ms": 1_000_000,
                    "status": "NORMAL",
                    "long_status": "NORMAL",
                    "short_status": "NORMAL",
                    "reason": "정상",
                    "long_reason": "정상",
                    "short_reason": "정상",
                },
            )
            snap = load_latest_snapshot(cfg, path=path, now_ms=1_020_000)

        self.assertEqual(snap.status, "STALE")
        self.assertEqual(snap.long_status, "DANGER")
        self.assertEqual(snap.short_status, "DANGER")

    def test_text_command_handles_missing_file(self) -> None:
        cfg = _cfg()
        with tempfile.TemporaryDirectory() as td:
            text = orderflow_guard_text(config=cfg, path=Path(td) / "missing.json")

        self.assertIn("호가창/orderflow 상태", text)
        self.assertIn("센서 파일 없음", text)


if __name__ == "__main__":
    unittest.main()
