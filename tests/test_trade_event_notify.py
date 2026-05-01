import tempfile
import unittest
from pathlib import Path

from cointrading.storage import TradingStore
from cointrading.trade_event_notify import (
    TradeEventNotifyState,
    apply_trade_event_notification_state,
    trade_event_notification_decision,
    trade_event_notification_text,
    trade_summary_text,
)


def _insert_strategy_cycle(store: TradingStore, *, timestamp_ms: int = 1_000) -> int:
    return store.insert_strategy_cycle(
        strategy="trend_follow",
        execution_mode="taker_trend",
        symbol="ETHUSDC",
        side="long",
        status="ENTRY_SUBMITTED",
        reason="entry submitted; test setup",
        quantity=0.25,
        entry_price=100.0,
        target_price=101.0,
        stop_price=99.5,
        entry_order_type="MARKET",
        take_profit_bps=100,
        stop_loss_bps=50,
        max_hold_seconds=3_600,
        maker_one_way_bps=0.0,
        taker_one_way_bps=4.0,
        entry_deadline_ms=timestamp_ms + 60_000,
        dry_run=True,
        last_mid_price=100.0,
        timestamp_ms=timestamp_ms,
    )


class TradeEventNotifyTests(unittest.TestCase):
    def test_entry_event_is_reported_once(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            store = TradingStore(Path(directory) / "cointrading.sqlite")
            _insert_strategy_cycle(store)
            state = TradeEventNotifyState(initialized_ms=0)

            events, include_summary = trade_event_notification_decision(
                store,
                state,
                summary_interval_minutes=0,
                timestamp_ms=2_000,
            )
            self.assertFalse(include_summary)
            self.assertEqual([event.event_type for event in events], ["entry_submitted"])

            text = trade_event_notification_text(events, store, include_summary=False, timestamp_ms=2_000)
            self.assertIn("거래 이벤트", text)
            self.assertIn("진입 시도", text)
            self.assertIn("추세 추종", text)

            apply_trade_event_notification_state(state, events, summary_sent=False, timestamp_ms=2_000)
            events, _ = trade_event_notification_decision(
                store,
                state,
                summary_interval_minutes=0,
                timestamp_ms=3_000,
            )
            self.assertEqual(events, [])

    def test_exit_events_use_human_take_profit_and_stop_labels(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            store = TradingStore(Path(directory) / "cointrading.sqlite")
            cycle_id = _insert_strategy_cycle(store)
            store.update_strategy_cycle(
                cycle_id,
                status="CLOSED",
                reason="take_profit",
                opened_ms=2_000,
                closed_ms=3_000,
                last_mid_price=101.0,
                realized_pnl=0.25,
                timestamp_ms=3_000,
            )
            state = TradeEventNotifyState(initialized_ms=0, notified_keys=(f"strategy:{cycle_id}:entry_submitted",))

            events, _ = trade_event_notification_decision(
                store,
                state,
                summary_interval_minutes=0,
                timestamp_ms=4_000,
            )

            self.assertEqual([event.event_type for event in events], ["take_profit"])
            text = trade_event_notification_text(events, store, include_summary=False, timestamp_ms=4_000)
            self.assertIn("익절", text)
            self.assertIn("실현손익: +0.250000 USDC", text)

    def test_summary_includes_active_current_price_and_unrealized_pnl(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            store = TradingStore(Path(directory) / "cointrading.sqlite")
            cycle_id = _insert_strategy_cycle(store)
            store.update_strategy_cycle(
                cycle_id,
                status="OPEN",
                reason="strategy exit waiting",
                opened_ms=2_000,
                last_mid_price=102.0,
                timestamp_ms=3_000,
            )

            text = trade_summary_text(store, timestamp_ms=4_000)

            self.assertIn("상황 보고", text)
            self.assertIn("현재 진행 중: 1개", text)
            self.assertIn("진입 100", text)
            self.assertIn("현재 102", text)
            self.assertIn("미실현 +0.500000 USDC", text)

    def test_startup_floor_keeps_old_backfill_suppressed_after_state_exists(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            store = TradingStore(Path(directory) / "cointrading.sqlite")
            _insert_strategy_cycle(store, timestamp_ms=1_000)
            state = TradeEventNotifyState(
                initialized_ms=2_000_000,
                notified_keys=("strategy:999:entry_submitted",),
            )

            events, _ = trade_event_notification_decision(
                store,
                state,
                summary_interval_minutes=0,
                timestamp_ms=2_100_000,
            )

            self.assertEqual(events, [])


if __name__ == "__main__":
    unittest.main()
