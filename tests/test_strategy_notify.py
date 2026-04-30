import unittest

from cointrading.strategy_notify import (
    StrategyNotifyState,
    apply_strategy_notification_state,
    strategy_notification_decision,
    strategy_notification_text,
)


def _row(decision="BLOCKED", execution_mode="maker_post_only"):
    return {
        "evaluated_ms": 1_000,
        "source": "signal_grid",
        "execution_mode": execution_mode,
        "symbol": "BTCUSDC",
        "regime": "aligned_long",
        "side": "long",
        "take_profit_bps": 12.0,
        "stop_loss_bps": 4.0,
        "max_hold_seconds": 60,
        "sample_count": 30,
        "win_rate": 0.60,
        "avg_pnl_bps": 1.2,
        "sum_pnl_bps": 36.0,
        "decision": decision,
        "reason": "test",
    }


class StrategyNotifyTests(unittest.TestCase):
    def test_notification_sends_on_decision_change_once(self) -> None:
        rows = [_row()]
        state = StrategyNotifyState()

        should_send, reason, signature = strategy_notification_decision(
            rows,
            state,
            periodic_minutes=360,
            timestamp_ms=1_000,
        )
        self.assertTrue(should_send)
        self.assertEqual(reason, "전략 판정 변화")

        apply_strategy_notification_state(
            state,
            signature=signature,
            reason=reason,
            timestamp_ms=1_000,
        )
        should_send, _, _ = strategy_notification_decision(
            rows,
            state,
            periodic_minutes=360,
            timestamp_ms=2_000,
        )
        self.assertFalse(should_send)

        changed = [_row(decision="APPROVED")]
        should_send, reason, _ = strategy_notification_decision(
            changed,
            state,
            periodic_minutes=360,
            timestamp_ms=3_000,
        )
        self.assertTrue(should_send)
        self.assertEqual(reason, "전략 판정 변화")

    def test_notification_sends_periodic_report(self) -> None:
        rows = [_row()]
        state = StrategyNotifyState(last_signature="not-empty", last_periodic_ms=1_000)

        _, _, signature = strategy_notification_decision(
            rows,
            StrategyNotifyState(),
            periodic_minutes=360,
            timestamp_ms=1_000,
        )
        state.last_signature = signature
        should_send, reason, _ = strategy_notification_decision(
            rows,
            state,
            periodic_minutes=360,
            timestamp_ms=1_000 + 360 * 60_000,
        )

        self.assertTrue(should_send)
        self.assertEqual(reason, "주기 보고")

    def test_notification_text_includes_execution_mode(self) -> None:
        text = strategy_notification_text([_row(execution_mode="taker_momentum")], reason="수동")

        self.assertIn("taker_momentum", text)
        self.assertIn("승인 0개", text)


if __name__ == "__main__":
    unittest.main()
