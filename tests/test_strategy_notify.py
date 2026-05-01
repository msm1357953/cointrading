import unittest

from cointrading.config import TradingConfig
from cointrading.strategy_notify import (
    StrategyNotifyState,
    apply_strategy_notification_state,
    strategy_notification_decision,
    strategy_notification_text,
)


def _row(decision="BLOCKED", execution_mode="maker_post_only", **overrides):
    row = {
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
    row.update(overrides)
    return row


def _cycle():
    return {
        "symbol": "BTCUSDC",
        "strategy": "range_reversion",
        "side": "long",
        "status": "OPEN",
        "reason": "entry filled",
        "realized_pnl": None,
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

        self.assertIn("전략유형별", text)
        self.assertIn("초단기 모멘텀", text)
        self.assertIn("주문방식별", text)
        self.assertIn("시장가/테이커 추세", text)
        self.assertIn("승인 0개", text)
        self.assertIn("실제 주문/포지션 보고가 아닙니다", text)

    def test_notification_text_groups_approved_parameter_variants(self) -> None:
        text = strategy_notification_text(
            [
                _row(decision="APPROVED", take_profit_bps=20.0, max_hold_seconds=300),
                _row(decision="APPROVED", take_profit_bps=16.0, max_hold_seconds=180),
                _row(decision="APPROVED", symbol="XRPUSDC", side="short", regime="aligned_short"),
            ],
            reason="수동",
            config=TradingConfig(
                dry_run=True,
                live_trading_enabled=False,
                live_strategy_lifecycle_enabled=False,
            ),
            active_strategy_cycles=[_cycle()],
        )

        self.assertIn("안전상태: dry-run ON, live OFF, 전략 live OFF", text)
        self.assertIn(
            "BTCUSDC 롱 / 전략=메이커 스캘핑 / 조건=상승 정렬 / 주문=지정가 메이커 / 파라미터 2개",
            text,
        )
        self.assertIn(
            "XRPUSDC 숏 / 전략=메이커 스캘핑 / 조건=하락 정렬 / 주문=지정가 메이커",
            text,
        )
        self.assertIn("현재 전략 상태머신", text)
        self.assertIn("레인지 평균회귀 BTCUSDC 롱 보유 중", text)


if __name__ == "__main__":
    unittest.main()
