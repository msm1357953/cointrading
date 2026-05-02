from __future__ import annotations

from pathlib import Path
import tempfile
import unittest

from cointrading.config import TradingConfig
from cointrading.models import Kline
from cointrading.refined_entry_gate import (
    ENTRY_BLOCKED,
    ENTRY_READY,
    ENTRY_WAIT,
    RefinedEntryNotifyState,
    apply_refined_entry_notification_state,
    evaluate_refined_entry_candidates,
    refined_entry_notification_decision,
    refined_entry_text,
)
from cointrading.strategy_miner import (
    MinedStrategyResult,
    RuleCondition,
    TradeSummary,
    write_strategy_refine_report,
)


class FakeKlineClient:
    def __init__(self, klines: list[Kline]) -> None:
        self._klines = klines

    def klines(self, symbol: str, interval: str, limit: int = 500) -> list[Kline]:
        return self._klines[-limit:]


class RefinedEntryGateTests(unittest.TestCase):
    def test_current_matching_survived_candidate_becomes_ready(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / "strategy_refine_latest.json"
            summary = TradeSummary(
                count=30,
                win_rate=0.6,
                avg_pnl_bps=12.0,
                sum_pnl=1.0,
                profit_factor=2.0,
                payoff_ratio=1.5,
                max_drawdown_pct=0.01,
                max_consecutive_loss=2,
            )
            result = MinedStrategyResult(
                symbol="ETHUSDC",
                interval="1h",
                rule_id="rule",
                action="breakout_long",
                side="long",
                condition=RuleCondition(),
                take_profit_bps=180.0,
                stop_loss_bps=60.0,
                max_hold_bars=24,
                full_summary=summary,
                selected_windows=4,
                positive_test_windows=3,
                test_summary=summary,
                decision="SURVIVED",
                reason="ok",
            )
            write_strategy_refine_report(
                source,
                results=[result],
                symbols=["ETHUSDC"],
                interval="1h",
                start_date="2025-01-01",
                end_date="2026-04-30",
                train_months=6,
                test_months=1,
                source_path=None,
                source_count=1,
            )

            candidates, warnings = evaluate_refined_entry_candidates(
                FakeKlineClient(_klines()),
                config=TradingConfig(supervisor_min_samples=100),
                source_path=source,
                symbols=["ETHUSDC"],
                current_ms=200 * 3_600_000,
            )

        self.assertEqual(warnings, ())
        self.assertEqual(candidates[0].decision, ENTRY_READY)
        text = refined_entry_text(candidates)
        self.assertIn("진입후보 1개", text)
        self.assertIn("ETHUSDC", text)

    def test_matching_candidate_with_weak_payoff_is_blocked(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / "strategy_refine_latest.json"
            weak = TradeSummary(
                count=40,
                win_rate=0.55,
                avg_pnl_bps=12.0,
                sum_pnl=1.0,
                profit_factor=1.8,
                payoff_ratio=1.1,
                max_drawdown_pct=0.01,
                max_consecutive_loss=2,
            )
            result = MinedStrategyResult(
                symbol="ETHUSDC",
                interval="1h",
                rule_id="rule",
                action="breakout_long",
                side="long",
                condition=RuleCondition(),
                take_profit_bps=90.0,
                stop_loss_bps=60.0,
                max_hold_bars=24,
                full_summary=weak,
                selected_windows=4,
                positive_test_windows=3,
                test_summary=weak,
                decision="SURVIVED",
                reason="ok",
            )
            write_strategy_refine_report(
                source,
                results=[result],
                symbols=["ETHUSDC"],
                interval="1h",
                start_date="2025-01-01",
                end_date="2026-04-30",
                train_months=6,
                test_months=1,
                source_path=None,
                source_count=1,
            )

            candidates, warnings = evaluate_refined_entry_candidates(
                FakeKlineClient(_klines()),
                config=TradingConfig(supervisor_min_samples=100),
                source_path=source,
                symbols=["ETHUSDC"],
                current_ms=200 * 3_600_000,
            )

        self.assertEqual(warnings, ())
        self.assertEqual(candidates[0].decision, ENTRY_BLOCKED)
        self.assertIn("손익비", candidates[0].reason)
        self.assertIn("목표/손절비", candidates[0].reason)

    def test_waiting_payoff_candidate_sends_periodic_watch_notification(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / "strategy_refine_latest.json"
            summary = TradeSummary(
                count=40,
                win_rate=0.55,
                avg_pnl_bps=18.0,
                sum_pnl=1.0,
                profit_factor=2.0,
                payoff_ratio=2.0,
                max_drawdown_pct=0.01,
                max_consecutive_loss=2,
            )
            result = MinedStrategyResult(
                symbol="ETHUSDC",
                interval="1h",
                rule_id="rule",
                action="breakout_short",
                side="short",
                condition=RuleCondition(require_low_breakout=True),
                take_profit_bps=180.0,
                stop_loss_bps=60.0,
                max_hold_bars=24,
                full_summary=summary,
                selected_windows=4,
                positive_test_windows=3,
                test_summary=summary,
                decision="SURVIVED",
                reason="ok",
            )
            write_strategy_refine_report(
                source,
                results=[result],
                symbols=["ETHUSDC"],
                interval="1h",
                start_date="2025-01-01",
                end_date="2026-04-30",
                train_months=6,
                test_months=1,
                source_path=None,
                source_count=1,
            )

            candidates, _ = evaluate_refined_entry_candidates(
                FakeKlineClient(_klines()),
                config=TradingConfig(supervisor_min_samples=100),
                source_path=source,
                symbols=["ETHUSDC"],
                current_ms=200 * 3_600_000,
            )

        self.assertEqual(candidates[0].decision, ENTRY_WAIT)
        should_send, reason, signature, selected = refined_entry_notification_decision(
            candidates,
            RefinedEntryNotifyState(),
            watch_periodic_minutes=360,
        )
        self.assertTrue(should_send)
        self.assertEqual(reason, "정제 관찰후보 주기 보고")
        self.assertEqual([row.decision for row in selected], [ENTRY_WAIT])

        state = apply_refined_entry_notification_state(
            RefinedEntryNotifyState(),
            signature=signature,
            watch=True,
        )
        should_send, reason, _, selected = refined_entry_notification_decision(
            candidates,
            state,
            watch_periodic_minutes=360,
        )
        self.assertFalse(should_send)
        self.assertEqual(reason, "현재 진입후보 없음; 관찰후보 주기 미도래")
        self.assertEqual(selected, [])


def _klines() -> list[Kline]:
    rows = []
    close = 100.0
    for index in range(140):
        close += 0.1
        rows.append(
            Kline(
                open_time=index * 3_600_000,
                open=close - 0.05,
                high=close + 0.20,
                low=close - 0.20,
                close=close,
                volume=100.0,
                close_time=((index + 1) * 3_600_000) - 1,
            )
        )
    return rows


if __name__ == "__main__":
    unittest.main()
