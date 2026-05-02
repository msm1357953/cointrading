from __future__ import annotations

import unittest

from cointrading.config import TradingConfig
from cointrading.strategy_miner import (
    MinedStrategyResult,
    RuleCondition,
    TradeSummary,
    default_candidate_rules,
    strategy_mine_text,
    strategy_refine_text,
)


class StrategyMinerTests(unittest.TestCase):
    def test_default_candidate_rules_include_multiple_families(self) -> None:
        rules = default_candidate_rules("1h", TradingConfig())
        actions = {rule.action for rule in rules}

        self.assertIn("trend_long", actions)
        self.assertIn("breakout_short", actions)
        self.assertIn("range_short", actions)

    def test_strategy_mine_text_reports_walk_forward_outcome(self) -> None:
        summary = TradeSummary(
            count=30,
            win_rate=0.5,
            avg_pnl_bps=5.0,
            sum_pnl=1.0,
            profit_factor=1.2,
            payoff_ratio=1.0,
            max_drawdown_pct=0.01,
            max_consecutive_loss=3,
        )
        result = MinedStrategyResult(
            symbol="ETHUSDC",
            interval="1h",
            rule_id="rule",
            action="breakout_long",
            side="long",
            condition=RuleCondition(require_high_breakout=True),
            take_profit_bps=180.0,
            stop_loss_bps=60.0,
            max_hold_bars=24,
            full_summary=summary,
            selected_windows=4,
            positive_test_windows=3,
            test_summary=summary,
            decision="SURVIVED",
            reason="테스트",
        )

        text = strategy_mine_text([result])

        self.assertIn("전략 발굴", text)
        self.assertIn("생존 1개", text)
        self.assertIn("ETHUSDC", text)

    def test_strategy_refine_text_marks_watch_as_not_live_ready(self) -> None:
        summary = TradeSummary(
            count=15,
            win_rate=0.45,
            avg_pnl_bps=2.0,
            sum_pnl=0.5,
            profit_factor=1.1,
            payoff_ratio=1.2,
            max_drawdown_pct=0.02,
            max_consecutive_loss=4,
        )
        result = MinedStrategyResult(
            symbol="XRPUSDC",
            interval="1h",
            rule_id="rule",
            action="trend_short",
            side="short",
            condition=RuleCondition(max_trend_4h_bps=-20.0),
            take_profit_bps=200.0,
            stop_loss_bps=70.0,
            max_hold_bars=48,
            full_summary=summary,
            selected_windows=2,
            positive_test_windows=1,
            test_summary=summary,
            decision="WATCH",
            reason="관찰",
        )

        text = strategy_refine_text([result], source_count=1)

        self.assertIn("전략 2차 정제", text)
        self.assertIn("실전 승격 후보는 아직 없습니다", text)
        self.assertIn("XRPUSDC", text)


if __name__ == "__main__":
    unittest.main()
