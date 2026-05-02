from __future__ import annotations

import unittest

from cointrading.config import TradingConfig
from cointrading.strategy_miner import (
    MinedStrategyResult,
    RuleCondition,
    TradeSummary,
    default_candidate_rules,
    strategy_mine_text,
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


if __name__ == "__main__":
    unittest.main()
