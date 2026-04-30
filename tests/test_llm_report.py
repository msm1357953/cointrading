import tempfile
import unittest
from pathlib import Path

from cointrading.config import TradingConfig
from cointrading.llm_report import (
    LLMReportState,
    build_report_context,
    build_report_prompt,
    fallback_report_text,
    llm_report_due,
)
from cointrading.market_regime import MarketRegimeSnapshot
from cointrading.storage import TradingStore


class LLMReportTests(unittest.TestCase):
    def test_report_due_respects_interval(self) -> None:
        state = LLMReportState(last_sent_ms=1_000)

        self.assertFalse(
            llm_report_due(state, interval_hours=8, timestamp_ms=1_000 + 3_600_000)
        )
        self.assertTrue(
            llm_report_due(state, interval_hours=8, timestamp_ms=1_000 + 8 * 3_600_000)
        )
        self.assertTrue(llm_report_due(state, interval_hours=8, force=True))

    def test_context_includes_market_and_strategy_data(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            store = TradingStore(Path(directory) / "cointrading.sqlite")
            store.insert_market_regime(
                MarketRegimeSnapshot(
                    symbol="BTCUSDC",
                    macro_regime="macro_bull",
                    trade_bias="long",
                    allowed_strategies=("trend_long_15m_1h",),
                    blocked_reason="",
                    last_price=100.0,
                    trend_1h_bps=20.0,
                    trend_4h_bps=50.0,
                    realized_vol_bps=10.0,
                    atr_bps=15.0,
                    timestamp_ms=1_000,
                )
            )
            store.insert_strategy_evaluations(
                [
                    {
                        "source": "signal_grid",
                        "execution_mode": "maker_post_only",
                        "symbol": "BTCUSDC",
                        "regime": "aligned_long",
                        "side": "long",
                        "take_profit_bps": 12.0,
                        "stop_loss_bps": 4.0,
                        "max_hold_seconds": 180,
                        "sample_count": 50,
                        "win_count": 30,
                        "loss_count": 20,
                        "win_rate": 0.6,
                        "avg_pnl_bps": 1.5,
                        "sum_pnl_bps": 75.0,
                        "avg_win_bps": 8.0,
                        "avg_loss_bps": -4.0,
                        "decision": "APPROVED",
                        "reason": "test",
                    }
                ],
                timestamp_ms=2_000,
            )

            context = build_report_context(store, TradingConfig(scalp_symbols=("BTCUSDC",)))
            prompt = build_report_prompt(context)
            fallback = fallback_report_text(context)

            self.assertIn("BTCUSDC", context)
            self.assertIn("APPROVED", context)
            self.assertIn("주문 실행", prompt)
            self.assertIn("LLM 리포트 대체 요약", fallback)


if __name__ == "__main__":
    unittest.main()
