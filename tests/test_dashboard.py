import unittest

from cointrading.config import TradingConfig
from cointrading.dashboard import _cycle_table_rows, _dashboard_limit, _is_authorized, _page


class DashboardTests(unittest.TestCase):
    def test_dashboard_allows_without_configured_token(self) -> None:
        self.assertTrue(_is_authorized("", {}, ""))

    def test_dashboard_accepts_query_token(self) -> None:
        self.assertTrue(_is_authorized("", {"token": ["secret"]}, "secret"))

    def test_dashboard_accepts_bearer_token(self) -> None:
        self.assertTrue(_is_authorized("Bearer secret", {}, "secret"))

    def test_dashboard_rejects_bad_token(self) -> None:
        self.assertFalse(_is_authorized("Bearer bad", {"token": ["bad"]}, "secret"))

    def test_dashboard_uses_event_stream_without_meta_refresh(self) -> None:
        html = _page(
            {
                "generated_at": "2026-04-30 18:00:00 KST",
                "row_limit": "200",
                "overview": "<div>overview</div>",
                "paper_summary": "<div>paper</div>",
                "strategy_summary": "<div>strategy</div>",
                "probe_summary": "<div>probe</div>",
                "active_paper_rows": "",
                "paper_rows": "",
                "mode_summary": "",
                "report": "ok",
                "signal_rows": "",
                "order_rows": "",
                "cycle_rows": "",
                "strategy_cycle_rows": "",
                "strategy_rows": "",
                "probe_rows": "",
                "market_regime_rows": "",
                "market_context_rows": "",
                "performance_rows": "",
                "strategy_performance_rows": "",
                "exit_reason_rows": "",
                "strategy_exit_reason_rows": "",
            },
            TradingConfig(),
        )
        self.assertIn("new EventSource", html)
        self.assertIn('data-tab="paper"', html)
        self.assertIn('data-tab="research"', html)
        self.assertIn('data-tab="market"', html)
        self.assertIn('data-tab="strategies"', html)
        self.assertIn('id="active-paper-rows"', html)
        self.assertIn("현재가", html)
        self.assertIn("미실현", html)
        self.assertIn("실현손익", html)
        self.assertIn("최근 <span id=\"row-limit\">200</span>개", html)
        self.assertNotIn("http-equiv=\"refresh\"", html)

    def test_dashboard_limit_defaults_and_bounds(self) -> None:
        self.assertEqual(_dashboard_limit({}), 200)
        self.assertEqual(_dashboard_limit({"limit": ["500"]}), 500)
        self.assertEqual(_dashboard_limit({"limit": ["5000"]}), 1000)
        self.assertEqual(_dashboard_limit({"limit": ["bad"]}), 200)

    def test_cycle_rows_show_current_price_and_unrealized_pnl(self) -> None:
        rows = _cycle_table_rows(
            [
                {
                    "updated_ms": 1_714_500_000_000,
                    "symbol": "BTCUSDC",
                    "side": "long",
                    "status": "OPEN",
                    "reason": "",
                    "quantity": 1.0,
                    "entry_price": 100.0,
                    "target_price": 110.0,
                    "stop_price": 95.0,
                    "maker_one_way_bps": 0.0,
                    "taker_one_way_bps": 0.0,
                    "realized_pnl": None,
                    "last_mid_price": 100.5,
                }
            ],
            cycle_type="스캘핑",
            price_by_symbol={"BTCUSDC": 101.0},
        )

        html = rows[0][1]
        self.assertIn("<td>101</td>", html)
        self.assertIn("+1.000000", html)


if __name__ == "__main__":
    unittest.main()
