import unittest

from cointrading.config import TradingConfig
from cointrading.dashboard import _dashboard_limit, _is_authorized, _page


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
                "report": "ok",
                "signal_rows": "",
                "order_rows": "",
                "cycle_rows": "",
                "performance_rows": "",
                "exit_reason_rows": "",
            },
            TradingConfig(),
        )
        self.assertIn("new EventSource", html)
        self.assertIn('data-tab="performance"', html)
        self.assertIn("최근 <span id=\"row-limit\">200</span>개 표시", html)
        self.assertNotIn("http-equiv=\"refresh\"", html)

    def test_dashboard_limit_defaults_and_bounds(self) -> None:
        self.assertEqual(_dashboard_limit({}), 200)
        self.assertEqual(_dashboard_limit({"limit": ["500"]}), 500)
        self.assertEqual(_dashboard_limit({"limit": ["5000"]}), 1000)
        self.assertEqual(_dashboard_limit({"limit": ["bad"]}), 200)


if __name__ == "__main__":
    unittest.main()
