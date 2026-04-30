import unittest

from cointrading.config import TradingConfig
from cointrading.dashboard import _is_authorized, _page


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
        self.assertNotIn("http-equiv=\"refresh\"", html)


if __name__ == "__main__":
    unittest.main()
