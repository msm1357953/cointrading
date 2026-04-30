import unittest

from cointrading.dashboard import _is_authorized


class DashboardTests(unittest.TestCase):
    def test_dashboard_allows_without_configured_token(self) -> None:
        self.assertTrue(_is_authorized("", {}, ""))

    def test_dashboard_accepts_query_token(self) -> None:
        self.assertTrue(_is_authorized("", {"token": ["secret"]}, "secret"))

    def test_dashboard_accepts_bearer_token(self) -> None:
        self.assertTrue(_is_authorized("Bearer secret", {}, "secret"))

    def test_dashboard_rejects_bad_token(self) -> None:
        self.assertFalse(_is_authorized("Bearer bad", {"token": ["bad"]}, "secret"))


if __name__ == "__main__":
    unittest.main()
