import json
import tempfile
import unittest
from pathlib import Path

from cointrading.config import TelegramConfig, TradingConfig
from cointrading.consecutive_bar_alert import detect_run, run_check
from cointrading.models import Kline


def _bar(open_time: int, *, o: float, c: float, span_ms: int = 15 * 60_000) -> Kline:
    return Kline(open_time=open_time, open=o, high=max(o, c) + 1.0,
                 low=min(o, c) - 1.0, close=c, volume=10.0,
                 close_time=open_time + span_ms - 1)


class FakeAlertClient:
    def __init__(self) -> None:
        self.kline_responses: dict[str, list[Kline]] = {}

    def klines(self, symbol: str, interval: str, limit: int = 500):
        return list(self.kline_responses.get(symbol, []))[:limit] or []


class FakeTelegram:
    def __init__(self) -> None:
        self.sent: list[str] = []

    def send_message(self, text: str, chat_id=None):
        self.sent.append(text)
        return {"ok": True}


class DetectRunTests(unittest.TestCase):
    def test_run_of_seven_down(self) -> None:
        bars = [_bar(i * 60_000, o=100 - i, c=99 - i) for i in range(7)]  # all down
        partial = _bar(7 * 60_000, o=92, c=91)  # forming
        run = detect_run(bars + [partial])
        self.assertIsNotNone(run)
        self.assertEqual(run.n, 7)
        self.assertEqual(run.direction, "down")

    def test_run_of_three_up_after_one_down(self) -> None:
        bars = [
            _bar(0, o=100, c=99),    # down
            _bar(1, o=99, c=100),    # up
            _bar(2, o=100, c=101),   # up
            _bar(3, o=101, c=102),   # up
        ]
        partial = _bar(4, o=102, c=102)
        run = detect_run(bars + [partial])
        self.assertEqual(run.n, 3)
        self.assertEqual(run.direction, "up")

    def test_doji_not_counted(self) -> None:
        bars = [
            _bar(0, o=100, c=99),
            _bar(1, o=100, c=100),  # doji — terminal
        ]
        partial = _bar(2, o=100, c=100)
        run = detect_run(bars + [partial])
        self.assertIsNone(run)

    def test_too_few_bars(self) -> None:
        self.assertIsNone(detect_run([]))
        self.assertIsNone(detect_run([_bar(0, o=100, c=99)]))


class RunCheckTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.state_path = Path(self.tmp.name) / "state.json"
        self.client = FakeAlertClient()
        self.tcfg = TelegramConfig(
            bot_token="tok", default_chat_id="1",
            allowed_chat_ids=frozenset({"1"}), commands_enabled=False,
        )
        self.cfg = TradingConfig()

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def _patch_telegram(self, monkeypatch_target):
        # Replace TelegramClient with our fake by monkey-patching the module-level import
        from cointrading import consecutive_bar_alert as mod
        self.fake = FakeTelegram()
        self._orig_tcli = mod.TelegramClient
        mod.TelegramClient = lambda cfg: self.fake
        self.addCleanup(lambda: setattr(mod, "TelegramClient", self._orig_tcli))

    def test_alert_fires_on_seven_down(self) -> None:
        self._patch_telegram(None)
        # 7 closed down bars + 1 partial
        bars = [_bar(i * 900_000, o=100 - i, c=99 - i) for i in range(7)]
        bars.append(_bar(7 * 900_000, o=93, c=93))  # partial doji
        self.client.kline_responses["BTCUSDC"] = bars

        from cointrading.consecutive_bar_alert import run_check
        # Inject our fake client through the module
        from cointrading import consecutive_bar_alert as mod
        orig_client_factory = mod.BinanceUSDMClient
        mod.BinanceUSDMClient = lambda config: self.client
        try:
            result = run_check(
                symbols=("BTCUSDC",), thresholds=(6, 7),
                state_path=self.state_path,
                config=self.cfg, telegram_config=self.tcfg,
            )
        finally:
            mod.BinanceUSDMClient = orig_client_factory

        self.assertEqual(result["telegram_sent"], 1)
        self.assertEqual(result["symbols"]["BTCUSDC"]["alerted"], True)
        self.assertEqual(result["symbols"]["BTCUSDC"]["threshold"], 7)
        self.assertIn("연속 7봉", self.fake.sent[0])
        self.assertIn("음봉", self.fake.sent[0])

        # Re-running should NOT re-alert (latch on same bar)
        result2 = run_check(
            symbols=("BTCUSDC",), thresholds=(6, 7),
            state_path=self.state_path,
            config=self.cfg, telegram_config=self.tcfg,
        )
        self.assertEqual(result2["telegram_sent"], 0)
        # restore
        mod.BinanceUSDMClient = orig_client_factory

    def test_alert_fires_on_six_then_seven(self) -> None:
        self._patch_telegram(None)
        from cointrading import consecutive_bar_alert as mod
        orig = mod.BinanceUSDMClient
        mod.BinanceUSDMClient = lambda config: self.client

        try:
            # Round 1: exactly 6 down bars
            bars6 = [_bar(i * 900_000, o=100 - i, c=99 - i) for i in range(6)]
            bars6.append(_bar(6 * 900_000, o=94, c=94))  # partial
            self.client.kline_responses["BTCUSDC"] = bars6
            r1 = run_check(symbols=("BTCUSDC",), thresholds=(6, 7),
                           state_path=self.state_path,
                           config=self.cfg, telegram_config=self.tcfg)
            self.assertTrue(r1["symbols"]["BTCUSDC"]["alerted"])
            self.assertEqual(r1["symbols"]["BTCUSDC"]["threshold"], 6)

            # Round 2: now 7 down bars (next bar also down), expect a new alert at threshold=7
            bars7 = [_bar(i * 900_000, o=100 - i, c=99 - i) for i in range(7)]
            bars7.append(_bar(7 * 900_000, o=93, c=93))  # partial
            self.client.kline_responses["BTCUSDC"] = bars7
            r2 = run_check(symbols=("BTCUSDC",), thresholds=(6, 7),
                           state_path=self.state_path,
                           config=self.cfg, telegram_config=self.tcfg)
            self.assertTrue(r2["symbols"]["BTCUSDC"]["alerted"])
            self.assertEqual(r2["symbols"]["BTCUSDC"]["threshold"], 7)
            self.assertEqual(len(self.fake.sent), 2)
        finally:
            mod.BinanceUSDMClient = orig

    def test_no_alert_below_threshold(self) -> None:
        self._patch_telegram(None)
        from cointrading import consecutive_bar_alert as mod
        orig = mod.BinanceUSDMClient
        mod.BinanceUSDMClient = lambda config: self.client
        try:
            # only 4 down bars
            bars = [_bar(i * 900_000, o=100 - i, c=99 - i) for i in range(4)]
            bars.append(_bar(4 * 900_000, o=96, c=96))
            self.client.kline_responses["BTCUSDC"] = bars
            r = run_check(symbols=("BTCUSDC",), thresholds=(6, 7),
                          state_path=self.state_path,
                          config=self.cfg, telegram_config=self.tcfg)
            self.assertFalse(r["symbols"]["BTCUSDC"]["alerted"])
            self.assertEqual(self.fake.sent, [])
        finally:
            mod.BinanceUSDMClient = orig


if __name__ == "__main__":
    unittest.main()
