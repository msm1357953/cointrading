import json
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path

from cointrading.config import TelegramConfig, TradingConfig
from cointrading.live_trade_monitor import (
    INTERESTING_TYPES,
    aggregate,
    format_summary,
    run_monitor,
)


class FakeIncomeClient:
    def __init__(self) -> None:
        self.events: list[dict] = []
        self.calls: list[dict] = []

    def income_history(self, **params):
        self.calls.append(params)
        return list(self.events)


class FakeTelegram:
    def __init__(self) -> None:
        self.sent: list[str] = []

    def send_message(self, text, chat_id=None):
        self.sent.append(text)
        return {"ok": True}


def _ev(*, time_ms, symbol, type_, income, tran_id):
    return {
        "time": time_ms, "symbol": symbol, "incomeType": type_,
        "income": str(income), "tranId": tran_id, "asset": "USDC",
    }


class AggregateTests(unittest.TestCase):
    def test_groups_by_symbol_and_type(self) -> None:
        events = [
            _ev(time_ms=1000, symbol="BTCUSDC", type_="REALIZED_PNL", income=10.0, tran_id="1"),
            _ev(time_ms=1100, symbol="BTCUSDC", type_="COMMISSION", income=-0.5, tran_id="2"),
            _ev(time_ms=1200, symbol="BTCUSDC", type_="REALIZED_PNL", income=-3.0, tran_id="3"),
            _ev(time_ms=1300, symbol="ETHUSDC", type_="REALIZED_PNL", income=5.0, tran_id="4"),
            _ev(time_ms=1400, symbol="ETHUSDC", type_="FUNDING_FEE", income=-0.2, tran_id="5"),
        ]
        agg = aggregate(events)
        self.assertEqual(set(agg.keys()), {"BTCUSDC", "ETHUSDC"})
        self.assertAlmostEqual(agg["BTCUSDC"].realized_pnl, 7.0)
        self.assertEqual(agg["BTCUSDC"].realized_count, 2)
        self.assertAlmostEqual(agg["BTCUSDC"].commission, -0.5)
        self.assertAlmostEqual(agg["ETHUSDC"].realized_pnl, 5.0)
        self.assertAlmostEqual(agg["ETHUSDC"].funding_fee, -0.2)


class FormatSummaryTests(unittest.TestCase):
    def test_message_contains_per_symbol_and_grand_net(self) -> None:
        events = [
            _ev(time_ms=1000, symbol="BTCUSDC", type_="REALIZED_PNL", income=10.0, tran_id="1"),
            _ev(time_ms=1100, symbol="BTCUSDC", type_="COMMISSION", income=-0.5, tran_id="2"),
        ]
        text = format_summary(events, window_minutes=5)
        self.assertIn("BTCUSDC", text)
        self.assertIn("실현 손익", text)
        self.assertIn("수수료", text)
        self.assertIn("순 합계", text)
        self.assertIn("+10.0", text)
        self.assertIn("-0.5", text)


class RunMonitorTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.state_path = Path(self.tmp.name) / "state.json"
        self.cfg = TradingConfig()
        self.tcfg = TelegramConfig(
            bot_token="tok", default_chat_id="1",
            allowed_chat_ids=frozenset({"1"}), commands_enabled=False,
        )
        self.client = FakeIncomeClient()
        # Patch client + telegram
        from cointrading import live_trade_monitor as mod
        self._orig_client = mod.BinanceUSDMClient
        self._orig_tcli = mod.TelegramClient
        mod.BinanceUSDMClient = lambda config: self.client
        self.fake_tg = FakeTelegram()
        mod.TelegramClient = lambda cfg: self.fake_tg

    def tearDown(self) -> None:
        from cointrading import live_trade_monitor as mod
        mod.BinanceUSDMClient = self._orig_client
        mod.TelegramClient = self._orig_tcli
        self.tmp.cleanup()

    def test_first_run_with_events_sends_telegram(self) -> None:
        self.client.events = [
            _ev(time_ms=1_700_000_000_000, symbol="BTCUSDC", type_="REALIZED_PNL",
                income=15.5, tran_id="A"),
            _ev(time_ms=1_700_000_001_000, symbol="BTCUSDC", type_="COMMISSION",
                income=-1.2, tran_id="B"),
        ]
        result = run_monitor(state_path=self.state_path,
                             config=self.cfg, telegram_config=self.tcfg,
                             lookback_minutes_first_run=60)
        self.assertTrue(result["ok"])
        self.assertEqual(result["new_events"], 2)
        self.assertEqual(result["telegram_sent"], 1)
        self.assertEqual(len(self.fake_tg.sent), 1)
        self.assertIn("BTCUSDC", self.fake_tg.sent[0])
        # State should advance
        state = json.loads(self.state_path.read_text())
        self.assertGreater(state["last_seen_time_ms"], 0)
        self.assertIn("A", state["recent_tran_ids"])
        self.assertIn("B", state["recent_tran_ids"])

    def test_dedupe_on_second_run(self) -> None:
        self.client.events = [
            _ev(time_ms=1_700_000_000_000, symbol="BTCUSDC", type_="REALIZED_PNL",
                income=15.5, tran_id="A"),
        ]
        run_monitor(state_path=self.state_path, config=self.cfg,
                    telegram_config=self.tcfg, lookback_minutes_first_run=60)
        # Reset telegram capture
        self.fake_tg.sent.clear()
        # Same event again
        result2 = run_monitor(state_path=self.state_path, config=self.cfg,
                              telegram_config=self.tcfg)
        self.assertEqual(result2["new_events"], 0)
        self.assertEqual(result2["telegram_sent"], 0)
        self.assertEqual(self.fake_tg.sent, [])

    def test_filters_uninteresting_types(self) -> None:
        self.client.events = [
            _ev(time_ms=1_700_000_000_000, symbol="BTCUSDC", type_="TRANSFER",
                income=100.0, tran_id="X"),
            _ev(time_ms=1_700_000_001_000, symbol="BTCUSDC", type_="REALIZED_PNL",
                income=2.0, tran_id="Y"),
        ]
        result = run_monitor(state_path=self.state_path, config=self.cfg,
                             telegram_config=self.tcfg, lookback_minutes_first_run=60)
        self.assertEqual(result["new_events"], 1)  # only the REALIZED_PNL


if __name__ == "__main__":
    unittest.main()
