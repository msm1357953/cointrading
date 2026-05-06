import json
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path

from cointrading.config import TelegramConfig, TradingConfig
from cointrading.consecutive_auto_lifecycle import (
    AutoState,
    ConsecutiveAutoEngine,
    STATUS_CLOSED,
    STATUS_OPEN,
    STATUS_STOPPED,
    STRATEGY_NAME,
    compute_run_extents,
    load_state,
    safeguard_block_reason,
    save_state,
)
from cointrading.consecutive_bar_alert import RunResult
from cointrading.models import Kline
from cointrading.storage import TradingStore
from cointrading.telegram_bot import TelegramBotState, TelegramCommandProcessor


def _bar(t, *, o, h, l, c) -> Kline:
    return Kline(open_time=t, open=o, high=h, low=l, close=c, volume=10.0,
                 close_time=t + 900_000 - 1)


class FakeAutoClient:
    def __init__(self) -> None:
        self.next_id = 1000
        self.book = {"bidPrice": "100.0", "askPrice": "100.04"}
        self.entry_override: float | None = None
        self.canned_status: dict[int, dict] = {}
        self.cancels: list[int] = []
        self.set_leverage_calls: list[tuple] = []
        self.set_margin_calls: list[tuple] = []

    def book_ticker(self, symbol):
        return self.book

    def exchange_info(self, symbol=None):
        sym = symbol or "BTCUSDC"
        return {
            "symbols": [{
                "symbol": sym,
                "filters": [
                    {"filterType": "PRICE_FILTER", "tickSize": "0.0001"},
                    {"filterType": "LOT_SIZE", "minQty": "0.0001", "maxQty": "10000", "stepSize": "0.0001"},
                    {"filterType": "MIN_NOTIONAL", "notional": "1"},
                ],
            }]
        }

    def set_leverage(self, *, symbol, leverage):
        self.set_leverage_calls.append((symbol, leverage))
        return {"symbol": symbol, "leverage": leverage}

    def set_margin_type(self, *, symbol, margin_type):
        self.set_margin_calls.append((symbol, margin_type))
        return {"symbol": symbol, "marginType": margin_type}

    def new_order(self, intent):
        oid = self.next_id
        self.next_id += 1
        if intent.order_type == "MARKET":
            avg = self.entry_override or float(self.book["askPrice"])
            resp = {
                "orderId": oid, "symbol": intent.symbol,
                "status": "FILLED",
                "avgPrice": f"{avg:.8f}",
                "executedQty": f"{intent.quantity:.8f}",
            }
        else:  # STOP_MARKET / TAKE_PROFIT_MARKET
            resp = {
                "orderId": oid, "symbol": intent.symbol,
                "status": "NEW",
                "stopPrice": f"{intent.stop_price:.8f}" if intent.stop_price else "0",
            }
        return resp

    def order_status(self, *, symbol, order_id=None, orig_client_order_id=None):
        if order_id is not None and order_id in self.canned_status:
            return self.canned_status[order_id]
        return {"status": "UNKNOWN"}

    def cancel_order(self, *, symbol, order_id=None, orig_client_order_id=None):
        if order_id is not None:
            self.cancels.append(order_id)
        return {"status": "CANCELED"}

    def account_balance(self):
        # 1000 USDC available — matches the test config's initial_equity
        return [
            {"asset": "USDC", "balance": "1000.0", "availableBalance": "1000.0"},
            {"asset": "BNB", "balance": "0.0", "availableBalance": "0.0"},
        ]


def _config(**overrides) -> TradingConfig:
    base = replace(
        TradingConfig(),
        dry_run=False,
        live_trading_enabled=True,
        consecutive_auto_symbol="BTCUSDC",
        consecutive_auto_threshold=6,
        consecutive_auto_leverage=5,
        consecutive_auto_margin_pct=0.10,
        consecutive_auto_sl_buffer_bps=10.0,
        consecutive_auto_tp_rr=1.0,
        consecutive_auto_time_exit_minutes=60,
        consecutive_auto_daily_loss_pct=0.03,
        consecutive_auto_max_consecutive_losses=3,
        consecutive_auto_max_trades_per_day=5,
        consecutive_auto_freshness_seconds=600,
        initial_equity=1000.0,
        taker_fee_rate=0.0004,
    )
    return replace(base, **overrides)


class SafeguardTests(unittest.TestCase):
    def setUp(self) -> None:
        self.cfg = _config()

    def test_auto_off_blocks(self) -> None:
        s = AutoState(auto_mode=False)
        self.assertEqual(safeguard_block_reason(s, self.cfg), "auto mode OFF")

    def test_daily_loss_blocks(self) -> None:
        s = AutoState(auto_mode=True, daily_realized_pnl=-30.5)
        # cap = 1000 * 0.03 = 30. -30.5 <= -30 -> block
        reason = safeguard_block_reason(s, self.cfg)
        self.assertIsNotNone(reason)
        self.assertIn("daily loss", reason)

    def test_consecutive_losses_block(self) -> None:
        s = AutoState(auto_mode=True, consecutive_losses=3)
        reason = safeguard_block_reason(s, self.cfg)
        self.assertIsNotNone(reason)
        self.assertIn("consecutive", reason)

    def test_daily_trade_cap_blocks(self) -> None:
        s = AutoState(auto_mode=True, daily_trade_count=5)
        reason = safeguard_block_reason(s, self.cfg)
        self.assertIsNotNone(reason)
        self.assertIn("trade cap", reason)

    def test_clean_state_passes(self) -> None:
        s = AutoState(auto_mode=True)
        self.assertIsNone(safeguard_block_reason(s, self.cfg))


class StateRoundTripTests(unittest.TestCase):
    def test_save_load(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            p = Path(td) / "auto.json"
            s = AutoState(auto_mode=True, daily_realized_pnl=-3.5,
                          daily_trade_count=2, consecutive_losses=1,
                          paused_reason="", daily_kst_date="2026-05-07")
            save_state(s, p)
            loaded = load_state(p)
            self.assertEqual(loaded.auto_mode, True)
            self.assertEqual(loaded.daily_realized_pnl, -3.5)
            self.assertEqual(loaded.daily_trade_count, 2)
            self.assertEqual(loaded.consecutive_losses, 1)


class ComputeRunExtentsTests(unittest.TestCase):
    def test_extents_from_run(self) -> None:
        # 6 down bars: highs are 100..95, lows are 99..94
        bars = [_bar(i*900_000, o=100-i, h=100-i+1, l=99-i, c=99-i) for i in range(6)]
        bars.append(_bar(6*900_000, o=94, h=94, l=94, c=94))  # partial
        run = RunResult(bar=bars[-2], n=6, direction="down", doji_count=0)
        extents = compute_run_extents(bars, run)
        self.assertEqual(extents.run_high, 101.0)  # bar 0's high
        self.assertEqual(extents.run_low, 94.0)    # bar 5's low
        self.assertEqual(extents.last_close, 94.0)


class AutoEngineTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.db = Path(self.tmp.name) / "db.sqlite"
        self.state_p = Path(self.tmp.name) / "state.json"
        self.cfg = _config()
        self.store = TradingStore(self.db)
        self.client = FakeAutoClient()

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def _engine(self) -> ConsecutiveAutoEngine:
        return ConsecutiveAutoEngine(
            config=self.cfg, storage=self.store, client=self.client,
            state_path=self.state_p,
        )

    def _down_run_klines(self):
        # Build bars whose latest CLOSED bar closed ~30s ago so freshness check passes.
        import time as _t
        now_ms = int(_t.time() * 1000)
        bars = []
        for i in range(6):
            # i=5 is the most recent closed bar. Its close_time = now_ms - 30s.
            close_t = now_ms - 30_000 - (5 - i) * 900_000
            open_t = close_t - 900_000 + 1
            bars.append(Kline(open_time=open_t,
                              open=100 - i, high=100 - i + 0.5,
                              low=99 - i - 0.5, close=99 - i, volume=10.0,
                              close_time=close_t))
        # Partial bar (forming): starts right after last closed bar's close
        partial_open = bars[-1].close_time + 1
        bars.append(Kline(open_time=partial_open, open=94.0, high=94.0,
                          low=94.0, close=94.0, volume=1.0,
                          close_time=partial_open + 900_000 - 1))
        return bars

    def test_open_long_on_down_run(self) -> None:
        klines = self._down_run_klines()
        run = RunResult(bar=klines[-2], n=6, direction="down", doji_count=0)
        s = AutoState(auto_mode=True)
        save_state(s, self.state_p)

        engine = self._engine()

        self.client.entry_override = 100.04
        outcome = engine.maybe_open(run=run, klines=klines, state=s)
        self.assertEqual(outcome.action, "opened",
                         msg=f"action={outcome.action} detail={outcome.detail}")

        cycle = self.store.active_strategy_cycle(STRATEGY_NAME, "BTCUSDC")
        self.assertIsNotNone(cycle)
        self.assertEqual(cycle["status"], STATUS_OPEN)
        self.assertEqual(cycle["side"], "long")
        self.assertEqual(cycle["dry_run"], 0)
        self.assertAlmostEqual(cycle["entry_price"], 100.04)
        # SL at run_low (= 93.5) * (1 - 0.001) = 93.4065
        self.assertAlmostEqual(cycle["stop_price"], 93.5 * 0.999, places=4)
        # TP = open of bar BEFORE the trigger bar (prior_bar_open).
        # In _down_run_klines the prior bar (closed[-2]) had open = 96.0
        # (i=4 in the loop: open = 100 - 4 = 96).
        self.assertAlmostEqual(cycle["target_price"], 96.0, places=4)
        # Leverage + margin type were set
        self.assertEqual(self.client.set_leverage_calls, [("BTCUSDC", 5)])
        self.assertEqual(self.client.set_margin_calls, [("BTCUSDC", "ISOLATED")])
        # max_hold_deadline is the EARLIER of (now + 60min, next 15m boundary)
        # — typically the 15m boundary fires first.
        import time
        now_ms = int(time.time() * 1000)
        self.assertLess(cycle["max_hold_deadline_ms"], now_ms + 60 * 60_000 + 1)
        self.assertLessEqual(cycle["max_hold_deadline_ms"] - now_ms, 60 * 60_000)

    def test_block_when_auto_off(self) -> None:
        klines = self._down_run_klines()
        run = RunResult(bar=klines[-2], n=6, direction="down", doji_count=0)
        s = AutoState(auto_mode=False)
        engine = self._engine()
        outcome = engine.maybe_open(run=run, klines=klines, state=s)
        self.assertEqual(outcome.action, "blocked")
        self.assertIn("auto mode OFF", outcome.detail)

    def test_block_when_threshold_not_met(self) -> None:
        klines = self._down_run_klines()
        run = RunResult(bar=klines[-2], n=4, direction="down", doji_count=0)
        s = AutoState(auto_mode=True)
        # extend freshness to avoid that block
        self.cfg = _config(consecutive_auto_freshness_seconds=10**9)
        engine = self._engine()
        outcome = engine.maybe_open(run=run, klines=klines, state=s)
        self.assertEqual(outcome.action, "no_op")
        self.assertIn("threshold", outcome.detail)


class TelegramAutoToggleTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tcfg = TelegramConfig(allowed_chat_ids=frozenset({"1"}),
                                   commands_enabled=True)
        self.cfg = _config()
        self.state = TelegramBotState()

    def _processor(self) -> TelegramCommandProcessor:
        return TelegramCommandProcessor(self.tcfg, self.cfg, self.state)

    def test_auto_on_off_status_flow(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            # Patch state path
            from cointrading import consecutive_auto_lifecycle as mod
            orig = mod.default_state_path
            mod.default_state_path = lambda: Path(td) / "auto.json"
            try:
                p = self._processor()
                # initial status: should report OFF
                s1 = p.handle_text("1", "자동상태")
                self.assertIn("OFF", s1)

                # turn on
                on = p.handle_text("1", "자동")
                self.assertIn("자동 진입 ON", on)
                self.assertIn("5x ISOLATED", on)
                # Notional now scales with current account balance, not a fixed config value
                self.assertIn("진입 시점 USDC 잔고", on)
                self.assertIn("직전 봉의 시작가", on)  # new TP description
                self.assertIn("강제청산", on)             # new bar-end force-close

                # status now ON
                s2 = p.handle_text("1", "자동상태")
                self.assertIn("ON", s2)

                # turn off
                off = p.handle_text("1", "수동")
                self.assertIn("자동 진입 OFF", off)

                # already off
                off2 = p.handle_text("1", "수동")
                self.assertIn("이미 수동", off2)
            finally:
                mod.default_state_path = orig

    def test_auto_on_blocked_when_live_env_not_armed(self) -> None:
        cfg = _config(dry_run=True, live_trading_enabled=False)
        with tempfile.TemporaryDirectory() as td:
            from cointrading import consecutive_auto_lifecycle as mod
            orig = mod.default_state_path
            mod.default_state_path = lambda: Path(td) / "auto.json"
            try:
                p = TelegramCommandProcessor(self.tcfg, cfg, self.state)
                msg = p.handle_text("1", "자동")
                self.assertIn("실패", msg)
                self.assertIn("DRY_RUN", msg)
            finally:
                mod.default_state_path = orig


if __name__ == "__main__":
    unittest.main()
