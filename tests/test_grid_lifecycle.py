import tempfile
import time
import unittest
from dataclasses import replace
import json
from pathlib import Path

from cointrading.config import TelegramConfig, TradingConfig
from cointrading.grid_lifecycle import (
    MODE_LONG,
    MODE_STOPPED,
    MakerGridEngine,
    GridState,
    STATUS_ENTRY_SUBMITTED,
    STATUS_OPEN,
    STRATEGY_NAME,
    _kst_today_str,
    grid_recommendation_text,
    load_state,
    save_state,
)
from cointrading.models import Kline
from cointrading.storage import TradingStore
from cointrading.telegram_bot import TelegramBotState, TelegramCommandProcessor


def _cfg(**overrides) -> TradingConfig:
    base = replace(
        TradingConfig(),
        dry_run=False,
        testnet=False,
        live_trading_enabled=True,
        grid_live_enabled=True,
        grid_symbol="BTCUSDC",
        grid_leverage=20,
        grid_layer_notional_pct=0.05,
        grid_max_layer_notional=1000.0,
        grid_max_layers=3,
        grid_gap_min_usdc=50.0,
        grid_gap_max_usdc=150.0,
        grid_take_profit_min_usdc=30.0,
        grid_take_profit_max_usdc=80.0,
        grid_entry_order_ttl_seconds=600,
        initial_equity=1000.0,
        maker_fee_rate=0.0,
        taker_fee_rate=0.0004,
        orderflow_guard_enabled=False,
        grid_position_filter_enabled=False,
        grid_liquidity_filter_enabled=False,
    )
    return replace(base, **overrides)


def _klines(start: float, step: float, n: int, interval_ms: int) -> list[Kline]:
    rows = []
    t0 = 1_000_000
    price = start
    for i in range(n):
        o = price
        c = price + step
        h = max(o, c) + 30
        l = min(o, c) - 30
        rows.append(
            Kline(
                open_time=t0 + i * interval_ms,
                open=o,
                high=h,
                low=l,
                close=c,
                volume=10.0,
                close_time=t0 + (i + 1) * interval_ms - 1,
            )
        )
        price = c
    return rows


class FakeGridClient:
    def __init__(self) -> None:
        self.book = {"bidPrice": "80000.0", "askPrice": "80000.5"}
        self.orders = []
        self.next_order_id = 1000
        self.canned_status = {}
        self.cancels = []
        self.leverage_calls = []
        self.margin_calls = []

    def book_ticker(self, symbol):
        return self.book

    def klines(self, symbol, interval, limit=500):
        if interval == "5m":
            return _klines(79_000, 2.0, min(limit, 300), 300_000)
        if interval == "15m":
            return _klines(79_000, 90.0, min(limit, 10), 900_000)
        if interval == "1h":
            return _klines(78_000, 250.0, min(limit, 10), 3_600_000)
        return _klines(79_000, 1.0, min(limit, 10), 60_000)

    def account_balance(self):
        return [
            {"asset": "USDC", "balance": "20000.0", "availableBalance": "20000.0"},
            {"asset": "BNB", "balance": "0.1", "availableBalance": "0.1"},
        ]

    def exchange_info(self, symbol=None):
        sym = symbol or "BTCUSDC"
        return {
            "symbols": [{
                "symbol": sym,
                "filters": [
                    {"filterType": "PRICE_FILTER", "tickSize": "0.1"},
                    {"filterType": "LOT_SIZE", "minQty": "0.001", "maxQty": "1000", "stepSize": "0.001"},
                    {"filterType": "MIN_NOTIONAL", "notional": "20"},
                ],
            }]
        }

    def set_leverage(self, *, symbol, leverage):
        self.leverage_calls.append((symbol, leverage))
        return {"symbol": symbol, "leverage": leverage}

    def set_margin_type(self, *, symbol, margin_type):
        self.margin_calls.append((symbol, margin_type))
        return {"symbol": symbol, "marginType": margin_type}

    def new_order(self, intent):
        oid = self.next_order_id
        self.next_order_id += 1
        self.orders.append(intent)
        if intent.order_type == "MARKET":
            return {
                "orderId": oid,
                "status": "FILLED",
                "avgPrice": self.book["bidPrice"],
                "executedQty": f"{intent.quantity:.8f}",
            }
        return {"orderId": oid, "status": "NEW", "price": str(intent.price)}

    def order_status(self, *, symbol, order_id=None, orig_client_order_id=None):
        if order_id in self.canned_status:
            return self.canned_status[order_id]
        return {"status": "NEW"}

    def cancel_order(self, *, symbol, order_id=None, orig_client_order_id=None):
        self.cancels.append(order_id or orig_client_order_id)
        return {"status": "CANCELED"}


class GridEngineTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.db = Path(self.tmp.name) / "db.sqlite"
        self.state = Path(self.tmp.name) / "grid_state.json"
        self.store = TradingStore(self.db)
        self.client = FakeGridClient()
        self.cfg = _cfg()

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def _engine(self):
        return MakerGridEngine(
            config=self.cfg,
            storage=self.store,
            client=self.client,
            state_path=self.state,
        )

    def test_long_mode_places_post_only_entry_levels(self) -> None:
        save_state(GridState(mode=MODE_LONG), self.state)
        result = self._engine().step()
        self.assertEqual(len(result.opened), 3)
        self.assertEqual(self.client.leverage_calls, [("BTCUSDC", 20)])
        self.assertEqual(self.client.margin_calls, [("BTCUSDC", "ISOLATED")])
        first = self.client.orders[0]
        self.assertEqual(first.side, "BUY")
        self.assertEqual(first.order_type, "LIMIT")
        self.assertEqual(first.time_in_force, "GTX")
        self.assertFalse(first.reduce_only)
        self.assertLess(first.price, 80000.0)
        cycle = self.store.recent_strategy_cycles(limit=1)[0]
        self.assertEqual(cycle["strategy"], STRATEGY_NAME)
        self.assertEqual(cycle["status"], STATUS_ENTRY_SUBMITTED)
        self.assertEqual(cycle["side"], "long")
        state = load_state(self.state)
        self.assertEqual(state.daily_order_count, 3)

    def test_entry_fill_submits_reduce_only_take_profit(self) -> None:
        save_state(GridState(mode=MODE_LONG), self.state)
        self._engine().step()
        entry_order = self.store.recent_orders(limit=3)[-1]
        import json
        exchange_id = json.loads(entry_order["response_json"])["orderId"]
        self.client.canned_status[exchange_id] = {
            "status": "FILLED",
            "avgPrice": "79950.0",
            "executedQty": "0.001",
        }

        result = self._engine().step()
        self.assertTrue(any(item["action"] == "entry_filled" for item in result.managed))
        tp = self.client.orders[-1]
        self.assertEqual(tp.side, "SELL")
        self.assertEqual(tp.order_type, "LIMIT")
        self.assertEqual(tp.time_in_force, "GTX")
        self.assertTrue(tp.reduce_only)
        cycle = self.store.recent_strategy_cycles(limit=1)[0]
        self.assertEqual(cycle["status"], STATUS_OPEN)

    def test_multiple_filled_layers_share_basket_take_profit_price(self) -> None:
        save_state(GridState(mode=MODE_LONG), self.state)
        self._engine().step()
        entry_rows = self.store.recent_orders(limit=3)
        fill_prices = ["79950.0", "79900.0", "79850.0"]
        for row, fill_price in zip(entry_rows, fill_prices):
            exchange_id = json.loads(row["response_json"])["orderId"]
            self.client.canned_status[exchange_id] = {
                "status": "FILLED",
                "avgPrice": fill_price,
                "executedQty": "0.001",
            }

        result = self._engine().step()

        self.assertTrue(any(item["action"] == "basket_tp_synced" for item in result.managed))
        tp_orders = [order for order in self.client.orders if order.reduce_only]
        self.assertEqual(len(tp_orders), 3)
        tp_prices = {round(float(order.price), 2) for order in tp_orders}
        self.assertEqual(len(tp_prices), 1)
        tp_price = tp_prices.pop()
        self.assertGreaterEqual(tp_price, 79930.0)
        self.assertLessEqual(tp_price, 79930.1)

    def test_recommendation_shows_levels_and_command(self) -> None:
        text = grid_recommendation_text(
            config=self.cfg,
            store=self.store,
            client=self.client,
            state_path=self.state,
        )
        self.assertIn("띠기 추천", text)
        self.assertIn("간격", text)
        self.assertIn("예상 주문 레벨", text)
        self.assertIn("시작 명령", text)
        self.assertIn("20x", text)

    def test_orderflow_danger_needs_confirmation_then_cancels_pending_entry(self) -> None:
        orderflow_path = Path(self.tmp.name) / "orderflow.json"
        orderflow_path.write_text(json.dumps({
            "symbol": "BTCUSDC",
            "updated_ms": int(time.time() * 1000),
            "status": "DANGER",
            "long_status": "DANGER",
            "short_status": "NORMAL",
            "reason": "롱 DANGER: 내 쪽 0.1% depth 급감 70%",
            "long_reason": "내 쪽 0.1% depth 급감 70%",
            "short_reason": "정상",
        }))
        self.cfg = _cfg(
            orderflow_guard_enabled=True,
            orderflow_guard_path=str(orderflow_path),
        )
        save_state(GridState(mode=MODE_LONG), self.state)

        first = self._engine().step()

        self.assertEqual(len(first.opened), 1)
        self.assertEqual(len(self.client.orders), 1)
        state = load_state(self.state)
        self.assertEqual(state.orderflow_long_danger_count, 1)

        second = self._engine().step()
        self.assertEqual(second.opened, [])
        self.assertFalse(any(item.get("reason") == "risk_halt" for item in second.skipped))
        state = load_state(self.state)
        self.assertEqual(state.orderflow_long_danger_count, 2)

        third = self._engine().step()
        self.assertEqual(third.opened, [])
        self.assertEqual(third.skipped[0]["reason"], "risk_halt")
        self.assertIn("확정", third.skipped[0]["detail"])
        self.assertTrue(any(item.get("action") == "orderflow_danger_cancel" for item in third.risk))
        self.assertEqual(len(self.client.cancels), 1)

    def test_grid_step_records_decision_history_for_dashboard(self) -> None:
        save_state(GridState(mode=MODE_LONG), self.state)

        result = self._engine().step()

        self.assertEqual(len(result.opened), 3)
        decisions = self.store.recent_grid_decisions(limit=5)
        self.assertEqual(len(decisions), 1)
        self.assertEqual(decisions[0]["action"], "entry_submitted")
        self.assertEqual(decisions[0]["opened_count"], 3)
        self.assertEqual(decisions[0]["active_entries"], 3)

    def test_loss_cooldown_blocks_temporarily_not_by_daily_pnl(self) -> None:
        future = int(time.time() * 1000) + 600_000
        save_state(
            GridState(
                mode=MODE_LONG,
                daily_kst_date=_kst_today_str(int(time.time() * 1000)),
                daily_realized_pnl=-999.0,
                consecutive_losses=3,
                loss_cooldown_until_ms=future,
            ),
            self.state,
        )

        blocked = self._engine().step()

        self.assertEqual(blocked.opened, [])
        self.assertEqual(blocked.skipped[0]["reason"], "safeguard")
        self.assertIn("cooldown", blocked.skipped[0]["detail"])

        past = int(time.time() * 1000) - 1
        save_state(
            GridState(
                mode=MODE_LONG,
                daily_kst_date=_kst_today_str(int(time.time() * 1000)),
                daily_realized_pnl=-999.0,
                consecutive_losses=3,
                loss_cooldown_until_ms=past,
            ),
            self.state,
        )

        resumed = self._engine().step()

        self.assertEqual(len(resumed.opened), 3)

    def test_daily_realized_loss_is_accounting_not_entry_stop(self) -> None:
        save_state(GridState(mode=MODE_LONG, daily_realized_pnl=-999.0), self.state)

        result = self._engine().step()

        self.assertEqual(len(result.opened), 3)
        self.assertFalse(any(item.get("action") == "daily_stop" for item in result.risk))

    def test_manual_long_soft_blocks_when_price_is_near_range_top(self) -> None:
        self.cfg = _cfg(
            grid_position_filter_enabled=True,
            grid_long_max_range_position=0.82,
        )
        save_state(GridState(mode=MODE_LONG), self.state)

        result = self._engine().step()

        self.assertEqual(result.opened, [])
        self.assertEqual(self.client.orders, [])
        self.assertEqual(result.skipped[0]["reason"], "soft_filter")
        self.assertIn("롱 위치 차단", result.skipped[0]["detail"])

    def test_force_long_bypasses_position_soft_filter_only(self) -> None:
        self.cfg = _cfg(
            grid_position_filter_enabled=True,
            grid_long_max_range_position=0.82,
        )
        save_state(GridState(mode=MODE_LONG, force_entry=True), self.state)

        result = self._engine().step()

        self.assertEqual(len(result.opened), 3)
        state = load_state(self.state)
        self.assertTrue(state.force_entry)


class TelegramGridTests(unittest.TestCase):
    def test_grid_mode_commands(self) -> None:
        cfg = _cfg()
        client = FakeGridClient()
        tcfg = TelegramConfig(allowed_chat_ids=frozenset({"1"}), commands_enabled=True)
        with tempfile.TemporaryDirectory() as td:
            from cointrading import grid_lifecycle as mod
            orig = mod.default_state_path
            mod.default_state_path = lambda: Path(td) / "grid_state.json"
            try:
                processor = TelegramCommandProcessor(
                    tcfg, cfg, TelegramBotState(), exchange_client=client
                )
                on = processor.handle_text("1", "띠기 롱 시작")
                self.assertIn("띠기 롱 모드 ON", on)
                self.assertIn("20x ISOLATED", on)
                state = mod.load_state()
                self.assertEqual(state.mode, "LONG")
                self.assertFalse(state.force_entry)

                force = processor.handle_text("1", "띠기 롱 강제 시작")
                self.assertIn("띠기 롱 강제 모드 ON", force)
                self.assertIn("위치/유동성 필터: 우회", force)
                state = mod.load_state()
                self.assertTrue(state.force_entry)

                rec = processor.handle_text("1", "띠기 추천")
                self.assertIn("띠기 추천", rec)

                off = processor.handle_text("1", "띠기 정지")
                self.assertIn("띠기 정지", off)
                self.assertEqual(mod.load_state().mode, MODE_STOPPED)
                self.assertFalse(mod.load_state().force_entry)
            finally:
                mod.default_state_path = orig


if __name__ == "__main__":
    unittest.main()
