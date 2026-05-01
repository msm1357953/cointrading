import tempfile
import unittest
from pathlib import Path

from cointrading.config import TradingConfig
from cointrading.live_guard import (
    LiveOneShotState,
    consume_live_one_shot,
    validate_live_one_shot,
)
from cointrading.live_supervisor_notify import (
    LiveSupervisorNotifyState,
    actionable_supervisor_reports,
    supervisor_candidate_notification_decision,
    supervisor_candidate_notification_text,
)
from cointrading.market_context import collect_market_context
from cointrading.market_regime import MarketRegimeSnapshot
from cointrading.storage import TradingStore
from cointrading.symbol_supervisor import DECISION_BLOCKED, supervise_symbols


class FakeSupervisorClient:
    def book_ticker(self, symbol):
        return {
            "symbol": symbol,
            "bidPrice": "99.90",
            "bidQty": "10",
            "askPrice": "100.10",
            "askQty": "8",
        }

    def mark_price(self, symbol):
        return {
            "symbol": symbol,
            "markPrice": "100.05",
            "indexPrice": "100.00",
            "lastFundingRate": "0.0001",
            "nextFundingTime": "100000",
        }

    def open_interest(self, symbol):
        return {"symbol": symbol, "openInterest": "1234.5"}

    def order_book(self, symbol, limit=20):
        return {
            "bids": [["99.90", "10"], ["99.80", "5"]],
            "asks": [["100.10", "8"], ["100.20", "4"]],
        }

    def exchange_info(self, symbol=None):
        symbol = symbol or "BTCUSDC"
        return {
            "symbols": [
                {
                    "symbol": symbol,
                    "filters": [
                        {"filterType": "PRICE_FILTER", "tickSize": "0.01"},
                        {
                            "filterType": "LOT_SIZE",
                            "minQty": "0.01",
                            "maxQty": "1000",
                            "stepSize": "0.01",
                        },
                        {"filterType": "MIN_NOTIONAL", "notional": "5"},
                    ],
                }
            ]
        }

    def _signed_request(self, method, path, params=None):
        if path == "/fapi/v1/openOrders":
            return []
        raise AssertionError(path)

    def account_info(self):
        return {"positions": []}


def _macro(symbol="BTCUSDC"):
    return MarketRegimeSnapshot(
        symbol=symbol,
        macro_regime="macro_bull",
        trade_bias="long",
        allowed_strategies=("trend_long_15m_1h",),
        blocked_reason="",
        last_price=100.0,
        trend_1h_bps=20.0,
        trend_4h_bps=50.0,
        realized_vol_bps=10.0,
        atr_bps=20.0,
        timestamp_ms=1_000,
    )


class MarketContextSupervisorTests(unittest.TestCase):
    def test_collect_market_context_records_funding_oi_and_depth(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            store = TradingStore(Path(directory) / "cointrading.sqlite")
            snapshot = collect_market_context(
                FakeSupervisorClient(),
                "BTCUSDC",
                timestamp_ms=1_000,
            )
            store.insert_market_context(snapshot)
            row = store.latest_market_context("BTCUSDC")
            assert row is not None

            self.assertAlmostEqual(float(row["premium_bps"]), 5.0)
            self.assertAlmostEqual(float(row["funding_rate"]), 0.0001)
            self.assertAlmostEqual(float(row["open_interest"]), 1234.5)
            self.assertGreater(float(row["depth_bid_notional"]), 0)
            self.assertEqual(store.summary_counts()["market_contexts"], 1)

    def test_supervisor_blocks_live_until_runtime_flags_and_one_shot_are_enabled(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            store = TradingStore(Path(directory) / "cointrading.sqlite")
            store.insert_market_regime(_macro("BTCUSDC"))

            context = collect_market_context(
                FakeSupervisorClient(),
                "BTCUSDC",
                timestamp_ms=1_000,
            )
            store.insert_market_context(context)
            store.insert_strategy_evaluations(
                [
                    {
                        "source": "signal_grid",
                        "execution_mode": "taker_trend",
                        "symbol": "BTCUSDC",
                        "regime": "aligned_long",
                        "side": "long",
                        "take_profit_bps": 20.0,
                        "stop_loss_bps": 4.0,
                        "max_hold_seconds": 300,
                        "sample_count": 150,
                        "win_count": 90,
                        "loss_count": 60,
                        "win_rate": 0.60,
                        "avg_pnl_bps": 1.5,
                        "sum_pnl_bps": 225.0,
                        "decision": "APPROVED",
                        "reason": "ok",
                    }
                ],
                timestamp_ms=1_000,
            )
            paper_pnls = [0.03] * 14 + [-0.01] * 6
            for idx, realized_pnl in enumerate(paper_pnls):
                status = "CLOSED" if realized_pnl > 0 else "STOPPED"
                reason = "take_profit" if realized_pnl > 0 else "stop_loss"
                cycle_id = store.insert_strategy_cycle(
                    strategy="trend_follow",
                    execution_mode="taker_trend",
                    symbol="BTCUSDC",
                    side="long",
                    status=status,
                    quantity=0.1,
                    entry_price=100.0,
                    target_price=101.0,
                    stop_price=99.0,
                    entry_order_type="MARKET",
                    take_profit_bps=100,
                    stop_loss_bps=50,
                    max_hold_seconds=3600,
                    maker_one_way_bps=0.0,
                    taker_one_way_bps=3.6,
                    entry_deadline_ms=2_000 + idx,
                    dry_run=True,
                    timestamp_ms=2_000 + idx,
                )
                store.update_strategy_cycle(
                    cycle_id,
                    status=status,
                    reason=reason,
                    realized_pnl=realized_pnl,
                    timestamp_ms=3_000 + idx,
                )
            config = TradingConfig(
                dry_run=True,
                live_trading_enabled=False,
                live_strategy_lifecycle_enabled=False,
                supervisor_data_max_age_minutes=10,
                supervisor_min_cycle_count=20,
                supervisor_recent_cycle_count=20,
                supervisor_min_payoff_ratio=1.2,
                runtime_risk_enabled=False,
            )

            report = supervise_symbols(
                FakeSupervisorClient(),
                store,
                config,
                ["BTCUSDC"],
                notional=25,
                current_ms=5_000,
            )[0]

            self.assertEqual(report.decision, DECISION_BLOCKED)
            self.assertIn("dry-run이 켜져 있어 실전 주문은 잠겨 있습니다.", report.reasons)
            self.assertIn("원샷 live 허가가 꺼져 있습니다.", report.reasons)
            self.assertIsNotNone(report.best_candidate)

            actionable = actionable_supervisor_reports([report])
            self.assertEqual([item.symbol for item in actionable], ["BTCUSDC"])
            should_send, reason, signature, reports = supervisor_candidate_notification_decision(
                [report],
                LiveSupervisorNotifyState(),
            )
            self.assertTrue(should_send)
            self.assertEqual(reason, "진입 후보 감지")
            self.assertIn("BTCUSDC", signature)
            text = supervisor_candidate_notification_text(reports, reason=reason, notional=25)
            self.assertIn("주문상태: 실행 안 함", text)
            self.assertIn(
                "후보: 전략=추세 추종 주문=시장가 추세추종 조건=상승 정렬 방향=롱",
                text,
            )

    def test_supervisor_blocks_actionable_alert_when_recent_paper_is_weak(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            store = TradingStore(Path(directory) / "cointrading.sqlite")
            store.insert_market_regime(_macro("BTCUSDC"))
            store.insert_market_context(
                collect_market_context(
                    FakeSupervisorClient(),
                    "BTCUSDC",
                    timestamp_ms=1_000,
                )
            )
            store.insert_strategy_evaluations(
                [
                    {
                        "source": "signal_grid",
                        "execution_mode": "taker_trend",
                        "symbol": "BTCUSDC",
                        "regime": "aligned_long",
                        "side": "long",
                        "take_profit_bps": 20.0,
                        "stop_loss_bps": 4.0,
                        "max_hold_seconds": 300,
                        "sample_count": 150,
                        "win_count": 90,
                        "loss_count": 60,
                        "win_rate": 0.60,
                        "avg_pnl_bps": 1.5,
                        "sum_pnl_bps": 225.0,
                        "decision": "APPROVED",
                        "reason": "ok",
                    }
                ],
                timestamp_ms=1_000,
            )
            paper_pnls = [0.01] * 8 + [-0.03] * 12
            for idx, realized_pnl in enumerate(paper_pnls):
                status = "CLOSED" if realized_pnl > 0 else "STOPPED"
                reason = "take_profit" if realized_pnl > 0 else "stop_loss"
                cycle_id = store.insert_strategy_cycle(
                    strategy="trend_follow",
                    execution_mode="taker_trend",
                    symbol="BTCUSDC",
                    side="long",
                    status=status,
                    quantity=0.1,
                    entry_price=100.0,
                    target_price=101.0,
                    stop_price=99.0,
                    entry_order_type="MARKET",
                    take_profit_bps=100,
                    stop_loss_bps=50,
                    max_hold_seconds=3600,
                    maker_one_way_bps=0.0,
                    taker_one_way_bps=3.6,
                    entry_deadline_ms=2_000 + idx,
                    dry_run=True,
                    timestamp_ms=2_000 + idx,
                )
                store.update_strategy_cycle(
                    cycle_id,
                    status=status,
                    reason=reason,
                    realized_pnl=realized_pnl,
                    timestamp_ms=3_000 + idx,
                )
            config = TradingConfig(
                dry_run=True,
                live_trading_enabled=False,
                live_strategy_lifecycle_enabled=False,
                supervisor_data_max_age_minutes=10,
                supervisor_min_cycle_count=20,
                supervisor_recent_cycle_count=20,
                supervisor_min_payoff_ratio=1.2,
                runtime_risk_enabled=False,
            )

            report = supervise_symbols(
                FakeSupervisorClient(),
                store,
                config,
                ["BTCUSDC"],
                notional=25,
                current_ms=5_000,
            )[0]

            self.assertEqual(report.decision, DECISION_BLOCKED)
            self.assertIn("paper 평균손익이 -0.014000로 양수가 아닙니다.", report.reasons)
            self.assertIn("최근 paper 평균손익이 -0.014000로 양수가 아닙니다.", report.reasons)
            self.assertEqual(actionable_supervisor_reports([report]), [])

    def test_supervisor_blocks_unsupported_live_execution_mode(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            store = TradingStore(Path(directory) / "cointrading.sqlite")
            store.insert_market_regime(_macro("BTCUSDC"))
            store.insert_market_context(
                collect_market_context(
                    FakeSupervisorClient(),
                    "BTCUSDC",
                    timestamp_ms=1_000,
                )
            )
            store.insert_strategy_evaluations(
                [
                    {
                        "source": "signal_grid",
                        "execution_mode": "taker_momentum",
                        "symbol": "BTCUSDC",
                        "regime": "aligned_long",
                        "side": "long",
                        "take_profit_bps": 20.0,
                        "stop_loss_bps": 4.0,
                        "max_hold_seconds": 300,
                        "sample_count": 150,
                        "win_count": 90,
                        "loss_count": 60,
                        "win_rate": 0.60,
                        "avg_pnl_bps": 1.5,
                        "sum_pnl_bps": 225.0,
                        "decision": "APPROVED",
                        "reason": "ok",
                    }
                ],
                timestamp_ms=1_000,
            )
            config = TradingConfig(
                dry_run=True,
                live_trading_enabled=False,
                live_scalp_lifecycle_enabled=True,
                live_strategy_lifecycle_enabled=True,
                supervisor_data_max_age_minutes=10,
                runtime_risk_enabled=False,
            )

            report = supervise_symbols(
                FakeSupervisorClient(),
                store,
                config,
                ["BTCUSDC"],
                notional=25,
                current_ms=5_000,
            )[0]

            self.assertIn("지원되지 않는 live 실행방식입니다: taker_momentum", report.reasons)
            self.assertEqual(actionable_supervisor_reports([report]), [])

    def test_live_one_shot_guard_consumes_after_first_use(self) -> None:
        config = TradingConfig(
            dry_run=False,
            live_one_shot_required=True,
            live_one_shot_enabled=True,
            live_one_shot_symbol="ETHUSDC",
            live_one_shot_strategy="trend_follow",
            live_one_shot_notional=25,
        )
        state = LiveOneShotState()

        allowed = validate_live_one_shot(
            config,
            symbol="ETHUSDC",
            strategy="trend_follow",
            notional=25,
            state=state,
        )
        self.assertTrue(allowed.allowed)

        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "one-shot.json"
            consume_live_one_shot(
                symbol="ETHUSDC",
                strategy="trend_follow",
                notional=25,
                cycle_id=1,
                state_path=path,
            )
            consumed = LiveOneShotState.load(path)
            blocked = validate_live_one_shot(
                config,
                symbol="ETHUSDC",
                strategy="trend_follow",
                notional=25,
                state=consumed,
            )

        self.assertFalse(blocked.allowed)
        self.assertIn("already consumed", blocked.reason)


if __name__ == "__main__":
    unittest.main()
