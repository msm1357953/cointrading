import tempfile
import unittest
from pathlib import Path

from cointrading.config import TradingConfig
from cointrading.execution_gate import evaluate_simple_strategy_gate
from cointrading.storage import TradingStore
from cointrading.strategy_router import SETUP_PASS, StrategySetup


def _setup(strategy="trend_follow", side="long") -> StrategySetup:
    return StrategySetup(
        strategy=strategy,
        execution_mode="taker_trend" if strategy == "trend_follow" else "maker_range",
        status=SETUP_PASS,
        side=side,
        horizon="15m-4h",
        live_supported=True,
        reason="test setup",
    )


def _insert_terminal_strategy_cycle(
    store: TradingStore,
    *,
    symbol: str = "ETHUSDC",
    strategy: str = "trend_follow",
    status: str = "CLOSED",
    pnl: float = 0.1,
    dry_run: bool = False,
    timestamp_ms: int = 1_000,
) -> None:
    cycle_id = store.insert_strategy_cycle(
        strategy=strategy,
        execution_mode="taker_trend" if strategy == "trend_follow" else "maker_range",
        symbol=symbol,
        side="long",
        status=status,
        quantity=0.25,
        entry_price=100.0,
        target_price=101.0,
        stop_price=99.0,
        entry_order_type="MARKET",
        take_profit_bps=60,
        stop_loss_bps=30,
        max_hold_seconds=7_200,
        maker_one_way_bps=0.0,
        taker_one_way_bps=4.0,
        entry_deadline_ms=timestamp_ms + 60_000,
        dry_run=dry_run,
        timestamp_ms=timestamp_ms,
    )
    store.update_strategy_cycle(
        cycle_id,
        status=status,
        reason="take_profit" if pnl > 0 else "stop_loss",
        closed_ms=timestamp_ms,
        realized_pnl=pnl,
        timestamp_ms=timestamp_ms,
    )


class ExecutionGateTests(unittest.TestCase):
    def test_gate_allows_first_live_trend_entry(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            store = TradingStore(Path(directory) / "cointrading.sqlite")

            decision = evaluate_simple_strategy_gate(
                store,
                TradingConfig(),
                _setup(),
                symbol="ETHUSDC",
                dry_run=False,
                timestamp_ms=1_700_000_000_000,
            )

            self.assertTrue(decision.allowed)

    def test_gate_is_live_only_by_default(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            store = TradingStore(Path(directory) / "cointrading.sqlite")

            decision = evaluate_simple_strategy_gate(
                store,
                TradingConfig(),
                _setup(strategy="range_reversion"),
                symbol="ETHUSDC",
                dry_run=True,
                timestamp_ms=1_700_000_000_000,
            )

            self.assertTrue(decision.allowed)
            self.assertEqual(decision.reason, "simple trade gate live-only")

    def test_gate_blocks_non_allowed_strategy_for_live(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            store = TradingStore(Path(directory) / "cointrading.sqlite")

            decision = evaluate_simple_strategy_gate(
                store,
                TradingConfig(),
                _setup(strategy="range_reversion"),
                symbol="ETHUSDC",
                dry_run=False,
                timestamp_ms=1_700_000_000_000,
            )

            self.assertFalse(decision.allowed)
            self.assertIn("허용 전략은 trend_follow뿐", decision.reason)

    def test_gate_blocks_daily_entry_limit(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            store = TradingStore(Path(directory) / "cointrading.sqlite")
            ts = 1_700_000_000_000
            _insert_terminal_strategy_cycle(store, dry_run=False, timestamp_ms=ts - 10_000)

            decision = evaluate_simple_strategy_gate(
                store,
                TradingConfig(simple_trade_gate_daily_entry_limit=1),
                _setup(),
                symbol="BTCUSDC",
                dry_run=False,
                timestamp_ms=ts,
            )

            self.assertFalse(decision.allowed)
            self.assertIn("하루 한도 1회", decision.reason)

    def test_gate_blocks_symbol_cooldown(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            store = TradingStore(Path(directory) / "cointrading.sqlite")
            ts = 1_700_000_000_000
            _insert_terminal_strategy_cycle(
                store,
                symbol="ETHUSDC",
                dry_run=False,
                timestamp_ms=ts - 30 * 60_000,
            )

            decision = evaluate_simple_strategy_gate(
                store,
                TradingConfig(
                    simple_trade_gate_daily_entry_limit=0,
                    simple_trade_gate_cooldown_minutes=60,
                ),
                _setup(),
                symbol="ETHUSDC",
                dry_run=False,
                timestamp_ms=ts,
            )

            self.assertFalse(decision.allowed)
            self.assertIn("쿨다운 중", decision.reason)

    def test_gate_blocks_consecutive_losses(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            store = TradingStore(Path(directory) / "cointrading.sqlite")
            ts = 1_700_000_000_000
            _insert_terminal_strategy_cycle(store, symbol="BTCUSDC", pnl=-0.1, timestamp_ms=ts - 20_000)
            _insert_terminal_strategy_cycle(store, symbol="ETHUSDC", pnl=-0.2, timestamp_ms=ts - 10_000)

            decision = evaluate_simple_strategy_gate(
                store,
                TradingConfig(simple_trade_gate_daily_entry_limit=0),
                _setup(),
                symbol="SOLUSDC",
                dry_run=False,
                timestamp_ms=ts,
            )

            self.assertFalse(decision.allowed)
            self.assertIn("2회 연속 손실", decision.reason)


if __name__ == "__main__":
    unittest.main()
