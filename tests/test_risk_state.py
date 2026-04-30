import tempfile
import unittest
from pathlib import Path

from cointrading.config import TradingConfig
from cointrading.market_regime import MarketRegimeSnapshot
from cointrading.risk_state import (
    RISK_CAUTION,
    RISK_DEFENSIVE,
    RISK_HALT,
    RISK_NORMAL,
    evaluate_runtime_risk,
)
from cointrading.storage import TradingStore


def _store() -> tuple[tempfile.TemporaryDirectory, TradingStore]:
    directory = tempfile.TemporaryDirectory()
    return directory, TradingStore(Path(directory.name) / "cointrading.sqlite")


def _insert_cycle(
    store: TradingStore,
    *,
    status: str,
    reason: str,
    pnl: float | None,
    timestamp_ms: int,
) -> None:
    cycle_id = store.insert_scalp_cycle(
        symbol="BTCUSDC",
        side="long",
        status=status,
        reason=reason,
        quantity=1.0,
        entry_price=100.0,
        target_price=101.0,
        stop_price=99.0,
        maker_one_way_bps=0.0,
        taker_one_way_bps=4.0,
        entry_deadline_ms=timestamp_ms + 10_000,
        timestamp_ms=timestamp_ms,
    )
    fields = {"reason": reason}
    if pnl is not None:
        fields["realized_pnl"] = pnl
        fields["closed_ms"] = timestamp_ms
    store.update_scalp_cycle(cycle_id, status=status, timestamp_ms=timestamp_ms, **fields)


class RuntimeRiskStateTests(unittest.TestCase):
    def test_normal_mode_allows_entries(self) -> None:
        directory, store = _store()
        with directory:
            snapshot = evaluate_runtime_risk(
                store,
                TradingConfig(runtime_risk_min_events=4),
                current_ms=1_000_000,
            )
            self.assertEqual(snapshot.mode, RISK_NORMAL)
            self.assertTrue(snapshot.allows_new_entries)

    def test_stop_loss_cluster_moves_to_defensive(self) -> None:
        directory, store = _store()
        with directory:
            for index in range(6):
                _insert_cycle(
                    store,
                    status="STOPPED",
                    reason="stop_loss",
                    pnl=-0.1,
                    timestamp_ms=1_000_000 + index,
                )
            for index in range(2):
                _insert_cycle(
                    store,
                    status="CLOSED",
                    reason="take_profit",
                    pnl=0.05,
                    timestamp_ms=1_000_100 + index,
                )

            snapshot = evaluate_runtime_risk(
                store,
                TradingConfig(
                    runtime_risk_min_events=4,
                    runtime_risk_stop_loss_ratio_halt=0.90,
                    runtime_risk_stop_loss_ratio_defensive=0.55,
                ),
                current_ms=1_010_000,
            )

            self.assertEqual(snapshot.mode, RISK_DEFENSIVE)
            self.assertFalse(snapshot.allows_new_entries)

    def test_caution_allows_dry_run_but_blocks_live(self) -> None:
        directory, store = _store()
        with directory:
            for index in range(2):
                _insert_cycle(
                    store,
                    status="STOPPED",
                    reason="stop_loss",
                    pnl=-0.1,
                    timestamp_ms=1_000_000 + index,
                )
            for index in range(3):
                _insert_cycle(
                    store,
                    status="CLOSED",
                    reason="take_profit",
                    pnl=0.05,
                    timestamp_ms=1_000_100 + index,
                )

            dry_run = evaluate_runtime_risk(
                store,
                TradingConfig(
                    dry_run=True,
                    runtime_risk_min_events=4,
                    runtime_risk_stop_loss_ratio_caution=0.40,
                    runtime_risk_stop_loss_ratio_defensive=0.80,
                ),
                current_ms=1_010_000,
            )
            live = evaluate_runtime_risk(
                store,
                TradingConfig(
                    dry_run=False,
                    runtime_risk_min_events=4,
                    runtime_risk_stop_loss_ratio_caution=0.40,
                    runtime_risk_stop_loss_ratio_defensive=0.80,
                ),
                current_ms=1_010_000,
            )

            self.assertEqual(dry_run.mode, RISK_CAUTION)
            self.assertTrue(dry_run.allows_new_entries)
            self.assertEqual(live.mode, RISK_CAUTION)
            self.assertFalse(live.allows_new_entries)

    def test_btc_breakout_moves_to_defensive(self) -> None:
        directory, store = _store()
        with directory:
            store.insert_market_regime(
                MarketRegimeSnapshot(
                    symbol="BTCUSDC",
                    macro_regime="macro_breakout",
                    trade_bias="long",
                    allowed_strategies=("breakout_trend_reduced_size",),
                    blocked_reason="high volatility expansion",
                    last_price=100.0,
                    trend_1h_bps=80.0,
                    trend_4h_bps=100.0,
                    realized_vol_bps=50.0,
                    atr_bps=95.0,
                    timestamp_ms=1_000_000,
                )
            )

            snapshot = evaluate_runtime_risk(
                store,
                TradingConfig(runtime_risk_min_events=4),
                symbol="DOGEUSDC",
                current_ms=1_010_000,
            )

            self.assertEqual(snapshot.mode, RISK_DEFENSIVE)
            self.assertFalse(snapshot.allows_new_entries)

    def test_daily_loss_limit_halts(self) -> None:
        directory, store = _store()
        with directory:
            _insert_cycle(
                store,
                status="STOPPED",
                reason="stop_loss",
                pnl=-11.0,
                timestamp_ms=1_000_000,
            )

            snapshot = evaluate_runtime_risk(
                store,
                TradingConfig(initial_equity=1000.0, runtime_risk_daily_loss_pct=0.01),
                current_ms=1_010_000,
            )

            self.assertEqual(snapshot.mode, RISK_HALT)
            self.assertFalse(snapshot.allows_new_entries)


if __name__ == "__main__":
    unittest.main()
