import csv
import tempfile
import unittest
from pathlib import Path

from cointrading.config import TradingConfig
from cointrading.models import Kline
from cointrading.scalping import (
    SCALP_LOG_FIELDS,
    ScalpSignalEngine,
    scalp_report_text,
)


def _klines(closes: list[float]) -> list[Kline]:
    rows: list[Kline] = []
    for index, close in enumerate(closes):
        rows.append(
            Kline(
                open_time=index * 60_000,
                open=close,
                high=close * 1.001,
                low=close * 0.999,
                close=close,
                volume=100.0,
                close_time=((index + 1) * 60_000) - 1,
            )
        )
    return rows


def _book(bid_qty: str = "1000", ask_qty: str = "500"):
    return {
        "bids": [["100.00", bid_qty], ["99.99", "100"]],
        "asks": [["100.01", ask_qty], ["100.02", "100"]],
    }


class ScalpingTests(unittest.TestCase):
    def test_aligned_signal_requires_depth_volatility_and_fee_buffer(self) -> None:
        signal = ScalpSignalEngine().evaluate(
            symbol="BTCUSDT",
            book_ticker={"bidPrice": "100.00", "askPrice": "100.01"},
            order_book=_book(),
            klines=_klines([100.00, 100.02, 100.04, 100.06, 100.08, 100.10]),
            trading_config=TradingConfig(),
            commission_rate={
                "makerCommissionRate": "0.000200",
                "takerCommissionRate": "0.000500",
            },
        )
        self.assertEqual(signal.side, "long")
        self.assertEqual(signal.regime, "aligned_long")
        self.assertTrue(signal.trade_allowed)
        self.assertGreater(signal.edge_after_maker_bps, 1.0)

    def test_panic_volatility_blocks_entry(self) -> None:
        signal = ScalpSignalEngine().evaluate(
            symbol="BTCUSDT",
            book_ticker={"bidPrice": "100.00", "askPrice": "100.01"},
            order_book=_book(),
            klines=_klines([100.00, 101.00, 99.00, 102.00, 98.00, 103.00]),
            trading_config=TradingConfig(),
            commission_rate={
                "makerCommissionRate": "0.000200",
                "takerCommissionRate": "0.000500",
            },
        )
        self.assertEqual(signal.side, "flat")
        self.assertEqual(signal.regime, "panic_volatility")
        self.assertFalse(signal.trade_allowed)

    def test_negative_spread_blocks_entry(self) -> None:
        signal = ScalpSignalEngine().evaluate(
            symbol="BTCUSDT",
            book_ticker={"bidPrice": "100.02", "askPrice": "100.01"},
            order_book=_book(),
            klines=_klines([100.00, 100.02, 100.04, 100.06, 100.08, 100.10]),
            trading_config=TradingConfig(),
            commission_rate={
                "makerCommissionRate": "0.000000",
                "takerCommissionRate": "0.000400",
            },
        )
        self.assertEqual(signal.side, "flat")
        self.assertEqual(signal.regime, "invalid_spread")
        self.assertFalse(signal.trade_allowed)

    def test_report_migrates_old_scalp_log_schema(self) -> None:
        old_fields = [field for field in SCALP_LOG_FIELDS if field not in {
            "regime",
            "trade_allowed",
            "edge_after_maker_bps",
            "book_bid_notional",
            "book_ask_notional",
            "book_depth_notional",
        }]
        row = {field: "" for field in old_fields}
        row.update(
            {
                "timestamp_ms": "1",
                "iso_time": "2026-04-30T00:00:00+00:00",
                "symbol": "BTCUSDT",
                "side": "long",
                "reason": "bid imbalance with positive momentum",
                "mid_price": "100",
                "maker_roundtrip_bps": "4",
                "taker_roundtrip_bps": "10",
                "horizon_5m_bps": "8",
            }
        )
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "scalp_signals.csv"
            with path.open("w", newline="") as file:
                writer = csv.DictWriter(file, fieldnames=old_fields)
                writer.writeheader()
                writer.writerow(row)

            report = scalp_report_text(path)

            self.assertIn("5분 후: 표본=1", report)
            self.assertIn("이전로그", report)
            with path.open() as file:
                self.assertEqual(csv.DictReader(file).fieldnames, SCALP_LOG_FIELDS)

    def test_report_can_focus_on_active_usdc_symbols(self) -> None:
        rows = [
            {
                "timestamp_ms": "1",
                "iso_time": "2026-04-30T00:00:00+00:00",
                "symbol": "BTCUSDT",
                "side": "long",
                "reason": "legacy",
                "regime": "legacy",
                "trade_allowed": "true",
                "mid_price": "100",
                "spread_bps": "",
                "imbalance": "",
                "momentum_bps": "",
                "realized_vol_bps": "",
                "maker_roundtrip_bps": "4",
                "taker_roundtrip_bps": "10",
                "edge_after_maker_bps": "",
                "book_bid_notional": "",
                "book_ask_notional": "",
                "book_depth_notional": "",
                "bnb_fee_discount_enabled": "false",
                "bnb_fee_discount_active": "false",
                "latest_funding_rate": "",
                "horizon_1m_bps": "",
                "horizon_3m_bps": "",
                "horizon_5m_bps": "8",
            },
            {
                "timestamp_ms": "2",
                "iso_time": "2026-04-30T00:01:00+00:00",
                "symbol": "BTCUSDC",
                "side": "long",
                "reason": "bid imbalance with positive momentum",
                "regime": "aligned_long",
                "trade_allowed": "true",
                "mid_price": "100",
                "spread_bps": "",
                "imbalance": "",
                "momentum_bps": "",
                "realized_vol_bps": "",
                "maker_roundtrip_bps": "0",
                "taker_roundtrip_bps": "7.2",
                "edge_after_maker_bps": "",
                "book_bid_notional": "",
                "book_ask_notional": "",
                "book_depth_notional": "",
                "bnb_fee_discount_enabled": "true",
                "bnb_fee_discount_active": "true",
                "latest_funding_rate": "",
                "horizon_1m_bps": "",
                "horizon_3m_bps": "",
                "horizon_5m_bps": "8",
            },
        ]
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "scalp_signals.csv"
            with path.open("w", newline="") as file:
                writer = csv.DictWriter(file, fieldnames=SCALP_LOG_FIELDS)
                writer.writeheader()
                writer.writerows(rows)

            report = scalp_report_text(path, symbols=("BTCUSDC", "ETHUSDC"))

        self.assertIn("대상: BTCUSDC, ETHUSDC", report)
        self.assertIn("전체 로그: 1개", report)
        self.assertIn("메이커순익=8.000bps", report)
        self.assertNotIn("이전로그", report)


if __name__ == "__main__":
    unittest.main()
