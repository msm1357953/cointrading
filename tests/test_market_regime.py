import tempfile
import unittest
from pathlib import Path

from cointrading.market_regime import (
    MACRO_BEAR,
    MACRO_BULL,
    MACRO_PANIC,
    evaluate_market_regime,
    market_regime_rows_text,
    scalp_allowed_by_macro,
)
from cointrading.models import Kline
from cointrading.storage import TradingStore


def _klines(count: int, start: float, step: float, width: float = 0.001) -> list[Kline]:
    rows = []
    price = start
    for index in range(count):
        close = price + step
        high = max(price, close) * (1.0 + width)
        low = min(price, close) * (1.0 - width)
        rows.append(
            Kline(
                open_time=index * 60_000,
                open=price,
                high=high,
                low=low,
                close=close,
                volume=100.0,
                close_time=((index + 1) * 60_000) - 1,
            )
        )
        price = close
    return rows


class MarketRegimeTests(unittest.TestCase):
    def test_classifies_bull_and_routes_long(self) -> None:
        snapshot = evaluate_market_regime(
            "BTCUSDC",
            _klines(80, 100.0, 0.05),
            _klines(80, 100.0, 0.25),
            timestamp_ms=1_000,
        )

        self.assertEqual(snapshot.macro_regime, MACRO_BULL)
        self.assertEqual(snapshot.trade_bias, "long")
        self.assertIn("trend_long_15m_1h", snapshot.allowed_strategies)

    def test_classifies_bear_and_blocks_long_scalp(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            store = TradingStore(Path(directory) / "cointrading.sqlite")
            snapshot = evaluate_market_regime(
                "BTCUSDC",
                _klines(80, 120.0, -0.05),
                _klines(80, 120.0, -0.25),
                timestamp_ms=1_000,
            )
            store.insert_market_regime(snapshot)
            row = store.latest_market_regime("BTCUSDC")

            self.assertEqual(snapshot.macro_regime, MACRO_BEAR)
            allowed, reason = scalp_allowed_by_macro(row, "long", max_age_ms=60_000, current_ms=2_000)

            self.assertFalse(allowed)
            self.assertIn("bear regime blocks long", reason)

    def test_panic_routes_to_flat(self) -> None:
        snapshot = evaluate_market_regime(
            "BTCUSDC",
            _klines(80, 100.0, 0.1, width=0.05),
            _klines(80, 100.0, 0.1, width=0.05),
            timestamp_ms=1_000,
        )

        self.assertEqual(snapshot.macro_regime, MACRO_PANIC)
        self.assertEqual(snapshot.allowed_strategies, ())
        self.assertIn("진입 금지", snapshot.to_text())

    def test_market_regime_text_from_store_row(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            store = TradingStore(Path(directory) / "cointrading.sqlite")
            snapshot = evaluate_market_regime(
                "ETHUSDC",
                _klines(80, 100.0, 0.05),
                _klines(80, 100.0, 0.25),
                timestamp_ms=1_000,
            )
            store.insert_market_regime(snapshot)

            text = market_regime_rows_text(store.latest_market_regimes())

            self.assertIn("장세 라우터", text)
            self.assertIn("ETHUSDC", text)
            self.assertIn("상승 추세", text)

    def test_current_market_regimes_can_filter_symbols(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            store = TradingStore(Path(directory) / "cointrading.sqlite")
            for symbol in ("BTCUSDC", "ETHUSDC"):
                store.insert_market_regime(
                    evaluate_market_regime(
                        symbol,
                        _klines(80, 100.0, 0.05),
                        _klines(80, 100.0, 0.25),
                        timestamp_ms=1_000,
                    )
                )

            rows = store.current_market_regimes(symbols=("BTCUSDC",))

            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["symbol"], "BTCUSDC")


if __name__ == "__main__":
    unittest.main()
