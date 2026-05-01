import unittest

from cointrading.indicators import build_technical_snapshot
from cointrading.models import Kline


def _klines() -> list[Kline]:
    rows = []
    close = 100.0
    for index in range(80):
        close += [0.08, -0.03, 0.07, -0.04, 0.06][index % 5]
        rows.append(
            Kline(
                open_time=index * 300_000,
                open=close,
                high=close + 0.2,
                low=close - 0.2,
                close=close,
                volume=100.0 if index < 79 else 160.0,
                close_time=((index + 1) * 300_000) - 1,
            )
        )
    return rows


class IndicatorTests(unittest.TestCase):
    def test_technical_snapshot_contains_rsi_ema_bollinger_and_volume(self) -> None:
        snapshot = build_technical_snapshot(_klines(), interval="5m")

        self.assertTrue(snapshot.enough)
        self.assertIsNotNone(snapshot.rsi14)
        self.assertIsNotNone(snapshot.ema_fast)
        self.assertIsNotNone(snapshot.ema_slow)
        self.assertGreater(snapshot.ema_gap_bps, 0)
        self.assertGreater(snapshot.volume_ratio, 1.0)
        self.assertIn("RSI=", snapshot.short_text())


if __name__ == "__main__":
    unittest.main()
