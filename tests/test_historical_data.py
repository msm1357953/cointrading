from __future__ import annotations

from datetime import date
from pathlib import Path
import tempfile
import unittest
import zipfile

from cointrading.historical_data import (
    _cache_path,
    binance_vision_kline_url,
    load_binance_vision_klines,
)


class HistoricalDataTests(unittest.TestCase):
    def test_loads_cached_binance_vision_monthly_klines(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            url = binance_vision_kline_url(
                symbol="BTCUSDC",
                interval="15m",
                period="monthly",
                value_date=date(2025, 1, 1),
            )
            path = _cache_path(root, url)
            path.parent.mkdir(parents=True, exist_ok=True)
            with zipfile.ZipFile(path, "w") as archive:
                archive.writestr(
                    "BTCUSDC-15m-2025-01.csv",
                    "\n".join(
                        [
                            "open_time,open,high,low,close,volume,close_time",
                            "1735689600000,100,101,99,100.5,10,1735690499999",
                            "1735690500000,100.5,102,100,101.5,12,1735691399999",
                        ]
                    ),
                )

            result = load_binance_vision_klines(
                symbol="BTCUSDC",
                interval="15m",
                start_date="2025-01-01",
                end_date="2025-01-01",
                history_dir=root,
            )

        self.assertEqual(result.symbol, "BTCUSDC")
        self.assertEqual(len(result.klines), 2)
        self.assertEqual(result.klines[0].open, 100.0)
        self.assertEqual(len(result.source_files), 1)
        self.assertEqual(result.missing_urls, [])


if __name__ == "__main__":
    unittest.main()
