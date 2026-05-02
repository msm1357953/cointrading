from __future__ import annotations

from datetime import date
import unittest

from cointrading.config import TradingConfig
from cointrading.historical_data import HistoricalKlineResult
from cointrading.meta_strategy import (
    backtest_meta_policy,
    meta_results_text,
    run_meta_backtest,
)
from cointrading.models import Kline


class MetaStrategyTests(unittest.TestCase):
    def test_meta_policy_generates_trades_from_clear_uptrend(self) -> None:
        klines = _trend_klines(520)
        trades, decisions = backtest_meta_policy(
            symbol="BTCUSDC",
            interval="15m",
            klines=klines,
            config=TradingConfig(
                taker_fee_rate=0.0,
                slippage_bps=0.0,
                trend_take_profit_bps=45.0,
                trend_stop_loss_bps=30.0,
                trend_max_hold_seconds=7200.0,
                breakout_take_profit_bps=45.0,
                breakout_stop_loss_bps=30.0,
                breakout_max_hold_seconds=7200.0,
            ),
            notional=100.0,
        )

        self.assertGreater(len(trades), 0)
        self.assertTrue(any(trade.side == "long" for trade in trades))
        self.assertTrue(decisions)

    def test_meta_report_text_explains_single_policy(self) -> None:
        history = HistoricalKlineResult(
            symbol="BTCUSDC",
            interval="15m",
            start_date=date(2025, 1, 1),
            end_date=date(2025, 1, 6),
            klines=_trend_klines(520),
            source_files=[],
            missing_urls=[],
        )
        result = run_meta_backtest(
            history=history,
            config=TradingConfig(taker_fee_rate=0.0, slippage_bps=0.0),
            notional=100.0,
        )
        text = meta_results_text([result])

        self.assertIn("상황판단형 메타전략", text)
        self.assertIn("장세 판단", text)
        self.assertIn("BTCUSDC", text)


def _trend_klines(count: int) -> list[Kline]:
    rows = []
    price = 100.0
    for index in range(count):
        open_price = price
        if index % 4 == 3:
            close = open_price * 0.9980
        else:
            close = open_price * 1.0015
        high = close * 1.002
        low = open_price * 0.999
        open_time = index * 900_000
        rows.append(
            Kline(
                open_time=open_time,
                open=open_price,
                high=high,
                low=low,
                close=close,
                volume=100.0 + index,
                close_time=open_time + 899_999,
            )
        )
        price = close
    return rows


if __name__ == "__main__":
    unittest.main()
