from __future__ import annotations

import unittest

from cointrading.models import Kline
from cointrading.tactical_radar import (
    RADAR_NEAR,
    RADAR_READY,
    RADAR_WATCH,
    TacticalRadarNotifyState,
    evaluate_tactical_symbol,
    tactical_radar_notification_decision,
    tactical_radar_text,
)


class TacticalRadarTests(unittest.TestCase):
    def test_upside_impulse_reports_watch_pullback_instead_of_silence(self) -> None:
        h1 = _trend_klines(step=0.10)
        values = [100.0 + i * 0.03 for i in range(112)]
        close = values[-1]
        for _ in range(8):
            close *= 1.0008
            values.append(close)
        klines_15m = [_kline(i, value, volume=300.0 if i >= 119 else 100.0) for i, value in enumerate(values)]

        signal = evaluate_tactical_symbol("BTCUSDC", klines_15m, h1, timestamp_ms=123)

        self.assertEqual(signal.decision, RADAR_WATCH)
        self.assertEqual(signal.scenario, "impulse_up_wait_pullback")
        self.assertIn("추격 금지", signal.reason)
        text = tactical_radar_text([signal])
        self.assertIn("상방 임펄스 눌림대기", text)
        self.assertIn("추격", text)

    def test_pullback_in_uptrend_reports_near_candidate(self) -> None:
        h1 = _trend_klines(step=0.10)
        values = [100.0 + i * 0.06 for i in range(95)]
        values.extend(
            [
                105.7,
                106.0,
                106.3,
                106.6,
                106.9,
                107.2,
                107.0,
                106.7,
                106.4,
                106.1,
                105.9,
                105.75,
                105.6,
                105.65,
                105.7,
                105.75,
                105.8,
                105.85,
                105.9,
                105.95,
                106.0,
                106.05,
                106.1,
                106.15,
                106.2,
            ]
        )
        klines_15m = [_kline(i, value, volume=120.0) for i, value in enumerate(values[-120:])]

        signal = evaluate_tactical_symbol("ETHUSDC", klines_15m, h1, timestamp_ms=123)

        self.assertEqual(signal.decision, RADAR_NEAR)
        self.assertEqual(signal.scenario, "pullback_long")
        self.assertIsNotNone(signal.trigger_price)
        self.assertIsNotNone(signal.stop_price)
        self.assertIsNotNone(signal.target_price)

    def test_watch_signal_can_send_periodic_notification(self) -> None:
        signal = evaluate_tactical_symbol(
            "BTCUSDC",
            [_kline(i, 100.0 + i * 0.05, volume=300.0 if i == 119 else 100.0) for i in range(120)],
            _trend_klines(step=0.10),
            timestamp_ms=123,
        )
        should_send, reason, _, selected = tactical_radar_notification_decision(
            [signal],
            TacticalRadarNotifyState(),
            periodic_minutes=30,
        )

        self.assertTrue(should_send)
        self.assertEqual(reason, "전술 레이더 주기 보고")
        self.assertEqual(selected, [signal])

    def test_major_level_retest_long_reports_ready(self) -> None:
        h1 = _trend_klines(step=0.10)
        values = [100.8 for _ in range(112)] + [100.6, 100.35, 100.2, 100.5, 100.7, 100.85, 101.0, 101.2]
        klines_15m = [
            _custom_kline(i, close, high=close * 1.001, low=100.12 if i == 114 else close * 0.999, volume=120.0)
            for i, close in enumerate(values)
        ]
        klines_4h = _major_breakout_4h(direction="up")

        signal = evaluate_tactical_symbol("BTCUSDC", klines_15m, h1, klines_4h, timestamp_ms=123)

        self.assertEqual(signal.decision, RADAR_READY)
        self.assertEqual(signal.scenario, "breakout_retest_long")
        self.assertEqual(signal.side, "long")
        self.assertGreater(signal.target_price or 0.0, signal.current_price)
        self.assertLess(signal.stop_price or 999.0, signal.current_price)

    def test_major_level_retest_short_reports_ready(self) -> None:
        h1 = _trend_klines(step=-0.10)
        values = [99.2 for _ in range(112)] + [99.4, 99.65, 99.85, 99.5, 99.25, 99.05, 98.95, 98.8]
        klines_15m = [
            _custom_kline(i, close, high=99.9 if i == 114 else close * 1.001, low=close * 0.999, volume=120.0)
            for i, close in enumerate(values)
        ]
        klines_4h = _major_breakout_4h(direction="down")

        signal = evaluate_tactical_symbol("BTCUSDC", klines_15m, h1, klines_4h, timestamp_ms=123)

        self.assertEqual(signal.decision, RADAR_READY)
        self.assertEqual(signal.scenario, "breakout_retest_short")
        self.assertEqual(signal.side, "short")
        self.assertLess(signal.target_price or 999.0, signal.current_price)
        self.assertGreater(signal.stop_price or 0.0, signal.current_price)


def _trend_klines(*, step: float) -> list[Kline]:
    return [_kline(i, 100.0 + i * step, volume=100.0) for i in range(120)]


def _kline(index: int, close: float, *, volume: float) -> Kline:
    return Kline(
        open_time=index * 60_000,
        open=close * 0.999,
        high=close * 1.001,
        low=close * 0.999,
        close=close,
        volume=volume,
        close_time=((index + 1) * 60_000) - 1,
    )


def _custom_kline(index: int, close: float, *, high: float, low: float, volume: float) -> Kline:
    return Kline(
        open_time=index * 60_000,
        open=close * 0.999,
        high=high,
        low=low,
        close=close,
        volume=volume,
        close_time=((index + 1) * 60_000) - 1,
    )


def _major_breakout_4h(*, direction: str) -> list[Kline]:
    rows: list[Kline] = []
    if direction == "up":
        for index in range(98):
            rows.append(_custom_kline(index, 99.2, high=100.0, low=98.6, volume=100.0))
        rows.append(_custom_kline(98, 101.0, high=101.2, low=99.7, volume=140.0))
        rows.append(_custom_kline(99, 101.1, high=101.3, low=100.4, volume=80.0))
    else:
        for index in range(98):
            rows.append(_custom_kline(index, 100.8, high=101.4, low=100.0, volume=100.0))
        rows.append(_custom_kline(98, 99.0, high=100.3, low=98.8, volume=140.0))
        rows.append(_custom_kline(99, 98.9, high=99.6, low=98.7, volume=80.0))
    return rows


if __name__ == "__main__":
    unittest.main()
