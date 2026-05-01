from __future__ import annotations

from dataclasses import dataclass
import statistics

from cointrading.models import Kline


def sma(values: list[float], window: int) -> float | None:
    if window <= 0:
        raise ValueError("window must be positive")
    if len(values) < window:
        return None
    return sum(values[-window:]) / window


def returns(values: list[float]) -> list[float]:
    output: list[float] = []
    for previous, current in zip(values, values[1:]):
        if previous == 0:
            output.append(0.0)
        else:
            output.append((current / previous) - 1.0)
    return output


def realized_volatility(values: list[float], window: int) -> float | None:
    rets = returns(values)
    if len(rets) < window:
        return None
    return statistics.pstdev(rets[-window:])


@dataclass(frozen=True)
class TechnicalSnapshot:
    interval: str
    sample_count: int
    close: float
    rsi14: float | None
    ema_fast: float | None
    ema_slow: float | None
    ema_gap_bps: float
    ema_slope_bps: float
    bollinger_position: float | None
    bollinger_width_bps: float
    atr_bps: float
    realized_vol_bps: float
    volume_ratio: float
    high_breakout: bool
    low_breakout: bool

    @property
    def enough(self) -> bool:
        return self.sample_count >= 60 and self.rsi14 is not None and self.ema_slow is not None

    def short_text(self) -> str:
        rsi = "n/a" if self.rsi14 is None else f"{self.rsi14:.1f}"
        pos = "n/a" if self.bollinger_position is None else f"{self.bollinger_position:.2f}"
        return (
            f"{self.interval} RSI={rsi} "
            f"EMAgap={self.ema_gap_bps:+.1f}bps "
            f"EMAslope={self.ema_slope_bps:+.1f}bps "
            f"BBpos={pos} ATR={self.atr_bps:.1f}bps "
            f"vol={self.volume_ratio:.2f}x"
        )


def build_technical_snapshot(
    klines: list[Kline],
    *,
    interval: str,
    fast_period: int = 20,
    slow_period: int = 60,
) -> TechnicalSnapshot:
    if not klines:
        return TechnicalSnapshot(
            interval=interval,
            sample_count=0,
            close=0.0,
            rsi14=None,
            ema_fast=None,
            ema_slow=None,
            ema_gap_bps=0.0,
            ema_slope_bps=0.0,
            bollinger_position=None,
            bollinger_width_bps=0.0,
            atr_bps=0.0,
            realized_vol_bps=0.0,
            volume_ratio=0.0,
            high_breakout=False,
            low_breakout=False,
        )

    closes = [row.close for row in klines]
    close = closes[-1]
    fast_series = ema_series(closes, fast_period)
    slow_series = ema_series(closes, slow_period)
    ema_fast = fast_series[-1] if len(closes) >= fast_period else None
    ema_slow = slow_series[-1] if len(closes) >= slow_period else None
    ema_gap_bps = (
        ((ema_fast / ema_slow) - 1.0) * 10_000.0
        if ema_fast is not None and ema_slow is not None and ema_slow > 0
        else 0.0
    )
    ema_slope_bps = 0.0
    if ema_fast is not None and len(fast_series) >= 5 and fast_series[-5] > 0:
        ema_slope_bps = ((fast_series[-1] / fast_series[-5]) - 1.0) * 10_000.0

    bb_position, bb_width = bollinger_position(closes, period=20, width=2.0)
    return TechnicalSnapshot(
        interval=interval,
        sample_count=len(klines),
        close=close,
        rsi14=rsi(closes, period=14),
        ema_fast=ema_fast,
        ema_slow=ema_slow,
        ema_gap_bps=ema_gap_bps,
        ema_slope_bps=ema_slope_bps,
        bollinger_position=bb_position,
        bollinger_width_bps=bb_width,
        atr_bps=atr_bps(klines, lookback=14),
        realized_vol_bps=realized_vol_bps(klines, lookback=20),
        volume_ratio=volume_ratio(klines, lookback=20),
        high_breakout=is_high_breakout(klines, lookback=20),
        low_breakout=is_low_breakout(klines, lookback=20),
    )


def rsi(values: list[float], *, period: int = 14) -> float | None:
    if len(values) <= period:
        return None
    gains = []
    losses = []
    for previous, current in zip(values[-period - 1 : -1], values[-period:]):
        change = current - previous
        if change >= 0:
            gains.append(change)
        else:
            losses.append(abs(change))
    avg_gain = sum(gains) / period
    avg_loss = sum(losses) / period
    if avg_loss == 0:
        return 100.0 if avg_gain > 0 else 50.0
    rs = avg_gain / avg_loss
    return 100.0 - (100.0 / (1.0 + rs))


def ema_series(values: list[float], period: int) -> list[float]:
    if not values:
        return []
    alpha = 2.0 / (period + 1.0)
    result = [values[0]]
    for value in values[1:]:
        result.append((value * alpha) + (result[-1] * (1.0 - alpha)))
    return result


def bollinger_position(
    values: list[float],
    *,
    period: int = 20,
    width: float = 2.0,
) -> tuple[float | None, float]:
    if len(values) < period:
        return None, 0.0
    window = values[-period:]
    mid = sum(window) / len(window)
    deviation = statistics.pstdev(window)
    upper = mid + (deviation * width)
    lower = mid - (deviation * width)
    band_width = upper - lower
    close = window[-1]
    width_bps = (band_width / close) * 10_000.0 if close > 0 else 0.0
    if band_width <= 0:
        return None, width_bps
    return (close - lower) / band_width, width_bps


def atr_bps(klines: list[Kline], *, lookback: int = 14) -> float:
    rows = klines[-lookback:]
    if not rows:
        return 0.0
    close = rows[-1].close
    if close <= 0:
        return 0.0
    true_ranges = []
    previous_close = klines[-lookback - 1].close if len(klines) > lookback else rows[0].open
    for row in rows:
        true_ranges.append(
            max(row.high - row.low, abs(row.high - previous_close), abs(row.low - previous_close))
        )
        previous_close = row.close
    return (sum(true_ranges) / len(true_ranges) / close) * 10_000.0


def realized_vol_bps(klines: list[Kline], *, lookback: int = 20) -> float:
    closes = [row.close for row in klines[-lookback - 1 :]]
    returns = [
        ((current / previous) - 1.0) * 10_000.0
        for previous, current in zip(closes, closes[1:])
        if previous > 0
    ]
    if len(returns) < 2:
        return 0.0
    return statistics.pstdev(returns)


def volume_ratio(klines: list[Kline], *, lookback: int = 20) -> float:
    if len(klines) < 3:
        return 0.0
    rows = klines[-lookback - 1 : -1]
    if not rows:
        return 0.0
    average = sum(row.volume for row in rows) / len(rows)
    return klines[-1].volume / average if average > 0 else 0.0


def is_high_breakout(klines: list[Kline], *, lookback: int = 20) -> bool:
    if len(klines) <= lookback:
        return False
    prior = klines[-lookback - 1 : -1]
    return klines[-1].close > max(row.high for row in prior)


def is_low_breakout(klines: list[Kline], *, lookback: int = 20) -> bool:
    if len(klines) <= lookback:
        return False
    prior = klines[-lookback - 1 : -1]
    return klines[-1].close < min(row.low for row in prior)
