from __future__ import annotations

from dataclasses import dataclass
import json
import statistics
from typing import Iterable

from cointrading.models import Kline
from cointrading.storage import kst_from_ms, now_ms


MACRO_BULL = "macro_bull"
MACRO_BEAR = "macro_bear"
MACRO_RANGE = "macro_range"
MACRO_BREAKOUT = "macro_breakout"
MACRO_PANIC = "macro_panic"


@dataclass(frozen=True)
class MarketRegimeSnapshot:
    symbol: str
    macro_regime: str
    trade_bias: str
    allowed_strategies: tuple[str, ...]
    blocked_reason: str
    last_price: float
    trend_1h_bps: float
    trend_4h_bps: float
    realized_vol_bps: float
    atr_bps: float
    timestamp_ms: int

    def to_text(self) -> str:
        allowed = ", ".join(self.allowed_strategies) if self.allowed_strategies else "없음"
        return "\n".join(
            [
                f"장세 라우터: {self.symbol}",
                f"시각: {kst_from_ms(self.timestamp_ms)}",
                f"큰 장세: {macro_regime_ko(self.macro_regime)}",
                f"거래 편향: {trade_bias_ko(self.trade_bias)}",
                f"허용 전략: {allowed}",
                f"차단 이유: {self.blocked_reason or '없음'}",
                f"현재가: {self.last_price:.6f}",
                f"1시간 추세: {self.trend_1h_bps:.2f} bps",
                f"4시간 추세: {self.trend_4h_bps:.2f} bps",
                f"실현 변동성: {self.realized_vol_bps:.2f} bps",
                f"ATR: {self.atr_bps:.2f} bps",
            ]
        )


def evaluate_market_regime(
    symbol: str,
    klines_15m: list[Kline],
    klines_1h: list[Kline],
    *,
    timestamp_ms: int | None = None,
) -> MarketRegimeSnapshot:
    klines = klines_15m if len(klines_15m) >= 30 else klines_1h
    if len(klines) < 10:
        price = klines[-1].close if klines else 0.0
        return MarketRegimeSnapshot(
            symbol=symbol.upper(),
            macro_regime=MACRO_RANGE,
            trade_bias="neutral",
            allowed_strategies=("observe_only",),
            blocked_reason="insufficient macro candles",
            last_price=price,
            trend_1h_bps=0.0,
            trend_4h_bps=0.0,
            realized_vol_bps=0.0,
            atr_bps=0.0,
            timestamp_ms=timestamp_ms or now_ms(),
        )

    last_price = klines[-1].close
    trend_1h_bps = _trend_bps(klines_15m, bars=4) if len(klines_15m) >= 5 else _trend_bps(klines, 1)
    trend_4h_bps = _trend_bps(klines_1h, bars=4) if len(klines_1h) >= 5 else _trend_bps(klines, 4)
    realized_vol_bps = _realized_vol_bps(klines, lookback=20)
    atr_bps = _atr_bps(klines, lookback=14)
    fast_ma = _sma([item.close for item in klines[-20:]])
    slow_ma = _sma([item.close for item in klines[-60:]]) if len(klines) >= 60 else _sma(
        [item.close for item in klines]
    )
    ma_gap_bps = ((fast_ma / slow_ma) - 1.0) * 10_000.0 if slow_ma > 0 else 0.0

    if atr_bps >= 220.0 or realized_vol_bps >= 140.0:
        return _snapshot(
            symbol,
            MACRO_PANIC,
            "flat",
            (),
            "panic volatility; 신규 진입 금지",
            last_price,
            trend_1h_bps,
            trend_4h_bps,
            realized_vol_bps,
            atr_bps,
            timestamp_ms,
        )
    if atr_bps >= 90.0 and abs(trend_1h_bps) >= 35.0:
        bias = "long" if trend_1h_bps > 0 else "short"
        return _snapshot(
            symbol,
            MACRO_BREAKOUT,
            bias,
            ("breakout_trend_reduced_size",),
            "high volatility expansion; scalping disabled",
            last_price,
            trend_1h_bps,
            trend_4h_bps,
            realized_vol_bps,
            atr_bps,
            timestamp_ms,
        )
    if ma_gap_bps >= 8.0 and trend_4h_bps >= 20.0 and trend_1h_bps >= -15.0:
        return _snapshot(
            symbol,
            MACRO_BULL,
            "long",
            ("trend_long_15m_1h", "pullback_long", "long_scalp_only"),
            "",
            last_price,
            trend_1h_bps,
            trend_4h_bps,
            realized_vol_bps,
            atr_bps,
            timestamp_ms,
        )
    if ma_gap_bps <= -8.0 and trend_4h_bps <= -20.0 and trend_1h_bps <= 15.0:
        return _snapshot(
            symbol,
            MACRO_BEAR,
            "short",
            ("trend_short_15m_1h", "rally_short", "short_scalp_only"),
            "",
            last_price,
            trend_1h_bps,
            trend_4h_bps,
            realized_vol_bps,
            atr_bps,
            timestamp_ms,
        )
    return _snapshot(
        symbol,
        MACRO_RANGE,
        "neutral",
        ("range_mean_reversion", "strict_maker_scalp"),
        "",
        last_price,
        trend_1h_bps,
        trend_4h_bps,
        realized_vol_bps,
        atr_bps,
        timestamp_ms,
    )


def market_regime_rows_text(rows: Iterable) -> str:
    rows = list(rows)
    if not rows:
        return "아직 장세 라우터 기록이 없습니다."
    lines = ["장세 라우터"]
    for row in rows:
        strategies = row["allowed_strategies_json"]
        try:
            allowed = ", ".join(json.loads(strategies)) if strategies else ""
        except json.JSONDecodeError:
            allowed = str(strategies or "")
        lines.append(
            " ".join(
                [
                    f"{kst_from_ms(int(row['timestamp_ms']))}",
                    f"{row['symbol']}",
                    f"{macro_regime_ko(row['macro_regime'])}",
                    f"편향={trade_bias_ko(row['trade_bias'])}",
                    f"1h={float(row['trend_1h_bps']):.1f}bps",
                    f"4h={float(row['trend_4h_bps']):.1f}bps",
                    f"ATR={float(row['atr_bps']):.1f}bps",
                    f"전략={allowed or '없음'}",
                ]
            )
        )
    return "\n".join(lines)


def scalp_allowed_by_macro(row, side: str, *, max_age_ms: int, current_ms: int | None = None) -> tuple[bool, str]:
    if row is None:
        return True, "macro regime unavailable"
    ts = current_ms or now_ms()
    age_ms = ts - int(row["timestamp_ms"])
    if age_ms > max_age_ms:
        return True, "macro regime stale"
    regime = str(row["macro_regime"])
    if regime == MACRO_PANIC:
        return False, "macro router: panic regime blocks new scalps"
    if regime == MACRO_BREAKOUT:
        return False, "macro router: breakout regime routes away from scalping"
    if regime == MACRO_BULL and side == "short":
        return False, "macro router: bull regime blocks short scalps"
    if regime == MACRO_BEAR and side == "long":
        return False, "macro router: bear regime blocks long scalps"
    return True, f"macro router allows {side} in {regime}"


def macro_regime_ko(regime: str) -> str:
    return {
        MACRO_BULL: "상승 추세",
        MACRO_BEAR: "하락 추세",
        MACRO_RANGE: "횡보/레인지",
        MACRO_BREAKOUT: "변동성 돌파",
        MACRO_PANIC: "패닉/진입금지",
    }.get(regime, regime)


def trade_bias_ko(bias: str) -> str:
    return {
        "long": "롱 우위",
        "short": "숏 우위",
        "neutral": "중립",
        "flat": "신규 진입 금지",
    }.get(bias, bias)


def _snapshot(
    symbol: str,
    macro_regime: str,
    trade_bias: str,
    allowed_strategies: tuple[str, ...],
    blocked_reason: str,
    last_price: float,
    trend_1h_bps: float,
    trend_4h_bps: float,
    realized_vol_bps: float,
    atr_bps: float,
    timestamp_ms: int | None,
) -> MarketRegimeSnapshot:
    return MarketRegimeSnapshot(
        symbol=symbol.upper(),
        macro_regime=macro_regime,
        trade_bias=trade_bias,
        allowed_strategies=allowed_strategies,
        blocked_reason=blocked_reason,
        last_price=last_price,
        trend_1h_bps=trend_1h_bps,
        trend_4h_bps=trend_4h_bps,
        realized_vol_bps=realized_vol_bps,
        atr_bps=atr_bps,
        timestamp_ms=timestamp_ms or now_ms(),
    )


def _trend_bps(klines: list[Kline], bars: int) -> float:
    if len(klines) <= bars:
        return 0.0
    start = klines[-bars - 1].close
    end = klines[-1].close
    if start <= 0:
        return 0.0
    return ((end / start) - 1.0) * 10_000.0


def _realized_vol_bps(klines: list[Kline], lookback: int) -> float:
    closes = [item.close for item in klines[-lookback - 1 :]]
    if len(closes) < 3:
        return 0.0
    returns = []
    for previous, current in zip(closes, closes[1:]):
        if previous > 0:
            returns.append(((current / previous) - 1.0) * 10_000.0)
    if len(returns) < 2:
        return 0.0
    return statistics.pstdev(returns)


def _atr_bps(klines: list[Kline], lookback: int) -> float:
    rows = klines[-lookback:]
    if not rows:
        return 0.0
    close = rows[-1].close
    if close <= 0:
        return 0.0
    true_ranges = []
    previous_close = klines[-lookback - 1].close if len(klines) > lookback else rows[0].open
    for row in rows:
        high_low = row.high - row.low
        high_prev = abs(row.high - previous_close)
        low_prev = abs(row.low - previous_close)
        true_ranges.append(max(high_low, high_prev, low_prev))
        previous_close = row.close
    return (sum(true_ranges) / len(true_ranges) / close) * 10_000.0


def _sma(values: list[float]) -> float:
    if not values:
        return 0.0
    return sum(values) / len(values)
