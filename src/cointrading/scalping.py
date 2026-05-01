from __future__ import annotations

from dataclasses import dataclass
import csv
from datetime import datetime, timezone
import os
from pathlib import Path
import time
from typing import Any, Iterable

from cointrading.config import TradingConfig
from cointrading.models import Kline, SignalSide


@dataclass(frozen=True)
class ScalpConfig:
    max_spread_bps: float = 1.5
    min_imbalance: float = 0.18
    min_momentum_bps: float = 1.5
    trend_momentum_bps: float = 6.0
    min_edge_after_maker_bps: float = 1.0
    max_realized_vol_bps: float = 35.0
    panic_realized_vol_bps: float = 60.0
    min_side_depth_notional: float = 50_000.0
    max_abs_funding_rate: float = 0.001
    bnb_futures_fee_discount_pct: float = 0.10
    lookback_bars: int = 5
    depth_levels: int = 10


@dataclass(frozen=True)
class ScalpSignal:
    symbol: str
    side: SignalSide
    reason: str
    regime: str
    trade_allowed: bool
    mid_price: float
    spread_bps: float
    imbalance: float
    momentum_bps: float
    realized_vol_bps: float
    maker_roundtrip_bps: float
    taker_roundtrip_bps: float
    edge_after_maker_bps: float
    book_bid_notional: float
    book_ask_notional: float
    book_depth_notional: float
    bnb_fee_discount_enabled: bool
    bnb_fee_discount_active: bool
    latest_funding_rate: float | None = None

    def to_text(self) -> str:
        funding = "n/a"
        if self.latest_funding_rate is not None:
            funding = f"{self.latest_funding_rate:.5%}"
        bnb_discount = "적용 중" if self.bnb_fee_discount_active else "미적용"
        if self.bnb_fee_discount_enabled and not self.bnb_fee_discount_active:
            bnb_discount = "설정 켜짐, BNB 잔고 부족"
        return "\n".join(
            [
                f"스캘핑 신호: {self.symbol}",
                f"판단: {_side_ko(self.side)}",
                f"장 상태: {_regime_ko(self.regime)}",
                f"진입 허용: {'가능' if self.trade_allowed else '금지'}",
                f"이유: {_reason_ko(self.reason)}",
                f"중간가: {self.mid_price:.4f}",
                f"스프레드: {self.spread_bps:.3f} bps",
                f"호가 불균형: {self.imbalance:.3f}",
                f"모멘텀: {self.momentum_bps:.3f} bps",
                f"단기 변동성: {self.realized_vol_bps:.3f} bps",
                f"메이커 순여유: {self.edge_after_maker_bps:.3f} bps",
                f"상위호가 유동성: {self.book_depth_notional:,.0f} {_quote_asset(self.symbol)}",
                f"메이커 왕복 비용: {self.maker_roundtrip_bps:.2f} bps",
                f"테이커 왕복 비용: {self.taker_roundtrip_bps:.2f} bps",
                f"BNB 수수료 할인: {bnb_discount}",
                f"최근 펀딩비: {funding}",
            ]
        )


class ScalpSignalEngine:
    def __init__(self, config: ScalpConfig | None = None) -> None:
        self.config = config or ScalpConfig()

    def evaluate(
        self,
        symbol: str,
        book_ticker: dict[str, Any],
        order_book: dict[str, Any],
        klines: list[Kline],
        trading_config: TradingConfig,
        commission_rate: dict[str, Any] | None = None,
        latest_funding_rate: float | None = None,
        bnb_fee_discount_enabled: bool = False,
        bnb_balance: float | None = None,
    ) -> ScalpSignal:
        bid = float(book_ticker["bidPrice"])
        ask = float(book_ticker["askPrice"])
        mid = (bid + ask) / 2.0
        spread_bps = ((ask - bid) / mid) * 10_000.0 if mid > 0 else 0.0
        book_bid_notional, book_ask_notional = self._depth_notional(order_book)
        book_depth_notional = book_bid_notional + book_ask_notional
        imbalance = self._depth_imbalance(book_bid_notional, book_ask_notional)
        momentum_bps = self._momentum_bps(klines)
        realized_vol_bps = self._realized_vol_bps(klines)
        maker_rate = _rate_value(
            commission_rate,
            "makerCommissionRate",
            trading_config.maker_fee_rate,
        )
        taker_rate = _rate_value(
            commission_rate,
            "takerCommissionRate",
            trading_config.taker_fee_rate,
        )
        bnb_fee_discount_active = bnb_fee_discount_enabled and (bnb_balance or 0.0) > 0.0
        if bnb_fee_discount_active:
            maker_rate *= 1.0 - self.config.bnb_futures_fee_discount_pct
            taker_rate *= 1.0 - self.config.bnb_futures_fee_discount_pct
        maker_roundtrip_bps = maker_rate * 2.0 * 10_000.0
        taker_roundtrip_bps = taker_rate * 2.0 * 10_000.0
        edge_after_maker_bps = abs(momentum_bps) - maker_roundtrip_bps

        side, reason, regime, trade_allowed = self._decide(
            spread_bps=spread_bps,
            imbalance=imbalance,
            momentum_bps=momentum_bps,
            realized_vol_bps=realized_vol_bps,
            maker_roundtrip_bps=maker_roundtrip_bps,
            book_bid_notional=book_bid_notional,
            book_ask_notional=book_ask_notional,
            latest_funding_rate=latest_funding_rate,
        )
        return ScalpSignal(
            symbol=symbol,
            side=side,
            reason=reason,
            regime=regime,
            trade_allowed=trade_allowed,
            mid_price=mid,
            spread_bps=spread_bps,
            imbalance=imbalance,
            momentum_bps=momentum_bps,
            realized_vol_bps=realized_vol_bps,
            maker_roundtrip_bps=maker_roundtrip_bps,
            taker_roundtrip_bps=taker_roundtrip_bps,
            edge_after_maker_bps=edge_after_maker_bps,
            book_bid_notional=book_bid_notional,
            book_ask_notional=book_ask_notional,
            book_depth_notional=book_depth_notional,
            bnb_fee_discount_enabled=bnb_fee_discount_enabled,
            bnb_fee_discount_active=bnb_fee_discount_active,
            latest_funding_rate=latest_funding_rate,
        )

    def _decide(
        self,
        spread_bps: float,
        imbalance: float,
        momentum_bps: float,
        realized_vol_bps: float,
        maker_roundtrip_bps: float,
        book_bid_notional: float,
        book_ask_notional: float,
        latest_funding_rate: float | None,
    ) -> tuple[SignalSide, str, str, bool]:
        if spread_bps < 0:
            return "flat", "negative spread snapshot", "invalid_spread", False
        if spread_bps > self.config.max_spread_bps:
            return "flat", "spread too wide", "wide_spread", False
        if min(book_bid_notional, book_ask_notional) < self.config.min_side_depth_notional:
            return "flat", "book depth too thin", "thin_book", False
        if realized_vol_bps > self.config.panic_realized_vol_bps:
            return "flat", "panic volatility", "panic_volatility", False
        if realized_vol_bps > self.config.max_realized_vol_bps:
            return "flat", "volatility too high", "high_volatility", False
        if (
            latest_funding_rate is not None
            and abs(latest_funding_rate) > self.config.max_abs_funding_rate
        ):
            return "flat", "funding rate too high", "funding_risk", False

        edge_after_maker_bps = abs(momentum_bps) - maker_roundtrip_bps
        if imbalance >= self.config.min_imbalance and momentum_bps >= self.config.min_momentum_bps:
            if edge_after_maker_bps < self.config.min_edge_after_maker_bps:
                return "flat", "edge too small after fees", "low_edge", False
            return "long", "bid imbalance with positive momentum", "aligned_long", True
        if imbalance <= -self.config.min_imbalance and momentum_bps <= -self.config.min_momentum_bps:
            if edge_after_maker_bps < self.config.min_edge_after_maker_bps:
                return "flat", "edge too small after fees", "low_edge", False
            return "short", "ask imbalance with negative momentum", "aligned_short", True
        if abs(momentum_bps) >= self.config.trend_momentum_bps:
            return "flat", "trend without book confirmation", "trend_without_book", False
        if abs(imbalance) >= self.config.min_imbalance:
            return "flat", "book imbalance without momentum", "book_without_momentum", False
        return "flat", "no aligned microstructure edge", "quiet_chop", False

    def _depth_notional(self, order_book: dict[str, Any]) -> tuple[float, float]:
        bids = order_book.get("bids", [])[: self.config.depth_levels]
        asks = order_book.get("asks", [])[: self.config.depth_levels]
        bid_notional = sum(float(price) * float(qty) for price, qty in bids)
        ask_notional = sum(float(price) * float(qty) for price, qty in asks)
        return bid_notional, ask_notional

    def _depth_imbalance(self, bid_notional: float, ask_notional: float) -> float:
        total = bid_notional + ask_notional
        if total <= 0:
            return 0.0
        return (bid_notional - ask_notional) / total

    def _momentum_bps(self, klines: list[Kline]) -> float:
        lookback = self.config.lookback_bars
        if len(klines) <= lookback:
            return 0.0
        start = klines[-lookback - 1].close
        end = klines[-1].close
        if start <= 0:
            return 0.0
        return ((end / start) - 1.0) * 10_000.0

    def _realized_vol_bps(self, klines: list[Kline]) -> float:
        lookback = self.config.lookback_bars
        closes = [item.close for item in klines]
        if len(closes) <= lookback:
            return 0.0
        returns = []
        for prev, current in zip(closes[-lookback - 1 :], closes[-lookback:]):
            if prev > 0:
                returns.append((current / prev) - 1.0)
        if not returns:
            return 0.0
        mean = sum(returns) / len(returns)
        variance = sum((item - mean) ** 2 for item in returns) / len(returns)
        return (variance**0.5) * 10_000.0


def _rate_value(payload: dict[str, Any] | None, key: str, default: float) -> float:
    if not payload or payload.get(key) in {None, ""}:
        return default
    return float(payload[key])


SCALP_LOG_FIELDS = [
    "timestamp_ms",
    "iso_time",
    "symbol",
    "side",
    "reason",
    "regime",
    "trade_allowed",
    "mid_price",
    "spread_bps",
    "imbalance",
    "momentum_bps",
    "realized_vol_bps",
    "maker_roundtrip_bps",
    "taker_roundtrip_bps",
    "edge_after_maker_bps",
    "book_bid_notional",
    "book_ask_notional",
    "book_depth_notional",
    "bnb_fee_discount_enabled",
    "bnb_fee_discount_active",
    "latest_funding_rate",
    "horizon_1m_bps",
    "horizon_3m_bps",
    "horizon_5m_bps",
]


def default_scalp_log_path() -> Path:
    configured = os.getenv("COINTRADING_SCALP_LOG_PATH", "").strip()
    if configured:
        return Path(configured).expanduser()
    return Path(__file__).resolve().parents[2] / "data" / "scalp_signals.csv"


def append_scalp_signal(path: Path, signal: ScalpSignal) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    _ensure_scalp_log_schema(path)
    exists = path.exists()
    now_ms = int(time.time() * 1000)
    row = {
        "timestamp_ms": str(now_ms),
        "iso_time": datetime.fromtimestamp(now_ms / 1000, timezone.utc).isoformat(),
        "symbol": signal.symbol,
        "side": signal.side,
        "reason": signal.reason,
        "regime": signal.regime,
        "trade_allowed": "true" if signal.trade_allowed else "false",
        "mid_price": f"{signal.mid_price:.12f}",
        "spread_bps": f"{signal.spread_bps:.12f}",
        "imbalance": f"{signal.imbalance:.12f}",
        "momentum_bps": f"{signal.momentum_bps:.12f}",
        "realized_vol_bps": f"{signal.realized_vol_bps:.12f}",
        "maker_roundtrip_bps": f"{signal.maker_roundtrip_bps:.12f}",
        "taker_roundtrip_bps": f"{signal.taker_roundtrip_bps:.12f}",
        "edge_after_maker_bps": f"{signal.edge_after_maker_bps:.12f}",
        "book_bid_notional": f"{signal.book_bid_notional:.12f}",
        "book_ask_notional": f"{signal.book_ask_notional:.12f}",
        "book_depth_notional": f"{signal.book_depth_notional:.12f}",
        "bnb_fee_discount_enabled": "true" if signal.bnb_fee_discount_enabled else "false",
        "bnb_fee_discount_active": "true" if signal.bnb_fee_discount_active else "false",
        "latest_funding_rate": (
            "" if signal.latest_funding_rate is None else f"{signal.latest_funding_rate:.12f}"
        ),
        "horizon_1m_bps": "",
        "horizon_3m_bps": "",
        "horizon_5m_bps": "",
    }
    with path.open("a", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=SCALP_LOG_FIELDS)
        if not exists:
            writer.writeheader()
        writer.writerow(row)


def score_scalp_log(path: Path, current_mid_by_symbol: dict[str, float]) -> int:
    if not path.exists():
        return 0
    _ensure_scalp_log_schema(path)
    with path.open() as file:
        rows = list(csv.DictReader(file))
    now_ms = int(time.time() * 1000)
    updated = 0
    horizons = {
        "horizon_1m_bps": 60_000,
        "horizon_3m_bps": 180_000,
        "horizon_5m_bps": 300_000,
    }
    for row in rows:
        side = row.get("side", "flat")
        if side == "flat":
            continue
        symbol = row["symbol"]
        current_mid = current_mid_by_symbol.get(symbol)
        if current_mid is None:
            continue
        elapsed_ms = now_ms - int(row["timestamp_ms"])
        entry_mid = float(row["mid_price"])
        signed_return_bps = _signed_return_bps(side, entry_mid, current_mid)
        for field, horizon_ms in horizons.items():
            if row.get(field):
                continue
            if elapsed_ms >= horizon_ms:
                row[field] = f"{signed_return_bps:.12f}"
                updated += 1
    if updated:
        with path.open("w", newline="") as file:
            writer = csv.DictWriter(file, fieldnames=SCALP_LOG_FIELDS)
            writer.writeheader()
            writer.writerows(rows)
    return updated


def scalp_report_text(
    path: Path,
    symbol: str | None = None,
    symbols: Iterable[str] | None = None,
) -> str:
    if not path.exists():
        return "아직 스캘핑 신호 로그가 없습니다."
    _ensure_scalp_log_schema(path)
    with path.open() as file:
        rows = list(csv.DictReader(file))
    if symbol:
        rows = [row for row in rows if row.get("symbol") == symbol.upper()]
    elif symbols is not None:
        allowed_symbols = {item.upper() for item in symbols}
        rows = [row for row in rows if row.get("symbol") in allowed_symbols]
    return scalp_report_rows_text(rows, symbol=symbol, symbols=symbols)


def scalp_report_rows_text(
    rows: list[dict[str, str]],
    symbol: str | None = None,
    symbols: Iterable[str] | None = None,
) -> str:
    if not rows:
        if symbol:
            target = symbol.upper()
        elif symbols is not None:
            target = ", ".join(sorted({item.upper() for item in symbols}))
        else:
            target = "all symbols"
        return f"{target} 스캘핑 신호가 아직 없습니다."

    lines = ["스캘핑 dry-run 리포트"]
    if symbol:
        lines.append(f"심볼: {symbol.upper()}")
    elif symbols is not None:
        lines.append(f"대상: {', '.join(sorted({item.upper() for item in symbols}))}")
    lines.append(f"전체 로그: {len(rows)}개")
    for horizon in ("horizon_1m_bps", "horizon_3m_bps", "horizon_5m_bps"):
        scored_rows = _scored_rows(rows, horizon)
        scored = [float(row[horizon]) for row in scored_rows]
        if not scored:
            lines.append(f"{_horizon_ko(horizon)}: 아직 채점된 신호 없음")
            continue
        wins = [value for value in scored if value > 0]
        avg = sum(scored) / len(scored)
        avg_after_maker = avg - _avg_cost(scored_rows, "maker_roundtrip_bps")
        avg_after_taker = avg - _avg_cost(scored_rows, "taker_roundtrip_bps")
        lines.append(
            f"{_horizon_ko(horizon)}: 표본={len(scored)} 승률={len(wins)/len(scored):.1%} "
            f"평균={avg:.3f}bps 메이커순익={avg_after_maker:.3f}bps "
            f"테이커순익={avg_after_taker:.3f}bps"
        )
    lines.extend(_decision_lines(rows))
    lines.extend(_regime_distribution_lines(rows))
    lines.extend(_breakdown_lines(rows, "horizon_5m_bps"))
    return "\n".join(lines)


def _ensure_scalp_log_schema(path: Path) -> None:
    if not path.exists():
        return
    with path.open() as file:
        reader = csv.DictReader(file)
        if reader.fieldnames == SCALP_LOG_FIELDS:
            return
        rows = [{field: row.get(field, "") for field in SCALP_LOG_FIELDS} for row in reader]
    with path.open("w", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=SCALP_LOG_FIELDS)
        writer.writeheader()
        writer.writerows(rows)


def _scored_rows(rows: list[dict[str, str]], horizon: str) -> list[dict[str, str]]:
    return [
        row
        for row in rows
        if row.get("side") in {"long", "short"} and row.get(horizon)
    ]


def _decision_lines(rows: list[dict[str, str]]) -> list[str]:
    scored_rows = _scored_rows(rows, "horizon_5m_bps")
    if not scored_rows:
        return ["판단: 아직 live 진입 판단 불가. dry-run 표본부터 더 모아야 합니다."]
    values = [float(row["horizon_5m_bps"]) for row in scored_rows]
    avg = sum(values) / len(values)
    maker_net = avg - _avg_cost(scored_rows, "maker_roundtrip_bps")
    if len(scored_rows) < 50:
        return [f"판단: 5분 표본 {len(scored_rows)}개라 아직 부족합니다. live 금지."]
    if maker_net <= 0:
        return [f"판단: 5분 메이커순익 평균 {maker_net:.3f}bps라 live 금지."]
    return [
        f"판단: 5분 메이커순익 평균 {maker_net:.3f}bps. 그래도 post-only 테스트 전 live 금지."
    ]


def _regime_distribution_lines(rows: list[dict[str, str]]) -> list[str]:
    counts: dict[str, int] = {}
    for row in rows:
        regime = row.get("regime") or "legacy"
        counts[regime] = counts.get(regime, 0) + 1
    if not counts:
        return []
    top = sorted(counts.items(), key=lambda item: item[1], reverse=True)[:5]
    joined = ", ".join(f"{_regime_ko(regime)} {count}" for regime, count in top)
    return [f"장 상태 분포: {joined}"]


def _breakdown_lines(rows: list[dict[str, str]], horizon: str) -> list[str]:
    scored_rows = _scored_rows(rows, horizon)
    if not scored_rows:
        return []
    lines = ["5분 상세"]
    lines.extend(_group_summary_lines(scored_rows, horizon, "방향", "side", _side_group_ko))
    lines.extend(_group_summary_lines(scored_rows, horizon, "장상태", "regime", _regime_ko))
    return lines


def _group_summary_lines(
    rows: list[dict[str, str]],
    horizon: str,
    label: str,
    field: str,
    name_fn,
) -> list[str]:
    grouped: dict[str, list[dict[str, str]]] = {}
    for row in rows:
        key = row.get(field) or "legacy"
        grouped.setdefault(key, []).append(row)
    lines: list[str] = []
    for key, group_rows in sorted(grouped.items(), key=lambda item: len(item[1]), reverse=True)[:4]:
        values = [float(row[horizon]) for row in group_rows]
        avg = sum(values) / len(values)
        wins = [value for value in values if value > 0]
        maker_net = avg - _avg_cost(group_rows, "maker_roundtrip_bps")
        lines.append(
            f"- {label} {_call_name_fn(name_fn, key)}: 표본={len(values)} "
            f"승률={len(wins)/len(values):.1%} 메이커순익={maker_net:.3f}bps"
        )
    return lines


def _call_name_fn(name_fn, value: str) -> str:
    return name_fn(value)


def _signed_return_bps(side: str, entry_mid: float, current_mid: float) -> float:
    if entry_mid <= 0:
        return 0.0
    raw = ((current_mid / entry_mid) - 1.0) * 10_000.0
    if side == "long":
        return raw
    if side == "short":
        return -raw
    return 0.0


def _avg_cost(rows: list[dict[str, str]], field: str) -> float:
    values = [float(row[field]) for row in rows if row.get(field)]
    if not values:
        return 0.0
    return sum(values) / len(values)


def _side_ko(side: SignalSide) -> str:
    return {
        "long": "롱 후보",
        "short": "숏 후보",
        "flat": "관망",
    }[side]


def _quote_asset(symbol: str) -> str:
    for quote in ("USDC", "USDT", "BUSD"):
        if symbol.upper().endswith(quote):
            return quote
    return "quote"


def _side_group_ko(side: str) -> str:
    return {
        "long": "롱",
        "short": "숏",
        "flat": "관망",
        "legacy": "이전로그",
    }.get(side, side)


def _reason_ko(reason: str) -> str:
    return {
        "spread too wide": "스프레드가 너무 넓음",
        "negative spread snapshot": "호가 스냅샷이 비정상임",
        "book depth too thin": "상위 호가 유동성이 얇음",
        "panic volatility": "급변동 구간",
        "volatility too high": "단기 변동성이 너무 큼",
        "funding rate too high": "펀딩비가 과열됨",
        "edge too small after fees": "수수료 차감 후 여유가 부족함",
        "bid imbalance with positive momentum": "매수호가 우위와 상승 모멘텀 일치",
        "ask imbalance with negative momentum": "매도호가 우위와 하락 모멘텀 일치",
        "trend without book confirmation": "가격은 움직이나 호가 확인이 부족함",
        "book imbalance without momentum": "호가 쏠림은 있으나 가격 모멘텀이 부족함",
        "no aligned microstructure edge": "호가와 모멘텀이 일치하지 않음",
    }.get(reason, reason)


def _regime_ko(regime: str) -> str:
    return {
        "aligned_long": "롱 스캘핑 가능",
        "aligned_short": "숏 스캘핑 가능",
        "wide_spread": "스프레드 위험",
        "invalid_spread": "호가 데이터 이상",
        "thin_book": "유동성 부족",
        "panic_volatility": "급변동 금지",
        "high_volatility": "고변동 금지",
        "funding_risk": "펀딩비 위험",
        "low_edge": "수수료 우위 부족",
        "trend_without_book": "추세/호가 불일치",
        "book_without_momentum": "호가/가격 불일치",
        "quiet_chop": "방향성 약함",
        "legacy": "이전로그",
    }.get(regime, regime)


def _horizon_ko(horizon: str) -> str:
    return {
        "horizon_1m_bps": "1분 후",
        "horizon_3m_bps": "3분 후",
        "horizon_5m_bps": "5분 후",
    }.get(horizon, horizon)
