from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable

from cointrading.exchange.binance_usdm import BinanceUSDMClient
from cointrading.storage import kst_from_ms, now_ms


@dataclass(frozen=True)
class MarketContextSnapshot:
    symbol: str
    mark_price: float
    index_price: float
    premium_bps: float
    funding_rate: float | None
    next_funding_ms: int | None
    open_interest: float | None
    bid_price: float
    ask_price: float
    spread_bps: float
    top_bid_notional: float
    top_ask_notional: float
    depth_bid_notional: float
    depth_ask_notional: float
    depth_imbalance: float
    timestamp_ms: int
    raw: dict[str, Any]

    def to_text(self) -> str:
        funding = "n/a" if self.funding_rate is None else f"{self.funding_rate * 10_000:.3f}bps"
        open_interest = "n/a" if self.open_interest is None else f"{self.open_interest:.4f}"
        return "\n".join(
            [
                f"시장상황: {self.symbol}",
                f"시각: {kst_from_ms(self.timestamp_ms)}",
                f"mark/index: {self.mark_price:.8f} / {self.index_price:.8f}",
                f"프리미엄: {self.premium_bps:.3f}bps",
                f"펀딩: {funding}",
                f"미결제약정: {open_interest}",
                f"스프레드: {self.spread_bps:.3f}bps",
                f"1호가 유동성: bid {self.top_bid_notional:.2f}, ask {self.top_ask_notional:.2f}",
                f"호가 깊이: bid {self.depth_bid_notional:.2f}, ask {self.depth_ask_notional:.2f}",
                f"호가 불균형: {self.depth_imbalance:.3f}",
            ]
        )


def collect_market_context(
    client: BinanceUSDMClient,
    symbol: str,
    *,
    depth_limit: int = 20,
    timestamp_ms: int | None = None,
) -> MarketContextSnapshot:
    symbol = symbol.upper()
    ts = timestamp_ms or now_ms()
    ticker = client.book_ticker(symbol)
    mark = client.mark_price(symbol)
    open_interest_row = client.open_interest(symbol)
    depth = client.order_book(symbol, limit=depth_limit)

    bid_price = _float(ticker.get("bidPrice"))
    ask_price = _float(ticker.get("askPrice"))
    bid_qty = _float(ticker.get("bidQty"))
    ask_qty = _float(ticker.get("askQty"))
    mid = (bid_price + ask_price) / 2.0 if bid_price > 0 and ask_price > 0 else 0.0
    spread_bps = ((ask_price - bid_price) / mid) * 10_000.0 if mid > 0 else 0.0

    mark_price = _float(mark.get("markPrice"))
    index_price = _float(mark.get("indexPrice"))
    premium_bps = ((mark_price / index_price) - 1.0) * 10_000.0 if index_price > 0 else 0.0
    funding_rate = _optional_float(mark.get("lastFundingRate"))
    next_funding_ms = _optional_int(mark.get("nextFundingTime"))
    open_interest = _optional_float(open_interest_row.get("openInterest"))

    depth_bid_notional = _side_notional(depth.get("bids", []))
    depth_ask_notional = _side_notional(depth.get("asks", []))
    depth_total = depth_bid_notional + depth_ask_notional
    depth_imbalance = (
        (depth_bid_notional - depth_ask_notional) / depth_total if depth_total > 0 else 0.0
    )

    return MarketContextSnapshot(
        symbol=symbol,
        mark_price=mark_price,
        index_price=index_price,
        premium_bps=premium_bps,
        funding_rate=funding_rate,
        next_funding_ms=next_funding_ms,
        open_interest=open_interest,
        bid_price=bid_price,
        ask_price=ask_price,
        spread_bps=spread_bps,
        top_bid_notional=bid_price * bid_qty,
        top_ask_notional=ask_price * ask_qty,
        depth_bid_notional=depth_bid_notional,
        depth_ask_notional=depth_ask_notional,
        depth_imbalance=depth_imbalance,
        timestamp_ms=ts,
        raw={"ticker": ticker, "mark": mark, "open_interest": open_interest_row, "depth": depth},
    )


def market_context_rows_text(rows: Iterable) -> str:
    rows = list(rows)
    if not rows:
        return "시장상황 기록이 아직 없습니다."
    lines = ["시장상황 수집"]
    for row in rows:
        funding = row["funding_rate"]
        funding_text = "n/a" if funding is None else f"{float(funding) * 10_000:.3f}bps"
        open_interest = row["open_interest"]
        oi_text = "n/a" if open_interest is None else f"{float(open_interest):.4f}"
        lines.append(
            " ".join(
                [
                    f"{kst_from_ms(int(row['timestamp_ms']))}",
                    str(row["symbol"]),
                    f"mark={float(row['mark_price']):.6f}",
                    f"premium={float(row['premium_bps']):.3f}bps",
                    f"funding={funding_text}",
                    f"OI={oi_text}",
                    f"spread={float(row['spread_bps']):.3f}bps",
                    f"depth={float(row['depth_bid_notional']) + float(row['depth_ask_notional']):.2f}",
                    f"imb={float(row['depth_imbalance']):.3f}",
                ]
            )
        )
    return "\n".join(lines)


def _side_notional(levels: Iterable) -> float:
    total = 0.0
    for row in levels:
        try:
            price = float(row[0])
            qty = float(row[1])
        except (TypeError, ValueError, IndexError):
            continue
        total += price * qty
    return total


def _float(value: Any) -> float:
    return float(value or 0.0)


def _optional_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    return float(value)


def _optional_int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    return int(value)
