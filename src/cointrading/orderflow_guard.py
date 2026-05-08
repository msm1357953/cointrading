"""Lightweight BTCUSDC orderflow guard fed by Binance websocket streams.

The service keeps only a short rolling window in memory and writes one small
JSON snapshot per second. Trading engines can then make a cheap synchronous
decision without opening their own websocket connection.
"""
from __future__ import annotations

import asyncio
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone
import json
import time
from pathlib import Path
from typing import Any

from cointrading.config import TradingConfig
from cointrading.storage import kst_from_ms


SEVERITY = {
    "DISABLED": 0,
    "NORMAL": 0,
    "CAUTION": 1,
    "UNKNOWN": 2,
    "STALE": 2,
    "DANGER": 3,
}


def default_output_path(config: TradingConfig | None = None) -> Path:
    cfg = config or TradingConfig.from_env()
    path = Path(cfg.orderflow_guard_path)
    if path.is_absolute():
        return path
    return Path(__file__).resolve().parents[2] / path


def _now_ms() -> int:
    return int(time.time() * 1000)


def _float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _max_status(*statuses: str) -> str:
    return max(statuses, key=lambda item: SEVERITY.get(item, 0))


def _book_depth_within(
    levels: list[tuple[float, float]],
    *,
    mid: float,
    pct: float,
    side: str,
) -> float:
    if mid <= 0:
        return 0.0
    if side == "bid":
        cutoff = mid * (1.0 - pct)
        return sum(price * qty for price, qty in levels if price >= cutoff)
    cutoff = mid * (1.0 + pct)
    return sum(price * qty for price, qty in levels if price <= cutoff)


def _safe_ratio(numerator: float, denominator: float, default: float = 1.0) -> float:
    if denominator <= 0:
        return default
    return numerator / denominator


@dataclass(frozen=True)
class OrderflowSnapshot:
    symbol: str
    updated_ms: int
    status: str
    long_status: str
    short_status: str
    reason: str
    long_reason: str
    short_reason: str
    data: dict[str, Any]

    @property
    def age_seconds(self) -> float:
        if self.updated_ms <= 0:
            return 999999.0
        return max(0.0, (_now_ms() - self.updated_ms) / 1000.0)

    def side_status(self, side: str) -> str:
        return self.long_status if side == "long" else self.short_status

    def side_reason(self, side: str) -> str:
        return self.long_reason if side == "long" else self.short_reason


class OrderflowWindow:
    def __init__(self, *, window_seconds: float) -> None:
        self.window_ms = max(1000, int(window_seconds * 1000))
        self.depth_rows: deque[dict[str, Any]] = deque()
        self.trade_rows: deque[dict[str, Any]] = deque()
        self.last_depth: dict[str, Any] | None = None

    def add_depth(
        self,
        *,
        event_ms: int,
        bids: list[tuple[float, float]],
        asks: list[tuple[float, float]],
    ) -> None:
        if not bids or not asks:
            return
        bid = bids[0][0]
        ask = asks[0][0]
        if bid <= 0 or ask <= 0 or ask < bid:
            return
        mid = (bid + ask) / 2.0
        row = {
            "event_ms": event_ms,
            "bid": bid,
            "ask": ask,
            "mid": mid,
            "spread_bps": (ask - bid) / mid * 10_000.0 if mid > 0 else 0.0,
            "bid_depth_005": _book_depth_within(bids, mid=mid, pct=0.0005, side="bid"),
            "ask_depth_005": _book_depth_within(asks, mid=mid, pct=0.0005, side="ask"),
            "bid_depth_010": _book_depth_within(bids, mid=mid, pct=0.0010, side="bid"),
            "ask_depth_010": _book_depth_within(asks, mid=mid, pct=0.0010, side="ask"),
            "bid_depth_020": _book_depth_within(bids, mid=mid, pct=0.0020, side="bid"),
            "ask_depth_020": _book_depth_within(asks, mid=mid, pct=0.0020, side="ask"),
        }
        self.depth_rows.append(row)
        self.last_depth = row
        self.prune(event_ms)

    def add_trade(
        self,
        *,
        event_ms: int,
        price: float,
        quantity: float,
        buyer_is_maker: bool,
    ) -> None:
        if price <= 0 or quantity <= 0:
            return
        notional = price * quantity
        self.trade_rows.append({
            "event_ms": event_ms,
            "notional": notional,
            "taker_buy": 0.0 if buyer_is_maker else notional,
            "taker_sell": notional if buyer_is_maker else 0.0,
        })
        self.prune(event_ms)

    def prune(self, now_ms: int) -> None:
        cutoff = now_ms - self.window_ms
        while self.depth_rows and int(self.depth_rows[0]["event_ms"]) < cutoff:
            self.depth_rows.popleft()
        while self.trade_rows and int(self.trade_rows[0]["event_ms"]) < cutoff:
            self.trade_rows.popleft()

    def to_snapshot(self, *, symbol: str, config: TradingConfig, now_ms: int | None = None) -> dict[str, Any]:
        ts = now_ms or _now_ms()
        self.prune(ts)
        if self.last_depth is None:
            return _snapshot_dict(
                symbol=symbol,
                updated_ms=ts,
                status="UNKNOWN",
                long_status="UNKNOWN",
                short_status="UNKNOWN",
                reason="아직 호가창 데이터가 없습니다",
                long_reason="호가창 미수신",
                short_reason="호가창 미수신",
                extra={
                    "window_seconds": self.window_ms / 1000.0,
                    "depth_events": 0,
                    "trade_events": len(self.trade_rows),
                },
            )

        latest = dict(self.last_depth)
        mids = [float(row["mid"]) for row in self.depth_rows if float(row.get("mid", 0.0)) > 0]
        first_mid = mids[0] if mids else latest["mid"]
        mid_velocity_bps = (
            (latest["mid"] - first_mid) / first_mid * 10_000.0
            if first_mid > 0 else 0.0
        )
        max_bid_depth = max(
            [float(row.get("bid_depth_010", 0.0)) for row in self.depth_rows] or [0.0]
        )
        max_ask_depth = max(
            [float(row.get("ask_depth_010", 0.0)) for row in self.depth_rows] or [0.0]
        )
        bid_depth_drop = 1.0 - _safe_ratio(float(latest["bid_depth_010"]), max_bid_depth)
        ask_depth_drop = 1.0 - _safe_ratio(float(latest["ask_depth_010"]), max_ask_depth)

        taker_buy = sum(float(row["taker_buy"]) for row in self.trade_rows)
        taker_sell = sum(float(row["taker_sell"]) for row in self.trade_rows)
        taker_total = taker_buy + taker_sell
        taker_buy_ratio = taker_buy / taker_total if taker_total > 0 else 0.5
        taker_sell_ratio = taker_sell / taker_total if taker_total > 0 else 0.5

        bid_ask_ratio = _safe_ratio(float(latest["bid_depth_010"]), float(latest["ask_depth_010"]))
        ask_bid_ratio = _safe_ratio(float(latest["ask_depth_010"]), float(latest["bid_depth_010"]))
        latest.update({
            "bid_ask_ratio_010": bid_ask_ratio,
            "ask_bid_ratio_010": ask_bid_ratio,
            "bid_depth_drop_010": max(0.0, bid_depth_drop),
            "ask_depth_drop_010": max(0.0, ask_depth_drop),
            "taker_buy_usdc": taker_buy,
            "taker_sell_usdc": taker_sell,
            "taker_buy_ratio": taker_buy_ratio,
            "taker_sell_ratio": taker_sell_ratio,
            "mid_velocity_bps": mid_velocity_bps,
            "window_seconds": self.window_ms / 1000.0,
            "depth_events": len(self.depth_rows),
            "trade_events": len(self.trade_rows),
        })
        long_status, long_reason = _side_status("long", latest, config)
        short_status, short_reason = _side_status("short", latest, config)
        global_status = _max_status(long_status, short_status)
        reason = "; ".join(
            item
            for item in (
                f"롱 {long_status}: {long_reason}" if long_status != "NORMAL" else "",
                f"숏 {short_status}: {short_reason}" if short_status != "NORMAL" else "",
            )
            if item
        ) or "정상"
        return _snapshot_dict(
            symbol=symbol,
            updated_ms=ts,
            status=global_status,
            long_status=long_status,
            short_status=short_status,
            reason=reason,
            long_reason=long_reason,
            short_reason=short_reason,
            extra=latest,
        )


def _side_status(side: str, row: dict[str, Any], config: TradingConfig) -> tuple[str, str]:
    spread = float(row.get("spread_bps", 0.0))
    if spread >= config.orderflow_guard_spread_danger_bps:
        return "DANGER", f"스프레드 확대 {spread:.2f}bps"

    if side == "long":
        depth_drop = float(row.get("bid_depth_drop_010", 0.0))
        book_ratio = float(row.get("bid_ask_ratio_010", 1.0))
        taker_ratio = float(row.get("taker_sell_ratio", 0.5))
        flow_label = "시장가 매도"
        velocity = float(row.get("mid_velocity_bps", 0.0))
        danger_velocity = velocity <= -config.orderflow_guard_velocity_danger_bps
        caution_velocity = velocity <= -config.orderflow_guard_velocity_caution_bps
    else:
        depth_drop = float(row.get("ask_depth_drop_010", 0.0))
        book_ratio = float(row.get("ask_bid_ratio_010", 1.0))
        taker_ratio = float(row.get("taker_buy_ratio", 0.5))
        flow_label = "시장가 매수"
        velocity = float(row.get("mid_velocity_bps", 0.0))
        danger_velocity = velocity >= config.orderflow_guard_velocity_danger_bps
        caution_velocity = velocity >= config.orderflow_guard_velocity_caution_bps

    total_flow = float(row.get("taker_buy_usdc", 0.0)) + float(row.get("taker_sell_usdc", 0.0))
    if depth_drop >= config.orderflow_guard_depth_drop_danger:
        return "DANGER", f"내 쪽 0.1% depth 급감 {depth_drop * 100:.0f}%"
    if book_ratio <= config.orderflow_guard_imbalance_danger:
        return "DANGER", f"내 쪽 호가 얇음 ratio={book_ratio:.2f}"
    if total_flow >= config.orderflow_guard_min_trade_notional_usdc and (
        taker_ratio >= config.orderflow_guard_taker_ratio_danger
    ):
        return "DANGER", f"{flow_label} 쏠림 {taker_ratio * 100:.0f}%"
    if danger_velocity:
        return "DANGER", f"불리한 가격속도 {velocity:+.2f}bps"

    cautions: list[str] = []
    if depth_drop >= config.orderflow_guard_depth_drop_caution:
        cautions.append(f"depth 감소 {depth_drop * 100:.0f}%")
    if book_ratio <= config.orderflow_guard_imbalance_caution:
        cautions.append(f"호가 ratio={book_ratio:.2f}")
    if total_flow >= config.orderflow_guard_min_trade_notional_usdc and (
        taker_ratio >= config.orderflow_guard_taker_ratio_caution
    ):
        cautions.append(f"{flow_label} {taker_ratio * 100:.0f}%")
    if caution_velocity:
        cautions.append(f"가격속도 {velocity:+.2f}bps")
    if cautions:
        return "CAUTION", ", ".join(cautions)
    return "NORMAL", "정상"


def _snapshot_dict(
    *,
    symbol: str,
    updated_ms: int,
    status: str,
    long_status: str,
    short_status: str,
    reason: str,
    long_reason: str,
    short_reason: str,
    extra: dict[str, Any],
) -> dict[str, Any]:
    return {
        "symbol": symbol,
        "updated_ms": updated_ms,
        "updated_iso": datetime.fromtimestamp(updated_ms / 1000, timezone.utc).isoformat(),
        "status": status,
        "long_status": long_status,
        "short_status": short_status,
        "reason": reason,
        "long_reason": long_reason,
        "short_reason": short_reason,
        **extra,
    }


def write_snapshot(path: Path, snapshot: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    tmp_path.write_text(json.dumps(snapshot, ensure_ascii=False, indent=2, sort_keys=True))
    tmp_path.replace(path)


def load_latest_snapshot(
    config: TradingConfig | None = None,
    *,
    path: Path | None = None,
    now_ms: int | None = None,
) -> OrderflowSnapshot:
    cfg = config or TradingConfig.from_env()
    if not cfg.orderflow_guard_enabled:
        return OrderflowSnapshot(
            symbol=cfg.orderflow_guard_symbol,
            updated_ms=now_ms or _now_ms(),
            status="DISABLED",
            long_status="NORMAL",
            short_status="NORMAL",
            reason="orderflow guard disabled",
            long_reason="disabled",
            short_reason="disabled",
            data={},
        )
    p = path or default_output_path(cfg)
    if not p.exists():
        return OrderflowSnapshot(
            symbol=cfg.orderflow_guard_symbol,
            updated_ms=0,
            status="UNKNOWN",
            long_status="DANGER",
            short_status="DANGER",
            reason="호가창 센서 파일 없음",
            long_reason="호가창 센서 파일 없음",
            short_reason="호가창 센서 파일 없음",
            data={},
        )
    try:
        data = json.loads(p.read_text())
    except (OSError, json.JSONDecodeError) as exc:
        return OrderflowSnapshot(
            symbol=cfg.orderflow_guard_symbol,
            updated_ms=0,
            status="UNKNOWN",
            long_status="DANGER",
            short_status="DANGER",
            reason=f"호가창 센서 읽기 실패: {exc}",
            long_reason="호가창 센서 읽기 실패",
            short_reason="호가창 센서 읽기 실패",
            data={},
        )
    updated_ms = int(_float(data.get("updated_ms"), 0.0))
    age = ((now_ms or _now_ms()) - updated_ms) / 1000.0 if updated_ms > 0 else 999999.0
    status = str(data.get("status", "UNKNOWN"))
    long_status = str(data.get("long_status", "UNKNOWN"))
    short_status = str(data.get("short_status", "UNKNOWN"))
    reason = str(data.get("reason", ""))
    long_reason = str(data.get("long_reason", ""))
    short_reason = str(data.get("short_reason", ""))
    if age > cfg.orderflow_guard_stale_seconds:
        status = "STALE"
        long_status = "DANGER"
        short_status = "DANGER"
        reason = f"호가창 센서 오래됨 age={age:.1f}s"
        long_reason = reason
        short_reason = reason
    return OrderflowSnapshot(
        symbol=str(data.get("symbol", cfg.orderflow_guard_symbol)),
        updated_ms=updated_ms,
        status=status,
        long_status=long_status,
        short_status=short_status,
        reason=reason,
        long_reason=long_reason,
        short_reason=short_reason,
        data=data,
    )


def orderflow_guard_text(
    config: TradingConfig | None = None,
    *,
    path: Path | None = None,
) -> str:
    cfg = config or TradingConfig.from_env()
    snap = load_latest_snapshot(cfg, path=path)
    data = snap.data
    age = snap.age_seconds
    updated = "없음" if snap.updated_ms <= 0 else kst_from_ms(snap.updated_ms)
    lines = [
        "■ 호가창/orderflow 상태",
        f"심볼: {snap.symbol}",
        f"업데이트: {updated} (age {age:.1f}s)",
        f"전체: {snap.status} - {snap.reason}",
        f"롱 진입: {snap.long_status} - {snap.long_reason}",
        f"숏 진입: {snap.short_status} - {snap.short_reason}",
    ]
    if data:
        lines.extend([
            "",
            f"bid/ask: {_float(data.get('bid')):.2f} / {_float(data.get('ask')):.2f}",
            f"스프레드: {_float(data.get('spread_bps')):.3f} bps",
            (
                "0.1% depth: "
                f"bid {_float(data.get('bid_depth_010')):,.0f} / "
                f"ask {_float(data.get('ask_depth_010')):,.0f} USDC"
            ),
            (
                "호가 비율: "
                f"bid/ask {_float(data.get('bid_ask_ratio_010')):.2f}, "
                f"ask/bid {_float(data.get('ask_bid_ratio_010')):.2f}"
            ),
            (
                "depth 감소: "
                f"bid {_float(data.get('bid_depth_drop_010')) * 100:.0f}% / "
                f"ask {_float(data.get('ask_depth_drop_010')) * 100:.0f}%"
            ),
            (
                "체결흐름: "
                f"매수 {_float(data.get('taker_buy_usdc')):,.0f} / "
                f"매도 {_float(data.get('taker_sell_usdc')):,.0f} USDC"
            ),
            f"가격속도({cfg.orderflow_guard_window_seconds:.0f}s): {_float(data.get('mid_velocity_bps')):+.2f} bps",
        ])
    lines.extend([
        "",
        "해석: DANGER가 연속 확정되면 띠기 신규 진입 차단/미체결 진입 취소, CAUTION이나 관찰중 DANGER는 신규 겹수를 1개로 축소.",
    ])
    return "\n".join(lines)


async def _consume_depth(url: str, window: OrderflowWindow) -> None:
    import websockets

    async for websocket in websockets.connect(url, ping_interval=20, ping_timeout=20):
        try:
            async for raw in websocket:
                payload = json.loads(raw)
                data = payload.get("data", payload)
                event_ms = int(_float(data.get("E") or data.get("T"), _now_ms()))
                bids = [(_float(price), _float(qty)) for price, qty in data.get("b", [])]
                asks = [(_float(price), _float(qty)) for price, qty in data.get("a", [])]
                window.add_depth(event_ms=event_ms, bids=bids, asks=asks)
        except Exception:
            await asyncio.sleep(1.0)
            continue


async def _consume_trades(url: str, window: OrderflowWindow) -> None:
    import websockets

    async for websocket in websockets.connect(url, ping_interval=20, ping_timeout=20):
        try:
            async for raw in websocket:
                payload = json.loads(raw)
                data = payload.get("data", payload)
                event_ms = int(_float(data.get("E") or data.get("T"), _now_ms()))
                window.add_trade(
                    event_ms=event_ms,
                    price=_float(data.get("p")),
                    quantity=_float(data.get("q")),
                    buyer_is_maker=bool(data.get("m")),
                )
        except Exception:
            await asyncio.sleep(1.0)
            continue


async def _snapshot_writer(
    *,
    symbol: str,
    config: TradingConfig,
    window: OrderflowWindow,
    output: Path,
) -> None:
    while True:
        snapshot = window.to_snapshot(symbol=symbol, config=config)
        write_snapshot(output, snapshot)
        await asyncio.sleep(max(0.2, config.orderflow_guard_write_interval_seconds))


async def run_guard_forever(
    *,
    symbol: str | None = None,
    output: Path | None = None,
    config: TradingConfig | None = None,
) -> None:
    cfg = config or TradingConfig.from_env()
    sym = (symbol or cfg.orderflow_guard_symbol).upper()
    out = output or default_output_path(cfg)
    stream_symbol = sym.lower()
    depth_url = f"wss://fstream.binance.com/stream?streams={stream_symbol}@depth20@100ms"
    trade_url = f"wss://fstream.binance.com/stream?streams={stream_symbol}@aggTrade"
    window = OrderflowWindow(window_seconds=cfg.orderflow_guard_window_seconds)
    await asyncio.gather(
        _consume_depth(depth_url, window),
        _consume_trades(trade_url, window),
        _snapshot_writer(symbol=sym, config=cfg, window=window, output=out),
    )


def run_guard_forever_cmd(*, symbol: str | None = None, output: Path | None = None) -> None:
    asyncio.run(run_guard_forever(symbol=symbol, output=output))
