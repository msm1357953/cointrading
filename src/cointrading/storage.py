from __future__ import annotations

import csv
from dataclasses import asdict
from datetime import datetime, timezone
import json
from pathlib import Path
import sqlite3
import time
from typing import Any, Iterable

from cointrading.models import OrderIntent
from cointrading.scalping import SCALP_LOG_FIELDS, ScalpSignal


SIGNAL_NUMERIC_FIELDS = {
    "timestamp_ms",
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
    "latest_funding_rate",
    "horizon_1m_bps",
    "horizon_3m_bps",
    "horizon_5m_bps",
}


def default_db_path() -> Path:
    return Path(__file__).resolve().parents[2] / "data" / "cointrading.sqlite"


def now_ms() -> int:
    return int(time.time() * 1000)


def iso_from_ms(timestamp_ms: int) -> str:
    return datetime.fromtimestamp(timestamp_ms / 1000, timezone.utc).isoformat()


class TradingStore:
    def __init__(self, path: Path | None = None) -> None:
        self.path = path or default_db_path()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._init_schema()

    def connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("PRAGMA foreign_keys=ON")
        return connection

    def _init_schema(self) -> None:
        with self.connect() as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS signals (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp_ms INTEGER NOT NULL,
                    iso_time TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    side TEXT NOT NULL,
                    reason TEXT NOT NULL,
                    regime TEXT NOT NULL,
                    trade_allowed INTEGER NOT NULL,
                    mid_price REAL NOT NULL,
                    spread_bps REAL NOT NULL,
                    imbalance REAL NOT NULL,
                    momentum_bps REAL NOT NULL,
                    realized_vol_bps REAL NOT NULL,
                    maker_roundtrip_bps REAL NOT NULL,
                    taker_roundtrip_bps REAL NOT NULL,
                    edge_after_maker_bps REAL NOT NULL,
                    book_bid_notional REAL NOT NULL,
                    book_ask_notional REAL NOT NULL,
                    book_depth_notional REAL NOT NULL,
                    bnb_fee_discount_enabled INTEGER NOT NULL,
                    bnb_fee_discount_active INTEGER NOT NULL,
                    latest_funding_rate REAL,
                    horizon_1m_bps REAL,
                    horizon_3m_bps REAL,
                    horizon_5m_bps REAL,
                    UNIQUE(timestamp_ms, symbol)
                );

                CREATE INDEX IF NOT EXISTS idx_signals_symbol_time
                    ON signals(symbol, timestamp_ms);
                CREATE INDEX IF NOT EXISTS idx_signals_side_time
                    ON signals(side, timestamp_ms);

                CREATE TABLE IF NOT EXISTS orders (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp_ms INTEGER NOT NULL,
                    iso_time TEXT NOT NULL,
                    client_order_id TEXT,
                    signal_id INTEGER,
                    symbol TEXT NOT NULL,
                    side TEXT NOT NULL,
                    order_type TEXT NOT NULL,
                    time_in_force TEXT,
                    quantity REAL NOT NULL,
                    price REAL,
                    reduce_only INTEGER NOT NULL,
                    dry_run INTEGER NOT NULL,
                    status TEXT NOT NULL,
                    reason TEXT,
                    intent_json TEXT NOT NULL,
                    response_json TEXT,
                    FOREIGN KEY(signal_id) REFERENCES signals(id)
                );

                CREATE INDEX IF NOT EXISTS idx_orders_symbol_time
                    ON orders(symbol, timestamp_ms);

                CREATE TABLE IF NOT EXISTS fills (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp_ms INTEGER NOT NULL,
                    iso_time TEXT NOT NULL,
                    order_id INTEGER,
                    exchange_trade_id TEXT,
                    symbol TEXT NOT NULL,
                    side TEXT NOT NULL,
                    price REAL NOT NULL,
                    quantity REAL NOT NULL,
                    commission REAL NOT NULL,
                    commission_asset TEXT NOT NULL,
                    realized_pnl REAL,
                    raw_json TEXT,
                    FOREIGN KEY(order_id) REFERENCES orders(id)
                );

                CREATE INDEX IF NOT EXISTS idx_fills_symbol_time
                    ON fills(symbol, timestamp_ms);

                CREATE TABLE IF NOT EXISTS fee_snapshots (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp_ms INTEGER NOT NULL,
                    iso_time TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    maker_bps REAL NOT NULL,
                    taker_bps REAL NOT NULL,
                    bnb_fee_discount_enabled INTEGER NOT NULL,
                    bnb_fee_discount_active INTEGER NOT NULL,
                    raw_json TEXT
                );

                CREATE INDEX IF NOT EXISTS idx_fee_snapshots_symbol_time
                    ON fee_snapshots(symbol, timestamp_ms);
                """
            )

    def insert_signal(self, signal: ScalpSignal, timestamp_ms: int | None = None) -> int:
        ts = timestamp_ms or now_ms()
        payload = _signal_payload(signal, ts)
        columns = list(payload)
        placeholders = ", ".join("?" for _ in columns)
        updates = ", ".join(
            f"{column}=excluded.{column}"
            for column in columns
            if column not in {"timestamp_ms", "symbol"}
        )
        sql = (
            f"INSERT INTO signals ({', '.join(columns)}) VALUES ({placeholders}) "
            f"ON CONFLICT(timestamp_ms, symbol) DO UPDATE SET {updates}"
        )
        with self.connect() as connection:
            cursor = connection.execute(sql, [payload[column] for column in columns])
            if cursor.lastrowid:
                return int(cursor.lastrowid)
            row = connection.execute(
                "SELECT id FROM signals WHERE timestamp_ms=? AND symbol=?",
                (payload["timestamp_ms"], payload["symbol"]),
            ).fetchone()
            return int(row["id"])

    def update_signal_scores(
        self,
        signal_id: int,
        scores: dict[str, float],
    ) -> None:
        if not scores:
            return
        allowed = {"horizon_1m_bps", "horizon_3m_bps", "horizon_5m_bps"}
        updates = {key: value for key, value in scores.items() if key in allowed}
        if not updates:
            return
        assignment = ", ".join(f"{field}=?" for field in updates)
        with self.connect() as connection:
            connection.execute(
                f"UPDATE signals SET {assignment} WHERE id=?",
                [*updates.values(), signal_id],
            )

    def list_signals(
        self,
        symbol: str | None = None,
        symbols: Iterable[str] | None = None,
        limit: int | None = None,
    ) -> list[dict[str, str]]:
        where: list[str] = []
        params: list[Any] = []
        if symbol:
            where.append("symbol=?")
            params.append(symbol.upper())
        elif symbols is not None:
            active = [item.upper() for item in symbols]
            if not active:
                return []
            where.append(f"symbol IN ({', '.join('?' for _ in active)})")
            params.extend(active)
        sql = "SELECT * FROM signals"
        if where:
            sql += " WHERE " + " AND ".join(where)
        sql += " ORDER BY timestamp_ms ASC"
        if limit is not None:
            sql += " LIMIT ?"
            params.append(limit)
        with self.connect() as connection:
            return [_signal_row_to_report(row) for row in connection.execute(sql, params)]

    def pending_score_rows(
        self,
        current_timestamp_ms: int | None = None,
    ) -> list[sqlite3.Row]:
        ts = current_timestamp_ms or now_ms()
        with self.connect() as connection:
            return list(
                connection.execute(
                    """
                    SELECT * FROM signals
                    WHERE side IN ('long', 'short')
                      AND (
                        (horizon_1m_bps IS NULL AND ? - timestamp_ms >= 60000)
                        OR (horizon_3m_bps IS NULL AND ? - timestamp_ms >= 180000)
                        OR (horizon_5m_bps IS NULL AND ? - timestamp_ms >= 300000)
                      )
                    ORDER BY timestamp_ms ASC
                    """,
                    (ts, ts, ts),
                )
            )

    def migrate_csv_signals(self, path: Path) -> int:
        if not path.exists():
            return 0
        count = 0
        with path.open() as file:
            for row in csv.DictReader(file):
                payload = _csv_signal_payload(row)
                columns = list(payload)
                placeholders = ", ".join("?" for _ in columns)
                updates = ", ".join(
                    f"{column}=excluded.{column}"
                    for column in columns
                    if column not in {"timestamp_ms", "symbol"}
                )
                sql = (
                    f"INSERT INTO signals ({', '.join(columns)}) VALUES ({placeholders}) "
                    f"ON CONFLICT(timestamp_ms, symbol) DO UPDATE SET {updates}"
                )
                with self.connect() as connection:
                    connection.execute(sql, [payload[column] for column in columns])
                count += 1
        return count

    def insert_order_attempt(
        self,
        intent: OrderIntent,
        *,
        status: str,
        dry_run: bool,
        reason: str = "",
        response: dict[str, Any] | None = None,
        signal_id: int | None = None,
        timestamp_ms: int | None = None,
    ) -> int:
        ts = timestamp_ms or now_ms()
        intent_payload = asdict(intent)
        with self.connect() as connection:
            cursor = connection.execute(
                """
                INSERT INTO orders (
                    timestamp_ms, iso_time, client_order_id, signal_id, symbol, side,
                    order_type, time_in_force, quantity, price, reduce_only, dry_run,
                    status, reason, intent_json, response_json
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    ts,
                    iso_from_ms(ts),
                    intent.client_order_id,
                    signal_id,
                    intent.symbol,
                    intent.side,
                    intent.order_type,
                    intent.time_in_force,
                    intent.quantity,
                    intent.price,
                    1 if intent.reduce_only else 0,
                    1 if dry_run else 0,
                    status,
                    reason,
                    json.dumps(intent_payload, sort_keys=True),
                    json.dumps(response, sort_keys=True) if response is not None else None,
                ),
            )
            return int(cursor.lastrowid)

    def record_fee_snapshot(
        self,
        symbol: str,
        maker_bps: float,
        taker_bps: float,
        *,
        bnb_fee_discount_enabled: bool,
        bnb_fee_discount_active: bool,
        raw: dict[str, Any] | None = None,
        timestamp_ms: int | None = None,
    ) -> int:
        ts = timestamp_ms or now_ms()
        with self.connect() as connection:
            cursor = connection.execute(
                """
                INSERT INTO fee_snapshots (
                    timestamp_ms, iso_time, symbol, maker_bps, taker_bps,
                    bnb_fee_discount_enabled, bnb_fee_discount_active, raw_json
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    ts,
                    iso_from_ms(ts),
                    symbol.upper(),
                    maker_bps,
                    taker_bps,
                    1 if bnb_fee_discount_enabled else 0,
                    1 if bnb_fee_discount_active else 0,
                    json.dumps(raw, sort_keys=True) if raw is not None else None,
                ),
            )
            return int(cursor.lastrowid)

    def recent_orders(self, limit: int = 10) -> list[sqlite3.Row]:
        with self.connect() as connection:
            return list(
                connection.execute(
                    "SELECT * FROM orders ORDER BY timestamp_ms DESC LIMIT ?",
                    (limit,),
                )
            )

    def summary_counts(self) -> dict[str, int]:
        with self.connect() as connection:
            return {
                "signals": int(connection.execute("SELECT COUNT(*) FROM signals").fetchone()[0]),
                "orders": int(connection.execute("SELECT COUNT(*) FROM orders").fetchone()[0]),
                "fills": int(connection.execute("SELECT COUNT(*) FROM fills").fetchone()[0]),
                "fee_snapshots": int(
                    connection.execute("SELECT COUNT(*) FROM fee_snapshots").fetchone()[0]
                ),
            }


def _signal_payload(signal: ScalpSignal, timestamp_ms: int) -> dict[str, Any]:
    return {
        "timestamp_ms": timestamp_ms,
        "iso_time": iso_from_ms(timestamp_ms),
        "symbol": signal.symbol,
        "side": signal.side,
        "reason": signal.reason,
        "regime": signal.regime,
        "trade_allowed": 1 if signal.trade_allowed else 0,
        "mid_price": signal.mid_price,
        "spread_bps": signal.spread_bps,
        "imbalance": signal.imbalance,
        "momentum_bps": signal.momentum_bps,
        "realized_vol_bps": signal.realized_vol_bps,
        "maker_roundtrip_bps": signal.maker_roundtrip_bps,
        "taker_roundtrip_bps": signal.taker_roundtrip_bps,
        "edge_after_maker_bps": signal.edge_after_maker_bps,
        "book_bid_notional": signal.book_bid_notional,
        "book_ask_notional": signal.book_ask_notional,
        "book_depth_notional": signal.book_depth_notional,
        "bnb_fee_discount_enabled": 1 if signal.bnb_fee_discount_enabled else 0,
        "bnb_fee_discount_active": 1 if signal.bnb_fee_discount_active else 0,
        "latest_funding_rate": signal.latest_funding_rate,
        "horizon_1m_bps": None,
        "horizon_3m_bps": None,
        "horizon_5m_bps": None,
    }


def _csv_signal_payload(row: dict[str, str]) -> dict[str, Any]:
    timestamp_ms = int(float(row["timestamp_ms"]))
    payload: dict[str, Any] = {
        "timestamp_ms": timestamp_ms,
        "iso_time": row.get("iso_time") or iso_from_ms(timestamp_ms),
        "symbol": row.get("symbol", ""),
        "side": row.get("side", "flat"),
        "reason": row.get("reason", ""),
        "regime": row.get("regime") or "legacy",
        "trade_allowed": _bool_int(row.get("trade_allowed")),
        "mid_price": _float_or_zero(row.get("mid_price")),
        "spread_bps": _float_or_zero(row.get("spread_bps")),
        "imbalance": _float_or_zero(row.get("imbalance")),
        "momentum_bps": _float_or_zero(row.get("momentum_bps")),
        "realized_vol_bps": _float_or_zero(row.get("realized_vol_bps")),
        "maker_roundtrip_bps": _float_or_zero(row.get("maker_roundtrip_bps")),
        "taker_roundtrip_bps": _float_or_zero(row.get("taker_roundtrip_bps")),
        "edge_after_maker_bps": _float_or_zero(row.get("edge_after_maker_bps")),
        "book_bid_notional": _float_or_zero(row.get("book_bid_notional")),
        "book_ask_notional": _float_or_zero(row.get("book_ask_notional")),
        "book_depth_notional": _float_or_zero(row.get("book_depth_notional")),
        "bnb_fee_discount_enabled": _bool_int(row.get("bnb_fee_discount_enabled")),
        "bnb_fee_discount_active": _bool_int(row.get("bnb_fee_discount_active")),
        "latest_funding_rate": _float_or_none(row.get("latest_funding_rate")),
        "horizon_1m_bps": _float_or_none(row.get("horizon_1m_bps")),
        "horizon_3m_bps": _float_or_none(row.get("horizon_3m_bps")),
        "horizon_5m_bps": _float_or_none(row.get("horizon_5m_bps")),
    }
    return payload


def _signal_row_to_report(row: sqlite3.Row) -> dict[str, str]:
    result: dict[str, str] = {}
    for field in SCALP_LOG_FIELDS:
        value = row[field]
        if value is None:
            result[field] = ""
        elif field in {"trade_allowed", "bnb_fee_discount_enabled", "bnb_fee_discount_active"}:
            result[field] = "true" if int(value) else "false"
        else:
            result[field] = str(value)
    return result


def _float_or_zero(value: str | None) -> float:
    parsed = _float_or_none(value)
    return 0.0 if parsed is None else parsed


def _float_or_none(value: str | None) -> float | None:
    if value is None or str(value).strip() == "":
        return None
    return float(value)


def _bool_int(value: str | None) -> int:
    if value is None:
        return 0
    return 1 if str(value).strip().lower() in {"1", "true", "yes", "y", "on"} else 0
