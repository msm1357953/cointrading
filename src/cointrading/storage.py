from __future__ import annotations

import csv
from contextlib import contextmanager
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import sqlite3
import time
from typing import Any, Iterable, Iterator

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


KST = timezone(timedelta(hours=9), name="KST")


def kst_from_ms(timestamp_ms: int) -> str:
    return datetime.fromtimestamp(timestamp_ms / 1000, KST).strftime("%Y-%m-%d %H:%M:%S KST")


class TradingStore:
    def __init__(self, path: Path | None = None) -> None:
        self.path = path or default_db_path()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._init_schema()

    @contextmanager
    def connect(self) -> Iterator[sqlite3.Connection]:
        connection = sqlite3.connect(self.path)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("PRAGMA foreign_keys=ON")
        try:
            yield connection
        except Exception:
            connection.rollback()
            raise
        else:
            connection.commit()
        finally:
            connection.close()

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

                CREATE TABLE IF NOT EXISTS scalp_cycles (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    created_ms INTEGER NOT NULL,
                    created_iso TEXT NOT NULL,
                    updated_ms INTEGER NOT NULL,
                    updated_iso TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    side TEXT NOT NULL,
                    status TEXT NOT NULL,
                    reason TEXT,
                    entry_signal_id INTEGER,
                    entry_order_id INTEGER,
                    exit_order_id INTEGER,
                    quantity REAL NOT NULL,
                    entry_price REAL NOT NULL,
                    target_price REAL NOT NULL,
                    stop_price REAL NOT NULL,
                    maker_one_way_bps REAL NOT NULL,
                    taker_one_way_bps REAL NOT NULL,
                    entry_deadline_ms INTEGER NOT NULL,
                    exit_deadline_ms INTEGER,
                    max_hold_deadline_ms INTEGER,
                    opened_ms INTEGER,
                    closed_ms INTEGER,
                    last_mid_price REAL,
                    realized_pnl REAL,
                    reprice_count INTEGER NOT NULL DEFAULT 0,
                    strategy_evaluation_id INTEGER,
                    strategy_take_profit_bps REAL,
                    strategy_stop_loss_bps REAL,
                    strategy_max_hold_seconds INTEGER,
                    FOREIGN KEY(entry_signal_id) REFERENCES signals(id),
                    FOREIGN KEY(entry_order_id) REFERENCES orders(id),
                    FOREIGN KEY(exit_order_id) REFERENCES orders(id),
                    FOREIGN KEY(strategy_evaluation_id) REFERENCES strategy_evaluations(id)
                );

                CREATE INDEX IF NOT EXISTS idx_scalp_cycles_symbol_status
                    ON scalp_cycles(symbol, status, updated_ms);

                CREATE TABLE IF NOT EXISTS market_regimes (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp_ms INTEGER NOT NULL,
                    iso_time TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    macro_regime TEXT NOT NULL,
                    trade_bias TEXT NOT NULL,
                    allowed_strategies_json TEXT NOT NULL,
                    blocked_reason TEXT,
                    last_price REAL NOT NULL,
                    trend_1h_bps REAL NOT NULL,
                    trend_4h_bps REAL NOT NULL,
                    realized_vol_bps REAL NOT NULL,
                    atr_bps REAL NOT NULL,
                    UNIQUE(timestamp_ms, symbol)
                );

                CREATE INDEX IF NOT EXISTS idx_market_regimes_symbol_time
                    ON market_regimes(symbol, timestamp_ms);

                CREATE TABLE IF NOT EXISTS strategy_evaluations (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    evaluated_ms INTEGER NOT NULL,
                    evaluated_iso TEXT NOT NULL,
                    source TEXT NOT NULL,
                    execution_mode TEXT NOT NULL DEFAULT 'maker_post_only',
                    symbol TEXT NOT NULL,
                    regime TEXT NOT NULL,
                    side TEXT NOT NULL,
                    take_profit_bps REAL NOT NULL,
                    stop_loss_bps REAL NOT NULL,
                    max_hold_seconds INTEGER NOT NULL,
                    sample_count INTEGER NOT NULL,
                    win_count INTEGER NOT NULL,
                    loss_count INTEGER NOT NULL,
                    win_rate REAL NOT NULL,
                    avg_pnl_bps REAL NOT NULL,
                    sum_pnl_bps REAL NOT NULL,
                    avg_win_bps REAL,
                    avg_loss_bps REAL,
                    decision TEXT NOT NULL,
                    reason TEXT NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_strategy_evaluations_latest
                    ON strategy_evaluations(
                        source, symbol, regime, side,
                        take_profit_bps, stop_loss_bps, max_hold_seconds, evaluated_ms
                    );
                """
            )
            _ensure_column(
                connection,
                "strategy_evaluations",
                "execution_mode",
                "TEXT NOT NULL DEFAULT 'maker_post_only'",
            )
            _ensure_column(connection, "scalp_cycles", "strategy_evaluation_id", "INTEGER")
            _ensure_column(connection, "scalp_cycles", "strategy_take_profit_bps", "REAL")
            _ensure_column(connection, "scalp_cycles", "strategy_stop_loss_bps", "REAL")
            _ensure_column(connection, "scalp_cycles", "strategy_max_hold_seconds", "INTEGER")
            connection.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_strategy_evaluations_latest_mode
                    ON strategy_evaluations(
                        source, execution_mode, symbol, regime, side,
                        take_profit_bps, stop_loss_bps, max_hold_seconds, evaluated_ms
                    )
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

    def order_by_id(self, order_id: int) -> sqlite3.Row | None:
        with self.connect() as connection:
            return connection.execute(
                "SELECT * FROM orders WHERE id=?",
                (order_id,),
            ).fetchone()

    def update_order_attempt(
        self,
        order_id: int,
        *,
        status: str | None = None,
        reason: str | None = None,
        response: dict[str, Any] | None = None,
    ) -> None:
        updates: dict[str, Any] = {}
        if status is not None:
            updates["status"] = status
        if reason is not None:
            updates["reason"] = reason
        if response is not None:
            updates["response_json"] = json.dumps(response, sort_keys=True)
        if not updates:
            return
        assignment = ", ".join(f"{field}=?" for field in updates)
        with self.connect() as connection:
            connection.execute(
                f"UPDATE orders SET {assignment} WHERE id=?",
                [*updates.values(), order_id],
            )

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

    def record_fill(
        self,
        *,
        order_id: int | None,
        symbol: str,
        side: str,
        price: float,
        quantity: float,
        commission: float,
        commission_asset: str,
        realized_pnl: float | None = None,
        exchange_trade_id: str | None = None,
        raw: dict[str, Any] | None = None,
        timestamp_ms: int | None = None,
    ) -> int:
        ts = timestamp_ms or now_ms()
        with self.connect() as connection:
            cursor = connection.execute(
                """
                INSERT INTO fills (
                    timestamp_ms, iso_time, order_id, exchange_trade_id, symbol, side,
                    price, quantity, commission, commission_asset, realized_pnl, raw_json
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    ts,
                    iso_from_ms(ts),
                    order_id,
                    exchange_trade_id,
                    symbol.upper(),
                    side,
                    price,
                    quantity,
                    commission,
                    commission_asset,
                    realized_pnl,
                    json.dumps(raw, sort_keys=True) if raw is not None else None,
                ),
            )
            return int(cursor.lastrowid)

    def insert_market_regime(self, snapshot) -> int:
        with self.connect() as connection:
            cursor = connection.execute(
                """
                INSERT INTO market_regimes (
                    timestamp_ms, iso_time, symbol, macro_regime, trade_bias,
                    allowed_strategies_json, blocked_reason, last_price,
                    trend_1h_bps, trend_4h_bps, realized_vol_bps, atr_bps
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(timestamp_ms, symbol) DO UPDATE SET
                    iso_time=excluded.iso_time,
                    macro_regime=excluded.macro_regime,
                    trade_bias=excluded.trade_bias,
                    allowed_strategies_json=excluded.allowed_strategies_json,
                    blocked_reason=excluded.blocked_reason,
                    last_price=excluded.last_price,
                    trend_1h_bps=excluded.trend_1h_bps,
                    trend_4h_bps=excluded.trend_4h_bps,
                    realized_vol_bps=excluded.realized_vol_bps,
                    atr_bps=excluded.atr_bps
                """,
                (
                    int(snapshot.timestamp_ms),
                    iso_from_ms(int(snapshot.timestamp_ms)),
                    snapshot.symbol.upper(),
                    snapshot.macro_regime,
                    snapshot.trade_bias,
                    json.dumps(list(snapshot.allowed_strategies), sort_keys=True),
                    snapshot.blocked_reason,
                    float(snapshot.last_price),
                    float(snapshot.trend_1h_bps),
                    float(snapshot.trend_4h_bps),
                    float(snapshot.realized_vol_bps),
                    float(snapshot.atr_bps),
                ),
            )
            if cursor.lastrowid:
                return int(cursor.lastrowid)
            row = connection.execute(
                "SELECT id FROM market_regimes WHERE timestamp_ms=? AND symbol=?",
                (int(snapshot.timestamp_ms), snapshot.symbol.upper()),
            ).fetchone()
            return int(row["id"])

    def latest_market_regime(self, symbol: str) -> sqlite3.Row | None:
        with self.connect() as connection:
            return connection.execute(
                """
                SELECT *
                FROM market_regimes
                WHERE symbol=?
                ORDER BY timestamp_ms DESC
                LIMIT 1
                """,
                (symbol.upper(),),
            ).fetchone()

    def latest_market_regimes(
        self,
        *,
        symbols: Iterable[str] | None = None,
        limit: int = 100,
    ) -> list[sqlite3.Row]:
        params: list[Any] = []
        where = ""
        if symbols is not None:
            active = [symbol.upper() for symbol in symbols]
            if not active:
                return []
            where = f"WHERE symbol IN ({', '.join('?' for _ in active)})"
            params.extend(active)
        params.append(limit)
        with self.connect() as connection:
            return list(
                connection.execute(
                    f"""
                    SELECT *
                    FROM market_regimes
                    {where}
                    ORDER BY timestamp_ms DESC
                    LIMIT ?
                    """,
                    params,
                )
            )

    def current_market_regimes(self, *, symbols: Iterable[str] | None = None) -> list[sqlite3.Row]:
        params: list[Any] = []
        symbol_filter = ""
        if symbols is not None:
            active = [symbol.upper() for symbol in symbols]
            if not active:
                return []
            symbol_filter = f"AND m.symbol IN ({', '.join('?' for _ in active)})"
            params.extend(active)
        with self.connect() as connection:
            return list(
                connection.execute(
                    f"""
                    SELECT m.*
                    FROM market_regimes m
                    JOIN (
                        SELECT symbol, MAX(timestamp_ms) AS timestamp_ms
                        FROM market_regimes
                        GROUP BY symbol
                    ) latest
                      ON latest.symbol=m.symbol
                     AND latest.timestamp_ms=m.timestamp_ms
                    WHERE 1=1
                    {symbol_filter}
                    ORDER BY m.symbol ASC
                    """,
                    params,
                )
            )

    def insert_scalp_cycle(
        self,
        *,
        symbol: str,
        side: str,
        status: str,
        quantity: float,
        entry_price: float,
        target_price: float,
        stop_price: float,
        maker_one_way_bps: float,
        taker_one_way_bps: float,
        entry_deadline_ms: int,
        reason: str = "",
        entry_signal_id: int | None = None,
        entry_order_id: int | None = None,
        exit_order_id: int | None = None,
        exit_deadline_ms: int | None = None,
        max_hold_deadline_ms: int | None = None,
        opened_ms: int | None = None,
        last_mid_price: float | None = None,
        strategy_evaluation_id: int | None = None,
        strategy_take_profit_bps: float | None = None,
        strategy_stop_loss_bps: float | None = None,
        strategy_max_hold_seconds: int | None = None,
        timestamp_ms: int | None = None,
    ) -> int:
        ts = timestamp_ms or now_ms()
        with self.connect() as connection:
            cursor = connection.execute(
                """
                INSERT INTO scalp_cycles (
                    created_ms, created_iso, updated_ms, updated_iso, symbol, side,
                    status, reason, entry_signal_id, entry_order_id, exit_order_id,
                    quantity, entry_price, target_price, stop_price,
                    maker_one_way_bps, taker_one_way_bps, entry_deadline_ms,
                    exit_deadline_ms, max_hold_deadline_ms, opened_ms, last_mid_price,
                    strategy_evaluation_id, strategy_take_profit_bps,
                    strategy_stop_loss_bps, strategy_max_hold_seconds
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    ts,
                    iso_from_ms(ts),
                    ts,
                    iso_from_ms(ts),
                    symbol.upper(),
                    side,
                    status,
                    reason,
                    entry_signal_id,
                    entry_order_id,
                    exit_order_id,
                    quantity,
                    entry_price,
                    target_price,
                    stop_price,
                    maker_one_way_bps,
                    taker_one_way_bps,
                    entry_deadline_ms,
                    exit_deadline_ms,
                    max_hold_deadline_ms,
                    opened_ms,
                    last_mid_price,
                    strategy_evaluation_id,
                    strategy_take_profit_bps,
                    strategy_stop_loss_bps,
                    strategy_max_hold_seconds,
                ),
            )
            return int(cursor.lastrowid)

    def update_scalp_cycle(
        self,
        cycle_id: int,
        *,
        timestamp_ms: int | None = None,
        **fields: Any,
    ) -> None:
        allowed = {
            "status",
            "reason",
            "exit_order_id",
            "quantity",
            "entry_price",
            "target_price",
            "stop_price",
            "entry_deadline_ms",
            "exit_deadline_ms",
            "max_hold_deadline_ms",
            "opened_ms",
            "closed_ms",
            "last_mid_price",
            "realized_pnl",
            "reprice_count",
        }
        updates = {key: value for key, value in fields.items() if key in allowed}
        ts = timestamp_ms or now_ms()
        updates["updated_ms"] = ts
        updates["updated_iso"] = iso_from_ms(ts)
        if not updates:
            return
        assignment = ", ".join(f"{field}=?" for field in updates)
        with self.connect() as connection:
            connection.execute(
                f"UPDATE scalp_cycles SET {assignment} WHERE id=?",
                [*updates.values(), cycle_id],
            )

    def active_scalp_cycle(self, symbol: str) -> sqlite3.Row | None:
        with self.connect() as connection:
            return connection.execute(
                """
                SELECT * FROM scalp_cycles
                WHERE symbol=? AND status IN ('ENTRY_SUBMITTED', 'OPEN', 'EXIT_SUBMITTED')
                ORDER BY updated_ms DESC
                LIMIT 1
                """,
                (symbol.upper(),),
            ).fetchone()

    def active_scalp_cycles(self) -> list[sqlite3.Row]:
        with self.connect() as connection:
            return list(
                connection.execute(
                    """
                    SELECT * FROM scalp_cycles
                    WHERE status IN ('ENTRY_SUBMITTED', 'OPEN', 'EXIT_SUBMITTED')
                    ORDER BY updated_ms DESC
                    """
                )
            )

    def recent_scalp_cycles(self, limit: int = 10) -> list[sqlite3.Row]:
        with self.connect() as connection:
            return list(
                connection.execute(
                    "SELECT * FROM scalp_cycles ORDER BY updated_ms DESC LIMIT ?",
                    (limit,),
                )
            )

    def scalp_cycle_exit_reasons(self) -> list[sqlite3.Row]:
        with self.connect() as connection:
            return list(
                connection.execute(
                    """
                    SELECT status, reason, COUNT(*) AS count,
                           AVG(realized_pnl) AS avg_pnl,
                           SUM(COALESCE(realized_pnl, 0)) AS sum_pnl
                    FROM scalp_cycles
                    GROUP BY status, reason
                    ORDER BY count DESC
                    """
                )
            )

    def scalp_cycle_performance(self) -> list[sqlite3.Row]:
        with self.connect() as connection:
            return list(
                connection.execute(
                    """
                    SELECT symbol, side, COUNT(*) AS count,
                           SUM(CASE WHEN status='CLOSED' THEN 1 ELSE 0 END) AS wins,
                           SUM(CASE WHEN status='STOPPED' THEN 1 ELSE 0 END) AS losses,
                           AVG(realized_pnl) AS avg_pnl,
                           SUM(COALESCE(realized_pnl, 0)) AS sum_pnl
                    FROM scalp_cycles
                    WHERE realized_pnl IS NOT NULL
                    GROUP BY symbol, side
                    ORDER BY sum_pnl ASC
                    """
                )
            )

    def recent_orders(self, limit: int = 10) -> list[sqlite3.Row]:
        with self.connect() as connection:
            return list(
                connection.execute(
                    "SELECT * FROM orders ORDER BY timestamp_ms DESC LIMIT ?",
                    (limit,),
                )
            )

    def insert_strategy_evaluations(
        self,
        rows: Iterable[dict[str, Any]],
        *,
        timestamp_ms: int | None = None,
    ) -> int:
        ts = timestamp_ms or now_ms()
        count = 0
        with self.connect() as connection:
            for row in rows:
                connection.execute(
                    """
                    INSERT INTO strategy_evaluations (
                        evaluated_ms, evaluated_iso, source, execution_mode,
                        symbol, regime, side,
                        take_profit_bps, stop_loss_bps, max_hold_seconds,
                        sample_count, win_count, loss_count, win_rate,
                        avg_pnl_bps, sum_pnl_bps, avg_win_bps, avg_loss_bps,
                        decision, reason
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        ts,
                        iso_from_ms(ts),
                        row["source"],
                        row.get("execution_mode", "maker_post_only"),
                        row["symbol"],
                        row["regime"],
                        row["side"],
                        row["take_profit_bps"],
                        row["stop_loss_bps"],
                        row["max_hold_seconds"],
                        row["sample_count"],
                        row["win_count"],
                        row["loss_count"],
                        row["win_rate"],
                        row["avg_pnl_bps"],
                        row["sum_pnl_bps"],
                        row.get("avg_win_bps"),
                        row.get("avg_loss_bps"),
                        row["decision"],
                        row["reason"],
                    ),
                )
                count += 1
        return count

    def latest_strategy_evaluations(
        self,
        *,
        source: str | None = None,
        execution_mode: str | None = None,
        limit: int = 100,
    ) -> list[sqlite3.Row]:
        params: list[Any] = []
        where_parts: list[str] = []
        if source:
            where_parts.append("source=?")
            params.append(source)
        if execution_mode:
            where_parts.append("execution_mode=?")
            params.append(execution_mode)
        where = "WHERE " + " AND ".join(where_parts) if where_parts else ""
        params.append(limit)
        with self.connect() as connection:
            return list(
                connection.execute(
                    f"""
                    SELECT *
                    FROM strategy_evaluations
                    {where}
                    ORDER BY evaluated_ms DESC, decision ASC, avg_pnl_bps DESC
                    LIMIT ?
                    """,
                    params,
                )
            )

    def latest_strategy_evaluation(
        self,
        *,
        symbol: str,
        regime: str,
        side: str,
        take_profit_bps: float,
        stop_loss_bps: float,
        max_hold_seconds: int,
        execution_mode: str = "maker_post_only",
        source: str | None = None,
    ) -> sqlite3.Row | None:
        params: list[Any] = [
            execution_mode,
            symbol.upper(),
            regime,
            side,
            take_profit_bps,
            stop_loss_bps,
            max_hold_seconds,
        ]
        source_sql = ""
        if source:
            source_sql = "AND source=?"
            params.append(source)
        with self.connect() as connection:
            return connection.execute(
                f"""
                SELECT *
                FROM strategy_evaluations
                WHERE execution_mode=?
                  AND symbol=?
                  AND regime=?
                  AND side=?
                  AND ABS(take_profit_bps - ?) < 0.000001
                  AND ABS(stop_loss_bps - ?) < 0.000001
                  AND max_hold_seconds=?
                  {source_sql}
                ORDER BY evaluated_ms DESC
                LIMIT 1
                """,
                params,
            ).fetchone()

    def latest_strategy_candidate(
        self,
        *,
        symbol: str,
        regime: str,
        side: str,
        execution_mode: str = "maker_post_only",
        decision: str | None = None,
        source: str | None = None,
    ) -> sqlite3.Row | None:
        params: list[Any] = [execution_mode, symbol.upper(), regime, side]
        decision_sql = ""
        if decision:
            decision_sql = "AND decision=?"
            params.append(decision)
        source_sql = ""
        if source:
            source_sql = "AND source=?"
            params.append(source)
        with self.connect() as connection:
            latest = connection.execute(
                "SELECT MAX(evaluated_ms) FROM strategy_evaluations"
            ).fetchone()[0]
            if latest is None:
                return None
            params.append(latest)
            return connection.execute(
                f"""
                SELECT *
                FROM strategy_evaluations
                WHERE execution_mode=?
                  AND symbol=?
                  AND regime=?
                  AND side=?
                  {decision_sql}
                  {source_sql}
                  AND evaluated_ms=?
                ORDER BY avg_pnl_bps DESC, sample_count DESC
                LIMIT 1
                """,
                params,
            ).fetchone()

    def latest_strategy_batch(self, *, limit: int | None = None) -> list[sqlite3.Row]:
        with self.connect() as connection:
            latest = connection.execute(
                "SELECT MAX(evaluated_ms) FROM strategy_evaluations"
            ).fetchone()[0]
            if latest is None:
                return []
            sql = """
                SELECT *
                FROM strategy_evaluations
                WHERE evaluated_ms=?
                ORDER BY decision ASC, execution_mode ASC, avg_pnl_bps DESC
            """
            params: list[Any] = [latest]
            if limit is not None:
                sql += " LIMIT ?"
                params.append(limit)
            return list(connection.execute(sql, params))

    def summary_counts(self) -> dict[str, int]:
        with self.connect() as connection:
            return {
                "signals": int(connection.execute("SELECT COUNT(*) FROM signals").fetchone()[0]),
                "orders": int(connection.execute("SELECT COUNT(*) FROM orders").fetchone()[0]),
                "fills": int(connection.execute("SELECT COUNT(*) FROM fills").fetchone()[0]),
                "fee_snapshots": int(
                    connection.execute("SELECT COUNT(*) FROM fee_snapshots").fetchone()[0]
                ),
                "scalp_cycles": int(
                    connection.execute("SELECT COUNT(*) FROM scalp_cycles").fetchone()[0]
                ),
                "strategy_evaluations": int(
                    connection.execute("SELECT COUNT(*) FROM strategy_evaluations").fetchone()[0]
                ),
                "market_regimes": int(
                    connection.execute("SELECT COUNT(*) FROM market_regimes").fetchone()[0]
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


def _ensure_column(
    connection: sqlite3.Connection,
    table_name: str,
    column_name: str,
    ddl: str,
) -> None:
    columns = {row["name"] for row in connection.execute(f"PRAGMA table_info({table_name})")}
    if column_name not in columns:
        connection.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {ddl}")
