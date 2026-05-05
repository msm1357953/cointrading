"""Wick reversion long-only strategy lifecycle (paper-only).

Hypothesis verified by cointrading.research.wick_scalp_backtest on 2026-05-05:
on a 5-minute candle with a long lower wick (lower_wick / range >= 0.7) and
an intrabar drop of at least 1% (open - low >= 1% of open), price tends to
mean-revert over the next ~2 hours. Long-only.

State machine:

    (no position)
        |  most-recent-closed 5m bar matches the wick pattern
        |  (one cycle per symbol; trigger bar must be fresh)
        v
    OPEN ---- mark <= stop_price ----> STOPPED (-3%)
        |
        |  now >= max_hold_deadline (2h)
        v
    CLOSED

This module mirrors funding_lifecycle.py for parity. Live trading requires
the same triple gate (config.dry_run=False, live_trading_enabled=True,
wick_carry_live_enabled=True). Live wiring is intentionally deferred until
~5 paper cycles validate behaviour on the VM.
"""
from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import Any, Callable

from cointrading.config import TradingConfig
from cointrading.exchange.binance_usdm import BinanceAPIError, BinanceUSDMClient
from cointrading.models import Kline
from cointrading.storage import TradingStore

logger = logging.getLogger(__name__)

STRATEGY_NAME = "wick_long"
EXECUTION_MODE = "taker_market"
STATUS_OPEN = "OPEN"
STATUS_CLOSED = "CLOSED"
STATUS_STOPPED = "STOPPED"


def _now_ms() -> int:
    return int(time.time() * 1000)


@dataclass
class StepResult:
    managed: list[dict[str, Any]]
    opened: list[dict[str, Any]]
    skipped: list[dict[str, Any]]
    ts_ms: int

    def as_dict(self) -> dict[str, Any]:
        return {
            "ts_ms": self.ts_ms,
            "managed": self.managed,
            "opened": self.opened,
            "skipped": self.skipped,
        }


def detect_wick(
    kline: Kline, *, min_wick_ratio: float, min_drop_pct: float
) -> tuple[bool, float, float]:
    """Return (triggered, wick_ratio, drop_pct).

    A long-lower-wick candle has the body in the upper part of the range
    and a deep low that recovered before bar close.
    """
    o, h, l, c = kline.open, kline.high, kline.low, kline.close
    rng = h - l
    if rng <= 0 or o <= 0:
        return False, 0.0, 0.0
    body_low = min(o, c)
    lower_wick = body_low - l
    if lower_wick < 0:
        return False, 0.0, 0.0
    wick_ratio = lower_wick / rng
    drop_pct = (o - l) / o
    triggered = wick_ratio >= min_wick_ratio and drop_pct >= min_drop_pct
    return triggered, wick_ratio, drop_pct


class WickReversionEngine:
    def __init__(
        self,
        *,
        config: TradingConfig,
        storage: TradingStore,
        client: BinanceUSDMClient,
        now_ms_fn: Callable[[], int] = _now_ms,
    ) -> None:
        self.config = config
        self.storage = storage
        self.client = client
        self._now_ms_fn = now_ms_fn

    # ----- Public -----

    def step(self) -> StepResult:
        managed = self._manage_open_positions()
        opened, skipped = self._check_new_entries()
        return StepResult(
            managed=managed, opened=opened, skipped=skipped,
            ts_ms=self._now_ms_fn(),
        )

    def is_live_armed(self) -> bool:
        cfg = self.config
        return (
            (not cfg.dry_run)
            and cfg.live_trading_enabled
            and cfg.wick_carry_live_enabled
        )

    # ----- Manage open cycles -----

    def _manage_open_positions(self) -> list[dict[str, Any]]:
        results: list[dict[str, Any]] = []
        now = self._now_ms_fn()
        for cycle in self.storage.active_strategy_cycles():
            if cycle["strategy"] != STRATEGY_NAME:
                continue
            if cycle["status"] != STATUS_OPEN:
                continue
            symbol = cycle["symbol"]

            mark_price = self._fetch_mark_price(symbol)
            if mark_price is None:
                results.append({"id": cycle["id"], "symbol": symbol, "action": "skip_no_price"})
                continue

            entry_price = float(cycle["entry_price"])
            stop_price = float(cycle["stop_price"])
            max_hold_deadline_ms = cycle["max_hold_deadline_ms"]

            if mark_price <= stop_price:
                self._close_cycle(cycle, mark_price, STATUS_STOPPED, "stop_loss")
                results.append({
                    "id": cycle["id"], "symbol": symbol, "action": "stopped",
                    "entry": entry_price, "exit": mark_price,
                })
                continue

            if max_hold_deadline_ms is not None and now >= int(max_hold_deadline_ms):
                self._close_cycle(cycle, mark_price, STATUS_CLOSED, "time_exit")
                results.append({
                    "id": cycle["id"], "symbol": symbol, "action": "closed_time",
                    "entry": entry_price, "exit": mark_price,
                })
                continue

            self.storage.update_strategy_cycle(
                int(cycle["id"]), last_mid_price=mark_price, timestamp_ms=now,
            )
            results.append({
                "id": cycle["id"], "symbol": symbol, "action": "hold",
                "entry": entry_price, "mark": mark_price,
            })
        return results

    # ----- New entries -----

    def _check_new_entries(self) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        cfg = self.config
        if not cfg.wick_carry_enabled:
            return [], [{"reason": "wick_carry_disabled"}]

        opened: list[dict[str, Any]] = []
        skipped: list[dict[str, Any]] = []
        now = self._now_ms_fn()
        freshness_ms = int(cfg.wick_carry_freshness_seconds) * 1000
        cooldown_ms = int(cfg.wick_carry_cooldown_seconds) * 1000

        for symbol in cfg.wick_carry_symbols:
            existing = self.storage.active_strategy_cycle(STRATEGY_NAME, symbol)
            if existing is not None:
                skipped.append({"symbol": symbol, "reason": "already_open", "cycle_id": existing["id"]})
                continue

            if self._symbol_in_cooldown(symbol, now, cooldown_ms):
                skipped.append({"symbol": symbol, "reason": "cooldown"})
                continue

            try:
                klines = self.client.klines(symbol, "5m", limit=2)
            except BinanceAPIError as exc:
                logger.warning("wick_carry: klines failed for %s: %s", symbol, exc)
                skipped.append({"symbol": symbol, "reason": "klines_api_error"})
                continue
            if not klines or len(klines) < 2:
                skipped.append({"symbol": symbol, "reason": "no_klines"})
                continue

            # The most recent fully-closed 5m bar is the second-to-last in the response.
            # The last is partial (still forming) when fetched mid-interval.
            trigger_bar = klines[-2]
            bar_age_ms = now - int(trigger_bar.close_time)
            if bar_age_ms < 0 or bar_age_ms > freshness_ms:
                skipped.append({
                    "symbol": symbol, "reason": "bar_not_fresh",
                    "age_seconds": bar_age_ms / 1000.0,
                })
                continue

            triggered, wick_ratio, drop_pct = detect_wick(
                trigger_bar,
                min_wick_ratio=cfg.wick_carry_min_wick_ratio,
                min_drop_pct=cfg.wick_carry_min_drop_pct,
            )
            if not triggered:
                skipped.append({
                    "symbol": symbol, "reason": "no_wick_signal",
                    "wick_ratio": wick_ratio, "drop_pct": drop_pct,
                })
                continue

            mark_price = self._fetch_mark_price(symbol)
            if mark_price is None:
                skipped.append({"symbol": symbol, "reason": "no_price"})
                continue

            cycle_id = self._open_paper_cycle(
                symbol=symbol,
                trigger_bar=trigger_bar,
                wick_ratio=wick_ratio,
                drop_pct=drop_pct,
                entry_price=mark_price,
            )
            opened.append({
                "symbol": symbol, "cycle_id": cycle_id,
                "wick_ratio": wick_ratio, "drop_pct": drop_pct,
                "entry_price": mark_price,
                "trigger_bar_open_time_ms": trigger_bar.open_time,
            })
        return opened, skipped

    def _symbol_in_cooldown(self, symbol: str, now_ms: int, cooldown_ms: int) -> bool:
        with self.storage.connect() as connection:
            row = connection.execute(
                """
                SELECT closed_ms FROM strategy_cycles
                WHERE strategy=? AND symbol=? AND status IN (?, ?)
                ORDER BY closed_ms DESC
                LIMIT 1
                """,
                (STRATEGY_NAME, symbol.upper(), STATUS_CLOSED, STATUS_STOPPED),
            ).fetchone()
        if row is None or row["closed_ms"] is None:
            return False
        return (now_ms - int(row["closed_ms"])) < cooldown_ms

    def _open_paper_cycle(
        self,
        *,
        symbol: str,
        trigger_bar: Kline,
        wick_ratio: float,
        drop_pct: float,
        entry_price: float,
    ) -> int:
        cfg = self.config
        notional = cfg.wick_carry_notional
        if entry_price <= 0:
            raise ValueError(f"non-positive entry price for {symbol}: {entry_price}")
        quantity = notional / entry_price
        stop_loss_bps = cfg.wick_carry_stop_loss_bps
        max_hold_seconds = int(cfg.wick_carry_max_hold_seconds)
        stop_price = entry_price * (1.0 - stop_loss_bps / 10_000.0)
        target_price = entry_price * 1.05  # placeholder; no TP

        now = self._now_ms_fn()
        max_hold_deadline_ms = now + max_hold_seconds * 1000

        cycle_id = self.storage.insert_strategy_cycle(
            strategy=STRATEGY_NAME,
            execution_mode=EXECUTION_MODE,
            symbol=symbol,
            side="long",
            status=STATUS_OPEN,
            quantity=quantity,
            entry_price=entry_price,
            target_price=target_price,
            stop_price=stop_price,
            entry_order_type="MARKET",
            take_profit_bps=0.0,
            stop_loss_bps=stop_loss_bps,
            max_hold_seconds=max_hold_seconds,
            maker_one_way_bps=cfg.maker_fee_rate * 10_000.0,
            taker_one_way_bps=cfg.taker_fee_rate * 10_000.0,
            entry_deadline_ms=now,
            dry_run=True,
            reason=f"wick={wick_ratio:.2f} drop={drop_pct * 100:+.2f}%",
            opened_ms=now,
            max_hold_deadline_ms=max_hold_deadline_ms,
            last_mid_price=entry_price,
            setup={
                "hypothesis": "wick_reversion_long",
                "wick_ratio": wick_ratio,
                "intrabar_drop_pct": drop_pct,
                "trigger_bar_open_time_ms": trigger_bar.open_time,
                "trigger_bar_close_time_ms": trigger_bar.close_time,
                "trigger_bar_high": trigger_bar.high,
                "trigger_bar_low": trigger_bar.low,
                "min_wick_ratio": cfg.wick_carry_min_wick_ratio,
                "min_drop_pct": cfg.wick_carry_min_drop_pct,
                "hold_hours": max_hold_seconds / 3600.0,
                "stop_loss_bps": stop_loss_bps,
                "notional": notional,
            },
            timestamp_ms=now,
        )
        logger.info(
            "wick_carry: opened paper %s id=%d wick=%.2f drop=%+.2f%% px=%.6f stop=%.6f",
            symbol, cycle_id, wick_ratio, drop_pct * 100, entry_price, stop_price,
        )
        return cycle_id

    def _close_cycle(
        self, cycle: Any, exit_price: float, status: str, reason: str,
    ) -> None:
        entry_price = float(cycle["entry_price"])
        quantity = float(cycle["quantity"])
        side = cycle["side"]
        if side == "long":
            ret_pct = (exit_price - entry_price) / entry_price
        else:
            ret_pct = (entry_price - exit_price) / entry_price
        round_trip_fee_pct = 2.0 * float(cycle["taker_one_way_bps"]) / 10_000.0
        net_ret_pct = ret_pct - round_trip_fee_pct
        notional = entry_price * quantity
        realized_pnl = notional * net_ret_pct

        now = self._now_ms_fn()
        self.storage.update_strategy_cycle(
            int(cycle["id"]),
            status=status, reason=reason, closed_ms=now,
            last_mid_price=exit_price, realized_pnl=realized_pnl,
            timestamp_ms=now,
        )
        logger.info(
            "wick_carry: %s id=%d %s entry=%.6f exit=%.6f pnl=%.4f",
            status, int(cycle["id"]), reason, entry_price, exit_price, realized_pnl,
        )

    def _fetch_mark_price(self, symbol: str) -> float | None:
        try:
            ticker = self.client.book_ticker(symbol)
        except BinanceAPIError as exc:
            logger.warning("wick_carry: book_ticker failed for %s: %s", symbol, exc)
            return None
        try:
            bid = float(ticker.get("bidPrice", 0) or 0)
            ask = float(ticker.get("askPrice", 0) or 0)
        except (ValueError, TypeError):
            return None
        if bid <= 0 or ask <= 0:
            return None
        return (bid + ask) / 2.0


def run_step_once(*, config: TradingConfig | None = None) -> StepResult:
    cfg = config or TradingConfig.from_env()
    storage = TradingStore()
    client = BinanceUSDMClient(config=cfg)
    engine = WickReversionEngine(config=cfg, storage=storage, client=client)
    return engine.step()
