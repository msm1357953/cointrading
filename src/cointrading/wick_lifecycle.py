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
from cointrading.live_execution import (
    query_live_order_status,
    realized_pnl_from_close,
    submit_live_market_close,
    submit_live_market_long,
    submit_protective_stop,
)
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
            if int(cycle["dry_run"]) == 0:
                results.append(self._manage_live_cycle(cycle, now))
            else:
                results.append(self._manage_paper_cycle(cycle, now))
        return results

    def _manage_paper_cycle(self, cycle: Any, now: int) -> dict[str, Any]:
        symbol = cycle["symbol"]
        mark_price = self._fetch_mark_price(symbol)
        if mark_price is None:
            return {"id": cycle["id"], "symbol": symbol, "action": "skip_no_price"}
        entry_price = float(cycle["entry_price"])
        stop_price = float(cycle["stop_price"])
        max_hold_deadline_ms = cycle["max_hold_deadline_ms"]

        if mark_price <= stop_price:
            self._close_paper_cycle(cycle, mark_price, STATUS_STOPPED, "stop_loss")
            return {"id": cycle["id"], "symbol": symbol, "action": "stopped",
                    "entry": entry_price, "exit": mark_price}
        if max_hold_deadline_ms is not None and now >= int(max_hold_deadline_ms):
            self._close_paper_cycle(cycle, mark_price, STATUS_CLOSED, "time_exit")
            return {"id": cycle["id"], "symbol": symbol, "action": "closed_time",
                    "entry": entry_price, "exit": mark_price}

        self.storage.update_strategy_cycle(
            int(cycle["id"]), last_mid_price=mark_price, timestamp_ms=now,
        )
        return {"id": cycle["id"], "symbol": symbol, "action": "hold",
                "entry": entry_price, "mark": mark_price}

    def _manage_live_cycle(self, cycle: Any, now: int) -> dict[str, Any]:
        symbol = cycle["symbol"]
        entry_price = float(cycle["entry_price"])

        if cycle["exit_order_id"] is not None:
            status_resp = query_live_order_status(
                client=self.client, store=self.storage,
                order_id=int(cycle["exit_order_id"]),
            )
            if status_resp is not None and str(status_resp.get("status")) == "FILLED":
                avg_exit = float(status_resp.get("avgPrice") or 0.0)
                exec_qty = float(status_resp.get("executedQty") or float(cycle["quantity"]))
                if avg_exit <= 0:
                    avg_exit = self._fetch_mark_price(symbol) or entry_price
                pnl = realized_pnl_from_close(
                    cycle=cycle, avg_exit_price=avg_exit,
                    executed_qty=exec_qty, config=self.config,
                )
                self.storage.update_strategy_cycle(
                    int(cycle["id"]), status=STATUS_STOPPED, reason="exchange_stop_filled",
                    closed_ms=now, last_mid_price=avg_exit, realized_pnl=pnl,
                    timestamp_ms=now,
                )
                logger.info("wick LIVE STOPPED id=%d %s entry=%.6f exit=%.6f pnl=%.4f",
                            int(cycle["id"]), symbol, entry_price, avg_exit, pnl)
                return {"id": cycle["id"], "symbol": symbol, "action": "stopped",
                        "entry": entry_price, "exit": avg_exit}

        max_hold_deadline_ms = cycle["max_hold_deadline_ms"]
        if max_hold_deadline_ms is not None and now >= int(max_hold_deadline_ms):
            close = submit_live_market_close(
                client=self.client, store=self.storage, config=self.config,
                cycle=cycle, reason="time_exit", timestamp_ms=now,
            )
            if not close.success:
                logger.warning("wick LIVE time-exit failed: %s", close.detail)
                return {"id": cycle["id"], "symbol": symbol, "action": "time_exit_error",
                        "detail": close.detail}
            pnl = realized_pnl_from_close(
                cycle=cycle, avg_exit_price=close.avg_price,
                executed_qty=close.executed_qty, config=self.config,
            )
            self.storage.update_strategy_cycle(
                int(cycle["id"]), status=STATUS_CLOSED, reason="time_exit",
                closed_ms=now, last_mid_price=close.avg_price, realized_pnl=pnl,
                exit_order_id=close.order_id, timestamp_ms=now,
            )
            logger.info("wick LIVE CLOSED id=%d %s entry=%.6f exit=%.6f pnl=%.4f",
                        int(cycle["id"]), symbol, entry_price, close.avg_price, pnl)
            return {"id": cycle["id"], "symbol": symbol, "action": "closed_time",
                    "entry": entry_price, "exit": close.avg_price}

        mark_price = self._fetch_mark_price(symbol)
        if mark_price is not None:
            self.storage.update_strategy_cycle(
                int(cycle["id"]), last_mid_price=mark_price, timestamp_ms=now,
            )
        return {"id": cycle["id"], "symbol": symbol, "action": "hold",
                "entry": entry_price, "mark": mark_price or 0.0}

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

            if self.is_live_armed():
                cycle_id, detail, entry_price = self._open_live_cycle(
                    symbol=symbol, trigger_bar=trigger_bar,
                    wick_ratio=wick_ratio, drop_pct=drop_pct,
                )
                if cycle_id is None:
                    skipped.append({"symbol": symbol, "reason": f"live_open_failed: {detail}"})
                    continue
            else:
                cycle_id = self._open_paper_cycle(
                    symbol=symbol, trigger_bar=trigger_bar,
                    wick_ratio=wick_ratio, drop_pct=drop_pct,
                    entry_price=mark_price,
                )
                entry_price = mark_price

            opened.append({
                "symbol": symbol, "cycle_id": cycle_id,
                "wick_ratio": wick_ratio, "drop_pct": drop_pct,
                "entry_price": entry_price,
                "trigger_bar_open_time_ms": trigger_bar.open_time,
                "mode": "LIVE" if self.is_live_armed() else "paper",
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

    def _open_live_cycle(
        self, *, symbol: str, trigger_bar: Kline,
        wick_ratio: float, drop_pct: float,
    ) -> tuple[int | None, str, float]:
        cfg = self.config
        notional = cfg.wick_carry_notional
        mid = self._fetch_mark_price(symbol)
        if mid is None or mid <= 0:
            return None, "no indicative price", 0.0
        intended_qty = notional / mid
        now = self._now_ms_fn()

        entry = submit_live_market_long(
            client=self.client, store=self.storage, config=cfg,
            symbol=symbol, quantity=intended_qty, strategy_label="wk",
            timestamp_ms=now,
        )
        if not entry.success:
            return None, entry.detail, 0.0

        avg_entry = entry.avg_price
        actual_qty = entry.executed_qty
        stop_price = avg_entry * (1.0 - cfg.wick_carry_stop_loss_bps / 10_000.0)
        max_hold_seconds = int(cfg.wick_carry_max_hold_seconds)
        max_hold_deadline_ms = now + max_hold_seconds * 1000

        cycle_id = self.storage.insert_strategy_cycle(
            strategy=STRATEGY_NAME, execution_mode=EXECUTION_MODE,
            symbol=symbol, side="long", status=STATUS_OPEN,
            quantity=actual_qty, entry_price=avg_entry,
            target_price=avg_entry * 1.05, stop_price=stop_price,
            entry_order_type="MARKET", take_profit_bps=0.0,
            stop_loss_bps=cfg.wick_carry_stop_loss_bps,
            max_hold_seconds=max_hold_seconds,
            maker_one_way_bps=cfg.maker_fee_rate * 10_000.0,
            taker_one_way_bps=cfg.taker_fee_rate * 10_000.0,
            entry_deadline_ms=now,
            entry_order_id=entry.order_id,
            dry_run=False,
            reason=f"LIVE wick={wick_ratio:.2f} drop={drop_pct * 100:+.2f}%",
            opened_ms=now, max_hold_deadline_ms=max_hold_deadline_ms,
            last_mid_price=avg_entry,
            setup={
                "hypothesis": "wick_reversion_long",
                "wick_ratio": wick_ratio,
                "intrabar_drop_pct": drop_pct,
                "trigger_bar_open_time_ms": trigger_bar.open_time,
                "trigger_bar_close_time_ms": trigger_bar.close_time,
                "min_wick_ratio": cfg.wick_carry_min_wick_ratio,
                "min_drop_pct": cfg.wick_carry_min_drop_pct,
                "hold_hours": max_hold_seconds / 3600.0,
                "stop_loss_bps": cfg.wick_carry_stop_loss_bps,
                "notional": notional, "live": True,
            },
            timestamp_ms=now,
        )

        stop = submit_protective_stop(
            client=self.client, store=self.storage, config=cfg,
            symbol=symbol, quantity=actual_qty, stop_price=stop_price,
            strategy_label="wk", timestamp_ms=now,
        )
        if stop.order_id is None:
            logger.error("wick LIVE stop submit FAILED, emergency exit: %s", stop.detail)
            cycle_row = self.storage.active_strategy_cycle(STRATEGY_NAME, symbol)
            if cycle_row is not None:
                close = submit_live_market_close(
                    client=self.client, store=self.storage, config=cfg,
                    cycle=cycle_row, reason="emergency_no_stop", timestamp_ms=now,
                )
                pnl = realized_pnl_from_close(
                    cycle=cycle_row, avg_exit_price=close.avg_price or avg_entry,
                    executed_qty=close.executed_qty or actual_qty, config=cfg,
                )
                self.storage.update_strategy_cycle(
                    cycle_id, status=STATUS_STOPPED, reason="emergency_no_stop",
                    closed_ms=now, realized_pnl=pnl, timestamp_ms=now,
                )
            return None, f"emergency exit (no stop): {stop.detail}", avg_entry

        self.storage.update_strategy_cycle(
            cycle_id, exit_order_id=stop.order_id, timestamp_ms=now,
        )
        logger.info(
            "wick LIVE OPEN id=%d %s qty=%.6f entry=%.6f stop=%.6f stop_order=%d",
            cycle_id, symbol, actual_qty, avg_entry, stop_price, stop.order_id,
        )
        return cycle_id, "live entry filled, protective stop set", avg_entry

    def _close_paper_cycle(
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
