"""Funding-rate mean-reversion long-only strategy lifecycle (paper-only).

Hypothesis verified by cointrading.research.funding_carry_backtest on 2026-05-05:
when perpetual funding settles negative, longs typically rebound over the next
~24 hours. Long-only because the symmetric short side did not survive
out-of-sample on USDC majors.

This module manages a tiny state machine:

    (no position)
        |
        |  funding_rate <= -threshold
        |  (one cycle per symbol; checked within `check_window` of settlement)
        v
    OPEN  ----- mark_price <= stop_price -----> STOPPED  (hard SL)
        |
        |  now >= max_hold_deadline
        v
    CLOSED  (24h time exit)

Live trading requires THREE gates:
    config.dry_run == False
    config.live_trading_enabled == True
    config.funding_carry_live_enabled == True
This module ONLY runs the paper path. Live wiring is intentionally deferred
until ~30+ paper cycles validate behavior on the VM.
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
from cointrading.storage import TradingStore

logger = logging.getLogger(__name__)

STRATEGY_NAME = "funding_carry_long"
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


class FundingCarryEngine:
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
            managed=managed,
            opened=opened,
            skipped=skipped,
            ts_ms=self._now_ms_fn(),
        )

    def is_live_armed(self) -> bool:
        cfg = self.config
        return (
            (not cfg.dry_run)
            and cfg.live_trading_enabled
            and cfg.funding_carry_live_enabled
        )

    # ----- Manage existing OPEN cycles -----

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

        # 1) Check protective stop status: if FILLED, exchange stopped us out.
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
                logger.info("funding_carry LIVE STOPPED id=%d %s entry=%.6f exit=%.6f pnl=%.4f",
                            int(cycle["id"]), symbol, entry_price, avg_exit, pnl)
                return {"id": cycle["id"], "symbol": symbol, "action": "stopped",
                        "entry": entry_price, "exit": avg_exit}

        # 2) Time exit
        max_hold_deadline_ms = cycle["max_hold_deadline_ms"]
        if max_hold_deadline_ms is not None and now >= int(max_hold_deadline_ms):
            close = submit_live_market_close(
                client=self.client, store=self.storage, config=self.config,
                cycle=cycle, reason="time_exit", timestamp_ms=now,
            )
            if not close.success:
                logger.warning("funding_carry LIVE time-exit failed: %s", close.detail)
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
            logger.info("funding_carry LIVE CLOSED id=%d %s entry=%.6f exit=%.6f pnl=%.4f",
                        int(cycle["id"]), symbol, entry_price, close.avg_price, pnl)
            return {"id": cycle["id"], "symbol": symbol, "action": "closed_time",
                    "entry": entry_price, "exit": close.avg_price}

        # 3) Refresh last_mid_price for dashboard
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
        if not cfg.funding_carry_enabled:
            return [], [{"reason": "funding_carry_disabled"}]

        opened: list[dict[str, Any]] = []
        skipped: list[dict[str, Any]] = []
        threshold = cfg.funding_carry_threshold
        window_ms = int(cfg.funding_carry_check_window_minutes) * 60 * 1000
        now = self._now_ms_fn()

        for symbol in cfg.funding_carry_symbols:
            existing = self.storage.active_strategy_cycle(STRATEGY_NAME, symbol)
            if existing is not None:
                skipped.append({"symbol": symbol, "reason": "already_open", "cycle_id": existing["id"]})
                continue

            try:
                funding_resp = self.client.funding_rate(symbol, limit=1)
            except BinanceAPIError as exc:
                logger.warning("funding_carry: funding_rate failed for %s: %s", symbol, exc)
                skipped.append({"symbol": symbol, "reason": "funding_api_error"})
                continue

            if not funding_resp:
                skipped.append({"symbol": symbol, "reason": "no_funding_data"})
                continue

            record = funding_resp[0]
            try:
                funding_rate = float(record["fundingRate"])
                funding_time_ms = int(record["fundingTime"])
            except (KeyError, ValueError, TypeError):
                skipped.append({"symbol": symbol, "reason": "funding_parse_error"})
                continue

            age_ms = now - funding_time_ms
            if age_ms < 0 or age_ms > window_ms:
                skipped.append({
                    "symbol": symbol, "reason": "outside_window",
                    "age_minutes": age_ms / 60000.0,
                })
                continue

            if funding_rate > -threshold:
                skipped.append({
                    "symbol": symbol, "reason": "rate_above_threshold",
                    "funding_rate": funding_rate, "threshold": threshold,
                })
                continue

            mark_price = self._fetch_mark_price(symbol)
            if mark_price is None:
                skipped.append({"symbol": symbol, "reason": "no_price"})
                continue

            if self.is_live_armed():
                cycle_id, detail, entry_price = self._open_live_cycle(
                    symbol=symbol, funding_rate=funding_rate,
                    funding_time_ms=funding_time_ms,
                )
                if cycle_id is None:
                    skipped.append({"symbol": symbol, "reason": f"live_open_failed: {detail}"})
                    continue
            else:
                cycle_id = self._open_paper_cycle(
                    symbol=symbol, funding_rate=funding_rate,
                    funding_time_ms=funding_time_ms, entry_price=mark_price,
                )
                entry_price = mark_price

            opened.append({
                "symbol": symbol, "cycle_id": cycle_id,
                "funding_rate": funding_rate, "funding_time_ms": funding_time_ms,
                "entry_price": entry_price,
                "mode": "LIVE" if self.is_live_armed() else "paper",
            })

        return opened, skipped

    def _open_paper_cycle(
        self,
        *,
        symbol: str,
        funding_rate: float,
        funding_time_ms: int,
        entry_price: float,
    ) -> int:
        cfg = self.config
        notional = cfg.funding_carry_notional
        if entry_price <= 0:
            raise ValueError(f"non-positive entry price for {symbol}: {entry_price}")
        quantity = notional / entry_price
        stop_loss_bps = cfg.funding_carry_stop_loss_bps
        max_hold_seconds = int(cfg.funding_carry_max_hold_seconds)
        stop_price = entry_price * (1.0 - stop_loss_bps / 10_000.0)
        # We do not use a take-profit; record a placeholder above current price.
        target_price = entry_price * 1.05

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
            dry_run=True,  # paper-only path
            reason=f"funding={funding_rate * 100:+.4f}% trigger=-{cfg.funding_carry_threshold * 100:.4f}%",
            opened_ms=now,
            max_hold_deadline_ms=max_hold_deadline_ms,
            last_mid_price=entry_price,
            setup={
                "hypothesis": "funding_mean_reversion_long",
                "funding_rate": funding_rate,
                "funding_time_ms": funding_time_ms,
                "threshold": cfg.funding_carry_threshold,
                "hold_hours": max_hold_seconds / 3600.0,
                "stop_loss_bps": stop_loss_bps,
                "notional": notional,
            },
            timestamp_ms=now,
        )
        logger.info(
            "funding_carry: opened paper %s id=%d funding=%+.4f%% px=%.6f stop=%.6f hold=%dh",
            symbol, cycle_id, funding_rate * 100, entry_price, stop_price, max_hold_seconds // 3600,
        )
        return cycle_id

    def _open_live_cycle(
        self, *, symbol: str, funding_rate: float, funding_time_ms: int,
    ) -> tuple[int | None, str, float]:
        """Submit MARKET BUY, then attach reduce-only protective STOP_MARKET.
        Returns (cycle_id_or_None, detail, avg_entry_price).
        If protective stop fails after entry, immediately submit a market exit
        so we are never naked-long without a stop.
        """
        cfg = self.config
        notional = cfg.funding_carry_notional
        # Need an indicative price to size the qty. Use book mid.
        mid = self._fetch_mark_price(symbol)
        if mid is None or mid <= 0:
            return None, "no indicative price", 0.0
        intended_qty = notional / mid
        now = self._now_ms_fn()

        entry = submit_live_market_long(
            client=self.client, store=self.storage, config=cfg,
            symbol=symbol, quantity=intended_qty, strategy_label="fc",
            timestamp_ms=now,
        )
        if not entry.success:
            return None, entry.detail, 0.0

        avg_entry = entry.avg_price
        actual_qty = entry.executed_qty
        stop_price = avg_entry * (1.0 - cfg.funding_carry_stop_loss_bps / 10_000.0)
        max_hold_seconds = int(cfg.funding_carry_max_hold_seconds)
        max_hold_deadline_ms = now + max_hold_seconds * 1000

        # Insert cycle row first (so we can attach stop's order_id later)
        cycle_id = self.storage.insert_strategy_cycle(
            strategy=STRATEGY_NAME, execution_mode=EXECUTION_MODE,
            symbol=symbol, side="long", status=STATUS_OPEN,
            quantity=actual_qty, entry_price=avg_entry,
            target_price=avg_entry * 1.05, stop_price=stop_price,
            entry_order_type="MARKET", take_profit_bps=0.0,
            stop_loss_bps=cfg.funding_carry_stop_loss_bps,
            max_hold_seconds=max_hold_seconds,
            maker_one_way_bps=cfg.maker_fee_rate * 10_000.0,
            taker_one_way_bps=cfg.taker_fee_rate * 10_000.0,
            entry_deadline_ms=now,
            entry_order_id=entry.order_id,
            dry_run=False,
            reason=f"LIVE funding={funding_rate * 100:+.4f}%",
            opened_ms=now, max_hold_deadline_ms=max_hold_deadline_ms,
            last_mid_price=avg_entry,
            setup={
                "hypothesis": "funding_mean_reversion_long",
                "funding_rate": funding_rate,
                "funding_time_ms": funding_time_ms,
                "threshold": cfg.funding_carry_threshold,
                "hold_hours": max_hold_seconds / 3600.0,
                "stop_loss_bps": cfg.funding_carry_stop_loss_bps,
                "notional": notional, "live": True,
            },
            timestamp_ms=now,
        )

        # Submit protective stop
        stop = submit_protective_stop(
            client=self.client, store=self.storage, config=cfg,
            symbol=symbol, quantity=actual_qty, stop_price=stop_price,
            strategy_label="fc", timestamp_ms=now,
        )
        if stop.order_id is None:
            # DEFENSIVE: protective stop failed → immediately exit position
            logger.error("funding_carry LIVE stop submit FAILED, emergency exit: %s", stop.detail)
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
            "funding_carry LIVE OPEN id=%d %s qty=%.6f entry=%.6f stop=%.6f stop_order=%d",
            cycle_id, symbol, actual_qty, avg_entry, stop_price, stop.order_id,
        )
        return cycle_id, "live entry filled, protective stop set", avg_entry

    def _close_paper_cycle(
        self,
        cycle: Any,
        exit_price: float,
        status: str,
        reason: str,
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
            status=status,
            reason=reason,
            closed_ms=now,
            last_mid_price=exit_price,
            realized_pnl=realized_pnl,
            timestamp_ms=now,
        )
        logger.info(
            "funding_carry: %s id=%d %s entry=%.6f exit=%.6f pnl=%.4f",
            status, int(cycle["id"]), reason, entry_price, exit_price, realized_pnl,
        )

    # ----- Helpers -----

    def _fetch_mark_price(self, symbol: str) -> float | None:
        try:
            ticker = self.client.book_ticker(symbol)
        except BinanceAPIError as exc:
            logger.warning("funding_carry: book_ticker failed for %s: %s", symbol, exc)
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
    engine = FundingCarryEngine(config=cfg, storage=storage, client=client)
    return engine.step()
