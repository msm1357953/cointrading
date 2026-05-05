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
            symbol = cycle["symbol"]

            mark_price = self._fetch_mark_price(symbol)
            if mark_price is None:
                results.append({"id": cycle["id"], "symbol": symbol, "action": "skip_no_price"})
                continue

            entry_price = float(cycle["entry_price"])
            stop_price = float(cycle["stop_price"])
            max_hold_deadline_ms = cycle["max_hold_deadline_ms"]

            # Long: stop triggers when mark <= stop_price
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

            # Otherwise just refresh last_mid_price for the dashboard
            self.storage.update_strategy_cycle(
                int(cycle["id"]),
                last_mid_price=mark_price,
                timestamp_ms=now,
            )
            results.append({
                "id": cycle["id"], "symbol": symbol, "action": "hold",
                "entry": entry_price, "mark": mark_price,
            })
        return results

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

            cycle_id = self._open_paper_cycle(
                symbol=symbol,
                funding_rate=funding_rate,
                funding_time_ms=funding_time_ms,
                entry_price=mark_price,
            )
            opened.append({
                "symbol": symbol,
                "cycle_id": cycle_id,
                "funding_rate": funding_rate,
                "funding_time_ms": funding_time_ms,
                "entry_price": mark_price,
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

    def _close_cycle(
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
