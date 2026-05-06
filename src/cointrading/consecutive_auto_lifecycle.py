"""Telegram-toggled auto-execution for the consecutive-bar pattern.

When the owner sends '자동' to Telegram, this engine starts taking real
counter-trend trades on the configured symbol whenever the alert
threshold is hit:

    LONG  on N consecutive DOWN bars (after one-doji tolerance)
    SHORT on N consecutive UP bars

Position sizing:
    notional = capital * margin_pct * leverage
    margin   = capital * margin_pct        (e.g. 100 USDC at 10%)

Stops are STRUCTURAL:
    SL  = run_low  * (1 - buffer_bps/10000)        for LONG
        = run_high * (1 + buffer_bps/10000)        for SHORT
    TP  = entry +/- (entry - SL) * tp_rr            (1:1 by default)

Each cycle has BOTH a STOP_MARKET reduce-only order AND a
TAKE_PROFIT_MARKET reduce-only order on the exchange. On every step we
poll their statuses; whichever fills first wins, the other is
cancelled. A 60-minute time exit submits a market close.

SAFEGUARDS — automatically flip auto-mode OFF when any of:
    * realised_pnl_today <= -capital * daily_loss_pct
    * consecutive_losses >= max_consecutive_losses
    * trades_today >= max_trades_per_day

EV is documented negative on the raw signal at retail-taker fees. The
owner accepted the trade-off after seeing the math; the safeguards bound
the daily downside.
"""
from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from cointrading.config import TradingConfig
from cointrading.consecutive_bar_alert import RunResult
from cointrading.exchange.binance_usdm import BinanceAPIError, BinanceUSDMClient
from cointrading.live_execution import (
    cancel_local_order,
    query_live_order_status,
    realized_pnl_from_close,
    submit_live_market_close,
    submit_live_market_long,
    submit_protective_stop,
    submit_take_profit_market,
)
from cointrading.models import Kline, OrderIntent
from cointrading.storage import TradingStore, default_db_path


logger = logging.getLogger(__name__)


STRATEGY_NAME = "consecutive_auto"
EXECUTION_MODE = "taker_market_oco"
STATUS_OPEN = "OPEN"
STATUS_CLOSED = "CLOSED"
STATUS_STOPPED = "STOPPED"


def default_state_path() -> Path:
    return default_db_path().parent / "consecutive_auto_state.json"


def _now_ms() -> int:
    return int(time.time() * 1000)


def _kst_today_str(now_ms: int) -> str:
    dt = datetime.fromtimestamp(now_ms / 1000, tz=timezone.utc) + timedelta(hours=9)
    return dt.strftime("%Y-%m-%d")


# ----------------------- State management -----------------------


@dataclass
class AutoState:
    auto_mode: bool = False
    paused_reason: str = ""           # if auto-disabled by safeguard, why
    last_alerted_bar_open_time_ms: int | None = None
    daily_kst_date: str = ""
    daily_realized_pnl: float = 0.0
    daily_trade_count: int = 0
    consecutive_losses: int = 0

    def to_dict(self) -> dict:
        return {
            "auto_mode": self.auto_mode,
            "paused_reason": self.paused_reason,
            "last_alerted_bar_open_time_ms": self.last_alerted_bar_open_time_ms,
            "daily_kst_date": self.daily_kst_date,
            "daily_realized_pnl": self.daily_realized_pnl,
            "daily_trade_count": self.daily_trade_count,
            "consecutive_losses": self.consecutive_losses,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "AutoState":
        return cls(
            auto_mode=bool(data.get("auto_mode", False)),
            paused_reason=str(data.get("paused_reason", "")),
            last_alerted_bar_open_time_ms=data.get("last_alerted_bar_open_time_ms"),
            daily_kst_date=str(data.get("daily_kst_date", "")),
            daily_realized_pnl=float(data.get("daily_realized_pnl", 0.0)),
            daily_trade_count=int(data.get("daily_trade_count", 0)),
            consecutive_losses=int(data.get("consecutive_losses", 0)),
        )


def load_state(path: Path | None = None) -> AutoState:
    p = path or default_state_path()
    if not p.exists():
        return AutoState()
    try:
        return AutoState.from_dict(json.loads(p.read_text()))
    except (OSError, json.JSONDecodeError):
        return AutoState()


def save_state(state: AutoState, path: Path | None = None) -> None:
    p = path or default_state_path()
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(state.to_dict(), indent=2, sort_keys=True))


def reset_daily_if_needed(state: AutoState, now_ms: int) -> None:
    today = _kst_today_str(now_ms)
    if state.daily_kst_date != today:
        state.daily_kst_date = today
        state.daily_realized_pnl = 0.0
        state.daily_trade_count = 0


def safeguard_block_reason(state: AutoState, cfg: TradingConfig) -> str | None:
    """Return a string reason why auto must NOT trigger right now, or None if OK."""
    if not state.auto_mode:
        return "auto mode OFF"
    daily_loss_cap = cfg.initial_equity * cfg.consecutive_auto_daily_loss_pct
    if state.daily_realized_pnl <= -daily_loss_cap:
        return f"daily loss cap reached ({state.daily_realized_pnl:+.2f} <= -{daily_loss_cap:.2f})"
    if state.consecutive_losses >= cfg.consecutive_auto_max_consecutive_losses:
        return f"{state.consecutive_losses} consecutive losses"
    if state.daily_trade_count >= cfg.consecutive_auto_max_trades_per_day:
        return f"daily trade cap reached ({state.daily_trade_count})"
    return None


# ----------------------- Run extents from klines -----------------------


@dataclass
class RunExtents:
    run_high: float
    run_low: float
    last_close: float
    prior_bar_open: float  # open of the bar IMMEDIATELY BEFORE the trigger bar — used as TP target


def compute_run_extents(klines: list[Kline], run: RunResult) -> RunExtents:
    """Look at the last (run.n + run.doji_count) closed bars to find the
    run's high and low for structural stop placement, plus the open of the
    bar immediately before the trigger bar (used as the take-profit target)."""
    closed = klines[:-1]  # drop partial
    span = run.n + run.doji_count
    span = max(1, min(span, len(closed)))
    run_bars = closed[-span:]
    run_high = max(b.high for b in run_bars)
    run_low = min(b.low for b in run_bars)
    # Trigger bar = closed[-1]; the "prior bar" = closed[-2] (one before trigger)
    prior_bar_open = closed[-2].open if len(closed) >= 2 else closed[-1].open
    return RunExtents(
        run_high=run_high, run_low=run_low,
        last_close=closed[-1].close,
        prior_bar_open=prior_bar_open,
    )


# ----------------------- Engine -----------------------


@dataclass
class StepOutcome:
    action: str   # "no_op" | "opened" | "managed" | "blocked" | "error"
    detail: str = ""
    cycle_id: int | None = None
    extra: dict = field(default_factory=dict)


class ConsecutiveAutoEngine:
    def __init__(
        self,
        *,
        config: TradingConfig,
        storage: TradingStore,
        client: BinanceUSDMClient,
        state_path: Path | None = None,
    ) -> None:
        self.config = config
        self.storage = storage
        self.client = client
        self.state_path = state_path or default_state_path()

    def is_live_armed(self) -> bool:
        cfg = self.config
        return (not cfg.dry_run) and cfg.live_trading_enabled

    # ----- Entry path -----

    def maybe_open(
        self, *, run: RunResult, klines: list[Kline], state: AutoState,
    ) -> StepOutcome:
        cfg = self.config
        if not self.is_live_armed():
            return StepOutcome("blocked", "live env not armed (DRY_RUN=true or LIVE_TRADING_ENABLED=false)")
        block = safeguard_block_reason(state, cfg)
        if block:
            return StepOutcome("blocked", f"safeguard: {block}")
        if run.n < cfg.consecutive_auto_threshold:
            return StepOutcome("no_op", f"run {run.n} < threshold {cfg.consecutive_auto_threshold}")

        # Already alerted on this bar -> don't double-open
        last_bar = klines[-2] if len(klines) >= 2 else None
        if last_bar is None:
            return StepOutcome("no_op", "no closed bar")
        if state.last_alerted_bar_open_time_ms == last_bar.open_time:
            return StepOutcome("no_op", "already opened on this bar")

        # Freshness
        now_ms = _now_ms()
        bar_age_ms = now_ms - int(last_bar.close_time)
        if bar_age_ms < 0 or bar_age_ms > cfg.consecutive_auto_freshness_seconds * 1000:
            return StepOutcome("no_op", f"bar not fresh (age {bar_age_ms / 1000:.0f}s)")

        # Don't double-open if already an OPEN cycle for this strategy/symbol
        existing = self.storage.active_strategy_cycle(STRATEGY_NAME, cfg.consecutive_auto_symbol)
        if existing is not None:
            return StepOutcome("no_op", f"already open cycle id={existing['id']}")

        # Counter-trend: down run -> long, up run -> short
        side = "long" if run.direction == "down" else "short"
        extents = compute_run_extents(klines, run)

        return self._submit_live_cycle(
            run=run, side=side, extents=extents, state=state, now_ms=now_ms,
        )

    def _submit_live_cycle(
        self, *, run: RunResult, side: str, extents: RunExtents,
        state: AutoState, now_ms: int,
    ) -> StepOutcome:
        cfg = self.config
        symbol = cfg.consecutive_auto_symbol
        bnb_topup_summary: dict | None = None

        if cfg.bnb_fee_topup_before_auto_entry:
            from cointrading.bnb_fee_manager import ensure_bnb_fee_balance
            topup = ensure_bnb_fee_balance(client=self.client, config=cfg)
            if topup.action != "disabled":
                bnb_topup_summary = {
                    "ok": topup.ok,
                    "action": topup.action,
                    "message": topup.message,
                    "quote_amount_usdc": topup.quote_amount_usdc,
                    "transferred_bnb": topup.transferred_bnb,
                    "futures_bnb_available": topup.futures_bnb_available,
                }
            if not topup.ok:
                logger.warning("consecutive_auto: BNB top-up check failed: %s", topup.message)
                if cfg.bnb_fee_topup_required_for_live:
                    return StepOutcome("blocked", f"BNB fee top-up failed: {topup.message}")

        # Set leverage + isolated margin (idempotent — Binance returns
        # -4046 if already set, our client handles that)
        try:
            self.client.set_leverage(symbol=symbol, leverage=cfg.consecutive_auto_leverage)
        except BinanceAPIError as exc:
            return StepOutcome("error", f"set_leverage failed: {exc}")
        try:
            self.client.set_margin_type(symbol=symbol, margin_type="ISOLATED")
        except BinanceAPIError as exc:
            return StepOutcome("error", f"set_margin_type failed: {exc}")

        # Indicative price for sizing
        try:
            ticker = self.client.book_ticker(symbol)
            mid = (float(ticker["bidPrice"]) + float(ticker["askPrice"])) / 2.0
        except (BinanceAPIError, KeyError, ValueError) as exc:
            return StepOutcome("error", f"book_ticker failed: {exc}")
        if mid <= 0:
            return StepOutcome("error", "non-positive mid")

        # Sizing now uses CURRENT futures-wallet USDC, not initial_equity from config.
        # This way the auto-mode scales with the account: a $21k balance opens
        # 5x bigger positions than a $1k balance, all bounded by the leverage
        # bracket cap that Binance enforces.
        capital = self._current_capital_usdc()
        notional = capital * cfg.consecutive_auto_margin_pct * cfg.consecutive_auto_leverage
        intended_qty = notional / mid

        # ---- Entry ----
        if side == "long":
            entry_result = submit_live_market_long(
                client=self.client, store=self.storage, config=cfg,
                symbol=symbol, quantity=intended_qty, strategy_label="ca",
                timestamp_ms=now_ms,
            )
        else:
            entry_result = self._submit_market_short(
                symbol=symbol, quantity=intended_qty, now_ms=now_ms,
            )
        if not entry_result.success:
            return StepOutcome("error", f"entry failed: {entry_result.detail}")

        avg_entry = entry_result.avg_price
        actual_qty = entry_result.executed_qty

        # ---- Structural SL ----
        buf = cfg.consecutive_auto_sl_buffer_bps / 10_000.0
        if side == "long":
            sl_price = extents.run_low * (1.0 - buf)
            sl_distance = max(0.0, avg_entry - sl_price)
        else:
            sl_price = extents.run_high * (1.0 + buf)
            sl_distance = max(0.0, sl_price - avg_entry)
        sl_loss_bps = sl_distance / avg_entry * 10_000.0 if avg_entry > 0 else 0.0

        # ---- TP at the OPEN of the bar BEFORE the trigger bar ----
        tp_price = float(extents.prior_bar_open)
        tp_distance = max(0.0, abs(tp_price - avg_entry))
        tp_gain_bps = tp_distance / avg_entry * 10_000.0 if avg_entry > 0 else 0.0

        # ---- Persist cycle row + force-close at the next 15m bar boundary ----
        # In addition to the configured time-exit (e.g., 60 min), force-close
        # at the close of the bar we're entering DURING. This caps any single
        # auto-trade to "one bar's worth of waiting" at most.
        cfg_time_exit_ms = cfg.consecutive_auto_time_exit_minutes * 60 * 1000
        bar_minutes = {"5m": 5, "15m": 15, "1h": 60, "4h": 240}.get(
            cfg.consecutive_auto_interval, 15
        )
        bar_ms = bar_minutes * 60 * 1000
        next_bar_close_ms = ((now_ms // bar_ms) + 1) * bar_ms
        max_hold_deadline_ms = min(now_ms + cfg_time_exit_ms, next_bar_close_ms)
        max_hold_seconds = max(1, int((max_hold_deadline_ms - now_ms) / 1000))
        cycle_id = self.storage.insert_strategy_cycle(
            strategy=STRATEGY_NAME, execution_mode=EXECUTION_MODE,
            symbol=symbol, side=side, status=STATUS_OPEN,
            quantity=actual_qty, entry_price=avg_entry,
            target_price=tp_price, stop_price=sl_price,
            entry_order_type="MARKET", take_profit_bps=tp_gain_bps,
            stop_loss_bps=sl_loss_bps,
            max_hold_seconds=max_hold_seconds,
            maker_one_way_bps=cfg.maker_fee_rate * 10_000.0,
            taker_one_way_bps=cfg.taker_fee_rate * 10_000.0,
            entry_deadline_ms=now_ms,
            entry_order_id=entry_result.order_id,
            dry_run=False,
            reason=f"AUTO {run.direction} run={run.n}+doji{run.doji_count} {side}",
            opened_ms=now_ms, max_hold_deadline_ms=max_hold_deadline_ms,
            last_mid_price=avg_entry,
            setup={
                "hypothesis": "consecutive_bar_counter_trend",
                "run_direction": run.direction, "run_n": run.n,
                "run_doji_count": run.doji_count,
                "run_high": extents.run_high, "run_low": extents.run_low,
                "prior_bar_open": extents.prior_bar_open,
                "tp_price": tp_price, "sl_price": sl_price,
                "tp_gain_bps": tp_gain_bps, "sl_loss_bps": sl_loss_bps,
                "rr_ratio": (tp_gain_bps / sl_loss_bps) if sl_loss_bps > 0 else None,
                "leverage": cfg.consecutive_auto_leverage,
                "notional": notional, "margin_pct": cfg.consecutive_auto_margin_pct,
                "capital_at_entry": capital,
                "bnb_topup": bnb_topup_summary,
                "next_bar_close_ms": next_bar_close_ms,
                "live": True,
            },
            timestamp_ms=now_ms,
        )

        # ---- Submit SL ----
        sl_result = submit_protective_stop(
            client=self.client, store=self.storage, config=cfg,
            symbol=symbol, quantity=actual_qty, stop_price=sl_price,
            strategy_label="ca", timestamp_ms=now_ms,
        )
        if sl_result.order_id is None:
            self._emergency_close(cycle_id, symbol, side, actual_qty, "SL submit failed", now_ms)
            return StepOutcome("error", f"SL failed, emergency close: {sl_result.detail}",
                               cycle_id=cycle_id)

        # ---- Submit TP ----
        tp_result = submit_take_profit_market(
            client=self.client, store=self.storage, config=cfg,
            symbol=symbol, side=side, quantity=actual_qty, tp_price=tp_price,
            strategy_label="ca", timestamp_ms=now_ms,
        )
        if tp_result.order_id is None:
            # SL is already in place — TP failure is recoverable.
            # Keep the position; we'll just rely on SL + time exit.
            logger.warning("consecutive_auto: TP submit failed (continuing with SL-only): %s",
                           tp_result.detail)

        # Save SL as exit_order_id, TP into setup_json (since we have only one column)
        setup_json_extra = {"tp_order_id": tp_result.order_id}
        with self.storage.connect() as connection:
            row = connection.execute(
                "SELECT setup_json FROM strategy_cycles WHERE id=?", (cycle_id,)
            ).fetchone()
            if row is not None and row["setup_json"]:
                existing_setup = json.loads(row["setup_json"])
            else:
                existing_setup = {}
            existing_setup.update(setup_json_extra)
            connection.execute(
                "UPDATE strategy_cycles SET setup_json=? WHERE id=?",
                (json.dumps(existing_setup, sort_keys=True), cycle_id),
            )
        self.storage.update_strategy_cycle(
            cycle_id, exit_order_id=sl_result.order_id, timestamp_ms=now_ms,
        )

        # Update state — latch this bar so we don't re-open
        state.last_alerted_bar_open_time_ms = run.bar.open_time
        state.daily_trade_count += 1
        save_state(state, self.state_path)

        logger.info(
            "consecutive_auto OPEN id=%d %s %s qty=%.6f entry=%.6f sl=%.6f tp=%.6f notional=%.2f",
            cycle_id, symbol, side.upper(), actual_qty, avg_entry,
            sl_price, tp_price, notional,
        )
        return StepOutcome(
            "opened", f"{side} entry {avg_entry:.6f} sl {sl_price:.6f} tp {tp_price:.6f}",
            cycle_id=cycle_id,
            extra={
                "side": side, "entry": avg_entry, "sl": sl_price, "tp": tp_price,
                "sl_loss_bps": sl_loss_bps, "tp_gain_bps": tp_gain_bps,
                "notional": notional, "leverage": cfg.consecutive_auto_leverage,
                "capital": capital,
                "bnb_topup": bnb_topup_summary,
                "run_n": run.n, "run_direction": run.direction,
                "next_bar_close_ms": next_bar_close_ms,
            },
        )

    def _current_capital_usdc(self) -> float:
        """Read the current futures-wallet USDC available balance.
        Falls back to config.initial_equity if the API call fails or returns 0."""
        try:
            balances = self.client.account_balance()
        except BinanceAPIError as exc:
            logger.warning("consecutive_auto: account_balance failed, using initial_equity: %s", exc)
            return self.config.initial_equity
        for b in balances:
            if b.get("asset") == "USDC":
                avail = float(b.get("availableBalance") or 0)
                bal = float(b.get("balance") or 0)
                if avail > 0:
                    return avail
                if bal > 0:
                    return bal
        return self.config.initial_equity

    def _submit_market_short(
        self, *, symbol: str, quantity: float, now_ms: int,
    ):
        """Inline short MARKET submit. Not in live_execution.py because
        funding/wick are long-only; this is the only short path."""
        from cointrading.live_execution import LiveEntryResult, normalize_market_intent, client_order_id
        intent = OrderIntent(
            symbol=symbol, side="SELL", quantity=quantity,
            order_type="MARKET", response_type="RESULT", reduce_only=False,
            client_order_id=client_order_id("ca", symbol),
        )
        normalized, reason = normalize_market_intent(self.client, intent, self.config)
        if normalized is None:
            order_id = self.storage.insert_order_attempt(
                intent, status="BLOCKED", dry_run=False, reason=reason,
                timestamp_ms=now_ms,
            )
            return LiveEntryResult(False, order_id, 0.0, 0.0, reason)
        try:
            response = self.client.new_order(normalized)
        except BinanceAPIError as exc:
            detail = f"market short failed: {exc}"
            order_id = self.storage.insert_order_attempt(
                normalized, status="ERROR", dry_run=False, reason=detail,
                timestamp_ms=now_ms,
            )
            return LiveEntryResult(False, order_id, 0.0, 0.0, detail)
        avg_price = float(response.get("avgPrice") or 0.0)
        executed_qty = float(response.get("executedQty") or 0.0)
        status = str(response.get("status", "SUBMITTED"))
        order_id = self.storage.insert_order_attempt(
            normalized, status=status, dry_run=False, reason="live_short_entry",
            response=response, timestamp_ms=now_ms,
        )
        if status not in ("FILLED", "PARTIALLY_FILLED") or avg_price <= 0 or executed_qty <= 0:
            return LiveEntryResult(False, order_id, avg_price, executed_qty,
                                   f"unexpected entry status: {status}")
        return LiveEntryResult(True, order_id, avg_price, executed_qty,
                               "live short entry filled", response=response)

    def _emergency_close(
        self, cycle_id: int, symbol: str, side: str, quantity: float,
        reason: str, now_ms: int,
    ) -> None:
        cycle_row = self.storage.active_strategy_cycle(STRATEGY_NAME, symbol)
        if cycle_row is None:
            return
        close = submit_live_market_close(
            client=self.client, store=self.storage, config=self.config,
            cycle=cycle_row, reason="emergency", timestamp_ms=now_ms,
        )
        pnl = realized_pnl_from_close(
            cycle=cycle_row, avg_exit_price=close.avg_price or 0.0,
            executed_qty=close.executed_qty or quantity, config=self.config,
        )
        self.storage.update_strategy_cycle(
            cycle_id, status=STATUS_STOPPED, reason=f"emergency: {reason}",
            closed_ms=now_ms, realized_pnl=pnl, timestamp_ms=now_ms,
        )

    # ----- Manage path (called every step) -----

    def manage_open_cycles(self, state: AutoState) -> list[dict]:
        results: list[dict] = []
        now_ms = _now_ms()
        cycle = self.storage.active_strategy_cycle(STRATEGY_NAME, self.config.consecutive_auto_symbol)
        if cycle is None or cycle["status"] != STATUS_OPEN:
            return results
        if int(cycle["dry_run"]) != 0:
            return results  # paper-only path is unused here

        symbol = str(cycle["symbol"])
        side = str(cycle["side"])

        # Read TP order id from setup_json
        tp_order_id: int | None = None
        if cycle["setup_json"]:
            try:
                setup = json.loads(cycle["setup_json"])
                tp_order_id = setup.get("tp_order_id")
            except (json.JSONDecodeError, TypeError):
                pass

        sl_order_id = cycle["exit_order_id"]

        # Poll SL first
        if sl_order_id is not None:
            status = query_live_order_status(
                client=self.client, store=self.storage, order_id=int(sl_order_id),
            )
            if status is not None and str(status.get("status")) == "FILLED":
                avg_exit = float(status.get("avgPrice") or 0.0) or self._safe_mark(symbol, cycle)
                exec_qty = float(status.get("executedQty") or float(cycle["quantity"]))
                if tp_order_id is not None:
                    cancel_local_order(client=self.client, store=self.storage,
                                       local_order_id=int(tp_order_id))
                pnl = realized_pnl_from_close(
                    cycle=cycle, avg_exit_price=avg_exit,
                    executed_qty=exec_qty, config=self.config,
                )
                self._close_and_update_state(cycle, STATUS_STOPPED, "exchange_sl_filled",
                                             avg_exit, pnl, state, now_ms)
                results.append({"id": cycle["id"], "action": "stopped", "exit": avg_exit, "pnl": pnl})
                return results

        # Poll TP
        if tp_order_id is not None:
            status = query_live_order_status(
                client=self.client, store=self.storage, order_id=int(tp_order_id),
            )
            if status is not None and str(status.get("status")) == "FILLED":
                avg_exit = float(status.get("avgPrice") or 0.0) or self._safe_mark(symbol, cycle)
                exec_qty = float(status.get("executedQty") or float(cycle["quantity"]))
                if sl_order_id is not None:
                    cancel_local_order(client=self.client, store=self.storage,
                                       local_order_id=int(sl_order_id))
                pnl = realized_pnl_from_close(
                    cycle=cycle, avg_exit_price=avg_exit,
                    executed_qty=exec_qty, config=self.config,
                )
                self._close_and_update_state(cycle, STATUS_CLOSED, "exchange_tp_filled",
                                             avg_exit, pnl, state, now_ms)
                results.append({"id": cycle["id"], "action": "closed_tp", "exit": avg_exit, "pnl": pnl})
                return results

        # Time exit
        max_hold_deadline_ms = cycle["max_hold_deadline_ms"]
        if max_hold_deadline_ms is not None and now_ms >= int(max_hold_deadline_ms):
            # Cancel both protective orders
            if sl_order_id is not None:
                cancel_local_order(client=self.client, store=self.storage,
                                   local_order_id=int(sl_order_id))
            if tp_order_id is not None:
                cancel_local_order(client=self.client, store=self.storage,
                                   local_order_id=int(tp_order_id))
            # Submit reduce-only market close (manual since side may be short)
            close = self._submit_reduce_only_market_close(cycle, side, now_ms)
            if not close.success:
                results.append({"id": cycle["id"], "action": "time_exit_error", "detail": close.detail})
                return results
            pnl = realized_pnl_from_close(
                cycle=cycle, avg_exit_price=close.avg_price,
                executed_qty=close.executed_qty, config=self.config,
            )
            self._close_and_update_state(cycle, STATUS_CLOSED, "time_exit",
                                         close.avg_price, pnl, state, now_ms)
            results.append({"id": cycle["id"], "action": "closed_time", "exit": close.avg_price, "pnl": pnl})
            return results

        # Hold — update last_mid_price for dashboard
        mark = self._safe_mark(symbol, cycle)
        self.storage.update_strategy_cycle(
            int(cycle["id"]), last_mid_price=mark, timestamp_ms=now_ms,
        )
        results.append({"id": cycle["id"], "action": "hold", "mark": mark})
        return results

    def _safe_mark(self, symbol: str, cycle: Any) -> float:
        try:
            ticker = self.client.book_ticker(symbol)
            return (float(ticker["bidPrice"]) + float(ticker["askPrice"])) / 2.0
        except (BinanceAPIError, KeyError, ValueError):
            return float(cycle["entry_price"])

    def _submit_reduce_only_market_close(self, cycle: Any, side: str, now_ms: int):
        """Reduce-only MARKET close in the opposite direction of the cycle side."""
        from cointrading.live_execution import LiveEntryResult, normalize_market_intent, client_order_id
        symbol = str(cycle["symbol"])
        quantity = float(cycle["quantity"])
        order_side = "SELL" if side == "long" else "BUY"
        intent = OrderIntent(
            symbol=symbol, side=order_side, quantity=quantity,
            order_type="MARKET", response_type="RESULT", reduce_only=True,
            client_order_id=client_order_id("time_ca", symbol),
        )
        normalized, reason = normalize_market_intent(self.client, intent, self.config)
        if normalized is None:
            self.storage.insert_order_attempt(
                intent, status="BLOCKED", dry_run=False, reason=reason, timestamp_ms=now_ms,
            )
            return LiveEntryResult(False, None, 0.0, 0.0, reason)
        try:
            response = self.client.new_order(normalized)
        except BinanceAPIError as exc:
            detail = f"time-exit close failed: {exc}"
            self.storage.insert_order_attempt(
                normalized, status="ERROR", dry_run=False, reason=detail, timestamp_ms=now_ms,
            )
            return LiveEntryResult(False, None, 0.0, 0.0, detail)
        avg_price = float(response.get("avgPrice") or 0.0)
        executed_qty = float(response.get("executedQty") or 0.0)
        status = str(response.get("status", "SUBMITTED"))
        order_id = self.storage.insert_order_attempt(
            normalized, status=status, dry_run=False, reason="time_exit",
            response=response, timestamp_ms=now_ms,
        )
        return LiveEntryResult(
            status in ("FILLED", "PARTIALLY_FILLED"),
            order_id, avg_price, executed_qty, f"close {status}",
            response=response,
        )

    def _close_and_update_state(
        self, cycle: Any, status: str, reason: str, avg_exit: float,
        pnl: float, state: AutoState, now_ms: int,
    ) -> None:
        self.storage.update_strategy_cycle(
            int(cycle["id"]), status=status, reason=reason,
            closed_ms=now_ms, last_mid_price=avg_exit, realized_pnl=pnl,
            timestamp_ms=now_ms,
        )
        # Update auto state
        reset_daily_if_needed(state, now_ms)
        state.daily_realized_pnl += pnl
        if pnl > 0:
            state.consecutive_losses = 0
        else:
            state.consecutive_losses += 1

        # Auto-disable if any safeguard triggered by this close
        cfg = self.config
        if state.daily_realized_pnl <= -cfg.initial_equity * cfg.consecutive_auto_daily_loss_pct:
            state.auto_mode = False
            state.paused_reason = f"daily loss cap: {state.daily_realized_pnl:+.2f}"
        elif state.consecutive_losses >= cfg.consecutive_auto_max_consecutive_losses:
            state.auto_mode = False
            state.paused_reason = f"{state.consecutive_losses} consecutive losses"
        elif state.daily_trade_count >= cfg.consecutive_auto_max_trades_per_day:
            state.auto_mode = False
            state.paused_reason = f"daily trade cap reached ({state.daily_trade_count})"

        save_state(state, self.state_path)
        logger.info("consecutive_auto %s id=%d reason=%s pnl=%+.4f, daily_pnl=%+.4f",
                    status, int(cycle["id"]), reason, pnl, state.daily_realized_pnl)
