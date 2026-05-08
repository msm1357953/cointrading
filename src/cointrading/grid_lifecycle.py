"""Maker-only BTCUSDC grid ("띠기") lifecycle.

This is intentionally separate from the retired scalping/tactical engines.
It only acts when Telegram sets a state-file mode:

    LONG  - place post-only maker BUY levels below price, TP with maker SELL.
    SHORT - place post-only maker SELL levels above price, TP with maker BUY.
    AUTO  - choose LONG/SHORT from a simple trend filter, otherwise wait.

Sizing is by notional exposure, never by margin * leverage. With 20x isolated,
5% notional on a 20k USDC account is a 1k USDC position that uses roughly
50 USDC margin. The leverage setting is used to reduce locked margin, not to
multiply the intended exposure.
"""
from __future__ import annotations

import json
import logging
import statistics
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from cointrading.config import TelegramConfig, TradingConfig
from cointrading.exchange.binance_usdm import BinanceAPIError, BinanceUSDMClient
from cointrading.live_execution import (
    cancel_local_order,
    client_order_id,
    normalize_market_intent,
    query_live_order_status,
)
from cointrading.models import Kline, OrderIntent
from cointrading.orderflow_guard import load_latest_snapshot
from cointrading.storage import TradingStore, default_db_path, kst_from_ms, now_ms as store_now_ms
from cointrading.telegram_bot import TelegramClient, TelegramConfigError


logger = logging.getLogger(__name__)


STRATEGY_NAME = "maker_grid"
EXECUTION_MODE = "post_only_grid"
STATUS_ENTRY_SUBMITTED = "ENTRY_SUBMITTED"
STATUS_OPEN = "OPEN"
STATUS_CLOSED = "CLOSED"
STATUS_STOPPED = "STOPPED"

MODE_STOPPED = "STOPPED"
MODE_LONG = "LONG"
MODE_SHORT = "SHORT"
MODE_AUTO = "AUTO"
ACTIVE_MODES = {MODE_LONG, MODE_SHORT, MODE_AUTO}


def default_state_path() -> Path:
    return default_db_path().parent / "grid_state.json"


def _now_ms() -> int:
    return int(time.time() * 1000)


def _kst_today_str(ts_ms: int) -> str:
    dt = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc) + timedelta(hours=9)
    return dt.strftime("%Y-%m-%d")


@dataclass
class GridState:
    mode: str = MODE_STOPPED
    force_entry: bool = False
    paused_reason: str = ""
    daily_kst_date: str = ""
    daily_realized_pnl: float = 0.0
    daily_order_count: int = 0
    consecutive_losses: int = 0
    loss_cooldown_until_ms: int = 0
    orderflow_long_danger_count: int = 0
    orderflow_short_danger_count: int = 0
    orderflow_long_recovery_count: int = 0
    orderflow_short_recovery_count: int = 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "mode": self.mode,
            "force_entry": self.force_entry,
            "paused_reason": self.paused_reason,
            "daily_kst_date": self.daily_kst_date,
            "daily_realized_pnl": self.daily_realized_pnl,
            "daily_order_count": self.daily_order_count,
            "consecutive_losses": self.consecutive_losses,
            "loss_cooldown_until_ms": self.loss_cooldown_until_ms,
            "orderflow_long_danger_count": self.orderflow_long_danger_count,
            "orderflow_short_danger_count": self.orderflow_short_danger_count,
            "orderflow_long_recovery_count": self.orderflow_long_recovery_count,
            "orderflow_short_recovery_count": self.orderflow_short_recovery_count,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "GridState":
        mode = str(data.get("mode", MODE_STOPPED)).upper()
        if mode not in {MODE_STOPPED, MODE_LONG, MODE_SHORT, MODE_AUTO}:
            mode = MODE_STOPPED
        return cls(
            mode=mode,
            force_entry=bool(data.get("force_entry", False)),
            paused_reason=str(data.get("paused_reason", "")),
            daily_kst_date=str(data.get("daily_kst_date", "")),
            daily_realized_pnl=float(data.get("daily_realized_pnl", 0.0)),
            daily_order_count=int(data.get("daily_order_count", 0)),
            consecutive_losses=int(data.get("consecutive_losses", 0)),
            loss_cooldown_until_ms=int(data.get("loss_cooldown_until_ms", 0)),
            orderflow_long_danger_count=int(data.get("orderflow_long_danger_count", 0)),
            orderflow_short_danger_count=int(data.get("orderflow_short_danger_count", 0)),
            orderflow_long_recovery_count=int(data.get("orderflow_long_recovery_count", 0)),
            orderflow_short_recovery_count=int(data.get("orderflow_short_recovery_count", 0)),
        )


def load_state(path: Path | None = None) -> GridState:
    p = path or default_state_path()
    if not p.exists():
        return GridState()
    try:
        return GridState.from_dict(json.loads(p.read_text()))
    except (OSError, json.JSONDecodeError, TypeError, ValueError):
        return GridState()


def save_state(state: GridState, path: Path | None = None) -> None:
    p = path or default_state_path()
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(state.to_dict(), indent=2, sort_keys=True))


def reset_daily_if_needed(state: GridState, ts_ms: int) -> None:
    today = _kst_today_str(ts_ms)
    if state.daily_kst_date != today:
        state.daily_kst_date = today
        state.daily_realized_pnl = 0.0
        state.daily_order_count = 0
        state.consecutive_losses = 0
        state.loss_cooldown_until_ms = 0


def live_gate_text(config: TradingConfig) -> str:
    gates = [
        f"DRY_RUN={'false' if not config.dry_run else 'true'}",
        f"LIVE_TRADING_ENABLED={'true' if config.live_trading_enabled else 'false'}",
        f"GRID_LIVE_ENABLED={'true' if config.grid_live_enabled else 'false'}",
    ]
    return ", ".join(gates)


def is_live_armed(config: TradingConfig) -> bool:
    return (not config.dry_run) and config.live_trading_enabled and config.grid_live_enabled


def safeguard_block_reason(
    state: GridState,
    config: TradingConfig,
    *,
    ts_ms: int | None = None,
) -> str | None:
    if not config.grid_enabled:
        return "grid disabled"
    if state.mode not in ACTIVE_MODES:
        return "grid mode STOPPED"
    now = ts_ms or _now_ms()
    if state.loss_cooldown_until_ms > now:
        return (
            f"{state.consecutive_losses} consecutive losses; cooldown until "
            f"{kst_from_ms(state.loss_cooldown_until_ms)}"
        )
    if state.daily_order_count >= config.grid_max_orders_per_day:
        return f"daily order cap reached ({state.daily_order_count})"
    return None


@dataclass(frozen=True)
class GridMarket:
    bid: float
    ask: float
    mid: float
    gap_usdc: float
    take_profit_usdc: float
    atr_5m: float
    atr_5m_median_24h: float
    ret_15m: float
    ret_1h: float
    big_candle: bool
    effective_side: str | None
    risk_label: str
    risk_reason: str
    orderflow_status: str = "UNKNOWN"
    orderflow_long_status: str = "UNKNOWN"
    orderflow_short_status: str = "UNKNOWN"
    orderflow_reason: str = ""
    orderflow_age_seconds: float | None = None
    orderflow_bid_depth_010: float = 0.0
    orderflow_ask_depth_010: float = 0.0
    range_low_15m: float = 0.0
    range_high_15m: float = 0.0
    range_position_15m: float = 0.5


@dataclass
class StepResult:
    managed: list[dict[str, Any]] = field(default_factory=list)
    opened: list[dict[str, Any]] = field(default_factory=list)
    skipped: list[dict[str, Any]] = field(default_factory=list)
    risk: list[dict[str, Any]] = field(default_factory=list)
    ts_ms: int = 0

    def as_dict(self) -> dict[str, Any]:
        return {
            "ts_ms": self.ts_ms,
            "managed": self.managed,
            "opened": self.opened,
            "skipped": self.skipped,
            "risk": self.risk,
        }


class MakerGridEngine:
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

    def step(self) -> StepResult:
        ts = _now_ms()
        state = load_state(self.state_path)
        reset_daily_if_needed(state, ts)
        if state.loss_cooldown_until_ms and state.loss_cooldown_until_ms <= ts:
            state.loss_cooldown_until_ms = 0
            if "cooldown until" in state.paused_reason:
                state.paused_reason = ""
        result = StepResult(ts_ms=ts)

        active_cycles = self._active_grid_cycles()
        if state.mode == MODE_STOPPED and not active_cycles:
            result.skipped.append({"reason": "mode_stopped", "detail": state.paused_reason})
            if self.config.grid_paper_log_enabled:
                try:
                    market = self._load_market()
                except Exception as exc:  # noqa: BLE001
                    self._record_decision(
                        state=state,
                        market=None,
                        result=result,
                        action="observe_error",
                        reason=f"market_load_failed: {exc}",
                    )
                else:
                    self._update_orderflow_state(state, market)
                    self._record_decision(
                        state=state,
                        market=market,
                        result=result,
                        intended_side=market.effective_side,
                        action="observe",
                        reason="mode_stopped",
                    )
            save_state(state, self.state_path)
            return result

        market = self._load_market()
        self._update_orderflow_state(state, market)
        if state.mode == MODE_STOPPED:
            cancelled = self._cancel_pending_entries(ts, "manual_stop")
            if cancelled:
                result.risk.append({"action": "manual_stop_cancel", "cancelled": cancelled})
            result.managed.extend(self._manage_cycles(market, ts, state))
            result.managed.extend(self._sync_basket_take_profit_orders(market, ts, state))
            result.risk.extend(self._apply_risk_controls(market, ts, state))
            result.skipped.append({"reason": "mode_stopped", "detail": state.paused_reason})
            self._record_decision(
                state=state,
                market=market,
                result=result,
                action="stopped_manage",
                reason=state.paused_reason or "mode_stopped",
            )
            save_state(state, self.state_path)
            return result

        result.managed.extend(self._manage_cycles(market, ts, state))
        result.managed.extend(self._sync_basket_take_profit_orders(market, ts, state))
        danger_cancelled = self._cancel_entries_on_confirmed_orderflow_danger(ts, state)
        if danger_cancelled:
            result.risk.append({"action": "orderflow_danger_cancel", "cancelled": danger_cancelled})
        result.risk.extend(self._apply_risk_controls(market, ts, state))
        if not is_live_armed(self.config):
            result.skipped.append({"reason": "live_gate_locked", "detail": live_gate_text(self.config)})
            self._record_decision(
                state=state,
                market=market,
                result=result,
                intended_side=self._effective_side_for_state(state, market),
                action="live_gate_locked",
                reason=live_gate_text(self.config),
            )
            save_state(state, self.state_path)
            return result

        block = safeguard_block_reason(state, self.config, ts_ms=ts)
        if block:
            result.skipped.append({"reason": "safeguard", "detail": block})
            self._record_decision(
                state=state,
                market=market,
                result=result,
                intended_side=self._effective_side_for_state(state, market),
                action="safeguard",
                reason=block,
            )
            save_state(state, self.state_path)
            return result

        side = self._effective_side_for_state(state, market)
        if side is None:
            result.skipped.append({
                "reason": "auto_wait",
                "detail": market.risk_reason or "AUTO mode has no clear side",
            })
            self._record_decision(
                state=state,
                market=market,
                result=result,
                action="auto_wait",
                reason=market.risk_reason or "AUTO mode has no clear side",
            )
            save_state(state, self.state_path)
            return result
        active_side = self._active_grid_side()
        if active_side is not None and active_side != side:
            result.skipped.append({
                "reason": "active_side_lock",
                "side": side,
                "detail": f"active {active_side} grid exists; wait until it closes before switching",
            })
            self._record_decision(
                state=state,
                market=market,
                result=result,
                intended_side=side,
                action="active_side_lock",
                reason=f"active {active_side} grid exists",
            )
            save_state(state, self.state_path)
            return result
        if self._side_halted(side, market, state):
            self._cancel_pending_entries(ts, f"risk_{market.risk_label}")
            result.skipped.append({
                "reason": "risk_halt",
                "side": side,
                "detail": self._side_halt_reason(side, market, state),
            })
            self._record_decision(
                state=state,
                market=market,
                result=result,
                intended_side=side,
                action="risk_halt",
                reason=self._side_halt_reason(side, market, state),
            )
            save_state(state, self.state_path)
            return result
        if self._soft_filter_blocked(side, market, state):
            result.skipped.append({
                "reason": "soft_filter",
                "side": side,
                "detail": self._soft_filter_reason(side, market),
                "force_hint": "띠기 롱 강제 시작 / 띠기 숏 강제 시작",
            })
            self._record_decision(
                state=state,
                market=market,
                result=result,
                intended_side=side,
                action="soft_filter",
                reason=self._soft_filter_reason(side, market),
            )
            save_state(state, self.state_path)
            return result

        result.opened.extend(self._place_missing_entries(side, market, ts, state))
        self._record_decision(
            state=state,
            market=market,
            result=result,
            intended_side=side,
            action="entry_submitted" if result.opened else "entry_wait",
            reason="placed missing grid entries" if result.opened else "grid layers already occupied",
        )
        save_state(state, self.state_path)
        return result

    # ----- Market / risk -----

    def _load_market(self) -> GridMarket:
        symbol = self.config.grid_symbol
        ticker = self.client.book_ticker(symbol)
        bid = float(ticker["bidPrice"])
        ask = float(ticker["askPrice"])
        mid = (bid + ask) / 2.0

        klines_5m = self.client.klines(symbol=symbol, interval="5m", limit=300)
        klines_15m = self.client.klines(
            symbol=symbol,
            interval="15m",
            limit=max(10, self.config.grid_position_lookback_15m_bars),
        )
        klines_1h = self.client.klines(symbol=symbol, interval="1h", limit=10)
        closed_5m = _closed_klines(klines_5m)
        atr_values = _true_ranges(closed_5m)
        recent_atr = sum(atr_values[-14:]) / min(14, len(atr_values)) if atr_values else mid * 0.001
        median_atr = statistics.median(atr_values[-288:]) if atr_values else recent_atr
        if median_atr <= 0:
            median_atr = recent_atr or mid * 0.001

        gap = _clamp(
            recent_atr * self.config.grid_gap_atr_mult,
            self.config.grid_gap_min_usdc,
            self.config.grid_gap_max_usdc,
        )
        take_profit = _clamp(
            recent_atr * self.config.grid_take_profit_atr_mult,
            self.config.grid_take_profit_min_usdc,
            self.config.grid_take_profit_max_usdc,
        )
        ret_15m = _return_from_klines(klines_15m, bars=1)
        ret_1h = _return_from_klines(klines_1h, bars=1)
        latest = closed_5m[-1] if closed_5m else None
        big_candle = False
        if latest is not None and recent_atr > 0:
            body = abs(latest.close - latest.open)
            close_near_low = latest.close <= latest.low + (latest.high - latest.low) * 0.25
            close_near_high = latest.close >= latest.high - (latest.high - latest.low) * 0.25
            big_candle = (
                body >= recent_atr * self.config.grid_big_candle_atr_multiple
                and (close_near_low or close_near_high)
            )
        atr_spike = recent_atr >= median_atr * self.config.grid_atr_spike_multiple
        if atr_spike:
            risk_label = "HALT"
            risk_reason = f"ATR spike {recent_atr:.2f} >= {median_atr:.2f}*{self.config.grid_atr_spike_multiple:.1f}"
        elif big_candle:
            risk_label = "HALT"
            risk_reason = "large 5m candle"
        else:
            risk_label = "NORMAL"
            risk_reason = ""

        range_low, range_high, range_position = _range_position_from_klines(
            klines_15m,
            mid=mid,
            lookback=self.config.grid_position_lookback_15m_bars,
        )
        auto_side = _auto_side(klines_15m, klines_1h)
        orderflow = load_latest_snapshot(self.config)
        return GridMarket(
            bid=bid,
            ask=ask,
            mid=mid,
            gap_usdc=gap,
            take_profit_usdc=take_profit,
            atr_5m=recent_atr,
            atr_5m_median_24h=median_atr,
            ret_15m=ret_15m,
            ret_1h=ret_1h,
            big_candle=big_candle,
            effective_side=auto_side,
            risk_label=risk_label,
            risk_reason=risk_reason,
            orderflow_status=orderflow.status,
            orderflow_long_status=orderflow.long_status,
            orderflow_short_status=orderflow.short_status,
            orderflow_reason=orderflow.reason,
            orderflow_age_seconds=orderflow.age_seconds,
            orderflow_bid_depth_010=float(orderflow.data.get("bid_depth_010") or 0.0),
            orderflow_ask_depth_010=float(orderflow.data.get("ask_depth_010") or 0.0),
            range_low_15m=range_low,
            range_high_15m=range_high,
            range_position_15m=range_position,
        )

    def _side_halted(self, side: str, market: GridMarket, state: GridState) -> bool:
        if market.risk_label == "HALT":
            return True
        side_orderflow_status = _side_orderflow_status(side, market)
        if self.config.orderflow_guard_enabled and side_orderflow_status in {
            "STALE",
            "UNKNOWN",
        }:
            return True
        if self.config.orderflow_guard_enabled and side_orderflow_status == "DANGER":
            return self._confirmed_orderflow_danger_from_state(side, state)
        if side == "long":
            return (
                market.ret_15m <= -self.config.grid_overheat_15m_return_pct
                or market.ret_1h <= -self.config.grid_overheat_1h_return_pct
            )
        return (
            market.ret_15m >= self.config.grid_overheat_15m_return_pct
            or market.ret_1h >= self.config.grid_overheat_1h_return_pct
        )

    def _side_halt_reason(self, side: str, market: GridMarket, state: GridState) -> str:
        if market.risk_reason:
            return market.risk_reason
        side_orderflow_status = _side_orderflow_status(side, market)
        if self.config.orderflow_guard_enabled and side_orderflow_status in {
            "STALE",
            "UNKNOWN",
        }:
            return f"orderflow {side_orderflow_status}: {market.orderflow_reason}"
        if self.config.orderflow_guard_enabled and side_orderflow_status == "DANGER":
            count = self._orderflow_danger_count(side, state)
            threshold = self.config.grid_orderflow_confirmations
            suffix = "확정" if count >= threshold else f"관찰중 {count}/{threshold}"
            return f"orderflow DANGER {suffix}: {market.orderflow_reason}"
        if side == "long":
            if market.ret_15m <= -self.config.grid_overheat_15m_return_pct:
                return f"15m adverse move {market.ret_15m * 100:+.2f}%"
            if market.ret_1h <= -self.config.grid_overheat_1h_return_pct:
                return f"1h adverse move {market.ret_1h * 100:+.2f}%"
        else:
            if market.ret_15m >= self.config.grid_overheat_15m_return_pct:
                return f"15m adverse move {market.ret_15m * 100:+.2f}%"
            if market.ret_1h >= self.config.grid_overheat_1h_return_pct:
                return f"1h adverse move {market.ret_1h * 100:+.2f}%"
        return "risk halt"

    def _soft_filter_blocked(self, side: str, market: GridMarket, state: GridState) -> bool:
        if state.force_entry:
            return False
        return bool(self._soft_filter_reason(side, market))

    def _soft_filter_reason(self, side: str, market: GridMarket) -> str:
        reasons = _soft_filter_reasons(side, market, self.config)
        return "; ".join(reasons)

    def _update_orderflow_state(self, state: GridState, market: GridMarket) -> None:
        self._update_orderflow_side_state(state, "long", market.orderflow_long_status)
        self._update_orderflow_side_state(state, "short", market.orderflow_short_status)

    def _update_orderflow_side_state(self, state: GridState, side: str, status: str) -> None:
        danger_attr = f"orderflow_{side}_danger_count"
        recovery_attr = f"orderflow_{side}_recovery_count"
        if status == "DANGER":
            count = min(
                self.config.grid_orderflow_confirmations,
                getattr(state, danger_attr) + 1,
            )
            setattr(state, danger_attr, count)
            setattr(state, recovery_attr, 0)
            return
        if status in {"NORMAL", "CAUTION", "DISABLED"}:
            recovery = min(
                self.config.grid_orderflow_recovery_confirmations,
                getattr(state, recovery_attr) + 1,
            )
            setattr(state, recovery_attr, recovery)
            if getattr(state, recovery_attr) >= self.config.grid_orderflow_recovery_confirmations:
                setattr(state, danger_attr, 0)
            return
        setattr(state, danger_attr, self.config.grid_orderflow_confirmations)
        setattr(state, recovery_attr, 0)

    def _cancel_entries_on_confirmed_orderflow_danger(
        self,
        ts: int,
        state: GridState,
    ) -> int:
        cancelled = 0
        for side in ("long", "short"):
            if not self._confirmed_orderflow_danger_from_state(side, state):
                continue
            for cycle in self._active_grid_cycles(status=STATUS_ENTRY_SUBMITTED):
                if str(cycle["side"]) != side:
                    continue
                if cycle["entry_order_id"] is not None:
                    cancel_local_order(
                        client=self.client,
                        store=self.storage,
                        local_order_id=int(cycle["entry_order_id"]),
                    )
                self.storage.update_strategy_cycle(
                    int(cycle["id"]),
                    status=STATUS_STOPPED,
                    reason=f"orderflow_{side}_danger",
                    closed_ms=ts,
                    timestamp_ms=ts,
                )
                cancelled += 1
        return cancelled

    def _confirmed_orderflow_danger_from_state(self, side: str, state: GridState) -> bool:
        count = self._orderflow_danger_count(side, state)
        return count >= self.config.grid_orderflow_confirmations

    def _orderflow_danger_count(self, side: str, state: GridState) -> int:
        return (
            state.orderflow_long_danger_count
            if side == "long"
            else state.orderflow_short_danger_count
        )

    def _record_decision(
        self,
        *,
        state: GridState,
        market: GridMarket | None,
        result: StepResult,
        action: str,
        reason: str,
        intended_side: str | None = None,
    ) -> None:
        if not self.config.grid_paper_log_enabled:
            return
        try:
            cycles = self._active_grid_cycles()
            active_entries = sum(1 for cycle in cycles if cycle["status"] == STATUS_ENTRY_SUBMITTED)
            active_opens = sum(1 for cycle in cycles if cycle["status"] == STATUS_OPEN)
            self.storage.insert_grid_decision(
                symbol=self.config.grid_symbol,
                mode=state.mode,
                intended_side=intended_side,
                effective_side=market.effective_side if market is not None else None,
                action=action,
                reason=reason,
                timestamp_ms=result.ts_ms,
                live_armed=is_live_armed(self.config),
                force_entry=state.force_entry,
                bid=market.bid if market is not None else None,
                ask=market.ask if market is not None else None,
                mid=market.mid if market is not None else None,
                gap_usdc=market.gap_usdc if market is not None else None,
                take_profit_usdc=market.take_profit_usdc if market is not None else None,
                atr_5m=market.atr_5m if market is not None else None,
                atr_5m_median_24h=market.atr_5m_median_24h if market is not None else None,
                ret_15m=market.ret_15m if market is not None else None,
                ret_1h=market.ret_1h if market is not None else None,
                range_position_15m=market.range_position_15m if market is not None else None,
                risk_label=market.risk_label if market is not None else None,
                risk_reason=market.risk_reason if market is not None else None,
                orderflow_status=market.orderflow_status if market is not None else None,
                orderflow_long_status=market.orderflow_long_status if market is not None else None,
                orderflow_short_status=market.orderflow_short_status if market is not None else None,
                orderflow_reason=market.orderflow_reason if market is not None else None,
                orderflow_long_danger_count=state.orderflow_long_danger_count,
                orderflow_short_danger_count=state.orderflow_short_danger_count,
                orderflow_long_recovery_count=state.orderflow_long_recovery_count,
                orderflow_short_recovery_count=state.orderflow_short_recovery_count,
                active_entries=active_entries,
                active_opens=active_opens,
                opened_count=len(result.opened),
                managed_count=len(result.managed),
                risk_count=len(result.risk),
                raw=result.as_dict(),
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("maker_grid: failed to record grid decision: %s", exc)

    def _effective_side_for_state(self, state: GridState, market: GridMarket) -> str | None:
        if state.mode == MODE_LONG:
            return "long"
        if state.mode == MODE_SHORT:
            return "short"
        if state.mode == MODE_AUTO:
            return market.effective_side
        return None

    def _apply_risk_controls(
        self,
        market: GridMarket,
        ts: int,
            state: GridState,
    ) -> list[dict[str, Any]]:
        capital = self._current_capital_usdc()
        unrealized = self._grid_unrealized_pnl(market.mid)
        loss_pct = (-unrealized / capital) if unrealized < 0 and capital > 0 else 0.0
        events: list[dict[str, Any]] = []
        if loss_pct >= self.config.grid_stop_loss_pct:
            state.mode = MODE_STOPPED
            state.paused_reason = f"grid stop loss {loss_pct:.2%}"
            cancelled = self._cancel_pending_entries(ts, "grid_stop_loss")
            closed = self._close_all_open_cycles(market.mid, ts, "grid_stop_loss", state)
            events.append({
                "action": "grid_stop",
                "unrealized": unrealized,
                "loss_pct": loss_pct,
                "cancelled": cancelled,
                "closed": closed,
            })
        elif loss_pct >= self.config.grid_reduce_loss_pct:
            cancelled = self._cancel_pending_entries(ts, "grid_reduce_loss")
            closed = self._close_oldest_half(market.mid, ts, "grid_reduce_loss", state)
            events.append({
                "action": "grid_reduce",
                "unrealized": unrealized,
                "loss_pct": loss_pct,
                "cancelled": cancelled,
                "closed": closed,
            })
        elif loss_pct >= self.config.grid_warning_loss_pct:
            cancelled = self._cancel_pending_entries(ts, "grid_warning_loss")
            events.append({
                "action": "grid_warning",
                "unrealized": unrealized,
                "loss_pct": loss_pct,
                "cancelled": cancelled,
            })
        return events

    # ----- Cycle management -----

    def _manage_cycles(self, market: GridMarket, ts: int, state: GridState) -> list[dict[str, Any]]:
        events: list[dict[str, Any]] = []
        for cycle in self._active_grid_cycles():
            status = str(cycle["status"])
            if status == STATUS_ENTRY_SUBMITTED:
                events.extend(self._manage_entry_order(cycle, market, ts, state))
            elif status == STATUS_OPEN:
                events.extend(self._manage_open_cycle(cycle, market, ts, state))
        return events

    def _manage_entry_order(
        self,
        cycle: Any,
        market: GridMarket,
        ts: int,
        state: GridState,
    ) -> list[dict[str, Any]]:
        if cycle["entry_order_id"] is None:
            self.storage.update_strategy_cycle(
                int(cycle["id"]), status=STATUS_STOPPED,
                reason="entry_order_missing", timestamp_ms=ts,
            )
            return [{"id": cycle["id"], "action": "entry_missing"}]
        age_ms = ts - int(cycle["created_ms"])
        if age_ms >= self.config.grid_entry_order_ttl_seconds * 1000:
            cancel_local_order(
                client=self.client, store=self.storage, local_order_id=int(cycle["entry_order_id"])
            )
            self.storage.update_strategy_cycle(
                int(cycle["id"]), status=STATUS_STOPPED,
                reason="entry_ttl_expired", closed_ms=ts, timestamp_ms=ts,
            )
            return [{"id": cycle["id"], "action": "entry_cancelled_ttl"}]
        status_resp = query_live_order_status(
            client=self.client, store=self.storage, order_id=int(cycle["entry_order_id"])
        )
        if status_resp is None:
            return [{"id": cycle["id"], "action": "entry_waiting"}]
        status = str(status_resp.get("status", ""))
        if status not in {"FILLED", "PARTIALLY_FILLED"}:
            return [{"id": cycle["id"], "action": "entry_waiting", "status": status}]

        avg_entry = float(status_resp.get("avgPrice") or 0.0) or float(cycle["entry_price"])
        qty = float(status_resp.get("executedQty") or 0.0) or float(cycle["quantity"])
        side = str(cycle["side"])
        target = _target_price(side, avg_entry, float(cycle["take_profit_bps"]), market.take_profit_usdc)
        if self.config.grid_basket_take_profit_enabled:
            self.storage.update_strategy_cycle(
                int(cycle["id"]),
                status=STATUS_OPEN,
                reason="entry_filled_basket_tp_pending",
                quantity=qty,
                entry_price=avg_entry,
                target_price=target,
                exit_order_id=None,
                opened_ms=ts,
                last_mid_price=market.mid,
                timestamp_ms=ts,
            )
            return [{
                "id": cycle["id"],
                "action": "entry_filled",
                "side": side,
                "entry": avg_entry,
                "target": target,
                "qty": qty,
                "basket_tp_pending": True,
            }]
        tp_order_id, detail = self._submit_take_profit_limit(
            side=side, quantity=qty, target_price=target, ts=ts
        )
        if tp_order_id is None:
            closed = self._close_cycle_market(cycle, market.mid, ts, "tp_submit_failed", state)
            return [{"id": cycle["id"], "action": "emergency_close", "detail": detail, "closed": closed}]

        self.storage.update_strategy_cycle(
            int(cycle["id"]),
            status=STATUS_OPEN,
            reason="entry_filled_tp_submitted",
            quantity=qty,
            entry_price=avg_entry,
            target_price=target,
            exit_order_id=tp_order_id,
            opened_ms=ts,
            last_mid_price=market.mid,
            timestamp_ms=ts,
        )
        _merge_cycle_setup(self.storage, int(cycle["id"]), {"tp_order_id": tp_order_id}, ts)
        return [{
            "id": cycle["id"],
            "action": "entry_filled",
            "side": side,
            "entry": avg_entry,
            "target": target,
            "qty": qty,
        }]

    def _manage_open_cycle(
        self,
        cycle: Any,
        market: GridMarket,
        ts: int,
        state: GridState,
    ) -> list[dict[str, Any]]:
        if cycle["exit_order_id"] is not None:
            status_resp = query_live_order_status(
                client=self.client, store=self.storage, order_id=int(cycle["exit_order_id"])
            )
            if status_resp is not None and str(status_resp.get("status")) == "FILLED":
                avg_exit = float(status_resp.get("avgPrice") or 0.0) or float(cycle["target_price"])
                qty = float(status_resp.get("executedQty") or 0.0) or float(cycle["quantity"])
                pnl = _cycle_pnl(cycle, avg_exit, qty, self.config.maker_fee_rate)
                self.storage.update_strategy_cycle(
                    int(cycle["id"]),
                    status=STATUS_CLOSED,
                    reason="maker_tp_filled",
                    closed_ms=ts,
                    last_mid_price=avg_exit,
                    realized_pnl=pnl,
                    timestamp_ms=ts,
                )
                _apply_closed_pnl(state, pnl, self.config, ts)
                return [{
                    "id": cycle["id"],
                    "action": "closed_tp",
                    "exit": avg_exit,
                    "pnl": pnl,
                }]
        self.storage.update_strategy_cycle(
            int(cycle["id"]), last_mid_price=market.mid, timestamp_ms=ts,
        )
        return [{"id": cycle["id"], "action": "hold", "mark": market.mid}]

    def _sync_basket_take_profit_orders(
        self,
        market: GridMarket,
        ts: int,
        state: GridState,
    ) -> list[dict[str, Any]]:
        if not self.config.grid_basket_take_profit_enabled:
            return []
        events: list[dict[str, Any]] = []
        for side in ("long", "short"):
            cycles = [
                cycle
                for cycle in self._active_grid_cycles(status=STATUS_OPEN)
                if str(cycle["side"]) == side
            ]
            if not cycles:
                continue
            avg_entry = _weighted_average_entry(cycles)
            if avg_entry <= 0:
                continue
            desired_target = _target_price(side, avg_entry, 0.0, market.take_profit_usdc)
            total_qty = sum(float(cycle["quantity"]) for cycle in cycles)
            needs_reprice = False
            for cycle in cycles:
                current_target = float(cycle["target_price"] or 0.0)
                if cycle["exit_order_id"] is None:
                    needs_reprice = True
                    break
                if abs(current_target - desired_target) >= self.config.grid_basket_reprice_min_usdc:
                    needs_reprice = True
                    break
            if not needs_reprice:
                continue
            for cycle in cycles:
                if cycle["exit_order_id"] is not None:
                    cancel_local_order(
                        client=self.client,
                        store=self.storage,
                        local_order_id=int(cycle["exit_order_id"]),
                    )
                    self.storage.update_strategy_cycle(
                        int(cycle["id"]),
                        exit_order_id=None,
                        timestamp_ms=ts,
                    )
            submitted = 0
            for cycle in cycles:
                tp_order_id, detail = self._submit_take_profit_limit(
                    side=side,
                    quantity=float(cycle["quantity"]),
                    target_price=desired_target,
                    ts=ts,
                )
                if tp_order_id is None:
                    closed = self._close_cycle_market(
                        cycle, market.mid, ts, "basket_tp_submit_failed", state
                    )
                    events.append({
                        "id": cycle["id"],
                        "action": "emergency_close",
                        "detail": detail,
                        "closed": closed,
                    })
                    continue
                self.storage.update_strategy_cycle(
                    int(cycle["id"]),
                    target_price=desired_target,
                    exit_order_id=tp_order_id,
                    reason="basket_tp_synced",
                    timestamp_ms=ts,
                )
                _merge_cycle_setup(
                    self.storage,
                    int(cycle["id"]),
                    {
                        "basket_avg_entry": avg_entry,
                        "basket_total_qty": total_qty,
                        "basket_target_price": desired_target,
                    },
                    ts,
                )
                submitted += 1
            if submitted:
                events.append({
                    "action": "basket_tp_synced",
                    "side": side,
                    "cycles": len(cycles),
                    "submitted": submitted,
                    "avg_entry": avg_entry,
                    "target": desired_target,
                    "total_qty": total_qty,
                })
        return events

    # ----- Entry placement -----

    def _place_missing_entries(
        self,
        side: str,
        market: GridMarket,
        ts: int,
        state: GridState,
    ) -> list[dict[str, Any]]:
        occupied = self._occupied_levels(side)
        max_layers = self.config.grid_max_layers
        if market.risk_label != "NORMAL":
            max_layers = min(max_layers, 1)
        side_orderflow_status = _side_orderflow_status(side, market)
        if self.config.orderflow_guard_enabled and side_orderflow_status == "CAUTION":
            max_layers = min(max_layers, 1)
        if (
            self.config.orderflow_guard_enabled
            and side_orderflow_status == "DANGER"
            and not self._confirmed_orderflow_danger_from_state(side, state)
        ):
            max_layers = min(max_layers, 1)
        events: list[dict[str, Any]] = []
        if len(occupied) >= max_layers:
            return events
        try:
            self.client.set_leverage(symbol=self.config.grid_symbol, leverage=self.config.grid_leverage)
            self.client.set_margin_type(symbol=self.config.grid_symbol, margin_type="ISOLATED")
        except BinanceAPIError as exc:
            return [{"action": "blocked", "reason": f"leverage_or_margin_failed: {exc}"}]

        capital = self._current_capital_usdc()
        notional = min(
            capital * self.config.grid_layer_notional_pct,
            self.config.grid_max_layer_notional,
        )
        if notional <= 0:
            return [{"action": "blocked", "reason": "no_capital"}]

        for level in range(1, max_layers + 1):
            if level in occupied:
                continue
            if state.daily_order_count >= self.config.grid_max_orders_per_day:
                events.append({"action": "blocked", "reason": "daily_order_cap"})
                break
            price = (
                market.bid - market.gap_usdc * level
                if side == "long"
                else market.ask + market.gap_usdc * level
            )
            if price <= 0:
                continue
            quantity = notional / price
            order_id, normalized_price, normalized_qty, detail = self._submit_entry_limit(
                side=side, quantity=quantity, entry_price=price, ts=ts
            )
            if order_id is None or normalized_price <= 0 or normalized_qty <= 0:
                events.append({"action": "entry_blocked", "level": level, "reason": detail})
                continue
            target_price = _target_price(side, normalized_price, 0.0, market.take_profit_usdc)
            stop_price = (
                normalized_price * (1.0 - self.config.grid_stop_loss_pct)
                if side == "long"
                else normalized_price * (1.0 + self.config.grid_stop_loss_pct)
            )
            cycle_id = self.storage.insert_strategy_cycle(
                strategy=STRATEGY_NAME,
                execution_mode=EXECUTION_MODE,
                symbol=self.config.grid_symbol,
                side=side,
                status=STATUS_ENTRY_SUBMITTED,
                quantity=normalized_qty,
                entry_price=normalized_price,
                target_price=target_price,
                stop_price=stop_price,
                entry_order_type="LIMIT",
                take_profit_bps=0.0,
                stop_loss_bps=self.config.grid_stop_loss_pct * 10_000.0,
                max_hold_seconds=0,
                maker_one_way_bps=self.config.maker_fee_rate * 10_000.0,
                taker_one_way_bps=self.config.taker_fee_rate * 10_000.0,
                entry_deadline_ms=ts + self.config.grid_entry_order_ttl_seconds * 1000,
                entry_order_id=order_id,
                dry_run=False,
                reason=f"{state.mode} grid level={level}",
                last_mid_price=market.mid,
                setup={
                    "hypothesis": "maker_grid_repeated_small_profit",
                    "grid_level": level,
                    "mode": state.mode,
                    "leverage": self.config.grid_leverage,
                    "notional": normalized_price * normalized_qty,
                    "layer_notional_pct": self.config.grid_layer_notional_pct,
                    "gap_usdc": market.gap_usdc,
                    "take_profit_usdc": market.take_profit_usdc,
                    "atr_5m": market.atr_5m,
                    "capital_at_order": capital,
                    "live": True,
                },
                timestamp_ms=ts,
            )
            state.daily_order_count += 1
            events.append({
                "id": cycle_id,
                "action": "entry_submitted",
                "side": side,
                "level": level,
                "price": normalized_price,
                "target": target_price,
                "notional": normalized_price * normalized_qty,
            })
        return events

    def _submit_entry_limit(
        self,
        *,
        side: str,
        quantity: float,
        entry_price: float,
        ts: int,
    ) -> tuple[int | None, float, float, str]:
        order_side = "BUY" if side == "long" else "SELL"
        intent = OrderIntent(
            symbol=self.config.grid_symbol,
            side=order_side,
            quantity=quantity,
            order_type="LIMIT",
            price=entry_price,
            time_in_force="GTX",
            reduce_only=False,
            client_order_id=client_order_id("mg", self.config.grid_symbol),
        )
        normalized, reason = normalize_market_intent(self.client, intent, self.config)
        if normalized is None:
            order_id = self.storage.insert_order_attempt(
                intent, status="BLOCKED", dry_run=False, reason=reason, timestamp_ms=ts
            )
            return order_id, 0.0, 0.0, reason
        try:
            response = self.client.new_order(normalized)
        except BinanceAPIError as exc:
            detail = f"entry limit failed: {exc}"
            order_id = self.storage.insert_order_attempt(
                normalized, status="ERROR", dry_run=False, reason=detail, timestamp_ms=ts
            )
            return order_id, 0.0, 0.0, detail
        order_id = self.storage.insert_order_attempt(
            normalized,
            status=str(response.get("status", "SUBMITTED")),
            dry_run=False,
            reason="grid_entry",
            response=response,
            timestamp_ms=ts,
        )
        return order_id, float(normalized.price or 0.0), float(normalized.quantity), "submitted"

    def _submit_take_profit_limit(
        self,
        *,
        side: str,
        quantity: float,
        target_price: float,
        ts: int,
    ) -> tuple[int | None, str]:
        order_side = "SELL" if side == "long" else "BUY"
        intent = OrderIntent(
            symbol=self.config.grid_symbol,
            side=order_side,
            quantity=quantity,
            order_type="LIMIT",
            price=target_price,
            time_in_force="GTX",
            reduce_only=True,
            client_order_id=client_order_id("mgtp", self.config.grid_symbol),
        )
        normalized, reason = normalize_market_intent(self.client, intent, self.config)
        if normalized is None:
            self.storage.insert_order_attempt(
                intent, status="BLOCKED", dry_run=False, reason=reason, timestamp_ms=ts
            )
            return None, reason
        try:
            response = self.client.new_order(normalized)
        except BinanceAPIError as exc:
            detail = f"tp limit failed: {exc}"
            self.storage.insert_order_attempt(
                normalized, status="ERROR", dry_run=False, reason=detail, timestamp_ms=ts
            )
            return None, detail
        order_id = self.storage.insert_order_attempt(
            normalized,
            status=str(response.get("status", "SUBMITTED")),
            dry_run=False,
            reason="grid_take_profit",
            response=response,
            timestamp_ms=ts,
        )
        return order_id, "submitted"

    # ----- Cancels / emergency closes -----

    def _cancel_pending_entries(self, ts: int, reason: str) -> int:
        cancelled = 0
        for cycle in self._active_grid_cycles(status=STATUS_ENTRY_SUBMITTED):
            if cycle["entry_order_id"] is not None:
                cancel_local_order(
                    client=self.client,
                    store=self.storage,
                    local_order_id=int(cycle["entry_order_id"]),
                )
            self.storage.update_strategy_cycle(
                int(cycle["id"]),
                status=STATUS_STOPPED,
                reason=reason,
                closed_ms=ts,
                timestamp_ms=ts,
            )
            cancelled += 1
        return cancelled

    def _close_all_open_cycles(
        self, market_price: float, ts: int, reason: str, state: GridState
    ) -> int:
        closed = 0
        for cycle in self._active_grid_cycles(status=STATUS_OPEN):
            self._close_cycle_market(cycle, market_price, ts, reason, state)
            closed += 1
        return closed

    def _close_oldest_half(
        self, market_price: float, ts: int, reason: str, state: GridState
    ) -> int:
        open_cycles = self._active_grid_cycles(status=STATUS_OPEN)
        if not open_cycles:
            return 0
        total_notional = sum(float(c["entry_price"]) * float(c["quantity"]) for c in open_cycles)
        target = total_notional * 0.5
        closed = 0
        closed_notional = 0.0
        for cycle in sorted(open_cycles, key=lambda c: int(c["created_ms"])):
            self._close_cycle_market(cycle, market_price, ts, reason, state)
            closed += 1
            closed_notional += float(cycle["entry_price"]) * float(cycle["quantity"])
            if closed_notional >= target:
                break
        return closed

    def _close_cycle_market(
        self,
        cycle: Any,
        market_price: float,
        ts: int,
        reason: str,
        state: GridState | None = None,
    ) -> dict[str, Any]:
        if cycle["exit_order_id"] is not None:
            cancel_local_order(
                client=self.client, store=self.storage, local_order_id=int(cycle["exit_order_id"])
            )
        side = str(cycle["side"])
        order_side = "SELL" if side == "long" else "BUY"
        intent = OrderIntent(
            symbol=self.config.grid_symbol,
            side=order_side,
            quantity=float(cycle["quantity"]),
            order_type="MARKET",
            response_type="RESULT",
            reduce_only=True,
            client_order_id=client_order_id("mgclose", self.config.grid_symbol),
        )
        normalized, norm_reason = normalize_market_intent(self.client, intent, self.config)
        if normalized is None:
            self.storage.insert_order_attempt(
                intent, status="BLOCKED", dry_run=False, reason=norm_reason, timestamp_ms=ts
            )
            return {"ok": False, "reason": norm_reason}
        try:
            response = self.client.new_order(normalized)
        except BinanceAPIError as exc:
            detail = f"grid market close failed: {exc}"
            self.storage.insert_order_attempt(
                normalized, status="ERROR", dry_run=False, reason=detail, timestamp_ms=ts
            )
            return {"ok": False, "reason": detail}
        order_id = self.storage.insert_order_attempt(
            normalized,
            status=str(response.get("status", "SUBMITTED")),
            dry_run=False,
            reason=reason,
            response=response,
            timestamp_ms=ts,
        )
        avg_exit = float(response.get("avgPrice") or 0.0) or market_price
        qty = float(response.get("executedQty") or 0.0) or float(cycle["quantity"])
        pnl = _cycle_pnl(cycle, avg_exit, qty, self.config.taker_fee_rate)
        self.storage.update_strategy_cycle(
            int(cycle["id"]),
            status=STATUS_STOPPED,
            reason=reason,
            exit_order_id=order_id,
            closed_ms=ts,
            last_mid_price=avg_exit,
            realized_pnl=pnl,
            timestamp_ms=ts,
        )
        if state is not None:
            _apply_closed_pnl(state, pnl, self.config, ts)
        return {"ok": True, "exit": avg_exit, "qty": qty, "pnl": pnl}

    # ----- Queries / helpers -----

    def _active_grid_cycles(self, *, status: str | None = None) -> list[Any]:
        params: list[Any] = [STRATEGY_NAME]
        status_sql = "AND status IN (?, ?)"
        params.extend([STATUS_ENTRY_SUBMITTED, STATUS_OPEN])
        if status is not None:
            status_sql = "AND status=?"
            params = [STRATEGY_NAME, status]
        with self.storage.connect() as connection:
            return list(connection.execute(
                f"""
                SELECT *
                FROM strategy_cycles
                WHERE strategy=?
                  {status_sql}
                ORDER BY created_ms ASC
                """,
                params,
            ))

    def _occupied_levels(self, side: str) -> set[int]:
        levels: set[int] = set()
        for cycle in self._active_grid_cycles():
            if str(cycle["side"]) != side:
                continue
            setup = _cycle_setup(cycle)
            level = setup.get("grid_level")
            if isinstance(level, int):
                levels.add(level)
            elif isinstance(level, float):
                levels.add(int(level))
        return levels

    def _active_grid_side(self) -> str | None:
        sides = {str(cycle["side"]) for cycle in self._active_grid_cycles()}
        if len(sides) == 1:
            return next(iter(sides))
        if len(sides) > 1:
            return "mixed"
        return None

    def _current_capital_usdc(self) -> float:
        try:
            balances = self.client.account_balance()
        except BinanceAPIError as exc:
            logger.warning("maker_grid: account_balance failed, using initial_equity: %s", exc)
            return self.config.initial_equity
        for row in balances:
            if row.get("asset") == "USDC":
                available = float(row.get("availableBalance") or 0.0)
                balance = float(row.get("balance") or 0.0)
                return available or balance or self.config.initial_equity
        return self.config.initial_equity

    def _grid_unrealized_pnl(self, market_price: float) -> float:
        pnl = 0.0
        for cycle in self._active_grid_cycles(status=STATUS_OPEN):
            entry = float(cycle["entry_price"])
            qty = float(cycle["quantity"])
            if str(cycle["side"]) == "long":
                pnl += (market_price - entry) * qty
            else:
                pnl += (entry - market_price) * qty
        return pnl


def _closed_klines(klines: list[Kline]) -> list[Kline]:
    if len(klines) <= 1:
        return klines
    return klines[:-1]


def _true_ranges(klines: list[Kline]) -> list[float]:
    if len(klines) < 2:
        return [abs(k.high - k.low) for k in klines]
    out: list[float] = []
    prev_close = klines[0].close
    for k in klines[1:]:
        out.append(max(k.high - k.low, abs(k.high - prev_close), abs(k.low - prev_close)))
        prev_close = k.close
    return out


def _return_from_klines(klines: list[Kline], *, bars: int) -> float:
    closed = _closed_klines(klines)
    if len(closed) <= bars:
        return 0.0
    start = closed[-bars - 1].close
    end = closed[-1].close
    if start <= 0:
        return 0.0
    return (end - start) / start


def _range_position_from_klines(
    klines: list[Kline],
    *,
    mid: float,
    lookback: int,
) -> tuple[float, float, float]:
    closed = _closed_klines(klines)
    rows = closed[-lookback:] if lookback > 0 else closed
    if not rows:
        return mid, mid, 0.5
    low = min(k.low for k in rows)
    high = max(k.high for k in rows)
    if high <= low:
        return low, high, 0.5
    return low, high, _clamp((mid - low) / (high - low), 0.0, 1.0)


def _soft_filter_reasons(
    side: str,
    market: GridMarket,
    config: TradingConfig,
) -> list[str]:
    reasons: list[str] = []
    if config.grid_position_filter_enabled:
        if side == "long" and market.range_position_15m >= config.grid_long_max_range_position:
            reasons.append(
                f"롱 위치 차단: 최근 15m range 상단 {market.range_position_15m * 100:.0f}%"
            )
        if side == "short" and market.range_position_15m <= config.grid_short_min_range_position:
            reasons.append(
                f"숏 위치 차단: 최근 15m range 하단 {market.range_position_15m * 100:.0f}%"
            )
    if config.grid_liquidity_filter_enabled:
        total_depth = market.orderflow_bid_depth_010 + market.orderflow_ask_depth_010
        side_depth = (
            market.orderflow_bid_depth_010 if side == "long" else market.orderflow_ask_depth_010
        )
        if side_depth > 0 and side_depth < config.grid_min_side_depth_010_usdc:
            reasons.append(
                f"유동성 부족: 내 쪽 0.1% depth {side_depth:,.0f} < "
                f"{config.grid_min_side_depth_010_usdc:,.0f} USDC"
            )
        if total_depth > 0 and total_depth < config.grid_min_total_depth_010_usdc:
            reasons.append(
                f"유동성 부족: 양쪽 0.1% depth 합 {total_depth:,.0f} < "
                f"{config.grid_min_total_depth_010_usdc:,.0f} USDC"
            )
    return reasons


def _side_orderflow_status(side: str, market: GridMarket) -> str:
    return market.orderflow_long_status if side == "long" else market.orderflow_short_status


def _orderflow_confirmation_text(state: GridState, config: TradingConfig) -> str:
    return (
        f"롱 DANGER {state.orderflow_long_danger_count}/{config.grid_orderflow_confirmations}"
        f"·회복 {state.orderflow_long_recovery_count}/{config.grid_orderflow_recovery_confirmations}, "
        f"숏 DANGER {state.orderflow_short_danger_count}/{config.grid_orderflow_confirmations}"
        f"·회복 {state.orderflow_short_recovery_count}/{config.grid_orderflow_recovery_confirmations}"
    )


def _auto_side(klines_15m: list[Kline], klines_1h: list[Kline]) -> str | None:
    ret_15m_4 = _return_from_klines(klines_15m, bars=4)
    ret_1h_3 = _return_from_klines(klines_1h, bars=3)
    if ret_15m_4 >= 0.003 and ret_1h_3 >= 0.003:
        return "long"
    if ret_15m_4 <= -0.003 and ret_1h_3 <= -0.003:
        return "short"
    return None


def _target_price(side: str, entry_price: float, take_profit_bps: float, take_profit_usdc: float) -> float:
    if take_profit_bps > 0:
        delta = entry_price * take_profit_bps / 10_000.0
    else:
        delta = take_profit_usdc
    return entry_price + delta if side == "long" else entry_price - delta


def _weighted_average_entry(cycles: list[Any]) -> float:
    total_qty = sum(float(cycle["quantity"]) for cycle in cycles)
    if total_qty <= 0:
        return 0.0
    return sum(float(cycle["entry_price"]) * float(cycle["quantity"]) for cycle in cycles) / total_qty


def _cycle_pnl(cycle: Any, exit_price: float, quantity: float, exit_fee_rate: float) -> float:
    entry = float(cycle["entry_price"])
    qty = quantity if quantity > 0 else float(cycle["quantity"])
    if str(cycle["side"]) == "long":
        gross = (exit_price - entry) * qty
    else:
        gross = (entry - exit_price) * qty
    entry_fee = entry * qty * float(cycle["maker_one_way_bps"]) / 10_000.0
    exit_fee = exit_price * qty * exit_fee_rate
    return gross - entry_fee - exit_fee


def _apply_closed_pnl(
    state: GridState,
    pnl: float,
    config: TradingConfig,
    ts_ms: int,
) -> None:
    state.daily_realized_pnl += pnl
    if pnl > 0:
        state.consecutive_losses = 0
        state.loss_cooldown_until_ms = 0
    else:
        state.consecutive_losses += 1
        if state.consecutive_losses >= config.grid_max_consecutive_losses:
            state.loss_cooldown_until_ms = ts_ms + config.grid_loss_cooldown_seconds * 1000
            state.paused_reason = (
                f"{state.consecutive_losses} consecutive losses; cooldown until "
                f"{kst_from_ms(state.loss_cooldown_until_ms)}"
            )


def _cycle_setup(cycle: Any) -> dict[str, Any]:
    raw = cycle["setup_json"]
    if not raw:
        return {}
    try:
        data = json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        return {}
    return data if isinstance(data, dict) else {}


def _merge_cycle_setup(store: TradingStore, cycle_id: int, data: dict[str, Any], ts: int) -> None:
    with store.connect() as connection:
        row = connection.execute(
            "SELECT setup_json FROM strategy_cycles WHERE id=?", (cycle_id,)
        ).fetchone()
        setup: dict[str, Any] = {}
        if row is not None and row["setup_json"]:
            try:
                loaded = json.loads(row["setup_json"])
                if isinstance(loaded, dict):
                    setup = loaded
            except (json.JSONDecodeError, TypeError):
                setup = {}
        setup.update(data)
        connection.execute(
            "UPDATE strategy_cycles SET setup_json=?, updated_ms=?, updated_iso=? WHERE id=?",
            (json.dumps(setup, sort_keys=True), ts, datetime.fromtimestamp(ts / 1000, timezone.utc).isoformat(), cycle_id),
        )


def _basket_summary(cycles: list[Any]) -> dict[str, Any] | None:
    if not cycles:
        return None
    side = str(cycles[0]["side"])
    same_side = [cycle for cycle in cycles if str(cycle["side"]) == side]
    if not same_side:
        return None
    total_qty = sum(float(cycle["quantity"]) for cycle in same_side)
    if total_qty <= 0:
        return None
    return {
        "side": side,
        "total_qty": total_qty,
        "avg_entry": _weighted_average_entry(same_side),
    }


def _clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def grid_status_text(
    *,
    config: TradingConfig | None = None,
    store: TradingStore | None = None,
    client: BinanceUSDMClient | None = None,
    state_path: Path | None = None,
) -> str:
    cfg = config or TradingConfig.from_env()
    st = store or TradingStore()
    cl = client or BinanceUSDMClient(config=cfg)
    state = load_state(state_path)
    reset_daily_if_needed(state, store_now_ms())
    engine = MakerGridEngine(config=cfg, storage=st, client=cl, state_path=state_path)
    try:
        market = engine._load_market()
    except Exception as exc:  # noqa: BLE001
        market = None
        market_error = str(exc)
    else:
        market_error = ""
    cycles = engine._active_grid_cycles()
    entries = [c for c in cycles if c["status"] == STATUS_ENTRY_SUBMITTED]
    opens = [c for c in cycles if c["status"] == STATUS_OPEN]
    unrealized = engine._grid_unrealized_pnl(market.mid) if market is not None else 0.0
    actual_open_orders: list[dict[str, Any]] = []
    actual_positions: list[dict[str, Any]] = []
    try:
        actual_open_orders = cl.open_orders(cfg.grid_symbol)
    except (AttributeError, BinanceAPIError):
        actual_open_orders = []
    try:
        account = cl.account_info()
        for row in account.get("positions", []):
            if row.get("symbol") != cfg.grid_symbol:
                continue
            qty = float(row.get("positionAmt") or 0.0)
            if abs(qty) > 0:
                actual_positions.append(row)
    except (AttributeError, BinanceAPIError, ValueError):
        actual_positions = []
    lines = [
        "■ 띠기 maker grid",
        f"  모드       : {state.mode}",
        f"  강제진입   : {'ON' if state.force_entry else 'OFF'}",
        f"  라이브 게이트: {'준비됨' if is_live_armed(cfg) else '잠김'} ({live_gate_text(cfg)})",
        f"  심볼/레버리지: {cfg.grid_symbol} {cfg.grid_leverage}x ISOLATED",
        f"  진입 기준  : 계좌 USDC × {cfg.grid_layer_notional_pct * 100:.1f}% 명목가치"
        f" (상한 {cfg.grid_max_layer_notional:.0f} USDC)",
        f"  최대 겹수  : {cfg.grid_max_layers}",
        f"  TP 방식    : {'평단 묶음 TP' if cfg.grid_basket_take_profit_enabled else '레이어별 TP'}",
        "",
        f"  오늘 손익  : {state.daily_realized_pnl:+.4f} USDC (표시용, 단독 차단 없음)",
        f"  오늘 주문  : {state.daily_order_count}/{cfg.grid_max_orders_per_day}",
        f"  연속 손실  : {state.consecutive_losses}/{cfg.grid_max_consecutive_losses}",
        f"  활성 주문  : 진입대기 {len(entries)} / 포지션 {len(opens)}",
        f"  거래소 실제: 미체결 {len(actual_open_orders)} / 포지션 {len(actual_positions)}",
        f"  미실현손익 : {unrealized:+.4f} USDC",
    ]
    if actual_open_orders and not entries:
        lines.append("  주의       : DB에 없는 거래소 미체결 주문이 있습니다. 수동/외부 주문 가능성.")
    for order in actual_open_orders[:3]:
        lines.append(
            "  실주문     : "
            f"{order.get('side')} {order.get('type')} {order.get('origQty')} @ "
            f"{order.get('price')} reduceOnly={order.get('reduceOnly')} "
            f"id={order.get('clientOrderId')}"
        )
    for pos in actual_positions[:3]:
        lines.append(
            "  실포지션   : "
            f"amt={pos.get('positionAmt')} entry={pos.get('entryPrice')} "
            f"uPnL={pos.get('unrealizedProfit')}"
        )
    if state.paused_reason:
        lines.append(f"  정지 사유  : {state.paused_reason}")
    if state.loss_cooldown_until_ms > store_now_ms():
        lines.append(f"  손실 쿨다운: {kst_from_ms(state.loss_cooldown_until_ms)}까지 신규 진입 대기")
    basket = _basket_summary(opens)
    if basket is not None:
        lines.append(
            f"  평단/수량   : {basket['side']} avg {basket['avg_entry']:.2f} / "
            f"qty {basket['total_qty']:.6f}"
        )
    if market is not None:
        lines.extend([
            "",
            f"  현재가     : bid {market.bid:.2f} / ask {market.ask:.2f}",
            f"  간격/익절  : gap {market.gap_usdc:.2f} / TP {market.take_profit_usdc:.2f} USDC",
            f"  ATR        : 5m {market.atr_5m:.2f} / 24h중앙 {market.atr_5m_median_24h:.2f}",
            f"  변동       : 15m {market.ret_15m * 100:+.2f}% / 1h {market.ret_1h * 100:+.2f}%",
            f"  AUTO 판단  : {market.effective_side or '대기'}",
            (
                f"  위치       : 최근 15m range {market.range_position_15m * 100:.0f}% "
                f"({market.range_low_15m:.2f}~{market.range_high_15m:.2f})"
            ),
            (
                "  호가창     : "
                f"전체 {market.orderflow_status}, 롱 {market.orderflow_long_status}, "
                f"숏 {market.orderflow_short_status}, age "
                f"{market.orderflow_age_seconds:.1f}s"
                if market.orderflow_age_seconds is not None
                else (
                    "  호가창     : "
                    f"전체 {market.orderflow_status}, 롱 {market.orderflow_long_status}, "
                    f"숏 {market.orderflow_short_status}"
                )
            ),
        ])
        if market.risk_reason:
            lines.append(f"  위험       : {market.risk_label} - {market.risk_reason}")
        if market.orderflow_reason and market.orderflow_status != "NORMAL":
            lines.append(f"  호가 사유  : {market.orderflow_reason}")
        if cfg.orderflow_guard_enabled:
            lines.append(f"  호가 확정  : {_orderflow_confirmation_text(state, cfg)}")
        for side_name, label in (("long", "롱"), ("short", "숏")):
            soft_reason = _soft_filter_reasons(side_name, market, cfg)
            if soft_reason:
                lines.append(f"  {label} 필터  : {'; '.join(soft_reason)}")
    elif market_error:
        lines.append(f"  시장 조회 오류: {market_error}")
    lines.extend([
        "",
        "명령: 띠기 롱 시작 / 띠기 숏 시작 / 띠기 자동 시작 / 띠기 정지 / 띠기 상태",
    ])
    return "\n".join(lines)


def grid_recommendation_text(
    *,
    config: TradingConfig | None = None,
    store: TradingStore | None = None,
    client: BinanceUSDMClient | None = None,
    state_path: Path | None = None,
) -> str:
    cfg = config or TradingConfig.from_env()
    st = store or TradingStore()
    cl = client or BinanceUSDMClient(config=cfg)
    engine = MakerGridEngine(config=cfg, storage=st, client=cl, state_path=state_path)
    state = load_state(state_path)
    try:
        market = engine._load_market()
    except Exception as exc:  # noqa: BLE001
        return "\n".join(["■ 띠기 추천", f"시장 데이터 조회 실패: {exc}"])

    capital = engine._current_capital_usdc()
    layer_notional = min(capital * cfg.grid_layer_notional_pct, cfg.grid_max_layer_notional)
    layer_margin = layer_notional / max(1, cfg.grid_leverage)
    side = market.effective_side
    reason = "15m/1h 다중봉 추세 필터 통과" if side else "15m/1h 다중봉 추세가 애매해서 대기"
    if market.risk_label == "HALT":
        side = None
        reason = f"과열/급변동 차단: {market.risk_reason}"
    if side and cfg.orderflow_guard_enabled:
        side_orderflow_status = _side_orderflow_status(side, market)
        if side_orderflow_status in {"STALE", "UNKNOWN"}:
            side = None
            reason = f"호가창 차단: {market.orderflow_reason}"
        elif side_orderflow_status == "DANGER":
            count = (
                state.orderflow_long_danger_count
                if side == "long"
                else state.orderflow_short_danger_count
            )
            threshold = cfg.grid_orderflow_confirmations
            if count >= threshold:
                side = None
                reason = f"호가창 DANGER 확정 {count}/{threshold}: {market.orderflow_reason}"
            else:
                reason += f" / 호가창 DANGER 관찰중 {count}/{threshold}: 신규 겹수 1개로 축소"
        elif side_orderflow_status == "CAUTION":
            reason += f" / 호가창 주의: {market.orderflow_reason}"
    if side:
        soft_reasons = _soft_filter_reasons(side, market, cfg)
        if soft_reasons:
            side = None
            reason = " / ".join(soft_reasons)

    lines = [
        "■ 띠기 추천",
        f"현재가: bid {market.bid:.2f} / ask {market.ask:.2f}",
        f"추천: {side or '대기'} ({reason})",
        f"간격: {market.gap_usdc:.2f} USDC, 익절폭: {market.take_profit_usdc:.2f} USDC",
        f"진입당: 명목 {layer_notional:.2f} USDC / 예상 증거금 {layer_margin:.2f} USDC @ {cfg.grid_leverage}x",
        f"최대 겹수: {cfg.grid_max_layers}개, 총 명목 최대 약 {layer_notional * cfg.grid_max_layers:.2f} USDC",
        f"변동: 15m {market.ret_15m * 100:+.2f}% / 1h {market.ret_1h * 100:+.2f}%",
        f"위치: 최근 15m range {market.range_position_15m * 100:.0f}% ({market.range_low_15m:.2f}~{market.range_high_15m:.2f})",
        f"ATR: 5m {market.atr_5m:.2f}, 24h 중앙 {market.atr_5m_median_24h:.2f}",
        (
            "호가창: "
            f"전체 {market.orderflow_status}, 롱 {market.orderflow_long_status}, "
            f"숏 {market.orderflow_short_status}"
        ),
        f"호가 확정: {_orderflow_confirmation_text(state, cfg)}",
    ]
    if side:
        lines.extend(["", "예상 주문 레벨:"])
        for level in range(1, cfg.grid_max_layers + 1):
            entry = (
                market.bid - market.gap_usdc * level
                if side == "long"
                else market.ask + market.gap_usdc * level
            )
            target = entry + market.take_profit_usdc if side == "long" else entry - market.take_profit_usdc
            lines.append(
                f"  L{level}: {'BUY' if side == 'long' else 'SELL'} {entry:.2f} -> TP {target:.2f}"
            )
        lines.extend([
            "",
            f"시작 명령: 띠기 {'롱' if side == 'long' else '숏'} 시작",
        ])
    else:
        lines.extend([
            "",
            "시작하지 않는 쪽을 추천.",
            "정말 의도적으로 무시하려면 '띠기 롱 강제 시작' 또는 '띠기 숏 강제 시작'.",
        ])
    lines.append(f"라이브 게이트: {'준비됨' if is_live_armed(cfg) else '잠김'} ({live_gate_text(cfg)})")
    return "\n".join(lines)


def set_grid_mode_text(
    mode: str,
    *,
    config: TradingConfig | None = None,
    state_path: Path | None = None,
    force_entry: bool = False,
) -> str:
    cfg = config or TradingConfig.from_env()
    normalized = mode.upper()
    if normalized not in {MODE_STOPPED, MODE_LONG, MODE_SHORT, MODE_AUTO}:
        return "띠기 모드가 이상합니다. 롱/숏/자동/정지 중 하나여야 합니다."
    if normalized != MODE_STOPPED and not is_live_armed(cfg):
        return "\n".join([
            "띠기 시작 실패",
            "라이브 게이트가 아직 잠겨 있습니다.",
            live_gate_text(cfg),
        ])
    state = load_state(state_path)
    reset_daily_if_needed(state, store_now_ms())
    state.mode = normalized
    state.force_entry = bool(force_entry and normalized in {MODE_LONG, MODE_SHORT})
    state.paused_reason = "manual stop" if normalized == MODE_STOPPED else ""
    if normalized == MODE_STOPPED:
        state.force_entry = False
    save_state(state, state_path)
    if normalized == MODE_STOPPED:
        return "띠기 정지 설정 완료. 다음 grid-step에서 미체결 진입 주문을 취소하고, 열린 포지션은 TP/손실한도 기준으로 관리합니다."
    side_text = {"LONG": "롱", "SHORT": "숏", "AUTO": "자동"}[normalized]
    return "\n".join([
        f"띠기 {side_text}{' 강제' if state.force_entry else ''} 모드 ON",
        f"심볼: {cfg.grid_symbol}",
        f"레버리지: {cfg.grid_leverage}x ISOLATED",
        f"진입당: 계좌 USDC × {cfg.grid_layer_notional_pct * 100:.1f}% 명목가치"
        f" (상한 {cfg.grid_max_layer_notional:.0f} USDC)",
        f"최대 겹수: {cfg.grid_max_layers}",
        f"위치/유동성 필터: {'우회' if state.force_entry else '적용'}",
        f"손실한도: 경고 {cfg.grid_warning_loss_pct:.2%}, 축소 {cfg.grid_reduce_loss_pct:.2%}, 정지 {cfg.grid_stop_loss_pct:.2%}",
        f"연속손실: {cfg.grid_max_consecutive_losses}회면 {cfg.grid_loss_cooldown_seconds // 60}분 쿨다운 후 재평가",
        "주문 방식: 진입/익절 모두 post-only maker. 익절은 열린 레이어 평단 기준 묶음 TP.",
        "강제 모드여도 손실한도/orderflow DANGER 확정/급변동 차단은 유지됩니다.",
        "손실한도 초과 시에만 reduce-only market.",
    ])


def run_step_once(
    *,
    config: TradingConfig | None = None,
    state_path: Path | None = None,
) -> StepResult:
    cfg = config or TradingConfig.from_env()
    store = TradingStore()
    client = BinanceUSDMClient(config=cfg)
    engine = MakerGridEngine(config=cfg, storage=store, client=client, state_path=state_path)
    return engine.step()


def step_result_notification_text(result: StepResult) -> str:
    notable = [*result.risk, *[e for e in result.managed if e.get("action") != "hold"], *result.opened]
    if not notable:
        return ""
    lines = ["■ 띠기 진행 알림", f"시각: {kst_from_ms(result.ts_ms)}"]
    for item in notable[:10]:
        action = item.get("action", "")
        if action == "entry_submitted":
            lines.append(
                f"- 진입 주문: {item.get('side')} L{item.get('level')} "
                f"price={float(item.get('price', 0)):.2f} "
                f"target={float(item.get('target', 0)):.2f} "
                f"notional={float(item.get('notional', 0)):.2f}"
            )
        elif action == "entry_filled":
            lines.append(
                f"- 진입 체결: {item.get('side')} entry={float(item.get('entry', 0)):.2f} "
                f"target={float(item.get('target', 0)):.2f}"
            )
        elif action == "closed_tp":
            lines.append(f"- 익절 체결: pnl={float(item.get('pnl', 0)):+.4f} USDC")
        elif action == "basket_tp_synced":
            lines.append(
                f"- 묶음 TP 재배치: {item.get('side')} {item.get('cycles')}개 "
                f"평단={float(item.get('avg_entry', 0)):.2f} "
                f"TP={float(item.get('target', 0)):.2f}"
            )
        elif action == "orderflow_danger_cancel":
            lines.append(
                f"- 호가창 위험 확정: 미체결 진입 주문 {int(item.get('cancelled', 0))}개 취소"
            )
        elif action.startswith("grid_"):
            lines.append(
                f"- 위험조치: {action} unreal={float(item.get('unrealized', 0)):+.4f} "
                f"loss={float(item.get('loss_pct', 0)):.2%}"
            )
        else:
            lines.append(f"- {action}: {item}")
    return "\n".join(lines)


def run_step_and_notify(
    *,
    state_path: Path | None = None,
    config: TradingConfig | None = None,
    telegram_config: TelegramConfig | None = None,
) -> dict[str, Any]:
    cfg = config or TradingConfig.from_env()
    result = run_step_once(config=cfg, state_path=state_path)
    text = step_result_notification_text(result)
    sent = 0
    if text:
        try:
            TelegramClient(telegram_config or TelegramConfig.from_env()).send_message(text)
            sent = 1
        except (TelegramConfigError, Exception) as exc:  # noqa: BLE001
            logger.warning("grid notify failed: %s", exc)
    payload = result.as_dict()
    payload["telegram_sent"] = sent
    return payload
