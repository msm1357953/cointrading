"""Paper-only micro grid ("스캘핑 띠기") lifecycle.

This module deliberately never submits exchange orders. It shadows tiny
maker-grid variants so the dashboard can accumulate fill/TP/stop evidence
before the owner considers any real-money version.
"""
from __future__ import annotations

from dataclasses import dataclass, field, replace
import json
from typing import Any

from cointrading.config import TradingConfig
from cointrading.exchange.binance_usdm import BinanceUSDMClient
from cointrading.grid_lifecycle import (
    MODE_AUTO,
    MODE_LONG,
    MODE_SHORT,
    MODE_STOPPED,
    MakerGridEngine,
    default_state_path,
    load_state,
)
from cointrading.storage import TradingStore, default_db_path, iso_from_ms, now_ms


STRATEGY_NAME = "micro_grid_paper"
EXECUTION_MODE = "paper_micro_grid"
STATUS_ENTRY_SUBMITTED = "ENTRY_SUBMITTED"
STATUS_OPEN = "OPEN"
STATUS_CLOSED = "CLOSED"
STATUS_STOPPED = "STOPPED"


@dataclass(frozen=True)
class MicroGridVariant:
    name: str
    gap_usdc: float
    take_profit_usdc: float


@dataclass
class MicroGridPaperStepResult:
    ts_ms: int
    opened: list[dict[str, Any]] = field(default_factory=list)
    managed: list[dict[str, Any]] = field(default_factory=list)
    skipped: list[dict[str, Any]] = field(default_factory=list)

    def as_dict(self) -> dict[str, Any]:
        return {
            "ts_ms": self.ts_ms,
            "opened": self.opened,
            "managed": self.managed,
            "skipped": self.skipped,
        }


class MicroGridPaperEngine:
    def __init__(
        self,
        *,
        config: TradingConfig,
        storage: TradingStore,
        client: BinanceUSDMClient,
        state_path=None,
    ) -> None:
        self.config = config
        self.storage = storage
        self.client = client
        self.state_path = state_path or default_state_path()

    def step(self) -> MicroGridPaperStepResult:
        ts = now_ms()
        result = MicroGridPaperStepResult(ts_ms=ts)
        if not self.config.micro_grid_paper_enabled:
            result.skipped.append({"reason": "disabled"})
            return result

        market = self._load_market()
        sides = self._candidate_sides(market)
        result.managed.extend(self._stop_disallowed_cycles(sides, market, ts))
        result.managed.extend(self._manage_existing_cycles(market, ts))
        if market.risk_label == "HALT":
            result.skipped.append({"reason": "risk_halt", "detail": market.risk_reason})
            return result
        if not sides:
            result.skipped.append({"reason": "auto_wait", "detail": "paper has no clear one-way side"})
            return result

        active = self._active_cycles()
        active_keys = {_cycle_key(cycle) for cycle in active}
        remaining_slots = max(0, self.config.micro_grid_paper_max_active_cycles - len(active))
        if remaining_slots <= 0:
            result.skipped.append({"reason": "active_cap", "active": len(active)})
            return result

        for side in sides:
            side_block = self._side_block_reason(side, market)
            if side_block:
                result.skipped.append({"reason": "side_block", "side": side, "detail": side_block})
                continue
            for variant in micro_grid_variants(self.config):
                if remaining_slots <= 0:
                    return result
                key = (variant.name, side)
                if key in active_keys:
                    continue
                cycle_id = self._submit_paper_entry(side, variant, market, ts)
                result.opened.append({
                    "cycle_id": cycle_id,
                    "variant": variant.name,
                    "side": side,
                    "entry_price": _entry_price(side, market.mid, variant.gap_usdc),
                    "gap_usdc": variant.gap_usdc,
                    "take_profit_usdc": variant.take_profit_usdc,
                })
                active_keys.add(key)
                remaining_slots -= 1
        return result

    def _candidate_sides(self, market) -> list[str]:
        state = load_state(self.state_path)
        if state.mode == MODE_LONG:
            return ["long"]
        if state.mode == MODE_SHORT:
            return ["short"]
        if state.mode in {MODE_AUTO, MODE_STOPPED}:
            open_sides = {
                str(cycle["side"])
                for cycle in self._active_cycles()
                if str(cycle["status"]) == STATUS_OPEN
            }
            if len(open_sides) == 1:
                return [next(iter(open_sides))]
            if market.effective_side in {"long", "short"}:
                return [market.effective_side]
        return []

    def _stop_disallowed_cycles(self, allowed_sides: list[str], market, ts: int) -> list[dict[str, Any]]:
        allowed = set(allowed_sides)
        managed: list[dict[str, Any]] = []
        for cycle in self._active_cycles():
            side = str(cycle["side"])
            if side in allowed:
                continue
            status = str(cycle["status"])
            if status == STATUS_ENTRY_SUBMITTED:
                self.storage.update_strategy_cycle(
                    int(cycle["id"]),
                    status=STATUS_STOPPED,
                    reason="paper_direction_changed",
                    closed_ms=ts,
                    last_mid_price=market.mid,
                    realized_pnl=0.0,
                    timestamp_ms=ts,
                )
                managed.append({
                    "action": "paper_direction_changed",
                    "cycle_id": int(cycle["id"]),
                    "side": side,
                    "status": status,
                })
            elif status == STATUS_OPEN and allowed:
                pnl = _cycle_pnl(cycle, market.mid, self.config.maker_fee_rate)
                self.storage.update_strategy_cycle(
                    int(cycle["id"]),
                    status=STATUS_STOPPED,
                    reason="paper_direction_changed",
                    closed_ms=ts,
                    last_mid_price=market.mid,
                    realized_pnl=pnl,
                    timestamp_ms=ts,
                )
                managed.append({
                    "action": "paper_direction_changed",
                    "cycle_id": int(cycle["id"]),
                    "side": side,
                    "status": status,
                    "realized_pnl": pnl,
                })
        return managed

    def _load_market(self):
        market_config = replace(self.config, grid_symbol=self.config.micro_grid_paper_symbol)
        return MakerGridEngine(
            config=market_config,
            storage=self.storage,
            client=self.client,
        )._load_market()

    def _active_cycles(self) -> list[Any]:
        return [
            row
            for row in self.storage.active_strategy_cycles()
            if str(row["strategy"]) == STRATEGY_NAME
        ]

    def _manage_existing_cycles(self, market, ts: int) -> list[dict[str, Any]]:
        managed: list[dict[str, Any]] = []
        for cycle in self._active_cycles():
            status = str(cycle["status"])
            if status == STATUS_ENTRY_SUBMITTED:
                managed_item = self._manage_entry_submitted(cycle, market, ts)
            elif status == STATUS_OPEN:
                managed_item = self._manage_open(cycle, market.mid, ts)
            else:
                managed_item = None
            if managed_item is not None:
                managed.append(managed_item)
        return managed

    def _manage_entry_submitted(self, cycle, market, ts: int) -> dict[str, Any] | None:
        side = str(cycle["side"])
        mid = market.mid
        entry_price = float(cycle["entry_price"])
        if _entry_filled(side, mid, entry_price):
            self.storage.update_strategy_cycle(
                int(cycle["id"]),
                status=STATUS_OPEN,
                reason="paper_entry_filled",
                opened_ms=ts,
                max_hold_deadline_ms=ts + self.config.micro_grid_paper_max_hold_seconds * 1000,
                last_mid_price=mid,
                timestamp_ms=ts,
            )
            return {
                "action": "entry_filled",
                "cycle_id": int(cycle["id"]),
                "variant": _setup_value(cycle, "variant", ""),
                "side": side,
                "mid": mid,
            }
        reanchored = self._reanchor_entry_if_needed(cycle, market, ts)
        if reanchored is not None:
            return reanchored
        if int(cycle["entry_deadline_ms"] or 0) <= ts:
            self.storage.update_strategy_cycle(
                int(cycle["id"]),
                status=STATUS_STOPPED,
                reason="entry_timeout",
                closed_ms=ts,
                last_mid_price=mid,
                realized_pnl=0.0,
                timestamp_ms=ts,
            )
            return {
                "action": "entry_timeout",
                "cycle_id": int(cycle["id"]),
                "variant": _setup_value(cycle, "variant", ""),
                "side": side,
            }
        self.storage.update_strategy_cycle(int(cycle["id"]), last_mid_price=mid, timestamp_ms=ts)
        return None

    def _reanchor_entry_if_needed(self, cycle, market, ts: int) -> dict[str, Any] | None:
        side = str(cycle["side"])
        gap = float(_setup_value(cycle, "gap_usdc", 0.0) or 0.0)
        take_profit = float(_setup_value(cycle, "take_profit_usdc", 0.0) or 0.0)
        if gap <= 0 or take_profit <= 0:
            return None
        current_entry = float(cycle["entry_price"])
        desired_entry = _entry_price(side, market.mid, gap)
        threshold = max(1.0, gap * 0.5)
        if abs(desired_entry - current_entry) < threshold:
            return None
        stop_usdc = max(gap, take_profit) * self.config.micro_grid_paper_stop_gap_multiple
        target = _target_price(side, desired_entry, take_profit)
        stop = _stop_price(side, desired_entry, stop_usdc)
        self.storage.update_strategy_cycle(
            int(cycle["id"]),
            reason="paper_reanchored",
            entry_price=desired_entry,
            target_price=target,
            stop_price=stop,
            entry_deadline_ms=ts + self.config.micro_grid_paper_entry_ttl_seconds * 1000,
            last_mid_price=market.mid,
            reprice_count=int(cycle["reprice_count"] or 0) + 1,
            timestamp_ms=ts,
        )
        _merge_cycle_setup(
            self.storage,
            int(cycle["id"]),
            {
                "entry_mid": market.mid,
                "reanchored_at_ms": ts,
            },
            ts,
        )
        return {
            "action": "entry_reanchored",
            "cycle_id": int(cycle["id"]),
            "variant": _setup_value(cycle, "variant", ""),
            "side": side,
            "old_entry": current_entry,
            "new_entry": desired_entry,
        }

    def _manage_open(self, cycle, mid: float, ts: int) -> dict[str, Any] | None:
        side = str(cycle["side"])
        target = float(cycle["target_price"])
        stop = float(cycle["stop_price"])
        reason = ""
        status = STATUS_STOPPED
        if _target_hit(side, mid, target):
            reason = "take_profit"
            status = STATUS_CLOSED
        elif _stop_hit(side, mid, stop):
            reason = "stop_loss"
        elif int(cycle["max_hold_deadline_ms"] or 0) <= ts:
            reason = "max_hold_exit"
        if not reason:
            self.storage.update_strategy_cycle(int(cycle["id"]), last_mid_price=mid, timestamp_ms=ts)
            return None
        pnl = _cycle_pnl(cycle, mid, self.config.maker_fee_rate)
        self.storage.update_strategy_cycle(
            int(cycle["id"]),
            status=status,
            reason=reason,
            closed_ms=ts,
            last_mid_price=mid,
            realized_pnl=pnl,
            timestamp_ms=ts,
        )
        return {
            "action": reason,
            "cycle_id": int(cycle["id"]),
            "variant": _setup_value(cycle, "variant", ""),
            "side": side,
            "exit_price": mid,
            "realized_pnl": pnl,
        }

    def _submit_paper_entry(self, side: str, variant: MicroGridVariant, market, ts: int) -> int:
        entry_price = _entry_price(side, market.mid, variant.gap_usdc)
        target_price = _target_price(side, entry_price, variant.take_profit_usdc)
        stop_usdc = max(variant.gap_usdc, variant.take_profit_usdc) * self.config.micro_grid_paper_stop_gap_multiple
        stop_price = _stop_price(side, entry_price, stop_usdc)
        quantity = self.config.micro_grid_paper_notional / entry_price
        setup = {
            "variant": variant.name,
            "gap_usdc": variant.gap_usdc,
            "take_profit_usdc": variant.take_profit_usdc,
            "stop_usdc": stop_usdc,
            "entry_mid": market.mid,
            "range_position_15m": market.range_position_15m,
            "ret_15m": market.ret_15m,
            "ret_1h": market.ret_1h,
            "orderflow_status": _side_orderflow_status(side, market),
            "hypothesis": "micro_grid_repeated_small_profit",
        }
        return self.storage.insert_strategy_cycle(
            strategy=STRATEGY_NAME,
            execution_mode=EXECUTION_MODE,
            symbol=self.config.micro_grid_paper_symbol,
            side=side,
            status=STATUS_ENTRY_SUBMITTED,
            reason="paper_entry_wait",
            quantity=quantity,
            entry_price=entry_price,
            target_price=target_price,
            stop_price=stop_price,
            entry_order_type="LIMIT",
            take_profit_bps=variant.take_profit_usdc / entry_price * 10_000.0,
            stop_loss_bps=stop_usdc / entry_price * 10_000.0,
            max_hold_seconds=self.config.micro_grid_paper_max_hold_seconds,
            maker_one_way_bps=self.config.maker_fee_rate * 10_000.0,
            taker_one_way_bps=self.config.taker_fee_rate * 10_000.0,
            entry_deadline_ms=ts + self.config.micro_grid_paper_entry_ttl_seconds * 1000,
            max_hold_deadline_ms=None,
            opened_ms=None,
            last_mid_price=market.mid,
            dry_run=True,
            setup=setup,
            timestamp_ms=ts,
        )

    def _side_block_reason(self, side: str, market) -> str:
        if not self.config.orderflow_guard_enabled:
            return ""
        status = _side_orderflow_status(side, market)
        if status in {"STALE", "UNKNOWN"}:
            return f"orderflow {status}: {market.orderflow_reason}"
        if status == "DANGER":
            state = load_state(self.state_path)
            count = (
                state.orderflow_long_danger_count
                if side == "long"
                else state.orderflow_short_danger_count
            )
            threshold = self.config.grid_orderflow_confirmations
            if count >= threshold:
                return f"orderflow DANGER confirmed {count}/{threshold}: {market.orderflow_reason}"
        return ""


def micro_grid_variants(config: TradingConfig) -> list[MicroGridVariant]:
    gaps = list(config.micro_grid_paper_gaps_usdc)
    take_profits = list(config.micro_grid_paper_take_profits_usdc)
    if not gaps:
        gaps = [5.0, 10.0, 15.0, 20.0]
    if not take_profits:
        take_profits = [5.0, 8.0, 10.0, 15.0]
    if len(take_profits) == 1 and len(gaps) > 1:
        take_profits = take_profits * len(gaps)
    pairs = zip(gaps, take_profits)
    return [
        MicroGridVariant(name=f"micro_{gap:g}_{tp:g}", gap_usdc=float(gap), take_profit_usdc=float(tp))
        for gap, tp in pairs
    ]


def run_step_once() -> MicroGridPaperStepResult:
    config = TradingConfig.from_env()
    return MicroGridPaperEngine(
        config=config,
        storage=TradingStore(default_db_path()),
        client=BinanceUSDMClient(config=config),
    ).step()


def _cycle_key(cycle) -> tuple[str, str]:
    return (str(_setup_value(cycle, "variant", "")), str(cycle["side"]))


def _setup_value(cycle, key: str, default: Any = None) -> Any:
    raw = cycle["setup_json"]
    if not raw:
        return default
    try:
        return json.loads(raw).get(key, default)
    except (TypeError, ValueError, json.JSONDecodeError):
        return default


def _merge_cycle_setup(store: TradingStore, cycle_id: int, extra: dict[str, Any], ts: int) -> None:
    setup: dict[str, Any] = {}
    with store.connect() as connection:
        row = connection.execute(
            "SELECT setup_json FROM strategy_cycles WHERE id=?",
            (cycle_id,),
        ).fetchone()
        if row is not None and row["setup_json"]:
            try:
                loaded = json.loads(row["setup_json"])
                if isinstance(loaded, dict):
                    setup.update(loaded)
            except (TypeError, ValueError, json.JSONDecodeError):
                setup = {}
        setup.update(extra)
        connection.execute(
            "UPDATE strategy_cycles SET setup_json=?, updated_ms=?, updated_iso=? WHERE id=?",
            (json.dumps(setup, sort_keys=True), ts, iso_from_ms(ts), cycle_id),
        )


def _side_orderflow_status(side: str, market) -> str:
    return market.orderflow_long_status if side == "long" else market.orderflow_short_status


def _entry_price(side: str, mid: float, gap_usdc: float) -> float:
    return mid - gap_usdc if side == "long" else mid + gap_usdc


def _target_price(side: str, entry_price: float, take_profit_usdc: float) -> float:
    return entry_price + take_profit_usdc if side == "long" else entry_price - take_profit_usdc


def _stop_price(side: str, entry_price: float, stop_usdc: float) -> float:
    return entry_price - stop_usdc if side == "long" else entry_price + stop_usdc


def _entry_filled(side: str, mid: float, entry_price: float) -> bool:
    return mid <= entry_price if side == "long" else mid >= entry_price


def _target_hit(side: str, mid: float, target_price: float) -> bool:
    return mid >= target_price if side == "long" else mid <= target_price


def _stop_hit(side: str, mid: float, stop_price: float) -> bool:
    return mid <= stop_price if side == "long" else mid >= stop_price


def _cycle_pnl(cycle, exit_price: float, maker_fee_rate: float) -> float:
    entry = float(cycle["entry_price"])
    qty = float(cycle["quantity"])
    if str(cycle["side"]) == "long":
        gross = (exit_price - entry) * qty
    else:
        gross = (entry - exit_price) * qty
    fees = abs(entry * qty) * maker_fee_rate + abs(exit_price * qty) * maker_fee_rate
    return gross - fees
