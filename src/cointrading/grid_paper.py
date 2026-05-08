"""Paper-only regular maker grid ("띠기") lifecycle.

The live maker-grid is intentionally Telegram-controlled. This companion
engine records what the regular wide grid would have done, without creating
Binance orders, so the dashboard can accumulate evidence while live mode is
stopped.
"""
from __future__ import annotations

from dataclasses import dataclass, field
import json
from typing import Any

from cointrading.config import TradingConfig
from cointrading.exchange.binance_usdm import BinanceUSDMClient
from cointrading.grid_lifecycle import (
    MakerGridEngine,
    _side_orderflow_status,
    _soft_filter_reasons,
)
from cointrading.storage import TradingStore, default_db_path, now_ms


STRATEGY_NAME = "maker_grid_paper"
EXECUTION_MODE = "paper_maker_grid"
STATUS_ENTRY_SUBMITTED = "ENTRY_SUBMITTED"
STATUS_OPEN = "OPEN"
STATUS_CLOSED = "CLOSED"
STATUS_STOPPED = "STOPPED"


@dataclass
class GridPaperStepResult:
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


class GridPaperEngine:
    def __init__(
        self,
        *,
        config: TradingConfig,
        storage: TradingStore,
        client: BinanceUSDMClient,
    ) -> None:
        self.config = config
        self.storage = storage
        self.client = client

    def step(self) -> GridPaperStepResult:
        ts = now_ms()
        result = GridPaperStepResult(ts_ms=ts)
        if not self.config.grid_paper_enabled:
            result.skipped.append({"reason": "disabled"})
            return result

        market = MakerGridEngine(
            config=self.config,
            storage=self.storage,
            client=self.client,
        )._load_market()
        result.managed.extend(self._manage_existing_cycles(market, ts))
        if market.risk_label == "HALT":
            result.skipped.append({"reason": "risk_halt", "detail": market.risk_reason})
            return result

        active = self._active_cycles()
        active_keys = {_cycle_key(cycle) for cycle in active}
        remaining_slots = max(0, self.config.grid_paper_max_active_cycles - len(active))
        if remaining_slots <= 0:
            result.skipped.append({"reason": "active_cap", "active": len(active)})
            return result

        for side in ("long", "short"):
            block_reason = self._side_block_reason(side, market)
            if block_reason:
                result.skipped.append({"reason": "side_block", "side": side, "detail": block_reason})
                continue
            for layer in range(1, self.config.grid_max_layers + 1):
                if remaining_slots <= 0:
                    return result
                key = (side, layer)
                if key in active_keys:
                    continue
                cycle_id = self._submit_paper_entry(side, layer, market, ts)
                result.opened.append({
                    "cycle_id": cycle_id,
                    "side": side,
                    "layer": layer,
                    "entry_price": _entry_price(side, market.mid, market.gap_usdc, layer),
                    "gap_usdc": market.gap_usdc,
                    "take_profit_usdc": market.take_profit_usdc,
                })
                active_keys.add(key)
                remaining_slots -= 1
        return result

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
                item = self._manage_entry(cycle, market.mid, ts)
            elif status == STATUS_OPEN:
                item = self._manage_open(cycle, market.mid, ts)
            else:
                item = None
            if item is not None:
                managed.append(item)
        return managed

    def _manage_entry(self, cycle, mid: float, ts: int) -> dict[str, Any] | None:
        side = str(cycle["side"])
        entry = float(cycle["entry_price"])
        if _entry_filled(side, mid, entry):
            self.storage.update_strategy_cycle(
                int(cycle["id"]),
                status=STATUS_OPEN,
                reason="paper_entry_filled",
                opened_ms=ts,
                max_hold_deadline_ms=ts + self.config.grid_paper_max_hold_seconds * 1000,
                last_mid_price=mid,
                timestamp_ms=ts,
            )
            return {
                "action": "entry_filled",
                "cycle_id": int(cycle["id"]),
                "side": side,
                "layer": _setup_value(cycle, "layer"),
            }
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
                "side": side,
                "layer": _setup_value(cycle, "layer"),
            }
        self.storage.update_strategy_cycle(int(cycle["id"]), last_mid_price=mid, timestamp_ms=ts)
        return None

    def _manage_open(self, cycle, mid: float, ts: int) -> dict[str, Any] | None:
        side = str(cycle["side"])
        reason = ""
        status = STATUS_STOPPED
        if _target_hit(side, mid, float(cycle["target_price"])):
            reason = "take_profit"
            status = STATUS_CLOSED
        elif _stop_hit(side, mid, float(cycle["stop_price"])):
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
            "side": side,
            "layer": _setup_value(cycle, "layer"),
            "realized_pnl": pnl,
        }

    def _submit_paper_entry(self, side: str, layer: int, market, ts: int) -> int:
        entry = _entry_price(side, market.mid, market.gap_usdc, layer)
        target = _target_price(side, entry, market.take_profit_usdc)
        stop_usdc = max(
            market.gap_usdc * self.config.grid_paper_stop_gap_multiple,
            market.take_profit_usdc * 2.0,
        )
        stop = _stop_price(side, entry, stop_usdc)
        qty = self.config.grid_paper_notional / entry
        setup = {
            "layer": layer,
            "gap_usdc": market.gap_usdc,
            "take_profit_usdc": market.take_profit_usdc,
            "stop_usdc": stop_usdc,
            "entry_mid": market.mid,
            "range_position_15m": market.range_position_15m,
            "ret_15m": market.ret_15m,
            "ret_1h": market.ret_1h,
            "orderflow_status": _side_orderflow_status(side, market),
            "hypothesis": "regular_maker_grid_paper",
        }
        return self.storage.insert_strategy_cycle(
            strategy=STRATEGY_NAME,
            execution_mode=EXECUTION_MODE,
            symbol=self.config.grid_symbol,
            side=side,
            status=STATUS_ENTRY_SUBMITTED,
            reason="paper_entry_wait",
            quantity=qty,
            entry_price=entry,
            target_price=target,
            stop_price=stop,
            entry_order_type="LIMIT",
            take_profit_bps=market.take_profit_usdc / entry * 10_000.0,
            stop_loss_bps=stop_usdc / entry * 10_000.0,
            max_hold_seconds=self.config.grid_paper_max_hold_seconds,
            maker_one_way_bps=self.config.maker_fee_rate * 10_000.0,
            taker_one_way_bps=self.config.taker_fee_rate * 10_000.0,
            entry_deadline_ms=ts + self.config.grid_paper_entry_ttl_seconds * 1000,
            last_mid_price=market.mid,
            dry_run=True,
            setup=setup,
            timestamp_ms=ts,
        )

    def _side_block_reason(self, side: str, market) -> str:
        if self.config.orderflow_guard_enabled:
            status = _side_orderflow_status(side, market)
            if status in {"STALE", "UNKNOWN", "DANGER"}:
                return f"orderflow {status}: {market.orderflow_reason}"
        reasons = _soft_filter_reasons(side, market, self.config)
        return "; ".join(reasons)


def run_step_once() -> GridPaperStepResult:
    config = TradingConfig.from_env()
    return GridPaperEngine(
        config=config,
        storage=TradingStore(default_db_path()),
        client=BinanceUSDMClient(config=config),
    ).step()


def _cycle_key(cycle) -> tuple[str, int]:
    return (str(cycle["side"]), int(_setup_value(cycle, "layer", 0) or 0))


def _setup_value(cycle, key: str, default: Any = None) -> Any:
    raw = cycle["setup_json"]
    if not raw:
        return default
    try:
        return json.loads(raw).get(key, default)
    except (TypeError, ValueError, json.JSONDecodeError):
        return default


def _entry_price(side: str, mid: float, gap_usdc: float, layer: int) -> float:
    return mid - gap_usdc * layer if side == "long" else mid + gap_usdc * layer


def _target_price(side: str, entry: float, take_profit_usdc: float) -> float:
    return entry + take_profit_usdc if side == "long" else entry - take_profit_usdc


def _stop_price(side: str, entry: float, stop_usdc: float) -> float:
    return entry - stop_usdc if side == "long" else entry + stop_usdc


def _entry_filled(side: str, mid: float, entry: float) -> bool:
    return mid <= entry if side == "long" else mid >= entry


def _target_hit(side: str, mid: float, target: float) -> bool:
    return mid >= target if side == "long" else mid <= target


def _stop_hit(side: str, mid: float, stop: float) -> bool:
    return mid <= stop if side == "long" else mid >= stop


def _cycle_pnl(cycle, exit_price: float, maker_fee_rate: float) -> float:
    entry = float(cycle["entry_price"])
    qty = float(cycle["quantity"])
    if str(cycle["side"]) == "long":
        gross = (exit_price - entry) * qty
    else:
        gross = (entry - exit_price) * qty
    fees = abs(entry * qty) * maker_fee_rate + abs(exit_price * qty) * maker_fee_rate
    return gross - fees
