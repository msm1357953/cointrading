from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path

from cointrading.config import TradingConfig
from cointrading.storage import now_ms


@dataclass(frozen=True)
class LiveGuardDecision:
    allowed: bool
    reason: str


@dataclass
class LiveOneShotState:
    consumed: bool = False
    symbol: str = ""
    strategy: str = ""
    notional: float = 0.0
    cycle_id: int | None = None
    consumed_ms: int = 0

    @classmethod
    def load(cls, path: Path | None = None) -> "LiveOneShotState":
        path = path or default_live_one_shot_state_path()
        if not path.exists():
            return cls()
        payload = json.loads(path.read_text())
        return cls(
            consumed=bool(payload.get("consumed", False)),
            symbol=str(payload.get("symbol", "")),
            strategy=str(payload.get("strategy", "")),
            notional=float(payload.get("notional", 0.0)),
            cycle_id=payload.get("cycle_id"),
            consumed_ms=int(payload.get("consumed_ms", 0)),
        )

    def save(self, path: Path | None = None) -> None:
        path = path or default_live_one_shot_state_path()
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            json.dumps(
                {
                    "consumed": self.consumed,
                    "symbol": self.symbol,
                    "strategy": self.strategy,
                    "notional": self.notional,
                    "cycle_id": self.cycle_id,
                    "consumed_ms": self.consumed_ms,
                },
                indent=2,
                sort_keys=True,
            )
        )


def default_live_one_shot_state_path() -> Path:
    return Path(__file__).resolve().parents[2] / "data" / "live_one_shot_state.json"


def validate_live_one_shot(
    config: TradingConfig,
    *,
    symbol: str,
    strategy: str,
    notional: float,
    state: LiveOneShotState | None = None,
) -> LiveGuardDecision:
    if config.dry_run or not config.live_one_shot_required:
        return LiveGuardDecision(True, "live one-shot guard not required")
    state = state or LiveOneShotState.load()
    if state.consumed:
        return LiveGuardDecision(False, "live one-shot already consumed")
    if not config.live_one_shot_enabled:
        return LiveGuardDecision(False, "live one-shot flag is disabled")
    if config.live_one_shot_symbol and symbol.upper() != config.live_one_shot_symbol.upper():
        return LiveGuardDecision(
            False,
            f"live one-shot symbol is {config.live_one_shot_symbol}",
        )
    if config.live_one_shot_strategy and strategy != config.live_one_shot_strategy:
        return LiveGuardDecision(
            False,
            f"live one-shot strategy is {config.live_one_shot_strategy}",
        )
    if notional > config.live_one_shot_notional:
        return LiveGuardDecision(
            False,
            f"live one-shot notional {config.live_one_shot_notional:.2f} exceeded",
        )
    return LiveGuardDecision(True, "live one-shot guard ok")


def consume_live_one_shot(
    *,
    symbol: str,
    strategy: str,
    notional: float,
    cycle_id: int,
    state_path: Path | None = None,
) -> None:
    state = LiveOneShotState(
        consumed=True,
        symbol=symbol.upper(),
        strategy=strategy,
        notional=notional,
        cycle_id=cycle_id,
        consumed_ms=now_ms(),
    )
    state.save(state_path)
