from __future__ import annotations

from dataclasses import dataclass
import os
from pathlib import Path


_DOTENV_LOADED = False


def _load_dotenv() -> None:
    global _DOTENV_LOADED
    if _DOTENV_LOADED:
        return
    _DOTENV_LOADED = True
    project_root = Path(__file__).resolve().parents[2]
    path = project_root / ".env"
    if not path.exists():
        return
    for raw_line in path.read_text().splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip("\"'")
        if key and key not in os.environ:
            os.environ[key] = value


def _get_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "y", "on"}


def _get_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    return float(raw)


def _get_str(name: str, default: str = "") -> str:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip()


def _get_first_str(names: tuple[str, ...], default: str = "") -> str:
    for name in names:
        value = _get_str(name)
        if value:
            return value
    return default


def _get_csv_set(name: str) -> set[str]:
    raw = os.getenv(name, "")
    return {item.strip() for item in raw.split(",") if item.strip()}


@dataclass(frozen=True)
class TradingConfig:
    initial_equity: float = 1000.0
    equity_asset: str = "USDC"
    max_drawdown_pct: float = 0.10
    daily_loss_pct: float = 0.03
    risk_per_trade_pct: float = 0.005
    max_notional_multiplier: float = 1.5
    max_leverage: float = 2.0
    taker_fee_rate: float = 0.0005
    maker_fee_rate: float = 0.0002
    slippage_bps: float = 2.0
    dry_run: bool = True
    testnet: bool = True

    @classmethod
    def from_env(cls) -> "TradingConfig":
        _load_dotenv()
        return cls(
            initial_equity=_get_float("COINTRADING_INITIAL_EQUITY", cls.initial_equity),
            equity_asset=_get_str("COINTRADING_EQUITY_ASSET", cls.equity_asset).upper(),
            max_drawdown_pct=_get_float(
                "COINTRADING_MAX_DRAWDOWN_PCT", cls.max_drawdown_pct
            ),
            daily_loss_pct=_get_float("COINTRADING_DAILY_LOSS_PCT", cls.daily_loss_pct),
            risk_per_trade_pct=_get_float(
                "COINTRADING_RISK_PER_TRADE_PCT", cls.risk_per_trade_pct
            ),
            max_notional_multiplier=_get_float(
                "COINTRADING_MAX_NOTIONAL_MULTIPLIER",
                cls.max_notional_multiplier,
            ),
            max_leverage=_get_float("COINTRADING_MAX_LEVERAGE", cls.max_leverage),
            taker_fee_rate=_get_float("COINTRADING_TAKER_FEE_RATE", cls.taker_fee_rate),
            maker_fee_rate=_get_float("COINTRADING_MAKER_FEE_RATE", cls.maker_fee_rate),
            slippage_bps=_get_float("COINTRADING_SLIPPAGE_BPS", cls.slippage_bps),
            dry_run=_get_bool("COINTRADING_DRY_RUN", cls.dry_run),
            testnet=_get_bool("COINTRADING_TESTNET", cls.testnet),
        )


@dataclass(frozen=True)
class TelegramConfig:
    bot_token: str = ""
    default_chat_id: str = ""
    allowed_chat_ids: frozenset[str] = frozenset()
    commands_enabled: bool = False

    @classmethod
    def from_env(cls) -> "TelegramConfig":
        _load_dotenv()
        default_chat_id = _get_first_str(("TELEGRAM_CHAT_ID", "TELEGRAM_DEFAULT_CHAT_ID"))
        allowed_chat_ids = _get_csv_set("TELEGRAM_ALLOWED_CHAT_IDS")
        if default_chat_id and not allowed_chat_ids:
            allowed_chat_ids = {default_chat_id}
        return cls(
            bot_token=_get_first_str(("TELEGRAM_BOT_TOKEN", "TELEGRAM_BOT")),
            default_chat_id=default_chat_id,
            allowed_chat_ids=frozenset(allowed_chat_ids),
            commands_enabled=_get_bool("TELEGRAM_COMMANDS_ENABLED", cls.commands_enabled),
        )
