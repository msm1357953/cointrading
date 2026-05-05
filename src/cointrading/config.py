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


def _get_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    return int(raw)


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


def _get_csv_tuple(name: str, default: tuple[str, ...]) -> tuple[str, ...]:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    values = tuple(item.strip().upper() for item in raw.split(",") if item.strip())
    return values or default


def _get_csv_lower_tuple(name: str, default: tuple[str, ...]) -> tuple[str, ...]:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    values = tuple(item.strip().lower() for item in raw.split(",") if item.strip())
    return values or default


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
    scalp_symbols: tuple[str, ...] = ("BTCUSDC", "ETHUSDC")
    live_trading_enabled: bool = False
    post_only_order_notional: float = 25.0
    max_single_order_notional: float = 50.0
    min_live_edge_bps: float = 4.0
    scalp_take_profit_bps: float = 16.0
    scalp_min_exit_bps: float = 2.0
    scalp_stop_loss_bps: float = 4.0
    scalp_entry_timeout_seconds: float = 45.0
    scalp_exit_reprice_seconds: float = 45.0
    scalp_max_hold_seconds: float = 300.0
    scalp_requote_bps: float = 1.5
    strategy_gate_enabled: bool = True
    strategy_min_samples: int = 100
    strategy_early_block_samples: int = 8
    strategy_min_expectancy_bps: float = 0.5
    strategy_min_win_rate: float = 0.42
    strategy_max_loss_win_ratio: float = 1.5
    strategy_execution_mode: str = "maker_post_only"
    strategy_taker_slippage_bps: float = 1.0
    strategy_notify_interval_minutes: int = 360
    strategy_lifecycle_enabled: bool = True
    strategy_order_notional: float = 25.0
    strategy_entry_timeout_seconds: float = 120.0
    trend_take_profit_bps: float = 90.0
    trend_stop_loss_bps: float = 30.0
    trend_max_hold_seconds: float = 14_400.0
    range_take_profit_bps: float = 30.0
    range_stop_loss_bps: float = 15.0
    range_max_hold_seconds: float = 3_600.0
    breakout_take_profit_bps: float = 120.0
    breakout_stop_loss_bps: float = 40.0
    breakout_max_hold_seconds: float = 7_200.0
    strategy_adaptive_exits_enabled: bool = True
    simple_trade_gate_enabled: bool = True
    simple_trade_gate_apply_to_dry_run: bool = False
    simple_trade_gate_allowed_strategies: tuple[str, ...] = ("trend_follow",)
    simple_trade_gate_cooldown_minutes: int = 60
    simple_trade_gate_daily_entry_limit: int = 1
    simple_trade_gate_max_consecutive_losses: int = 2
    macro_regime_gate_enabled: bool = True
    macro_regime_max_age_minutes: int = 30
    runtime_risk_enabled: bool = True
    runtime_risk_lookback_minutes: int = 30
    runtime_risk_min_events: int = 8
    runtime_risk_stop_loss_ratio_caution: float = 0.40
    runtime_risk_stop_loss_ratio_defensive: float = 0.55
    runtime_risk_stop_loss_ratio_halt: float = 0.75
    runtime_risk_requote_ratio_caution: float = 0.30
    runtime_risk_requote_ratio_defensive: float = 0.45
    runtime_risk_requote_ratio_halt: float = 0.65
    runtime_risk_daily_loss_pct: float = 0.01
    runtime_risk_btc_vol_defensive_bps: float = 120.0
    runtime_risk_btc_atr_defensive_bps: float = 180.0
    live_scalp_lifecycle_enabled: bool = False
    live_strategy_lifecycle_enabled: bool = False
    live_one_shot_required: bool = True
    live_one_shot_enabled: bool = False
    live_one_shot_symbol: str = ""
    live_one_shot_strategy: str = ""
    live_one_shot_notional: float = 25.0
    tactical_live_scenarios: tuple[str, ...] = (
        "pullback_long",
        "pullback_short",
        "key_level_breakout_long",
        "key_level_breakout_short",
        "breakout_retest_long",
        "breakout_retest_short",
    )
    tactical_live_min_closed_cycles: int = 30
    tactical_live_min_avg_pnl: float = 0.0
    tactical_live_min_win_rate: float = 0.40
    tactical_live_max_adverse_exit_ratio: float = 0.65
    tactical_live_early_evidence_enabled: bool = False
    tactical_live_early_min_closed_cycles: int = 5
    tactical_live_early_min_avg_pnl: float = 0.0
    tactical_live_early_min_win_rate: float = 0.45
    tactical_live_early_max_adverse_exit_ratio: float = 0.60
    tactical_live_early_max_notional: float = 80.0
    refined_entry_min_test_count: int = 20
    refined_entry_min_avg_pnl_bps: float = 10.0
    refined_entry_min_full_avg_pnl_bps: float = 5.0
    refined_entry_min_profit_factor: float = 1.5
    refined_entry_min_full_profit_factor: float = 1.1
    refined_entry_min_payoff_ratio: float = 1.5
    refined_entry_min_risk_reward_ratio: float = 2.0
    refined_entry_min_win_rate_edge: float = 0.05
    refined_entry_min_positive_window_ratio: float = 0.50
    supervisor_require_refined_entry_ready: bool = True
    supervisor_min_samples: int = 100
    supervisor_min_avg_pnl_bps: float = 0.5
    supervisor_min_cycle_count: int = 20
    supervisor_min_cycle_sum_pnl: float = 0.0
    supervisor_min_payoff_ratio: float = 1.2
    supervisor_recent_cycle_count: int = 20
    supervisor_min_recent_cycle_sum_pnl: float = 0.0
    supervisor_max_adverse_exit_ratio: float = 0.65
    supervisor_data_max_age_minutes: int = 10
    llm_enabled: bool = True
    llm_provider: str = "gemini"
    llm_model: str = "gemini-3.1-pro-preview"
    llm_api_key: str = ""
    # Funding-rate mean-reversion (long-only) strategy.
    # Edge verified via cointrading.research.funding_carry_backtest on 2026-05-05.
    funding_carry_enabled: bool = False
    funding_carry_symbols: tuple[str, ...] = (
        "BTCUSDC", "ETHUSDC", "SOLUSDC", "XRPUSDC", "DOGEUSDC",
    )
    funding_carry_threshold: float = 0.0001  # |funding| trigger; 0.0001 = 0.01%
    funding_carry_notional: float = 80.0
    # SL widened from 300 to 500 bps after 2026-05-05 backtest grid: tighter SL
    # cut winners and lowered EV; wider SL gives the 24h reversion room to play
    # out (stop frequency 26% -> 9%, mean +33 -> +53 bps).
    funding_carry_stop_loss_bps: float = 500.0
    funding_carry_max_hold_seconds: int = 86_400  # 24h
    funding_carry_check_window_minutes: int = 60
    funding_carry_live_enabled: bool = False  # third gate; even if dry_run=False & live_trading_enabled=True, must be True for live

    @classmethod
    def from_env(cls) -> "TradingConfig":
        _load_dotenv()
        llm_api_key = _get_first_str(("GEMINI_API_KEY", "GEMINI_KEY", "gemini_key"))
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
            scalp_symbols=_get_csv_tuple("COINTRADING_SCALP_SYMBOLS", cls.scalp_symbols),
            live_trading_enabled=_get_bool(
                "COINTRADING_LIVE_TRADING_ENABLED",
                cls.live_trading_enabled,
            ),
            post_only_order_notional=_get_float(
                "COINTRADING_POST_ONLY_ORDER_NOTIONAL",
                cls.post_only_order_notional,
            ),
            max_single_order_notional=_get_float(
                "COINTRADING_MAX_SINGLE_ORDER_NOTIONAL",
                cls.max_single_order_notional,
            ),
            min_live_edge_bps=_get_float("COINTRADING_MIN_LIVE_EDGE_BPS", cls.min_live_edge_bps),
            scalp_take_profit_bps=_get_float(
                "COINTRADING_SCALP_TAKE_PROFIT_BPS",
                cls.scalp_take_profit_bps,
            ),
            scalp_min_exit_bps=_get_float(
                "COINTRADING_SCALP_MIN_EXIT_BPS",
                cls.scalp_min_exit_bps,
            ),
            scalp_stop_loss_bps=_get_float(
                "COINTRADING_SCALP_STOP_LOSS_BPS",
                cls.scalp_stop_loss_bps,
            ),
            scalp_entry_timeout_seconds=_get_float(
                "COINTRADING_SCALP_ENTRY_TIMEOUT_SECONDS",
                cls.scalp_entry_timeout_seconds,
            ),
            scalp_exit_reprice_seconds=_get_float(
                "COINTRADING_SCALP_EXIT_REPRICE_SECONDS",
                cls.scalp_exit_reprice_seconds,
            ),
            scalp_max_hold_seconds=_get_float(
                "COINTRADING_SCALP_MAX_HOLD_SECONDS",
                cls.scalp_max_hold_seconds,
            ),
            scalp_requote_bps=_get_float(
                "COINTRADING_SCALP_REQUOTE_BPS",
                cls.scalp_requote_bps,
            ),
            strategy_gate_enabled=_get_bool(
                "COINTRADING_STRATEGY_GATE_ENABLED",
                cls.strategy_gate_enabled,
            ),
            strategy_min_samples=_get_int(
                "COINTRADING_STRATEGY_MIN_SAMPLES",
                cls.strategy_min_samples,
            ),
            strategy_early_block_samples=_get_int(
                "COINTRADING_STRATEGY_EARLY_BLOCK_SAMPLES",
                cls.strategy_early_block_samples,
            ),
            strategy_min_expectancy_bps=_get_float(
                "COINTRADING_STRATEGY_MIN_EXPECTANCY_BPS",
                cls.strategy_min_expectancy_bps,
            ),
            strategy_min_win_rate=_get_float(
                "COINTRADING_STRATEGY_MIN_WIN_RATE",
                cls.strategy_min_win_rate,
            ),
            strategy_max_loss_win_ratio=_get_float(
                "COINTRADING_STRATEGY_MAX_LOSS_WIN_RATIO",
                cls.strategy_max_loss_win_ratio,
            ),
            strategy_execution_mode=_get_str(
                "COINTRADING_STRATEGY_EXECUTION_MODE",
                cls.strategy_execution_mode,
            ),
            strategy_taker_slippage_bps=_get_float(
                "COINTRADING_STRATEGY_TAKER_SLIPPAGE_BPS",
                cls.strategy_taker_slippage_bps,
            ),
            strategy_notify_interval_minutes=_get_int(
                "COINTRADING_STRATEGY_NOTIFY_INTERVAL_MINUTES",
                cls.strategy_notify_interval_minutes,
            ),
            strategy_lifecycle_enabled=_get_bool(
                "COINTRADING_STRATEGY_LIFECYCLE_ENABLED",
                cls.strategy_lifecycle_enabled,
            ),
            strategy_order_notional=_get_float(
                "COINTRADING_STRATEGY_ORDER_NOTIONAL",
                cls.strategy_order_notional,
            ),
            strategy_entry_timeout_seconds=_get_float(
                "COINTRADING_STRATEGY_ENTRY_TIMEOUT_SECONDS",
                cls.strategy_entry_timeout_seconds,
            ),
            trend_take_profit_bps=_get_float(
                "COINTRADING_TREND_TAKE_PROFIT_BPS",
                cls.trend_take_profit_bps,
            ),
            trend_stop_loss_bps=_get_float(
                "COINTRADING_TREND_STOP_LOSS_BPS",
                cls.trend_stop_loss_bps,
            ),
            trend_max_hold_seconds=_get_float(
                "COINTRADING_TREND_MAX_HOLD_SECONDS",
                cls.trend_max_hold_seconds,
            ),
            range_take_profit_bps=_get_float(
                "COINTRADING_RANGE_TAKE_PROFIT_BPS",
                cls.range_take_profit_bps,
            ),
            range_stop_loss_bps=_get_float(
                "COINTRADING_RANGE_STOP_LOSS_BPS",
                cls.range_stop_loss_bps,
            ),
            range_max_hold_seconds=_get_float(
                "COINTRADING_RANGE_MAX_HOLD_SECONDS",
                cls.range_max_hold_seconds,
            ),
            breakout_take_profit_bps=_get_float(
                "COINTRADING_BREAKOUT_TAKE_PROFIT_BPS",
                cls.breakout_take_profit_bps,
            ),
            breakout_stop_loss_bps=_get_float(
                "COINTRADING_BREAKOUT_STOP_LOSS_BPS",
                cls.breakout_stop_loss_bps,
            ),
            breakout_max_hold_seconds=_get_float(
                "COINTRADING_BREAKOUT_MAX_HOLD_SECONDS",
                cls.breakout_max_hold_seconds,
            ),
            strategy_adaptive_exits_enabled=_get_bool(
                "COINTRADING_STRATEGY_ADAPTIVE_EXITS_ENABLED",
                cls.strategy_adaptive_exits_enabled,
            ),
            simple_trade_gate_enabled=_get_bool(
                "COINTRADING_SIMPLE_TRADE_GATE_ENABLED",
                cls.simple_trade_gate_enabled,
            ),
            simple_trade_gate_apply_to_dry_run=_get_bool(
                "COINTRADING_SIMPLE_TRADE_GATE_APPLY_TO_DRY_RUN",
                cls.simple_trade_gate_apply_to_dry_run,
            ),
            simple_trade_gate_allowed_strategies=_get_csv_lower_tuple(
                "COINTRADING_SIMPLE_TRADE_GATE_ALLOWED_STRATEGIES",
                cls.simple_trade_gate_allowed_strategies,
            ),
            simple_trade_gate_cooldown_minutes=_get_int(
                "COINTRADING_SIMPLE_TRADE_GATE_COOLDOWN_MINUTES",
                cls.simple_trade_gate_cooldown_minutes,
            ),
            simple_trade_gate_daily_entry_limit=_get_int(
                "COINTRADING_SIMPLE_TRADE_GATE_DAILY_ENTRY_LIMIT",
                cls.simple_trade_gate_daily_entry_limit,
            ),
            simple_trade_gate_max_consecutive_losses=_get_int(
                "COINTRADING_SIMPLE_TRADE_GATE_MAX_CONSECUTIVE_LOSSES",
                cls.simple_trade_gate_max_consecutive_losses,
            ),
            macro_regime_gate_enabled=_get_bool(
                "COINTRADING_MACRO_REGIME_GATE_ENABLED",
                cls.macro_regime_gate_enabled,
            ),
            macro_regime_max_age_minutes=_get_int(
                "COINTRADING_MACRO_REGIME_MAX_AGE_MINUTES",
                cls.macro_regime_max_age_minutes,
            ),
            runtime_risk_enabled=_get_bool(
                "COINTRADING_RUNTIME_RISK_ENABLED",
                cls.runtime_risk_enabled,
            ),
            runtime_risk_lookback_minutes=_get_int(
                "COINTRADING_RUNTIME_RISK_LOOKBACK_MINUTES",
                cls.runtime_risk_lookback_minutes,
            ),
            runtime_risk_min_events=_get_int(
                "COINTRADING_RUNTIME_RISK_MIN_EVENTS",
                cls.runtime_risk_min_events,
            ),
            runtime_risk_stop_loss_ratio_caution=_get_float(
                "COINTRADING_RUNTIME_RISK_STOP_LOSS_RATIO_CAUTION",
                cls.runtime_risk_stop_loss_ratio_caution,
            ),
            runtime_risk_stop_loss_ratio_defensive=_get_float(
                "COINTRADING_RUNTIME_RISK_STOP_LOSS_RATIO_DEFENSIVE",
                cls.runtime_risk_stop_loss_ratio_defensive,
            ),
            runtime_risk_stop_loss_ratio_halt=_get_float(
                "COINTRADING_RUNTIME_RISK_STOP_LOSS_RATIO_HALT",
                cls.runtime_risk_stop_loss_ratio_halt,
            ),
            runtime_risk_requote_ratio_caution=_get_float(
                "COINTRADING_RUNTIME_RISK_REQUOTE_RATIO_CAUTION",
                cls.runtime_risk_requote_ratio_caution,
            ),
            runtime_risk_requote_ratio_defensive=_get_float(
                "COINTRADING_RUNTIME_RISK_REQUOTE_RATIO_DEFENSIVE",
                cls.runtime_risk_requote_ratio_defensive,
            ),
            runtime_risk_requote_ratio_halt=_get_float(
                "COINTRADING_RUNTIME_RISK_REQUOTE_RATIO_HALT",
                cls.runtime_risk_requote_ratio_halt,
            ),
            runtime_risk_daily_loss_pct=_get_float(
                "COINTRADING_RUNTIME_RISK_DAILY_LOSS_PCT",
                cls.runtime_risk_daily_loss_pct,
            ),
            runtime_risk_btc_vol_defensive_bps=_get_float(
                "COINTRADING_RUNTIME_RISK_BTC_VOL_DEFENSIVE_BPS",
                cls.runtime_risk_btc_vol_defensive_bps,
            ),
            runtime_risk_btc_atr_defensive_bps=_get_float(
                "COINTRADING_RUNTIME_RISK_BTC_ATR_DEFENSIVE_BPS",
                cls.runtime_risk_btc_atr_defensive_bps,
            ),
            live_scalp_lifecycle_enabled=_get_bool(
                "COINTRADING_LIVE_SCALP_LIFECYCLE_ENABLED",
                cls.live_scalp_lifecycle_enabled,
            ),
            live_strategy_lifecycle_enabled=_get_bool(
                "COINTRADING_LIVE_STRATEGY_LIFECYCLE_ENABLED",
                cls.live_strategy_lifecycle_enabled,
            ),
            live_one_shot_required=_get_bool(
                "COINTRADING_LIVE_ONE_SHOT_REQUIRED",
                cls.live_one_shot_required,
            ),
            live_one_shot_enabled=_get_bool(
                "COINTRADING_LIVE_ONE_SHOT_ENABLED",
                cls.live_one_shot_enabled,
            ),
            live_one_shot_symbol=_get_str(
                "COINTRADING_LIVE_ONE_SHOT_SYMBOL",
                cls.live_one_shot_symbol,
            ).upper(),
            live_one_shot_strategy=_get_str(
                "COINTRADING_LIVE_ONE_SHOT_STRATEGY",
                cls.live_one_shot_strategy,
            ),
            live_one_shot_notional=_get_float(
                "COINTRADING_LIVE_ONE_SHOT_NOTIONAL",
                cls.live_one_shot_notional,
            ),
            tactical_live_scenarios=_get_csv_lower_tuple(
                "COINTRADING_TACTICAL_LIVE_SCENARIOS",
                cls.tactical_live_scenarios,
            ),
            tactical_live_min_closed_cycles=_get_int(
                "COINTRADING_TACTICAL_LIVE_MIN_CLOSED_CYCLES",
                cls.tactical_live_min_closed_cycles,
            ),
            tactical_live_min_avg_pnl=_get_float(
                "COINTRADING_TACTICAL_LIVE_MIN_AVG_PNL",
                cls.tactical_live_min_avg_pnl,
            ),
            tactical_live_min_win_rate=_get_float(
                "COINTRADING_TACTICAL_LIVE_MIN_WIN_RATE",
                cls.tactical_live_min_win_rate,
            ),
            tactical_live_max_adverse_exit_ratio=_get_float(
                "COINTRADING_TACTICAL_LIVE_MAX_ADVERSE_EXIT_RATIO",
                cls.tactical_live_max_adverse_exit_ratio,
            ),
            tactical_live_early_evidence_enabled=_get_bool(
                "COINTRADING_TACTICAL_LIVE_EARLY_EVIDENCE_ENABLED",
                cls.tactical_live_early_evidence_enabled,
            ),
            tactical_live_early_min_closed_cycles=_get_int(
                "COINTRADING_TACTICAL_LIVE_EARLY_MIN_CLOSED_CYCLES",
                cls.tactical_live_early_min_closed_cycles,
            ),
            tactical_live_early_min_avg_pnl=_get_float(
                "COINTRADING_TACTICAL_LIVE_EARLY_MIN_AVG_PNL",
                cls.tactical_live_early_min_avg_pnl,
            ),
            tactical_live_early_min_win_rate=_get_float(
                "COINTRADING_TACTICAL_LIVE_EARLY_MIN_WIN_RATE",
                cls.tactical_live_early_min_win_rate,
            ),
            tactical_live_early_max_adverse_exit_ratio=_get_float(
                "COINTRADING_TACTICAL_LIVE_EARLY_MAX_ADVERSE_EXIT_RATIO",
                cls.tactical_live_early_max_adverse_exit_ratio,
            ),
            tactical_live_early_max_notional=_get_float(
                "COINTRADING_TACTICAL_LIVE_EARLY_MAX_NOTIONAL",
                cls.tactical_live_early_max_notional,
            ),
            refined_entry_min_test_count=_get_int(
                "COINTRADING_REFINED_ENTRY_MIN_TEST_COUNT",
                cls.refined_entry_min_test_count,
            ),
            refined_entry_min_avg_pnl_bps=_get_float(
                "COINTRADING_REFINED_ENTRY_MIN_AVG_PNL_BPS",
                cls.refined_entry_min_avg_pnl_bps,
            ),
            refined_entry_min_full_avg_pnl_bps=_get_float(
                "COINTRADING_REFINED_ENTRY_MIN_FULL_AVG_PNL_BPS",
                cls.refined_entry_min_full_avg_pnl_bps,
            ),
            refined_entry_min_profit_factor=_get_float(
                "COINTRADING_REFINED_ENTRY_MIN_PROFIT_FACTOR",
                cls.refined_entry_min_profit_factor,
            ),
            refined_entry_min_full_profit_factor=_get_float(
                "COINTRADING_REFINED_ENTRY_MIN_FULL_PROFIT_FACTOR",
                cls.refined_entry_min_full_profit_factor,
            ),
            refined_entry_min_payoff_ratio=_get_float(
                "COINTRADING_REFINED_ENTRY_MIN_PAYOFF_RATIO",
                cls.refined_entry_min_payoff_ratio,
            ),
            refined_entry_min_risk_reward_ratio=_get_float(
                "COINTRADING_REFINED_ENTRY_MIN_RISK_REWARD_RATIO",
                cls.refined_entry_min_risk_reward_ratio,
            ),
            refined_entry_min_win_rate_edge=_get_float(
                "COINTRADING_REFINED_ENTRY_MIN_WIN_RATE_EDGE",
                cls.refined_entry_min_win_rate_edge,
            ),
            refined_entry_min_positive_window_ratio=_get_float(
                "COINTRADING_REFINED_ENTRY_MIN_POSITIVE_WINDOW_RATIO",
                cls.refined_entry_min_positive_window_ratio,
            ),
            supervisor_require_refined_entry_ready=_get_bool(
                "COINTRADING_SUPERVISOR_REQUIRE_REFINED_ENTRY_READY",
                cls.supervisor_require_refined_entry_ready,
            ),
            supervisor_min_samples=_get_int(
                "COINTRADING_SUPERVISOR_MIN_SAMPLES",
                cls.supervisor_min_samples,
            ),
            supervisor_min_avg_pnl_bps=_get_float(
                "COINTRADING_SUPERVISOR_MIN_AVG_PNL_BPS",
                cls.supervisor_min_avg_pnl_bps,
            ),
            supervisor_min_cycle_count=_get_int(
                "COINTRADING_SUPERVISOR_MIN_CYCLE_COUNT",
                cls.supervisor_min_cycle_count,
            ),
            supervisor_min_cycle_sum_pnl=_get_float(
                "COINTRADING_SUPERVISOR_MIN_CYCLE_SUM_PNL",
                cls.supervisor_min_cycle_sum_pnl,
            ),
            supervisor_min_payoff_ratio=_get_float(
                "COINTRADING_SUPERVISOR_MIN_PAYOFF_RATIO",
                cls.supervisor_min_payoff_ratio,
            ),
            supervisor_recent_cycle_count=_get_int(
                "COINTRADING_SUPERVISOR_RECENT_CYCLE_COUNT",
                cls.supervisor_recent_cycle_count,
            ),
            supervisor_min_recent_cycle_sum_pnl=_get_float(
                "COINTRADING_SUPERVISOR_MIN_RECENT_CYCLE_SUM_PNL",
                cls.supervisor_min_recent_cycle_sum_pnl,
            ),
            supervisor_max_adverse_exit_ratio=_get_float(
                "COINTRADING_SUPERVISOR_MAX_ADVERSE_EXIT_RATIO",
                cls.supervisor_max_adverse_exit_ratio,
            ),
            supervisor_data_max_age_minutes=_get_int(
                "COINTRADING_SUPERVISOR_DATA_MAX_AGE_MINUTES",
                cls.supervisor_data_max_age_minutes,
            ),
            llm_enabled=_get_bool("COINTRADING_LLM_ENABLED", cls.llm_enabled),
            llm_provider=_get_str("COINTRADING_LLM_PROVIDER", cls.llm_provider),
            llm_model=_get_str("COINTRADING_LLM_MODEL", cls.llm_model),
            llm_api_key=llm_api_key,
            funding_carry_enabled=_get_bool(
                "COINTRADING_FUNDING_CARRY_ENABLED", cls.funding_carry_enabled
            ),
            funding_carry_symbols=_get_csv_tuple(
                "COINTRADING_FUNDING_CARRY_SYMBOLS", cls.funding_carry_symbols
            ),
            funding_carry_threshold=_get_float(
                "COINTRADING_FUNDING_CARRY_THRESHOLD", cls.funding_carry_threshold
            ),
            funding_carry_notional=_get_float(
                "COINTRADING_FUNDING_CARRY_NOTIONAL", cls.funding_carry_notional
            ),
            funding_carry_stop_loss_bps=_get_float(
                "COINTRADING_FUNDING_CARRY_STOP_LOSS_BPS", cls.funding_carry_stop_loss_bps
            ),
            funding_carry_max_hold_seconds=_get_int(
                "COINTRADING_FUNDING_CARRY_MAX_HOLD_SECONDS", cls.funding_carry_max_hold_seconds
            ),
            funding_carry_check_window_minutes=_get_int(
                "COINTRADING_FUNDING_CARRY_CHECK_WINDOW_MINUTES",
                cls.funding_carry_check_window_minutes,
            ),
            funding_carry_live_enabled=_get_bool(
                "COINTRADING_FUNDING_CARRY_LIVE_ENABLED", cls.funding_carry_live_enabled
            ),
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
