from __future__ import annotations

import argparse
import csv
from dataclasses import replace
import math
from pathlib import Path
import time
from typing import Iterable

from cointrading.account import account_summary_text
from cointrading.backtest import Backtester
from cointrading.config import TelegramConfig, TradingConfig
from cointrading.dashboard import run_dashboard
from cointrading.execution import build_post_only_intent, place_post_only_maker
from cointrading.exchange.binance_usdm import BinanceAPIError, BinanceUSDMClient
from cointrading.exchange_filters import SymbolFilters
from cointrading.llm_report import (
    GeminiReportClient,
    LLMReportState,
    build_report_context,
    build_report_prompt,
    default_llm_report_state_path,
    fallback_report_text,
    llm_report_due,
)
from cointrading.live_supervisor_notify import (
    LiveSupervisorNotifyState,
    apply_live_supervisor_notify_state,
    default_live_supervisor_notify_state_path,
    supervisor_candidate_notification_decision,
    supervisor_candidate_notification_text,
)
from cointrading.market_context import collect_market_context, market_context_rows_text
from cointrading.market_regime import evaluate_market_regime
from cointrading.models import Kline
from cointrading.research_probe import (
    ProbeNotifyState,
    apply_probe_notification_state,
    default_probe_notify_state_path,
    default_probe_report_path,
    probe_notification_decision,
    run_vibe_style_probe,
    vibe_probe_text,
    write_probe_report,
)
from cointrading.risk_state import evaluate_runtime_risk
from cointrading.scalp_lifecycle import manage_cycle, start_cycle_from_signal
from cointrading.scalping import (
    ScalpSignalEngine,
    append_scalp_signal,
    default_scalp_log_path,
    scalp_report_rows_text,
    scalp_report_text,
    score_scalp_log,
)
from cointrading.storage import TradingStore, default_db_path
from cointrading.strategy_eval import evaluate_and_store_strategy, strategy_evaluation_text
from cointrading.strategy_lifecycle import manage_strategy_cycle, start_strategy_cycle_from_setup
from cointrading.strategy_notify import (
    StrategyNotifyState,
    apply_strategy_notification_state,
    default_strategy_notify_state_path,
    strategy_notification_decision,
    strategy_notification_text,
)
from cointrading.strategy_router import evaluate_strategy_setups, strategy_setups_text
from cointrading.symbol_supervisor import (
    refresh_supervisor_inputs,
    supervise_symbols,
    supervisor_report_text,
)
from cointrading.strategies import MovingAverageCrossStrategy
from cointrading.telegram_bot import (
    TelegramBotState,
    TelegramClient,
    TelegramCommandProcessor,
    default_state_path,
    poll_forever,
    poll_once,
)
from cointrading.trade_event_notify import (
    TradeEventNotifyState,
    apply_trade_event_notification_state,
    default_trade_event_notify_state_path,
    trade_event_notification_decision,
    trade_event_notification_text,
)


DEFAULT_FEE_SYMBOLS = ["BTCUSDC", "ETHUSDC"]


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="cointrading")
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("explain-mdd")
    subparsers.add_parser("demo-backtest")

    fetch = subparsers.add_parser("fetch-klines")
    fetch.add_argument("--symbol", default="BTCUSDT")
    fetch.add_argument("--interval", default="1h")
    fetch.add_argument("--limit", type=int, default=200)
    fetch.add_argument("--output", type=Path)

    backtest_csv = subparsers.add_parser("backtest-csv")
    backtest_csv.add_argument("path", type=Path)
    backtest_csv.add_argument("--symbol", default="BTCUSDT")

    subparsers.add_parser("binance-account")

    scalp_check_parser = subparsers.add_parser("scalp-check")
    scalp_check_parser.add_argument("--symbol", default="BTCUSDC")
    scalp_check_parser.add_argument("--depth-limit", type=int, default=20)
    scalp_check_parser.add_argument("--kline-limit", type=int, default=30)

    scalp_collect_parser = subparsers.add_parser("scalp-collect")
    scalp_collect_parser.add_argument("--symbols", nargs="+")
    scalp_collect_parser.add_argument("--log-path", type=Path, default=default_scalp_log_path())
    scalp_collect_parser.add_argument("--db-path", type=Path, default=default_db_path())

    scalp_score_parser = subparsers.add_parser("scalp-score")
    scalp_score_parser.add_argument("--symbols", nargs="+")
    scalp_score_parser.add_argument("--log-path", type=Path, default=default_scalp_log_path())
    scalp_score_parser.add_argument("--db-path", type=Path, default=default_db_path())

    scalp_report_parser = subparsers.add_parser("scalp-report")
    scalp_report_parser.add_argument("--symbol")
    scalp_report_parser.add_argument("--all-symbols", action="store_true")
    scalp_report_parser.add_argument("--log-path", type=Path, default=default_scalp_log_path())
    scalp_report_parser.add_argument("--db-path", type=Path, default=default_db_path())
    scalp_report_parser.add_argument("--csv", action="store_true")

    migrate_parser = subparsers.add_parser("migrate-csv-to-db")
    migrate_parser.add_argument("--log-path", type=Path, default=default_scalp_log_path())
    migrate_parser.add_argument("--db-path", type=Path, default=default_db_path())

    db_summary_parser = subparsers.add_parser("db-summary")
    db_summary_parser.add_argument("--db-path", type=Path, default=default_db_path())

    risk_mode_parser = subparsers.add_parser("risk-mode")
    risk_mode_parser.add_argument("--db-path", type=Path, default=default_db_path())

    maker_once_parser = subparsers.add_parser("maker-once")
    maker_once_parser.add_argument("--symbol", default="BTCUSDC")
    maker_once_parser.add_argument("--db-path", type=Path, default=default_db_path())

    scalp_engine_parser = subparsers.add_parser("scalp-engine-step")
    scalp_engine_parser.add_argument("--symbols", nargs="+")
    scalp_engine_parser.add_argument("--log-path", type=Path, default=default_scalp_log_path())
    scalp_engine_parser.add_argument("--db-path", type=Path, default=default_db_path())

    strategy_engine_parser = subparsers.add_parser("strategy-engine-step")
    strategy_engine_parser.add_argument("--symbols", nargs="+")
    strategy_engine_parser.add_argument("--log-path", type=Path, default=default_scalp_log_path())
    strategy_engine_parser.add_argument("--db-path", type=Path, default=default_db_path())

    strategy_evaluate_parser = subparsers.add_parser("strategy-evaluate")
    strategy_evaluate_parser.add_argument("--db-path", type=Path, default=default_db_path())
    strategy_evaluate_parser.add_argument("--limit", type=int, default=25)

    strategy_notify_parser = subparsers.add_parser("strategy-notify")
    strategy_notify_parser.add_argument("--db-path", type=Path, default=default_db_path())
    strategy_notify_parser.add_argument(
        "--state-path",
        type=Path,
        default=default_strategy_notify_state_path(),
    )
    strategy_notify_parser.add_argument("--periodic-minutes", type=int)
    strategy_notify_parser.add_argument("--limit", type=int, default=8)
    strategy_notify_parser.add_argument("--force", action="store_true")

    market_regime_parser = subparsers.add_parser("market-regime")
    market_regime_parser.add_argument("--symbols", nargs="+")
    market_regime_parser.add_argument("--db-path", type=Path, default=default_db_path())
    market_regime_parser.add_argument("--store", action="store_true")

    market_regime_collect_parser = subparsers.add_parser("market-regime-collect")
    market_regime_collect_parser.add_argument("--symbols", nargs="+")
    market_regime_collect_parser.add_argument("--db-path", type=Path, default=default_db_path())

    market_context_parser = subparsers.add_parser("market-context")
    market_context_parser.add_argument("--symbols", nargs="+")
    market_context_parser.add_argument("--db-path", type=Path, default=default_db_path())
    market_context_parser.add_argument("--store", action="store_true")

    market_context_collect_parser = subparsers.add_parser("market-context-collect")
    market_context_collect_parser.add_argument("--symbols", nargs="+")
    market_context_collect_parser.add_argument("--db-path", type=Path, default=default_db_path())

    llm_report_parser = subparsers.add_parser("llm-report")
    llm_report_parser.add_argument("--db-path", type=Path, default=default_db_path())
    llm_report_parser.add_argument("--state-path", type=Path, default=default_llm_report_state_path())
    llm_report_parser.add_argument("--interval-hours", type=int, default=8)
    llm_report_parser.add_argument("--force", action="store_true")
    llm_report_parser.add_argument("--send-telegram", action="store_true")
    llm_report_parser.add_argument("--fallback", action="store_true")

    dashboard_parser = subparsers.add_parser("dashboard")
    dashboard_parser.add_argument("--host", default="127.0.0.1")
    dashboard_parser.add_argument("--port", type=int, default=8080)
    dashboard_parser.add_argument("--db-path", type=Path, default=default_db_path())

    fee_status_parser = subparsers.add_parser("fee-status")
    fee_status_parser.add_argument("--symbols", nargs="+", default=DEFAULT_FEE_SYMBOLS)
    fee_status_parser.add_argument("--db-path", type=Path, default=default_db_path())

    live_preflight_parser = subparsers.add_parser("live-preflight")
    live_preflight_parser.add_argument("--symbols", nargs="+")
    live_preflight_parser.add_argument("--notional", type=float, default=10.0)
    live_preflight_parser.add_argument("--db-path", type=Path, default=default_db_path())

    live_supervisor_parser = subparsers.add_parser("live-supervisor")
    live_supervisor_parser.add_argument("--symbols", nargs="+")
    live_supervisor_parser.add_argument("--notional", type=float, default=25.0)
    live_supervisor_parser.add_argument("--db-path", type=Path, default=default_db_path())

    live_supervisor_notify_parser = subparsers.add_parser("live-supervisor-notify")
    live_supervisor_notify_parser.add_argument("--symbols", nargs="+")
    live_supervisor_notify_parser.add_argument("--notional", type=float)
    live_supervisor_notify_parser.add_argument("--db-path", type=Path, default=default_db_path())
    live_supervisor_notify_parser.add_argument(
        "--state-path",
        type=Path,
        default=default_live_supervisor_notify_state_path(),
    )
    live_supervisor_notify_parser.add_argument("--force", action="store_true")

    trade_event_notify_parser = subparsers.add_parser("trade-event-notify")
    trade_event_notify_parser.add_argument("--db-path", type=Path, default=default_db_path())
    trade_event_notify_parser.add_argument(
        "--state-path",
        type=Path,
        default=default_trade_event_notify_state_path(),
    )
    trade_event_notify_parser.add_argument("--summary-interval-minutes", type=int, default=60)
    trade_event_notify_parser.add_argument("--event-limit", type=int, default=10)
    trade_event_notify_parser.add_argument("--force-summary", action="store_true")
    trade_event_notify_parser.add_argument("--no-send", action="store_true")

    vibe_probe_parser = subparsers.add_parser("vibe-probe")
    vibe_probe_parser.add_argument("--symbols", nargs="+")
    vibe_probe_parser.add_argument("--interval", default="15m")
    vibe_probe_parser.add_argument("--limit", type=int, default=1000)
    vibe_probe_parser.add_argument("--notional", type=float)
    vibe_probe_parser.add_argument("--output", type=Path, default=default_probe_report_path())

    vibe_probe_notify_parser = subparsers.add_parser("vibe-probe-notify")
    vibe_probe_notify_parser.add_argument("--symbols", nargs="+")
    vibe_probe_notify_parser.add_argument("--interval", default="15m")
    vibe_probe_notify_parser.add_argument("--limit", type=int, default=1000)
    vibe_probe_notify_parser.add_argument("--notional", type=float)
    vibe_probe_notify_parser.add_argument("--output", type=Path, default=default_probe_report_path())
    vibe_probe_notify_parser.add_argument(
        "--state-path",
        type=Path,
        default=default_probe_notify_state_path(),
    )
    vibe_probe_notify_parser.add_argument("--periodic-minutes", type=int, default=360)
    vibe_probe_notify_parser.add_argument("--force", action="store_true")
    vibe_probe_notify_parser.add_argument("--no-send", action="store_true")

    subparsers.add_parser("telegram-me")

    telegram_send_parser = subparsers.add_parser("telegram-send")
    telegram_send_parser.add_argument("text", nargs="+")
    telegram_send_parser.add_argument("--chat-id")

    telegram_updates_parser = subparsers.add_parser("telegram-updates")
    telegram_updates_parser.add_argument("--limit", type=int, default=10)
    telegram_updates_parser.add_argument("--timeout", type=int, default=0)

    telegram_poll_parser = subparsers.add_parser("telegram-poll")
    telegram_poll_parser.add_argument("--once", action="store_true")
    telegram_poll_parser.add_argument("--timeout", type=int, default=20)
    telegram_poll_parser.add_argument("--interval", type=float, default=1.0)

    args = parser.parse_args(argv)

    if args.command == "explain-mdd":
        explain_mdd()
    elif args.command == "demo-backtest":
        run_demo_backtest()
    elif args.command == "fetch-klines":
        fetch_klines(args.symbol, args.interval, args.limit, args.output)
    elif args.command == "backtest-csv":
        run_backtest_csv(args.path, args.symbol)
    elif args.command == "binance-account":
        binance_account()
    elif args.command == "scalp-check":
        scalp_check(args.symbol, args.depth_limit, args.kline_limit)
    elif args.command == "scalp-collect":
        scalp_collect(_active_scalp_symbols(args.symbols), args.log_path, args.db_path)
    elif args.command == "scalp-score":
        scalp_score(_active_scalp_symbols(args.symbols), args.log_path, args.db_path)
    elif args.command == "scalp-report":
        active_symbols = None if args.all_symbols else _active_scalp_symbols(None)
        if args.csv:
            print(scalp_report_text(args.log_path, args.symbol, symbols=active_symbols))
        else:
            print(scalp_report_db_text(args.db_path, args.log_path, args.symbol, active_symbols))
    elif args.command == "migrate-csv-to-db":
        migrate_csv_to_db(args.log_path, args.db_path)
    elif args.command == "db-summary":
        db_summary(args.db_path)
    elif args.command == "risk-mode":
        risk_mode(args.db_path)
    elif args.command == "maker-once":
        maker_once(args.symbol, args.db_path)
    elif args.command == "scalp-engine-step":
        scalp_engine_step(_active_scalp_symbols(args.symbols), args.log_path, args.db_path)
    elif args.command == "strategy-engine-step":
        strategy_engine_step(_active_scalp_symbols(args.symbols), args.log_path, args.db_path)
    elif args.command == "strategy-evaluate":
        strategy_evaluate(args.db_path, args.limit)
    elif args.command == "strategy-notify":
        strategy_notify(
            args.db_path,
            args.state_path,
            args.periodic_minutes,
            args.limit,
            args.force,
        )
    elif args.command == "market-regime":
        market_regime(_active_scalp_symbols(args.symbols), args.db_path, args.store)
    elif args.command == "market-regime-collect":
        market_regime(_active_scalp_symbols(args.symbols), args.db_path, True)
    elif args.command == "market-context":
        market_context(_active_scalp_symbols(args.symbols), args.db_path, args.store)
    elif args.command == "market-context-collect":
        market_context(_active_scalp_symbols(args.symbols), args.db_path, True)
    elif args.command == "llm-report":
        llm_report(
            args.db_path,
            args.state_path,
            args.interval_hours,
            args.force,
            args.send_telegram,
            args.fallback,
        )
    elif args.command == "dashboard":
        run_dashboard(args.host, args.port, args.db_path)
    elif args.command == "fee-status":
        fee_status(args.symbols, args.db_path)
    elif args.command == "live-preflight":
        live_preflight(_active_scalp_symbols(args.symbols), args.notional, args.db_path)
    elif args.command == "live-supervisor":
        live_supervisor(_active_scalp_symbols(args.symbols), args.notional, args.db_path)
    elif args.command == "live-supervisor-notify":
        live_supervisor_notify(
            _active_scalp_symbols(args.symbols),
            args.notional,
            args.db_path,
            args.state_path,
            args.force,
        )
    elif args.command == "trade-event-notify":
        trade_event_notify(
            args.db_path,
            args.state_path,
            args.summary_interval_minutes,
            args.event_limit,
            args.force_summary,
            args.no_send,
        )
    elif args.command == "vibe-probe":
        vibe_probe(
            _active_scalp_symbols(args.symbols),
            args.interval,
            args.limit,
            args.notional,
            args.output,
        )
    elif args.command == "vibe-probe-notify":
        vibe_probe_notify(
            _active_scalp_symbols(args.symbols),
            args.interval,
            args.limit,
            args.notional,
            args.output,
            args.state_path,
            args.periodic_minutes,
            args.force,
            args.no_send,
        )
    elif args.command == "telegram-me":
        telegram_me()
    elif args.command == "telegram-send":
        telegram_send(" ".join(args.text), args.chat_id)
    elif args.command == "telegram-updates":
        telegram_updates(args.limit, args.timeout)
    elif args.command == "telegram-poll":
        telegram_poll(args.once, args.timeout, args.interval)
    else:
        parser.error(f"unknown command: {args.command}")


def explain_mdd() -> None:
    start = 1000
    high = 1100
    low = 880
    dd = (high - low) / high
    print(f"Start equity: {start} USDT")
    print(f"Peak equity: {high} USDT")
    print(f"Later low: {low} USDT")
    print(f"Max drawdown from peak: {dd:.2%}")


def run_demo_backtest() -> None:
    klines = list(_demo_klines())
    result = _run_backtest(klines, "BTCUSDT")
    _print_metrics(result.metrics)


def fetch_klines(
    symbol: str,
    interval: str,
    limit: int,
    output: Path | None,
) -> None:
    client = BinanceUSDMClient()
    klines = client.klines(symbol=symbol, interval=interval, limit=limit)
    if output is None:
        for item in klines[-5:]:
            print(
                item.open_time,
                item.open,
                item.high,
                item.low,
                item.close,
                item.volume,
            )
        print(f"Fetched {len(klines)} klines for {symbol}.")
        return

    with output.open("w", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(["open_time", "open", "high", "low", "close", "volume", "close_time"])
        for item in klines:
            writer.writerow(
                [
                    item.open_time,
                    item.open,
                    item.high,
                    item.low,
                    item.close,
                    item.volume,
                    item.close_time,
                ]
            )
    print(f"Wrote {len(klines)} rows to {output}")


def run_backtest_csv(path: Path, symbol: str) -> None:
    klines = _read_klines_csv(path)
    result = _run_backtest(klines, symbol)
    _print_metrics(result.metrics)


def binance_account() -> None:
    client = BinanceUSDMClient()
    print(account_summary_text(client.account_info()))


def scalp_check(symbol: str, depth_limit: int, kline_limit: int) -> None:
    symbol = symbol.upper()
    trading_config = TradingConfig.from_env()
    client = BinanceUSDMClient(config=trading_config)
    commission = None
    try:
        commission = client.commission_rate(symbol)
    except BinanceAPIError:
        commission = None
    funding_rows = client.funding_rate(symbol, limit=1)
    latest_funding = None
    if funding_rows:
        latest_funding = float(funding_rows[-1]["fundingRate"])
    bnb_fee_enabled, bnb_balance = _fee_context(client)
    signal = ScalpSignalEngine().evaluate(
        symbol=symbol,
        book_ticker=client.book_ticker(symbol),
        order_book=client.order_book(symbol, limit=depth_limit),
        klines=client.klines(symbol, interval="1m", limit=kline_limit),
        trading_config=trading_config,
        commission_rate=commission,
        latest_funding_rate=latest_funding,
        bnb_fee_discount_enabled=bnb_fee_enabled,
        bnb_balance=bnb_balance,
    )
    print(signal.to_text())


def scalp_collect(symbols: list[str], log_path: Path, db_path: Path) -> None:
    count = 0
    store = TradingStore(db_path)
    for symbol in symbols:
        signal = _scalp_signal(symbol.upper())
        append_scalp_signal(log_path, signal)
        store.insert_signal(signal)
        count += 1
    print(f"Collected {count} scalp signal(s) into {db_path} and {log_path}")


def scalp_score(symbols: list[str], log_path: Path, db_path: Path) -> None:
    client = BinanceUSDMClient(config=TradingConfig.from_env())
    mids: dict[str, float] = {}
    for symbol in symbols:
        ticker = client.book_ticker(symbol.upper())
        bid = float(ticker["bidPrice"])
        ask = float(ticker["askPrice"])
        mids[symbol.upper()] = (bid + ask) / 2.0
    updated = score_scalp_log(log_path, mids)
    store = TradingStore(db_path)
    store.migrate_csv_signals(log_path)
    updated_db = _score_scalp_store(store, mids)
    print(f"Updated {updated} CSV score field(s), {updated_db} DB score field(s).")


def scalp_report_db_text(
    db_path: Path,
    csv_path: Path,
    symbol: str | None,
    active_symbols: list[str] | None,
) -> str:
    store = TradingStore(db_path)
    store.migrate_csv_signals(csv_path)
    rows = store.list_signals(symbol=symbol, symbols=active_symbols)
    return scalp_report_rows_text(rows, symbol=symbol, symbols=active_symbols)


def migrate_csv_to_db(log_path: Path, db_path: Path) -> None:
    count = TradingStore(db_path).migrate_csv_signals(log_path)
    print(f"Migrated {count} CSV signal row(s) into {db_path}")


def db_summary(db_path: Path) -> None:
    store = TradingStore(db_path)
    counts = store.summary_counts()
    print("SQLite summary")
    for key, value in counts.items():
        print(f"{key}: {value}")


def risk_mode(db_path: Path) -> None:
    print(evaluate_runtime_risk(TradingStore(db_path), TradingConfig.from_env()).to_text())


def maker_once(symbol: str, db_path: Path) -> None:
    symbol = symbol.upper()
    config = TradingConfig.from_env()
    client = BinanceUSDMClient(config=config)
    store = TradingStore(db_path)
    signal = _scalp_signal(symbol)
    signal_id = store.insert_signal(signal)
    result = place_post_only_maker(client, store, signal, config, signal_id=signal_id)
    print(signal.to_text())
    print("")
    print(f"post-only decision: {'allowed' if result.decision.allowed else 'blocked'}")
    print(f"reason: {result.decision.reason}")
    if result.decision.intent is not None:
        print(f"intent: {result.decision.intent}")
    if result.response is not None:
        print(f"response: {result.response}")
    print(f"order_log_id: {result.order_id}")


def scalp_engine_step(symbols: list[str], log_path: Path, db_path: Path) -> None:
    config = TradingConfig.from_env()
    client = BinanceUSDMClient(config=config)
    store = TradingStore(db_path)
    paused = TelegramBotState.load(default_state_path()).paused
    lines: list[str] = []
    for symbol in symbols:
        symbol = symbol.upper()
        active_cycle = store.active_scalp_cycle(symbol)
        if active_cycle is not None:
            ticker = client.book_ticker(symbol)
            bid = float(ticker["bidPrice"])
            ask = float(ticker["askPrice"])
            result = manage_cycle(client, store, active_cycle, config, bid=bid, ask=ask)
            lines.append(f"{symbol}: {result.action} - {result.detail}")
            continue

        if paused:
            lines.append(f"{symbol}: paused - no new entry")
            continue

        signal = _scalp_signal(symbol, client=client, trading_config=config)
        append_scalp_signal(log_path, signal)
        signal_id = store.insert_signal(signal)
        result = start_cycle_from_signal(
            client,
            store,
            signal,
            config,
            signal_id=signal_id,
        )
        lines.append(f"{symbol}: {result.action} - {result.detail}")
    print("\n".join(lines))


def strategy_engine_step(symbols: list[str], log_path: Path, db_path: Path) -> None:
    config = TradingConfig.from_env()
    client = BinanceUSDMClient(config=config)
    store = TradingStore(db_path)
    paused = TelegramBotState.load(default_state_path()).paused
    lines: list[str] = []

    for cycle in store.active_strategy_cycles():
        ticker = client.book_ticker(str(cycle["symbol"]))
        result = manage_strategy_cycle(
            client,
            store,
            cycle,
            config,
            bid=float(ticker["bidPrice"]),
            ask=float(ticker["askPrice"]),
        )
        lines.append(f"{cycle['strategy']} {cycle['symbol']}: {result.action} - {result.detail}")

    active_symbols = store.active_cycle_symbols()
    for symbol in symbols:
        symbol = symbol.upper()
        if symbol in active_symbols:
            lines.append(f"{symbol}: cycle already active for symbol")
            continue
        if paused:
            lines.append(f"{symbol}: paused - no strategy entry")
            continue

        ticker = client.book_ticker(symbol)
        bid = float(ticker["bidPrice"])
        ask = float(ticker["askPrice"])
        signal = _scalp_signal(symbol, client=client, trading_config=config)
        append_scalp_signal(log_path, signal)
        store.insert_signal(signal)
        klines_5m = client.klines(symbol=symbol, interval="5m", limit=120)
        klines_15m = client.klines(symbol=symbol, interval="15m", limit=120)
        try:
            macro_row = evaluate_market_regime(
                symbol=symbol,
                klines_15m=klines_15m,
                klines_1h=client.klines(symbol=symbol, interval="1h", limit=120),
            )
            store.insert_market_regime(macro_row)
        except BinanceAPIError:
            macro_row = store.latest_market_regime(symbol)
        risk = evaluate_runtime_risk(store, config, symbol=symbol)
        setups = evaluate_strategy_setups(
            scalp_signal=signal,
            macro_row=macro_row,
            runtime_risk=risk,
            macro_max_age_ms=config.macro_regime_max_age_minutes * 60_000,
            klines_5m=klines_5m,
            klines_15m=klines_15m,
        )
        candidates = [
            setup
            for setup in setups
            if setup.strategy != "maker_scalp" and setup.live_supported and setup.status == "PASS"
        ]
        if not candidates:
            lines.append(f"{symbol}: no strategy PASS candidate")
            continue
        result = start_strategy_cycle_from_setup(
            client,
            store,
            candidates[0],
            config,
            symbol=symbol,
            bid=bid,
            ask=ask,
        )
        lines.append(f"{symbol}: {result.strategy} {result.action} - {result.detail}")
    print("\n".join(lines))


def strategy_evaluate(db_path: Path, limit: int) -> None:
    store = TradingStore(db_path)
    rows = evaluate_and_store_strategy(store, TradingConfig.from_env())
    print(strategy_evaluation_text(rows, limit=limit))
    print(f"stored {len(rows)} strategy evaluation row(s) into {db_path}")


def strategy_notify(
    db_path: Path,
    state_path: Path,
    periodic_minutes: int | None,
    limit: int,
    force: bool,
) -> None:
    config = TradingConfig.from_env()
    store = TradingStore(db_path)
    rows = store.latest_strategy_batch()
    if not rows:
        evaluate_and_store_strategy(store, config)
        rows = store.latest_strategy_batch()
    state = StrategyNotifyState.load(state_path)
    should_send, reason, signature = strategy_notification_decision(
        rows,
        state,
        periodic_minutes=periodic_minutes
        if periodic_minutes is not None
        else config.strategy_notify_interval_minutes,
        force=force,
    )
    text = strategy_notification_text(
        rows,
        reason=reason,
        limit=limit,
        config=config,
        active_strategy_cycles=store.active_strategy_cycles(),
    )
    print(text)
    if not should_send:
        print("notification: skipped")
        return

    telegram_config = TelegramConfig.from_env()
    try:
        TelegramClient(telegram_config).send_message(text)
    except Exception as exc:
        print(f"notification: failed - {exc}")
        return
    state = apply_strategy_notification_state(state, signature=signature, reason=reason)
    state.save(state_path)
    print("notification: sent")


def market_regime(symbols: list[str], db_path: Path, store_rows: bool) -> None:
    config = TradingConfig.from_env()
    client = BinanceUSDMClient(config=config)
    store = TradingStore(db_path)
    snapshots = []
    for symbol in symbols:
        try:
            snapshot = _market_regime_snapshot(symbol.upper(), client)
        except BinanceAPIError as exc:
            print(f"{symbol.upper()}: market regime fetch failed - {exc}")
            continue
        snapshots.append(snapshot)
        if store_rows:
            store.insert_market_regime(snapshot)
    if snapshots:
        print("\n\n".join(snapshot.to_text() for snapshot in snapshots))
    else:
        print("No market regime snapshots collected.")
    if store_rows:
        print(f"\nstored {len(snapshots)} market regime row(s) into {db_path}")


def market_context(symbols: list[str], db_path: Path, store_rows: bool) -> None:
    config = TradingConfig.from_env()
    client = BinanceUSDMClient(config=config)
    store = TradingStore(db_path)
    snapshots = []
    for symbol in symbols:
        try:
            snapshot = collect_market_context(client, symbol.upper())
        except BinanceAPIError as exc:
            print(f"{symbol.upper()}: market context fetch failed - {exc}")
            continue
        snapshots.append(snapshot)
        if store_rows:
            store.insert_market_context(snapshot)
    if snapshots:
        print("\n\n".join(snapshot.to_text() for snapshot in snapshots))
    else:
        rows = store.latest_market_contexts(symbols=symbols)
        print(market_context_rows_text(rows))
    if store_rows:
        print(f"\nstored {len(snapshots)} market context row(s) into {db_path}")


def live_supervisor(symbols: list[str], notional: float, db_path: Path) -> None:
    config = TradingConfig.from_env()
    client = BinanceUSDMClient(config=config)
    store = TradingStore(db_path)
    warnings = refresh_supervisor_inputs(client, store, symbols)
    reports = supervise_symbols(
        client,
        store,
        config,
        symbols,
        notional=notional,
    )
    if warnings:
        print("수집 경고")
        for warning in warnings:
            print(f"- {warning}")
        print("")
    print(supervisor_report_text(reports))


def live_supervisor_notify(
    symbols: list[str],
    notional: float | None,
    db_path: Path,
    state_path: Path,
    force: bool,
) -> None:
    config = TradingConfig.from_env()
    notional = notional if notional is not None else config.live_one_shot_notional
    client = BinanceUSDMClient(config=config)
    store = TradingStore(db_path)
    warnings = refresh_supervisor_inputs(client, store, symbols)
    reports = supervise_symbols(
        client,
        store,
        config,
        symbols,
        notional=notional,
    )
    state = LiveSupervisorNotifyState.load(state_path)
    should_send, reason, signature, actionable = supervisor_candidate_notification_decision(
        reports,
        state,
        force=force,
    )
    text = supervisor_candidate_notification_text(actionable, reason=reason, notional=notional)
    if warnings:
        text = "\n".join(["수집 경고", *[f"- {warning}" for warning in warnings], "", text])
    print(text)
    if not should_send:
        print("live-supervisor-notify: skipped")
        return
    try:
        TelegramClient(TelegramConfig.from_env()).send_message(_telegram_safe_text(text))
    except Exception as exc:
        print(f"live-supervisor-notify: failed - {exc}")
        return
    apply_live_supervisor_notify_state(state, signature=signature).save(state_path)
    print("live-supervisor-notify: sent")


def trade_event_notify(
    db_path: Path,
    state_path: Path,
    summary_interval_minutes: int,
    event_limit: int,
    force_summary: bool,
    no_send: bool,
) -> None:
    store = TradingStore(db_path)
    state = TradeEventNotifyState.load(state_path)
    events, include_summary = trade_event_notification_decision(
        store,
        state,
        summary_interval_minutes=summary_interval_minutes,
        force_summary=force_summary,
    )
    text = trade_event_notification_text(
        events,
        store,
        include_summary=include_summary,
        event_limit=event_limit,
    )
    print(text)
    if not events and not include_summary:
        print("trade-event-notify: skipped")
        return
    if no_send:
        print("trade-event-notify: print only")
        return
    try:
        TelegramClient(TelegramConfig.from_env()).send_message(_telegram_safe_text(text))
    except Exception as exc:
        print(f"trade-event-notify: failed - {exc}")
        return
    apply_trade_event_notification_state(
        state,
        events,
        summary_sent=include_summary,
    ).save(state_path)
    print("trade-event-notify: sent")


def vibe_probe(
    symbols: list[str],
    interval: str,
    limit: int,
    notional: float | None,
    output: Path,
) -> None:
    config = TradingConfig.from_env()
    market_config = replace(config, testnet=False)
    client = BinanceUSDMClient(config=market_config)
    results, trades = run_vibe_style_probe(
        symbols=symbols,
        interval=interval,
        limit=limit,
        notional=notional,
        config=config,
        client=client,
    )
    write_probe_report(
        output,
        results=results,
        trades=trades,
        symbols=symbols,
        interval=interval,
        limit=limit,
    )
    print(vibe_probe_text(results))
    print(f"\nreport: {output}")


def vibe_probe_notify(
    symbols: list[str],
    interval: str,
    limit: int,
    notional: float | None,
    output: Path,
    state_path: Path,
    periodic_minutes: int,
    force: bool,
    no_send: bool,
) -> None:
    config = TradingConfig.from_env()
    market_config = replace(config, testnet=False)
    client = BinanceUSDMClient(config=market_config)
    results, trades = run_vibe_style_probe(
        symbols=symbols,
        interval=interval,
        limit=limit,
        notional=notional,
        config=config,
        client=client,
    )
    write_probe_report(
        output,
        results=results,
        trades=trades,
        symbols=symbols,
        interval=interval,
        limit=limit,
    )
    state = ProbeNotifyState.load(state_path)
    should_send, reason, signature = probe_notification_decision(
        results,
        state,
        periodic_minutes=periodic_minutes,
        force=force,
    )
    text = "\n".join(
        [
            "자동 리서치 프로브",
            f"사유: {reason}",
            vibe_probe_text(results, limit=8),
        ]
    )
    print(text)
    if not should_send:
        print("vibe-probe-notify: skipped")
        return
    if no_send:
        print("vibe-probe-notify: print only")
        apply_probe_notification_state(state, signature=signature).save(state_path)
        return
    try:
        TelegramClient(TelegramConfig.from_env()).send_message(_telegram_safe_text(text))
    except Exception as exc:
        print(f"vibe-probe-notify: failed - {exc}")
        return
    apply_probe_notification_state(state, signature=signature).save(state_path)
    print("vibe-probe-notify: sent")


def llm_report(
    db_path: Path,
    state_path: Path,
    interval_hours: int,
    force: bool,
    send_telegram: bool,
    use_fallback: bool,
) -> None:
    config = TradingConfig.from_env()
    state = LLMReportState.load(state_path)
    if not llm_report_due(state, interval_hours=interval_hours, force=force):
        print("llm-report: skipped, interval not due")
        return

    context = build_report_context(TradingStore(db_path), config)
    text = ""
    if use_fallback:
        text = fallback_report_text(context)
    elif not config.llm_enabled:
        print("llm-report: skipped, LLM disabled")
        return
    elif config.llm_provider.lower() != "gemini":
        print(f"llm-report: skipped, unsupported provider {config.llm_provider}")
        return
    elif not config.llm_api_key:
        print("llm-report: skipped, Gemini API key missing")
        return
    else:
        prompt = build_report_prompt(context)
        text = GeminiReportClient(config.llm_api_key, config.llm_model).generate(prompt)

    text = _telegram_safe_text(text)
    print(text)
    if send_telegram:
        TelegramClient(TelegramConfig.from_env()).send_message(text)
        state.last_sent_ms = int(time.time() * 1000)
        state.save(state_path)
        print("llm-report: sent")


def _score_scalp_store(store: TradingStore, current_mid_by_symbol: dict[str, float]) -> int:
    current_ms = int(time.time() * 1000)
    updated = 0
    horizons = {
        "horizon_1m_bps": 60_000,
        "horizon_3m_bps": 180_000,
        "horizon_5m_bps": 300_000,
    }
    for row in store.pending_score_rows(current_ms):
        symbol = row["symbol"]
        current_mid = current_mid_by_symbol.get(symbol)
        if current_mid is None:
            continue
        signed_return_bps = _signed_return_bps(row["side"], row["mid_price"], current_mid)
        scores: dict[str, float] = {}
        for field, horizon_ms in horizons.items():
            if row[field] is None and current_ms - int(row["timestamp_ms"]) >= horizon_ms:
                scores[field] = signed_return_bps
        if scores:
            store.update_signal_scores(int(row["id"]), scores)
            updated += len(scores)
    return updated


def _signed_return_bps(side: str, entry_mid: float, current_mid: float) -> float:
    if entry_mid <= 0:
        return 0.0
    raw = ((current_mid / entry_mid) - 1.0) * 10_000.0
    if side == "long":
        return raw
    if side == "short":
        return -raw
    return 0.0


def _active_scalp_symbols(symbols: list[str] | None) -> list[str]:
    if symbols:
        return [symbol.upper() for symbol in symbols]
    return list(TradingConfig.from_env().scalp_symbols)


def _scalp_signal(
    symbol: str,
    *,
    client: BinanceUSDMClient | None = None,
    trading_config: TradingConfig | None = None,
):
    trading_config = trading_config or TradingConfig.from_env()
    client = client or BinanceUSDMClient(config=trading_config)
    commission = None
    try:
        commission = client.commission_rate(symbol)
    except BinanceAPIError:
        commission = None
    funding_rows = client.funding_rate(symbol, limit=1)
    latest_funding = None
    if funding_rows:
        latest_funding = float(funding_rows[-1]["fundingRate"])
    bnb_fee_enabled, bnb_balance = _fee_context(client)
    return ScalpSignalEngine().evaluate(
        symbol=symbol,
        book_ticker=client.book_ticker(symbol),
        order_book=client.order_book(symbol, limit=20),
        klines=client.klines(symbol, interval="1m", limit=30),
        trading_config=trading_config,
        commission_rate=commission,
        latest_funding_rate=latest_funding,
        bnb_fee_discount_enabled=bnb_fee_enabled,
        bnb_balance=bnb_balance,
    )


def _market_regime_snapshot(symbol: str, client: BinanceUSDMClient):
    return evaluate_market_regime(
        symbol=symbol,
        klines_15m=client.klines(symbol=symbol, interval="15m", limit=120),
        klines_1h=client.klines(symbol=symbol, interval="1h", limit=120),
    )


def _telegram_safe_text(text: str, limit: int = 3500) -> str:
    clean = text.strip()
    if len(clean) <= limit:
        return clean
    return clean[: limit - 20].rstrip() + "\n...(truncated)"


def fee_status(symbols: list[str], db_path: Path | None = None) -> None:
    client = BinanceUSDMClient(config=TradingConfig.from_env())
    bnb_fee_enabled, bnb_balance = _fee_context(client)
    bnb_fee_active = bnb_fee_enabled and bnb_balance > 0
    usdc_balance = _asset_balance(client, "USDC")
    try:
        multi_assets = bool(client.multi_assets_margin().get("multiAssetsMargin"))
    except BinanceAPIError:
        multi_assets = False
    print("선물 수수료 상태")
    print(f"BNB 수수료 할인 설정: {'켜짐' if bnb_fee_enabled else '꺼짐'}")
    print(f"선물 지갑 BNB 잔고: {bnb_balance:.8f} BNB")
    print(
        "실제 할인 적용: "
        f"{'가능' if bnb_fee_active else '불가'}"
    )
    print(f"Multi-Assets Mode: {'켜짐' if multi_assets else '꺼짐'}")
    print(f"선물 지갑 USDC 잔고: {usdc_balance:.8f} USDC")
    print(
        "USDC 심볼 live 준비: "
        f"{'가능' if multi_assets or usdc_balance > 0 else '불가'}"
    )
    for symbol in symbols:
        try:
            commission = client.commission_rate(symbol.upper())
        except BinanceAPIError as exc:
            print(f"{symbol.upper()}: 수수료 조회 실패 ({exc})")
            continue
        maker = float(commission["makerCommissionRate"]) * 10_000.0
        taker = float(commission["takerCommissionRate"]) * 10_000.0
        if bnb_fee_active:
            maker *= 0.90
            taker *= 0.90
        if db_path is not None:
            TradingStore(db_path).record_fee_snapshot(
                symbol.upper(),
                maker,
                taker,
                bnb_fee_discount_enabled=bnb_fee_enabled,
                bnb_fee_discount_active=bnb_fee_active,
                raw=commission,
            )
        print(f"{symbol.upper()}: maker {maker:.2f}bps, taker {taker:.2f}bps")


def live_preflight(symbols: list[str], notional: float, db_path: Path) -> None:
    config = TradingConfig.from_env()
    client = BinanceUSDMClient(config=config)
    store = TradingStore(db_path)
    risk = evaluate_runtime_risk(store, config)
    usdc_balance = _asset_balance(client, config.equity_asset)
    bnb_fee_enabled, bnb_balance = _fee_context(client)

    print("live preflight")
    print(f"dry_run={config.dry_run}")
    print(f"live_trading_enabled={config.live_trading_enabled}")
    print(f"live_scalp_lifecycle_enabled={config.live_scalp_lifecycle_enabled}")
    print(f"requested_notional={notional:.2f} {config.equity_asset}")
    print(f"max_single_order_notional={config.max_single_order_notional:.2f}")
    print(f"{config.equity_asset}_available={usdc_balance:.8f}")
    print(f"bnb_fee_discount_possible={bnb_fee_enabled and bnb_balance > 0}")
    print(f"runtime_risk={risk.mode} allows_new_entries={risk.allows_new_entries}")
    if not risk.allows_new_entries:
        print("preflight_block=runtime risk blocks new entries")
    if notional > config.max_single_order_notional:
        print("preflight_block=requested notional exceeds max single order cap")
    print("")

    for symbol in symbols:
        symbol = symbol.upper()
        try:
            ticker = client.book_ticker(symbol)
            bid = float(ticker["bidPrice"])
            ask = float(ticker["askPrice"])
            mid = (bid + ask) / 2.0
            filters = SymbolFilters.from_exchange_info(client.exchange_info(symbol), symbol)
            signal = _scalp_signal(symbol, client=client, trading_config=config)
            klines_5m = client.klines(symbol=symbol, interval="5m", limit=120)
            klines_15m = client.klines(symbol=symbol, interval="15m", limit=120)
            try:
                macro_row = evaluate_market_regime(
                    symbol=symbol,
                    klines_15m=klines_15m,
                    klines_1h=client.klines(symbol=symbol, interval="1h", limit=120),
                )
                store.insert_market_regime(macro_row)
            except BinanceAPIError:
                macro_row = store.latest_market_regime(symbol)
            setups = evaluate_strategy_setups(
                scalp_signal=signal,
                macro_row=macro_row,
                runtime_risk=risk,
                macro_max_age_ms=config.macro_regime_max_age_minutes * 60_000,
                klines_5m=klines_5m,
                klines_15m=klines_15m,
            )
            decision = build_post_only_intent(signal, config, notional=notional)
            normalized = None
            filter_reason = decision.reason
            if decision.intent is not None:
                normalized, filter_reason = filters.normalize_intent(decision.intent)
            min_notional = filters.min_order_notional_at(mid)
        except BinanceAPIError as exc:
            print(f"{symbol}: BLOCK exchange error: {exc}")
            continue
        except ValueError as exc:
            print(f"{symbol}: BLOCK filter error: {exc}")
            continue

        print(f"{symbol}")
        print(f"  mid={mid:.8f} min_order_notional≈{float(min_notional):.4f}")
        print(
            "  filters="
            f"tick={filters.tick_size} step={filters.step_size} "
            f"minQty={filters.min_qty} minNotional={filters.min_notional}"
        )
        print(f"  signal={signal.side} regime={signal.regime} reason={signal.reason}")
        print(
            "\n".join(
                f"  {line}"
                for line in strategy_setups_text(
                    setups,
                    symbol=symbol,
                    notional=notional,
                    runtime_risk=risk,
                ).splitlines()
            )
        )
        if normalized is None:
            print(f"  maker_scalp_decision=BLOCK {filter_reason}")
            continue
        print(
            "  maker_scalp_decision=OK "
            f"side={normalized.side} qty={normalized.quantity:.12f} "
            f"price={normalized.price:.12f} notional≈{normalized.quantity * (normalized.price or mid):.4f}"
        )


def _fee_context(client: BinanceUSDMClient) -> tuple[bool, float]:
    try:
        bnb_fee_enabled = bool(client.fee_burn_status().get("feeBurn"))
    except BinanceAPIError:
        bnb_fee_enabled = False
    try:
        balances = client.account_balance()
    except BinanceAPIError:
        return bnb_fee_enabled, 0.0
    for row in balances:
        if row.get("asset") == "BNB":
            return bnb_fee_enabled, float(row.get("availableBalance") or row.get("balance") or 0)
    return bnb_fee_enabled, 0.0


def _asset_balance(client: BinanceUSDMClient, asset: str) -> float:
    try:
        balances = client.account_balance()
    except BinanceAPIError:
        return 0.0
    for row in balances:
        if row.get("asset") == asset:
            return float(row.get("availableBalance") or row.get("balance") or 0)
    return 0.0


def telegram_me() -> None:
    client = TelegramClient(TelegramConfig.from_env())
    result = client.get_me()["result"]
    username = result.get("username", "")
    bot_id = result.get("id", "")
    print(f"Telegram bot: @{username} ({bot_id})")


def telegram_send(text: str, chat_id: str | None) -> None:
    client = TelegramClient(TelegramConfig.from_env())
    result = client.send_message(text, chat_id=chat_id)["result"]
    print(f"Sent Telegram message_id={result.get('message_id')}")


def telegram_updates(limit: int, timeout: int) -> None:
    client = TelegramClient(TelegramConfig.from_env())
    updates = client.get_updates(limit=limit, timeout=timeout)
    if not updates:
        print("No Telegram updates.")
        return
    for update in updates:
        message = update.get("message") or update.get("edited_message") or {}
        chat = message.get("chat") or {}
        sender = message.get("from") or {}
        text = (message.get("text") or "").replace("\n", " ")[:80]
        print(
            "update_id={update_id} chat_id={chat_id} from=@{username} text={text}".format(
                update_id=update.get("update_id"),
                chat_id=chat.get("id"),
                username=sender.get("username", ""),
                text=text,
            )
        )


def telegram_poll(once: bool, timeout: int, interval: float) -> None:
    telegram_config = TelegramConfig.from_env()
    trading_config = TradingConfig.from_env()
    client = TelegramClient(telegram_config)
    state_path = default_state_path()
    state = TelegramBotState.load(state_path)
    processor = TelegramCommandProcessor(telegram_config, trading_config, state)
    if once:
        handled = poll_once(client, processor, state, state_path, timeout=timeout)
        print(f"Handled {handled} Telegram command(s).")
        return
    print("Polling Telegram commands. Press Ctrl+C to stop.")
    poll_forever(client, processor, state, state_path, interval_seconds=interval)


def _run_backtest(klines: list[Kline], symbol: str):
    config = TradingConfig.from_env()
    strategy = MovingAverageCrossStrategy(symbol=symbol)
    backtester = Backtester(config=config, strategy=strategy)
    return backtester.run(klines)


def _print_metrics(metrics) -> None:
    print(f"Final equity: {metrics.final_equity:.2f} USDT")
    print(f"Total return: {metrics.total_return_pct:.2%}")
    print(f"Max drawdown: {metrics.max_drawdown_pct:.2%}")
    print(f"Trades: {metrics.trade_count}")
    print(f"Win rate: {metrics.win_rate_pct:.2%}")
    print(f"Fees: {metrics.total_fees:.4f} USDT")


def _demo_klines() -> Iterable[Kline]:
    price = 50_000.0
    for index in range(180):
        drift = 80.0 if index < 90 else -90.0
        wave = math.sin(index / 5.0) * 120.0
        close = price + drift + wave
        high = max(price, close) * 1.002
        low = min(price, close) * 0.998
        yield Kline(
            open_time=index * 3_600_000,
            open=price,
            high=high,
            low=low,
            close=close,
            volume=100.0 + index,
            close_time=((index + 1) * 3_600_000) - 1,
        )
        price = close


def _read_klines_csv(path: Path) -> list[Kline]:
    rows: list[Kline] = []
    with path.open() as file:
        reader = csv.DictReader(file)
        for row in reader:
            rows.append(
                Kline(
                    open_time=int(row["open_time"]),
                    open=float(row["open"]),
                    high=float(row["high"]),
                    low=float(row["low"]),
                    close=float(row["close"]),
                    volume=float(row["volume"]),
                    close_time=int(row["close_time"]),
                )
            )
    return rows


if __name__ == "__main__":
    main()
