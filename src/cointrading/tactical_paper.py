from __future__ import annotations

from dataclasses import dataclass, replace
from typing import Iterable

from cointrading.config import TradingConfig
from cointrading.execution import dry_run_order_response
from cointrading.exchange.binance_usdm import BinanceAPIError, BinanceUSDMClient
from cointrading.exchange_filters import SymbolFilters
from cointrading.live_guard import consume_live_one_shot, validate_live_one_shot
from cointrading.models import OrderIntent
from cointrading.storage import TradingStore, now_ms
from cointrading.strategy_lifecycle import manage_strategy_cycle
from cointrading.tactical_radar import (
    RADAR_READY,
    TacticalRadarSignal,
    evaluate_tactical_radar,
)
from cointrading.risk_state import evaluate_runtime_risk, risk_mode_ko


TACTICAL_STRATEGY_PREFIX = "tactical_"


@dataclass(frozen=True)
class TacticalPaperResult:
    symbol: str
    action: str
    detail: str
    cycle_id: int | None = None


def run_tactical_paper_step(
    client: BinanceUSDMClient,
    store: TradingStore,
    config: TradingConfig,
    *,
    symbols: Iterable[str] | None = None,
    notional: float | None = None,
    timestamp_ms: int | None = None,
) -> tuple[list[TacticalPaperResult], tuple[str, ...]]:
    ts = timestamp_ms or now_ms()
    paper_config = replace(config, dry_run=True)
    results: list[TacticalPaperResult] = []
    warnings: list[str] = []

    for cycle in store.active_strategy_cycles():
        if not _is_paper_tactical_cycle(cycle):
            continue
        symbol = str(cycle["symbol"]).upper()
        try:
            ticker = client.book_ticker(symbol)
            managed = manage_strategy_cycle(
                client,
                store,
                cycle,
                paper_config,
                bid=float(ticker["bidPrice"]),
                ask=float(ticker["askPrice"]),
                timestamp_ms=ts,
            )
        except (BinanceAPIError, KeyError, ValueError) as exc:
            warnings.append(f"{symbol} 전술 paper 상태 갱신 실패: {exc}")
            continue
        results.append(
            TacticalPaperResult(
                symbol=symbol,
                action=managed.action,
                detail=f"{cycle['strategy']}: {managed.detail}",
                cycle_id=managed.cycle_id,
            )
        )

    signals, radar_warnings = evaluate_tactical_radar(client, config=paper_config, symbols=symbols)
    warnings.extend(radar_warnings)
    active_symbols = store.active_cycle_symbols()
    order_notional = min(
        float(notional if notional is not None else paper_config.strategy_order_notional),
        paper_config.max_single_order_notional,
    )

    for signal in signals:
        symbol = signal.symbol.upper()
        if signal.decision != RADAR_READY:
            continue
        if symbol in active_symbols:
            results.append(TacticalPaperResult(symbol, "skip", "이미 해당 심볼 상태머신이 열려 있음"))
            continue
        try:
            ticker = client.book_ticker(symbol)
            result = start_tactical_paper_cycle_from_signal(
                store,
                signal,
                paper_config,
                bid=float(ticker["bidPrice"]),
                ask=float(ticker["askPrice"]),
                notional=order_notional,
                timestamp_ms=ts,
            )
        except (BinanceAPIError, KeyError, ValueError) as exc:
            warnings.append(f"{symbol} 전술 paper 시작 실패: {exc}")
            continue
        results.append(result)
        if result.cycle_id is not None:
            active_symbols.add(symbol)

    if not results:
        results.append(TacticalPaperResult("-", "wait", "전술후보 READY 없음. 근접/감시는 기록만 하고 진입 paper는 만들지 않음."))
    return results, tuple(warnings)


def run_tactical_live_step(
    client: BinanceUSDMClient,
    store: TradingStore,
    config: TradingConfig,
    *,
    symbols: Iterable[str] | None = None,
    notional: float | None = None,
    timestamp_ms: int | None = None,
) -> tuple[list[TacticalPaperResult], tuple[str, ...]]:
    ts = timestamp_ms or now_ms()
    results: list[TacticalPaperResult] = []
    warnings: list[str] = []

    for cycle in store.active_strategy_cycles():
        if not _is_live_tactical_cycle(cycle):
            continue
        symbol = str(cycle["symbol"]).upper()
        try:
            ticker = client.book_ticker(symbol)
            managed = manage_strategy_cycle(
                client,
                store,
                cycle,
                config,
                bid=float(ticker["bidPrice"]),
                ask=float(ticker["askPrice"]),
                timestamp_ms=ts,
            )
        except (BinanceAPIError, KeyError, ValueError) as exc:
            warnings.append(f"{symbol} 전술 live 상태 갱신 실패: {exc}")
            continue
        results.append(
            TacticalPaperResult(
                symbol=symbol,
                action=managed.action,
                detail=f"{cycle['strategy']}: {managed.detail}",
                cycle_id=managed.cycle_id,
            )
        )

    if _has_active_live_tactical_cycle(store):
        return results or [TacticalPaperResult("-", "wait", "전술 live 사이클이 이미 진행 중")], tuple(warnings)

    signals, radar_warnings = evaluate_tactical_radar(client, config=config, symbols=symbols)
    warnings.extend(radar_warnings)
    order_notional = min(
        float(notional if notional is not None else config.live_one_shot_notional),
        config.max_single_order_notional,
    )

    for signal in signals:
        if signal.decision != RADAR_READY:
            continue
        result = start_tactical_live_cycle_from_signal(
            client,
            store,
            signal,
            config,
            notional=order_notional,
            timestamp_ms=ts,
        )
        results.append(result)
        if result.cycle_id is not None:
            break

    if not results:
        results.append(TacticalPaperResult("-", "wait", "전술후보 READY 없음. live 진입 없음."))
    return results, tuple(warnings)


def start_tactical_paper_cycle_from_signal(
    store: TradingStore,
    signal: TacticalRadarSignal,
    config: TradingConfig,
    *,
    bid: float,
    ask: float,
    notional: float,
    timestamp_ms: int | None = None,
) -> TacticalPaperResult:
    ts = timestamp_ms or now_ms()
    symbol = signal.symbol.upper()
    side = signal.side
    if signal.decision != RADAR_READY:
        return TacticalPaperResult(symbol, "skip", "READY 전술후보가 아니라 paper 진입하지 않음")
    if side not in {"long", "short"}:
        return TacticalPaperResult(symbol, "blocked", f"전술 방향이 진입 방향이 아님: {side}")
    if signal.stop_price is None or signal.target_price is None:
        return TacticalPaperResult(symbol, "blocked", "목표/손절 가격이 없어 paper 진입 불가")

    entry_price = ask if side == "long" else bid
    if entry_price <= 0:
        return TacticalPaperResult(symbol, "blocked", "현재 호가가 유효하지 않음")
    exit_bps = _exit_bps(side, entry_price, signal.target_price, signal.stop_price)
    if exit_bps is None:
        return TacticalPaperResult(symbol, "blocked", "목표/손절이 진입 방향과 맞지 않음")
    take_profit_bps, stop_loss_bps = exit_bps
    roundtrip_fee_bps = config.taker_fee_rate * 20_000.0
    if take_profit_bps <= roundtrip_fee_bps + 2.0:
        return TacticalPaperResult(
            symbol,
            "blocked",
            f"목표폭 {take_profit_bps:.1f}bps가 왕복 테이커 비용 {roundtrip_fee_bps:.1f}bps 대비 너무 작음",
        )

    quantity = float(notional) / entry_price
    order_intent = OrderIntent(
        symbol=symbol,
        side="BUY" if side == "long" else "SELL",
        quantity=quantity,
        order_type="MARKET",
        reduce_only=False,
        client_order_id=f"cttp{symbol.lower()}{ts}"[:36],
    )
    order_id = store.insert_order_attempt(
        order_intent,
        status="DRY_RUN",
        dry_run=True,
        reason=f"tactical paper entry; {signal.scenario}; {signal.reason}",
        response=dry_run_order_response(order_intent),
        timestamp_ms=ts,
    )
    cycle_id = store.insert_strategy_cycle(
        strategy=_strategy_name(signal),
        execution_mode="paper_tactical",
        symbol=symbol,
        side=side,
        status="ENTRY_SUBMITTED",
        reason=f"tactical paper entry submitted; {signal.reason}",
        entry_order_id=order_id,
        quantity=quantity,
        entry_price=entry_price,
        target_price=signal.target_price,
        stop_price=signal.stop_price,
        entry_order_type="MARKET",
        take_profit_bps=take_profit_bps,
        stop_loss_bps=stop_loss_bps,
        max_hold_seconds=_max_hold_seconds(signal),
        maker_one_way_bps=config.maker_fee_rate * 10_000.0,
        taker_one_way_bps=config.taker_fee_rate * 10_000.0,
        entry_deadline_ms=ts,
        dry_run=True,
        last_mid_price=(bid + ask) / 2.0,
        setup={
            "source": "tactical_radar",
            "scenario": signal.scenario,
            "confidence": signal.confidence,
            "reason": signal.reason,
            "detail": signal.detail,
            "current_price": signal.current_price,
            "trigger_price": signal.trigger_price,
            "radar_timestamp_ms": signal.timestamp_ms,
        },
        timestamp_ms=ts,
    )
    return TacticalPaperResult(
        symbol,
        "entry_submitted",
        (
            f"{_strategy_name(signal)} {side} paper 시작 "
            f"entry={entry_price:.8g} TP={take_profit_bps:.1f}bps "
            f"SL={stop_loss_bps:.1f}bps H={_max_hold_seconds(signal)}s"
        ),
        cycle_id,
    )


def start_tactical_live_cycle_from_signal(
    client: BinanceUSDMClient,
    store: TradingStore,
    signal: TacticalRadarSignal,
    config: TradingConfig,
    *,
    notional: float,
    timestamp_ms: int | None = None,
) -> TacticalPaperResult:
    ts = timestamp_ms or now_ms()
    symbol = signal.symbol.upper()
    strategy = _strategy_name(signal)
    if config.dry_run:
        return TacticalPaperResult(symbol, "blocked", "dry-run이 켜져 있어 live 전술 진입 불가")
    if not config.live_trading_enabled:
        return TacticalPaperResult(symbol, "blocked", "live trading 플래그가 꺼져 있음")
    if not config.live_strategy_lifecycle_enabled:
        return TacticalPaperResult(symbol, "blocked", "live 전략 상태머신 플래그가 꺼져 있음")
    if signal.decision != RADAR_READY:
        return TacticalPaperResult(symbol, "skip", "READY 전술후보가 아니라 live 진입하지 않음")
    if signal.side not in {"long", "short"}:
        return TacticalPaperResult(symbol, "blocked", f"전술 방향이 진입 방향이 아님: {signal.side}")
    if signal.stop_price is None or signal.target_price is None:
        return TacticalPaperResult(symbol, "blocked", "목표/손절 가격이 없어 live 진입 불가")

    risk = evaluate_runtime_risk(store, config, symbol=symbol, current_ms=ts)
    if not risk.allows_new_entries:
        return TacticalPaperResult(
            symbol,
            "blocked",
            f"런타임 위험모드 {risk_mode_ko(risk.mode)}: {risk.reasons[0] if risk.reasons else '신규 진입 차단'}",
        )
    if symbol in _active_live_symbols(store):
        return TacticalPaperResult(symbol, "skip", "해당 심볼 live 상태머신이 이미 열려 있음")
    actual_orders, actual_positions, actual_warning = _actual_exchange_symbols(client)
    if actual_warning:
        return TacticalPaperResult(symbol, "blocked", actual_warning)
    if symbol in actual_orders:
        return TacticalPaperResult(symbol, "blocked", "바이낸스 실제 열린 주문이 이미 있음")
    if symbol in actual_positions:
        return TacticalPaperResult(symbol, "blocked", "바이낸스 실제 포지션이 이미 있음")

    ticker = client.book_ticker(symbol)
    bid = float(ticker["bidPrice"])
    ask = float(ticker["askPrice"])
    entry_price = ask if signal.side == "long" else bid
    if entry_price <= 0:
        return TacticalPaperResult(symbol, "blocked", "현재 호가가 유효하지 않음")
    exit_bps = _exit_bps(signal.side, entry_price, signal.target_price, signal.stop_price)
    if exit_bps is None:
        return TacticalPaperResult(symbol, "blocked", "목표/손절이 진입 방향과 맞지 않음")
    take_profit_bps, stop_loss_bps = exit_bps
    roundtrip_fee_bps = config.taker_fee_rate * 20_000.0
    if take_profit_bps <= roundtrip_fee_bps + 2.0:
        return TacticalPaperResult(
            symbol,
            "blocked",
            f"목표폭 {take_profit_bps:.1f}bps가 왕복 테이커 비용 {roundtrip_fee_bps:.1f}bps 대비 너무 작음",
        )

    guard = validate_live_one_shot(
        config,
        symbol=symbol,
        strategy=strategy,
        notional=notional,
    )
    if not guard.allowed:
        return TacticalPaperResult(symbol, "blocked", guard.reason)

    intent = OrderIntent(
        symbol=symbol,
        side="BUY" if signal.side == "long" else "SELL",
        quantity=float(notional) / entry_price,
        order_type="MARKET",
        reduce_only=False,
        client_order_id=f"cttl{symbol.lower()}{ts}"[:36],
    )
    normalized, filter_reason = _normalize_live_intent(client, intent)
    if normalized is None:
        return TacticalPaperResult(symbol, "blocked", filter_reason)
    response = client.new_order(normalized)
    order_id = store.insert_order_attempt(
        normalized,
        status=str(response.get("status", "SUBMITTED")),
        dry_run=False,
        reason=f"tactical live entry; {signal.scenario}; {signal.reason}; {filter_reason}",
        response=response,
        timestamp_ms=ts,
    )
    cycle_id = store.insert_strategy_cycle(
        strategy=strategy,
        execution_mode="live_tactical",
        symbol=symbol,
        side=signal.side,
        status="ENTRY_SUBMITTED",
        reason=f"tactical live entry submitted; {signal.reason}",
        entry_order_id=order_id,
        quantity=normalized.quantity,
        entry_price=entry_price,
        target_price=signal.target_price,
        stop_price=signal.stop_price,
        entry_order_type="MARKET",
        take_profit_bps=take_profit_bps,
        stop_loss_bps=stop_loss_bps,
        max_hold_seconds=_max_hold_seconds(signal),
        maker_one_way_bps=config.maker_fee_rate * 10_000.0,
        taker_one_way_bps=config.taker_fee_rate * 10_000.0,
        entry_deadline_ms=ts + 60_000,
        dry_run=False,
        last_mid_price=(bid + ask) / 2.0,
        setup={
            "source": "tactical_radar_live",
            "scenario": signal.scenario,
            "confidence": signal.confidence,
            "reason": signal.reason,
            "detail": signal.detail,
            "current_price": signal.current_price,
            "trigger_price": signal.trigger_price,
            "radar_timestamp_ms": signal.timestamp_ms,
        },
        timestamp_ms=ts,
    )
    if config.live_one_shot_required:
        consume_live_one_shot(symbol=symbol, strategy=strategy, notional=notional, cycle_id=cycle_id)
    return TacticalPaperResult(
        symbol,
        "entry_submitted",
        (
            f"{strategy} {signal.side} LIVE 시작 "
            f"entry~{entry_price:.8g} TP={take_profit_bps:.1f}bps "
            f"SL={stop_loss_bps:.1f}bps H={_max_hold_seconds(signal)}s"
        ),
        cycle_id,
    )


def tactical_paper_results_text(results: Iterable[TacticalPaperResult], warnings: Iterable[str] = ()) -> str:
    lines = ["전술 paper 브리지"]
    lines.append("실주문이 아니라, 레이더 READY 후보를 paper 상태머신으로 검증하는 단계입니다.")
    for warning in list(warnings)[:5]:
        lines.append(f"경고: {warning}")
    for result in results:
        prefix = result.symbol if result.symbol != "-" else "전체"
        lines.append(f"- {prefix}: {result.action} - {result.detail}")
    return "\n".join(lines)


def tactical_live_results_text(results: Iterable[TacticalPaperResult], warnings: Iterable[str] = ()) -> str:
    lines = ["전술 live 브리지"]
    lines.append("실제 주문 경로입니다. 원샷 1회, live 플래그, Binance 실제 주문/포지션 확인을 모두 통과해야 합니다.")
    for warning in list(warnings)[:5]:
        lines.append(f"경고: {warning}")
    for result in results:
        prefix = result.symbol if result.symbol != "-" else "전체"
        lines.append(f"- {prefix}: {result.action} - {result.detail}")
    return "\n".join(lines)


def _is_tactical_cycle(cycle) -> bool:
    return str(cycle["strategy"]).startswith(TACTICAL_STRATEGY_PREFIX)


def _is_paper_tactical_cycle(cycle) -> bool:
    return _is_tactical_cycle(cycle) and int(cycle["dry_run"]) == 1


def _is_live_tactical_cycle(cycle) -> bool:
    return _is_tactical_cycle(cycle) and int(cycle["dry_run"]) == 0


def _has_active_live_tactical_cycle(store: TradingStore) -> bool:
    return any(_is_live_tactical_cycle(cycle) for cycle in store.active_strategy_cycles())


def _active_live_symbols(store: TradingStore) -> set[str]:
    symbols = {
        str(cycle["symbol"]).upper()
        for cycle in store.active_strategy_cycles()
        if int(cycle["dry_run"]) == 0
    }
    symbols.update(
        str(cycle["symbol"]).upper()
        for cycle in store.active_scalp_cycles()
        if int(cycle["dry_run"]) == 0
    )
    return symbols


def _actual_exchange_symbols(client: BinanceUSDMClient) -> tuple[set[str], set[str], str]:
    try:
        orders = client._signed_request("GET", "/fapi/v1/openOrders")
        account = client.account_info()
    except BinanceAPIError as exc:
        return set(), set(), f"바이낸스 실제 주문/포지션 확인 실패: {exc}"
    order_symbols = {str(row.get("symbol", "")).upper() for row in orders if row.get("symbol")}
    position_symbols = {
        str(row.get("symbol", "")).upper()
        for row in account.get("positions", [])
        if float(row.get("positionAmt") or 0.0) != 0.0
    }
    return order_symbols, position_symbols, ""


def _normalize_live_intent(
    client: BinanceUSDMClient,
    intent: OrderIntent,
) -> tuple[OrderIntent | None, str]:
    try:
        filters = SymbolFilters.from_exchange_info(client.exchange_info(intent.symbol), intent.symbol)
        normalized, reason = filters.normalize_intent(intent)
        if normalized is None:
            return None, reason
        ticker = client.book_ticker(intent.symbol)
        mid = (float(ticker["bidPrice"]) + float(ticker["askPrice"])) / 2.0
        min_notional = float(filters.min_order_notional_at(mid))
    except (AttributeError, BinanceAPIError, KeyError, ValueError) as exc:
        return None, f"exchange filter error: {exc}"
    notional = normalized.quantity * mid
    if min_notional > 0 and notional < min_notional:
        return None, f"market notional {notional:.4f} below minimum {min_notional:.4f}"
    return normalized, reason


def _strategy_name(signal: TacticalRadarSignal) -> str:
    scenario = signal.scenario.lower().replace("-", "_")
    side = signal.side.lower()
    if scenario.endswith(side):
        return f"{TACTICAL_STRATEGY_PREFIX}{scenario}"
    return f"{TACTICAL_STRATEGY_PREFIX}{scenario}_{side}"


def _exit_bps(side: str, entry_price: float, target_price: float, stop_price: float) -> tuple[float, float] | None:
    if side == "long":
        take_profit_bps = ((target_price / entry_price) - 1.0) * 10_000.0
        stop_loss_bps = ((entry_price / stop_price) - 1.0) * 10_000.0 if stop_price > 0 else -1.0
    elif side == "short":
        take_profit_bps = ((entry_price / target_price) - 1.0) * 10_000.0 if target_price > 0 else -1.0
        stop_loss_bps = ((stop_price / entry_price) - 1.0) * 10_000.0
    else:
        return None
    if take_profit_bps <= 0 or stop_loss_bps <= 0:
        return None
    return round(take_profit_bps, 1), round(stop_loss_bps, 1)


def _max_hold_seconds(signal: TacticalRadarSignal) -> int:
    if "failed_breakout" in signal.scenario:
        return 900
    return 1800
