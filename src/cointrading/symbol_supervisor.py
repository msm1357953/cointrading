from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Iterable

from cointrading.config import TradingConfig
from cointrading.execution_gate import evaluate_simple_strategy_gate, strategy_name_from_execution_mode
from cointrading.exchange.binance_usdm import BinanceAPIError, BinanceUSDMClient
from cointrading.exchange_filters import SymbolFilters
from cointrading.market_context import collect_market_context
from cointrading.market_regime import MACRO_BEAR, MACRO_BULL, macro_regime_ko, trade_bias_ko
from cointrading.market_regime import evaluate_market_regime
from cointrading.risk_state import RuntimeRiskSnapshot, evaluate_runtime_risk, risk_mode_ko
from cointrading.storage import TradingStore, kst_from_ms, now_ms
from cointrading.strategy_lifecycle import strategy_plan_from_setup
from cointrading.strategy_notify import MODE_LABELS, REGIME_LABELS, SIDE_LABELS, strategy_family_label
from cointrading.strategy_router import SETUP_PASS, StrategySetup


DECISION_READY = "READY"
DECISION_WAIT = "WAIT"
DECISION_BLOCKED = "BLOCKED"
SCALP_LIVE_EXECUTION_MODES = {"maker_post_only"}
STRATEGY_LIVE_EXECUTION_MODES = {"taker_trend", "maker_range", "taker_breakout"}
SUPPORTED_LIVE_EXECUTION_MODES = SCALP_LIVE_EXECUTION_MODES | STRATEGY_LIVE_EXECUTION_MODES


def refresh_supervisor_inputs(
    client: BinanceUSDMClient,
    store: TradingStore,
    symbols: Iterable[str],
) -> tuple[str, ...]:
    warnings: list[str] = []
    for symbol in symbols:
        symbol = symbol.upper()
        try:
            store.insert_market_context(collect_market_context(client, symbol))
        except BinanceAPIError as exc:
            warnings.append(f"{symbol} 시장상황 수집 실패: {exc}")
        try:
            snapshot = evaluate_market_regime(
                symbol=symbol,
                klines_15m=client.klines(symbol=symbol, interval="15m", limit=120),
                klines_1h=client.klines(symbol=symbol, interval="1h", limit=120),
            )
            store.insert_market_regime(snapshot)
        except BinanceAPIError as exc:
            warnings.append(f"{symbol} 장세 수집 실패: {exc}")
    return tuple(warnings)


@dataclass(frozen=True)
class SupervisorReport:
    symbol: str
    decision: str
    reasons: tuple[str, ...]
    warnings: tuple[str, ...]
    best_candidate: dict | None
    macro_summary: str
    context_summary: str
    notional: float
    min_order_notional: float
    runtime_risk_mode: str
    active_locked: bool
    actual_exit_profile: str

    def to_text(self) -> str:
        candidate = _candidate_line(self.best_candidate)
        reasons = "\n".join(f"- {reason}" for reason in self.reasons) or "- 없음"
        warnings = "\n".join(f"- {warning}" for warning in self.warnings) or "- 없음"
        return "\n".join(
            [
                f"실전감독: {self.symbol}",
                f"판정: {_decision_ko(self.decision)}",
                f"요청규모: {self.notional:.2f}",
                f"최소주문: {self.min_order_notional:.2f}",
                f"위험모드: {risk_mode_ko(self.runtime_risk_mode)}",
                f"장세: {self.macro_summary}",
                f"시장상황: {self.context_summary}",
                f"후보: {candidate}",
                f"실제 주문계획: {self.actual_exit_profile or '없음'}",
                "차단/대기 이유:",
                reasons,
                "주의:",
                warnings,
            ]
        )


def supervise_symbols(
    client: BinanceUSDMClient,
    store: TradingStore,
    config: TradingConfig,
    symbols: Iterable[str],
    *,
    notional: float,
    current_ms: int | None = None,
) -> list[SupervisorReport]:
    symbol_list = [symbol.upper() for symbol in symbols]
    ts = current_ms or now_ms()
    risk = evaluate_runtime_risk(store, config, current_ms=ts)
    actual_orders, actual_positions, actual_warnings = _actual_exchange_symbols(client)
    active_symbols = store.active_cycle_symbols()
    candidates = _latest_candidates_by_symbol(store.latest_strategy_batch())
    strategy_perf = _performance_by_symbol_side(store.strategy_cycle_performance())
    scalp_perf = _performance_by_symbol_side(store.scalp_cycle_performance())
    recent_limit = max(200, config.supervisor_recent_cycle_count * max(1, len(symbol_list)) * 8)
    strategy_recent_perf = _recent_performance_by_symbol_side(
        store.recent_strategy_cycles(limit=recent_limit),
        config.supervisor_recent_cycle_count,
    )
    scalp_recent_perf = _recent_performance_by_symbol_side(
        store.recent_scalp_cycles(limit=recent_limit),
        config.supervisor_recent_cycle_count,
    )
    reports = []
    for symbol in symbol_list:
        reports.append(
            supervise_symbol(
                client,
                store,
                config,
                symbol,
                notional=notional,
                runtime_risk=risk,
                active_symbols=active_symbols,
                actual_open_order_symbols=actual_orders,
                actual_position_symbols=actual_positions,
                actual_exchange_warnings=actual_warnings,
                candidates=candidates.get(symbol, []),
                strategy_perf=strategy_perf,
                scalp_perf=scalp_perf,
                strategy_recent_perf=strategy_recent_perf,
                scalp_recent_perf=scalp_recent_perf,
                current_ms=ts,
            )
        )
    return reports


def supervise_symbol(
    client: BinanceUSDMClient,
    store: TradingStore,
    config: TradingConfig,
    symbol: str,
    *,
    notional: float,
    runtime_risk: RuntimeRiskSnapshot,
    active_symbols: set[str],
    actual_open_order_symbols: set[str],
    actual_position_symbols: set[str],
    actual_exchange_warnings: tuple[str, ...],
    candidates: list,
    strategy_perf: dict[tuple[str, str], dict],
    scalp_perf: dict[tuple[str, str], dict],
    strategy_recent_perf: dict[tuple[str, str], dict],
    scalp_recent_perf: dict[tuple[str, str], dict],
    current_ms: int,
) -> SupervisorReport:
    reasons: list[str] = []
    warnings: list[str] = []
    symbol = symbol.upper()

    macro = store.latest_market_regime(symbol)
    context = store.latest_market_context(symbol)
    macro_summary = _macro_summary(macro, current_ms, config)
    context_summary = _context_summary(context, current_ms, config)
    if macro is None:
        reasons.append("장세 데이터가 없습니다.")
    elif _is_stale(int(macro["timestamp_ms"]), current_ms, config.supervisor_data_max_age_minutes):
        reasons.append("장세 데이터가 오래되었습니다.")
    if context is None:
        reasons.append("시장상황 데이터가 없습니다.")
    elif _is_stale(int(context["timestamp_ms"]), current_ms, config.supervisor_data_max_age_minutes):
        reasons.append("시장상황 데이터가 오래되었습니다.")

    min_order_notional = _min_order_notional(client, symbol)
    if notional < min_order_notional:
        reasons.append(f"요청규모 {notional:.2f}가 최소주문 {min_order_notional:.2f}보다 작습니다.")
    if notional > config.max_single_order_notional:
        reasons.append(
            f"요청규모 {notional:.2f}가 단일 주문 한도 {config.max_single_order_notional:.2f}보다 큽니다."
        )

    if symbol in active_symbols:
        reasons.append("해당 심볼의 paper/live 상태머신이 이미 열려 있습니다.")
    if symbol in actual_open_order_symbols:
        reasons.append("바이낸스 실제 열린 주문이 있습니다.")
    if symbol in actual_position_symbols:
        reasons.append("바이낸스 실제 포지션이 이미 있습니다.")
    if actual_exchange_warnings:
        reasons.append("바이낸스 실제 주문/포지션 확인에 실패해 실전을 차단합니다.")
        warnings.extend(actual_exchange_warnings)
    if not runtime_risk.allows_new_entries:
        reasons.append(f"런타임 위험모드가 신규 진입을 차단합니다: {runtime_risk.reasons[0]}")

    best_candidate = _best_macro_aligned_candidate(candidates, macro)
    if best_candidate is None:
        reasons.append("현재 장세와 일치하는 승인 전략 후보가 없습니다.")
    else:
        if int(best_candidate["sample_count"]) < config.supervisor_min_samples:
            reasons.append(
                f"후보 표본 {int(best_candidate['sample_count'])}개가 기준 "
                f"{config.supervisor_min_samples}개보다 적습니다."
            )
        if float(best_candidate["avg_pnl_bps"]) < config.supervisor_min_avg_pnl_bps:
            reasons.append(
                f"후보 평균손익 {float(best_candidate['avg_pnl_bps']):.3f}bps가 기준 "
                f"{config.supervisor_min_avg_pnl_bps:.3f}bps보다 낮습니다."
            )
        _append_performance_reasons(
            reasons,
            warnings,
            symbol,
            str(best_candidate["side"]),
            best_candidate,
            strategy_perf,
            scalp_perf,
            strategy_recent_perf,
            scalp_recent_perf,
            config,
        )
        _append_simple_gate_reasons(reasons, store, config, symbol, best_candidate, current_ms)
    actual_exit_profile = _actual_strategy_exit_profile(
        reasons,
        client,
        store,
        config,
        symbol,
        best_candidate,
        macro,
        current_ms,
    )

    if config.dry_run:
        reasons.append("dry-run이 켜져 있어 실전 주문은 잠겨 있습니다.")
    if not config.live_trading_enabled:
        reasons.append("live trading 플래그가 꺼져 있습니다.")
    _append_live_mode_reasons(reasons, config, best_candidate)
    if config.live_one_shot_required:
        _append_one_shot_reasons(reasons, config, symbol, notional, best_candidate)

    decision = DECISION_READY
    if reasons:
        decision = DECISION_BLOCKED
    elif warnings:
        decision = DECISION_WAIT

    return SupervisorReport(
        symbol=symbol,
        decision=decision,
        reasons=tuple(reasons),
        warnings=tuple(warnings),
        best_candidate=_row_dict(best_candidate) if best_candidate is not None else None,
        macro_summary=macro_summary,
        context_summary=context_summary,
        notional=notional,
        min_order_notional=min_order_notional,
        runtime_risk_mode=runtime_risk.mode,
        active_locked=symbol in active_symbols,
        actual_exit_profile=actual_exit_profile,
    )


def supervisor_report_text(reports: Iterable[SupervisorReport]) -> str:
    reports = list(reports)
    if not reports:
        return "실전감독 결과가 없습니다."
    lines = ["실전감독 요약"]
    counts = {DECISION_READY: 0, DECISION_WAIT: 0, DECISION_BLOCKED: 0}
    for report in reports:
        counts[report.decision] = counts.get(report.decision, 0) + 1
    lines.append(
        "판정: "
        f"가능 {counts.get(DECISION_READY, 0)}, "
        f"대기 {counts.get(DECISION_WAIT, 0)}, "
        f"차단 {counts.get(DECISION_BLOCKED, 0)}"
    )
    lines.append("")
    for report in reports:
        lines.append(report.to_text())
        lines.append("")
    return "\n".join(lines).rstrip()


def _append_performance_reasons(
    reasons: list[str],
    warnings: list[str],
    symbol: str,
    side: str,
    best_candidate,
    strategy_perf: dict[tuple[str, str], dict],
    scalp_perf: dict[tuple[str, str], dict],
    strategy_recent_perf: dict[tuple[str, str], dict],
    scalp_recent_perf: dict[tuple[str, str], dict],
    config: TradingConfig,
) -> None:
    key = (symbol, side)
    perf = _candidate_perf(best_candidate, key, strategy_perf, scalp_perf)
    if perf is None:
        reasons.append("해당 심볼/방향의 상태머신 paper 성과가 없습니다.")
        return
    count = int(perf["count"])
    sum_pnl = float(perf["sum_pnl"])
    avg_pnl = float(perf["avg_pnl"])
    if count < config.supervisor_min_cycle_count:
        reasons.append(
            f"paper 종료 표본 {count}개가 기준 {config.supervisor_min_cycle_count}개보다 적습니다."
        )
    if sum_pnl <= config.supervisor_min_cycle_sum_pnl:
        reasons.append(
            f"paper 누적손익 {sum_pnl:.6f}이 기준 {config.supervisor_min_cycle_sum_pnl:.6f} 이하입니다."
        )
    if avg_pnl <= 0:
        reasons.append(f"paper 평균손익이 {avg_pnl:.6f}로 양수가 아닙니다.")
    payoff_ratio = _payoff_ratio_value(perf)
    if payoff_ratio is None:
        reasons.append("paper 손익비를 산출할 만큼 익절/손절 표본이 모두 있지 않습니다.")
    elif payoff_ratio < config.supervisor_min_payoff_ratio:
        reasons.append(
            f"paper 손익비 {payoff_ratio:.2f}가 기준 {config.supervisor_min_payoff_ratio:.2f}보다 낮습니다."
        )
    recent = _candidate_perf(best_candidate, key, strategy_recent_perf, scalp_recent_perf)
    if recent is None:
        reasons.append("최근 paper 성과 표본이 없습니다.")
        return
    recent_count = int(recent["count"])
    recent_sum = float(recent["sum_pnl"])
    recent_avg = float(recent["avg_pnl"])
    if recent_count < config.supervisor_recent_cycle_count:
        reasons.append(
            f"최근 paper 표본 {recent_count}개가 기준 {config.supervisor_recent_cycle_count}개보다 적습니다."
        )
    if recent_sum <= config.supervisor_min_recent_cycle_sum_pnl:
        reasons.append(
            f"최근 paper 누적손익 {recent_sum:.6f}이 기준 "
            f"{config.supervisor_min_recent_cycle_sum_pnl:.6f} 이하입니다."
        )
    if recent_avg <= 0:
        reasons.append(f"최근 paper 평균손익이 {recent_avg:.6f}로 양수가 아닙니다.")
    adverse_ratio = float(recent.get("adverse_exit_ratio", 0.0))
    if adverse_ratio > config.supervisor_max_adverse_exit_ratio:
        reasons.append(
            f"최근 stop/max_hold 종료 비율 {adverse_ratio:.1%}가 기준 "
            f"{config.supervisor_max_adverse_exit_ratio:.1%}보다 높습니다."
        )


def _append_one_shot_reasons(
    reasons: list[str],
    config: TradingConfig,
    symbol: str,
    notional: float,
    best_candidate,
) -> None:
    if not config.live_one_shot_enabled:
        reasons.append("원샷 live 허가가 꺼져 있습니다.")
        return
    if config.live_one_shot_symbol and symbol != config.live_one_shot_symbol:
        reasons.append(f"원샷 허가 심볼이 {config.live_one_shot_symbol}입니다.")
    if notional > config.live_one_shot_notional:
        reasons.append(
            f"요청규모 {notional:.2f}가 원샷 허가 규모 {config.live_one_shot_notional:.2f}보다 큽니다."
        )
    if config.live_one_shot_strategy and best_candidate is not None:
        mode = str(best_candidate["execution_mode"])
        if config.live_one_shot_strategy != mode:
            reasons.append(f"원샷 허가 실행방식이 {config.live_one_shot_strategy}입니다.")


def _append_simple_gate_reasons(
    reasons: list[str],
    store: TradingStore,
    config: TradingConfig,
    symbol: str,
    best_candidate,
    current_ms: int,
) -> None:
    setup = {
        "strategy": strategy_name_from_execution_mode(str(best_candidate["execution_mode"])),
    }
    decision = evaluate_simple_strategy_gate(
        store,
        config,
        setup,
        symbol=symbol,
        dry_run=False,
        timestamp_ms=current_ms,
    )
    if not decision.allowed:
        reasons.append(decision.reason)


def _actual_strategy_exit_profile(
    reasons: list[str],
    client: BinanceUSDMClient,
    store: TradingStore,
    config: TradingConfig,
    symbol: str,
    best_candidate,
    macro,
    current_ms: int,
) -> str:
    if best_candidate is None:
        return ""
    mode = str(best_candidate["execution_mode"])
    if mode not in STRATEGY_LIVE_EXECUTION_MODES:
        return ""
    prices = _book_bid_ask(client, symbol)
    if prices is None:
        reasons.append("현재 호가를 조회하지 못해 실제 전략 주문계획을 산출할 수 없습니다.")
        return ""
    bid, ask = prices
    macro_for_plan = macro
    if macro is not None and _is_stale(
        int(macro["timestamp_ms"]),
        current_ms,
        config.macro_regime_max_age_minutes,
    ):
        macro_for_plan = None
    setup = StrategySetup(
        strategy=strategy_name_from_execution_mode(mode),
        execution_mode=mode,
        status=SETUP_PASS,
        side=str(best_candidate["side"]),
        horizon="supervisor",
        live_supported=True,
        reason="supervisor candidate",
    )
    plan = strategy_plan_from_setup(
        setup,
        config,
        symbol=symbol,
        bid=bid,
        ask=ask,
        macro_row=macro_for_plan,
    )
    if plan is None:
        reasons.append("실제 전략 주문계획을 만들 수 없습니다.")
        return ""
    summary = (
        f"{plan.strategy} {plan.side} {plan.entry_order_type} "
        f"TP={plan.take_profit_bps:.1f} SL={plan.stop_loss_bps:.1f} "
        f"H={plan.max_hold_seconds}s profile={plan.exit_profile}"
    )
    exact = store.latest_strategy_evaluation(
        symbol=symbol,
        regime=plan.strategy,
        side=plan.side,
        take_profit_bps=plan.take_profit_bps,
        stop_loss_bps=plan.stop_loss_bps,
        max_hold_seconds=plan.max_hold_seconds,
        execution_mode=plan.execution_mode,
        source="strategy_cycles",
    )
    if exact is None:
        reasons.append(
            "실전 실제 exit profile이 paper에서 아직 승인되지 않았습니다: "
            f"{summary}"
        )
        return summary
    if str(exact["decision"]) != "APPROVED":
        reasons.append(
            "실전 실제 exit profile이 paper 승인 상태가 아닙니다: "
            f"{summary} / {exact['decision']} {exact['reason']}"
        )
        return summary
    return (
        f"{summary} / paper 승인 "
        f"n={int(exact['sample_count'])} avg={float(exact['avg_pnl_bps']):+.3f}bps"
    )


def _append_live_mode_reasons(reasons: list[str], config: TradingConfig, best_candidate) -> None:
    if best_candidate is None:
        if not config.live_strategy_lifecycle_enabled and not config.live_scalp_lifecycle_enabled:
            reasons.append("live 상태머신 플래그가 꺼져 있습니다.")
        return
    mode = str(best_candidate["execution_mode"])
    if mode not in SUPPORTED_LIVE_EXECUTION_MODES:
        reasons.append(f"지원되지 않는 live 실행방식입니다: {mode}")
        return
    if mode in SCALP_LIVE_EXECUTION_MODES and not config.live_scalp_lifecycle_enabled:
        reasons.append("live 상태머신 플래그가 꺼져 있습니다. 스캘핑 live 상태머신 비활성.")
    if mode in STRATEGY_LIVE_EXECUTION_MODES and not config.live_strategy_lifecycle_enabled:
        reasons.append("live 상태머신 플래그가 꺼져 있습니다. 전략 live 상태머신 비활성.")


def _best_macro_aligned_candidate(candidates: list, macro) -> object | None:
    approved = [row for row in candidates if str(row["decision"]) == "APPROVED"]
    if macro is not None:
        regime = str(macro["macro_regime"])
        bias = str(macro["trade_bias"])
        if regime == MACRO_BULL:
            approved = [row for row in approved if str(row["side"]) == "long"]
        elif regime == MACRO_BEAR:
            approved = [row for row in approved if str(row["side"]) == "short"]
        elif bias in {"long", "short"}:
            approved = [row for row in approved if str(row["side"]) == bias]
    if not approved:
        return None
    return sorted(
        approved,
        key=lambda row: (-float(row["avg_pnl_bps"]), -int(row["sample_count"])),
    )[0]


def _latest_candidates_by_symbol(rows: Iterable) -> dict[str, list]:
    result: dict[str, list] = {}
    for row in rows:
        result.setdefault(str(row["symbol"]).upper(), []).append(row)
    return result


def _performance_by_symbol_side(rows: Iterable) -> dict[tuple[str, str], dict]:
    result: dict[tuple[str, str], dict] = {}
    for row in rows:
        key = (str(row["symbol"]).upper(), str(row["side"]))
        current = result.get(key)
        payload = {
            "count": int(row["count"]),
            "wins": int(row["wins"] or 0),
            "losses": int(row["losses"] or 0),
            "avg_win_pnl": _optional_float(row["avg_win_pnl"]),
            "avg_loss_pnl": _optional_float(row["avg_loss_pnl"]),
            "avg_pnl": float(row["avg_pnl"] or 0.0),
            "sum_pnl": float(row["sum_pnl"] or 0.0),
        }
        if current is None or payload["sum_pnl"] > current["sum_pnl"]:
            result[key] = payload
    return result


def _recent_performance_by_symbol_side(rows: Iterable, window: int) -> dict[tuple[str, str], dict]:
    grouped: dict[tuple[str, str], list] = {}
    for row in rows:
        if row["realized_pnl"] is None:
            continue
        key = (str(row["symbol"]).upper(), str(row["side"]))
        values = grouped.setdefault(key, [])
        if len(values) < window:
            values.append(row)
    return {key: _cycle_rows_performance(values) for key, values in grouped.items()}


def _cycle_rows_performance(rows: list) -> dict:
    pnl_values = [float(row["realized_pnl"]) for row in rows]
    wins = [value for value in pnl_values if value > 0]
    losses = [value for value in pnl_values if value <= 0]
    adverse = [
        row
        for row in rows
        if str(row["reason"] or "") in {"stop_loss", "max_hold_exit"}
        or str(row["status"]) == "STOPPED"
    ]
    count = len(pnl_values)
    return {
        "count": count,
        "wins": len(wins),
        "losses": len(losses),
        "avg_win_pnl": sum(wins) / len(wins) if wins else None,
        "avg_loss_pnl": sum(losses) / len(losses) if losses else None,
        "avg_pnl": sum(pnl_values) / count if count else 0.0,
        "sum_pnl": sum(pnl_values),
        "adverse_exit_ratio": len(adverse) / count if count else 0.0,
    }


def _candidate_perf(best_candidate, key: tuple[str, str], strategy_perf: dict, scalp_perf: dict):
    mode = str(best_candidate["execution_mode"])
    if mode in SCALP_LIVE_EXECUTION_MODES:
        return scalp_perf.get(key)
    if mode in STRATEGY_LIVE_EXECUTION_MODES:
        return strategy_perf.get(key)
    return None


def _payoff_ratio_value(perf: dict) -> float | None:
    avg_win = perf.get("avg_win_pnl")
    avg_loss = perf.get("avg_loss_pnl")
    if avg_win is None or avg_loss is None:
        return None
    avg_win = float(avg_win)
    avg_loss = abs(float(avg_loss))
    if avg_win <= 0 or avg_loss <= 0:
        return None
    return avg_win / avg_loss


def _optional_float(value) -> float | None:
    if value is None:
        return None
    return float(value)


def _actual_exchange_symbols(client: BinanceUSDMClient) -> tuple[set[str], set[str], tuple[str, ...]]:
    try:
        orders = client._signed_request("GET", "/fapi/v1/openOrders")
        account = client.account_info()
    except BinanceAPIError as exc:
        return set(), set(), (f"실제 주문/포지션 조회 실패: {exc}",)
    order_symbols = {str(row.get("symbol", "")).upper() for row in orders if row.get("symbol")}
    position_symbols = {
        str(row.get("symbol", "")).upper()
        for row in account.get("positions", [])
        if float(row.get("positionAmt") or 0.0) != 0.0
    }
    return order_symbols, position_symbols, tuple()


def _min_order_notional(client: BinanceUSDMClient, symbol: str) -> float:
    try:
        prices = _book_bid_ask(client, symbol)
        if prices is None:
            return float(Decimal("inf"))
        bid, ask = prices
        mid = (bid + ask) / 2.0
        filters = SymbolFilters.from_exchange_info(client.exchange_info(symbol), symbol)
        return float(filters.min_order_notional_at(mid))
    except (BinanceAPIError, ValueError, KeyError):
        return float(Decimal("inf"))


def _book_bid_ask(client: BinanceUSDMClient, symbol: str) -> tuple[float, float] | None:
    try:
        ticker = client.book_ticker(symbol)
        bid = float(ticker["bidPrice"])
        ask = float(ticker["askPrice"])
    except (AttributeError, BinanceAPIError, KeyError, ValueError):
        return None
    if bid <= 0 or ask <= 0:
        return None
    return bid, ask


def _macro_summary(row, current_ms: int, config: TradingConfig) -> str:
    if row is None:
        return "없음"
    age = (current_ms - int(row["timestamp_ms"])) / 60_000.0
    stale = " / 오래됨" if age > config.supervisor_data_max_age_minutes else ""
    return (
        f"{macro_regime_ko(str(row['macro_regime']))} "
        f"{trade_bias_ko(str(row['trade_bias']))} "
        f"1h={float(row['trend_1h_bps']):.1f}bps "
        f"ATR={float(row['atr_bps']):.1f}bps age={age:.1f}m{stale}"
    )


def _context_summary(row, current_ms: int, config: TradingConfig) -> str:
    if row is None:
        return "없음"
    age = (current_ms - int(row["timestamp_ms"])) / 60_000.0
    stale = " / 오래됨" if age > config.supervisor_data_max_age_minutes else ""
    funding = row["funding_rate"]
    funding_text = "n/a" if funding is None else f"{float(funding) * 10_000:.3f}bps"
    return (
        f"premium={float(row['premium_bps']):.3f}bps "
        f"funding={funding_text} "
        f"spread={float(row['spread_bps']):.3f}bps "
        f"imb={float(row['depth_imbalance']):.3f} age={age:.1f}m{stale}"
    )


def _candidate_line(row: dict | None) -> str:
    if row is None:
        return "없음"
    strategy = strategy_family_label(row)
    mode = MODE_LABELS.get(str(row["execution_mode"]), str(row["execution_mode"]))
    regime = REGIME_LABELS.get(str(row["regime"]), str(row["regime"]))
    side = SIDE_LABELS.get(str(row["side"]), str(row["side"]))
    return (
        f"전략={strategy} 주문={mode} 조건={regime} 방향={side} "
        f"TP={float(row['take_profit_bps']):.1f} "
        f"SL={float(row['stop_loss_bps']):.1f} "
        f"H={int(row['max_hold_seconds'])}s "
        f"n={int(row['sample_count'])} "
        f"avg={float(row['avg_pnl_bps']):+.3f}bps"
    )


def _decision_ko(decision: str) -> str:
    return {
        DECISION_READY: "실전 가능",
        DECISION_WAIT: "대기",
        DECISION_BLOCKED: "차단",
    }.get(decision, decision)


def _row_dict(row) -> dict | None:
    if row is None:
        return None
    return {key: row[key] for key in row.keys()}


def _is_stale(timestamp_ms: int, current_ms: int, max_age_minutes: int) -> bool:
    return current_ms - timestamp_ms > max_age_minutes * 60_000
