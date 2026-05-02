from __future__ import annotations

from dataclasses import asdict, dataclass
import json
from pathlib import Path
from typing import Any, Iterable

from cointrading.config import TradingConfig
from cointrading.exchange.binance_usdm import BinanceAPIError, BinanceUSDMClient
from cointrading.meta_strategy import MarketFeatureSnapshot
from cointrading.storage import kst_from_ms, now_ms
from cointrading.strategy_miner import (
    MinedStrategyResult,
    current_feature_snapshot,
    default_strategy_refine_report_path,
    load_strategy_refine_report,
    mined_result_matches_feature,
    strategy_action_ko,
    strategy_results_from_report,
)


ENTRY_READY = "READY"
ENTRY_WAIT = "WAIT"
ENTRY_BLOCKED = "BLOCKED"


@dataclass(frozen=True)
class RefinedEntryCandidate:
    symbol: str
    interval: str
    action: str
    side: str
    decision: str
    reason: str
    source_decision: str
    take_profit_bps: float
    stop_loss_bps: float
    max_hold_bars: int
    current_price: float
    target_price: float
    stop_price: float
    feature_time_ms: int
    test_count: int
    test_win_rate: float
    test_avg_pnl_bps: float
    test_profit_factor: float
    test_payoff_ratio: float
    full_avg_pnl_bps: float
    full_profit_factor: float
    risk_reward_ratio: float
    breakeven_win_rate: float
    win_rate_edge: float
    positive_test_windows: int
    selected_windows: int
    positive_test_window_ratio: float
    feature_summary: str


@dataclass(frozen=True)
class RefinedEntryNotifyState:
    last_signature: str = ""
    last_sent_ms: int = 0

    @classmethod
    def load(cls, path: Path) -> "RefinedEntryNotifyState":
        if not path.exists():
            return cls()
        payload = json.loads(path.read_text(encoding="utf-8"))
        return cls(
            last_signature=str(payload.get("last_signature", "")),
            last_sent_ms=int(payload.get("last_sent_ms", 0) or 0),
        )

    def save(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(asdict(self), ensure_ascii=False, indent=2), encoding="utf-8")


def default_refined_entry_report_path() -> Path:
    return Path(__file__).resolve().parents[2] / "data" / "refined_entry_latest.json"


def default_refined_entry_notify_state_path() -> Path:
    return Path(__file__).resolve().parents[2] / "data" / "refined_entry_notify_state.json"


def evaluate_refined_entry_candidates(
    client: BinanceUSDMClient,
    *,
    config: TradingConfig,
    source_path: Path | None = None,
    symbols: Iterable[str] | None = None,
    kline_limit: int = 500,
    limit: int = 20,
    current_ms: int | None = None,
) -> tuple[list[RefinedEntryCandidate], tuple[str, ...]]:
    ts = current_ms or now_ms()
    report_path = source_path or default_strategy_refine_report_path()
    report = load_strategy_refine_report(report_path)
    if report is None:
        return [], (f"정제 리포트가 없습니다: {report_path}",)
    rows = [row for row in strategy_results_from_report(report) if row.decision == "SURVIVED"]
    if symbols:
        allowed_symbols = {symbol.upper() for symbol in symbols}
        rows = [row for row in rows if row.symbol.upper() in allowed_symbols]
    warnings: list[str] = []
    by_symbol_interval: dict[tuple[str, str], list[MinedStrategyResult]] = {}
    for row in rows:
        by_symbol_interval.setdefault((row.symbol.upper(), row.interval), []).append(row)

    output: list[RefinedEntryCandidate] = []
    for (symbol, interval), candidates in sorted(by_symbol_interval.items()):
        try:
            klines = client.klines(symbol=symbol, interval=interval, limit=kline_limit)
        except BinanceAPIError as exc:
            warnings.append(f"{symbol} {interval} 현재 캔들 조회 실패: {exc}")
            continue
        feature = current_feature_snapshot(
            symbol=symbol,
            interval=interval,
            klines=klines,
            current_ms=ts,
        )
        if feature is None:
            warnings.append(f"{symbol} {interval} 현재 feature 산출에 필요한 닫힌 봉이 부족합니다.")
            continue
        for row in candidates:
            output.append(_candidate_from_result(row, feature, config))

    ranked = sorted(output, key=_entry_rank_key)
    return ranked[:limit], tuple(warnings)


def write_refined_entry_report(
    path: Path,
    *,
    candidates: list[RefinedEntryCandidate],
    warnings: Iterable[str],
    source_path: Path | None,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "generated_ms": now_ms(),
        "source_path": str(source_path or default_strategy_refine_report_path()),
        "warnings": list(warnings),
        "candidates": [asdict(candidate) for candidate in candidates],
    }
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def load_refined_entry_report(path: Path | None = None) -> dict[str, Any] | None:
    report_path = path or default_refined_entry_report_path()
    if not report_path.exists():
        return None
    return json.loads(report_path.read_text(encoding="utf-8"))


def refined_entry_text(
    candidates: list[RefinedEntryCandidate],
    *,
    warnings: Iterable[str] = (),
    generated_ms: int | None = None,
    limit: int = 8,
) -> str:
    lines = [
        "현재장 정제후보 게이트",
        "과거 SURVIVED 후보의 조건이 지금 닫힌 봉에서도 맞는지 보는 진입 후보 판단입니다.",
    ]
    if generated_ms:
        lines.append(f"생성시각: {kst_from_ms(generated_ms)}")
    ready = [row for row in candidates if row.decision == ENTRY_READY]
    wait = [row for row in candidates if row.decision == ENTRY_WAIT]
    blocked = [row for row in candidates if row.decision == ENTRY_BLOCKED]
    lines.append(f"요약: 진입후보 {len(ready)}개, 대기 {len(wait)}개, 차단 {len(blocked)}개")
    if ready:
        best = ready[0]
        lines.append(
            "최상위 후보: "
            f"{best.symbol} {strategy_action_ko(best.action)} "
            f"현재={best.current_price:.8g} 목표={best.target_price:.8g} 손절={best.stop_price:.8g} "
            f"과거승률={best.test_win_rate:.1%} 평균={best.test_avg_pnl_bps:+.2f}bps"
        )
        lines.append("주의: 이 단계도 주문 실행이 아닙니다. live-supervisor/preflight/live flag가 따로 통과해야 합니다.")
    else:
        lines.append("현재 닫힌 봉 기준으로 바로 진입할 정제후보는 없습니다.")
    warnings = list(warnings)
    if warnings:
        lines.append("경고:")
        lines.extend(f"- {warning}" for warning in warnings[:5])
    lines.append("")
    for row in candidates[:limit]:
        lines.append(
            "- "
            f"{_decision_ko(row.decision)} {row.symbol} {strategy_action_ko(row.action)} "
            f"현재={row.current_price:.8g} TP={row.take_profit_bps:.0f}/SL={row.stop_loss_bps:.0f}/"
            f"H={row.max_hold_bars}봉 "
            f"승률={row.test_win_rate:.1%} 평균={row.test_avg_pnl_bps:+.2f}bps "
            f"PF={row.test_profit_factor:.2f} 손익비={row.test_payoff_ratio:.2f} "
            f"승률여유={row.win_rate_edge:+.1%} - {row.reason}"
        )
    return "\n".join(lines)


def refined_entry_report_text(path: Path | None = None, *, limit: int = 8) -> str:
    payload = load_refined_entry_report(path)
    if payload is None:
        return "현재장 정제후보 결과가 없습니다. 먼저 refine-entry-check를 실행해야 합니다."
    candidates = [_candidate_from_dict(row) for row in payload.get("candidates", []) or []]
    warnings = tuple(str(row) for row in payload.get("warnings", []) or [])
    return refined_entry_text(
        candidates,
        warnings=warnings,
        generated_ms=int(payload.get("generated_ms", 0) or 0),
        limit=limit,
    )


def ready_refined_entry_candidates(candidates: Iterable[RefinedEntryCandidate]) -> list[RefinedEntryCandidate]:
    return [candidate for candidate in candidates if candidate.decision == ENTRY_READY]


def refined_entry_signature(candidates: Iterable[RefinedEntryCandidate]) -> str:
    parts = [
        (
            candidate.symbol,
            candidate.action,
            candidate.side,
            round(candidate.take_profit_bps, 3),
            round(candidate.stop_loss_bps, 3),
            candidate.max_hold_bars,
            candidate.feature_time_ms,
        )
        for candidate in ready_refined_entry_candidates(candidates)
    ]
    return json.dumps(parts, ensure_ascii=False, sort_keys=True)


def refined_entry_notification_decision(
    candidates: Iterable[RefinedEntryCandidate],
    state: RefinedEntryNotifyState,
    *,
    force: bool = False,
) -> tuple[bool, str, str, list[RefinedEntryCandidate]]:
    ready = ready_refined_entry_candidates(candidates)
    signature = refined_entry_signature(ready)
    if not ready:
        return False, "현재 진입후보 없음", signature, []
    if force:
        return True, "강제 전송", signature, ready
    if signature != state.last_signature:
        return True, "현재장 정제 진입후보 변화", signature, ready
    return False, "변화 없음", signature, ready


def apply_refined_entry_notification_state(
    state: RefinedEntryNotifyState,
    *,
    signature: str,
) -> RefinedEntryNotifyState:
    return RefinedEntryNotifyState(last_signature=signature, last_sent_ms=now_ms())


def _candidate_from_result(
    row: MinedStrategyResult,
    feature: MarketFeatureSnapshot,
    config: TradingConfig,
) -> RefinedEntryCandidate:
    matches = mined_result_matches_feature(row, feature)
    risk_reward_ratio = _risk_reward_ratio(row.take_profit_bps, row.stop_loss_bps)
    payoff_ratio = float(row.test_summary.payoff_ratio)
    breakeven_win_rate = _breakeven_win_rate(payoff_ratio)
    win_rate_edge = row.test_summary.win_rate - breakeven_win_rate
    positive_window_ratio = _positive_window_ratio(row.positive_test_windows, row.selected_windows)
    quality_reasons = _quality_block_reasons(
        row,
        config,
        risk_reward_ratio=risk_reward_ratio,
        breakeven_win_rate=breakeven_win_rate,
        win_rate_edge=win_rate_edge,
        positive_window_ratio=positive_window_ratio,
    )
    if quality_reasons:
        decision = ENTRY_BLOCKED
        reason = "과거 손익비/기대값 기준 미달: " + "; ".join(quality_reasons[:3])
    elif matches:
        decision = ENTRY_READY
        reason = "현재 feature가 손익비 검증 생존 조건과 일치합니다."
    else:
        decision = ENTRY_WAIT
        reason = _mismatch_reason(row, feature)
    target = _target_price(feature.close, row.side, row.take_profit_bps)
    stop = _stop_price(feature.close, row.side, row.stop_loss_bps)
    return RefinedEntryCandidate(
        symbol=row.symbol,
        interval=row.interval,
        action=row.action,
        side=row.side,
        decision=decision,
        reason=reason,
        source_decision=row.decision,
        take_profit_bps=row.take_profit_bps,
        stop_loss_bps=row.stop_loss_bps,
        max_hold_bars=row.max_hold_bars,
        current_price=feature.close,
        target_price=target,
        stop_price=stop,
        feature_time_ms=feature.timestamp_ms,
        test_count=row.test_summary.count,
        test_win_rate=row.test_summary.win_rate,
        test_avg_pnl_bps=row.test_summary.avg_pnl_bps,
        test_profit_factor=row.test_summary.profit_factor,
        test_payoff_ratio=row.test_summary.payoff_ratio,
        full_avg_pnl_bps=row.full_summary.avg_pnl_bps,
        full_profit_factor=row.full_summary.profit_factor,
        risk_reward_ratio=risk_reward_ratio,
        breakeven_win_rate=breakeven_win_rate,
        win_rate_edge=win_rate_edge,
        positive_test_windows=row.positive_test_windows,
        selected_windows=row.selected_windows,
        positive_test_window_ratio=positive_window_ratio,
        feature_summary=_feature_summary(feature),
    )


def _entry_rank_key(row: RefinedEntryCandidate) -> tuple[int, float, float, int]:
    rank = {ENTRY_READY: 0, ENTRY_WAIT: 1, ENTRY_BLOCKED: 2}.get(row.decision, 3)
    return (
        rank,
        -row.test_avg_pnl_bps,
        -row.test_profit_factor,
        -row.test_count,
    )


def _mismatch_reason(row: MinedStrategyResult, feature: MarketFeatureSnapshot) -> str:
    return f"현재 feature가 조건 불일치: {_feature_summary(feature)}"


def _feature_summary(feature: MarketFeatureSnapshot) -> str:
    rsi = "n/a" if feature.rsi14 is None else f"{feature.rsi14:.1f}"
    bb = "n/a" if feature.bollinger_position is None else f"{feature.bollinger_position:.2f}"
    return (
        f"ema={feature.ema_gap_bps:+.1f}bps slope={feature.ema_slope_bps:+.1f}bps "
        f"4h={feature.trend_4h_bps:+.1f}bps 24h={feature.trend_24h_bps:+.1f}bps "
        f"RSI={rsi} BB={bb} vol={feature.volume_ratio:.2f} ATR={feature.atr_bps:.1f}bps "
        f"HB={feature.high_breakout} LB={feature.low_breakout}"
    )


def _target_price(entry_price: float, side: str, bps: float) -> float:
    ratio = bps / 10_000.0
    return entry_price * (1.0 + ratio) if side == "long" else entry_price * (1.0 - ratio)


def _stop_price(entry_price: float, side: str, bps: float) -> float:
    ratio = bps / 10_000.0
    return entry_price * (1.0 - ratio) if side == "long" else entry_price * (1.0 + ratio)


def _candidate_from_dict(row: dict[str, Any]) -> RefinedEntryCandidate:
    return RefinedEntryCandidate(
        symbol=str(row.get("symbol", "")),
        interval=str(row.get("interval", "")),
        action=str(row.get("action", "")),
        side=str(row.get("side", "")),
        decision=str(row.get("decision", "")),
        reason=str(row.get("reason", "")),
        source_decision=str(row.get("source_decision", "")),
        take_profit_bps=float(row.get("take_profit_bps", 0.0) or 0.0),
        stop_loss_bps=float(row.get("stop_loss_bps", 0.0) or 0.0),
        max_hold_bars=int(row.get("max_hold_bars", 0) or 0),
        current_price=float(row.get("current_price", 0.0) or 0.0),
        target_price=float(row.get("target_price", 0.0) or 0.0),
        stop_price=float(row.get("stop_price", 0.0) or 0.0),
        feature_time_ms=int(row.get("feature_time_ms", 0) or 0),
        test_count=int(row.get("test_count", 0) or 0),
        test_win_rate=float(row.get("test_win_rate", 0.0) or 0.0),
        test_avg_pnl_bps=float(row.get("test_avg_pnl_bps", 0.0) or 0.0),
        test_profit_factor=float(row.get("test_profit_factor", 0.0) or 0.0),
        test_payoff_ratio=float(row.get("test_payoff_ratio", 0.0) or 0.0),
        full_avg_pnl_bps=float(row.get("full_avg_pnl_bps", 0.0) or 0.0),
        full_profit_factor=float(row.get("full_profit_factor", 0.0) or 0.0),
        risk_reward_ratio=float(row.get("risk_reward_ratio", 0.0) or 0.0),
        breakeven_win_rate=float(row.get("breakeven_win_rate", 0.0) or 0.0),
        win_rate_edge=float(row.get("win_rate_edge", 0.0) or 0.0),
        positive_test_windows=int(row.get("positive_test_windows", 0) or 0),
        selected_windows=int(row.get("selected_windows", 0) or 0),
        positive_test_window_ratio=float(row.get("positive_test_window_ratio", 0.0) or 0.0),
        feature_summary=str(row.get("feature_summary", "")),
    )


def _decision_ko(decision: str) -> str:
    return {
        ENTRY_READY: "진입후보",
        ENTRY_WAIT: "대기",
        ENTRY_BLOCKED: "차단",
    }.get(decision, decision)


def _quality_block_reasons(
    row: MinedStrategyResult,
    config: TradingConfig,
    *,
    risk_reward_ratio: float,
    breakeven_win_rate: float,
    win_rate_edge: float,
    positive_window_ratio: float,
) -> list[str]:
    reasons: list[str] = []
    min_count = max(config.refined_entry_min_test_count, config.supervisor_min_samples // 5)
    if row.test_summary.count < min_count:
        reasons.append(f"테스트 표본 {row.test_summary.count} < {min_count}")
    if row.test_summary.avg_pnl_bps < config.refined_entry_min_avg_pnl_bps:
        reasons.append(
            f"테스트 평균 {row.test_summary.avg_pnl_bps:+.2f}bps < "
            f"{config.refined_entry_min_avg_pnl_bps:.2f}bps"
        )
    if row.full_summary.avg_pnl_bps < config.refined_entry_min_full_avg_pnl_bps:
        reasons.append(
            f"전체 평균 {row.full_summary.avg_pnl_bps:+.2f}bps < "
            f"{config.refined_entry_min_full_avg_pnl_bps:.2f}bps"
        )
    if row.test_summary.profit_factor < config.refined_entry_min_profit_factor:
        reasons.append(
            f"테스트 PF {row.test_summary.profit_factor:.2f} < "
            f"{config.refined_entry_min_profit_factor:.2f}"
        )
    if row.full_summary.profit_factor < config.refined_entry_min_full_profit_factor:
        reasons.append(
            f"전체 PF {row.full_summary.profit_factor:.2f} < "
            f"{config.refined_entry_min_full_profit_factor:.2f}"
        )
    if row.test_summary.payoff_ratio < config.refined_entry_min_payoff_ratio:
        reasons.append(
            f"테스트 손익비 {row.test_summary.payoff_ratio:.2f} < "
            f"{config.refined_entry_min_payoff_ratio:.2f}"
        )
    if risk_reward_ratio < config.refined_entry_min_risk_reward_ratio:
        reasons.append(
            f"목표/손절비 {risk_reward_ratio:.2f} < "
            f"{config.refined_entry_min_risk_reward_ratio:.2f}"
        )
    if win_rate_edge < config.refined_entry_min_win_rate_edge:
        reasons.append(
            f"승률여유 {win_rate_edge:+.1%} < "
            f"{config.refined_entry_min_win_rate_edge:.1%}"
        )
    if positive_window_ratio < config.refined_entry_min_positive_window_ratio:
        reasons.append(
            f"양수 테스트윈도우 {positive_window_ratio:.1%} < "
            f"{config.refined_entry_min_positive_window_ratio:.1%}"
        )
    return reasons


def _risk_reward_ratio(take_profit_bps: float, stop_loss_bps: float) -> float:
    if stop_loss_bps <= 0:
        return 0.0
    return take_profit_bps / stop_loss_bps


def _breakeven_win_rate(payoff_ratio: float) -> float:
    if payoff_ratio <= 0:
        return 1.0
    return 1.0 / (1.0 + payoff_ratio)


def _positive_window_ratio(positive_windows: int, selected_windows: int) -> float:
    if selected_windows <= 0:
        return 0.0
    return positive_windows / selected_windows
