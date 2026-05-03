from __future__ import annotations

from html import escape
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import time
from urllib.parse import parse_qs, urlparse

from cointrading.config import TradingConfig
from cointrading.market_regime import macro_regime_ko, trade_bias_ko
from cointrading.meta_strategy import (
    default_meta_report_path,
    load_meta_report,
    meta_action_ko,
)
from cointrading.refined_entry_gate import (
    default_refined_entry_report_path,
    load_refined_entry_report,
)
from cointrading.risk_state import evaluate_runtime_risk, risk_mode_ko
from cointrading.research_probe import default_probe_report_path, load_probe_report
from cointrading.scalping import scalp_report_rows_text
from cointrading.storage import TradingStore, default_db_path, kst_from_ms, now_ms
from cointrading.strategy_miner import (
    default_strategy_mine_report_path,
    default_strategy_refine_report_path,
    load_strategy_mine_report,
    load_strategy_refine_report,
    strategy_action_ko,
)
from cointrading.strategy_notify import (
    MODE_LABELS,
    REGIME_LABELS,
    REASON_LABELS,
    SIDE_LABELS,
    SOURCE_LABELS,
    STATUS_LABELS,
    STRATEGY_LABELS,
    strategy_family_label,
)
from cointrading.tactical_radar import (
    default_tactical_radar_report_path,
    load_tactical_radar_report,
)


DEFAULT_DASHBOARD_ROW_LIMIT = 200
MAX_DASHBOARD_ROW_LIMIT = 1000


def run_dashboard(host: str = "127.0.0.1", port: int = 8080, db_path: Path | None = None) -> None:
    store_path = db_path or default_db_path()
    config = TradingConfig.from_env()
    auth_token = os.getenv("COINTRADING_DASHBOARD_AUTH_TOKEN", "").strip()

    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def do_GET(self) -> None:
            parsed = urlparse(self.path)
            if parsed.path not in {"/", "/signals", "/orders", "/events"}:
                self.send_error(404)
                return
            query = parse_qs(parsed.query)
            if not _is_authorized(self.headers.get("Authorization", ""), query, auth_token):
                self._send_text(401, "Unauthorized\n")
                return
            symbol = query.get("symbol", [None])[0]
            limit = _dashboard_limit(query)
            if parsed.path == "/events":
                self._send_events(store_path, config, symbol, limit)
                return
            store = TradingStore(store_path)
            body = _page(_snapshot(store, config, symbol, limit), config)
            payload = body.encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(payload)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(payload)

        def _send_events(
            self,
            store_path: Path,
            config: TradingConfig,
            symbol: str | None,
            limit: int,
        ) -> None:
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream; charset=utf-8")
            self.send_header("Cache-Control", "no-store")
            self.send_header("Connection", "keep-alive")
            self.end_headers()
            while True:
                try:
                    payload = json.dumps(
                        _snapshot(TradingStore(store_path), config, symbol, limit),
                        ensure_ascii=False,
                    )
                    self.wfile.write(f"data: {payload}\n\n".encode("utf-8"))
                    self.wfile.flush()
                    time.sleep(5)
                except (BrokenPipeError, ConnectionResetError):
                    return

        def _send_text(self, status: int, text: str) -> None:
            payload = text.encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.send_header("Content-Length", str(len(payload)))
            if status == 401:
                self.send_header("WWW-Authenticate", "Bearer")
            self.end_headers()
            self.wfile.write(payload)

        def log_message(self, format: str, *args) -> None:
            return

    server = ThreadingHTTPServer((host, port), Handler)
    print(f"Dashboard: http://{host}:{port}")
    server.serve_forever()


def _is_authorized(
    authorization_header: str,
    query: dict[str, list[str]],
    auth_token: str,
) -> bool:
    if not auth_token:
        return True
    if query.get("token", [""])[0] == auth_token:
        return True
    return authorization_header.strip() == f"Bearer {auth_token}"


def _dashboard_limit(query: dict[str, list[str]]) -> int:
    raw_limit = query.get("limit", [""])[0]
    if not raw_limit:
        return DEFAULT_DASHBOARD_ROW_LIMIT
    try:
        limit = int(raw_limit)
    except ValueError:
        return DEFAULT_DASHBOARD_ROW_LIMIT
    return max(1, min(limit, MAX_DASHBOARD_ROW_LIMIT))


def _snapshot(
    store: TradingStore,
    config: TradingConfig,
    symbol: str | None,
    limit: int = DEFAULT_DASHBOARD_ROW_LIMIT,
) -> dict[str, str]:
    rows = store.list_signals(symbol=symbol, symbols=config.scalp_symbols if not symbol else None)
    report = scalp_report_rows_text(
        rows,
        symbol=symbol,
        symbols=config.scalp_symbols if not symbol else None,
    )
    risk_state = evaluate_runtime_risk(store, config)
    scalp_cycles = store.recent_scalp_cycles(limit=limit)
    strategy_cycles = store.recent_strategy_cycles(limit=limit)
    active_scalp_cycles = [row for row in scalp_cycles if _is_active_status(row["status"])]
    active_strategy_cycles = [row for row in strategy_cycles if _is_active_status(row["status"])]
    strategy_rows = store.latest_strategy_evaluations(limit=limit)
    latest_strategy_batch = store.latest_strategy_batch()
    market_regime_rows = store.latest_market_regimes(symbols=config.scalp_symbols, limit=limit)
    market_context_rows = store.latest_market_contexts(symbols=config.scalp_symbols, limit=limit)
    price_by_symbol = _price_by_symbol(market_context_rows)
    scalp_performance = store.scalp_cycle_performance()
    strategy_performance = store.strategy_cycle_performance()
    scalp_exit_reasons = store.scalp_cycle_exit_reasons()
    strategy_exit_reasons = store.strategy_cycle_exit_reasons()
    probe_report = load_probe_report(default_probe_report_path()) or {}
    meta_report = load_meta_report(default_meta_report_path()) or {}
    mine_report = load_strategy_mine_report(default_strategy_mine_report_path()) or {}
    refine_report = load_strategy_refine_report(default_strategy_refine_report_path()) or {}
    refined_entry_report = load_refined_entry_report(default_refined_entry_report_path()) or {}
    tactical_radar_report = load_tactical_radar_report(default_tactical_radar_report_path()) or {}
    active_unrealized = _active_unrealized_total(
        active_scalp_cycles,
        active_strategy_cycles,
        price_by_symbol,
    )
    paper_rows = _paper_rows_html(scalp_cycles, strategy_cycles, price_by_symbol)
    return {
        "generated_at": kst_from_ms(now_ms()),
        "row_limit": str(limit),
        "mode_summary": _mode_summary_html(config),
        "risk_state": risk_state.to_text(),
        "overview": _overview_html(
            config=config,
            risk_state=risk_state,
            active_scalp_cycles=active_scalp_cycles,
            active_strategy_cycles=active_strategy_cycles,
            active_unrealized=active_unrealized,
            scalp_performance=scalp_performance,
            strategy_performance=strategy_performance,
            latest_strategy_batch=latest_strategy_batch,
        ),
        "active_paper_rows": _active_paper_rows_html(
            active_scalp_cycles,
            active_strategy_cycles,
            price_by_symbol,
        ),
        "paper_rows": paper_rows,
        "paper_summary": _paper_summary_html(
            active_scalp_cycles=active_scalp_cycles,
            active_strategy_cycles=active_strategy_cycles,
            active_unrealized=active_unrealized,
            scalp_performance=scalp_performance,
            strategy_performance=strategy_performance,
            scalp_exit_reasons=scalp_exit_reasons,
            strategy_exit_reasons=strategy_exit_reasons,
        ),
        "strategy_summary": _strategy_summary_html(latest_strategy_batch),
        "tactical_radar_summary": _tactical_radar_summary_html(tactical_radar_report),
        "meta_summary": _meta_summary_html(meta_report),
        "mine_summary": _mine_summary_html(mine_report),
        "refine_summary": _refine_summary_html(refine_report),
        "refined_entry_summary": _refined_entry_summary_html(refined_entry_report),
        "probe_summary": _probe_summary_html(probe_report),
        "report": report,
        "signal_rows": _signal_rows_html(rows[-limit:]) or _empty_table_row(5, "최근 신호 없음"),
        "order_rows": _order_rows_html(store.recent_orders(limit=limit)) or _empty_table_row(5, "최근 주문/차단 없음"),
        "cycle_rows": _cycle_rows_html(scalp_cycles) or _empty_table_row(6, "스캘핑 상태머신 기록 없음"),
        "strategy_cycle_rows": _strategy_cycle_rows_html(strategy_cycles) or _empty_table_row(7, "전략 상태머신 기록 없음"),
        "strategy_rows": _strategy_rows_html(strategy_rows) or _empty_table_row(16, "전략 평가 결과 없음"),
        "tactical_radar_rows": _tactical_radar_rows_html(tactical_radar_report) or _empty_table_row(13, "전술 레이더 결과 없음"),
        "meta_rows": _meta_rows_html(meta_report) or _empty_table_row(13, "메타전략 백테스트 결과 없음"),
        "meta_action_rows": _meta_action_rows_html(meta_report) or _empty_table_row(7, "메타전략 행동별 결과 없음"),
        "refined_entry_rows": _refined_entry_rows_html(refined_entry_report) or _empty_table_row(18, "현재장 정제후보 결과 없음"),
        "refine_rows": _mine_rows_html(refine_report) or _empty_table_row(12, "전략 정제 결과 없음"),
        "mine_rows": _mine_rows_html(mine_report) or _empty_table_row(12, "전략 발굴 결과 없음"),
        "probe_rows": _probe_rows_html(probe_report) or _empty_table_row(13, "리서치 프로브 결과 없음"),
        "market_regime_rows": _market_regime_rows_html(market_regime_rows) or _empty_table_row(10, "장세 라우터 기록 없음"),
        "market_context_rows": _market_context_rows_html(market_context_rows) or _empty_table_row(8, "시장상황 기록 없음"),
        "performance_rows": _performance_rows_html(scalp_performance) or _empty_table_row(12, "스캘핑 종료 표본 없음"),
        "strategy_performance_rows": _strategy_performance_rows_html(strategy_performance) or _empty_table_row(13, "전략 종료 표본 없음"),
        "exit_reason_rows": _exit_reason_rows_html(scalp_exit_reasons) or _empty_table_row(6, "스캘핑 종료 사유 없음"),
        "strategy_exit_reason_rows": _strategy_exit_reason_rows_html(strategy_exit_reasons) or _empty_table_row(6, "전략 종료 사유 없음"),
    }


def _overview_html(
    *,
    config: TradingConfig,
    risk_state,
    active_scalp_cycles,
    active_strategy_cycles,
    active_unrealized,
    scalp_performance,
    strategy_performance,
    latest_strategy_batch,
) -> str:
    active_count = len(active_scalp_cycles) + len(active_strategy_cycles)
    scalp_sum = sum(float(row["sum_pnl"] or 0.0) for row in scalp_performance)
    strategy_sum = sum(float(row["sum_pnl"] or 0.0) for row in strategy_performance)
    approved = [row for row in latest_strategy_batch if str(row["decision"]) == "APPROVED"]
    sample_low = [row for row in latest_strategy_batch if str(row["decision"]) == "SAMPLE_LOW"]
    blocked = [row for row in latest_strategy_batch if str(row["decision"]) == "BLOCKED"]
    live_locked = config.dry_run or not config.live_trading_enabled
    rows = [
        _metric_html("실전", "잠김" if live_locked else "가능 플래그 ON", "good" if live_locked else "warn"),
        _metric_html("위험모드", _risk_mode_line(risk_state), "good" if risk_state.allows_new_entries else "block"),
        _metric_html("진행 중 Paper", str(active_count), "warn" if active_count else "good"),
        _metric_html("미실현 손익", f"{active_unrealized:+.6f}", _pnl_tone(active_unrealized)),
        _metric_html("Paper 손익", f"{scalp_sum + strategy_sum:+.6f}", _pnl_tone(scalp_sum + strategy_sum)),
        _metric_html("승인 후보", str(len(approved)), "good" if approved else "warn"),
        _metric_html("표본부족/차단", f"{len(sample_low)} / {len(blocked)}", "warn" if sample_low else "muted"),
    ]
    return "\n".join(rows)


def _mode_summary_html(config: TradingConfig) -> str:
    items = [
        ("dry-run", "ON" if config.dry_run else "OFF", "good" if config.dry_run else "warn"),
        ("live", "ON" if config.live_trading_enabled else "OFF", "warn" if config.live_trading_enabled else "good"),
        (
            "scalp live",
            "ON" if config.live_scalp_lifecycle_enabled else "OFF",
            "warn" if config.live_scalp_lifecycle_enabled else "good",
        ),
        (
            "strategy live",
            "ON" if config.live_strategy_lifecycle_enabled else "OFF",
            "warn" if config.live_strategy_lifecycle_enabled else "good",
        ),
        (
            "one-shot",
            "ON" if config.live_one_shot_enabled else "OFF",
            "warn" if config.live_one_shot_enabled else "good",
        ),
    ]
    return "\n".join(
        f'<span class="chip {_tone_class(tone)}">{escape(label)} {escape(value)}</span>'
        for label, value, tone in items
    )


def _metric_html(label: str, value: str, tone: str = "muted") -> str:
    return (
        f'<div class="metric {_tone_class(tone)}">'
        f'<span class="metric-label">{escape(label)}</span>'
        f'<strong>{escape(value)}</strong>'
        "</div>"
    )


def _active_paper_rows_html(scalp_cycles, strategy_cycles, price_by_symbol: dict[str, float]) -> str:
    rows = [
        *_cycle_table_rows(scalp_cycles, cycle_type="스캘핑", price_by_symbol=price_by_symbol),
        *_cycle_table_rows(strategy_cycles, cycle_type="전략", price_by_symbol=price_by_symbol),
    ]
    if not rows:
        return '<tr><td colspan="14" class="empty">진행 중인 paper 사이클 없음</td></tr>'
    ranked = sorted(rows, key=lambda item: item[0], reverse=True)
    return "\n".join(row for _, row in ranked)


def _paper_rows_html(scalp_cycles, strategy_cycles, price_by_symbol: dict[str, float]) -> str:
    rows = [
        *_cycle_table_rows(scalp_cycles, cycle_type="스캘핑", price_by_symbol=price_by_symbol),
        *_cycle_table_rows(strategy_cycles, cycle_type="전략", price_by_symbol=price_by_symbol),
    ]
    if not rows:
        return '<tr><td colspan="14" class="empty">paper 사이클 기록 없음</td></tr>'
    ranked = sorted(rows, key=lambda item: item[0], reverse=True)
    return "\n".join(row for _, row in ranked)


def _cycle_table_rows(
    cycles,
    *,
    cycle_type: str,
    price_by_symbol: dict[str, float],
) -> list[tuple[int, str]]:
    output: list[tuple[int, str]] = []
    for cycle in cycles:
        updated_ms = int(cycle["updated_ms"])
        strategy = _row_get(cycle, "strategy", "maker_scalp") if cycle_type == "전략" else "maker_scalp"
        current_price = _current_price_for_cycle(cycle, price_by_symbol)
        unrealized_pnl = _unrealized_pnl(cycle, current_price, cycle_type=cycle_type)
        entry_price = _fmt_price(cycle["entry_price"])
        current_price_text = _fmt_price(current_price)
        target_price = _fmt_price(cycle["target_price"])
        stop_price = _fmt_price(cycle["stop_price"])
        quantity = _fmt_qty(cycle["quantity"])
        unrealized_text = _fmt_signed_pnl(unrealized_pnl)
        pnl = _fmt_signed_pnl(cycle["realized_pnl"])
        status = str(cycle["status"])
        reason = str(cycle["reason"] or "")
        row = (
            f'<tr class="{_status_row_class(status)}">'
            f"<td>{escape(kst_from_ms(updated_ms))}</td>"
            f"<td>{escape(cycle_type)}</td>"
            f"<td>{escape(_strategy_label(str(strategy)))}</td>"
            f"<td>{escape(str(cycle['symbol']))}</td>"
            f"<td>{escape(_side_label(str(cycle['side'])))}</td>"
            f"<td>{_status_pill(status)}</td>"
            f"<td>{escape(quantity)}</td>"
            f"<td>{escape(entry_price)}</td>"
            f"<td>{escape(current_price_text)}</td>"
            f"<td>{escape(target_price)}</td>"
            f"<td>{escape(stop_price)}</td>"
            f'<td class="{_pnl_cell_class(unrealized_pnl)}">{escape(unrealized_text)}</td>'
            f'<td class="{_pnl_cell_class(cycle["realized_pnl"])}">{escape(pnl)}</td>'
            f"<td>{escape(_reason_label(reason))}</td>"
            "</tr>"
        )
        output.append((updated_ms, row))
    return output


def _price_by_symbol(rows) -> dict[str, float]:
    prices: dict[str, float] = {}
    for row in rows:
        symbol = _row_get(row, "symbol")
        mark_price = _row_get(row, "mark_price")
        if not symbol or mark_price is None:
            continue
        prices[str(symbol).upper()] = float(mark_price)
    return prices


def _current_price_for_cycle(cycle, price_by_symbol: dict[str, float]) -> float | None:
    symbol = str(_row_get(cycle, "symbol", "")).upper()
    if symbol in price_by_symbol:
        return price_by_symbol[symbol]
    return _float_or_none(_row_get(cycle, "last_mid_price"))


def _active_unrealized_total(
    scalp_cycles,
    strategy_cycles,
    price_by_symbol: dict[str, float],
) -> float:
    values = [
        *(
            _unrealized_pnl(cycle, _current_price_for_cycle(cycle, price_by_symbol), cycle_type="스캘핑")
            for cycle in scalp_cycles
        ),
        *(
            _unrealized_pnl(cycle, _current_price_for_cycle(cycle, price_by_symbol), cycle_type="전략")
            for cycle in strategy_cycles
        ),
    ]
    return sum(value for value in values if value is not None)


def _unrealized_pnl(cycle, current_price: float | None, *, cycle_type: str) -> float | None:
    status = str(_row_get(cycle, "status", ""))
    if status not in {"OPEN", "EXIT_SUBMITTED"}:
        return None
    entry_price = _float_or_none(_row_get(cycle, "entry_price"))
    quantity = _float_or_none(_row_get(cycle, "quantity"))
    if current_price is None or entry_price is None or quantity is None:
        return None
    side = str(_row_get(cycle, "side", "")).lower()
    if side == "long":
        gross = (current_price - entry_price) * quantity
    elif side == "short":
        gross = (entry_price - current_price) * quantity
    else:
        return None
    entry_fee_bps = _entry_fee_bps(cycle, cycle_type=cycle_type)
    exit_fee_bps = float(_row_get(cycle, "taker_one_way_bps", 0.0) or 0.0)
    fees = _fee_amount(entry_price, quantity, entry_fee_bps) + _fee_amount(
        current_price,
        quantity,
        exit_fee_bps,
    )
    return gross - fees


def _entry_fee_bps(cycle, *, cycle_type: str) -> float:
    if cycle_type == "전략":
        entry_order_type = str(_row_get(cycle, "entry_order_type", "")).upper()
        if entry_order_type == "MARKET":
            return float(_row_get(cycle, "taker_one_way_bps", 0.0) or 0.0)
    return float(_row_get(cycle, "maker_one_way_bps", 0.0) or 0.0)


def _fee_amount(price: float, quantity: float, fee_bps: float) -> float:
    return abs(price * quantity) * fee_bps / 10_000.0


def _row_get(row, key: str, default=None):
    if isinstance(row, dict):
        return row.get(key, default)
    try:
        return row[key]
    except (IndexError, KeyError, TypeError):
        return default


def _float_or_none(value) -> float | None:
    if value is None or value == "":
        return None
    return float(value)


def _paper_summary_html(
    *,
    active_scalp_cycles,
    active_strategy_cycles,
    active_unrealized,
    scalp_performance,
    strategy_performance,
    scalp_exit_reasons,
    strategy_exit_reasons,
) -> str:
    active_count = len(active_scalp_cycles) + len(active_strategy_cycles)
    scalp_sum = sum(float(row["sum_pnl"] or 0.0) for row in scalp_performance)
    strategy_sum = sum(float(row["sum_pnl"] or 0.0) for row in strategy_performance)
    scalp_closed = sum(int(row["count"] or 0) for row in scalp_performance)
    strategy_closed = sum(int(row["count"] or 0) for row in strategy_performance)
    worst_reasons = sorted(
        [*scalp_exit_reasons, *strategy_exit_reasons],
        key=lambda row: float(row["sum_pnl"] or 0.0),
    )
    reason_text = "없음"
    if worst_reasons:
        reason = worst_reasons[0]["reason"] or worst_reasons[0]["status"]
        reason_text = f"{_reason_label(str(reason))} ({_fmt_pnl(worst_reasons[0]['sum_pnl'])})"
    return "\n".join(
        [
            _metric_html("진행 중", str(active_count), "warn" if active_count else "good"),
            _metric_html("미실현 손익", f"{active_unrealized:+.6f}", _pnl_tone(active_unrealized)),
            _metric_html("종료 표본", f"{scalp_closed + strategy_closed}", "muted"),
            _metric_html("스캘핑 합계", f"{scalp_sum:+.6f}", _pnl_tone(scalp_sum)),
            _metric_html("전략 합계", f"{strategy_sum:+.6f}", _pnl_tone(strategy_sum)),
            _metric_html("최악 종료사유", reason_text, "warn" if worst_reasons else "muted"),
        ]
    )


def _strategy_summary_html(rows) -> str:
    rows = list(rows)
    if not rows:
        return _metric_html("전략 평가", "없음", "warn")
    approved = [row for row in rows if str(row["decision"]) == "APPROVED"]
    sample_low = [row for row in rows if str(row["decision"]) == "SAMPLE_LOW"]
    blocked = [row for row in rows if str(row["decision"]) == "BLOCKED"]
    best = sorted(
        approved,
        key=lambda row: (-float(row["avg_pnl_bps"] or 0.0), -int(row["sample_count"] or 0)),
    )
    best_text = "없음"
    tone = "warn"
    if best:
        row = best[0]
        best_text = (
            f"{row['symbol']} {_side_label(str(row['side']))} "
            f"{strategy_family_label(row)} "
            f"{float(row['avg_pnl_bps']):+.3f}bps"
        )
        tone = "good"
    return "\n".join(
        [
            _metric_html("승인", str(len(approved)), "good" if approved else "warn"),
            _metric_html("표본부족", str(len(sample_low)), "warn" if sample_low else "muted"),
            _metric_html("차단", str(len(blocked)), "block" if blocked else "muted"),
            _metric_html("상위 후보", best_text, tone),
        ]
    )


def _meta_summary_html(report: dict) -> str:
    rows = list(report.get("results", []) or [])
    if not rows:
        return _metric_html("메타전략", "결과 없음", "warn")
    ready = [row for row in rows if str(row.get("decision")) == "PAPER_READY"]
    observe = [row for row in rows if str(row.get("decision")) == "OBSERVE"]
    blocked = [row for row in rows if str(row.get("decision")) == "BLOCKED"]
    generated_ms = int(report.get("generated_ms", 0) or 0)
    ranked = sorted(rows, key=lambda row: float(row.get("avg_pnl_bps", 0.0) or 0.0), reverse=True)
    best = ranked[0] if ranked else None
    best_text = "없음"
    best_tone = "warn"
    if best is not None:
        best_text = (
            f"{best.get('symbol', '')} "
            f"{float(best.get('avg_pnl_bps', 0.0) or 0.0):+.2f}bps "
            f"PF={float(best.get('profit_factor', 0.0) or 0.0):.2f}"
        )
        best_tone = "good" if str(best.get("decision")) == "PAPER_READY" else "warn"
    return "\n".join(
        [
            _metric_html("최근 실행", kst_from_ms(generated_ms) if generated_ms else "없음", "muted"),
            _metric_html("Paper 후보", str(len(ready)), "good" if ready else "warn"),
            _metric_html("관찰/차단", f"{len(observe)} / {len(blocked)}", "warn" if observe else "muted"),
            _metric_html("상위 정책", best_text, best_tone),
        ]
    )


def _mine_summary_html(report: dict) -> str:
    rows = list(report.get("results", []) or [])
    if not rows:
        return _metric_html("전략 발굴", "결과 없음", "warn")
    survived = [row for row in rows if str(row.get("decision")) == "SURVIVED"]
    watch = [row for row in rows if str(row.get("decision")) == "WATCH"]
    generated_ms = int(report.get("generated_ms", 0) or 0)
    ranked = sorted(
        rows,
        key=lambda row: float((row.get("test_summary") or {}).get("avg_pnl_bps", 0.0) or 0.0),
        reverse=True,
    )
    best = ranked[0]
    best_summary = best.get("test_summary") or {}
    best_text = (
        f"{best.get('symbol', '')} {strategy_action_ko(str(best.get('action', '')))} "
        f"{float(best_summary.get('avg_pnl_bps', 0.0) or 0.0):+.2f}bps"
    )
    return "\n".join(
        [
            _metric_html("최근 실행", kst_from_ms(generated_ms) if generated_ms else "없음", "muted"),
            _metric_html("생존", str(len(survived)), "good" if survived else "warn"),
            _metric_html("관찰", str(len(watch)), "warn" if watch else "muted"),
            _metric_html("상위 후보", best_text, "good" if survived else "warn"),
        ]
    )


def _refine_summary_html(report: dict) -> str:
    rows = list(report.get("results", []) or [])
    if not rows:
        return _metric_html("전략 2차 정제", "결과 없음", "warn")
    survived = [row for row in rows if str(row.get("decision")) == "SURVIVED"]
    watch = [row for row in rows if str(row.get("decision")) == "WATCH"]
    generated_ms = int(report.get("generated_ms", 0) or 0)
    source_count = int(report.get("source_count", 0) or 0)
    ranked = sorted(
        rows,
        key=lambda row: float((row.get("test_summary") or {}).get("avg_pnl_bps", 0.0) or 0.0),
        reverse=True,
    )
    best = ranked[0]
    best_summary = best.get("test_summary") or {}
    best_text = (
        f"{best.get('symbol', '')} {strategy_action_ko(str(best.get('action', '')))} "
        f"{float(best_summary.get('avg_pnl_bps', 0.0) or 0.0):+.2f}bps"
    )
    return "\n".join(
        [
            _metric_html("최근 실행", kst_from_ms(generated_ms) if generated_ms else "없음", "muted"),
            _metric_html("정제 대상", str(source_count), "muted"),
            _metric_html("생존", str(len(survived)), "good" if survived else "warn"),
            _metric_html("관찰", str(len(watch)), "warn" if watch else "muted"),
            _metric_html("상위 정제", best_text, "good" if survived else "warn"),
        ]
    )


def _refined_entry_summary_html(report: dict) -> str:
    rows = list(report.get("candidates", []) or [])
    if not rows:
        return _metric_html("현재장 진입후보", "결과 없음", "warn")
    ready = [row for row in rows if str(row.get("decision")) == "READY"]
    wait = [row for row in rows if str(row.get("decision")) == "WAIT"]
    observe = [row for row in rows if str(row.get("decision")) == "OBSERVE"]
    generated_ms = int(report.get("generated_ms", 0) or 0)
    best = rows[0]
    best_text = (
        f"{best.get('symbol', '')} {strategy_action_ko(str(best.get('action', '')))} "
        f"{float(best.get('test_avg_pnl_bps', 0.0) or 0.0):+.2f}bps"
    )
    return "\n".join(
        [
            _metric_html("최근 실행", kst_from_ms(generated_ms) if generated_ms else "없음", "muted"),
            _metric_html("진입후보", str(len(ready)), "good" if ready else "warn"),
            _metric_html("근접대기", str(len(wait)), "warn" if wait else "muted"),
            _metric_html("먼관찰", str(len(observe)), "muted"),
            _metric_html("상위 매칭", best_text, "good" if ready else "warn"),
        ]
    )


def _tactical_radar_summary_html(report: dict) -> str:
    rows = list(report.get("signals", []) or [])
    if not rows:
        return _metric_html("전술 레이더", "결과 없음", "warn")
    ready = [row for row in rows if str(row.get("decision")) == "READY"]
    near = [row for row in rows if str(row.get("decision")) == "NEAR"]
    watch = [row for row in rows if str(row.get("decision")) == "WATCH"]
    avoid = [row for row in rows if str(row.get("decision")) == "AVOID"]
    generated_ms = int(report.get("generated_ms", 0) or 0)
    best = rows[0]
    best_text = (
        f"{best.get('symbol', '')} {_scenario_label(str(best.get('scenario', '')))} "
        f"{_decision_label(str(best.get('decision', '')))}"
    )
    return "\n".join(
        [
            _metric_html("최근 실행", kst_from_ms(generated_ms) if generated_ms else "없음", "muted"),
            _metric_html("진입가능", str(len(ready)), "good" if ready else "muted"),
            _metric_html("근접/감시", f"{len(near)} / {len(watch)}", "warn" if near or watch else "muted"),
            _metric_html("관망", str(len(avoid)), "muted"),
            _metric_html("상위 전술", best_text, "good" if ready else "warn" if near or watch else "muted"),
        ]
    )


def _probe_summary_html(report: dict) -> str:
    rows = list(report.get("results", []) or [])
    if not rows:
        return _metric_html("리서치 프로브", "결과 없음", "warn")
    approved = [row for row in rows if str(row.get("decision")) == "APPROVED"]
    watch = [row for row in rows if str(row.get("decision")) == "WATCH"]
    blocked = [row for row in rows if str(row.get("decision")) == "BLOCKED"]
    generated_ms = int(report.get("generated_ms", 0) or 0)
    ranked = sorted(rows, key=lambda row: float(row.get("avg_pnl_bps", 0.0) or 0.0), reverse=True)
    best = ranked[0] if ranked else None
    best_text = "없음"
    best_tone = "warn"
    if best is not None:
        best_text = (
            f"{best.get('symbol', '')} {_strategy_label(str(best.get('strategy', '')))} "
            f"{float(best.get('avg_pnl_bps', 0.0) or 0.0):+.2f}bps"
        )
        best_tone = "good" if str(best.get("decision")) == "APPROVED" else "warn"
    return "\n".join(
        [
            _metric_html("최근 실행", kst_from_ms(generated_ms) if generated_ms else "없음", "muted"),
            _metric_html("승인", str(len(approved)), "good" if approved else "warn"),
            _metric_html("관찰/차단", f"{len(watch)} / {len(blocked)}", "warn" if watch else "muted"),
            _metric_html("상위 결과", best_text, best_tone),
        ]
    )


def _meta_rows_html(report: dict) -> str:
    rows = list(report.get("results", []) or [])
    ranked = sorted(rows, key=lambda row: float(row.get("avg_pnl_bps", 0.0) or 0.0), reverse=True)
    return "\n".join(
        "<tr>"
        f"<td>{_decision_pill(str(row.get('decision', '')))}</td>"
        f"<td>{escape(str(row.get('symbol', '')))}</td>"
        f"<td>{escape(str(row.get('interval', '')))}</td>"
        f"<td>{escape(kst_from_ms(int(row.get('start_ms', 0) or 0)))}</td>"
        f"<td>{escape(kst_from_ms(int(row.get('end_ms', 0) or 0)))}</td>"
        f"<td>{int(row.get('sample_bars', 0) or 0)}</td>"
        f"<td>{int(row.get('trade_count', 0) or 0)}</td>"
        f"<td>{escape(_fmt_pct(float(row.get('win_rate', 0.0) or 0.0)))}</td>"
        f"<td>{float(row.get('avg_pnl_bps', 0.0) or 0.0):+.2f}</td>"
        f"<td>{float(row.get('sum_pnl', 0.0) or 0.0):+.6f}</td>"
        f"<td>{float(row.get('profit_factor', 0.0) or 0.0):.2f}</td>"
        f"<td>{escape(_fmt_pct(float(row.get('max_drawdown_pct', 0.0) or 0.0)))}</td>"
        f"<td>{escape(str(row.get('reason', '')))}</td>"
        "</tr>"
        for row in ranked
    )


def _meta_action_rows_html(report: dict) -> str:
    rows = []
    for result in report.get("results", []) or []:
        symbol = str(result.get("symbol", ""))
        for action in result.get("action_summaries", []) or []:
            rows.append((symbol, action))
    ranked = sorted(rows, key=lambda item: float(item[1].get("avg_pnl_bps", 0.0) or 0.0), reverse=True)
    return "\n".join(
        "<tr>"
        f"<td>{escape(symbol)}</td>"
        f"<td>{escape(meta_action_ko(str(action.get('key', ''))))}</td>"
        f"<td>{int(action.get('count', 0) or 0)}</td>"
        f"<td>{escape(_fmt_pct(float(action.get('win_rate', 0.0) or 0.0)))}</td>"
        f"<td>{float(action.get('avg_pnl_bps', 0.0) or 0.0):+.2f}</td>"
        f"<td>{float(action.get('profit_factor', 0.0) or 0.0):.2f}</td>"
        f"<td>{float(action.get('sum_pnl', 0.0) or 0.0):+.6f}</td>"
        "</tr>"
        for symbol, action in ranked
    )


def _mine_rows_html(report: dict) -> str:
    rows = list(report.get("results", []) or [])
    return "\n".join(
        "<tr>"
        f"<td>{_decision_pill(str(row.get('decision', '')))}</td>"
        f"<td>{escape(str(row.get('symbol', '')))}</td>"
        f"<td>{escape(strategy_action_ko(str(row.get('action', ''))))}</td>"
        f"<td>{float(row.get('take_profit_bps', 0.0) or 0.0):.0f}</td>"
        f"<td>{float(row.get('stop_loss_bps', 0.0) or 0.0):.0f}</td>"
        f"<td>{int(row.get('max_hold_bars', 0) or 0)}</td>"
        f"<td>{int(row.get('positive_test_windows', 0) or 0)}/{int(row.get('selected_windows', 0) or 0)}</td>"
        f"<td>{int((row.get('test_summary') or {}).get('count', 0) or 0)}</td>"
        f"<td>{float((row.get('test_summary') or {}).get('avg_pnl_bps', 0.0) or 0.0):+.2f}</td>"
        f"<td>{float((row.get('test_summary') or {}).get('profit_factor', 0.0) or 0.0):.2f}</td>"
        f"<td>{float((row.get('full_summary') or {}).get('avg_pnl_bps', 0.0) or 0.0):+.2f}</td>"
        f"<td>{escape(str(row.get('reason', '')))}</td>"
        "</tr>"
        for row in rows
    )


def _refined_entry_rows_html(report: dict) -> str:
    rows = list(report.get("candidates", []) or [])
    return "\n".join(
        "<tr>"
        f"<td>{_decision_pill(str(row.get('decision', '')))}</td>"
        f"<td>{escape(str(row.get('symbol', '')))}</td>"
        f"<td>{escape(strategy_action_ko(str(row.get('action', ''))))}</td>"
        f"<td>{escape(_side_label(str(row.get('side', ''))))}</td>"
        f"<td>{escape(kst_from_ms(int(row.get('feature_time_ms', 0) or 0)))}</td>"
        f"<td>{_fmt_price(row.get('current_price'))}</td>"
        f"<td>{_fmt_price(row.get('target_price'))}</td>"
        f"<td>{_fmt_price(row.get('stop_price'))}</td>"
        f"<td>{float(row.get('take_profit_bps', 0.0) or 0.0):.0f}</td>"
        f"<td>{float(row.get('stop_loss_bps', 0.0) or 0.0):.0f}</td>"
        f"<td>{int(row.get('max_hold_bars', 0) or 0)}</td>"
        f"<td>{int(row.get('test_count', 0) or 0)}</td>"
        f"<td>{escape(_fmt_pct(float(row.get('test_win_rate', 0.0) or 0.0)))}</td>"
        f"<td>{float(row.get('test_avg_pnl_bps', 0.0) or 0.0):+.2f}</td>"
        f"<td>{float(row.get('test_profit_factor', 0.0) or 0.0):.2f}</td>"
        f"<td>{float(row.get('test_payoff_ratio', 0.0) or 0.0):.2f}</td>"
        f"<td>{escape(_fmt_pct(float(row.get('win_rate_edge', 0.0) or 0.0)))}</td>"
        f"<td>{escape(str(row.get('reason', '')))}</td>"
        "</tr>"
        for row in rows
    )


def _tactical_radar_rows_html(report: dict) -> str:
    rows = list(report.get("signals", []) or [])
    return "\n".join(
        "<tr>"
        f"<td>{_decision_pill(str(row.get('decision', '')))}</td>"
        f"<td>{escape(str(row.get('symbol', '')))}</td>"
        f"<td>{escape(_scenario_label(str(row.get('scenario', ''))))}</td>"
        f"<td>{escape(_side_label(str(row.get('side', ''))))}</td>"
        f"<td>{escape(kst_from_ms(int(row.get('timestamp_ms', 0) or 0)))}</td>"
        f"<td>{_fmt_price(row.get('current_price'))}</td>"
        f"<td>{_fmt_price(row.get('trigger_price'))}</td>"
        f"<td>{_fmt_price(row.get('target_price'))}</td>"
        f"<td>{_fmt_price(row.get('stop_price'))}</td>"
        f"<td>{float(row.get('confidence', 0.0) or 0.0):.0%}</td>"
        f"<td>{float(row.get('change_2h_bps', 0.0) or 0.0):+.1f}</td>"
        f"<td>{float(row.get('pullback_bps', 0.0) or 0.0):.1f}</td>"
        f"<td>{escape(str(row.get('reason', '')))} / {escape(str(row.get('detail', '')))}</td>"
        "</tr>"
        for row in rows
    )


def _probe_rows_html(report: dict) -> str:
    rows = list(report.get("results", []) or [])
    ranked = sorted(rows, key=lambda row: float(row.get("avg_pnl_bps", 0.0) or 0.0), reverse=True)
    return "\n".join(
        "<tr>"
        f"<td>{_decision_pill(str(row.get('decision', '')))}</td>"
        f"<td>{escape(str(row.get('symbol', '')))}</td>"
        f"<td>{escape(_strategy_label(str(row.get('strategy', ''))))}</td>"
        f"<td>{escape(str(row.get('interval', '')))}</td>"
        f"<td>{int(row.get('sample_bars', 0) or 0)}</td>"
        f"<td>{int(row.get('trade_count', 0) or 0)}</td>"
        f"<td>{escape(_fmt_pct(float(row.get('win_rate', 0.0) or 0.0)))}</td>"
        f"<td>{float(row.get('avg_pnl_bps', 0.0) or 0.0):+.2f}</td>"
        f"<td>{float(row.get('sum_pnl', 0.0) or 0.0):+.6f}</td>"
        f"<td>{float(row.get('profit_factor', 0.0) or 0.0):.2f}</td>"
        f"<td>{float(row.get('payoff_ratio', 0.0) or 0.0):.2f}</td>"
        f"<td>{escape(_fmt_pct(float(row.get('max_drawdown_pct', 0.0) or 0.0)))}</td>"
        f"<td>{escape(str(row.get('reason', '')))}</td>"
        "</tr>"
        for row in ranked
    )


def _signal_rows_html(rows: list[dict[str, str]]) -> str:
    return "\n".join(
        "<tr>"
        f"<td>{escape(_fmt_kst(row.get('timestamp_ms')))}</td>"
        f"<td>{escape(row.get('symbol', ''))}</td>"
        f"<td>{escape(_side_label(row.get('side', '')))}</td>"
        f"<td>{escape(_regime_label(row.get('regime', '')))}</td>"
        f"<td>{escape(row.get('horizon_5m_bps', ''))}</td>"
        "</tr>"
        for row in reversed(rows)
    )


def _order_rows_html(orders) -> str:
    return "\n".join(
        "<tr>"
        f"<td>{escape(kst_from_ms(int(order['timestamp_ms'])))}</td>"
        f"<td>{escape(order['symbol'])}</td>"
        f"<td>{escape(_side_label(str(order['side'])))}</td>"
        f"<td>{escape(_status_label(str(order['status'])))}</td>"
        f"<td>{escape(_reason_label(str(order['reason'] or '')))}</td>"
        "</tr>"
        for order in orders
    )


def _cycle_rows_html(cycles) -> str:
    return "\n".join(
        "<tr>"
        f"<td>{escape(kst_from_ms(int(cycle['updated_ms'])))}</td>"
        f"<td>{escape(cycle['symbol'])}</td>"
        f"<td>{escape(_side_label(str(cycle['side'])))}</td>"
        f"<td>{_status_pill(str(cycle['status']))}</td>"
        f"<td>{escape(_reason_label(str(cycle['reason'] or '')))}</td>"
        f"<td>{escape(_fmt_pnl(cycle['realized_pnl']))}</td>"
        "</tr>"
        for cycle in cycles
    )


def _strategy_cycle_rows_html(cycles) -> str:
    return "\n".join(
        "<tr>"
        f"<td>{escape(kst_from_ms(int(cycle['updated_ms'])))}</td>"
        f"<td>{escape(_strategy_label(str(cycle['strategy'])))}</td>"
        f"<td>{escape(cycle['symbol'])}</td>"
        f"<td>{escape(_side_label(str(cycle['side'])))}</td>"
        f"<td>{_status_pill(str(cycle['status']))}</td>"
        f"<td>{escape(_reason_label(str(cycle['reason'] or '')))}</td>"
        f"<td>{escape(_fmt_pnl(cycle['realized_pnl']))}</td>"
        "</tr>"
        for cycle in cycles
    )


def _strategy_rows_html(rows) -> str:
    return "\n".join(
        "<tr>"
        f"<td>{escape(kst_from_ms(int(row['evaluated_ms'])))}</td>"
        f"<td>{_decision_pill(str(row['decision']))}</td>"
        f"<td>{escape(_source_label(str(row['source'])))}</td>"
        f"<td>{escape(strategy_family_label(row))}</td>"
        f"<td>{escape(_mode_label(str(row['execution_mode'])))}</td>"
        f"<td>{escape(row['symbol'])}</td>"
        f"<td>{escape(_regime_label(str(row['regime'])))}</td>"
        f"<td>{escape(_side_label(str(row['side'])))}</td>"
        f"<td>{float(row['take_profit_bps']):.1f}</td>"
        f"<td>{float(row['stop_loss_bps']):.1f}</td>"
        f"<td>{int(row['max_hold_seconds'])}s</td>"
        f"<td>{int(row['sample_count'])}</td>"
        f"<td>{escape(_fmt_pct(float(row['win_rate'])))}</td>"
        f"<td>{float(row['avg_pnl_bps']):.3f}</td>"
        f"<td>{float(row['sum_pnl_bps']):.3f}</td>"
        f"<td>{escape(row['reason'])}</td>"
        "</tr>"
        for row in rows
    )


def _market_regime_rows_html(rows) -> str:
    return "\n".join(
        "<tr>"
        f"<td>{escape(kst_from_ms(int(row['timestamp_ms'])))}</td>"
        f"<td>{escape(row['symbol'])}</td>"
        f"<td>{escape(macro_regime_ko(row['macro_regime']))}</td>"
        f"<td>{escape(trade_bias_ko(row['trade_bias']))}</td>"
        f"<td>{float(row['trend_1h_bps']):.2f}</td>"
        f"<td>{float(row['trend_4h_bps']):.2f}</td>"
        f"<td>{float(row['realized_vol_bps']):.2f}</td>"
        f"<td>{float(row['atr_bps']):.2f}</td>"
        f"<td>{escape(_allowed_strategies(row['allowed_strategies_json']))}</td>"
        f"<td>{escape(_reason_label(str(row['blocked_reason'] or '')))}</td>"
        "</tr>"
        for row in rows
    )


def _performance_rows_html(rows) -> str:
    return "\n".join(
        "<tr>"
        f"<td>{escape(row['symbol'])}</td>"
        f"<td>{escape(_side_label(str(row['side'])))}</td>"
        f"<td>{row['count']}</td>"
        f"<td>{row['wins']}</td>"
        f"<td>{row['losses']}</td>"
        f"<td>{escape(_fmt_pct(_ratio(row['wins'], row['count'])))}</td>"
        f"<td>{escape(_fmt_pnl(row['avg_win_pnl']))}</td>"
        f"<td>{escape(_fmt_pnl(row['avg_loss_pnl']))}</td>"
        f"<td>{escape(_payoff_ratio(row['avg_win_pnl'], row['avg_loss_pnl']))}</td>"
        f"<td>{escape(_breakeven_win_rate(row['avg_win_pnl'], row['avg_loss_pnl']))}</td>"
        f"<td>{escape(_fmt_pnl(row['avg_pnl']))}</td>"
        f"<td>{escape(_fmt_pnl(row['sum_pnl']))}</td>"
        "</tr>"
        for row in rows
    )


def _strategy_performance_rows_html(rows) -> str:
    return "\n".join(
        "<tr>"
        f"<td>{escape(_strategy_label(str(row['strategy'])))}</td>"
        f"<td>{escape(row['symbol'])}</td>"
        f"<td>{escape(_side_label(str(row['side'])))}</td>"
        f"<td>{row['count']}</td>"
        f"<td>{row['wins']}</td>"
        f"<td>{row['losses']}</td>"
        f"<td>{escape(_fmt_pct(_ratio(row['wins'], row['count'])))}</td>"
        f"<td>{escape(_fmt_pnl(row['avg_win_pnl']))}</td>"
        f"<td>{escape(_fmt_pnl(row['avg_loss_pnl']))}</td>"
        f"<td>{escape(_payoff_ratio(row['avg_win_pnl'], row['avg_loss_pnl']))}</td>"
        f"<td>{escape(_breakeven_win_rate(row['avg_win_pnl'], row['avg_loss_pnl']))}</td>"
        f"<td>{escape(_fmt_pnl(row['avg_pnl']))}</td>"
        f"<td>{escape(_fmt_pnl(row['sum_pnl']))}</td>"
        "</tr>"
        for row in rows
    )


def _exit_reason_rows_html(rows) -> str:
    return "\n".join(
        "<tr>"
        "<td>스캘핑</td>"
        f"<td>{escape(_status_label(str(row['status'])))}</td>"
        f"<td>{escape(_reason_label(str(row['reason'] or '')))}</td>"
        f"<td>{row['count']}</td>"
        f"<td>{escape(_fmt_pnl(row['avg_pnl']))}</td>"
        f"<td>{escape(_fmt_pnl(row['sum_pnl']))}</td>"
        "</tr>"
        for row in rows
    )


def _strategy_exit_reason_rows_html(rows) -> str:
    return "\n".join(
        "<tr>"
        f"<td>{escape(_strategy_label(str(row['strategy'])))}</td>"
        f"<td>{escape(_status_label(str(row['status'])))}</td>"
        f"<td>{escape(_reason_label(str(row['reason'] or '')))}</td>"
        f"<td>{row['count']}</td>"
        f"<td>{escape(_fmt_pnl(row['avg_pnl']))}</td>"
        f"<td>{escape(_fmt_pnl(row['sum_pnl']))}</td>"
        "</tr>"
        for row in rows
    )


def _market_context_rows_html(rows) -> str:
    return "\n".join(
        "<tr>"
        f"<td>{escape(kst_from_ms(int(row['timestamp_ms'])))}</td>"
        f"<td>{escape(row['symbol'])}</td>"
        f"<td>{float(row['premium_bps']):.3f}</td>"
        f"<td>{_funding_bps(row['funding_rate'])}</td>"
        f"<td>{float(row['spread_bps']):.3f}</td>"
        f"<td>{float(row['depth_bid_notional']) + float(row['depth_ask_notional']):.2f}</td>"
        f"<td>{float(row['depth_imbalance']):+.3f}</td>"
        f"<td>{_fmt_price(row['mark_price'])}</td>"
        "</tr>"
        for row in rows
    )


def _page(snapshot: dict[str, str], config: TradingConfig) -> str:
    row_limit = escape(snapshot.get("row_limit", str(DEFAULT_DASHBOARD_ROW_LIMIT)))
    risk_state = snapshot.get("risk_state", "런타임 위험모드\n아직 산출된 내용이 없습니다.")
    mode_summary = snapshot.get("mode_summary", "")
    overview = snapshot.get("overview", "")
    active_paper_rows = snapshot.get("active_paper_rows", "") or _empty_table_row(14, "진행 중인 paper 사이클 없음")
    paper_rows = snapshot.get("paper_rows", "") or _empty_table_row(14, "paper 사이클 기록 없음")
    paper_summary = snapshot.get("paper_summary", "")
    strategy_summary = snapshot.get("strategy_summary", "")
    tactical_radar_rows = snapshot.get("tactical_radar_rows", "") or _empty_table_row(13, "전술 레이더 결과 없음")
    signal_rows = snapshot["signal_rows"] or _empty_table_row(5, "최근 신호 없음")
    order_rows = snapshot["order_rows"] or _empty_table_row(5, "최근 주문/차단 없음")
    cycle_rows = snapshot["cycle_rows"] or _empty_table_row(6, "스캘핑 상태머신 기록 없음")
    strategy_cycle_rows = snapshot.get("strategy_cycle_rows", "") or _empty_table_row(7, "전략 상태머신 기록 없음")
    strategy_rows = snapshot["strategy_rows"] or _empty_table_row(16, "전략 평가 결과 없음")
    meta_rows = snapshot.get("meta_rows", "") or _empty_table_row(13, "메타전략 백테스트 결과 없음")
    meta_action_rows = snapshot.get("meta_action_rows", "") or _empty_table_row(7, "메타전략 행동별 결과 없음")
    refined_entry_rows = snapshot.get("refined_entry_rows", "") or _empty_table_row(18, "현재장 정제후보 결과 없음")
    refine_rows = snapshot.get("refine_rows", "") or _empty_table_row(12, "전략 정제 결과 없음")
    mine_rows = snapshot.get("mine_rows", "") or _empty_table_row(12, "전략 발굴 결과 없음")
    market_regime_rows = snapshot["market_regime_rows"] or _empty_table_row(10, "장세 라우터 기록 없음")
    market_context_rows = snapshot.get("market_context_rows", "") or _empty_table_row(8, "시장상황 기록 없음")
    performance_rows = snapshot["performance_rows"] or _empty_table_row(12, "스캘핑 종료 표본 없음")
    strategy_performance_rows = snapshot.get("strategy_performance_rows", "") or _empty_table_row(13, "전략 종료 표본 없음")
    exit_reason_rows = snapshot["exit_reason_rows"] or _empty_table_row(6, "스캘핑 종료 사유 없음")
    strategy_exit_reason_rows = snapshot.get("strategy_exit_reason_rows", "") or _empty_table_row(6, "전략 종료 사유 없음")
    return f"""<!doctype html>
<html lang="ko">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Cointrading Dashboard</title>
  <style>
    :root {{
      --bg: #f4f6f8;
      --panel: #ffffff;
      --ink: #15171a;
      --muted: #647080;
      --line: #d9e0e8;
      --head: #eef3f8;
      --good-bg: #e8f7ef;
      --good: #14783e;
      --warn-bg: #fff4d7;
      --warn: #8a5a00;
      --block-bg: #fde8e8;
      --block: #b42318;
      --accent: #0f5f8c;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      font-family: system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      margin: 0;
      background: var(--bg);
      color: var(--ink);
    }}
    .shell {{ padding: 24px 28px 36px; }}
    header {{
      display: flex;
      justify-content: space-between;
      align-items: flex-start;
      gap: 20px;
      margin-bottom: 14px;
    }}
    h1 {{ margin: 0; font-size: 28px; line-height: 1.15; }}
    h2 {{ margin: 0 0 12px; font-size: 20px; }}
    h3 {{ margin: 24px 0 10px; font-size: 16px; }}
    pre {{
      white-space: pre-wrap;
      background: #18212f;
      color: #e8edf4;
      padding: 16px;
      border-radius: 6px;
      overflow: auto;
    }}
    table {{
      width: 100%;
      border-collapse: collapse;
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 6px;
      overflow: hidden;
    }}
    th, td {{
      text-align: left;
      padding: 9px 10px;
      border-bottom: 1px solid var(--line);
      font-size: 13px;
      vertical-align: top;
    }}
    th {{
      background: var(--head);
      font-weight: 700;
      color: #263241;
      position: sticky;
      top: 0;
      z-index: 1;
    }}
    tr:last-child td {{ border-bottom: 0; }}
    .table-wrap {{ overflow-x: auto; border-radius: 6px; }}
    .muted {{ color: var(--muted); }}
    .status-dot {{
      width: 9px;
      height: 9px;
      border-radius: 999px;
      background: #16a34a;
      display: inline-block;
      margin-right: 7px;
    }}
    .headline-meta {{ text-align: right; line-height: 1.7; }}
    .mode-line {{ display: flex; gap: 6px; flex-wrap: wrap; justify-content: flex-end; }}
    nav {{
      display: flex;
      gap: 8px;
      flex-wrap: wrap;
      margin: 18px 0 22px;
      border-bottom: 1px solid var(--line);
      padding-bottom: 10px;
    }}
    button {{
      border: 1px solid var(--line);
      background: var(--panel);
      padding: 8px 12px;
      border-radius: 6px;
      cursor: pointer;
      font-weight: 650;
      color: #293545;
    }}
    button.active {{ background: #1d3345; color: #fff; border-color: #1d3345; }}
    .tab-panel {{ display: none; }}
    .tab-panel.active {{ display: block; }}
    .metric-grid {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(160px, 1fr));
      gap: 10px;
      margin: 10px 0 18px;
    }}
    .metric {{
      background: var(--panel);
      border: 1px solid var(--line);
      border-left: 4px solid #8a97a8;
      border-radius: 6px;
      padding: 12px;
      min-height: 74px;
    }}
    .metric strong {{ display: block; margin-top: 7px; font-size: 19px; line-height: 1.2; }}
    .metric-label {{ color: var(--muted); font-size: 12px; font-weight: 700; }}
    .tone-good {{ border-color: #bfe4cd; border-left-color: var(--good); background: var(--good-bg); }}
    .tone-warn {{ border-color: #f2d88d; border-left-color: var(--warn); background: var(--warn-bg); }}
    .tone-block {{ border-color: #f2b8b5; border-left-color: var(--block); background: var(--block-bg); }}
    .tone-muted {{ border-left-color: #8a97a8; }}
    .chip, .pill {{
      display: inline-flex;
      align-items: center;
      border: 1px solid var(--line);
      border-radius: 999px;
      padding: 2px 8px;
      font-size: 12px;
      font-weight: 700;
      white-space: nowrap;
      background: #fff;
    }}
    .chip.tone-good, .pill.tone-good {{ color: var(--good); background: var(--good-bg); border-color: #bfe4cd; }}
    .chip.tone-warn, .pill.tone-warn {{ color: var(--warn); background: var(--warn-bg); border-color: #f2d88d; }}
    .chip.tone-block, .pill.tone-block {{ color: var(--block); background: var(--block-bg); border-color: #f2b8b5; }}
    .row-active td {{ background: #f0f8ff; }}
    .row-stopped td {{ background: #fff8f1; }}
    .pnl-positive {{ color: var(--good); font-weight: 700; }}
    .pnl-negative {{ color: var(--block); font-weight: 700; }}
    .pnl-flat {{ color: var(--muted); }}
    .empty {{ color: var(--muted); text-align: center; padding: 18px; }}
    .split {{
      display: grid;
      grid-template-columns: minmax(0, 1fr) minmax(300px, 0.42fr);
      gap: 16px;
      align-items: start;
    }}
    @media (max-width: 920px) {{
      .shell {{ padding: 16px; }}
      header {{ display: block; }}
      .headline-meta {{ text-align: left; margin-top: 10px; }}
      .mode-line {{ justify-content: flex-start; }}
      .split {{ display: block; }}
      th, td {{ font-size: 12px; padding: 8px; }}
    }}
  </style>
</head>
<body>
  <div class="shell">
    <header>
      <div>
        <h1>Cointrading</h1>
        <p class="muted"><span id="stream-status" class="status-dot"></span>대상: {escape(", ".join(config.scalp_symbols))} · 최근 <span id="row-limit">{row_limit}</span>개 · <span id="generated-at">{escape(snapshot["generated_at"])}</span></p>
      </div>
      <div id="mode-summary" class="mode-line">{mode_summary}</div>
    </header>
    <nav>
      <button class="active" data-tab="overview">개요</button>
      <button data-tab="paper">Paper</button>
      <button data-tab="radar">레이더</button>
      <button data-tab="research">메타전략</button>
      <button data-tab="strategies">전략</button>
      <button data-tab="market">시장</button>
      <button data-tab="risk">위험</button>
      <button data-tab="signals">신호</button>
      <button data-tab="orders">주문</button>
      <button data-tab="raw">원본요약</button>
    </nav>
    <section id="tab-overview" class="tab-panel active">
      <h2>개요</h2>
      <div id="overview" class="metric-grid">{overview}</div>
      <div class="split">
        <div>
          <h3>진행 중 Paper</h3>
          <div class="table-wrap">
            <table>
              <thead><tr><th>갱신</th><th>구분</th><th>전략</th><th>심볼</th><th>방향</th><th>상태</th><th>수량</th><th>진입가</th><th>현재가</th><th>목표가</th><th>손절가</th><th>미실현</th><th>실현손익</th><th>이유</th></tr></thead>
              <tbody id="active-paper-rows">{active_paper_rows}</tbody>
            </table>
          </div>
        </div>
        <div>
          <h3>전략 후보</h3>
          <div id="strategy-summary" class="metric-grid">{strategy_summary}</div>
          <h3>전술 레이더</h3>
          <div id="tactical-radar-summary" class="metric-grid">{snapshot.get("tactical_radar_summary", "")}</div>
        </div>
      </div>
    </section>
    <section id="tab-radar" class="tab-panel">
      <h2>전술 레이더</h2>
      <div id="tactical-radar-summary-tab" class="metric-grid">{snapshot.get("tactical_radar_summary", "")}</div>
      <p class="muted">실전 주문이 아니라 현재 장에서 할 수 있는 전술 단계를 보여줍니다. 추격금지, 눌림대기, 근접, 진입가능을 분리합니다.</p>
      <div class="table-wrap">
        <table>
          <thead><tr><th>판정</th><th>심볼</th><th>시나리오</th><th>방향</th><th>시각</th><th>현재가</th><th>트리거</th><th>목표가</th><th>손절가</th><th>확신</th><th>2h</th><th>눌림/반등</th><th>이유</th></tr></thead>
          <tbody id="tactical-radar-rows">{tactical_radar_rows}</tbody>
        </table>
      </div>
    </section>
    <section id="tab-paper" class="tab-panel">
      <h2>Paper 사이클</h2>
      <div id="paper-summary" class="metric-grid">{paper_summary}</div>
      <div class="table-wrap">
        <table>
          <thead><tr><th>갱신</th><th>구분</th><th>전략</th><th>심볼</th><th>방향</th><th>상태</th><th>수량</th><th>진입가</th><th>현재가</th><th>목표가</th><th>손절가</th><th>미실현</th><th>실현손익</th><th>이유</th></tr></thead>
          <tbody id="paper-rows">{paper_rows}</tbody>
        </table>
      </div>
      <h3>스캘핑 성과</h3>
      <div class="table-wrap">
        <table>
          <thead><tr><th>심볼</th><th>방향</th><th>종료</th><th>익절</th><th>손실</th><th>익절률</th><th>평균익</th><th>평균손</th><th>손익비</th><th>필요승률</th><th>평균손익</th><th>합계손익</th></tr></thead>
          <tbody id="performance-rows">{performance_rows}</tbody>
        </table>
      </div>
      <h3>전략 성과</h3>
      <div class="table-wrap">
        <table>
          <thead><tr><th>전략</th><th>심볼</th><th>방향</th><th>종료</th><th>익절</th><th>손실</th><th>익절률</th><th>평균익</th><th>평균손</th><th>손익비</th><th>필요승률</th><th>평균손익</th><th>합계손익</th></tr></thead>
          <tbody id="strategy-performance-rows">{strategy_performance_rows}</tbody>
        </table>
      </div>
      <h3>종료 사유</h3>
      <div class="table-wrap">
        <table>
          <thead><tr><th>구분</th><th>상태</th><th>이유</th><th>개수</th><th>평균손익</th><th>합계손익</th></tr></thead>
          <tbody id="exit-reason-rows">{exit_reason_rows}</tbody>
          <tbody id="strategy-exit-reason-rows">{strategy_exit_reason_rows}</tbody>
        </table>
      </div>
    </section>
    <section id="tab-strategies" class="tab-panel">
      <h2>전략 후보</h2>
      <div id="strategy-summary-tab" class="metric-grid">{strategy_summary}</div>
      <div class="table-wrap">
        <table>
          <thead><tr><th>평가</th><th>판정</th><th>출처</th><th>전략</th><th>주문방식</th><th>심볼</th><th>장상태</th><th>방향</th><th>TP</th><th>SL</th><th>보유</th><th>표본</th><th>승률</th><th>평균bps</th><th>합계bps</th><th>이유</th></tr></thead>
          <tbody id="strategy-rows">{strategy_rows}</tbody>
        </table>
      </div>
    </section>
    <section id="tab-research" class="tab-panel">
      <h2>상황판단형 메타전략</h2>
      <div id="meta-summary" class="metric-grid">{snapshot.get("meta_summary", "")}</div>
      <p class="muted">실주문 없음. 장기 Binance 공개 캔들로 상승/하락/횡보/돌파/패닉을 판단하고, 그 시점에 하나의 행동만 선택한 결과입니다.</p>
      <h3>현재장 진입후보</h3>
      <div id="refined-entry-summary" class="metric-grid">{snapshot.get("refined_entry_summary", "")}</div>
      <p class="muted">2차 정제에서 살아남은 후보가 지금 닫힌 봉 feature에도 맞는지 보는 단계입니다. 여기서 진입후보가 떠도 실주문은 live-supervisor, preflight, live flag가 따로 통과해야 합니다.</p>
      <div class="table-wrap">
        <table>
          <thead><tr><th>판정</th><th>심볼</th><th>행동</th><th>방향</th><th>feature 시각</th><th>현재가</th><th>목표가</th><th>손절가</th><th>TP</th><th>SL</th><th>보유봉</th><th>테스트 n</th><th>승률</th><th>평균bps</th><th>PF</th><th>손익비</th><th>승률여유</th><th>이유</th></tr></thead>
          <tbody id="refined-entry-rows">{refined_entry_rows}</tbody>
        </table>
      </div>
      <h3>2차 정제 후보</h3>
      <div id="refine-summary" class="metric-grid">{snapshot.get("refine_summary", "")}</div>
      <p class="muted">1차 WATCH 후보만 대상으로 TP/SL/보유시간/필터를 주변값으로 다시 흔든 결과입니다. 생존 후보가 없으면 실전 승격이 아니라 paper 관찰 대상입니다.</p>
      <div class="table-wrap">
        <table>
          <thead><tr><th>판정</th><th>심볼</th><th>행동</th><th>TP</th><th>SL</th><th>보유봉</th><th>WF 양수</th><th>테스트 n</th><th>테스트 평균bps</th><th>테스트 PF</th><th>전체 평균bps</th><th>이유</th></tr></thead>
          <tbody id="refine-rows">{refine_rows}</tbody>
        </table>
      </div>
      <h3>데이터 기반 전략 발굴</h3>
      <div id="mine-summary" class="metric-grid">{snapshot.get("mine_summary", "")}</div>
      <div class="table-wrap">
        <table>
          <thead><tr><th>판정</th><th>심볼</th><th>행동</th><th>TP</th><th>SL</th><th>보유봉</th><th>WF 양수</th><th>테스트 n</th><th>테스트 평균bps</th><th>테스트 PF</th><th>전체 평균bps</th><th>이유</th></tr></thead>
          <tbody id="mine-rows">{mine_rows}</tbody>
        </table>
      </div>
      <h3>고정 메타정책 백테스트</h3>
      <div class="table-wrap">
        <table>
          <thead><tr><th>판정</th><th>심볼</th><th>봉</th><th>시작</th><th>종료</th><th>표본봉</th><th>거래수</th><th>승률</th><th>평균bps</th><th>합계손익</th><th>PF</th><th>MDD</th><th>이유</th></tr></thead>
          <tbody id="meta-rows">{meta_rows}</tbody>
        </table>
      </div>
      <h3>행동별 성과</h3>
      <div class="table-wrap">
        <table>
          <thead><tr><th>심볼</th><th>행동</th><th>거래수</th><th>승률</th><th>평균bps</th><th>PF</th><th>합계손익</th></tr></thead>
          <tbody id="meta-action-rows">{meta_action_rows}</tbody>
        </table>
      </div>
      <h3>이전 프로브</h3>
      <div id="probe-summary" class="metric-grid">{snapshot.get("probe_summary", "")}</div>
      <p class="muted">아래는 예전 방식의 개별 후보 프로브입니다. 실전 판단의 중심은 위 메타전략 결과입니다.</p>
      <div class="table-wrap">
        <table>
          <thead><tr><th>판정</th><th>심볼</th><th>전략</th><th>봉</th><th>표본봉</th><th>거래수</th><th>승률</th><th>평균bps</th><th>합계손익</th><th>PF</th><th>손익비</th><th>MDD</th><th>이유</th></tr></thead>
          <tbody id="probe-rows">{snapshot.get("probe_rows", _empty_table_row(13, "리서치 프로브 결과 없음"))}</tbody>
        </table>
      </div>
    </section>
    <section id="tab-market" class="tab-panel">
      <h2>시장</h2>
      <h3>장세 라우터</h3>
      <div class="table-wrap">
        <table>
          <thead><tr><th>시각</th><th>심볼</th><th>큰 장세</th><th>편향</th><th>1h bps</th><th>4h bps</th><th>변동성</th><th>ATR</th><th>허용 전략</th><th>차단 이유</th></tr></thead>
          <tbody id="market-regime-rows">{market_regime_rows}</tbody>
        </table>
      </div>
      <h3>시장상황</h3>
      <div class="table-wrap">
        <table>
          <thead><tr><th>시각</th><th>심볼</th><th>프리미엄bps</th><th>펀딩bps</th><th>스프레드bps</th><th>호가깊이</th><th>불균형</th><th>mark</th></tr></thead>
          <tbody id="market-context-rows">{market_context_rows}</tbody>
        </table>
      </div>
    </section>
    <section id="tab-risk" class="tab-panel">
      <h2>위험</h2>
      <pre id="risk-state">{escape(risk_state)}</pre>
    </section>
    <section id="tab-signals" class="tab-panel">
      <h2>최근 신호</h2>
      <div class="table-wrap">
        <table>
          <thead><tr><th>시간</th><th>심볼</th><th>방향</th><th>장상태</th><th>5분 bps</th></tr></thead>
          <tbody id="signal-rows">{signal_rows}</tbody>
        </table>
      </div>
    </section>
    <section id="tab-orders" class="tab-panel">
      <h2>최근 주문/차단</h2>
      <div class="table-wrap">
        <table>
          <thead><tr><th>시간</th><th>심볼</th><th>방향</th><th>상태</th><th>이유</th></tr></thead>
          <tbody id="order-rows">{order_rows}</tbody>
        </table>
      </div>
    </section>
    <section id="tab-raw" class="tab-panel">
      <h2>원본요약</h2>
      <pre id="report">{escape(snapshot["report"])}</pre>
      <h3>스캘핑 상태머신</h3>
      <div class="table-wrap">
        <table>
          <thead><tr><th>갱신</th><th>심볼</th><th>방향</th><th>상태</th><th>이유</th><th>실현손익</th></tr></thead>
          <tbody id="cycle-rows">{cycle_rows}</tbody>
        </table>
      </div>
      <h3>전략 상태머신</h3>
      <div class="table-wrap">
        <table>
          <thead><tr><th>갱신</th><th>전략</th><th>심볼</th><th>방향</th><th>상태</th><th>이유</th><th>실현손익</th></tr></thead>
          <tbody id="strategy-cycle-rows">{strategy_cycle_rows}</tbody>
        </table>
      </div>
    </section>
  </div>
  <script>
    document.querySelectorAll("button[data-tab]").forEach((button) => {{
      button.addEventListener("click", () => {{
        document.querySelectorAll("button[data-tab]").forEach((item) => item.classList.remove("active"));
        document.querySelectorAll(".tab-panel").forEach((item) => item.classList.remove("active"));
        button.classList.add("active");
        document.getElementById(`tab-${{button.dataset.tab}}`).classList.add("active");
      }});
    }});
    const statusDot = document.getElementById("stream-status");
    const events = new EventSource(`/events${{window.location.search}}`);
    events.onmessage = (event) => {{
      const data = JSON.parse(event.data);
      document.getElementById("generated-at").textContent = data.generated_at;
      document.getElementById("row-limit").textContent = data.row_limit;
      document.getElementById("mode-summary").innerHTML = data.mode_summary;
      document.getElementById("overview").innerHTML = data.overview;
      document.getElementById("active-paper-rows").innerHTML = data.active_paper_rows;
      document.getElementById("paper-rows").innerHTML = data.paper_rows;
      document.getElementById("paper-summary").innerHTML = data.paper_summary;
      document.getElementById("strategy-summary").innerHTML = data.strategy_summary;
      document.getElementById("strategy-summary-tab").innerHTML = data.strategy_summary;
      document.getElementById("tactical-radar-summary").innerHTML = data.tactical_radar_summary;
      document.getElementById("tactical-radar-summary-tab").innerHTML = data.tactical_radar_summary;
      document.getElementById("meta-summary").innerHTML = data.meta_summary;
      document.getElementById("refined-entry-summary").innerHTML = data.refined_entry_summary;
      document.getElementById("refine-summary").innerHTML = data.refine_summary;
      document.getElementById("mine-summary").innerHTML = data.mine_summary;
      document.getElementById("probe-summary").innerHTML = data.probe_summary;
      document.getElementById("risk-state").textContent = data.risk_state;
      document.getElementById("report").textContent = data.report;
      document.getElementById("signal-rows").innerHTML = data.signal_rows;
      document.getElementById("order-rows").innerHTML = data.order_rows;
      document.getElementById("cycle-rows").innerHTML = data.cycle_rows;
      document.getElementById("strategy-cycle-rows").innerHTML = data.strategy_cycle_rows;
      document.getElementById("strategy-rows").innerHTML = data.strategy_rows;
      document.getElementById("tactical-radar-rows").innerHTML = data.tactical_radar_rows;
      document.getElementById("refined-entry-rows").innerHTML = data.refined_entry_rows;
      document.getElementById("refine-rows").innerHTML = data.refine_rows;
      document.getElementById("mine-rows").innerHTML = data.mine_rows;
      document.getElementById("meta-rows").innerHTML = data.meta_rows;
      document.getElementById("meta-action-rows").innerHTML = data.meta_action_rows;
      document.getElementById("probe-rows").innerHTML = data.probe_rows;
      document.getElementById("market-regime-rows").innerHTML = data.market_regime_rows;
      document.getElementById("market-context-rows").innerHTML = data.market_context_rows;
      document.getElementById("performance-rows").innerHTML = data.performance_rows;
      document.getElementById("strategy-performance-rows").innerHTML = data.strategy_performance_rows;
      document.getElementById("exit-reason-rows").innerHTML = data.exit_reason_rows;
      document.getElementById("strategy-exit-reason-rows").innerHTML = data.strategy_exit_reason_rows;
      statusDot.style.background = "#16a34a";
    }};
    events.onerror = () => {{
      statusDot.style.background = "#dc2626";
    }};
  </script>
</body>
</html>"""


def _is_active_status(status: str) -> bool:
    return str(status) in {"ENTRY_SUBMITTED", "OPEN", "EXIT_SUBMITTED"}


def _empty_table_row(colspan: int, message: str) -> str:
    return f'<tr><td colspan="{colspan}" class="empty">{escape(message)}</td></tr>'


def _risk_mode_line(risk_state) -> str:
    mode = str(getattr(risk_state, "mode", "UNKNOWN"))
    return risk_mode_ko(mode)


def _tone_class(tone: str) -> str:
    return {
        "good": "tone-good",
        "warn": "tone-warn",
        "block": "tone-block",
        "muted": "tone-muted",
    }.get(tone, "tone-muted")


def _pnl_tone(value: float) -> str:
    if value > 0:
        return "good"
    if value < 0:
        return "block"
    return "muted"


def _pnl_cell_class(value) -> str:
    value_float = _float_or_none(value)
    if value_float is None:
        return ""
    if value_float > 0:
        return "pnl-positive"
    if value_float < 0:
        return "pnl-negative"
    return "pnl-flat"


def _status_pill(status: str) -> str:
    tone = "muted"
    if status == "CLOSED":
        tone = "good"
    elif status in {"ENTRY_SUBMITTED", "OPEN", "EXIT_SUBMITTED"}:
        tone = "warn"
    elif status in {"STOPPED", "CANCELLED"}:
        tone = "block"
    return f'<span class="pill {_tone_class(tone)}">{escape(_status_label(status))}</span>'


def _decision_pill(decision: str) -> str:
    tone = "muted"
    if decision in {"APPROVED", "PAPER_READY", "SURVIVED", "READY"}:
        tone = "good"
    elif decision in {"SAMPLE_LOW", "WATCH", "OBSERVE", "WAIT", "NEAR"}:
        tone = "warn"
    elif decision in {"BLOCKED", "REJECTED", "AVOID"}:
        tone = "block"
    return f'<span class="pill {_tone_class(tone)}">{escape(_decision_label(decision))}</span>'


def _status_row_class(status: str) -> str:
    if _is_active_status(status):
        return "row-active"
    if status in {"STOPPED", "CANCELLED"}:
        return "row-stopped"
    return ""


def _decision_label(decision: str) -> str:
    return {
        "APPROVED": "승인",
        "PAPER_READY": "Paper 후보",
        "SURVIVED": "생존",
        "REJECTED": "탈락",
        "OBSERVE": "관찰보류",
        "BLOCKED": "차단",
        "SAMPLE_LOW": "표본부족",
        "WATCH": "관찰",
        "READY": "진입후보",
        "WAIT": "근접대기",
        "NEAR": "근접",
        "AVOID": "관망",
    }.get(decision, decision)


def _scenario_label(scenario: str) -> str:
    return {
        "pullback_long": "눌림 롱",
        "pullback_short": "반등 숏",
        "impulse_up_wait_pullback": "상방 임펄스 눌림대기",
        "impulse_down_wait_bounce": "하방 임펄스 반등대기",
        "failed_breakout_short": "상방 실패돌파 숏",
        "no_tactical_edge": "전술 없음",
        "data_low": "표본부족",
    }.get(scenario, scenario)


def _source_label(source: str) -> str:
    return SOURCE_LABELS.get(source, source)


def _mode_label(mode: str) -> str:
    return MODE_LABELS.get(mode, mode)


def _regime_label(regime: str) -> str:
    return REGIME_LABELS.get(regime, regime)


def _side_label(side: str) -> str:
    return SIDE_LABELS.get(side, side)


def _strategy_label(strategy: str) -> str:
    return STRATEGY_LABELS.get(strategy, strategy)


def _status_label(status: str) -> str:
    return STATUS_LABELS.get(status, status)


def _reason_label(reason: str) -> str:
    return REASON_LABELS.get(reason, reason)


def _fmt_pnl(value) -> str:
    if value is None:
        return ""
    return f"{float(value):.6f}"


def _fmt_signed_pnl(value) -> str:
    if value is None:
        return ""
    return f"{float(value):+.6f}"


def _fmt_price(value) -> str:
    if value is None:
        return ""
    return f"{float(value):.8f}".rstrip("0").rstrip(".")


def _fmt_qty(value) -> str:
    if value is None:
        return ""
    return f"{float(value):.8f}".rstrip("0").rstrip(".")


def _funding_bps(value) -> str:
    if value is None:
        return "n/a"
    return f"{float(value) * 10_000.0:.3f}"


def _fmt_pct(value: float) -> str:
    return f"{value:.1%}"


def _ratio(numerator, denominator) -> float:
    denominator_value = float(denominator or 0)
    if denominator_value <= 0:
        return 0.0
    return float(numerator or 0) / denominator_value


def _payoff_ratio(avg_win, avg_loss) -> str:
    if avg_win is None or avg_loss is None:
        return ""
    win = float(avg_win)
    loss = float(avg_loss)
    if win <= 0 or loss >= 0:
        return ""
    return f"{win / abs(loss):.2f}"


def _breakeven_win_rate(avg_win, avg_loss) -> str:
    if avg_win is None or avg_loss is None:
        return ""
    win = float(avg_win)
    loss = abs(float(avg_loss))
    if win <= 0 or loss <= 0:
        return ""
    return _fmt_pct(loss / (win + loss))


def _fmt_kst(value: str | int | None) -> str:
    if value in {None, ""}:
        return ""
    return kst_from_ms(int(float(value)))


def _allowed_strategies(raw: str) -> str:
    try:
        values = json.loads(raw or "[]")
    except json.JSONDecodeError:
        return raw or ""
    return ", ".join(_strategy_label(str(item)) for item in values)
