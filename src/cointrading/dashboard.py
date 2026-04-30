from __future__ import annotations

from html import escape
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import time
from urllib.parse import parse_qs, urlparse

from cointrading.config import TradingConfig
from cointrading.scalping import scalp_report_rows_text
from cointrading.storage import TradingStore, default_db_path, kst_from_ms, now_ms


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
    return {
        "generated_at": kst_from_ms(now_ms()),
        "row_limit": str(limit),
        "report": report,
        "signal_rows": _signal_rows_html(rows[-limit:]),
        "order_rows": _order_rows_html(store.recent_orders(limit=limit)),
        "cycle_rows": _cycle_rows_html(store.recent_scalp_cycles(limit=limit)),
        "strategy_rows": _strategy_rows_html(store.latest_strategy_evaluations(limit=limit)),
        "performance_rows": _performance_rows_html(store.scalp_cycle_performance()),
        "exit_reason_rows": _exit_reason_rows_html(store.scalp_cycle_exit_reasons()),
    }


def _signal_rows_html(rows: list[dict[str, str]]) -> str:
    return "\n".join(
        "<tr>"
        f"<td>{escape(_fmt_kst(row.get('timestamp_ms')))}</td>"
        f"<td>{escape(row.get('symbol', ''))}</td>"
        f"<td>{escape(row.get('side', ''))}</td>"
        f"<td>{escape(row.get('regime', ''))}</td>"
        f"<td>{escape(row.get('horizon_5m_bps', ''))}</td>"
        "</tr>"
        for row in reversed(rows)
    )


def _order_rows_html(orders) -> str:
    return "\n".join(
        "<tr>"
        f"<td>{escape(kst_from_ms(int(order['timestamp_ms'])))}</td>"
        f"<td>{escape(order['symbol'])}</td>"
        f"<td>{escape(order['side'])}</td>"
        f"<td>{escape(order['status'])}</td>"
        f"<td>{escape(order['reason'] or '')}</td>"
        "</tr>"
        for order in orders
    )


def _cycle_rows_html(cycles) -> str:
    return "\n".join(
        "<tr>"
        f"<td>{escape(kst_from_ms(int(cycle['updated_ms'])))}</td>"
        f"<td>{escape(cycle['symbol'])}</td>"
        f"<td>{escape(cycle['side'])}</td>"
        f"<td>{escape(cycle['status'])}</td>"
        f"<td>{escape(cycle['reason'] or '')}</td>"
        f"<td>{escape(_fmt_pnl(cycle['realized_pnl']))}</td>"
        "</tr>"
        for cycle in cycles
    )


def _strategy_rows_html(rows) -> str:
    return "\n".join(
        "<tr>"
        f"<td>{escape(kst_from_ms(int(row['evaluated_ms'])))}</td>"
        f"<td>{escape(row['decision'])}</td>"
        f"<td>{escape(row['source'])}</td>"
        f"<td>{escape(row['execution_mode'])}</td>"
        f"<td>{escape(row['symbol'])}</td>"
        f"<td>{escape(row['regime'])}</td>"
        f"<td>{escape(row['side'])}</td>"
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


def _performance_rows_html(rows) -> str:
    return "\n".join(
        "<tr>"
        f"<td>{escape(row['symbol'])}</td>"
        f"<td>{escape(row['side'])}</td>"
        f"<td>{row['count']}</td>"
        f"<td>{row['wins']}</td>"
        f"<td>{row['losses']}</td>"
        f"<td>{escape(_fmt_pct(_ratio(row['wins'], row['count'])))}</td>"
        f"<td>{escape(_fmt_pnl(row['avg_pnl']))}</td>"
        f"<td>{escape(_fmt_pnl(row['sum_pnl']))}</td>"
        "</tr>"
        for row in rows
    )


def _exit_reason_rows_html(rows) -> str:
    return "\n".join(
        "<tr>"
        f"<td>{escape(row['status'])}</td>"
        f"<td>{escape(row['reason'] or '')}</td>"
        f"<td>{row['count']}</td>"
        f"<td>{escape(_fmt_pnl(row['avg_pnl']))}</td>"
        f"<td>{escape(_fmt_pnl(row['sum_pnl']))}</td>"
        "</tr>"
        for row in rows
    )


def _page(snapshot: dict[str, str], config: TradingConfig) -> str:
    row_limit = escape(snapshot.get("row_limit", str(DEFAULT_DASHBOARD_ROW_LIMIT)))
    signal_rows = snapshot["signal_rows"]
    order_rows = snapshot["order_rows"]
    cycle_rows = snapshot["cycle_rows"]
    strategy_rows = snapshot["strategy_rows"]
    performance_rows = snapshot["performance_rows"]
    exit_reason_rows = snapshot["exit_reason_rows"]
    return f"""<!doctype html>
<html lang="ko">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Cointrading Dashboard</title>
  <style>
    body {{ font-family: system-ui, -apple-system, sans-serif; margin: 24px; background: #f7f8fa; color: #15171a; }}
    header {{ display: flex; justify-content: space-between; gap: 16px; align-items: baseline; }}
    pre {{ white-space: pre-wrap; background: #111827; color: #e5e7eb; padding: 16px; border-radius: 6px; }}
    table {{ width: 100%; border-collapse: collapse; background: white; border-radius: 6px; overflow: hidden; }}
    th, td {{ text-align: left; padding: 10px; border-bottom: 1px solid #e5e7eb; font-size: 14px; }}
    th {{ background: #eef2f7; }}
    .muted {{ color: #5f6875; }}
    .status {{ width: 8px; height: 8px; border-radius: 999px; background: #16a34a; display: inline-block; margin-right: 6px; }}
    nav {{ display: flex; gap: 8px; flex-wrap: wrap; margin: 18px 0; }}
    button {{ border: 1px solid #cfd6df; background: white; padding: 8px 12px; border-radius: 6px; cursor: pointer; }}
    button.active {{ background: #111827; color: white; border-color: #111827; }}
    .tab-panel {{ display: none; }}
    .tab-panel.active {{ display: block; }}
  </style>
</head>
<body>
  <header>
    <h1>Cointrading</h1>
    <p class="muted"><span id="stream-status" class="status"></span>대상: {escape(", ".join(config.scalp_symbols))} · 최근 <span id="row-limit">{row_limit}</span>개 표시 · <span id="generated-at">{escape(snapshot["generated_at"])}</span></p>
  </header>
  <nav>
    <button class="active" data-tab="summary">요약</button>
    <button data-tab="performance">성과</button>
    <button data-tab="strategies">전략후보</button>
    <button data-tab="cycles">상태머신</button>
    <button data-tab="signals">신호</button>
    <button data-tab="orders">주문</button>
  </nav>
  <section id="tab-summary" class="tab-panel active">
    <h2>요약</h2>
    <pre id="report">{escape(snapshot["report"])}</pre>
  </section>
  <section id="tab-performance" class="tab-panel">
    <h2>방향별 성과</h2>
    <table>
      <thead><tr><th>심볼</th><th>방향</th><th>종료</th><th>익절</th><th>손실</th><th>익절률</th><th>평균손익</th><th>합계손익</th></tr></thead>
      <tbody id="performance-rows">{performance_rows}</tbody>
    </table>
    <h2>종료 사유</h2>
    <table>
      <thead><tr><th>상태</th><th>이유</th><th>개수</th><th>평균손익</th><th>합계손익</th></tr></thead>
      <tbody id="exit-reason-rows">{exit_reason_rows}</tbody>
    </table>
  </section>
  <section id="tab-strategies" class="tab-panel">
    <h2>전략 후보</h2>
    <table>
      <thead><tr><th>평가</th><th>판정</th><th>출처</th><th>실행</th><th>심볼</th><th>장상태</th><th>방향</th><th>TP</th><th>SL</th><th>보유</th><th>표본</th><th>승률</th><th>평균bps</th><th>합계bps</th><th>이유</th></tr></thead>
      <tbody id="strategy-rows">{strategy_rows}</tbody>
    </table>
  </section>
  <section id="tab-cycles" class="tab-panel">
    <h2>스캘핑 상태머신</h2>
    <table>
      <thead><tr><th>갱신</th><th>심볼</th><th>방향</th><th>상태</th><th>이유</th><th>실현손익</th></tr></thead>
      <tbody id="cycle-rows">{cycle_rows}</tbody>
    </table>
  </section>
  <section id="tab-signals" class="tab-panel">
    <h2>최근 신호</h2>
    <table>
      <thead><tr><th>시간</th><th>심볼</th><th>방향</th><th>장상태</th><th>5분 bps</th></tr></thead>
      <tbody id="signal-rows">{signal_rows}</tbody>
    </table>
  </section>
  <section id="tab-orders" class="tab-panel">
    <h2>최근 주문/차단</h2>
    <table>
      <thead><tr><th>시간</th><th>심볼</th><th>방향</th><th>상태</th><th>이유</th></tr></thead>
      <tbody id="order-rows">{order_rows}</tbody>
    </table>
  </section>
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
      document.getElementById("report").textContent = data.report;
      document.getElementById("signal-rows").innerHTML = data.signal_rows;
      document.getElementById("order-rows").innerHTML = data.order_rows;
      document.getElementById("cycle-rows").innerHTML = data.cycle_rows;
      document.getElementById("strategy-rows").innerHTML = data.strategy_rows;
      document.getElementById("performance-rows").innerHTML = data.performance_rows;
      document.getElementById("exit-reason-rows").innerHTML = data.exit_reason_rows;
      statusDot.style.background = "#16a34a";
    }};
    events.onerror = () => {{
      statusDot.style.background = "#dc2626";
    }};
  </script>
</body>
</html>"""


def _fmt_pnl(value) -> str:
    if value is None:
        return ""
    return f"{float(value):.6f}"


def _fmt_pct(value: float) -> str:
    return f"{value:.1%}"


def _ratio(numerator, denominator) -> float:
    denominator_value = float(denominator or 0)
    if denominator_value <= 0:
        return 0.0
    return float(numerator or 0) / denominator_value


def _fmt_kst(value: str | int | None) -> str:
    if value in {None, ""}:
        return ""
    return kst_from_ms(int(float(value)))
