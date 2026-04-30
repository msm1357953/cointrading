from __future__ import annotations

from html import escape
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

from cointrading.config import TradingConfig
from cointrading.scalping import scalp_report_rows_text
from cointrading.storage import TradingStore, default_db_path


def run_dashboard(host: str = "127.0.0.1", port: int = 8080, db_path: Path | None = None) -> None:
    store_path = db_path or default_db_path()
    config = TradingConfig.from_env()

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            parsed = urlparse(self.path)
            if parsed.path not in {"/", "/signals", "/orders"}:
                self.send_error(404)
                return
            query = parse_qs(parsed.query)
            symbol = query.get("symbol", [None])[0]
            store = TradingStore(store_path)
            rows = store.list_signals(symbol=symbol, symbols=config.scalp_symbols if not symbol else None)
            report = scalp_report_rows_text(
                rows,
                symbol=symbol,
                symbols=config.scalp_symbols if not symbol else None,
            )
            orders = store.recent_orders(limit=10)
            body = _page(report, rows[-25:], orders, config)
            payload = body.encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def log_message(self, format: str, *args) -> None:
            return

    server = ThreadingHTTPServer((host, port), Handler)
    print(f"Dashboard: http://{host}:{port}")
    server.serve_forever()


def _page(rows_text: str, rows: list[dict[str, str]], orders, config: TradingConfig) -> str:
    signal_rows = "\n".join(
        "<tr>"
        f"<td>{escape(row.get('iso_time', ''))}</td>"
        f"<td>{escape(row.get('symbol', ''))}</td>"
        f"<td>{escape(row.get('side', ''))}</td>"
        f"<td>{escape(row.get('regime', ''))}</td>"
        f"<td>{escape(row.get('horizon_5m_bps', ''))}</td>"
        "</tr>"
        for row in reversed(rows)
    )
    order_rows = "\n".join(
        "<tr>"
        f"<td>{escape(order['iso_time'])}</td>"
        f"<td>{escape(order['symbol'])}</td>"
        f"<td>{escape(order['side'])}</td>"
        f"<td>{escape(order['status'])}</td>"
        f"<td>{escape(order['reason'] or '')}</td>"
        "</tr>"
        for order in orders
    )
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
  </style>
</head>
<body>
  <header>
    <h1>Cointrading</h1>
    <p class="muted">대상: {escape(", ".join(config.scalp_symbols))}</p>
  </header>
  <h2>요약</h2>
  <pre>{escape(rows_text)}</pre>
  <h2>최근 신호</h2>
  <table>
    <thead><tr><th>시간</th><th>심볼</th><th>방향</th><th>장상태</th><th>5분 bps</th></tr></thead>
    <tbody>{signal_rows}</tbody>
  </table>
  <h2>최근 주문/차단</h2>
  <table>
    <thead><tr><th>시간</th><th>심볼</th><th>방향</th><th>상태</th><th>이유</th></tr></thead>
    <tbody>{order_rows}</tbody>
  </table>
</body>
</html>"""
