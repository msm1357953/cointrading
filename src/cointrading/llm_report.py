from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from cointrading.config import TradingConfig
from cointrading.market_regime import macro_regime_ko, trade_bias_ko
from cointrading.storage import TradingStore, kst_from_ms, now_ms


class LLMReportError(RuntimeError):
    pass


@dataclass
class LLMReportState:
    last_sent_ms: int = 0

    @classmethod
    def load(cls, path: Path) -> "LLMReportState":
        if not path.exists():
            return cls()
        payload = json.loads(path.read_text())
        return cls(last_sent_ms=int(payload.get("last_sent_ms", 0)))

    def save(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps({"last_sent_ms": self.last_sent_ms}, indent=2, sort_keys=True))


def default_llm_report_state_path() -> Path:
    return Path(__file__).resolve().parents[2] / "data" / "llm_report_state.json"


class GeminiReportClient:
    BASE_URL = "https://generativelanguage.googleapis.com/v1beta"

    def __init__(self, api_key: str, model: str, timeout: float = 45.0) -> None:
        if not api_key:
            raise LLMReportError("Gemini API key is missing")
        self.api_key = api_key
        self.model = model
        self.timeout = timeout

    def generate(self, prompt: str) -> str:
        model = self.model.removeprefix("models/")
        url = f"{self.BASE_URL}/models/{model}:generateContent"
        payload = {
            "contents": [{"role": "user", "parts": [{"text": prompt}]}],
            "generationConfig": {
                "temperature": 0.2,
                "topP": 0.8,
                "maxOutputTokens": 4096,
            },
        }
        request = Request(
            url,
            data=json.dumps(payload).encode("utf-8"),
            method="POST",
            headers={
                "Content-Type": "application/json",
                "x-goog-api-key": self.api_key,
            },
        )
        try:
            with urlopen(request, timeout=self.timeout) as response:
                data = json.loads(response.read().decode("utf-8"))
        except HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")
            raise LLMReportError(f"Gemini API error {exc.code}: {detail[:500]}") from exc
        except URLError as exc:
            raise LLMReportError(f"Gemini API request failed: {exc}") from exc

        text = _extract_candidate_text(data)
        if not text:
            raise LLMReportError("Gemini API returned no text")
        return text.strip()


def llm_report_due(
    state: LLMReportState,
    *,
    interval_hours: int,
    force: bool = False,
    timestamp_ms: int | None = None,
) -> bool:
    if force:
        return True
    ts = timestamp_ms or now_ms()
    return state.last_sent_ms <= 0 or ts - state.last_sent_ms >= interval_hours * 3_600_000


def build_report_context(store: TradingStore, config: TradingConfig) -> str:
    market_rows = store.current_market_regimes(symbols=config.scalp_symbols)
    strategy_rows = store.latest_strategy_batch(limit=12)
    performance_rows = store.scalp_cycle_performance()
    exit_rows = store.scalp_cycle_exit_reasons()[:10]
    recent_cycles = store.recent_scalp_cycles(limit=8)
    recent_orders = store.recent_orders(limit=8)

    lines = [
        f"generated_at: {kst_from_ms(now_ms())}",
        f"dry_run: {config.dry_run}",
        f"live_trading_enabled: {config.live_trading_enabled}",
        f"symbols: {', '.join(config.scalp_symbols)}",
        "",
        "market_regimes:",
    ]
    if market_rows:
        for row in market_rows:
            lines.append(
                "- "
                f"{row['symbol']} {macro_regime_ko(row['macro_regime'])} "
                f"bias={trade_bias_ko(row['trade_bias'])} "
                f"1h={float(row['trend_1h_bps']):.1f}bps "
                f"4h={float(row['trend_4h_bps']):.1f}bps "
                f"atr={float(row['atr_bps']):.1f}bps "
                f"allowed={_json_join(row['allowed_strategies_json'])} "
                f"blocked={row['blocked_reason'] or ''}"
            )
    else:
        lines.append("- no market regime rows")

    lines.extend(["", "strategy_candidates:"])
    if strategy_rows:
        for row in strategy_rows:
            lines.append(
                "- "
                f"{row['decision']} {row['execution_mode']} {row['symbol']} "
                f"{row['regime']} {row['side']} "
                f"TP={float(row['take_profit_bps']):.1f} "
                f"SL={float(row['stop_loss_bps']):.1f} "
                f"H={int(row['max_hold_seconds'])}s "
                f"n={int(row['sample_count'])} "
                f"win={float(row['win_rate']):.1%} "
                f"avg={float(row['avg_pnl_bps']):.3f}bps "
                f"reason={row['reason']}"
            )
    else:
        lines.append("- no strategy rows")

    lines.extend(["", "paper_performance:"])
    if performance_rows:
        for row in performance_rows[:10]:
            lines.append(
                "- "
                f"{row['symbol']} {row['side']} count={int(row['count'])} "
                f"wins={int(row['wins'] or 0)} losses={int(row['losses'] or 0)} "
                f"avg_pnl={_fmt_optional(row['avg_pnl'])} "
                f"sum_pnl={_fmt_optional(row['sum_pnl'])}"
            )
    else:
        lines.append("- no closed paper performance")

    lines.extend(["", "exit_reasons:"])
    for row in exit_rows:
        lines.append(
            "- "
            f"{row['status']} {row['reason'] or ''} count={int(row['count'])} "
            f"avg_pnl={_fmt_optional(row['avg_pnl'])} sum_pnl={_fmt_optional(row['sum_pnl'])}"
        )

    lines.extend(["", "recent_cycles:"])
    for row in recent_cycles:
        lines.append(
            "- "
            f"{kst_from_ms(int(row['updated_ms']))} {row['symbol']} {row['side']} "
            f"{row['status']} reason={row['reason'] or ''} pnl={_fmt_optional(row['realized_pnl'])}"
        )

    lines.extend(["", "recent_orders:"])
    for row in recent_orders:
        lines.append(
            "- "
            f"{kst_from_ms(int(row['timestamp_ms']))} {row['symbol']} {row['side']} "
            f"{row['status']} reason={row['reason'] or ''}"
        )
    return "\n".join(lines)


def build_report_prompt(context: str) -> str:
    return "\n".join(
        [
            "너는 코인 선물 자동매매 시스템의 리스크 감시자다.",
            "주문 실행, 매수/매도 지시, 레버리지 확대 지시는 절대 하지 마라.",
            "아래 데이터는 dry-run/paper 중심의 상태 요약이다.",
            "한국어로 텔레그램에 보낼 짧은 리포트를 작성하라.",
            "형식:",
            "1. 현재 장세",
            "2. 전략 상태",
            "3. 위험 신호",
            "4. 다음 관찰 포인트",
            "규칙: 1200자 이내, 과장 금지, 실전 진입 권유 금지, 숫자를 근거로 말하기.",
            "이모지는 쓰지 말고, 데이터가 부족하면 부족하다고 말하기.",
            "",
            "데이터:",
            context,
        ]
    )


def fallback_report_text(context: str) -> str:
    lines = context.splitlines()
    picked = [line for line in lines if line.startswith("- ")][:12]
    return "\n".join(
        [
            "LLM 리포트 대체 요약",
            "Gemini 호출 없이 원자료 일부만 표시합니다.",
            *picked,
        ]
    )


def _extract_candidate_text(payload: dict[str, Any]) -> str:
    parts: list[str] = []
    for candidate in payload.get("candidates", []):
        content = candidate.get("content") or {}
        for part in content.get("parts", []):
            text = part.get("text")
            if text:
                parts.append(str(text))
    return "\n".join(parts)


def _json_join(raw: str) -> str:
    try:
        values = json.loads(raw or "[]")
    except json.JSONDecodeError:
        return raw or ""
    return ", ".join(str(item) for item in values)


def _fmt_optional(value) -> str:
    if value is None:
        return "n/a"
    return f"{float(value):.6f}"
