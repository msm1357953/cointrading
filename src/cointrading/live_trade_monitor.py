"""Telegram alerts for every fill on the Binance account — including
manual trades placed directly on the exchange.

The bot's own auto/paper lifecycles already send their own open/close
messages. This module exists for the OTHER half: the owner sometimes
trades manually on the Binance app and wants the same kind of
real-time PnL summary.

It works by polling `/fapi/v1/income` once per minute. The income
endpoint reports REALIZED_PNL, COMMISSION, FUNDING_FEE, and other
events for every fill; we group them by (symbol, incomeType) and
send a single concise summary per polling cycle when there is new
activity.

Deduplication is via `tranId` (or `(time, symbol, incomeType)` if
tranId is missing). The last 500 tranIds are kept in the state file
to handle boundary overlap on `startTime`.

State file (default: data/live_trade_monitor_state.json):

    {
        "last_seen_time_ms": 1778..,
        "recent_tran_ids": ["12345", "12346", ...]
    }
"""
from __future__ import annotations

import argparse
import json
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any

from cointrading.config import TelegramConfig, TradingConfig
from cointrading.exchange.binance_usdm import BinanceAPIError, BinanceUSDMClient
from cointrading.storage import default_db_path
from cointrading.telegram_bot import TelegramClient, TelegramConfigError


logger = logging.getLogger(__name__)


DEFAULT_LOOKBACK_MINUTES = 60
INTERESTING_TYPES = ("REALIZED_PNL", "COMMISSION", "FUNDING_FEE")
RECENT_TRAN_IDS_KEEP = 500


def default_state_path() -> Path:
    return default_db_path().parent / "live_trade_monitor_state.json"


@dataclass
class MonitorState:
    last_seen_time_ms: int = 0
    recent_tran_ids: list[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "last_seen_time_ms": self.last_seen_time_ms,
            "recent_tran_ids": self.recent_tran_ids,
        }


def _load_state(path: Path) -> MonitorState:
    if not path.exists():
        return MonitorState()
    try:
        d = json.loads(path.read_text())
        return MonitorState(
            last_seen_time_ms=int(d.get("last_seen_time_ms", 0)),
            recent_tran_ids=list(d.get("recent_tran_ids", [])),
        )
    except (OSError, json.JSONDecodeError):
        return MonitorState()


def _save_state(path: Path, state: MonitorState) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(state.to_dict(), indent=2, sort_keys=True))


def _event_id(ev: dict) -> str:
    tid = ev.get("tranId") or ev.get("tradeId")
    if tid is not None:
        return str(tid)
    # Fallback: time + symbol + type composite (unique enough for dedupe)
    return f"{ev.get('time')}_{ev.get('symbol', '-')}_{ev.get('incomeType', '-')}"


@dataclass
class GroupedAgg:
    realized_pnl: float = 0.0
    realized_count: int = 0
    commission: float = 0.0
    commission_count: int = 0
    funding_fee: float = 0.0
    funding_count: int = 0
    other: float = 0.0


def aggregate(events: list[dict]) -> dict[str, GroupedAgg]:
    by_symbol: dict[str, GroupedAgg] = {}
    for ev in events:
        sym = str(ev.get("symbol") or "-")
        amt = float(ev.get("income", 0) or 0)
        t = str(ev.get("incomeType") or "")
        agg = by_symbol.setdefault(sym, GroupedAgg())
        if t == "REALIZED_PNL":
            agg.realized_pnl += amt
            agg.realized_count += 1
        elif t == "COMMISSION":
            agg.commission += amt
            agg.commission_count += 1
        elif t == "FUNDING_FEE":
            agg.funding_fee += amt
            agg.funding_count += 1
        else:
            agg.other += amt
    return by_symbol


def format_summary(events: list[dict], window_minutes: int) -> str:
    grouped = aggregate(events)
    lines = [f"📊 거래 알림 (최근 {window_minutes}분 활동)"]
    grand_realized = grand_comm = grand_fund = 0.0
    for sym, agg in sorted(grouped.items()):
        if agg.realized_count + agg.commission_count + agg.funding_count == 0 and agg.other == 0:
            continue
        lines.append(f"  {sym}")
        if agg.realized_count > 0:
            lines.append(f"    실현 손익  : {agg.realized_pnl:+.4f} USDC ({agg.realized_count}건)")
            grand_realized += agg.realized_pnl
        if agg.commission_count > 0:
            lines.append(f"    수수료     : {agg.commission:+.4f} USDC ({agg.commission_count}건)")
            grand_comm += agg.commission
        if agg.funding_count > 0:
            lines.append(f"    펀딩비     : {agg.funding_fee:+.4f} USDC ({agg.funding_count}건)")
            grand_fund += agg.funding_fee
        if agg.other != 0:
            lines.append(f"    기타       : {agg.other:+.4f} USDC")
        net = agg.realized_pnl + agg.commission + agg.funding_fee + agg.other
        lines.append(f"    소계       : {net:+.4f} USDC")
    grand_net = grand_realized + grand_comm + grand_fund
    lines.append("  ──────────────")
    lines.append(f"  순 합계      : {grand_net:+.4f} USDC")
    return "\n".join(lines)


def run_monitor(
    *,
    state_path: Path | None = None,
    config: TradingConfig | None = None,
    telegram_config: TelegramConfig | None = None,
    lookback_minutes_first_run: int = DEFAULT_LOOKBACK_MINUTES,
) -> dict:
    cfg = config or TradingConfig.from_env()
    tcfg = telegram_config or TelegramConfig.from_env()
    state_path = state_path or default_state_path()
    state = _load_state(state_path)

    client = BinanceUSDMClient(config=cfg)
    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    if state.last_seen_time_ms <= 0:
        # Bootstrap: only look at the last `lookback_minutes_first_run`
        # so we don't dump weeks of history into Telegram.
        start_ms = now_ms - lookback_minutes_first_run * 60_000
    else:
        # Overlap by 60s to handle boundary races; tranId dedupe protects us.
        start_ms = state.last_seen_time_ms - 60_000

    try:
        events = client.income_history(start_time=start_ms, limit=1000)
    except BinanceAPIError as exc:
        logger.warning("live_trade_monitor: income fetch failed: %s", exc)
        return {"ok": False, "error": str(exc)}

    seen = set(state.recent_tran_ids)
    new_events: list[dict] = []
    for ev in events:
        if str(ev.get("incomeType", "")) not in INTERESTING_TYPES:
            continue
        tid = _event_id(ev)
        if tid in seen:
            continue
        new_events.append(ev)
        seen.add(tid)

    if not new_events:
        # Even with no new events, advance the watermark so we don't keep
        # re-asking for the same window forever.
        state.last_seen_time_ms = now_ms
        _save_state(state_path, state)
        return {"ok": True, "new_events": 0, "telegram_sent": 0}

    sent = 0
    if tcfg.bot_token and tcfg.default_chat_id:
        try:
            tclient = TelegramClient(tcfg)
            window_minutes = max(
                1,
                int((now_ms - min(int(e.get("time", now_ms)) for e in new_events)) / 60_000),
            )
            text = format_summary(new_events, window_minutes)
            tclient.send_message(text)
            sent = 1
        except TelegramConfigError as exc:
            logger.warning("live_trade_monitor: telegram init failed: %s", exc)
        except Exception as exc:  # noqa: BLE001
            logger.warning("live_trade_monitor: telegram send failed: %s", exc)

    # Advance watermark to the latest event time we saw
    latest = max(int(e.get("time", 0) or 0) for e in new_events)
    state.last_seen_time_ms = max(latest, now_ms)
    # Trim recent tran ids to last RECENT_TRAN_IDS_KEEP for memory efficiency
    state.recent_tran_ids = list(seen)[-RECENT_TRAN_IDS_KEEP:]
    _save_state(state_path, state)

    return {
        "ok": True,
        "new_events": len(new_events),
        "telegram_sent": sent,
        "watermark_ms": state.last_seen_time_ms,
    }


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(description="Notify Telegram on every Binance fill (incl. manual)")
    p.add_argument("--state-path", type=Path, default=default_state_path())
    p.add_argument("--lookback-minutes", type=int, default=DEFAULT_LOOKBACK_MINUTES)
    args = p.parse_args(argv)
    result = run_monitor(state_path=args.state_path,
                         lookback_minutes_first_run=args.lookback_minutes)
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
