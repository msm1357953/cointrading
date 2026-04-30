from __future__ import annotations

from typing import Any


def account_summary_text(account: dict[str, Any]) -> str:
    open_positions = [
        item
        for item in account.get("positions", [])
        if _to_float(item.get("positionAmt")) != 0.0
    ]
    return "\n".join(
        [
            "Binance 선물 계좌",
            f"지갑: {_fmt_usd_m(account.get('totalWalletBalance'))}",
            f"사용 가능: {_fmt_usd_m(account.get('availableBalance'))}",
            f"미실현 손익: {_fmt_usd_m(account.get('totalUnrealizedProfit'))}",
            f"유지 증거금: {_fmt_usd_m(account.get('totalMaintMargin'))}",
            f"열린 포지션: {len(open_positions)}",
        ]
    )


def _fmt_usd_m(value: Any) -> str:
    return f"{_to_float(value):.4f} USD-M"


def _to_float(value: Any) -> float:
    if value is None or value == "":
        return 0.0
    return float(value)
