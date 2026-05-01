from __future__ import annotations

from typing import Any


USD_MARGIN_ASSETS = {"USDT", "USDC", "BUSD", "FDUSD"}


def account_summary_text(account: dict[str, Any]) -> str:
    open_positions = [
        item
        for item in account.get("positions", [])
        if _to_float(item.get("positionAmt")) != 0.0
    ]
    lines = [
        "Binance 선물 계좌",
        f"지갑: {_fmt_account_amount(account, 'totalWalletBalance', 'walletBalance')}",
        f"사용 가능: {_fmt_account_amount(account, 'availableBalance', 'availableBalance')}",
        f"미실현 손익: {_fmt_account_amount(account, 'totalUnrealizedProfit', 'unrealizedProfit')}",
        f"유지 증거금: {_fmt_account_amount(account, 'totalMaintMargin', 'maintMargin')}",
    ]
    bnb_wallet = _asset_value(account, "BNB", "walletBalance")
    if bnb_wallet != 0.0:
        lines.append(f"BNB 수수료 지갑: {bnb_wallet:.8f} BNB")
    lines.append(f"열린 포지션: {len(open_positions)}")
    return "\n".join(lines)


def _fmt_account_amount(account: dict[str, Any], total_key: str, asset_key: str) -> str:
    total = _to_float(account.get(total_key))
    asset_amounts = _asset_amounts(account, asset_key)
    if total != 0.0 or not asset_amounts:
        return _fmt_usd_m(total)
    return _fmt_assets(asset_amounts)


def _asset_amounts(account: dict[str, Any], key: str) -> list[tuple[str, float]]:
    amounts = []
    for asset in account.get("assets", []):
        name = str(asset.get("asset", "")).upper()
        if name not in USD_MARGIN_ASSETS:
            continue
        value = _to_float(asset.get(key))
        if value != 0.0:
            amounts.append((name, value))
    return amounts


def _asset_value(account: dict[str, Any], asset_name: str, key: str) -> float:
    target = asset_name.upper()
    for asset in account.get("assets", []):
        if str(asset.get("asset", "")).upper() == target:
            return _to_float(asset.get(key))
    return 0.0


def _fmt_assets(amounts: list[tuple[str, float]]) -> str:
    return " + ".join(f"{value:.4f} {asset}" for asset, value in amounts)


def _fmt_usd_m(value: Any) -> str:
    return f"{_to_float(value):.4f} USD-M"


def _to_float(value: Any) -> float:
    if value is None or value == "":
        return 0.0
    return float(value)
