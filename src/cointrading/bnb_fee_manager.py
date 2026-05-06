"""Small BNB fee-discount fuel manager.

This module is deliberately not a strategy. It only keeps a tiny amount of BNB
in the USD-M futures wallet so futures commission can use the BNB discount.

Live top-up requires all of:
  - COINTRADING_BNB_FEE_TOPUP_ENABLED=true
  - COINTRADING_BNB_FEE_TOPUP_LIVE_ENABLED=true
  - COINTRADING_DRY_RUN=false
  - COINTRADING_TESTNET=false

The flow is:
  USD-M futures USDC -> spot USDC -> spot MARKET buy BNB -> USD-M futures BNB.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
from typing import Any

from cointrading.config import TradingConfig
from cointrading.exchange.binance_usdm import BinanceAPIError, BinanceUSDMClient
from cointrading.storage import default_db_path, now_ms


KST = timezone(timedelta(hours=9), name="KST")


def default_state_path() -> Path:
    return default_db_path().parent / "bnb_fee_topup_state.json"


def _kst_date(timestamp_ms: int) -> str:
    return datetime.fromtimestamp(timestamp_ms / 1000, KST).strftime("%Y-%m-%d")


@dataclass
class BnbFeeTopupState:
    daily_kst_date: str = ""
    daily_quote_spent_usdc: float = 0.0
    last_topup_ms: int | None = None
    last_action: str = ""
    last_message: str = ""

    @classmethod
    def load(cls, path: Path | None = None) -> "BnbFeeTopupState":
        p = path or default_state_path()
        if not p.exists():
            return cls()
        try:
            data = json.loads(p.read_text())
        except (OSError, json.JSONDecodeError):
            return cls()
        return cls(
            daily_kst_date=str(data.get("daily_kst_date", "")),
            daily_quote_spent_usdc=float(data.get("daily_quote_spent_usdc", 0.0)),
            last_topup_ms=data.get("last_topup_ms"),
            last_action=str(data.get("last_action", "")),
            last_message=str(data.get("last_message", "")),
        )

    def reset_daily_if_needed(self, timestamp_ms: int) -> None:
        today = _kst_date(timestamp_ms)
        if self.daily_kst_date != today:
            self.daily_kst_date = today
            self.daily_quote_spent_usdc = 0.0

    def save(self, path: Path | None = None) -> None:
        p = path or default_state_path()
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(json.dumps(self.__dict__, indent=2, sort_keys=True))


@dataclass
class BnbFeeTopupResult:
    ok: bool
    action: str
    message: str
    futures_bnb_available: float = 0.0
    futures_usdc_available: float = 0.0
    bnb_target: float = 0.0
    quote_amount_usdc: float = 0.0
    bought_bnb: float = 0.0
    transferred_bnb: float = 0.0
    bnb_ask_price: float = 0.0
    raw: dict[str, Any] | None = None

    def to_text(self) -> str:
        lines = [
            f"결과: {self.action}",
            self.message,
            f"선물 BNB: {self.futures_bnb_available:.8f} / 목표 {self.bnb_target:.8f}",
            f"선물 USDC: {self.futures_usdc_available:.4f}",
        ]
        if self.quote_amount_usdc:
            lines.append(f"사용 USDC: {self.quote_amount_usdc:.4f}")
        if self.bought_bnb or self.transferred_bnb:
            lines.append(
                f"매수 BNB: {self.bought_bnb:.8f}, 선물 이동: {self.transferred_bnb:.8f}"
            )
        return "\n".join(lines)


@dataclass
class BnbTargetEstimate:
    target_bnb: float
    estimated_auto_notional_usdc: float
    estimated_daily_fee_usdc: float
    bnb_ask_price: float


def estimate_bnb_target(
    *,
    config: TradingConfig,
    futures_usdc_available: float,
    bnb_ask_price: float,
    planned_notional_usdc: float | None = None,
) -> BnbTargetEstimate:
    static_target = max(config.bnb_fee_topup_target_bnb, config.bnb_fee_topup_min_bnb)
    target = static_target
    estimated_notional = 0.0
    estimated_daily_fee = 0.0
    if config.bnb_fee_topup_dynamic_target_enabled and bnb_ask_price > 0:
        estimated_notional = planned_notional_usdc
        if estimated_notional is None or estimated_notional <= 0:
            estimated_notional = (
                futures_usdc_available
                * config.consecutive_auto_margin_pct
                * config.consecutive_auto_leverage
            )
        estimated_daily_fee = (
            estimated_notional
            * 2.0
            * config.taker_fee_rate
            * 0.90
            * config.consecutive_auto_max_trades_per_day
            * config.bnb_fee_topup_fee_buffer_multiplier
        )
        dynamic_target = estimated_daily_fee / bnb_ask_price
        target = max(static_target, dynamic_target)
    max_target = max(config.bnb_fee_topup_max_target_bnb, static_target)
    target = min(target, max_target)
    return BnbTargetEstimate(
        target_bnb=target,
        estimated_auto_notional_usdc=estimated_notional,
        estimated_daily_fee_usdc=estimated_daily_fee,
        bnb_ask_price=bnb_ask_price,
    )


def futures_asset_balance(client: BinanceUSDMClient, asset: str) -> float:
    balances = client.account_balance()
    target = asset.upper()
    for row in balances:
        if row.get("asset") == target:
            return float(row.get("availableBalance") or row.get("balance") or 0.0)
    return 0.0


def spot_asset_free(client: BinanceUSDMClient, asset: str) -> float:
    account = client.spot_account()
    target = asset.upper()
    for row in account.get("balances", []):
        if row.get("asset") == target:
            return float(row.get("free") or 0.0)
    return 0.0


def bnb_fee_status_text(
    *,
    client: BinanceUSDMClient,
    config: TradingConfig,
    state_path: Path | None = None,
) -> str:
    state = BnbFeeTopupState.load(state_path)
    state.reset_daily_if_needed(now_ms())
    try:
        fee_burn = bool(client.fee_burn_status().get("feeBurn"))
    except (AttributeError, BinanceAPIError):
        fee_burn = False
    try:
        bnb = futures_asset_balance(client, "BNB")
    except (AttributeError, BinanceAPIError):
        bnb = 0.0
    try:
        usdc = futures_asset_balance(client, "USDC")
    except (AttributeError, BinanceAPIError):
        usdc = 0.0
    try:
        ticker = client.spot_book_ticker(config.bnb_fee_topup_symbol)
        ask = float(ticker["askPrice"])
    except (AttributeError, BinanceAPIError, KeyError, ValueError):
        ask = 0.0
    target = estimate_bnb_target(
        config=config,
        futures_usdc_available=usdc,
        bnb_ask_price=ask,
    )
    return "\n".join([
        "■ BNB 수수료 연료",
        f"  BNB 할인 설정     : {'켜짐' if fee_burn else '꺼짐'}",
        f"  자동 보충         : {'켜짐' if config.bnb_fee_topup_enabled else '꺼짐'}",
        f"  실제 매수/이체    : {'허용' if config.bnb_fee_topup_live_enabled and not config.dry_run and not config.testnet else '잠김'}",
        f"  보충 심볼         : {config.bnb_fee_topup_symbol}",
        f"  선물 BNB          : {bnb:.8f}",
        f"  보충 목표         : {target.target_bnb:.8f} BNB",
        f"  산정 기준         : 예상 노셔널 {target.estimated_auto_notional_usdc:.2f} USDC, "
        f"하루 수수료 {target.estimated_daily_fee_usdc:.2f} USDC",
        f"  선물 USDC         : {usdc:.4f}",
        f"  1회/일 한도       : {config.bnb_fee_topup_max_quote_usdc:.2f} / {config.bnb_fee_topup_daily_quote_limit_usdc:.2f} USDC",
        f"  오늘 보충 사용액  : {state.daily_quote_spent_usdc:.4f} USDC",
        "",
        "명령: BNB보충 또는 BNB보충 15",
    ])


def ensure_bnb_fee_balance(
    *,
    client: BinanceUSDMClient,
    config: TradingConfig,
    quote_amount_usdc: float | None = None,
    force: bool = False,
    state_path: Path | None = None,
) -> BnbFeeTopupResult:
    ts = now_ms()
    state = BnbFeeTopupState.load(state_path)
    state.reset_daily_if_needed(ts)

    target_bnb = max(config.bnb_fee_topup_target_bnb, config.bnb_fee_topup_min_bnb)

    if not config.bnb_fee_topup_enabled:
        return BnbFeeTopupResult(
            True, "disabled", "BNB 자동 보충이 설정상 꺼져 있습니다.",
            bnb_target=target_bnb,
        )

    if config.testnet:
        return BnbFeeTopupResult(
            False, "blocked", "testnet 모드에서는 스팟/월렛 보충을 실행하지 않습니다.",
            bnb_target=target_bnb,
        )

    try:
        fee_burn = bool(client.fee_burn_status().get("feeBurn"))
    except (AttributeError, BinanceAPIError) as exc:
        return BnbFeeTopupResult(False, "error", f"BNB 할인 상태 확인 실패: {exc}",
                                 bnb_target=target_bnb)
    if not fee_burn:
        return BnbFeeTopupResult(False, "blocked", "Binance futures BNB feeBurn이 꺼져 있습니다.",
                                 bnb_target=target_bnb)

    try:
        futures_bnb = futures_asset_balance(client, "BNB")
        futures_usdc = futures_asset_balance(client, "USDC")
    except (AttributeError, BinanceAPIError) as exc:
        return BnbFeeTopupResult(False, "error", f"선물 잔고 확인 실패: {exc}",
                                 bnb_target=target_bnb)

    try:
        ticker = client.spot_book_ticker(config.bnb_fee_topup_symbol)
        ask = float(ticker["askPrice"])
    except (AttributeError, BinanceAPIError, KeyError, ValueError) as exc:
        return BnbFeeTopupResult(False, "error", f"BNB spot 호가 확인 실패: {exc}",
                                 futures_bnb_available=futures_bnb,
                                 futures_usdc_available=futures_usdc,
                                 bnb_target=target_bnb)
    if ask <= 0:
        return BnbFeeTopupResult(False, "error", "BNB ask price가 비정상입니다.",
                                 futures_bnb_available=futures_bnb,
                                 futures_usdc_available=futures_usdc,
                                 bnb_target=target_bnb)

    target = estimate_bnb_target(
        config=config,
        futures_usdc_available=futures_usdc,
        bnb_ask_price=ask,
    )
    target_bnb = target.target_bnb
    threshold = target_bnb if (force or config.bnb_fee_topup_dynamic_target_enabled) else config.bnb_fee_topup_min_bnb
    if futures_bnb >= threshold:
        return BnbFeeTopupResult(
            True, "sufficient", "선물 BNB 잔고가 충분합니다.",
            futures_bnb_available=futures_bnb,
            futures_usdc_available=futures_usdc,
            bnb_target=target_bnb,
            bnb_ask_price=ask,
        )

    if not config.bnb_fee_topup_live_enabled or config.dry_run:
        return BnbFeeTopupResult(
            True, "dry_run",
            "보충이 필요하지만 실제 매수/이체 잠금 상태라 실행하지 않았습니다.",
            futures_bnb_available=futures_bnb,
            futures_usdc_available=futures_usdc,
            bnb_target=target_bnb,
            bnb_ask_price=ask,
        )

    try:
        permissions = client.api_key_permissions()
    except (AttributeError, BinanceAPIError) as exc:
        return BnbFeeTopupResult(False, "error", f"API 권한 확인 실패: {exc}",
                                 futures_bnb_available=futures_bnb,
                                 futures_usdc_available=futures_usdc,
                                 bnb_target=target_bnb,
                                 bnb_ask_price=ask)
    if not permissions.get("permitsUniversalTransfer"):
        return BnbFeeTopupResult(False, "blocked", "API에 Universal Transfer 권한이 없습니다.",
                                 futures_bnb_available=futures_bnb,
                                 futures_usdc_available=futures_usdc,
                                 bnb_target=target_bnb,
                                 bnb_ask_price=ask)
    if not permissions.get("enableSpotAndMarginTrading"):
        return BnbFeeTopupResult(False, "blocked", "API에 Spot trading 권한이 없습니다.",
                                 futures_bnb_available=futures_bnb,
                                 futures_usdc_available=futures_usdc,
                                 bnb_target=target_bnb,
                                 bnb_ask_price=ask)

    try:
        existing_spot_bnb = spot_asset_free(client, "BNB")
    except (AttributeError, BinanceAPIError, ValueError):
        existing_spot_bnb = 0.0
    missing_to_threshold = max(0.0, threshold - futures_bnb)
    if existing_spot_bnb > 0 and missing_to_threshold > 0:
        transfer_existing = min(existing_spot_bnb, missing_to_threshold)
        try:
            client.universal_transfer(
                transfer_type="MAIN_UMFUTURE",
                asset="BNB",
                amount=transfer_existing,
            )
        except (AttributeError, BinanceAPIError, ValueError) as exc:
            return BnbFeeTopupResult(
                False, "error", f"스팟 BNB 선물 이동 실패: {exc}",
                futures_bnb_available=futures_bnb,
                futures_usdc_available=futures_usdc,
                bnb_target=target_bnb,
                bnb_ask_price=ask,
            )
        futures_bnb += transfer_existing
        if futures_bnb >= threshold:
            return BnbFeeTopupResult(
                True, "transferred_existing", "스팟에 남아 있던 BNB를 선물 지갑으로 이동했습니다.",
                futures_bnb_available=futures_bnb,
                futures_usdc_available=futures_usdc,
                bnb_target=target_bnb,
                transferred_bnb=transfer_existing,
                bnb_ask_price=ask,
            )

    if quote_amount_usdc is None:
        needed_bnb = max(0.0, target_bnb - futures_bnb)
        quote_amount_usdc = needed_bnb * ask * 1.02
    quote_amount_usdc = max(config.bnb_fee_topup_min_quote_usdc, quote_amount_usdc)
    quote_amount_usdc = min(config.bnb_fee_topup_max_quote_usdc, quote_amount_usdc)

    if state.daily_quote_spent_usdc + quote_amount_usdc > config.bnb_fee_topup_daily_quote_limit_usdc:
        return BnbFeeTopupResult(
            False, "blocked",
            "오늘 BNB 보충 USDC 한도를 초과해서 실행하지 않았습니다.",
            futures_bnb_available=futures_bnb,
            futures_usdc_available=futures_usdc,
            bnb_target=target_bnb,
            quote_amount_usdc=quote_amount_usdc,
            bnb_ask_price=ask,
        )
    if futures_usdc < quote_amount_usdc:
        return BnbFeeTopupResult(
            False, "blocked",
            "선물 USDC 잔고가 부족해서 BNB 보충을 실행하지 않았습니다.",
            futures_bnb_available=futures_bnb,
            futures_usdc_available=futures_usdc,
            bnb_target=target_bnb,
            quote_amount_usdc=quote_amount_usdc,
            bnb_ask_price=ask,
        )

    transfer_to_spot: dict[str, Any] | None = None
    try:
        transfer_to_spot = client.universal_transfer(
            transfer_type="UMFUTURE_MAIN",
            asset="USDC",
            amount=quote_amount_usdc,
        )
        order = client.spot_market_order_quote(
            symbol=config.bnb_fee_topup_symbol,
            side="BUY",
            quote_order_qty=quote_amount_usdc,
            response_type="FULL",
        )
        bought_bnb = float(order.get("executedQty") or 0.0)
        spot_free_bnb = spot_asset_free(client, "BNB")
        transfer_bnb = min(bought_bnb, spot_free_bnb) if spot_free_bnb > 0 else bought_bnb * 0.999
        if transfer_bnb <= 0:
            raise BinanceAPIError("spot BNB free balance is zero after buy")
        transfer_to_futures = client.universal_transfer(
            transfer_type="MAIN_UMFUTURE",
            asset="BNB",
            amount=transfer_bnb,
        )
    except (AttributeError, BinanceAPIError, ValueError) as exc:
        if transfer_to_spot is not None:
            try:
                client.universal_transfer(
                    transfer_type="MAIN_UMFUTURE",
                    asset="USDC",
                    amount=quote_amount_usdc,
                )
            except Exception:
                pass
        return BnbFeeTopupResult(
            False, "error", f"BNB 보충 실행 실패: {exc}",
            futures_bnb_available=futures_bnb,
            futures_usdc_available=futures_usdc,
            bnb_target=target_bnb,
            quote_amount_usdc=quote_amount_usdc,
            bnb_ask_price=ask,
        )

    state.daily_quote_spent_usdc += quote_amount_usdc
    state.last_topup_ms = ts
    state.last_action = "topped_up"
    state.last_message = f"{quote_amount_usdc:.4f} USDC로 BNB 보충"
    state.save(state_path)

    return BnbFeeTopupResult(
        True, "topped_up", "BNB 보충 완료.",
        futures_bnb_available=futures_bnb,
        futures_usdc_available=futures_usdc,
        bnb_target=target_bnb,
        quote_amount_usdc=quote_amount_usdc,
        bought_bnb=bought_bnb,
        transferred_bnb=transfer_bnb,
        bnb_ask_price=ask,
        raw={
            "transfer_to_spot": transfer_to_spot,
            "spot_order": order,
            "transfer_to_futures": transfer_to_futures,
        },
    )
