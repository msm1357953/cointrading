from __future__ import annotations

import hashlib
import hmac
import json
import os
import time
from typing import Any
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from cointrading.config import TradingConfig
from cointrading.models import Kline, OrderIntent


class BinanceAPIError(RuntimeError):
    pass


class BinanceUSDMClient:
    MAINNET_BASE_URL = "https://fapi.binance.com"
    TESTNET_BASE_URL = "https://demo-fapi.binance.com"
    SPOT_MAINNET_BASE_URL = "https://api.binance.com"
    SPOT_TESTNET_BASE_URL = "https://testnet.binance.vision"

    def __init__(
        self,
        api_key: str | None = None,
        api_secret: str | None = None,
        config: TradingConfig | None = None,
        timeout: float = 10.0,
    ) -> None:
        self.config = config or TradingConfig.from_env()
        self.api_key = api_key if api_key is not None else os.getenv("BINANCE_API_KEY", "")
        self.api_secret = (
            api_secret if api_secret is not None else os.getenv("BINANCE_API_SECRET", "")
        )
        self.timeout = timeout
        self.base_url = (
            self.TESTNET_BASE_URL if self.config.testnet else self.MAINNET_BASE_URL
        )
        self.spot_base_url = (
            self.SPOT_TESTNET_BASE_URL if self.config.testnet else self.SPOT_MAINNET_BASE_URL
        )

    def server_time(self) -> dict[str, Any]:
        return self._request("GET", "/fapi/v1/time")

    def klines(self, symbol: str, interval: str, limit: int = 500) -> list[Kline]:
        params = {"symbol": symbol, "interval": interval, "limit": limit}
        rows = self._request("GET", "/fapi/v1/klines", params=params)
        return [Kline.from_binance(row) for row in rows]

    def order_book(self, symbol: str, limit: int = 20) -> dict[str, Any]:
        return self._request("GET", "/fapi/v1/depth", {"symbol": symbol, "limit": limit})

    def book_ticker(self, symbol: str) -> dict[str, Any]:
        return self._request("GET", "/fapi/v1/ticker/bookTicker", {"symbol": symbol})

    def mark_price(self, symbol: str) -> dict[str, Any]:
        return self._request("GET", "/fapi/v1/premiumIndex", {"symbol": symbol})

    def open_interest(self, symbol: str) -> dict[str, Any]:
        return self._request("GET", "/fapi/v1/openInterest", {"symbol": symbol})

    def funding_rate(self, symbol: str, limit: int = 1) -> list[dict[str, Any]]:
        return self._request("GET", "/fapi/v1/fundingRate", {"symbol": symbol, "limit": limit})

    def exchange_info(self, symbol: str | None = None) -> dict[str, Any]:
        params = {"symbol": symbol.upper()} if symbol else None
        return self._request("GET", "/fapi/v1/exchangeInfo", params=params)

    def account_info(self) -> dict[str, Any]:
        return self._signed_request("GET", "/fapi/v3/account")

    def account_balance(self) -> list[dict[str, Any]]:
        return self._signed_request("GET", "/fapi/v3/balance")

    def commission_rate(self, symbol: str) -> dict[str, Any]:
        return self._signed_request("GET", "/fapi/v1/commissionRate", {"symbol": symbol})

    def fee_burn_status(self) -> dict[str, Any]:
        return self._signed_request("GET", "/fapi/v1/feeBurn")

    def multi_assets_margin(self) -> dict[str, Any]:
        return self._signed_request("GET", "/fapi/v1/multiAssetsMargin")

    def api_key_permissions(self) -> dict[str, Any]:
        return self._signed_request(
            "GET", "/sapi/v1/account/apiRestrictions", base_url=self.SPOT_MAINNET_BASE_URL
        )

    def spot_account(self) -> dict[str, Any]:
        return self._signed_request("GET", "/api/v3/account", base_url=self.spot_base_url)

    def spot_book_ticker(self, symbol: str) -> dict[str, Any]:
        return self._request(
            "GET", "/api/v3/ticker/bookTicker",
            {"symbol": symbol.upper()},
            base_url=self.spot_base_url,
        )

    def spot_market_order_quote(
        self,
        *,
        symbol: str,
        side: str,
        quote_order_qty: float,
        response_type: str = "FULL",
    ) -> dict[str, Any]:
        params = {
            "symbol": symbol.upper(),
            "side": side.upper(),
            "type": "MARKET",
            "quoteOrderQty": self._format_float(quote_order_qty),
            "newOrderRespType": response_type,
        }
        if self.config.dry_run:
            return {"dryRun": True, "endpoint": "/api/v3/order", "params": params}
        return self._signed_request("POST", "/api/v3/order", params=params, base_url=self.spot_base_url)

    def universal_transfer_history(
        self,
        *,
        transfer_type: str,
        size: int = 10,
    ) -> dict[str, Any]:
        return self._signed_request(
            "GET",
            "/sapi/v1/asset/transfer",
            {"type": transfer_type, "size": int(size)},
            base_url=self.SPOT_MAINNET_BASE_URL,
        )

    def universal_transfer(
        self,
        *,
        transfer_type: str,
        asset: str,
        amount: float,
    ) -> dict[str, Any]:
        params = {
            "type": transfer_type,
            "asset": asset.upper(),
            "amount": self._format_float(amount),
        }
        if self.config.dry_run:
            return {"dryRun": True, "endpoint": "/sapi/v1/asset/transfer", "params": params}
        return self._signed_request(
            "POST", "/sapi/v1/asset/transfer", params=params,
            base_url=self.SPOT_MAINNET_BASE_URL,
        )

    def new_order(self, intent: OrderIntent) -> dict[str, Any]:
        params: dict[str, Any] = {
            "symbol": intent.symbol,
            "side": intent.side,
            "type": intent.order_type,
            "quantity": self._format_float(intent.quantity),
        }
        if intent.price is not None:
            params["price"] = self._format_float(intent.price)
        if intent.stop_price is not None:
            params["stopPrice"] = self._format_float(intent.stop_price)
        if intent.time_in_force:
            params["timeInForce"] = intent.time_in_force
        if intent.working_type:
            params["workingType"] = intent.working_type
        if intent.response_type:
            params["newOrderRespType"] = intent.response_type
        if intent.reduce_only:
            params["reduceOnly"] = "true"
        if intent.client_order_id:
            params["newClientOrderId"] = intent.client_order_id

        if self.config.dry_run:
            return {
                "dryRun": True,
                "endpoint": "/fapi/v1/order",
                "params": params,
            }
        return self._signed_request("POST", "/fapi/v1/order", params=params)

    def income_history(
        self,
        *,
        symbol: str | None = None,
        income_type: str | None = None,
        start_time: int | None = None,
        end_time: int | None = None,
        limit: int = 1000,
    ) -> list[dict[str, Any]]:
        params: dict[str, Any] = {"limit": int(limit)}
        if symbol:
            params["symbol"] = symbol
        if income_type:
            params["incomeType"] = income_type
        if start_time is not None:
            params["startTime"] = int(start_time)
        if end_time is not None:
            params["endTime"] = int(end_time)
        result = self._signed_request("GET", "/fapi/v1/income", params=params)
        return list(result) if isinstance(result, list) else []

    def set_leverage(self, *, symbol: str, leverage: int) -> dict[str, Any]:
        params = {"symbol": symbol, "leverage": int(leverage)}
        if self.config.dry_run:
            return {"dryRun": True, "endpoint": "/fapi/v1/leverage", "params": params}
        return self._signed_request("POST", "/fapi/v1/leverage", params=params)

    def set_margin_type(self, *, symbol: str, margin_type: str) -> dict[str, Any]:
        """margin_type in {'ISOLATED', 'CROSSED'}. Returns API response.
        Binance returns code -4046 if the margin type is already set; treat as success."""
        params = {"symbol": symbol, "marginType": margin_type.upper()}
        if self.config.dry_run:
            return {"dryRun": True, "endpoint": "/fapi/v1/marginType", "params": params}
        try:
            return self._signed_request("POST", "/fapi/v1/marginType", params=params)
        except BinanceAPIError as exc:
            # -4046 "No need to change margin type" — already set, treat as ok
            if "-4046" in str(exc) or "No need to change margin type" in str(exc):
                return {"already_set": True, "marginType": margin_type.upper()}
            raise

    def order_status(
        self,
        *,
        symbol: str,
        order_id: int | None = None,
        orig_client_order_id: str | None = None,
    ) -> dict[str, Any]:
        params: dict[str, Any] = {"symbol": symbol}
        if order_id is not None:
            params["orderId"] = order_id
        elif orig_client_order_id:
            params["origClientOrderId"] = orig_client_order_id
        else:
            raise BinanceAPIError("order_id or orig_client_order_id is required")
        return self._signed_request("GET", "/fapi/v1/order", params=params)

    def cancel_order(
        self,
        *,
        symbol: str,
        order_id: int | None = None,
        orig_client_order_id: str | None = None,
    ) -> dict[str, Any]:
        params: dict[str, Any] = {"symbol": symbol}
        if order_id is not None:
            params["orderId"] = order_id
        elif orig_client_order_id:
            params["origClientOrderId"] = orig_client_order_id
        else:
            raise BinanceAPIError("order_id or orig_client_order_id is required")
        return self._signed_request("DELETE", "/fapi/v1/order", params=params)

    def account_trades(
        self,
        *,
        symbol: str,
        order_id: int | None = None,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        params: dict[str, Any] = {"symbol": symbol, "limit": limit}
        if order_id is not None:
            params["orderId"] = order_id
        return self._signed_request("GET", "/fapi/v1/userTrades", params=params)

    def _signed_request(
        self,
        method: str,
        path: str,
        params: dict[str, Any] | None = None,
        *,
        base_url: str | None = None,
    ) -> dict[str, Any]:
        if not self.api_key or not self.api_secret:
            raise BinanceAPIError("BINANCE_API_KEY and BINANCE_API_SECRET are required")
        signed_params = dict(params or {})
        signed_params["timestamp"] = int(time.time() * 1000)
        signed_params["recvWindow"] = 5000
        query = urlencode(signed_params)
        signature = hmac.new(
            self.api_secret.encode("utf-8"),
            query.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()
        signed_params["signature"] = signature
        return self._request(
            method, path, params=signed_params, signed=True,
            base_url=base_url,
        )

    def _request(
        self,
        method: str,
        path: str,
        params: dict[str, Any] | None = None,
        signed: bool = False,
        *,
        base_url: str | None = None,
    ) -> Any:
        query = urlencode(params or {})
        url = f"{base_url or self.base_url}{path}"
        if query:
            url = f"{url}?{query}"
        request = Request(url, method=method.upper())
        if signed or self.api_key:
            request.add_header("X-MBX-APIKEY", self.api_key)
        try:
            with urlopen(request, timeout=self.timeout) as response:
                payload = response.read().decode("utf-8")
        except HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")
            raise BinanceAPIError(f"Binance API error {exc.code}: {detail}") from exc
        return json.loads(payload)

    @staticmethod
    def _format_float(value: float) -> str:
        return f"{value:.12f}".rstrip("0").rstrip(".")
