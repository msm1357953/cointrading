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

    def new_order(self, intent: OrderIntent) -> dict[str, Any]:
        params: dict[str, Any] = {
            "symbol": intent.symbol,
            "side": intent.side,
            "type": intent.order_type,
            "quantity": self._format_float(intent.quantity),
        }
        if intent.price is not None:
            params["price"] = self._format_float(intent.price)
        if intent.time_in_force:
            params["timeInForce"] = intent.time_in_force
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
        return self._request(method, path, params=signed_params, signed=True)

    def _request(
        self,
        method: str,
        path: str,
        params: dict[str, Any] | None = None,
        signed: bool = False,
    ) -> Any:
        query = urlencode(params or {})
        url = f"{self.base_url}{path}"
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
