from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
import re
import time
from typing import Any
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from cointrading.account import account_summary_text
from cointrading.config import TelegramConfig, TradingConfig
from cointrading.exchange.binance_usdm import BinanceAPIError, BinanceUSDMClient
from cointrading.scalping import (
    ScalpSignalEngine,
    default_scalp_log_path,
    scalp_report_rows_text,
)
from cointrading.storage import TradingStore, default_db_path, kst_from_ms
from cointrading.strategy_notify import strategy_notification_text


DEFAULT_FEE_SYMBOLS = ["BTCUSDC", "ETHUSDC"]


class TelegramAPIError(RuntimeError):
    pass


class TelegramConfigError(RuntimeError):
    pass


@dataclass
class TelegramBotState:
    paused: bool = False
    last_update_id: int | None = None

    @classmethod
    def load(cls, path: Path) -> "TelegramBotState":
        if not path.exists():
            return cls()
        payload = json.loads(path.read_text())
        return cls(
            paused=bool(payload.get("paused", False)),
            last_update_id=payload.get("last_update_id"),
        )

    def save(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            json.dumps(
                {
                    "paused": self.paused,
                    "last_update_id": self.last_update_id,
                },
                indent=2,
                sort_keys=True,
            )
        )


class TelegramClient:
    BASE_URL = "https://api.telegram.org"

    def __init__(self, config: TelegramConfig, timeout: float = 35.0) -> None:
        if not config.bot_token:
            raise TelegramConfigError("TELEGRAM_BOT or TELEGRAM_BOT_TOKEN is required")
        self.config = config
        self.timeout = timeout

    def get_me(self) -> dict[str, Any]:
        return self._api_request("getMe")

    def send_message(self, text: str, chat_id: str | None = None) -> dict[str, Any]:
        target_chat_id = chat_id or self.config.default_chat_id
        if not target_chat_id:
            raise TelegramConfigError("TELEGRAM_CHAT_ID or --chat-id is required")
        return self._api_request(
            "sendMessage",
            {
                "chat_id": target_chat_id,
                "text": text,
                "disable_web_page_preview": "true",
            },
            method="POST",
        )

    def get_updates(
        self,
        offset: int | None = None,
        limit: int = 10,
        timeout: int = 0,
    ) -> list[dict[str, Any]]:
        params: dict[str, Any] = {"limit": limit, "timeout": timeout}
        if offset is not None:
            params["offset"] = offset
        response = self._api_request("getUpdates", params)
        return list(response.get("result", []))

    def _api_request(
        self,
        method_name: str,
        params: dict[str, Any] | None = None,
        method: str = "GET",
    ) -> dict[str, Any]:
        url = f"{self.BASE_URL}/bot{self.config.bot_token}/{method_name}"
        data = None
        request_url = url
        encoded = urlencode(params or {}).encode("utf-8")
        if method == "GET" and encoded:
            request_url = f"{url}?{encoded.decode('utf-8')}"
        elif method == "POST":
            data = encoded

        request = Request(request_url, data=data, method=method)
        if method == "POST":
            request.add_header("Content-Type", "application/x-www-form-urlencoded")
        try:
            with urlopen(request, timeout=self.timeout) as response:
                payload = json.loads(response.read().decode("utf-8"))
        except HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")
            raise TelegramAPIError(f"Telegram API error {exc.code}: {detail}") from exc

        if not payload.get("ok"):
            raise TelegramAPIError(f"Telegram API returned not ok: {payload}")
        return payload


class TelegramCommandProcessor:
    SYMBOL_PATTERN = re.compile(r"^[A-Z0-9]{3,20}$")
    COMMAND_ALIASES = {
        "start": "help",
        "help": "help",
        "도움말": "help",
        "명령어": "help",
        "상태": "status",
        "status": "status",
        "계좌": "account",
        "account": "account",
        "위험": "risk",
        "리스크": "risk",
        "risk": "risk",
        "수수료": "fees",
        "fee": "fees",
        "fees": "fees",
        "가격": "price",
        "price": "price",
        "스캘핑": "scalp",
        "스캘프": "scalp",
        "신호": "scalp",
        "장상태": "scalp",
        "scalp": "scalp",
        "보고": "scalp_report",
        "요약": "scalp_report",
        "리포트": "scalp_report",
        "결과": "scalp_report",
        "결과보고": "scalp_report",
        "스캘핑보고": "scalp_report",
        "스캘프보고": "scalp_report",
        "전략": "strategy",
        "전략후보": "strategy",
        "strategy": "strategy",
        "주문": "orders",
        "주문기록": "orders",
        "orders": "orders",
        "포지션": "cycles",
        "사이클": "cycles",
        "상태머신": "cycles",
        "cycles": "cycles",
        "scalp_report": "scalp_report",
        "scalp-report": "scalp_report",
        "정지": "pause",
        "멈춰": "pause",
        "pause": "pause",
        "재개": "resume",
        "resume": "resume",
    }

    def __init__(
        self,
        telegram_config: TelegramConfig,
        trading_config: TradingConfig,
        state: TelegramBotState,
        exchange_client: BinanceUSDMClient | None = None,
        price_client: BinanceUSDMClient | None = None,
    ) -> None:
        self.telegram_config = telegram_config
        self.trading_config = trading_config
        self.state = state
        self.exchange_client = (
            exchange_client or price_client or BinanceUSDMClient(config=trading_config)
        )

    def handle_text(self, chat_id: str, text: str) -> str:
        command, args = self._parse_command(text)
        if command == "":
            return ""
        if command == "help":
            return self.help_text(chat_id)
        if not self.telegram_config.commands_enabled:
            return (
                "텔레그램 명령이 아직 꺼져 있습니다. TELEGRAM_ALLOWED_CHAT_IDS 설정 뒤 "
                "TELEGRAM_COMMANDS_ENABLED=true로 켜야 합니다."
            )
        if not self.is_authorized(chat_id):
            return (
                f"허용되지 않은 chat_id={chat_id} 입니다. 이 ID를 "
                "TELEGRAM_ALLOWED_CHAT_IDS에 추가해야 합니다."
            )
        if command == "status":
            return self.status_text()
        if command == "risk":
            return self.risk_text()
        if command == "fees":
            return self.fees_text(args)
        if command == "account":
            return self.account_text()
        if command == "price":
            return self.price_text(args)
        if command == "scalp":
            return self.scalp_text(args)
        if command == "scalp_report":
            return self.scalp_report_text(args)
        if command == "strategy":
            return self.strategy_text()
        if command == "orders":
            return self.orders_text()
        if command == "cycles":
            return self.cycles_text()
        if command == "pause":
            self.state.paused = True
            return "정지했습니다. 이후 자동매매 루프는 신규 진입을 거부해야 합니다."
        if command == "resume":
            self.state.paused = False
            return "재개했습니다. dry-run 안전장치는 그대로 유지됩니다."
        return "알 수 없는 명령입니다. '도움말'이라고 보내보세요."

    def is_authorized(self, chat_id: str) -> bool:
        return str(chat_id) in self.telegram_config.allowed_chat_ids

    def help_text(self, chat_id: str) -> str:
        return "\n".join(
            [
                "사용 가능한 명령어",
                "상태 - 봇/거래 모드 확인",
                "계좌 - Binance 선물 계좌 요약",
                "위험 - 리스크 한도 확인",
                "수수료 - BNB 할인과 현재 수수료 확인",
                "가격 BTCUSDC - 현재 가격 확인",
                "스캘핑 BTCUSDC - 현재 스캘핑 신호와 장 상태 확인",
                "보고 - 스캘핑 dry-run 결과와 장 상태별 성과 요약",
                "보고 BTCUSDC - BTCUSDC만 결과 요약",
                "보고 전체 - 예전 USDT 로그까지 포함",
                "전략 - maker/taker/hybrid 전략 후보 요약",
                "주문 - 최근 dry-run 주문/차단 기록",
                "포지션 - 스캘핑 상태머신 기록",
                "정지 - 자동매매 신규 진입 정지",
                "재개 - 자동매매 신규 진입 재개",
                f"chat_id: {chat_id}",
            ]
        )

    def status_text(self) -> str:
        mode = "testnet" if self.trading_config.testnet else "mainnet"
        dry_run = "on" if self.trading_config.dry_run else "off"
        paused = "yes" if self.state.paused else "no"
        return "\n".join(
            [
                "현재 상태",
                f"모드: {mode}",
                f"dry-run: {dry_run}",
                f"정지 상태: {paused}",
                "초기 기준 자산: "
                f"{self.trading_config.initial_equity:.2f} {self.trading_config.equity_asset}",
                f"스캘핑 대상: {', '.join(self.trading_config.scalp_symbols)}",
            ]
        )

    def risk_text(self) -> str:
        return "\n".join(
            [
                "리스크 한도",
                f"최대 낙폭 정지: {self.trading_config.max_drawdown_pct:.2%}",
                f"일손실 정지: {self.trading_config.daily_loss_pct:.2%}",
                f"거래당 리스크: {self.trading_config.risk_per_trade_pct:.2%}",
                f"최대 노출: 자산의 {self.trading_config.max_notional_multiplier:.2f}배",
                f"최대 레버리지: {self.trading_config.max_leverage:.2f}배",
            ]
        )

    def fees_text(self, args: list[str]) -> str:
        symbols = [item.upper() for item in args] if args else DEFAULT_FEE_SYMBOLS
        bnb_fee_enabled, bnb_balance = self._fee_context()
        usdc_balance = self._asset_balance("USDC")
        try:
            multi_assets = bool(self.exchange_client.multi_assets_margin().get("multiAssetsMargin"))
        except (AttributeError, BinanceAPIError):
            multi_assets = False
        lines = [
            "선물 수수료 상태",
            f"BNB 수수료 할인 설정: {'켜짐' if bnb_fee_enabled else '꺼짐'}",
            f"선물 지갑 BNB 잔고: {bnb_balance:.8f} BNB",
            "실제 할인 적용: "
            f"{'가능' if bnb_fee_enabled and bnb_balance > 0 else '불가'}",
            f"Multi-Assets Mode: {'켜짐' if multi_assets else '꺼짐'}",
            f"선물 지갑 USDC 잔고: {usdc_balance:.8f} USDC",
            "USDC 심볼 live 준비: "
            f"{'가능' if multi_assets or usdc_balance > 0 else '불가'}",
        ]
        for symbol in symbols:
            if not self.SYMBOL_PATTERN.match(symbol):
                return "심볼 형식이 이상합니다. 예: BTCUSDC"
            try:
                commission = self.exchange_client.commission_rate(symbol)
            except BinanceAPIError:
                continue
            maker = float(commission["makerCommissionRate"]) * 10_000.0
            taker = float(commission["takerCommissionRate"]) * 10_000.0
            if bnb_fee_enabled and bnb_balance > 0:
                maker *= 0.90
                taker *= 0.90
            lines.append(f"{symbol}: maker {maker:.2f}bps, taker {taker:.2f}bps")
        return "\n".join(lines)

    def price_text(self, args: list[str]) -> str:
        symbol = args[0].upper() if args else self.trading_config.scalp_symbols[0]
        if not self.SYMBOL_PATTERN.match(symbol):
            return "심볼 형식이 이상합니다. 예: BTCUSDC"
        klines = self.exchange_client.klines(symbol=symbol, interval="1m", limit=1)
        if not klines:
            return f"{symbol} 가격 데이터가 없습니다."
        latest = klines[-1]
        return f"{symbol} 최근 1분봉 종가: {latest.close:.4f}"

    def account_text(self) -> str:
        return account_summary_text(self.exchange_client.account_info())

    def scalp_text(self, args: list[str]) -> str:
        symbol = args[0].upper() if args else self.trading_config.scalp_symbols[0]
        if not self.SYMBOL_PATTERN.match(symbol):
            return "심볼 형식이 이상합니다. 예: BTCUSDC"
        funding_rows = self.exchange_client.funding_rate(symbol, limit=1)
        latest_funding = None
        if funding_rows:
            latest_funding = float(funding_rows[-1]["fundingRate"])
        commission = None
        try:
            commission = self.exchange_client.commission_rate(symbol)
        except BinanceAPIError:
            commission = None
        bnb_fee_enabled, bnb_balance = self._fee_context()
        signal = ScalpSignalEngine().evaluate(
            symbol=symbol,
            book_ticker=self.exchange_client.book_ticker(symbol),
            order_book=self.exchange_client.order_book(symbol, limit=20),
            klines=self.exchange_client.klines(symbol, interval="1m", limit=30),
            trading_config=self.trading_config,
            commission_rate=commission,
            latest_funding_rate=latest_funding,
            bnb_fee_discount_enabled=bnb_fee_enabled,
            bnb_balance=bnb_balance,
        )
        return signal.to_text()

    def scalp_report_text(self, args: list[str]) -> str:
        store = TradingStore(default_db_path())
        store.migrate_csv_signals(default_scalp_log_path())
        if args and args[0].lower() in {"all", "전체"}:
            rows = store.list_signals()
            return _with_recent_order_summary(scalp_report_rows_text(rows), store)
        symbol = args[0].upper() if args else None
        if symbol and not self.SYMBOL_PATTERN.match(symbol):
            return "심볼 형식이 이상합니다. 예: BTCUSDC"
        rows = store.list_signals(
            symbol,
            symbols=self.trading_config.scalp_symbols,
        )
        return _with_recent_order_summary(
            scalp_report_rows_text(
                rows,
                symbol=symbol,
                symbols=self.trading_config.scalp_symbols,
            ),
            store,
        )

    def orders_text(self) -> str:
        store = TradingStore(default_db_path())
        orders = store.recent_orders(limit=5)
        if not orders:
            return "아직 주문 기록이 없습니다."
        lines = ["최근 주문 기록 (KST)"]
        for order in orders:
            price = "n/a" if order["price"] is None else f"{float(order['price']):.4f}"
            lines.append(
                f"{kst_from_ms(int(order['timestamp_ms']))} "
                f"{order['symbol']} {order['side']} {order['status']} "
                f"qty={float(order['quantity']):.6f} price={price}"
            )
        return "\n".join(lines)

    def strategy_text(self) -> str:
        store = TradingStore(default_db_path())
        rows = store.latest_strategy_batch(limit=200)
        return strategy_notification_text(rows, reason="수동 조회", limit=8)

    def cycles_text(self) -> str:
        store = TradingStore(default_db_path())
        cycles = store.recent_scalp_cycles(limit=5)
        if not cycles:
            return "아직 스캘핑 상태머신 기록이 없습니다."
        lines = ["최근 스캘핑 상태머신 (KST)"]
        for cycle in cycles:
            pnl = ""
            if cycle["realized_pnl"] is not None:
                pnl = f" pnl={float(cycle['realized_pnl']):.6f}"
            lines.append(
                f"{kst_from_ms(int(cycle['updated_ms']))} "
                f"{cycle['symbol']} {cycle['side']} {cycle['status']} "
                f"{cycle['reason'] or ''}{pnl}"
            )
        return "\n".join(lines)

    def _fee_context(self) -> tuple[bool, float]:
        try:
            bnb_fee_enabled = bool(self.exchange_client.fee_burn_status().get("feeBurn"))
        except (AttributeError, BinanceAPIError):
            bnb_fee_enabled = False
        try:
            balances = self.exchange_client.account_balance()
        except (AttributeError, BinanceAPIError):
            return bnb_fee_enabled, 0.0
        for row in balances:
            if row.get("asset") == "BNB":
                return bnb_fee_enabled, float(row.get("availableBalance") or row.get("balance") or 0)
        return bnb_fee_enabled, 0.0

    def _asset_balance(self, asset: str) -> float:
        try:
            balances = self.exchange_client.account_balance()
        except (AttributeError, BinanceAPIError):
            return 0.0
        for row in balances:
            if row.get("asset") == asset:
                return float(row.get("availableBalance") or row.get("balance") or 0)
        return 0.0

    @staticmethod
    def _parse_command(text: str) -> tuple[str, list[str]]:
        stripped = text.strip()
        if not stripped:
            return "", []
        parts = stripped.split()
        raw_command = parts[0]
        if raw_command.startswith("/"):
            raw_command = raw_command[1:]
        raw_command = raw_command.split("@", 1)[0].lower().replace("-", "_")
        command = TelegramCommandProcessor.COMMAND_ALIASES.get(raw_command, raw_command)
        return command, parts[1:]


def default_state_path() -> Path:
    return Path(__file__).resolve().parents[2] / "data" / "telegram_state.json"


def poll_once(
    client: TelegramClient,
    processor: TelegramCommandProcessor,
    state: TelegramBotState,
    state_path: Path,
    limit: int = 10,
    timeout: int = 20,
) -> int:
    offset = state.last_update_id + 1 if state.last_update_id is not None else None
    updates = client.get_updates(offset=offset, limit=limit, timeout=timeout)
    handled = 0
    for update in updates:
        update_id = int(update["update_id"])
        state.last_update_id = update_id
        message = update.get("message") or update.get("edited_message") or {}
        chat = message.get("chat") or {}
        text = message.get("text") or ""
        chat_id = str(chat.get("id", ""))
        if chat_id and text:
            reply = processor.handle_text(chat_id, text)
            if reply:
                client.send_message(reply, chat_id=chat_id)
                handled += 1
    state.save(state_path)
    return handled


def poll_forever(
    client: TelegramClient,
    processor: TelegramCommandProcessor,
    state: TelegramBotState,
    state_path: Path,
    interval_seconds: float = 1.0,
    timeout: int = 20,
) -> None:
    while True:
        try:
            poll_once(client, processor, state, state_path, timeout=timeout)
        except Exception as exc:
            print(f"Telegram polling error: {exc}", flush=True)
        time.sleep(interval_seconds)


def _with_recent_order_summary(text: str, store: TradingStore) -> str:
    orders = store.recent_orders(limit=3)
    if not orders:
        return text
    lines = [text, "최근 주문/차단"]
    for order in orders:
        lines.append(f"- {order['symbol']} {order['side']} {order['status']}: {order['reason']}")
    return "\n".join(lines)
