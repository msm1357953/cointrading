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
from cointrading.exchange_filters import SymbolFilters
from cointrading.market_context import collect_market_context, market_context_rows_text
from cointrading.market_regime import evaluate_market_regime, market_regime_rows_text
from cointrading.meta_strategy import meta_report_text
from cointrading.refined_entry_gate import refined_entry_report_text
from cointrading.risk_state import evaluate_runtime_risk, risk_mode_ko
from cointrading.research_probe import vibe_probe_report_text
from cointrading.scalping import (
    ScalpSignalEngine,
    default_scalp_log_path,
    scalp_report_rows_text,
)
from cointrading.storage import TradingStore, default_db_path, kst_from_ms
from cointrading.strategy_miner import strategy_mine_report_text, strategy_refine_report_text
from cointrading.strategy_notify import strategy_notification_text
from cointrading.strategy_router import evaluate_strategy_setups, strategy_setups_text
from cointrading.symbol_supervisor import (
    refresh_supervisor_inputs,
    supervise_symbols,
    supervisor_report_text,
)
from cointrading.tactical_radar import evaluate_tactical_radar, tactical_radar_text


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
        "위험모드": "risk",
        "리스크": "risk",
        "리스크모드": "risk",
        "risk": "risk",
        "수수료": "fees",
        "fee": "fees",
        "fees": "fees",
        "가격": "price",
        "price": "price",
        "시장": "market",
        "장세": "market",
        "시장상태": "market",
        "장상태": "market",
        "라우터": "market",
        "macro": "market",
        "market": "market",
        "시장상황": "market_context",
        "컨텍스트": "market_context",
        "market_context": "market_context",
        "주문": "orders",
        "주문기록": "orders",
        "orders": "orders",
        "포지션": "cycles",
        "사이클": "cycles",
        "상태머신": "cycles",
        "cycles": "cycles",
        "정지": "pause",
        "멈춰": "pause",
        "pause": "pause",
        "재개": "resume",
        "resume": "resume",
        # Funding-carry mean reversion (active strategy)
        "펀딩": "funding",
        "펀비": "funding",
        "funding": "funding",
        "펀딩보고": "funding_report",
        "펀딩결과": "funding_report",
        "펀비결과": "funding_report",
        "펀비보고": "funding_report",
        "펀딩준비": "funding_ready",
        "펀비준비": "funding_ready",
        "펀딩설정": "funding_config",
        "펀비설정": "funding_config",
        # Wick reversion (active strategy)
        "꼬리": "wick",
        "wick": "wick",
        "꼬리잡기": "wick",
        "꼬리보고": "wick_report",
        "꼬리결과": "wick_report",
        "꼬리준비": "wick_ready",
        "꼬리설정": "wick_config",
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
        if command == "market":
            return self.market_text(args)
        if command == "market_context":
            return self.market_context_text(args)
        if command == "orders":
            return self.orders_text()
        if command == "cycles":
            return self.cycles_text()
        if command == "funding":
            return self.funding_text()
        if command == "funding_report":
            return self.funding_report_text()
        if command == "funding_ready":
            return self.funding_ready_text()
        if command == "funding_config":
            return self.funding_config_text()
        if command == "wick":
            return self.wick_text()
        if command == "wick_report":
            return self.wick_report_text()
        if command == "wick_ready":
            return self.wick_ready_text()
        if command == "wick_config":
            return self.wick_config_text()
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
                "■ 봇 / 계좌",
                "상태       - 봇 모드 + 펀딩 전략 현황 한 눈에",
                "계좌       - Binance 선물 계좌 요약",
                "위험       - 리스크 한도 / 런타임 리스크 모드",
                "수수료     - BNB 할인과 현재 수수료",
                "가격 BTCUSDC - 현재 가격",
                "정지 / 재개 - 자동 진입 일시정지/해제",
                "",
                "■ 펀딩 평균회귀 전략",
                "펀딩       - OPEN 페이퍼 + 5심볼 현재 펀딩비 (★=트리거)",
                "펀딩보고   - 페이퍼 누적 성과 (n, 승률, PnL, 심볼별)",
                "펀딩준비   - 라이브 게이트 (5+ closed, sum≥0, WR≥40%)",
                "펀딩설정   - 임계값 / 노셔널 / 심볼 / SL / 보유시간",
                "",
                "■ 꼬리 잡기 전략 (5분봉)",
                "꼬리       - OPEN 페이퍼 + 트리거 조건",
                "꼬리보고   - 페이퍼 누적 성과",
                "꼬리준비   - 라이브 게이트",
                "꼬리설정   - wick/drop 임계값 / SL / 보유시간",
                "",
                "■ 시장 데이터",
                "장세       - 매크로 regime 분류",
                "시장상황 BTCUSDC - 펀딩/프리미엄/OI/스프레드/유동성",
                "주문       - 최근 주문/차단 기록",
                "포지션     - 활성 사이클",
                "",
                f"chat_id: {chat_id}",
            ]
        )

    def status_text(self) -> str:
        from cointrading.funding_carry_notify import evaluate_live_ready as fund_eval
        from cointrading.funding_lifecycle import (
            STATUS_CLOSED as F_CLOSED, STATUS_OPEN as F_OPEN,
            STATUS_STOPPED as F_STOPPED, STRATEGY_NAME as F_NAME,
        )
        from cointrading.wick_carry_notify import evaluate_live_ready as wick_eval
        from cointrading.wick_lifecycle import (
            STATUS_CLOSED as W_CLOSED, STATUS_OPEN as W_OPEN,
            STATUS_STOPPED as W_STOPPED, STRATEGY_NAME as W_NAME,
        )

        cfg = self.trading_config
        mode = "testnet" if cfg.testnet else "mainnet"
        dry_run = "on" if cfg.dry_run else "off"
        paused = "yes" if self.state.paused else "no"
        store = TradingStore(default_db_path())
        risk_state = evaluate_runtime_risk(store, cfg)

        def _counts(strategy: str, statuses_open: tuple, statuses_closed: tuple) -> tuple[int, int]:
            with store.connect() as connection:
                open_n = connection.execute(
                    "SELECT COUNT(*) FROM strategy_cycles WHERE strategy=? AND status=?",
                    (strategy, statuses_open[0]),
                ).fetchone()[0]
                closed_n = connection.execute(
                    "SELECT COUNT(*) FROM strategy_cycles WHERE strategy=? AND status IN (?, ?)",
                    (strategy, statuses_closed[0], statuses_closed[1]),
                ).fetchone()[0]
            return open_n, closed_n

        f_open, f_closed = _counts(F_NAME, (F_OPEN,), (F_CLOSED, F_STOPPED))
        w_open, w_closed = _counts(W_NAME, (W_OPEN,), (W_CLOSED, W_STOPPED))
        f_ready = fund_eval(store)
        w_ready = wick_eval(store)
        f_armed = (not cfg.dry_run) and cfg.live_trading_enabled and cfg.funding_carry_live_enabled
        w_armed = (not cfg.dry_run) and cfg.live_trading_enabled and cfg.wick_carry_live_enabled

        return "\n".join(
            [
                "■ 봇 상태",
                f"  모드        : {mode}",
                f"  dry-run     : {dry_run}",
                f"  일시정지    : {paused}",
                f"  위험모드    : {risk_mode_ko(risk_state.mode)}",
                f"  신규 진입   : {'허용' if risk_state.allows_new_entries else '차단'}",
                f"  기준 자산   : {cfg.initial_equity:.2f} {cfg.equity_asset}",
                "",
                "■ 펀딩 평균회귀 (활성 전략 #1)",
                f"  활성={cfg.funding_carry_enabled}  OPEN {f_open}  CLOSED {f_closed} ({f_ready.win_n}W/{f_ready.loss_n}L)  PnL {f_ready.sum_pnl:+.4f}",
                f"  라이브 게이트: {'✅' if f_ready.ready else '❌'}  모드: {'🔴 ARMED' if f_armed else '안전 (페이퍼)'}",
                "",
                "■ 꼬리 잡기 (활성 전략 #2)",
                f"  활성={cfg.wick_carry_enabled}  OPEN {w_open}  CLOSED {w_closed} ({w_ready.win_n}W/{w_ready.loss_n}L)  PnL {w_ready.sum_pnl:+.4f}",
                f"  라이브 게이트: {'✅' if w_ready.ready else '❌'}  모드: {'🔴 ARMED' if w_armed else '안전 (페이퍼)'}",
                "",
                "자세한 정보: '펀딩' / '펀딩보고' / '꼬리' / '꼬리보고'",
            ]
        )

    def risk_text(self) -> str:
        risk_state = evaluate_runtime_risk(
            TradingStore(default_db_path()),
            self.trading_config,
        )
        return "\n".join(
            [
                "리스크 한도",
                f"최대 낙폭 정지: {self.trading_config.max_drawdown_pct:.2%}",
                f"일손실 정지: {self.trading_config.daily_loss_pct:.2%}",
                f"런타임 일손실 정지: {self.trading_config.runtime_risk_daily_loss_pct:.2%}",
                f"거래당 리스크: {self.trading_config.risk_per_trade_pct:.2%}",
                f"최대 노출: 자산의 {self.trading_config.max_notional_multiplier:.2f}배",
                f"최대 레버리지: {self.trading_config.max_leverage:.2f}배",
                "",
                risk_state.to_text(),
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

    def market_text(self, args: list[str]) -> str:
        store = TradingStore(default_db_path())
        if args:
            symbol = args[0].upper()
            if not self.SYMBOL_PATTERN.match(symbol):
                return "심볼 형식이 이상합니다. 예: BTCUSDC"
            row = store.latest_market_regime(symbol)
            return market_regime_rows_text([row] if row is not None else [])
        return market_regime_rows_text(
            store.current_market_regimes(symbols=self.trading_config.scalp_symbols)
        )

    def market_context_text(self, args: list[str]) -> str:
        store = TradingStore(default_db_path())
        symbols = [item.upper() for item in args] if args else list(self.trading_config.scalp_symbols)
        for symbol in symbols:
            if not self.SYMBOL_PATTERN.match(symbol):
                return "심볼 형식이 이상합니다. 예: BTCUSDC"
        snapshots = []
        for symbol in symbols:
            try:
                snapshot = collect_market_context(self.exchange_client, symbol)
            except BinanceAPIError:
                continue
            store.insert_market_context(snapshot)
            snapshots.append(snapshot)
        if snapshots:
            return "\n\n".join(snapshot.to_text() for snapshot in snapshots)
        return market_context_rows_text(store.latest_market_contexts(symbols=symbols))

    def tactical_radar_text(self, args: list[str]) -> str:
        symbols = [item.upper() for item in args] if args else list(self.trading_config.scalp_symbols)
        for symbol in symbols:
            if not self.SYMBOL_PATTERN.match(symbol):
                return "심볼 형식이 이상합니다. 예: BTCUSDC"
        signals, warnings = evaluate_tactical_radar(
            self.exchange_client,
            config=self.trading_config,
            symbols=symbols,
        )
        return tactical_radar_text(signals, warnings=warnings, limit=8)

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
        rows = store.latest_strategy_batch()
        return strategy_notification_text(
            rows,
            reason="수동 조회",
            limit=8,
            config=self.trading_config,
            active_strategy_cycles=store.active_strategy_cycles(),
        )

    def research_text(self) -> str:
        return meta_report_text(limit=8)

    def strategy_mine_text(self) -> str:
        return strategy_mine_report_text(limit=8)

    def strategy_refine_text(self) -> str:
        return strategy_refine_report_text(limit=8)

    def refined_entry_text(self) -> str:
        return refined_entry_report_text(limit=8)

    def probe_text(self) -> str:
        return vibe_probe_report_text(limit=8)

    def entry_check_text(self, args: list[str]) -> str:
        symbol, notional = self._entry_check_args(args)
        if not self.SYMBOL_PATTERN.match(symbol):
            return "심볼 형식이 이상합니다. 예: ETHUSDC"
        store = TradingStore(default_db_path())
        risk = evaluate_runtime_risk(store, self.trading_config)
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
        klines_5m = self.exchange_client.klines(symbol=symbol, interval="5m", limit=120)
        klines_15m = self.exchange_client.klines(symbol=symbol, interval="15m", limit=120)
        try:
            macro_row = evaluate_market_regime(
                symbol=symbol,
                klines_15m=klines_15m,
                klines_1h=self.exchange_client.klines(symbol=symbol, interval="1h", limit=120),
            )
            store.insert_market_regime(macro_row)
        except BinanceAPIError:
            macro_row = store.latest_market_regime(symbol)
        setups = evaluate_strategy_setups(
            scalp_signal=signal,
            macro_row=macro_row,
            runtime_risk=risk,
            macro_max_age_ms=self.trading_config.macro_regime_max_age_minutes * 60_000,
            klines_5m=klines_5m,
            klines_15m=klines_15m,
        )
        lines: list[str] = []
        try:
            ticker = self.exchange_client.book_ticker(symbol)
            mid = (float(ticker["bidPrice"]) + float(ticker["askPrice"])) / 2.0
            filters = SymbolFilters.from_exchange_info(
                self.exchange_client.exchange_info(symbol),
                symbol,
            )
            lines.append(
                f"최소 주문 규모: 약 {filters.min_order_notional_at(mid):.4f} "
                f"{self.trading_config.equity_asset}"
            )
        except (AttributeError, BinanceAPIError, ValueError):
            pass
        lines.append(
            strategy_setups_text(
                setups,
                symbol=symbol,
                notional=notional,
                runtime_risk=risk,
            )
        )
        lines.append("주의: 이 명령은 점검만 하고 live 주문은 넣지 않습니다.")
        return "\n".join(lines)

    def live_supervisor_text(self, args: list[str]) -> str:
        symbol, notional = self._entry_check_args(args)
        symbols = [symbol] if args else list(self.trading_config.scalp_symbols)
        if symbol and not self.SYMBOL_PATTERN.match(symbol):
            return "심볼 형식이 이상합니다. 예: ETHUSDC"
        store = TradingStore(default_db_path())
        warnings = refresh_supervisor_inputs(self.exchange_client, store, symbols)
        reports = supervise_symbols(
            self.exchange_client,
            store,
            self.trading_config,
            symbols,
            notional=notional,
        )
        text = supervisor_report_text(reports)
        if not warnings:
            return text
        return "\n".join(["수집 경고", *[f"- {warning}" for warning in warnings], "", text])

    def cycles_text(self) -> str:
        store = TradingStore(default_db_path())
        cycles = store.recent_scalp_cycles(limit=5)
        strategy_cycles = store.recent_strategy_cycles(limit=5)
        if not cycles and not strategy_cycles:
            return "아직 상태머신 기록이 없습니다."
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
        if strategy_cycles:
            lines.append("")
            lines.append("최근 전략 상태머신 (KST)")
        for cycle in strategy_cycles:
            pnl = ""
            if cycle["realized_pnl"] is not None:
                pnl = f" pnl={float(cycle['realized_pnl']):.6f}"
            lines.append(
                f"{kst_from_ms(int(cycle['updated_ms']))} "
                f"{cycle['strategy']} {cycle['symbol']} {cycle['side']} {cycle['status']} "
                f"{cycle['reason'] or ''}{pnl}"
            )
        return "\n".join(lines)

    # ----- Funding-carry strategy (active) -----

    def funding_text(self) -> str:
        from cointrading.funding_lifecycle import STATUS_OPEN, STRATEGY_NAME

        cfg = self.trading_config
        store = TradingStore(default_db_path())
        with store.connect() as connection:
            open_rows = list(connection.execute(
                "SELECT * FROM strategy_cycles WHERE strategy=? AND status=? ORDER BY opened_ms DESC",
                (STRATEGY_NAME, STATUS_OPEN),
            ))

        lines = [f"■ 펀딩 평균회귀 (active={cfg.funding_carry_enabled})"]
        if not open_rows:
            lines.append("OPEN 페이퍼 없음.")
        else:
            lines.append(f"OPEN 페이퍼 {len(open_rows)}건:")
            for row in open_rows:
                lines.append(
                    f"  {row['symbol']} entry={float(row['entry_price']):.6f} "
                    f"stop={float(row['stop_price']):.6f} "
                    f"opened={kst_from_ms(int(row['opened_ms']))}"
                )

        lines.append("")
        lines.append("현재 펀딩비 (5심볼):")
        for symbol in cfg.funding_carry_symbols:
            try:
                resp = self.exchange_client.funding_rate(symbol, limit=1)
            except (AttributeError, BinanceAPIError):
                lines.append(f"  {symbol}: API 오류")
                continue
            if not resp:
                lines.append(f"  {symbol}: 데이터 없음")
                continue
            rate = float(resp[0]["fundingRate"])
            mark = "★" if rate <= -cfg.funding_carry_threshold else " "
            lines.append(f"  {mark} {symbol}: {rate * 100:+.4f}%")
        lines.append(f"트리거: ≤ -{cfg.funding_carry_threshold * 100:.4f}% (★ 표시)")
        return "\n".join(lines)

    def funding_report_text(self) -> str:
        from cointrading.funding_carry_notify import evaluate_live_ready
        from cointrading.funding_lifecycle import STATUS_CLOSED, STATUS_STOPPED, STRATEGY_NAME

        store = TradingStore(default_db_path())
        with store.connect() as connection:
            rows = list(connection.execute(
                """
                SELECT symbol, status, realized_pnl FROM strategy_cycles
                WHERE strategy=? AND status IN (?, ?)
                """,
                (STRATEGY_NAME, STATUS_CLOSED, STATUS_STOPPED),
            ))
        if not rows:
            return "■ 펀딩 페이퍼 결과\n아직 닫힌 사이클이 없습니다."

        wins = [r for r in rows if r["realized_pnl"] is not None and r["realized_pnl"] > 0]
        losses = [r for r in rows if r["realized_pnl"] is not None and r["realized_pnl"] <= 0]
        sum_pnl = sum(float(r["realized_pnl"] or 0) for r in rows)
        n = len(rows)
        wr = len(wins) / n if n else 0.0
        avg_win = sum(float(r["realized_pnl"]) for r in wins) / len(wins) if wins else 0.0
        avg_loss = sum(float(r["realized_pnl"]) for r in losses) / len(losses) if losses else 0.0

        per_symbol: dict[str, list[float]] = {}
        for r in rows:
            per_symbol.setdefault(r["symbol"], []).append(float(r["realized_pnl"] or 0))

        lines = [
            "■ 펀딩 페이퍼 결과 (전체)",
            f"  closed: {n}건 (승 {len(wins)} / 패 {len(losses)})",
            f"  승률: {wr * 100:.0f}%",
            f"  평균 승: {avg_win:+.4f}  평균 패: {avg_loss:+.4f}",
            f"  누적 PnL: {sum_pnl:+.4f} USDC",
            "",
            "심볼별:",
        ]
        for symbol in sorted(per_symbol):
            pnls = per_symbol[symbol]
            lines.append(
                f"  {symbol}: n={len(pnls)} sum={sum(pnls):+.4f} avg={sum(pnls)/len(pnls):+.4f}"
            )
        status = evaluate_live_ready(store)
        lines.append("")
        lines.append("라이브 게이트: " + ("✅ 통과" if status.ready else "❌ 미통과"))
        for r in status.reasons:
            lines.append(f"  - {r}")
        return "\n".join(lines)

    def funding_ready_text(self) -> str:
        from cointrading.funding_carry_notify import (
            LIVE_READY_MIN_CLOSED,
            LIVE_READY_MIN_SUM_PNL,
            LIVE_READY_MIN_WIN_RATE,
            evaluate_live_ready,
        )

        store = TradingStore(default_db_path())
        status = evaluate_live_ready(store)
        cfg = self.trading_config
        lines = [
            "■ 펀딩 라이브 게이트",
            f"  closed cycles : {status.closed_n} / 필요 ≥{LIVE_READY_MIN_CLOSED}",
            f"  sum PnL       : {status.sum_pnl:+.4f} USDC / 필요 ≥{LIVE_READY_MIN_SUM_PNL:+.2f}",
            f"  win rate      : {status.win_rate * 100:.0f}% / 필요 ≥{LIVE_READY_MIN_WIN_RATE * 100:.0f}%",
            f"  ({status.win_n}W / {status.loss_n}L)",
            "",
        ]
        if status.ready:
            lines.append("✅ 게이트 통과. 라이브 검토 가능.")
        else:
            lines.append("❌ 미충족 항목:")
            for r in status.reasons:
                lines.append(f"  - {r}")
        lines.append("")
        lines.append("현재 라이브 모드 (3중 게이트):")
        lines.append(f"  COINTRADING_DRY_RUN={'true' if cfg.dry_run else 'false'}")
        lines.append(f"  COINTRADING_LIVE_TRADING_ENABLED={'true' if cfg.live_trading_enabled else 'false'}")
        lines.append(f"  COINTRADING_FUNDING_CARRY_LIVE_ENABLED={'true' if cfg.funding_carry_live_enabled else 'false'}")
        lines.append("(라이브 진입 코드는 아직 미구현 — 게이트 통과 후 추가 작업 필요)")
        return "\n".join(lines)

    def funding_config_text(self) -> str:
        cfg = self.trading_config
        lines = [
            "■ 펀딩 전략 설정",
            f"  활성       : {cfg.funding_carry_enabled}",
            f"  심볼       : {', '.join(cfg.funding_carry_symbols)}",
            f"  진입 임계값 : funding ≤ -{cfg.funding_carry_threshold * 100:.4f}%",
            f"  체크 윈도우 : 펀딩 정산 후 {cfg.funding_carry_check_window_minutes}분 이내",
            f"  노셔널     : {cfg.funding_carry_notional} USDC",
            f"  스탑로스   : -{cfg.funding_carry_stop_loss_bps:.0f} bps",
            f"  보유시간   : {cfg.funding_carry_max_hold_seconds // 3600}시간",
            "",
            "정산 시각 (UTC): 00:00, 08:00, 16:00",
            "(KST: 09:00, 17:00, 01:00 다음날)",
        ]
        return "\n".join(lines)

    # ----- end funding -----

    # ----- Wick reversion strategy (active) -----

    def wick_text(self) -> str:
        from cointrading.wick_lifecycle import STATUS_OPEN as WICK_OPEN, STRATEGY_NAME as WICK_NAME

        cfg = self.trading_config
        store = TradingStore(default_db_path())
        with store.connect() as connection:
            open_rows = list(connection.execute(
                "SELECT * FROM strategy_cycles WHERE strategy=? AND status=? ORDER BY opened_ms DESC",
                (WICK_NAME, WICK_OPEN),
            ))
        lines = [f"■ 꼬리 잡기 (active={cfg.wick_carry_enabled})"]
        if not open_rows:
            lines.append("OPEN 페이퍼 없음.")
        else:
            lines.append(f"OPEN 페이퍼 {len(open_rows)}건:")
            for row in open_rows:
                lines.append(
                    f"  {row['symbol']} entry={float(row['entry_price']):.6f} "
                    f"stop={float(row['stop_price']):.6f} "
                    f"opened={kst_from_ms(int(row['opened_ms']))}"
                )
        lines.append("")
        lines.append(
            f"트리거: 5분봉 lower_wick≥{cfg.wick_carry_min_wick_ratio:.2f} + "
            f"intrabar drop≥{cfg.wick_carry_min_drop_pct * 100:.1f}%"
        )
        lines.append(f"보유: {cfg.wick_carry_max_hold_seconds // 3600}h, SL: -{cfg.wick_carry_stop_loss_bps:.0f}bps")
        return "\n".join(lines)

    def wick_report_text(self) -> str:
        from cointrading.wick_carry_notify import evaluate_live_ready as wick_eval_ready
        from cointrading.wick_lifecycle import (
            STATUS_CLOSED as WICK_CLOSED,
            STATUS_STOPPED as WICK_STOPPED,
            STRATEGY_NAME as WICK_NAME,
        )

        store = TradingStore(default_db_path())
        with store.connect() as connection:
            rows = list(connection.execute(
                """
                SELECT symbol, status, realized_pnl FROM strategy_cycles
                WHERE strategy=? AND status IN (?, ?)
                """,
                (WICK_NAME, WICK_CLOSED, WICK_STOPPED),
            ))
        if not rows:
            return "■ 꼬리 페이퍼 결과\n아직 닫힌 사이클이 없습니다."

        wins = [r for r in rows if r["realized_pnl"] is not None and r["realized_pnl"] > 0]
        losses = [r for r in rows if r["realized_pnl"] is not None and r["realized_pnl"] <= 0]
        sum_pnl = sum(float(r["realized_pnl"] or 0) for r in rows)
        n = len(rows)
        wr = len(wins) / n if n else 0.0
        avg_win = sum(float(r["realized_pnl"]) for r in wins) / len(wins) if wins else 0.0
        avg_loss = sum(float(r["realized_pnl"]) for r in losses) / len(losses) if losses else 0.0

        per_symbol: dict[str, list[float]] = {}
        for r in rows:
            per_symbol.setdefault(r["symbol"], []).append(float(r["realized_pnl"] or 0))

        lines = [
            "■ 꼬리 페이퍼 결과 (전체)",
            f"  closed: {n}건 (승 {len(wins)} / 패 {len(losses)})",
            f"  승률: {wr * 100:.0f}%",
            f"  평균 승: {avg_win:+.4f}  평균 패: {avg_loss:+.4f}",
            f"  누적 PnL: {sum_pnl:+.4f} USDC",
            "",
            "심볼별:",
        ]
        for symbol in sorted(per_symbol):
            pnls = per_symbol[symbol]
            lines.append(
                f"  {symbol}: n={len(pnls)} sum={sum(pnls):+.4f} avg={sum(pnls) / len(pnls):+.4f}"
            )
        status = wick_eval_ready(store)
        lines.append("")
        lines.append("라이브 게이트: " + ("✅ 통과" if status.ready else "❌ 미통과"))
        for r in status.reasons:
            lines.append(f"  - {r}")
        return "\n".join(lines)

    def wick_ready_text(self) -> str:
        from cointrading.wick_carry_notify import (
            LIVE_READY_MIN_CLOSED as WICK_MIN_CLOSED,
            LIVE_READY_MIN_SUM_PNL as WICK_MIN_SUM,
            LIVE_READY_MIN_WIN_RATE as WICK_MIN_WR,
            evaluate_live_ready as wick_eval_ready,
        )

        store = TradingStore(default_db_path())
        status = wick_eval_ready(store)
        cfg = self.trading_config
        lines = [
            "■ 꼬리 라이브 게이트",
            f"  closed cycles : {status.closed_n} / 필요 ≥{WICK_MIN_CLOSED}",
            f"  sum PnL       : {status.sum_pnl:+.4f} USDC / 필요 ≥{WICK_MIN_SUM:+.2f}",
            f"  win rate      : {status.win_rate * 100:.0f}% / 필요 ≥{WICK_MIN_WR * 100:.0f}%",
            f"  ({status.win_n}W / {status.loss_n}L)",
            "",
        ]
        if status.ready:
            lines.append("✅ 게이트 통과. 라이브 검토 가능.")
        else:
            lines.append("❌ 미충족 항목:")
            for r in status.reasons:
                lines.append(f"  - {r}")
        lines.append("")
        lines.append("현재 라이브 모드 (3중 게이트):")
        lines.append(f"  COINTRADING_DRY_RUN={'true' if cfg.dry_run else 'false'}")
        lines.append(f"  COINTRADING_LIVE_TRADING_ENABLED={'true' if cfg.live_trading_enabled else 'false'}")
        lines.append(f"  COINTRADING_WICK_CARRY_LIVE_ENABLED={'true' if cfg.wick_carry_live_enabled else 'false'}")
        lines.append("(라이브 진입 코드는 아직 미구현 — 게이트 통과 후 추가 작업 필요)")
        return "\n".join(lines)

    def wick_config_text(self) -> str:
        cfg = self.trading_config
        lines = [
            "■ 꼬리 전략 설정",
            f"  활성       : {cfg.wick_carry_enabled}",
            f"  심볼       : {', '.join(cfg.wick_carry_symbols)}",
            f"  wick 임계값 : ≥{cfg.wick_carry_min_wick_ratio:.2f}",
            f"  drop 임계값 : ≥{cfg.wick_carry_min_drop_pct * 100:.1f}% (intrabar (open-low)/open)",
            f"  fresh 윈도우: 봉 마감 후 {cfg.wick_carry_freshness_seconds}초 이내",
            f"  cooldown    : 청산 후 {cfg.wick_carry_cooldown_seconds // 60}분 재진입 금지",
            f"  노셔널     : {cfg.wick_carry_notional} USDC",
            f"  스탑로스   : -{cfg.wick_carry_stop_loss_bps:.0f} bps",
            f"  보유시간   : {cfg.wick_carry_max_hold_seconds // 3600}시간",
            "",
            "기준봉: 5분봉 마감 직후 체크. 1분 timer 발화.",
        ]
        return "\n".join(lines)

    # ----- end wick -----

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

    def _entry_check_args(self, args: list[str]) -> tuple[str, float]:
        symbol = self.trading_config.scalp_symbols[0]
        notional = self.trading_config.post_only_order_notional
        for arg in args:
            upper = arg.upper()
            if self.SYMBOL_PATTERN.match(upper) and any(
                upper.endswith(asset) for asset in ("USDC", "USDT")
            ):
                symbol = upper
                continue
            try:
                notional = float(arg)
            except ValueError:
                continue
        return symbol, notional

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
