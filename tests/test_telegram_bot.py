import unittest
from unittest.mock import patch

from cointrading.config import TelegramConfig, TradingConfig
from cointrading.models import Kline
from cointrading.telegram_bot import TelegramBotState, TelegramCommandProcessor


class FakeExchangeClient:
    def klines(self, symbol: str, interval: str, limit: int):
        return [
            Kline(
                open_time=1,
                open=100.0,
                high=101.0,
                low=99.0,
                close=100.5,
                volume=1.0,
                close_time=2,
            )
        ]

    def account_info(self):
        return {
            "totalWalletBalance": "1181.55089676",
            "availableBalance": "1181.55089676",
            "totalUnrealizedProfit": "0.00000000",
            "totalMaintMargin": "0.00000000",
            "positions": [
                {"symbol": "BTCUSDT", "positionAmt": "0"},
                {"symbol": "ETHUSDT", "positionAmt": "0.10"},
            ],
        }

    def account_balance(self):
        return [
            {"asset": "BNB", "balance": "0.10000000", "availableBalance": "0.10000000"},
            {"asset": "USDT", "balance": "1000.00000000", "availableBalance": "1000.00000000"},
            {"asset": "USDC", "balance": "0.00000000", "availableBalance": "0.00000000"},
        ]

    def fee_burn_status(self):
        return {"feeBurn": True}

    def multi_assets_margin(self):
        return {"multiAssetsMargin": False}

    def book_ticker(self, symbol: str):
        return {
            "symbol": symbol,
            "bidPrice": "100.00",
            "bidQty": "10",
            "askPrice": "100.01",
            "askQty": "8",
        }

    def order_book(self, symbol: str, limit: int = 20):
        return {
            "bids": [["100.00", "20"], ["99.99", "10"]],
            "asks": [["100.01", "5"], ["100.02", "5"]],
        }

    def funding_rate(self, symbol: str, limit: int = 1):
        return [{"symbol": symbol, "fundingRate": "0.00001000"}]

    def commission_rate(self, symbol: str):
        return {
            "symbol": symbol,
            "makerCommissionRate": "0.000200",
            "takerCommissionRate": "0.000500",
        }


class TelegramCommandTests(unittest.TestCase):
    def test_commands_are_disabled_by_default(self) -> None:
        processor = TelegramCommandProcessor(
            TelegramConfig(allowed_chat_ids=frozenset({"123"})),
            TradingConfig(),
            TelegramBotState(),
            exchange_client=FakeExchangeClient(),
        )
        reply = processor.handle_text("123", "/status")
        self.assertIn("꺼져", reply)

    def test_unauthorized_chat_is_rejected(self) -> None:
        processor = TelegramCommandProcessor(
            TelegramConfig(
                allowed_chat_ids=frozenset({"123"}),
                commands_enabled=True,
            ),
            TradingConfig(),
            TelegramBotState(),
            exchange_client=FakeExchangeClient(),
        )
        reply = processor.handle_text("999", "/status")
        self.assertIn("허용되지 않은", reply)

    def test_status_for_authorized_chat(self) -> None:
        processor = TelegramCommandProcessor(
            TelegramConfig(
                allowed_chat_ids=frozenset({"123"}),
                commands_enabled=True,
            ),
            TradingConfig(dry_run=True, testnet=False),
            TelegramBotState(),
            exchange_client=FakeExchangeClient(),
        )
        reply = processor.handle_text("123", "/status")
        self.assertIn("모드: mainnet", reply)
        self.assertIn("dry-run: on", reply)
        self.assertIn("초기 기준 자산: 1000.00 USDC", reply)

    def test_pause_and_resume_mutate_state(self) -> None:
        state = TelegramBotState()
        processor = TelegramCommandProcessor(
            TelegramConfig(
                allowed_chat_ids=frozenset({"123"}),
                commands_enabled=True,
            ),
            TradingConfig(),
            state,
            exchange_client=FakeExchangeClient(),
        )
        processor.handle_text("123", "/pause")
        self.assertTrue(state.paused)
        processor.handle_text("123", "/resume")
        self.assertFalse(state.paused)

    def test_price_command_uses_price_client(self) -> None:
        processor = TelegramCommandProcessor(
            TelegramConfig(
                allowed_chat_ids=frozenset({"123"}),
                commands_enabled=True,
            ),
            TradingConfig(),
            TelegramBotState(),
            exchange_client=FakeExchangeClient(),
        )
        reply = processor.handle_text("123", "/price BTCUSDT")
        self.assertIn("BTCUSDT 최근 1분봉 종가: 100.5000", reply)

    def test_account_command_uses_account_info(self) -> None:
        processor = TelegramCommandProcessor(
            TelegramConfig(
                allowed_chat_ids=frozenset({"123"}),
                commands_enabled=True,
            ),
            TradingConfig(),
            TelegramBotState(),
            exchange_client=FakeExchangeClient(),
        )
        reply = processor.handle_text("123", "/account")
        self.assertIn("지갑: 1181.5509 USDT", reply)
        self.assertIn("열린 포지션: 1", reply)

    def test_fee_command_reports_bnb_discount(self) -> None:
        processor = TelegramCommandProcessor(
            TelegramConfig(
                allowed_chat_ids=frozenset({"123"}),
                commands_enabled=True,
            ),
            TradingConfig(),
            TelegramBotState(),
            exchange_client=FakeExchangeClient(),
        )
        reply = processor.handle_text("123", "수수료 BTCUSDT")
        self.assertIn("BNB 수수료 할인 설정: 켜짐", reply)
        self.assertIn("실제 할인 적용: 가능", reply)
        self.assertIn("USDC 심볼 live 준비: 불가", reply)
        self.assertIn("BTCUSDT: maker 1.80bps, taker 4.50bps", reply)

    def test_scalp_command_uses_market_microstructure(self) -> None:
        processor = TelegramCommandProcessor(
            TelegramConfig(
                allowed_chat_ids=frozenset({"123"}),
                commands_enabled=True,
            ),
            TradingConfig(),
            TelegramBotState(),
            exchange_client=FakeExchangeClient(),
        )
        reply = processor.handle_text("123", "/scalp BTCUSDT")
        self.assertIn("스캘핑 신호: BTCUSDT", reply)
        self.assertIn("메이커 왕복 비용: 3.60 bps", reply)
        self.assertIn("테이커 왕복 비용: 9.00 bps", reply)
        self.assertIn("BNB 수수료 할인: 적용 중", reply)

    def test_scalp_report_command(self) -> None:
        processor = TelegramCommandProcessor(
            TelegramConfig(
                allowed_chat_ids=frozenset({"123"}),
                commands_enabled=True,
            ),
            TradingConfig(),
            TelegramBotState(),
            exchange_client=FakeExchangeClient(),
        )
        with patch("cointrading.telegram_bot.scalp_report_text", return_value="report ok"):
            reply = processor.handle_text("123", "/scalp_report BTCUSDT")
        self.assertEqual(reply, "report ok")

    def test_korean_plain_commands(self) -> None:
        processor = TelegramCommandProcessor(
            TelegramConfig(
                allowed_chat_ids=frozenset({"123"}),
                commands_enabled=True,
            ),
            TradingConfig(),
            TelegramBotState(),
            exchange_client=FakeExchangeClient(),
        )
        self.assertIn("현재 상태", processor.handle_text("123", "상태"))
        self.assertIn("스캘핑 신호", processor.handle_text("123", "스캘핑 BTCUSDT"))
        self.assertIn("선물 수수료 상태", processor.handle_text("123", "수수료"))
        with patch("cointrading.telegram_bot.scalp_report_text", return_value="report ok"):
            self.assertEqual(processor.handle_text("123", "보고 BTCUSDT"), "report ok")


if __name__ == "__main__":
    unittest.main()
