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
            {"asset": "USDT", "balance": "0.00000000", "availableBalance": "0.00000000"},
            {"asset": "USDC", "balance": "1000.00000000", "availableBalance": "1000.00000000"},
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

    def spot_book_ticker(self, symbol: str):
        return {
            "symbol": symbol,
            "bidPrice": "649.00",
            "bidQty": "10",
            "askPrice": "650.00",
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
        if symbol.endswith("USDC"):
            return {
                "symbol": symbol,
                "makerCommissionRate": "0",
                "takerCommissionRate": "0.000400",
            }
        return {
            "symbol": symbol,
            "makerCommissionRate": "0.000200",
            "takerCommissionRate": "0.000500",
        }

    def exchange_info(self, symbol: str | None = None):
        return {
            "symbols": [
                {
                    "symbol": symbol or "ETHUSDC",
                    "filters": [
                        {"filterType": "PRICE_FILTER", "tickSize": "0.01"},
                        {
                            "filterType": "LOT_SIZE",
                            "minQty": "0.001",
                            "maxQty": "1000",
                            "stepSize": "0.001",
                        },
                        {"filterType": "MIN_NOTIONAL", "notional": "20"},
                    ],
                }
            ]
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
        self.assertIn("봇 상태", reply)
        self.assertIn("mainnet", reply)
        self.assertIn("dry-run", reply)
        self.assertIn("펀딩 평균회귀", reply)
        self.assertIn("꼬리 잡기", reply)

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
        self.assertIn("지갑: 1181.5509 USD-M", reply)
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
        reply = processor.handle_text("123", "수수료 BTCUSDC")
        self.assertIn("BNB 수수료 할인 설정: 켜짐", reply)
        self.assertIn("실제 할인 적용: 가능", reply)
        self.assertIn("USDC 심볼 live 준비: 가능", reply)
        self.assertIn("BTCUSDC: maker 0.00bps, taker 3.60bps", reply)

    def test_bnb_status_command(self) -> None:
        processor = TelegramCommandProcessor(
            TelegramConfig(
                allowed_chat_ids=frozenset({"123"}),
                commands_enabled=True,
            ),
            TradingConfig(
                dry_run=False,
                testnet=False,
                bnb_fee_topup_enabled=True,
                bnb_fee_topup_live_enabled=True,
            ),
            TelegramBotState(),
            exchange_client=FakeExchangeClient(),
        )
        reply = processor.handle_text("123", "BNB")
        self.assertIn("BNB 수수료 연료", reply)
        self.assertIn("선물 BNB", reply)
        self.assertIn("BNB보충", reply)

    def test_bnb_topup_command_reports_dry_run_lock(self) -> None:
        processor = TelegramCommandProcessor(
            TelegramConfig(
                allowed_chat_ids=frozenset({"123"}),
                commands_enabled=True,
            ),
            TradingConfig(
                dry_run=True,
                testnet=False,
                bnb_fee_topup_enabled=True,
                bnb_fee_topup_live_enabled=True,
                bnb_fee_topup_target_bnb=0.2,
            ),
            TelegramBotState(),
            exchange_client=FakeExchangeClient(),
        )
        reply = processor.handle_text("123", "BNB 보충 15")
        self.assertIn("BNB 보충", reply)
        self.assertIn("dry_run", reply)

    def test_market_command_reads_latest_regime_rows(self) -> None:
        processor = TelegramCommandProcessor(
            TelegramConfig(
                allowed_chat_ids=frozenset({"123"}),
                commands_enabled=True,
            ),
            TradingConfig(scalp_symbols=("BTCUSDC",)),
            TelegramBotState(),
            exchange_client=FakeExchangeClient(),
        )
        with (
            patch("cointrading.telegram_bot.TradingStore") as store_cls,
            patch("cointrading.telegram_bot.market_regime_rows_text", return_value="market ok") as text_fn,
        ):
            store = store_cls.return_value
            store.current_market_regimes.return_value = ["row"]
            reply = processor.handle_text("123", "장세")

        self.assertEqual(reply, "market ok")
        store.current_market_regimes.assert_called_once_with(symbols=("BTCUSDC",))
        text_fn.assert_called_once_with(["row"])

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
        self.assertIn("봇 상태", processor.handle_text("123", "상태"))
        self.assertIn("선물 수수료 상태", processor.handle_text("123", "수수료"))
        self.assertIn("펀딩 전략 설정", processor.handle_text("123", "펀딩설정"))
        self.assertIn("펀딩 라이브 게이트", processor.handle_text("123", "펀딩준비"))
        # Removed-legacy commands return the unknown-command message
        self.assertIn("알 수 없는", processor.handle_text("123", "스캘핑 BTCUSDC"))
        self.assertIn("알 수 없는", processor.handle_text("123", "레이더"))


if __name__ == "__main__":
    unittest.main()
