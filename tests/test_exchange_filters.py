import unittest

from cointrading.exchange_filters import SymbolFilters
from cointrading.models import OrderIntent


def _exchange_info():
    return {
        "symbols": [
            {
                "symbol": "BTCUSDC",
                "filters": [
                    {"filterType": "PRICE_FILTER", "tickSize": "0.10"},
                    {
                        "filterType": "LOT_SIZE",
                        "minQty": "0.001",
                        "maxQty": "1000",
                        "stepSize": "0.001",
                    },
                    {"filterType": "MIN_NOTIONAL", "notional": "5"},
                ],
            }
        ]
    }


class ExchangeFilterTests(unittest.TestCase):
    def test_buy_post_only_price_rounds_down_and_quantity_floors(self) -> None:
        filters = SymbolFilters.from_exchange_info(_exchange_info(), "BTCUSDC")
        intent, reason = filters.normalize_intent(
            OrderIntent(
                symbol="BTCUSDC",
                side="BUY",
                quantity=0.062345,
                order_type="LIMIT",
                price=100.19,
                time_in_force="GTX",
            )
        )

        self.assertEqual(reason, "exchange filters ok")
        assert intent is not None
        self.assertEqual(intent.quantity, 0.062)
        self.assertEqual(intent.price, 100.1)

    def test_sell_post_only_price_rounds_up(self) -> None:
        filters = SymbolFilters.from_exchange_info(_exchange_info(), "BTCUSDC")
        intent, _ = filters.normalize_intent(
            OrderIntent(
                symbol="BTCUSDC",
                side="SELL",
                quantity=0.062345,
                order_type="LIMIT",
                price=100.11,
                time_in_force="GTX",
            )
        )

        assert intent is not None
        self.assertEqual(intent.quantity, 0.062)
        self.assertEqual(intent.price, 100.2)

    def test_too_small_notional_blocks_live_order(self) -> None:
        filters = SymbolFilters.from_exchange_info(_exchange_info(), "BTCUSDC")
        intent, reason = filters.normalize_intent(
            OrderIntent(
                symbol="BTCUSDC",
                side="BUY",
                quantity=0.001,
                order_type="LIMIT",
                price=100.0,
                time_in_force="GTX",
            )
        )

        self.assertIsNone(intent)
        self.assertIn("below minNotional", reason)

    def test_stop_market_preserves_stop_fields_and_rounds_trigger(self) -> None:
        filters = SymbolFilters.from_exchange_info(_exchange_info(), "BTCUSDC")
        intent, reason = filters.normalize_intent(
            OrderIntent(
                symbol="BTCUSDC",
                side="SELL",
                quantity=0.062345,
                order_type="STOP_MARKET",
                stop_price=99.91,
                working_type="MARK_PRICE",
                reduce_only=True,
            )
        )

        self.assertEqual(reason, "exchange filters ok")
        assert intent is not None
        self.assertEqual(intent.quantity, 0.062)
        self.assertEqual(intent.stop_price, 100.0)
        self.assertEqual(intent.working_type, "MARK_PRICE")
        self.assertTrue(intent.reduce_only)


if __name__ == "__main__":
    unittest.main()
