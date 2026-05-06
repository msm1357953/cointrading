import tempfile
import unittest
from dataclasses import replace
from pathlib import Path

from cointrading.bnb_fee_manager import (
    BnbFeeTopupState,
    _kst_date,
    ensure_bnb_fee_balance,
)
from cointrading.config import TradingConfig
from cointrading.storage import now_ms


class FakeBnbClient:
    def __init__(self) -> None:
        self.futures_bnb = 0.00003
        self.futures_usdc = 100.0
        self.spot_bnb = 0.0
        self.transfers: list[tuple[str, str, float]] = []
        self.orders: list[tuple[str, str, float]] = []

    def fee_burn_status(self):
        return {"feeBurn": True}

    def account_balance(self):
        return [
            {"asset": "BNB", "balance": str(self.futures_bnb), "availableBalance": str(self.futures_bnb)},
            {"asset": "USDC", "balance": str(self.futures_usdc), "availableBalance": str(self.futures_usdc)},
        ]

    def api_key_permissions(self):
        return {
            "enableSpotAndMarginTrading": True,
            "permitsUniversalTransfer": True,
        }

    def spot_book_ticker(self, symbol):
        return {"symbol": symbol, "bidPrice": "649.0", "askPrice": "650.0"}

    def universal_transfer(self, *, transfer_type, asset, amount):
        self.transfers.append((transfer_type, asset, amount))
        if transfer_type == "UMFUTURE_MAIN" and asset == "USDC":
            self.futures_usdc -= amount
        if transfer_type == "MAIN_UMFUTURE" and asset == "BNB":
            self.futures_bnb += amount
            self.spot_bnb -= amount
        return {"tranId": len(self.transfers)}

    def spot_market_order_quote(self, *, symbol, side, quote_order_qty, response_type="FULL"):
        self.orders.append((symbol, side, quote_order_qty))
        bought = quote_order_qty / 650.0
        self.spot_bnb += bought
        return {
            "symbol": symbol,
            "status": "FILLED",
            "executedQty": f"{bought:.10f}",
            "cummulativeQuoteQty": f"{quote_order_qty:.8f}",
        }

    def spot_account(self):
        return {"balances": [{"asset": "BNB", "free": str(self.spot_bnb)}]}


def _cfg(**overrides) -> TradingConfig:
    base = replace(
        TradingConfig(),
        dry_run=False,
        testnet=False,
        bnb_fee_topup_enabled=True,
        bnb_fee_topup_live_enabled=True,
        bnb_fee_topup_min_bnb=0.003,
        bnb_fee_topup_target_bnb=0.02,
        bnb_fee_topup_min_quote_usdc=5.0,
        bnb_fee_topup_max_quote_usdc=20.0,
        bnb_fee_topup_daily_quote_limit_usdc=40.0,
    )
    return replace(base, **overrides)


class BnbFeeManagerTests(unittest.TestCase):
    def test_disabled_does_nothing(self) -> None:
        client = FakeBnbClient()
        result = ensure_bnb_fee_balance(
            client=client,
            config=_cfg(bnb_fee_topup_enabled=False),
        )
        self.assertTrue(result.ok)
        self.assertEqual(result.action, "disabled")
        self.assertEqual(client.transfers, [])
        self.assertEqual(client.orders, [])

    def test_sufficient_does_nothing(self) -> None:
        client = FakeBnbClient()
        client.futures_bnb = 0.004
        result = ensure_bnb_fee_balance(client=client, config=_cfg())
        self.assertTrue(result.ok)
        self.assertEqual(result.action, "sufficient")
        self.assertEqual(client.transfers, [])

    def test_topup_buys_on_spot_and_moves_bnb_to_futures(self) -> None:
        client = FakeBnbClient()
        with tempfile.TemporaryDirectory() as td:
            state_path = Path(td) / "bnb.json"
            result = ensure_bnb_fee_balance(
                client=client,
                config=_cfg(),
                state_path=state_path,
            )
            self.assertTrue(result.ok)
            self.assertEqual(result.action, "topped_up")
            self.assertEqual(client.transfers[0][0], "UMFUTURE_MAIN")
            self.assertEqual(client.orders[0][0], "BNBUSDC")
            self.assertEqual(client.transfers[-1][0], "MAIN_UMFUTURE")
            self.assertGreater(client.futures_bnb, 0.003)
            state = BnbFeeTopupState.load(state_path)
            self.assertGreater(state.daily_quote_spent_usdc, 0)

    def test_daily_cap_blocks(self) -> None:
        client = FakeBnbClient()
        with tempfile.TemporaryDirectory() as td:
            state_path = Path(td) / "bnb.json"
            state = BnbFeeTopupState(
                daily_kst_date=_kst_date(now_ms()),
                daily_quote_spent_usdc=39.0,
            )
            state.save(state_path)
            result = ensure_bnb_fee_balance(
                client=client,
                config=_cfg(bnb_fee_topup_daily_quote_limit_usdc=40.0),
                quote_amount_usdc=5.0,
                force=True,
                state_path=state_path,
            )
            self.assertFalse(result.ok)
            self.assertEqual(result.action, "blocked")
            self.assertIn("한도", result.message)


if __name__ == "__main__":
    unittest.main()
