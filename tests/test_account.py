import unittest

from cointrading.account import account_summary_text


class AccountSummaryTest(unittest.TestCase):
    def test_uses_top_level_totals_when_available(self) -> None:
        text = account_summary_text(
            {
                "totalWalletBalance": "1181.55089676",
                "availableBalance": "1180.00000000",
                "totalUnrealizedProfit": "1.25000000",
                "totalMaintMargin": "0.50000000",
                "positions": [{"symbol": "ETHUSDC", "positionAmt": "0.10"}],
            }
        )

        self.assertIn("지갑: 1181.5509 USD-M", text)
        self.assertIn("열린 포지션: 1", text)

    def test_falls_back_to_usdc_asset_when_top_level_total_is_zero(self) -> None:
        text = account_summary_text(
            {
                "totalWalletBalance": "0.00000000",
                "availableBalance": "0.00000000",
                "totalUnrealizedProfit": "0.00000000",
                "totalMaintMargin": "0.00000000",
                "assets": [
                    {
                        "asset": "BNB",
                        "walletBalance": "0.29052301",
                        "availableBalance": "0.29052301",
                    },
                    {
                        "asset": "USDC",
                        "walletBalance": "1000.00000000",
                        "availableBalance": "1000.00000000",
                    },
                ],
                "positions": [],
            }
        )

        self.assertIn("지갑: 1000.0000 USDC", text)
        self.assertIn("사용 가능: 1000.0000 USDC", text)
        self.assertIn("BNB 수수료 지갑: 0.29052301 BNB", text)
        self.assertIn("열린 포지션: 0", text)


if __name__ == "__main__":
    unittest.main()
