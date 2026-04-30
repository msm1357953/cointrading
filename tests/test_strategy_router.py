import unittest

from cointrading.market_regime import MACRO_BULL, MACRO_PANIC, MarketRegimeSnapshot
from cointrading.risk_state import RISK_NORMAL, RuntimeRiskSnapshot
from cointrading.scalping import ScalpSignal
from cointrading.strategy_router import (
    SETUP_BLOCK,
    SETUP_PASS,
    SETUP_WATCH,
    evaluate_strategy_setups,
    strategy_setups_text,
)


def _risk() -> RuntimeRiskSnapshot:
    return RuntimeRiskSnapshot(
        mode=RISK_NORMAL,
        allows_new_entries=True,
        reasons=("ok",),
        generated_ms=1_000,
        lookback_minutes=30,
        recent_cycle_count=0,
        recent_closed_count=0,
        recent_stop_loss_count=0,
        recent_requote_count=0,
        recent_stop_loss_ratio=0.0,
        recent_requote_ratio=0.0,
        kst_day_pnl=0.0,
        btc_macro_regime="",
        btc_realized_vol_bps=0.0,
        btc_atr_bps=0.0,
    )


def _macro(regime: str, bias: str) -> MarketRegimeSnapshot:
    return MarketRegimeSnapshot(
        symbol="ETHUSDC",
        macro_regime=regime,
        trade_bias=bias,
        allowed_strategies=("trend_long_15m_1h",),
        blocked_reason="",
        last_price=2_250.0,
        trend_1h_bps=20.0,
        trend_4h_bps=40.0,
        realized_vol_bps=20.0,
        atr_bps=30.0,
        timestamp_ms=1_000,
    )


def _signal(side: str = "flat", regime: str = "thin_book") -> ScalpSignal:
    return ScalpSignal(
        symbol="ETHUSDC",
        side=side,
        reason="book depth too thin" if regime == "thin_book" else "ok",
        regime=regime,
        trade_allowed=side in {"long", "short"},
        mid_price=2_250.0,
        spread_bps=0.5,
        imbalance=0.2,
        momentum_bps=3.0,
        realized_vol_bps=5.0,
        maker_roundtrip_bps=0.0,
        taker_roundtrip_bps=8.0,
        edge_after_maker_bps=3.0,
        book_bid_notional=1_000.0,
        book_ask_notional=1_000.0,
        book_depth_notional=2_000.0,
        bnb_fee_discount_enabled=True,
        bnb_fee_discount_active=True,
    )


class StrategyRouterTests(unittest.TestCase):
    def test_thin_book_blocks_only_maker_scalp_not_macro_trend_watch(self) -> None:
        setups = evaluate_strategy_setups(
            scalp_signal=_signal(),
            macro_row=_macro(MACRO_BULL, "long"),
            runtime_risk=_risk(),
            macro_max_age_ms=60_000,
            current_ms=2_000,
        )

        by_name = {setup.strategy: setup for setup in setups}
        self.assertEqual(by_name["maker_scalp"].status, SETUP_BLOCK)
        self.assertEqual(by_name["trend_follow"].status, SETUP_WATCH)
        self.assertIn("스캘핑만 차단", by_name["maker_scalp"].reason)
        self.assertIn("thin book", by_name["trend_follow"].reason)

    def test_aligned_scalp_can_be_live_supported_pass(self) -> None:
        setups = evaluate_strategy_setups(
            scalp_signal=_signal(side="long", regime="aligned_long"),
            macro_row=_macro(MACRO_BULL, "long"),
            runtime_risk=_risk(),
            macro_max_age_ms=60_000,
            current_ms=2_000,
        )

        scalp = {setup.strategy: setup for setup in setups}["maker_scalp"]
        self.assertEqual(scalp.status, SETUP_PASS)
        self.assertTrue(scalp.live_supported)

    def test_panic_blocks_macro_setups_text(self) -> None:
        setups = evaluate_strategy_setups(
            scalp_signal=_signal(side="long", regime="aligned_long"),
            macro_row=_macro(MACRO_PANIC, "flat"),
            runtime_risk=_risk(),
            macro_max_age_ms=60_000,
            current_ms=2_000,
        )

        text = strategy_setups_text(setups, symbol="ETHUSDC", notional=25, runtime_risk=_risk())

        self.assertIn("실전 엔진 결론: 지금 자동 주문 후보 없음", text)
        self.assertIn("패닉", text)


if __name__ == "__main__":
    unittest.main()
