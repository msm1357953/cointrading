from __future__ import annotations

from cointrading.config import TradingConfig
from cointrading.models import Kline
from cointrading.research_probe import (
    ProbeNotifyState,
    ProbeStrategy,
    ProbeTrade,
    backtest_probe_strategy,
    probe_notification_decision,
    summarize_probe_result,
    vibe_probe_text,
)


def test_probe_backtest_uses_pessimistic_stop_when_target_and_stop_same_bar() -> None:
    strategy = ProbeStrategy(
        name="test",
        label="테스트",
        take_profit_bps=100.0,
        stop_loss_bps=100.0,
        max_hold_bars=10,
        signal_fn=lambda _rows, index: "long" if index == 0 else "flat",
    )
    klines = [
        _kline(0, 100.0, 100.0, 100.0, 100.0),
        _kline(1, 100.0, 100.0, 100.0, 100.0),
        _kline(2, 100.0, 102.0, 98.0, 100.0),
    ]

    trades = backtest_probe_strategy(
        symbol="BTCUSDC",
        interval="15m",
        klines=klines,
        strategy=strategy,
        config=TradingConfig(taker_fee_rate=0.0, slippage_bps=0.0),
        notional=100.0,
    )

    assert len(trades) == 1
    assert trades[0].reason == "stop_loss"
    assert trades[0].exit_price == 99.0
    assert trades[0].pnl < 0


def test_probe_summary_approves_positive_stable_profile() -> None:
    strategy = ProbeStrategy(
        name="trend_follow",
        label="추세추종",
        take_profit_bps=90.0,
        stop_loss_bps=30.0,
        max_hold_bars=96,
        signal_fn=lambda _rows, _index: "flat",
    )
    trades = [
        ProbeTrade(
            strategy=strategy.name,
            symbol="ETHUSDC",
            side="long",
            entry_time_ms=index,
            exit_time_ms=index + 1,
            entry_price=100.0,
            exit_price=100.1,
            notional=100.0,
            pnl=0.05,
            pnl_bps=5.0,
            reason="take_profit",
            hold_bars=1,
        )
        for index in range(20)
    ]

    result = summarize_probe_result(
        symbol="ETHUSDC",
        interval="15m",
        sample_bars=1000,
        strategy=strategy,
        trades=trades,
        initial_equity=1000.0,
    )

    assert result.decision == "APPROVED"
    assert result.avg_pnl_bps == 5.0
    assert "승인" in vibe_probe_text([result])


def test_probe_notification_sends_periodic_even_without_approved() -> None:
    strategy = ProbeStrategy(
        name="trend_follow",
        label="추세추종",
        take_profit_bps=90.0,
        stop_loss_bps=30.0,
        max_hold_bars=96,
        signal_fn=lambda _rows, _index: "flat",
    )
    result = summarize_probe_result(
        symbol="BTCUSDC",
        interval="15m",
        sample_bars=100,
        strategy=strategy,
        trades=[],
        initial_equity=1000.0,
    )

    should_send, reason, signature = probe_notification_decision(
        [result],
        ProbeNotifyState(last_sent_ms=0),
        periodic_minutes=360,
        current_ms=360 * 60_000,
    )

    assert should_send
    assert reason == "주기 요약"
    assert signature == "NO_APPROVED"


def _kline(index: int, open_: float, high: float, low: float, close: float) -> Kline:
    open_time = index * 60_000
    return Kline(
        open_time=open_time,
        open=open_,
        high=high,
        low=low,
        close=close,
        volume=1.0,
        close_time=open_time + 59_999,
    )
