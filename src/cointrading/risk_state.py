from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, time as datetime_time, timedelta, timezone
import sqlite3

from cointrading.config import TradingConfig
from cointrading.market_regime import MACRO_BREAKOUT, MACRO_PANIC, macro_regime_ko
from cointrading.storage import TradingStore, kst_from_ms, now_ms


RISK_NORMAL = "NORMAL"
RISK_CAUTION = "CAUTION"
RISK_DEFENSIVE = "DEFENSIVE"
RISK_HALT = "HALT"

_LEVELS = {
    RISK_NORMAL: 0,
    RISK_CAUTION: 1,
    RISK_DEFENSIVE: 2,
    RISK_HALT: 3,
}


@dataclass(frozen=True)
class RuntimeRiskSnapshot:
    mode: str
    allows_new_entries: bool
    reasons: tuple[str, ...]
    generated_ms: int
    lookback_minutes: int
    recent_cycle_count: int
    recent_closed_count: int
    recent_stop_loss_count: int
    recent_requote_count: int
    recent_stop_loss_ratio: float
    recent_requote_ratio: float
    kst_day_pnl: float
    btc_macro_regime: str
    btc_realized_vol_bps: float
    btc_atr_bps: float

    def to_text(self) -> str:
        reasons = "\n".join(f"- {reason}" for reason in self.reasons) or "- 특이사항 없음"
        return "\n".join(
            [
                "런타임 위험모드",
                f"시각: {kst_from_ms(self.generated_ms)}",
                f"모드: {risk_mode_ko(self.mode)}",
                f"신규 진입: {'허용' if self.allows_new_entries else '차단'}",
                f"최근 {self.lookback_minutes}분 종료: {self.recent_closed_count}건",
                f"손절 비율: {self.recent_stop_loss_ratio:.1%} ({self.recent_stop_loss_count}건)",
                f"재호가 비율: {self.recent_requote_ratio:.1%} ({self.recent_requote_count}건)",
                f"KST 당일 실현손익: {self.kst_day_pnl:.6f}",
                "BTC 상태: "
                f"{macro_regime_ko(self.btc_macro_regime) if self.btc_macro_regime else '없음'} "
                f"vol={self.btc_realized_vol_bps:.1f}bps "
                f"ATR={self.btc_atr_bps:.1f}bps",
                "판단 근거:",
                reasons,
            ]
        )


def evaluate_runtime_risk(
    store: TradingStore,
    config: TradingConfig,
    *,
    symbol: str | None = None,
    current_ms: int | None = None,
) -> RuntimeRiskSnapshot:
    ts = current_ms or now_ms()
    lookback_minutes = max(1, int(config.runtime_risk_lookback_minutes))
    since_ms = ts - lookback_minutes * 60_000
    cycle_stats = _recent_cycle_stats(store, since_ms)
    day_pnl = _kst_day_realized_pnl(store, ts)
    btc_row = store.latest_market_regime("BTCUSDC")
    btc_stats = _btc_stats(btc_row)

    mode = RISK_NORMAL
    reasons: list[str] = []

    if not config.runtime_risk_enabled:
        reasons.append("런타임 위험모드가 설정으로 꺼져 있습니다.")
        return RuntimeRiskSnapshot(
            mode=RISK_NORMAL,
            allows_new_entries=True,
            reasons=tuple(reasons),
            generated_ms=ts,
            lookback_minutes=lookback_minutes,
            recent_cycle_count=cycle_stats["cycle_count"],
            recent_closed_count=cycle_stats["closed_count"],
            recent_stop_loss_count=cycle_stats["stop_loss_count"],
            recent_requote_count=cycle_stats["requote_count"],
            recent_stop_loss_ratio=cycle_stats["stop_loss_ratio"],
            recent_requote_ratio=cycle_stats["requote_ratio"],
            kst_day_pnl=day_pnl,
            btc_macro_regime=btc_stats["macro_regime"],
            btc_realized_vol_bps=btc_stats["realized_vol_bps"],
            btc_atr_bps=btc_stats["atr_bps"],
        )

    daily_loss_limit = max(0.0, config.initial_equity * config.runtime_risk_daily_loss_pct)
    if daily_loss_limit > 0 and day_pnl <= -daily_loss_limit:
        mode = _raise_mode(mode, RISK_HALT)
        reasons.append(
            f"KST 당일 손실 {day_pnl:.6f}이 한도 -{daily_loss_limit:.6f} 이하입니다."
        )

    closed_count = cycle_stats["closed_count"]
    if closed_count >= config.runtime_risk_min_events:
        stop_loss_ratio = cycle_stats["stop_loss_ratio"]
        if stop_loss_ratio >= config.runtime_risk_stop_loss_ratio_halt:
            mode = _raise_mode(mode, RISK_HALT)
            reasons.append(f"최근 손절 비율 {stop_loss_ratio:.1%}로 HALT 기준 초과.")
        elif stop_loss_ratio >= config.runtime_risk_stop_loss_ratio_defensive:
            mode = _raise_mode(mode, RISK_DEFENSIVE)
            reasons.append(f"최근 손절 비율 {stop_loss_ratio:.1%}로 방어모드.")
        elif stop_loss_ratio >= config.runtime_risk_stop_loss_ratio_caution:
            mode = _raise_mode(mode, RISK_CAUTION)
            reasons.append(f"최근 손절 비율 {stop_loss_ratio:.1%}로 주의모드.")

    cycle_count = cycle_stats["cycle_count"]
    if cycle_count >= config.runtime_risk_min_events:
        requote_ratio = cycle_stats["requote_ratio"]
        if requote_ratio >= config.runtime_risk_requote_ratio_halt:
            mode = _raise_mode(mode, RISK_HALT)
            reasons.append(f"최근 재호가 비율 {requote_ratio:.1%}로 HALT 기준 초과.")
        elif requote_ratio >= config.runtime_risk_requote_ratio_defensive:
            mode = _raise_mode(mode, RISK_DEFENSIVE)
            reasons.append(f"최근 재호가 비율 {requote_ratio:.1%}로 post-only 품질 악화.")
        elif requote_ratio >= config.runtime_risk_requote_ratio_caution:
            mode = _raise_mode(mode, RISK_CAUTION)
            reasons.append(f"최근 재호가 비율 {requote_ratio:.1%}로 주의모드.")

    btc_reason = _btc_stress_reason(btc_stats, config, symbol)
    if btc_reason:
        mode = _raise_mode(mode, RISK_DEFENSIVE)
        reasons.append(btc_reason)

    if not reasons:
        reasons.append("런타임 위험 기준상 신규 차단 조건은 없습니다.")

    allows_new_entries = mode == RISK_NORMAL or (mode == RISK_CAUTION and config.dry_run)
    if mode == RISK_CAUTION and not config.dry_run:
        reasons.append("실전 모드에서는 CAUTION도 신규 진입을 차단합니다.")

    return RuntimeRiskSnapshot(
        mode=mode,
        allows_new_entries=allows_new_entries,
        reasons=tuple(reasons),
        generated_ms=ts,
        lookback_minutes=lookback_minutes,
        recent_cycle_count=cycle_count,
        recent_closed_count=closed_count,
        recent_stop_loss_count=cycle_stats["stop_loss_count"],
        recent_requote_count=cycle_stats["requote_count"],
        recent_stop_loss_ratio=cycle_stats["stop_loss_ratio"],
        recent_requote_ratio=cycle_stats["requote_ratio"],
        kst_day_pnl=day_pnl,
        btc_macro_regime=btc_stats["macro_regime"],
        btc_realized_vol_bps=btc_stats["realized_vol_bps"],
        btc_atr_bps=btc_stats["atr_bps"],
    )


def risk_mode_ko(mode: str) -> str:
    return {
        RISK_NORMAL: "정상",
        RISK_CAUTION: "주의",
        RISK_DEFENSIVE: "방어",
        RISK_HALT: "중지",
    }.get(mode, mode)


def _recent_cycle_stats(store: TradingStore, since_ms: int) -> dict[str, float | int]:
    with store.connect() as connection:
        cycle_row = connection.execute(
            """
            SELECT
                COUNT(*) AS cycle_count,
                SUM(CASE WHEN status='REQUOTE' THEN 1 ELSE 0 END) AS requote_count
            FROM scalp_cycles
            WHERE created_ms >= ? OR updated_ms >= ?
            """,
            (since_ms, since_ms),
        ).fetchone()
        closed_row = connection.execute(
            """
            SELECT
                COUNT(*) AS closed_count,
                SUM(CASE WHEN reason='stop_loss' THEN 1 ELSE 0 END) AS stop_loss_count
            FROM scalp_cycles
            WHERE updated_ms >= ?
              AND status IN ('CLOSED', 'STOPPED')
              AND realized_pnl IS NOT NULL
            """,
            (since_ms,),
        ).fetchone()

    cycle_count = int((cycle_row or {})["cycle_count"] or 0)
    closed_count = int((closed_row or {})["closed_count"] or 0)
    requote_count = int((cycle_row or {})["requote_count"] or 0)
    stop_loss_count = int((closed_row or {})["stop_loss_count"] or 0)
    return {
        "cycle_count": cycle_count,
        "closed_count": closed_count,
        "requote_count": requote_count,
        "stop_loss_count": stop_loss_count,
        "requote_ratio": _ratio(requote_count, cycle_count),
        "stop_loss_ratio": _ratio(stop_loss_count, closed_count),
    }


def _kst_day_realized_pnl(store: TradingStore, current_ms: int) -> float:
    since_ms = _kst_day_start_ms(current_ms)
    with store.connect() as connection:
        row = connection.execute(
            """
            SELECT SUM(COALESCE(realized_pnl, 0)) AS pnl
            FROM scalp_cycles
            WHERE closed_ms >= ?
              AND realized_pnl IS NOT NULL
            """,
            (since_ms,),
        ).fetchone()
    return float(row["pnl"] or 0.0)


def _kst_day_start_ms(current_ms: int) -> int:
    kst = timezone(timedelta(hours=9), name="KST")
    current = datetime.fromtimestamp(current_ms / 1000, kst)
    start = datetime.combine(current.date(), datetime_time.min, tzinfo=kst)
    return int(start.timestamp() * 1000)


def _btc_stats(row: sqlite3.Row | None) -> dict[str, float | str]:
    if row is None:
        return {
            "macro_regime": "",
            "realized_vol_bps": 0.0,
            "atr_bps": 0.0,
        }
    return {
        "macro_regime": str(row["macro_regime"]),
        "realized_vol_bps": float(row["realized_vol_bps"]),
        "atr_bps": float(row["atr_bps"]),
    }


def _btc_stress_reason(
    stats: dict[str, float | str],
    config: TradingConfig,
    symbol: str | None,
) -> str:
    regime = str(stats["macro_regime"])
    vol = float(stats["realized_vol_bps"])
    atr = float(stats["atr_bps"])
    if not regime:
        return ""
    target = (symbol or "").upper()
    prefix = "BTC 급변동" if target and target != "BTCUSDC" else "BTC 상태"
    if regime == MACRO_PANIC:
        return f"{prefix}: {macro_regime_ko(regime)}라 신규 진입 방어모드."
    if regime == MACRO_BREAKOUT:
        return f"{prefix}: {macro_regime_ko(regime)}라 스캘핑 신규 진입 방어모드."
    if vol >= config.runtime_risk_btc_vol_defensive_bps:
        return f"{prefix}: 실현 변동성 {vol:.1f}bps가 기준을 초과."
    if atr >= config.runtime_risk_btc_atr_defensive_bps:
        return f"{prefix}: ATR {atr:.1f}bps가 기준을 초과."
    return ""


def _raise_mode(current: str, candidate: str) -> str:
    if _LEVELS[candidate] > _LEVELS[current]:
        return candidate
    return current


def _ratio(numerator: int, denominator: int) -> float:
    if denominator <= 0:
        return 0.0
    return numerator / denominator
