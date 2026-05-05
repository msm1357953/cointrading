"""Local research data lake for hypothesis testing.

Stores Binance public data as Parquet files under ``data/lake/`` so analysis
scripts don't have to re-download anything. Anything in this module is for
research only and never touches live trading code.

Layout::

    data/lake/
        klines/SYMBOL_INTERVAL.parquet      # OHLCV + open_time/close_time
        funding/SYMBOL.parquet              # fundingTime, fundingRate
        oi/SYMBOL_PERIOD.parquet            # 5m or 1h OI history

Production lifecycle modules MUST NOT depend on pandas/pyarrow. This module
is gated behind the ``research`` extra dependency.
"""
from __future__ import annotations

import json
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

import pandas as pd

from cointrading.historical_data import (
    HistoricalDataError,
    default_history_dir,
    load_binance_vision_klines,
    parse_yyyy_mm_dd,
)
from cointrading.research.funding_carry import (
    DEFAULT_SYMBOLS,
    fetch_funding_rate_history,
)


DEFAULT_INTERVALS = ("1m", "5m", "15m", "1h", "4h")
OI_API_URL = "https://fapi.binance.com/futures/data/openInterestHist"
OI_PERIOD = "5m"  # binance allows 5m/15m/30m/1h/2h/4h/6h/12h/1d
LS_TOP_POS_URL = "https://fapi.binance.com/futures/data/topLongShortPositionRatio"
LS_TOP_ACC_URL = "https://fapi.binance.com/futures/data/topLongShortAccountRatio"
LS_GLOBAL_URL = "https://fapi.binance.com/futures/data/globalLongShortAccountRatio"
TAKER_URL = "https://fapi.binance.com/futures/data/takerlongshortRatio"


def lake_root() -> Path:
    return Path(__file__).resolve().parents[3] / "data" / "lake"


def _ensure_dir(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


# ---------- Klines ----------


def kline_path(symbol: str, interval: str) -> Path:
    return lake_root() / "klines" / f"{symbol.upper()}_{interval}.parquet"


def build_klines_parquet(
    *,
    symbol: str,
    interval: str,
    start: date,
    end: date,
    force: bool = False,
) -> Path:
    out_path = kline_path(symbol, interval)
    if out_path.exists() and not force:
        return out_path
    _ensure_dir(out_path)
    res = load_binance_vision_klines(
        symbol=symbol, interval=interval, start_date=start, end_date=end
    )
    if not res.klines:
        raise HistoricalDataError(f"no klines for {symbol} {interval}")
    df = pd.DataFrame(
        [
            {
                "open_time": k.open_time,
                "close_time": k.close_time,
                "open": k.open,
                "high": k.high,
                "low": k.low,
                "close": k.close,
                "volume": k.volume,
            }
            for k in res.klines
        ]
    )
    df["open_time_dt"] = pd.to_datetime(df["open_time"], unit="ms", utc=True)
    df = df.sort_values("open_time").drop_duplicates("open_time").reset_index(drop=True)
    df.to_parquet(out_path, index=False)
    return out_path


def load_klines(
    symbol: str,
    interval: str,
    *,
    start: date | str | None = None,
    end: date | str | None = None,
) -> pd.DataFrame:
    p = kline_path(symbol, interval)
    if not p.exists():
        raise FileNotFoundError(f"missing lake file: {p}. Build via populate_lake().")
    df = pd.read_parquet(p)
    if start is not None:
        s = parse_yyyy_mm_dd(start) if isinstance(start, str) else start
        s_ms = int(datetime(s.year, s.month, s.day, tzinfo=timezone.utc).timestamp() * 1000)
        df = df[df["open_time"] >= s_ms]
    if end is not None:
        e = parse_yyyy_mm_dd(end) if isinstance(end, str) else end
        e_ms = int(datetime(e.year, e.month, e.day, 23, 59, 59, tzinfo=timezone.utc).timestamp() * 1000)
        df = df[df["open_time"] <= e_ms]
    return df.reset_index(drop=True)


# ---------- Funding rate ----------


def funding_lake_path(symbol: str) -> Path:
    return lake_root() / "funding" / f"{symbol.upper()}.parquet"


def build_funding_parquet(
    *, symbol: str, start: date, end: date, force: bool = False
) -> Path:
    out_path = funding_lake_path(symbol)
    if out_path.exists() and not force:
        return out_path
    _ensure_dir(out_path)
    obs = fetch_funding_rate_history(
        symbol=symbol, start=start, end=end, history_dir=default_history_dir()
    )
    if not obs:
        raise HistoricalDataError(f"no funding history for {symbol}")
    df = pd.DataFrame(
        [
            {"funding_time": o.funding_time_ms, "funding_rate": o.funding_rate}
            for o in obs
        ]
    )
    df["funding_time_dt"] = pd.to_datetime(df["funding_time"], unit="ms", utc=True)
    df = df.sort_values("funding_time").drop_duplicates("funding_time").reset_index(drop=True)
    df.to_parquet(out_path, index=False)
    return out_path


def load_funding(
    symbol: str,
    *,
    start: date | str | None = None,
    end: date | str | None = None,
) -> pd.DataFrame:
    p = funding_lake_path(symbol)
    if not p.exists():
        raise FileNotFoundError(f"missing lake file: {p}")
    df = pd.read_parquet(p)
    if start is not None:
        s = parse_yyyy_mm_dd(start) if isinstance(start, str) else start
        s_ms = int(datetime(s.year, s.month, s.day, tzinfo=timezone.utc).timestamp() * 1000)
        df = df[df["funding_time"] >= s_ms]
    if end is not None:
        e = parse_yyyy_mm_dd(end) if isinstance(end, str) else end
        e_ms = int(datetime(e.year, e.month, e.day, 23, 59, 59, tzinfo=timezone.utc).timestamp() * 1000)
        df = df[df["funding_time"] <= e_ms]
    return df.reset_index(drop=True)


# ---------- Open interest ----------


def oi_lake_path(symbol: str, period: str = OI_PERIOD) -> Path:
    return lake_root() / "oi" / f"{symbol.upper()}_{period}.parquet"


def _fetch_oi_window(
    symbol: str, period: str, start_ms: int, end_ms: int, timeout: float = 30.0
) -> list[dict]:
    url = (
        f"{OI_API_URL}?symbol={symbol}&period={period}"
        f"&startTime={start_ms}&endTime={end_ms}&limit=500"
    )
    request = Request(url, headers={"User-Agent": "cointrading-research/0.1"})
    try:
        with urlopen(request, timeout=timeout) as response:
            payload = json.loads(response.read())
    except (HTTPError, URLError) as exc:
        raise HistoricalDataError(f"OI fetch failed: {url}") from exc
    return list(payload) if isinstance(payload, list) else []


def build_oi_parquet(
    *,
    symbol: str,
    start: date,
    end: date,
    period: str = OI_PERIOD,
    force: bool = False,
) -> Path:
    """Note: Binance OI history goes back ~30 days only on /futures/data/openInterestHist.
    For deeper history we'd need a paid provider. We fetch what's available.
    """
    out_path = oi_lake_path(symbol, period)
    if out_path.exists() and not force:
        return out_path
    _ensure_dir(out_path)

    # Binance API limits to 30 days for OI history. Walk backward from end.
    start_ms = int(datetime(start.year, start.month, start.day, tzinfo=timezone.utc).timestamp() * 1000)
    end_ms = int(datetime(end.year, end.month, end.day, 23, 59, 59, tzinfo=timezone.utc).timestamp() * 1000)
    window_ms = 30 * 24 * 3600 * 1000  # 30 days per call window

    all_records: dict[int, dict] = {}
    cursor = end_ms
    while cursor > start_ms:
        window_start = max(start_ms, cursor - window_ms)
        records = _fetch_oi_window(symbol, period, window_start, cursor)
        if not records:
            break
        for rec in records:
            t = int(rec["timestamp"])
            all_records[t] = {
                "timestamp": t,
                "sum_open_interest": float(rec["sumOpenInterest"]),
                "sum_open_interest_value": float(rec["sumOpenInterestValue"]),
            }
        oldest = min(int(r["timestamp"]) for r in records)
        if oldest >= cursor:
            break
        cursor = oldest - 1
        time.sleep(0.1)  # be polite to API
        if oldest <= start_ms:
            break

    if not all_records:
        raise HistoricalDataError(f"no OI history for {symbol} (Binance only keeps ~30 days)")

    df = pd.DataFrame(sorted(all_records.values(), key=lambda r: r["timestamp"]))
    df["timestamp_dt"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
    df.to_parquet(out_path, index=False)
    return out_path


def load_oi(
    symbol: str,
    *,
    period: str = OI_PERIOD,
    start: date | str | None = None,
    end: date | str | None = None,
) -> pd.DataFrame:
    p = oi_lake_path(symbol, period)
    if not p.exists():
        raise FileNotFoundError(f"missing lake file: {p}")
    df = pd.read_parquet(p)
    if start is not None:
        s = parse_yyyy_mm_dd(start) if isinstance(start, str) else start
        s_ms = int(datetime(s.year, s.month, s.day, tzinfo=timezone.utc).timestamp() * 1000)
        df = df[df["timestamp"] >= s_ms]
    if end is not None:
        e = parse_yyyy_mm_dd(end) if isinstance(end, str) else end
        e_ms = int(datetime(e.year, e.month, e.day, 23, 59, 59, tzinfo=timezone.utc).timestamp() * 1000)
        df = df[df["timestamp"] <= e_ms]
    return df.reset_index(drop=True)


# ---------- /futures/data ratio endpoints (LS, taker) — Binance limits: ~30 days ----------


def _fetch_ratio_window(url: str, symbol: str, period: str, start_ms: int, end_ms: int,
                       timeout: float = 30.0) -> list[dict]:
    full_url = (
        f"{url}?symbol={symbol}&period={period}&startTime={start_ms}&endTime={end_ms}&limit=500"
    )
    request = Request(full_url, headers={"User-Agent": "cointrading-research/0.1"})
    try:
        with urlopen(request, timeout=timeout) as response:
            payload = json.loads(response.read())
    except HTTPError as exc:
        body = ""
        try:
            body = exc.read().decode("utf-8", errors="replace")[:200]
        except Exception:
            pass
        raise HistoricalDataError(
            f"ratio fetch failed (HTTP {exc.code}): {full_url} body={body!r}"
        ) from exc
    except URLError as exc:
        raise HistoricalDataError(f"ratio fetch failed (network): {full_url}") from exc
    return list(payload) if isinstance(payload, list) else []


def _build_ratio_parquet(
    *,
    symbol: str,
    period: str,
    api_url: str,
    out_path: Path,
    start: date,
    end: date,
    record_to_row,  # callable(record) -> dict
    force: bool = False,
) -> Path:
    if out_path.exists() and not force:
        return out_path
    _ensure_dir(out_path)
    start_ms = int(datetime(start.year, start.month, start.day, tzinfo=timezone.utc).timestamp() * 1000)
    end_ms = int(datetime(end.year, end.month, end.day, 23, 59, 59, tzinfo=timezone.utc).timestamp() * 1000)
    window_ms = 7 * 24 * 3600 * 1000  # 7-day windows are safer
    all_rows: dict[int, dict] = {}
    cursor = end_ms
    while cursor > start_ms:
        win_start = max(start_ms, cursor - window_ms)
        records = _fetch_ratio_window(api_url, symbol, period, win_start, cursor)
        if not records:
            break
        for rec in records:
            t = int(rec["timestamp"])
            if t in all_rows:
                continue
            all_rows[t] = record_to_row(rec)
        oldest = min(int(r["timestamp"]) for r in records)
        if oldest >= cursor:
            break
        cursor = oldest - 1
        time.sleep(0.1)
        if cursor <= start_ms:
            break
    if not all_rows:
        raise HistoricalDataError(
            f"no ratio data for {symbol} from {api_url} (Binance may limit to ~30 days)"
        )
    df = pd.DataFrame(sorted(all_rows.values(), key=lambda r: r["timestamp"]))
    df["timestamp_dt"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
    df.to_parquet(out_path, index=False)
    return out_path


def ls_top_position_path(symbol: str, period: str = "5m") -> Path:
    return lake_root() / "ls_top_position" / f"{symbol.upper()}_{period}.parquet"


def ls_global_account_path(symbol: str, period: str = "5m") -> Path:
    return lake_root() / "ls_global_account" / f"{symbol.upper()}_{period}.parquet"


def taker_volume_path(symbol: str, period: str = "5m") -> Path:
    return lake_root() / "taker_volume" / f"{symbol.upper()}_{period}.parquet"


def build_ls_top_position_parquet(
    *, symbol: str, period: str = "5m", start: date, end: date, force: bool = False
) -> Path:
    return _build_ratio_parquet(
        symbol=symbol, period=period, api_url=LS_TOP_POS_URL,
        out_path=ls_top_position_path(symbol, period),
        start=start, end=end, force=force,
        record_to_row=lambda r: {
            "timestamp": int(r["timestamp"]),
            "ls_top_pos_ratio": float(r["longShortRatio"]),
            "long_pos_pct": float(r["longAccount"]),
            "short_pos_pct": float(r["shortAccount"]),
        },
    )


def build_ls_global_account_parquet(
    *, symbol: str, period: str = "5m", start: date, end: date, force: bool = False
) -> Path:
    return _build_ratio_parquet(
        symbol=symbol, period=period, api_url=LS_GLOBAL_URL,
        out_path=ls_global_account_path(symbol, period),
        start=start, end=end, force=force,
        record_to_row=lambda r: {
            "timestamp": int(r["timestamp"]),
            "ls_global_acc_ratio": float(r["longShortRatio"]),
            "long_acc_pct": float(r["longAccount"]),
            "short_acc_pct": float(r["shortAccount"]),
        },
    )


def build_taker_volume_parquet(
    *, symbol: str, period: str = "5m", start: date, end: date, force: bool = False
) -> Path:
    return _build_ratio_parquet(
        symbol=symbol, period=period, api_url=TAKER_URL,
        out_path=taker_volume_path(symbol, period),
        start=start, end=end, force=force,
        record_to_row=lambda r: {
            "timestamp": int(r["timestamp"]),
            "taker_buy_sell_ratio": float(r["buySellRatio"]),
            "taker_buy_vol": float(r["buyVol"]),
            "taker_sell_vol": float(r["sellVol"]),
        },
    )


def _load_ratio(path: Path, *, start, end) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"missing lake file: {path}")
    df = pd.read_parquet(path)
    if start is not None:
        s = parse_yyyy_mm_dd(start) if isinstance(start, str) else start
        s_ms = int(datetime(s.year, s.month, s.day, tzinfo=timezone.utc).timestamp() * 1000)
        df = df[df["timestamp"] >= s_ms]
    if end is not None:
        e = parse_yyyy_mm_dd(end) if isinstance(end, str) else end
        e_ms = int(datetime(e.year, e.month, e.day, 23, 59, 59, tzinfo=timezone.utc).timestamp() * 1000)
        df = df[df["timestamp"] <= e_ms]
    return df.reset_index(drop=True)


def load_ls_top_position(symbol: str, *, period="5m", start=None, end=None) -> pd.DataFrame:
    return _load_ratio(ls_top_position_path(symbol, period), start=start, end=end)


def load_ls_global_account(symbol: str, *, period="5m", start=None, end=None) -> pd.DataFrame:
    return _load_ratio(ls_global_account_path(symbol, period), start=start, end=end)


def load_taker_volume(symbol: str, *, period="5m", start=None, end=None) -> pd.DataFrame:
    return _load_ratio(taker_volume_path(symbol, period), start=start, end=end)


# ---------- Incremental capture (for live data accumulation on VM) ----------


def _append_ratio_records(out_path: Path, new_rows: list[dict]) -> int:
    """Merge new rows into existing parquet by `timestamp`, dedupe, write back.
    Returns number of rows actually appended."""
    if not new_rows:
        return 0
    new_df = pd.DataFrame(new_rows)
    if "timestamp_dt" not in new_df.columns:
        new_df["timestamp_dt"] = pd.to_datetime(new_df["timestamp"], unit="ms", utc=True)

    if out_path.exists():
        existing = pd.read_parquet(out_path)
        before = len(existing)
        combined = pd.concat([existing, new_df], ignore_index=True)
        combined = combined.drop_duplicates("timestamp", keep="last")
        combined = combined.sort_values("timestamp").reset_index(drop=True)
        added = len(combined) - before
    else:
        _ensure_dir(out_path)
        combined = new_df.drop_duplicates("timestamp").sort_values("timestamp").reset_index(drop=True)
        added = len(combined)

    combined.to_parquet(out_path, index=False)
    return added


def capture_recent_ratios(
    *,
    symbols: tuple[str, ...] = DEFAULT_SYMBOLS,
    period: str = "5m",
    lookback_hours: int = 24,
) -> dict[str, int]:
    """Fetch the last `lookback_hours` of LS-top/LS-global/taker data for each
    symbol and merge into the lake. Designed to be run as a recurring cron
    (e.g., hourly) so the lake accumulates its own multi-month history that
    can later be analysed without the Binance ~30-day API constraint.
    """
    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    start_ms = now_ms - lookback_hours * 3_600_000
    summary: dict[str, int] = {}

    endpoints = [
        ("ls_top_pos", LS_TOP_POS_URL, ls_top_position_path,
         lambda r: {
             "timestamp": int(r["timestamp"]),
             "ls_top_pos_ratio": float(r["longShortRatio"]),
             "long_pos_pct": float(r["longAccount"]),
             "short_pos_pct": float(r["shortAccount"]),
         }),
        ("ls_global", LS_GLOBAL_URL, ls_global_account_path,
         lambda r: {
             "timestamp": int(r["timestamp"]),
             "ls_global_acc_ratio": float(r["longShortRatio"]),
             "long_acc_pct": float(r["longAccount"]),
             "short_acc_pct": float(r["shortAccount"]),
         }),
        ("taker", TAKER_URL, taker_volume_path,
         lambda r: {
             "timestamp": int(r["timestamp"]),
             "taker_buy_sell_ratio": float(r["buySellRatio"]),
             "taker_buy_vol": float(r["buyVol"]),
             "taker_sell_vol": float(r["sellVol"]),
         }),
    ]

    for symbol in symbols:
        for name, url, path_fn, row_fn in endpoints:
            key = f"{symbol}/{name}"
            try:
                records = _fetch_ratio_window(url, symbol, period, start_ms, now_ms)
            except HistoricalDataError as exc:
                summary[key] = -1
                continue
            rows = [row_fn(r) for r in records]
            added = _append_ratio_records(path_fn(symbol, period), rows)
            summary[key] = added
            time.sleep(0.05)  # be polite to API
    return summary


# ---------- Aligned dataset (the main analysis interface) ----------


def build_aligned_dataset(
    symbol: str,
    *,
    base_interval: str = "5m",
    start: date | str | None = None,
    end: date | str | None = None,
    include_oi: bool = True,
    include_funding: bool = True,
    higher_intervals: tuple[str, ...] = ("1h", "4h"),
) -> pd.DataFrame:
    """Build a single DataFrame with all features merged onto the base interval timeline.

    Columns:
      open_time, open, high, low, close, volume,
      lower_wick_ratio, body_ratio, upper_wick_ratio, intrabar_drop_pct,
      volume_zscore_24h,
      <higher>_close, <higher>_volume_zscore,    (forward-filled from higher TF)
      funding_rate (forward-filled, last known value),
      oi (forward-filled),
    """
    df = load_klines(symbol, base_interval, start=start, end=end).copy()
    if df.empty:
        return df

    # Candle anatomy (use numpy NaN, not pd.NA, to keep float dtype)
    import numpy as np
    rng = (df["high"] - df["low"]).where(df["high"] != df["low"], np.nan)
    body_low = df[["open", "close"]].min(axis=1)
    body_high = df[["open", "close"]].max(axis=1)
    df["lower_wick"] = (body_low - df["low"]).astype(float)
    df["upper_wick"] = (df["high"] - body_high).astype(float)
    df["body"] = (body_high - body_low).astype(float)
    df["lower_wick_ratio"] = (df["lower_wick"] / rng).astype(float)
    df["upper_wick_ratio"] = (df["upper_wick"] / rng).astype(float)
    df["body_ratio"] = (df["body"] / rng).astype(float)
    df["intrabar_drop_pct"] = ((df["open"] - df["low"]) / df["open"].replace(0, np.nan)).astype(float)
    df["intrabar_pump_pct"] = ((df["high"] - df["open"]) / df["open"].replace(0, np.nan)).astype(float)

    # Volume z-score on 24h rolling window for the base interval
    bars_per_24h = {"1m": 1440, "5m": 288, "15m": 96, "1h": 24, "4h": 6}.get(base_interval, 288)
    vol_mean = df["volume"].rolling(bars_per_24h, min_periods=max(20, bars_per_24h // 4)).mean()
    vol_std = df["volume"].rolling(bars_per_24h, min_periods=max(20, bars_per_24h // 4)).std()
    df["volume_zscore_24h"] = ((df["volume"] - vol_mean) / vol_std).astype(float)

    # Higher TF context (forward-fill)
    for hi in higher_intervals:
        try:
            hi_df = load_klines(symbol, hi, start=start, end=end)[["open_time", "close", "volume"]].copy()
        except FileNotFoundError:
            continue
        hi_df = hi_df.rename(columns={"close": f"{hi}_close", "volume": f"{hi}_volume"})
        hi_vol_mean = hi_df[f"{hi}_volume"].rolling(48, min_periods=12).mean()
        hi_vol_std = hi_df[f"{hi}_volume"].rolling(48, min_periods=12).std()
        hi_df[f"{hi}_volume_zscore"] = (hi_df[f"{hi}_volume"] - hi_vol_mean) / hi_vol_std
        df = pd.merge_asof(df.sort_values("open_time"), hi_df.sort_values("open_time"),
                           on="open_time", direction="backward")

    if include_funding:
        try:
            fdf = load_funding(symbol, start=start, end=end).rename(columns={"funding_time": "open_time"})
            df = pd.merge_asof(df.sort_values("open_time"), fdf.sort_values("open_time"),
                               on="open_time", direction="backward")
        except FileNotFoundError:
            pass

    if include_oi:
        try:
            oi_df = load_oi(symbol, start=start, end=end).rename(columns={"timestamp": "open_time"})
            oi_df = oi_df[["open_time", "sum_open_interest"]]
            df = pd.merge_asof(df.sort_values("open_time"), oi_df.sort_values("open_time"),
                               on="open_time", direction="backward")
            df = df.rename(columns={"sum_open_interest": "oi"})
            df["oi_change_1h"] = df["oi"].pct_change(periods=bars_per_24h // 24)
        except FileNotFoundError:
            pass

    # Optional: top-trader long/short position ratio
    try:
        ls_top = load_ls_top_position(symbol, start=start, end=end).rename(columns={"timestamp": "open_time"})
        ls_top = ls_top[["open_time", "ls_top_pos_ratio", "long_pos_pct", "short_pos_pct"]]
        df = pd.merge_asof(df.sort_values("open_time"), ls_top.sort_values("open_time"),
                           on="open_time", direction="backward")
    except FileNotFoundError:
        pass

    try:
        ls_glob = load_ls_global_account(symbol, start=start, end=end).rename(columns={"timestamp": "open_time"})
        ls_glob = ls_glob[["open_time", "ls_global_acc_ratio"]]
        df = pd.merge_asof(df.sort_values("open_time"), ls_glob.sort_values("open_time"),
                           on="open_time", direction="backward")
    except FileNotFoundError:
        pass

    try:
        taker = load_taker_volume(symbol, start=start, end=end).rename(columns={"timestamp": "open_time"})
        taker = taker[["open_time", "taker_buy_sell_ratio"]]
        df = pd.merge_asof(df.sort_values("open_time"), taker.sort_values("open_time"),
                           on="open_time", direction="backward")
    except FileNotFoundError:
        pass

    # Smart-money divergence: top traders vs all traders
    if "ls_top_pos_ratio" in df.columns and "ls_global_acc_ratio" in df.columns:
        df["smart_money_divergence"] = df["ls_top_pos_ratio"] - df["ls_global_acc_ratio"]

    return df


# ---------- Populate (one-shot bulk download) ----------


@dataclass
class PopulationResult:
    klines_built: int
    funding_built: int
    oi_built: int
    errors: list[str]


def populate_lake(
    *,
    symbols: tuple[str, ...] = DEFAULT_SYMBOLS,
    intervals: tuple[str, ...] = DEFAULT_INTERVALS,
    start: date,
    end: date,
    include_oi: bool = True,
    force: bool = False,
) -> PopulationResult:
    klines_built = 0
    funding_built = 0
    oi_built = 0
    errors: list[str] = []
    for symbol in symbols:
        for interval in intervals:
            try:
                p = build_klines_parquet(
                    symbol=symbol, interval=interval, start=start, end=end, force=force
                )
                klines_built += 1
                print(f"  klines {symbol} {interval} -> {p.relative_to(lake_root().parent.parent)}")
            except (HistoricalDataError, OSError) as exc:
                errors.append(f"klines {symbol} {interval}: {exc}")
                print(f"  [skip] klines {symbol} {interval}: {exc}")

        try:
            p = build_funding_parquet(symbol=symbol, start=start, end=end, force=force)
            funding_built += 1
            print(f"  funding {symbol} -> {p.relative_to(lake_root().parent.parent)}")
        except (HistoricalDataError, OSError) as exc:
            errors.append(f"funding {symbol}: {exc}")
            print(f"  [skip] funding {symbol}: {exc}")

        if include_oi:
            try:
                p = build_oi_parquet(symbol=symbol, start=start, end=end, force=force)
                oi_built += 1
                print(f"  oi {symbol} -> {p.relative_to(lake_root().parent.parent)}")
            except (HistoricalDataError, OSError) as exc:
                errors.append(f"oi {symbol}: {exc}")
                print(f"  [skip] oi {symbol}: {exc}")

    return PopulationResult(klines_built=klines_built, funding_built=funding_built,
                            oi_built=oi_built, errors=errors)


def main(argv: list[str] | None = None) -> None:
    import argparse
    p = argparse.ArgumentParser(description="Populate local research data lake")
    p.add_argument("--symbols", nargs="*", default=list(DEFAULT_SYMBOLS))
    p.add_argument("--intervals", nargs="*", default=list(DEFAULT_INTERVALS))
    p.add_argument("--start", default="2025-01-01")
    p.add_argument("--end", default=None)
    p.add_argument("--no-oi", action="store_true", help="skip OI download")
    p.add_argument("--force", action="store_true", help="rebuild even if file exists")
    args = p.parse_args(argv)

    start = parse_yyyy_mm_dd(args.start)
    end = parse_yyyy_mm_dd(args.end) if args.end else (date.today() - timedelta(days=1))

    print(f"=== Populating data lake at {lake_root()} ===")
    print(f"  symbols  : {args.symbols}")
    print(f"  intervals: {args.intervals}")
    print(f"  range    : {start} ~ {end}")
    print(f"  OI       : {'no' if args.no_oi else 'yes (limited to ~30 days)'}")

    result = populate_lake(
        symbols=tuple(args.symbols),
        intervals=tuple(args.intervals),
        start=start, end=end,
        include_oi=not args.no_oi,
        force=args.force,
    )
    print(f"\n  klines built : {result.klines_built}")
    print(f"  funding built: {result.funding_built}")
    print(f"  oi built     : {result.oi_built}")
    if result.errors:
        print(f"  errors       : {len(result.errors)}")


if __name__ == "__main__":
    main()
