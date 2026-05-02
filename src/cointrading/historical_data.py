from __future__ import annotations

from dataclasses import dataclass
import csv
from datetime import date, datetime, timedelta, timezone
import io
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen
import zipfile

from cointrading.models import Kline


BINANCE_VISION_BASE_URL = "https://data.binance.vision/data"


class HistoricalDataError(RuntimeError):
    pass


@dataclass(frozen=True)
class HistoricalKlineResult:
    symbol: str
    interval: str
    start_date: date
    end_date: date
    klines: list[Kline]
    source_files: list[Path]
    missing_urls: list[str]


def default_history_dir() -> Path:
    return Path(__file__).resolve().parents[2] / "data" / "binance_history"


def parse_yyyy_mm_dd(value: str | date) -> date:
    if isinstance(value, date):
        return value
    return datetime.strptime(value, "%Y-%m-%d").date()


def default_history_end_date() -> date:
    return datetime.now(timezone.utc).date() - timedelta(days=1)


def binance_vision_kline_url(
    *,
    symbol: str,
    interval: str,
    period: str,
    value_date: date,
    market: str = "futures/um",
) -> str:
    symbol = symbol.upper()
    if period == "monthly":
        suffix = value_date.strftime("%Y-%m")
    elif period == "daily":
        suffix = value_date.strftime("%Y-%m-%d")
    else:
        raise ValueError("period must be monthly or daily")
    filename = f"{symbol}-{interval}-{suffix}.zip"
    return f"{BINANCE_VISION_BASE_URL}/{market}/{period}/klines/{symbol}/{interval}/{filename}"


def load_binance_vision_klines(
    *,
    symbol: str,
    interval: str,
    start_date: str | date,
    end_date: str | date | None = None,
    history_dir: Path | None = None,
    market: str = "futures/um",
    timeout: float = 30.0,
) -> HistoricalKlineResult:
    start = parse_yyyy_mm_dd(start_date)
    end = parse_yyyy_mm_dd(end_date) if end_date is not None else default_history_end_date()
    if end < start:
        raise ValueError("end_date must be on or after start_date")

    symbol = symbol.upper()
    root = history_dir or default_history_dir()
    rows_by_open_time: dict[int, Kline] = {}
    source_files: list[Path] = []
    missing_urls: list[str] = []

    for month_start in _iter_month_starts(start, end):
        monthly_url = binance_vision_kline_url(
            symbol=symbol,
            interval=interval,
            period="monthly",
            value_date=month_start,
            market=market,
        )
        monthly_path = _cache_path(root, monthly_url)
        if _download_if_needed(monthly_url, monthly_path, timeout=timeout):
            source_files.append(monthly_path)
            for row in _read_zip_klines(monthly_path):
                if _row_in_range(row, start, end):
                    rows_by_open_time[row.open_time] = row
            continue

        missing_urls.append(monthly_url)
        for day in _iter_days(max(start, month_start), min(end, _month_end(month_start))):
            daily_url = binance_vision_kline_url(
                symbol=symbol,
                interval=interval,
                period="daily",
                value_date=day,
                market=market,
            )
            daily_path = _cache_path(root, daily_url)
            if _download_if_needed(daily_url, daily_path, timeout=timeout):
                source_files.append(daily_path)
                for row in _read_zip_klines(daily_path):
                    if _row_in_range(row, start, end):
                        rows_by_open_time[row.open_time] = row
            else:
                missing_urls.append(daily_url)

    return HistoricalKlineResult(
        symbol=symbol,
        interval=interval,
        start_date=start,
        end_date=end,
        klines=[rows_by_open_time[key] for key in sorted(rows_by_open_time)],
        source_files=source_files,
        missing_urls=missing_urls,
    )


def _cache_path(root: Path, url: str) -> Path:
    marker = "/data/"
    relative = url.split(marker, 1)[-1] if marker in url else url.rsplit("/", 1)[-1]
    return root / relative


def _download_if_needed(url: str, path: Path, *, timeout: float) -> bool:
    if path.exists() and path.stat().st_size > 0:
        return True
    path.parent.mkdir(parents=True, exist_ok=True)
    request = Request(url, headers={"User-Agent": "cointrading-research/0.1"})
    try:
        with urlopen(request, timeout=timeout) as response:
            payload = response.read()
    except HTTPError as exc:
        if exc.code == 404:
            return False
        raise HistoricalDataError(f"Binance public data HTTP {exc.code}: {url}") from exc
    except URLError as exc:
        raise HistoricalDataError(f"Binance public data download failed: {url}") from exc
    if not payload:
        return False
    temp_path = path.with_suffix(path.suffix + ".tmp")
    temp_path.write_bytes(payload)
    temp_path.replace(path)
    return True


def _read_zip_klines(path: Path) -> list[Kline]:
    with zipfile.ZipFile(path) as archive:
        names = [name for name in archive.namelist() if not name.endswith("/")]
        if not names:
            return []
        with archive.open(names[0]) as raw_file:
            text = io.TextIOWrapper(raw_file, encoding="utf-8")
            reader = csv.reader(text)
            return [_parse_kline_csv_row(row) for row in reader if _is_kline_row(row)]


def _is_kline_row(row: list[str]) -> bool:
    if len(row) < 7:
        return False
    try:
        int(row[0])
        float(row[1])
    except ValueError:
        return False
    return True


def _parse_kline_csv_row(row: list[str]) -> Kline:
    return Kline(
        open_time=int(row[0]),
        open=float(row[1]),
        high=float(row[2]),
        low=float(row[3]),
        close=float(row[4]),
        volume=float(row[5]),
        close_time=int(row[6]),
    )


def _row_in_range(row: Kline, start: date, end: date) -> bool:
    timestamp = datetime.fromtimestamp(row.open_time / 1000.0, tz=timezone.utc)
    return start <= timestamp.date() <= end


def _iter_month_starts(start: date, end: date):
    current = start.replace(day=1)
    last = end.replace(day=1)
    while current <= last:
        yield current
        if current.month == 12:
            current = current.replace(year=current.year + 1, month=1)
        else:
            current = current.replace(month=current.month + 1)


def _iter_days(start: date, end: date):
    current = start
    while current <= end:
        yield current
        current += timedelta(days=1)


def _month_end(month_start: date) -> date:
    if month_start.month == 12:
        next_month = month_start.replace(year=month_start.year + 1, month=1)
    else:
        next_month = month_start.replace(month=month_start.month + 1)
    return next_month - timedelta(days=1)
