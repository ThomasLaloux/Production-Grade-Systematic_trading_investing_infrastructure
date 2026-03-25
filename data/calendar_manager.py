"""
Finnhub Calendar Manager Module
=================================
Fetches, stores, and queries economic calendar events and market holidays
from the Finnhub API (free tier).

Data Model:
    calendar_news (one Parquet + CSV per year):
        datetime_utc | currency | event_title | impact | actual | forecast
        | previous | source

    calendar_holidays (one Parquet + CSV per year):
        date | country_code | exchange | holiday_name | trading_hour
        | market_impact (CLOSED / THIN_LIQUIDITY)

Update Logic:
    - On first call (no existing files): full backfill from _backfill_start
      to end of next week.
    - On subsequent calls: incremental update from last stored date to end
      of next week.
    - Daily cron or live engine startup triggers update().

Finnhub Free Tier Limits:
    - 60 API calls/minute
    - Economic calendar: GET /calendar/economic?from=YYYY-MM-DD&to=YYYY-MM-DD
    - Market holidays: GET /stock/market-holiday?exchange=XX

Classes:
    FinnhubCalendarManager:
        - update: smart backfill + daily update
        - get_news_events: query by date range, currency, impact
        - get_holidays: query by date range, exchange
        - get_market_status: CLOSED / THIN_LIQUIDITY / NORMAL

Usage:
    cal = FinnhubCalendarManager(
        _api_key="YOUR_KEY",
        _db_dir="data/calendar_news_db",
        _backfill_start="2015-01-01",
    )
    cal.update()
    events = cal.get_news_events(
        _from="2026-03-01", _to="2026-03-07", _currency="USD", _impact="high",
    )
"""

import logging
import os
import time
from datetime import datetime, timedelta, timezone, date
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import requests

import sys
sys.path.insert(0, str(Path(__file__).parent.parent))

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

FINNHUB_BASE_URL = "https://finnhub.io/api/v1"

# Finnhub economic calendar impact values → normalised labels
# Finnhub returns impact as 1 (low), 2 (medium), 3 (high)
IMPACT_MAP = {1: "low", 2: "medium", 3: "high"}

# Finnhub free tier: 60 calls/minute → ~1 call/second is safe
API_CALL_DELAY_SECONDS = 1.1

# Maximum date range per Finnhub economic calendar request (empirical)
MAX_CHUNK_DAYS = 90

# Exchange codes used for holiday queries — mapped to country/currency
# Exchange code → (country_code, primary_currency)
EXCHANGE_HOLIDAY_MAP = {
    "US": ("US", "USD"),
    "L":  ("GB", "GBP"),   # London Stock Exchange
    "T":  ("JP", "JPY"),   # Tokyo Stock Exchange
    "AX": ("AU", "AUD"),   # ASX
    "NZ": ("NZ", "NZD"),   # NZX
    "TO": ("CA", "CAD"),   # Toronto Stock Exchange
    "SW": ("CH", "CHF"),   # SIX Swiss Exchange
    "DE": ("DE", "EUR"),   # Deutsche Börse / XETRA
    "PA": ("FR", "EUR"),   # Euronext Paris
}

# Country code → currency (for mapping Finnhub economic calendar events)
COUNTRY_TO_CURRENCY = {
    "US": "USD", "GB": "GBP", "JP": "JPY", "AU": "AUD",
    "NZ": "NZD", "CA": "CAD", "CH": "CHF", "DE": "EUR",
    "FR": "EUR", "IT": "EUR", "ES": "EUR", "EU": "EUR",
    "CN": "CNY", "HK": "HKD", "SG": "SGD", "IN": "INR",
    "BR": "BRL", "MX": "MXN", "ZA": "ZAR", "SE": "SEK",
    "NO": "NOK", "DK": "DKK", "PL": "PLN", "RU": "RUB",
    "TR": "TRY", "KR": "KRW",
}

# Parquet column schemas
NEWS_COLUMNS = [
    "datetime_utc", "currency", "event_title", "impact",
    "actual", "forecast", "previous", "source",
]
HOLIDAY_COLUMNS = [
    "date", "country_code", "exchange", "currency",
    "holiday_name", "trading_hour", "market_impact",
]


class FinnhubCalendarManager:
    """
    Fetches, stores, and queries economic calendar events and market
    holidays from the Finnhub API.

    Storage layout:
        {db_dir}/news/YYYY.parquet   + YYYY.csv
        {db_dir}/holidays/YYYY.parquet + YYYY.csv
    """

    def __init__(
        self,
        _api_key: str,
        _db_dir: str = "data/calendar_news_db",
        _backfill_start: str = "2015-01-01",
    ):
        """
        Initialize FinnhubCalendarManager.

        Args:
            _api_key: Finnhub API key (free tier).
            _db_dir: Directory for Parquet/CSV storage.
            _backfill_start: Earliest date for historical backfill (YYYY-MM-DD).
        """
        self._api_key = _api_key
        self._db_dir = Path(_db_dir)
        self._backfill_start = datetime.strptime(_backfill_start, "%Y-%m-%d").date()

        # Create directories
        (self._db_dir / "news").mkdir(parents=True, exist_ok=True)
        (self._db_dir / "holidays").mkdir(parents=True, exist_ok=True)

        logger.info(
            f"FinnhubCalendarManager: initialized, db_dir={self._db_dir}, "
            f"backfill_start={self._backfill_start}"
        )

    # -----------------------------------------------------------------------
    # Public API
    # -----------------------------------------------------------------------

    def update(self, _test_mode: bool = False) -> Dict[str, int]:
        """
        Smart update: backfill if needed, then incremental to end of next week.

        Args:
            _test_mode: If True, only fetch from 2026-01-01 (for testing).

        Returns:
            Dict with counts: {'news_rows': N, 'holiday_rows': M}
        """
        end_date = self._end_of_next_week()
        start_date = self._backfill_start

        if _test_mode:
            start_date = date(2026, 1, 1)

        # Check existing data — resume from last stored date
        last_news_date = self._get_last_stored_date("news")
        last_holiday_date = self._get_last_stored_date("holidays")

        news_start = start_date
        if last_news_date is not None:
            # Start from day after last stored date
            news_start = max(start_date, last_news_date + timedelta(days=1))

        holiday_start = start_date
        if last_holiday_date is not None:
            holiday_start = max(start_date, last_holiday_date + timedelta(days=1))

        # --- Fetch news events ---
        news_count = 0
        if news_start <= end_date:
            logger.info(
                f"FinnhubCalendarManager: fetching news events "
                f"{news_start} → {end_date}"
            )
            try:
                news_count = self._fetch_and_store_news(news_start, end_date)
            except Exception as e:
                logger.warning(
                    f"FinnhubCalendarManager: news fetch failed — {e}. "
                    f"This may indicate a Finnhub subscription is required "
                    f"for the economic calendar endpoint."
                )
                print(
                    f"  [CalendarManager] WARNING: news fetch failed — {e}. "
                    f"Finnhub subscription may be required for economic calendar."
                )
        else:
            logger.info("FinnhubCalendarManager: news data is up to date")

        # --- Fetch holidays ---
        holiday_count = 0
        # Holidays endpoint returns current year holidays — fetch once per update
        logger.info("FinnhubCalendarManager: fetching market holidays")
        try:
            holiday_count = self._fetch_and_store_holidays()
        except Exception as e:
            logger.warning(
                f"FinnhubCalendarManager: holiday fetch failed — {e}. "
                f"Continuing without holiday data."
            )
            print(
                f"  [CalendarManager] WARNING: holiday fetch failed — {e}."
            )

        result = {"news_rows": news_count, "holiday_rows": holiday_count}
        logger.info(f"FinnhubCalendarManager: update complete — {result}")
        return result

    def get_news_events(
        self,
        _from: str,
        _to: str,
        _currency: Optional[str] = None,
        _impact: str = "high",
    ) -> pd.DataFrame:
        """
        Query news events by date range, optionally filtered by currency and impact.

        Args:
            _from: Start date (YYYY-MM-DD).
            _to: End date (YYYY-MM-DD).
            _currency: Filter by currency (e.g. "USD"). None = all.
            _impact: Filter by impact level: "high", "medium", "low", "all".

        Returns:
            DataFrame with NEWS_COLUMNS.
        """
        from_date = datetime.strptime(_from, "%Y-%m-%d").date()
        to_date = datetime.strptime(_to, "%Y-%m-%d").date()

        # Determine which year files to load
        years = range(from_date.year, to_date.year + 1)
        frames = []
        for year in years:
            path = self._db_dir / "news" / f"{year}.parquet"
            if path.exists():
                df = pd.read_parquet(path)
                frames.append(df)

        if not frames:
            return pd.DataFrame(columns=NEWS_COLUMNS)

        df = pd.concat(frames, ignore_index=True)

        # Filter by date range
        df["datetime_utc"] = pd.to_datetime(df["datetime_utc"], utc=True)
        mask = (
            (df["datetime_utc"].dt.date >= from_date)
            & (df["datetime_utc"].dt.date <= to_date)
        )
        df = df[mask]

        # Filter by impact
        if _impact != "all":
            df = df[df["impact"] == _impact]

        # Filter by currency
        if _currency is not None:
            df = df[df["currency"] == _currency.upper()]

        return df.sort_values("datetime_utc").reset_index(drop=True)

    def get_holidays(
        self,
        _from: str,
        _to: str,
        _exchange: Optional[str] = None,
        _currency: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        Query holidays by date range, optionally filtered by exchange or currency.

        Args:
            _from: Start date (YYYY-MM-DD).
            _to: End date (YYYY-MM-DD).
            _exchange: Filter by exchange code (e.g. "US"). None = all.
            _currency: Filter by currency (e.g. "USD"). None = all.

        Returns:
            DataFrame with HOLIDAY_COLUMNS.
        """
        from_date = datetime.strptime(_from, "%Y-%m-%d").date()
        to_date = datetime.strptime(_to, "%Y-%m-%d").date()

        years = range(from_date.year, to_date.year + 1)
        frames = []
        for year in years:
            path = self._db_dir / "holidays" / f"{year}.parquet"
            if path.exists():
                df = pd.read_parquet(path)
                frames.append(df)

        if not frames:
            return pd.DataFrame(columns=HOLIDAY_COLUMNS)

        df = pd.concat(frames, ignore_index=True)

        # Filter by date range
        df["date"] = pd.to_datetime(df["date"]).dt.date
        mask = (df["date"] >= from_date) & (df["date"] <= to_date)
        df = df[mask]

        if _exchange is not None:
            df = df[df["exchange"] == _exchange.upper()]

        if _currency is not None:
            df = df[df["currency"] == _currency.upper()]

        return df.sort_values("date").reset_index(drop=True)

    def get_holiday_dates(
        self,
        _year: int,
        _exchange: Optional[str] = None,
    ) -> List[str]:
        """
        Get a list of holiday date strings (YYYY-MM-DD) for a given year/exchange.

        Convenience method for MarketHoursFilter integration.

        Args:
            _year: Calendar year.
            _exchange: Exchange code (e.g. "US"). None = all exchanges.

        Returns:
            List of date strings.
        """
        df = self.get_holidays(
            _from=f"{_year}-01-01",
            _to=f"{_year}-12-31",
            _exchange=_exchange,
        )
        if df.empty:
            return []

        # Only fully CLOSED holidays (not early close)
        closed = df[df["market_impact"] == "CLOSED"]
        return [d.strftime("%Y-%m-%d") for d in closed["date"]]

    def get_market_status(
        self,
        _date: date,
        _instrument: str,
        _broker: str,
        _currency_map: 'InstrumentCurrencyMap',
    ) -> str:
        """
        Determine market status for an instrument on a given date.

        Args:
            _date: Date to check.
            _instrument: Instrument symbol (broker-specific, e.g. "XAUUSDp").
            _broker: Broker name.
            _currency_map: InstrumentCurrencyMap instance.

        Returns:
            "CLOSED" | "THIN_LIQUIDITY" | "NORMAL"
        """
        affected_currencies = _currency_map.get_affected_currencies(
            _instrument, _broker=_broker,
        )
        asset_class = _currency_map.get_asset_class(
            _instrument, _broker=_broker,
        )

        date_str = _date.strftime("%Y-%m-%d")
        holidays_df = self.get_holidays(
            _from=date_str, _to=date_str,
        )

        if holidays_df.empty:
            return "NORMAL"

        holiday_currencies = set(holidays_df["currency"].str.upper())

        # For indices/commodities: if the country currency has a CLOSED holiday → CLOSED
        if asset_class in ("index", "index futures", "commodity"):
            for curr in affected_currencies:
                if curr.upper() in holiday_currencies:
                    matched = holidays_df[
                        holidays_df["currency"].str.upper() == curr.upper()
                    ]
                    if any(matched["market_impact"] == "CLOSED"):
                        return "CLOSED"
            return "NORMAL"

        # For crypto: always NORMAL (24/7)
        if asset_class == "crypto":
            return "NORMAL"

        # For forex: check both leg currencies
        # If either has a holiday → THIN_LIQUIDITY (forex doesn't fully close)
        for curr in affected_currencies:
            if curr.upper() in holiday_currencies:
                return "THIN_LIQUIDITY"

        return "NORMAL"

    # -----------------------------------------------------------------------
    # Private: Finnhub API calls
    # -----------------------------------------------------------------------

    def _api_get(self, _endpoint: str, _params: Dict[str, Any]) -> Any:
        """
        Make a GET request to Finnhub API with rate limiting.

        Args:
            _endpoint: API endpoint path (e.g. "/calendar/economic").
            _params: Query parameters.

        Returns:
            Parsed JSON response.
        """
        _params["token"] = self._api_key
        url = f"{FINNHUB_BASE_URL}{_endpoint}"

        try:
            response = requests.get(url, params=_params, timeout=30)
            response.raise_for_status()
            time.sleep(API_CALL_DELAY_SECONDS)
            return response.json()
        except requests.exceptions.HTTPError as e:
            if response.status_code == 429:
                logger.warning(
                    "FinnhubCalendarManager: rate limited, waiting 60s..."
                )
                time.sleep(60)
                return self._api_get(_endpoint, _params)
            logger.error(f"FinnhubCalendarManager: HTTP error — {e}")
            raise
        except Exception as e:
            logger.error(f"FinnhubCalendarManager: request failed — {e}")
            raise

    def _fetch_economic_calendar(
        self, _from: date, _to: date,
    ) -> List[Dict[str, Any]]:
        """
        Fetch economic calendar events from Finnhub for a date range.

        Chunks requests into MAX_CHUNK_DAYS intervals.

        Returns:
            List of event dicts from Finnhub.
        """
        all_events = []
        chunk_start = _from

        while chunk_start <= _to:
            chunk_end = min(chunk_start + timedelta(days=MAX_CHUNK_DAYS - 1), _to)

            logger.debug(
                f"FinnhubCalendarManager: fetching economic calendar "
                f"{chunk_start} → {chunk_end}"
            )

            data = self._api_get("/calendar/economic", {
                "from": chunk_start.strftime("%Y-%m-%d"),
                "to": chunk_end.strftime("%Y-%m-%d"),
            })

            events = data.get("economicCalendar", [])
            if events:
                all_events.extend(events)
                logger.debug(
                    f"  → {len(events)} events in chunk"
                )

            chunk_start = chunk_end + timedelta(days=1)

        return all_events

    def _fetch_market_holidays(self, _exchange: str) -> List[Dict[str, Any]]:
        """
        Fetch market holidays for an exchange from Finnhub.

        Args:
            _exchange: Exchange code (e.g. "US", "L", "T").

        Returns:
            List of holiday dicts from Finnhub.
        """
        data = self._api_get("/stock/market-holiday", {
            "exchange": _exchange,
        })
        return data.get("data", [])

    # -----------------------------------------------------------------------
    # Private: Data processing and storage
    # -----------------------------------------------------------------------

    def _fetch_and_store_news(self, _from: date, _to: date) -> int:
        """
        Fetch economic calendar events and store as Parquet + CSV.

        Returns:
            Number of high-impact rows stored.
        """
        raw_events = self._fetch_economic_calendar(_from, _to)

        if not raw_events:
            logger.info("FinnhubCalendarManager: no news events returned")
            return 0

        # Parse into structured rows
        rows = []
        for evt in raw_events:
            country = evt.get("country", "")
            currency = COUNTRY_TO_CURRENCY.get(country, "")
            impact_num = evt.get("impact", 0)
            impact_label = IMPACT_MAP.get(impact_num, "unknown")

            # Parse datetime — Finnhub returns "YYYY-MM-DD HH:MM:SS" in UTC
            evt_time = evt.get("time", "00:00:00")
            evt_date = evt.get("date", "")
            if evt_date:
                try:
                    if evt_time and evt_time != "00:00:00":
                        dt_str = f"{evt_date} {evt_time}"
                        dt_utc = datetime.strptime(
                            dt_str, "%Y-%m-%d %H:%M:%S"
                        ).replace(tzinfo=timezone.utc)
                    else:
                        dt_utc = datetime.strptime(
                            evt_date, "%Y-%m-%d"
                        ).replace(tzinfo=timezone.utc)
                except ValueError:
                    dt_utc = datetime.strptime(
                        evt_date, "%Y-%m-%d"
                    ).replace(tzinfo=timezone.utc)

                rows.append({
                    "datetime_utc": dt_utc,
                    "currency": currency,
                    "event_title": evt.get("event", ""),
                    "impact": impact_label,
                    "actual": evt.get("actual", None),
                    "forecast": evt.get("estimate", None),
                    "previous": evt.get("prev", None),
                    "source": "finnhub",
                })

        if not rows:
            return 0

        df = pd.DataFrame(rows, columns=NEWS_COLUMNS)
        df["datetime_utc"] = pd.to_datetime(df["datetime_utc"], utc=True)

        # Store by year
        df["_year"] = df["datetime_utc"].dt.year
        total_stored = 0

        for year, year_df in df.groupby("_year"):
            year_df = year_df.drop(columns=["_year"])
            total_stored += self._upsert_year_file(
                year_df, "news", int(year),
                dedup_cols=["datetime_utc", "currency", "event_title"],
            )

        logger.info(
            f"FinnhubCalendarManager: stored {total_stored} news rows "
            f"({len(df[df['impact'] == 'high'])} high-impact)"
        )
        return total_stored

    def _fetch_and_store_holidays(self) -> int:
        """
        Fetch holidays for all tracked exchanges and store.

        Returns:
            Number of holiday rows stored.
        """
        rows = []

        for exchange_code, (country_code, currency) in EXCHANGE_HOLIDAY_MAP.items():
            try:
                holidays = self._fetch_market_holidays(exchange_code)
            except Exception as e:
                logger.warning(
                    f"FinnhubCalendarManager: failed to fetch holidays "
                    f"for exchange={exchange_code} — {e}"
                )
                continue

            for h in holidays:
                at_date = h.get("atDate", "")
                trading_hour = h.get("tradingHour", "")

                # Determine market_impact:
                # Empty tradingHour = fully closed
                # Non-empty tradingHour = early close (reduced hours)
                if not trading_hour:
                    market_impact = "CLOSED"
                else:
                    market_impact = "THIN_LIQUIDITY"

                if at_date:
                    rows.append({
                        "date": at_date,
                        "country_code": country_code,
                        "exchange": exchange_code,
                        "currency": currency,
                        "holiday_name": h.get("eventName", ""),
                        "trading_hour": trading_hour,
                        "market_impact": market_impact,
                    })

        if not rows:
            return 0

        df = pd.DataFrame(rows, columns=HOLIDAY_COLUMNS)
        df["date"] = pd.to_datetime(df["date"])

        # Store by year
        df["_year"] = df["date"].dt.year
        total_stored = 0

        for year, year_df in df.groupby("_year"):
            year_df = year_df.drop(columns=["_year"])
            year_df["date"] = year_df["date"].dt.strftime("%Y-%m-%d")
            total_stored += self._upsert_year_file(
                year_df, "holidays", int(year),
                dedup_cols=["date", "exchange", "holiday_name"],
            )

        logger.info(
            f"FinnhubCalendarManager: stored {total_stored} holiday rows"
        )
        return total_stored

    def _upsert_year_file(
        self,
        _new_df: pd.DataFrame,
        _category: str,
        _year: int,
        dedup_cols: List[str],
    ) -> int:
        """
        Upsert (append + deduplicate) data into a year-partitioned Parquet + CSV.

        Args:
            _new_df: New data to merge.
            _category: "news" or "holidays".
            _year: Year for the file.
            dedup_cols: Columns to use for deduplication.

        Returns:
            Total rows in the file after upsert.
        """
        parquet_path = self._db_dir / _category / f"{_year}.parquet"
        csv_path = self._db_dir / _category / f"{_year}.csv"

        if parquet_path.exists():
            existing = pd.read_parquet(parquet_path)
            combined = pd.concat([existing, _new_df], ignore_index=True)
        else:
            combined = _new_df.copy()

        # Deduplicate
        combined = combined.drop_duplicates(subset=dedup_cols, keep="last")

        # Sort
        sort_col = dedup_cols[0]  # datetime_utc or date
        combined = combined.sort_values(sort_col).reset_index(drop=True)

        # Write Parquet
        combined.to_parquet(parquet_path, index=False)

        # Write CSV (human debugging)
        combined.to_csv(csv_path, index=False)

        return len(combined)

    # -----------------------------------------------------------------------
    # Private: Utilities
    # -----------------------------------------------------------------------

    def _get_last_stored_date(self, _category: str) -> Optional[date]:
        """
        Get the last date with stored data for a category.

        Args:
            _category: "news" or "holidays".

        Returns:
            Last date, or None if no data exists.
        """
        category_dir = self._db_dir / _category
        parquet_files = sorted(category_dir.glob("*.parquet"))

        if not parquet_files:
            return None

        # Read the most recent year file
        latest_file = parquet_files[-1]
        try:
            df = pd.read_parquet(latest_file)
            if df.empty:
                return None

            if _category == "news":
                df["datetime_utc"] = pd.to_datetime(df["datetime_utc"], utc=True)
                return df["datetime_utc"].max().date()
            else:
                df["date"] = pd.to_datetime(df["date"])
                return df["date"].max().date()
        except Exception as e:
            logger.warning(
                f"FinnhubCalendarManager: error reading {latest_file} — {e}"
            )
            return None

    @staticmethod
    def _end_of_next_week() -> date:
        """Get the date of Sunday at the end of next week."""
        today = date.today()
        # Days until next Sunday (end of current week) + 7 for next week
        days_until_sunday = (6 - today.weekday()) % 7
        return today + timedelta(days=days_until_sunday + 7)
