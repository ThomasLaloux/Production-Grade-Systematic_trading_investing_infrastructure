"""
News Filter Module
====================
High-impact news blackout window filter for live trading.

Prevents new position entries during configurable time windows around
high-impact (red) economic events. Integrates with the live engine
via strategy_params.yaml configuration.

Configuration (in strategy_params.yaml per strategy):
    _news_filter_mode: "disabled" | "close_positions" | "hold_positions"
        - disabled: no filtering (default)
        - close_positions: close open positions + block new entries
        - hold_positions: keep open positions + block new entries
    _news_filter_before_minutes: int (minutes before event to start blackout)
        Negative values allowed when _inverse_news_filter=True (e.g. -5 means
        trading window starts 5 min AFTER the event).
    _news_filter_after_minutes: int (minutes after event to end blackout)
    _inverse_news_filter: bool (default False)
        - False: standard blackout — block trading DURING the window
        - True: inverse — ONLY allow trading DURING the window
          (for strategies that trade the news reaction)

Standard mode logic:
    Trading is BLOCKED when current time falls within
    [event - before_minutes, event + after_minutes] for any relevant event.

Inverse mode logic:
    Trading is ONLY ALLOWED when current time falls within
    [event - before_minutes, event + after_minutes] for at least one
    relevant event. Outside all windows (or when no events exist),
    new entries are blocked. When the trading window ENDS,
    _news_filter_mode applies (close or hold open positions).

Classes:
    NewsFilter:
        - is_in_blackout: check if instrument is in news blackout
        - get_upcoming_events: get future high-impact events for instrument

Usage:
    nf = NewsFilter(
        _calendar_manager=cal,
        _currency_map=mapper,
    )

    # Standard mode — block during news
    blackout, events = nf.is_in_blackout(
        _instrument="EURUSD", _broker="oanda",
        _server_time=now_utc,
        _before_minutes=30, _after_minutes=15,
    )

    # Inverse mode — trade ONLY during news reaction
    blackout, events = nf.is_in_blackout(
        _instrument="EURUSD", _broker="oanda",
        _server_time=now_utc,
        _before_minutes=-5, _after_minutes=15,
        _inverse=True,
    )
"""

import logging
from datetime import datetime, timedelta, timezone
from typing import List, Optional, Tuple

import pandas as pd

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

logger = logging.getLogger(__name__)


# Valid news filter modes (strategy_params.yaml)
VALID_NEWS_FILTER_MODES = ("disabled", "close_positions", "hold_positions")


class NewsFilter:
    """
    High-impact news blackout window filter.

    Checks whether an instrument is within a configurable time window
    around high-impact economic events. Used by the live engine to
    suppress new signal entries during volatile news releases.
    """

    def __init__(
        self,
        _calendar_manager: 'FinnhubCalendarManager',
        _currency_map: 'InstrumentCurrencyMap',
    ):
        """
        Initialize NewsFilter.

        Args:
            _calendar_manager: FinnhubCalendarManager instance for event queries.
            _currency_map: InstrumentCurrencyMap instance for instrument→currency mapping.
        """
        self._calendar_manager = _calendar_manager
        self._currency_map = _currency_map

        # Cache today's events to avoid repeated DB queries within a day
        self._cached_date: Optional[str] = None
        self._cached_events: Optional[pd.DataFrame] = None

    def is_in_blackout(
        self,
        _instrument: str,
        _broker: str,
        _server_time: datetime,
        _before_minutes: int = 0,
        _after_minutes: int = 0,
        _inverse: bool = False,
    ) -> Tuple[bool, List[dict]]:
        """
        Check if the instrument is in a news blackout (trading blocked).

        Standard mode (_inverse=False):
            Returns True when current time IS inside any event's
            [event - before_minutes, event + after_minutes] window.

        Inverse mode (_inverse=True):
            Returns True when current time is OUTSIDE all event windows.
            This blocks trading except during news reaction periods.
            If no relevant events exist today, returns True (all blocked).

        Args:
            _instrument: Instrument symbol (broker-specific).
            _broker: Broker name.
            _server_time: Current server time (UTC).
            _before_minutes: Minutes before event for window start.
                Positive = before event. Negative = after event
                (e.g. -5 means window starts 5 min after event).
            _after_minutes: Minutes after event for window end.
            _inverse: If True, invert: block OUTSIDE window instead
                of inside. For trade-the-news strategies.

        Returns:
            Tuple of (is_in_blackout, list_of_relevant_events).
            is_in_blackout=True means "do NOT trade".
            relevant_events: events whose window covers current time
            (standard) or nearest events defining the window (inverse).
        """
        if not _inverse and _before_minutes == 0 and _after_minutes == 0:
            return False, []

        # Get affected currencies for this instrument
        currencies = self._currency_map.get_affected_currencies(
            _instrument, _broker=_broker,
        )
        if not currencies:
            return (True, []) if _inverse else (False, [])

        # Ensure server_time is UTC
        if _server_time.tzinfo is None:
            _server_time = _server_time.replace(tzinfo=timezone.utc)

        # Load today's events (with 1 day buffer on each side for edge cases)
        today_str = _server_time.strftime("%Y-%m-%d")
        events = self._get_events_for_date(today_str)

        if events.empty:
            # No events: standard → not in blackout; inverse → all blocked
            return (True, []) if _inverse else (False, [])

        # Filter to relevant currencies
        mask = events["currency"].isin([c.upper() for c in currencies])
        relevant_events = events[mask]

        if relevant_events.empty:
            return (True, []) if _inverse else (False, [])

        # Check which events have their window covering current time.
        # Window for an event at time T: [T - before_minutes, T + after_minutes]
        # With negative before_minutes (e.g. -5): window starts at T + 5
        #
        # The check "is current_time inside [T - before, T + after]?" is
        # equivalent to "is T inside [now - after, now + before]?"
        window_start = _server_time - timedelta(minutes=_after_minutes)
        window_end = _server_time + timedelta(minutes=_before_minutes)

        in_window_events = []
        for _, row in relevant_events.iterrows():
            event_time = row["datetime_utc"]
            if isinstance(event_time, str):
                event_time = pd.Timestamp(event_time, tz="UTC")
            elif event_time.tzinfo is None:
                event_time = event_time.replace(tzinfo=timezone.utc)

            if window_start <= event_time <= window_end:
                in_window_events.append({
                    "datetime_utc": event_time,
                    "currency": row["currency"],
                    "event_title": row["event_title"],
                    "impact": row["impact"],
                })

        in_window = len(in_window_events) > 0

        if _inverse:
            # Inverse mode: blackout when OUTSIDE all windows
            is_blackout = not in_window
            if is_blackout:
                logger.info(
                    f"NewsFilter: INVERSE BLACKOUT for {_instrument} — "
                    f"outside all news trading windows"
                )
            else:
                logger.debug(
                    f"NewsFilter: inverse mode — {_instrument} inside "
                    f"news trading window ({len(in_window_events)} events)"
                )
            return is_blackout, in_window_events
        else:
            # Standard mode: blackout when INSIDE any window
            if in_window:
                event_names = [e["event_title"] for e in in_window_events]
                logger.info(
                    f"NewsFilter: BLACKOUT for {_instrument} — "
                    f"{len(in_window_events)} active events: {event_names}"
                )
            return in_window, in_window_events

    def get_upcoming_events(
        self,
        _instrument: str,
        _broker: str,
        _server_time: datetime,
        _horizon_minutes: int = 60,
    ) -> List[dict]:
        """
        Get upcoming high-impact events within horizon for an instrument.

        Args:
            _instrument: Instrument symbol.
            _broker: Broker name.
            _server_time: Current server time (UTC).
            _horizon_minutes: Look-ahead window in minutes.

        Returns:
            List of event dicts.
        """
        currencies = self._currency_map.get_affected_currencies(
            _instrument, _broker=_broker,
        )
        if not currencies:
            return []

        if _server_time.tzinfo is None:
            _server_time = _server_time.replace(tzinfo=timezone.utc)

        today_str = _server_time.strftime("%Y-%m-%d")
        events = self._get_events_for_date(today_str)

        if events.empty:
            return []

        mask = events["currency"].isin([c.upper() for c in currencies])
        relevant = events[mask]

        horizon_end = _server_time + timedelta(minutes=_horizon_minutes)
        upcoming = []

        for _, row in relevant.iterrows():
            event_time = row["datetime_utc"]
            if isinstance(event_time, str):
                event_time = pd.Timestamp(event_time, tz="UTC")
            elif event_time.tzinfo is None:
                event_time = event_time.replace(tzinfo=timezone.utc)

            if _server_time <= event_time <= horizon_end:
                upcoming.append({
                    "datetime_utc": event_time,
                    "currency": row["currency"],
                    "event_title": row["event_title"],
                    "impact": row["impact"],
                    "minutes_until": (event_time - _server_time).total_seconds() / 60,
                })

        return sorted(upcoming, key=lambda x: x["datetime_utc"])

    def _get_events_for_date(self, _date_str: str) -> pd.DataFrame:
        """
        Get high-impact events for a date (with 1-day buffer), using cache.

        Args:
            _date_str: Date string (YYYY-MM-DD).

        Returns:
            DataFrame of high-impact events.
        """
        if self._cached_date == _date_str and self._cached_events is not None:
            return self._cached_events

        # Fetch with 1-day buffer for events that span midnight
        from_date = (
            datetime.strptime(_date_str, "%Y-%m-%d") - timedelta(days=1)
        ).strftime("%Y-%m-%d")
        to_date = (
            datetime.strptime(_date_str, "%Y-%m-%d") + timedelta(days=1)
        ).strftime("%Y-%m-%d")

        try:
            events = self._calendar_manager.get_news_events(
                _from=from_date,
                _to=to_date,
                _impact="high",
            )
        except Exception as e:
            logger.warning(
                f"NewsFilter: failed to query events for {_date_str} — {e}"
            )
            events = pd.DataFrame()

        self._cached_date = _date_str
        self._cached_events = events
        return events

    def invalidate_cache(self) -> None:
        """Invalidate the daily event cache (call on day rollover)."""
        self._cached_date = None
        self._cached_events = None
