"""
Market Hours Filter Module
============================
Prevents signal evaluation and order submission outside market trading hours.

Features:
    - Per-asset-class session definitions from brokers.yaml
    - Per-day-of-week trading windows (multiple sessions per day supported)
    - Holiday calendar with 3 market status modes:
        CLOSED          — no trading, no heartbeat (indices on national holiday)
        THIN_LIQUIDITY  — trading allowed but with caution (forex on currency holiday)
        NORMAL          — business as usual
    - Timezone-aware checks: market hours are defined in the broker's
      reference timezone (America/New_York for MT5 brokers that follow
      US DST), and server_time (UTC) is converted before comparison.
    - This approach automatically handles the US DST transitions that
      MT5 brokers follow (second Sunday of March / first Sunday of Nov).

P1.2 Architecture:
    MT5 broker server time follows US DST rules applied to a UTC+2/+3
    offset — effectively America/New_York + 7 hours. By expressing
    market hours in ET and using ZoneInfo("America/New_York"), the
    IANA timezone database handles DST transitions automatically.
    The 2-week EU/US DST mismatch is eliminated.

P7.2 Additions:
    - Holidays sourced dynamically from FinnhubCalendarManager (calendar_news
      module) instead of static list in brokers.yaml.
    - MarketStatus enum: CLOSED / THIN_LIQUIDITY / NORMAL
    - get_market_status() method returns full status (not just bool)
    - Backward compatible: is_market_open() still returns bool

Data Source:
    Market hours are defined in brokers.yaml under each broker's
    market_hours section, keyed by asset class (forex, metals, indices).
    Times are expressed in Eastern Time (America/New_York).
    Holidays come from calendar_news.FinnhubCalendarManager.

Classes:
    MarketStatus(Enum)
        - CLOSED, THIN_LIQUIDITY, NORMAL
    MarketHoursFilter:
        - is_market_open: check if the market is currently open (bool)
        - get_market_status: full status check (MarketStatus)
        - next_open_time: get the next market open time
        - get_session_windows: get today's trading windows

Usage:
    market_hours = MarketHoursFilter(
        _market_hours_config=config,
        _asset_class="forex",
        _holidays=["2026-12-25", "2027-01-01"],
        _timezone="America/New_York",
    )
    if market_hours.is_market_open(_server_time=server_time):
        # evaluate signals

    status = market_hours.get_market_status(_server_time=server_time)
    if status == MarketStatus.CLOSED:
        # fully closed — skip everything
    elif status == MarketStatus.THIN_LIQUIDITY:
        # widen spread thresholds, tighten sizing
"""

import logging
from datetime import datetime, time as dtime, timedelta, timezone
from enum import Enum, auto
from typing import Any, Dict, List, Optional, Tuple

try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo  # Python < 3.9

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

logger = logging.getLogger(__name__)


class MarketStatus(Enum):
    """
    Market status for an instrument on a given day.

    CLOSED:          Market is fully closed (indices on national holiday,
                     COMEX on US holidays). No trading, no heartbeat.
    THIN_LIQUIDITY:  Market technically open but with thin liquidity
                     (forex when a leg currency has a bank holiday).
                     Trading allowed with caution: wider spread thresholds,
                     tighter sizing, possible signal suppression.
    NORMAL:          Business as usual. Crypto always NORMAL.
    """
    CLOSED = auto()
    THIN_LIQUIDITY = auto()
    NORMAL = auto()


# Day name to Python weekday index (Monday=0 ... Sunday=6)
_DAY_NAME_TO_INDEX = {
    'monday': 0,
    'tuesday': 1,
    'wednesday': 2,
    'thursday': 3,
    'friday': 4,
    'saturday': 5,
    'sunday': 6,
}


class MarketHoursFilter:
    """
    Prevents trading outside market hours.

    Checks server time against per-asset-class session definitions
    loaded from brokers.yaml. Session times are defined in the
    broker's reference timezone (typically America/New_York for MT5
    brokers). Incoming UTC server_time is converted to this timezone
    before comparison.

    P7.2: Supports three market statuses per holiday:
        CLOSED          — no trading at all
        THIN_LIQUIDITY  — trading with caution
        NORMAL          — business as usual
    """

    def __init__(
        self,
        _market_hours_config: Dict[str, Any],
        _asset_class: str,
        _holidays: List[str],
        _timezone: str,
        _holiday_statuses: Optional[Dict[str, MarketStatus]] = None,
    ):
        """
        Initialize MarketHoursFilter.

        Args:
            _market_hours_config: Market hours section from brokers.yaml for
                a specific broker. Contains per-asset-class session definitions.
            _asset_class: Asset class key (e.g. "forex", "metals", "indices").
            _holidays: List of holiday date strings (YYYY-MM-DD format).
                       All dates in this list are treated as CLOSED unless
                       overridden by _holiday_statuses.
            _timezone: IANA timezone string (e.g. "America/New_York").
            _holiday_statuses: Optional dict mapping date strings (YYYY-MM-DD)
                               to MarketStatus values. Overrides default CLOSED
                               status for holidays. This allows marking some
                               holidays as THIN_LIQUIDITY instead of CLOSED.
        """
        self._asset_class = _asset_class
        self._timezone_str = _timezone

        # Parse timezone — support IANA names and legacy UTC+N
        if _timezone.startswith('UTC') and ('+' in _timezone or '-' in _timezone):
            logger.warning(
                f"MarketHoursFilter: static UTC offset '{_timezone}' is "
                f"deprecated — use an IANA timezone (e.g. 'America/New_York') "
                f"for correct DST handling. Falling back to UTC."
            )
            self._tz = ZoneInfo("UTC")
        else:
            try:
                self._tz = ZoneInfo(_timezone)
            except (KeyError, Exception) as e:
                logger.error(
                    f"MarketHoursFilter: invalid timezone '{_timezone}' — {e}. "
                    f"Falling back to UTC."
                )
                self._tz = ZoneInfo("UTC")

        # Parse holidays with status
        # {date: MarketStatus} — default is CLOSED for all in _holidays list
        self._holiday_map: Dict[datetime, MarketStatus] = {}
        for h in _holidays:
            try:
                h_date = datetime.strptime(h, "%Y-%m-%d").date()
                # Default to CLOSED unless overridden
                status = MarketStatus.CLOSED
                if _holiday_statuses and h in _holiday_statuses:
                    status = _holiday_statuses[h]
                self._holiday_map[h_date] = status
            except ValueError:
                logger.warning(
                    f"MarketHoursFilter: invalid holiday date format '{h}', "
                    f"expected YYYY-MM-DD"
                )

        # Also add any statuses not in the _holidays list
        if _holiday_statuses:
            for h_str, status in _holiday_statuses.items():
                try:
                    h_date = datetime.strptime(h_str, "%Y-%m-%d").date()
                    if h_date not in self._holiday_map:
                        self._holiday_map[h_date] = status
                except ValueError:
                    pass

        # Backward compat: expose _holidays as list of dates (CLOSED only)
        self._holidays = [
            d for d, s in self._holiday_map.items()
            if s == MarketStatus.CLOSED
        ]

        # Parse session windows per day of week
        self._sessions: Dict[int, List[Tuple[dtime, dtime]]] = {}
        asset_hours = _market_hours_config.get(_asset_class, {})

        if not asset_hours:
            logger.warning(
                f"MarketHoursFilter: no market hours defined for "
                f"asset class '{_asset_class}'. All hours treated as open."
            )

        for day_name, sessions in asset_hours.items():
            day_idx = _DAY_NAME_TO_INDEX.get(day_name.lower())
            if day_idx is None:
                logger.warning(
                    f"MarketHoursFilter: unknown day name '{day_name}'"
                )
                continue

            parsed_sessions = []
            for session in sessions:
                try:
                    open_time = self._parse_time(session['open'])
                    close_time = self._parse_time(session['close'])
                    parsed_sessions.append((open_time, close_time))
                except (KeyError, ValueError) as e:
                    logger.warning(
                        f"MarketHoursFilter: invalid session definition "
                        f"for {day_name}: {session} — {e}"
                    )

            self._sessions[day_idx] = parsed_sessions

        logger.info(
            f"MarketHoursFilter: initialized for asset_class='{_asset_class}', "
            f"timezone='{_timezone}', "
            f"holidays={len(self._holiday_map)} "
            f"(CLOSED={len(self._holidays)}, "
            f"THIN={sum(1 for s in self._holiday_map.values() if s == MarketStatus.THIN_LIQUIDITY)}), "
            f"days_configured={len(self._sessions)}"
        )

    @staticmethod
    def _parse_time(_time_str: str) -> dtime:
        """Parse HH:MM time string to datetime.time object."""
        parts = _time_str.strip().split(':')
        return dtime(hour=int(parts[0]), minute=int(parts[1]))

    def _to_local(self, _server_time: datetime) -> datetime:
        """
        Convert server time to the broker's reference timezone.

        Args:
            _server_time: Server time (should be UTC-aware or naive UTC).

        Returns:
            Datetime in the broker's reference timezone.
        """
        if _server_time.tzinfo is None:
            # Assume naive datetimes are UTC
            _server_time = _server_time.replace(tzinfo=timezone.utc)
        return _server_time.astimezone(self._tz)

    def is_market_open(self, _server_time: datetime) -> bool:
        """
        Check if the market is currently open.

        Args:
            _server_time: Current broker server time (UTC).

        Returns:
            True if market is open (NORMAL or THIN_LIQUIDITY), False if CLOSED.
        """
        status = self.get_market_status(_server_time)
        return status != MarketStatus.CLOSED

    def get_market_status(self, _server_time: datetime) -> MarketStatus:
        """
        Get full market status (CLOSED / THIN_LIQUIDITY / NORMAL).

        Checks holidays first (with per-date status), then session windows.

        Args:
            _server_time: Current broker server time (UTC).

        Returns:
            MarketStatus enum value.
        """
        local_time = self._to_local(_server_time)
        local_date = local_time.date()

        # Check holidays (in local date)
        if local_date in self._holiday_map:
            holiday_status = self._holiday_map[local_date]
            if holiday_status == MarketStatus.CLOSED:
                logger.debug(
                    f"MarketHoursFilter: market CLOSED — holiday "
                    f"{local_date}"
                )
                return MarketStatus.CLOSED
            elif holiday_status == MarketStatus.THIN_LIQUIDITY:
                logger.debug(
                    f"MarketHoursFilter: THIN_LIQUIDITY — holiday "
                    f"{local_date}"
                )
                # Still check if within session windows
                if self._is_within_sessions(local_time):
                    return MarketStatus.THIN_LIQUIDITY
                else:
                    return MarketStatus.CLOSED

        # Get day of week (Monday=0 ... Sunday=6) in local timezone
        day_of_week = local_time.weekday()

        # Get sessions for this day
        sessions = self._sessions.get(day_of_week)

        # If no sessions configured for this day, market is closed
        if sessions is None:
            if not self._sessions:
                return MarketStatus.NORMAL  # fallback: no config → all open
            return MarketStatus.CLOSED

        # Empty list means explicitly closed (e.g. saturday: [])
        if len(sessions) == 0:
            return MarketStatus.CLOSED

        # Check if current time falls within any session
        current_time = local_time.time()
        for open_time, close_time in sessions:
            if open_time <= current_time <= close_time:
                return MarketStatus.NORMAL

        return MarketStatus.CLOSED

    def _is_within_sessions(self, _local_time: datetime) -> bool:
        """Check if local time is within any session window for its day."""
        day_of_week = _local_time.weekday()
        sessions = self._sessions.get(day_of_week, [])
        current_time = _local_time.time()
        for open_time, close_time in sessions:
            if open_time <= current_time <= close_time:
                return True
        return False

    def get_session_windows(
        self, _server_time: datetime,
    ) -> List[Tuple[dtime, dtime]]:
        """
        Get trading windows for the given day (in local timezone).

        Args:
            _server_time: Server time to determine the day.

        Returns:
            List of (open_time, close_time) tuples for the day.
            Empty list if market is closed all day.
        """
        local_time = self._to_local(_server_time)
        day_of_week = local_time.weekday()
        return self._sessions.get(day_of_week, [])

    def next_open_time(self, _server_time: datetime) -> Optional[datetime]:
        """
        Calculate the next market open time from the given server time.

        Looks forward up to 7 days to find the next open session.
        Returns time in the broker's reference timezone.

        Args:
            _server_time: Current broker server time (UTC).

        Returns:
            Datetime of next market open (in local tz), or None if no
            sessions found within 7 days.
        """
        local_time = self._to_local(_server_time)
        current_time = local_time.time()
        day_of_week = local_time.weekday()

        # Check remaining sessions today
        sessions_today = self._sessions.get(day_of_week, [])
        for open_time, close_time in sessions_today:
            if current_time < open_time:
                return local_time.replace(
                    hour=open_time.hour,
                    minute=open_time.minute,
                    second=0, microsecond=0,
                )

        # Check next 7 days
        for days_ahead in range(1, 8):
            next_date = local_time + timedelta(days=days_ahead)
            next_day_of_week = next_date.weekday()

            # Skip holidays
            if next_date.date() in self._holidays:
                continue

            sessions = self._sessions.get(next_day_of_week, [])
            if sessions:
                open_time = sessions[0][0]
                return next_date.replace(
                    hour=open_time.hour,
                    minute=open_time.minute,
                    second=0, microsecond=0,
                )

        return None

    @property
    def asset_class(self) -> str:
        """Get the asset class this filter is configured for."""
        return self._asset_class

    @property
    def holiday_map(self) -> Dict:
        """Get the holiday → MarketStatus mapping."""
        return self._holiday_map.copy()

    def update_holidays(
        self,
        _holidays: List[str],
        _holiday_statuses: Optional[Dict[str, 'MarketStatus']] = None,
    ) -> None:
        """
        Update holidays at runtime (e.g. daily hot-reload from calendar DB).

        Args:
            _holidays: List of holiday date strings (YYYY-MM-DD).
            _holiday_statuses: Optional status overrides per date.
        """
        self._holiday_map.clear()
        self._holidays.clear()

        for h in _holidays:
            try:
                h_date = datetime.strptime(h, "%Y-%m-%d").date()
                status = MarketStatus.CLOSED
                if _holiday_statuses and h in _holiday_statuses:
                    status = _holiday_statuses[h]
                self._holiday_map[h_date] = status
                if status == MarketStatus.CLOSED:
                    self._holidays.append(h_date)
            except ValueError:
                pass

        if _holiday_statuses:
            for h_str, status in _holiday_statuses.items():
                try:
                    h_date = datetime.strptime(h_str, "%Y-%m-%d").date()
                    if h_date not in self._holiday_map:
                        self._holiday_map[h_date] = status
                except ValueError:
                    pass

        logger.info(
            f"MarketHoursFilter: holidays updated — "
            f"{len(self._holiday_map)} total "
            f"(CLOSED={len(self._holidays)})"
        )
