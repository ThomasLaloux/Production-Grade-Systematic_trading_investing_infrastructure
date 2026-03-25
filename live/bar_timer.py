"""
Bar Timer Module
=================
Detects when a new bar has started (confirming the previous bar is complete).
This is the heartbeat of the live engine.

Architecture:
    - Polls broker server time at configurable intervals (default 5s for M15).
    - Compares current server time against the expected next bar boundary.
    - When server time crosses the boundary, the previous bar is confirmed complete.
    - Broker-agnostic: uses BrokerBase.get_server_time() abstract method.

Classes:
    BarTimer:
        - wait_for_bar_close: blocks until next bar boundary is crossed
        - get_current_bar_start: returns the start time of the current bar
        - get_next_bar_time: returns the expected start time of the next bar

Usage:
    timer = BarTimer(_broker=broker, _symbol=symbol, _timeframe="M15",
                     _poll_interval_seconds=5)
    timer.wait_for_bar_close()  # blocks until next M15 bar starts
"""

import time
import logging
from datetime import datetime, timezone, timedelta
from typing import Optional

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

logger = logging.getLogger(__name__)


# Timeframe to minutes mapping
_TF_MINUTES = {
    'M1': 1,
    'M5': 5,
    'M15': 15,
    'M30': 30,
    'H1': 60,
    'H4': 240,
    'D1': 1440,
}


class BarTimer:
    """
    Bar completion detection via server time polling.

    For M15 timeframes, tick-by-tick is unnecessary overhead. A 5-second
    poll is sufficient — the strategy only acts on completed bars.
    The broker handles intrabar SL/TP execution natively.
    """

    def __init__(
        self,
        _broker: 'BrokerBase',
        _symbol: str,
        _timeframe: str,
        _poll_interval_seconds: int,
    ):
        """
        Initialize BarTimer.

        Args:
            _broker: Broker instance (must implement get_server_time).
            _symbol: Symbol to track (used for server time via tick).
            _timeframe: Timeframe string (e.g. 'M15', 'M1', 'H1').
            _poll_interval_seconds: Seconds between server time polls.
        """
        self._broker = _broker
        self._symbol = _symbol
        self._timeframe = _timeframe
        self._poll_interval_seconds = _poll_interval_seconds

        if _timeframe not in _TF_MINUTES:
            raise ValueError(
                f"Unsupported timeframe '{_timeframe}'. "
                f"Supported: {list(_TF_MINUTES.keys())}"
            )
        self._tf_minutes = _TF_MINUTES[_timeframe]
        self._last_bar_start: Optional[datetime] = None

    def get_current_bar_start(self, _server_time: datetime) -> datetime:
        """
        Calculate the start time of the current bar given server time.

        Args:
            _server_time: Current broker server time (UTC).

        Returns:
            Start datetime of the bar that _server_time falls into.
        """
        # Truncate to the nearest timeframe boundary
        minutes_since_midnight = _server_time.hour * 60 + _server_time.minute
        bar_start_minutes = (minutes_since_midnight // self._tf_minutes) * self._tf_minutes

        bar_start = _server_time.replace(
            hour=bar_start_minutes // 60,
            minute=bar_start_minutes % 60,
            second=0,
            microsecond=0,
        )
        return bar_start

    def get_next_bar_time(self, _server_time: datetime) -> datetime:
        """
        Calculate when the next bar starts.

        Args:
            _server_time: Current broker server time (UTC).

        Returns:
            Start datetime of the next bar.
        """
        current_bar_start = self.get_current_bar_start(_server_time)
        return current_bar_start + timedelta(minutes=self._tf_minutes)

    def wait_for_bar_close(self) -> datetime:
        """
        Block until the current bar closes (next bar boundary is reached).

        Returns:
            Server time at which the new bar was detected.
        """
        server_time = self._broker.get_server_time(self._symbol)
        current_bar_start = self.get_current_bar_start(server_time)
        next_bar_time = self.get_next_bar_time(server_time)

        logger.info(
            f"BarTimer: waiting for bar close. "
            f"Current bar started at {current_bar_start}, "
            f"next bar at {next_bar_time}, "
            f"server time: {server_time}"
        )

        while True:
            time.sleep(self._poll_interval_seconds)
            server_time = self._broker.get_server_time(self._symbol)

            new_bar_start = self.get_current_bar_start(server_time)
            if new_bar_start >= next_bar_time:
                # New bar has started — previous bar is complete
                self._last_bar_start = new_bar_start
                logger.info(
                    f"BarTimer: new bar detected at {new_bar_start} "
                    f"(server time: {server_time})"
                )
                return server_time

    @property
    def last_bar_start(self) -> Optional[datetime]:
        """Get the start time of the last detected bar."""
        return self._last_bar_start

    @property
    def tf_minutes(self) -> int:
        """Get timeframe duration in minutes."""
        return self._tf_minutes
