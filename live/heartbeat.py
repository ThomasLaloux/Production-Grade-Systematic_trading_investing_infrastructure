"""
Heartbeat / Connectivity Monitor Module
==========================================
Proactive monitoring of broker connectivity and data freshness.

Features:
    - Pings broker connection at configurable intervals
    - Detects stale data (no new tick in X seconds during market hours)
    - Distinguishes between "market closed" and "connection lost"
    - Logs all connectivity events
    - File-based alerting for v1 (future: Telegram/email webhook)

This is different from reconnect logic in error handling — it's proactive
monitoring that runs alongside the main trading loop.

Architecture:
    - Runs as a background thread alongside the main bar-close loop
    - Does not block trading — only logs and flags anomalies
    - The main loop checks heartbeat.is_healthy() before critical operations

Classes:
    HeartbeatMonitor:
        - start: start background monitoring thread
        - stop: stop background monitoring
        - check_once: perform a single connectivity check
        - is_healthy: check if broker connection is healthy
        - last_check_time: timestamp of last successful check

Usage:
    heartbeat = HeartbeatMonitor(
        _broker=broker, _symbol="XAUUSDp",
        _check_interval_seconds=30,
        _stale_data_threshold_seconds=120,
        _market_hours_filter=market_hours,
    )
    heartbeat.start()
    # ... in main loop ...
    if heartbeat.is_healthy():
        # proceed with trading
    heartbeat.stop()
"""

import logging
import threading
import time
from datetime import datetime, timezone
from typing import Optional

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

logger = logging.getLogger(__name__)


class HeartbeatMonitor:
    """
    Proactive broker connectivity and data freshness monitor.

    Runs a background thread that periodically pings the broker.
    The main loop can query is_healthy() to check status.
    """

    def __init__(
        self,
        _broker: 'BrokerBase',
        _symbol: str,
        _check_interval_seconds: int,
        _stale_data_threshold_seconds: int,
        _market_hours_filter: Optional['MarketHoursFilter'],
    ):
        """
        Initialize HeartbeatMonitor.

        Args:
            _broker: Broker instance (must implement is_connected, get_server_time).
            _symbol: Symbol to use for server time checks.
            _check_interval_seconds: Seconds between health checks.
            _stale_data_threshold_seconds: If server time is older than this
                many seconds vs system time, flag as stale (during market hours).
            _market_hours_filter: Optional market hours filter to distinguish
                "market closed" from "connection lost". If None, all hours
                are treated as market hours.
        """
        self._broker = _broker
        self._symbol = _symbol
        self._check_interval_seconds = _check_interval_seconds
        self._stale_data_threshold_seconds = _stale_data_threshold_seconds
        self._market_hours_filter = _market_hours_filter

        # State
        self._healthy = True
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._last_check_time: Optional[datetime] = None
        self._last_server_time: Optional[datetime] = None
        self._consecutive_failures = 0
        self._lock = threading.Lock()

    def start(self) -> None:
        """Start the background monitoring thread."""
        if self._running:
            logger.warning("[HeartbeatMonitor] already running")
            return

        self._running = True
        self._thread = threading.Thread(
            target=self._monitor_loop,
            name="heartbeat-monitor",
            daemon=True,
        )
        self._thread.start()
        logger.info(
            f"[HeartbeatMonitor] started (interval={self._check_interval_seconds}s, "
            f"stale_threshold={self._stale_data_threshold_seconds}s)"
        )

    def stop(self) -> None:
        """Stop the background monitoring thread."""
        self._running = False
        if self._thread is not None:
            self._thread.join(timeout=self._check_interval_seconds + 2)
            self._thread = None
        logger.info("[HeartbeatMonitor] stopped")

    def check_once(self) -> bool:
        """
        Perform a single connectivity check.

        Returns:
            True if broker is healthy, False otherwise.
        """
        now = datetime.now(timezone.utc)

        try:
            # Check broker connection
            if not self._broker.is_connected():
                with self._lock:
                    self._healthy = False
                    self._consecutive_failures += 1
                logger.warning(
                    f"[HeartbeatMonitor] broker disconnected "
                    f"(failures={self._consecutive_failures})"
                )
                return False

            # Get server time
            server_time = self._broker.get_server_time(self._symbol)

            with self._lock:
                self._last_check_time = now
                self._last_server_time = server_time

            # Check data staleness (only during market hours)
            market_open = True
            if self._market_hours_filter is not None:
                market_open = self._market_hours_filter.is_market_open(
                    _server_time=server_time,
                )

            if market_open:
                # Check if server time is reasonably current
                time_diff = abs(
                    (now - server_time).total_seconds()
                    if server_time.tzinfo is not None
                    else (now.replace(tzinfo=None) - server_time).total_seconds()
                )
                if time_diff > self._stale_data_threshold_seconds:
                    with self._lock:
                        self._healthy = False
                        self._consecutive_failures += 1
                    logger.warning(
                        f"[HeartbeatMonitor] stale data detected — "
                        f"server_time={server_time}, now={now}, "
                        f"diff={time_diff:.0f}s > "
                        f"threshold={self._stale_data_threshold_seconds}s "
                        f"(market is OPEN)"
                    )
                    return False
            else:
                # Market is closed — stale data is expected
                logger.debug(
                    f"[HeartbeatMonitor] market closed — "
                    f"stale data check skipped"
                )

            # All checks passed
            with self._lock:
                self._healthy = True
                self._consecutive_failures = 0

            logger.debug(
                f"[HeartbeatMonitor] healthy — "
                f"server_time={server_time}, connected=True"
            )
            return True

        except Exception as e:
            with self._lock:
                self._healthy = False
                self._consecutive_failures += 1
            logger.error(
                f"[HeartbeatMonitor] check failed — {e} "
                f"(failures={self._consecutive_failures})"
            )
            return False

    def _monitor_loop(self) -> None:
        """Background monitoring loop."""
        while self._running:
            self.check_once()
            # Sleep in small increments to allow quick shutdown
            for _ in range(self._check_interval_seconds):
                if not self._running:
                    break
                time.sleep(1)

    def is_healthy(self) -> bool:
        """
        Check if broker connection is healthy.

        Thread-safe — can be called from the main trading loop.

        Returns:
            True if the last health check passed, False otherwise.
        """
        with self._lock:
            return self._healthy

    @property
    def last_check_time(self) -> Optional[datetime]:
        """Get timestamp of the last health check."""
        with self._lock:
            return self._last_check_time

    @property
    def last_server_time(self) -> Optional[datetime]:
        """Get the last server time received from broker."""
        with self._lock:
            return self._last_server_time

    @property
    def consecutive_failures(self) -> int:
        """Get the number of consecutive failed health checks."""
        with self._lock:
            return self._consecutive_failures
