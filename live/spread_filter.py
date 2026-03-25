"""
Spread Filter Module (P2.1)
=============================
Pre-trade spread check — skips signals when the bid-ask spread is
abnormally wide (news events, low liquidity, off-hours).

Rationale (from design doc Section 7.2e):
    Wide spreads during news events or low liquidity periods can turn
    profitable signals into losing trades. The threshold should be
    configurable per instrument.

Architecture:
    - Before placing any order, query the broker for current bid/ask
    - Calculate spread in points (ask - bid)
    - Compare against instrument-specific threshold (in points)
    - If spread > threshold: skip signal, log to journal
    - Threshold expressed as a multiplier of pip_size for portability

Classes:
    SpreadFilter:
        - check: returns (approved, spread_points, reason)

Usage:
    spread_filter = SpreadFilter(
        _broker=broker,
        _max_spread_pips=5.0,
        _pip_size=0.0001,
    )
    approved, spread_pts, reason = spread_filter.check(
        _symbol="XAUUSDp",
    )
    if not approved:
        print(f"Spread too wide: {reason}")
"""

import logging
import time
from typing import Tuple

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

logger = logging.getLogger(__name__)


class SpreadFilter:
    """
    Pre-trade spread check.

    Queries the broker for current bid/ask, calculates spread,
    and rejects signals when spread exceeds the configured threshold.
    """

    def __init__(
        self,
        _broker: 'BrokerBase',
        _max_spread_pips: float,
        _pip_size: float,
    ):
        """
        Initialize SpreadFilter.

        Args:
            _broker: Broker instance (must implement get_tick_data).
            _max_spread_pips: Maximum allowed spread in pips.
                If current spread > this value, the signal is skipped.
            _pip_size: Pip size for the instrument (e.g. 0.0001 for EURUSD,
                0.01 for XAUUSD). Used to convert raw spread to pips.
        """
        self._broker = _broker
        self._max_spread_pips = _max_spread_pips
        self._pip_size = _pip_size

        logger.info(
            f"SpreadFilter: initialized — "
            f"max_spread_pips={_max_spread_pips}, "
            f"pip_size={_pip_size}"
        )

    def check(self, _symbol: str) -> Tuple[bool, float, str]:
        """
        Check if the current spread is within acceptable limits.

        Queries the broker for live bid/ask and calculates spread
        in pips.

        Args:
            _symbol: Broker symbol to check (e.g. "XAUUSDp").

        Returns:
            Tuple of (approved: bool, spread_pips: float, reason: str).
            If approved=True, reason is "PASSED".
            If approved=False, reason describes the spread breach.
            If tick data unavailable, returns (True, 0.0, "NO_TICK_DATA")
                — fail-open to avoid blocking trades on data issues.
        """
        t_start = time.perf_counter()

        try:
            tick_data = self._broker.get_tick_data(_symbol=_symbol)
        except Exception as e:
            # Fail-open: if we can't get tick data, allow the trade
            # but log a warning. This prevents spread filter from
            # blocking trading entirely on connectivity issues.
            logger.warning(
                f"SpreadFilter: failed to get tick data for {_symbol} — {e}. "
                f"Failing OPEN (allowing trade)."
            )
            print(f"  [SpreadFilter] WARNING: no tick data, allowing trade")
            return True, 0.0, "NO_TICK_DATA"

        bid = tick_data.get('bid', 0.0)
        ask = tick_data.get('ask', 0.0)

        if bid <= 0 or ask <= 0:
            logger.warning(
                f"SpreadFilter: invalid bid/ask for {_symbol} — "
                f"bid={bid}, ask={ask}. Failing OPEN."
            )
            print(f"  [SpreadFilter] WARNING: invalid bid/ask, allowing trade")
            return True, 0.0, "INVALID_TICK_DATA"

        # Calculate spread in pips
        spread_raw = ask - bid
        spread_pips = spread_raw / self._pip_size if self._pip_size > 0 else 0.0

        elapsed_ms = (time.perf_counter() - t_start) * 1000

        if spread_pips > self._max_spread_pips:
            reason = (
                f"SPREAD_TOO_WIDE: {spread_pips:.1f} pips "
                f"> limit {self._max_spread_pips:.1f} pips "
                f"(bid={bid:.5f}, ask={ask:.5f})"
            )
            logger.warning(f"SpreadFilter: {reason}")
            print(f"  [SpreadFilter] REJECTED — {reason} ({elapsed_ms:.1f}ms)")
            return False, spread_pips, reason

        # Spread OK
        logger.info(
            f"SpreadFilter: PASSED — spread={spread_pips:.1f} pips "
            f"<= {self._max_spread_pips:.1f} pips "
            f"(bid={bid:.5f}, ask={ask:.5f}) ({elapsed_ms:.1f}ms)"
        )
        print(f"  [SpreadFilter] OK — {spread_pips:.1f} pips ({elapsed_ms:.1f}ms)")

        return True, spread_pips, "PASSED"
