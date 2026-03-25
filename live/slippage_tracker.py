"""
Slippage Tracker Module
========================
Tracks actual slippage (separate from commission). Computes the difference
between expected fill price (signal entry price at bar close) and actual
fill price returned by the broker.

Feeds back to adjust backtest expectations and informs execution quality
analysis.

Classes:
    SlippageTracker:
        - record: log a fill event with expected vs actual price
        - get_summary: aggregate slippage statistics
        - get_records: return all recorded slippage events

Usage:
    tracker = SlippageTracker()
    tracker.record(
        _symbol="XAUUSDp", _side=OrderSide.BUY,
        _expected_price=1850.0, _actual_price=1850.15,
        _quantity=0.10, _strategy="trend_retracement",
    )
    summary = tracker.get_summary()
"""

import logging
import time
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from core.data_types import OrderSide

logger = logging.getLogger(__name__)


class SlippageRecord:
    """Single slippage record for one fill event."""

    __slots__ = [
        'timestamp', 'symbol', 'side', 'expected_price',
        'actual_price', 'slippage', 'slippage_pct',
        'quantity', 'strategy',
    ]

    def __init__(
        self,
        _timestamp: datetime,
        _symbol: str,
        _side: OrderSide,
        _expected_price: float,
        _actual_price: float,
        _quantity: float,
        _strategy: str,
    ):
        self.timestamp = _timestamp
        self.symbol = _symbol
        self.side = _side
        self.expected_price = _expected_price
        self.actual_price = _actual_price
        self.quantity = _quantity
        self.strategy = _strategy

        # Slippage = actual - expected
        # Positive = paid more than expected (bad for buys)
        # For sells: slippage = expected - actual (positive = sold lower)
        if _side == OrderSide.BUY:
            self.slippage = _actual_price - _expected_price
        else:
            self.slippage = _expected_price - _actual_price

        if _expected_price > 0:
            self.slippage_pct = self.slippage / _expected_price
        else:
            self.slippage_pct = 0.0

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            'timestamp': self.timestamp.isoformat(),
            'symbol': self.symbol,
            'side': self.side.name,
            'expected_price': self.expected_price,
            'actual_price': self.actual_price,
            'slippage': self.slippage,
            'slippage_pct': self.slippage_pct,
            'quantity': self.quantity,
            'strategy': self.strategy,
        }


class SlippageTracker:
    """
    Tracks actual slippage per fill, separate from commission.

    All slippage values are signed:
        - Positive slippage = unfavorable (paid more / received less)
        - Negative slippage = favorable (paid less / received more)
    """

    def __init__(self):
        """Initialize SlippageTracker with empty records."""
        self._records: List[SlippageRecord] = []

    def record(
        self,
        _symbol: str,
        _side: OrderSide,
        _expected_price: float,
        _actual_price: float,
        _quantity: float,
        _strategy: str,
        _timestamp: Optional[datetime] = None,
    ) -> SlippageRecord:
        """
        Record a slippage event.

        Args:
            _symbol: Broker symbol.
            _side: OrderSide.BUY or OrderSide.SELL.
            _expected_price: Expected fill price (signal entry at bar close).
            _actual_price: Actual fill price from broker.
            _quantity: Filled quantity in lots.
            _strategy: Strategy name.
            _timestamp: Fill timestamp (defaults to now UTC if not provided).

        Returns:
            The created SlippageRecord.
        """
        if _timestamp is None:
            _timestamp = datetime.now(timezone.utc)

        rec = SlippageRecord(
            _timestamp=_timestamp,
            _symbol=_symbol,
            _side=_side,
            _expected_price=_expected_price,
            _actual_price=_actual_price,
            _quantity=_quantity,
            _strategy=_strategy,
        )
        self._records.append(rec)

        logger.info(
            f"SlippageTracker: {_side.name} {_symbol} — "
            f"expected={_expected_price:.5f}, actual={_actual_price:.5f}, "
            f"slippage={rec.slippage:.5f} ({rec.slippage_pct:.4%}), "
            f"qty={_quantity:.2f}"
        )
        print(
            f"  [SlippageTracker] {_side.name} {_symbol}: "
            f"slippage={rec.slippage:.5f} ({rec.slippage_pct:.4%})"
        )

        return rec

    def get_summary(self) -> Dict[str, Any]:
        """
        Get aggregate slippage statistics.

        Returns:
            Dict with:
                - total_records: number of recorded fills
                - mean_slippage: average slippage (signed)
                - mean_slippage_pct: average slippage as % of price
                - max_slippage: worst (most unfavorable) slippage
                - min_slippage: best (most favorable) slippage
                - total_slippage_cost: sum of slippage * quantity
                - by_symbol: per-symbol breakdown
                - by_strategy: per-strategy breakdown
        """
        if not self._records:
            return {
                'total_records': 0,
                'mean_slippage': 0.0,
                'mean_slippage_pct': 0.0,
                'max_slippage': 0.0,
                'min_slippage': 0.0,
                'total_slippage_cost': 0.0,
                'by_symbol': {},
                'by_strategy': {},
            }

        slippages = [r.slippage for r in self._records]
        slippage_pcts = [r.slippage_pct for r in self._records]
        total_cost = sum(r.slippage * r.quantity for r in self._records)

        # Per-symbol breakdown
        by_symbol: Dict[str, Dict[str, Any]] = defaultdict(
            lambda: {'count': 0, 'total_slippage': 0.0, 'slippages': []}
        )
        for r in self._records:
            by_symbol[r.symbol]['count'] += 1
            by_symbol[r.symbol]['total_slippage'] += r.slippage
            by_symbol[r.symbol]['slippages'].append(r.slippage)

        symbol_summary = {}
        for sym, data in by_symbol.items():
            symbol_summary[sym] = {
                'count': data['count'],
                'mean_slippage': (
                    data['total_slippage'] / data['count']
                    if data['count'] > 0 else 0.0
                ),
            }

        # Per-strategy breakdown
        by_strategy: Dict[str, Dict[str, Any]] = defaultdict(
            lambda: {'count': 0, 'total_slippage': 0.0}
        )
        for r in self._records:
            by_strategy[r.strategy]['count'] += 1
            by_strategy[r.strategy]['total_slippage'] += r.slippage

        strategy_summary = {}
        for strat, data in by_strategy.items():
            strategy_summary[strat] = {
                'count': data['count'],
                'mean_slippage': (
                    data['total_slippage'] / data['count']
                    if data['count'] > 0 else 0.0
                ),
            }

        return {
            'total_records': len(self._records),
            'mean_slippage': sum(slippages) / len(slippages),
            'mean_slippage_pct': sum(slippage_pcts) / len(slippage_pcts),
            'max_slippage': max(slippages),
            'min_slippage': min(slippages),
            'total_slippage_cost': total_cost,
            'by_symbol': symbol_summary,
            'by_strategy': strategy_summary,
        }

    def get_records(self) -> List[Dict[str, Any]]:
        """Return all recorded slippage events as list of dicts."""
        return [r.to_dict() for r in self._records]

    @property
    def record_count(self) -> int:
        """Get number of recorded slippage events."""
        return len(self._records)
