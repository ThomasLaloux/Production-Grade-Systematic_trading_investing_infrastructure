"""
Execution Quality Monitor Module (P2.2)
==========================================
Tracks per-broker execution quality metrics that go beyond simple
slippage tracking (which is handled by SlippageTracker in P1.2).

Metrics (from design doc Section 7.2f):
    - Average fill time (signal timestamp → fill timestamp)
    - Rejection rate (orders rejected / orders submitted)
    - Requote frequency (MT5-specific: requotes / orders submitted)
    - Slippage distribution by time-of-day (hourly buckets)
    - Slippage distribution by volatility regime (if available)

Architecture:
    - In-memory record store during session
    - Aggregation methods for on-demand reporting
    - Parquet persistence at session end for historical analysis
    - Informs broker selection decisions (comparative analysis)

Data Flow:
    1. OrderExecutor calls record_submission() when order is submitted
    2. OrderExecutor calls record_fill() when fill is received
    3. OrderExecutor calls record_rejection() on order rejection
    4. OrderExecutor calls record_requote() on MT5 requotes
    5. LiveEngine calls get_report() at shutdown for summary
    6. LiveEngine calls save_to_parquet() at shutdown for persistence

Classes:
    ExecutionRecord:
        Dataclass for a single execution quality data point.

    ExecutionQualityMonitor:
        - record_submission: log an order submission attempt
        - record_fill: log a successful fill with timing
        - record_rejection: log a rejected order
        - record_requote: log a requote event (MT5-specific)
        - get_report: generate aggregate metrics report
        - get_slippage_by_hour: slippage distribution by time-of-day
        - get_slippage_by_regime: slippage distribution by volatility regime
        - save_to_parquet: persist to parquet for historical analysis
        - record_count: total records tracked

Usage:
    monitor = ExecutionQualityMonitor(
        _broker_name="blackbull_mt5",
        _output_dir="audit/execution_quality",
    )
    monitor.record_submission(
        _symbol="XAUUSDp", _strategy="trend_retracement",
        _side="BUY", _quantity=0.1,
        _order_type="MARKET", _expected_price=1850.0,
    )
    monitor.record_fill(
        _symbol="XAUUSDp", _strategy="trend_retracement",
        _order_id="12345", _fill_price=1850.05,
        _fill_time_ms=45.2,
    )
    report = monitor.get_report()
"""

import logging
import time
from collections import defaultdict
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import sys
sys.path.insert(0, str(Path(__file__).parent.parent))

logger = logging.getLogger(__name__)


@dataclass
class ExecutionRecord:
    """
    Single execution quality data point.

    Captures timing, outcome, and context for one order lifecycle event.
    """
    timestamp: str                        # UTC ISO-8601 when event occurred
    event_type: str                       # SUBMISSION, FILL, REJECTION, REQUOTE
    broker_name: str                      # Broker identifier
    symbol: str                           # Broker symbol
    strategy: str                         # Strategy name
    side: str                             # BUY or SELL
    order_type: str                       # MARKET, LIMIT, etc.
    quantity: float                        # Lots
    expected_price: Optional[float]        # Signal entry price
    fill_price: Optional[float] = None     # Actual fill price
    slippage: Optional[float] = None       # fill - expected (signed)
    slippage_pct: Optional[float] = None   # slippage / expected (signed)
    fill_time_ms: Optional[float] = None   # Signal → fill latency (ms)
    order_id: str = ""                     # Broker-assigned order ID
    rejection_reason: str = ""             # Why order was rejected
    requote_price: Optional[float] = None  # New price offered on requote
    hour_utc: int = 0                      # Hour of day (0-23 UTC) for bucketing
    volatility_regime: str = ""            # Volatility regime label if available


class ExecutionQualityMonitor:
    """
    Tracks per-broker execution quality metrics.

    Goes beyond simple slippage tracking (SlippageTracker) to capture:
        - Fill latency distribution
        - Rejection and requote rates
        - Slippage patterns by time-of-day and volatility regime
    """

    def __init__(
        self,
        _broker_name: str,
        _output_dir: str,
    ):
        """
        Initialize ExecutionQualityMonitor.

        Args:
            _broker_name: Broker identifier for tagging records.
            _output_dir: Directory for parquet persistence (created if not exists).
        """
        self._broker_name = _broker_name
        self._output_dir = Path(_output_dir)
        self._output_dir.mkdir(parents=True, exist_ok=True)

        # In-memory record store
        self._records: List[ExecutionRecord] = []

        # Counters for fast aggregation
        self._submissions: int = 0
        self._fills: int = 0
        self._rejections: int = 0
        self._requotes: int = 0

        # Running sums for incremental averages
        self._total_fill_time_ms: float = 0.0
        self._total_slippage: float = 0.0
        self._total_abs_slippage: float = 0.0

        # Per-hour slippage buckets (0-23)
        self._slippage_by_hour: Dict[int, List[float]] = defaultdict(list)

        # Per-regime slippage buckets
        self._slippage_by_regime: Dict[str, List[float]] = defaultdict(list)

        logger.info(
            f"ExecutionQualityMonitor: initialized — "
            f"broker={_broker_name}, output_dir={self._output_dir}"
        )

    def record_submission(
        self,
        _symbol: str,
        _strategy: str,
        _side: str,
        _quantity: float,
        _order_type: str,
        _expected_price: Optional[float],
        _volatility_regime: str = "",
    ) -> None:
        """
        Record an order submission attempt.

        Called by OrderExecutor when an order is about to be submitted.

        Args:
            _symbol: Broker symbol.
            _strategy: Strategy name.
            _side: "BUY" or "SELL".
            _quantity: Lots.
            _order_type: "MARKET", "LIMIT", etc.
            _expected_price: Signal entry price (mid-price at signal time).
            _volatility_regime: Current volatility regime label (if available).
        """
        now = datetime.now(timezone.utc)
        record = ExecutionRecord(
            timestamp=now.isoformat(),
            event_type="SUBMISSION",
            broker_name=self._broker_name,
            symbol=_symbol,
            strategy=_strategy,
            side=_side,
            order_type=_order_type,
            quantity=_quantity,
            expected_price=_expected_price,
            hour_utc=now.hour,
            volatility_regime=_volatility_regime,
        )
        self._records.append(record)
        self._submissions += 1

        logger.debug(
            f"ExecMonitor: SUBMISSION {_side} {_quantity} {_symbol} "
            f"@ {_expected_price} ({_order_type})"
        )

    def record_fill(
        self,
        _symbol: str,
        _strategy: str,
        _order_id: str,
        _fill_price: float,
        _fill_time_ms: float,
        _expected_price: Optional[float] = None,
        _side: str = "",
        _quantity: float = 0.0,
        _order_type: str = "MARKET",
        _volatility_regime: str = "",
    ) -> None:
        """
        Record a successful order fill.

        Called by OrderExecutor when an order fill is confirmed.

        Args:
            _symbol: Broker symbol.
            _strategy: Strategy name.
            _order_id: Broker-assigned order ID.
            _fill_price: Actual fill price.
            _fill_time_ms: Signal-to-fill latency in milliseconds.
            _expected_price: Expected price (signal entry price).
            _side: "BUY" or "SELL".
            _quantity: Filled quantity in lots.
            _order_type: Order type ("MARKET", "LIMIT").
            _volatility_regime: Current volatility regime label.
        """
        now = datetime.now(timezone.utc)

        # Calculate slippage
        slippage = None
        slippage_pct = None
        if _expected_price is not None and _expected_price > 0:
            slippage = _fill_price - _expected_price
            # For SELL orders, positive slippage (higher fill) is favorable
            if _side == "SELL":
                slippage = -slippage
            slippage_pct = slippage / _expected_price

        record = ExecutionRecord(
            timestamp=now.isoformat(),
            event_type="FILL",
            broker_name=self._broker_name,
            symbol=_symbol,
            strategy=_strategy,
            side=_side,
            order_type=_order_type,
            quantity=_quantity,
            expected_price=_expected_price,
            fill_price=_fill_price,
            slippage=slippage,
            slippage_pct=slippage_pct,
            fill_time_ms=_fill_time_ms,
            order_id=_order_id,
            hour_utc=now.hour,
            volatility_regime=_volatility_regime,
        )
        self._records.append(record)
        self._fills += 1

        # Update running totals
        self._total_fill_time_ms += _fill_time_ms
        if slippage is not None:
            self._total_slippage += slippage
            self._total_abs_slippage += abs(slippage)

            # Bucket by hour
            self._slippage_by_hour[now.hour].append(slippage)

            # Bucket by regime
            if _volatility_regime:
                self._slippage_by_regime[_volatility_regime].append(slippage)

        logger.debug(
            f"ExecMonitor: FILL {_order_id} {_symbol} "
            f"@ {_fill_price} (expected={_expected_price}, "
            f"slippage={slippage}, latency={_fill_time_ms:.1f}ms)"
        )

    def record_rejection(
        self,
        _symbol: str,
        _strategy: str,
        _side: str,
        _quantity: float,
        _order_type: str,
        _expected_price: Optional[float],
        _rejection_reason: str,
        _volatility_regime: str = "",
    ) -> None:
        """
        Record a rejected order.

        Called by OrderExecutor when the broker rejects an order.

        Args:
            _symbol: Broker symbol.
            _strategy: Strategy name.
            _side: "BUY" or "SELL".
            _quantity: Lots attempted.
            _order_type: Order type.
            _expected_price: Expected price.
            _rejection_reason: Reason for rejection.
            _volatility_regime: Current volatility regime.
        """
        now = datetime.now(timezone.utc)
        record = ExecutionRecord(
            timestamp=now.isoformat(),
            event_type="REJECTION",
            broker_name=self._broker_name,
            symbol=_symbol,
            strategy=_strategy,
            side=_side,
            order_type=_order_type,
            quantity=_quantity,
            expected_price=_expected_price,
            rejection_reason=_rejection_reason,
            hour_utc=now.hour,
            volatility_regime=_volatility_regime,
        )
        self._records.append(record)
        self._rejections += 1

        logger.warning(
            f"ExecMonitor: REJECTION {_side} {_quantity} {_symbol} — "
            f"{_rejection_reason}"
        )

    def record_requote(
        self,
        _symbol: str,
        _strategy: str,
        _side: str,
        _quantity: float,
        _expected_price: Optional[float],
        _requote_price: float,
        _volatility_regime: str = "",
    ) -> None:
        """
        Record a requote event (MT5-specific).

        Called by OrderExecutor when the broker returns a requote
        instead of filling the order.

        Args:
            _symbol: Broker symbol.
            _strategy: Strategy name.
            _side: "BUY" or "SELL".
            _quantity: Lots attempted.
            _expected_price: Original requested price.
            _requote_price: New price offered by broker.
            _volatility_regime: Current volatility regime.
        """
        now = datetime.now(timezone.utc)
        record = ExecutionRecord(
            timestamp=now.isoformat(),
            event_type="REQUOTE",
            broker_name=self._broker_name,
            symbol=_symbol,
            strategy=_strategy,
            side=_side,
            order_type="MARKET",
            quantity=_quantity,
            expected_price=_expected_price,
            requote_price=_requote_price,
            hour_utc=now.hour,
            volatility_regime=_volatility_regime,
        )
        self._records.append(record)
        self._requotes += 1

        logger.warning(
            f"ExecMonitor: REQUOTE {_side} {_quantity} {_symbol} — "
            f"expected={_expected_price}, requote={_requote_price}"
        )

    def get_report(self) -> Dict[str, Any]:
        """
        Generate aggregate execution quality metrics report.

        Returns:
            Dict with:
                - broker_name: broker identifier
                - total_submissions: total orders submitted
                - total_fills: total fills received
                - total_rejections: total rejections
                - total_requotes: total requotes (MT5)
                - rejection_rate: rejections / submissions
                - requote_rate: requotes / submissions
                - avg_fill_time_ms: average fill latency
                - avg_slippage: average signed slippage (negative = unfavorable)
                - avg_abs_slippage: average absolute slippage
                - max_slippage: worst (most negative) slippage
                - min_slippage: best (most positive) slippage
                - slippage_by_hour: dict of {hour: {mean, count, std}}
                - slippage_by_regime: dict of {regime: {mean, count, std}}
        """
        report: Dict[str, Any] = {
            "broker_name": self._broker_name,
            "total_submissions": self._submissions,
            "total_fills": self._fills,
            "total_rejections": self._rejections,
            "total_requotes": self._requotes,
            "rejection_rate": (
                self._rejections / self._submissions
                if self._submissions > 0
                else 0.0
            ),
            "requote_rate": (
                self._requotes / self._submissions
                if self._submissions > 0
                else 0.0
            ),
            "avg_fill_time_ms": (
                self._total_fill_time_ms / self._fills
                if self._fills > 0
                else 0.0
            ),
            "avg_slippage": (
                self._total_slippage / self._fills
                if self._fills > 0
                else 0.0
            ),
            "avg_abs_slippage": (
                self._total_abs_slippage / self._fills
                if self._fills > 0
                else 0.0
            ),
        }

        # Min/max slippage from fill records
        fill_slippages = [
            r.slippage for r in self._records
            if r.event_type == "FILL" and r.slippage is not None
        ]
        if fill_slippages:
            report["max_slippage"] = max(fill_slippages)
            report["min_slippage"] = min(fill_slippages)
        else:
            report["max_slippage"] = 0.0
            report["min_slippage"] = 0.0

        # Fill time distribution
        fill_times = [
            r.fill_time_ms for r in self._records
            if r.event_type == "FILL" and r.fill_time_ms is not None
        ]
        if fill_times:
            report["max_fill_time_ms"] = max(fill_times)
            report["min_fill_time_ms"] = min(fill_times)
            report["median_fill_time_ms"] = sorted(fill_times)[len(fill_times) // 2]
        else:
            report["max_fill_time_ms"] = 0.0
            report["min_fill_time_ms"] = 0.0
            report["median_fill_time_ms"] = 0.0

        # Slippage by hour
        report["slippage_by_hour"] = self.get_slippage_by_hour()

        # Slippage by regime
        report["slippage_by_regime"] = self.get_slippage_by_regime()

        return report

    def get_slippage_by_hour(self) -> Dict[int, Dict[str, float]]:
        """
        Get slippage distribution bucketed by hour of day (UTC).

        Returns:
            Dict of {hour (0-23): {"mean": float, "count": int, "std": float}}
        """
        result: Dict[int, Dict[str, float]] = {}
        for hour, slippages in sorted(self._slippage_by_hour.items()):
            n = len(slippages)
            mean = sum(slippages) / n if n > 0 else 0.0
            variance = (
                sum((s - mean) ** 2 for s in slippages) / n
                if n > 1
                else 0.0
            )
            std = variance ** 0.5
            result[hour] = {"mean": mean, "count": n, "std": std}
        return result

    def get_slippage_by_regime(self) -> Dict[str, Dict[str, float]]:
        """
        Get slippage distribution bucketed by volatility regime.

        Returns:
            Dict of {regime: {"mean": float, "count": int, "std": float}}
        """
        result: Dict[str, Dict[str, float]] = {}
        for regime, slippages in sorted(self._slippage_by_regime.items()):
            n = len(slippages)
            mean = sum(slippages) / n if n > 0 else 0.0
            variance = (
                sum((s - mean) ** 2 for s in slippages) / n
                if n > 1
                else 0.0
            )
            std = variance ** 0.5
            result[regime] = {"mean": mean, "count": n, "std": std}
        return result

    def save_to_parquet(self) -> Optional[Path]:
        """
        Persist all records to a parquet file for historical analysis.

        Called at session end / engine shutdown.

        Returns:
            Path to created parquet file, or None if no records or pandas unavailable.
        """
        if not self._records:
            logger.info("ExecMonitor: no records to save")
            return None

        try:
            import pandas as pd
        except ImportError:
            logger.warning(
                "ExecMonitor: pandas not available, skipping parquet save"
            )
            return None

        try:
            records_dicts = [asdict(r) for r in self._records]
            df = pd.DataFrame(records_dicts)

            date_str = datetime.now(timezone.utc).strftime('%Y-%m-%d')
            parquet_path = (
                self._output_dir / f"{date_str}_execution_quality.parquet"
            )

            df.to_parquet(parquet_path, index=False, engine='pyarrow')

            logger.info(
                f"ExecMonitor: saved {len(df)} records to {parquet_path}"
            )
            print(f"  [ExecMonitor] Saved {len(df)} records → {parquet_path}")
            return parquet_path

        except Exception as e:
            logger.error(f"ExecMonitor: parquet save failed — {e}")
            print(f"  [ExecMonitor] WARNING: parquet save failed: {e}")
            return None

    @property
    def record_count(self) -> int:
        """Total records tracked this session."""
        return len(self._records)

    @property
    def fill_count(self) -> int:
        """Total fills tracked this session."""
        return self._fills

    @property
    def rejection_count(self) -> int:
        """Total rejections tracked this session."""
        return self._rejections

    @property
    def requote_count(self) -> int:
        """Total requotes tracked this session."""
        return self._requotes
