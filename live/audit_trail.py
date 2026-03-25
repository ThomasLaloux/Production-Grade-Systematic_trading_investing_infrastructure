"""
Audit Trail Module (P2.1)
===========================================
Append-only audit trail for every action in the live engine.

Architecture (from design doc Section 10):
    - In-memory buffer during session
    - Append-only CSV flush per record (crash-safe)
    - Parquet conversion at session end / shutdown
    - DuckDB can query across all daily parquet files for analysis
    - Daily files are NEVER modified after creation (immutability)

Record Types:
    - SIGNAL:       Every signal generated (with indicator values)
    - ORDER:        Every order submitted (with full request params)
    - FILL:         Every fill received (actual fill price/time)
    - RISK_CHECK:   Every risk check pass/fail
    - SPREAD_CHECK: Every spread check pass/fail
    - CYCLE:        Each bar-close cycle completion (timing)
    - ACCOUNT:      Account state at each bar close
    - EXIT:         Dynamic exit condition triggered
    - ENGINE:       Engine lifecycle events (start, shutdown, error)

File Structure:
    audit/<date>_audit.csv        — live append-only buffer (crash-safe)
    audit/<date>_audit.parquet    — converted at session end (analysis)

Classes:
    AuditTrail:
        - log: append a record to the journal
        - flush_to_parquet: convert current day's CSV to parquet
        - shutdown: flush remaining buffer, convert to parquet
        - query: DuckDB query across all parquet files (convenience)

Usage:
    journal = AuditTrail(
        _audit_dir="audit",
        _strategy="trend_retracement",
        _symbol="XAUUSDp",
        _timeframe="M15",
        _flush_interval_records=1,
    )
    journal.log(
        _event_type="SIGNAL",
        _details={"direction": "LONG", "entry_price": 1850.0, ...},
    )
    journal.shutdown()
"""

import csv
import io
import logging
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import sys
sys.path.insert(0, str(Path(__file__).parent.parent))

logger = logging.getLogger(__name__)


# --- Column schema for the audit trail ---
# Using a fixed schema ensures CSV structure consistency across sessions.
# New fields go into the 'details_json' column (JSON string) to avoid
# schema changes breaking existing CSV files.
JOURNAL_COLUMNS = [
    'timestamp',         # UTC ISO-8601 when the event occurred
    'event_type',        # SIGNAL, ORDER, FILL, RISK_CHECK, SPREAD_CHECK, CYCLE, ACCOUNT, EXIT, ENGINE
    'strategy',          # Strategy name
    'symbol',            # Broker symbol
    'timeframe',         # Timeframe (e.g. M15)
    'cycle_number',      # Cycle count at time of event
    'direction',         # LONG, SHORT, or empty
    'entry_price',       # Signal entry price or order price
    'stop_loss',         # SL price
    'take_profit',       # TP price
    'quantity',          # Order quantity in lots
    'order_id',          # Broker-assigned order ID
    'fill_price',        # Actual fill price
    'slippage',          # fill_price - entry_price (signed)
    'equity',            # Account equity at event time
    'balance',           # Account balance at event time
    'approved',          # Risk/spread check result: True/False
    'reason',            # Risk/spread check rejection reason
    'elapsed_ms',        # Timing in milliseconds
    'details_json',      # JSON string for additional details (extensible)
]


class AuditTrail:
    """
    Append-only audit trail for live trading.

    In-memory buffer → CSV flush per record → parquet at session end.
    CSV files are append-only and never modified after creation.
    Parquet files are written once at session end for efficient querying.
    """

    def __init__(
        self,
        _audit_dir: str,
        _strategy: str,
        _symbol: str,
        _timeframe: str,
        _flush_interval_records: int,
    ):
        """
        Initialize AuditTrail.

        Args:
            _audit_dir: Directory for audit files (created if not exists).
            _strategy: Strategy name for tagging records.
            _symbol: Broker symbol being traded.
            _timeframe: Timeframe string (e.g. "M15").
            _flush_interval_records: Flush CSV to disk every N records.
                1 = flush every record (safest, default).
        """
        self._audit_dir = Path(_audit_dir)
        self._strategy = _strategy
        self._symbol = _symbol
        self._timeframe = _timeframe
        self._flush_interval = _flush_interval_records

        # Create audit directory
        self._audit_dir.mkdir(parents=True, exist_ok=True)

        # In-memory buffer
        self._buffer: List[Dict[str, Any]] = []
        self._record_count = 0
        self._unflushed_count = 0

        # Current day's CSV file
        self._current_date: Optional[str] = None
        self._csv_path: Optional[Path] = None
        self._csv_file: Optional[io.TextIOWrapper] = None
        self._csv_writer: Optional[csv.DictWriter] = None

        # Open CSV for today
        self._rotate_csv()

        logger.info(
            f"AuditTrail: initialized — dir={self._audit_dir}, "
            f"strategy={_strategy}, symbol={_symbol}, "
            f"flush_interval={_flush_interval_records}"
        )

    def _rotate_csv(self) -> None:
        """
        Open or rotate to the current day's CSV file.

        Creates a new CSV file with headers if it doesn't exist.
        Appends to existing file if it does (e.g., engine restart same day).
        """
        today = datetime.now(timezone.utc).strftime('%Y-%m-%d')

        if self._current_date == today and self._csv_file is not None:
            return  # Already on current day

        # Close previous file if open
        if self._csv_file is not None:
            self._csv_file.flush()
            self._csv_file.close()

        self._current_date = today
        self._csv_path = self._audit_dir / f"{today}_audit.csv"

        # Check if file exists (for append vs create)
        file_exists = self._csv_path.exists()

        self._csv_file = open(self._csv_path, 'a', newline='', encoding='utf-8')
        self._csv_writer = csv.DictWriter(
            self._csv_file,
            fieldnames=JOURNAL_COLUMNS,
            extrasaction='ignore',
        )

        # Write header only for new files
        if not file_exists:
            self._csv_writer.writeheader()
            self._csv_file.flush()

        logger.info(f"AuditTrail: {'opened' if file_exists else 'created'} {self._csv_path}")

    def log(
        self,
        _event_type: str,
        _details: Optional[Dict[str, Any]] = None,
        _cycle_number: int = 0,
        _direction: str = "",
        _entry_price: Optional[float] = None,
        _stop_loss: Optional[float] = None,
        _take_profit: Optional[float] = None,
        _quantity: Optional[float] = None,
        _order_id: str = "",
        _fill_price: Optional[float] = None,
        _slippage: Optional[float] = None,
        _equity: Optional[float] = None,
        _balance: Optional[float] = None,
        _approved: Optional[bool] = None,
        _reason: str = "",
        _elapsed_ms: Optional[float] = None,
    ) -> None:
        """
        Append a record to the audit trail.

        Args:
            _event_type: Event type (SIGNAL, ORDER, FILL, RISK_CHECK, etc.).
            _details: Additional details as dict (serialized to JSON string).
            _cycle_number: Current cycle count.
            _direction: Trade direction (LONG, SHORT, or empty).
            _entry_price: Signal entry price or order price.
            _stop_loss: Stop loss price.
            _take_profit: Take profit price.
            _quantity: Order quantity in lots.
            _order_id: Broker-assigned order ID.
            _fill_price: Actual fill price.
            _slippage: fill_price - entry_price (signed).
            _equity: Account equity at event time.
            _balance: Account balance at event time.
            _approved: Risk/spread check result.
            _reason: Rejection reason or status message.
            _elapsed_ms: Timing in milliseconds.
        """
        # Rotate CSV if day changed
        self._rotate_csv()

        # Serialize details to JSON string
        import json
        details_str = json.dumps(_details) if _details else ""

        record = {
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'event_type': _event_type,
            'strategy': self._strategy,
            'symbol': self._symbol,
            'timeframe': self._timeframe,
            'cycle_number': _cycle_number,
            'direction': _direction,
            'entry_price': f"{_entry_price:.6f}" if _entry_price is not None else "",
            'stop_loss': f"{_stop_loss:.6f}" if _stop_loss is not None else "",
            'take_profit': f"{_take_profit:.6f}" if _take_profit is not None else "",
            'quantity': f"{_quantity:.4f}" if _quantity is not None else "",
            'order_id': _order_id,
            'fill_price': f"{_fill_price:.6f}" if _fill_price is not None else "",
            'slippage': f"{_slippage:.6f}" if _slippage is not None else "",
            'equity': f"{_equity:.2f}" if _equity is not None else "",
            'balance': f"{_balance:.2f}" if _balance is not None else "",
            'approved': str(_approved) if _approved is not None else "",
            'reason': _reason,
            'elapsed_ms': f"{_elapsed_ms:.1f}" if _elapsed_ms is not None else "",
            'details_json': details_str,
        }

        # Write to CSV immediately (crash-safe)
        self._csv_writer.writerow(record)
        self._record_count += 1
        self._unflushed_count += 1

        # Flush to disk based on interval
        if self._unflushed_count >= self._flush_interval:
            self._csv_file.flush()
            os.fsync(self._csv_file.fileno())
            self._unflushed_count = 0

        # Keep in memory buffer (for potential in-session queries)
        self._buffer.append(record)

    def flush_to_parquet(self, _date: Optional[str] = None) -> Optional[Path]:
        """
        Convert a day's CSV to parquet for efficient querying.

        Args:
            _date: Date string 'YYYY-MM-DD'. Defaults to current day.

        Returns:
            Path to created parquet file, or None if no data.
        """
        try:
            import pandas as pd
        except ImportError:
            logger.warning("AuditTrail: pandas not available, skipping parquet conversion")
            return None

        date_str = _date or self._current_date
        csv_path = self._audit_dir / f"{date_str}_audit.csv"

        if not csv_path.exists():
            logger.info(f"AuditTrail: no CSV for {date_str}, skipping parquet conversion")
            return None

        try:
            df = pd.read_csv(csv_path, dtype=str)

            if df.empty:
                logger.info(f"AuditTrail: empty CSV for {date_str}, skipping")
                return None

            parquet_path = self._audit_dir / f"{date_str}_audit.parquet"
            df.to_parquet(parquet_path, index=False, engine='pyarrow')

            logger.info(
                f"AuditTrail: converted {len(df)} records to {parquet_path}"
            )
            print(f"  [Journal] Converted {len(df)} records → {parquet_path}")
            return parquet_path

        except Exception as e:
            logger.error(f"AuditTrail: parquet conversion failed — {e}")
            print(f"  [Journal] WARNING: parquet conversion failed: {e}")
            return None

    def shutdown(self) -> None:
        """
        Clean shutdown: flush buffer, convert to parquet, close files.
        """
        # Flush any remaining records
        if self._csv_file is not None and not self._csv_file.closed:
            self._csv_file.flush()
            os.fsync(self._csv_file.fileno())

        # Log shutdown event
        self.log(
            _event_type="ENGINE",
            _reason="SHUTDOWN",
            _details={"total_records": self._record_count},
        )

        # Final flush
        if self._csv_file is not None and not self._csv_file.closed:
            self._csv_file.flush()
            os.fsync(self._csv_file.fileno())

        # Convert today's CSV to parquet
        self.flush_to_parquet()

        # Close CSV file
        if self._csv_file is not None and not self._csv_file.closed:
            self._csv_file.close()

        logger.info(
            f"AuditTrail: shutdown — {self._record_count} total records"
        )
        print(f"  [Journal] Shutdown — {self._record_count} total records written")

    @property
    def record_count(self) -> int:
        """Total records written this session."""
        return self._record_count

    @property
    def csv_path(self) -> Optional[Path]:
        """Current day's CSV file path."""
        return self._csv_path

    @property
    def audit_dir(self) -> Path:
        """Audit directory path."""
        return self._audit_dir
