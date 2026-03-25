"""
Shadow Mode Parity Report Module (P3.2)
=========================================
Comparison logic and reporting for backtest-vs-live signal parity.

Compares three dimensions of parity:
    1. Signals: timestamp, direction, entry price (exact match)
    2. SL/TP levels: stop loss and take profit (exact match)
    3. Position sizes: lot quantity (match after rounding)

Classes:
    SignalRecord
        - snapshot of a signal for comparison
    PositionSizeRecord
        - snapshot of a position size calculation
    ParityMismatch
        - single divergence between backtest and live paths
    ShadowParityReport
        - full parity report with pass/fail verdict
        - add_signal_pair, add_position_size_pair, add_backtest_only,
          add_live_only, finalize, print_summary, print_details,
          to_dict, to_dataframe

Usage:
    report = ShadowParityReport()
    report.add_signal_pair(bt_signal, live_signal)
    report.add_position_size_pair(timestamp, bt_qty, live_qty, tolerance=0.01)
    report.add_backtest_only(bt_signal)
    report.add_live_only(live_signal)
    report.finalize()
    report.print_summary()
    report.print_details()
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple
import pandas as pd


@dataclass
class SignalRecord:
    """Snapshot of a signal for comparison."""
    timestamp: datetime
    direction: str        # "LONG" or "SHORT"
    entry_price: float
    stop_loss: Optional[float] = None
    take_profit: Optional[float] = None
    bar_index: Optional[int] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            'timestamp': self.timestamp,
            'direction': self.direction,
            'entry_price': self.entry_price,
            'stop_loss': self.stop_loss,
            'take_profit': self.take_profit,
            'bar_index': self.bar_index,
        }


@dataclass
class PositionSizeRecord:
    """Snapshot of a position size calculation for comparison."""
    timestamp: datetime
    backtest_quantity: float
    live_quantity: float
    match: bool
    difference: float
    difference_pct: float


@dataclass
class ParityMismatch:
    """Single divergence between backtest and live paths."""
    timestamp: datetime
    field: str              # "direction", "entry_price", "stop_loss", "take_profit", "position_size", "signal_missing"
    backtest_value: Any
    live_value: Any
    severity: str           # "CRITICAL", "WARNING", "INFO"
    description: str


class ShadowParityReport:
    """
    Full parity report for shadow mode validation.

    Collects signal pairs, position size pairs, and orphan signals
    (present in one engine but not the other). Produces a pass/fail
    verdict with detailed divergence listing.

    Parity rules:
        - Signals: direction and entry_price must match exactly.
        - SL/TP: must match exactly (both come from the same
          calculate_sl_tp call in the strategy).
        - Position sizes: must match after lot-step rounding.
          A configurable tolerance (default: lot_step) is applied.
    """

    def __init__(self, _position_size_tolerance: float = 0.0):
        """
        Initialize parity report.

        Args:
            _position_size_tolerance: Absolute tolerance for position
                size comparison. 0.0 = exact match after rounding.
                Set to lot_step for rounding-only tolerance.
        """
        self._position_size_tolerance = _position_size_tolerance

        # Matched signal pairs
        self._signal_pairs: List[Tuple[SignalRecord, SignalRecord]] = []
        # Position size pairs
        self._position_sizes: List[PositionSizeRecord] = []
        # Orphan signals
        self._backtest_only: List[SignalRecord] = []
        self._live_only: List[SignalRecord] = []
        # Mismatches
        self._mismatches: List[ParityMismatch] = []

        # Summary counters
        self._total_bt_signals = 0
        self._total_live_signals = 0
        self._direction_matches = 0
        self._entry_price_matches = 0
        self._sl_matches = 0
        self._tp_matches = 0
        self._position_size_matches = 0
        self._is_finalized = False

    def add_signal_pair(
        self,
        _bt_signal: SignalRecord,
        _live_signal: SignalRecord,
    ) -> None:
        """
        Add a matched signal pair (same timestamp) for comparison.

        Args:
            _bt_signal: Signal from backtest engine.
            _live_signal: Signal from live engine simulation.
        """
        self._signal_pairs.append((_bt_signal, _live_signal))

        # Compare direction
        if _bt_signal.direction == _live_signal.direction:
            self._direction_matches += 1
        else:
            self._mismatches.append(ParityMismatch(
                timestamp=_bt_signal.timestamp,
                field="direction",
                backtest_value=_bt_signal.direction,
                live_value=_live_signal.direction,
                severity="CRITICAL",
                description=(
                    f"Direction mismatch: backtest={_bt_signal.direction}, "
                    f"live={_live_signal.direction}"
                ),
            ))

        # Compare entry price
        if abs(_bt_signal.entry_price - _live_signal.entry_price) < 1e-10:
            self._entry_price_matches += 1
        else:
            self._mismatches.append(ParityMismatch(
                timestamp=_bt_signal.timestamp,
                field="entry_price",
                backtest_value=_bt_signal.entry_price,
                live_value=_live_signal.entry_price,
                severity="CRITICAL",
                description=(
                    f"Entry price mismatch: backtest={_bt_signal.entry_price:.6f}, "
                    f"live={_live_signal.entry_price:.6f}"
                ),
            ))

        # Compare stop loss
        bt_sl = _bt_signal.stop_loss
        live_sl = _live_signal.stop_loss
        if bt_sl is None and live_sl is None:
            self._sl_matches += 1
        elif bt_sl is not None and live_sl is not None and abs(bt_sl - live_sl) < 1e-10:
            self._sl_matches += 1
        else:
            self._mismatches.append(ParityMismatch(
                timestamp=_bt_signal.timestamp,
                field="stop_loss",
                backtest_value=bt_sl,
                live_value=live_sl,
                severity="CRITICAL",
                description=(
                    f"Stop loss mismatch: backtest={bt_sl}, "
                    f"live={live_sl}"
                ),
            ))

        # Compare take profit
        bt_tp = _bt_signal.take_profit
        live_tp = _live_signal.take_profit
        if bt_tp is None and live_tp is None:
            self._tp_matches += 1
        elif bt_tp is not None and live_tp is not None and abs(bt_tp - live_tp) < 1e-10:
            self._tp_matches += 1
        else:
            self._mismatches.append(ParityMismatch(
                timestamp=_bt_signal.timestamp,
                field="take_profit",
                backtest_value=bt_tp,
                live_value=live_tp,
                severity="CRITICAL",
                description=(
                    f"Take profit mismatch: backtest={bt_tp}, "
                    f"live={live_tp}"
                ),
            ))

    def add_position_size_pair(
        self,
        _timestamp: datetime,
        _bt_quantity: float,
        _live_quantity: float,
        _tolerance: Optional[float] = None,
    ) -> None:
        """
        Add a position size pair for comparison.

        Args:
            _timestamp: Signal timestamp.
            _bt_quantity: Position size from backtest engine.
            _live_quantity: Position size from live position sizer.
            _tolerance: Override tolerance for this pair.
        """
        tolerance = _tolerance if _tolerance is not None else self._position_size_tolerance
        diff = abs(_bt_quantity - _live_quantity)
        diff_pct = (diff / _bt_quantity * 100) if _bt_quantity > 0 else 0.0
        match = diff <= tolerance

        self._position_sizes.append(PositionSizeRecord(
            timestamp=_timestamp,
            backtest_quantity=_bt_quantity,
            live_quantity=_live_quantity,
            match=match,
            difference=diff,
            difference_pct=diff_pct,
        ))

        if match:
            self._position_size_matches += 1
        else:
            severity = "WARNING" if diff_pct < 5.0 else "CRITICAL"
            self._mismatches.append(ParityMismatch(
                timestamp=_timestamp,
                field="position_size",
                backtest_value=_bt_quantity,
                live_value=_live_quantity,
                severity=severity,
                description=(
                    f"Position size mismatch: backtest={_bt_quantity:.4f}, "
                    f"live={_live_quantity:.4f}, diff={diff:.4f} ({diff_pct:.2f}%)"
                ),
            ))

    def add_backtest_only(self, _signal: SignalRecord) -> None:
        """Add a signal present in backtest but missing from live."""
        self._backtest_only.append(_signal)
        self._mismatches.append(ParityMismatch(
            timestamp=_signal.timestamp,
            field="signal_missing",
            backtest_value=_signal.direction,
            live_value=None,
            severity="CRITICAL",
            description=(
                f"Signal in BACKTEST only: {_signal.direction} @ "
                f"{_signal.entry_price:.6f} (bar={_signal.bar_index})"
            ),
        ))

    def add_live_only(self, _signal: SignalRecord) -> None:
        """Add a signal present in live but missing from backtest."""
        self._live_only.append(_signal)
        self._mismatches.append(ParityMismatch(
            timestamp=_signal.timestamp,
            field="signal_missing",
            backtest_value=None,
            live_value=_signal.direction,
            severity="CRITICAL",
            description=(
                f"Signal in LIVE only: {_signal.direction} @ "
                f"{_signal.entry_price:.6f} (bar={_signal.bar_index})"
            ),
        ))

    def finalize(
        self,
        _total_bt_signals: int,
        _total_live_signals: int,
    ) -> None:
        """
        Finalize the report with total signal counts.

        Args:
            _total_bt_signals: Total signals from backtest.
            _total_live_signals: Total signals from live simulation.
        """
        self._total_bt_signals = _total_bt_signals
        self._total_live_signals = _total_live_signals
        self._is_finalized = True

    @property
    def is_pass(self) -> bool:
        """Overall parity verdict: True if no CRITICAL mismatches."""
        if not self._is_finalized:
            return False
        critical = [m for m in self._mismatches if m.severity == "CRITICAL"]
        return len(critical) == 0

    @property
    def verdict(self) -> str:
        """Human-readable verdict string."""
        if not self._is_finalized:
            return "NOT_FINALIZED"
        return "PASS" if self.is_pass else "FAIL"

    @property
    def total_mismatches(self) -> int:
        return len(self._mismatches)

    @property
    def critical_mismatches(self) -> int:
        return len([m for m in self._mismatches if m.severity == "CRITICAL"])

    @property
    def warning_mismatches(self) -> int:
        return len([m for m in self._mismatches if m.severity == "WARNING"])

    def print_summary(self) -> None:
        """Print compact parity summary to terminal."""
        n_pairs = len(self._signal_pairs)
        n_pos = len(self._position_sizes)

        print(f"\n{'='*70}")
        print(f"SHADOW MODE PARITY REPORT — {self.verdict}")
        print(f"{'='*70}")
        print(f"  Backtest signals:  {self._total_bt_signals}")
        print(f"  Live signals:      {self._total_live_signals}")
        print(f"  Matched pairs:     {n_pairs}")
        print(f"  Backtest-only:     {len(self._backtest_only)}")
        print(f"  Live-only:         {len(self._live_only)}")
        print(f"{'─'*70}")

        if n_pairs > 0:
            print(f"  SIGNAL PARITY:")
            print(f"    Direction:       {self._direction_matches}/{n_pairs} "
                  f"({'PASS' if self._direction_matches == n_pairs else 'FAIL'})")
            print(f"    Entry price:     {self._entry_price_matches}/{n_pairs} "
                  f"({'PASS' if self._entry_price_matches == n_pairs else 'FAIL'})")
            print(f"    Stop loss:       {self._sl_matches}/{n_pairs} "
                  f"({'PASS' if self._sl_matches == n_pairs else 'FAIL'})")
            print(f"    Take profit:     {self._tp_matches}/{n_pairs} "
                  f"({'PASS' if self._tp_matches == n_pairs else 'FAIL'})")

        if n_pos > 0:
            print(f"  POSITION SIZE PARITY:")
            print(f"    Matches:         {self._position_size_matches}/{n_pos} "
                  f"({'PASS' if self._position_size_matches == n_pos else 'FAIL'})")
            if self._position_size_tolerance > 0:
                print(f"    Tolerance:       {self._position_size_tolerance:.4f} lots")

        print(f"{'─'*70}")
        print(f"  MISMATCHES:")
        print(f"    Critical:        {self.critical_mismatches}")
        print(f"    Warning:         {self.warning_mismatches}")
        print(f"{'─'*70}")
        print(f"  VERDICT:           {self.verdict}")
        print(f"{'='*70}\n")

    def print_details(self, _max_mismatches: int = 50) -> None:
        """
        Print detailed mismatch list.

        Args:
            _max_mismatches: Maximum mismatches to print (default 50).
        """
        if not self._mismatches:
            print("  No mismatches found — full parity confirmed.")
            return

        print(f"\n--- Mismatch Details (showing up to {_max_mismatches}) ---")
        for i, m in enumerate(self._mismatches[:_max_mismatches]):
            print(f"  [{m.severity}] {m.timestamp} | {m.field}: {m.description}")

        remaining = len(self._mismatches) - _max_mismatches
        if remaining > 0:
            print(f"  ... and {remaining} more mismatches (use to_dataframe() for full list)")

    def to_dict(self) -> Dict[str, Any]:
        """Serialize report to dictionary."""
        return {
            'verdict': self.verdict,
            'is_pass': self.is_pass,
            'total_bt_signals': self._total_bt_signals,
            'total_live_signals': self._total_live_signals,
            'matched_pairs': len(self._signal_pairs),
            'backtest_only': len(self._backtest_only),
            'live_only': len(self._live_only),
            'direction_matches': self._direction_matches,
            'entry_price_matches': self._entry_price_matches,
            'sl_matches': self._sl_matches,
            'tp_matches': self._tp_matches,
            'position_size_matches': self._position_size_matches,
            'total_position_size_checks': len(self._position_sizes),
            'total_mismatches': self.total_mismatches,
            'critical_mismatches': self.critical_mismatches,
            'warning_mismatches': self.warning_mismatches,
            'position_size_tolerance': self._position_size_tolerance,
        }

    def to_dataframe(self) -> pd.DataFrame:
        """
        Export all mismatches as a DataFrame for analysis.

        Returns:
            DataFrame with columns: timestamp, field, backtest_value,
            live_value, severity, description.
        """
        if not self._mismatches:
            return pd.DataFrame(columns=[
                'timestamp', 'field', 'backtest_value',
                'live_value', 'severity', 'description',
            ])

        rows = []
        for m in self._mismatches:
            rows.append({
                'timestamp': m.timestamp,
                'field': m.field,
                'backtest_value': m.backtest_value,
                'live_value': m.live_value,
                'severity': m.severity,
                'description': m.description,
            })
        return pd.DataFrame(rows)

    def get_position_size_dataframe(self) -> pd.DataFrame:
        """
        Export all position size comparisons as a DataFrame.

        Returns:
            DataFrame with columns: timestamp, backtest_quantity,
            live_quantity, match, difference, difference_pct.
        """
        if not self._position_sizes:
            return pd.DataFrame(columns=[
                'timestamp', 'backtest_quantity', 'live_quantity',
                'match', 'difference', 'difference_pct',
            ])

        rows = []
        for ps in self._position_sizes:
            rows.append({
                'timestamp': ps.timestamp,
                'backtest_quantity': ps.backtest_quantity,
                'live_quantity': ps.live_quantity,
                'match': ps.match,
                'difference': ps.difference,
                'difference_pct': ps.difference_pct,
            })
        return pd.DataFrame(rows)
