"""
Shadow Mode Runner Module (P3.2)
==================================
Live validation / shadow mode — validates that rolling-window indicator
computation produces the same signals as full-history computation.

P1.1 Unification Note:
    With the unified _evaluate_bar() approach, backtest and live code paths
    are now IDENTICAL by construction — both call the same _evaluate_bar()
    method. The only remaining source of signal divergence is the rolling
    window effect on indicator computation (shorter history → different
    indicator warmup tail values).

    Therefore, the 'full' mode (which tested backtest vs live code path
    parity) has been removed — it is now guaranteed to pass by design.
    The 'rolling' mode is retained as the sole mode, testing the genuine
    concern of windowed indicator computation.

Architecture:
    The runner replays historical data through both code paths:

    BACKTEST PATH (reference):
        strategy.initialize(full_data) → strategy.generate_signals()
        → BacktestEngine._calculate_position_size() per signal

    LIVE-SIMULATION PATH (rolling window):
        For each bar from warmup to end:
            strategy.initialize(data[window]) → generate_signals(_mode='live')
            → PositionSizer.calculate() per signal

    The live-sim path mirrors exactly what LiveTradingEngine._run_cycle()
    does: reinitialize strategy with a rolling window of data, then
    evaluate signal at the last bar. This catches:
        - Rolling window edge effects on indicator computation
        - Position sizing differences between BacktestEngine's internal
          sizer and the live PositionSizer

Classes:
    ShadowRunner:
        - run: execute shadow mode comparison (rolling window only)
        - _run_backtest_path: collect backtest signals
        - _run_live_sim_rolling: collect live-sim signals
        - _compare: build parity report

Usage:
    from live import ShadowRunner

    runner = ShadowRunner(
        _strategy_class=SMACrossStrategy,
        _strategy_params={'_fast_period': 50, '_slow_period': 100,
                          '_atr_period': 30, '_sl_atr_mult': 1.5, '_rr': 20.0},
        _data=df,
        _instrument=instrument_metadata,
        _initial_capital=100000.0,
        _risk_pct=0.005,
        _commission=7.0,
        _slippage=0.0001,
    )
    report = runner.run()
    report.print_summary()
    report.print_details()
"""

import copy
import logging
import math
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple, Type

import numpy as np
import pandas as pd

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from strategies import StrategyBase, Signal, TradeDirection
from backtest import BacktestEngine
from core.data_types import InstrumentMetadata

from .position_sizer import PositionSizer
from .shadow_report import (
    ShadowParityReport, SignalRecord, PositionSizeRecord,
    ParityMismatch,
)

logger = logging.getLogger(__name__)


class ShadowRunner:
    """
    Shadow mode runner — validates backtest-live parity.

    Runs the same strategy on the same data through both the backtest
    and live code paths, then compares signals, SL/TP, and position
    sizes.
    """

    def __init__(
        self,
        _strategy_class: Type[StrategyBase],
        _strategy_params: Dict[str, Any],
        _data: pd.DataFrame,
        _instrument: Optional[InstrumentMetadata] = None,
        _initial_capital: float = 100000.0,
        _risk_pct: float = 0.005,
        _commission: float = 7.0,
        _slippage: float = 0.0001,
        _contract_multiplier: float = 100000.0,
        _history_window: int = 5000,
        _position_size_tolerance: float = 0.0,
    ):
        """
        Initialize ShadowRunner.

        Args:
            _strategy_class: Strategy class (not instance) to test.
            _strategy_params: Strategy constructor kwargs (without _mode).
            _data: Full OHLCV DataFrame for the test period.
            _instrument: InstrumentMetadata for position sizing. When
                provided, both engines use instrument-based sizing.
            _initial_capital: Starting capital for both engines.
            _risk_pct: Risk per trade as fraction of capital.
            _commission: Commission per round-turn in quote currency.
            _slippage: Slippage as fraction of price.
            _contract_multiplier: Contract multiplier (used when
                _instrument is None).
            _history_window: Rolling window size for live-sim mode.
                Set to 0 or len(data) for no windowing.
            _position_size_tolerance: Tolerance for position size
                comparison (in lots). 0.0 = exact match required.
        """
        self._strategy_class = _strategy_class
        self._strategy_params = _strategy_params
        self._data = _data.copy()
        self._instrument = _instrument
        self._initial_capital = _initial_capital
        self._risk_pct = _risk_pct
        self._commission = _commission
        self._slippage = _slippage
        self._contract_multiplier = _contract_multiplier
        self._history_window = _history_window
        self._position_size_tolerance = _position_size_tolerance

        # Ensure timestamps are datetime
        if not pd.api.types.is_datetime64_any_dtype(self._data['timestamp']):
            self._data['timestamp'] = pd.to_datetime(self._data['timestamp'])

        # Create live-mode PositionSizer (only when instrument available)
        self._live_sizer: Optional[PositionSizer] = None
        if self._instrument is not None:
            self._live_sizer = PositionSizer(
                _instrument_metadata=self._instrument,
            )

    def run(self, _mode: str = 'rolling') -> ShadowParityReport:
        """
        Execute shadow mode comparison (rolling window only).

        Tests whether the rolling-window indicator computation in the
        live pipeline produces the same signals as full-history backtest.

        With the P1.1 unified _evaluate_bar() approach, backtest/live
        code path parity is guaranteed by construction. The only
        remaining divergence source is windowed indicator computation,
        which this method tests.

        Args:
            _mode: Must be 'rolling' (the only supported mode after P1.1).
                   Accepted for backward compatibility.

        Returns:
            ShadowParityReport with parity verdict and details.
        """
        if _mode not in ('rolling',):
            logger.warning(
                f"ShadowRunner: mode '{_mode}' is deprecated after P1.1 "
                f"unification. Using 'rolling' (the only remaining mode)."
            )
            _mode = 'rolling'

        print(f"\n{'='*70}")
        print(f"SHADOW MODE — Backtest-Live Parity Validation")
        print(f"{'='*70}")
        print(f"  Strategy:       {self._strategy_class.__name__}")
        print(f"  Data bars:      {len(self._data)}")
        print(f"  Mode:           {_mode}")
        print(f"  Capital:        {self._initial_capital:,.2f}")
        print(f"  Risk/trade:     {self._risk_pct*100:.2f}%")
        if self._instrument is not None:
            print(f"  Instrument:     {self._instrument.symbol} "
                  f"(pip={self._instrument.pip_size}, "
                  f"lot_step={self._instrument.lot_step})")
        print(f"  History window: {self._history_window}")
        print(f"{'='*70}\n")

        # --- Regime computed by strategy.initialize() ---
        # Phase 8.4: Strategy computes regime internally during initialize().
        # No external regime model needed.
        data = self._data.copy()

        # --- Run backtest path ---
        t0 = time.perf_counter()
        bt_signals = self._run_backtest_path(data)
        t_bt = time.perf_counter() - t0
        print(f"  [Backtest path] {len(bt_signals)} signals "
              f"({t_bt*1000:.0f}ms)")

        # --- Run live-sim path (rolling window) ---
        t0 = time.perf_counter()
        live_signals = self._run_live_sim_rolling(data)
        t_live = time.perf_counter() - t0
        print(f"  [Live-sim path] {len(live_signals)} signals "
              f"({t_live*1000:.0f}ms)")

        # --- Compare ---
        report = self._compare(bt_signals, live_signals, data)

        return report

    def _run_backtest_path(
        self, _data: pd.DataFrame,
    ) -> List[Tuple[SignalRecord, float]]:
        """
        Run the backtest path and collect signals + position sizes.

        Returns:
            List of (SignalRecord, position_quantity) tuples.
        """
        # Create strategy instance in backtest mode
        params = self._strategy_params.copy()
        params['_mode'] = 'backtest'
        strategy = self._strategy_class(**params)

        # Create backtest engine
        engine = BacktestEngine(
            _initial_capital=self._initial_capital,
            _commission=self._commission,
            _slippage=self._slippage,
            _risk_pct=self._risk_pct,
            _contract_multiplier=self._contract_multiplier,
            _instrument=self._instrument,
        )

        # We need signals AND position sizes. The backtest engine
        # processes signals internally and computes position sizes per
        # signal. To extract both, we:
        # 1. Generate signals from the strategy
        # 2. For each signal, compute position size using the engine's
        #    logic (replicating _calculate_position_size)
        # 3. Return signal + size pairs

        strategy.initialize(_data)
        signals = strategy.generate_signals()

        results = []
        capital = self._initial_capital

        for sig in signals:
            # Create signal record
            record = SignalRecord(
                timestamp=sig.timestamp,
                direction=sig.direction.name,
                entry_price=sig.entry_price,
                stop_loss=sig.stop_loss,
                take_profit=sig.take_profit,
                bar_index=sig.metadata.get('bar_index'),
                metadata=sig.metadata.copy(),
            )

            # Calculate position size using backtest logic
            quantity = self._calculate_bt_position_size(
                _signal=sig, _capital=capital,
            )

            results.append((record, quantity))

            # Note: we do NOT update capital here because the backtest
            # engine's sizing depends on running capital. For shadow
            # mode, we compare at initial capital to isolate the sizing
            # formula difference. If strict capital-tracked sizing is
            # needed, run the full BacktestEngine and extract from trades.

        return results

    def _run_live_sim_rolling(
        self, _data: pd.DataFrame,
    ) -> List[Tuple[SignalRecord, float]]:
        """
        Run the live simulation path in rolling-window mode.

        Re-initializes strategy with a windowed DataFrame for each
        bar, exactly as the LiveTradingEngine does. Tests windowing
        effects on indicators.

        Uses generate_signals(_mode='live') — the unified API that
        evaluates only the last bar (O(1) signal logic).

        Returns:
            List of (SignalRecord, position_quantity) tuples.
        """
        params = self._strategy_params.copy()
        params['_mode'] = 'live'

        warmup_strategy = self._strategy_class(**params.copy())
        warmup_strategy.initialize(_data)
        warmup = warmup_strategy.get_warmup_period()

        window = self._history_window
        if window <= 0 or window >= len(_data):
            window = len(_data)

        results = []
        n_bars = len(_data)

        # Progress tracking
        report_interval = max(1, n_bars // 20)

        for i in range(warmup, n_bars):
            # Build windowed data (same as LiveTradingEngine._trim_to_window)
            start_idx = max(0, i + 1 - window)
            window_data = _data.iloc[start_idx:i + 1].reset_index(drop=True)

            # Skip if window too small for warmup
            if len(window_data) < warmup:
                continue

            # Create fresh strategy instance and initialize with window
            strategy = self._strategy_class(**params.copy())

            # Apply regime if needed
            # Phase 8.4: regime is computed by strategy.initialize()
            # No external regime model application needed here.

            strategy.initialize(window_data)

            # Evaluate signal at last bar using unified API
            live_signals = strategy.generate_signals(_mode='live')
            signal = live_signals[0] if live_signals else None

            if signal is not None:
                # Map window-local timestamp to original data timestamp
                original_ts = _data['timestamp'].iloc[i]

                record = SignalRecord(
                    timestamp=original_ts,
                    direction=signal.direction.name,
                    entry_price=signal.entry_price,
                    stop_loss=signal.stop_loss,
                    take_profit=signal.take_profit,
                    bar_index=i,
                    metadata=signal.metadata.copy(),
                )

                quantity = self._calculate_live_position_size(
                    _signal=signal,
                    _equity=self._initial_capital,
                )

                results.append((record, quantity))

            # Progress
            if (i - warmup) % report_interval == 0:
                pct = (i - warmup) / max(1, n_bars - warmup) * 100
                print(f"    [Rolling] {pct:.0f}% ({i}/{n_bars} bars)...")

        return results

    def _calculate_bt_position_size(
        self,
        _signal: Signal,
        _capital: float,
    ) -> float:
        """
        Replicate BacktestEngine._calculate_position_size() logic.

        This is a standalone copy of the backtest sizing formula so
        shadow mode doesn't need to run the full backtest engine.

        Args:
            _signal: Trading signal with entry_price and stop_loss.
            _capital: Current capital for sizing.

        Returns:
            Position size in lots.
        """
        if _signal.stop_loss is None:
            return 0.01

        sl_distance = abs(_signal.entry_price - _signal.stop_loss)
        if sl_distance == 0:
            return 0.01

        risk_amount = _capital * self._risk_pct

        if self._instrument is not None:
            pip_value_per_lot = self._instrument.calculate_pip_value(1.0)
            sl_pips = sl_distance / self._instrument.pip_size
            quantity = risk_amount / (sl_pips * pip_value_per_lot)

            # Round to lot step (backtest uses np.floor)
            quantity = np.floor(quantity / self._instrument.lot_step) * self._instrument.lot_step
            quantity = max(self._instrument.min_lot_size,
                          min(quantity, self._instrument.max_lot_size))
        else:
            quantity = risk_amount / (sl_distance * self._contract_multiplier)
            quantity = round(quantity, 2)
            quantity = max(0.01, min(quantity, 100.0))

        return quantity

    def _calculate_live_position_size(
        self,
        _signal: Signal,
        _equity: float,
    ) -> float:
        """
        Calculate position size using the live PositionSizer.

        Args:
            _signal: Trading signal with entry_price and stop_loss.
            _equity: Current equity for sizing.

        Returns:
            Position size in lots.
        """
        if self._live_sizer is not None and _signal.stop_loss is not None:
            return self._live_sizer.calculate(
                _equity=_equity,
                _risk_pct=self._risk_pct,
                _entry_price=_signal.entry_price,
                _stop_loss=_signal.stop_loss,
            )

        # Fallback: use the same formula as backtest (no instrument)
        return self._calculate_bt_position_size(
            _signal=_signal, _capital=_equity,
        )

    def _compare(
        self,
        _bt_signals: List[Tuple[SignalRecord, float]],
        _live_signals: List[Tuple[SignalRecord, float]],
        _data: pd.DataFrame,
    ) -> ShadowParityReport:
        """
        Compare backtest and live-sim signals and build parity report.

        Matching is by timestamp. Signals at the same timestamp are
        paired. Orphan signals (present in one but not the other) are
        flagged.

        Args:
            _bt_signals: Backtest (SignalRecord, quantity) pairs.
            _live_signals: Live-sim (SignalRecord, quantity) pairs.
            _data: Original data (for reference).

        Returns:
            Finalized ShadowParityReport.
        """
        report = ShadowParityReport(
            _position_size_tolerance=self._position_size_tolerance,
        )

        # Index by timestamp
        bt_by_ts = {rec.timestamp: (rec, qty) for rec, qty in _bt_signals}
        live_by_ts = {rec.timestamp: (rec, qty) for rec, qty in _live_signals}

        all_timestamps = sorted(set(bt_by_ts.keys()) | set(live_by_ts.keys()))

        for ts in all_timestamps:
            bt_entry = bt_by_ts.get(ts)
            live_entry = live_by_ts.get(ts)

            if bt_entry is not None and live_entry is not None:
                bt_rec, bt_qty = bt_entry
                live_rec, live_qty = live_entry

                # Signal comparison
                report.add_signal_pair(bt_rec, live_rec)

                # Position size comparison
                # Determine tolerance: use lot_step if instrument available,
                # otherwise use configured tolerance
                tolerance = self._position_size_tolerance
                if tolerance == 0.0 and self._instrument is not None:
                    # Allow exactly one lot_step of rounding tolerance
                    tolerance = self._instrument.lot_step

                report.add_position_size_pair(
                    _timestamp=ts,
                    _bt_quantity=bt_qty,
                    _live_quantity=live_qty,
                    _tolerance=tolerance,
                )

            elif bt_entry is not None:
                bt_rec, _ = bt_entry
                report.add_backtest_only(bt_rec)

            else:
                live_rec, _ = live_entry
                report.add_live_only(live_rec)

        report.finalize(
            _total_bt_signals=len(_bt_signals),
            _total_live_signals=len(_live_signals),
        )

        return report
