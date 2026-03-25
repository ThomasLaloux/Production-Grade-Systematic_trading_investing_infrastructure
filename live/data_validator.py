"""
Data Validator Module
======================
Validates and updates historical data before live trading starts.

Checks:
    1. Recency: Is the most recent bar in storage from the current or
       previous trading session (not stale)?
    2. Warmup coverage: Is there enough history to satisfy
       strategy.get_warmup_period()?
    3. Forward-fill: Missing intraday bars are forward-filled (option 2).

Does NOT expect continuous bars — gaps from nights, weekends, holidays
are normal and expected.

Classes:
    DataValidator:
        - validate: check data recency and warmup, sync if needed
        - forward_fill_gaps: fill missing intraday bars

Usage:
    validator = DataValidator(
        _data_manager=data_manager, _max_gap_bars=5, _forward_fill_gaps=True,
    )
    df = validator.validate(
        _source_name="blackbull_mt5", _symbol="XAUUSDp",
        _timeframe="M15", _warmup_bars=500,
    )
"""

import logging
from datetime import datetime, timezone
from typing import Optional

import pandas as pd
import numpy as np

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

logger = logging.getLogger(__name__)


class DataValidator:
    """
    Validates historical data recency and warmup coverage.

    Ensures data is recent enough and has sufficient history for
    strategy indicator computation. Forward-fills missing intraday
    bars to maintain time series continuity.
    """

    def __init__(
        self,
        _data_manager: 'DataManager',
        _max_gap_bars: int,
        _forward_fill_gaps: bool,
    ):
        """
        Initialize DataValidator.

        Args:
            _data_manager: DataManager instance for data sync.
            _max_gap_bars: Max acceptable consecutive missing bars within a
                          session before a warning is logged.
            _forward_fill_gaps: If True, forward-fill missing intraday bars.
        """
        self._data_manager = _data_manager
        self._max_gap_bars = _max_gap_bars
        self._forward_fill_gaps = _forward_fill_gaps

    def validate(
        self,
        _source_name: str,
        _symbol: str,
        _timeframe: str,
        _warmup_bars: int,
    ) -> pd.DataFrame:
        """
        Validate data recency and warmup, sync if needed.

        Steps:
            1. Sync data with broker to get latest bars.
            2. Resample to target timeframe if needed.
            3. Check warmup coverage.
            4. Forward-fill gaps if enabled.

        Args:
            _source_name: Data source / broker name (e.g. "blackbull_mt5").
            _symbol: Broker symbol (e.g. "XAUUSDp").
            _timeframe: Target timeframe (e.g. "M15").
            _warmup_bars: Minimum bars required for strategy warmup.

        Returns:
            Validated and cleaned DataFrame with OHLCV data.

        Raises:
            ValueError: If warmup coverage is insufficient after sync.
        """
        logger.info(
            f"DataValidator: validating {_symbol} on {_source_name}, "
            f"TF={_timeframe}, warmup={_warmup_bars}"
        )
        print(f"  [DataValidator] Syncing {_symbol} from {_source_name}...")

        # Step 1: Sync data with broker (incremental update)
        self._data_manager.sync_data(
            _source_name=_source_name,
            _symbol=_symbol,
            _timeframe="M1",
            _run_quality_check=False,
        )

        # Step 2: Load and resample to target timeframe
        df = self._data_manager.get_ohlcv(
            _source_name=_source_name,
            _symbol=_symbol,
            _timeframe=_timeframe,
            _validate=False,
        )

        if df is None or df.empty:
            raise ValueError(
                f"DataValidator: no data available for {_symbol} on {_source_name}"
            )

        logger.info(
            f"DataValidator: loaded {len(df)} {_timeframe} bars, "
            f"range: {df['timestamp'].iloc[0]} to {df['timestamp'].iloc[-1]}"
        )
        print(f"  [DataValidator] Loaded {len(df)} {_timeframe} bars "
              f"({df['timestamp'].iloc[0]} to {df['timestamp'].iloc[-1]})")

        # Step 3: Check warmup coverage
        if len(df) < _warmup_bars:
            raise ValueError(
                f"DataValidator: insufficient data for warmup. "
                f"Have {len(df)} bars, need {_warmup_bars}. "
                f"Download more historical data first."
            )

        # Step 4: Forward-fill gaps if enabled
        if self._forward_fill_gaps:
            df = self.forward_fill_gaps(df, _timeframe)

        logger.info(
            f"DataValidator: validation complete. "
            f"{len(df)} bars available (warmup requires {_warmup_bars})"
        )
        print(f"  [DataValidator] Validation OK: {len(df)} bars "
              f"(warmup needs {_warmup_bars})")

        return df

    def forward_fill_gaps(
        self,
        _df: pd.DataFrame,
        _timeframe: str,
    ) -> pd.DataFrame:
        """
        Forward-fill missing intraday bars.

        For missing bars within a session, duplicate the last known close
        as OHLC (flat bar, volume = 0). This keeps the time series
        continuous for indicator computation without introducing
        artificial price movement.

        Args:
            _df: Input DataFrame with timestamp, open, high, low, close, volume.
            _timeframe: Timeframe string for expected frequency.

        Returns:
            DataFrame with gaps forward-filled.
        """
        if _df.empty or len(_df) < 2:
            return _df

        # Map timeframe to pandas frequency
        freq_map = {
            'M1': '1min', 'M5': '5min', 'M15': '15min',
            'M30': '30min', 'H1': '1h', 'H4': '4h', 'D1': '1D',
        }
        freq = freq_map.get(_timeframe)
        if freq is None:
            logger.warning(
                f"DataValidator: unsupported timeframe '{_timeframe}' for forward-fill"
            )
            return _df

        # Create expected time index
        df = _df.copy()
        df = df.set_index('timestamp')

        # Find gaps by checking time differences
        expected_delta = pd.Timedelta(freq)
        time_diffs = df.index.to_series().diff()

        # Only fill gaps that are small (< max_gap_bars * tf_duration)
        # Larger gaps (overnight, weekend) are left as-is — they're natural
        max_gap_duration = expected_delta * self._max_gap_bars
        gaps_to_fill = (time_diffs > expected_delta) & (time_diffs <= max_gap_duration)
        gaps_filled = gaps_to_fill.sum()

        if gaps_filled > 0:
            # Reindex to fill small gaps with forward-fill
            full_index = pd.date_range(
                start=df.index[0], end=df.index[-1], freq=freq,
            )
            # Only keep timestamps that fall within the range of actual data
            # (avoid creating bars in overnight/weekend gaps)
            df_reindexed = df.reindex(full_index)

            # Forward-fill OHLC values, set volume to 0 for filled bars
            filled_mask = df_reindexed['close'].isna()
            df_reindexed[['open', 'high', 'low', 'close']] = (
                df_reindexed[['open', 'high', 'low', 'close']].ffill()
            )
            df_reindexed.loc[filled_mask, 'volume'] = 0
            df_reindexed['volume'] = df_reindexed['volume'].fillna(0)

            # Drop rows that are still NaN (before first valid data point)
            df_reindexed = df_reindexed.dropna(subset=['close'])

            # Reset index
            df_reindexed = df_reindexed.reset_index()
            df_reindexed = df_reindexed.rename(columns={'index': 'timestamp'})

            filled_count = len(df_reindexed) - len(df)
            if filled_count > 0:
                logger.info(
                    f"DataValidator: forward-filled {filled_count} missing bars"
                )
                print(f"  [DataValidator] Forward-filled {filled_count} missing bars")

            return df_reindexed

        # No gaps to fill
        df = df.reset_index()
        return df
