"""
Timeframe Utilities Module
==========================
Utilities for timeframe handling and multi-timeframe (MTF) support.

Classes:
    TimeframeManager:
        - timeframe operations and MTF alignment with look-ahead prevention
        - resample_ohlcv
        - align_mtf_data
        - get_higher_timeframes
        - get_lower_timeframes
        - validate_mtf_request
        - calculate_bars_per_higher_tf
        - get_current_htf_bar_start

Usage:
    resampled = TimeframeManager.resample_ohlcv(df, Timeframe.M1, Timeframe.H1)
    aligned = TimeframeManager.align_mtf_data(base_df, htf_df, Timeframe.H1, Timeframe.H4)
    higher = TimeframeManager.get_higher_timeframes(Timeframe.H1)
    lower = TimeframeManager.get_lower_timeframes(Timeframe.H1)
    valid, err = TimeframeManager.validate_mtf_request(Timeframe.H1, [Timeframe.H4, Timeframe.D1])
    bars = TimeframeManager.calculate_bars_per_higher_tf(Timeframe.M1, Timeframe.H1)
    start = TimeframeManager.get_current_htf_bar_start(datetime.now(), Timeframe.H4)
"""

from pathlib import Path
from datetime import datetime, timedelta
from typing import List, Optional, Tuple
import pandas as pd

import sys
sys.path.insert(0, str(Path(__file__).parent.parent))

from core.data_types import Timeframe
from core.exceptions import DataError


class TimeframeManager:
    """Timeframe operations and MTF alignment with look-ahead prevention."""
    
    @classmethod
    def resample_ohlcv(cls, _data: pd.DataFrame, _source_tf: Timeframe, _target_tf: Timeframe) -> pd.DataFrame:
        """Resample OHLCV to higher timeframe."""
        if _target_tf.to_minutes() < _source_tf.to_minutes():
            raise DataError("Cannot resample to lower timeframe", {"source": _source_tf.value, "target": _target_tf.value})
        
        if _target_tf == _source_tf:
            return _data.copy()
        
        df = _data.copy()
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        df = df.set_index("timestamp")
        
        resampled = df.resample(_target_tf.to_pandas_freq(), label="left", closed="left").agg({
            "open": "first", "high": "max", "low": "min", "close": "last", "volume": "sum",
        }).dropna()
        
        return resampled.reset_index()
    
    @classmethod
    def align_mtf_data(
        cls,
        _base_data: pd.DataFrame,
        _higher_tf_data: pd.DataFrame,
        _base_tf: Timeframe,
        _higher_tf: Timeframe,
        _prevent_lookahead: bool = True,
    ) -> pd.DataFrame:
        """Align higher TF data with base TF, preventing look-ahead bias."""
        base_df = _base_data.copy()
        htf_df = _higher_tf_data.copy()
        
        base_df["timestamp"] = pd.to_datetime(base_df["timestamp"])
        htf_df["timestamp"] = pd.to_datetime(htf_df["timestamp"])
        
        base_df = base_df.sort_values("timestamp").reset_index(drop=True)
        htf_df = htf_df.sort_values("timestamp").reset_index(drop=True)
        
        htf_prefix = f"htf_{_higher_tf.value}_"
        htf_renamed = htf_df.rename(columns={
            "open": f"{htf_prefix}open", "high": f"{htf_prefix}high",
            "low": f"{htf_prefix}low", "close": f"{htf_prefix}close", "volume": f"{htf_prefix}volume",
        })
        
        if _prevent_lookahead:
            htf_renamed["timestamp"] = htf_renamed["timestamp"] + timedelta(minutes=_higher_tf.to_minutes())
        
        return pd.merge_asof(base_df, htf_renamed, on="timestamp", direction="backward", suffixes=("", "_htf"))
    
    @classmethod
    def get_higher_timeframes(cls, _base_tf: Timeframe) -> List[Timeframe]:
        """Get all timeframes higher than given one."""
        return [tf for tf in Timeframe if tf.to_minutes() > _base_tf.to_minutes()]
    
    @classmethod
    def get_lower_timeframes(cls, _base_tf: Timeframe) -> List[Timeframe]:
        """Get all timeframes lower than given one."""
        return sorted([tf for tf in Timeframe if tf.to_minutes() < _base_tf.to_minutes()],
                      key=lambda x: x.to_minutes(), reverse=True)
    
    @classmethod
    def validate_mtf_request(cls, _base_tf: Timeframe, _requested_tfs: List[Timeframe]) -> Tuple[bool, Optional[str]]:
        """Validate MTF config."""
        if len(_requested_tfs) != len(set(_requested_tfs)):
            return False, "Duplicate timeframes in request"
        
        base_minutes = _base_tf.to_minutes()
        for tf in _requested_tfs:
            tf_minutes = tf.to_minutes()
            if tf_minutes <= base_minutes:
                return False, f"{tf.value} is not higher than base {_base_tf.value}"
            if tf_minutes % base_minutes != 0:
                return False, f"{tf.value} is not a clean multiple of {_base_tf.value}"
        
        return True, None
    
    @classmethod
    def calculate_bars_per_higher_tf(cls, _base_tf: Timeframe, _higher_tf: Timeframe) -> int:
        """Calculate how many base TF bars fit in one higher TF bar."""
        return _higher_tf.to_minutes() // _base_tf.to_minutes()
    
    @classmethod
    def get_current_htf_bar_start(cls, _timestamp: datetime, _higher_tf: Timeframe) -> datetime:
        """Get start time of the current higher TF bar."""
        if _higher_tf == Timeframe.D1:
            return _timestamp.replace(hour=0, minute=0, second=0, microsecond=0)
        elif _higher_tf == Timeframe.W1:
            days_since_monday = _timestamp.weekday()
            return (_timestamp - timedelta(days=days_since_monday)).replace(hour=0, minute=0, second=0, microsecond=0)
        elif _higher_tf in (Timeframe.MN1, Timeframe.MN3):
            return _timestamp.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        else:
            minutes = _higher_tf.to_minutes()
            total_minutes = _timestamp.hour * 60 + _timestamp.minute
            bar_start_minutes = (total_minutes // minutes) * minutes
            return _timestamp.replace(hour=bar_start_minutes // 60, minute=bar_start_minutes % 60, second=0, microsecond=0)