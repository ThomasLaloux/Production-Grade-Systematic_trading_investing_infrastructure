"""
Yahoo Data Source Module
========================
Yahoo Finance data source implementation (data only, no trading).

Instance Creation:
    Implicit: Created by DataManager._get_data_source("yahoo") when
    download_data() or sync_data() is called with _source_name="yahoo".

Classes:
    DataSourceYahoo(DataSourceBase):
        - Yahoo Finance historical data loader via yfinance

Usage:
    # Typically accessed via DataManager, not directly:
    data_manager = DataManager(_config=config)
    df = data_manager.download_data(_source_name="yahoo", _symbol="EURUSD", ...)
    
    # Direct usage (less common):
    source = DataSourceYahoo(_config=config)
    df = source.load_historical_data(_symbol="EURUSD", _timeframe=Timeframe.H1, _start_date=dt1, _end_date=dt2)
"""

from datetime import datetime
from typing import List
import pandas as pd

from core.data_types import Timeframe
from core.exceptions import DataError
from .data_source_base import DataSourceBase

try:
    import yfinance as yf
    YFINANCE_AVAILABLE = True
except ImportError:
    YFINANCE_AVAILABLE = False


class DataSourceYahoo(DataSourceBase):
    """Yahoo Finance historical data loader."""
    
    INTERVAL_MAP = {
        Timeframe.M1: "1m",
        Timeframe.M5: "5m",
        Timeframe.M15: "15m",
        Timeframe.H1: "1h",
        Timeframe.D1: "1d",
        Timeframe.W1: "1wk",
        Timeframe.MN1: "1mo",
        Timeframe.MN3: "3mo",
    }
    
    @property
    def source_name(self) -> str:
        return "yahoo"
    
    def load_historical_data(
        self,
        _symbol: str,
        _timeframe: Timeframe,
        _start_date: datetime,
        _end_date: datetime,
    ) -> pd.DataFrame:
        """Load historical data from Yahoo Finance."""
        if not YFINANCE_AVAILABLE:
            raise DataError("yfinance package not installed. Run: pip install yfinance")
        
        yahoo_symbol = self._map_symbol(_symbol)
        interval = self.INTERVAL_MAP.get(_timeframe)
        
        if interval is None:
            if _timeframe == Timeframe.M3:
                return self._resample_m1_to_m3(_symbol, _start_date, _end_date)
            elif _timeframe == Timeframe.H4:
                return self._resample_m1_to_h4(_symbol, _start_date, _end_date)
            else:
                raise DataError(f"Unsupported timeframe: {_timeframe.value}")
        
        try:
            ticker = yf.Ticker(yahoo_symbol)
            df = ticker.history(start=_start_date, end=_end_date, interval=interval)
            
            if df.empty:
                return pd.DataFrame()
            
            return self._normalize_dataframe(df, _symbol, _timeframe.value)
        except Exception as e:
            raise DataError(f"Yahoo download failed: {e}", {"symbol": _symbol})
    
    def _resample_m1_to_m3(self, _symbol: str, _start_date: datetime, _end_date: datetime) -> pd.DataFrame:
        """Resample M1 data to M3."""
        df = self.load_historical_data(_symbol, Timeframe.M1, _start_date, _end_date)
        if df.empty:
            return df
        
        df = df.set_index("timestamp")
        resampled = df.resample("3min").agg({
            "open": "first", "high": "max", "low": "min", "close": "last", "volume": "sum"
        }).dropna()
        
        resampled["symbol"] = _symbol
        resampled["timeframe"] = "M3"
        return resampled.reset_index()
    
    def _resample_m1_to_h4(self, _symbol: str, _start_date: datetime, _end_date: datetime) -> pd.DataFrame:
        """Resample M1 data to H4."""
        df = self.load_historical_data(_symbol, Timeframe.M1, _start_date, _end_date)
        if df.empty:
            return df
        
        df = df.set_index("timestamp")
        resampled = df.resample("4h").agg({
            "open": "first", "high": "max", "low": "min", "close": "last", "volume": "sum"
        }).dropna()
        
        resampled["symbol"] = _symbol
        resampled["timeframe"] = "H4"
        return resampled.reset_index()
    
    def get_available_symbols(self) -> List[str]:
        """Get available symbols from config."""
        return list(self._symbol_mapping.keys())
    
    def get_supported_timeframes(self) -> List[Timeframe]:
        """Get supported timeframes (including derived via resampling)."""
        return [Timeframe.M1, Timeframe.M3, Timeframe.M5, Timeframe.M15, 
                Timeframe.H1, Timeframe.H4, Timeframe.D1, Timeframe.W1, Timeframe.MN1, Timeframe.MN3]
