"""
Data Source Base Module
=======================
Abstract base class for all data source implementations.

Classes:
    DataSourceBase(ABC):
        - abstract base class for data source implementations with shared logic
        - load_historical_data (abstract)
        - get_available_symbols (abstract)
        - get_supported_timeframes (abstract)
        - source_name (property, abstract)

Usage:
    # DataSourceBase is inherited by: DataSourceYahoo, DataSourceOanda, DataSourceMT5, DataSourceIB
    # Instance creation is handled implicitly by DataManager._get_data_source()
    
    source = DataSourceYahoo(_config=config)
    df = source.load_historical_data(_symbol="EURUSD", _timeframe=Timeframe.H1, _start_date=dt1, _end_date=dt2)
    symbols = source.get_available_symbols()
    timeframes = source.get_supported_timeframes()
    name = source.source_name
"""

from abc import ABC, abstractmethod
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Any
import pandas as pd

import sys
sys.path.insert(0, str(Path(__file__).parent.parent))

from core.data_types import Timeframe


class DataSourceBase(ABC):
    """
    Abstract base class for data source implementations.
    
    Provides shared logic for symbol mapping and data normalization.
    Concrete implementations: DataSourceYahoo, DataSourceOanda, DataSourceMT5, DataSourceIB
    """
    
    def __init__(self, _config: Any):
        self._config = _config
        self._source_config: Dict[str, Any] = {}
        self._symbol_mapping: Dict[str, str] = {}
        self._load_config()
    
    def _load_config(self) -> None:
        """Load data source configuration from brokers.yaml."""
        try:
            self._source_config = self._config.get_broker_config(self.source_name)
            self._symbol_mapping = self._source_config.get("symbol_mapping", {})
        except Exception:
            self._source_config = {}
    
    def _map_symbol(self, _symbol: str) -> str:
        """Map standard symbol to source-specific symbol."""
        return self._symbol_mapping.get(_symbol, _symbol)
    
    def _normalize_dataframe(self, _df: pd.DataFrame, _symbol: str, _timeframe: str) -> pd.DataFrame:
        """Normalize DataFrame to standard OHLCV format."""
        df = _df.copy()
        
        column_map = {
            "Date": "timestamp", "Datetime": "timestamp", "Time": "timestamp",
            "Open": "open", "High": "high", "Low": "low", "Close": "close", "Volume": "volume",
            "Adj Close": "adj_close",
        }
        df = df.rename(columns={k: v for k, v in column_map.items() if k in df.columns})
        
        if "timestamp" not in df.columns and df.index.name in ("Date", "Datetime", None):
            df = df.reset_index()
            if "index" in df.columns:
                df = df.rename(columns={"index": "timestamp"})
            elif "Date" in df.columns:
                df = df.rename(columns={"Date": "timestamp"})
            elif "Datetime" in df.columns:
                df = df.rename(columns={"Datetime": "timestamp"})
        
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        df["symbol"] = _symbol
        df["timeframe"] = _timeframe
        
        required = ["timestamp", "open", "high", "low", "close", "volume"]
        for col in required:
            if col not in df.columns:
                if col == "volume":
                    df["volume"] = 0
                else:
                    raise ValueError(f"Missing required column: {col}")
        
        return df[required + ["symbol", "timeframe"]].sort_values("timestamp").reset_index(drop=True)
    
    @abstractmethod
    def load_historical_data(
        self,
        _symbol: str,
        _timeframe: Timeframe,
        _start_date: datetime,
        _end_date: datetime,
    ) -> pd.DataFrame:
        """Load historical OHLCV data. Must be implemented by subclasses."""
        pass
    
    @abstractmethod
    def get_available_symbols(self) -> List[str]:
        """Get list of available symbols. Must be implemented by subclasses."""
        pass
    
    @abstractmethod
    def get_supported_timeframes(self) -> List[Timeframe]:
        """Get list of supported timeframes. Must be implemented by subclasses."""
        pass
    
    @property
    @abstractmethod
    def source_name(self) -> str:
        """Get data source name. Must be implemented by subclasses."""
        pass
