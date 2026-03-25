"""
MT5 Data Source Base Module
===========================
Base class for MetaTrader 5 data source implementations (Windows only).
Docs: https://www.mql5.com/en/docs/python_metatrader5

Classes:
    DataSourceMT5Base(DataSourceBase):
        - Base MT5 data loader with all MT5 API logic
        - Inherited by DataSourceIcmMT5, DataSourceBlackbullMT5
"""

from pathlib import Path
from datetime import datetime, timezone
from typing import List, Optional
import pandas as pd

import sys
sys.path.insert(0, str(Path(__file__).parent.parent))

from core.data_types import Timeframe
from core.exceptions import DataError
from .data_source_base import DataSourceBase

try:
    import MetaTrader5 as mt5
    MT5_AVAILABLE = True
except ImportError:
    MT5_AVAILABLE = False


class DataSourceMT5Base(DataSourceBase):
    """
    Base MT5 historical data loader.
    
    Contains all MT5 API logic. Broker-specific subclasses only override:
    - source_name property
    - Optional: symbol mappings via config
    """
    
    TIMEFRAME_MAP = {
        Timeframe.M1: mt5.TIMEFRAME_M1 if MT5_AVAILABLE else 1,
        Timeframe.M3: mt5.TIMEFRAME_M3 if MT5_AVAILABLE else 3,
        Timeframe.M5: mt5.TIMEFRAME_M5 if MT5_AVAILABLE else 5,
        Timeframe.M15: mt5.TIMEFRAME_M15 if MT5_AVAILABLE else 15,
        #Timeframe.M30: mt5.TIMEFRAME_M30 if MT5_AVAILABLE else 30,
        Timeframe.H1: mt5.TIMEFRAME_H1 if MT5_AVAILABLE else 60,
        Timeframe.H4: mt5.TIMEFRAME_H4 if MT5_AVAILABLE else 240,
        Timeframe.D1: mt5.TIMEFRAME_D1 if MT5_AVAILABLE else 1440,
        Timeframe.W1: mt5.TIMEFRAME_W1 if MT5_AVAILABLE else 10080,
        Timeframe.MN1: mt5.TIMEFRAME_MN1 if MT5_AVAILABLE else 43200,
    }
    
    def __init__(self, _config):
        super().__init__(_config)
        self._initialized = False
    
    def _ensure_initialized(self) -> bool:
        """Initialize MT5 connection if not already done."""
        if not MT5_AVAILABLE:
            raise DataError("MetaTrader5 package not installed (Windows only)")
        
        if self._initialized:
            return True
        
        # Get connection config for this specific broker
        conn_config = self._source_config.get("connection", {})
        path = conn_config.get("path")
        server = conn_config.get("server")
        login = conn_config.get("login")
        password = conn_config.get("password")
        timeout = conn_config.get("timeout", 60000)
        
        # Initialize with optional path
        init_kwargs = {"timeout": timeout}
        if path:
            init_kwargs["path"] = path
        if login:
            init_kwargs["login"] = login
        if password:
            init_kwargs["password"] = password
        if server:
            init_kwargs["server"] = server
        
        if not mt5.initialize(**init_kwargs):
            error = mt5.last_error()
            raise DataError(f"MT5 initialize failed: {error}", {"source": self.source_name})
        
        self._initialized = True
        return True
    
    def _shutdown(self) -> None:
        """Shutdown MT5 connection."""
        if MT5_AVAILABLE and self._initialized:
            mt5.shutdown()
            self._initialized = False
    
    def load_historical_data(
        self,
        _symbol: str,
        _timeframe: Timeframe,
        _start_date: datetime,
        _end_date: datetime,
    ) -> pd.DataFrame:
        """Load historical data from MT5."""
        self._ensure_initialized()
        
        broker_symbol = self._map_symbol(_symbol)
        mt5_tf = self.TIMEFRAME_MAP.get(_timeframe)
        
        if mt5_tf is None:
            raise DataError(f"Unsupported timeframe: {_timeframe.value}", {"source": self.source_name})
        
        # Ensure symbol is selected in Market Watch
        if not mt5.symbol_select(broker_symbol, True):
            raise DataError(f"Failed to select symbol: {broker_symbol}", {"source": self.source_name})
        
        # MT5 copy_rates_range expects timezone-aware datetime in UTC
        start_utc = _start_date.replace(tzinfo=timezone.utc) if _start_date.tzinfo is None else _start_date
        end_utc = _end_date.replace(tzinfo=timezone.utc) if _end_date.tzinfo is None else _end_date
        
        rates = mt5.copy_rates_range(broker_symbol, mt5_tf, start_utc, end_utc)
        
        if rates is None or len(rates) == 0:
            error = mt5.last_error()
            if error[0] != 0:  # 0 means no error
                raise DataError(f"MT5 copy_rates_range failed: {error}", {"symbol": broker_symbol})
            return pd.DataFrame()
        
        df = pd.DataFrame(rates)
        df["timestamp"] = pd.to_datetime(df["time"], unit="s")
        df = df.rename(columns={"tick_volume": "volume"})
        
        # Drop unnecessary columns
        cols_to_drop = ["time", "spread", "real_volume"]
        df = df.drop(columns=[c for c in cols_to_drop if c in df.columns], errors="ignore")
        
        return self._normalize_dataframe(df, _symbol, _timeframe.value)
    
    def get_available_symbols(self) -> List[str]:
        """Get available symbols from config."""
        return list(self._symbol_mapping.keys())
    
    def get_supported_timeframes(self) -> List[Timeframe]:
        """Get supported timeframes."""
        return list(self.TIMEFRAME_MAP.keys())
    
    def get_mt5_symbols(self) -> Optional[List[str]]:
        """Get all symbols available in MT5 terminal."""
        if not MT5_AVAILABLE:
            return None
        
        self._ensure_initialized()
        symbols = mt5.symbols_get()
        if symbols is None:
            return None
        return [s.name for s in symbols]
    
    @property
    def source_name(self) -> str:
        """Must be overridden by subclasses."""
        raise NotImplementedError("Subclasses must implement source_name")
    
    def __del__(self):
        """Cleanup on deletion."""
        self._shutdown()
