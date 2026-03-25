"""
IB Data Source Module
=====================
Interactive Brokers historical data source implementation.

Instance Creation:
    Implicit: Created by DataManager._get_data_source("ib") when
    download_data() or sync_data() is called with _source_name="ib".

Classes:
    DataSourceIB(DataSourceBase):
        - IB historical data loader via ib_insync

Usage:
    # Typically accessed via DataManager, not directly:
    data_manager = DataManager(_config=config)
    df = data_manager.download_data(_source_name="ib", _symbol="EURUSD", ...)
    
    # Direct usage (less common):
    source = DataSourceIB(_config=config)
    df = source.load_historical_data(_symbol="EURUSD", _timeframe=Timeframe.H1, _start_date=dt1, _end_date=dt2)
"""

from datetime import datetime
from typing import Any, List, Optional
import pandas as pd

from core.data_types import Timeframe
from core.exceptions import DataError
from .data_source_base import DataSourceBase

try:
    from ib_insync import IB, Forex
    IB_AVAILABLE = True
except ImportError:
    IB_AVAILABLE = False


class DataSourceIB(DataSourceBase):
    """IB historical data loader."""
    
    BAR_SIZE_MAP = {
        Timeframe.M1: "1 min",
        Timeframe.M3: "3 mins",
        Timeframe.M5: "5 mins",
        Timeframe.M15: "15 mins",
        Timeframe.H1: "1 hour",
        Timeframe.H4: "4 hours",
        Timeframe.D1: "1 day",
        Timeframe.W1: "1 week",
        Timeframe.MN1: "1 month",
    }
    
    def __init__(self, _config: Any):
        super().__init__(_config)
        self._ib: Optional[IB] = None
    
    @property
    def source_name(self) -> str:
        return "ib"
    
    def load_historical_data(
        self,
        _symbol: str,
        _timeframe: Timeframe,
        _start_date: datetime,
        _end_date: datetime,
    ) -> pd.DataFrame:
        """Load historical data from IB."""
        if not IB_AVAILABLE:
            raise DataError("ib_insync package not installed")
        
        conn_config = self._source_config.get("connection", {})
        host = conn_config.get("host", "127.0.0.1")
        port = conn_config.get("port", 7497)
        
        self._ib = IB()
        self._ib.connect(host, port, clientId=99)
        
        try:
            broker_symbol = self._map_symbol(_symbol)
            contract = Forex(broker_symbol.replace(".", ""))
            
            bar_size = self.BAR_SIZE_MAP.get(_timeframe, "1 hour")
            duration = f"{(_end_date - _start_date).days + 1} D"
            
            bars = self._ib.reqHistoricalData(
                contract,
                endDateTime=_end_date,
                durationStr=duration,
                barSizeSetting=bar_size,
                whatToShow="MIDPOINT",
                useRTH=False,
            )
            
            if not bars:
                return pd.DataFrame()
            
            df = pd.DataFrame([{
                "timestamp": bar.date,
                "open": bar.open,
                "high": bar.high,
                "low": bar.low,
                "close": bar.close,
                "volume": bar.volume,
            } for bar in bars])
            
            return self._normalize_dataframe(df, _symbol, _timeframe.value)
        finally:
            self._ib.disconnect()
    
    def get_available_symbols(self) -> List[str]:
        """Get available symbols from config."""
        return list(self._symbol_mapping.keys())
    
    def get_supported_timeframes(self) -> List[Timeframe]:
        """Get supported timeframes."""
        return list(self.BAR_SIZE_MAP.keys())
