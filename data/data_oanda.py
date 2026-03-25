"""
Oanda Data Source Module
========================
Oanda historical data source implementation.

Instance Creation:
    Implicit: Created by DataManager._get_data_source("oanda") when
    download_data() or sync_data() is called with _source_name="oanda".

Classes:
    DataSourceOanda(DataSourceBase):
        - Oanda historical data loader via REST API

Usage:
    # Typically accessed via DataManager, not directly:
    data_manager = DataManager(_config=config)
    df = data_manager.download_data(_source_name="oanda", _symbol="EURUSD", ...)
    
    # Direct usage (less common):
    source = DataSourceOanda(_config=config)
    df = source.load_historical_data(_symbol="EURUSD", _timeframe=Timeframe.H1, _start_date=dt1, _end_date=dt2)
"""

from datetime import datetime
from typing import List
import pandas as pd

from core.data_types import Timeframe
from core.exceptions import DataError
from .data_source_base import DataSourceBase

try:
    import requests
    REQUESTS_AVAILABLE = True
except ImportError:
    REQUESTS_AVAILABLE = False


class DataSourceOanda(DataSourceBase):
    """Oanda historical data loader."""
    
    ENDPOINTS = {
        "practice": "https://api-fxpractice.oanda.com",
        "live": "https://api-fxtrade.oanda.com",
    }
    
    GRANULARITY_MAP = {
        Timeframe.M1: "M1", Timeframe.M3: "M3", Timeframe.M5: "M5", Timeframe.M15: "M15",
        Timeframe.H1: "H1", Timeframe.H4: "H4", Timeframe.D1: "D", Timeframe.W1: "W", Timeframe.MN1: "M",
    }
    
    @property
    def source_name(self) -> str:
        return "oanda"
    
    def load_historical_data(
        self,
        _symbol: str,
        _timeframe: Timeframe,
        _start_date: datetime,
        _end_date: datetime,
    ) -> pd.DataFrame:
        """Load historical OHLCV data from Oanda."""
        if not REQUESTS_AVAILABLE:
            raise DataError("requests library not installed")
        
        api_config = self._source_config.get("api", {})
        token = api_config.get("token", "")
        environment = api_config.get("environment", "practice")
        
        base_url = self.ENDPOINTS.get(environment, self.ENDPOINTS["practice"])
        broker_symbol = self._map_symbol(_symbol)
        granularity = self.GRANULARITY_MAP.get(_timeframe, "H1")
        
        headers = {"Authorization": f"Bearer {token}"}
        
        all_candles = []
        current_start = _start_date
        
        while current_start < _end_date:
            params = {
                "granularity": granularity,
                "from": current_start.isoformat() + "Z",
                "to": _end_date.isoformat() + "Z",
                "count": 5000,
            }
            
            response = requests.get(
                f"{base_url}/v3/instruments/{broker_symbol}/candles",
                headers=headers,
                params=params,
            )
            
            if response.status_code != 200:
                break
            
            candles = response.json().get("candles", [])
            if not candles:
                break
            
            all_candles.extend(candles)
            last_time = candles[-1].get("time", "")
            current_start = datetime.fromisoformat(last_time.replace("Z", "+00:00")).replace(tzinfo=None)
            
            if len(candles) < 5000:
                break
        
        if not all_candles:
            return pd.DataFrame()
        
        rows = []
        for candle in all_candles:
            mid = candle.get("mid", {})
            rows.append({
                "timestamp": candle.get("time", "").replace("Z", ""),
                "open": float(mid.get("o", 0)),
                "high": float(mid.get("h", 0)),
                "low": float(mid.get("l", 0)),
                "close": float(mid.get("c", 0)),
                "volume": int(candle.get("volume", 0)),
            })
        
        df = pd.DataFrame(rows)
        return self._normalize_dataframe(df, _symbol, _timeframe.value)
    
    def get_available_symbols(self) -> List[str]:
        """Get available symbols from config."""
        return list(self._symbol_mapping.keys())
    
    def get_supported_timeframes(self) -> List[Timeframe]:
        """Get supported timeframes."""
        return list(self.GRANULARITY_MAP.keys())
