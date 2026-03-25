"""
Data Manager Module
===================
Central data management: download, sync, query, quality checks.

Data Storage:
    - All data stored at M1 resolution
    - Higher timeframes (M15, H1, etc.) resampled on demand via get_ohlcv()
    - File format: {base_path}/{source_name}/{symbol}.parquet

Instance Creation:
    DataManager is created explicitly in __main__.py:
        data_manager = DataManager(_data_config=data_config, _broker_config=broker_config, _data_path="data/ohlcv")
    
    Data source instances (DataSourceYahoo, etc.) are created implicitly inside
    DataManager._get_data_source() when download_data() or sync_data() is called.

Classes:
    DataManager:
        - central data management with external source integration
        - download_data (always downloads M1)
        - sync_data (syncs M1 data)
        - get_ohlcv (resamples to requested timeframe)
        - get_mtf_data
        - query
        - check_quality
        - get_summary
        - list_available_data
        - get_instrument_metadata
        - resample_and_save
        - close

Usage:
    data_config = DataConfigurator("data/instruments.yaml")
    broker_config = BrokersConfigurator("brokers/brokers.yaml")
    mgr = DataManager(_data_config=data_config, _broker_config=broker_config, _data_path="data/ohlcv")
    
    # Download M1 data (stored as {symbol}.parquet)
    df = mgr.download_data(_source_name="blackbull_mt5", _symbol="EURUSDp", _start_date="2025-01-01", _end_date="2026-01-01")
    
    # Sync M1 data
    df = mgr.sync_data(_source_name="blackbull_mt5", _symbol="EURUSDp", _end_date="2026-01-20")
    
    # Get M15 data (resampled from M1 on demand)
    df_m15 = mgr.get_ohlcv(_source_name="blackbull_mt5", _symbol="EURUSDp", _timeframe="M15")
"""

from pathlib import Path
from datetime import datetime
from typing import Optional, List, Dict, Any, Union
import pandas as pd

import sys
sys.path.insert(0, str(Path(__file__).parent.parent))

from core.data_types import Timeframe, InstrumentMetadata
from core.exceptions import DataError
from .parquet_handler import ParquetHandler
from .duckdb_handler import DuckDBHandler, DUCKDB_AVAILABLE
from .data_quality import DataQualityChecker, QualityReport
from .timeframe_ops import TimeframeManager


class DataManager:
    """
    Central data management with external source integration.
    
    Data is always stored at M1 resolution. Higher timeframes are resampled on demand.
    
    Instance Creation:
        Explicit: Created in __main__.py via DataManager(_data_config=data_config, _broker_config=broker_config)
        
    Data Source Creation (implicit):
        When download_data() or sync_data() is called, _get_data_source() lazily creates the appropriate data source instance:
        - "yahoo" -> DataSourceYahoo (from data/data_yahoo.py)
        - "oanda" -> DataSourceOanda (from data/data_oanda.py)
        - "icm_mt5" -> DataSourceIcmMT5 (from data/data_icm_mt5.py)
        - "blackbull_mt5" -> DataSourceBlackbullMT5 (from data/data_blackbull_mt5.py)
        - "ib" -> DataSourceIB (from data/data_ib.py)
    """
    
    DEFAULT_START_DATE = datetime(2025, 1, 1)
    
    def __init__(
        self,
        _data_config: Any = None,
        _broker_config: Any = None,
        _data_path: str = "./data/ohlcv",
        _enable_duckdb: bool = True,
        _config: Any = None,  # backward compatibility
    ):
        """
        Initialize DataManager.
        
        Args:
            _data_config: DataConfigurator for instrument metadata
            _broker_config: BrokersConfigurator for broker connection info
            _data_path: Path to OHLCV data storage
            _enable_duckdb: Enable DuckDB for queries
            _config: Backward compatibility - combined config with both get_instrument and get_broker_config
        """
        # Handle backward compatibility with single config
        if _config is not None:
            self._data_config = _config
            self._broker_config = _config
        else:
            self._data_config = _data_config
            self._broker_config = _broker_config
        
        self._data_path = Path(_data_path)
        self._parquet = ParquetHandler(_base_path=str(self._data_path))
        self._duckdb: Optional[DuckDBHandler] = None
        if _enable_duckdb and DUCKDB_AVAILABLE:
            self._duckdb = DuckDBHandler(_parquet_path=str(self._data_path))
        self._quality_checker = DataQualityChecker()
        self._data_sources: Dict[str, Any] = {}
    
    def download_data(
        self,
        _source_name: str,
        _symbol: str,
        _start_date: Union[str, datetime],
        _end_date: Union[str, datetime],
        _timeframe: str = "M1",
        _run_quality_check: bool = True,
    ) -> pd.DataFrame:
        """
        Download data from external source and store in Parquet.
        
        Note: Data is always downloaded at the specified timeframe but stored as-is.
              For consistency, it's recommended to download M1 data.
        """
        start = datetime.fromisoformat(_start_date) if isinstance(_start_date, str) else _start_date
        end = datetime.fromisoformat(_end_date) if isinstance(_end_date, str) else _end_date
        
        data_source = self._get_data_source(_source_name.lower())
        tf = Timeframe.from_string(_timeframe)
        df = data_source.load_historical_data(_symbol=_symbol, _timeframe=tf, _start_date=start, _end_date=end)
        
        if df.empty:
            print(f"No data returned from {_source_name} for {_symbol}")
            return df
        
        if _run_quality_check:
            report = self._quality_checker.run_all_checks(df, _symbol, _timeframe)
            if not report.passed:
                print(f"Warning: Quality issues for {_symbol}:\n{self._quality_checker.get_summary_report(report)}")
        
        self._parquet.write_data(df, _source_name=_source_name, _symbol=_symbol)
        print(f"Saved {len(df)} {_timeframe} bars for {_symbol} from {_source_name}")
        
        if self._duckdb:
            self._duckdb.refresh_tables()
        
        return df
    
    def sync_data(
        self,
        _source_name: str,
        _symbol: str,
        _end_date: Optional[Union[str, datetime]] = None,
        _timeframe: str = "M1",
        _run_quality_check: bool = True,
    ) -> pd.DataFrame:
        """Sync local data with external source."""
        end = datetime.fromisoformat(_end_date) if isinstance(_end_date, str) else (_end_date or datetime.now())
        
        if self._parquet.file_exists(_source_name, _symbol):
            _, last_date = self._parquet.get_date_range(_source_name, _symbol)
            start = last_date
            if start >= end:
                print(f"{_symbol} already up to date")
                return self._parquet.read_data(_source_name, _symbol)
        else:
            start = self.DEFAULT_START_DATE
            print(f"No local data for {_symbol}, downloading from {start.date()}")
        
        data_source = self._get_data_source(_source_name.lower())
        tf = Timeframe.from_string(_timeframe)
        new_df = data_source.load_historical_data(_symbol=_symbol, _timeframe=tf, _start_date=start, _end_date=end)
        
        if new_df.empty:
            print(f"No new data from {_source_name} for {_symbol}")
            if self._parquet.file_exists(_source_name, _symbol):
                return self._parquet.read_data(_source_name, _symbol)
            return new_df
        
        if _run_quality_check:
            report = self._quality_checker.run_all_checks(new_df, _symbol, "M1")
            if not report.passed:
                print(f"Warning: Quality issues:\n{self._quality_checker.get_summary_report(report)}")
        
        rows_added = self._parquet.append_data(new_df, _source_name=_source_name, _symbol=_symbol)
        print(f"  [DataManager] Added {rows_added} bars for {_symbol}")
        
        if self._duckdb:
            self._duckdb.refresh_tables()
        
        return self._parquet.read_data(_source_name, _symbol)
    
    def get_ohlcv(
        self,
        _source_name: str,
        _symbol: str,
        _timeframe: str = "M1",
        _start_date: Optional[datetime] = None,
        _end_date: Optional[datetime] = None,
        _validate: bool = True,
    ) -> pd.DataFrame:
        """
        Get OHLCV data from local storage.
        
        Data is stored at M1 resolution. If a higher timeframe is requested,
        the data is resampled on demand.
        
        When _validate is True (default), runs data quality checks on load
        to detect and log gaps, duplicates, and OHLC inconsistencies.
        Session-break and holiday gaps are automatically suppressed when
        broker config and instrument metadata are available.
        """
        df = self._parquet.read_data(_source_name, _symbol, _start_date, _end_date)
        
        if df.empty:
            return df
        
        # Validate and repair data quality on load
        if _validate and not df.empty:
            # Resolve broker market hours and asset class for smart gap filtering
            market_hours = None
            asset_class = None
            if self._broker_config is not None:
                try:
                    market_hours = self._broker_config.get_market_hours(_source_name)
                except Exception:
                    pass
            if self._data_config is not None:
                try:
                    meta = self._data_config.get_instrument(_symbol, _broker=_source_name)
                    asset_class = getattr(meta, 'asset_class', None)
                except Exception:
                    pass

            df, _report = self._quality_checker.validate_and_repair(
                df, _symbol, _timeframe if _timeframe.upper() != "M1" else "M1",
                _broker_name=_source_name,
                _asset_class=asset_class,
                _market_hours=market_hours,
            )
        
        if _timeframe.upper() != "M1":
            target_tf = Timeframe.from_string(_timeframe)
            df = TimeframeManager.resample_ohlcv(df, Timeframe.M1, target_tf)
        
        return df
    
    def get_mtf_data(
        self,
        _source_name: str,
        _symbol: str,
        _base_timeframe: str,
        _higher_timeframes: List[str],
        _start_date: Optional[datetime] = None,
        _end_date: Optional[datetime] = None,
        _prevent_lookahead: bool = True,
    ) -> pd.DataFrame:
        """Get multi-timeframe data with aligned higher timeframes."""
        df = self.get_ohlcv(_source_name, _symbol, _base_timeframe, _start_date, _end_date)
        if df.empty:
            return df
        
        base_tf = Timeframe.from_string(_base_timeframe)
        htf_list = [Timeframe.from_string(tf) for tf in _higher_timeframes]
        
        valid, err = TimeframeManager.validate_mtf_request(base_tf, htf_list)
        if not valid:
            raise DataError(f"Invalid MTF request: {err}")
        
        # Get aligned data for each higher timeframe
        for htf in htf_list:
            htf_df = TimeframeManager.resample_ohlcv(df, base_tf, htf)
            aligned_df = TimeframeManager.align_mtf_data(
                _base_tf_data=df,
                _base_tf=base_tf,
                _htf_data=htf_df,
                _htf=htf,
                _prevent_lookahead=_prevent_lookahead
            )
            
            # Merge aligned higher timeframe columns
            htf_prefix = htf.value.lower()
            for col in ['open', 'high', 'low', 'close', 'volume']:
                if col in aligned_df.columns:
                    new_col_name = f"{htf_prefix}_{col}"
                    df[new_col_name] = aligned_df[col].values
        
        return df
    
    def query(self, _sql: str) -> pd.DataFrame:
        """Execute SQL query using DuckDB."""
        if self._duckdb is None:
            raise DataError("DuckDB not available. Initialize with _enable_duckdb=True")
        return self._duckdb.execute_query(_sql)
    
    def check_quality(
        self,
        _source_name: str,
        _symbol: str,
        _timeframe: str = "M1"
    ) -> QualityReport:
        """Run quality checks on stored data."""
        df = self.get_ohlcv(_source_name, _symbol, _timeframe)
        return self._quality_checker.run_all_checks(df, _symbol, _timeframe)
    
    def get_summary(
        self,
        _source_name: str,
        _symbol: str,
    ) -> Dict[str, Any]:
        """Get summary statistics for stored data."""
        if self._duckdb:
            return self._duckdb.get_summary_statistics(_source_name, _symbol)
        
        df = self.get_ohlcv(_source_name, _symbol)
        return {
            'count': len(df),
            'start_date': df['timestamp'].min(),
            'end_date': df['timestamp'].max(),
            'price_range': (df['low'].min(), df['high'].max()),
        }
    
    def list_available_data(self, _source_name: Optional[str] = None) -> List[Dict]:
        """List all available data files."""
        return self._parquet.list_files(_source_name)
    
    def get_instrument_metadata(self, _symbol: str) -> Optional[InstrumentMetadata]:
        """Get instrument metadata from config."""
        if self._data_config:
            try:
                return self._data_config.get_instrument(_symbol)
            except:
                return None
        return None
    
    def resample_and_save(
        self,
        _source_name: str,
        _symbol: str,
        _target_timeframe: str,
    ) -> str:
        """
        Resample M1 data to target timeframe and return the resampled DataFrame.
        
        Note: This does NOT save to a separate file - data is always stored at M1.
        Use get_ohlcv() with _timeframe parameter for on-demand resampling.
        """
        # Read M1 data
        df = self._parquet.read_data(_source_name, _symbol)
        
        # Resample
        target_tf = Timeframe.from_string(_target_timeframe)
        resampled = TimeframeManager.resample_ohlcv(df, Timeframe.M1, target_tf)
        
        return resampled
    
    def _get_data_source(self, _source_name: str) -> Any:
        """Get or create data source instance."""
        if _source_name in self._data_sources:
            return self._data_sources[_source_name]
        
        # Lazy import and instantiation
        source_name = _source_name.lower()
        
        if source_name == "yahoo":
            from .data_yahoo import DataSourceYahoo
            source = DataSourceYahoo()
        elif source_name == "oanda":
            from .data_oanda import DataSourceOanda
            source = DataSourceOanda(self._broker_config)
        elif source_name == "icm_mt5":
            from .data_icm_mt5 import DataSourceIcmMT5
            source = DataSourceIcmMT5(self._broker_config)
        elif source_name == "blackbull_mt5":
            from .data_blackbull_mt5 import DataSourceBlackbullMT5
            source = DataSourceBlackbullMT5(self._broker_config)
        elif source_name == "ib":
            from .data_ib import DataSourceIB
            source = DataSourceIB(self._broker_config)
        else:
            raise DataError(f"Unknown data source: {_source_name}", 
                          {"available": ["yahoo", "oanda", "icm_mt5", "blackbull_mt5", "ib"]})
        
        self._data_sources[_source_name] = source
        return source
    
    def close(self) -> None:
        """Close database connections."""
        if self._duckdb:
            self._duckdb.close()
