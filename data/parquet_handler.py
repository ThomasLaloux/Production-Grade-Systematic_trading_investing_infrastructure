"""
Parquet Handler Module
======================
Handles reading/writing OHLCV data to Parquet files.

Directory structure: {base_path}/{source_name}/{symbol}.parquet
Data stored at M1 resolution. Higher timeframes resampled on read.

Classes:
    ParquetHandler:
        - parquet file operations for OHLCV data
        - write_data
        - read_data
        - append_data
        - get_date_range
        - list_files
        - file_exists
        - delete_file
        - get_file_path
        - base_path (property)

Usage:
    handler = ParquetHandler(_base_path="data/ohlcv")
    handler.write_data(df, _source_name="yahoo", _symbol="EURUSD")
    df = handler.read_data(_source_name="yahoo", _symbol="EURUSD")
    df = handler.read_data(_source_name="yahoo", _symbol="EURUSD", _start_date=dt1, _end_date=dt2)
    rows = handler.append_data(new_df, _source_name="yahoo", _symbol="EURUSD")
    min_dt, max_dt = handler.get_date_range(_source_name="yahoo", _symbol="EURUSD")
    files = handler.list_files()
    files = handler.list_files(_source_name="yahoo")
    exists = handler.file_exists(_source_name="yahoo", _symbol="EURUSD")
    handler.delete_file(_source_name="yahoo", _symbol="EURUSD")
"""

from pathlib import Path
from datetime import datetime
from typing import Optional, List, Tuple
import pandas as pd

import sys
sys.path.insert(0, str(Path(__file__).parent.parent))

from core.exceptions import DataError


class ParquetHandler:
    """Parquet file operations for OHLCV data."""
    
    REQUIRED_COLUMNS = ["timestamp", "open", "high", "low", "close", "volume"]
    
    def __init__(self, _base_path: str):
        self._base_path = Path(_base_path)
        self._base_path.mkdir(parents=True, exist_ok=True)
    
    def _get_source_path(self, _source_name: str) -> Path:
        """Get/create directory for a data source."""
        source_path = self._base_path / _source_name.lower()
        source_path.mkdir(parents=True, exist_ok=True)
        return source_path
    
    def get_file_path(self, _source_name: str, _symbol: str) -> Path:
        """Get file path: {base}/{source}/{symbol}.parquet"""
        return self._get_source_path(_source_name) / f"{_symbol}.parquet"
    
    def write_data(
        self,
        _data: pd.DataFrame,
        _source_name: str,
        _symbol: str,
        _compression: str = "snappy",
    ) -> Path:
        """Write DataFrame to Parquet."""
        self._validate_dataframe(_data)
        
        df = _data.copy()
        if not pd.api.types.is_datetime64_any_dtype(df["timestamp"]):
            df["timestamp"] = pd.to_datetime(df["timestamp"])
        df = df.sort_values("timestamp").reset_index(drop=True)
        
        df["symbol"] = _symbol
        df["source"] = _source_name.lower()
        
        file_path = self.get_file_path(_source_name, _symbol)
        
        try:
            df.to_parquet(file_path, engine="pyarrow", compression=_compression, index=False)
            return file_path
        except Exception as e:
            raise DataError(f"Failed to write Parquet: {file_path}", {"error": str(e)})
    
    def read_data(
        self,
        _source_name: str,
        _symbol: str,
        _start_date: Optional[datetime] = None,
        _end_date: Optional[datetime] = None,
        _columns: Optional[List[str]] = None,
    ) -> pd.DataFrame:
        """Read OHLCV data from Parquet file."""
        file_path = self.get_file_path(_source_name, _symbol)
        
        if not file_path.exists():
            raise DataError(f"File not found: {file_path}", {"source": _source_name, "symbol": _symbol})
        
        try:
            df = pd.read_parquet(file_path, engine="pyarrow", columns=_columns)
            
            if _start_date is not None or _end_date is not None:
                df["timestamp"] = pd.to_datetime(df["timestamp"])
                if _start_date is not None:
                    df = df[df["timestamp"] >= pd.Timestamp(_start_date)]
                if _end_date is not None:
                    df = df[df["timestamp"] <= pd.Timestamp(_end_date)]
            
            return df.reset_index(drop=True)
        except Exception as e:
            raise DataError(f"Failed to read Parquet: {file_path}", {"error": str(e)})
    
    def append_data(
        self,
        _data: pd.DataFrame,
        _source_name: str,
        _symbol: str,
        _deduplicate: bool = True,
    ) -> int:
        """Append data to existing file. Returns rows added."""
        self._validate_dataframe(_data)
        
        file_path = self.get_file_path(_source_name, _symbol)
        
        if file_path.exists():
            existing_df = self.read_data(_source_name, _symbol)
            combined_df = pd.concat([existing_df, _data], ignore_index=True)
            
            if _deduplicate:
                combined_df["timestamp"] = pd.to_datetime(combined_df["timestamp"])
                combined_df = combined_df.drop_duplicates(subset=["timestamp"], keep="last")
            
            combined_df = combined_df.sort_values("timestamp").reset_index(drop=True)
            rows_added = len(combined_df) - len(existing_df)
        else:
            combined_df = _data.copy()
            rows_added = len(combined_df)
        
        self.write_data(combined_df, _source_name, _symbol)
        return rows_added
    
    def get_date_range(self, _source_name: str, _symbol: str) -> Tuple[datetime, datetime]:
        """Get min/max dates in file."""
        df = self.read_data(_source_name, _symbol, _columns=["timestamp"])
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        return (df["timestamp"].min().to_pydatetime(), df["timestamp"].max().to_pydatetime())
    
    def list_files(self, _source_name: Optional[str] = None) -> List[dict]:
        """List all Parquet files. Optionally filter by source."""
        files = []
        
        if _source_name:
            source_dirs = [self._base_path / _source_name.lower()]
        else:
            source_dirs = [d for d in self._base_path.iterdir() if d.is_dir()]
        
        for source_dir in source_dirs:
            if not source_dir.exists():
                continue
            for file_path in source_dir.glob("*.parquet"):
                files.append({
                    "source": source_dir.name,
                    "symbol": file_path.stem,
                    "path": str(file_path),
                    "size_mb": file_path.stat().st_size / (1024 * 1024),
                })
        return files
    
    def file_exists(self, _source_name: str, _symbol: str) -> bool:
        """Check if Parquet file exists."""
        return self.get_file_path(_source_name, _symbol).exists()
    
    def delete_file(self, _source_name: str, _symbol: str) -> bool:
        """Delete Parquet file. Returns True if deleted."""
        file_path = self.get_file_path(_source_name, _symbol)
        if file_path.exists():
            file_path.unlink()
            return True
        return False
    
    def _validate_dataframe(self, _data: pd.DataFrame) -> None:
        """Validate DataFrame has required columns."""
        if _data is None or _data.empty:
            raise DataError("DataFrame is empty or None")
        
        missing = set(self.REQUIRED_COLUMNS) - set(_data.columns)
        if missing:
            raise DataError(f"Missing columns: {missing}", {"required": self.REQUIRED_COLUMNS})
    
    @property
    def base_path(self) -> Path:
        return self._base_path
