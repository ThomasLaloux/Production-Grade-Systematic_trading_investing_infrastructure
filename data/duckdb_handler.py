"""
DuckDB Handler Module
=====================
SQL query interface over Parquet files using DuckDB.

Classes:
    DuckDBHandler:
        - SQL query interface for Parquet files
        - execute_query
        - query_ohlcv
        - get_summary_statistics
        - get_latest_bar
        - get_bar_count
        - find_price_spikes
        - find_volume_anomalies
        - find_timestamp_gaps
        - get_available_tables
        - refresh_tables
        - close

Usage:
    db = DuckDBHandler(_parquet_path="data/ohlcv")
    df = db.execute_query("SELECT * FROM 'data/ohlcv/yahoo/EURUSD.parquet' WHERE close > 1.10")
    df = db.query_ohlcv(_source_name="yahoo", _symbol="EURUSD", _start_date=dt1, _end_date=dt2)
    stats = db.get_summary_statistics(_source_name="yahoo", _symbol="EURUSD")
    bar = db.get_latest_bar(_source_name="yahoo", _symbol="EURUSD")
    count = db.get_bar_count(_source_name="yahoo", _symbol="EURUSD")
    spikes = db.find_price_spikes(_source_name="yahoo", _symbol="EURUSD", _threshold_pct=5.0)
    db.close()
"""

from pathlib import Path
from datetime import datetime
from typing import Optional, List, Dict, Any
import pandas as pd

import sys
sys.path.insert(0, str(Path(__file__).parent.parent))

from core.exceptions import DataError

try:
    import duckdb
    DUCKDB_AVAILABLE = True
except ImportError:
    DUCKDB_AVAILABLE = False


class DuckDBHandler:
    """DuckDB query interface for Parquet files."""
    
    def __init__(self, _parquet_path: str, _database: str = ":memory:", _read_only: bool = False):
        if not DUCKDB_AVAILABLE:
            raise DataError("DuckDB not installed. Run: pip install duckdb")
        
        self._parquet_path = Path(_parquet_path)
        self._connection = duckdb.connect(_database, read_only=_read_only)
        self._registered_tables: Dict[str, str] = {}
        self._auto_register()
    
    def _auto_register(self) -> None:
        """Register all Parquet files as views."""
        if not self._parquet_path.exists():
            return
        for source_dir in self._parquet_path.iterdir():
            if source_dir.is_dir():
                for file_path in source_dir.glob("*.parquet"):
                    table_name = f"{source_dir.name}_{file_path.stem}"
                    self._register(str(file_path), table_name)
    
    def _register(self, _file_path: str, _table_name: str) -> None:
        """Register a Parquet file as a view."""
        self._connection.execute(f'CREATE OR REPLACE VIEW "{_table_name}" AS SELECT * FROM read_parquet(\'{_file_path}\')')
        self._registered_tables[_table_name] = _file_path
    
    def _get_file_path(self, _source_name: str, _symbol: str) -> str:
        """Get parquet file path."""
        return str(self._parquet_path / _source_name.lower() / f"{_symbol}.parquet")
    
    def execute_query(self, _query: str, _params: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
        """Execute raw SQL query."""
        try:
            result = self._connection.execute(_query, _params) if _params else self._connection.execute(_query)
            return result.fetchdf()
        except Exception as e:
            raise DataError(f"Query failed", {"query": _query[:100], "error": str(e)})
    
    def query_ohlcv(
        self,
        _source_name: str,
        _symbol: str,
        _start_date: Optional[datetime] = None,
        _end_date: Optional[datetime] = None,
        _columns: Optional[List[str]] = None,
        _limit: Optional[int] = None,
        _ascending: bool = True,
    ) -> pd.DataFrame:
        """Query OHLCV data with filters."""
        file_path = self._get_file_path(_source_name, _symbol)
        
        cols = ", ".join(_columns) if _columns else "*"
        conditions = []
        if _start_date:
            conditions.append(f"timestamp >= '{_start_date.isoformat()}'")
        if _end_date:
            conditions.append(f"timestamp <= '{_end_date.isoformat()}'")
        
        where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        order = "ASC" if _ascending else "DESC"
        limit = f"LIMIT {_limit}" if _limit else ""
        
        return self.execute_query(f"SELECT {cols} FROM read_parquet('{file_path}') {where} ORDER BY timestamp {order} {limit}")
    
    def get_summary_statistics(self, _source_name: str, _symbol: str) -> Dict[str, Any]:
        """Get summary statistics."""
        file_path = self._get_file_path(_source_name, _symbol)
        df = self.execute_query(f"""
            SELECT COUNT(*) as cnt, MIN(timestamp) as min_dt, MAX(timestamp) as max_dt,
                   MIN(low) as min_p, MAX(high) as max_p, AVG(close) as avg_c
            FROM read_parquet('{file_path}')
        """)
        r = df.iloc[0]
        return {
            "source": _source_name, "symbol": _symbol, "row_count": int(r["cnt"]),
            "date_range": (r["min_dt"], r["max_dt"]), "price_range": (r["min_p"], r["max_p"]),
        }
    
    def get_latest_bar(self, _source_name: str, _symbol: str) -> Optional[pd.Series]:
        """Get most recent bar."""
        df = self.query_ohlcv(_source_name, _symbol, _limit=1, _ascending=False)
        return df.iloc[0] if not df.empty else None
    
    def get_bar_count(self, _source_name: str, _symbol: str) -> int:
        """Get total bar count."""
        file_path = self._get_file_path(_source_name, _symbol)
        df = self.execute_query(f"SELECT COUNT(*) as cnt FROM read_parquet('{file_path}')")
        return int(df.iloc[0]["cnt"])
    
    def find_price_spikes(self, _source_name: str, _symbol: str, _threshold_pct: float = 5.0) -> pd.DataFrame:
        """Find bars with price change > threshold %."""
        file_path = self._get_file_path(_source_name, _symbol)
        return self.execute_query(f"""
            SELECT timestamp, open, high, low, close, ABS(close-open)/open*100 as pct
            FROM read_parquet('{file_path}') WHERE ABS(close-open)/open > {_threshold_pct/100}
        """)
    
    def find_volume_anomalies(self, _source_name: str, _symbol: str, _multiplier: float = 100.0) -> pd.DataFrame:
        """Find zero or extremely high volume bars."""
        file_path = self._get_file_path(_source_name, _symbol)
        return self.execute_query(f"""
            WITH s AS (SELECT AVG(volume) as av FROM read_parquet('{file_path}'))
            SELECT t.timestamp, t.volume FROM read_parquet('{file_path}') t, s
            WHERE t.volume = 0 OR t.volume > s.av * {_multiplier}
        """)
    
    def find_timestamp_gaps(self, _source_name: str, _symbol: str, _expected_minutes: int = 1, _multiplier: float = 2.0) -> pd.DataFrame:
        """Find gaps > expected interval."""
        file_path = self._get_file_path(_source_name, _symbol)
        threshold = _expected_minutes * 60 * _multiplier
        return self.execute_query(f"""
            WITH o AS (SELECT timestamp, LAG(timestamp) OVER (ORDER BY timestamp) as prev FROM read_parquet('{file_path}'))
            SELECT prev as gap_start, timestamp as gap_end FROM o
            WHERE prev IS NOT NULL AND EXTRACT(EPOCH FROM (timestamp-prev)) > {threshold}
        """)
    
    def get_available_tables(self) -> List[str]:
        return list(self._registered_tables.keys())
    
    def refresh_tables(self) -> None:
        self._auto_register()
    
    def close(self) -> None:
        if self._connection:
            self._connection.close()
            self._connection = None
    
    def __enter__(self):
        return self
    
    def __exit__(self, *args):
        self.close()
        return False
