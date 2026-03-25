"""
Data Module
===========
Data management: Parquet storage, DuckDB queries, external source loading,
instrument configuration, economic calendar, and market holidays.

Storage: data/{source}/{symbol}.parquet at M1 resolution.
Calendar DB: data/calendar_news_db/news/YYYY.parquet + CSV
             data/calendar_news_db/holidays/YYYY.parquet + CSV

Modules:
    data_manager: DataManager
    data_configurator: DataConfigurator
    data_source_base: DataSourceBase
    data_mt5_base: DataSourceMT5Base
    data_icm_mt5: DataSourceIcmMT5
    data_blackbull_mt5: DataSourceBlackbullMT5
    data_yahoo: DataSourceYahoo
    data_oanda: DataSourceOanda
    data_ib: DataSourceIB
    parquet_handler: ParquetHandler
    duckdb_handler: DuckDBHandler
    data_quality: DataQualityChecker, QualityReport
    timeframe_ops: TimeframeManager
    calendar_manager: FinnhubCalendarManager  (P7.2)
    instrument_currency_map: InstrumentCurrencyMap  (P7.2)
    news_filter: NewsFilter  (P7.2)

Classes:
    DataManager:
        - download_data, sync_data, get_ohlcv, get_mtf_data, query, check_quality
        - get_summary, list_available_data, get_instrument_metadata, resample_and_save, close
    DataConfigurator:
        - load, get_instrument, get_all_instruments, get_all_instruments_metadata
        - add_instrument, remove_instrument, list_brokers, set_current_broker
        - current_broker (property), reload, save, to_dict
    DataSourceBase:
        - load_historical_data (abstract), get_available_symbols (abstract)
        - get_supported_timeframes (abstract), source_name (property, abstract)
    ParquetHandler:
        - write_data, read_data, append_data, get_date_range, list_files
        - file_exists, delete_file, get_file_path, base_path (property)
    DuckDBHandler:
        - execute_query, query_ohlcv, get_summary_statistics, get_latest_bar
        - get_bar_count, find_price_spikes, find_volume_anomalies
        - find_timestamp_gaps, get_available_tables, refresh_tables, close
    DataQualityChecker:
        - run_all_checks, check_price_spikes, check_timestamp_gaps
        - check_volume_anomalies, get_summary_report
    QualityReport:
        - to_dataframe, get_summary, plot_report, visualize_issues
    TimeframeManager:
        - resample_ohlcv, align_mtf_data, get_higher_timeframes, get_lower_timeframes
        - validate_mtf_request, calculate_bars_per_higher_tf, get_current_htf_bar_start
    FinnhubCalendarManager (P7.2):
        - update, get_news_events, get_holidays, get_holiday_dates, get_market_status
    InstrumentCurrencyMap (P7.2):
        - get_affected_currencies, get_instruments_for_currency, get_asset_class
    NewsFilter (P7.2):
        - is_in_blackout, get_upcoming_events, invalidate_cache
"""

from .data_manager import DataManager
from .data_configurator import DataConfigurator
from .data_source_base import DataSourceBase
from .parquet_handler import ParquetHandler
from .duckdb_handler import DuckDBHandler, DUCKDB_AVAILABLE
from .data_quality import DataQualityChecker, QualityReport
from .data_quality import generate_us_market_holidays, get_us_holidays_for_range
from .timeframe_ops import TimeframeManager

from .data_blackbull_mt5 import DataSourceBlackbullMT5 # optional
from .data_icm_mt5 import DataSourceIcmMT5 # optional
from .data_ib import DataSourceIB          # optional
from .data_oanda import DataSourceOanda    # optional
from .data_yahoo import DataSourceYahoo    # optional

# P7.2: Calendar news — economic calendar, market holidays, news filter
try:
    from .calendar_manager import FinnhubCalendarManager
    from .instrument_currency_map import InstrumentCurrencyMap
    from .news_filter import NewsFilter
    CALENDAR_NEWS_AVAILABLE = True
except ImportError:
    CALENDAR_NEWS_AVAILABLE = False

__all__ = [
    "DataManager", "DataConfigurator", "DataSourceBase", "ParquetHandler",
    "DuckDBHandler", "DUCKDB_AVAILABLE",
    "DataQualityChecker", "QualityReport", "TimeframeManager",
    "generate_us_market_holidays", "get_us_holidays_for_range",
    "DataSourceBlackbullMT5", "DataSourceIcmMT5",
    "DataSourceIB", "DataSourceOanda", "DataSourceYahoo",
    "CALENDAR_NEWS_AVAILABLE",
]

# Conditionally add calendar exports
if CALENDAR_NEWS_AVAILABLE:
    __all__.extend([
        "FinnhubCalendarManager", "InstrumentCurrencyMap", "NewsFilter",
    ])
