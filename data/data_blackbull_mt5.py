"""
Blackbull Markets MT5 Data Source Module
========================================
Blackbull Markets specific MT5 data source implementation.

Classes:
    DataSourceBlackbullMT5(DataSourceMT5Base):
        - Blackbull Markets MT5 data loader
        - Inherits all MT5 logic from DataSourceMT5Base
"""

from .data_mt5_base import DataSourceMT5Base

class DataSourceBlackbullMT5(DataSourceMT5Base):
    """Blackbull Markets MT5 data loader."""
    
    @property
    def source_name(self) -> str:
        return "blackbull_mt5"
