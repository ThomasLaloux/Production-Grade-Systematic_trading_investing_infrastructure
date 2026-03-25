"""
ICM Markets MT5 Data Source Module
==================================
ICM Markets specific MT5 data source implementation.

Classes:
    DataSourceIcmMT5(DataSourceMT5Base):
        - ICM Markets MT5 data loader
        - Inherits all MT5 logic from DataSourceMT5Base
"""

from .data_mt5_base import DataSourceMT5Base


class DataSourceIcmMT5(DataSourceMT5Base):
    """ICM Markets MT5 data loader."""
    
    @property
    def source_name(self) -> str:
        return "icm_mt5"
