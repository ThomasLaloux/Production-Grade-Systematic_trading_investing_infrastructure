"""
ICM Markets MT5 Broker Module
=============================
ICM Markets specific MT5 broker implementation.

Classes:
    BrokerIcmMT5(BrokerMT5Base):
        - ICM Markets MT5 broker
        - Inherits all MT5 logic from BrokerMT5Base
"""

from .broker_mt5_base import BrokerMT5Base


class BrokerIcmMT5(BrokerMT5Base):
    """ICM Markets MT5 broker implementation."""
    
    @property
    def broker_name(self) -> str:
        return "icm_mt5"
