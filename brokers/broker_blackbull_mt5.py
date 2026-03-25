"""
Blackbull Markets MT5 Broker Module
===================================
Blackbull Markets specific MT5 broker implementation.

Classes:
    BrokerBlackbullMT5(BrokerMT5Base):
        - Blackbull Markets MT5 broker
        - Inherits all MT5 logic from BrokerMT5Base
"""

from .broker_mt5_base import BrokerMT5Base


class BrokerBlackbullMT5(BrokerMT5Base):
    """Blackbull Markets MT5 broker implementation."""
    
    @property
    def broker_name(self) -> str:
        return "blackbull_mt5"
