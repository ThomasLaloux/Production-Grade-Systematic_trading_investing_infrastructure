"""
Broker Manager Module
=====================
Factory for creating broker instances.

Classes:
    BrokerManager:
        - factory for creating broker instances
        - create
        - register
        - list_brokers

Usage:
    broker = BrokerManager.create(_broker_name="oanda", _config=config)
    broker = BrokerManager.create(_broker_name="icm_mt5", _config=config)
    broker = BrokerManager.create(_broker_name="blackbull_mt5", _config=config)
    BrokerManager.register("custom", CustomBroker)
    available = BrokerManager.list_brokers()  # ["oanda", "icm_mt5", "blackbull_mt5", "ib", ...]
"""

from pathlib import Path
from typing import Any, Dict, List

import sys
sys.path.insert(0, str(Path(__file__).parent.parent))

from core.exceptions import BrokerError


class BrokerManager:
    """Factory for creating broker instances."""
    
    _brokers: Dict[str, type] = {}
    
    @classmethod
    def create(cls, _broker_name: str, _config: Any):
        """Create broker instance by name."""
        from .broker_base import BrokerBase
        
        name = _broker_name.lower()
        
        if name == "yahoo":
            raise BrokerError("Yahoo is data-only source, not a trading broker", {"source": name})
        
        if name in cls._brokers:
            return cls._brokers[name](_config)
        
        if name == "oanda":
            from .broker_oanda import BrokerOanda
            return BrokerOanda(_config)
        elif name == "icm_mt5":
            from .broker_icm_mt5 import BrokerIcmMT5
            return BrokerIcmMT5(_config)
        elif name == "blackbull_mt5":
            from .broker_blackbull_mt5 import BrokerBlackbullMT5
            return BrokerBlackbullMT5(_config)
        elif name == "ib":
            from .broker_ib import BrokerIB
            return BrokerIB(_config)
        else:
            raise BrokerError(f"Unknown broker: {_broker_name}", {"available": cls.list_brokers()})
    
    @classmethod
    def register(cls, _name: str, _broker_class: type) -> None:
        """Register custom broker implementation."""
        cls._brokers[_name.lower()] = _broker_class
    
    @classmethod
    def list_brokers(cls) -> List[str]:
        """List available broker names."""
        builtin = ["oanda", "icm_mt5", "blackbull_mt5", "ib"]
        custom = list(cls._brokers.keys())
        return builtin + [b for b in custom if b not in builtin]
