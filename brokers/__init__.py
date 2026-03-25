"""
Brokers Module
==============
Broker implementations for order management and broker configuration.

Modules:
    broker_manager: BrokerManager
    brokers_configurator: BrokersConfigurator
    broker_base: BrokerBase
    broker_mt5_base: BrokerMT5Base
    broker_icm_mt5: BrokerIcmMT5
    broker_blackbull_mt5: BrokerBlackbullMT5
    broker_oanda: BrokerOanda
    broker_ib: BrokerIB

Classes:
    BrokerManager:
        - create, register, list_brokers
    BrokersConfigurator:
        - load, get_broker_config, get_connection_config, get_api_config
        - get_symbol_mapping, get_endpoints, is_enabled, get_enabled_brokers
        - list_brokers, set_broker_config, update_broker_config
        - enable_broker, disable_broker, translate_symbol, reverse_translate_symbol
        - reload, save, to_dict
    BrokerBase:
        - connect (abstract), disconnect (abstract), is_connected (abstract)
        - submit_order (abstract), cancel_order (abstract), modify_order (abstract)
        - get_positions (abstract), get_pending_orders (abstract)
        - get_account_info (abstract), get_instrument_metadata (abstract)
        - broker_name (property, abstract), has_open_position (implemented)
        - count_positions (implemented)
"""

from .broker_manager import BrokerManager
from .brokers_configurator import BrokersConfigurator
from .broker_base import BrokerBase

from .broker_blackbull_mt5 import BrokerBlackbullMT5 # optional
from .broker_icm_mt5 import BrokerIcmMT5             # optional
from .broker_oanda import BrokerOanda                # optional
from .broker_ib import BrokerIB                      # optional

__all__ = [
    "BrokerManager", "BrokersConfigurator", "BrokerBase", 
    "BrokerIcmMT5", "BrokerBlackbullMT5", "BrokerOanda", "BrokerIB"
]


def __getattr__(name):
    """Lazy import broker implementations."""
    if name == "BrokerOanda":
        from .broker_oanda import BrokerOanda
        return BrokerOanda
    elif name == "BrokerMT5Base":
        from .broker_mt5_base import BrokerMT5Base
        return BrokerMT5Base
    elif name == "BrokerIcmMT5":
        from .broker_icm_mt5 import BrokerIcmMT5
        return BrokerIcmMT5
    elif name == "BrokerBlackbullMT5":
        from .broker_blackbull_mt5 import BrokerBlackbullMT5
        return BrokerBlackbullMT5
    elif name == "BrokerIB":
        from .broker_ib import BrokerIB
        return BrokerIB
    elif name == "BrokersConfigurator":
        from .brokers_configurator import BrokersConfigurator
        return BrokersConfigurator
    raise AttributeError(f"module 'brokers' has no attribute '{name}'")
