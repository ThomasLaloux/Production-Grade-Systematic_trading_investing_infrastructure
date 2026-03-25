"""
Brokers Configurator Module
===========================
YAML-based configuration for broker connections and settings.

Classes:
    BrokersConfigurator:
        - __init__(_path: str = None)
        - load(_path: str) -> None
        - get_broker_config(_broker_name: str) -> Dict[str, Any]
        - get_connection_config(_broker_name: str) -> Dict[str, Any]
        - get_symbol_mapping(_broker_name: str) -> Dict[str, str]
        - get_api_config(_broker_name: str) -> Dict[str, Any]
        - is_enabled(_broker_name: str) -> bool
        - get_enabled_brokers() -> List[str]
        - list_brokers() -> List[str]
        - set_broker_config(_broker_name: str, _config: Dict[str, Any]) -> None
        - reload() -> None
        - save(_destination: str) -> None
        - to_dict() -> Dict[str, Any]

Usage:
    # Initialize with YAML file
    broker_cfg = BrokersConfigurator(_path="brokers/brokers.yaml")
    
    # Get broker configuration
    bb_config = broker_cfg.get_broker_config("blackbull_mt5")
    
    # Get connection details
    conn = broker_cfg.get_connection_config("blackbull_mt5")
    # {"path": "...", "server": "...", "login": ..., "password": "...", "timeout": ...}
    
    # Get symbol mapping
    mapping = broker_cfg.get_symbol_mapping("blackbull_mt5")
    # {"EURUSD": "EURUSDp", "GBPUSD": "GBPUSDp", ...}
    
    # Check if broker is enabled
    if broker_cfg.is_enabled("blackbull_mt5"):
        ...
    
    # Get all enabled brokers
    enabled = broker_cfg.get_enabled_brokers()  # ["blackbull_mt5", "yahoo"]
"""

from pathlib import Path
from typing import Any, Dict, List, Optional
import yaml
import sys

sys.path.insert(0, str(Path(__file__).parent.parent))

from core.exceptions import ConfigurationError


class BrokersConfigurator:
    """YAML-based configuration manager for broker connections and settings."""

    def __init__(self, _path: Optional[str] = None):
        """
        Initialize BrokersConfigurator.
        
        Args:
            _path: Path to brokers YAML file (optional)
        """
        self._brokers: Dict[str, Dict[str, Any]] = {}
        self._config_path: Optional[str] = None
        
        if _path:
            self.load(_path)
    
    def load(self, _path: str) -> None:
        """
        Load broker configurations from YAML file.
        
        Structure:
            brokers:
                broker_name:
                    enabled: true/false
                    connection:  # or 'api' for REST brokers
                        ...
                    symbol_mapping:
                        INTERNAL_SYMBOL: BROKER_SYMBOL
        
        Args:
            _path: Path to brokers YAML file
        
        Raises:
            ConfigurationError: If file not found or invalid format
        """
        self._config_path = _path
        path = Path(_path)
        
        if not path.exists():
            raise ConfigurationError(f"Brokers file not found: {_path}", {"path": str(path.absolute())})
        
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = yaml.safe_load(f) or {}
        except yaml.YAMLError as e:
            raise ConfigurationError(f"YAML parse error: {_path}", {"error": str(e)})
        
        if not isinstance(data, dict) or "brokers" not in data:
            raise ConfigurationError("Invalid format: expected dict with 'brokers' key", {"path": _path})
        
        self._brokers = data["brokers"]
    
    def get_broker_config(self, _broker_name: str) -> Dict[str, Any]:
        """
        Get complete broker configuration by name.
        
        Args:
            _broker_name: Broker identifier (e.g., "blackbull_mt5", "oanda")
        
        Returns:
            Complete broker configuration dict
        
        Raises:
            ConfigurationError: If broker not found
        """
        if _broker_name not in self._brokers:
            raise ConfigurationError(
                f"Broker not found: {_broker_name}", 
                {"available": list(self._brokers.keys())}
            )
        return self._brokers[_broker_name].copy()
    
    def get_connection_config(self, _broker_name: str) -> Dict[str, Any]:
        """
        Get connection configuration for MT5/IB brokers.
        
        Args:
            _broker_name: Broker identifier
        
        Returns:
            Connection config dict (path, server, login, password, timeout, etc.)
        
        Raises:
            ConfigurationError: If broker not found or no connection config
        """
        config = self.get_broker_config(_broker_name)
        
        if "connection" in config:
            return config["connection"].copy()
        
        raise ConfigurationError(
            f"No connection config for broker: {_broker_name}",
            {"has_keys": list(config.keys())}
        )
    
    def get_api_config(self, _broker_name: str) -> Dict[str, Any]:
        """
        Get API configuration for REST brokers (OANDA, etc.).
        
        Args:
            _broker_name: Broker identifier
        
        Returns:
            API config dict (token, account_id, environment, etc.)
        
        Raises:
            ConfigurationError: If broker not found or no API config
        """
        config = self.get_broker_config(_broker_name)
        
        if "api" in config:
            return config["api"].copy()
        
        raise ConfigurationError(
            f"No API config for broker: {_broker_name}",
            {"has_keys": list(config.keys())}
        )
    
    def get_symbol_mapping(self, _broker_name: str) -> Dict[str, str]:
        """
        Get symbol mapping for a broker.
        
        Args:
            _broker_name: Broker identifier
        
        Returns:
            Dict mapping internal symbols to broker symbols
            e.g., {"EURUSD": "EURUSDp", "GBPUSD": "GBPUSDp"}
        
        Raises:
            ConfigurationError: If broker not found
        """
        config = self.get_broker_config(_broker_name)
        return config.get("symbol_mapping", {}).copy()
    
    def get_endpoints(self, _broker_name: str) -> Dict[str, str]:
        """
        Get API endpoints for REST brokers.
        
        Args:
            _broker_name: Broker identifier
        
        Returns:
            Dict of endpoint URLs (practice, live, etc.)
        
        Raises:
            ConfigurationError: If broker not found
        """
        config = self.get_broker_config(_broker_name)
        return config.get("endpoints", {}).copy()
    
    def get_market_hours(self, _broker_name: str) -> Dict[str, Any]:
        """
        Get market hours configuration for a broker.
        
        Returns the market_hours section from the broker config, containing
        per-asset-class session definitions, holidays, and timezone.
        
        Args:
            _broker_name: Broker identifier
        
        Returns:
            Dict with asset class keys (forex, metals, indices),
            holidays list, and timezone string.
            Empty dict if no market hours defined.
        """
        config = self.get_broker_config(_broker_name)
        return config.get("market_hours", {}).copy()
    
    def is_enabled(self, _broker_name: str) -> bool:
        """
        Check if broker is enabled.
        
        Args:
            _broker_name: Broker identifier
        
        Returns:
            True if broker is enabled, False otherwise
        """
        if _broker_name not in self._brokers:
            return False
        return self._brokers[_broker_name].get("enabled", False)
    
    def get_enabled_brokers(self) -> List[str]:
        """
        Get list of enabled brokers.
        
        Returns:
            List of broker names that are enabled
        """
        return [
            name for name, config in self._brokers.items()
            if config.get("enabled", False)
        ]
    
    def list_brokers(self) -> List[str]:
        """
        Get list of all configured brokers.
        
        Returns:
            List of all broker names
        """
        return list(self._brokers.keys())
    
    def set_broker_config(self, _broker_name: str, _config: Dict[str, Any]) -> None:
        """
        Set or update broker configuration programmatically.
        
        Args:
            _broker_name: Broker identifier
            _config: Complete broker configuration dict
        """
        self._brokers[_broker_name] = _config
    
    def update_broker_config(self, _broker_name: str, **kwargs) -> None:
        """
        Update specific fields in broker configuration.
        
        Args:
            _broker_name: Broker identifier
            **kwargs: Fields to update
        
        Raises:
            ConfigurationError: If broker not found
        """
        if _broker_name not in self._brokers:
            raise ConfigurationError(
                f"Broker not found: {_broker_name}",
                {"available": list(self._brokers.keys())}
            )
        self._brokers[_broker_name].update(kwargs)
    
    def enable_broker(self, _broker_name: str) -> None:
        """
        Enable a broker.
        
        Args:
            _broker_name: Broker identifier
        
        Raises:
            ConfigurationError: If broker not found
        """
        if _broker_name not in self._brokers:
            raise ConfigurationError(
                f"Broker not found: {_broker_name}",
                {"available": list(self._brokers.keys())}
            )
        self._brokers[_broker_name]["enabled"] = True
    
    def disable_broker(self, _broker_name: str) -> None:
        """
        Disable a broker.
        
        Args:
            _broker_name: Broker identifier
        
        Raises:
            ConfigurationError: If broker not found
        """
        if _broker_name not in self._brokers:
            raise ConfigurationError(
                f"Broker not found: {_broker_name}",
                {"available": list(self._brokers.keys())}
            )
        self._brokers[_broker_name]["enabled"] = False
    
    def translate_symbol(self, _symbol: str, _broker_name: str) -> str:
        """
        Translate internal symbol to broker-specific symbol.
        
        Args:
            _symbol: Internal symbol (e.g., "EURUSD")
            _broker_name: Broker identifier
        
        Returns:
            Broker-specific symbol (e.g., "EURUSDp" for blackbull_mt5)
            Returns original symbol if no mapping found
        """
        mapping = self.get_symbol_mapping(_broker_name)
        return mapping.get(_symbol, _symbol)
    
    def reverse_translate_symbol(self, _broker_symbol: str, _broker_name: str) -> str:
        """
        Translate broker-specific symbol to internal symbol.
        
        Args:
            _broker_symbol: Broker-specific symbol (e.g., "EURUSDp")
            _broker_name: Broker identifier
        
        Returns:
            Internal symbol (e.g., "EURUSD")
            Returns original symbol if no reverse mapping found
        """
        mapping = self.get_symbol_mapping(_broker_name)
        reverse_mapping = {v: k for k, v in mapping.items()}
        return reverse_mapping.get(_broker_symbol, _broker_symbol)
    
    def reload(self) -> None:
        """Reload brokers from the original file."""
        if self._config_path:
            self.load(self._config_path)
    
    def save(self, _destination: str) -> None:
        """
        Save brokers configuration to YAML file.
        
        Args:
            _destination: Path to save YAML file
        """
        data = {"brokers": self._brokers}
        
        try:
            with open(_destination, "w", encoding="utf-8") as f:
                yaml.dump(data, f, default_flow_style=False, sort_keys=False)
        except IOError as e:
            raise ConfigurationError(f"Failed to save: {_destination}", {"error": str(e)})
    
    def to_dict(self) -> Dict[str, Any]:
        """
        Get complete brokers configuration as dictionary.
        
        Returns:
            Dict with all broker configurations
        """
        return self._brokers.copy()
