"""
Data Configurator Module
========================
YAML-based configuration for instruments metadata.

Classes:
    DataConfigurator:
        - __init__(_path: str = None)
        - load(_path: str) -> None
        - get_instrument(_symbol: str, _broker: str = None) -> InstrumentMetadata
        - get_all_instruments(_broker: str = None) -> List[str]
        - get_all_instruments_metadata(_broker: str = None) -> Dict[str, InstrumentMetadata]
        - add_instrument(_instrument: InstrumentMetadata, _broker: str = None) -> None
        - remove_instrument(_symbol: str, _broker: str = None) -> bool
        - list_brokers() -> List[str]
        - set_current_broker(_broker: str) -> None
        - current_broker (property) -> str
        - reload() -> None
        - save(_destination: str) -> None

Usage:
    # Initialize with YAML file
    data_cfg = DataConfigurator(_path="data/instruments.yaml")
    
    # Get instrument metadata
    eurusd = data_cfg.get_instrument("EURUSD", _broker="oanda")
    eurusd = data_cfg.get_instrument("EURUSDp")  # searches all brokers
    
    # Get all instruments for a broker
    symbols = data_cfg.get_all_instruments(_broker="blackbull_mt5")  # ["EURUSDp", "GBPUSDp", ...]
    
    # Get all metadata
    all_meta = data_cfg.get_all_instruments_metadata(_broker="blackbull_mt5")
    
    # Add/remove instruments programmatically
    data_cfg.add_instrument(InstrumentMetadata(...), _broker="oanda")
    data_cfg.remove_instrument("XAUUSD", _broker="oanda")
    
    # Set default broker
    data_cfg.set_current_broker("blackbull_mt5")
"""

from pathlib import Path
from typing import Any, Dict, List, Optional
import yaml
import sys

sys.path.insert(0, str(Path(__file__).parent.parent))

from core.data_types import InstrumentMetadata
from core.exceptions import ConfigurationError


class DataConfigurator:
    """YAML-based configuration manager for instrument metadata."""

    def __init__(self, _path: Optional[str] = None):
        """
        Initialize DataConfigurator.
        
        Args:
            _path: Path to instruments YAML file (optional)
        """
        self._instruments: Dict[str, Dict[str, InstrumentMetadata]] = {}  # broker -> symbol -> metadata
        self._config_path: Optional[str] = None
        self._current_broker: str = ""
        
        if _path:
            self.load(_path)
    
    def load(self, _path: str) -> None:
        """
        Load instrument metadata from YAML file.
        
        Structure:
            instruments:
                broker_name:
                    SYMBOL:
                        pip_size: 0.0001
                        contract_size: 100000
                        commission: 7.0
                        ...
        
        Args:
            _path: Path to instruments YAML file
        
        Raises:
            ConfigurationError: If file not found or invalid format
        """
        self._config_path = _path
        path = Path(_path)
        
        if not path.exists():
            raise ConfigurationError(f"Instruments file not found: {_path}", {"path": str(path.absolute())})
        
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = yaml.safe_load(f) or {}
        except yaml.YAMLError as e:
            raise ConfigurationError(f"YAML parse error: {_path}", {"error": str(e)})
        
        if not isinstance(data, dict) or "instruments" not in data:
            raise ConfigurationError("Invalid format: expected dict with 'instruments' key", {"path": _path})
        
        self._instruments.clear()
        
        for broker_name, symbols in data["instruments"].items():
            self._instruments[broker_name] = {}
            
            for symbol, spec in symbols.items():
                try:
                    self._instruments[broker_name][symbol] = InstrumentMetadata(
                        symbol=symbol,
                        pip_size=float(spec.get("pip_size", 0.0001)),
                        contract_size=float(spec.get("contract_size", 100000)),
                        commission=float(spec.get("commission", 0.0)),
                        min_lot_size=float(spec.get("min_lot_size", 0.01)),
                        max_lot_size=float(spec.get("max_lot_size", 100.0)),
                        lot_step=float(spec.get("lot_step", 0.01)),
                        currency_base=spec.get("currency_base", ""),
                        currency_quote=spec.get("currency_quote", ""),
                        description=spec.get("description", ""),
                        broker=broker_name,
                        asset_class=spec.get("asset_class", ""),
                    )
                except (KeyError, ValueError, TypeError) as e:
                    raise ConfigurationError(f"Invalid spec for {broker_name}/{symbol}", {"error": str(e)})
    
    def get_instrument(self, _symbol: str, _broker: Optional[str] = None) -> InstrumentMetadata:
        """
        Get instrument metadata by symbol and broker.
        
        Args:
            _symbol: Instrument symbol (e.g., "EURUSD", "EURUSDp")
            _broker: Broker name (optional, uses current_broker or searches all)
        
        Returns:
            InstrumentMetadata for the symbol
        
        Raises:
            ConfigurationError: If instrument not found
        """
        broker = _broker or self._current_broker
        
        if not broker:
            # Try to find in any broker
            for broker_name, symbols in self._instruments.items():
                if _symbol in symbols:
                    return symbols[_symbol]
            raise ConfigurationError(
                f"Instrument not found: {_symbol}", 
                {"available_brokers": list(self._instruments.keys())}
            )
        
        if broker not in self._instruments:
            raise ConfigurationError(
                f"Broker not found: {broker}", 
                {"available": list(self._instruments.keys())}
            )
        
        if _symbol not in self._instruments[broker]:
            raise ConfigurationError(
                f"Instrument not found: {_symbol} for broker {broker}", 
                {"available": list(self._instruments[broker].keys())}
            )
        
        return self._instruments[broker][_symbol]
    
    def get_all_instruments(self, _broker: Optional[str] = None) -> List[str]:
        """
        Get all instrument symbols for a broker.
        
        Args:
            _broker: Broker name (optional, uses current_broker or returns all)
        
        Returns:
            List of symbol strings
        
        Raises:
            ConfigurationError: If broker not found
        """
        broker = _broker or self._current_broker
        
        if not broker:
            # Return all instruments flattened
            result = []
            for symbols in self._instruments.values():
                result.extend(symbols.keys())
            return result
        
        if broker not in self._instruments:
            raise ConfigurationError(
                f"Broker not found: {broker}", 
                {"available": list(self._instruments.keys())}
            )
        
        return list(self._instruments[broker].keys())
    
    def get_all_instruments_metadata(self, _broker: Optional[str] = None) -> Dict[str, InstrumentMetadata]:
        """
        Get all instrument metadata for a broker.
        
        Args:
            _broker: Broker name (optional, uses current_broker or returns all)
        
        Returns:
            Dict mapping symbol -> InstrumentMetadata
        
        Raises:
            ConfigurationError: If broker not found
        """
        broker = _broker or self._current_broker
        
        if not broker:
            # Return all instruments flattened
            result = {}
            for symbols in self._instruments.values():
                result.update(symbols)
            return result
        
        if broker not in self._instruments:
            raise ConfigurationError(
                f"Broker not found: {broker}", 
                {"available": list(self._instruments.keys())}
            )
        
        return self._instruments[broker].copy()
    
    def add_instrument(self, _instrument: InstrumentMetadata, _broker: Optional[str] = None) -> None:
        """
        Add or update instrument metadata programmatically.
        
        Args:
            _instrument: InstrumentMetadata instance
            _broker: Broker name (optional, uses instrument.broker or current_broker)
        
        Raises:
            ConfigurationError: If broker cannot be determined
        """
        broker = _broker or _instrument.broker or self._current_broker
        if not broker:
            raise ConfigurationError("Broker must be specified")
        
        if broker not in self._instruments:
            self._instruments[broker] = {}
        
        self._instruments[broker][_instrument.symbol] = _instrument
    
    def remove_instrument(self, _symbol: str, _broker: Optional[str] = None) -> bool:
        """
        Remove instrument metadata.
        
        Args:
            _symbol: Instrument symbol to remove
            _broker: Broker name (optional, removes from all if not specified)
        
        Returns:
            True if removed, False otherwise
        """
        broker = _broker or self._current_broker
        
        if not broker:
            # Try to remove from all brokers
            removed = False
            for broker_instruments in self._instruments.values():
                if _symbol in broker_instruments:
                    del broker_instruments[_symbol]
                    removed = True
            return removed
        
        if broker in self._instruments and _symbol in self._instruments[broker]:
            del self._instruments[broker][_symbol]
            return True
        return False
    
    def list_brokers(self) -> List[str]:
        """
        Get list of brokers with instrument configurations.
        
        Returns:
            List of broker names
        """
        return list(self._instruments.keys())
    
    def set_current_broker(self, _broker: str) -> None:
        """
        Set the current broker for instrument lookups.
        
        Args:
            _broker: Broker name
        """
        self._current_broker = _broker
    
    @property
    def current_broker(self) -> str:
        """Get current broker name."""
        return self._current_broker
    
    def reload(self) -> None:
        """Reload instruments from the original file."""
        if self._config_path:
            self.load(self._config_path)
    
    def save(self, _destination: str) -> None:
        """
        Save instruments to YAML file.
        
        Args:
            _destination: Path to save YAML file
        """
        # Convert InstrumentMetadata back to dict format
        data = {"instruments": {}}
        for broker, symbols in self._instruments.items():
            data["instruments"][broker] = {}
            for symbol, meta in symbols.items():
                data["instruments"][broker][symbol] = {
                    "pip_size": meta.pip_size,
                    "contract_size": meta.contract_size,
                    "commission": meta.commission,
                    "min_lot_size": meta.min_lot_size,
                    "max_lot_size": meta.max_lot_size,
                    "lot_step": meta.lot_step,
                    "currency_base": meta.currency_base,
                    "currency_quote": meta.currency_quote,
                    "description": meta.description,
                    "asset_class": meta.asset_class,
                }
        
        try:
            with open(_destination, "w", encoding="utf-8") as f:
                yaml.dump(data, f, default_flow_style=False, sort_keys=False)
        except IOError as e:
            raise ConfigurationError(f"Failed to save: {_destination}", {"error": str(e)})
    
    def to_dict(self) -> Dict[str, Any]:
        """
        Get complete instruments configuration as dictionary.
        
        Returns:
            Dict with broker -> symbol -> metadata dict
        """
        result = {}
        for broker, symbols in self._instruments.items():
            result[broker] = {sym: vars(inst) for sym, inst in symbols.items()}
        return result
