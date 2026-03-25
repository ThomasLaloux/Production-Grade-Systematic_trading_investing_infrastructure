"""
Broker Base Module
==================
Abstract base class for all broker implementations.

Classes:
    BrokerBase(ABC):
        - abstract base class for all broker implementations
        - connect (abstract)
        - disconnect (abstract)
        - is_connected (abstract)
        - submit_order (abstract)
        - cancel_order (abstract)
        - modify_order (abstract)
        - get_positions (abstract)
        - get_pending_orders (abstract)
        - get_account_info (abstract)
        - get_instrument_metadata (abstract)
        - get_server_time (abstract)
        - broker_name (property, abstract)
        - has_open_position (implemented)
        - count_positions (implemented)
        - close_position (implemented)

Usage:
    # BrokerBase is inherited by: BrokerOanda, BrokerMT5, BrokerIB
    # Instance creation is handled by BrokerManager.create()
"""

from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import sys
sys.path.insert(0, str(Path(__file__).parent.parent))

from core.data_types import (
    Timeframe, Order, Position, PositionSide,
    OrderType, OrderSide, OrderStatus, InstrumentMetadata
)
from core.exceptions import BrokerError, OrderError, BrokerConnectionError


class BrokerBase(ABC):
    """Abstract base class for all broker implementations."""
    
    def __init__(self, _config: Any):
        self._config = _config
        self._connected = False
        self._broker_config: Dict[str, Any] = {}
        self._symbol_mapping: Dict[str, str] = {}
        self._reverse_mapping: Dict[str, str] = {}
        self._positions: List[Position] = []
        self._load_config()
    
    def _load_config(self) -> None:
        """Load broker-specific configuration."""
        try:
            self._broker_config = self._config.get_broker_config(self.broker_name)
            self._symbol_mapping = self._broker_config.get("symbol_mapping", {})
            self._reverse_mapping = {v: k for k, v in self._symbol_mapping.items()}
        except Exception:
            self._broker_config = {}
    
    def _map_symbol(self, _symbol: str) -> str:
        """Map standard symbol to broker-specific symbol."""
        return self._symbol_mapping.get(_symbol, _symbol)
    
    def _reverse_map_symbol(self, _broker_symbol: str) -> str:
        """Map broker-specific symbol back to standard symbol."""
        return self._reverse_mapping.get(_broker_symbol, _broker_symbol)
    
    def _validate_order_params(
        self,
        _symbol: str,
        _order_type: OrderType,
        _side: OrderSide,
        _quantity: float,
        _price: Optional[float] = None,
    ) -> None:
        """Validate order parameters before submission."""
        if _quantity <= 0:
            raise OrderError("Quantity must be positive", {"quantity": _quantity})
        
        if _order_type in (OrderType.LIMIT, OrderType.STOP_LIMIT) and _price is None:
            raise OrderError(f"{_order_type.name} order requires price", {"order_type": _order_type.name})
        
        try:
            instrument = self._config.get_instrument(_symbol, self.broker_name)
            if _quantity < instrument.min_lot_size:
                raise OrderError(f"Quantity below minimum {instrument.min_lot_size}", {"quantity": _quantity, "min": instrument.min_lot_size})
            if _quantity > instrument.max_lot_size:
                raise OrderError(f"Quantity above maximum {instrument.max_lot_size}", {"quantity": _quantity, "max": instrument.max_lot_size})
        except Exception:
            pass
    
    def _filter_positions(
        self,
        _positions: List[Position],
        _symbol: Optional[str] = None,
        _side: Optional[PositionSide] = None,
        _strategy: Optional[str] = None,
    ) -> List[Position]:
        """Filter positions by symbol, side, and/or strategy."""
        result = _positions
        if _symbol is not None:
            result = [p for p in result if p.symbol == _symbol]
        if _side is not None:
            result = [p for p in result if p.side == _side]
        if _strategy is not None:
            result = [p for p in result if p.strategy == _strategy]
        return result
    
    def has_open_position(
        self,
        _symbol: Optional[str] = None,
        _side: Optional[PositionSide] = None,
        _strategy: Optional[str] = None,
    ) -> bool:
        """Check if any open position exists matching filters."""
        positions = self.get_positions(_symbol, _side, _strategy)
        return len(positions) > 0
    
    def count_positions(
        self,
        _symbol: Optional[str] = None,
        _side: Optional[PositionSide] = None,
        _strategy: Optional[str] = None,
    ) -> int:
        """Count open positions matching filters."""
        positions = self.get_positions(_symbol, _side, _strategy)
        return len(positions)
    
    def close_position(
        self, 
        _position_id: str
    ) -> Order:
        """Close a specific position by ID."""
        positions = self.get_positions()
        pos = next((p for p in positions if p.position_id == _position_id), None)
        
        if not pos:
            raise OrderError(f"Position not found: {_position_id}")
        
        close_side = OrderSide.SELL if pos.side == PositionSide.LONG else OrderSide.BUY
        
        return self.submit_order(
            _symbol=pos.symbol,
            _order_type=OrderType.MARKET,
            _side=close_side,
            _quantity=pos.quantity,
            _position_id=_position_id,
        )
    
    @abstractmethod
    def connect(self) -> bool:
        """Connect to broker. Returns True if successful."""
        pass
    
    @abstractmethod
    def disconnect(self) -> None:
        """Disconnect from broker."""
        pass
    
    @abstractmethod
    def is_connected(self) -> bool:
        """Check if connected to broker."""
        pass
    
    @abstractmethod
    def submit_order(
        self,
        _symbol: str,
        _order_type: OrderType,
        _side: OrderSide,
        _quantity: float,
        _price: Optional[float] = None,
        _stop_loss: Optional[float] = None,
        _take_profit: Optional[float] = None,
        _client_order_id: Optional[str] = None,
        _strategy: Optional[str] = None,
    ) -> Order:
        """Submit order to broker. Returns Order with broker-assigned ID."""
        pass
    
    @abstractmethod
    def cancel_order(self, _order_id: str) -> bool:
        """Cancel pending order. Returns True if successful."""
        pass
    
    @abstractmethod
    def modify_order(
        self,
        _order_id: str,
        _price: Optional[float] = None,
        _stop_loss: Optional[float] = None,
        _take_profit: Optional[float] = None,
    ) -> Order:
        """Modify existing order. Returns updated Order."""
        pass
    
    @abstractmethod
    def get_positions(
        self,
        _symbol: Optional[str] = None,
        _side: Optional[PositionSide] = None,
        _strategy: Optional[str] = None,
    ) -> List[Position]:
        """Get open positions with optional filters."""
        pass
    
    @abstractmethod
    def get_pending_orders(self, _symbol: Optional[str] = None) -> List[Order]:
        """Get pending orders, optionally filtered by symbol."""
        pass
    
    @abstractmethod
    def get_account_info(self) -> Dict[str, Any]:
        """Get account information (balance, equity, margin, etc.)."""
        pass
    
    @abstractmethod
    def get_instrument_metadata(self, _symbol: str) -> InstrumentMetadata:
        """Get instrument specifications."""
        pass
    
    @abstractmethod
    def get_server_time(self, _symbol: str) -> datetime:
        """
        Get the broker server's current timestamp (UTC).

        Used by BarTimer to detect bar-close boundaries without relying
        on the local clock, which may drift or differ from the broker's
        trade-server time.

        Args:
            _symbol: Symbol to query (some brokers return per-symbol tick
                     timestamps; others return a single server clock).

        Returns:
            Timezone-aware datetime in UTC.
        """
        pass

    @abstractmethod
    def get_tick_data(self, _symbol: str) -> Dict[str, Any]:
        """
        Get current bid/ask/last tick data for a symbol (P2.1).

        Used by SpreadFilter to check bid-ask spread before order
        submission.

        Args:
            _symbol: Broker symbol to query (e.g. "XAUUSDp").

        Returns:
            Dict with keys: 'bid', 'ask', 'last', 'time'.
            - bid: current best bid price (float)
            - ask: current best ask price (float)
            - last: last trade price (float, may equal bid or ask)
            - time: tick timestamp as datetime (UTC)
        """
        pass
    
    @property
    @abstractmethod
    def broker_name(self) -> str:
        """Get broker name."""
        pass
