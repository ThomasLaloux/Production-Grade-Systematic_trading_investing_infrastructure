"""
MT5 Broker Base Module
======================
Base class for MetaTrader 5 broker implementations (Windows only).

Classes:
    BrokerMT5Base(BrokerBase):
        - Base MT5 broker with all MT5 API logic
        - Inherited by BrokerIcmMT5, BrokerBlackbullMT5
"""

from pathlib import Path
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import sys
sys.path.insert(0, str(Path(__file__).parent.parent))

from core.data_types import (
    Order, Position, PositionSide,
    OrderType, OrderSide, OrderStatus, InstrumentMetadata
)
from core.exceptions import BrokerConnectionError, OrderError
from .broker_base import BrokerBase

try:
    import MetaTrader5 as mt5
    MT5_AVAILABLE = True
except ImportError:
    MT5_AVAILABLE = False


class BrokerMT5Base(BrokerBase):
    """
    Base MetaTrader 5 broker implementation.
    
    Contains all MT5 API logic. Broker-specific subclasses only override:
    - broker_name property
    - Optional: symbol mappings via config
    """
    
    def connect(self) -> bool:
        """Connect to MT5 terminal."""
        if not MT5_AVAILABLE:
            raise BrokerConnectionError("MetaTrader5 package not installed (Windows only)", {"broker": self.broker_name})
        
        conn_config = self._broker_config.get("connection", {})
        path = conn_config.get("path")
        server = conn_config.get("server")
        login = conn_config.get("login")
        password = conn_config.get("password")
        timeout = conn_config.get("timeout", 60000)
        
        # Build init kwargs
        init_kwargs = {"timeout": timeout}
        if path:
            init_kwargs["path"] = path
        if login:
            init_kwargs["login"] = login
        if password:
            init_kwargs["password"] = password
        if server:
            init_kwargs["server"] = server
        
        if not mt5.initialize(**init_kwargs):
            error = mt5.last_error()
            raise BrokerConnectionError(f"MT5 initialize failed: {error}", {"broker": self.broker_name})
        
        # Verify connection
        terminal_info = mt5.terminal_info()
        if terminal_info is None:
            mt5.shutdown()
            raise BrokerConnectionError("MT5 terminal_info returned None", {"broker": self.broker_name})
        
        if not terminal_info.connected:
            mt5.shutdown()
            raise BrokerConnectionError("MT5 terminal not connected to trade server", {"broker": self.broker_name})
        
        self._connected = True
        return True
    
    def disconnect(self) -> None:
        """Disconnect from MT5."""
        if MT5_AVAILABLE and self._connected:
            mt5.shutdown()
        self._connected = False
    
    def is_connected(self) -> bool:
        """Check connection status."""
        if not MT5_AVAILABLE or not self._connected:
            return False
        
        terminal_info = mt5.terminal_info()
        return terminal_info is not None and terminal_info.connected
    
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
        _position_id: Optional[str] = None,
    ) -> Order:
        """Submit order to MT5."""
        if not self.is_connected():
            raise BrokerConnectionError("Not connected to MT5", {"broker": self.broker_name})
        
        self._validate_order_params(_symbol, _order_type, _side, _quantity, _price)
        
        broker_symbol = self._map_symbol(_symbol)
        symbol_info = mt5.symbol_info(broker_symbol)
        
        if symbol_info is None:
            raise OrderError(f"Symbol not found: {broker_symbol}", {"symbol": _symbol})
        
        if not symbol_info.visible:
            if not mt5.symbol_select(broker_symbol, True):
                raise OrderError(f"Failed to select symbol: {broker_symbol}", {"symbol": _symbol})
        
        # Map order types
        order_type_map = {
            (OrderType.MARKET, OrderSide.BUY): mt5.ORDER_TYPE_BUY,
            (OrderType.MARKET, OrderSide.SELL): mt5.ORDER_TYPE_SELL,
            (OrderType.LIMIT, OrderSide.BUY): mt5.ORDER_TYPE_BUY_LIMIT,
            (OrderType.LIMIT, OrderSide.SELL): mt5.ORDER_TYPE_SELL_LIMIT,
            (OrderType.STOP, OrderSide.BUY): mt5.ORDER_TYPE_BUY_STOP,
            (OrderType.STOP, OrderSide.SELL): mt5.ORDER_TYPE_SELL_STOP,
        }
        
        mt5_order_type = order_type_map.get((_order_type, _side))
        if mt5_order_type is None:
            raise OrderError(f"Unsupported order type: {_order_type.name} {_side.name}")
        
        # Get current price for market orders
        tick = mt5.symbol_info_tick(broker_symbol)
        if tick is None:
            raise OrderError(f"Failed to get tick for: {broker_symbol}", {"symbol": _symbol})
        
        price = tick.ask if _side == OrderSide.BUY else tick.bid
        
        # Build request
        request = {
            "action": mt5.TRADE_ACTION_DEAL if _order_type == OrderType.MARKET else mt5.TRADE_ACTION_PENDING,
            "symbol": broker_symbol,
            "volume": float(_quantity),
            "type": mt5_order_type,
            "price": _price if _price and _order_type != OrderType.MARKET else price,
            "deviation": 20,
            "magic": hash(_strategy or "") % 1000000 if _strategy else 0,
            "comment": f"py_{_strategy or 'manual'}",
            "type_time": mt5.ORDER_TIME_GTC,
            "type_filling": mt5.ORDER_FILLING_IOC,
        }
        
        if _stop_loss:
            request["sl"] = float(_stop_loss)
        if _take_profit:
            request["tp"] = float(_take_profit)
        if _position_id:
             request["position"] = int(_position_id)  # Add this line

        # Send order
        result = mt5.order_send(request)
        
        if result is None:
            error = mt5.last_error()
            raise OrderError(f"Order send returned None: {error}", {"symbol": _symbol})
        
        if result.retcode != mt5.TRADE_RETCODE_DONE:
            raise OrderError(
                f"Order failed: {result.comment}",
                {"retcode": result.retcode, "symbol": _symbol}
            )
        
        return Order(
            order_id=str(result.order),
            client_order_id=_client_order_id or "",
            symbol=_symbol,
            order_type=_order_type,
            side=_side,
            quantity=_quantity,
            price=result.price,
            stop_loss=_stop_loss,
            take_profit=_take_profit,
            status=OrderStatus.FILLED if _order_type == OrderType.MARKET else OrderStatus.PENDING,
            filled_quantity=result.volume,
            average_fill_price=result.price,
            created_at=datetime.now(),
            broker=self.broker_name,
            strategy=_strategy or "",
        )
    
    def cancel_order(self, _order_id: str) -> bool:
        """Cancel pending order."""
        if not self.is_connected():
            return False
        
        request = {
            "action": mt5.TRADE_ACTION_REMOVE,
            "order": int(_order_id),
        }
        result = mt5.order_send(request)
        return result is not None and result.retcode == mt5.TRADE_RETCODE_DONE
    
    def modify_order(
        self,
        _order_id: str,
        _price: Optional[float] = None,
        _stop_loss: Optional[float] = None,
        _take_profit: Optional[float] = None,
    ) -> Order:
        """Modify existing pending order."""
        if not self.is_connected():
            raise BrokerConnectionError("Not connected to MT5", {"broker": self.broker_name})
        
        # Get existing order
        orders = mt5.orders_get(ticket=int(_order_id))
        if not orders:
            raise OrderError(f"Order not found: {_order_id}")
        
        order = orders[0]
        
        request = {
            "action": mt5.TRADE_ACTION_MODIFY,
            "order": int(_order_id),
            "symbol": order.symbol,
            "price": _price if _price else order.price_open,
            "sl": _stop_loss if _stop_loss else order.sl,
            "tp": _take_profit if _take_profit else order.tp,
            "type_time": order.type_time,
            "expiration": order.time_expiration,
        }
        
        result = mt5.order_send(request)
        
        if result is None or result.retcode != mt5.TRADE_RETCODE_DONE:
            error_msg = result.comment if result else "Unknown error"
            raise OrderError(f"Order modification failed: {error_msg}")
        
        symbol = self._reverse_map_symbol(order.symbol)
        return Order(
            order_id=_order_id,
            symbol=symbol,
            quantity=order.volume_current,
            price=_price if _price else order.price_open,
            stop_loss=_stop_loss if _stop_loss else order.sl,
            take_profit=_take_profit if _take_profit else order.tp,
            status=OrderStatus.PENDING,
            created_at=datetime.fromtimestamp(order.time_setup),
            broker=self.broker_name,
        )
    
    def get_positions(
        self,
        _symbol: Optional[str] = None,
        _side: Optional[PositionSide] = None,
        _strategy: Optional[str] = None,
    ) -> List[Position]:
        """Get open positions."""
        if not self.is_connected():
            return []
        
        mt5_positions = mt5.positions_get()
        if mt5_positions is None:
            return []
        
        positions = []
        for pos in mt5_positions:
            symbol = self._reverse_map_symbol(pos.symbol)
            side = PositionSide.LONG if pos.type == mt5.POSITION_TYPE_BUY else PositionSide.SHORT
            
            positions.append(Position(
                position_id=str(pos.ticket),
                symbol=symbol,
                side=side,
                quantity=pos.volume,
                entry_price=pos.price_open,
                current_price=pos.price_current,
                unrealized_pnl=pos.profit,
                stop_loss=pos.sl if pos.sl > 0 else None,
                take_profit=pos.tp if pos.tp > 0 else None,
                opened_at=datetime.fromtimestamp(pos.time),
                broker=self.broker_name,
                strategy=str(pos.magic) if pos.magic else "",
            ))
        
        return self._filter_positions(positions, _symbol, _side, _strategy)
    
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
    
    def get_pending_orders(self, _symbol: Optional[str] = None) -> List[Order]:
        """Get pending orders."""
        if not self.is_connected():
            return []
        
        mt5_orders = mt5.orders_get()
        if mt5_orders is None:
            return []
        
        orders = []
        for order in mt5_orders:
            symbol = self._reverse_map_symbol(order.symbol)
            if _symbol and symbol != _symbol:
                continue
            
            # Map MT5 order type to our OrderType/OrderSide
            side = OrderSide.BUY if order.type in [
                mt5.ORDER_TYPE_BUY, mt5.ORDER_TYPE_BUY_LIMIT, mt5.ORDER_TYPE_BUY_STOP
            ] else OrderSide.SELL
            
            if order.type in [mt5.ORDER_TYPE_BUY_LIMIT, mt5.ORDER_TYPE_SELL_LIMIT]:
                order_type = OrderType.LIMIT
            elif order.type in [mt5.ORDER_TYPE_BUY_STOP, mt5.ORDER_TYPE_SELL_STOP]:
                order_type = OrderType.STOP
            else:
                order_type = OrderType.MARKET
            
            orders.append(Order(
                order_id=str(order.ticket),
                symbol=symbol,
                order_type=order_type,
                side=side,
                quantity=order.volume_current,
                price=order.price_open,
                stop_loss=order.sl if order.sl > 0 else None,
                take_profit=order.tp if order.tp > 0 else None,
                status=OrderStatus.PENDING,
                created_at=datetime.fromtimestamp(order.time_setup),
                broker=self.broker_name,
                strategy=str(order.magic) if order.magic else "",
            ))
        
        return orders
    
    def get_account_info(self) -> Dict[str, Any]:
        """Get account information."""
        if not self.is_connected():
            return {}
        
        account = mt5.account_info()
        if account is None:
            return {}
        
        return {
            "balance": account.balance,
            "equity": account.equity,
            "margin_used": account.margin,
            "margin_available": account.margin_free,
            "unrealized_pnl": account.profit,
            "currency": account.currency,
            "leverage": account.leverage,
            "login": account.login,
            "server": account.server,
            "name": account.name,
            "trade_allowed": account.trade_allowed,
        }
    
    def get_instrument_metadata(self, _symbol: str) -> InstrumentMetadata:
        """Get instrument metadata from config."""
        return self._config.get_instrument(_symbol, self.broker_name)
    
    def get_server_time(self, _symbol: str) -> datetime:
        """
        Get MT5 server time via the latest tick timestamp for *_symbol*.

        Falls back to mt5.symbol_info_tick().time which is a Unix epoch int
        representing the broker's trade-server clock in UTC.
        """
        if not self._connected:
            raise BrokerConnectionError("MT5 not connected")

        broker_symbol = self._map_symbol(_symbol)
        tick = mt5.symbol_info_tick(broker_symbol)
        if tick is None:
            raise BrokerConnectionError(
                f"MT5: no tick data for {broker_symbol} — symbol may be invalid"
            )
        return datetime.fromtimestamp(tick.time, tz=timezone.utc)

    def get_tick_data(self, _symbol: str) -> Dict[str, Any]:
        """
        Get current bid/ask/last tick data from MT5 (P2.1).

        Uses mt5.symbol_info_tick() which returns the latest tick
        with bid, ask, last, and time fields.

        Args:
            _symbol: Broker symbol (e.g. "XAUUSDp").

        Returns:
            Dict with 'bid', 'ask', 'last', 'time'.
        """
        if not self._connected:
            raise BrokerConnectionError("MT5 not connected")

        broker_symbol = self._map_symbol(_symbol)
        tick = mt5.symbol_info_tick(broker_symbol)
        if tick is None:
            raise BrokerConnectionError(
                f"MT5: no tick data for {broker_symbol} — symbol may be invalid"
            )

        return {
            'bid': tick.bid,
            'ask': tick.ask,
            'last': tick.last,
            'time': datetime.fromtimestamp(tick.time, tz=timezone.utc),
        }

    @property
    def broker_name(self) -> str:
        """Must be overridden by subclasses."""
        raise NotImplementedError("Subclasses must implement broker_name")
