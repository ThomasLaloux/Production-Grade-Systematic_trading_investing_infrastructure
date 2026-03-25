"""
IB Broker Module
================
Interactive Brokers implementation via ib_insync.

Classes:
    BrokerIB(BrokerBase):
        - Interactive Brokers TWS/Gateway implementation

Usage:
    broker = BrokerIB(_config=config)
    broker.connect()
    order = broker.submit_order(_symbol="EURUSD", _order_type=OrderType.MARKET, _side=OrderSide.BUY, _quantity=0.1)
    positions = broker.get_positions(_symbol="EURUSD")
    broker.disconnect()
"""

from datetime import datetime
from typing import Any, Dict, List, Optional

from core.data_types import (
    Order, Position, PositionSide,
    OrderType, OrderSide, OrderStatus, InstrumentMetadata
)
from core.exceptions import BrokerConnectionError, OrderError
from .broker_base import BrokerBase

try:
    from ib_insync import IB, Forex, MarketOrder, LimitOrder, StopOrder
    IB_AVAILABLE = True
except ImportError:
    IB_AVAILABLE = False


class BrokerIB(BrokerBase):
    """Interactive Brokers TWS/Gateway implementation."""
    
    @property
    def broker_name(self) -> str:
        return "ib"
    
    def __init__(self, _config: Any):
        super().__init__(_config)
        self._ib: Optional[IB] = None
    
    def connect(self) -> bool:
        """Connect to TWS or IB Gateway."""
        if not IB_AVAILABLE:
            raise BrokerConnectionError("ib_insync package not installed", {"broker": self.broker_name})
        
        conn_config = self._broker_config.get("connection", {})
        host = conn_config.get("host", "127.0.0.1")
        port = conn_config.get("port", 7497)
        client_id = conn_config.get("client_id", 1)
        
        self._ib = IB()
        
        try:
            self._ib.connect(host, port, clientId=client_id)
            self._connected = True
            return True
        except Exception as e:
            raise BrokerConnectionError(f"IB connection failed: {e}", {"broker": self.broker_name})
    
    def disconnect(self) -> None:
        """Disconnect from IB."""
        if self._ib and self._ib.isConnected():
            self._ib.disconnect()
        self._connected = False
    
    def is_connected(self) -> bool:
        """Check connection status."""
        return self._ib is not None and self._ib.isConnected()
    
    def _get_contract(self, _symbol: str):
        """Create IB contract from symbol."""
        broker_symbol = self._map_symbol(_symbol)
        if "." in broker_symbol:
            base, quote = broker_symbol.split(".")
            return Forex(base + quote)
        return Forex(broker_symbol)
    
    def _map_order_status(self, _ib_status: str) -> OrderStatus:
        """Map IB status to OrderStatus."""
        status_map = {
            "Submitted": OrderStatus.PENDING,
            "Filled": OrderStatus.FILLED,
            "Cancelled": OrderStatus.CANCELLED,
            "Inactive": OrderStatus.REJECTED,
            "PendingSubmit": OrderStatus.PENDING,
            "PreSubmitted": OrderStatus.PENDING,
        }
        return status_map.get(_ib_status, OrderStatus.PENDING)
    
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
        """Submit order to IB."""
        if not self.is_connected():
            raise BrokerConnectionError("Not connected to IB", {"broker": self.broker_name})
        
        self._validate_order_params(_symbol, _order_type, _side, _quantity, _price)
        
        contract = self._get_contract(_symbol)
        action = "BUY" if _side == OrderSide.BUY else "SELL"
        
        if _order_type == OrderType.MARKET:
            ib_order = MarketOrder(action, _quantity)
        elif _order_type == OrderType.LIMIT:
            ib_order = LimitOrder(action, _quantity, _price)
        elif _order_type == OrderType.STOP:
            ib_order = StopOrder(action, _quantity, _price)
        else:
            raise OrderError(f"Unsupported order type: {_order_type.name}")
        
        trade = self._ib.placeOrder(contract, ib_order)
        self._ib.sleep(1)
        
        return Order(
            order_id=str(trade.order.orderId),
            client_order_id=_client_order_id or "",
            symbol=_symbol,
            order_type=_order_type,
            side=_side,
            quantity=_quantity,
            price=_price,
            stop_loss=_stop_loss,
            take_profit=_take_profit,
            status=self._map_order_status(trade.orderStatus.status),
            filled_quantity=trade.orderStatus.filled,
            average_fill_price=trade.orderStatus.avgFillPrice,
            created_at=datetime.now(),
            broker=self.broker_name,
            strategy=_strategy or "",
        )
    
    def cancel_order(self, _order_id: str) -> bool:
        """Cancel pending order."""
        if not self.is_connected():
            return False
        
        for trade in self._ib.openTrades():
            if str(trade.order.orderId) == _order_id:
                self._ib.cancelOrder(trade.order)
                return True
        return False
    
    def modify_order(
        self,
        _order_id: str,
        _price: Optional[float] = None,
        _stop_loss: Optional[float] = None,
        _take_profit: Optional[float] = None,
    ) -> Order:
        """Modify existing order."""
        raise NotImplementedError("IB order modification not implemented")
    
    def get_positions(
        self,
        _symbol: Optional[str] = None,
        _side: Optional[PositionSide] = None,
        _strategy: Optional[str] = None,
    ) -> List[Position]:
        """Get open positions."""
        if not self.is_connected():
            return []
        
        ib_positions = self._ib.positions()
        
        positions = []
        for pos in ib_positions:
            symbol = self._reverse_map_symbol(pos.contract.symbol)
            side = PositionSide.LONG if pos.position > 0 else PositionSide.SHORT
            
            positions.append(Position(
                position_id=f"{pos.contract.conId}",
                symbol=symbol,
                side=side,
                quantity=abs(pos.position),
                entry_price=pos.avgCost,
                broker=self.broker_name,
            ))
        
        return self._filter_positions(positions, _symbol, _side, _strategy)
    
    def get_pending_orders(self, _symbol: Optional[str] = None) -> List[Order]:
        """Get pending orders."""
        if not self.is_connected():
            return []
        
        orders = []
        for trade in self._ib.openTrades():
            symbol = self._reverse_map_symbol(trade.contract.symbol)
            if _symbol and symbol != _symbol:
                continue
            
            orders.append(Order(
                order_id=str(trade.order.orderId),
                symbol=symbol,
                quantity=trade.order.totalQuantity,
                price=trade.order.lmtPrice if trade.order.lmtPrice else None,
                status=self._map_order_status(trade.orderStatus.status),
                broker=self.broker_name,
            ))
        
        return orders
    
    def get_account_info(self) -> Dict[str, Any]:
        """Get account information."""
        if not self.is_connected():
            return {}
        
        account_values = self._ib.accountValues()
        
        info = {}
        for av in account_values:
            if av.tag == "NetLiquidation":
                info["equity"] = float(av.value)
            elif av.tag == "TotalCashValue":
                info["balance"] = float(av.value)
            elif av.tag == "MaintMarginReq":
                info["margin_used"] = float(av.value)
            elif av.tag == "AvailableFunds":
                info["margin_available"] = float(av.value)
            elif av.tag == "UnrealizedPnL":
                info["unrealized_pnl"] = float(av.value)
        
        return info
    
    def get_instrument_metadata(self, _symbol: str) -> InstrumentMetadata:
        """Get instrument metadata from config."""
        return self._config.get_instrument(_symbol, self.broker_name)

    def get_server_time(self, _symbol: str) -> datetime:
        """
        Get IB server time via ``ib.reqCurrentTime()``.

        Returns a timezone-aware UTC datetime. The *_symbol* parameter is
        accepted for interface conformance but not used — IB returns a
        single server clock.
        """
        if not self.is_connected():
            raise BrokerConnectionError("IB not connected")

        try:
            server_dt = self._ib.reqCurrentTime()
            # ib_insync returns a naive datetime in UTC; make it aware
            if server_dt.tzinfo is None:
                from datetime import timezone
                server_dt = server_dt.replace(tzinfo=timezone.utc)
            return server_dt
        except Exception as e:
            raise BrokerConnectionError(f"IB: failed to get server time — {e}")

    def get_tick_data(self, _symbol: str) -> Dict[str, Any]:
        """
        Get current bid/ask tick data from IB (P2.1).

        Uses ib_insync's reqMktData snapshot to get current
        bid/ask/last prices.

        Args:
            _symbol: Broker symbol (e.g. "XAUUSD").

        Returns:
            Dict with 'bid', 'ask', 'last', 'time'.
        """
        if not self.is_connected():
            raise BrokerConnectionError("IB not connected")

        try:
            contract = self._get_contract(_symbol)
            self._ib.qualifyContracts(contract)
            ticker = self._ib.reqMktData(contract, snapshot=True)
            self._ib.sleep(1)  # Wait for snapshot data

            bid = ticker.bid if ticker.bid and ticker.bid > 0 else 0.0
            ask = ticker.ask if ticker.ask and ticker.ask > 0 else 0.0
            last = ticker.last if ticker.last and ticker.last > 0 else 0.0

            self._ib.cancelMktData(contract)

            return {
                'bid': bid,
                'ask': ask,
                'last': last,
                'time': datetime.now(timezone.utc),
            }
        except BrokerConnectionError:
            raise
        except Exception as e:
            raise BrokerConnectionError(f"IB: failed to get tick data — {e}")
