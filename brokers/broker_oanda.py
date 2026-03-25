"""
Oanda Broker Module
===================
Oanda fxTrade API implementation.

Classes:
    BrokerOanda(BrokerBase):
        - oanda fxTrade REST API v20 implementation

Usage:
    broker = BrokerOanda(_config=config)
    broker.connect()
    order = broker.submit_order(_symbol="EURUSD", _order_type=OrderType.MARKET, _side=OrderSide.BUY, _quantity=0.1)
    positions = broker.get_positions(_symbol="EURUSD", _side=PositionSide.LONG)
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
    import requests
    REQUESTS_AVAILABLE = True
except ImportError:
    REQUESTS_AVAILABLE = False


class BrokerOanda(BrokerBase):
    """Oanda fxTrade REST API v20 implementation."""
    
    ENDPOINTS = {
        "practice": "https://api-fxpractice.oanda.com",
        "live": "https://api-fxtrade.oanda.com",
    }
    
    @property
    def broker_name(self) -> str:
        return "oanda"
    
    def __init__(self, _config: Any):
        super().__init__(_config)
        self._session: Optional[requests.Session] = None
        self._account_id: str = ""
        self._base_url: str = ""
    
    def connect(self) -> bool:
        """Connect to Oanda API."""
        if not REQUESTS_AVAILABLE:
            raise BrokerConnectionError("requests library not installed", {"broker": self.broker_name})
        
        api_config = self._broker_config.get("api", {})
        token = api_config.get("token", "")
        self._account_id = api_config.get("account_id", "")
        environment = api_config.get("environment", "practice")
        
        if not token or not self._account_id:
            raise BrokerConnectionError("Missing API token or account ID", {"broker": self.broker_name})
        
        self._base_url = self.ENDPOINTS.get(environment, self.ENDPOINTS["practice"])
        
        self._session = requests.Session()
        self._session.headers.update({
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        })
        
        try:
            response = self._session.get(f"{self._base_url}/v3/accounts/{self._account_id}/summary")
            if response.status_code == 200:
                self._connected = True
                return True
            else:
                raise BrokerConnectionError(f"Connection failed: {response.status_code}", {"response": response.text})
        except requests.RequestException as e:
            raise BrokerConnectionError(f"Connection error: {e}", {"broker": self.broker_name})
    
    def disconnect(self) -> None:
        """Disconnect from Oanda API."""
        if self._session:
            self._session.close()
            self._session = None
        self._connected = False
    
    def is_connected(self) -> bool:
        """Check if connected."""
        return self._connected and self._session is not None
    
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
        """Submit order to Oanda."""
        if not self.is_connected():
            raise BrokerConnectionError("Not connected to Oanda", {"broker": self.broker_name})
        
        self._validate_order_params(_symbol, _order_type, _side, _quantity, _price)
        
        broker_symbol = self._map_symbol(_symbol)
        units = int(_quantity * 100000)
        if _side == OrderSide.SELL:
            units = -units
        
        order_data: Dict[str, Any] = {"instrument": broker_symbol, "units": str(units)}
        
        if _order_type == OrderType.MARKET:
            order_data["type"] = "MARKET"
        elif _order_type == OrderType.LIMIT:
            order_data["type"] = "LIMIT"
            order_data["price"] = str(_price)
        elif _order_type == OrderType.STOP:
            order_data["type"] = "STOP"
            order_data["price"] = str(_price)
        elif _order_type == OrderType.STOP_LIMIT:
            order_data["type"] = "STOP"
            order_data["price"] = str(_price)
        
        if _stop_loss:
            order_data["stopLossOnFill"] = {"price": str(_stop_loss)}
        if _take_profit:
            order_data["takeProfitOnFill"] = {"price": str(_take_profit)}
        if _client_order_id:
            order_data["clientExtensions"] = {"id": _client_order_id}
        
        try:
            response = self._session.post(
                f"{self._base_url}/v3/accounts/{self._account_id}/orders",
                json={"order": order_data}
            )
            
            if response.status_code in (200, 201):
                result = response.json()
                order_fill = result.get("orderFillTransaction", {})
                order_create = result.get("orderCreateTransaction", {})
                
                return Order(
                    order_id=order_fill.get("id", order_create.get("id", "")),
                    client_order_id=_client_order_id or "",
                    symbol=_symbol,
                    order_type=_order_type,
                    side=_side,
                    quantity=_quantity,
                    price=_price,
                    stop_loss=_stop_loss,
                    take_profit=_take_profit,
                    status=OrderStatus.FILLED if order_fill else OrderStatus.PENDING,
                    filled_quantity=_quantity if order_fill else 0.0,
                    average_fill_price=float(order_fill.get("price", 0)) if order_fill else 0.0,
                    created_at=datetime.now(),
                    broker=self.broker_name,
                    strategy=_strategy or "",
                )
            else:
                raise OrderError(f"Order rejected: {response.text}", {"status": response.status_code})
        except requests.RequestException as e:
            raise OrderError(f"Order submission failed: {e}", {"symbol": _symbol})
    
    def cancel_order(self, _order_id: str) -> bool:
        """Cancel pending order."""
        if not self.is_connected():
            raise BrokerConnectionError("Not connected", {"broker": self.broker_name})
        
        try:
            response = self._session.put(f"{self._base_url}/v3/accounts/{self._account_id}/orders/{_order_id}/cancel")
            return response.status_code == 200
        except requests.RequestException:
            return False
    
    def modify_order(
        self,
        _order_id: str,
        _price: Optional[float] = None,
        _stop_loss: Optional[float] = None,
        _take_profit: Optional[float] = None,
    ) -> Order:
        """Modify existing order (cancel and replace)."""
        if not self.is_connected():
            raise BrokerConnectionError("Not connected", {"broker": self.broker_name})
        
        raise NotImplementedError("Order modification via cancel/replace not implemented")
    
    def get_positions(
        self,
        _symbol: Optional[str] = None,
        _side: Optional[PositionSide] = None,
        _strategy: Optional[str] = None,
    ) -> List[Position]:
        """Get open positions with optional filters."""
        if not self.is_connected():
            raise BrokerConnectionError("Not connected", {"broker": self.broker_name})
        
        try:
            response = self._session.get(f"{self._base_url}/v3/accounts/{self._account_id}/openPositions")
            
            if response.status_code != 200:
                return []
            
            positions = []
            for pos_data in response.json().get("positions", []):
                instrument = pos_data.get("instrument", "")
                symbol = self._reverse_map_symbol(instrument)
                
                long_units = float(pos_data.get("long", {}).get("units", 0))
                short_units = float(pos_data.get("short", {}).get("units", 0))
                
                if long_units > 0:
                    positions.append(Position(
                        position_id=f"{instrument}_long",
                        symbol=symbol,
                        side=PositionSide.LONG,
                        quantity=long_units / 100000,
                        entry_price=float(pos_data.get("long", {}).get("averagePrice", 0)),
                        unrealized_pnl=float(pos_data.get("long", {}).get("unrealizedPL", 0)),
                        broker=self.broker_name,
                    ))
                
                if abs(short_units) > 0:
                    positions.append(Position(
                        position_id=f"{instrument}_short",
                        symbol=symbol,
                        side=PositionSide.SHORT,
                        quantity=abs(short_units) / 100000,
                        entry_price=float(pos_data.get("short", {}).get("averagePrice", 0)),
                        unrealized_pnl=float(pos_data.get("short", {}).get("unrealizedPL", 0)),
                        broker=self.broker_name,
                    ))
            
            return self._filter_positions(positions, _symbol, _side, _strategy)
        except requests.RequestException:
            return []
    
    def get_pending_orders(self, _symbol: Optional[str] = None) -> List[Order]:
        """Get pending orders."""
        if not self.is_connected():
            raise BrokerConnectionError("Not connected", {"broker": self.broker_name})
        
        try:
            response = self._session.get(f"{self._base_url}/v3/accounts/{self._account_id}/pendingOrders")
            
            if response.status_code != 200:
                return []
            
            orders = []
            for order_data in response.json().get("orders", []):
                symbol = self._reverse_map_symbol(order_data.get("instrument", ""))
                
                if _symbol and symbol != _symbol:
                    continue
                
                orders.append(Order(
                    order_id=order_data.get("id", ""),
                    symbol=symbol,
                    quantity=abs(float(order_data.get("units", 0))) / 100000,
                    price=float(order_data.get("price", 0)),
                    status=OrderStatus.PENDING,
                    broker=self.broker_name,
                ))
            
            return orders
        except requests.RequestException:
            return []
    
    def get_account_info(self) -> Dict[str, Any]:
        """Get account information."""
        if not self.is_connected():
            raise BrokerConnectionError("Not connected", {"broker": self.broker_name})
        
        try:
            response = self._session.get(f"{self._base_url}/v3/accounts/{self._account_id}/summary")
            
            if response.status_code == 200:
                account = response.json().get("account", {})
                return {
                    "balance": float(account.get("balance", 0)),
                    "equity": float(account.get("NAV", 0)),
                    "margin_used": float(account.get("marginUsed", 0)),
                    "margin_available": float(account.get("marginAvailable", 0)),
                    "unrealized_pnl": float(account.get("unrealizedPL", 0)),
                    "currency": account.get("currency", "USD"),
                }
            return {}
        except requests.RequestException:
            return {}
    
    def get_instrument_metadata(self, _symbol: str) -> InstrumentMetadata:
        """Get instrument metadata from config."""
        return self._config.get_instrument(_symbol, self.broker_name)

    def get_server_time(self, _symbol: str) -> datetime:
        """
        Get Oanda server time via the pricing endpoint for *_symbol*.

        Parses the ``time`` field from the ``/v3/accounts/{id}/pricing``
        response, which is the Oanda server's trade-clock timestamp in
        RFC 3339 format (UTC).
        """
        if not self.is_connected():
            raise BrokerConnectionError("Oanda not connected")

        broker_symbol = self._map_symbol(_symbol)
        try:
            response = self._session.get(
                f"{self._base_url}/v3/accounts/{self._account_id}/pricing",
                params={"instruments": broker_symbol},
            )
            response.raise_for_status()
            data = response.json()
            # Oanda returns time as '2024-01-15T12:30:00.000000000Z'
            time_str = data.get("time", "")
            # Strip nanosecond precision to microsecond for fromisoformat
            if "." in time_str:
                base, frac = time_str.split(".")
                frac = frac.rstrip("Z")[:6]
                time_str = f"{base}.{frac}+00:00"
            else:
                time_str = time_str.rstrip("Z") + "+00:00"
            return datetime.fromisoformat(time_str)
        except Exception as e:
            raise BrokerConnectionError(f"Oanda: failed to get server time — {e}")

    def get_tick_data(self, _symbol: str) -> Dict[str, Any]:
        """
        Get current bid/ask tick data from Oanda pricing endpoint (P2.1).

        Uses the /v3/accounts/{id}/pricing endpoint which returns
        current bid/ask prices for the instrument.

        Args:
            _symbol: Broker symbol (e.g. "EUR_USD").

        Returns:
            Dict with 'bid', 'ask', 'last', 'time'.
        """
        if not self.is_connected():
            raise BrokerConnectionError("Oanda not connected")

        broker_symbol = self._map_symbol(_symbol)
        try:
            response = self._session.get(
                f"{self._base_url}/v3/accounts/{self._account_id}/pricing",
                params={"instruments": broker_symbol},
            )
            response.raise_for_status()
            data = response.json()

            prices = data.get("prices", [])
            if not prices:
                raise BrokerConnectionError(
                    f"Oanda: no pricing data for {broker_symbol}"
                )

            price_data = prices[0]
            bids = price_data.get("bids", [])
            asks = price_data.get("asks", [])

            bid = float(bids[0]["price"]) if bids else 0.0
            ask = float(asks[0]["price"]) if asks else 0.0
            mid = (bid + ask) / 2 if (bid > 0 and ask > 0) else 0.0

            return {
                'bid': bid,
                'ask': ask,
                'last': mid,  # Oanda doesn't provide last trade price; use mid
                'time': datetime.now(timezone.utc),
            }
        except BrokerConnectionError:
            raise
        except Exception as e:
            raise BrokerConnectionError(f"Oanda: failed to get tick data — {e}")
