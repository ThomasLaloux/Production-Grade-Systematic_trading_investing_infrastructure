"""
Data Types Module
=================
Fundamental data types, enums, and dataclasses for the trading system.

Classes:
    Timeframe(M1/M3/M5/M15/H1/H4/D1/W1/MN1/MN3):
        - supported timeframes for OHLCV data
        - to_minutes
        - to_pandas_freq
        - from_string
    OrderType(MARKET/LIMIT/STOP/STOP_LIMIT):
        - order execution types
    OrderSide(BUY/SELL):
        - order direction
    OrderStatus(PENDING/FILLED/PARTIALLY_FILLED/CANCELLED/REJECTED/EXPIRED):
        - order lifecycle status
    PositionSide(LONG/SHORT):
        - position direction
    DataSource(MT5/YAHOO/INTERACTIVE_BROKERS/OANDA/LOCAL):
        - supported data sources for historical data
    BrokerType(MT5/INTERACTIVE_BROKERS/OANDA):
        - supported brokers for live trading
    OHLCV(timestamp, open, high, low, close, volume, symbol, timeframe):
        - OHLCV price bar representation
        - to_dict
    InstrumentMetadata(symbol, pip_size, contract_size, commission, min_lot_size, max_lot_size, lot_step, currency_base, currency_quote, description, broker, asset_class):
        - instrument specification per broker, reusable across strategies
        - calculate_pip_value
    Order(order_id, client_order_id, symbol, order_type, side, quantity, price, stop_loss, take_profit, status, filled_quantity, average_fill_price, created_at, updated_at, broker, strategy, metadata):
        - order representation with full lifecycle tracking
    Position(position_id, client_position_id, symbol, side, quantity, entry_price, current_price, unrealized_pnl, realized_pnl, stop_loss, take_profit, opened_at, broker, strategy, metadata):
        - open position representation
        - update_pnl
    DataQualityIssue(timestamp, issue_type, description, severity, value, expected_range):
        - data quality issue found during validation

Usage:
    tf = Timeframe.H1                                       # get timeframe enum
    tf = Timeframe.from_string("H4")                        # parse from string
    minutes = Timeframe.H1.to_minutes()                     # convert to minutes (60)
    freq = Timeframe.H1.to_pandas_freq()                    # pandas frequency ("1h")
    otype = OrderType.MARKET                                # order type enum
    otype = OrderType.LIMIT                                 # limit order
    side = OrderSide.BUY                                    # buy side
    status = OrderStatus.PENDING                            # order pending
    status = OrderStatus.FILLED                             # order filled
    is_filled = (order.status == OrderStatus.FILLED)        # check if filled
    is_active = order.status in (OrderStatus.PENDING, OrderStatus.PARTIALLY_FILLED)
    pside = PositionSide.LONG                               # long position
    order = Order(symbol="EURUSD", order_type=OrderType.MARKET, side=OrderSide.BUY, quantity=0.1, strategy="momentum")
    order = Order(symbol="EURUSD", order_type=OrderType.LIMIT, side=OrderSide.SELL, quantity=0.5, price=1.1050)
    pos = Position(symbol="EURUSD", side=PositionSide.LONG, quantity=0.5, strategy="trend")
    pos.update_pnl(_current_price=1.105, _pip_value=10.0)   # update unrealized PnL
    meta = InstrumentMetadata(symbol="EURUSD", pip_size=0.0001, contract_size=100000, commission=0, min_lot_size=0.01, max_lot_size=100, broker="oanda")
    pip_val = meta.calculate_pip_value(_lot_size=1.0)       # calculate pip value
"""

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum, auto
from typing import Optional, Dict, Any


class Timeframe(Enum):
    """Supported timeframes for OHLCV data."""
    M1 = "M1"
    M3 = "M3"
    M5 = "M5"
    M15 = "M15"
    H1 = "H1"
    H4 = "H4"
    D1 = "D1"
    W1 = "W1"
    MN1 = "MN1"
    MN3 = "MN3"
    
    def to_minutes(self) -> int:
        """Convert timeframe to minutes."""
        mapping = {
            Timeframe.M1: 1, Timeframe.M3: 3, Timeframe.M5: 5, Timeframe.M15: 15,
            Timeframe.H1: 60, Timeframe.H4: 240, Timeframe.D1: 1440,
            Timeframe.W1: 10080, Timeframe.MN1: 43200, Timeframe.MN3: 129600,
        }
        return mapping[self]
    
    def to_pandas_freq(self) -> str:
        """Convert timeframe to pandas resample frequency string."""
        mapping = {
            Timeframe.M1: "1min", Timeframe.M3: "3min", Timeframe.M5: "5min",
            Timeframe.M15: "15min", Timeframe.H1: "1h", Timeframe.H4: "4h",
            Timeframe.D1: "1D", Timeframe.W1: "1W", Timeframe.MN1: "1ME", Timeframe.MN3: "1QE",
        }
        return mapping[self]
    
    @classmethod
    def from_string(cls, _timeframe_str: str) -> "Timeframe":
        """Parse timeframe from string (e.g., "H1", "H4", "D1")."""
        normalized = _timeframe_str.upper().strip()
        for tf in cls:
            if tf.value == normalized:
                return tf
        raise ValueError(f"Unknown timeframe: {_timeframe_str}. Valid: {[t.value for t in cls]}")


class OrderType(Enum):
    """Order execution types."""
    MARKET = auto()
    LIMIT = auto()
    STOP = auto()
    STOP_LIMIT = auto()


class OrderSide(Enum):
    """Order direction."""
    BUY = auto()
    SELL = auto()


class OrderStatus(Enum):
    """Order lifecycle status."""
    PENDING = auto()
    FILLED = auto()
    PARTIALLY_FILLED = auto()
    CANCELLED = auto()
    REJECTED = auto()
    EXPIRED = auto()


class PositionSide(Enum):
    """Position direction."""
    LONG = auto()
    SHORT = auto()


class DataSource(Enum):
    """Supported data sources for historical data."""
    MT5 = "mt5"
    YAHOO = "yahoo"
    INTERACTIVE_BROKERS = "ib"
    OANDA = "oanda"
    LOCAL = "local"


class BrokerType(Enum):
    """Supported brokers for live trading."""
    MT5 = "mt5"
    INTERACTIVE_BROKERS = "ib"
    OANDA = "oanda"


@dataclass
class OHLCV:
    """OHLCV price bar representation."""
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float
    symbol: str = ""
    timeframe: str = ""
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "timestamp": self.timestamp, "open": self.open, "high": self.high,
            "low": self.low, "close": self.close, "volume": self.volume,
            "symbol": self.symbol, "timeframe": self.timeframe,
        }


@dataclass
class InstrumentMetadata:
    """Instrument specification per broker, reusable across strategies."""
    symbol: str
    pip_size: float
    contract_size: float
    commission: float
    min_lot_size: float
    max_lot_size: float
    lot_step: float = 0.01
    currency_base: str = ""
    currency_quote: str = ""
    description: str = ""
    broker: str = ""
    asset_class: str = ""
    
    def calculate_pip_value(self, _lot_size: float) -> float:
        """Calculate pip value for given lot size."""
        return self.pip_size * self.contract_size * _lot_size


@dataclass
class Order:
    """Order representation with full lifecycle tracking."""
    order_id: str = ""
    client_order_id: str = ""
    symbol: str = ""
    order_type: OrderType = OrderType.MARKET
    side: OrderSide = OrderSide.BUY
    quantity: float = 0.0
    price: Optional[float] = None
    stop_loss: Optional[float] = None
    take_profit: Optional[float] = None
    status: OrderStatus = OrderStatus.PENDING
    filled_quantity: float = 0.0
    average_fill_price: float = 0.0
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    broker: str = ""
    strategy: str = ""
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class Position:
    """Open position representation."""
    position_id: str = ""
    client_position_id: str = ""
    symbol: str = ""
    side: PositionSide = PositionSide.LONG
    quantity: float = 0.0
    entry_price: float = 0.0
    current_price: float = 0.0
    unrealized_pnl: float = 0.0
    realized_pnl: float = 0.0
    stop_loss: Optional[float] = None
    take_profit: Optional[float] = None
    opened_at: Optional[datetime] = None
    broker: str = ""
    strategy: str = ""
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def update_pnl(self, _current_price: float, _pip_value: float) -> None:
        """Update unrealized PnL based on current price."""
        self.current_price = _current_price
        price_diff = _current_price - self.entry_price
        if self.side == PositionSide.SHORT:
            price_diff = -price_diff
        self.unrealized_pnl = price_diff * self.quantity * _pip_value


@dataclass
class DataQualityIssue:
    """Data quality issue found during validation."""
    timestamp: datetime
    issue_type: str
    description: str
    severity: str = "warning"
    value: Optional[float] = None
    expected_range: Optional[tuple] = None
