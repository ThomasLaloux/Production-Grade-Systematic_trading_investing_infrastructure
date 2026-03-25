"""
Exceptions Module
=================
Custom exception classes for the trading system.

Classes:
    TradingSystemError(Exception):
        - base exception for all trading system errors
        - __init__(message, details)
        - __str__
    ConfigurationError(TradingSystemError):
        - configuration-related errors (missing files, invalid format)
    DataError(TradingSystemError):
        - data-related errors (missing data, validation failures)
    BrokerError(TradingSystemError):
        - broker-related errors (connection, API errors)
    BrokerConnectionError(BrokerError):
        - broker connection failures
    OrderError(BrokerError):
        - order submission/modification errors
    ValidationError(TradingSystemError):
        - input validation errors

Usage:
    raise ConfigurationError("File not found", {"path": "/config/instruments.yaml"})
    raise DataError("Missing data", {"symbol": "EURUSD", "timeframe": "H1"})
    raise BrokerError("API error", {"code": 401, "message": "Unauthorized"})
    raise BrokerConnectionError("Connection failed", {"broker": "oanda"})
    raise OrderError("Invalid quantity", {"quantity": -1, "min": 0.01})
    raise ValidationError("Invalid symbol", {"symbol": "XYZ", "parameter": "_symbol"})
    try:
        broker.connect()
    except BrokerConnectionError as e:
        print(f"Connection failed: {e}")
        print(f"Details: {e.details}")
"""

from typing import Any, Dict, Optional


class TradingSystemError(Exception):
    """Base exception for all trading system errors."""
    
    def __init__(self, _message: str, _details: Optional[Dict[str, Any]] = None):
        self.message = _message
        self.details = _details or {}
        super().__init__(self.message)
    
    def __str__(self) -> str:
        if self.details:
            return f"{self.message} | Details: {self.details}"
        return self.message


class ConfigurationError(TradingSystemError):
    """Configuration-related errors (missing files, invalid format)."""
    pass


class DataError(TradingSystemError):
    """Data-related errors (missing data, validation failures)."""
    pass


class BrokerError(TradingSystemError):
    """Broker-related errors (connection, API errors)."""
    pass


class BrokerConnectionError(BrokerError):
    """Broker connection failures."""
    pass


class OrderError(BrokerError):
    """Order submission/modification errors."""
    pass


class ValidationError(TradingSystemError):
    """Input validation errors."""
    
    def __init__(self, _message: str, _details: Optional[Dict[str, Any]] = None):
        super().__init__(_message, _details)
        self.parameter = _details.get("parameter") if _details else None
