"""
Order Executor Module (P2.1 + P2.2)
======================================
Handles order submission with SL/TP attached (broker-managed).

Features:
    - Market and limit order support
    - SL/TP attached to the order (broker handles intrabar execution)
    - Client order ID for idempotency (P2.2)
    - Partial fill handling: re-enter unfilled quantity at market (P2.1)
    - Execution quality monitoring integration (P2.2)
    - Logging of all order submissions

Idempotent Order Submission (P2.2):
    Uses _client_order_id to prevent duplicate orders on retries after
    timeouts. Signal timestamps are mapped to deterministic client order
    IDs using a hash of (strategy, symbol, timeframe, signal_timestamp,
    direction). This ensures:
        - Retrying the same signal produces the same client_order_id
        - The broker rejects duplicates (if it supports idempotency)
        - Local tracking prevents re-submission of already-sent orders

    The executor maintains a set of submitted client_order_ids for the
    current session. If a client_order_id has already been submitted,
    the order is skipped with a warning.

Partial Fill Handling (P2.1):
    For limit orders, the broker may only fill a portion of the requested
    quantity. When a partial fill is detected (filled_quantity < requested),
    the executor automatically re-enters for the unfilled remainder at
    market price, preserving the same SL/TP levels. This ensures the
    strategy's intended position size is achieved.

    For market orders, partial fills are rare but handled similarly:
    if filled_quantity < requested, a follow-up market order is submitted
    for the remainder.

    Configurable:
        _max_partial_fill_retries: max re-entry attempts (prevents loops)
        _partial_fill_enabled: enable/disable partial fill re-entry

Classes:
    OrderExecutor:
        - submit_market_order: place a market order with SL/TP
        - submit_limit_order: place a limit order with SL/TP
        - generate_client_order_id: deterministic ID from signal context
        - _handle_partial_fill: re-enter for unfilled quantity

Usage:
    executor = OrderExecutor(
        _broker=broker,
        _partial_fill_enabled=True,
        _max_partial_fill_retries=3,
        _idempotency_enabled=True,
    )
    # Generate deterministic client_order_id from signal context
    client_id = executor.generate_client_order_id(
        _strategy="trend_retracement", _symbol="XAUUSDp",
        _timeframe="M15", _signal_timestamp="2025-01-15T14:30:00",
        _direction="BUY",
    )
    order = executor.submit_market_order(
        _symbol="XAUUSDp", _side=OrderSide.BUY, _quantity=0.1,
        _stop_loss=1840.0, _take_profit=1870.0, _strategy="trend_retracement",
        _client_order_id=client_id,
    )
"""

import hashlib
import logging
import time
from typing import List, Optional, Set

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from core.data_types import Order, OrderType, OrderSide
from core.exceptions import OrderError

logger = logging.getLogger(__name__)


class OrderExecutor:
    """
    Handles order submission with SL/TP attached to the order.

    SL/TP are delegated to the broker for intrabar execution.
    The strategy only evaluates signals at bar close; the broker
    manages intrabar SL/TP fills natively.

    P2.1: Partial fill handling — if a limit or market order is only
    partially filled, the executor re-enters for the remainder at market.

    P2.2: Idempotent order submission — deterministic client_order_id
    prevents duplicate orders on retries. Execution quality monitoring
    integration for fill latency, rejection, and requote tracking.
    """

    def __init__(
        self,
        _broker: 'BrokerBase',
        _partial_fill_enabled: bool = True,
        _max_partial_fill_retries: int = 3,
        _idempotency_enabled: bool = True,
        _execution_monitor: Optional['ExecutionQualityMonitor'] = None,
    ):
        """
        Initialize OrderExecutor.

        Args:
            _broker: Broker instance (must implement submit_order).
            _partial_fill_enabled: Enable partial fill re-entry (P2.1).
            _max_partial_fill_retries: Max re-entry attempts for unfilled
                quantity. Prevents infinite loops on persistent partial fills.
            _idempotency_enabled: Enable client_order_id tracking to prevent
                duplicate submissions (P2.2).
            _execution_monitor: Optional ExecutionQualityMonitor for tracking
                fill latency, rejections, requotes (P2.2).
        """
        self._broker = _broker
        self._partial_fill_enabled = _partial_fill_enabled
        self._max_partial_fill_retries = _max_partial_fill_retries
        self._idempotency_enabled = _idempotency_enabled
        self._execution_monitor = _execution_monitor

        # P2.2: Track submitted client_order_ids for idempotency
        self._submitted_ids: Set[str] = set()

    @staticmethod
    def generate_client_order_id(
        _strategy: str,
        _symbol: str,
        _timeframe: str,
        _signal_timestamp: str,
        _direction: str,
    ) -> str:
        """
        Generate a deterministic client_order_id from signal context.

        Maps signal timestamps to deterministic IDs so that retrying the
        same signal produces the same client_order_id. This ensures:
            - The broker rejects duplicates (if it supports idempotency)
            - Local tracking prevents re-submission of already-sent orders

        The ID is a truncated SHA-256 hash of the concatenated inputs,
        prefixed with 'co_' for readability. The 16-character hex suffix
        provides ~64 bits of collision resistance, which is more than
        sufficient for order deduplication within a single session.

        Args:
            _strategy: Strategy name.
            _symbol: Broker symbol.
            _timeframe: Timeframe string.
            _signal_timestamp: Signal bar timestamp (ISO format or any
                consistent string representation).
            _direction: Trade direction ("BUY" or "SELL").

        Returns:
            Deterministic client order ID string (e.g. "co_a1b2c3d4e5f67890").
        """
        payload = f"{_strategy}|{_symbol}|{_timeframe}|{_signal_timestamp}|{_direction}"
        hash_hex = hashlib.sha256(payload.encode('utf-8')).hexdigest()[:16]
        return f"co_{hash_hex}"

    def _check_idempotency(self, _client_order_id: Optional[str]) -> bool:
        """
        Check if a client_order_id has already been submitted.

        Args:
            _client_order_id: The client order ID to check.

        Returns:
            True if the order should proceed (not a duplicate).
            False if the order is a duplicate and should be skipped.
        """
        if not self._idempotency_enabled:
            return True

        if _client_order_id is None:
            return True

        if _client_order_id in self._submitted_ids:
            logger.warning(
                f"OrderExecutor: DUPLICATE client_order_id={_client_order_id}. "
                f"Order skipped (idempotency)."
            )
            print(f"  [OrderExecutor] DUPLICATE order skipped: "
                  f"client_order_id={_client_order_id}")
            return False

        return True

    def _mark_submitted(self, _client_order_id: Optional[str]) -> None:
        """
        Mark a client_order_id as submitted.

        Args:
            _client_order_id: The client order ID to mark.
        """
        if self._idempotency_enabled and _client_order_id is not None:
            self._submitted_ids.add(_client_order_id)

    def submit_market_order(
        self,
        _symbol: str,
        _side: OrderSide,
        _quantity: float,
        _stop_loss: Optional[float],
        _take_profit: Optional[float],
        _strategy: str,
        _client_order_id: Optional[str] = None,
        _expected_price: Optional[float] = None,
        _volatility_regime: str = "",
    ) -> Optional[Order]:
        """
        Submit a market order with SL/TP.

        If partial fill handling is enabled and the fill is partial,
        a follow-up market order is submitted for the remainder.

        P2.2: If idempotency is enabled and the client_order_id has already
        been submitted, the order is skipped and None is returned.

        Args:
            _symbol: Broker symbol (e.g. "XAUUSDp").
            _side: OrderSide.BUY or OrderSide.SELL.
            _quantity: Position size in lots.
            _stop_loss: Stop loss price (broker-managed).
            _take_profit: Take profit price (broker-managed).
            _strategy: Strategy name for tagging.
            _client_order_id: Optional client order ID for idempotency (P2.2).
            _expected_price: Expected fill price for execution quality tracking (P2.2).
            _volatility_regime: Current volatility regime for slippage bucketing (P2.2).

        Returns:
            Order object with broker-assigned ID (primary fill), or None
            if the order was skipped due to idempotency.

        Raises:
            OrderError: If order submission fails.
        """
        # --- P2.2: Idempotency check ---
        if not self._check_idempotency(_client_order_id):
            return None

        t_start = time.perf_counter()

        logger.info(
            f"OrderExecutor: submitting MARKET {_side.name} "
            f"{_quantity:.2f} lots {_symbol}, "
            f"SL={_stop_loss}, TP={_take_profit}, "
            f"strategy={_strategy}, "
            f"client_order_id={_client_order_id}"
        )

        # --- P2.2: Record submission to execution monitor ---
        if self._execution_monitor is not None:
            self._execution_monitor.record_submission(
                _symbol=_symbol,
                _strategy=_strategy,
                _side=_side.name,
                _quantity=_quantity,
                _order_type="MARKET",
                _expected_price=_expected_price,
                _volatility_regime=_volatility_regime,
            )

        try:
            order = self._broker.submit_order(
                _symbol=_symbol,
                _order_type=OrderType.MARKET,
                _side=_side,
                _quantity=_quantity,
                _stop_loss=_stop_loss,
                _take_profit=_take_profit,
                _client_order_id=_client_order_id,
                _strategy=_strategy,
            )
        except OrderError as e:
            logger.error(f"OrderExecutor: MARKET order failed — {e}")

            # --- P2.2: Record rejection ---
            if self._execution_monitor is not None:
                self._execution_monitor.record_rejection(
                    _symbol=_symbol,
                    _strategy=_strategy,
                    _side=_side.name,
                    _quantity=_quantity,
                    _order_type="MARKET",
                    _expected_price=_expected_price,
                    _rejection_reason=str(e),
                    _volatility_regime=_volatility_regime,
                )
            raise

        elapsed_ms = (time.perf_counter() - t_start) * 1000

        # --- P2.2: Mark as submitted for idempotency ---
        self._mark_submitted(_client_order_id)

        logger.info(
            f"OrderExecutor: MARKET order filled — "
            f"order_id={order.order_id}, price={order.price}, "
            f"filled={order.filled_quantity}/{_quantity}, "
            f"client_order_id={_client_order_id}, "
            f"elapsed={elapsed_ms:.1f}ms"
        )
        print(f"  [OrderExecutor] MARKET {_side.name} filled: "
              f"qty={_quantity:.2f}, price={order.price}, "
              f"SL={_stop_loss}, TP={_take_profit} ({elapsed_ms:.1f}ms)")

        # --- P2.2: Record fill to execution monitor ---
        if self._execution_monitor is not None and order.price is not None:
            self._execution_monitor.record_fill(
                _symbol=_symbol,
                _strategy=_strategy,
                _order_id=str(order.order_id),
                _fill_price=order.price,
                _fill_time_ms=elapsed_ms,
                _expected_price=_expected_price,
                _side=_side.name,
                _quantity=_quantity,
                _order_type="MARKET",
                _volatility_regime=_volatility_regime,
            )

        # --- P2.1: Partial fill handling ---
        if self._partial_fill_enabled:
            self._handle_partial_fill(
                _primary_order=order,
                _requested_quantity=_quantity,
                _symbol=_symbol,
                _side=_side,
                _stop_loss=_stop_loss,
                _take_profit=_take_profit,
                _strategy=_strategy,
                _volatility_regime=_volatility_regime,
            )

        return order

    def submit_limit_order(
        self,
        _symbol: str,
        _side: OrderSide,
        _quantity: float,
        _price: float,
        _stop_loss: Optional[float],
        _take_profit: Optional[float],
        _strategy: str,
        _client_order_id: Optional[str] = None,
        _expected_price: Optional[float] = None,
        _volatility_regime: str = "",
    ) -> Optional[Order]:
        """
        Submit a limit order with SL/TP.

        If partial fill handling is enabled and the fill is partial,
        a follow-up MARKET order is submitted for the remainder
        (since the limit price may no longer be achievable).

        P2.2: If idempotency is enabled and the client_order_id has already
        been submitted, the order is skipped and None is returned.

        Args:
            _symbol: Broker symbol.
            _side: OrderSide.BUY or OrderSide.SELL.
            _quantity: Position size in lots.
            _price: Limit price.
            _stop_loss: Stop loss price (broker-managed).
            _take_profit: Take profit price (broker-managed).
            _strategy: Strategy name for tagging.
            _client_order_id: Optional client order ID for idempotency (P2.2).
            _expected_price: Expected fill price for execution quality tracking (P2.2).
            _volatility_regime: Current volatility regime for slippage bucketing (P2.2).

        Returns:
            Order object with broker-assigned ID, or None if skipped.

        Raises:
            OrderError: If order submission fails.
        """
        # --- P2.2: Idempotency check ---
        if not self._check_idempotency(_client_order_id):
            return None

        t_start = time.perf_counter()

        logger.info(
            f"OrderExecutor: submitting LIMIT {_side.name} "
            f"{_quantity:.2f} lots {_symbol} @ {_price}, "
            f"SL={_stop_loss}, TP={_take_profit}, "
            f"strategy={_strategy}, "
            f"client_order_id={_client_order_id}"
        )

        # --- P2.2: Record submission ---
        if self._execution_monitor is not None:
            self._execution_monitor.record_submission(
                _symbol=_symbol,
                _strategy=_strategy,
                _side=_side.name,
                _quantity=_quantity,
                _order_type="LIMIT",
                _expected_price=_expected_price if _expected_price is not None else _price,
                _volatility_regime=_volatility_regime,
            )

        try:
            order = self._broker.submit_order(
                _symbol=_symbol,
                _order_type=OrderType.LIMIT,
                _side=_side,
                _quantity=_quantity,
                _price=_price,
                _stop_loss=_stop_loss,
                _take_profit=_take_profit,
                _client_order_id=_client_order_id,
                _strategy=_strategy,
            )
        except OrderError as e:
            logger.error(f"OrderExecutor: LIMIT order failed — {e}")

            # --- P2.2: Record rejection ---
            if self._execution_monitor is not None:
                self._execution_monitor.record_rejection(
                    _symbol=_symbol,
                    _strategy=_strategy,
                    _side=_side.name,
                    _quantity=_quantity,
                    _order_type="LIMIT",
                    _expected_price=_expected_price if _expected_price is not None else _price,
                    _rejection_reason=str(e),
                    _volatility_regime=_volatility_regime,
                )
            raise

        elapsed_ms = (time.perf_counter() - t_start) * 1000

        # --- P2.2: Mark as submitted ---
        self._mark_submitted(_client_order_id)

        logger.info(
            f"OrderExecutor: LIMIT order placed — "
            f"order_id={order.order_id}, price={_price}, "
            f"filled={order.filled_quantity}/{_quantity}, "
            f"client_order_id={_client_order_id}, "
            f"elapsed={elapsed_ms:.1f}ms"
        )
        print(f"  [OrderExecutor] LIMIT {_side.name} placed: "
              f"qty={_quantity:.2f}, limit={_price}, "
              f"SL={_stop_loss}, TP={_take_profit} ({elapsed_ms:.1f}ms)")

        # --- P2.2: Record fill if immediately filled ---
        if (self._execution_monitor is not None
                and order.filled_quantity is not None
                and order.price is not None):
            self._execution_monitor.record_fill(
                _symbol=_symbol,
                _strategy=_strategy,
                _order_id=str(order.order_id),
                _fill_price=order.price,
                _fill_time_ms=elapsed_ms,
                _expected_price=_expected_price if _expected_price is not None else _price,
                _side=_side.name,
                _quantity=order.filled_quantity,
                _order_type="LIMIT",
                _volatility_regime=_volatility_regime,
            )

        # --- P2.1: Partial fill handling for limit orders ---
        if self._partial_fill_enabled and order.filled_quantity is not None:
            self._handle_partial_fill(
                _primary_order=order,
                _requested_quantity=_quantity,
                _symbol=_symbol,
                _side=_side,
                _stop_loss=_stop_loss,
                _take_profit=_take_profit,
                _strategy=_strategy,
                _volatility_regime=_volatility_regime,
            )

        return order

    def _handle_partial_fill(
        self,
        _primary_order: Order,
        _requested_quantity: float,
        _symbol: str,
        _side: OrderSide,
        _stop_loss: Optional[float],
        _take_profit: Optional[float],
        _strategy: str,
        _volatility_regime: str = "",
    ) -> List[Order]:
        """
        Handle partial fills by re-entering for the unfilled quantity at market.

        If the primary order's filled_quantity is less than the requested
        quantity, submits follow-up MARKET orders for the remainder.

        Args:
            _primary_order: The primary order that may be partially filled.
            _requested_quantity: Originally requested quantity in lots.
            _symbol: Broker symbol.
            _side: Order side.
            _stop_loss: SL price for follow-up orders.
            _take_profit: TP price for follow-up orders.
            _strategy: Strategy name.
            _volatility_regime: Volatility regime for execution monitoring (P2.2).

        Returns:
            List of follow-up Order objects (empty if fully filled).
        """
        follow_ups: List[Order] = []

        filled = _primary_order.filled_quantity
        if filled is None:
            # If broker doesn't report filled quantity, assume full fill
            return follow_ups

        remaining = _requested_quantity - filled

        # Tolerance for floating point comparison (sub-lot residuals)
        if remaining < 0.001:
            return follow_ups

        logger.info(
            f"OrderExecutor: PARTIAL FILL detected — "
            f"filled={filled:.4f}/{_requested_quantity:.4f}, "
            f"remaining={remaining:.4f}"
        )
        print(f"  [OrderExecutor] PARTIAL FILL: "
              f"{filled:.4f}/{_requested_quantity:.4f} filled, "
              f"re-entering {remaining:.4f} at market")

        retry = 0
        while remaining >= 0.001 and retry < self._max_partial_fill_retries:
            retry += 1
            t_retry_start = time.perf_counter()

            logger.info(
                f"OrderExecutor: partial fill re-entry #{retry} — "
                f"MARKET {_side.name} {remaining:.4f} lots {_symbol}"
            )

            # --- P2.2: Record re-entry submission ---
            if self._execution_monitor is not None:
                self._execution_monitor.record_submission(
                    _symbol=_symbol,
                    _strategy=_strategy,
                    _side=_side.name,
                    _quantity=remaining,
                    _order_type="MARKET",
                    _expected_price=_primary_order.price,
                    _volatility_regime=_volatility_regime,
                )

            try:
                follow_up = self._broker.submit_order(
                    _symbol=_symbol,
                    _order_type=OrderType.MARKET,
                    _side=_side,
                    _quantity=remaining,
                    _stop_loss=_stop_loss,
                    _take_profit=_take_profit,
                    _strategy=_strategy,
                )
                follow_ups.append(follow_up)

                follow_filled = follow_up.filled_quantity or remaining
                remaining -= follow_filled

                retry_elapsed_ms = (time.perf_counter() - t_retry_start) * 1000

                logger.info(
                    f"OrderExecutor: re-entry #{retry} filled — "
                    f"order_id={follow_up.order_id}, "
                    f"filled={follow_filled:.4f}, "
                    f"remaining={remaining:.4f}"
                )
                print(f"  [OrderExecutor] Re-entry #{retry}: "
                      f"filled {follow_filled:.4f}, "
                      f"remaining {remaining:.4f}")

                # --- P2.2: Record re-entry fill ---
                if self._execution_monitor is not None and follow_up.price is not None:
                    self._execution_monitor.record_fill(
                        _symbol=_symbol,
                        _strategy=_strategy,
                        _order_id=str(follow_up.order_id),
                        _fill_price=follow_up.price,
                        _fill_time_ms=retry_elapsed_ms,
                        _expected_price=_primary_order.price,
                        _side=_side.name,
                        _quantity=follow_filled,
                        _order_type="MARKET",
                        _volatility_regime=_volatility_regime,
                    )

            except OrderError as e:
                logger.error(
                    f"OrderExecutor: partial fill re-entry #{retry} failed — {e}"
                )
                print(f"  [OrderExecutor] Re-entry #{retry} FAILED: {e}")

                # --- P2.2: Record re-entry rejection ---
                if self._execution_monitor is not None:
                    self._execution_monitor.record_rejection(
                        _symbol=_symbol,
                        _strategy=_strategy,
                        _side=_side.name,
                        _quantity=remaining,
                        _order_type="MARKET",
                        _expected_price=_primary_order.price,
                        _rejection_reason=str(e),
                        _volatility_regime=_volatility_regime,
                    )
                break

        if remaining >= 0.001:
            logger.warning(
                f"OrderExecutor: could not fill full quantity — "
                f"unfilled={remaining:.4f} after {retry} retries"
            )
            print(f"  [OrderExecutor] WARNING: {remaining:.4f} lots unfilled "
                  f"after {retry} retries")

        return follow_ups

    @property
    def submitted_order_ids(self) -> Set[str]:
        """Get the set of submitted client order IDs (P2.2)."""
        return set(self._submitted_ids)
