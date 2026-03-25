"""
Paper Broker Module (P3.1)
============================
Simulated fill engine for paper trading mode.

Architecture:
    PaperBroker wraps a real broker (for live market data) and intercepts
    all order submission, position management, and account state with
    simulated logic. This allows the full live pipeline to execute without
    capital at risk:
        - Data feeds: delegated to the real broker (get_server_time,
          get_tick_data, get_instrument_metadata)
        - Order execution: simulated fill at current market price +/-
          configurable slippage
        - Positions: tracked in memory with SL/TP monitoring
        - Account: virtual balance/equity with P&L tracking

SL/TP Handling:
    When get_positions() is called (each cycle), PaperBroker checks every
    open position's SL/TP against the latest tick from the real broker.
    If SL or TP has been breached, the position is auto-closed at the
    stop/target price, and the P&L is credited to the virtual account.
    This mirrors how a real broker handles SL/TP intrabar — the check
    happens each cycle (bar close), so intrabar fills at exact SL/TP
    price are assumed (conservative fill assumption).

Fill Model:
    Market orders fill at:
        BUY:  ask + slippage_points
        SELL: bid - slippage_points
    where slippage_points = slippage_pips * pip_size.

    Commission is applied per-lot at fill time and deducted from the
    virtual balance immediately.

    Limit orders fill at the requested price if the current market
    price has crossed the limit level (simplified: immediate check
    only — no persistent pending orders in v1).

Classes:
    PaperBroker:
        - submit_order: simulated fill at market + slippage
        - get_positions: returns virtual positions (with SL/TP check)
        - get_account_info: returns virtual balance/equity
        - connect / disconnect / is_connected: delegated to real broker
        - get_server_time / get_tick_data / get_instrument_metadata:
          delegated to real broker
        - cancel_order / modify_order / get_pending_orders: no-ops or
          operate on virtual state

Usage:
    from live.paper_broker import PaperBroker

    paper = PaperBroker(
        _real_broker=broker,
        _data_configurator=data_configurator,
        _initial_balance=100000.0,
        _slippage_pips=0.5,
        _commission_per_lot=7.0,
        _default_pip_size=0.01,
        _default_contract_size=100.0,
    )
    # Use paper exactly like a real broker — same BrokerBase API
    order = paper.submit_order(
        _symbol="XAUUSDp", _order_type=OrderType.MARKET,
        _side=OrderSide.BUY, _quantity=0.1,
        _stop_loss=1840.0, _take_profit=1870.0,
    )
"""

import logging
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from core.data_types import (
    Order, Position, InstrumentMetadata,
    OrderType, OrderSide, OrderStatus, PositionSide,
)
from core.exceptions import OrderError, BrokerError

logger = logging.getLogger(__name__)


class PaperBroker:
    """
    Simulated fill engine for paper trading.

    Wraps a real broker for market data (tick prices, server time,
    instrument metadata) and intercepts all order/position/account
    operations with virtual state. Uses the same BrokerBase interface
    so it can be a drop-in replacement for any real broker in the
    LiveTradingEngine.

    P3.1: _paper_trade flag routes all orders here instead of the
    real broker. The full live pipeline (risk checks, spread filter,
    audit trail, slippage tracking, execution monitoring, kill switch,
    idempotency) operates identically — only the fill is simulated.
    """

    def __init__(
        self,
        _real_broker: 'BrokerBase',
        _data_configurator: 'DataConfigurator',
        _initial_balance: float = 100_000.0,
        _slippage_pips: float = 0.5,
        _commission_per_lot: float = 7.0,
        _default_pip_size: float = 0.01,
        _default_contract_size: float = 100.0,
    ):
        """
        Initialize PaperBroker.

        Args:
            _real_broker: Connected real broker instance for market data.
                Must implement get_server_time, get_tick_data,
                get_instrument_metadata, is_connected, connect, disconnect.
            _data_configurator: DataConfigurator for instrument metadata
                lookup (pip_size, contract_size, lot constraints).
            _initial_balance: Starting virtual balance (default: 100,000).
            _slippage_pips: Simulated slippage in pips applied to each
                fill. BUY fills at ask + slippage; SELL at bid - slippage.
            _commission_per_lot: Commission per lot (round-turn) deducted
                at fill time.
            _default_pip_size: Fallback pip size if instrument metadata
                is not available.
            _default_contract_size: Fallback contract size if instrument
                metadata is not available.
        """
        self._real_broker = _real_broker
        self._data_configurator = _data_configurator

        # --- Virtual account ---
        self._initial_balance = _initial_balance
        self._balance = _initial_balance
        self._realized_pnl = 0.0

        # --- Fill model ---
        self._slippage_pips = _slippage_pips
        self._commission_per_lot = _commission_per_lot
        self._default_pip_size = _default_pip_size
        self._default_contract_size = _default_contract_size

        # --- Virtual positions and orders ---
        self._positions: List[Position] = []
        self._closed_positions: List[Dict[str, Any]] = []
        self._orders: List[Order] = []
        self._order_counter = 0
        self._position_counter = 0

        # --- Instrument metadata cache ---
        self._instrument_cache: Dict[str, InstrumentMetadata] = {}

        logger.info(
            f"PaperBroker: initialized — balance={_initial_balance:.2f}, "
            f"slippage={_slippage_pips} pips, "
            f"commission={_commission_per_lot}/lot"
        )
        print(f"  [PaperBroker] Initialized: balance={_initial_balance:.2f}, "
              f"slippage={_slippage_pips} pips, "
              f"commission={_commission_per_lot}/lot")

    # =====================================================================
    #  Instrument metadata helpers
    # =====================================================================

    def _get_instrument(self, _symbol: str) -> Optional[InstrumentMetadata]:
        """
        Get instrument metadata, with caching.

        Tries the data configurator first, then falls back to the
        real broker, then defaults.

        Args:
            _symbol: Broker symbol.

        Returns:
            InstrumentMetadata or None if unavailable.
        """
        if _symbol in self._instrument_cache:
            return self._instrument_cache[_symbol]

        instrument = None

        # Try data configurator
        try:
            broker_name = self._real_broker.broker_name
            instrument = self._data_configurator.get_instrument(
                _symbol, _broker=broker_name,
            )
        except Exception:
            pass

        # Try real broker
        if instrument is None:
            try:
                instrument = self._real_broker.get_instrument_metadata(_symbol)
            except Exception:
                pass

        if instrument is not None:
            self._instrument_cache[_symbol] = instrument

        return instrument

    def _get_pip_size(self, _symbol: str) -> float:
        """Get pip size for symbol, with fallback."""
        inst = self._get_instrument(_symbol)
        if inst is not None:
            return inst.pip_size
        return self._default_pip_size

    def _get_contract_size(self, _symbol: str) -> float:
        """Get contract size for symbol, with fallback."""
        inst = self._get_instrument(_symbol)
        if inst is not None:
            return inst.contract_size
        return self._default_contract_size

    # =====================================================================
    #  Market data — delegated to real broker
    # =====================================================================

    def get_server_time(self, _symbol: str) -> datetime:
        """
        Get broker server time (delegated to real broker).

        Args:
            _symbol: Symbol to query.

        Returns:
            Timezone-aware datetime in UTC.
        """
        return self._real_broker.get_server_time(_symbol=_symbol)

    def get_tick_data(self, _symbol: str) -> Dict[str, Any]:
        """
        Get current bid/ask/last tick data (delegated to real broker).

        Args:
            _symbol: Broker symbol.

        Returns:
            Dict with 'bid', 'ask', 'last', 'time'.
        """
        return self._real_broker.get_tick_data(_symbol=_symbol)

    def get_instrument_metadata(self, _symbol: str) -> InstrumentMetadata:
        """
        Get instrument metadata (delegated to real broker).

        Args:
            _symbol: Broker symbol.

        Returns:
            InstrumentMetadata.
        """
        return self._real_broker.get_instrument_metadata(_symbol=_symbol)

    # =====================================================================
    #  Connection — delegated to real broker
    # =====================================================================

    def connect(self) -> bool:
        """Connect (delegated to real broker for data feed)."""
        return self._real_broker.connect()

    def disconnect(self) -> None:
        """Disconnect (delegated to real broker)."""
        self._real_broker.disconnect()

    def is_connected(self) -> bool:
        """Check connection (delegated to real broker)."""
        return self._real_broker.is_connected()

    @property
    def broker_name(self) -> str:
        """
        Broker name.

        Returns the real broker's name prefixed with 'paper_' to make
        it clear in logs and audit trails that this is a simulated
        execution environment.
        """
        return f"paper_{self._real_broker.broker_name}"

    # =====================================================================
    #  Order execution — simulated fills
    # =====================================================================

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
        """
        Submit a simulated order.

        Market orders are filled immediately at current market price
        +/- slippage. Limit orders are filled at the requested price
        if the current market has crossed the limit level.

        If _position_id is provided, this is a close order — the
        specified position is closed at current market price.

        Args:
            _symbol: Broker symbol.
            _order_type: MARKET or LIMIT.
            _side: BUY or SELL.
            _quantity: Size in lots.
            _price: Limit price (required for LIMIT orders).
            _stop_loss: SL price (attached to resulting position).
            _take_profit: TP price (attached to resulting position).
            _client_order_id: Optional client order ID for idempotency.
            _strategy: Strategy name for tagging.
            _position_id: If set, close this position instead of opening.

        Returns:
            Order with simulated fill details.

        Raises:
            OrderError: If order parameters are invalid or fill fails.
        """
        # Validate quantity
        if _quantity <= 0:
            raise OrderError(
                "Quantity must be positive",
                {"quantity": _quantity},
            )

        # Validate lot constraints from instrument metadata
        inst = self._get_instrument(_symbol)
        if inst is not None:
            if _quantity < inst.min_lot_size:
                raise OrderError(
                    f"Quantity {_quantity} below minimum {inst.min_lot_size}",
                    {"quantity": _quantity, "min": inst.min_lot_size},
                )
            if _quantity > inst.max_lot_size:
                raise OrderError(
                    f"Quantity {_quantity} above maximum {inst.max_lot_size}",
                    {"quantity": _quantity, "max": inst.max_lot_size},
                )

        # Handle position close
        if _position_id is not None:
            return self._close_position_order(
                _position_id=_position_id,
                _symbol=_symbol,
                _side=_side,
                _quantity=_quantity,
                _client_order_id=_client_order_id,
                _strategy=_strategy,
            )

        # Get current tick data for fill price
        try:
            tick = self._real_broker.get_tick_data(_symbol=_symbol)
        except Exception as e:
            raise OrderError(
                f"Cannot get tick data for simulated fill: {e}",
                {"symbol": _symbol},
            )

        bid = tick.get('bid', 0.0)
        ask = tick.get('ask', 0.0)

        if bid <= 0 or ask <= 0:
            raise OrderError(
                f"Invalid tick data for {_symbol}: bid={bid}, ask={ask}",
                {"symbol": _symbol, "bid": bid, "ask": ask},
            )

        pip_size = self._get_pip_size(_symbol)
        slippage_points = self._slippage_pips * pip_size

        # Calculate fill price
        if _order_type == OrderType.MARKET:
            if _side == OrderSide.BUY:
                fill_price = ask + slippage_points
            else:
                fill_price = bid - slippage_points
        elif _order_type == OrderType.LIMIT:
            if _price is None:
                raise OrderError(
                    "LIMIT order requires price",
                    {"order_type": "LIMIT"},
                )
            # Simplified limit fill: check if current market has crossed
            # the limit level. If so, fill at limit price.
            # BUY limit: fill if ask <= limit price
            # SELL limit: fill if bid >= limit price
            if _side == OrderSide.BUY and ask <= _price:
                fill_price = _price
            elif _side == OrderSide.SELL and bid >= _price:
                fill_price = _price
            else:
                raise OrderError(
                    f"LIMIT {_side.name} @ {_price} not filled — "
                    f"market bid={bid}, ask={ask}",
                    {"limit_price": _price, "bid": bid, "ask": ask},
                )
        elif _order_type in (OrderType.STOP, OrderType.STOP_LIMIT):
            # Simplified: treat STOP as MARKET for paper trading
            if _side == OrderSide.BUY:
                fill_price = ask + slippage_points
            else:
                fill_price = bid - slippage_points
        else:
            raise OrderError(
                f"Unsupported order type: {_order_type}",
                {"order_type": str(_order_type)},
            )

        # Check margin / sufficient balance
        contract_size = self._get_contract_size(_symbol)
        notional = fill_price * _quantity * contract_size
        commission = self._commission_per_lot * _quantity

        # Simple margin check: require at least 1% of notional + commission
        required_margin = notional * 0.01 + commission
        if self._balance < required_margin:
            raise OrderError(
                f"Insufficient paper balance for order: "
                f"required={required_margin:.2f}, available={self._balance:.2f}",
                {"required": required_margin, "balance": self._balance},
            )

        # Generate order ID
        self._order_counter += 1
        order_id = f"paper_{self._order_counter:06d}"

        # Deduct commission
        self._balance -= commission

        now = datetime.now(timezone.utc)

        # Create order
        order = Order(
            order_id=order_id,
            client_order_id=_client_order_id or "",
            symbol=_symbol,
            order_type=_order_type,
            side=_side,
            quantity=_quantity,
            price=fill_price,
            stop_loss=_stop_loss,
            take_profit=_take_profit,
            status=OrderStatus.FILLED,
            filled_quantity=_quantity,
            average_fill_price=fill_price,
            created_at=now,
            updated_at=now,
            broker=self.broker_name,
            strategy=_strategy or "",
            metadata={
                "paper_trade": True,
                "bid_at_fill": bid,
                "ask_at_fill": ask,
                "slippage_pips": self._slippage_pips,
                "commission": commission,
            },
        )
        self._orders.append(order)

        # Create virtual position
        self._position_counter += 1
        position_id = f"paper_pos_{self._position_counter:06d}"

        pos_side = (PositionSide.LONG if _side == OrderSide.BUY
                    else PositionSide.SHORT)

        position = Position(
            position_id=position_id,
            client_position_id=_client_order_id or "",
            symbol=_symbol,
            side=pos_side,
            quantity=_quantity,
            entry_price=fill_price,
            current_price=fill_price,
            unrealized_pnl=0.0,
            realized_pnl=0.0,
            stop_loss=_stop_loss,
            take_profit=_take_profit,
            opened_at=now,
            broker=self.broker_name,
            strategy=_strategy or "",
            metadata={
                "paper_trade": True,
                "order_id": order_id,
                "commission": commission,
            },
        )
        self._positions.append(position)

        logger.info(
            f"PaperBroker: {_order_type.name} {_side.name} filled — "
            f"order_id={order_id}, price={fill_price:.5f}, "
            f"qty={_quantity:.4f}, SL={_stop_loss}, TP={_take_profit}, "
            f"commission={commission:.2f}, "
            f"bid={bid:.5f}, ask={ask:.5f}, "
            f"slippage_pips={self._slippage_pips}"
        )
        print(
            f"  [PaperBroker] {_side.name} filled @ {fill_price:.5f} "
            f"(bid={bid:.5f}, ask={ask:.5f}, "
            f"slip={self._slippage_pips}pip, "
            f"comm={commission:.2f})"
        )

        return order

    def _close_position_order(
        self,
        _position_id: str,
        _symbol: str,
        _side: OrderSide,
        _quantity: float,
        _client_order_id: Optional[str] = None,
        _strategy: Optional[str] = None,
    ) -> Order:
        """
        Close a virtual position.

        Args:
            _position_id: ID of the position to close.
            _symbol: Broker symbol.
            _side: Close side (opposite of position side).
            _quantity: Quantity to close.
            _client_order_id: Optional client order ID.
            _strategy: Strategy name.

        Returns:
            Order representing the close fill.

        Raises:
            OrderError: If position not found.
        """
        pos = next(
            (p for p in self._positions if p.position_id == _position_id),
            None,
        )
        if pos is None:
            raise OrderError(
                f"Paper position not found: {_position_id}",
                {"position_id": _position_id},
            )

        # Get current tick for close price
        try:
            tick = self._real_broker.get_tick_data(_symbol=_symbol)
        except Exception as e:
            raise OrderError(
                f"Cannot get tick data for close: {e}",
                {"symbol": _symbol},
            )

        bid = tick.get('bid', 0.0)
        ask = tick.get('ask', 0.0)
        pip_size = self._get_pip_size(_symbol)
        slippage_points = self._slippage_pips * pip_size

        # Close price: opposite direction
        if pos.side == PositionSide.LONG:
            close_price = bid - slippage_points
        else:
            close_price = ask + slippage_points

        # Calculate P&L
        pnl = self._calculate_pnl(
            _symbol=_symbol,
            _side=pos.side,
            _entry_price=pos.entry_price,
            _exit_price=close_price,
            _quantity=pos.quantity,
        )

        # Update account
        commission = self._commission_per_lot * pos.quantity
        self._balance += pnl - commission
        self._realized_pnl += pnl - commission

        # Generate close order
        self._order_counter += 1
        order_id = f"paper_{self._order_counter:06d}"
        now = datetime.now(timezone.utc)

        order = Order(
            order_id=order_id,
            client_order_id=_client_order_id or "",
            symbol=_symbol,
            order_type=OrderType.MARKET,
            side=_side,
            quantity=pos.quantity,
            price=close_price,
            status=OrderStatus.FILLED,
            filled_quantity=pos.quantity,
            average_fill_price=close_price,
            created_at=now,
            updated_at=now,
            broker=self.broker_name,
            strategy=_strategy or pos.strategy,
            metadata={
                "paper_trade": True,
                "close_reason": "manual",
                "pnl": pnl,
                "commission": commission,
                "position_id": _position_id,
            },
        )
        self._orders.append(order)

        # Archive and remove position
        self._closed_positions.append({
            "position": pos,
            "close_order": order,
            "pnl": pnl,
            "commission": commission,
            "closed_at": now,
        })
        self._positions = [
            p for p in self._positions if p.position_id != _position_id
        ]

        logger.info(
            f"PaperBroker: position {_position_id} closed — "
            f"price={close_price:.5f}, pnl={pnl:.2f}, "
            f"commission={commission:.2f}"
        )
        print(
            f"  [PaperBroker] Position {_position_id} closed @ "
            f"{close_price:.5f} (P&L={pnl:.2f}, comm={commission:.2f})"
        )

        return order

    def close_position(self, _position_id: str) -> Order:
        """
        Close a virtual position by ID.

        Determines the correct close side and delegates to submit_order
        (matching the BrokerBase.close_position pattern).

        Args:
            _position_id: Position ID to close.

        Returns:
            Order representing the close fill.

        Raises:
            OrderError: If position not found.
        """
        pos = next(
            (p for p in self._positions if p.position_id == _position_id),
            None,
        )
        if pos is None:
            raise OrderError(
                f"Paper position not found: {_position_id}",
                {"position_id": _position_id},
            )

        close_side = (OrderSide.SELL if pos.side == PositionSide.LONG
                      else OrderSide.BUY)

        return self.submit_order(
            _symbol=pos.symbol,
            _order_type=OrderType.MARKET,
            _side=close_side,
            _quantity=pos.quantity,
            _position_id=_position_id,
        )

    # =====================================================================
    #  Position management — virtual state with SL/TP monitoring
    # =====================================================================

    def get_positions(
        self,
        _symbol: Optional[str] = None,
        _side: Optional[PositionSide] = None,
        _strategy: Optional[str] = None,
    ) -> List[Position]:
        """
        Get open virtual positions with SL/TP check.

        Before returning positions, checks each position's SL/TP
        against the latest tick. If breached, the position is auto-
        closed at the stop/target price. This simulates the broker's
        intrabar SL/TP execution.

        Args:
            _symbol: Filter by symbol.
            _side: Filter by position side.
            _strategy: Filter by strategy name.

        Returns:
            List of open virtual positions.
        """
        # --- Check SL/TP for all positions ---
        self._check_sl_tp()

        # --- Filter ---
        result = list(self._positions)
        if _symbol is not None:
            result = [p for p in result if p.symbol == _symbol]
        if _side is not None:
            result = [p for p in result if p.side == _side]
        if _strategy is not None:
            result = [p for p in result if p.strategy == _strategy]

        # --- Update unrealized P&L ---
        for pos in result:
            try:
                tick = self._real_broker.get_tick_data(_symbol=pos.symbol)
                mid = (tick.get('bid', 0.0) + tick.get('ask', 0.0)) / 2.0
                if mid > 0:
                    pnl = self._calculate_pnl(
                        _symbol=pos.symbol,
                        _side=pos.side,
                        _entry_price=pos.entry_price,
                        _exit_price=mid,
                        _quantity=pos.quantity,
                    )
                    pos.current_price = mid
                    pos.unrealized_pnl = pnl
            except Exception:
                pass

        return result

    def _check_sl_tp(self) -> None:
        """
        Check all open positions for SL/TP breach.

        For each position with SL or TP set, get the latest tick and
        check if the stop or target has been hit. If so, auto-close
        at the SL/TP price (conservative fill assumption — no slippage
        on stops for simplicity; real stops can gap but this is paper).

        Long positions:
            SL hit if bid <= SL price
            TP hit if bid >= TP price
        Short positions:
            SL hit if ask >= SL price
            TP hit if ask <= TP price
        """
        positions_to_close: List[Dict[str, Any]] = []

        for pos in self._positions:
            if pos.stop_loss is None and pos.take_profit is None:
                continue

            try:
                tick = self._real_broker.get_tick_data(_symbol=pos.symbol)
            except Exception:
                continue

            bid = tick.get('bid', 0.0)
            ask = tick.get('ask', 0.0)

            if bid <= 0 or ask <= 0:
                continue

            close_price = None
            close_reason = None

            if pos.side == PositionSide.LONG:
                # SL check (exit at bid)
                if pos.stop_loss is not None and bid <= pos.stop_loss:
                    close_price = pos.stop_loss
                    close_reason = "SL_HIT"
                # TP check (exit at bid)
                elif pos.take_profit is not None and bid >= pos.take_profit:
                    close_price = pos.take_profit
                    close_reason = "TP_HIT"
            else:  # SHORT
                # SL check (exit at ask)
                if pos.stop_loss is not None and ask >= pos.stop_loss:
                    close_price = pos.stop_loss
                    close_reason = "SL_HIT"
                # TP check (exit at ask)
                elif pos.take_profit is not None and ask <= pos.take_profit:
                    close_price = pos.take_profit
                    close_reason = "TP_HIT"

            if close_price is not None:
                positions_to_close.append({
                    "position": pos,
                    "close_price": close_price,
                    "reason": close_reason,
                })

        # Close triggered positions
        for item in positions_to_close:
            pos = item["position"]
            close_price = item["close_price"]
            reason = item["reason"]

            pnl = self._calculate_pnl(
                _symbol=pos.symbol,
                _side=pos.side,
                _entry_price=pos.entry_price,
                _exit_price=close_price,
                _quantity=pos.quantity,
            )

            # Commission on close
            commission = self._commission_per_lot * pos.quantity
            self._balance += pnl - commission
            self._realized_pnl += pnl - commission

            # Generate close order
            self._order_counter += 1
            order_id = f"paper_{self._order_counter:06d}"
            now = datetime.now(timezone.utc)

            close_side = (OrderSide.SELL if pos.side == PositionSide.LONG
                          else OrderSide.BUY)

            order = Order(
                order_id=order_id,
                symbol=pos.symbol,
                order_type=OrderType.MARKET,
                side=close_side,
                quantity=pos.quantity,
                price=close_price,
                status=OrderStatus.FILLED,
                filled_quantity=pos.quantity,
                average_fill_price=close_price,
                created_at=now,
                updated_at=now,
                broker=self.broker_name,
                strategy=pos.strategy,
                metadata={
                    "paper_trade": True,
                    "close_reason": reason,
                    "pnl": pnl,
                    "commission": commission,
                    "position_id": pos.position_id,
                },
            )
            self._orders.append(order)

            self._closed_positions.append({
                "position": pos,
                "close_order": order,
                "pnl": pnl,
                "commission": commission,
                "closed_at": now,
                "reason": reason,
            })

            self._positions = [
                p for p in self._positions
                if p.position_id != pos.position_id
            ]

            logger.info(
                f"PaperBroker: {reason} — position {pos.position_id} "
                f"closed @ {close_price:.5f}, "
                f"entry={pos.entry_price:.5f}, pnl={pnl:.2f}, "
                f"commission={commission:.2f}"
            )
            print(
                f"  [PaperBroker] {reason}: {pos.position_id} "
                f"closed @ {close_price:.5f} "
                f"(entry={pos.entry_price:.5f}, P&L={pnl:.2f})"
            )

    def has_open_position(
        self,
        _symbol: Optional[str] = None,
        _side: Optional[PositionSide] = None,
        _strategy: Optional[str] = None,
    ) -> bool:
        """Check if any open virtual position exists matching filters."""
        return len(self.get_positions(_symbol, _side, _strategy)) > 0

    def count_positions(
        self,
        _symbol: Optional[str] = None,
        _side: Optional[PositionSide] = None,
        _strategy: Optional[str] = None,
    ) -> int:
        """Count open virtual positions matching filters."""
        return len(self.get_positions(_symbol, _side, _strategy))

    # =====================================================================
    #  Account — virtual state
    # =====================================================================

    def get_account_info(self) -> Dict[str, Any]:
        """
        Get virtual account information.

        Equity = balance + sum of unrealized P&L on open positions.

        Returns:
            Dict with balance, equity, margin_used, margin_available,
            realized_pnl, initial_balance, open_positions, closed_trades,
            paper_trade flag.
        """
        unrealized = 0.0
        for pos in self._positions:
            try:
                tick = self._real_broker.get_tick_data(_symbol=pos.symbol)
                mid = (tick.get('bid', 0.0) + tick.get('ask', 0.0)) / 2.0
                if mid > 0:
                    pnl = self._calculate_pnl(
                        _symbol=pos.symbol,
                        _side=pos.side,
                        _entry_price=pos.entry_price,
                        _exit_price=mid,
                        _quantity=pos.quantity,
                    )
                    unrealized += pnl
            except Exception:
                pass

        equity = self._balance + unrealized

        # Simple margin calculation (1% of notional)
        margin_used = 0.0
        for pos in self._positions:
            contract_size = self._get_contract_size(pos.symbol)
            margin_used += pos.entry_price * pos.quantity * contract_size * 0.01

        return {
            "balance": self._balance,
            "equity": equity,
            "margin_used": margin_used,
            "margin_available": equity - margin_used,
            "unrealized_pnl": unrealized,
            "realized_pnl": self._realized_pnl,
            "initial_balance": self._initial_balance,
            "open_positions": len(self._positions),
            "closed_trades": len(self._closed_positions),
            "paper_trade": True,
        }

    def get_pending_orders(
        self, _symbol: Optional[str] = None,
    ) -> List[Order]:
        """
        Get pending orders (none in paper mode — all fills are instant).

        Args:
            _symbol: Optional symbol filter.

        Returns:
            Empty list (paper mode fills all orders immediately).
        """
        return []

    def cancel_order(self, _order_id: str) -> bool:
        """
        Cancel a pending order (no-op in paper mode).

        Args:
            _order_id: Order ID.

        Returns:
            False (no pending orders in paper mode).
        """
        logger.info(f"PaperBroker: cancel_order — no-op (paper mode)")
        return False

    def modify_order(
        self,
        _order_id: str,
        _price: Optional[float] = None,
        _stop_loss: Optional[float] = None,
        _take_profit: Optional[float] = None,
    ) -> Order:
        """
        Modify an existing order or position's SL/TP.

        In paper mode, this modifies the SL/TP on the virtual position
        associated with the most recent order.

        Args:
            _order_id: Order ID.
            _price: New price (ignored for positions).
            _stop_loss: New SL.
            _take_profit: New TP.

        Returns:
            The modified order.

        Raises:
            OrderError: If order not found.
        """
        order = next(
            (o for o in self._orders if o.order_id == _order_id),
            None,
        )
        if order is None:
            raise OrderError(
                f"Paper order not found: {_order_id}",
                {"order_id": _order_id},
            )

        # Update SL/TP on associated position
        for pos in self._positions:
            pos_order_id = pos.metadata.get("order_id", "")
            if pos_order_id == _order_id or pos.position_id == _order_id:
                if _stop_loss is not None:
                    pos.stop_loss = _stop_loss
                if _take_profit is not None:
                    pos.take_profit = _take_profit
                break

        if _stop_loss is not None:
            order.stop_loss = _stop_loss
        if _take_profit is not None:
            order.take_profit = _take_profit
        order.updated_at = datetime.now(timezone.utc)

        return order

    # =====================================================================
    #  P&L calculation
    # =====================================================================

    def _calculate_pnl(
        self,
        _symbol: str,
        _side: PositionSide,
        _entry_price: float,
        _exit_price: float,
        _quantity: float,
    ) -> float:
        """
        Calculate P&L for a position.

        P&L = (exit - entry) * quantity * contract_size for LONG
        P&L = (entry - exit) * quantity * contract_size for SHORT

        Args:
            _symbol: Broker symbol.
            _side: Position side.
            _entry_price: Entry fill price.
            _exit_price: Exit fill price.
            _quantity: Position size in lots.

        Returns:
            P&L in account currency.
        """
        contract_size = self._get_contract_size(_symbol)

        if _side == PositionSide.LONG:
            pnl = (_exit_price - _entry_price) * _quantity * contract_size
        else:
            pnl = (_entry_price - _exit_price) * _quantity * contract_size

        return pnl

    # =====================================================================
    #  Summary / reporting
    # =====================================================================

    def get_paper_summary(self) -> Dict[str, Any]:
        """
        Get paper trading session summary.

        Returns:
            Dict with initial_balance, final_balance, final_equity,
            total_pnl, total_pnl_pct, total_trades, open_positions,
            winning_trades, losing_trades, win_rate, total_commission.
        """
        account = self.get_account_info()

        total_trades = len(self._closed_positions)
        wins = sum(1 for t in self._closed_positions if t['pnl'] > 0)
        losses = sum(1 for t in self._closed_positions if t['pnl'] <= 0)
        total_commission = sum(t['commission'] for t in self._closed_positions)
        # Also include open trade commissions
        for pos in self._positions:
            total_commission += pos.metadata.get('commission', 0.0)

        final_equity = account['equity']
        total_pnl = final_equity - self._initial_balance
        total_pnl_pct = (
            total_pnl / self._initial_balance
            if self._initial_balance > 0 else 0.0
        )

        return {
            "initial_balance": self._initial_balance,
            "final_balance": self._balance,
            "final_equity": final_equity,
            "total_pnl": total_pnl,
            "total_pnl_pct": total_pnl_pct,
            "total_trades": total_trades,
            "open_positions": len(self._positions),
            "winning_trades": wins,
            "losing_trades": losses,
            "win_rate": wins / total_trades if total_trades > 0 else 0.0,
            "total_commission": total_commission,
            "realized_pnl": self._realized_pnl,
            "unrealized_pnl": account['unrealized_pnl'],
        }

    @property
    def closed_trades(self) -> List[Dict[str, Any]]:
        """Get list of closed paper trades for analysis."""
        return list(self._closed_positions)

    @property
    def virtual_balance(self) -> float:
        """Get current virtual balance (excluding unrealized P&L)."""
        return self._balance

    @property
    def initial_balance(self) -> float:
        """Get initial virtual balance."""
        return self._initial_balance
