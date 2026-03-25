"""
State Reconciler Module
========================
Reconciles local state with broker state.

Stateless design: we DON'T maintain local position state.
Instead, we query the broker each cycle for the ground truth.

Handles: startup, reconnection, and periodic sync.

Note: For M1 strategies with multiple instruments, sequential broker
queries add up. This is an optimization point for multi-strategy/
multi-instrument deployments (future: cache with invalidation or
batch queries). For v1 single-instrument, querying each cycle is fine.

Classes:
    StateReconciler:
        - get_positions: query broker for open positions
        - has_position: check if a position exists for symbol/strategy
        - get_account_state: query broker for account info

Usage:
    reconciler = StateReconciler(_broker=broker)
    positions = reconciler.get_positions(_symbol="XAUUSDp", _strategy="trend_retracement")
    has_pos = reconciler.has_position(_symbol="XAUUSDp", _strategy="trend_retracement")
"""

import logging
import time
from typing import Any, Dict, List, Optional

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from core.data_types import Position, PositionSide

logger = logging.getLogger(__name__)


class StateReconciler:
    """
    Reconciles local view with broker state.

    Stateless: queries broker each cycle. No local position cache.
    This ensures consistency — the broker is always the source of truth.
    """

    def __init__(self, _broker: 'BrokerBase'):
        """
        Initialize StateReconciler.

        Args:
            _broker: Broker instance (must implement get_positions, get_account_info).
        """
        self._broker = _broker

    def get_positions(
        self,
        _symbol: Optional[str] = None,
        _side: Optional[PositionSide] = None,
        _strategy: Optional[str] = None,
    ) -> List[Position]:
        """
        Query broker for open positions with optional filters.

        Args:
            _symbol: Filter by symbol.
            _side: Filter by position side.
            _strategy: Filter by strategy name.

        Returns:
            List of Position objects from broker.
        """
        t_start = time.perf_counter()

        positions = self._broker.get_positions(
            _symbol=_symbol, _side=_side, _strategy=_strategy,
        )

        elapsed_ms = (time.perf_counter() - t_start) * 1000
        logger.debug(
            f"StateReconciler: queried positions — "
            f"count={len(positions)}, filters=(symbol={_symbol}, "
            f"side={_side}, strategy={_strategy}), "
            f"elapsed={elapsed_ms:.1f}ms"
        )
        print(f"  [StateReconciler] positions: {len(positions)} "
              f"(symbol={_symbol}, strategy={_strategy}) ({elapsed_ms:.1f}ms)")

        return positions

    def has_position(
        self,
        _symbol: Optional[str] = None,
        _side: Optional[PositionSide] = None,
        _strategy: Optional[str] = None,
    ) -> bool:
        """
        Check if any open position exists matching filters.

        Args:
            _symbol: Filter by symbol.
            _side: Filter by position side.
            _strategy: Filter by strategy name.

        Returns:
            True if at least one matching position exists.
        """
        positions = self.get_positions(
            _symbol=_symbol, _side=_side, _strategy=_strategy,
        )
        return len(positions) > 0

    def get_account_state(self) -> Dict[str, Any]:
        """
        Query broker for account information.

        Returns:
            Dict with balance, equity, margin_used, margin_available, etc.
        """
        t_start = time.perf_counter()

        account = self._broker.get_account_info()

        elapsed_ms = (time.perf_counter() - t_start) * 1000
        logger.debug(
            f"StateReconciler: queried account — "
            f"balance={account.get('balance')}, "
            f"equity={account.get('equity')}, "
            f"elapsed={elapsed_ms:.1f}ms"
        )
        print(f"  [StateReconciler] account: balance={account.get('balance')}, "
              f"equity={account.get('equity')} ({elapsed_ms:.1f}ms)")

        return account
