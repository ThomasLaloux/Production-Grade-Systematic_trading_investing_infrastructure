"""
Position Sizer Module
======================
Calculates position size with proper lot rounding.

Rules:
    - Round DOWN to nearest lot_step
    - Clamp to [min_lot_size, max_lot_size]
    - Same risk-based sizing as BacktestEngine._calculate_position_size
    - Lot sizing params (min_lot, max_lot, lot_step) from instruments.yaml

Classes:
    PositionSizer:
        - calculate: compute position size for a given signal

Usage:
    sizer = PositionSizer(_instrument_metadata=instrument_meta)
    quantity = sizer.calculate(
        _equity=100000, _risk_pct=0.005,
        _entry_price=1850.0, _stop_loss=1840.0,
    )
"""

import logging
import math
from typing import Optional

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from core.data_types import InstrumentMetadata

logger = logging.getLogger(__name__)


class PositionSizer:
    """
    Calculates position size with proper lot rounding.

    Uses instrument metadata from instruments.yaml for lot constraints.
    Risk-based sizing: Size = (Equity * risk_pct) / (SL_distance_in_pips * pip_value_per_lot)
    """

    def __init__(self, _instrument_metadata: InstrumentMetadata):
        """
        Initialize PositionSizer.

        Args:
            _instrument_metadata: Instrument metadata from instruments.yaml
                (contains pip_size, contract_size, min_lot_size, max_lot_size, lot_step).
        """
        self._instrument = _instrument_metadata

    def calculate(
        self,
        _equity: float,
        _risk_pct: float,
        _entry_price: float,
        _stop_loss: float,
    ) -> float:
        """
        Calculate position size based on risk.

        Size = (Equity * risk_pct) / (SL_distance_in_pips * pip_value_per_lot)

        Args:
            _equity: Current account equity.
            _risk_pct: Risk per trade as fraction of equity (e.g. 0.005 = 0.5%).
            _entry_price: Planned entry price.
            _stop_loss: Stop loss price.

        Returns:
            Position size in lots, rounded down to lot_step, clamped to [min, max].
        """
        sl_distance = abs(_entry_price - _stop_loss)
        if sl_distance == 0:
            logger.warning("PositionSizer: SL distance is zero, returning min lot size")
            return self._instrument.min_lot_size

        risk_amount = _equity * _risk_pct

        # Calculate pip value per 1.0 lot and SL distance in pips
        pip_value_per_lot = self._instrument.calculate_pip_value(1.0)
        sl_pips = sl_distance / self._instrument.pip_size

        if pip_value_per_lot <= 0 or sl_pips <= 0:
            logger.warning(
                f"PositionSizer: invalid pip_value ({pip_value_per_lot}) "
                f"or sl_pips ({sl_pips}), returning min lot size"
            )
            return self._instrument.min_lot_size

        quantity = risk_amount / (sl_pips * pip_value_per_lot)

        # Round DOWN to nearest lot_step
        lot_step = self._instrument.lot_step
        quantity = math.floor(quantity / lot_step) * lot_step

        # Clamp to [min_lot_size, max_lot_size]
        quantity = max(self._instrument.min_lot_size, min(quantity, self._instrument.max_lot_size))

        logger.info(
            f"PositionSizer: equity={_equity:.2f}, risk_pct={_risk_pct}, "
            f"sl_dist={sl_distance:.5f}, sl_pips={sl_pips:.1f}, "
            f"pip_val/lot={pip_value_per_lot:.2f}, "
            f"raw_qty={risk_amount / (sl_pips * pip_value_per_lot):.4f}, "
            f"rounded_qty={quantity:.2f}"
        )

        return quantity
