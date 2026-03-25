"""
Pre-Trade Risk Checks Module
==============================
Gate between signal and order execution. Validates that account-level
and strategy-level risk limits are not breached before submitting an order.

Checks:
    1. Max daily loss limit (cumulative daily P&L vs balance at start of day)
    2. Max total drawdown from initial equity (kill switch)
    3. Max trades per day (prevents runaway loops)
    4. Max position size per instrument (portfolio-level cap)
    5. Max exposure per asset class (placeholder for v2 multi-strategy)

This is a regulatory and operational necessity — even the smallest
prop desk runs pre-trade risk checks.

Classes:
    PreTradeRiskCheck:
        - approve: validate signal against all risk limits
        - reset_daily: reset daily counters (called on new trading day)

Usage:
    risk_check = PreTradeRiskCheck(
        _max_daily_dd_pct=0.05,
        _max_total_dd_pct=0.10,
        _max_trades_per_day=0,
        _max_position_lots=100.0,
    )
    approved, reason = risk_check.approve(
        _equity=99000, _daily_start_equity=100000,
        _initial_equity=100000, _daily_trade_count=3,
        _quantity=0.5,
    )
"""

import logging
import time
from typing import Tuple

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

logger = logging.getLogger(__name__)


class PreTradeRiskCheck:
    """
    Gate between signal and order execution.

    Validates account-level and strategy-level risk limits before
    any order is submitted. All thresholds are explicit — no hidden defaults.
    """

    def __init__(
        self,
        _max_daily_dd_pct: float,
        _max_total_dd_pct: float,
        _max_trades_per_day: int,
        _max_position_lots: float,
    ):
        """
        Initialize PreTradeRiskCheck.

        Args:
            _max_daily_dd_pct: Max daily drawdown as fraction of equity at
                start of day (e.g. 0.05 = 5%).
            _max_total_dd_pct: Max total drawdown as fraction of initial
                equity (e.g. 0.10 = 10%). Kill switch threshold.
            _max_trades_per_day: Max trades per day. 0 = unlimited.
            _max_position_lots: Max position size in lots for a single order.
                From instruments.yaml max_lot_size or a portfolio-level cap.
        """
        self._max_daily_dd_pct = _max_daily_dd_pct
        self._max_total_dd_pct = _max_total_dd_pct
        self._max_trades_per_day = _max_trades_per_day
        self._max_position_lots = _max_position_lots

    def approve(
        self,
        _equity: float,
        _daily_start_equity: float,
        _initial_equity: float,
        _daily_trade_count: int,
        _quantity: float,
    ) -> Tuple[bool, str]:
        """
        Validate signal against all risk limits.

        Args:
            _equity: Current account equity.
            _daily_start_equity: Equity at start of current trading day.
            _initial_equity: Equity at engine startup.
            _daily_trade_count: Number of trades already executed today.
            _quantity: Proposed position size in lots.

        Returns:
            Tuple of (approved: bool, reason: str).
            If approved=True, reason is "PASSED".
            If approved=False, reason describes which check failed.
        """
        t_start = time.perf_counter()

        # --- 1. Max daily drawdown ---
        if _daily_start_equity > 0:
            daily_dd = (_daily_start_equity - _equity) / _daily_start_equity
            if daily_dd >= self._max_daily_dd_pct:
                reason = (
                    f"DAILY_DD_BREACH: daily drawdown {daily_dd:.2%} "
                    f">= limit {self._max_daily_dd_pct:.2%}"
                )
                logger.warning(f"PreTradeRiskCheck: {reason}")
                print(f"  [RiskCheck] REJECTED — {reason}")
                return False, reason

        # --- 2. Max total drawdown (kill switch) ---
        if _initial_equity > 0:
            total_dd = (_initial_equity - _equity) / _initial_equity
            if total_dd >= self._max_total_dd_pct:
                reason = (
                    f"TOTAL_DD_BREACH: total drawdown {total_dd:.2%} "
                    f">= limit {self._max_total_dd_pct:.2%}"
                )
                logger.warning(f"PreTradeRiskCheck: {reason}")
                print(f"  [RiskCheck] REJECTED — {reason}")
                return False, reason

        # --- 3. Max trades per day ---
        if self._max_trades_per_day > 0:
            if _daily_trade_count >= self._max_trades_per_day:
                reason = (
                    f"MAX_TRADES_BREACH: {_daily_trade_count} trades today "
                    f">= limit {self._max_trades_per_day}"
                )
                logger.warning(f"PreTradeRiskCheck: {reason}")
                print(f"  [RiskCheck] REJECTED — {reason}")
                return False, reason

        # --- 4. Max position size ---
        if _quantity > self._max_position_lots:
            reason = (
                f"MAX_POSITION_BREACH: quantity {_quantity:.2f} lots "
                f"> limit {self._max_position_lots:.2f} lots"
            )
            logger.warning(f"PreTradeRiskCheck: {reason}")
            print(f"  [RiskCheck] REJECTED — {reason}")
            return False, reason

        # --- All checks passed ---
        elapsed_ms = (time.perf_counter() - t_start) * 1000
        logger.info(
            f"PreTradeRiskCheck: APPROVED "
            f"(daily_dd={(_daily_start_equity - _equity) / max(_daily_start_equity, 1):.2%}, "
            f"total_dd={(_initial_equity - _equity) / max(_initial_equity, 1):.2%}, "
            f"trades_today={_daily_trade_count}, "
            f"qty={_quantity:.2f}) "
            f"({elapsed_ms:.1f}ms)"
        )
        print(f"  [RiskCheck] APPROVED ({elapsed_ms:.1f}ms)")

        return True, "PASSED"
