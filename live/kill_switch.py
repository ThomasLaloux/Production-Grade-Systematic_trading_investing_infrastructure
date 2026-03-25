"""
Kill Switch / Emergency Flatten Module (P2.2)
===============================================
Emergency mechanism to immediately:
    - Close all open positions
    - Cancel all pending orders
    - Halt the engine

Triggers (from design doc Section 7.2g):
    - Manual command (method call)
    - Risk limit breach (called by PreTradeRiskCheck or engine)
    - External signal via file sentinel (separate thread monitors a file)

Architecture:
    - Runs a lightweight background thread monitoring a sentinel file
    - When the sentinel file is detected, the kill switch activates
    - Also callable directly via activate() for programmatic triggers
    - Accessible outside the main loop (separate thread) as required by spec
    - Thread-safe: uses a lock for state and an Event for signaling
    - Logs all kill switch events to the trade journal if available

Sentinel File Protocol:
    - The kill switch monitors a configurable file path (e.g. "KILL_SWITCH")
    - If the file exists, the kill switch activates
    - After activation, the file is renamed to KILL_SWITCH.triggered.<timestamp>
    - To trigger externally: simply create/touch the sentinel file
    - This allows external scripts, webhooks, or manual intervention

Classes:
    KillSwitch:
        - activate: immediately flatten all positions and halt engine
        - start_monitor: start background file sentinel monitor
        - stop_monitor: stop background monitor
        - is_triggered: check if kill switch has been activated
        - reset: reset the kill switch (for testing / restart)

Usage:
    kill_switch = KillSwitch(
        _broker=broker,
        _sentinel_path="KILL_SWITCH",
        _poll_interval_seconds=1,
    )
    kill_switch.start_monitor()

    # External trigger: touch KILL_SWITCH file
    # Programmatic trigger:
    kill_switch.activate(_reason="TOTAL_DD_BREACH")

    # Check status:
    if kill_switch.is_triggered:
        engine.shutdown()
"""

import logging
import os
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

import sys
sys.path.insert(0, str(Path(__file__).parent.parent))

from core.exceptions import OrderError

logger = logging.getLogger(__name__)


class KillSwitch:
    """
    Emergency kill switch — flatten all positions and halt engine.

    Monitors a sentinel file in a background thread. When the file
    appears (or activate() is called programmatically), the kill switch:
        1. Closes all open positions
        2. Cancels all pending orders
        3. Sets the triggered flag (checked by the main loop)
        4. Logs the event

    Thread-safe: all state is protected by a lock.
    """

    def __init__(
        self,
        _broker: 'BrokerBase',
        _sentinel_path: str,
        _poll_interval_seconds: int,
    ):
        """
        Initialize KillSwitch.

        Args:
            _broker: Broker instance (must implement get_positions,
                close_position, get_pending_orders, cancel_order).
            _sentinel_path: Path to the sentinel file to monitor.
                If this file exists, the kill switch activates.
            _poll_interval_seconds: Seconds between sentinel file checks.
        """
        self._broker = _broker
        self._sentinel_path = Path(_sentinel_path)
        self._poll_interval_seconds = _poll_interval_seconds

        # State
        self._triggered = False
        self._trigger_reason: str = ""
        self._trigger_time: Optional[datetime] = None
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._lock = threading.Lock()

        # Callback for engine shutdown (set by LiveTradingEngine)
        self._engine_halt_callback: Optional[Callable[[], None]] = None

        # Trade journal reference (set by LiveTradingEngine)
        self._audit_trail: Optional[Any] = None

        logger.info(
            f"KillSwitch: initialized — sentinel={self._sentinel_path}, "
            f"poll_interval={_poll_interval_seconds}s"
        )

    def set_engine_halt_callback(self, _callback: Callable[[], None]) -> None:
        """
        Set the callback to halt the engine when kill switch activates.

        Args:
            _callback: Callable that sets the engine's _running flag to False.
        """
        self._engine_halt_callback = _callback

    def set_audit_trail(self, _journal: Any) -> None:
        """
        Set the trade journal for logging kill switch events.

        Args:
            _journal: AuditTrail instance.
        """
        self._audit_trail = _journal

    def start_monitor(self) -> None:
        """Start the background sentinel file monitor thread."""
        if self._running:
            logger.warning("KillSwitch: monitor already running")
            return

        self._running = True
        self._thread = threading.Thread(
            target=self._monitor_loop,
            name="kill-switch-monitor",
            daemon=True,
        )
        self._thread.start()
        logger.info(
            f"KillSwitch: monitor started "
            f"(sentinel={self._sentinel_path}, "
            f"poll={self._poll_interval_seconds}s)"
        )
        print(f"  [KillSwitch] Monitor started "
              f"(sentinel={self._sentinel_path}, "
              f"poll every {self._poll_interval_seconds}s)")

    def stop_monitor(self) -> None:
        """Stop the background sentinel file monitor thread."""
        self._running = False
        if self._thread is not None:
            self._thread.join(timeout=self._poll_interval_seconds + 2)
            self._thread = None
        logger.info("KillSwitch: monitor stopped")

    def activate(self, _reason: str = "MANUAL") -> Dict[str, Any]:
        """
        Activate the kill switch — flatten all positions, cancel orders, halt engine.

        This is the core method. Called by:
            - The sentinel file monitor thread (external trigger)
            - PreTradeRiskCheck when drawdown breach is detected
            - Manual invocation by the operator

        Args:
            _reason: Reason for activation (e.g. "MANUAL", "TOTAL_DD_BREACH",
                "DAILY_DD_BREACH", "SENTINEL_FILE", "EXTERNAL_SIGNAL").

        Returns:
            Dict with activation results:
                - reason: activation reason
                - positions_closed: number of positions closed
                - orders_cancelled: number of orders cancelled
                - errors: list of error messages (if any)
                - trigger_time: ISO timestamp of activation
        """
        with self._lock:
            if self._triggered:
                logger.warning(
                    f"KillSwitch: already triggered at {self._trigger_time} "
                    f"(reason: {self._trigger_reason}). Skipping re-activation."
                )
                return {
                    "reason": self._trigger_reason,
                    "positions_closed": 0,
                    "orders_cancelled": 0,
                    "errors": ["ALREADY_TRIGGERED"],
                    "trigger_time": self._trigger_time.isoformat() if self._trigger_time else "",
                }

            self._triggered = True
            self._trigger_reason = _reason
            self._trigger_time = datetime.now(timezone.utc)

        trigger_time_str = self._trigger_time.isoformat()

        logger.critical(
            f"KillSwitch: ACTIVATED — reason={_reason}, time={trigger_time_str}"
        )
        print(f"\n{'!'*60}")
        print(f"  KILL SWITCH ACTIVATED — {_reason}")
        print(f"  Time: {trigger_time_str}")
        print(f"{'!'*60}")

        results: Dict[str, Any] = {
            "reason": _reason,
            "positions_closed": 0,
            "orders_cancelled": 0,
            "errors": [],
            "trigger_time": trigger_time_str,
        }

        # --- 1. Cancel all pending orders ---
        try:
            pending_orders = self._broker.get_pending_orders()
            for order in pending_orders:
                try:
                    self._broker.cancel_order(_order_id=order.order_id)
                    results["orders_cancelled"] += 1
                    logger.info(
                        f"KillSwitch: cancelled order {order.order_id}"
                    )
                    print(f"  [KillSwitch] Cancelled order: {order.order_id}")
                except (OrderError, Exception) as e:
                    error_msg = f"Failed to cancel order {order.order_id}: {e}"
                    results["errors"].append(error_msg)
                    logger.error(f"KillSwitch: {error_msg}")
                    print(f"  [KillSwitch] ERROR: {error_msg}")
        except Exception as e:
            error_msg = f"Failed to get pending orders: {e}"
            results["errors"].append(error_msg)
            logger.error(f"KillSwitch: {error_msg}")

        # --- 2. Close all open positions ---
        try:
            positions = self._broker.get_positions()
            for pos in positions:
                try:
                    self._broker.close_position(_position_id=pos.position_id)
                    results["positions_closed"] += 1
                    logger.info(
                        f"KillSwitch: closed position {pos.position_id} "
                        f"({pos.symbol}, {pos.side.name}, qty={pos.quantity})"
                    )
                    print(f"  [KillSwitch] Closed position: {pos.position_id} "
                          f"({pos.symbol} {pos.side.name} {pos.quantity})")
                except (OrderError, Exception) as e:
                    error_msg = (
                        f"Failed to close position {pos.position_id}: {e}"
                    )
                    results["errors"].append(error_msg)
                    logger.error(f"KillSwitch: {error_msg}")
                    print(f"  [KillSwitch] ERROR: {error_msg}")
        except Exception as e:
            error_msg = f"Failed to get positions: {e}"
            results["errors"].append(error_msg)
            logger.error(f"KillSwitch: {error_msg}")

        # --- 3. Log to trade journal ---
        if self._audit_trail is not None:
            try:
                self._audit_trail.log(
                    _event_type="KILL_SWITCH",
                    _reason=_reason,
                    _details={
                        "positions_closed": results["positions_closed"],
                        "orders_cancelled": results["orders_cancelled"],
                        "errors": results["errors"],
                        "trigger_time": trigger_time_str,
                    },
                )
            except Exception as e:
                logger.error(f"KillSwitch: failed to log to journal — {e}")

        # --- 4. Halt engine ---
        if self._engine_halt_callback is not None:
            try:
                self._engine_halt_callback()
                logger.info("KillSwitch: engine halt callback invoked")
            except Exception as e:
                error_msg = f"Engine halt callback failed: {e}"
                results["errors"].append(error_msg)
                logger.error(f"KillSwitch: {error_msg}")

        # --- Summary ---
        print(f"\n  [KillSwitch] Summary:")
        print(f"    Positions closed: {results['positions_closed']}")
        print(f"    Orders cancelled: {results['orders_cancelled']}")
        if results["errors"]:
            print(f"    Errors: {len(results['errors'])}")
            for err in results["errors"]:
                print(f"      - {err}")
        print(f"{'!'*60}\n")

        logger.critical(
            f"KillSwitch: completed — "
            f"closed={results['positions_closed']}, "
            f"cancelled={results['orders_cancelled']}, "
            f"errors={len(results['errors'])}"
        )

        return results

    def _monitor_loop(self) -> None:
        """
        Background loop monitoring the sentinel file.

        When the sentinel file is detected:
            1. Activate the kill switch
            2. Rename the sentinel file to prevent re-triggering
        """
        while self._running:
            try:
                if self._sentinel_path.exists():
                    logger.critical(
                        f"KillSwitch: sentinel file detected: {self._sentinel_path}"
                    )

                    # Activate kill switch
                    self.activate(_reason="SENTINEL_FILE")

                    # Rename sentinel file to prevent re-triggering
                    timestamp_str = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S')
                    triggered_path = self._sentinel_path.with_suffix(
                        f".triggered.{timestamp_str}"
                    )
                    try:
                        self._sentinel_path.rename(triggered_path)
                        logger.info(
                            f"KillSwitch: sentinel file renamed to {triggered_path}"
                        )
                    except OSError as e:
                        logger.error(
                            f"KillSwitch: failed to rename sentinel file — {e}"
                        )

                    # Stop monitoring after activation
                    self._running = False
                    return

            except Exception as e:
                logger.error(f"KillSwitch: monitor error — {e}")

            # Sleep in small increments for responsive shutdown
            for _ in range(self._poll_interval_seconds):
                if not self._running:
                    return
                time.sleep(1)

    @property
    def is_triggered(self) -> bool:
        """Check if the kill switch has been activated. Thread-safe."""
        with self._lock:
            return self._triggered

    @property
    def trigger_reason(self) -> str:
        """Get the reason for activation."""
        with self._lock:
            return self._trigger_reason

    @property
    def trigger_time(self) -> Optional[datetime]:
        """Get the timestamp of activation."""
        with self._lock:
            return self._trigger_time

    def reset(self) -> None:
        """
        Reset the kill switch (for testing or engine restart).

        WARNING: This does NOT re-open closed positions. Use with caution.
        """
        with self._lock:
            self._triggered = False
            self._trigger_reason = ""
            self._trigger_time = None
        logger.info("KillSwitch: reset")
